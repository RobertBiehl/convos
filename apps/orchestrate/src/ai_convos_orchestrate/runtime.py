"""Session runtime: start, stop, and resume headless agents, and deliver the user's words to any session.

Headless sessions started from the IDE are owned here and receive messages on stdin. Joined sessions
receive them through their provider's own channel (Claude Code messaging socket, codex queue);
whatever cannot be delivered stays pending for the agent's `inbox`.
"""
import collections, json, os, signal, subprocess, sys, threading, time, uuid
from pathlib import Path
from . import store, channels

PROVIDERS = {"claude-code": dict(label="Claude Code", models=["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5", "claude-fable-5-1"], efforts=["low", "medium", "high", "xhigh", "max"], permissions=["default", "acceptEdits", "plan", "bypassPermissions"])}
INHERITED = ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_PID", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_EFFORT", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "CONVOS_ORCHESTRATE_SCOPE", "CONVOS_ORCHESTRATE_AS")
SYSTEM = ("You were started from Convos Orchestrate, the user's agentic IDE (orchestrate session {sid}). The user watches and steers "
          "you there, not in a terminal. Report your work with the orchestrate skill's tools (`convos orchestrate ...`; this session is "
          "preselected, do not run join). Messages beginning with [orchestrate] are the user's comments and answers from the board, "
          "addressed to the session or a named item. Before starting, load these skills with the Skill tool: {skills}.")
PROCS, LOCK = {}, threading.RLock()

def alive(sid): return (p := PROCS.get(sid)) is not None and p["proc"].poll() is None
def busy(sid): return alive(sid) and PROCS[sid]["busy"]

def set_runtime(sid, state, **extra):
    s = store.load(sid)
    if (s["runtime"] or {}).get("state") != state or extra: store.emit(sid, "runtime", "orchestrate", runtime=dict(state=state, **extra))

def child_env(sid):
    env = {k: v for k, v in os.environ.items() if k not in INHERITED}
    return {**env, "CONVOS_ORCHESTRATE_SESSION": sid}

def command(sid, meta, resume):
    mcp = dict(mcpServers=dict(orchestrate=dict(command=sys.executable, args=["-m", "ai_convos_orchestrate.mcp"], env={k: v for k, v in child_env(sid).items() if k.startswith(("CONVOS_", "PATH", "HOME"))})))
    skills = ", ".join(["orchestrate", *[k for k in meta.get("skills", []) if k != "orchestrate"]])
    return ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
            "--model", meta["model"], *(["--effort", meta["effort"]] if meta.get("effort") else []),
            *(["--resume", meta["provider_session"]] if resume else ["--session-id", meta["provider_session"]]),
            "--permission-mode", meta.get("permission") or "default", "--allowedTools", "Bash(convos orchestrate:*)",
            "--append-system-prompt", SYSTEM.format(sid=sid, skills=skills),
            "--mcp-config", json.dumps(mcp), "--permission-prompt-tool", "mcp__orchestrate__permission"]

def start(sid, text=None):
    """Start (or resume) a managed session's process; optionally send it a user turn."""
    with LOCK:
        if not alive(sid):
            s = store.load(sid)
            meta, resume = s["session"], s["runtime"] is not None and s["runtime"].get("started")
            if meta["origin"] != "gui": raise ValueError("Only sessions started from orchestrate can be started here; joined sessions run in their own terminal")
            if meta["provider"] not in PROVIDERS: raise ValueError(f"Provider {meta['provider']} cannot be started headless yet")
            if not Path(meta["cwd"]).is_dir(): raise ValueError(f"Working directory is missing: {meta['cwd']}")
            log = open(store.sdir(sid)/"stderr.log", "ab")
            proc = subprocess.Popen(command(sid, meta, resume), cwd=meta["cwd"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, env=child_env(sid), start_new_session=True)
            PROCS[sid] = dict(proc=proc, busy=False, log=log)
            threading.Thread(target=read, args=(sid, proc), daemon=True, name=f"orchestrate-{sid}").start()
            set_runtime(sid, "idle", started=True, pid=proc.pid)
        if text: send(sid, text)

def send(sid, text):
    with LOCK:
        proc = PROCS[sid]["proc"]
        proc.stdin.write((json.dumps(dict(type="user", message=dict(role="user", content=[dict(type="text", text=text)]))) + "\n").encode())
        proc.stdin.flush()
        PROCS[sid]["busy"] = True
    set_runtime(sid, "working", started=True, pid=proc.pid)

def read(sid, proc):
    """The board is what the agent reports; its raw output only tells us when a turn ends."""
    for raw in proc.stdout:
        try: event = json.loads(raw)
        except ValueError: continue
        if event.get("type") == "result":
            PROCS[sid]["busy"] = False
            set_runtime(sid, "idle", started=True, pid=proc.pid, cost=event.get("total_cost_usd"), turns=event.get("num_turns"))
    code = proc.wait()
    with LOCK:
        if PROCS.get(sid, {}).get("proc") is proc: PROCS.pop(sid)["log"].close()
    set_runtime(sid, "stopped" if code in (0, -signal.SIGTERM, -signal.SIGINT, 128+signal.SIGTERM, 128+signal.SIGINT) else "failed", started=True, code=code)

def stop(sid):
    with LOCK: proc = PROCS.get(sid, {}).get("proc")
    if proc is None or proc.poll() is not None: return set_runtime(sid, "stopped", started=True)
    os.killpg(proc.pid, signal.SIGTERM)
    try: proc.wait(8)
    except subprocess.TimeoutExpired: os.killpg(proc.pid, signal.SIGKILL)

def settle():
    """After a reboot or server restart, no managed process survives: record that truthfully."""
    for sid in store.session_ids():
        s = store.load(sid)
        if s["session"]["origin"] == "gui" and (s["runtime"] or {}).get("state") in ("working", "idle", "starting") and not alive(sid):
            store.emit(sid, "runtime", "orchestrate", runtime={**{k: v for k, v in s["runtime"].items() if k != "at"}, "state": "stopped"})

def spawn(title, cwd, prompt, model, effort=None, skills=(), permission="default", parent=None, provider="claude-code"):
    if provider not in PROVIDERS: raise ValueError(f"Unknown provider {provider}")
    if model not in PROVIDERS[provider]["models"]: raise ValueError(f"Unknown model {model}")
    if not Path(cwd).expanduser().is_dir(): raise ValueError(f"No such directory: {cwd}")
    if parent:
        psid, _, pitem = parent.partition(":")
        if pitem and pitem not in store.load(psid)["items"]: raise ValueError(f"Parent item {parent} does not exist")
    meta = dict(id=(sid := store.new_id()), title=title.strip() or prompt.strip().splitlines()[0][:80], provider=provider, provider_session=str(uuid.uuid4()), model=model, effort=effort or None, cwd=str(Path(cwd).expanduser().resolve()), origin="gui", parent=parent or None, skills=sorted(set(skills) | {"orchestrate"}), permission=permission)
    store.emit(sid, "open", "user", prompt, session=meta)
    start(sid, prompt)
    return sid

def message(s, found):
    who = "from the user" if all(p["actor"] == "user" or p["type"] == "answer" for p in found) else "on your board"
    return "\n".join([f"[orchestrate] {len(found)} new {who} (session {s['session']['id']}):", *[store.describe(s, p) for p in found]])

DELIVERING = collections.defaultdict(threading.Lock)

def deliver(sid):
    """Route undelivered user words to the session exactly once. Returns the channel used, or 'pending' when the agent must pull."""
    with DELIVERING[sid]:
        s = store.load(sid)
        if not (found := store.pending(s)): return None
        text, meta = message(s, found), s["session"]
        if meta["origin"] == "gui": (start(sid, text), route := "stdin")
        elif not (route := channels.push(sid, meta, text)): return "pending"
        store.emit(sid, "deliver", "orchestrate", ids=[p["id"] for p in found], channel=route)
        return route
