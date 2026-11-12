"""Session runtime: start, stop, and resume headless agents, and deliver the user's words to any session.

Headless sessions started from the IDE are owned here and receive messages on stdin. Joined sessions
receive them through their provider's own channel (Claude Code messaging socket, codex queue);
whatever cannot be delivered stays pending for the agent's `inbox`.
"""
import collections, json, os, re, signal, subprocess, sys, threading, time, uuid
from pathlib import Path
from . import store, channels

PROVIDERS = {"claude-code": dict(label="Claude Code", models=["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5", "claude-fable-5-1"], efforts=["low", "medium", "high", "xhigh", "max"], permissions=["default", "acceptEdits", "plan", "bypassPermissions"])}
INHERITED = ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_PID", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_EFFORT", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "CONVOS_ORCHESTRATE_SCOPE", "CONVOS_ORCHESTRATE_AS")
SYSTEM = ("You were started from Convos Orchestrate, the user's agentic IDE (orchestrate session {sid}). The user watches and steers "
          "you there, not in a terminal. Report your work by updating the board with the orchestrate skill's tools (`convos orchestrate ...`; "
          "this session is preselected, do not run join); every changed row shows as unread to the user, so do not post status recaps in chat. Messages beginning with [orchestrate] are the user's comments and answers from the board, "
          "addressed to the session or a named item. Before starting, load these skills with the Skill tool: {skills}.")
PROCS, LOCK = {}, threading.RLock()
OPEN = ("working", "idle", "starting")

class Unreachable(ValueError): pass

def running(p):
    if p["proc"]: return p["proc"].poll() is None
    try: return os.kill(p["pid"], 0) is None
    except OSError: return False

def alive(sid): return (p := PROCS.get(sid)) is not None and running(p)
def busy(sid): return alive(sid) and PROCS[sid]["busy"]

def live_agents():
    """Every running agent process by session, ours or not: its command line carries the session id in the MCP config."""
    out = subprocess.run(["ps", "-axww", "-o", "pid=,command="], capture_output=True, text=True, timeout=5).stdout
    found = [(m[1], int(line.split()[0])) for line in out.splitlines() if (m := re.search(r'^\s*\d+ claude .*"CONVOS_ORCHESTRATE_SESSION": "(o[0-9a-f]{11})"', line))]
    return {sid: [pid for s, pid in found if s == sid] for sid, _ in found}

def agents(sid): return live_agents().get(sid, [])

def event(line):
    try: return json.loads(line)
    except ValueError: return None

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
            # an agent outlives the server that started it; pick it up, and never start a second one on its conversation
            if (others := agents(sid)) and not adopt(sid, s, others):
                raise Unreachable(f"An agent orchestrate cannot reach (pid {', '.join(map(str, others))}) is still running this session; your words wait until it ends, or stop it with x")
            if not alive(sid): launch(sid, meta, resume)
        if text: send(sid, text)

def launch(sid, meta, resume):
    d = store.sdir(sid)
    (fifo := d/"stdin.fifo").unlink(missing_ok=True)
    os.mkfifo(fifo, 0o600)
    # the agent holds its stdin pipe open for writing too, so it never reads EOF when the server goes away and the next server can
    # pick it up; its output goes to a file for the same reason
    own = os.open(fifo, os.O_RDWR)
    try:
        with open(d/"stdout.jsonl", "wb") as out, open(d/"stderr.log", "ab") as log:
            proc = subprocess.Popen(command(sid, meta, resume), cwd=meta["cwd"], stdin=own, stdout=out, stderr=log, env=child_env(sid), start_new_session=True)
    finally: os.close(own)
    hold(sid, proc.pid, proc, 0, False)
    set_runtime(sid, "idle", started=True, pid=proc.pid)

def hold(sid, pid, proc, offset, busy):
    """Own a running agent: write to its stdin pipe, follow its output file from offset."""
    fd = os.open(store.sdir(sid)/"stdin.fifo", os.O_WRONLY | os.O_NONBLOCK)  # refused at once when nothing reads the pipe
    os.set_blocking(fd, True)
    PROCS[sid] = p = dict(pid=pid, proc=proc, busy=busy, stdin=os.fdopen(fd, "wb"))
    threading.Thread(target=read, args=(sid, p, offset), daemon=True, name=f"orchestrate-{sid}").start()

def adopt(sid, s, pids):
    """Pick up the agent a previous server started. Agents from before agents had a pipe file cannot be reached; they finish their turn."""
    d, rt = store.sdir(sid), s["runtime"] or {}
    if len(pids) != 1 or not (d/"stdin.fifo").exists() or not (out := d/"stdout.jsonl").exists(): return False
    with out.open("rb") as f:
        size = f.seek(0, os.SEEK_END)
        f.seek(max(0, size-65536))
        tail = f.read()
    whole = tail[:tail.rfind(b"\n")+1]
    last = next((e for line in reversed(whole.splitlines()) if (e := event(line))), {})
    # still in the turn the last server gave it, unless a turn has ended since
    busy = rt.get("state") == "working" and last.get("type") != "result"
    try: hold(sid, pids[0], None, size-len(tail)+len(whole), busy)
    except OSError: return False
    set_runtime(sid, "working" if busy else "idle", started=True, pid=pids[0])
    return True

def send(sid, text):
    with LOCK:
        p = PROCS[sid]
        p["stdin"].write((json.dumps(dict(type="user", message=dict(role="user", content=[dict(type="text", text=text)]))) + "\n").encode())
        p["stdin"].flush()
        p["busy"] = True
    set_runtime(sid, "working", started=True, pid=p["pid"])

def read(sid, p, offset):
    """The board is what the agent reports; its output only tells us when a turn ends."""
    with (store.sdir(sid)/"stdout.jsonl").open("rb") as f:
        f.seek(offset)
        while True:
            done = not running(p)
            while (line := f.readline()).endswith(b"\n"):
                if (e := event(line)) and e.get("type") == "result":
                    p["busy"] = False
                    set_runtime(sid, "idle", started=True, pid=p["pid"], cost=e.get("total_cost_usd"), turns=e.get("num_turns"))
            f.seek(-len(line), os.SEEK_CUR)
            if done: break
            time.sleep(0.25)
    code = p["proc"].wait() if p["proc"] else None
    with LOCK:
        if PROCS.get(sid) is p: PROCS.pop(sid)
    p["stdin"].close()
    set_runtime(sid, "stopped" if code in (None, 0, -signal.SIGTERM, -signal.SIGINT, 128+signal.SIGTERM, 128+signal.SIGINT) else "failed", started=True, **({} if code is None else dict(code=code)))

def stop(sid):
    with LOCK: p = PROCS.get(sid)
    if p is None or not running(p):
        for pid in agents(sid): os.killpg(pid, signal.SIGTERM)
        return set_runtime(sid, "stopped", started=True)
    os.killpg(p["pid"], signal.SIGTERM)
    if p["proc"]:
        try: p["proc"].wait(8)
        except subprocess.TimeoutExpired: os.killpg(p["pid"], signal.SIGKILL)

def reconcile():
    """Keep managed sessions truthful across server restarts: pick up agents that outlived their server, show the ones that cannot
    be picked up as working, and once a session's last agent is gone, record it stopped and hand any waiting words to a new one."""
    live = live_agents()
    for sid in store.session_ids():
        if alive(sid) or not (s := store.peek(sid)) or s["session"]["origin"] != "gui" or s["closed"]: continue
        with LOCK:
            if alive(sid): continue
            state = (s["runtime"] or {}).get("state")
            if pids := live.get(sid):
                if not adopt(sid, s, pids) and state != "working": set_runtime(sid, "working", started=True, pid=pids[0])
            elif state in OPEN:
                set_runtime(sid, "stopped", started=True)
                if store.pending(store.load(sid)): deliver(sid)

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
    who = "from the user" if all(p["actor"] == "user" or p["type"] in ("answer", "dismissed") for p in found) else "on your board"
    return "\n".join([f"[orchestrate] {len(found)} new {who} (session {s['session']['id']}):", *[store.describe(s, p) for p in found]])

DELIVERING = collections.defaultdict(threading.Lock)

def deliver(sid):
    """Route undelivered user words to the session exactly once. Returns the channel used, or 'pending' when the agent must pull."""
    with DELIVERING[sid]:
        s = store.load(sid)
        if not (found := store.pending(s)): return None
        text, meta = message(s, found), s["session"]
        if meta["origin"] == "gui":
            try: start(sid, text)
            except Unreachable: return "pending"
            route = "stdin"
        elif not (route := channels.push(sid, meta, text)): return "pending"
        store.emit(sid, "deliver", "orchestrate", ids=[p["id"] for p in found], channel=route)
        return route
