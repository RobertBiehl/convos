"""Session runtime: start, stop, and resume headless agents, and deliver the user's words to any session.

Headless sessions started from the IDE are owned here. Claude Code runs as one process that takes each message on stdin.
Codex and Muse take one prompt per process, so each of their turns is a run of `codex exec` / `codex exec resume` or
`muse exec`, and words that arrive during a turn start the next one. Joined sessions receive messages through their
provider's own channel (Claude Code messaging socket, codex queue); whatever cannot be delivered stays pending for the
agent's `inbox`.
"""
import collections, json, mmap, os, re, shutil, signal, subprocess, sys, threading, time, uuid
from pathlib import Path
from . import store, channels, transcripts

TURNS = ("codex", "muse")  # providers that run one process per turn
PERMISSIONS = {"claude-code": ["default", "acceptEdits", "plan", "bypassPermissions"], "codex": ["workspace-write", "danger-full-access"], "muse": ["sandboxed", "yolo"]}
CLAUDE_CLASSES, CLAUDE_EFFORTS, SCANNED = ("opus", "sonnet", "haiku", "fable"), ["low", "medium", "high", "xhigh", "max"], {}

def claude_models():
    """The newest model of each class the installed Claude Code can run: its binary names every model id it knows."""
    path = Path(os.path.realpath(shutil.which("claude")))
    if SCANNED.get("claude") != (key := (str(path), path.stat().st_mtime_ns)):
        with path.open("rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m: ids = set(re.findall(rb"claude-(opus|sonnet|haiku|fable)-(\d+)(?:-(\d+))?", m))
        # a dated id (claude-sonnet-4-20250514) belongs to its major version
        found = [(c.decode(), int(a), int(b) if b and len(b) <= 2 else 0) for c, a, b in ids]
        newest = {c: max((a, b) for k, a, b in found if k == c) for c in CLAUDE_CLASSES if any(k == c for k, _, _ in found)}
        SCANNED.update(claude=key, claude_models=[f"claude-{c}-{a}" + (f"-{b}" if b else "") for c, (a, b) in newest.items()])
    return [dict(id=m, efforts=CLAUDE_EFFORTS) for m in SCANNED["claude_models"]]

def codex_models():
    """The models Codex lists (its models_cache.json, which Codex refreshes), the newest of each class: gpt-6.1-sol over gpt-6-sol."""
    try: rows = json.loads((Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"models_cache.json").read_text())["models"]
    except (OSError, ValueError, KeyError): return []
    version = lambda m: [int(x) for x in re.findall(r"\d+", (re.match(r"gpt-([\d.]+)-", m["slug"]) or [None, "0"])[1])]
    listed = [m for m in rows if m.get("visibility") == "list"]
    newest = {cls: max((m for m in listed if re.sub(r"^gpt-[\d.]+-", "", m["slug"]) == cls), key=version) for cls in dict.fromkeys(re.sub(r"^gpt-[\d.]+-", "", m["slug"]) for m in sorted(listed, key=lambda m: m.get("priority", 99)))}
    return [dict(id=m["slug"], efforts=[x["effort"] for x in m.get("supported_reasoning_levels") or []], effort=m.get("default_reasoning_level")) for m in newest.values()]

def muse_models():
    """Muse's model catalog (cached by Muse per provider): the visible models, the newest of each class, its default first."""
    root = Path(os.environ.get("XDG_DATA_HOME", Path.home()/".local"/"share"))/"muse"/"model-catalog"
    rows = [r for f in sorted(root.glob("*.json")) for r in json.loads(f.read_text()).get("rows") or [] if r.get("visibility") == "visible"]
    newest = {re.sub(r"-[\d.]+$", "", r["model_id"]): r for r in sorted(rows, key=lambda r: r.get("release_date") or "")}
    return [dict(id=r["model_id"], efforts=[v["tier"] for v in r.get("reasoning_effort_variants") or []]) for r in sorted(newest.values(), key=lambda r: not r.get("is_default"))]

def providers():
    """What a new session can run: each installed agent CLI with its current models, their efforts, and its permission modes."""
    found = dict(claude=("claude-code", "Claude Code", claude_models), codex=("codex", "Codex", codex_models), muse=("muse", "Muse", muse_models))
    return {key: dict(label=label, models=models(), permissions=PERMISSIONS[key]) for cli, (key, label, models) in found.items() if shutil.which(cli)}
INHERITED = ("CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_PID", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED", "CLAUDE_EFFORT", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "CONVOS_ORCHESTRATE_SCOPE", "CONVOS_ORCHESTRATE_AS")
SYSTEM = ("You were started from Convos Orchestrate, the user's agentic IDE (orchestrate session {sid}). The user watches and steers "
          "you there, not in a terminal. Report your work by updating the board with the orchestrate skill's tools (`convos orchestrate ...`; "
          "this session is preselected, do not run join); every changed row shows as unread to the user, so do not post status recaps in chat. Messages beginning with [orchestrate] are the user's comments and answers from the board, "
          "addressed to the session or a named item. {load}")
LOAD = {"claude-code": "Before starting, load these skills with the Skill tool: {}.", "codex": "Use these skills: {}.", "muse": "Before starting, read these skills: {}."}
TURN = " Each of your turns ends when you finish it; the user's next words start your next turn, so never wait for them."

def preamble(sid, meta):
    skills = ["orchestrate", *[k for k in meta.get("skills", []) if k != "orchestrate"]]
    load = LOAD[meta["provider"]].format(", ".join(f"${k}" if meta["provider"] == "codex" else k for k in skills))
    return SYSTEM.format(sid=sid, load=load) + (TURN if meta["provider"] in TURNS else "")
PROCS, LOCK = {}, threading.RLock()
OPEN = ("working", "idle", "starting")

class Unreachable(ValueError): pass
class Busy(Unreachable): pass  # a turn-based agent is mid-turn: words wait and start its next turn

def running(p):
    if p["proc"]: return p["proc"].poll() is None
    try: return os.kill(p["pid"], 0) is None
    except OSError: return False

def alive(sid): return (p := PROCS.get(sid)) is not None and running(p)
def busy(sid): return alive(sid) and PROCS[sid]["busy"]

# an agent's command line names its session: Claude Code's MCP config, Codex's shell environment setting, Muse's prompt file
MARK = re.compile(r'^\s*\d+ (?:\S*/)?(?:claude|codex|muse) .*?(?:"CONVOS_ORCHESTRATE_SESSION": "|CONVOS_ORCHESTRATE_SESSION="|/sessions/)(o[0-9a-f]{11})')

def live_agents():
    """Every running agent process by session, ours or not, found by the session id on its command line."""
    out = subprocess.run(["ps", "-axww", "-o", "pid=,command="], capture_output=True, text=True, timeout=5).stdout
    found = [(m[1], int(line.split()[0])) for line in out.splitlines() if (m := MARK.search(line))]
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
    if meta["provider"] == "codex":
        # the board lives under the Convos root, so a sandboxed agent may write there; its own session id marks the process
        config = [*([f'model_reasoning_effort="{meta["effort"]}"'] if meta.get("effort") else []), f'sandbox_mode="{meta.get("permission") or "workspace-write"}"',
                  f"sandbox_workspace_write.writable_roots={json.dumps([str(store.core.PROJECT_ROOT)])}", f'shell_environment_policy.set.CONVOS_ORCHESTRATE_SESSION="{sid}"']
        return ["codex", "exec", *(["resume", meta["provider_session"]] if resume else []), "--json", "--skip-git-repo-check", "-m", meta["model"], *[x for c in config for x in ("-c", c)], "-"]
    if meta["provider"] == "muse":
        return ["muse", "exec", "--json", "--session-id", meta["provider_session"], "--model", meta["model"], *(["--reasoning-effort", meta["effort"]] if meta.get("effort") else []),
                "--approval-mode", "never", "--trust-workspace", *(["--yolo"] if meta.get("permission") == "yolo" else []), "--prompt-file", str(store.sdir(sid)/"prompt.md")]
    mcp = dict(mcpServers=dict(orchestrate=dict(command=sys.executable, args=["-m", "ai_convos_orchestrate.mcp"], env={k: v for k, v in child_env(sid).items() if k.startswith(("CONVOS_", "PATH", "HOME"))})))
    return ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
            "--model", meta["model"], *(["--effort", meta["effort"]] if meta.get("effort") else []),
            *(["--resume", meta["provider_session"]] if resume else ["--session-id", meta["provider_session"]]),
            "--permission-mode", meta.get("permission") or "default", "--allow-dangerously-skip-permissions", "--allowedTools", "Bash(convos orchestrate:*)",
            "--append-system-prompt", preamble(sid, meta),
            "--mcp-config", json.dumps(mcp), "--permission-prompt-tool", "mcp__orchestrate__permission"]

def start(sid, text=None):
    """Start (or resume) a managed session's agent; optionally give it the user's words. A turn-based agent in the middle of
    a turn raises Busy: the words wait and start its next turn."""
    with LOCK:
        if not alive(sid):
            s = store.load(sid)
            meta, resume = s["session"], s["runtime"] is not None and s["runtime"].get("started")
            if meta["origin"] != "gui": raise ValueError("Only sessions started from orchestrate can be started here; joined sessions run in their own terminal")
            if meta["provider"] not in PERMISSIONS: raise ValueError(f"Provider {meta['provider']} cannot be started headless")
            if not Path(meta["cwd"]).is_dir(): raise ValueError(f"Working directory is missing: {meta['cwd']}")
            # an agent outlives the server that started it; pick it up, and never start a second one on its conversation
            if (others := agents(sid)) and not adopt(sid, s, others):
                raise Unreachable(f"An agent orchestrate cannot reach (pid {', '.join(map(str, others))}) is still running this session; your words wait until it ends, or stop it with x")
            if not alive(sid):
                if meta["provider"] in TURNS: return turn(sid, s, text) if text else None
                launch(sid, meta, resume)
        if PROCS[sid]["stdin"] is None:
            if text: raise Busy("A turn is running; your words start the next one")
            return
        if text: send(sid, text)

def turn(sid, s, text):
    """One turn of a Codex or Muse agent: its own process, the prompt from a file, its output followed like Claude Code's."""
    meta, d = s["session"], store.sdir(sid)
    first = not (s["runtime"] or {}).get("started") or meta["provider"] == "codex" and not meta.get("provider_session")
    (d/"prompt.md").write_text(f"{preamble(sid, meta)}\n\n{text}" if first else text)
    with open(d/"prompt.md", "rb") as prompt, open(d/"stdout.jsonl", "wb") as out, open(d/"stderr.log", "ab") as log:
        proc = subprocess.Popen(command(sid, meta, bool(meta.get("provider_session"))), cwd=meta["cwd"], stdin=prompt, stdout=out, stderr=log, env=child_env(sid), start_new_session=True)
    follow(sid, dict(pid=proc.pid, proc=proc, busy=True, stdin=None), 0)
    set_runtime(sid, "working", started=True, pid=proc.pid)

def follow(sid, p, offset):
    PROCS[sid] = p
    (reader := threading.Thread(target=read, args=(sid, p, offset), daemon=True, name=f"orchestrate-{sid}")).start()
    p["reader"] = reader

def launch(sid, meta, resume):
    d = store.sdir(sid)
    (fifo := d/"stdin.fifo").unlink(missing_ok=True)
    os.mkfifo(fifo, 0o600)
    # the agent holds its stdin pipe open for writing too, so it never reads EOF when the server goes away and the next server can
    # pick it up; its output goes to a file for the same reason
    own = os.open(fifo, os.O_RDWR)
    try:
        with open(d/"stdout.jsonl", "wb") as out, open(d/"stderr.log", "ab") as log:
            proc = subprocess.Popen(cmd := command(sid, meta, resume), cwd=meta["cwd"], stdin=own, stdout=out, stderr=log, env=child_env(sid), start_new_session=True)
    finally: os.close(own)
    hold(sid, proc.pid, proc, 0, False)
    PROCS[sid]["bypassable"] = "--allow-dangerously-skip-permissions" in cmd
    set_runtime(sid, "idle", started=True, pid=proc.pid)

def hold(sid, pid, proc, offset, busy):
    """Own a running agent: write to its stdin pipe, follow its output file from offset."""
    fd = os.open(store.sdir(sid)/"stdin.fifo", os.O_WRONLY | os.O_NONBLOCK)  # refused at once when nothing reads the pipe
    os.set_blocking(fd, True)
    follow(sid, dict(pid=pid, proc=proc, busy=busy, stdin=os.fdopen(fd, "wb")), offset)

def adopt(sid, s, pids):
    """Pick up the agent a previous server started. Agents from before agents had a pipe file cannot be reached; they finish their turn."""
    d, rt = store.sdir(sid), s["runtime"] or {}
    if s["session"]["provider"] in TURNS and len(pids) == 1:
        follow(sid, dict(pid=pids[0], proc=None, busy=True, stdin=None), 0)
        set_runtime(sid, "working", started=True, pid=pids[0])
        return True
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
    # agents launched before they could be switched into bypassPermissions need a restart for it
    cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pids[0])], capture_output=True, text=True).stdout
    PROCS[sid]["bypassable"] = "--allow-dangerously-skip-permissions" in cmd or "bypassPermissions" in cmd
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
    """The board is what the agent reports; its output only tells us when a turn ends, and which thread a Codex agent runs."""
    with (store.sdir(sid)/"stdout.jsonl").open("rb") as f:
        f.seek(offset)
        while True:
            done = not running(p)
            while (line := f.readline()).endswith(b"\n"):
                if (e := event(line)) and e.get("type") == "result":
                    p["busy"] = False
                    set_runtime(sid, "idle", started=True, pid=p["pid"], cost=e.get("total_cost_usd"), turns=e.get("num_turns"))
                # an agent launched before agents could be switched into bypassPermissions takes it only by starting again
                if e and e.get("type") == "control_response" and e["response"].get("error_code") == "bypass_not_launched": p["bypassable"] = False
                # Codex names its thread when the first turn starts; later turns resume it
                if e and e.get("type") == "thread.started" and store.load(sid)["session"].get("provider_session") != e["thread_id"]:
                    store.emit(sid, "meta", "orchestrate", changes=dict(provider_session=e["thread_id"]))
            f.seek(-len(line), os.SEEK_CUR)
            if p.get("relaunch") and not done and (not p["busy"] or asking(sid, p)):
                p["relaunch"] = False
                threading.Thread(target=relaunch, args=(sid, p), daemon=True).start()
            if done: break
            time.sleep(0.25)
    code = p["proc"].wait() if p["proc"] else None
    with LOCK:
        if PROCS.get(sid) is p: PROCS.pop(sid)
    if p["stdin"] is None and code in (None, 0):
        # a turn-based agent rests between turns; words that arrived during this one start the next
        set_runtime(sid, "idle", started=True)
        if store.pending(store.load(sid)): deliver(sid)
        return
    if p["stdin"]: p["stdin"].close()
    set_runtime(sid, "stopped" if code in (None, 0, -signal.SIGTERM, -signal.SIGINT, 128+signal.SIGTERM, 128+signal.SIGINT) else "failed", started=True, **({} if code is None else dict(code=code)))

def permit(sid, mode, confirmed=False):
    """Change a started session's permission. Codex and Muse take it with their next turn; a live Claude Code agent switches at
    once. One launched without the option to bypass permissions gets bypassPermissions only by restarting on its conversation,
    which the user confirms first: "confirm" asks for that, and the restart comes after its turn, or at once while it waits on a
    permission prompt."""
    meta = store.load(sid)["session"]
    if meta["origin"] != "gui": raise ValueError("A joined session keeps the permission of the terminal it runs in")
    if mode not in PERMISSIONS[meta["provider"]]: raise ValueError(f"{meta['provider']} permissions: {', '.join(PERMISSIONS[meta['provider']])}")
    with LOCK:
        live = alive(sid) and (p := PROCS[sid])["stdin"] is not None
        restart = live and mode == "bypassPermissions" and not p.get("bypassable")
    if restart and not confirmed: return "confirm"
    if mode == meta.get("permission") and not restart: return "unchanged"
    if mode != meta.get("permission"): store.emit(sid, "meta", "user", changes=dict(permission=mode))
    with LOCK:
        if not live: return "next turn" if meta["provider"] in TURNS else "next start"
        p["relaunch"] = restart
        if restart: return "restart"
        p["stdin"].write((json.dumps(dict(type="control_request", request_id=f"permission-{time.time_ns()}", request=dict(subtype="set_permission_mode", mode=mode))) + "\n").encode())
        p["stdin"].flush()
    return "now"

def switching(sid, meta):
    """A live agent that still runs without the bypassPermissions its session now has: it needs a restart, or one is coming."""
    p = PROCS.get(sid)
    if not p or not running(p) or p["stdin"] is None or meta.get("permission") != "bypassPermissions" or p.get("bypassable"): return None
    return "restarting" if p.get("relaunch") else "needs restart"

def asking(sid, p):
    """A busy agent stuck on a permission prompt, looked up at most every two seconds."""
    if time.time() < p.get("looked", 0) + 2: return False
    p["looked"] = time.time()
    return any(a["kind"] == "permission" for a in store.open_asks(store.peek(sid)))

def relaunch(sid, p):
    """Start an agent again on the same conversation with the session's current settings: once its turn is over, or at once
    while it waits on a permission prompt, which then goes away and the agent is told to go on."""
    mid, waiting = p["busy"], [a["id"] for a in store.open_asks(store.load(sid)) if a["kind"] == "permission"]
    stop(sid)
    p["reader"].join(15)
    for ask in waiting: store.emit(sid, "cancel", "orchestrate", ask=ask)
    start(sid, "[orchestrate] The user changed this session's permission, which restarted you in the middle of your turn; the tool call that waited for approval did not run. Continue where you left off." if mid else None)

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
            elif s["session"]["provider"] in TURNS and state in OPEN:
                # between turns a Codex or Muse agent has no process; a turn that ended while no server watched it leaves it resting
                if state != "idle": set_runtime(sid, "idle", started=True)
                if store.pending(store.load(sid)): deliver(sid)
            elif state in OPEN:
                set_runtime(sid, "stopped", started=True)
                if store.pending(store.load(sid)): deliver(sid)

def spawn(title, cwd, prompt, model=None, effort=None, skills=(), permission=None, parent=None, provider="claude-code"):
    if not (offer := providers().get(provider)): raise ValueError(f"{provider} cannot be started here; installed: {', '.join(providers()) or 'none'}")
    models = {m["id"]: m for m in offer["models"]}
    if (model := model or next(iter(models), None)) not in models: raise ValueError(f"{offer['label']} has no model {model}; current: {', '.join(models)}")
    if effort and effort not in models[model]["efforts"]: raise ValueError(f"{model} takes effort {', '.join(models[model]['efforts'])}")
    if not Path(cwd).expanduser().is_dir(): raise ValueError(f"No such directory: {cwd}")
    above = store.load(parent.partition(":")[0]) if parent else None
    if parent and (pitem := parent.partition(":")[2]) and pitem not in above["items"]: raise ValueError(f"Parent item {parent} does not exist")
    # a child on the same agent as its parent runs with the parent's permission unless told otherwise
    inherited = above["session"].get("permission") if above and above["session"]["provider"] == provider else None
    if (permission := permission or inherited or offer["permissions"][0]) not in offer["permissions"]: raise ValueError(f"{offer['label']} permissions: {', '.join(offer['permissions'])}")
    meta = dict(id=(sid := store.new_id()), title=title.strip() or prompt.strip().splitlines()[0][:80], provider=provider, provider_session=None if provider == "codex" else str(uuid.uuid4()), model=model, effort=effort or None, cwd=str(Path(cwd).expanduser().resolve()), origin="gui", parent=parent or None, skills=sorted(set(skills) | {"orchestrate"}), permission=permission)
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
        # an agent stopped by its usage limit cannot act on them; they go out once the limit resets
        if transcripts.holding(meta): return "pending"
        if meta["origin"] == "gui":
            try: start(sid, text)
            except Unreachable: return "pending"
            route = "stdin"
        elif not (route := channels.push(sid, meta, text)): return "pending"
        store.emit(sid, "deliver", "orchestrate", ids=[p["id"] for p in found], channel=route)
        return route
