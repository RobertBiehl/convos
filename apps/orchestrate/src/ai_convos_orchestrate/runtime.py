"""Session runtime: start, stop, and resume headless agents, and deliver the user's words to any session.

Headless sessions started from the IDE are owned here. Claude Code runs as one process that takes each message on stdin.
Codex and Muse take one prompt per process, so each of their turns is a run of `codex exec` / `codex exec resume` or
`muse exec`, and words that arrive during a turn start the next one. Joined sessions receive messages through their
provider's own channel (Claude Code messaging socket, codex queue); whatever cannot be delivered stays pending for the
agent's `inbox`.
"""
import collections, contextlib, faulthandler, json, mmap, os, queue, re, shutil, signal, subprocess, sys, threading, time, uuid
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

def claude_version():
    """The installed Claude Code's version, asked again only when its binary changes (an update replaces it); None when it cannot
    tell, which only hides the version: it never stops an agent from starting."""
    try:
        key = (str(path := Path(os.path.realpath(shutil.which("claude")))), path.stat().st_mtime_ns)
        if (hit := SCANNED.get("claude_version")) and hit[0] == key: return hit[1]
        out = subprocess.run([str(path), "--version"], capture_output=True, text=True, timeout=15).stdout
    except (OSError, TypeError, subprocess.SubprocessError): return None
    SCANNED["claude_version"] = (key, version := m[1] if (m := re.match(r"\s*(\d[\w.+-]*)", out)) else None)
    return version

def codex_models():
    """The models Codex lists (its models_cache.json, which Codex refreshes), the newest of each class: gpt-6.1-sol over gpt-6-sol."""
    try: rows = json.loads((Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"models_cache.json").read_text())["models"]
    except (OSError, ValueError, KeyError): return []
    version = lambda m: [int(x) for x in re.findall(r"\d+", (re.match(r"gpt-([\d.]+)-", m["slug"]) or [None, "0"])[1])]
    listed = [m for m in rows if m.get("visibility") == "list"]
    newest = {cls: max((m for m in listed if re.sub(r"^gpt-[\d.]+-", "", m["slug"]) == cls), key=version) for cls in dict.fromkeys(re.sub(r"^gpt-[\d.]+-", "", m["slug"]) for m in sorted(listed, key=lambda m: m.get("priority", 99)))}
    return [dict(id=m["slug"], efforts=[x["effort"] for x in m.get("supported_reasoning_levels") or []], effort=m.get("default_reasoning_level")) for m in newest.values()]

def codex_account(method, params):
    """One request to Codex's app server under the user's own Codex login: account reads and actions, never a model turn."""
    p = subprocess.Popen(["codex", "app-server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    (timer := threading.Timer(30, p.kill)).start()
    try:
        for rid, name, args in ((1, "initialize", dict(clientInfo=dict(name="convos-orchestrate", title="Convos Orchestrate", version="1"), capabilities=None)), (None, "initialized", None), (2, method, params)):
            p.stdin.write(json.dumps(dict(jsonrpc="2.0", method=name, **({} if rid is None else dict(id=rid)), **({} if args is None else dict(params=args)))) + "\n")
            p.stdin.flush()
            if rid is None: continue
            for line in p.stdout:
                if (reply := json.loads(line)).get("id") == rid: break
            else: raise ValueError("Codex's app server ended before it answered")
            if "error" in reply: raise ValueError(f"Codex: {reply['error'].get('message') or reply['error']}")
        return reply["result"]
    finally:
        timer.cancel()
        p.kill()

RESETS = {}

def codex_resets(fresh=False):
    """The user's Codex usage-limit resets (how many are left, the one that expires first) and whether Codex allows usage now.
    Read at most every 30 s; usage allowed again lifts every Codex limit orchestrate holds words for."""
    if not fresh and (hit := RESETS.get("read")) and time.monotonic() < hit[0]+30: return hit[1]
    r = codex_account("account/rateLimits/read", {})
    summary = r["rateLimitResetCredits"] or dict(availableCount=0, credits=None)
    left = sorted((c for c in summary["credits"] or [] if c["status"] == "available"), key=lambda c: c["expiresAt"] or float("inf"))
    value = dict(available=int(summary["availableCount"]), next=left[0] if left else None, allowed=r["ordinaryUsageAllowed"])
    if value["allowed"] is True: transcripts.release("codex")
    RESETS["read"] = (time.monotonic(), value)
    return value

def use_codex_reset():
    """Redeem the user's Codex reset that expires first; on success the words held for the limit go out."""
    pick = codex_resets(fresh=True)["next"]
    outcome = codex_account("account/rateLimitResetCredit/consume", dict(idempotencyKey=str(uuid.uuid4()), creditId=pick and pick["id"]))["outcome"]
    # nothingToReset: Codex says usage needs no reset, so nothing should wait for one either
    if outcome in ("reset", "nothingToReset"): transcripts.release("codex")
    RESETS.clear()
    return outcome

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
PROCS, LOCKS, RUNNING, GUARD = {}, {}, {}, threading.Lock()
OPEN = ("working", "idle", "starting")
LATE = 120  # seconds a Claude Code agent has to log taking the user's words from its stdin (it does within a second)
BYPASS = "changed this session's permission"  # why an agent restarts into bypassPermissions; also told to it mid-turn

class Unreachable(ValueError): pass
class Busy(Unreachable): pass  # a turn-based agent is mid-turn: words wait and start its next turn

def running(p):
    if p["proc"]: return p["proc"].poll() is None
    try: return os.kill(p["pid"], 0) is None
    except OSError: return False

def lock(sid):
    """Each session has its own lock: one waiting on the archive or on its agent holds up no other session."""
    return LOCKS.setdefault(sid, threading.RLock())

def once(key, fn, *args):
    """Run fn in the background unless its last run under key still goes on; the next round tries again. Every wait on the archive
    or on one agent stays inside its own run, and a run stuck for long is reported with every thread's stack (stuck)."""
    with GUARD:
        if key in RUNNING: return
        RUNNING[key] = [time.monotonic(), False]
    def run():
        try: fn(*args)
        except (ValueError, OSError, subprocess.SubprocessError, store.core.LockBusy) as error: print(f"orchestrate: {key}: {str(error)[:300]}", file=sys.stderr)
        finally: RUNNING.pop(key)
    threading.Thread(target=run, daemon=True, name=f"orchestrate {key}").start()

def stuck(secs=120):
    """Print every thread's stack once a background run has gone on for secs, so the next stall shows what it waits on."""
    for key, run in list(RUNNING.items()):
        if not run[1] and time.monotonic()-run[0] > secs:
            run[1] = True
            print(f"orchestrate: {key} still runs after {secs}s; every thread's stack follows", file=sys.stderr, flush=True)
            faulthandler.dump_traceback(sys.stderr, all_threads=True)

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
    # a permission prompt waits up to 6 h for the user (mcp.decide); Claude Code would give up on the tool after 30 min
    mcp = dict(mcpServers=dict(orchestrate=dict(command=sys.executable, args=["-m", "ai_convos_orchestrate.mcp"], env={k: v for k, v in child_env(sid).items() if k.startswith(("CONVOS_", "PATH", "HOME"))}, timeout=6*3600*1000)))
    return ["claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
            "--model", meta["model"], *(["--effort", meta["effort"]] if meta.get("effort") else []),
            *(["--resume", meta["provider_session"]] if resume else ["--session-id", meta["provider_session"]]),
            "--permission-mode", meta.get("permission") or "default", "--allow-dangerously-skip-permissions", "--allowedTools", "Bash(convos orchestrate:*)",
            "--append-system-prompt", preamble(sid, meta),
            "--mcp-config", json.dumps(mcp), "--permission-prompt-tool", "mcp__orchestrate__permission"]

def start(sid, text=None):
    """Start (or resume) a managed session's agent; optionally give it the user's words. A turn-based agent in the middle of
    a turn raises Busy: the words wait and start its next turn."""
    with lock(sid):
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
    PROCS[sid].update(bypassable="--allow-dangerously-skip-permissions" in cmd, version=claude_version())
    set_runtime(sid, "idle", started=True, pid=proc.pid)

def hold(sid, pid, proc, offset, busy):
    """Own a running agent: write to its stdin pipe, follow its output file from offset."""
    fd = os.open(store.sdir(sid)/"stdin.fifo", os.O_WRONLY | os.O_NONBLOCK)  # refused at once when nothing reads the pipe
    os.set_blocking(fd, True)
    p = dict(pid=pid, proc=proc, busy=busy, stdin=os.fdopen(fd, "wb"), outbox=queue.SimpleQueue(), given={})
    threading.Thread(target=write, args=(sid, p), daemon=True, name=f"orchestrate-{sid}-stdin").start()
    follow(sid, p, offset)

def write(sid, p):
    """An agent's stdin pipe has its own writer: an agent that stops reading fills the pipe, which holds up only its own words,
    never whoever gave them or another session."""
    while (line := p["outbox"].get()) is not None:
        try:
            p["stdin"].write(line)
            p["stdin"].flush()
        except OSError as error:
            print(f"orchestrate: {sid}: its agent's stdin failed: {error}", file=sys.stderr)
            break
    with contextlib.suppress(OSError): p["stdin"].close()

def tell(p, line): p["outbox"].put((json.dumps(line) + "\n").encode())

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
    # the Claude Code it runs is named in the first turn it reported; one that has not had a turn yet stays unknown
    with out.open("rb") as f: head = f.read(1 << 20)
    PROCS[sid].update(bypassable="--allow-dangerously-skip-permissions" in cmd or "bypassPermissions" in cmd, version=m[1].decode() if (m := re.search(rb'"claude_code_version":"([^"]+)"', head)) else None)
    set_runtime(sid, "working" if busy else "idle", started=True, pid=pids[0])
    return True

def send(sid, text):
    with lock(sid):
        tell(p := PROCS[sid], dict(type="user", message=dict(role="user", content=[dict(type="text", text=text)])))
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
            # only bypassPermissions makes a waiting permission prompt moot, and an agent deaf to the user has no turn worth waiting
            # for; any other restart waits for the turn to end
            if p.get("relaunch") and not done and (not p["busy"] or p.get("deaf") or p["relaunch"] == BYPASS and asking(sid, p)):
                threading.Thread(target=relaunch, args=(sid, p, p.pop("relaunch")), daemon=True).start()
            if done: break
            time.sleep(0.25)
    code = p["proc"].wait() if p["proc"] else None
    with lock(sid):
        if PROCS.get(sid) is p: PROCS.pop(sid)
    if p["stdin"] is None and code in (None, 0):
        # a turn-based agent rests between turns; words that arrived during this one start the next
        set_runtime(sid, "idle", started=True)
        if store.pending(store.load(sid)): once(f"deliver {sid}", deliver, sid)
        return
    if p["stdin"]: p["outbox"].put(None)
    set_runtime(sid, "stopped" if code in (None, 0, -signal.SIGTERM, -signal.SIGINT, 128+signal.SIGTERM, 128+signal.SIGINT) else "failed", started=True, **({} if code is None else dict(code=code)))

def permit(sid, mode, confirmed=False):
    """Change a started session's permission. Codex and Muse take it with their next turn; a live Claude Code agent switches at
    once. One launched without the option to bypass permissions gets bypassPermissions only by restarting on its conversation,
    which the user confirms first: "confirm" asks for that, and the restart comes after its turn, or at once while it waits on a
    permission prompt."""
    meta = store.load(sid)["session"]
    if meta["origin"] != "gui": raise ValueError("A joined session keeps the permission of the terminal it runs in")
    if mode not in PERMISSIONS[meta["provider"]]: raise ValueError(f"{meta['provider']} permissions: {', '.join(PERMISSIONS[meta['provider']])}")
    with lock(sid):
        live = alive(sid) and (p := PROCS[sid])["stdin"] is not None
        restart = live and mode == "bypassPermissions" and not p.get("bypassable")
    if restart and not confirmed: return "confirm"
    if mode == meta.get("permission") and not restart: return "unchanged"
    if mode != meta.get("permission"): store.emit(sid, "meta", "user", changes=dict(permission=mode))
    with lock(sid):
        if not live: return "next turn" if meta["provider"] in TURNS else "next start"
        # a restart pending only for bypassPermissions is not needed once the user picks another mode
        if restart or p.get("relaunch") == BYPASS: p["relaunch"] = restart and BYPASS
        if restart: return "restart"
        tell(p, dict(type="control_request", request_id=f"permission-{time.time_ns()}", request=dict(subtype="set_permission_mode", mode=mode)))
    return "now"

def tune(sid, model, effort):
    """Switch a started session's model or effort. Codex and Muse take them with their next turn; a live Claude Code agent restarts
    on its conversation for them, at once when idle, otherwise once its current turn ends."""
    meta = store.load(sid)["session"]
    if meta["origin"] != "gui": raise ValueError("A joined session runs with the model of the terminal it runs in")
    offer = providers()[meta["provider"]]
    known = {m["id"]: m for m in offer["models"]}
    # the model a session already runs stays selectable after a newer one replaces it in the list
    if model not in known and model != meta["model"]: raise ValueError(f"{offer['label']} has no model {model}; current: {', '.join(known)}")
    if effort and model in known and effort not in known[model]["efforts"]: raise ValueError(f"{model} takes effort {', '.join(known[model]['efforts']) or 'none'}")
    if not (changes := {k: v for k, v in dict(model=model, effort=effort or None).items() if v != meta.get(k)}): return "unchanged"
    store.emit(sid, "meta", "user", changes=changes)
    return restart(sid, "switched this session's model or effort")

def restart(sid, why="restarted this session"):
    """Start a live Claude Code agent again on its conversation, with the session's current settings and the Claude Code installed
    now (how an update reaches it): at once when idle, otherwise once its current turn ends. Codex and Muse start afresh every turn."""
    meta = store.load(sid)["session"]
    if meta["origin"] != "gui": raise ValueError("A joined session runs in its own terminal; restart it there")
    with lock(sid):
        if not alive(sid) or (p := PROCS[sid])["stdin"] is None: return "next turn" if meta["provider"] in TURNS else "next start"
        # one restart covers every reason; a pending one for bypassPermissions also goes ahead at a permission prompt
        p["relaunch"] = p.get("relaunch") or why
        return "after turn" if p["busy"] and not p.get("deaf") else "now"

def switching(sid, meta):
    """A live agent that restarts once its current turn ends ("restarting at prompt": or at its next permission prompt, for
    bypassPermissions), or that still runs without the bypassPermissions its session now has and needs a restart for it."""
    p = PROCS.get(sid)
    if not p or not running(p) or p["stdin"] is None: return None
    if p.get("relaunch"): return "restarting at prompt" if p["relaunch"] == BYPASS else "restarting"
    return "needs restart" if meta.get("permission") == "bypassPermissions" and not p.get("bypassable") else None

def agent(sid, meta):
    """The Claude Code a live agent runs and the one installed now; they differ after an update, which a restart applies."""
    p = PROCS.get(sid)
    if meta["provider"] != "claude-code" or not p or not running(p) or p["stdin"] is None: return None
    return dict(running=p.get("version"), installed=claude_version())

def listen(sid, meta):
    """Mark a live Claude Code agent deaf when its transcript logs neither taking words written to its stdin LATE seconds ago
    into its queue nor the words themselves: an agent that reads its input but stopped acting on it (Claude Code 2.1.327 after
    a skill reload). The board warns; a restart gives them to it again."""
    if meta["provider"] != "claude-code" or not (p := PROCS.get(sid)) or not running(p) or not p["stdin"] or not p["given"] or (heard := transcripts.arrived(meta)) is None: return
    took = (last := transcripts.enqueued(meta)) and transcripts.stamp(last).timestamp()
    # words count as taken once it logs taking a message after they were written (its log has millisecond times)
    p["given"] = {i: at for i, at in p["given"].items() if i not in heard and not (took and took >= at-0.001)}
    deaf = sorted((i for i, at in p["given"].items() if at <= time.time()-LATE), key=lambda i: (i[0], int(i[1:])))
    if deaf != p.get("deaf", []): print(f"orchestrate: {sid}: its agent {f'has not taken {len(deaf)} words from its stdin: ' + ', '.join(deaf[:8]) if deaf else 'took every word it was given'}", file=sys.stderr)
    p["deaf"] = deaf

def unheard(sid): return (p := PROCS.get(sid)) and p.get("deaf") or []

def asking(sid, p):
    """A busy agent stuck on a permission prompt, looked up at most every two seconds."""
    if time.time() < p.get("looked", 0) + 2: return False
    p["looked"] = time.time()
    return any(a["kind"] == "permission" for a in store.open_asks(store.peek(sid)))

def relaunch(sid, p, why):
    """Start an agent again on the same conversation with the session's current settings: once its turn is over, or at once
    while it waits on a permission prompt (for bypassPermissions), which then goes away and the agent is told to go on. Words
    the old agent was given but never showed in its transcript go to the new one again."""
    mid, waiting, deaf = p["busy"] and not p.get("deaf"), [a["id"] for a in store.open_asks(store.load(sid)) if a["kind"] == "permission"], p.get("deaf")
    stop(sid)
    p["reader"].join(15)
    for ask in waiting: store.emit(sid, "cancel", "orchestrate", ask=ask)
    start(sid, f"[orchestrate] The user {why}, which restarted you in the middle of your turn;{' the tool call that waited for approval did not run.' if waiting else ''} Continue where you left off." if mid else None)
    if deaf: deliver(sid, deaf)

def stop(sid):
    with lock(sid): p = PROCS.get(sid)
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
        with lock(sid):
            if alive(sid): continue
            state = (s["runtime"] or {}).get("state")
            if pids := live.get(sid):
                if not adopt(sid, s, pids) and state != "working": set_runtime(sid, "working", started=True, pid=pids[0])
            elif s["session"]["provider"] in TURNS and state in OPEN:
                # between turns a Codex or Muse agent has no process; a turn that ended while no server watched it leaves it resting
                if state != "idle": set_runtime(sid, "idle", started=True)
                if store.pending(store.load(sid)): once(f"deliver {sid}", deliver, sid)
            elif state in OPEN:
                set_runtime(sid, "stopped", started=True)
                if store.pending(store.load(sid)): once(f"deliver {sid}", deliver, sid)

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

def commands(pids):
    out = subprocess.run(["ps", "-ww", "-o", "pid=,command=", "-p", ",".join(map(str, pids))], capture_output=True, text=True).stdout if pids else ""
    return {int(pid): cmd for pid, _, cmd in (line.strip().partition(" ") for line in out.splitlines())}

def plan_move(s):
    """What moving a joined session into orchestrate does: which terminal agent it ends (Claude Code names each running session
    in its sessions folder, and the agent's own orchestrate calls record its pid; Codex's TUI holds its thread's transcript open),
    and the model, effort, permission and folder the conversation continues with. "blocked" says why it cannot move."""
    meta, path = s["session"], transcripts.transcript(s["session"])
    plan = dict(provider=meta["provider"], pid=None, version=None, busy=False, blocked=None)
    if meta["origin"] == "gui": return dict(plan, blocked="This session already runs in orchestrate")
    if meta["provider"] not in ("claude-code", "codex") or not path: return dict(plan, blocked=f"orchestrate cannot find this {meta['provider']} session's conversation to continue it")
    lines = [e for line in transcripts.tail(path) if (e := transcripts.entry(line))]
    if meta["provider"] == "claude-code":
        root = Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))
        listed = {}
        for f in (root/"sessions").glob("*.json"):
            try: d = json.loads(f.read_text())
            except (OSError, ValueError): continue
            if isinstance(d, dict) and "pid" in d: listed[d["pid"]] = d
        try: recorded = json.loads(channels.endpoint_path(meta["id"]).read_text())["pid"]
        except (OSError, ValueError, KeyError): recorded = None
        pids = {p for p, d in listed.items() if d.get("sessionId") == meta["provider_session"]} | ({recorded} if recorded else set())
        running = {p: c for p, c in commands(sorted(pids)).items() if re.match(r"(?:\S*/)?claude(?:\s|$)", c)}
        if headless := [p for p, c in running.items() if re.search(r"(?:^|\s)(?:-p|--print|--input-format)(?:\s|$)", c)]:
            return dict(plan, blocked=f"Another program runs this session headless (pid {headless[0]}); stop it there first")
        pid = max(running, default=None)
        cmd, info = running.get(pid, ""), listed.get(pid, {})
        # the terminal's flags, else the mode the conversation last ran in, as its transcript records it
        mode = m[1] if (m := re.search(r"--permission-mode[ =](\S+)", cmd)) else None if cmd else next((e["permissionMode"] for e in reversed(lines) if e.get("permissionMode")), None)
        permission = "bypassPermissions" if "--dangerously-skip-permissions" in cmd or mode == "bypassPermissions" else mode if mode in PERMISSIONS["claude-code"] else "default"
        model = next((e["message"]["model"] for e in reversed(lines) if e.get("type") == "assistant" and not e.get("isSidechain") and str(e["message"].get("model", "")).startswith("claude-")), None)
        # Claude Code keeps a conversation under the folder it was started in, which --resume must run in; its first entries name
        # it, later ones follow the agent into other folders
        with path.open("rb") as f: head = f.read(1 << 16)
        cwd = info.get("cwd") or (json.loads(m[0][6:]) if (m := re.search(rb'"cwd":"(?:[^"\\]|\\.)*"', head)) else meta["cwd"])
        return dict(plan, pid=pid, version=info.get("version"), busy=info.get("status") == "busy", model=model or meta.get("model") or (claude_models() or [dict(id=None)])[0]["id"],
                    effort=meta.get("effort") if meta.get("effort") in CLAUDE_EFFORTS else None, permission=permission, cwd=cwd)
    if channels.wake(meta) and channels.wake(meta)[0] == "collab": return dict(plan, blocked="collab runs this Codex thread; stop it there first")
    held = subprocess.run(["lsof", "-t", str(path)], capture_output=True, text=True).stdout.split()
    if holders := {p: c for p, c in commands([int(p) for p in held]).items() if re.match(r"(?:\S*/)?codex(?:\s|$)", c)}:
        pid, cmd = min(holders.items())
        how = "stop it with `codex app-server daemon stop`" if "app-server" in cmd else "exit it in its terminal"
        return dict(plan, pid=pid, blocked=f"Codex still has this thread open (pid {pid}); {how}, then move it")
    ctx = next((e["payload"] for e in reversed(lines) if e.get("type") == "turn_context"), {})
    sandbox = (ctx.get("sandbox_policy") or {}).get("type")
    return dict(plan, model=ctx.get("model") or meta.get("model"), effort=ctx.get("effort"), permission=sandbox if sandbox in PERMISSIONS["codex"] else "workspace-write", cwd=ctx.get("cwd") or meta["cwd"])

def move(sid):
    """Move a joined session into orchestrate: end the terminal agent that runs it and continue the same conversation headless
    here. From then on it is a session started here: restart, model and effort work on it, and its terminal is done with it."""
    s = store.load(sid)
    if (plan := plan_move(s))["blocked"]: raise ValueError(plan["blocked"])
    if not plan["model"]: raise ValueError("orchestrate cannot tell which model this session runs")
    if not Path(plan["cwd"]).is_dir(): raise ValueError(f"Working directory is missing: {plan['cwd']}")
    if pid := plan["pid"]:
        os.kill(pid, signal.SIGTERM)
        # ended once gone, or a zombie its terminal has yet to reap
        for _ in range(60):
            if subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()[:1] in ("", "Z"): break
            time.sleep(0.25)
        else: raise ValueError(f"Its terminal agent (pid {pid}) did not exit; quit it there, then move it again")
    store.emit(sid, "meta", "user", changes=dict(origin="gui", model=plan["model"], effort=plan["effort"], permission=plan["permission"], cwd=plan["cwd"], moved=store.now()))
    channels.endpoint_path(sid).unlink(missing_ok=True)
    if plan["provider"] in TURNS:
        # its next message starts a turn, which resumes the thread with orchestrate's preamble
        return set_runtime(sid, "idle")
    set_runtime(sid, "stopped", started=True)
    start(sid, "[orchestrate] The user moved this session from its terminal into orchestrate, which stopped you in the middle of your turn and now runs you headless. Continue where you left off." if plan["busy"] else None)
    if store.pending(store.load(sid)): deliver(sid)

def message(s, found):
    who = "from the user" if all(p["actor"] == "user" or p["type"] in ("answer", "dismissed") for p in found) else "on your board"
    return "\n".join([f"[orchestrate] {len(found)} new {who} (session {s['session']['id']}):", *[store.describe(s, p) for p in found]])

DELIVERING = collections.defaultdict(threading.Lock)

def deliver(sid, again=()):
    """Route undelivered user words to the session exactly once, with again the ids to give once more (to a restarted agent whose
    predecessor never showed them). Returns the channel used, or 'pending' when the agent must pull."""
    with DELIVERING[sid]:
        s = store.load(sid)
        if not (found := store.pending(s, again=set(again))): return None
        text, meta = message(s, found), s["session"]
        ids = [p["id"] for p in found]
        # an agent stopped by its usage limit cannot act on them; they go out once the limit resets
        if transcripts.holding(meta): return wait(sid, ids, "its usage limit has not reset")
        if meta["origin"] == "gui":
            at = time.time()
            try: start(sid, text)
            except Unreachable as error: return wait(sid, ids, str(error))
            route = "stdin"
        elif not (route := channels.push(sid, meta, text)): return wait(sid, ids, "its terminal agent cannot be reached")
        store.emit(sid, "deliver", "orchestrate", ids=ids, channel=route)
        print(f"orchestrate: {sid}: gave {', '.join(ids)} via {route}", file=sys.stderr)
        # what went into a Claude Code agent's stdin must show in its transcript (listen)
        if route == "stdin" and (p := PROCS.get(sid)) and p["stdin"]: p["given"].update(dict.fromkeys(ids, at))
        WAITING.pop(sid, None)
        return route

WAITING = {}

def wait(sid, ids, why):
    """Words that wait are logged once per reason, not on every retry."""
    if WAITING.get(sid) != (key := (tuple(ids), why)): print(f"orchestrate: {sid}: {', '.join(ids)} wait: {why}", file=sys.stderr)
    WAITING[sid] = key
    return "pending"
