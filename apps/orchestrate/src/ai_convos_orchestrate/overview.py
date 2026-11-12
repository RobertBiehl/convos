"""Convos on this machine, read-only: is the archive current, is capture running, the relay, the disk.

Reads core's state files and stats the session files the agents write. It never opens the archive, the
network, or the Keychain, so the IDE can refresh it every few seconds. Starting a sync is the one action:
it runs core's own `convos sync` in the background.
"""
import importlib.util, json, os, re, shutil, subprocess, sys, threading, time
from pathlib import Path
from ai_convos import cli as core

SOURCES = (("claude-code", "Claude Code"), ("codex", "Codex"), ("muse", "Muse"))
WEB = {"chatgpt": "ChatGPT", "claude": "Claude", "perplexity": "Perplexity"}
CACHE = dict(at=0, value=None)
LOCK = threading.Lock()

def read(path, default):
    try: return json.loads(Path(path).read_text())
    except (OSError, ValueError): return default

def label(source, root, p):
    """A short name for a session file: its project and session for Claude Code, the session id otherwise."""
    rel = p.relative_to(root).parts
    if source == "claude-code": return f"{rel[0].removeprefix(str(Path.home()).replace('/', '-')+'-')} · {(rel[1] if len(rel) > 2 else p.stem)[:8]}" + (" · subagent" if "subagents" in rel else "")
    return (p.parent.name if source == "muse" else re.sub(r"^rollout-[\dT:-]+-", "", p.stem))[:8]

def freshness(source, name, hooks, synced, at):
    """Every session file of a source against its last capture: a hook saw this exact mtime and size, or a sync read it after it last changed."""
    root = core.hook_root(source).expanduser()
    if not root.is_dir(): return None
    behind, total = [], 0
    for p in root.rglob("*.jsonl"):
        try: st = p.stat()
        except OSError: continue
        total += 1
        path = str(p.resolve())
        hook, seen = hooks.get(core.gen_id("hook", f"{source}:{path}")), synced.get(path, synced.get(str(p)))
        if hook == [st.st_mtime_ns, st.st_size] or (seen is not None and seen >= st.st_mtime): continue
        captured = max([*([hook[0]/1e9] if hook else []), *([seen] if seen is not None else [])], default=None)
        behind.append(dict(label=label(source, root, p), path=path, changed=st.st_mtime, captured=captured, writing=at-st.st_mtime < 60))
    behind.sort(key=lambda f: -f["changed"])
    return dict(source=source, name=name, total=total, current=total-len(behind), writing=[f for f in behind if f["writing"]],
                after=[f for f in behind if not f["writing"] and f["captured"]], never=[f for f in behind if not f["writing"] and not f["captured"]])

def sync_run():
    """The last `convos sync`: from its lock file, which outlives the run; running while its heartbeat is fresh and its process lives."""
    lock = read(core.DATA_DIR/".sync.lock", None)
    if not lock: return None
    try: alive = os.kill(lock["pid"], 0) is None
    except OSError: alive = False
    return dict(started=lock["started_at"], beat=lock["heartbeat_at"], stage=lock["stage"], running=alive and lock["stage"] != "finished" and time.time()-lock["heartbeat_at"] < 120)

def relay():
    """The optional relay product: its watcher's last pass and, on macOS, whether launchd runs it."""
    if not importlib.util.find_spec("ai_convos_remote"): return None
    from ai_convos_remote import paths
    base = paths()[0]
    if not base.exists(): return None
    watch, pid = read(base/"watch.json", {}), None
    if sys.platform == "darwin":
        out = subprocess.run(["launchctl", "list", "com.ai-convos.remote"], capture_output=True, text=True, timeout=3)
        pid = int(m[1]) if (m := re.search(r'"PID" = (\d+);', out.stdout)) else None
    return dict(succeeded=watch.get("succeeded_at"), failures=watch.get("failures", 0), error=watch.get("error"), pid=pid)

def disk():
    used = lambda p: p.stat().st_blocks*512
    backups = sorted(core.DATA_DIR.glob("convos.db.*.bak"), key=lambda p: p.stat().st_mtime)
    usage = shutil.disk_usage(core.DATA_DIR)
    return dict(total=usage.total, free=usage.free, archive=used(core.DB_PATH) if core.DB_PATH.exists() else 0,
                backups=[dict(name=p.name, size=used(p), at=p.stat().st_mtime) for p in backups])

def build():
    at, state = time.time(), core.load_state()
    hooks, progress = read(core.HOOK_STATE, {}), read(core.HOOK_PROGRESS, {})
    local = state.get("local", {})
    sources = [f for source, name in SOURCES if (f := freshness(source, name, hooks, local.get(source, {}).get("files", {}), at))]
    inbox = len(list(core.HOOK_DIR.glob("*.json")))+len(list(core.HOOK_DIR.glob("*.work"))) if core.HOOK_DIR.exists() else 0
    return dict(at=at, sources=sources,
                failed=[dict(source=s, path=p, label=label(s, root, Path(p)) if Path(p).is_relative_to(root := core.hook_root(s).expanduser()) else p) for s, cp in local.items() for p in cp.get("failed", [])],
                # the newest conversation a web sync captured: its time ends the checkpoint head
                web=[dict(source=k, name=WEB.get(k, k), newest=v.get("last_updated") or (m[0] if (m := re.search(r"\d{4}-\d\d-\d\dT[\d:.]+Z$", v.get("head") or "")) else None), browser=v.get("browser")) for k, v in state.get("web", {}).items()],
                sync=sync_run(), hooks=dict(pending=inbox, last=progress.get("completed_at", 0)/1e9 or None, processed=progress.get("processed", 0), failed=progress.get("failed", 0), error=progress.get("provenance_error")),
                relay=relay(), disk=disk())

def overview(fresh=False):
    with LOCK:
        if fresh or time.time()-CACHE["at"] > 4: CACHE.update(at=time.time(), value=build())
        return CACHE["value"]

def start_sync(local_only):
    """Run core's sync in the background; core's own lock refuses a second one."""
    if (run := sync_run()) and run["running"]: raise ValueError("A sync is already running")
    log = core.PROJECT_ROOT/"orchestrate"/"sync.log"
    core.secure_dir(log.parent)
    with log.open("ab") as out: subprocess.Popen([sys.executable, "-m", "ai_convos", "sync", *(["--local-only"] if local_only else [])], stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    CACHE["at"] = 0
    return dict(started=True, local_only=local_only)
