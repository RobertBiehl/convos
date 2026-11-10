"""Provider channels that put the user's words into a joined session.

Claude Code: each session binds an inbox socket and exports its path and token only to its own hooks and
Bash commands. The agent's own `convos orchestrate` calls record them here as local working state
(never in the archive), so orchestrate can post `{"type":"user",...}` lines the session treats as its own
child's. Codex: `codex queue --thread ID --message TEXT` queues for the thread.
"""
import json, os, shutil, socket, subprocess
from pathlib import Path
from . import store

def endpoint_path(sid): return store.sdir(sid)/"endpoint.json"

def record(sid):
    """Called from the agent's own shell: remember how to reach this live Claude Code session."""
    if not (sock := os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET")) or not (s := store.peek(sid)): return
    if s["session"]["origin"] == "gui" or s["session"].get("provider_session") != os.environ.get("CLAUDE_CODE_SESSION_ID"): return
    value = dict(socket=sock, token=os.environ.get("CLAUDE_CODE_MESSAGING_TOKEN"), pid=int(os.environ.get("CLAUDE_PID") or 0) or None, session=os.environ.get("CLAUDE_CODE_SESSION_ID"))
    try: current = json.loads(endpoint_path(sid).read_text())
    except (OSError, ValueError): current = None
    if current != value:
        store.core.atomic_json(endpoint_path(sid), value)
        os.chmod(endpoint_path(sid), 0o600)

def endpoint(s):
    try: value = json.loads(endpoint_path(s["session"]["id"]).read_text())
    except (OSError, ValueError): return None
    if value["session"] and s["session"].get("provider_session") and value["session"] != s["session"]["provider_session"]: return None
    try: (os.kill(value["pid"], 0) if value["pid"] else None)
    except ProcessLookupError: return None
    except PermissionError: pass
    return value if Path(value["socket"]).is_socket() else None

def reachable(s):
    meta = s["session"]
    if meta["origin"] == "gui": return False
    if meta["provider"] == "claude-code": return endpoint(s) is not None
    return meta["provider"] == "codex" and bool(meta.get("provider_session")) and shutil.which("codex") is not None

def push(sid, meta, text):
    s = store.load(sid)
    if meta["provider"] == "claude-code" and (point := endpoint(s)):
        lines = [*([dict(type="auth", token=point["token"])] if point["token"] else []), dict(type="user", message=dict(role="user", content=text))]
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(5)
            conn.connect(point["socket"])
            conn.sendall("".join(json.dumps(l) + "\n" for l in lines).encode())
        return "claude-socket"
    if meta["provider"] == "codex" and reachable(s):
        subprocess.run(["codex", "queue", "--thread", meta["provider_session"], "--message", text], check=True, capture_output=True, timeout=30)
        return "codex-queue"
    return None
