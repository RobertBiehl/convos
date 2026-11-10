"""Orchestrate history. Core owns every event; this module projects sessions into boards.

Each orchestrate session is one archive conversation (source='orchestrate'). Every change is one
message whose metadata carries a versioned event; projections in view.json are disposable caches
rebuilt from the archive whenever they are missing or marked dirty.
"""
import json, mimetypes, os, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from ai_convos import cli as core

V = 1
# DuckDB refuses a second attach of the same file inside one process, and the archive's file lock does not
# serialize threads, so the server's threads take turns here (always before a session lock).
ARCHIVE = threading.RLock()
STATUSES = ("planned", "working", "blocked", "done", "failed", "skipped")
# Session status precedence: anything that needs the user wins, then live work, then trouble.
PRECEDENCE = ("input", "working", "blocked", "failed", "planned")
STATUS_LABEL = {"input": "waiting for the user", "done": "done", "failed": "failed", "closed": "closed"}
ALLOC = {"item": ("item", "i", "items"), "comment": ("comment", "c", "comments"), "ask": ("ask", "q", "asks"), "attach": ("attachment", "a", "attachments")}
AGENT_KINDS = {"item", "update", "activity", "attach", "ask", "cancel", "comment", "deliver", "meta", "runtime"}
PROVIDER_ENV = (("claude-code", "CLAUDE_CODE_SESSION_ID"), ("codex", "CODEX_THREAD_ID"), ("codex", "CODEX_SESSION_ID"))

def now(): return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def root(): return core.PROJECT_ROOT/"orchestrate"
def sdir(sid): return root()/"sessions"/sid
def body_path(h): return core.DATA_DIR/"attachments"/h
def empty(): return dict(v=V, seq=0, session=None, prompt="", activity=None, runtime=None, closed=None, items={}, comments={}, asks={}, attachments={}, updated=None)
def new_id(): return "o"+uuid.uuid4().hex[:11]

def top(s, i):
    while s["items"][i]["parent"]: i = s["items"][i]["parent"]
    return i


def apply(s, e, body):
    kind, at, actor = e["kind"], e["at"], e["actor"]
    s["seq"], s["updated"] = e["seq"], at
    # only the user archives a finished row; anything new on it brings it back to the user's current work
    touched = e["item"] if kind == "update" else e["parent"] if kind == "item" else e["target"] if kind in ("comment", "attach", "ask") else None
    if touched: s["items"][top(s, touched)]["archived"] = None
    if kind == "open": s.update(session={**e["session"], "created": at}, prompt=body)
    elif kind == "meta": s["session"].update(e["changes"])
    elif kind == "activity": s["activity"] = dict(text=body, actor=actor, at=at) if body else None
    elif kind == "item": s["items"][e["item"]] = dict(id=e["item"], parent=e["parent"], title=e["title"], status=e["status"], description=body, actor=actor, created=at, updated=at, history=[dict(status=e["status"], actor=actor, at=at)])
    elif kind == "update":
        item = s["items"][e["item"]]
        item.update({**e["changes"], **({"description": body} if e["description"] else {}), "updated": at})
        if "status" in e["changes"]: item["history"].append(dict(status=e["changes"]["status"], actor=actor, at=at))
    elif kind == "comment": s["comments"][e["comment"]] = dict(id=e["comment"], target=e["target"], reply_to=e["reply_to"], actor=actor, text=body, files=e.get("files", []), at=at, delivered=None if actor in ("user", "orchestrate") else at)
    elif kind == "attach": s["attachments"][e["attachment"]] = dict(id=e["attachment"], target=e["target"], actor=actor, caption=body, at=at, **e["file"])
    elif kind == "ask": s["asks"][e["ask"]] = dict(id=e["ask"], target=e["target"], options=e["options"], multi=e["multi"], kind=e["ask_kind"], detail=e["detail"], text=body, actor=actor, at=at, answer=None, delivered=None, cancelled=None)
    elif kind == "answer": s["asks"][e["ask"]]["answer"] = dict(choices=e["choices"], text=body, actor=actor, at=at)
    elif kind == "cancel": s["asks"][e["ask"]]["cancelled"] = at
    elif kind == "deliver":
        for i in e["ids"]: (s["comments"] if i[0] == "c" else s["asks"])[i]["delivered"] = at
    elif kind == "runtime": s["runtime"] = {**e["runtime"], "at": at}
    elif kind == "archive":
        for i in e["items"]: s["items"][i].update(archived=at if e["archived"] else None, archived_by=actor if e["archived"] else None)
    elif kind == "close": s["closed"] = at
    elif kind == "reopen": s["closed"] = None
    return s

def within(s, target, scope):
    """True when target (an item id, or None for the session itself) lies inside the scope subtree."""
    if scope is None: return True
    while target is not None:
        if target == scope: return True
        target = s["items"][target]["parent"]
    return False

def check(s, e):
    kind, scope = e["kind"], e["scope"]
    def item(i, name="Item"):
        if i is not None and i not in s["items"]: raise ValueError(f"{name} {i} does not exist in this session")
        if not within(s, i, scope): raise ValueError(f"{name} {i or 'session'} is outside scope {scope}")
        return i
    if kind == "open": return
    if s["session"] is None: raise ValueError("Session history is missing its open event")
    if s["closed"] and kind not in ("reopen", "comment", "deliver", "runtime", "meta", "answer", "archive"): raise ValueError("Session is closed; reopen it first")
    if kind == "item": item(e["parent"] if e["parent"] is not None or scope is None else scope, "Parent")
    if kind in ("update", "attach", "comment", "ask"): item(e.get("item", e.get("target")))
    if kind in ("item", "update") and e.get("status", e.get("changes", {}).get("status", "planned")) not in STATUSES: raise ValueError(f"Status must be one of {', '.join(STATUSES)}")
    if kind == "comment" and e["reply_to"] is not None and e["reply_to"] not in s["comments"]: raise ValueError(f"Comment {e['reply_to']} does not exist")
    if kind == "comment" and not all(body_path(f["hash"]).is_file() for f in e.get("files", [])): raise ValueError("A comment image is missing from the archive")
    if kind in ("answer", "cancel"):
        if e["ask"] not in s["asks"]: raise ValueError(f"Question {e['ask']} does not exist")
        if (a := s["asks"][e["ask"]])["answer"] or a["cancelled"]: raise ValueError(f"Question {e['ask']} is already resolved")
        if kind == "answer" and a["options"] and not a["multi"] and len(e["choices"]) > 1: raise ValueError("This question takes one choice")
        if kind == "answer" and not set(e["choices"]) <= set(a["options"]): raise ValueError(f"Choices must be among: {', '.join(a['options'])}")
    if kind == "archive":
        if e["actor"] != "user" and not e["archived"]: raise ValueError("Only the user brings archived rows back")
        marks = None if e["actor"] == "user" else seen(s["session"]["id"])
        for i in e["items"]:
            if item(i) is None or s["items"][i]["parent"] is not None: raise ValueError(f"Only top-level rows are archived; {i} is not one")
            if marks is not None and (why := blocked(s, i, marks)): raise ValueError(why)
    if kind == "deliver" and not all(i in (s["comments"] if i[0] == "c" else s["asks"]) for i in e["ids"]): raise ValueError("Unknown delivery id")

def _conv(s, sid, at):
    meta = s["session"]
    return dict(id=sid, source="orchestrate", title=meta["title"], created_at=meta["created"], updated_at=at, model=meta.get("model"), cwd=meta["cwd"], git_branch=None, project_id=None, metadata=json.dumps(dict(orchestrate={k: meta[k] for k in ("title", "provider", "origin", "cwd", "parent")})))

def emit(sid, kind, actor, body="", scope=None, blob=None, **fields):
    """Append one event through the core writer, then refresh the projection. Returns (state, event)."""
    core.secure_dir(sdir(sid))
    with ARCHIVE:
        if kind == "open":
            with core._core(ready=True, purpose="schema.orchestrate"): pass
        preview, e = _append(sid, kind, actor, body, scope, blob, fields)
    if kind in ("open", "meta"): bind(preview)
    return preview, e

def _append(sid, kind, actor, body, scope, blob, fields):
    with core.operation_lock(sdir(sid)/"lock", "orchestrate.session"):
        if kind == "open" and (sdir(sid)/"view.json").exists(): raise ValueError(f"Session {sid} already exists")
        s = empty() if kind == "open" else load(sid)
        if kind in ALLOC: fields[ALLOC[kind][0]] = f"{ALLOC[kind][1]}{len(s[ALLOC[kind][2]])+1}"
        e = dict(v=V, seq=s["seq"]+1, kind=kind, actor=actor, at=now(), scope=scope, **fields)
        check(s, e)
        mid = f"orchestrate-{sid}-{e['seq']}"
        msg = dict(id=mid, conversation_id=sid, role="user" if actor == "user" else "assistant", content=body, thinking=None, created_at=e["at"], model=None, metadata=json.dumps(dict(orchestrate=e)), parent_id=None)
        files = [e["file"]] if blob else e.get("files", [])
        attachs = [dict(id=f"{mid}-file{n or ''}", message_id=mid, filename=f["name"], mime_type=f["mime"], size=f["size"], path=str(body_path(f["hash"])), url=None, created_at=e["at"]) for n, f in enumerate(files)]
        preview = apply(json.loads(json.dumps(s)), e, body)
        convs = [_conv(preview, sid, e["at"])] if kind in ("open", "meta", "close", "reopen") else []
        core.atomic_write(sdir(sid)/"dirty", "Rebuild the projection from the archive\n")
        changed = core.commit_result(core.ParseResult(convs=convs, msgs=[msg], attachs=attachs, scopes=[], edit_scopes=[]), purpose="orchestrate.append")[-1]
        if mid not in changed: raise ValueError("Core did not accept this orchestrate event")
        save(sid, preview)
    return preview, e

def save(sid, s):
    core.atomic_json(sdir(sid)/"view.json", s)
    (sdir(sid)/"dirty").unlink(missing_ok=True)

def rebuild(sid):
    s, after = empty(), 0
    while True:
        with ARCHIVE, core._core(read_only=True, purpose="orchestrate.rebuild") as db:
            rows = db.execute("SELECT metadata, content FROM messages WHERE conversation_id=? AND TRY_CAST(json_extract_string(metadata,'$.orchestrate.seq') AS BIGINT)>? ORDER BY TRY_CAST(json_extract_string(metadata,'$.orchestrate.seq') AS BIGINT) LIMIT 500", [sid, after]).fetchall()
        if not rows: break
        for metadata, body in rows:
            e = json.loads(metadata)["orchestrate"]
            if e["v"] != V or e["seq"] != s["seq"]+1: raise ValueError(f"Orchestrate history for {sid} is incomplete or unsupported; keep the archive and inspect it")
            apply(s, e, body or "")
        after = s["seq"]
    if not after: raise ValueError(f"Orchestrate session {sid} is not in the archive")
    core.secure_dir(sdir(sid))
    save(sid, s)
    bind(s)
    return s

def load(sid):
    path = sdir(sid)/"view.json"
    if not path.exists() or (sdir(sid)/"dirty").exists(): return rebuild(sid)
    try: s = json.loads(path.read_text())
    except ValueError: return rebuild(sid)
    return s if s.get("v") == V else rebuild(sid)

def peek(sid):
    """Cheap read for pollers: the cached projection, or None while a writer has it dirty."""
    try: return None if (sdir(sid)/"dirty").exists() else json.loads((sdir(sid)/"view.json").read_text())
    except (OSError, ValueError): return None

def session_ids(): return sorted(p.parent.name for p in (root()/"sessions").glob("*/view.json")) if (root()/"sessions").exists() else []

def recover():
    """Rebuild projections for every archived session whose cache is missing (fresh machine, wiped cache)."""
    if not core.DB_PATH.exists(): return []
    with ARCHIVE, core._core(read_only=True, purpose="orchestrate.sessions") as db: archived = [r[0] for r in db.execute("SELECT id FROM conversations WHERE source='orchestrate'").fetchall()]
    return [rebuild(sid)["session"]["id"] for sid in archived if not (sdir(sid)/"view.json").exists() or (sdir(sid)/"dirty").exists()]

def bindings():
    try: return json.loads((root()/"bindings.json").read_text())
    except (OSError, ValueError): return {}

def bind(s):
    meta = s["session"]
    if not meta.get("provider_session"): return
    key = f"{meta['provider']}:{meta['provider_session']}"
    with core.operation_lock(root()/"bindings.lock", "orchestrate.bindings"):
        if (data := bindings()).get(key) != meta["id"]: core.atomic_json(root()/"bindings.json", {**data, key: meta["id"]})

def provider_identity():
    return next(((p, v) for p, name in PROVIDER_ENV if (v := os.environ.get(name))), (None, None))

def resolve(explicit=None):
    if explicit: return explicit
    if sid := os.environ.get("CONVOS_ORCHESTRATE_SESSION"): return sid
    provider, session = provider_identity()
    if session and (sid := bindings().get(f"{provider}:{session}")): return sid
    raise ValueError("This session has not joined orchestrate. Run: convos orchestrate join --title 'WHAT YOU ARE DOING'")

# what the user has read. When they last looked at each row is UI working state kept next to the session, never archive data;
# rows unchanged since tracking began count as read. A row is unread when something they did not do themselves happened since.
def seen(sid):
    path = sdir(sid)/"seen.json"
    try: return json.loads(path.read_text())
    except (OSError, ValueError): pass
    with core.operation_lock(sdir(sid)/"seen.lock", "orchestrate.seen"):
        if not path.exists(): core.atomic_json(path, dict(since=now(), items={}))
    return json.loads(path.read_text())

def mark_seen(sid, items):
    with core.operation_lock(sdir(sid)/"seen.lock", "orchestrate.seen"):
        marks, at = seen(sid), now()
        core.atomic_json(sdir(sid)/"seen.json", {**marks, "items": {**marks["items"], **{i: at for i in items}}})

def changed(s, i):
    """The last time anything happened on a row that the user did not do themselves."""
    return max([s["items"][i]["updated"], *(c["at"] for c in s["comments"].values() if c["target"] == i and c["actor"] != "user"),
                *(a["at"] for a in s["attachments"].values() if a["target"] == i), *(a["at"] for a in s["asks"].values() if a["target"] == i)])

def unread(s, marks): return {i for i in s["items"] if changed(s, i) > marks["items"].get(i, marks["since"])}

def blocked(s, root, marks):
    """Why an agent may not archive this top-level row yet, or None: its tree must be finished and read by the user."""
    tree, waiting = [i for i in s["items"] if top(s, i) == root], {a["target"] for a in open_asks(s)}
    if busy := [f"{i} {s['items'][i]['status']}" for i in tree if s["items"][i]["status"] not in ("done", "skipped")] + [f"{i} needs the user" for i in tree if i in waiting]:
        return f"{root} still has open work ({', '.join(busy)}); leave it on the board"
    if new := sorted(set(tree) & unread(s, marks), key=lambda i: int(i[1:])):
        return f"The user has not looked at {', '.join(new)} since it last changed; leave {root} on the board until they have"
    return None

def open_asks(s): return [a for a in s["asks"].values() if a["answer"] is None and a["cancelled"] is None]

def effective(s):
    waiting = {a["target"] for a in open_asks(s)}
    return {i: "input" if i in waiting else item["status"] for i, item in s["items"].items()}

def status(s):
    if s["closed"]: return "closed"
    values = set(effective(s).values()) | ({"input"} if open_asks(s) else set()) | ({"working"} if (s["runtime"] or {}).get("state") == "working" else set())
    return next((x for x in PRECEDENCE if x in values), "done" if s["items"] else "idle")

def pending(s, scope=None):
    """User words the agent has not received yet: comments and answers, oldest first."""
    comments = [dict(type="comment", **c) for c in s["comments"].values() if c["delivered"] is None and within(s, c["target"], scope)]
    answers = [dict(type="answer", **a) for a in s["asks"].values() if a["answer"] and a["delivered"] is None and a["kind"] != "permission" and within(s, a["target"], scope)]
    return sorted(comments+answers, key=lambda x: x["answer"]["at"] if x["type"] == "answer" else x["at"])

def describe(s, entry):
    """One line of plain text an agent can act on, with the exact command to reply."""
    where = f"item {t} ({s['items'][t]['title']!r})" if (t := entry["target"]) else "the session"
    if entry["type"] == "answer":
        a = entry["answer"]
        note = f" | note: {a['text']!r}" if a["text"] else ""
        return f"- {entry['id']} answered on {where}: Q: {entry['text']!r} -> {', '.join(a['choices']) or '(no choice)'}{note}"
    if entry["actor"] == "orchestrate": return f"- {entry['id']} orchestrate notice on {where}: {entry['text']}"
    reply = f"convos orchestrate say{f' --on {t}' if t else ''} --reply {entry['id']} 'TEXT'"
    cid, sid = entry["id"], s["session"]["id"]
    images = "".join(f"\n  image: {local_file(sid, f, f'{cid}-{n+1}')}" for n, f in enumerate(entry.get("files", [])))
    return f"- {entry['id']} user comment on {where}{f' (reply to {r})' if (r := entry['reply_to']) else ''}: {entry['text']!r}{images}\n  reply in thread: {reply}"

def local_file(sid, f, label):
    """A path with the file's extension that points at the archived body, so agents' image readers recognise it."""
    path = sdir(sid)/"files"/f"{label}{Path(f['name']).suffix or mimetypes.guess_extension(f['mime']) or ''}"
    if not path.is_symlink():
        core.secure_dir(path.parent)
        path.symlink_to(body_path(f["hash"]))
    return path

def wait_for(sid, predicate, seconds):
    deadline = time.monotonic()+seconds
    while True:
        if (s := peek(sid)) is not None and (value := predicate(s)): return s, value
        if time.monotonic() >= deadline: return load(sid), None
        time.sleep(0.4)

def store_file(path): return store_bytes(Path(path).read_bytes())

def store_bytes(data):
    if (blob := core.attachment_body(data)) is None: raise ValueError(f"Attachment is larger than {core.ATTACHMENT_LIMIT} bytes")
    return blob, len(data)
