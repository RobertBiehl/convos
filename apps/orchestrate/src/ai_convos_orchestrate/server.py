"""Local IDE server: the web app, a JSON API, and server-sent events.

Binds loopback only. Every API request needs the per-server token (set as a SameSite=Strict cookie by
the launch URL), a loopback Host header, and, for writes, a custom header no cross-site form can send.
"""
import base64, collections, hashlib, hmac, http.server, json, os, re, secrets, socket, subprocess, sys, threading, time, urllib.parse, webbrowser
from pathlib import Path
from . import store, runtime, channels, overview

WEB = Path(__file__).with_name("web")
STATIC = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css", "/vendor/preact-htm.js": "vendor/preact-htm.js"}
TYPES = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8"}
SAFE_INLINE = re.compile(r"^(image/(png|jpeg|gif|webp|avif)|application/pdf|text/plain|text/markdown|text/html|application/json|video/(mp4|webm|ogg|quicktime)|audio/mpeg)$")
# an agent's HTML page runs its own scripts (tooltips, tables) in an opaque origin: no cookie, no API, no other result
PAGE = "sandbox allow-scripts allow-popups allow-popups-to-escape-sandbox"
BODY_LIMIT = 96 << 20  # comment images travel base64-encoded in the request
SERVER = dict(token=None, port=None, boot=secrets.token_hex(4))

def info_path(): return store.root()/"server.json"

def version():
    """Changes when the server restarts or the web app's files change and have settled for 2s: open tabs reload in place, never mid-edit."""
    stats = [(p.name, (st := p.stat()).st_mtime_ns, st.st_size) for p in sorted(WEB.rglob("*")) if p.is_file()]
    if SERVER.get("version") and time.time() - max(m for _, m, _ in stats)/1e9 < 2: return SERVER["version"]
    SERVER["version"] = SERVER["boot"] + "-" + hashlib.sha256(json.dumps(stats).encode()).hexdigest()[:12]
    return SERVER["version"]

def summary(s):
    meta, eff = s["session"], store.effective(s)
    return dict(id=meta["id"], title=meta["title"], provider=meta["provider"], model=meta.get("model"), effort=meta.get("effort"), cwd=meta["cwd"], origin=meta["origin"], parent=meta.get("parent"), skills=meta.get("skills", []),
                status=store.status(s), counts=dict(collections.Counter(eff.values())), items=len(s["items"]), asks=len(store.open_asks(s)), pending=len(store.pending(s)),
                activity=s["activity"], runtime=s["runtime"], live=runtime.alive(meta["id"]) or channels.reachable(s), busy=runtime.busy(meta["id"]), closed=s["closed"], created=meta["created"], updated=s["updated"], seq=s["seq"])

def detail(s): return dict(s, attachments={k: store.typed(a) for k, a in s["attachments"].items() if not a["hash"] or store.body_path(a["hash"]).is_file()}, summary=summary(s), effective=store.effective(s), pending_ids=[p["id"] for p in store.pending(s)], unread=sorted(store.unread(s, store.seen(s["session"]["id"]))))

def skills(cwd):
    roots = [Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"skills", *([Path(cwd)/".claude"/"skills"] if cwd else [])]
    def meta(path):
        head = path.read_text(errors="replace")[:2000].split("---")
        fields = dict(re.findall(r"^(name|description):\s*(.+)$", head[1], re.M)) if len(head) > 2 else {}
        return dict(name=fields.get("name", path.parent.name).strip(), description=fields.get("description", "").strip()[:240], scope="project" if cwd and str(path).startswith(str(Path(cwd))) else "user")
    found = {m["name"]: m for r in roots if r.is_dir() for p in sorted(r.glob("*/SKILL.md")) for m in [meta(p)]}
    return sorted(found.values(), key=lambda m: (m["name"] != "orchestrate", m["name"]))

def dirs(prefix):
    path = Path(prefix or "~").expanduser()
    base, stem = (path, "") if prefix.endswith("/") or not prefix else (path.parent, path.name)
    if not base.is_dir(): return []
    try: return [str(p) for p in sorted(base.iterdir()) if p.is_dir() and not p.name.startswith(".") and p.name.lower().startswith(stem.lower())][:40]
    except OSError: return []

def collab():
    try: import ai_convos_collab as c
    except ImportError: return dict(available=False, rooms=[])
    bound, rooms = store.bindings(), []
    for room in c.config()["rooms"].values():
        try: state = c.snapshot(room)
        except (ValueError, OSError) as error:
            rooms.append(dict(id=room["id"], name=room["name"], scope=room["scope"], error=str(error)))
            continue
        members = [dict(name=m["name"], focus=m.get("focus"), active=m["active"], provider=m.get("provider"), report=m.get("report"), runtime=m.get("runtime"), orchestrate=bound.get(f"{m.get('provider')}:{m.get('session')}")) for m in state["members"].values() if m["name"] != "user"]
        recent = sorted(state["messages"].values(), key=lambda m: m["seq"])[-25:]
        rooms.append(dict(id=room["id"], name=room["name"], scope=room["scope"], goal=state["goal"], paused=state["paused"], direction=(state["direction"] or {}).get("summary"), members=members,
                          messages=[dict(id=m["id"], kind=m["kind"], sender=m["sender"], target=m.get("target"), summary=m.get("summary"), at=m["at"], request=m.get("request"), responses=m.get("responses")) for m in recent]))
    return dict(available=True, rooms=rooms)

def background(fn, *args):
    def run():
        try: fn(*args)
        except (ValueError, OSError, subprocess.SubprocessError, store.core.LockBusy) as error: print(f"orchestrate: {fn.__name__} {args[:1]}: {error}", file=sys.stderr)
    threading.Thread(target=run, daemon=True).start()

def image(f):
    """An image pasted or dropped into a comment: stored as a content-addressed archive body."""
    if not re.fullmatch(r"image/(png|jpeg|gif|webp|avif)", f["mime"]): raise ValueError(f"Only images can be attached to comments, not {f['mime'] or 'this file'}")
    blob, size = store.store_bytes(base64.b64decode(f["data"], validate=True))
    return dict(kind="image", name=re.sub(r"[^A-Za-z0-9._ -]", "_", f["name"])[:120] or "image", mime=f["mime"], size=size, hash=blob.name, url=None)

def act(body):
    sid, kind = body["session"], body["kind"]
    if kind == "comment":
        text, files = body["text"].strip(), [image(f) for f in body.get("files", [])]
        if not text and not files: raise ValueError("Write something or attach an image")
        s, e = store.emit(sid, "comment", "user", text, target=body.get("target"), reply_to=body.get("reply_to"), **({"files": files} if files else {}))
        if body.get("target"): store.mark_seen(sid, [body["target"]])
        background(runtime.deliver, sid)
        return dict(comment=e["comment"])
    if kind == "answer":
        s, e = store.emit(sid, "answer", "user", body.get("text", "").strip(), ask=body["ask"], choices=body.get("choices", []))
        if t := s["asks"][body["ask"]]["target"]: store.mark_seen(sid, [t])
        if s["asks"][body["ask"]]["kind"] != "permission": background(runtime.deliver, sid)
        return dict(ask=body["ask"])
    if kind == "dismiss":
        if store.load(sid)["asks"][body["ask"]]["kind"] == "permission": raise ValueError("Answer a permission request with Allow or Deny")
        s, e = store.emit(sid, "cancel", "user", ask=body["ask"])
        if t := s["asks"][body["ask"]]["target"]: store.mark_seen(sid, [t])
        background(runtime.deliver, sid)
        return dict(ask=body["ask"])
    if kind == "set":
        store.emit(sid, "update", "user", "", item=body["item"], changes=dict(status=body["status"]), description=False)
        store.mark_seen(sid, [body["item"]])
        return dict(item=body["item"])
    if kind == "archive":
        store.emit(sid, "archive", "user", items=body["items"], archived=bool(body["archived"]))
        return dict(items=body["items"])
    if kind in ("close", "reopen"):
        if kind == "close" and runtime.alive(sid): runtime.stop(sid)
        store.emit(sid, kind, "user")
        return dict(ok=True)
    if kind == "stop": return (runtime.stop(sid), dict(ok=True))[1]
    if kind == "resume":
        background(runtime.start, sid, "[orchestrate] The user resumed this session. Check your board (convos orchestrate show) and continue where you left off.")
        return dict(ok=True)
    if kind == "deliver": return dict(channel=runtime.deliver(sid))
    raise ValueError(f"Unknown action {kind}")

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *args): pass

    def handle(self):
        try: super().handle()
        except (ConnectionResetError, BrokenPipeError, TimeoutError): pass

    def send(self, code, body=b"", ctype="application/json", headers=()):
        data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=True, separators=(",", ":")).encode()
        self.send_response(code)
        for k, v in [("Content-Type", ctype), ("Content-Length", str(len(data))), ("Cache-Control", "no-store"), ("X-Content-Type-Options", "nosniff"), ("Referrer-Policy", "no-referrer"), *headers]: self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def trusted(self):
        host_ok = self.headers.get("Host") in (f"127.0.0.1:{SERVER['port']}", f"localhost:{SERVER['port']}")
        origin = self.headers.get("Origin")
        origin_ok = origin is None or origin in (f"http://127.0.0.1:{SERVER['port']}", f"http://localhost:{SERVER['port']}")
        cookie = dict(p.strip().split("=", 1) for p in (self.headers.get("Cookie") or "").split(";") if "=" in p).get("orchestrate", "")
        return host_ok and origin_ok and hmac.compare_digest(cookie, SERVER["token"])

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        query = dict(urllib.parse.parse_qsl(url.query))
        if self.headers.get("Host") not in (f"127.0.0.1:{SERVER['port']}", f"localhost:{SERVER['port']}"): return self.send(421, {"error": "wrong host"})
        if url.path == "/" and "t" in query:
            if not hmac.compare_digest(query["t"], SERVER["token"]): return self.send(403, {"error": "bad token"})
            return self.send(303, b"", "text/plain", [("Location", "/"), ("Set-Cookie", f"orchestrate={SERVER['token']}; HttpOnly; SameSite=Strict; Path=/; Max-Age=31536000")])
        if url.path in STATIC:
            path = WEB/STATIC[url.path]
            return self.send(200, path.read_bytes(), TYPES[path.suffix], [("Content-Security-Policy", "default-src 'self'; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; frame-src 'self'; object-src 'self'")])
        if not self.trusted(): return self.send(401, {"error": "Open orchestrate with: convos orchestrate open"})
        try: self.route_get(url.path, query)
        except (ValueError, KeyError) as error: self.send(400, {"error": str(error)})
        except (BrokenPipeError, ConnectionResetError): pass

    def route_get(self, path, query):
        if path == "/api/sessions": return self.send(200, [summary(s) for sid in store.session_ids() if (s := store.peek(sid) or store.load(sid))])
        if m := re.fullmatch(r"/api/session/(o[0-9a-f]{11})", path): return self.send(200, detail(store.load(m[1])))
        if m := re.fullmatch(r"/api/blob/([0-9a-f]{64})", path):
            blob = store.body_path(m[1])
            mime = query.get("mime", "")
            safe = SAFE_INLINE.match(mime)
            name = re.sub(r"[^A-Za-z0-9._ -]", "_", query.get("name", m[1]))[:120]
            ctype, size = mime if safe else "application/octet-stream", blob.stat().st_size
            headers = [("Content-Security-Policy", PAGE if mime == "text/html" else "sandbox"), ("Content-Disposition", f"{'inline' if safe else 'attachment'}; filename=\"{name}\""), ("Accept-Ranges", "bytes")]
            # byte ranges, so videos can seek and start before the whole file is read
            if not (r := re.fullmatch(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))): return self.send(200, blob.read_bytes(), ctype, headers)
            start, end = (int(r[1]), min(int(r[2] or size-1), size-1)) if r[1] else (max(0, size-int(r[2])), size-1)
            if start > end: return self.send(416, b"", "text/plain", [("Content-Range", f"bytes */{size}")])
            with blob.open("rb") as f:
                f.seek(start)
                data = f.read(end-start+1)
            return self.send(206, data, ctype, [*headers, ("Content-Range", f"bytes {start}-{end}/{size}")])
        if path == "/api/meta": return self.send(200, dict(providers=runtime.PROVIDERS, home=str(Path.home()), image_limit=store.core.ATTACHMENT_LIMIT, body_limit=BODY_LIMIT, recent=sorted({store.peek(i)["session"]["cwd"] for i in store.session_ids() if store.peek(i)})[:30]))
        if path == "/api/skills": return self.send(200, skills(query.get("cwd")))
        if path == "/api/dirs": return self.send(200, dirs(query.get("prefix", "")))
        if path == "/api/collab": return self.send(200, collab())
        if path == "/api/convos": return self.send(200, overview.overview(query.get("fresh") == "1"))
        if path == "/api/stream": return self.stream()
        self.send(404, {"error": "not found"})

    def stream(self):
        self.send_response(200)
        for k, v in (("Content-Type", "text/event-stream"), ("Cache-Control", "no-store"), ("Connection", "keep-alive")): self.send_header(k, v)
        self.end_headers()
        seen, beat, current, tick = {}, time.monotonic(), None, 0
        while True:
            events = []
            if tick % 4 == 0 and (v := version()) != current: (events.append(("version", v)), current := v)
            tick += 1
            for sid in store.session_ids():
                try: mtime = (store.sdir(sid)/"view.json").stat().st_mtime_ns
                except OSError: continue
                if seen.get(sid) != mtime and (s := store.peek(sid)):
                    seen[sid] = mtime
                    events.append(("session", summary(s)))
            if not events and time.monotonic()-beat > 15: events.append(("ping", {}))
            if events:
                self.wfile.write("".join(f"event: {k}\ndata: {json.dumps(v, separators=(',', ':'))}\n\n" for k, v in events).encode())
                self.wfile.flush()
                beat = time.monotonic()
            time.sleep(0.25)

    def do_POST(self):
        if not self.trusted() or self.headers.get("X-Orchestrate") != "1": return self.send(401, {"error": "unauthorized"})
        try:
            if (n := int(self.headers.get("Content-Length", 0))) > BODY_LIMIT: return self.send(413, {"error": f"Too large: at most {BODY_LIMIT >> 20} MB per message"})
            body = json.loads(self.rfile.read(n) or b"{}")
            path = urllib.parse.urlparse(self.path).path
            if path == "/api/act": return self.send(200, act(body))
            if path == "/api/convos/sync": return self.send(200, overview.start_sync(bool(body.get("local_only"))))
            if path == "/api/seen":
                store.mark_seen(body["session"], [i for i in body["items"] if i in store.load(body["session"])["items"]])
                return self.send(200, dict(ok=True))
            if path == "/api/spawn": return self.send(200, dict(session=runtime.spawn(body.get("title", ""), body["cwd"], body["prompt"], body["model"], body.get("effort"), body.get("skills", []), body.get("permission", "default"), body.get("parent"))))
            self.send(404, {"error": "not found"})
        except store.core.LockBusy as error:
            # another process has the archive open (DuckDB lets one writer in, and a reader blocks it): name it, keep serving
            pid = m[1] if (m := re.search(r"PID (\d+)", str(error))) else None
            what = subprocess.run(["ps", "-o", "command=", "-p", pid], capture_output=True, text=True).stdout.strip()[:90] if pid else ""
            self.send(503, {"error": f"Nothing was saved: another process has the Convos archive open{f' (PID {pid}: {what})' if what else ''}. Try again once it finishes."})
        except (ValueError, KeyError, OSError) as error: self.send(400, {"error": str(error)})

NOTIFY = ("input", "done", "failed", "closed")

def notify_parent(s, state):
    """A child session reached a state its parent should know about: say so in the parent's item thread."""
    meta = s["session"]
    psid, _, item = meta["parent"].partition(":")
    if psid not in store.session_ids(): return
    parent = store.load(psid)
    target = item if item in parent["items"] else None
    text = f"Child session {meta['id']} ({meta['title']!r}) is now {store.STATUS_LABEL.get(state, state)}. Inspect it with: convos orchestrate -s {meta['id']} show"
    store.emit(psid, "comment", "orchestrate", text, target=target, reply_to=None)
    background(runtime.deliver, psid)

def pump():
    """Retry delivery to joined sessions, tell parents about their children, and keep managed runtime truthful."""
    last, recover_at = {sid: store.status(s) for sid in store.session_ids() if (s := store.peek(sid))}, 0
    while True:
        # startup's archive work runs here, so the IDE serves even while another process holds the archive;
        # a held archive makes each try wait 30 s, so retry once a minute rather than starve the IDE's own writes
        if recover_at is not None and time.monotonic() >= recover_at:
            try:
                store.recover()
                recover_at = None
            except store.core.LockBusy as error:
                recover_at = time.monotonic()+60
                print(f"orchestrate: startup waits for the archive: {str(error)[:200]}", file=sys.stderr)
        # agents outlive a server restart: pick them up again, and record the ones that ended
        try: runtime.reconcile()
        except (ValueError, OSError, subprocess.SubprocessError, store.core.LockBusy) as error: print(f"orchestrate: reconcile: {str(error)[:200]}", file=sys.stderr)
        time.sleep(3)
        for sid in store.session_ids():
            s = store.peek(sid)
            if s and (state := store.status(s)) != last.get(sid):
                if s["session"].get("parent") and state in NOTIFY and sid in last: background(notify_parent, s, state)
                last[sid] = state
            if s and s["closed"] and runtime.alive(sid) and not runtime.busy(sid): runtime.stop(sid)
            if s and not s["closed"] and s["session"]["origin"] != "gui" and store.pending(s) and channels.reachable(s):
                try: runtime.deliver(sid)
                except (ValueError, OSError, subprocess.SubprocessError, store.core.LockBusy) as error: print(f"orchestrate: deliver {sid}: {error}", file=sys.stderr)

def free_port(preferred, wait=6):
    """The preferred port keeps open tabs working across restarts; wait briefly for a predecessor to release it."""
    deadline = time.monotonic()+wait
    while True:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", preferred))
                return preferred
            except OSError:
                if time.monotonic() > deadline:
                    probe.bind(("127.0.0.1", 0))
                    return probe.getsockname()[1]
        time.sleep(0.25)

def serve(port=7311, open_browser=True):
    store.core.secure_dir(store.root())
    try: token = json.loads(info_path().read_text())["token"]
    except (OSError, ValueError, KeyError): token = secrets.token_urlsafe(24)
    SERVER.update(token=token, port=free_port(port))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", SERVER["port"]), Handler)
    httpd.daemon_threads = True
    store.core.atomic_json(info_path(), dict(pid=os.getpid(), port=SERVER["port"], token=SERVER["token"]))
    os.chmod(info_path(), 0o600)
    threading.Thread(target=pump, daemon=True, name="orchestrate-pump").start()
    url = f"http://127.0.0.1:{SERVER['port']}/?t={SERVER['token']}"
    print(f"Orchestrate at {url}", flush=True)
    if open_browser: webbrowser.open(url)
    try: httpd.serve_forever()
    finally:
        for sid in list(runtime.PROCS): runtime.stop(sid)

def running():
    try: data = json.loads(info_path().read_text())
    except (OSError, ValueError): return None
    try:
        with socket.create_connection(("127.0.0.1", data["port"]), timeout=0.5): return data
    except OSError: return None

def launch(port=7311):
    """Open the IDE, starting a detached server when none is running."""
    if not (data := running()):
        log = open(store.root()/"server.log", "ab")
        subprocess.Popen([sys.executable, "-m", "ai_convos_orchestrate.server", str(port)], stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        deadline = time.monotonic()+20
        while not (data := running()) and time.monotonic() < deadline: time.sleep(0.2)
        if not data: raise ValueError(f"The orchestrate server did not start; see {store.root()/'server.log'}")
    url = f"http://127.0.0.1:{data['port']}/?t={data['token']}"
    webbrowser.open(url)
    return url

def request(path, body):
    """Agents use the running server for actions that need the runtime (spawning child sessions)."""
    import urllib.request
    if not (data := running()): raise ValueError("The orchestrate IDE server is not running; ask the user to run: convos orchestrate open")
    req = urllib.request.Request(f"http://127.0.0.1:{data['port']}{path}", json.dumps(body).encode(), {"Content-Type": "application/json", "X-Orchestrate": "1", "Cookie": f"orchestrate={data['token']}", "Host": f"127.0.0.1:{data['port']}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response: return json.loads(response.read())
    except urllib.error.HTTPError as error: raise ValueError(json.loads(error.read()).get("error", str(error)))

if __name__ == "__main__":
    from ai_convos import cli
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else 7311, open_browser=False)
