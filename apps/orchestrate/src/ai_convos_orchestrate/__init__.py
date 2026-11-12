"""Convos Orchestrate: an agentic IDE. Agents report items, results, and questions; people steer them.

Core owns history. This product owns the agent tools (this CLI), delivery hooks, the headless
runtime, and the local web IDE.
"""
import functools, inspect, json, mimetypes, os, re, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import typer

app = typer.Typer(help="Agentic IDE: a durable home for agent sessions, their items, results, and conversations.", no_args_is_help=True)

def cmd(fn=None, name=None):
    """Register a command that reports ValueError cleanly and also accepts --json after the subcommand."""
    if fn is None: return lambda f: cmd(f, name)
    sig = inspect.signature(fn)
    @functools.wraps(fn)
    def run(*args, json_flag=False, **kwargs):
        if json_flag and "ctx" in kwargs: kwargs["ctx"].obj["json"] = True
        try: return fn(*args, **kwargs)
        except ValueError as error:
            typer.echo(f"orchestrate: {error}", err=True)
            raise typer.Exit(1)
    run.__signature__ = sig.replace(parameters=[*sig.parameters.values(), inspect.Parameter("json_flag", inspect.Parameter.KEYWORD_ONLY, default=typer.Option(False, "--json", hidden=True), annotation=bool)])
    return app.command(name=name)(run)

@app.callback()
def main(ctx: typer.Context,
         session: str = typer.Option(None, "--session", "-s", help="Orchestrate session id; defaults to the bound provider session"),
         actor: str = typer.Option("agent", "--as", envvar="CONVOS_ORCHESTRATE_AS", help="Who is acting: agent, or a subagent name"),
         scope: str = typer.Option(None, "--scope", envvar="CONVOS_ORCHESTRATE_SCOPE", help="Restrict writes to this item's subtree (subagents)"),
         as_json: bool = typer.Option(False, "--json")):
    ctx.obj = dict(session=session, actor=actor, scope=scope, json=as_json)

def out(ctx, value, text):
    typer.echo(json.dumps(value, ensure_ascii=True, separators=(",", ":")) if ctx.obj["json"] else text)

def report(ctx, value, text):
    """A command's output, then the board check: what the agent should tidy now (store.reminders)."""
    notes = store.reminders(store.load(store.resolve(ctx.obj["session"])), ctx.obj["scope"])
    out(ctx, {**value, "reminders": notes} if notes else value, "\n".join([text, *(["Board check:", *(f"- {n}" for n in notes)] if notes else [])]))

def eta_at(text):
    """45m, 2h, 1h30m, 18:30 (the next one), or an ISO time; 'none' clears."""
    if text.strip().lower() in ("", "none"): return None
    at = datetime.now().astimezone()
    if (m := re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?", text.replace(" ", ""))) and (m[1] or m[2]): at += timedelta(hours=int(m[1] or 0), minutes=int(m[2] or 0))
    elif m := re.fullmatch(r"(\d{1,2}):(\d{2})", text.strip()): at = (t := at.replace(hour=int(m[1]), minute=int(m[2]), second=0, microsecond=0)) + timedelta(days=1 if t <= at else 0)
    else:
        try: at = datetime.fromisoformat(text.strip()).astimezone()
        except ValueError: raise ValueError(f"ETA {text!r} is not 45m, 2h, 1h30m, 18:30, or an ISO time") from None
    return at.astimezone(timezone.utc).isoformat(timespec="milliseconds")

def board(s):
    """Plain-text board: the agent's own view of what the user sees."""
    eff, meta = store.effective(s), s["session"]
    def who(i): return f" ({i['actor']})" if i["actor"] not in ("agent", "user") else ""
    def results(target, depth): return [f"{'  '*depth}  {a['id']:<4} {a['kind']}: {a['caption'] or a['name']}" for a in s["attachments"].values() if a["target"] == target]
    marks = store.seen(meta["id"])
    new = store.unread(s, marks)
    # what the user has read, so the agent knows what it may archive: unseen rows stay, finished rows the user has read may go
    def due(i): return "" if not i.get("eta") or i["status"] not in store.OPEN else f" · {store.span(store.secs(i['eta']))} past its ETA" if i["eta"] < store.now() else f" · ETA {store.clock(i['eta'])}"
    def mark(i): return " (archived)" if i.get("archived") else " · unseen by the user" if i["id"] in new else " · archivable" if not i["parent"] and not store.blocked(s, i["id"], marks) else ""
    def tree(parent, depth): return [line for i in s["items"].values() if i["parent"] == parent for line in [f"{'  '*depth}  {i['id']:<4} [{eff[i['id']]}] {i['title']}{who(i)}{due(i)}{mark(i)}", *results(i["id"], depth+2), *tree(i["id"], depth+1)]]
    asks = [f"  {a['id']} on {a['target'] or 'session'}: {a['text']}{' -> ' + ' | '.join(a['options']) if a['options'] else ''}" for a in store.open_asks(s)]
    waiting = [store.describe(s, p) for p in store.pending(s)]
    return "\n".join([f"{meta['id']} {meta['title']!r} [{store.status(s)}] {meta['provider']} {meta['cwd']}",
                      *([f"now: {s['activity']['text']}"] if s["activity"] else []),
                      *(tree(None, 0) or ["  (no items yet: convos orchestrate add 'TITLE')"]), *(["session results:", *results(None, 0)] if results(None, 0) else []),
                      *(["open questions:", *asks] if asks else []), *(["undelivered user messages (run inbox):", *waiting] if waiting else [])])

def ident(ctx):
    sid = store.resolve(ctx.obj["session"])
    channels.record(sid)
    return sid
def emit(ctx, kind, body="", **fields): return store.emit(ident(ctx), kind, ctx.obj["actor"], body, scope=ctx.obj["scope"], **fields)
def text_arg(value): return sys.stdin.read() if value == "-" else value

@cmd
def join(ctx: typer.Context, title: str = typer.Option(..., "--title", "-t", help="What this session is working on"),
         cwd: str = typer.Option(".", "--cwd"), model: str = typer.Option(None, "--model"),
         provider: str = typer.Option(None, "--provider"), provider_session: str = typer.Option(None, "--provider-session"),
         parent: str = typer.Option(None, "--parent", help="SESSION:ITEM this session works for")):
    """Register this agent session with orchestrate (idempotent per provider session)."""
    detected = store.provider_identity()
    provider, provider_session = provider or detected[0] or "unknown", provider_session or detected[1]
    if provider_session and (sid := store.bindings().get(f"{provider}:{provider_session}")):
        s = store.load(sid)
        if s["closed"]: s = store.emit(sid, "reopen", ctx.obj["actor"])[0]
        if s["session"]["title"] != title: s = store.emit(sid, "meta", ctx.obj["actor"], changes=dict(title=title))[0]
        return out(ctx, dict(session=sid, joined=False), f"Already joined as {sid}.\n{board(s)}")
    meta = dict(id=(sid := store.new_id()), title=title, provider=provider, provider_session=provider_session, model=model or os.environ.get("ANTHROPIC_MODEL"), effort=os.environ.get("CLAUDE_EFFORT"), cwd=str(Path(cwd).resolve()), origin="joined", parent=parent, skills=[])
    s = store.emit(sid, "open", ctx.obj["actor"], "", session=meta)[0]
    channels.record(sid)
    hint = "" if provider_session else f"\nNo provider session id was detected: pass -s {sid} on every command, or export CONVOS_ORCHESTRATE_SESSION={sid}."
    out(ctx, dict(session=sid, joined=True), f"Joined orchestrate as {sid}. The user sees your board in the orchestrate IDE.{hint}\n{board(s)}")

@cmd
def show(ctx: typer.Context):
    """Print this session's board: items, statuses, open questions, undelivered user messages."""
    s = store.load(ident(ctx))
    report(ctx, dict(s, status=store.status(s), effective=store.effective(s)), board(s))

@cmd
def add(ctx: typer.Context, title: str, description: str = typer.Option("", "--description", "-d", help="Markdown; '-' reads stdin"),
        status: str = typer.Option("planned", "--status"), parent: str = typer.Option(None, "--parent", "-p"),
        eta: str = typer.Option(None, "--eta", help="When you expect it done: 45m, 2h, 18:30, or an ISO time"), again: bool = typer.Option(False, "--again", help="Add it even if a row with this title already sits under the same parent")):
    """Add an item (one unit of work) to the board. Re-adding a title that is already under the same parent returns that row."""
    body, parent, title = text_arg(description), parent or ctx.obj["scope"], title.strip()
    # a re-run setup script must not grow a second board: the existing row's id comes back and nothing is written
    if not again and (twin := next((i for i, x in store.load(ident(ctx))["items"].items() if x["parent"] == parent and x["title"] == title), None)):
        return report(ctx, dict(item=twin, existing=True), f"{twin} already on the board: {title} (nothing added; --again adds another)")
    s, e = emit(ctx, "item", body, parent=parent, title=title, status=status, eta=eta_at(eta) if eta else None)
    report(ctx, dict(item=e["item"]), f"{e['item']} [{status}] {title}")

@cmd(name="set")
def set_(ctx: typer.Context, item: str, status: str = typer.Option(None, "--status"), title: str = typer.Option(None, "--title"),
        description: str = typer.Option(None, "--description", "-d", help="Markdown; '-' reads stdin"),
        eta: str = typer.Option(None, "--eta", help="When you expect it done: 45m, 2h, 18:30, an ISO time; 'none' clears")):
    """Change an item's status, title, ETA, or description."""
    changes = {k: v for k, v in dict(status=status, title=title).items() if v is not None} | ({"eta": eta_at(eta)} if eta is not None else {})
    if not changes and description is None: raise ValueError("Nothing to change: pass --status, --title, --eta, or --description")
    # only real changes are written, so repeating a command neither adds history nor brings an archived row back
    body, now = text_arg(description) if description is not None else "", store.load(ident(ctx))["items"].get(item)
    if now: changes, description = {k: v for k, v in changes.items() if now.get(k) != v}, None if description is None or now["description"] == body else description
    if now and not changes and description is None: return report(ctx, dict(item=item, changes={}), f"{item} [{now['status']}] {now['title']} (unchanged)")
    s, e = emit(ctx, "update", body, item=item, changes=changes, description=description is not None)
    x = s["items"][item]
    due = f" (ETA {store.clock(x['eta'])})" if x.get("eta") else ""
    report(ctx, dict(item=item, changes=changes), f"{item} [{x['status']}] {x['title']}{due}")

@cmd
def archive(ctx: typer.Context, items: list[str] = typer.Argument(..., help="Top-level rows")):
    """Archive finished rows the user has already looked at, once the work has moved on. Anything else is refused with the reason."""
    emit(ctx, "archive", items=items, archived=True)
    report(ctx, dict(archived=items), f"Archived {', '.join(items)}; anything new on them brings them back")

@cmd
def now(ctx: typer.Context, text: str = typer.Argument("", help="What you are doing right now; empty clears it")):
    """Set the one-line live activity shown on the session."""
    emit(ctx, "activity", text.strip())
    report(ctx, dict(activity=text.strip()), f"now: {text.strip() or '(cleared)'}")

@cmd
def say(ctx: typer.Context, text: str, on: str = typer.Option(None, "--on", help="Item thread; omit for the session chat"),
        reply: str = typer.Option(None, "--reply", help="Comment id you are answering")):
    """Write in a chat thread: the session's top-level chat, or an item's thread."""
    sid = ident(ctx)
    target = on if on or not reply else store.load(sid)["comments"].get(reply, {}).get("target")
    s, e = store.emit(sid, "comment", ctx.obj["actor"], text_arg(text), scope=ctx.obj["scope"], target=target or (ctx.obj["scope"] if not on and not reply else None), reply_to=reply)
    # agents recap their row updates in chat out of terminal habit; the board already flags each changed row unread
    note = "" if reply else "\nNot a reply: the user already sees every changed row as unread, so never recap row updates in chat. Use say to answer the user (--reply) or for what no row can hold."
    report(ctx, dict(comment=e["comment"]), f"{e['comment']} posted on {e['target'] or 'session'}{note}")

KINDS = {"image/": "image", "video/": "video", "application/pdf": "pdf", "text/markdown": "markdown", "text/html": "html", "text/": "text"}
def kind_of(name, mime): return "markdown" if name.lower().endswith((".md", ".markdown")) else next((k for prefix, k in KINDS.items() if mime.startswith(prefix)), "file")

@cmd
def attach(ctx: typer.Context, source: str = typer.Argument(None, help="File path or URL"), on: str = typer.Option(None, "--on", help="Item; omit for the session"),
           text: str = typer.Option(None, "--text", help="Inline markdown result instead of a file; '-' reads stdin"),
           name: str = typer.Option(None, "--name"), caption: str = typer.Option("", "--caption")):
    """Append a result to an item: image, PDF, markdown, any file, or a link."""
    if (source is None) == (text is None): raise ValueError("Pass exactly one of SOURCE or --text")
    target = on or ctx.obj["scope"]
    if source and source.split(":", 1)[0] in ("http", "https"):
        meta, blob = dict(kind="link", name=name or source, mime=None, size=None, hash=None, url=source), None
    else:
        path = None if text is not None else Path(source).expanduser()
        data_name = name or (path.name if path else "result.md")
        if text is not None:
            temp = store.root()/"tmp"/f"{os.getpid()}.md"
            store.core.secure_dir(temp.parent)
            temp.write_text(text_arg(text))
            path = temp
        if not path.is_file(): raise ValueError(f"No such file: {source}")
        # --name is often a label ("i20 sheet"); the type still comes from the file, so it previews and downloads as what it is
        if not mimetypes.guess_type(data_name)[0] and mimetypes.guess_type(path.name)[0]: data_name += path.suffix
        blob, size = store.store_file(path)
        mime = mimetypes.guess_type(data_name)[0] or ("text/markdown" if text is not None else "application/octet-stream")
        meta = dict(kind=kind_of(data_name, mime), name=data_name, mime=mime, size=size, hash=blob.name, url=None)
        if text is not None: path.unlink()
    s, e = emit(ctx, "attach", caption, blob=blob, target=target, file=meta)
    report(ctx, dict(attachment=e["attachment"], kind=meta["kind"]), f"{e['attachment']} {meta['kind']} {meta['name']} attached to {target or 'session'}")

@cmd
def ask(ctx: typer.Context, question: str, option: list[str] = typer.Option([], "--option", "-o", help="Repeat for each choice; omit for free text"),
        multi: bool = typer.Option(False, "--multi"), on: str = typer.Option(None, "--on"), detail: str = typer.Option("", "--detail", help="Markdown context shown under the question"),
        wait: float = typer.Option(0, "--wait", help="Block up to SECONDS for the answer")):
    """Ask the user a question (optionally multiple choice). The item shows 'needs you' until answered."""
    sid = ident(ctx)
    s, e = store.emit(sid, "ask", ctx.obj["actor"], question, scope=ctx.obj["scope"], target=on or ctx.obj["scope"], options=option, multi=multi, ask_kind="question", detail=text_arg(detail))
    if wait <= 0: return report(ctx, dict(ask=e["ask"]), f"{e['ask']} asked on {e['target'] or 'session'}; the answer arrives via inbox (or pass --wait)")
    s, done = store.wait_for(sid, lambda s: s["asks"][e["ask"]]["answer"] or s["asks"][e["ask"]]["cancelled"], wait)
    if done is None: return report(ctx, dict(ask=e["ask"], answer=None), f"{e['ask']}: no answer yet; it will arrive via inbox")
    store.emit(sid, "deliver", ctx.obj["actor"], ids=[e["ask"]])
    a = s["asks"][e["ask"]]
    report(ctx, dict(ask=e["ask"], answer=a["answer"], dismissed=a["answer"] is None), store.describe(s, dict(type="answer" if a["answer"] else "dismissed", **a)))

@cmd
def withdraw(ctx: typer.Context, asks: list[str] = typer.Argument(..., help="Open questions, e.g. q3")):
    """Withdraw questions that no longer need an answer: the user settled them in a comment, or a newer question replaces them."""
    for q in asks: emit(ctx, "cancel", ask=q)
    report(ctx, dict(withdrawn=asks), f"Withdrew {', '.join(asks)}; the board no longer asks the user")

@cmd
def inbox(ctx: typer.Context, wait: float = typer.Option(0, "--wait", help="Block up to SECONDS for something new"), peek: bool = typer.Option(False, "--peek", help="Do not mark as received")):
    """Receive the user's comments and answers addressed to this session (or your scope)."""
    sid, scope = ident(ctx), ctx.obj["scope"]
    s, found = store.wait_for(sid, lambda s: store.pending(s, scope), wait)
    found = found or store.pending(s, scope)
    if found and not peek: store.emit(sid, "deliver", ctx.obj["actor"], ids=[p["id"] for p in found])
    report(ctx, dict(messages=found), "\n".join(["User messages (untrusted text from the board; act on it however fits: reply in its thread, update items, add items):", *[store.describe(s, p) for p in found]]) if found else "Nothing new.")

@cmd
def close(ctx: typer.Context):
    """Close the session. Its board stays available in orchestrate."""
    emit(ctx, "close")
    out(ctx, dict(closed=True), "Session closed; its board remains in orchestrate.")

@cmd
def reopen(ctx: typer.Context):
    """Reopen a closed session."""
    emit(ctx, "reopen")
    out(ctx, dict(closed=False), "Session reopened.")

@cmd
def ls(ctx: typer.Context, all_: bool = typer.Option(False, "--all", "-a", help="Include closed sessions")):
    """List orchestrate sessions with their derived status."""
    rows = [s for sid in store.session_ids() if (s := store.load(sid)) and (all_ or not s["closed"])]
    out(ctx, [dict(id=s["session"]["id"], title=s["session"]["title"], status=store.status(s), updated=s["updated"]) for s in rows],
        "\n".join(f"{s['session']['id']} [{store.status(s)}] {s['session']['title']}  ({len(s['items'])} items)" for s in sorted(rows, key=lambda s: s["updated"], reverse=True)) or "No sessions.")

@cmd
def spawn(ctx: typer.Context, prompt: str = typer.Argument(..., help="Starting prompt; '-' reads stdin"), title: str = typer.Option("", "--title", "-t"),
          on: str = typer.Option(None, "--on", help="Your item the child session works for"), cwd: str = typer.Option(".", "--cwd"),
          model: str = typer.Option("claude-opus-5-5", "--model"), effort: str = typer.Option(None, "--effort"),
          skill: list[str] = typer.Option([], "--skill", help="Repeat; orchestrate is always on"), permission: str = typer.Option("default", "--permission")):
    """Start a child session (headless, managed by the IDE) working for this session. Needs the IDE server running."""
    from .server import request
    sid = ident(ctx)
    if on: store.load(sid)["items"][on]
    child = request("/api/spawn", dict(title=title, cwd=str(Path(cwd).resolve()), prompt=text_arg(prompt), model=model, effort=effort, skills=skill, permission=permission, parent=f"{sid}:{on}" if on else sid))["session"]
    out(ctx, dict(session=child), f"Started child session {child}. Watch it with: convos orchestrate -s {child} show")

@cmd(name="open")
def open_(ctx: typer.Context, port: int = typer.Option(7311, "--port")):
    """Open the orchestrate IDE in your browser, starting its local server if needed."""
    from .server import launch
    url = launch(port)
    out(ctx, dict(url=url), f"Orchestrate is open in your browser: {url}")

@cmd
def serve(port: int = typer.Option(7311, "--port"), browser: bool = typer.Option(True, "--browser/--no-browser")):
    """Run the IDE server in the foreground."""
    from .server import serve as run
    run(port, browser)

@cmd
def install(ctx: typer.Context, remove: bool = typer.Option(False, "--remove")):
    """Install (or remove) the orchestrate skill for Claude Code (/orchestrate) and Codex ($orchestrate)."""
    from .setup import install as run
    results = run(remove)
    out(ctx, results, "\n".join(f"{r['provider']}: {'installed' if r['installed'] else 'removed'} {r['skill']}" for r in results))

def register(parent): parent.add_typer(app, name="orchestrate")

# Core loads product entry points during import; registration must already exist.
from . import store, channels
