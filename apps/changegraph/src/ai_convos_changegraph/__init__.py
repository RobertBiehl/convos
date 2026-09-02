"""Change graph over the convos DB (spec 02): blame / timeline / at / graph.

Reads the core DB only (read-only). Attribution is exact or labeled unknown -- never invented:
once an edit cannot be replayed (shell, missing or unmatched old_content), content is unknown
until the next full write.
"""
import json
import re
from typing import Optional

import typer
from ai_convos.cli import get_db

_Q = """SELECT fe.edit_type, fe.content, fe.old_content, fe.created_at, COALESCE(m.conversation_id, 'unknown'), COALESCE(c.source, '?'),
    (SELECT u.content FROM messages u WHERE u.conversation_id = m.conversation_id AND u.role = 'user'
     AND u.content != '' AND u.created_at <= fe.created_at ORDER BY u.created_at DESC LIMIT 1)
FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' LEFT JOIN messages m ON fe.message_id = m.id LEFT JOIN conversations c ON c.id = m.conversation_id
WHERE fe.file_path = ? ORDER BY fe.created_at, fe.id"""
_GQ = """SELECT fe.file_path, m.conversation_id, fe.message_id, c.source FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed'
JOIN messages m ON fe.message_id = m.id JOIN conversations c ON c.id = m.conversation_id"""

def edits_for(conn, path: str) -> list[dict]:
    return [dict(zip(("type", "content", "old", "ts", "conv", "source", "prompt"), r)) for r in conn.execute(_Q, [path]).fetchall()]

def cut(edits: list[dict], at: str | None) -> list[dict]:
    """Keep edits up to a conversation (id substring: through its last edit) or a timestamp (ISO prefix)."""
    if at is None: return edits
    if re.fullmatch(r"\d{4}(?:-\d{2}(?:-\d{2}(?:[ T].*)?)?)?", at): return [e for e in edits if (s := str(e["ts"])) <= at or s.startswith(at)]
    if idxs := [i for i, e in enumerate(edits) if at in e["conv"]]: return edits[:idxs[-1] + 1]
    return [e for e in edits if (s := str(e["ts"])) <= at or s.startswith(at)]

def replay(edits: list[dict]):
    """Apply edits in order. Returns (lines, prov) where prov[i] is the edit that last touched lines[i],
    or (None, None) once content is unknowable."""
    text, prov = None, None
    for e in edits:
        if e["type"] == "write":
            text,prov=(text:=e["content"].splitlines()),[e]*len(text)
        elif e["type"] == "edit" and e["old"] and text is not None and e["old"] in (joined := "\n".join(text)):
            pos,start,old_n=(pos:=joined.find(e["old"])),joined[:pos].count("\n"),e["old"].count("\n")+1
            text = (joined[:pos] + e["content"] + joined[pos + len(e["old"]):]).splitlines()
            prov = prov[:start] + [e] * (len(text) - (len(prov) - old_n)) + prov[start + old_n:]
        else: text, prov = None, None
    return text, prov

def _fail(message,code=1):
    typer.echo(message,err=True)
    raise typer.Exit(code)
def _conn(): return get_db(read_only=True,purpose="changegraph") or _fail("No database. Run `convos init` first.")
def _read(fn):
    conn=_conn()
    try: return fn(conn)
    finally: conn.close()

def _known_edits(file: str, at: str | None):
    edits=cut(_read(lambda conn:edits_for(conn,file)),at)
    if not edits: _fail(f"No edits recorded for {file}")
    text, prov = replay(edits)
    if text is None:
        e=edits[-1]
        _fail(f"content unknown: last edit is {e['type']} at {e['ts']} by conversation {e['conv']} ({e['source']})",2)
    return text, prov

def blame(file: str, line: Optional[int] = typer.Option(None, "--line"), at: Optional[str] = typer.Option(None, "--at"),
          fmt: str = typer.Option("text", "--format", "-f")):
    """Per-line attribution: which conversation / prompt produced each line of FILE."""
    text, prov = _known_edits(file, at)
    rows = [dict(line=i + 1, conv=e["conv"], source=e["source"], ts=str(e["ts"]), prompt=(e["prompt"] or "").split("\n")[0][:80], text=t)
            for i, (t, e) in enumerate(zip(text, prov)) if line is None or i + 1 == line]
    if fmt != "text": return typer.echo(json.dumps(rows,default=str))
    [typer.echo(f"{r['line']:>5} {r['conv'][:8]} {r['source']:<11} {r['ts']} | {r['text']}") for r in rows]

def timeline(file: str, fmt: str = typer.Option("text", "--format", "-f")):
    """Chronological edits to FILE across conversations and providers."""
    edits=_read(lambda conn:edits_for(conn,file))
    if not edits: _fail(f"No edits recorded for {file}")
    rows = [dict(ts=str(e["ts"]), conv=e["conv"], source=e["source"], type=e["type"],
                 exact=e["type"] == "write" or bool(e["old"]), prompt=(e["prompt"] or "").split("\n")[0][:80]) for e in edits]
    if fmt != "text": return typer.echo(json.dumps(rows,default=str))
    [typer.echo(f"{r['ts']} {r['conv'][:8]} {r['source']:<11} {r['type']:<9} {'exact' if r['exact'] else 'unknown'} | {r['prompt']}") for r in rows]

def at(file: str, point: str):
    """Reconstruct FILE content as of a conversation (id substring) or timestamp (time-travel)."""
    text, _ = _known_edits(file, point)
    typer.echo("\n".join(text))

def graph(target: Optional[str] = typer.Argument(None), fmt: str = typer.Option("json", "--format", "-f")):
    """Emit the file <-> conversation <-> message edge set (json or dot), filtered by file path or conversation id substring."""
    rows=_read(lambda conn:conn.execute(_GQ).fetchall())
    edges = [dict(file=f, conv=c, msg=m, source=s) for f, c, m, s in rows if target is None or target in f or target in c]
    if fmt == "dot": typer.echo("digraph convos {\n" + "\n".join(sorted({f'  "{e["conv"][:8]}" -> "{e["file"]}";' for e in edges})) + "\n}")
    else: typer.echo(json.dumps(edges, default=str))

def register(app: typer.Typer):
    from .tui import browse
    for cmd in (blame, timeline, at, graph, browse): app.command()(cmd)
