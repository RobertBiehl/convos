"""Local semantic navigation across the conversation archive."""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import typer

_CLEAN = """COALESCE(m.content,'')!='' AND json_extract_string(m.metadata,'$.history_of') IS NULL
AND NOT regexp_matches(m.content,'^(Base directory for this skill:|# AGENTS\\.md instructions for|<(codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)( |>))')"""

def _fail(message):
    typer.echo(message,err=True)
    raise typer.Exit(1)
def _db():
    from ai_convos.cli import get_db
    try: db=get_db(read_only=True,purpose="explore")
    except ValueError as e: _fail(str(e))
    if db is None: _fail("No database. Run `convos init` first.")
    return db
def _clip(value,n): return (value or "")[:n]+("..." if value and len(value)>n else "")
def _target(db,target):
    convs=db.execute("SELECT id,title,source,cwd FROM conversations WHERE starts_with(id,?) ORDER BY id=? DESC,id LIMIT 3",(target,target)).fetchall()
    msgs=db.execute("""SELECT m.id,c.id,c.title,c.source,c.cwd,m.role,m.content,m.embedding FROM messages m JOIN conversations c ON c.id=m.conversation_id
        WHERE starts_with(m.id,?) AND json_extract_string(m.metadata,'$.history_of') IS NULL ORDER BY m.id=? DESC,m.id LIMIT 3""",(target,target)).fetchall()
    exact=[("conversation",*r) for r in convs if r[0]==target]+[("message",*r) for r in msgs if r[0]==target]
    found=exact or [("conversation",*r) for r in convs]+[("message",*r) for r in msgs]
    if len(found)!=1: _fail("No matching target" if not found else "Ambiguous target prefix: "+", ".join(r[1] for r in found))
    kind,*row=found[0]
    if kind=="message":
        mid,cid,title,source,cwd,role,content,vector=row
        if vector is None: _fail("Target message has no embedding. Run `convos embed`.")
        return dict(type=kind,id=mid,conversation_id=cid,title=title,source=source,cwd=cwd,role=role,content=content,vector=vector)
    cid,title,source,cwd=row
    vectors=db.execute(f"""SELECT m.embedding FROM messages m WHERE m.conversation_id=? AND m.embedding IS NOT NULL AND m.role IN ('user','human') AND {_CLEAN}
        ORDER BY m.created_at DESC NULLS LAST,m.id DESC LIMIT 32""",(cid,)).fetchall() or db.execute(f"""SELECT m.embedding FROM messages m WHERE m.conversation_id=? AND m.embedding IS NOT NULL AND {_CLEAN}
        ORDER BY m.created_at DESC NULLS LAST,m.id DESC LIMIT 32""",(cid,)).fetchall()
    if not vectors: _fail("Target conversation has no usable embeddings. Run `convos embed`.")
    return dict(type=kind,id=cid,conversation_id=cid,title=title,source=source,cwd=cwd,role=None,content=None,vector=[sum(x)/len(vectors) for x in zip(*(r[0] for r in vectors))])
def _neighbors(db,seed,source=None,days=None,role=None,limit=10,context=300,minimum=-1.0,exclude=(),exclude_content=()):
    skip=sorted(set(exclude)|{seed["conversation_id"]})
    where=[f"c.id NOT IN ({','.join('?' for _ in skip)})"]
    params=[seed["vector"],*skip]
    if exclude_content:
        content=sorted(exclude_content)
        where.append(f"m.content NOT IN ({','.join('?' for _ in content)})")
        params+=content
    if source:
        where.append("c.source=?")
        params.append(source)
    if days:
        where.append("m.created_at>?")
        params.append(datetime.now()-timedelta(days=days))
    if role:
        where.append("m.role=?")
        params.append(role)
    rows=db.execute(f"""WITH scored AS (SELECT c.id conversation_id,c.title,c.source,c.cwd,c.updated_at,m.id message_id,m.role,m.content,m.created_at,m.embedding vector,
            list_cosine_similarity(m.embedding,?::FLOAT[]) similarity FROM messages m JOIN conversations c ON c.id=m.conversation_id
            WHERE m.embedding IS NOT NULL AND {_CLEAN} AND {' AND '.join(where)}),
        distinct_turns AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY content ORDER BY updated_at DESC NULLS LAST,message_id) duplicate_rank FROM scored),
        ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY conversation_id ORDER BY similarity DESC,message_id) rank FROM distinct_turns WHERE duplicate_rank=1 AND similarity>=?)
        SELECT similarity,message_id,role,content,created_at,title,source,conversation_id,cwd,vector FROM ranked WHERE rank=1 ORDER BY similarity DESC,conversation_id LIMIT ?""",params+[minimum,limit]).fetchall()
    return [dict(target_type=seed["type"],target_id=seed["id"],target_conversation_id=seed["conversation_id"],similarity=score,message_id=mid,role=r,content=_clip(content,context),raw_content=content,created_at=ts,title=title,source=src,conversation_id=cid,cwd=cwd,vector=vector) for score,mid,r,content,ts,title,src,cid,cwd,vector in rows]
def _public(row): return {k:v for k,v in row.items() if k not in ("vector","raw_content")}
def _child(row): return dict(type="message",id=row["message_id"],conversation_id=row["conversation_id"],title=row["title"],source=row["source"],cwd=row["cwd"],role=row["role"],content=row["content"],vector=row["vector"])
def _walk(db,root,depth=2,width=3,max_nodes=20,minimum=.65,source=None,days=None,role=None,context=160):
    nodes=[dict(conversation_id=root["conversation_id"],title=root["title"],source=root["source"],cwd=root["cwd"],depth=0,seed_type=root["type"],seed_id=root["id"],role=root["role"],content=_clip(root["content"],context))]
    edges=[]
    frontier=[root]
    seen={root["conversation_id"]}
    contents={root["content"]} if root["content"] else set()
    for level in range(1,depth+1):
        upcoming=[]
        for parent in frontier:
            if len(nodes)>=max_nodes: break
            for row in _neighbors(db,parent,source,days,role,min(width,max_nodes-len(nodes)),context,minimum,seen,contents):
                child=_child(row)
                seen.add(child["conversation_id"])
                contents.add(row["raw_content"])
                upcoming.append(child)
                nodes.append(dict(conversation_id=child["conversation_id"],title=child["title"],source=child["source"],cwd=child["cwd"],depth=level,seed_type="message",seed_id=child["id"],role=child["role"],content=child["content"]))
                edges.append(dict(depth=level,from_conversation_id=parent["conversation_id"],to_conversation_id=child["conversation_id"],similarity=row["similarity"],message_id=row["message_id"],role=row["role"],content=row["content"],created_at=row["created_at"]))
        frontier=upcoming
        if not frontier or len(nodes)>=max_nodes: break
    return dict(root=nodes[0],nodes=nodes,edges=edges)
def related(target: str, source: Optional[str]=typer.Option(None,"-s"), days: Optional[int]=typer.Option(None,"-d",min=1), role: Optional[str]=typer.Option(None,"-r"), limit: int=typer.Option(10,"-n",min=1,max=100), context: int=typer.Option(300,"-c",min=1), fmt: str=typer.Option("text","-f","--format")):
    """Find conversations semantically related to a conversation or exact turn."""
    db=_db()
    seed=_target(db,target)
    data=[_public(row) for row in _neighbors(db,seed,source,days,role,limit,context)]
    db.close()
    if fmt!="text":
        if fmt=="jsonl": [typer.echo(json.dumps(row,default=str)) for row in data]
        else: typer.echo(json.dumps(data,default=str))
        return
    typer.echo(f"Related to {seed['type']} {seed['id']} in [{seed['source']}] {seed['title'] or 'Untitled'} ({seed['conversation_id']})")
    if not data:
        typer.echo("No related conversations")
        return
    for i,row in enumerate(data,1): typer.echo(f"\n{i}. {row['similarity']:.3f} [{row['source']}] {row['title'] or 'Untitled'} ({row['conversation_id']})\n   {row['role']} @ {row['created_at'] or '?'} ({row['message_id']})\n   {row['content']}\n   read: convos read {row['conversation_id'][:8]} --around {row['message_id'][:8]}")
def trail(target: str, depth: int=typer.Option(2,"--depth",min=1,max=3), width: int=typer.Option(3,"--width",min=1,max=8), max_nodes: int=typer.Option(20,"--max-nodes",min=2,max=100), minimum: float=typer.Option(.65,"--min-score",min=-1,max=1), source: Optional[str]=typer.Option(None,"-s"), days: Optional[int]=typer.Option(None,"-d",min=1), role: Optional[str]=typer.Option(None,"-r"), context: int=typer.Option(160,"-c",min=1), fmt: str=typer.Option("text","-f","--format")):
    """Walk a bounded multi-hop semantic trail with exact evidence."""
    db=_db()
    try:
        root=_target(db,target)
        result=_walk(db,root,depth,width,max_nodes,minimum,source,days,role,context)
    finally: db.close()
    nodes,edges=result["nodes"],result["edges"]
    if fmt=="json":
        typer.echo(json.dumps(result,default=str))
        return
    if fmt=="jsonl":
        typer.echo(json.dumps(dict(record="root",**nodes[0]),default=str))
        [typer.echo(json.dumps(dict(record="edge",**edge,node=next(n for n in nodes if n["conversation_id"]==edge["to_conversation_id"])),default=str)) for edge in edges]
        return
    if fmt=="dot":
        esc=lambda value:str(value or "Untitled").replace("\\","\\\\").replace('"','\\"').replace("\n"," ")
        typer.echo("digraph trail {\n"+ "\n".join([f'  "{n["conversation_id"]}" [label="{esc(n["source"])} | {esc(n["title"])} | {n["conversation_id"][:8]}"];' for n in nodes]+[f'  "{e["from_conversation_id"]}" -> "{e["to_conversation_id"]}" [label="{e["similarity"]:.3f} | {e["message_id"][:8]}"];' for e in edges])+"\n}")
        return
    typer.echo(f"Semantic trail from {root['type']} {root['id']}\n[0] [{root['source']}] {root['title'] or 'Untitled'} ({root['conversation_id']})")
    by_id={n["conversation_id"]:n for n in nodes}
    for edge in edges:
        node,indent=by_id[edge["to_conversation_id"]],"  "*(edge["depth"]-1)
        typer.echo(f"\n{indent}-> {edge['similarity']:.3f} [{edge['from_conversation_id'][:8]} -> {node['conversation_id'][:8]}] [{node['source']}] {node['title'] or 'Untitled'} ({node['conversation_id']})\n{indent}   evidence: {edge['role']} @ {edge['created_at'] or '?'} ({edge['message_id']})\n{indent}   {edge['content']}\n{indent}   read: convos read {node['conversation_id'][:8]} --around {edge['message_id'][:8]}")
def register(app: typer.Typer):
    for command in (related,trail): app.command()(command)
