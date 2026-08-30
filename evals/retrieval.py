"""Development-only retrieval evaluation over exact archive identities."""
import json
from pathlib import Path
from typing import Optional

import typer
from typer.testing import CliRunner
from ai_convos.cli import app as core_app, drain_hooks, ensure_db_ready, get_db, hybrid_hits

KEYS={"name","query","expect","mode","source","days","role","cwd","conversation","k"}
MODES={"hybrid","literal","both"}

def load_cases(path):
    path=Path(path).expanduser()
    if not path.is_file(): raise ValueError(f"Evaluation file not found: {path}")
    cases=[json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    for i,case in enumerate(cases,1):
        if not isinstance(case,dict) or set(case)-KEYS: raise ValueError(f"Case {i}: unknown or invalid fields")
        if not case.get("name") or not isinstance(case["name"],str) or not case.get("query") or not isinstance(case["query"],str) or not isinstance(case.get("expect"),list) or not case["expect"] or not all(isinstance(x,str) and len(x)>=8 for x in case["expect"]): raise ValueError(f"Case {i}: name, query, and non-empty expect ID prefixes of at least 8 characters are required")
        if case.get("mode","hybrid") not in MODES or "k" in case and (not isinstance(case["k"],int) or not 1<=case["k"]<=100) or "days" in case and (not isinstance(case["days"],int) or case["days"]<1) or any(key in case and not isinstance(case[key],str) for key in ("source","role","cwd","conversation")): raise ValueError(f"Case {i}: invalid option value")
    if not cases: raise ValueError("Evaluation file has no cases")
    return cases
def ground(cases):
    drain_hooks()
    db=get_db(read_only=True)
    if db is None or not ensure_db_ready(db):
        if db: db.close()
        raise ValueError("Archive not ready. Run `convos init` or `convos sync`.")
    for prefix in sorted({x for case in cases for x in case["expect"]}):
        rows=db.execute("SELECT id FROM conversations WHERE starts_with(id,?) UNION ALL SELECT id FROM messages WHERE starts_with(id,?)",[prefix,prefix]).fetchall()
        if len(rows)!=1:
            db.close()
            raise ValueError(f"Expected ID prefix {prefix} is {'missing' if not rows else 'ambiguous'}")
    db.close()
def _literal(case,k):
    args=["search",case["query"],"-n",str(k),"-f","json"]
    for key,flag in (("source","-s"),("days","-d"),("role","-r"),("cwd","--cwd"),("conversation","--conversation")):
        if case.get(key) is not None: args.extend((flag,str(case[key])))
    result=CliRunner().invoke(core_app,args)
    if result.exit_code: raise ValueError(result.output.strip() or "literal retrieval failed")
    try: return json.loads(result.output)
    except json.JSONDecodeError as e: raise ValueError(f"literal retrieval failed: {result.output.strip()}") from e
def _rank(hits,expected):
    return next((i for i,h in enumerate(hits,1) if any(str(h[key]).startswith(prefix) for prefix in expected for key in ("conversation_id","message_id"))),None)
def run(cases,mode="hybrid",limit=10):
    results=[]
    for case in cases:
        for engine in (("hybrid","literal") if case.get("mode",mode)=="both" else (case.get("mode",mode),)):
            k=case.get("k",limit)
            try:
                hits=hybrid_hits(case["query"],case.get("source"),case.get("days"),case.get("role"),k,local_only=True,cwd=case.get("cwd"),conversation=case.get("conversation")) if engine=="hybrid" else _literal(case,k)
                rank=_rank(hits,case["expect"])
                results.append(dict(name=case["name"],engine=engine,status="hit" if rank else "miss",rank=rank,k=k,returned=[dict(conversation_id=h["conversation_id"],message_id=h["message_id"],score=h["score"]) for h in hits],read=f"convos read {hits[rank-1]['conversation_id']} --around {hits[rank-1]['message_id']}" if rank else None))
            except Exception as e: results.append(dict(name=case["name"],engine=engine,status="error",error=str(e),rank=None,k=k,returned=[],read=None))
    engines={engine:(lambda rows:dict(runs=len(rows),hits=sum(r["rank"] is not None for r in rows),hit_rate=sum(r["rank"] is not None for r in rows)/len(rows),mrr=sum(1/r["rank"] if r["rank"] else 0 for r in rows)/len(rows),errors=sum(r["status"]=="error" for r in rows)))([r for r in results if r["engine"]==engine]) for engine in sorted({r["engine"] for r in results})}
    return dict(status="ready" if not any(r["status"]=="error" for r in results) else "errors",cases=len(cases),runs=len(results),engines=engines,results=results)
def render(data):
    typer.echo("# Convos retrieval eval")
    for engine,m in data["engines"].items(): typer.echo(f"\n{engine}: {m['hits']}/{m['runs']} hit@k ({m['hit_rate']:.1%}), MRR {m['mrr']:.3f}, errors {m['errors']}")
    for row in data["results"]: typer.echo(f"- {row['status'].upper()} [{row['engine']}] {row['name']}: "+(f"rank {row['rank']}; `{row['read']}`" if row["rank"] else row.get("error","no relevant ID returned")))
def eval_cmd(cases:Path,mode:str=typer.Option("hybrid","--mode"),limit:int=typer.Option(10,"-k",min=1,max=100),min_hit_rate:float=typer.Option(0.0,"--min-hit-rate",min=0,max=1),fmt:str=typer.Option("text","-f","--format")):
    """Measure retrieval against private exact-ID relevance judgments."""
    if mode not in MODES: raise typer.BadParameter("must be hybrid, literal, or both","--mode")
    if fmt not in ("text","json"): raise typer.BadParameter("must be text or json","--format")
    try:
        loaded=load_cases(cases)
        ground(loaded)
        data=run(loaded,mode,limit)
    except (ValueError,json.JSONDecodeError) as e:
        typer.echo(str(e),err=True)
        raise typer.Exit(1)
    typer.echo(json.dumps(data)) if fmt=="json" else render(data)
    if data["status"]=="errors" or any(m["hit_rate"]<min_hit_rate for m in data["engines"].values()): raise typer.Exit(1)

if __name__ == "__main__": typer.run(eval_cmd)
