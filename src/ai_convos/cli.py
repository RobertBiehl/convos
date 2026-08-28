#!/usr/bin/env python3
import base64, contextlib, json, time, zipfile, hashlib, struct, sqlite3, subprocess, ssl, urllib.request, re, os, sysconfig, site, csv, sys, shutil, shlex, fcntl, signal, tempfile, duckdb, typer
from importlib.metadata import entry_points, version; from concurrent.futures import ThreadPoolExecutor, as_completed; from contextlib import ExitStack; from datetime import datetime, timedelta, timezone; from functools import lru_cache; from pathlib import Path; from typing import Optional; from hashlib import pbkdf2_hmac; from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from .migrations import fts_needs_rebuild, migrate_remote_changes, migrate_remote_data, migrate_remote_ids, migration_memory, remote_id_migration_scope

app = typer.Typer(help="AI Conversations DB - searchable archive for Claude, ChatGPT, and Codex")
def find_root(): return Path(r).expanduser() if (r := os.environ.get("CONVOS_PROJECT_ROOT")) else Path.home()/".convos"
PROJECT_ROOT = find_root(); DATA_DIR, DB_PATH = PROJECT_ROOT / "data", PROJECT_ROOT / "data" / "convos.db"; STATE_PATH = DATA_DIR / "sync_state.json"
HOOK_DIR,HOOK_STATE,HOOK_PROGRESS,HOOK_EMBED_DIRTY,HOOK_FTS_DIRTY,_NOISE_RE,_NOISE,HOOK_DRAIN_EVENTS,HOOK_DRAIN_SECONDS,CHATGPT_BURST,CHATGPT_RATE,PARSER_EPOCH=DATA_DIR/"hook_inbox",DATA_DIR/"hook_state.json",DATA_DIR/"hook_progress.json",DATA_DIR/"hook_embeddings_dirty",DATA_DIR/"hook_fts_dirty",(_NR:=r"^(Base directory for this skill:|# AGENTS\.md instructions for|<(codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)( |>))"),f" AND NOT regexp_matches(content,'{_NR}')",8,10,20,8/15,3  # conservative policy below the observed ~200-detail failure point
_INJECTED_RE=r"(?s)(?:# AGENTS\.md instructions for [^\n]+\n\n<INSTRUCTIONS>\n.*\n</INSTRUCTIONS>|<(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)(?: [^>]*)?>.*</(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)>)\s*"
MESSAGE_ORDER,MESSAGE_ORDER_DESC="m.created_at NULLS FIRST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) NULLS LAST,m.id","m.created_at DESC NULLS LAST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) DESC NULLS LAST,m.id DESC"

def open_db(path=None,read_only=False,wait=30,deadline=None):
    path=Path(path or DB_PATH); path.parent.mkdir(parents=True,exist_ok=True); deadline=deadline if deadline is not None else time.monotonic()+wait
    if read_only and not path.exists(): return None
    while True:
        try: return duckdb.connect(str(path),read_only=read_only)
        except Exception as e:
            if "Conflicting lock is held" not in str(e): raise
            if time.monotonic()<deadline: time.sleep(.05); continue
            raise ValueError(f"Database stayed locked by another convos process for {wait:g} seconds.") from e
class _LockedDB:
    def __init__(self,db,lock): self.db,self.stack=db,ExitStack(); self.stack.callback(lock.close); self.stack.callback(db.close)
    def __getattr__(self,name): return getattr(self.db,name)
    def close(self): self.stack.close()
def _flock(lock,op,deadline,wait):
    while True:
        try: return fcntl.flock(lock,op|fcntl.LOCK_NB)
        except BlockingIOError: time.sleep(min(.05,remaining)) if (remaining:=deadline-time.monotonic())>0 else (_ for _ in ()).throw(ValueError(f"Database stayed locked by another convos process for {wait:g} seconds."))
def get_db(read_only:bool=False,wait=30):
    HOOK_DIR.mkdir(parents=True,exist_ok=True); deadline=time.monotonic()+wait; lock=(HOOK_DIR/".db.lock").open("w")
    try: _flock(lock,fcntl.LOCK_SH if read_only else fcntl.LOCK_EX,deadline,wait); db=open_db(read_only=read_only,wait=wait,deadline=deadline)
    except BaseException: lock.close(); raise
    return _LockedDB(db,lock) if db is not None else (lock.close() or None)
@contextlib.contextmanager
def _transaction(db):
    db.execute("BEGIN")
    try: yield
    except BaseException: db.execute("ROLLBACK"); raise
    else: db.execute("COMMIT")
def required(value,error): return value if value else (_ for _ in ()).throw(error)

def load_state():
    try: return json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    except Exception: return {}

def atomic_write(path: Path, text):
    if path.is_symlink() or path.exists() and not path.is_file(): typer.echo(f"Refusing unsafe managed file: {path}", err=True); raise typer.Exit(1)
    path.parent.mkdir(parents=True, exist_ok=True); mode = path.stat().st_mode&0o777 if path.exists() else 0o600; fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent); f = os.fdopen(fd, "w"); f.write(text); f.close(); os.chmod(tmp, mode); durable_replace(tmp,path)
def atomic_json(path: Path, data): atomic_write(path, json.dumps(data))
ATTACHMENT_LIMIT=32*1024**2
def durable_replace(tmp,path): tmp,path=map(Path,(tmp,path)); fd=os.open(tmp,os.O_RDONLY); os.fsync(fd); os.close(fd); os.replace(tmp,path); fd=os.open(path.parent,os.O_RDONLY); os.fsync(fd); os.close(fd)
def attachment_body(data,root=None):
    if len(data)>ATTACHMENT_LIMIT: return None
    root=Path(root or DATA_DIR)/"attachments"; required(not root.is_symlink(),ValueError("attachment directory must not be a symlink")); root.mkdir(parents=True,exist_ok=True); os.chmod(root,0o700); blob=hashlib.sha256(data).hexdigest(); path=root/blob
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=len(data): raise ValueError("attachment body conflicts with content hash")
        with path.open("rb") as source: actual=hashlib.file_digest(source,"sha256").hexdigest()
        if actual!=blob: raise ValueError("attachment body conflicts with content hash")
        os.chmod(path,0o600); return path
    fd,tmp=tempfile.mkstemp(prefix=f".{blob}.",dir=root); out=os.fdopen(fd,"wb")
    try:
        os.chmod(tmp,0o600); out.write(data); out.close(); durable_replace(tmp,path)
        return path
    except BaseException: out.close(); Path(tmp).unlink(missing_ok=True); raise

def detect_source(path: Path):
    if path.is_dir(): return "codex" if (path / "sessions").exists() else "claude-code"
    if path.suffix == ".zip" or "chatgpt" in path.name.lower(): return "chatgpt"
    data = required(json.loads(path.read_text()),ValueError(f"Empty export: {path}")); return "chatgpt" if "mapping" in data[0] else "claude" if "chat_messages" in data[0] else "chatgpt"

def stat_mtime(path: Path): return st.st_mtime if (st:=safe_parse(f"path stat {path}",Path.stat,path)) else None
def latest_mtime(path: Path, globs: tuple[str, ...] = ("*.jsonl", "*.json", "*.zip")): return max((m for g in globs for p in path.rglob(g) if (m := stat_mtime(p)) is not None), default=0)

_PROVENANCE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS provenance;
CREATE TABLE IF NOT EXISTS provenance.repositories(id VARCHAR PRIMARY KEY,lineage VARCHAR,roots JSON,remotes JSON,last_head VARCHAR,observed_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS provenance.repository_checkouts(id VARCHAR PRIMARY KEY,repository VARCHAR,root VARCHAR UNIQUE,branch VARCHAR,head VARCHAR);
CREATE TABLE IF NOT EXISTS provenance.repository_aliases(repository VARCHAR,evidence VARCHAR,PRIMARY KEY(repository,evidence));
CREATE TABLE IF NOT EXISTS provenance.conversation_scopes(conversation VARCHAR PRIMARY KEY,cwd VARCHAR,repository VARCHAR,root VARCHAR,checkout VARCHAR,observed_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS provenance.files(id VARCHAR PRIMARY KEY,repository VARCHAR,path VARCHAR,kind VARCHAR);
CREATE TABLE IF NOT EXISTS provenance.file_versions(id VARCHAR PRIMARY KEY,file_id VARCHAR,content_hash VARCHAR,observed_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS provenance.file_edit_scopes(file_edit_id VARCHAR PRIMARY KEY,path VARCHAR,repository VARCHAR,root VARCHAR,checkout VARCHAR,observed_at TIMESTAMP,route VARCHAR);
CREATE TABLE IF NOT EXISTS provenance.file_edit_files(file_edit_id VARCHAR PRIMARY KEY,file_id VARCHAR,old_content_hash VARCHAR,new_content_hash VARCHAR,evidence VARCHAR);
CREATE TABLE IF NOT EXISTS provenance.git_checkpoints(id VARCHAR PRIMARY KEY,repository VARCHAR,head VARCHAR,state_hash VARCHAR,paths JSON,observed_at TIMESTAMP,capture_source VARCHAR);
CREATE TABLE IF NOT EXISTS provenance.checkpoint_edits(checkpoint_id VARCHAR,file_edit_id VARCHAR,evidence VARCHAR,PRIMARY KEY(checkpoint_id,file_edit_id));
CREATE TABLE IF NOT EXISTS provenance.local_facts(kind VARCHAR,entity VARCHAR,PRIMARY KEY(kind,entity));
CREATE SCHEMA IF NOT EXISTS remote;
CREATE TABLE IF NOT EXISTS remote.row_origins(table_name VARCHAR,physical_row_id VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,author_device_id VARCHAR,source_row_id VARCHAR,source_event_id VARCHAR,content_key VARCHAR,observed_at TIMESTAMP,proof_id VARCHAR,PRIMARY KEY(table_name,physical_row_id));
CREATE TABLE IF NOT EXISTS remote.row_signers(author_user_id VARCHAR,author_device_id VARCHAR,root_public VARCHAR,certificate JSON,PRIMARY KEY(author_user_id,author_device_id));
CREATE TABLE IF NOT EXISTS remote.workspace_controls(workspace_id VARCHAR,revision UINTEGER,epoch UINTEGER,state_hash VARCHAR,control JSON,PRIMARY KEY(workspace_id,revision));
CREATE TABLE IF NOT EXISTS remote.row_proofs(id VARCHAR PRIMARY KEY,workspace_id VARCHAR,authorization_workspace_id VARCHAR,row_kind VARCHAR,source_row_id VARCHAR,encoding_v USMALLINT,content_hash VARCHAR,revision VARCHAR,previous_revision VARCHAR,state VARCHAR,author_user_id VARCHAR,author_device_id VARCHAR,authorization_epoch UINTEGER,signature VARCHAR);
CREATE TABLE IF NOT EXISTS remote.row_conflicts(proof_id VARCHAR PRIMARY KEY,body JSON);
CREATE TABLE IF NOT EXISTS remote.provider_session_aliases(workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,revision VARCHAR,source VARCHAR,session_id VARCHAR,members JSON,canonical_source_row_id VARCHAR,proof JSON,PRIMARY KEY(author_user_id,object_id,revision));
CREATE TABLE IF NOT EXISTS remote.provenance_origins(kind VARCHAR,physical_entity VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,source_entity VARCHAR,proof_id VARCHAR,PRIMARY KEY(kind,physical_entity,workspace_id,author_user_id));
CREATE TABLE IF NOT EXISTS attachment_bodies(attachment_id VARCHAR PRIMARY KEY,content_hash VARCHAR NOT NULL,size UINTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS core_schema(singleton BOOLEAN PRIMARY KEY,version USMALLINT NOT NULL);
CREATE TABLE IF NOT EXISTS core_migrations(name VARCHAR PRIMARY KEY,state VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS archive_state(singleton BOOLEAN PRIMARY KEY,archive_id UUID NOT NULL,generation UBIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS archive_changes(kind VARCHAR,entity VARCHAR,generation UBIGINT,PRIMARY KEY(kind,entity));
"""
ARCHIVE_COLUMNS={"conversations":["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"],"messages":["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"],"tool_calls":["id","message_id","tool_name","input","output","status","duration_ms","created_at"],"attachments":["id","message_id","filename","mime_type","size","path","url","created_at"],"artifacts":["id","conversation_id","artifact_type","title","content","language","created_at","version"],"file_edits":["id","message_id","file_path","edit_type","content","created_at","old_content"]}
_MSG_UPDATES,_MSG_UPS,PROVENANCE_KINDS=(u:=",".join(f"{c}=excluded.{c}" for c in ARCHIVE_COLUMNS["messages"][1:])+",embedding=excluded.embedding"),f"INSERT INTO messages ({','.join(ARCHIVE_COLUMNS['messages'])},embedding) SELECT x.*,m.embedding FROM (VALUES ({','.join('?'*len(ARCHIVE_COLUMNS['messages']))})) x({','.join(ARCHIVE_COLUMNS['messages'])}) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content ON CONFLICT(id) DO UPDATE SET {u}",{"repository.observed","file.observed","file.version","edit.observed","git.checkpoint","checkpoint.link"}
def provenance_digest(v): return hashlib.sha256(v if isinstance(v,bytes) else json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()).hexdigest()
def remote_id(author,table,source): return provenance_digest(f"{author}:{table}:{source}")[:16] if source is not None else None
def _archive_touch(db,rows=()): generation=(db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone() or [0])[0]; rows and _insert_pages(db,"archive_changes",[(kind,entity,generation) for kind,entity in rows],mode=" OR REPLACE"); return generation
def archive_changes(db,since): return db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],db.execute("SELECT kind,entity FROM archive_changes WHERE generation>?",(since,)).fetchall()
def archive_state(db):
    archive_id,generation=db.execute("SELECT archive_id::VARCHAR,generation FROM archive_state WHERE singleton").fetchone(); local=sum(db.execute(f"SELECT COUNT(*) FROM {table} x WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name=? AND o.physical_row_id=x.id)",(table,)).fetchone()[0] for table in ARCHIVE_COLUMNS); return archive_id,generation,local
def _git_run(root,*args): return subprocess.run(("git","-C",str(root),*args),capture_output=True,check=True).stdout
def _git_maybe(root,*args):
    try: return _git_run(root,*args)
    except (subprocess.CalledProcessError,FileNotFoundError): return b""
@lru_cache(maxsize=4096)
def _git_root(path):
    if not (probe:=(probe if (probe:=Path(path)).is_dir() else probe.parent)).exists(): return None
    try: return Path(_git_run(probe,"rev-parse","--show-toplevel").decode().strip()).resolve()
    except subprocess.CalledProcessError as e:
        if b"not a git repository" in e.stderr.lower(): return None
        raise
def _remote(url):
    p=__import__("urllib.parse").parse.urlparse(re.sub(r"^(?:[^/@]+@)?([^:/]+):",r"ssh://\1/",url) if "://" not in url else url)
    port=p.port
    return f"https://{p.hostname.lower()}{f':{port}' if port and port!={'ssh':22,'https':443,'http':80}.get(p.scheme.lower()) else ''}/{p.path.strip('/').removesuffix('.git')}" if p.hostname and p.path else None
def _git_remotes(root): return sorted({remote for line in _git_run(root,"remote","-v").decode(errors="replace").splitlines() if "\t" in line and not (url:=line.split("\t",1)[1].rsplit(" ",1)[0]).startswith(("/","file:")) and (remote:=_remote(url))})
def repository_evidence(value): return provenance_digest({"lineage":value["lineage"],"remotes":value["remotes"]}) if value["lineage"] else None
def _checkout(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}" if (stat:=next((p.stat() for p in [Path(root)/'.git'] if p.exists()),None)) else str(root))[:32]
def _unborn(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}:{getattr(stat,'st_birthtime_ns',stat.st_ctime_ns)}" if (stat:=next((p.stat() for p in [Path(root)/'.git/config',Path(root)/'.git'] if p.exists()),None)) else str(root))
def _git_marker(path):
    p=p if (p:=Path(path)).is_dir() else p.parent
    return next(((str(root.resolve()),_checkout(root)) for root in (p,*p.parents) if (root/".git").exists()),(None,None))
@lru_cache(maxsize=256)
def _repository(root): return (lambda root,roots,remotes,lineage:dict(id=provenance_digest({"lineage":lineage,"remotes":remotes}) if lineage else _unborn(root),lineage=lineage,root=str(root),roots=roots,remotes=remotes,checkout=_checkout(root)))(root:=Path(root),roots:=sorted(_git_maybe(root,"rev-list","--max-parents=0","HEAD").decode().split()),remotes:=_git_remotes(root),provenance_digest({"git_roots":roots}) if roots else None)
def repository_state(db): return {"roots":dict(db.execute("SELECT root,repository FROM provenance.repository_checkouts").fetchall()),"checkouts":dict(db.execute("SELECT id,repository FROM provenance.repository_checkouts").fetchall()),"checkout_roots":dict(db.execute("SELECT id,root FROM provenance.repository_checkouts").fetchall()),"lineages":dict(db.execute("SELECT id,lineage FROM provenance.repositories").fetchall()),"aliases":dict(db.execute("SELECT evidence,CASE WHEN COUNT(DISTINCT repository)=1 THEN MIN(repository) END FROM provenance.repository_aliases GROUP BY evidence").fetchall())}
def _refresh_repository(): (_git_root.cache_clear(),_repository.cache_clear())
def repository(path,known=None,refresh=True): return (lambda value,state,evidence,bound,resolved:{**value,"id":resolved or value["id"],"alias":None if resolved or not evidence else evidence})(value:={**_repository(str(root)),"head":_git_maybe(root,"rev-parse","--verify","HEAD").decode().strip(),"branch":_git_maybe(root,"symbolic-ref","--short","HEAD").decode().strip()},state:=repository_state(known) if known is not None and hasattr(known,"execute") else known or {"roots":{},"checkouts":{},"checkout_roots":{},"lineages":{},"aliases":{}},evidence:=repository_evidence(value),bound:=state["checkouts"].get(value["checkout"]),bound if value["lineage"] and state["lineages"].get(bound)==value["lineage"] else evidence and state["aliases"].get(evidence)) if (not refresh or _refresh_repository() is None) and (root:=_git_root(Path(path))) else None
def _cached_repository(cache,root,known): return cache[key] if (key:=str(root)) in cache else cache.setdefault(key,repository(root,known,False))
def _observe_checkout(db,repo):
    db.execute("DELETE FROM provenance.repository_checkouts WHERE root=? AND id<>?",(repo["root"],repo["checkout"]))
    db.execute("INSERT INTO provenance.repository_checkouts VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET repository=excluded.repository,root=excluded.root,branch=excluded.branch,head=excluded.head",(repo["checkout"],repo["id"],repo["root"],repo["branch"],repo["head"])) and repo["alias"] and db.execute("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",(repo["id"],repo["alias"]))
def capture_repository(path,db_path=None):
    connect=lambda read_only=False:open_db(db_path,read_only) if db_path else get_db(read_only)
    db=connect(); init_schema(db); known=repository_state(db); db.close(); repo=repository(path,known)
    if not repo: return None
    record=_provenance_record("repository.observed",repo["id"],{k:repo[k] for k in ("id","lineage","roots","remotes","head")},datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    db=connect()
    with contextlib.closing(db),_transaction(db):
        _observe_checkout(db,repo)
        project_provenance(db,record)
        db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",(record["kind"],record["entity"]))
    return repo
def _resolved(path,cwd=None): return str((Path(cwd)/p if not (p:=Path(path)).is_absolute() and cwd else p).expanduser().resolve())
def pending_scopes(conversations):
    captured=datetime.now(timezone.utc)
    return [(conversation,resolved,None,root,f"pending:{checkout}" if checkout else None,captured) for conversation,cwd in conversations for resolved in [_resolved(cwd) if cwd else None] for root,checkout in [_git_marker(resolved) if resolved else (None,None)]]
def snapshot_scopes(conversations,known=None):
    _refresh_repository()
    captured=datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    out=[]
    for conversation,cwd in conversations:
        resolved=str(Path(cwd).expanduser().resolve()) if cwd else None
        repo=repository(resolved,known,False) if resolved else None
        out.append((conversation,resolved,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,captured))
    return out
def _provenance_where(path,cwd,cache,known=None,frozen=False):
    p=Path(path) if frozen else Path(_resolved(path,cwd)); repo=_cached_repository(cache,root,known) if (root:=_git_root(p)) else None
    return (repo,p.relative_to(repo["root"]).as_posix(),"repository") if repo else (None,f"external/{provenance_digest(str(p))[:24]}/{p.name}","external")
def pending_edit_scopes(edits):
    captured=datetime.now(timezone.utc)
    return [(e["id"],f"external/{provenance_digest(route)[:24]}/{Path(route).name}",None,root,f"pending:{checkout}" if checkout else None,route,captured) for e in edits for route in [_resolved(e["path"],e["cwd"])] for root,checkout in [_git_marker(route)]]
def snapshot_edit_scopes(edits,known=None):
    _refresh_repository()
    captured=datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    cache={}
    return [(e["id"],path,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,route,captured) for e in edits for route in [e.get("route") or _resolved(e["path"],e["cwd"])] for repo,path,_ in [_provenance_where(route,None,cache,known,True)]]
def edit_scope_inputs(result):
    conversations={c["id"]:c["cwd"] for c in result.convs}
    messages={m["id"]:m["conversation_id"] for m in result.msgs}
    return [{"id":e["id"],"path":e["file_path"],"cwd":conversations.get(messages.get(e["message_id"]))} for e in result.edits]
def _checkpoint(repo,source): return (lambda paths,state:dict(id=provenance_digest({"repository":repo["id"],"head":repo["head"],"state":state}),repository=repo["id"],head=repo["head"],state_hash=state,paths=paths,capture_source=source))(sorted({x[3:].split(" -> ")[-1] for x in _git_run(repo["root"],"status","--porcelain=v1","-z").decode(errors="replace").split("\0") if len(x)>3}),provenance_digest((_git_maybe(repo["root"],"diff","--binary","HEAD")+_git_maybe(repo["root"],"diff","--binary","--cached","HEAD")) if repo["head"] else _git_run(repo["root"],"status","--porcelain=v1","-z")))
def _provenance_record(kind,entity,payload,observed_at): return dict(kind=kind,entity=entity,payload=payload,observed_at=observed_at)
def _provenance_edits(core,edit_ids=None):
    ids=sorted(set(edit_ids or ())) if edit_ids is not None else None
    selected=" AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_edit_id=fe.id)"+(f" AND fe.id IN ({','.join('?'*len(ids))})" if ids else " AND FALSE" if ids==[] else "")
    sql="""SELECT fe.id,fe.file_path,fe.edit_type,fe.content,fe.old_content,CAST(fe.created_at AS VARCHAR),m.id,m.conversation_id,c.cwd,s.path,s.repository,s.root,s.checkout,s.route,CAST(s.observed_at AS VARCHAR) FROM file_edits fe JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id LEFT JOIN provenance.file_edit_scopes s ON s.file_edit_id=fe.id WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id)"""+selected+" ORDER BY fe.created_at,fe.id"
    return [dict(zip(("id","path","type","content","old","ts","turn","conversation","cwd","scope_path","repository","root","checkout","route","scope_at"),r)) for r in core.execute(sql,ids or ()).fetchall()]
def _observe_provenance(edits,source="sync",known=None,conversations=(),cache=None):
    cache={} if cache is None and _refresh_repository() is None else cache
    captured,records,repos,versions,fulls,scopes=datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),[],{},{},{},[]
    for e in edits:
        rid,path=e["repository"],e["scope_path"]
        repo=_cached_repository(cache,e["root"],known) if e["root"] else None
        repo=repo if repo and (repo["id"],repo["checkout"])==(rid,e["checkout"]) else None
        kind="repository" if rid else "external"
        fid=provenance_digest({"repository":rid,"path":path})
        if repo and rid not in repos: repos[rid]=repo; records.append(_provenance_record("repository.observed",rid,{k:repo[k] for k in ("id","lineage","roots","remotes","head")},captured))
        target=Path(repo["root"],path) if repo else None; key=str(target) if target else None
        if key not in fulls: fulls[key]=provenance_digest(target.read_bytes()) if target and target.is_file() else None
        full=fulls[key]
        records += [_provenance_record("file.observed",fid,{"id":fid,"repository":rid,"path":path,"kind":kind},captured),_provenance_record("edit.observed",e["id"],{"id":e["id"],"turn":e["turn"],"file":fid,"repository":rid,"old_content_hash":provenance_digest((e["old"] or "").encode()) if e["old"] is not None else None,"new_content_hash":provenance_digest((e["content"] or "").encode()),"evidence":"captured_exact" if e["type"]=="write" or e["old"] is not None else "content_unknown"},captured)]
        if full: versions[(rid,fid)]=(e["id"],provenance_digest((e["content"] or "").encode()),full,path)
    for rid,repo in repos.items():
        cp=_checkpoint(repo,source); records.append(_provenance_record("git.checkpoint",cp["id"],cp,captured))
        for (r,fid),(edit,after,full,path) in versions.items():
            if r==rid: vid=provenance_digest({"file":fid,"content":full}); records.append(_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":full},captured))
            if r==rid and path not in cp["paths"] and after==full: records.append(_provenance_record("checkpoint.link",provenance_digest({"checkpoint":cp["id"],"edit":edit}),{"checkpoint":cp["id"],"edit":edit,"evidence":"full_content_match"},captured))
    for conversation,cwd,rid,root,checkout,observed in conversations:
        repo=_cached_repository(cache,root,known) if root else None
        if repo and (repo["id"],repo["checkout"])==(rid,checkout) and rid not in repos:
            repos[rid]=repo
            records.append(_provenance_record("repository.observed",rid,{k:repo[k] for k in ("id","lineage","roots","remotes","head")},observed))
    return records,repos,scopes
def observe_provenance(core): return _observe_provenance(_provenance_edits(core),known=repository_state(core))[0]
def provenance_records(db,only=None):
    out=[]; ids=lambda kind:[entity for k,entity in only or () if k==kind]; rows=lambda kind,sql,column="id":[] if only is not None and not ids(kind) else db.execute(sql+(f" WHERE {column} IN (SELECT UNNEST(?))" if only is not None else ""),[ids(kind)] if only is not None else []).fetchall()
    for r in rows("repository.observed","SELECT id,lineage,CAST(roots AS VARCHAR),CAST(remotes AS VARCHAR),last_head,observed_at FROM provenance.repositories"): out.append(_provenance_record("repository.observed",r[0],dict(id=r[0],lineage=r[1],roots=json.loads(r[2]),remotes=json.loads(r[3]),head=r[4]),r[5]))
    for r in rows("file.observed","SELECT * FROM provenance.files"): out.append(_provenance_record("file.observed",r[0],dict(zip(("id","repository","path","kind"),r)),None))
    for r in rows("file.version","SELECT * FROM provenance.file_versions"): out.append(_provenance_record("file.version",r[0],dict(zip(("id","file","content_hash"),r[:3])),r[3]))
    for r in rows("edit.observed","SELECT x.file_edit_id,fe.message_id,x.file_id,f.repository,x.old_content_hash,x.new_content_hash,x.evidence FROM provenance.file_edit_files x JOIN file_edits fe ON fe.id=x.file_edit_id JOIN provenance.files f ON f.id=x.file_id","x.file_edit_id"): out.append(_provenance_record("edit.observed",r[0],dict(zip(("id","turn","file","repository","old_content_hash","new_content_hash","evidence"),r)),None))
    for r in rows("git.checkpoint","SELECT id,repository,head,state_hash,CAST(paths AS VARCHAR),observed_at,capture_source FROM provenance.git_checkpoints"): out.append(_provenance_record("git.checkpoint",r[0],dict(id=r[0],repository=r[1],head=r[2],state_hash=r[3],paths=json.loads(r[4]),capture_source=r[6]),r[5]))
    for r in db.execute("SELECT * FROM provenance.checkpoint_edits").fetchall():
        if only is None or provenance_digest({"checkpoint":r[0],"edit":r[1]}) in ids("checkpoint.link"): out.append(_provenance_record("checkpoint.link",provenance_digest({"checkpoint":r[0],"edit":r[1]}),dict(zip(("checkpoint","edit","evidence"),r)),None))
    return out
def project_provenance(db,value,map_id=lambda table,value:value,touch=True):
    p,k=value["payload"],value["kind"]; observed=value["observed_at"]
    if k not in PROVENANCE_KINDS: return False
    if k!="checkpoint.link" and p["id"]!=value["entity"]: raise ValueError("provenance entity mismatch")
    if k=="repository.observed":
        old=db.execute("SELECT lineage FROM provenance.repositories WHERE id=?",(p["id"],)).fetchone()
        if old and old[0] and p["lineage"] and old[0]!=p["lineage"]: raise ValueError("repository lineage conflict")
        db.execute("INSERT INTO provenance.repositories VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lineage=excluded.lineage,roots=excluded.roots,remotes=excluded.remotes,last_head=COALESCE(excluded.last_head,repositories.last_head),observed_at=COALESCE(excluded.observed_at,repositories.observed_at)",(p["id"],p["lineage"],json.dumps(p["roots"]),json.dumps(p["remotes"]),p.get("head"),observed))
    elif k=="file.observed":
        if p["id"]!=provenance_digest({"repository":p["repository"],"path":p["path"]}): raise ValueError("provenance file identity mismatch")
        db.execute("INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",(p["id"],p["repository"],p["path"],p["kind"]))
    elif k=="file.version":
        if p["id"]!=provenance_digest({"file":p["file"],"content":p["content_hash"]}): raise ValueError("provenance version identity mismatch")
        db.execute("INSERT OR IGNORE INTO provenance.file_versions VALUES (?,?,?,?)",(p["id"],p["file"],p["content_hash"],observed))
    elif k=="edit.observed":
        edit,turn=map_id("file_edits",p["id"]),map_id("messages",p["turn"]); row=db.execute("SELECT message_id FROM file_edits WHERE id=?",(edit,)).fetchone()
        if not row or row[0]!=turn: raise ValueError("provenance edit/turn mismatch")
        old=db.execute("SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id=?",(edit,)).fetchone()
        if old and old[0]!=p["file"]: raise ValueError("provenance edit scope conflict")
        db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,?,?,?) ON CONFLICT(file_edit_id) DO UPDATE SET old_content_hash=excluded.old_content_hash,new_content_hash=excluded.new_content_hash,evidence=excluded.evidence",(edit,p["file"],p["old_content_hash"],p["new_content_hash"],p["evidence"]))
    elif k=="git.checkpoint":
        if p["id"]!=provenance_digest({"repository":p["repository"],"head":p["head"],"state":p["state_hash"]}): raise ValueError("provenance checkpoint identity mismatch")
        db.execute("INSERT OR IGNORE INTO provenance.git_checkpoints VALUES (?,?,?,?,?,?,?)",(p["id"],p["repository"],p["head"],p["state_hash"],json.dumps(p["paths"]),observed,p["capture_source"]))
    elif k=="checkpoint.link":
        if value["entity"]!=provenance_digest({"checkpoint":p["checkpoint"],"edit":p["edit"]}): raise ValueError("provenance checkpoint link mismatch")
        db.execute("INSERT OR IGNORE INTO provenance.checkpoint_edits VALUES (?,?,?)",(p["checkpoint"],map_id("file_edits",p["edit"]),p["evidence"]))
    touch and _archive_touch(db,[(k,value["entity"])])
    return True
def capture_provenance(path=None,edit_ids=None,conversation_ids=None,source="sync"):
    connect,targeted,eids,cids,core=(lambda read_only=False:open_db(path,read_only) if path else get_db(read_only),edit_ids is not None or conversation_ids is not None,sorted(set(edit_ids or ())),sorted(set(conversation_ids or ())),open_db(path,True) if path else get_db(True))
    try:
        if eids: cids=sorted(set(cids)|{r[0] for r in core.execute("SELECT DISTINCT m.conversation_id FROM file_edits fe JOIN messages m ON m.id=fe.message_id WHERE fe.id IN (SELECT UNNEST(?))",[eids]).fetchall()})
        missing=core.execute("SELECT c.id,c.cwd FROM conversations c WHERE c.cwd IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) AND NOT EXISTS (SELECT 1 FROM provenance.conversation_scopes s WHERE s.conversation=c.id)"+(" AND c.id IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall()
        missing_edits=[dict(id=r[0],path=r[1],cwd=r[2]) for r in core.execute("SELECT fe.id,fe.file_path,c.cwd FROM file_edits fe JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_scopes s WHERE s.file_edit_id=fe.id)"+(" AND fe.id IN (SELECT UNNEST(?))" if targeted else ""),[eids] if targeted else []).fetchall()]
    finally: core.close()
    scopes,edit_scopes=pending_scopes(missing),pending_edit_scopes(missing_edits)
    if scopes or edit_scopes:
        core=connect()
        with contextlib.closing(core),_transaction(core): (scopes and core.executemany("INSERT OR IGNORE INTO provenance.conversation_scopes VALUES (?,?,?,?,?,?)",scopes),edit_scopes and core.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",edit_scopes))
    core=connect(True)
    try:
        edits,known=_provenance_edits(core,edit_ids),repository_state(core)
        conversations=core.execute("SELECT s.conversation,s.cwd,s.repository,s.root,s.checkout,CAST(s.observed_at AS VARCHAR) FROM provenance.conversation_scopes s WHERE (s.checkout LIKE 'pending:%' OR s.repository IS NOT NULL AND NOT EXISTS (SELECT 1 FROM provenance.repository_checkouts c WHERE (c.id,c.repository,c.root)=(s.checkout,s.repository,s.root)))"+(" AND s.conversation IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall()
        files=[] if targeted else core.execute("SELECT f.id,f.repository,f.path,c.root,c.id FROM provenance.files f JOIN provenance.repository_checkouts c ON c.repository=f.repository").fetchall()
    finally: core.close()
    _refresh_repository()
    cache,scopes,observed_conversations={},[],[]
    for conversation,cwd,rid,root,checkout,observed in conversations:
        if checkout and checkout.startswith("pending:"):
            marker=checkout.removeprefix("pending:"); repo=_cached_repository(cache,root,known) if (root,marker)==_git_marker(cwd) else None; scopes.append((conversation,cwd,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,observed)); rid,root,checkout=(repo["id"],repo["root"],repo["checkout"]) if repo else (None,None,None)
        observed_conversations.append((conversation,cwd,rid,root,checkout,observed))
    conversations,edit_scopes=observed_conversations,[]
    for e in edits:
        if e["checkout"] and e["checkout"].startswith("pending:"):
            marker=e["checkout"].removeprefix("pending:"); repo,relative,_=_provenance_where(e["route"],None,cache,known,True) if (e["root"],marker)==_git_marker(e["route"]) else (None,e["scope_path"],"external"); edit_scopes.append((e["id"],relative,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,e["route"],e["scope_at"]))
    frozen={r[0]:r for r in edit_scopes}
    for e in edits:
        if e["id"] in frozen: _,e["scope_path"],e["repository"],e["root"],e["checkout"],e["route"],e["scope_at"]=frozen[e["id"]]
    records,repos,_,captured=(*_observe_provenance(edits,source,known,conversations,cache),datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    for root,rid in (() if targeted else known["roots"].items()):
        if (repo:=_cached_repository(cache,root,known)) and repo["id"]==rid: repos.setdefault(rid,repo)
    for rid,repo in repos.items():
        if not any(r["kind"]=="git.checkpoint" and r["payload"]["repository"]==rid for r in records): cp=_checkpoint(repo,source); records.append(_provenance_record("git.checkpoint",cp["id"],cp,captured))
    for fid,rid,relative,root,checkout in files:
        target,repo=Path(root,relative),_cached_repository(cache,root,known)
        if repo and (repo["id"],repo["checkout"])==(rid,checkout) and target.is_file(): content,vid=(content:=provenance_digest(target.read_bytes())),provenance_digest({"file":fid,"content":content}); records.append(_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":content},captured))
    stale,touched,core=([] if targeted else [root for root,rid in known["roots"].items() if not (repo:=_cached_repository(cache,root,known)) or repo["id"]!=rid]),sorted({e["conversation"] for e in edits}),connect()
    with contextlib.closing(core),_transaction(core):
        stale and core.execute("DELETE FROM provenance.repository_checkouts WHERE root IN (SELECT UNNEST(?))",[stale])
        scopes and core.executemany("UPDATE provenance.conversation_scopes SET cwd=?,repository=?,root=?,checkout=?,observed_at=? WHERE conversation=? AND checkout LIKE 'pending:%'",[(cwd,rid,root,checkout,observed,conversation) for conversation,cwd,rid,root,checkout,observed in scopes])
        edit_scopes and core.executemany("UPDATE provenance.file_edit_scopes SET path=?,repository=?,root=?,checkout=?,route=?,observed_at=? WHERE file_edit_id=? AND checkout LIKE 'pending:%'",[(relative,rid,root,checkout,route,observed,edit) for edit,relative,rid,root,checkout,route,observed in edit_scopes])
        [_observe_checkout(core,r) for r in repos.values()]; [project_provenance(core,r) for r in records]
        records and core.executemany("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",[(r["kind"],r["entity"]) for r in records]); (scopes or touched) and _archive_touch(core,[("conversations",r[0]) for r in scopes]+[("conversations",c) for c in touched])
    return records
def project_archive_row(db,table,columns,values,origin=None,touch=True):
    if table not in ARCHIVE_COLUMNS or columns!=ARCHIVE_COLUMNS[table] or len(values)!=len(columns): raise ValueError("record schema/entity mismatch")
    required=("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at")
    if origin and set(origin) not in (set(required),set(required)|{"proof_id"}): raise ValueError("record origin schema mismatch")
    updates=_MSG_UPDATES if table=="messages" else ",".join(f"{c}=excluded.{c}" for c in columns[1:]); db.execute(_MSG_UPS,values) if table=="messages" else db.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?'*len(columns))}) ON CONFLICT(id) DO UPDATE SET {updates}",values)
    if origin: db.execute("INSERT OR REPLACE INTO remote.row_origins VALUES (?,?,?,?,?,?,?,?,?,?)",(table,values[0],*(origin[k] for k in required),origin.get("proof_id")))
    touch and _archive_touch(db,[(table,values[0])])
def _insert_pages(db,target,rows,columns=None,conflict="",mode="",embedding=False): schema={r[0]:r[1] for r in db.execute(f"DESCRIBE {target}").fetchall()}; columns=columns or list(schema); shape=json.dumps([{c:schema[c] for c in columns}]); norm=lambda c,v:json.loads(v) if schema[c]=="JSON" and isinstance(v,str) else v; extra=",embedding" if embedding else ""; source="SELECT x.*,m.embedding FROM UNNEST(from_json(?,?)) t(x) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content" if embedding else "SELECT x.* FROM UNNEST(from_json(?,?)) t(x)"; [db.execute(f"INSERT{mode} INTO {target} ({','.join(columns)}{extra}) {source}{conflict}",(json.dumps([{c:norm(c,v) for c,v in zip(columns,row)} for row in page],default=str,ensure_ascii=True,allow_nan=False),shape)) for page in (rows[i:i+500] for i in range(0,len(rows),500))]
def project_archive_rows(db,table,columns,rows): fields=("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at"); values=[r[0] for r in rows]; origins=[(table,v[0],*(o[k] for k in fields),o.get("proof_id")) for v,o in rows if o]; updates=_MSG_UPDATES if table=="messages" else ','.join(f"{c}=excluded.{c}" for c in columns[1:]); required(table in ARCHIVE_COLUMNS and columns==ARCHIVE_COLUMNS[table] and not any(len(v)!=len(columns) or o and set(o) not in (set(fields),set(fields)|{"proof_id"}) for v,o in rows),ValueError("record schema/entity mismatch")); _insert_pages(db,table,values,columns,f" ON CONFLICT(id) DO UPDATE SET {updates}",embedding=table=="messages"); origins and _insert_pages(db,"remote.row_origins",origins,mode=" OR REPLACE")
def project_row_proofs(db,proofs,root_public,certificate):
    if not proofs: return []
    fields=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature"); expected={"v","kind",*fields}; signer=(proofs[0]["author_user_id"],proofs[0]["author_device_id"]); packed=json.dumps(certificate,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False)
    columns=("workspace_id","authorization_workspace_id","row_kind","source_row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    if any(set(proof)!=expected or proof["v"]!=1 or proof["kind"]!="row.proof" or (proof["author_user_id"],proof["author_device_id"])!=signer for proof in proofs) or set(certificate)!={"v","user","device","issued_at","signature"} or certificate["v"]!=1 or set(certificate["device"])!={"id","name","sign_public","box_public"} or (certificate["user"],certificate["device"]["id"])!=signer: raise ValueError("row proof storage schema mismatch")
    if (old:=db.execute("SELECT root_public,CAST(certificate AS VARCHAR) FROM remote.row_signers WHERE author_user_id=? AND author_device_id=?",signer).fetchone()) and (old[0]!=root_public or json.loads(old[1])["device"]!=certificate["device"]): raise ValueError("row signer conflict")
    db.execute("INSERT OR IGNORE INTO remote.row_signers VALUES (?,?,?,?)",(*signer,root_public,packed)); ids=[provenance_digest(proof) for proof in proofs]; rows=[(pid,*(proof[k] for k in fields)) for pid,proof in zip(ids,proofs)]; _insert_pages(db,"remote.row_proofs",rows,("id",*columns),mode=" OR IGNORE"); return ids
def project_row_proof(db,proof,root_public,certificate): return project_row_proofs(db,[proof],root_public,certificate)[0]
def project_provider_alias(db,record): fields=("workspace_id","author_user_id","object_id","revision","source","session_id","members","canonical_source_row_id","proof"); required(set(record)==set(fields) and isinstance(p:=record["proof"],dict) and (p.get("workspace"),p.get("author_user_id"),p.get("object_id"),p.get("revision"))==tuple(record[k] for k in fields[:4]) and record["object_id"]=="provider-session:"+provenance_digest([record["source"],record["session_id"]]) and isinstance(record["members"],list) and record["members"] and all(isinstance(x,str) and x for x in record["members"]) and record["members"]==sorted(set(record["members"])) and record["canonical_source_row_id"]==record["members"][0],ValueError("provider alias storage schema mismatch")); _insert_pages(db,"remote.provider_session_aliases",[tuple(record[f] for f in fields)],fields,mode=" OR IGNORE")
def project_workspace_controls(db,controls):
    for value in controls:
        if not {"workspace","revision","epoch"}<=set(value) or not isinstance(value["revision"],int) or not isinstance(value["epoch"],int): raise ValueError("workspace control storage schema mismatch")
        raw=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False); key=(value["workspace"],value["revision"]); proof=provenance_digest(value); old=db.execute("SELECT state_hash FROM remote.workspace_controls WHERE workspace_id=? AND revision=?",key).fetchone()
        if old and old[0]!=proof: raise ValueError("workspace control conflict")
        db.execute("INSERT OR IGNORE INTO remote.workspace_controls VALUES (?,?,?,?,?)",(*key,value["epoch"],proof,raw))
    return len(controls)
def _logical_archive(row,proof,proof_id,native=False):
    table,source,columns=row["kind"],row["id"],ARCHIVE_COLUMNS[row["kind"]]; mapped=lambda kind,value:value if native else remote_id(proof["author_user_id"],kind,value); physical,origin=mapped(table,source),None if native else {"workspace_id":proof["workspace"],"author_user_id":proof["author_user_id"],"author_device_id":proof["author_device_id"],"source_row_id":source,"source_event_id":proof["revision"],"content_key":f"{table}:{source}","observed_at":None,"proof_id":proof_id}; data={"id":source,**row["data"]}; parents=dict({"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}.get(table,()))
    values=[physical if c=="id" else mapped(parents[c],data[c]) if c in parents else json.dumps(data[c],sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False) if c in ("metadata","input","output") and data.get(c) is not None else data.get(c) for c in columns]; return table,physical,values,origin,data
def project_logical_rows(db,items):
    grouped={}; out=[]; delayed=[]
    for row,proof,pid,native in items:
        if row["kind"] in PROVENANCE_KINDS or row["state"]=="deleted": delayed.append((row,proof,pid,native)); continue
        table,physical,values,origin,data=_logical_archive(row,proof,pid,native); grouped.setdefault(table,[]).append((values,origin)); out.append((table,physical)); table=="attachments" and data["body_hash"] and grouped.setdefault("attachment_bodies",[]).append((physical,data["body_hash"],data["size"]))
    [project_archive_rows(db,table,ARCHIVE_COLUMNS[table],rows) for table,rows in grouped.items() if table!="attachment_bodies"]; grouped.get("attachment_bodies") and _insert_pages(db,"attachment_bodies",grouped["attachment_bodies"],mode=" OR REPLACE"); out += [(item[0]["kind"],project_logical_row(db,*item,touch=False)) for item in delayed]; out and _archive_touch(db,out); return out
def project_logical_row(db,row,proof,proof_id,native=False,touch=True):
    table,source=row["kind"],row["id"]; mapped=lambda kind,value:value if native else remote_id(proof["author_user_id"],kind,value); physical=mapped(table,source); origin=None if native else {"workspace_id":proof["workspace"],"author_user_id":proof["author_user_id"],"author_device_id":proof["author_device_id"],"source_row_id":source,"source_event_id":proof["revision"],"content_key":f"{table}:{source}","observed_at":None,"proof_id":proof_id}
    if table in PROVENANCE_KINDS:
        data={"id":source,**row["data"]}; observed=data.pop("observed_at",None); value={"kind":table,"entity":source,"payload":data,"observed_at":observed}; project_provenance(db,value,mapped,False); physical=mapped("file_edits",source) if table=="edit.observed" else provenance_digest({"checkpoint":data["checkpoint"],"edit":mapped("file_edits",data["edit"])}) if table=="checkpoint.link" else source; touch and _archive_touch(db,[(table,physical)])
        if native: db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",(table,physical))
        else: db.execute("INSERT OR REPLACE INTO remote.provenance_origins VALUES (?,?,?,?,?,?)",(table,physical,proof["workspace"],proof["author_user_id"],source,proof_id))
        return physical
    if row["state"]=="deleted": db.execute(f"DELETE FROM {table} WHERE id=?",(physical,)); table=="attachments" and db.execute("DELETE FROM attachment_bodies WHERE attachment_id=?",(physical,)); origin and db.execute("INSERT OR REPLACE INTO remote.row_origins VALUES (?,?,?,?,?,?,?,?,?,?)",(table,physical,*(origin[k] for k in ("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at","proof_id")))); touch and _archive_touch(db,[(table,physical)]); return physical
    table,physical,values,origin,data=_logical_archive(row,proof,proof_id,native)
    project_archive_row(db,table,ARCHIVE_COLUMNS[table],values,origin,touch); table=="attachments" and data["body_hash"] and db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(physical,data["body_hash"],data["size"])); return physical
def set_attachment_path(db,row_id,path): db.execute("UPDATE attachments SET path=? WHERE id=?",(str(path),row_id))
def index_attachment_body(db,row_id,path,size=None):
    path=Path(path); actual=path.stat().st_size if path.is_file() and not path.is_symlink() else -1
    if actual<0 or actual>ATTACHMENT_LIMIT or size is not None and actual!=size: return None
    with path.open("rb") as source: body_hash=hashlib.file_digest(source,"sha256").hexdigest(); old=db.execute("SELECT content_hash,size FROM attachment_bodies WHERE attachment_id=?",(row_id,)).fetchone()
    size is None and db.execute("UPDATE attachments SET size=? WHERE id=? AND size IS NULL",(actual,row_id)); old==(body_hash,actual) or (db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(row_id,body_hash,actual)),_archive_touch(db,[("attachments",row_id)])); return body_hash
def project_attachment_body(db_path,data,body_hash):
    required(provenance_digest(data)==body_hash,ValueError("attachment body hash mismatch")); path=attachment_body(data,Path(db_path).parent); db=open_db(db_path); rows=db.execute("SELECT b.attachment_id,a.path FROM attachment_bodies b JOIN attachments a ON a.id=b.attachment_id WHERE b.content_hash=?",(body_hash,)).fetchall(); begun=False
    try:
        if rows and path and (ids:=[row_id for row_id,old in rows if old!=str(path)]): db.execute("BEGIN"); begun=True; [set_attachment_path(db,row_id,path) for row_id in ids]; db.execute("COMMIT"); begun=False
        return len(rows)
    except BaseException: begun and db.execute("ROLLBACK"); raise
    finally: db.close()
def _backup_copy(source,target):
    command=("cp","-c",str(source),str(target)) if sys.platform=="darwin" else ("cp","--reflink=auto",str(source),str(target)) if sys.platform.startswith("linux") else None
    if not command or subprocess.run(command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode: shutil.copyfile(source,target)
def _file_sha256(path):
    with Path(path).open("rb") as source: return hashlib.file_digest(source,"sha256").hexdigest()
def _migration_backup(conn,version=1):
    tables={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}; current=(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    if "conversations" not in tables or current>=version: return None
    path=Path(next((r[2] for r in conn.execute("PRAGMA database_list").fetchall() if r[2]),"")); backup=path.with_name(f"{path.name}.pre-v{version}.bak") if path.name else None
    if not backup: return None
    conn.execute("CHECKPOINT")
    if backup.exists():
        if backup.is_symlink() or not backup.is_file(): raise ValueError("core migration backup path is unsafe")
        check=duckdb.connect(str(backup),read_only=True); check.execute("SELECT COUNT(*) FROM conversations").fetchone(); check.close(); source=_file_sha256(path)
        if source==_file_sha256(backup): return backup
        backup=backup.with_name(f"{backup.name}.{source[:12]}")
    fd,tmp=tempfile.mkstemp(prefix=f".{backup.name}.",dir=backup.parent); os.close(fd)
    try: _backup_copy(path,tmp); os.chmod(tmp,0o600); check=duckdb.connect(tmp,read_only=True); check.execute("SELECT COUNT(*) FROM conversations").fetchone(); check.close(); durable_replace(tmp,backup); return backup
    except BaseException: Path(tmp).unlink(missing_ok=True); raise
def _repository_alias_migration(conn):
    _refresh_repository()
    ambiguous=set()
    for rid, in conn.execute("SELECT DISTINCT repository FROM provenance.repository_checkouts").fetchall():
        evidence={repository_evidence(value) for root, in conn.execute("SELECT root FROM provenance.repository_checkouts WHERE repository=?",(rid,)).fetchall() if (git_root:=_git_root(root)) and (value:=_repository(str(git_root)))["lineage"]}
        if len(evidence)>1: ambiguous.add(rid)
    aliases=[(rid,repository_evidence({"lineage":lineage,"remotes":json.loads(remotes)})) for rid,lineage,remotes in conn.execute("SELECT id,lineage,CAST(remotes AS VARCHAR) FROM provenance.repositories WHERE lineage IS NOT NULL").fetchall() if rid not in ambiguous]
    return aliases,ambiguous
def session_bindings(conn): return {(source,session):cid for source,session,cid in conn.execute("SELECT source,session_id,conversation_id FROM provider_sessions").fetchall()}
def init_schema(conn):
    tables={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
    current=(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    scope=remote_id_migration_scope(conn,remote_id) if current<2 else set()
    _migration_backup(conn)
    current==1 and scope and ("core_migrations" not in tables or not conn.execute("SELECT 1 FROM core_migrations WHERE name='remote_ids'").fetchone()) and _migration_backup(conn,2)
    current==2 and _migration_backup(conn,3)
    current in (3,4) and _migration_backup(conn,current+1)
    conn.execute("""CREATE TABLE IF NOT EXISTS conversations (
        id VARCHAR PRIMARY KEY, source VARCHAR NOT NULL, title VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP,
        model VARCHAR, cwd VARCHAR, git_branch VARCHAR, project_id VARCHAR, metadata JSON)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, role VARCHAR NOT NULL, content VARCHAR,
        thinking VARCHAR, created_at TIMESTAMP, model VARCHAR, metadata JSON, embedding FLOAT[768])""")
    if not conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone():
        conn.execute("ALTER TABLE messages ADD COLUMN embedding FLOAT[768]")
    conn.execute("""CREATE TABLE IF NOT EXISTS tool_calls (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, tool_name VARCHAR, input JSON, output JSON,
        status VARCHAR, duration_ms INTEGER, created_at TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS attachments (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, filename VARCHAR, mime_type VARCHAR,
        size INTEGER, path VARCHAR, url VARCHAR, created_at TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS artifacts (
        id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, artifact_type VARCHAR, title VARCHAR,
        content TEXT, language VARCHAR, created_at TIMESTAMP, version INTEGER)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS file_edits (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, file_path VARCHAR, edit_type VARCHAR,
        content TEXT, created_at TIMESTAMP)""")
    conn.execute("ALTER TABLE file_edits ADD COLUMN IF NOT EXISTS old_content TEXT")
    conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS parent_id VARCHAR")  # thread tree; ALTER (not CREATE) keeps column order identical for fresh and migrated dbs
    local_facts=bool(conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='provenance' AND table_name='local_facts'").fetchone())
    conn.execute(_PROVENANCE_SCHEMA)
    with _transaction(conn):
        cols={r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='provenance' AND table_name='file_edit_files'").fetchall()}; [(conn.execute(f"ALTER TABLE provenance.file_edit_files RENAME COLUMN {old} TO {new}"),cols.add(new)) for old,new in (("before_hash","old_content_hash"),("after_hash","new_content_hash")) if old in cols and new not in cols]
        conn.execute("ALTER TABLE provenance.conversation_scopes ADD COLUMN IF NOT EXISTS checkout VARCHAR")
        conn.execute("ALTER TABLE provenance.file_edit_scopes ADD COLUMN IF NOT EXISTS route VARCHAR")
        conn.execute("ALTER TABLE provenance.git_checkpoints ADD COLUMN IF NOT EXISTS capture_source VARCHAR; ALTER TABLE remote.row_origins ADD COLUMN IF NOT EXISTS proof_id VARCHAR; ALTER TABLE remote.row_proofs ADD COLUMN IF NOT EXISTS authorization_workspace_id VARCHAR; UPDATE remote.row_proofs SET authorization_workspace_id=workspace_id WHERE authorization_workspace_id IS NULL; DROP TABLE IF EXISTS provenance.assertions; DROP TABLE IF EXISTS provenance.capture_gaps; INSERT INTO core_schema SELECT TRUE,1 WHERE NOT EXISTS (SELECT 1 FROM core_schema WHERE singleton)")
        [index_attachment_body(conn,*r) for r in conn.execute("SELECT id,path,size FROM attachments WHERE path IS NOT NULL AND id NOT IN (SELECT attachment_id FROM attachment_bodies)").fetchall()]; facts=[(r["kind"],r["entity"]) for r in provenance_records(conn)] if not local_facts else []; facts and conn.executemany("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",facts)
    conn.execute("INSERT INTO archive_state SELECT TRUE,uuid(),0 WHERE NOT EXISTS (SELECT 1 FROM archive_state)")
    migration=(conn.execute("SELECT state FROM core_migrations WHERE name='remote_ids'").fetchone() or [None])[0]
    with migration_memory(conn) if current<2 else contextlib.nullcontext():
        if current<2 and (scope or migration and (migration.startswith("data") or migration=="changes")):
            with _transaction(conn):
                result=migrate_remote_data(conn,ARCHIVE_COLUMNS,migration=="data_direct") if migration and migration.startswith("data") else migrate_remote_changes(conn) if migration=="changes" else migrate_remote_ids(conn,ARCHIVE_COLUMNS)
                conn.execute(f"INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','{'fts' if (rebuild:=fts_needs_rebuild(conn) if migration else result[1]) else 'done'}'); INSERT OR REPLACE INTO core_schema SELECT TRUE,2 WHERE {not rebuild}; DELETE FROM core_migrations WHERE name='remote_ids' AND {not rebuild}")
            pending,current=rebuild,current if rebuild else 2
        else: pending=bool(conn.execute("SELECT 1 FROM core_migrations WHERE name='remote_ids' AND state='fts'").fetchone())
        if current<2 and not scope and not pending and fts_needs_rebuild(conn): pending=bool(conn.execute("INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','fts')"))
        conn.execute("INSTALL fts; LOAD fts")
        if pending:
            rebuild_fts_index(conn)
            with _transaction(conn): conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,2); DELETE FROM core_migrations WHERE name='remote_ids'")
        elif current<2 and not scope: conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,2)")
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<3:
        aliases,ambiguous=_repository_alias_migration(conn)
        unknown=[(edit,provenance_digest({"repository":None,"path":(path:=f"external/{provenance_digest(edit)[:24]}/unknown")}),None,None,"legacy_scope_conflict",None,path,"external") for edit, in conn.execute("SELECT file_edit_id FROM provenance.file_edit_files GROUP BY file_edit_id HAVING COUNT(*)>1").fetchall()]
        legacy_edits=[(r[0],f"external/{provenance_digest(r[0])[:24]}/unknown",None,None,None,None,None) for r in conn.execute("SELECT id FROM file_edits fe WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id)").fetchall()]
        with _transaction(conn):
            conn.execute("CREATE TABLE provenance.file_edit_files_v3(file_edit_id VARCHAR PRIMARY KEY,file_id VARCHAR,old_content_hash VARCHAR,new_content_hash VARCHAR,evidence VARCHAR); INSERT INTO provenance.file_edit_files_v3 SELECT * FROM provenance.file_edit_files QUALIFY count(*) OVER (PARTITION BY file_edit_id)=1")
            unknown and conn.executemany("INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",[(fid,repo,path,kind) for edit,fid,old,new,evidence,repo,path,kind in unknown])
            unknown and conn.executemany("INSERT INTO provenance.file_edit_files_v3 VALUES (?,?,?,?,?)",[(edit,fid,old,new,evidence) for edit,fid,old,new,evidence,repo,path,kind in unknown])
            conn.execute("DROP TABLE provenance.file_edit_files; ALTER TABLE provenance.file_edit_files_v3 RENAME TO file_edit_files")
            if ambiguous: (conn.execute("DELETE FROM provenance.repository_checkouts WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)]),conn.execute("DELETE FROM provenance.repository_aliases WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)]))
            aliases and conn.executemany("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",aliases)
            legacy_edits and conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",legacy_edits)
            conn.execute("INSERT OR IGNORE INTO provenance.conversation_scopes SELECT c.id,NULL,NULL,NULL,NULL,NULL FROM conversations c WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); INSERT OR REPLACE INTO core_schema VALUES (TRUE,3)")
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<4:
        metadata,recoveries=(metadata:=conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='conversations' AND column_name='metadata'").fetchone()),(conn.execute("SELECT 'conversations',c.id FROM conversations c WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) UNION ALL SELECT 'messages',m.id FROM messages m WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)").fetchall() if metadata else [])
        required(not (duplicates:=conn.execute("SELECT source,json_extract_string(metadata,'$.session_id') sid,count(*) FROM conversations c WHERE json_extract_string(metadata,'$.session_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) GROUP BY source,sid HAVING count(*)>1").fetchall() if metadata else []),ValueError(f"provider session identity conflicts require repair: {duplicates[:3]}"))
        with _transaction(conn):
            conn.execute("CREATE TABLE IF NOT EXISTS provider_sessions(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); DELETE FROM provider_sessions"+("; UPDATE conversations c SET metadata=json_merge_patch(c.metadata,'{\"capture_mode\":\"history\"}') WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); UPDATE messages m SET metadata=json_merge_patch(m.metadata,CASE json_extract_string(m.metadata,'$.recovered') WHEN 'history.jsonl' THEN '{\"capture_mode\":\"history\"}' ELSE '{\"capture_mode\":\"recovery\"}' END) WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)" if metadata else ""))
            conn.execute((("INSERT INTO provider_sessions SELECT source,json_extract_string(metadata,'$.session_id'),id FROM conversations c WHERE json_extract_string(metadata,'$.session_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id);" if metadata else "")+"INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)"))
            recoveries and _archive_touch(conn,recoveries)
    with _transaction(conn) if (binding_v5:=conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]==4) else contextlib.nullcontext(): binding_v5 and conn.execute("CREATE TABLE provider_sessions_v5(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); INSERT INTO provider_sessions_v5 SELECT * FROM provider_sessions; DROP TABLE provider_sessions; ALTER TABLE provider_sessions_v5 RENAME TO provider_sessions; INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)")
    with _transaction(conn):
        conn.execute("""CREATE OR REPLACE TEMP TABLE core_legacy_conflicts AS SELECT x.file_edit_id edit,x.file_id old_id,f.path,sha256(json_object('path',f.path,'repository',NULL)) new_id,EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='edit.observed' AND l.entity=x.file_edit_id) is_local FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id WHERE x.evidence='legacy_scope_conflict' AND (x.file_id<>sha256(json_object('path',f.path,'repository',NULL)) OR EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='edit.observed' AND l.entity=x.file_edit_id) AND NOT EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='file.observed' AND l.entity=sha256(json_object('path',f.path,'repository',NULL)))); INSERT OR IGNORE INTO provenance.files SELECT new_id,NULL,path,'external' FROM core_legacy_conflicts; UPDATE provenance.file_edit_files x SET file_id=c.new_id FROM core_legacy_conflicts c WHERE x.file_edit_id=c.edit; INSERT OR IGNORE INTO provenance.local_facts SELECT 'file.observed',new_id FROM core_legacy_conflicts WHERE is_local; DELETE FROM provenance.local_facts l USING core_legacy_conflicts c WHERE l.kind='file.observed' AND l.entity=c.old_id AND c.old_id<>c.new_id; DELETE FROM provenance.files f USING core_legacy_conflicts c WHERE f.id=c.old_id AND c.old_id<>c.new_id AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_versions v WHERE v.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.kind='file.observed' AND o.physical_entity=f.id); UPDATE archive_state SET generation=generation+1 WHERE singleton AND EXISTS(SELECT 1 FROM core_legacy_conflicts WHERE is_local); INSERT OR REPLACE INTO archive_changes SELECT kind,entity,generation FROM archive_state,(SELECT 'file_edits' kind,edit entity FROM core_legacy_conflicts WHERE is_local UNION ALL SELECT 'edit.observed',edit FROM core_legacy_conflicts WHERE is_local UNION ALL SELECT 'file.observed',new_id FROM core_legacy_conflicts WHERE is_local) WHERE singleton; DROP TABLE core_legacy_conflicts""")

def counts_by_source(conn):
    q = [("conversations", "source", 0), ("messages m JOIN conversations c ON c.id = m.conversation_id", "c.source", 1),
         ("tool_calls tc JOIN messages m ON tc.message_id = m.id JOIN conversations c ON c.id = m.conversation_id", "c.source", 2),
         ("attachments a JOIN messages m ON a.message_id = m.id JOIN conversations c ON c.id = m.conversation_id", "c.source", 3),
         ("file_edits fe JOIN messages m ON fe.message_id = m.id JOIN conversations c ON c.id = m.conversation_id", "c.source", 4)]
    out = {}; [out.setdefault(src, [0]*5).__setitem__(i, n) for tbl, col, i in q for src, n in conn.execute(f"SELECT {col}, COUNT(*) FROM {tbl} GROUP BY {col}").fetchall()]; return out

def load_fts(conn, allow_install: bool = False):
    try: allow_install and conn.execute("INSTALL fts"); conn.execute("LOAD fts")
    except Exception as e: raise ValueError("FTS extension not available. Run `convos init` once with network access.") from e

def ensure_fts_index(conn):
    if not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'fts_main_messages'").fetchone(): conn.execute("PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1)")

def rebuild_fts_index(conn): conn.execute("PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1)")

def ensure_db_ready(conn):
    if conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'messages'").fetchone(): return True
    typer.echo("Database not initialized. Run `convos init` or `convos sync`."); return False

def gen_id(source: str, oid: str) -> str: return hashlib.sha256(f"{source}:{oid}".encode()).hexdigest()[:16]
def ts_from_epoch(t):
    try: return datetime.fromtimestamp(float(t)) if t is not None and t!="" else None
    except Exception: return None
def ts_from_iso(t): return datetime.fromisoformat(t.replace("Z", "+00:00")) if t else None
def ts_any(t): return ts_from_epoch(t) or (ts_from_iso(t) if isinstance(t, str) else None)  # chatgpt list api sends iso, exports send epoch

def extract_content(content) -> dict:
    if isinstance(content, str): return {"text": content, "thinking": None, "tools": [], "attachments": []}
    if not isinstance(content, list): return {"text": "", "thinking": None, "tools": [], "attachments": []}
    blocks = [b for b in content if isinstance(b, dict)]
    return {
        "text": "\n".join(b.get("text", "") or b.get("thinking", "") if b.get("type") in ("text", None) else "" for b in blocks).strip() or
                "\n".join(str(b) for b in content if isinstance(b, str)).strip(),
        "thinking": "\n".join(b["thinking"] for b in blocks if b.get("type") == "thinking" and b.get("thinking")).strip() or None,
        "tools": [{"name": b["name"], "input": b.get("input", {}), "id": b.get("id")} for b in blocks if b.get("type") == "tool_use"] +
                 [{"id": b.get("tool_use_id"), "output": b.get("content", ""), "error":b.get("is_error",False)} for b in blocks if b.get("type") == "tool_result"],
        "attachments": [{"filename": b.get("name", b.get("file_name")), "mime_type": b.get("content_type", b.get("file_type")),
                        "size": b.get("size", b.get("file_size")), "url": b.get("asset_pointer", b.get("url"))}
                       for b in blocks if b.get("type") in ("image_asset_pointer", "file") or b.get("content_type") in ("image_asset_pointer", "file")]
    }

def read_safari_cookies(domain: str) -> dict[str, str]:
    path = Path.home() / "Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies"
    if not path.exists(): path = Path.home() / "Library/Cookies/Cookies.binarycookies"
    if not path.exists(): return {}
    cookies = {}
    target = domain.lstrip(".").lower()
    try:
        with open(path, 'rb') as f:
            if f.read(4) != b'cook': return {}
            num_pages = struct.unpack('>I', f.read(4))[0]
            page_sizes = [struct.unpack('>I', f.read(4))[0] for _ in range(num_pages)]
            for size in page_sizes:
                page = f.read(size)
                if page[:4] != b'\x00\x00\x01\x00': continue
                num_cookies = struct.unpack('<I', page[4:8])[0]
                offsets = [struct.unpack('<I', page[8+i*4:12+i*4])[0] for i in range(num_cookies)]
                for off in offsets:
                    url_off, name_off, val_off = struct.unpack('<I', page[off+16:off+20])[0], struct.unpack('<I', page[off+20:off+24])[0], struct.unpack('<I', page[off+28:off+32])[0]
                    def read_str(o): return page[off+o:page.find(b'\x00', off+o)].decode('utf-8', errors='ignore')
                    c_domain, c_name, c_val = read_str(url_off), read_str(name_off), read_str(val_off)
                    cd = c_domain.lstrip(".").lower()
                    if target in cd or cd in target or cd.endswith(target) or target.endswith(cd):
                        cookies[c_name] = c_val
    except PermissionError as e:
        raise ValueError(
            "Safari cookies are not readable. Grant Full Disk Access to your terminal or use -b chrome."
        ) from e
    return cookies

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:
    profile = profile or os.environ.get("CONVOS_CHROME_PROFILE", "Default")
    db_path = Path.home() / "Library/Application Support/Google/Chrome" / profile / "Cookies"
    if not db_path.exists(): db_path = Path.home() / "Library/Application Support/Google/Chrome" / profile / "Network/Cookies"
    if not db_path.exists(): return {}
    result = subprocess.run(["security", "find-generic-password", "-w", "-a", "Chrome", "-s", "Chrome Safe Storage"], capture_output=True, text=True, timeout=10)
    if result.returncode != 0: return {}
    key = pbkdf2_hmac('sha1', result.stdout.strip().encode(), b'saltysalt', 1003, 16)
    cookies = {}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&nolock=1", uri=True)
    for name, encrypted, host in conn.execute("SELECT name, encrypted_value, host_key FROM cookies WHERE host_key LIKE ?", (f"%{domain}%",)):
        if encrypted[:3] == b'v10':
            cipher = Cipher(algorithms.AES(key), modes.CBC(b' ' * 16))
            dec = cipher.decryptor(); decrypted = dec.update(encrypted[3:]) + dec.finalize()
            cookies[name] = (v[32:] if not (v := decrypted[:-decrypted[-1]])[:32].isascii() else v).decode('utf-8', errors='ignore')
    conn.close()
    return cookies

def get_cookies(domain: str, browser: str = "safari", profile: str | None = None) -> dict[str, str]: return read_safari_cookies(domain) if browser == "safari" else read_chrome_cookies(domain, profile=profile)

def get_cookies_any(domains: list[str], browser: str = "safari", profile: str | None = None) -> dict[str, str]:
    cookies = {}
    for d in domains: cookies.update(get_cookies(d, browser, profile=profile))
    return cookies

def safari_cookie_domains():
    path = Path.home() / "Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies"
    if not path.exists(): path = Path.home() / "Library/Cookies/Cookies.binarycookies"
    if not path.exists(): return set()
    domains = set()
    with open(path, 'rb') as f:
        if f.read(4) != b'cook': return domains
        num_pages = struct.unpack('>I', f.read(4))[0]
        page_sizes = [struct.unpack('>I', f.read(4))[0] for _ in range(num_pages)]
        for size in page_sizes:
            page = f.read(size)
            if page[:4] != b'\x00\x00\x01\x00': continue
            num_cookies = struct.unpack('<I', page[4:8])[0]
            offsets = [struct.unpack('<I', page[8+i*4:12+i*4])[0] for i in range(num_cookies)]
            for off in offsets:
                url_off = struct.unpack('<I', page[off+16:off+20])[0]
                end = page.find(b'\x00', off+url_off)
                if end != -1: domains.add(page[off+url_off:end].decode('utf-8', errors='ignore'))
    return domains

def chrome_cookie_domains(profile: str | None = None):
    profile = profile or os.environ.get("CONVOS_CHROME_PROFILE", "Default")
    db_path = Path.home() / "Library/Application Support/Google/Chrome" / profile / "Cookies"
    if not db_path.exists(): db_path = Path.home() / "Library/Application Support/Google/Chrome" / profile / "Network/Cookies"
    if not db_path.exists(): return set()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&nolock=1", uri=True)
    domains = {r[0] for r in conn.execute("SELECT DISTINCT host_key FROM cookies")}
    conn.close()
    return domains

def chrome_profiles() -> list[str]:
    base = Path.home() / "Library/Application Support/Google/Chrome"
    if not base.exists(): return []
    return [p.name for p in base.iterdir() if p.is_dir() and ((p / "Cookies").exists() or (p / "Network/Cookies").exists())]

def chatgpt_profiles(browser: str) -> list[str | None]:
    if browser != "chrome": return [None]
    if prof := os.environ.get("CONVOS_CHROME_PROFILE"): return [prof]
    return chrome_profiles() or [None]

def chatgpt_cookie_base(browser: str, hosts: list[tuple[str, list[str]]], profile: str | None):
    for url, domains in hosts:
        if c := get_cookies_any(domains, browser, profile=profile): return c, url
    raise ValueError(f"No ChatGPT cookies found in {browser}" + (f" profile {profile}" if profile else ""))

def chatgpt_headers(cookies, base, ua, debug_profile: str | None = None):
    headers = {"Origin": base, "Referer": f"{base}/", "User-Agent": ua, "Accept": "application/json",
               "Accept-Language": "en-US,en;q=0.9", "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors",
               "Sec-Fetch-Dest": "empty"}
    try:
        session = fetch_json(f"{base}/api/auth/session", cookies, headers, timeout=10, retries=0, rate_limit_backoff=300)
        if token := session.get("accessToken"): headers["Authorization"] = f"Bearer {token}"
        if aid := session.get("account", {}).get("id"): headers["ChatGPT-Account-ID"] = aid
        if debug_profile: typer.echo(f"  chatgpt chrome profile={debug_profile} user={session.get('user', {}).get('email')}", flush=True)
    except Exception:
        pass
    return headers

def merge_results(dst: "ParseResult", src: "ParseResult"): dst.convs += src.convs; dst.msgs += src.msgs; dst.tools += src.tools; dst.attachs += src.attachs

def fetch_json(url: str, cookies: dict[str, str], headers: dict = None, timeout: int = 15, retries: int = 1, before_request=None, rate_limit_backoff=None) -> dict:
    parts = []
    for k, v in cookies.items():
        s = f"{k}={v}"
        try: s.encode("latin-1")
        except UnicodeEncodeError: continue
        parts.append(s)
    cookie_str = "; ".join(parts)
    hdrs = {"Cookie": cookie_str, "User-Agent": "Mozilla/5.0", "Accept": "application/json", **(headers or {})}
    req = urllib.request.Request(url, headers=hdrs)
    for i in range(retries+1):
        before_request and before_request()
        try:
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if i == retries: raise
            code, retry = getattr(e, "code", None), (getattr(e, "headers", None) or {}).get("Retry-After", "")
            delay = max(rate_limit_backoff or 30*(i+1), int(retry)) if code == 429 and str(retry).isdigit() else rate_limit_backoff or 30*(i+1) if code == 429 else 1+i
            if code == 429: typer.echo(f"  rate limited; retrying in {delay}s", err=True)
            time.sleep(delay)

class ParseResult:
    def __init__(self, convs=None, msgs=None, tools=None, attachs=None, artifacts=None, edits=None):
        self.convs, self.msgs, self.tools, self.attachs, self.artifacts, self.edits = convs or [], msgs or [], tools or [], attachs or [], artifacts or [], edits or []

def log_parse_error(context: str, err: Exception): typer.echo(f"  parse error ({context}): {type(err).__name__}: {err}", err=True)

def safe_parse(context: str, fn, *args, **kwargs):
    try: return fn(*args, **kwargs)
    except Exception as e: log_parse_error(context, e); return None

def parse_source(path: Path, source: Optional[str] = None, bindings=None) -> ParseResult:
    parsers = {"chatgpt": parse_chatgpt, "claude": parse_claude, "claude-code": parse_claude_code, "codex": parse_codex}
    src = source or detect_source(path)
    if src not in parsers: raise ValueError(f"Unknown source: {src}")
    return parsers[src](path,bindings=bindings) if src in ("claude-code","codex") else parsers[src](path)

def chatgpt_mapping(cid: str, mapping: dict) -> tuple[list, list, list]:
    tree_path=lambda nid:(*tree_path(parent),(mapping[parent].get("children") or [x for x in mapping if mapping[x].get("parent")==parent]).index(nid)) if (parent:=mapping[nid].get("parent")) in mapping else (tuple(mapping).index(nid),)
    def pmid(nid):  # nearest ancestor that carries a message (roots are message-less)
        while (nid := mapping[nid].get("parent")) and not mapping[nid].get("message"): pass
        return gen_id("chatgpt", f"{cid}:{nid}") if nid else None
    msgs, tools, attachs = [], [], []
    for provider_index,(nid,node) in enumerate((nid,mapping[nid]) for nid in sorted(mapping,key=tree_path)):
        if not (msg := node.get("message")): continue
        mid, role, meta = gen_id("chatgpt", f"{cid}:{nid}"), msg.get("author", {}).get("role", "unknown"), msg.get("metadata", {})
        ts, model = ts_any(msg.get("create_time")), meta.get("model_slug")
        if tool := bool(role == "tool" or meta.get("invoked_plugin")):
            tools.append(dict(id=gen_id("chatgpt", f"tool:{mid}"), message_id=mid, tool_name=meta.get("invoked_plugin", {}).get("namespace", role),
                              input=json.dumps(meta.get("args", {})), output=json.dumps(msg.get("content", {})), status="complete", duration_ms=None, created_at=ts))
        content,parts=(lambda c:(c,c.get("parts",[])))(msg.get("content") or {})
        att = [dict(id=gen_id("chatgpt", f"attach:{mid}:{i}"), message_id=mid, filename=p.get("name", ""), mime_type=p.get("content_type"),
                    size=p.get("size"), path=None, url=p.get("asset_pointer"), created_at=ts)
               for i, p in enumerate(parts) if isinstance(p, dict) and p.get("content_type") in ("image_asset_pointer", "file")]
        text = "\n".join(dict.fromkeys(p.strip() for p in [*parts,content.get("text")] if isinstance(p, str) and p.strip()))
        msgs.append(dict(id=mid, conversation_id=cid, role=role, content=text, thinking=None, created_at=ts, model=model, metadata=json.dumps({**meta,"provider_index":provider_index}), parent_id=pmid(nid)))
        attachs += att
    return msgs, tools, attachs

def fetch_chatgpt(browser: str = "safari", limit: int = 0, profiles: list[str | None] | None = None, known: dict | None = None, legacy: set | None = None, frontiers: dict | None = None, sink=None) -> ParseResult:
    hosts = [("https://chatgpt.com", ["chatgpt.com"]), ("https://chat.openai.com", ["chat.openai.com", "openai.com"])]
    debug = os.environ.get("CONVOS_CHATGPT_DEBUG")
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15" if browser == "safari" else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    known, legacy, frontiers, limiters = known or {}, legacy or set(), frontiers or {}, {}

    def fetch_with_profile(profile: str | None) -> ParseResult:
        cookies, base = chatgpt_cookie_base(browser, hosts, profile)
        headers = chatgpt_headers(cookies, base, ua, debug_profile=profile if debug else None)
        key, account, r = profile or "default", headers.get("ChatGPT-Account-ID"), ParseResult(); saved = frontiers.get(key, {}); matched = account and isinstance(saved, dict) and saved.get("account") == account; frontier, boundary = (ts_any(saved.get("updated")), saved.get("id")) if matched else (None, None); bucket = limiters.setdefault(("account",account) if account else ("profile",browser,key), [CHATGPT_BURST,time.monotonic()])
        def pace():
            now = time.monotonic(); bucket[0] = min(CHATGPT_BURST, bucket[0]+(now-bucket[1])*CHATGPT_RATE); bucket[1] = now
            if bucket[0] < 1: time.sleep((1-bucket[0])/CHATGPT_RATE); bucket[:] = [0,time.monotonic()]
            else: bucket[0] -= 1
        def parse_item_raw(item):
            cid, gizmo = gen_id("chatgpt", item["id"]), item.get("gizmo_id"); conv = fetch_json(f"{base}/backend-api/conversation/{item['id']}", cookies, headers, timeout=20, retries=2, before_request=pace, rate_limit_backoff=300)
            msgs, tools, attachs = chatgpt_mapping(cid, conv.get("mapping", {})); times,current=[m["created_at"] for m in msgs if m["created_at"]],(conv.get("mapping",{}).get(conv.get("current_node")) or {}).get("message")
            return dict(conv=dict(id=cid, source="chatgpt", title=item.get("title"), created_at=ts_any(conv.get("create_time") or item.get("create_time")) or min(times, key=datetime.timestamp, default=None), updated_at=ts_any(conv.get("update_time") or item.get("update_time")) or max(times, key=datetime.timestamp, default=None), model=item.get("model"), cwd=None, git_branch=None,
                                  project_id=gizmo, metadata=json.dumps({"session_id":item["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"api","remote_update_time":conv.get("update_time") or item.get("update_time"), **({"remote_complete":current.get("metadata",{}).get("is_complete",current.get("author",{}).get("role")=="assistant" and current.get("status") not in ("in_progress","running"))} if current else {}), **({"gizmo_id":gizmo} if gizmo else {})})), msgs=msgs, tools=tools, attachs=attachs)
        listed, seen, tail, offset, fetched, total = [], set(), set(), 0, 0, None
        while True:
            data = fetch_json(f"{base}/backend-api/conversations?offset={offset}&limit=100&order=updated", cookies, headers, timeout=20, retries=1, before_request=pace, rate_limit_backoff=300)
            raw, reported, keys = data.get("items", []), data.get("total"), ",".join(data.keys())
            if debug: print(f"  chatgpt page offset={offset} items={len(raw)} total={reported} keys={keys}", flush=True)
            if total is not None and reported is not None and reported < total: raise RuntimeError(f"unstable list total {total}->{reported}")
            total = reported if reported is not None else total
            if not raw:
                if reported is not None and offset < reported: raise RuntimeError(f"incomplete list at {offset}/{reported}")
                break
            ids = [it["id"] for it in raw]
            if offset and not tail.intersection(ids): raise RuntimeError(f"unstable list at offset {offset}")
            tail = set(ids[-20:]); items = [it for it in {it["id"]:it for it in raw}.values() if it["id"] not in seen]; seen.update(it["id"] for it in items); cut, stop = len(items), bool(frontier and any((updated := ts_any(it.get("update_time"))) and updated.timestamp() < frontier.timestamp() for it in items))
            if not frontier and boundary and (at := next((i for i, it in enumerate(items) if it["id"] == boundary), None)) is not None: items, cut, stop = items[:at+1]+[it for it in items[at+1:] if gen_id("chatgpt", it["id"]) not in known], len(items), True
            listed += items[:cut]
            if stop or reported is not None and offset+len(raw) >= reported: break
            offset += max(1, len(raw)-20)
        page = [it for it in listed if (cid := gen_id("chatgpt", it["id"])) not in known or known[cid] is None or (updated := ts_any(it.get("update_time"))) is None or updated.timestamp() > (frontier.timestamp() if frontier and cid in legacy else known[cid]+(5 if cid in legacy else 0))][:limit or len(listed)]
        if len(page) > CHATGPT_BURST: typer.echo(f"  chatgpt pacing bulk fetch ({CHATGPT_BURST} burst, then {int(CHATGPT_RATE*300)} requests/5m)")
        for at in range(0, len(page), 20):
            results = []
            for item in page[at:at+20]:
                try: results.append(parse_item_raw(item))
                except Exception as e: raise RuntimeError(f"detail fetch failed: {e}") from e
            chunk = ParseResult([x["conv"] for x in results], [m for x in results for m in x["msgs"]], [t for x in results for t in x["tools"]], [a for x in results for a in x["attachs"]]); sink(chunk) if sink else merge_results(r, chunk)
            fetched += len(results); typer.echo(f"  chatgpt details {fetched}")
        return r
    out, errs = ParseResult(), []
    for profile in profiles if profiles is not None else chatgpt_profiles(browser):
        try: merge_results(out, fetch_with_profile(profile))
        except Exception as e: errs.append(f"{profile or 'default'}: {e}")
    if errs and (profiles is not None or not out.convs or any("detail fetch failed" in e for e in errs)): raise ValueError("ChatGPT fetch failed -- " + " | ".join(errs))
    return out

def fetch_claude(browser: str = "safari", limit: int = 0, since: datetime = None) -> ParseResult:
    cookies = get_cookies("claude.ai", browser)
    if not cookies: raise ValueError(f"No Claude cookies found in {browser}")
    headers = {"Origin": "https://claude.ai", "Referer": "https://claude.ai/",
               "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
               "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9",
               "anthropic-client-sha": "unknown", "anthropic-client-version": "unknown"}
    print("  claude listing...", flush=True)
    orgs = fetch_json("https://claude.ai/api/organizations", cookies, headers)
    org_id = orgs[0]["uuid"] if orgs else None
    if not org_id: raise ValueError("Could not get Claude org ID")
    r = ParseResult()
    data = fetch_json(f"https://claude.ai/api/organizations/{org_id}/chat_conversations", cookies, headers)
    items = data if limit == 0 else data[:limit]
    if items: print(f"  claude total {len(items)}", flush=True)
    fetched, step = 0, max(1, len(items)//10)
    def parse_item_raw(item):
        nonlocal fetched
        updated = ts_from_iso(item.get("updated_at") or item.get("created_at"))
        if since and updated and updated <= since:
            return False
        cid = gen_id("claude", item["uuid"])
        project = item.get("project_uuid")
        r.convs.append(dict(id=cid, source="claude", title=item.get("name"), created_at=ts_from_iso(item.get("created_at")),
                           updated_at=ts_from_iso(item.get("updated_at")), model=item.get("model"), cwd=None, git_branch=None,
                           project_id=project, metadata=json.dumps({"session_id":item["uuid"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"api",**({"project_uuid":project} if project else {})})))
        conv = fetch_json(f"https://claude.ai/api/organizations/{org_id}/chat_conversations/{item['uuid']}", cookies, headers)
        for m in conv.get("chat_messages", []):
            mid = gen_id("claude", f"{cid}:{m.get('uuid', '')}")
            ts = ts_from_iso(m.get("created_at"))
            for i, a in enumerate(m.get("attachments", [])):
                r.attachs.append(dict(id=gen_id("claude", f"attach:{mid}:{i}"), message_id=mid, filename=a.get("file_name"),
                                     mime_type=a.get("file_type"), size=a.get("file_size"), path=None, url=a.get("url"), created_at=ts))
            content = m.get("content", []) if isinstance(m.get("content"), list) else [{"type": "text", "text": m.get("text", "")}]
            text_parts = []
            for j, block in enumerate(content):
                if isinstance(block, dict):
                    if block.get("type") == "tool_use":
                        r.tools.append(dict(id=gen_id("claude", f"tool:{mid}:{j}"), message_id=mid, tool_name=block.get("name"),
                                           input=json.dumps(block.get("input", {})), output="{}", status="pending", duration_ms=None, created_at=ts))
                    elif block.get("type") == "tool_result":
                        r.tools.append(dict(id=gen_id("claude", f"toolres:{mid}:{j}"), message_id=mid, tool_name=block.get("tool_use_id"),
                                           input="{}", output=json.dumps(block.get("content", "")), status="complete", duration_ms=None, created_at=ts))
                    elif block.get("type") == "text": text_parts.append(block.get("text", ""))
                elif isinstance(block, str): text_parts.append(block)
            if text := "\n".join(text_parts).strip():
                r.msgs.append(dict(id=mid, conversation_id=cid, role="user" if m.get("sender")=="human" else m.get("sender", "unknown"), content=text, thinking=None, created_at=ts, model=m.get("model"), metadata="{}", parent_id=None))
        fetched += 1
        return True
    for idx, item in enumerate(items):
        cid = item.get("uuid") if isinstance(item, dict) else "unknown"
        did_fetch = parse_item_raw(item)
        if did_fetch is False:
            if idx == len(items)-1 or (idx+1) % step == 0: print(f"  claude fetched {fetched}/{len(items)}", flush=True)
            continue
        if idx == len(items)-1 or (idx+1) % step == 0: print(f"  claude fetched {fetched}/{len(items)}", flush=True)
    return r

def parse_chatgpt(path: Path) -> ParseResult:
    data = json.load(zipfile.ZipFile(path).open('conversations.json')) if path.suffix == ".zip" else json.loads(path.read_text())
    r = ParseResult()
    def parse_conv(c):
        cid, gizmo = gen_id("chatgpt", c.get("id", "")), c.get("gizmo_id")
        conv = dict(id=cid, source="chatgpt", title=c.get("title"), created_at=ts_any(c.get("create_time")),
                    updated_at=ts_any(c.get("update_time")), model=c.get("default_model_slug"), cwd=None, git_branch=None,
                    project_id=gizmo, metadata=json.dumps({"session_id":c.get("id", ""),"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export",**({"gizmo_id":gizmo} if gizmo else {})}))
        msgs, tools, attachs = chatgpt_mapping(cid, c.get("mapping", {}))
        return dict(conv=conv, msgs=msgs, tools=tools, attachs=attachs)
    for idx, c in enumerate(data):
        cid = c.get("id") if isinstance(c, dict) else idx
        if p := safe_parse(f"chatgpt export conv {cid}", parse_conv, c):
            r.convs.append(p["conv"]); r.msgs += p["msgs"]; r.tools += p["tools"]; r.attachs += p["attachs"]
    return r

def parse_claude(path: Path) -> ParseResult:
    data = json.loads(path.read_text())
    def parse_conv(c):
        cid = gen_id("claude", c["uuid"] if "uuid" in c else c["id"])
        msgs_data = c.get("chat_messages", [])
        parsed=[(i,m,gen_id("claude",f"{cid}:{m['uuid'] if 'uuid' in m else m['id']}"),extract_content(m.get("text") or m.get("content", ""))) for i,m in enumerate(msgs_data)]
        results={t["id"]:t for i,m,mid,ec in parsed for t in ec["tools"] if "output" in t and t.get("id")}
        return {
            "conv": dict(id=cid, source="claude", title=c.get("name") or c.get("title"), created_at=ts_from_iso(c.get("created_at")),
                        updated_at=ts_from_iso(c.get("updated_at")), model=c.get("model"), cwd=None, git_branch=None, project_id=None, metadata=json.dumps({"session_id":c["uuid"] if "uuid" in c else c["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export"})),
            "msgs":[dict(id=mid,conversation_id=cid,role="user" if m.get("sender")=="human" else m.get("sender","unknown"),content=ec["text"],thinking=ec["thinking"],created_at=ts_from_iso(m.get("created_at")),model=m.get("model"),metadata=json.dumps({"provider_index":i}),parent_id=None) for i,m,mid,ec in parsed if ec["text"] or ec["tools"] or ec["attachments"] or m.get("attachments")],
            "tools":[dict(id=gen_id("claude",f"tool:{mid}:{t.get('id') or j}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((results.get(t.get("id")) or {}).get("output","")),status="failed" if (results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in results else "pending",duration_ms=None,created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,t in enumerate(ec["tools"]) if "name" in t],
            "attachs":[dict(id=gen_id("claude",f"attach:{mid}:{j}"),message_id=mid,filename=a.get("name",a.get("file_name")),mime_type=a.get("content_type",a.get("file_type")),size=a.get("size",a.get("file_size")),path=None,url=a.get("asset_pointer",a.get("url")),created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,a in enumerate([*ec["attachments"],*m.get("attachments",[])])]}
    parsed = [p for idx, c in enumerate(data)
              if (p := safe_parse(f"claude export conv {c.get('uuid') if isinstance(c, dict) else idx}", parse_conv, c))]
    return ParseResult(convs=[p["conv"] for p in parsed],msgs=[m for p in parsed for m in p["msgs"]],tools=[t for p in parsed for t in p["tools"]],attachs=[a for p in parsed for a in p["attachs"]])

def iter_jsonl(path: Path):
    for i, line in enumerate(path.open(), start=1):
        if not line.strip(): continue
        try: yield i-1,json.loads(line)
        except Exception as e:
            log_parse_error(f"jsonl {path} line {i}", e)

def parse_claude_code_session(jsonl: Path, bindings=None) -> dict:
    events = [e for _,e in iter_jsonl(jsonl)]
    if not events: return None
    system,root_session,explicit_agent,sidechain=next((e for e in events if e.get("type")=="system"),{}),next((e.get("sessionId") or e.get("session_id") for e in events if e.get("sessionId") or e.get("session_id")),jsonl.stem),next((e["agentId"] for e in events if e.get("agentId")),None),any(e.get("isSidechain") for e in events)
    agent_id=explicit_agent or (jsonl.stem if "subagents" in jsonl.parts else None)
    src,kind,parent,cid="claude-code","subagent" if agent_id or sidechain else "main",root_session if agent_id and agent_id!=root_session else None,(bindings or {}).get(("claude-code",agent_id or root_session),gen_id("claude-code",str(jsonl)))
    timestamps = [ts_from_iso(e["timestamp"]) for e in events if "timestamp" in e]
    msg_events,tool_results=[(i,e) for i,e in enumerate(events) if "message" in e],{t["id"]:t for e in events if "message" in e for t in extract_content(e["message"].get("content",[]))["tools"] if "output" in t and t.get("id")}
    uuid2id = {e["uuid"]: gen_id(src, f"{cid}:{idx}") for idx, (i, e) in enumerate(msg_events) if "uuid" in e}

    def make_msg(idx, i, e):
        c = extract_content(e["message"].get("content", e["message"].get("text", "")))
        return dict(id=gen_id(src,f"{cid}:{idx}"),conversation_id=cid,role="user" if e["type"] in ("human","user") else e["type"],content=c["text"],thinking=c["thinking"],created_at=ts_from_iso(e.get("timestamp")),model=e["message"].get("model") if e["type"]=="assistant" else None,metadata=json.dumps({"provider_index":i}),parent_id=uuid2id.get(e.get("parentUuid")))

    def make_tools(idx, i, e):
        c, ts = extract_content(e["message"].get("content", [])), ts_from_iso(e.get("timestamp"))
        mid = gen_id(src, f"{cid}:{idx}")
        return [dict(id=gen_id(src,f"tool:{cid}:{t.get('id') or f'{idx}:{j}'}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((tool_results.get(t.get("id")) or {}).get("output","")),status="failed" if (tool_results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in tool_results else "pending",duration_ms=None,created_at=ts) for j,t in enumerate(c["tools"]) if "name" in t]

    def make_edits(idx, i, e):
        c, ts = extract_content(e["message"].get("content", [])), ts_from_iso(e.get("timestamp"))
        mid = gen_id(src, f"{cid}:{idx}")
        return [dict(id=gen_id(src, f"edit:{cid}:{idx}:{j}"), message_id=mid, file_path=t["input"]["file_path"],
                    edit_type=t["name"].lower(), content=t["input"].get("content") or t["input"].get("new_string", ""), created_at=ts,
                    old_content=t["input"].get("old_string"))
               for j, t in enumerate(c["tools"]) if t.get("name") in ("Write", "Edit", "MultiEdit") and t.get("input", {}).get("file_path") and t.get("id") in tool_results and not tool_results[t["id"]].get("error")]

    msgs = [make_msg(idx, i, e) for idx, (i, e) in enumerate(msg_events) if (c := extract_content(e["message"].get("content", "")))["text"] or c["tools"]]  # keep tool-only turns: tools/edits reference them
    if not msgs: return None
    return {
        "conv": dict(id=cid, source=src, title=f"{jsonl.parent.name.replace('-Users-', '~/').replace('-', '/')} ({jsonl.stem[:8]})",
                    created_at=timestamps[0] if timestamps else None, updated_at=timestamps[-1] if timestamps else None,
                    model=next((m["model"] for m in msgs if m["model"] and m["model"]!="<synthetic>"),None), cwd=system.get("cwd") or next((e.get("cwd") for e in events if e.get("cwd")),None), git_branch=system.get("gitBranch") or next((e.get("gitBranch") for e in events if e.get("gitBranch")),None), project_id=None,
                    metadata=json.dumps({k:v for k,v in {"session_id":agent_id or root_session,"parent_session_id":parent,"session_kind":kind,"session_kind_evidence":"exact" if explicit_agent or sidechain else "inferred","agent_id":agent_id,"agent_name":next((e.get("agentName") for e in events if e.get("agentName")),None),"agent_role":next((e.get("agentType") for e in events if e.get("agentType")),None),"agent_depth":next((e.get("agentDepth") for e in events if e.get("agentDepth") is not None),None),"originator":system.get("entrypoint"),"client_version":system.get("version"),"capture_mode":"transcript"}.items() if v is not None})),
        "msgs":msgs,"tools":[t for idx,(i,e) in enumerate(msg_events) for t in make_tools(idx,i,e)],"edits":[ed for idx,(i,e) in enumerate(msg_events) for ed in make_edits(idx,i,e)]}

def _parse_sessions(paths,parser,bindings):
    bound={} if bindings is None else bindings
    sessions=[(bound.setdefault((s["conv"]["source"],m["session_id"]),s["conv"]["id"]),s)[1] if (m:=json.loads(s["conv"]["metadata"] or "{}")).get("session_id") else s for path in sorted(paths) if (s:=safe_parse(f"{parser.__name__.removeprefix('parse_').replace('_session','').replace('_','-')} session {path}",parser,path,bound))]
    return ParseResult(convs=[s["conv"] for s in sessions],msgs=[m for s in sessions for m in s["msgs"]],tools=[t for s in sessions for t in s["tools"]],attachs=[a for s in sessions for a in s.get("attachs",[])],edits=[e for s in sessions for e in s["edits"]])
def parse_claude_code(projects_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult: return _parse_sessions(files or projects_dir.rglob("*.jsonl"),parse_claude_code_session,bindings)

def parse_codex_session(jsonl: Path, bindings=None) -> dict | None:
    events,timestamps=[],{}
    for i,e in iter_jsonl(jsonl):
        if "timestamp" in e: timestamps[i]=ts_from_iso(e["timestamp"])
        if e.get("type") in ("session_meta","turn_context") or e.get("type")=="response_item" and isinstance(e.get("payload"),dict) and e["payload"].get("type") in ("message","function_call","function_call_output","custom_tool_call","custom_tool_call_output"): events.append((i,e))
    if not events: return None
    meta,contexts=next((e["payload"] for _,e in events if e.get("type")=="session_meta"),{}),[(i,e["payload"].get("model")) for i,e in events if e.get("type")=="turn_context" and e.get("payload",{}).get("model")]
    subagent,spawn,provider_id=(lambda s:(s,s.get("thread_spawn",{}) if isinstance(s,dict) else {},meta.get("id") or jsonl.stem))((meta.get("source") or {}).get("subagent") if isinstance(meta.get("source"),dict) else None)
    src,items,cid="codex",[(i,e["payload"]) for i,e in events if e.get("type")=="response_item"],(bindings or {}).get(("codex",provider_id),gen_id("codex",str(jsonl)))

    extract_msg_text,model_at=(lambda p:"\n".join(b["text"] for b in p.get("content",[]) if isinstance(b,dict) and b.get("type") in ("input_text","output_text","text") and b.get("text"))),(lambda i:next((m for j,m in reversed(contexts) if j<=i),None))
    def image(i,j,b):
        url=b["image_url"]; data=url.startswith("data:"); head,encoded=url.split(",",1) if data else ("",""); mime=head[5:-7] if head.startswith("data:image/") and head.endswith(";base64") else b.get("mime_type"); required(not data or mime,ValueError("invalid Codex image data URL")); size=len(encoded.rstrip("="))*3//4 if data else None; body=base64.b64decode(encoded,validate=True) if data and len(encoded)<=4*((ATTACHMENT_LIMIT+2)//3) else None; path=attachment_body(body) if body is not None else None; ext={"image/jpeg":"jpg"}.get(mime,mime.rsplit("/",1)[-1] if mime and re.fullmatch(r"image/[a-z0-9.+-]+",mime) else "bin")
        return dict(id=gen_id(src,f"attach:{cid}:{i}:{j}"),message_id=gen_id(src,f"{cid}:{i}"),filename=f"image-{j+1}.{ext}",mime_type=mime,size=len(body) if body is not None else size,path=str(path) if path else None,url=None if data else url,created_at=timestamps.get(i))
    def norm_args(p): return json.loads(a) if isinstance((a := p.get("arguments", {})), str) else a

    mitems = [(i, p, t) for i, p in items if p.get("type") == "message" and ((t := extract_msg_text(p)) or any(isinstance(b,dict) and b.get("type")=="input_image" for b in p.get("content",[])))]
    if not (msgs := [dict(id=gen_id(src, f"{cid}:{i}"), conversation_id=cid, role=p["role"], content=t.strip(), thinking=None, created_at=timestamps.get(i), model=model_at(i), metadata=json.dumps({"provider_index":i}), parent_id=None)
                     for i, p, t in mitems]): return None
    anchor = lambda k: gen_id(src, f"{cid}:{next((i for i, _, _ in reversed(mitems) if i <= k), mitems[0][0])}")  # function_call items are not messages; attach to nearest preceding one

    function_out={p.get("call_id"):p.get("output","") for _,p in items if p.get("type")=="function_call_output"}
    failed=lambda out:any(x in json.dumps(out).lower() for x in ("timed out","timeout","script failed","verification failed","is_error\": true")) or bool(re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+[1-9]\d*",json.dumps(out).lower()))
    edit_ok=lambda call:call in function_out and not failed(function_out[call]) and (not str(function_out[call]).strip() or str(function_out[call]).strip().lower()=="ok" or bool(re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+0\b|script completed|success\. updated",json.dumps(function_out[call]).lower())))
    tools = [dict(id=gen_id(src,f"tool:{cid}:{p.get('call_id') or i}"),message_id=anchor(i),tool_name=p["name"],
                 input=json.dumps(args),output=json.dumps(function_out.get(p.get("call_id"),"")),status="failed" if p.get("call_id") in function_out and failed(function_out[p.get("call_id")]) else "complete" if p.get("call_id") in function_out else "pending",duration_ms=None,
                 created_at=timestamps.get(i))
            for i,p in items if p.get("type")=="function_call" and (args:=norm_args(p)) is not None]
    custom_out = {p.get("call_id"):p.get("output", "") for _, p in items if p.get("type") == "custom_tool_call_output"}
    tools += [dict(id=gen_id(src, f"custom:{cid}:{i}"), message_id=anchor(i), tool_name=p["name"], input=json.dumps({"code":p.get("input", "")}), output=json.dumps(custom_out.get(p.get("call_id"), "")), status="failed" if any(x in json.dumps(custom_out.get(p.get("call_id"), "")).lower() for x in ("script failed","verification failed")) or re.search(r"exit code: [1-9]\d*",json.dumps(custom_out.get(p.get("call_id"), "")).lower()) else "complete" if p.get("call_id") in custom_out or p.get("status") == "completed" else p.get("status", "pending"), duration_ms=None, created_at=timestamps.get(i)) for i, p in items if p.get("type") == "custom_tool_call"]

    def patch_edits(args):
        """Exact file edits from patches/heredocs; retain commands for redirects with unknown content."""
        cmd = args.get("cmd") or args.get("command") or ""
        cmd = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
        root = args.get("workdir") or meta.get("cwd") or ""
        if "*** Begin Patch" not in cmd:
            head = cmd.split("\n", 1)[0]
            if (hm := re.search(r"<<-?\s*'?(\w+)'?", head)) and (tm := re.search(r"(?:(?<![0-9&])>{1,2}\s*|\btee\s+(?:-a\s+)?)([^\s;|&<>'\"]+)", head)) \
               and (body := re.search(rf"\n(.*)\n{hm.group(1)}\s*$", cmd, re.S)) and tm.group(1) != "/dev/null":
                return [(os.path.join(root, tm.group(1)), "write", body.group(1), None)]
            if (tm := re.search(r"(?<![0-9&])>{1,2}\s*([^\s;|&<>'\"]+\.[A-Za-z]{1,5})\b", head)) and tm.group(1) != "/dev/null":
                return [(os.path.join(root, tm.group(1)), "shell", cmd, None)]
            return []
        out, path, op, old, new = [], None, None, [], []
        def flush():
            if path and (old or new or op != "edit"): out.append((path, op, "\n".join(new), "\n".join(old) or None))
            old.clear(); new.clear()
        for ln in cmd.split("*** Begin Patch", 1)[1].split("*** End Patch", 1)[0].splitlines():
            if m := re.match(r"\*\*\* (Update|Add|Delete) File: (.+)", ln):
                flush(); op, path = {"Update": "edit", "Add": "write", "Delete": "delete"}[m.group(1)], os.path.join(root, m.group(2).strip())
            elif ln.startswith("@@"): flush()
            elif ln.startswith("***"): pass  # e.g. *** End of File
            elif ln.startswith("+"): new.append(ln[1:])
            elif ln.startswith("-"): old.append(ln[1:])
            elif path: old.append(ln[1:] if ln.startswith(" ") else ln); new.append(ln[1:] if ln.startswith(" ") else ln)
        flush(); return out

    def custom_edits(p):
        code = p.get("input", ""); names = re.findall(r"await\s+tools\.apply_patch\(\s*(\w+)\s*\)", code); out=json.dumps(custom_out.get(p.get("call_id"), "")).lower(); ok=any(x in out for x in ("script completed","exit code: 0","success. updated")) and not any(x in out for x in ("script failed","verification failed")) and not re.search(r"exit code: [1-9]\d*",out)
        if not ok or p.get("name") == "apply_patch": return patch_edits({"cmd":code}) if ok else []
        vals = {n:v for n, s in re.findall(r"(?:const|let|var)\s+(\w+)\s*=\s*(\"(?:\\.|[^\"\\])*\")", code, re.S) if n in names and (v := safe_parse("codex custom edit", json.loads, s)) is not None}
        patches = [vals[n] for n in names if n in vals] + [p for s in re.findall(r"await\s+tools\.apply_patch\(\s*(\"(?:\\.|[^\"\\])*\")\s*\)", code, re.S) if (p := safe_parse("codex custom edit", json.loads, s)) is not None]
        return [e for patch in patches if "*** Begin Patch" in patch for e in patch_edits({"cmd":patch})]

    edits = [dict(id=gen_id(src, f"edit:{cid}:{i}:{j}"), message_id=anchor(i), file_path=fp, edit_type=op,
                 content=c, created_at=timestamps.get(i), old_content=o)
            for i, p in items if p.get("type") == "function_call" and p.get("name") in ("exec_command", "shell_command", "shell")
            and edit_ok(p.get("call_id")) and (args := norm_args(p)) for j, (fp, op, c, o) in enumerate(patch_edits(args))]
    edits += [dict(id=gen_id(src, f"edit:{cid}:{i}:{j}"), message_id=anchor(i), file_path=fp, edit_type=op, content=c, created_at=timestamps.get(i), old_content=o) for i, p in items if p.get("type") == "custom_tool_call" for j, (fp, op, c, o) in enumerate(custom_edits(p))]

    return {"conv":dict(id=cid,source=src,title=meta.get("cwd") or jsonl.stem,created_at=min(timestamps.values(),default=None),updated_at=max(timestamps.values(),default=None),model=next((m["model"] for m in msgs if m["role"]=="assistant" and m["model"]),None),cwd=meta.get("cwd"),git_branch=(meta.get("git") or {}).get("branch"),project_id=None,metadata=json.dumps({k:v for k,v in {"session_id":provider_id,"parent_session_id":spawn.get("parent_thread_id") or meta.get("parent_thread_id"),"session_kind":"subagent" if subagent is not None else "main","session_kind_evidence":"exact" if subagent is not None else "inferred","agent_name":spawn.get("agent_nickname") or meta.get("agent_nickname"),"agent_role":spawn.get("agent_role") or (subagent if isinstance(subagent,str) else meta.get("agent_role")),"agent_depth":spawn.get("depth"),"originator":meta.get("originator"),"client_version":meta.get("cli_version"),"capture_mode":"transcript","git_repository":(meta.get("git") or {}).get("repository_url"),"git_commit":(meta.get("git") or {}).get("commit_hash"),"forked_from_id":meta.get("forked_from_id"),"thread_source":meta.get("thread_source")}.items() if v is not None})),"msgs":msgs,"tools":tools,"attachs":[image(i,j,b) for i,p,t in mitems for j,b in enumerate(x for x in p.get("content",[]) if isinstance(x,dict) and x.get("type")=="input_image")],"edits":edits}

def parse_codex(codex_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult:
    if not (sessions_dir:=codex_dir/"sessions").exists(): return ParseResult()
    return _parse_sessions(files or sessions_dir.rglob("*.jsonl"),parse_codex_session,bindings)

_CONV_UPS = "INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET source=excluded.source,title=excluded.title,created_at=CASE WHEN conversations.created_at IS NULL OR excluded.created_at < conversations.created_at THEN excluded.created_at ELSE conversations.created_at END,updated_at=CASE WHEN conversations.updated_at IS NULL OR excluded.updated_at > conversations.updated_at THEN excluded.updated_at ELSE conversations.updated_at END,model=COALESCE(excluded.model,conversations.model),cwd=COALESCE(excluded.cwd,conversations.cwd),git_branch=COALESCE(excluded.git_branch,conversations.git_branch),project_id=COALESCE(excluded.project_id,conversations.project_id),metadata=excluded.metadata"

def _id_conflict(rows,fields): return next((rid for rid in {r["id"] for r in rows} if len({tuple(str(r[k]) for k in fields) for r in rows if r["id"]==rid})>1),None)
def upsert(conn, r: ParseResult):
    required(not (conflict:=_id_conflict(r.convs,("source","cwd","git_branch","project_id","metadata")) or _id_conflict(r.msgs,("conversation_id","role","content","thinking","created_at","model","metadata","parent_id"))),ValueError(f"divergent provider session in import batch: {conflict}"))
    quarantined={c["id"] for c in r.convs if c["source"]=="codex" and (meta:=json.loads(c["metadata"] or "{}")).get("session_kind")=="main" and not meta.get("parent_session_id") and any(m["conversation_id"]==c["id"] and m["role"]=="user" for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="user" and not re.fullmatch(_INJECTED_RE,m["content"]) for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="assistant" for m in r.msgs) and not any(x.get("conversation_id")==c["id"] or x.get("message_id") in {m["id"] for m in r.msgs if m["conversation_id"]==c["id"]} for x in [*r.tools,*r.attachs,*r.artifacts,*r.edits])}
    [c.__setitem__("metadata",json.dumps({**json.loads(c["metadata"] or "{}"),"capture_mode":"startup-stub-candidate"})) for c in r.convs if c["id"] in quarantined]
    cids,mids,bindings=[c["id"] for c in r.convs],[m["id"] for m in r.msgs],[(c["source"],meta["session_id"],c["id"]) for c in r.convs if (meta:=json.loads(c["metadata"] or "{}")).get("session_id")]
    required(not (conflict:=conn.execute("SELECT p.source,p.session_id,p.conversation_id,json_extract_string(j.value,'$.conversation_id') FROM provider_sessions p JOIN json_each(?) j ON p.source=json_extract_string(j.value,'$.source') AND p.session_id=json_extract_string(j.value,'$.session_id') WHERE p.conversation_id<>json_extract_string(j.value,'$.conversation_id') LIMIT 1",(json.dumps([dict(source=s,session_id=i,conversation_id=c) for s,i,c in bindings]),)).fetchone() if bindings else None),ValueError(f"provider session identity conflict: {conflict}"))
    cur = conn.execute
    old_convs = {x[0]:x for x in cur(f"SELECT * FROM conversations WHERE id IN ({','.join(['?']*len(cids))})", cids).fetchall()} if cids else {}
    old_msgs = {x[0]: x for x in cur(f"SELECT * FROM messages WHERE id IN ({','.join(['?']*len(mids))})", mids).fetchall()} if mids else {}
    new_convs = set(cids) - set(old_convs)
    changed_rows,changed_msgs={m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][:8]+old_msgs[m["id"]][9:]!=tuple(m.values())},{m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][2:5] != tuple(m[k] for k in ("role", "content", "thinking"))}
    updated = {m["conversation_id"] for m in r.msgs if m["id"] in changed_msgs} - new_convs
    [conn.execute(_CONV_UPS,list(c.values())) for c in r.convs if old_convs.get(c["id"])!=tuple(c.values())]
    bindings and conn.executemany("INSERT INTO provider_sessions VALUES (?,?,?) ON CONFLICT(source,session_id) DO NOTHING",list(dict.fromkeys(bindings)))
    frozen=getattr(r,"scopes",None)
    frozen=frozen if frozen is not None else pending_scopes([(c["id"],c["cwd"]) for c in r.convs])
    frozen and conn.executemany("INSERT OR IGNORE INTO provenance.conversation_scopes VALUES (?,?,?,?,?,?)",frozen)
    for m in r.msgs:
        vals = list(m.values()); old = old_msgs.get(m["id"])
        if old and old[2:5] != tuple(vals[2:5]): hist = list(old); hist[0] = gen_id("history", f"messages:{m['id']}:{json.dumps(hist[2:5], default=str)}"); meta = json.loads(hist[7] or "{}"); hist[7] = json.dumps({**meta, "history_of":m["id"], "superseded_at":datetime.now().isoformat()}); changed_msgs.add(hist[0]); conn.execute("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", hist)
    [conn.execute(_MSG_UPS,list(m.values())) for m in r.msgs if m["id"] in changed_rows]
    historical=[]
    def replace_preserving(table, rows):
        if not rows: return
        ids = [r["id"] for r in rows]; old = {x[0]: x for x in cur(f"SELECT * FROM {table} WHERE id IN ({','.join(['?']*len(ids))})", ids).fetchall()}
        for r in rows:
            vals = list(r.values())
            skip = {"tool_calls":7, "attachments":7, "artifacts":6, "file_edits":5}[table]; prev = old.get(r["id"]); payload = lambda x: tuple(v for i, v in enumerate(x) if i not in (0, skip))
            if not prev or payload(prev)!=payload(vals): historical.append((table,r["id"]))
            if prev and payload(prev) != payload(vals): hist = list(prev); hist[0] = gen_id("history", f"{table}:{r['id']}:{json.dumps(payload(hist), default=str)}"); historical.append((table,hist[0])); cur(f"INSERT INTO {table} VALUES ({','.join(['?']*len(hist))}) ON CONFLICT DO NOTHING", hist)
            if not prev or payload(prev)!=payload(vals): cur(f"INSERT OR REPLACE INTO {table} VALUES ({','.join(['?']*len(vals))})", vals)
    [replace_preserving(t, rows) for t, rows in (("tool_calls", r.tools), ("attachments", r.attachs), ("artifacts", r.artifacts), ("file_edits", r.edits))]
    (frozen_edits:=getattr(r,"edit_scopes",None)) is not None or (frozen_edits:=pending_edit_scopes(edit_scope_inputs(r)))
    frozen_edits and conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",frozen_edits)
    [index_attachment_body(conn,a["id"],a["path"],a.get("size")) for a in r.attachs if a.get("path")]
    changed_convs={x[0] for x in cur(f"SELECT * FROM conversations WHERE id IN ({','.join(['?']*len(cids))})",cids).fetchall() if old_convs.get(x[0])!=x} if cids else set(); archive_msgs=changed_rows|changed_msgs-set(mids)
    if changed_convs or archive_msgs or historical: _archive_touch(conn,[("conversations",x) for x in changed_convs]+[("messages",x) for x in archive_msgs]+historical)
    r.provenance_edits,r.provenance_conversations={x["id"] for x in r.edits}&{i for t,i in historical if t=="file_edits"},changed_convs
    return len(r.convs), len(r.msgs), len(r.tools), len(r.attachs), len(r.edits), len(new_convs), len(updated), changed_msgs

def hook_root(source): return Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"projects" if source == "claude-code" else Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"sessions"
def hook_result(source, path, bindings=None): s = (parse_claude_code_session if source == "claude-code" else parse_codex_session)(path,bindings); return ParseResult(convs=[s["conv"]], msgs=s["msgs"], tools=s["tools"], attachs=s.get("attachs",[]), edits=s["edits"]) if s else ParseResult()
def enqueue_hook(source, payload):
    path = Path(payload["transcript_path"]).expanduser().resolve(); root = hook_root(source).expanduser().resolve()
    if source not in ("claude-code", "codex") or path.suffix != ".jsonl" or not path.is_relative_to(root): raise ValueError(f"Invalid {source} transcript path")
    st, key = path.stat(), gen_id("hook", f"{source}:{path}"); atomic_json(HOOK_DIR/f"{key}.json", dict(source=source, path=str(path), mtime=st.st_mtime_ns, size=st.st_size))
    subprocess.Popen([sys.executable, "-m", "ai_convos", "drain-hooks", "--no-block"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
def retry_hook(work, force=False):
    q,target=(q:=work.with_suffix(".json")),q if q.exists() else work
    if force: atomic_json(target,{**json.loads(target.read_text()),"retry":True})
    work.unlink(missing_ok=True) if q.exists() else os.replace(work, q)
def merge_embed_dirty(ids):
    old = set(json.loads(HOOK_EMBED_DIRTY.read_text())) if HOOK_EMBED_DIRTY.exists() else set(); atomic_json(HOOK_EMBED_DIRTY, sorted(old | set(ids)))
def mark_dirty(ids):
    if not ids: return
    with (HOOK_DIR/".lock").open("w") as lock: fcntl.flock(lock, fcntl.LOCK_EX); HOOK_FTS_DIRTY.touch(); merge_embed_dirty(ids)
def drain_hooks(embed=False, local_only=False,block=False):
    HOOK_DIR.mkdir(parents=True, exist_ok=True); done,claims,failed,started = [],[],0,time.monotonic()
    with (HOOK_DIR/".drain.lock").open("w") as drain:
        try: fcntl.flock(drain,fcntl.LOCK_EX|(0 if block else fcntl.LOCK_NB))
        except BlockingIOError: return 0
        with (HOOK_DIR/".lock").open("w") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX); state=json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {}
            for w in HOOK_DIR.glob("*.work"):
                e=json.loads(w.read_text()); done.append((w,w.stem,e["snap"],set(e["changed"]))) if "changed" in e else retry_hook(w,True)
            for q in sorted(HOOK_DIR.glob("*.json"),key=lambda p:(p.stat().st_mtime_ns,p.name))[:HOOK_DRAIN_EVENTS]: work=q.with_suffix(".work"); os.replace(q,work); claims.append(work)
        if claims: init_schema(conn:=get_db()); bindings=session_bindings(conn)
        claims and conn.close()
        for n,work in enumerate(claims):
            if n and time.monotonic()-started>=HOOK_DRAIN_SECONDS: break
            try:
                e,path,key,st,snap=(e:=json.loads(work.read_text())),(path:=Path(e["path"])),work.stem,(st:=path.stat()),[st.st_mtime_ns,st.st_size]
                if state.get(key)==snap: work.unlink(); continue
                r=hook_result(e["source"],path,bindings); st2=path.stat()
                if snap!=[st2.st_mtime_ns,st2.st_size]:
                    with (HOOK_DIR/".lock").open("w") as lock: fcntl.flock(lock,fcntl.LOCK_EX); retry_hook(work)
                    continue
                if not r.convs: done.append((work,key,snap,set())); continue
                init_schema(conn:=get_db())
                with contextlib.closing(conn),_transaction(conn): changed=upsert(conn,r)[-1]|({m["id"] for m in r.msgs} if e.get("retry") else set())
                capture_provenance(edit_ids=[x["id"] for x in r.edits],conversation_ids=[x["id"] for x in r.convs],source=f"{e['source']}.hook"); atomic_json(work,{**e,"snap":snap,"changed":sorted(changed)}); done.append((work,key,snap,changed))
            except FileNotFoundError: work.unlink(missing_ok=True)
            except Exception as error:
                with (HOOK_DIR/".lock").open("w") as lock: fcntl.flock(lock,fcntl.LOCK_EX); retry_hook(work)
                failed+=1
                log_parse_error(f"hook inbox {work}",error)
        if done:
            with (HOOK_DIR/".lock").open("w") as lock:
                fcntl.flock(lock,fcntl.LOCK_EX); dirty=set().union(*(d for _,_,_,d in done)); HOOK_FTS_DIRTY.touch() if dirty else None; dirty and merge_embed_dirty(dirty)
                for _,key,snap,_ in done: state[key]=snap
                atomic_json(HOOK_STATE,state); [work.unlink(missing_ok=True) for work,_,_,_ in done]
    atomic_json(HOOK_PROGRESS,dict(completed_at=time.time_ns(),processed=len(done),failed=failed,pending=len(pending:=[*HOOK_DIR.glob("*.json"),*HOOK_DIR.glob("*.work")]),oldest=min((p.stat().st_mtime_ns for p in pending),default=None)))
    if pending and len(pending)>failed: subprocess.Popen([sys.executable,"-m","ai_convos","drain-hooks","--no-block"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    if embed:
        try: embed_hook_pending(local_only=local_only)
        except Exception as e: log_parse_error("hook embeddings", e)
    return len(done)

def flush_fts():
    HOOK_DIR.mkdir(parents=True, exist_ok=True)
    with (HOOK_DIR/".fts.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with (HOOK_DIR/".lock").open("w") as dirty_lock:
            fcntl.flock(dirty_lock, fcntl.LOCK_EX); claims = list(DATA_DIR.glob(f".{HOOK_FTS_DIRTY.name}.*"))
            if claims: HOOK_FTS_DIRTY.touch(); [p.unlink(missing_ok=True) for p in claims]
            if not HOOK_FTS_DIRTY.exists(): return False
            claim = HOOK_FTS_DIRTY.with_name(f".{HOOK_FTS_DIRTY.name}.{os.getpid()}"); os.replace(HOOK_FTS_DIRTY, claim)
        try:
            conn = get_db(wait=0)
            try: rebuild_fts_index(conn)
            finally: conn.close()
        except BaseException: HOOK_FTS_DIRTY.touch(); raise
        finally: claim.unlink(missing_ok=True)
    return True

_MODEL, _MCFG, _LLAMA_LOG = None, dict(repo_id="ggml-org/embeddinggemma-300m-qat-q8_0-GGUF", filename="embeddinggemma-300m-qat-Q8_0.gguf", revision="66f974f8cd48cc3b9c41c516b95508e75b4bee64", embedding=True, n_ctx=16384, n_batch=2048, n_ubatch=2048, n_seq_max=8, n_gpu_layers=-1), None
def embedding_model_path(local_only=False): from huggingface_hub import hf_hub_download; return hf_hub_download(_MCFG["repo_id"],_MCFG["filename"],revision=_MCFG["revision"],local_files_only=local_only)
def _llama(local_only=False):
    global _MODEL, _LLAMA_LOG
    if _MODEL is None:
        from llama_cpp import Llama; import llama_cpp.llama_cpp as lc, warnings; warnings.filterwarnings("ignore", message="The `local_dir_use_symlinks` argument is deprecated.*", category=UserWarning)
        if _LLAMA_LOG is None: _LLAMA_LOG = lc.llama_log_callback(lambda *_: None); lc.llama_log_set(_LLAMA_LOG, None)
        cfg = _MCFG.copy(); nseq = cfg.pop("n_seq_max", 0); [cfg.pop(k) for k in ("repo_id","filename","revision")]
        if nseq:
            orig = lc.llama_context_default_params
            lc.llama_context_default_params = lambda o=orig, n=nseq: (setattr(p := o(), "n_seq_max", n) or p)
        try: _MODEL = Llama(model_path=embedding_model_path(local_only), **cfg, verbose=False)
        finally:
            if nseq: lc.llama_context_default_params = orig
    return _MODEL
def embed_texts(ss: list[str], doc: bool = False, local_only=False) -> list[list[float]]:
    p = "task: search result | document: " if doc else "task: search result | query: "; return [d["embedding"] for d in _llama(local_only).create_embedding([p + (s or "")[:1600] for s in ss])["data"]]
def embed_text(s: str, doc: bool = False, local_only=False) -> list[float]: return embed_texts([s], doc, local_only)[0]
def embed_pending(batch: int = 32, ids=None, local_only=False):
    if ids == []: return
    Q = "FROM messages WHERE embedding IS NULL AND content IS NOT NULL AND content != ''" + _NOISE + (f" AND id IN ({','.join(['?']*len(ids))})" if ids is not None else ""); ps = ids or []
    conn = get_db(read_only=True); n = conn.execute(f"SELECT COUNT(*) {Q}", ps).fetchone()[0]; conn.close()
    if not n: return
    typer.echo(f"Embedding {n} messages...", err=True); done = 0
    while True:
        conn = get_db(read_only=True); rows = conn.execute(f"SELECT id, content {Q} ORDER BY LEAST(length(content),1600) LIMIT ?", ps + [batch]).fetchall(); conn.close()
        if not rows: break
        updates = []
        for ch in [rows[i:i+_MCFG["n_seq_max"]] for i in range(0, len(rows), _MCFG["n_seq_max"])]:
            updates += [(e, mid) for (mid, _), e in zip(ch, embed_texts([c for _, c in ch], doc=True, local_only=local_only))]
        conn = get_db(); conn.executemany("UPDATE messages SET embedding=? WHERE id=? AND embedding IS NULL", updates); conn.close()
        done += len(rows); typer.echo(f"  {done}/{n}\r", nl=False, err=True)
    typer.echo(err=True)
def embed_hook_pending(all_msgs=False, batch=32, local_only=False):
    HOOK_DIR.mkdir(parents=True, exist_ok=True)
    with (HOOK_DIR/".embed.lock").open("w") as embed_lock:
        fcntl.flock(embed_lock, fcntl.LOCK_EX); claim = HOOK_EMBED_DIRTY.with_name(f".{HOOK_EMBED_DIRTY.name}.{os.getpid()}")
        with (HOOK_DIR/".lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if HOOK_EMBED_DIRTY.exists(): os.replace(HOOK_EMBED_DIRTY, claim)
            elif not all_msgs: return
        ids = json.loads(claim.read_text()) if claim.exists() else []
        try: embed_pending(batch, None if all_msgs else ids, local_only)
        except BaseException:
            with (HOOK_DIR/".lock").open("w") as lock: fcntl.flock(lock, fcntl.LOCK_EX); merge_embed_dirty(ids)
            raise
        finally: claim.unlink(missing_ok=True)

def _ro():
    try: c = get_db(read_only=True)
    except ValueError as e: typer.echo(str(e), err=True); return None
    if c is None: typer.echo("Database not found. Run `convos init` or `convos sync`."); return None
    if not ensure_db_ready(c): c.close(); return None
    return c
def _hybrid_ro():
    if (c := _ro()) is None: return None
    if c.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone(): return c
    c.close(); c = get_db(); init_schema(c); c.close(); return _ro()
def _fts_ro(hybrid=False):
    try: flush_fts()
    except Exception as e: typer.echo(f"FTS refresh failed; using last indexed snapshot: {e}", err=True)
    if (c := _hybrid_ro() if hybrid else _ro()) is None: return None
    try: load_fts(c)
    except ValueError as e: c.close(); typer.echo(str(e)); return None
    if not c.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone():
        if HOOK_FTS_DIRTY.exists(): c.close(); typer.echo("FTS index unavailable until its first refresh can acquire the database",err=True); return None
        c.close(); w = get_db(); load_fts(w); ensure_fts_index(w); w.close(); c = _ro(); load_fts(c)
    return c
def _filt(source, days, role, cwd=None, conversation=None):
    w,p=map(list,zip(*pairs)) if (pairs:=[(q,v) for v,q in ((source,"c.source = ?"),(datetime.now()-timedelta(days=days) if days else None,"m.created_at > ?"),(role,"m.role = ?")) if v]) else ([],[])
    if cwd: raw,resolved=map(str,(Path(cwd).expanduser().absolute(),Path(cwd).expanduser().resolve())); w.append("(c.cwd=? OR starts_with(c.cwd,?) OR c.cwd=? OR starts_with(c.cwd,?))"); p.extend((raw,raw.rstrip("/")+"/",resolved,resolved.rstrip("/")+"/"))
    if conversation: w.append("starts_with(c.id,?)"); p.append(conversation)
    return w, p
def _clip(s, n): return (s or "")[:n] + ("..." if s and len(s) > n else "")
def _fmt_hit(content, ts, role, title, src, cid, cwd, q, ctx, meta):
    p = _clip(content, ctx)
    for w in q.split(): p = re.sub(f"({re.escape(w)})", r"\033[1;33m\1\033[0m", p, flags=re.I)
    typer.echo(f"\n{'='*60}\n[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid[:8]})\n{role} @ {ts or '?'} ({meta})\n{'-'*40}\n{p}")

def emit(data, fmt):
    if fmt == "jsonl" and isinstance(data, list): [typer.echo(json.dumps(r, default=str)) for r in data]
    else: typer.echo(json.dumps(data, default=str))

@app.command("hook", hidden=True)
@app.command("capture", hidden=True)
def capture(source: str):
    try: enqueue_hook(source, json.loads(sys.stdin.read() or "{}"))
    except Exception as e: log_parse_error(f"{source} hook", e)

@app.command("drain-hooks", hidden=True)
def drain_hooks_cmd(block:bool=typer.Option(False,"--block/--no-block",hidden=True)): drain_hooks(block=block)

@app.command()
def init():
    conn = get_db(); init_schema(conn); rebuild_fts_index(conn); conn.close(); HOOK_FTS_DIRTY.unlink(missing_ok=True)
    sync(False, 300, True, True, False, False, True); install_skills(); install_hooks(False, False); [typer.echo(ep.load()()) for ep in entry_points(group="convos.init")]
    typer.echo(f"Database initialized at {DB_PATH}")

@app.command()
def search(query: str, source: Optional[str] = typer.Option(None, "-s"), days: Optional[int] = typer.Option(None, "-d"), role: Optional[str] = typer.Option(None, "-r"), cwd: Optional[Path] = typer.Option(None, "--cwd", "-w"), conversation: Optional[str] = typer.Option(None, "--conversation"), thinking: bool = typer.Option(False, "--thinking", "-t"), limit: int = typer.Option(20, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _fts_ro()) is None: return
    w, p = _filt(source, days, role, cwd, conversation)
    results = conn.execute(f"""SELECT m.id, m.content, m.thinking, m.role, m.created_at, fts_main_messages.match_bm25(m.id, ?) as score, c.title, c.source, c.id, c.cwd
        FROM messages m JOIN conversations c ON m.conversation_id = c.id WHERE score IS NOT NULL{' AND ' + ' AND '.join(w) if w else ''}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY score DESC)=1 ORDER BY score DESC LIMIT ?""", [query] + p + [limit]).fetchall()
    conn.close()
    if fmt != "text": emit([dict(message_id=mid, role=r, content=_clip(content, context), thinking=_clip(think, context) if thinking and think else None, created_at=ts, score=score, title=title, source=src, conversation_id=cid, cwd=cwd) for mid, content, think, r, ts, score, title, src, cid, cwd in results], fmt); return
    if not results: typer.echo("No results"); return
    for _, content, think, r, ts, score, title, src, cid, cwd in results:
        _fmt_hit(content, ts, r, title, src, cid, cwd, query, context, f"score: {score:.2f}")
        if thinking and think: typer.echo(f"\n[THINKING]\n{_clip(think, context)}")
    typer.echo(f"\n{len(results)} results")

@app.command("read")
def read_cmd(conversation: str, limit: int = typer.Option(20, "-n", min=1), context: int = typer.Option(2000, "-c", min=1), around: Optional[str] = typer.Option(None, "--around", "-a"), thinking: bool = typer.Option(False, "--thinking", "-t"), fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _ro()) is None: return
    cs = conn.execute("SELECT id,title,source,cwd FROM conversations WHERE starts_with(id, ?) ORDER BY updated_at DESC NULLS LAST LIMIT 2", [conversation]).fetchall()
    if len(cs) != 1:
        conn.close(); typer.echo("No matching conversation" if not cs else "Ambiguous prefix: " + ", ".join(c[0] for c in cs), err=True); raise typer.Exit(1)
    cid, title, src, cwd = cs[0]
    base = f"SELECT m.id,m.role,m.content,m.thinking,m.created_at,ROW_NUMBER() OVER (ORDER BY {MESSAGE_ORDER}) pos FROM messages m WHERE m.conversation_id=? AND json_extract_string(m.metadata,'$.history_of') IS NULL AND (COALESCE(m.content,'')!='' OR COALESCE(m.thinking,'')!='')"
    if around and len(mids := conn.execute("SELECT id FROM messages WHERE conversation_id=? AND starts_with(id,?) AND json_extract_string(metadata,'$.history_of') IS NULL LIMIT 2", [cid, around]).fetchall()) != 1:
        conn.close(); typer.echo("No matching message" if not mids else "Ambiguous message prefix: " + ", ".join(m[0] for m in mids), err=True); raise typer.Exit(1)
    rows = conn.execute(f"WITH b AS ({base}),t AS (SELECT pos FROM b WHERE id=?) SELECT id,role,content,thinking,created_at FROM (SELECT b.*,abs(b.pos-t.pos) d FROM b,t ORDER BY d,b.pos LIMIT ?) ORDER BY pos", [cid, mids[0][0], limit]).fetchall() if around else conn.execute(f"SELECT id,role,content,thinking,created_at FROM ({base}) ORDER BY pos DESC LIMIT ?", [cid, limit]).fetchall()[::-1]
    conn.close()
    data = [dict(id=mid, role=role, content=_clip(content, context), thinking=_clip(think, context) if thinking and think else None, created_at=ts) for mid, role, content, think, ts in rows]
    if fmt != "text": emit(data, fmt); return
    typer.echo(f"[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid})")
    [typer.echo(f"\n{m['role']} @ {m['created_at'] or '?'}\n{m['content']}{f'''\n[THINKING]\n{m['thinking']}''' if m['thinking'] else ''}") for m in data]; typer.echo(f"\n{len(data)} messages")

def hybrid_hits(q, source=None, days=None, role=None, limit=10, local_only=False, cwd=None, conversation=None):
    drain_hooks(embed=True, local_only=local_only); conn = _fts_ro(True)
    if conn is None: raise ValueError("Archive retrieval is unavailable")
    if not conn.execute("SELECT COUNT(*) FROM messages WHERE embedding IS NOT NULL").fetchone()[0]: conn.close(); raise ValueError("No embeddings yet. Run `convos embed`, or use `convos search` for BM25 only.")
    try: qv = embed_text(q,False,True) if local_only else embed_text(q,False)
    except Exception as e: conn.close(); raise ValueError(f"Hybrid embedding failed: {e}") from e
    w, p = _filt(source, days, role, cwd, conversation); rows = conn.execute(f"""WITH qe AS (SELECT ?::FLOAT[768] AS v),
        base AS (SELECT m.id, m.embedding FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.content IS NOT NULL{_NOISE}{' AND ' + ' AND '.join(w) if w else ''}),
        fts AS (SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS r FROM (SELECT id, fts_main_messages.match_bm25(id, ?) AS score FROM base) s WHERE score IS NOT NULL LIMIT 50),
        vec AS (SELECT b.id, ROW_NUMBER() OVER (ORDER BY array_cosine_similarity(b.embedding, qe.v) DESC) AS r FROM base b, qe WHERE b.embedding IS NOT NULL LIMIT 50),
        fused AS (SELECT id, SUM(1.0/(60+r)) AS rrf FROM (SELECT id, r FROM fts UNION ALL SELECT id, r FROM vec) GROUP BY id)
        SELECT fused.rrf, m.id, m.role, m.content, m.created_at, c.title, c.source, c.id, c.cwd FROM fused JOIN messages m ON m.id = fused.id JOIN conversations c ON c.id = m.conversation_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY fused.rrf DESC)=1 ORDER BY fused.rrf DESC LIMIT ?""", [qv] + p + [q, limit]).fetchall()
    conn.close(); return [dict(score=score, message_id=mid, role=r, content=content, created_at=ts, title=title, source=src, conversation_id=cid, cwd=cwd) for score, mid, r, content, ts, title, src, cid, cwd in rows]
@app.command("query")
def query_cmd(q: str, source: Optional[str] = typer.Option(None, "-s"), days: Optional[int] = typer.Option(None, "-d"), role: Optional[str] = typer.Option(None, "-r"), cwd: Optional[Path] = typer.Option(None, "--cwd", "-w"), conversation: Optional[str] = typer.Option(None, "--conversation"), limit: int = typer.Option(10, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format")):
    try: rows = hybrid_hits(q, source, days, role, limit, cwd=cwd, conversation=conversation)
    except ValueError as e: typer.echo(str(e), err=True); return
    if not rows: typer.echo("No results"); return
    if fmt != "text": emit([{**r,"content":_clip(r["content"],context)} for r in rows], fmt); return
    for x in rows: _fmt_hit(x["content"], x["created_at"], x["role"], x["title"], x["source"], x["conversation_id"], x["cwd"], q, context, f"score: {x['score']:.4f}")
    typer.echo(f"\n{len(rows)} results")

@app.command("embed")
def embed_cmd(batch: int = typer.Option(32, "-b")):
    conn = get_db(); init_schema(conn); conn.close()
    try: embed_hook_pending(True, batch); typer.echo("Embeddings ready")
    except Exception as e: typer.echo(f"Embedding failed: {e}", err=True)

@app.command()
def doctor(verbose: bool = typer.Option(False, "-v")):
    typer.echo(f"convos: {version('convos')}")
    pending = len(list(HOOK_DIR.glob("*.json"))) + len(list(HOOK_DIR.glob("*.work"))); state = json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {}; progress=json.loads(HOOK_PROGRESS.read_text()) if HOOK_PROGRESS.exists() else {}; last = max((v[0] for v in state.values()), default=0); age=max(0,time.time_ns()-(progress.get("oldest") or time.time_ns()))/1e9 if pending else 0
    dirty = len(json.loads(HOOK_EMBED_DIRTY.read_text())) if HOOK_EMBED_DIRTY.exists() else 0; claims = len(list(DATA_DIR.glob(f".{HOOK_EMBED_DIRTY.name}.*")))
    typer.echo(f"ingest: pending={pending}, embedding_ids={dirty}, embedding_claims={claims}, last={datetime.fromtimestamp(last/1e9).isoformat(timespec='seconds') if last else 'never'}, oldest={age:.0f}s, last_batch={progress.get('processed',0)} ok/{progress.get('failed',0)} failed")
    if DB_PATH.exists():
        try:
            conn = get_db(read_only=True); cols = set(conn.execute("SELECT table_name,column_name FROM information_schema.columns").fetchall()); required = {"conversations":("id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"), "messages":("id","conversation_id","role","content","thinking","created_at","model","metadata","embedding","parent_id"), "tool_calls":("id","message_id","tool_name","input","output","status","duration_ms","created_at"), "attachments":("id","message_id","filename","mime_type","size","path","url","created_at"), "artifacts":("id","conversation_id","artifact_type","title","content","language","created_at","version"), "file_edits":("id","message_id","file_path","edit_type","content","created_at","old_content")}; missing = [f"{t}.{c}" for t, cs in required.items() for c in cs if (t,c) not in cols]
            convs, msgs, unembedded, latest = conn.execute(f"SELECT (SELECT COUNT(*) FROM conversations),(SELECT COUNT(*) FROM messages),(SELECT COUNT(*) FROM messages WHERE embedding IS NULL AND COALESCE(content,'')!=''{_NOISE}),(SELECT MAX(updated_at) FROM conversations)").fetchone() if not missing else (0,0,0,None); fts = bool(conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()); conn.close()
            typer.echo(f"archive: {convs} convs, {msgs} msgs, {unembedded} unembedded, {DB_PATH.stat().st_size/1024**3:.1f} GB, latest={latest or 'never'}, schema={'ready' if not missing else 'missing:' + ','.join(missing)}, fts={'yes' if fts else 'no'}")
            if missing or not fts: typer.echo("repair: convos init")
            elif unembedded: typer.echo("repair: convos embed")
        except Exception as e: typer.echo(f"archive: unavailable ({e})")
    else: typer.echo(f"archive: missing ({DB_PATH})"); typer.echo("repair: convos init")
    _,skill,_,dests = _skill_paths(); expected = skill.read_text() if skill.exists() else None; current = sum(expected is not None and p.is_file() and not p.is_symlink() and p.read_text() == expected for p in dests); typer.echo(f"skills: {current}/2 current"); current == 2 or typer.echo("repair: convos install-skills")
    install_hooks(status=True)
    for ep in entry_points(group="convos.doctor"):
        try: typer.echo(ep.load()())
        except Exception as e: typer.echo(f"{ep.name}: unavailable ({e})")
    def has(domains, host): return any(host in d or d in host for d in domains)
    targets = ["chatgpt.com", "chat.openai.com", "openai.com", "claude.ai"]
    for name, getter in [("safari", safari_cookie_domains), ("chrome", chrome_cookie_domains)]:
        try: domains = getter()
        except PermissionError: typer.echo(f"{name}: no access to cookies"); continue
        summary = ", ".join(f"{t}={'yes' if has(domains, t) else 'no'}" for t in targets)
        typer.echo(f"{name}: {summary}")
        if verbose:
            cg = read_safari_cookies("chatgpt.com") if name == "safari" else read_chrome_cookies("chatgpt.com")
            keys = set(cg.keys())
            sig = [k for k in ["__Secure-next-auth.session-token", "__Secure-next-auth.session-token.0",
                               "__Secure-next-auth.session-token.1", "cf_clearance", "__cf_bm"] if k in keys]
            typer.echo(f"{name}: chatgpt cookies={len(cg)} keys={','.join(sig) if sig else 'none'}")

def _skill_paths(): rel = Path("skills")/"convos"/"SKILL.md"; shares = [Path(p)/"share"/"convos" for p in (sysconfig.get_paths().get("data",""),site.getuserbase())]; roots = [PROJECT_ROOT,Path(__file__).resolve().parents[2],*shares]; skill = next((r/rel for r in roots if (r/rel).exists()),roots[-1]/rel); homes = [Path(os.environ.get("CODEX_HOME",Path.home()/".codex")),Path(os.environ.get("CLAUDE_CONFIG_DIR",Path.home()/".claude"))]; return rel,skill,homes,[home/rel for home in homes]
@app.command()
def install_skills():
    rel, skill, homes, dests = _skill_paths(); olds = [home/"skills"/"agent-convos"/"SKILL.md" for home in homes]
    if not skill.exists(): typer.echo(f"Missing skill: {skill}", err=True); raise typer.Exit(1)
    text = skill.read_text(); legacy = text.replace("name: convos","name: agent-convos",1).replace("# Convos","# Agent Convos",1); resolved = [Path(os.path.realpath(p)) for p in dests]
    if unsafe := next((p for home,p,target in zip(homes,dests,resolved) if p.is_symlink() or p.exists() and not p.is_file() or any(q.is_symlink() and resolved.count(target)<2 or q.exists() and not q.is_dir() for q in [home/Path(*rel.parts[:i]) for i in range(1,len(rel.parts))])), None): typer.echo(f"Refusing unsafe managed file: {unsafe}", err=True); raise typer.Exit(1)
    for dest,old in zip(dests,olds): atomic_write(dest, text); typer.echo(f"Installed {dest}"); (old.unlink(),typer.echo(f"Removed legacy {old}")) if old.is_file() and not old.is_symlink() and old.read_text()==legacy else None

def _capture_command(source): root=Path(os.environ.get("CONVOS_PROJECT_ROOT",PROJECT_ROOT)).expanduser().resolve(); return f"{f'CONVOS_PROJECT_ROOT={shlex.quote(str(root))} ' if root!=Path.home()/'.convos' else ''}{shlex.quote(str(Path(sys.executable).with_name('convos')))} capture {source}"
def _managed_hook(h, source): return h.get("command", "").endswith("convos remote hook") or h.get("command", "").endswith((f" hook {source}", f" capture {source}")) and h.get("statusMessage") in ("Updating conversation archive", "Saving conversation to Convos")
def edit_hook_config(path, events, source, remove=False):
    data = json.loads(path.read_text()) if path.exists() else {}; hooks = data.setdefault("hooks", {})
    for event in list(hooks):
        for group in hooks[event]: group["hooks"] = [h for h in group.get("hooks", []) if not _managed_hook(h, source)]
        hooks[event] = [g for g in hooks[event] if g.get("hooks")]
        if not hooks[event]: del hooks[event]
    if not remove:
        cmd = _capture_command(source)
        for event in events: hooks.setdefault(event, []).append(dict(hooks=[dict(type="command", command=cmd, timeout=5, statusMessage="Saving conversation to Convos")]))
    return data, sum(_managed_hook(h, source) for gs in hooks.values() for g in gs for h in g.get("hooks", []))

@app.command("install-hooks")
def install_hooks(remove: bool = typer.Option(False, "--remove"), status: bool = typer.Option(False, "--status")):
    cfgs = [(Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"settings.json", ("Stop", "SessionEnd"), "claude-code"), (Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"hooks.json", ("Stop",), "codex")]
    if not status and (unsafe := next((p for p,_,_ in cfgs if p.is_symlink() or p.exists() and not p.is_file() or p.parent.exists() and not p.parent.is_dir()), None)): typer.echo(f"Refusing unsafe managed file: {unsafe}", err=True); raise typer.Exit(1)
    if not status: cfgs = [(path,events,source,*edit_hook_config(path, events, source, remove)) for path,events,source in cfgs]
    for path, events, source, *planned in cfgs:
        if status:
            expected = _capture_command(source); data = json.loads(path.read_text()) if path.exists() else {}; n = sum(sum(h.get("command") == expected and h.get("statusMessage") == "Saving conversation to Convos" for g in data.get("hooks", {}).get(event, []) for h in g.get("hooks", [])) == 1 for event in events); n *= sum(_managed_hook(h, source) for gs in data.get("hooks", {}).values() for g in gs for h in g.get("hooks", [])) == len(events)
        else: data, n = planned; atomic_json(path, data)
        typer.echo(f"{source}: {n} hook{'s' if n != 1 else ''}{' installed' if not status and not remove else ''} ({path})" + ("; repair: convos install-hooks" if status and n != len(events) else ""))
    if not status and not remove: typer.echo("Start a new agent session; in Codex, review the user hook with `/hooks`.")

@app.command()
def export(output: Path, fmt: str = typer.Option("json", "-f"), source: Optional[str] = typer.Option(None, "-s")):
    if (conn := _ro()) is None: return
    where, params = ("WHERE c.source = ?", [source]) if source else ("", [])
    if fmt == "json":
        rows = conn.execute(f"SELECT c.id, c.source, c.title, c.created_at, c.updated_at, c.model, c.cwd, c.git_branch, c.project_id FROM conversations c {where}", params).fetchall()
        result = []
        for r in rows:
            msgs = [dict(role=m[0], content=m[1], thinking=m[2], created_at=str(m[3]) if m[3] else None, model=m[4])
                   for m in conn.execute(f"SELECT m.role,m.content,m.thinking,m.created_at,m.model FROM messages m WHERE m.conversation_id=? ORDER BY {MESSAGE_ORDER}",[r[0]]).fetchall()]
            tcs = [dict(tool=t[0], input=json.loads(t[1]), output=json.loads(t[2]), status=t[3])
                  for t in conn.execute("SELECT tool_name, input, output, status FROM tool_calls tc JOIN messages m ON tc.message_id = m.id WHERE m.conversation_id = ?", [r[0]]).fetchall()]
            edits = [dict(file=e[0], type=e[1], content=e[2])
                    for e in conn.execute("SELECT fe.file_path, fe.edit_type, fe.content FROM file_edits fe JOIN messages m ON fe.message_id = m.id WHERE m.conversation_id = ?", [r[0]]).fetchall()]
            result.append(dict(id=r[0], source=r[1], title=r[2], created_at=str(r[3]) if r[3] else None, updated_at=str(r[4]) if r[4] else None,
                              model=r[5], cwd=r[6], git_branch=r[7], project_id=r[8], messages=msgs, tool_calls=tcs, file_edits=edits))
        output.write_text(json.dumps(result, indent=2))
    else:
        cur = conn.execute(f"SELECT c.id,c.source,c.title,c.cwd,m.role,m.content,m.created_at FROM conversations c JOIN messages m ON c.id=m.conversation_id {where} ORDER BY c.created_at,{MESSAGE_ORDER}",params)
        with output.open("w", newline="") as f: w = csv.writer(f); w.writerow([d[0] for d in cur.description]); w.writerows(cur.fetchall())
    conn.close(); typer.echo(f"Exported to {output}")

def _sync_leader():
    try: fcntl.flock(lock:=(DATA_DIR/".sync.lock").open("w"),fcntl.LOCK_EX|fcntl.LOCK_NB); return lock
    except BlockingIOError: lock.close(); typer.echo("Sync already running; no work was started"); raise typer.Exit()

@app.command()
def sync(watch: bool = typer.Option(False, "-w"), interval: int = typer.Option(300, "-i"), claude_code: bool = True, codex: bool = True, full: bool = typer.Option(False, "--full", help="Re-parse/re-fetch all sources and reconcile all provenance"), verbose: bool = typer.Option(False, "-v", "--verbose"), local_only: bool = typer.Option(False, "--local-only", help="Import local agent sessions and configured exports without contacting web sources.")):
    if sys.argv[1:2] == ["sync"]: signal.signal(signal.SIGINT, signal.SIG_DFL)
    conn = get_db(); init_schema(conn); conn.close()
    drain_hooks()
    state, dirty, local, web, imports = {}, False, {}, {}, {}; chatgpt_ok, chatgpt_frontiers, offline = {}, {}, local_only is True
    def set_state(section, key, val):
        nonlocal dirty
        if state.setdefault(section, {}).get(key) != val: state[section][key] = val; dirty = True
    def plan_local(name, path, parser, bindings, sink):
        if not path.exists(): return None
        if name in ("codex", "claude-code"):
            prev, mt = local.get(name, {}).get("files", {}), {str(p):m for p in path.rglob("*.jsonl") if (m:=stat_mtime(p)) is not None}; files=list(map(Path,mt))
            if not (chg := files if full or local.get(name,{}).get("parser")!=PARSER_EPOCH else [p for p in files if mt.get(str(p), 0) > prev.get(str(p), 0)]): return None
            return dict(name=name, label=name.replace("-", " ").title(), source=name, func=lambda p=path, fs=chg, saved=(saved:=[]): [saved.append(sink(parser(p,fs[i:i+20],bindings))) for i in range(0,len(fs),20)] and ParseResult(), saved=saved, state=("local", name, {"parser":PARSER_EPOCH,"files":mt}))
        mtime = latest_mtime(path)
        if not full and mtime <= local.get(name, {}).get("mtime", 0): return None
        return dict(name=name, label=name.replace("-", " ").title(), source=name, func=lambda p=path: parser(p), state=("local", name, {"mtime": mtime}))
    def probe_chatgpt(browser):
        hosts = [("https://chatgpt.com", ["chatgpt.com"]), ("https://chat.openai.com", ["chat.openai.com", "openai.com"])]
        profiles = chatgpt_profiles(browser)
        errors, heads, ok, frontiers, accounts = [], [], [], {}, set()
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15" if browser == "safari" else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        for profile in profiles:
            try:
                cookies, base = chatgpt_cookie_base(browser, hosts, profile)
                headers = chatgpt_headers(cookies, base, ua)
                items = fetch_json(f"{base}/backend-api/conversations?offset=0&limit=1&order=updated", cookies, headers, rate_limit_backoff=300)["items"]
                account = headers.get("ChatGPT-Account-ID")
                if items and (not account or account not in accounts): item = items[0]; heads.append(f"{profile or 'default'}:{item['id']}:{item.get('update_time')}"); ok.append(profile); frontiers[profile or "default"] = {"account":account,"updated":item.get("update_time"),"id":item["id"]}; account and accounts.add(account)
            except Exception as e:
                errors.append(f"chatgpt.com{f'/{profile}' if profile else ''}: {e}")
        if heads: errors and typer.echo("chatgpt profiles skipped: " + " | ".join(errors), err=True); chatgpt_ok[browser], chatgpt_frontiers[browser] = ok, frontiers; return "|".join(heads)
        raise ValueError(f"ChatGPT request failed in {browser}: " + " | ".join(errors)) if errors else ValueError("ChatGPT request failed")
    def probe_claude(browser):
        cookies = get_cookies("claude.ai", browser)
        if not cookies: raise ValueError(f"No Claude cookies found in {browser}")
        headers = {"Origin": "https://claude.ai", "Referer": "https://claude.ai/",
                   "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                   "Accept": "application/json", "Accept-Language": "en-US,en;q=0.9",
                   "anthropic-client-sha": "unknown", "anthropic-client-version": "unknown"}
        orgs = fetch_json("https://claude.ai/api/organizations", cookies, headers)
        org_id = orgs[0]["uuid"] if orgs else None
        if not org_id: raise ValueError("Could not get Claude org ID")
        items = fetch_json(f"https://claude.ai/api/organizations/{org_id}/chat_conversations", cookies, headers)
        if not items: return None
        return f"{(item := items[0])['uuid']}:{item.get('updated_at') or item.get('created_at')}"
    def plan_web(name, fetcher, probe, known=None, sink=None, legacy=None, frontier_ok=True):
        pref = web.get(name, {})
        forced = os.environ.get(f"CONVOS_{name.upper()}_BROWSER")
        order = [forced] if forced else [pref.get("browser")] + [b for b in ("safari", "chrome") if b != pref.get("browser")]
        errors = []
        for b in [x for x in order if x]:
            try:
                head = probe(b); lu = head.split(":", 1)[1] if (name == "claude" and head and ":" in head) else None
                current = {**(pref.get("frontiers", {}) if b == pref.get("browser") else {}), **chatgpt_frontiers.get(b, {})} if name == "chatgpt" else None
                st = {"browser": b, "head": head, **({"frontiers":current} if name == "chatgpt" else {"last_updated":lu} if lu else {})}
                if name != "chatgpt" and head is not None and b == pref.get("browser") and head == pref.get("head") and not full:
                    set_state("web", name, st); return None
                last = pref.get("last_updated")
                since = ts_from_iso(last) if (name == "claude" and last and not full) else None
                saved = None if name == "claude" else []
                coverage = pref.get("coverage"); frontier = pref.get("frontiers") if frontier_ok and not full and b == pref.get("browser") and known and isinstance(coverage, list) and set(coverage) <= set(known) else None
                func = (lambda b=b, since=since: fetcher(b, since=since)) if name == "claude" else (lambda b=b, saved=saved: fetcher(b, profiles=chatgpt_ok[b], known=known, legacy=legacy, frontiers=frontier, sink=lambda r: saved.append(sink(r))))
                return dict(name=name, label=name.title(), source=name, func=func, state=("web", name, st), saved=saved)
            except Exception as e:
                errors.append(f"{b}: {e}")
        if errors: typer.echo(f"{name}: no cookies found -- skipped" if all("cookies" in e.lower() for e in errors) else f"{name} sync failed: " + " | ".join(errors))
        return None
    def plan_import(path: Path):
        if not path.exists(): return None
        mtime = latest_mtime(path) if path.is_dir() else path.stat().st_mtime
        if mtime <= imports.get(str(path), {}).get("mtime", 0): return None
        return dict(name=f"import:{path}", label=f"import:{path}", func=lambda p=path: parse_source(p), state=("imports", str(path), {"mtime": mtime}))
    def do_sync():
        nonlocal state, dirty, local, web, imports
        sync_lock=_sync_leader(); state=load_state()
        local,web,imports=state.setdefault("local",{}),state.setdefault("web",{}),state.setdefault("imports",{}); chatgpt_ok.clear(); chatgpt_frontiers.clear()
        t0 = time.perf_counter(); dirty, total, changed, jobs, newc, updc, provenance_edits, provenance_conversations, repair_attempted = False, [0]*5, set(), [], 0, 0, set(), set(), set()
        def checkpoint(r):
            ids = {m["id"] for m in r.msgs}
            with (HOOK_DIR/".lock").open("w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX); HOOK_FTS_DIRTY.touch() if ids else None; ids and merge_embed_dirty(ids); conn = get_db()
                with contextlib.closing(conn),_transaction(conn): out = upsert(conn, r); known.update({c["id"]:(u.timestamp() if (u := ts_any(json.loads(c["metadata"]).get("remote_update_time"))) else None) for c in r.convs if c["source"]=="chatgpt"}); repair_attempted.update(c["id"] for c in r.convs if c["id"] in repair_order); return (*out,getattr(r,"provenance_edits",set()),getattr(r,"provenance_conversations",set()))
        conn = get_db(); repair=conn.execute("SELECT 1 FROM conversations WHERE source='chatgpt' AND (created_at IS NULL OR updated_at IS NULL) LIMIT 1").fetchone()
        if repair: conn.execute("BEGIN"); repaired=[r[0] for r in conn.execute("SELECT id FROM conversations WHERE source='chatgpt' AND (created_at IS NULL OR updated_at IS NULL)").fetchall()]; conn.execute("UPDATE conversations c SET created_at=COALESCE(c.created_at,t.first_seen),updated_at=COALESCE(c.updated_at,t.last_seen) FROM (SELECT conversation_id,MIN(created_at) first_seen,MAX(created_at) last_seen FROM messages GROUP BY conversation_id) t WHERE c.id=t.conversation_id AND c.source='chatgpt' AND (c.created_at IS NULL OR c.updated_at IS NULL)"); _archive_touch(conn,[("conversations",x) for x in repaired]); conn.execute("COMMIT")
        conn.close()
        conn = get_db(read_only=True)
        cur,bindings = counts_by_source(conn),session_bindings(conn)
        rows,candidates,prior_order,updated,repair_order=(rs:=conn.execute(f"SELECT c.id,c.updated_at,json_extract_string(c.metadata,'$.remote_update_time'),json_extract_string(c.metadata,'$.remote_complete'),(SELECT role FROM messages m WHERE m.conversation_id=c.id ORDER BY {MESSAGE_ORDER_DESC} LIMIT 1) FROM conversations c WHERE source='chatgpt'").fetchall()),(cs:={r[0] for r in conn.execute("SELECT DISTINCT m.conversation_id FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.source='chatgpt' AND json_extract_string(m.metadata,'$.provider_index') IS NULL QUALIFY count(*) OVER (PARTITION BY m.conversation_id,m.created_at)>1").fetchall()}),(po:=web.get("chatgpt",{}).get("order_repairs",{})),(us:={cid:v.timestamp() if (v:=ts_any(raw)) else ts.timestamp() if ts else None for cid,ts,raw,_,_ in rs}),{cid for cid in cs if po.get(cid)!=us[cid]}
        known = {cid:None if cid in repair_order or (complete=="false" or complete is None and role=="tool") and (v:=ts_any(raw) or ts) and (datetime.now()-v).total_seconds()<900 else updated[cid] for cid, ts, raw, complete, role in rows}
        legacy = {cid for cid, _, raw, _, _ in rows if raw is None}
        conn.close()
        fmt = lambda v: f"{v[0]} convs, {v[1]} msgs, {v[2]} tools, {v[3]} attachs, {v[4]} edits"
        start = lambda label, src=None: typer.echo(f"Syncing {label}" if not src else f"Syncing {label} ({fmt(cur.setdefault(src, [0]*5))})")
        if paths := [Path(p).expanduser() for p in os.environ.get("CONVOS_IMPORT_PATHS", "").split(",") if p.strip()]:
            start("imports")
            jobs += [j for p in paths if (j := plan_import(p))]
        if claude_code and (p := Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"projects").exists():
            start("Claude Code", "claude-code"); jobs += [j for j in [plan_local("claude-code", p, parse_claude_code,bindings,checkpoint)] if j]
        if codex and (p := Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))).exists():
            start("Codex", "codex"); jobs += [j for j in [plan_local("codex", p, parse_codex,bindings,checkpoint)] if j]
        if not offline: start("ChatGPT"+(f", provider order={len(candidates)-len(repair_order)} attempted/{len(candidates)} unresolved" if candidates else ""), "chatgpt"); jobs += [j for j in [plan_web("chatgpt", fetch_chatgpt, probe_chatgpt, {} if full else known, checkpoint, legacy, not repair_order)] if j]
        if not offline: start("Claude", "claude"); jobs += [j for j in [plan_web("claude", fetch_claude, probe_claude)] if j]
        verbose and typer.echo(f"Planning took {time.perf_counter()-t0:.2f}s")
        if jobs:
            with ThreadPoolExecutor(max_workers=min(4, len(jobs))) as ex:
                futs = {ex.submit(j["func"]): {**j, "t": time.perf_counter()} for j in jobs}
                for fut in as_completed(futs):
                    j = futs[fut]
                    try:
                        r = fut.result()
                    except Exception as e: typer.echo(f"{j['name']} failed: {e}"); r = None
                    if saved := j.get("saved"):
                        c,m,t,a,e,n,u,changed_ids,provenance_edits,provenance_conversations=(*[sum(s[i] for s in saved) for i in range(7)],set().union(*(s[7] for s in saved)),provenance_edits|set().union(*(s[8] for s in saved)),provenance_conversations|set().union(*(s[9] for s in saved)))
                    elif r is not None:
                        with (HOOK_DIR/".lock").open("w") as lock:
                            fcntl.flock(lock, fcntl.LOCK_EX)
                            conn = get_db()
                            with contextlib.closing(conn),_transaction(conn): c, m, t, a, e, n, u, changed_ids = upsert(conn,r)
                    else: continue
                    total = [total[i]+v for i, v in enumerate([c, m, t, a, e])]
                    newc, updc = newc+n, updc+u
                    changed |= changed_ids
                    if r is not None: provenance_edits|=getattr(r,"provenance_edits",set()); provenance_conversations|=getattr(r,"provenance_conversations",set())
                    if r is not None and (st := j.get("state")):
                        if j["name"] == "chatgpt": st[2]["coverage"],st[2]["order_repairs"]=sorted(known),{cid:known[cid] for cid in ({cid for cid in candidates if prior_order.get(cid)==updated[cid]}|repair_attempted)}
                        set_state(*st)
                    if src := j.get("source"): typer.echo(f"Updated {j['label']} ({n} new, {u} updated convs; {fmt([c, m, t, a, e])} processed){' before failure' if r is None else ''}{' in %.2fs' % (time.perf_counter()-j['t']) if verbose else ''}")
        capture_provenance() if full else capture_provenance(edit_ids=provenance_edits,conversation_ids=provenance_conversations)
        mark_dirty(changed)
        if dirty: atomic_json(STATE_PATH, state)
        sync_lock.close()
        verbose and typer.echo(f"Total sync time {time.perf_counter()-t0:.2f}s")
        return total, newc, updc
    if watch:
        typer.echo(f"Daemon mode (interval: {interval}s)")
        while True: r, n, u = do_sync(); typer.echo(f"[{datetime.now().isoformat()}] {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits"); time.sleep(interval)
    else:
        r, n, u = do_sync(); typer.echo(f"Updated {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits processed")
        fmt = lambda v: f"{v[0]} convs, {v[1]} msgs, {v[2]} tools, {v[3]} attachs, {v[4]} edits"; conn = get_db(read_only=True); total = [conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("conversations", "messages", "tool_calls", "attachments", "file_edits")]; conn.close(); typer.echo(f"Total: {fmt(total)}")

@app.command()
def sql(query: str, fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _ro()) is None: return
    try: cur = conn.execute(query); cols = [d[0] for d in cur.description]; rows = cur.fetchall()
    except Exception as e: conn.close(); typer.echo(f"Query failed: {e}", err=True); return
    conn.close()
    if fmt != "text": emit([dict(zip(cols, r)) for r in rows], fmt); return
    typer.echo(" | ".join(cols)); [typer.echo(" | ".join("" if v is None else str(v) for v in r)) for r in rows]; typer.echo(f"\n{len(rows)} rows")

for _ep in entry_points(group="convos.commands"):
    try: _ep.load()(app)
    except Exception as _e: typer.echo(f"plugin {_ep.name} failed: {_e}", err=True)  # a broken plugin must not kill the CLI

if __name__ == "__main__": app()
