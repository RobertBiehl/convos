#!/usr/bin/env python3
import base64, contextlib, csv, duckdb, fcntl, hashlib, itertools, json, os, re, shlex, shutil, signal, site, sqlite3, ssl, struct, subprocess, sys, sysconfig, tempfile, time, typer, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from importlib.metadata import entry_points, version
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from .migrations import fts_needs_rebuild, migrate_remote_changes, migrate_remote_data, migrate_remote_ids, migration_memory, remote_id_migration_scope

app = typer.Typer(help="AI Conversations DB - searchable archive for Claude, ChatGPT, and Codex")
def find_root(): return Path(r).expanduser() if (r := os.environ.get("CONVOS_PROJECT_ROOT")) else Path.home()/".convos"
PROJECT_ROOT,DATA_DIR,DB_PATH,STATE_PATH=(root:=find_root()),(data:=root/"data"),data/"convos.db",data/"sync_state.json"
HOOK_DIR,HOOK_STATE,HOOK_PROGRESS,HOOK_EMBED_DIRTY,HOOK_FTS_DIRTY,_NOISE_RE,_NOISE,HOOK_DRAIN_EVENTS,HOOK_DRAIN_SECONDS,CHATGPT_BURST,CHATGPT_RATE,PARSER_EPOCH=DATA_DIR/"hook_inbox",DATA_DIR/"hook_state.json",DATA_DIR/"hook_progress.json",DATA_DIR/"hook_embeddings_dirty",DATA_DIR/"hook_fts_dirty",(_NR:=r"^(Base directory for this skill:|# AGENTS\.md instructions for|<(codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)( |>))"),f" AND NOT regexp_matches(content,'{_NR}')",8,10,20,8/15,4  # conservative policy below the observed ~200-detail failure point
_CHATGPT_HOSTS,_BROWSER_UA,_CLAUDE_HEADERS=(("https://chatgpt.com",("chatgpt.com",)),("https://chat.openai.com",("chat.openai.com","openai.com"))),(ua:={"safari":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15","chrome":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}),{"Origin":"https://claude.ai","Referer":"https://claude.ai/","User-Agent":ua["safari"],"Accept":"application/json","Accept-Language":"en-US,en;q=0.9","anthropic-client-sha":"unknown","anthropic-client-version":"unknown"}
_INJECTED_RE,MESSAGE_ORDER,MESSAGE_ORDER_DESC=r"(?s)(?:# AGENTS\.md instructions for [^\n]+\n\n<INSTRUCTIONS>\n.*\n</INSTRUCTIONS>|<(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)(?: [^>]*)?>.*</(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)>)\s*","m.created_at NULLS FIRST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) NULLS LAST,m.id","m.created_at DESC NULLS LAST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) DESC NULLS LAST,m.id DESC"

def open_db(path=None,read_only=False,wait=30,deadline=None):
    path,deadline=Path(path or DB_PATH),deadline if deadline is not None else time.monotonic()+wait
    path.parent.mkdir(parents=True,exist_ok=True)
    if read_only and not path.exists(): return None
    while True:
        try: return duckdb.connect(str(path),read_only=read_only)
        except Exception as e:
            if "Conflicting lock is held" not in str(e): raise
            if time.monotonic()>=deadline: raise ValueError(f"Database stayed locked by another convos process for {wait:g} seconds.") from e
            time.sleep(.05)
class _LockedDB:
    def __init__(self,db,lock):
        self.db,self.stack=db,contextlib.ExitStack()
        [self.stack.callback(resource.close) for resource in (lock,db)]
    def __getattr__(self,name): return getattr(self.db,name)
    def close(self): self.stack.close()
def _flock(lock,op,deadline,wait):
    while True:
        try: return fcntl.flock(lock,op|fcntl.LOCK_NB)
        except BlockingIOError: time.sleep(min(.05,remaining)) if (remaining:=deadline-time.monotonic())>0 else (_ for _ in ()).throw(ValueError(f"Database stayed locked by another convos process for {wait:g} seconds."))
def get_db(read_only:bool=False,wait=30):
    HOOK_DIR.mkdir(parents=True,exist_ok=True)
    deadline,lock=time.monotonic()+wait,(HOOK_DIR/".db.lock").open("w")
    try:
        _flock(lock,fcntl.LOCK_SH if read_only else fcntl.LOCK_EX,deadline,wait)
        db=open_db(read_only=read_only,wait=wait,deadline=deadline)
    except BaseException:
        lock.close()
        raise
    return _LockedDB(db,lock) if db is not None else (lock.close() or None)
@contextlib.contextmanager
def _core(path=None,read_only=False,ready=False,wait=30):
    with contextlib.closing(open_db(path,read_only,wait) if path else get_db(read_only,wait)) as db:
        if ready: init_schema(db)
        yield db
@contextlib.contextmanager
def _locked(path,op=fcntl.LOCK_EX):
    with Path(path).open("w") as lock:
        fcntl.flock(lock,op)
        yield lock
@contextlib.contextmanager
def _transaction(db):
    db.execute("BEGIN")
    try: yield
    except BaseException:
        db.execute("ROLLBACK")
        raise
    else: db.execute("COMMIT")
def required(value,error): return value if value else (_ for _ in ()).throw(error)
def secure_dir(path,mode=0o700): return (required(not (path:=Path(path)).is_symlink(),ValueError("managed directory must not be a symlink")),path.mkdir(parents=True,exist_ok=True),os.chmod(path,mode),path)[-1]

def load_state():
    try: return json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    except Exception: return {}

def atomic_write(path: Path, text):
    if path.is_symlink() or path.exists() and not path.is_file(): raise typer.Exit(typer.echo(f"Refusing unsafe managed file: {path}",err=True) or 1)
    path.parent.mkdir(parents=True,exist_ok=True)
    atomic_publish(path,lambda tmp:tmp.write_text(text),path.stat().st_mode&0o777 if path.exists() else 0o600)
def atomic_json(path: Path, data): atomic_write(path, json.dumps(data))
ATTACHMENT_LIMIT=32*1024**2
def _fsync(path):
    fd=os.open(path,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
def durable_replace(tmp,path):
    tmp,path=map(Path,(tmp,path))
    _fsync(tmp)
    os.replace(tmp,path)
    _fsync(path.parent)
def atomic_publish(path,write,mode=0o600):
    path,(fd,tmp)=(path:=Path(path)),tempfile.mkstemp(prefix=f".{path.name}.",dir=path.parent)
    os.close(fd)
    try: (write(Path(tmp)),os.chmod(tmp,mode),durable_replace(tmp,path))
    finally: Path(tmp).unlink(missing_ok=True)
def attachment_body(data,root=None):
    if len(data)>ATTACHMENT_LIMIT: return None
    root=secure_dir(Path(root or DATA_DIR)/"attachments")
    blob,path=(blob:=hashlib.sha256(data).hexdigest()),root/blob
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=len(data): raise ValueError("attachment body conflicts with content hash")
        with path.open("rb") as source: actual=hashlib.file_digest(source,"sha256").hexdigest()
        return (required(actual==blob,ValueError("attachment body conflicts with content hash")),os.chmod(path,0o600),path)[-1]
    atomic_publish(path,lambda tmp:tmp.write_bytes(data))
    return path

def detect_source(path: Path):
    if path.is_dir(): return "codex" if (path / "sessions").exists() else "claude-code"
    if path.suffix == ".zip" or "chatgpt" in path.name.lower(): return "chatgpt"
    data=required(json.loads(path.read_text()),ValueError(f"Empty export: {path}"))
    return "chatgpt" if "mapping" in data[0] else "claude" if "chat_messages" in data[0] else "chatgpt"

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
CREATE TABLE IF NOT EXISTS provenance.file_edit_evidence(file_edit_id VARCHAR PRIMARY KEY,status VARCHAR NOT NULL CHECK(status IN ('confirmed','invalid','unknown','unverified')),reason VARCHAR NOT NULL,tool_call_id VARCHAR);
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
CREATE TABLE IF NOT EXISTS remote.file_edit_evidence_proofs(workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,revision VARCHAR,source_edit_id VARCHAR,edit_revision VARCHAR,status VARCHAR,reason VARCHAR,source_tool_call_id VARCHAR,tool_revision VARCHAR,proof JSON,PRIMARY KEY(workspace_id,author_user_id,object_id,revision));
CREATE TABLE IF NOT EXISTS remote.semantic_ancestors(object_kind VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,child_revision VARCHAR,ancestor_revision VARCHAR,PRIMARY KEY(object_kind,workspace_id,author_user_id,object_id,child_revision,ancestor_revision));
CREATE TABLE IF NOT EXISTS remote.provenance_origins(kind VARCHAR,physical_entity VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,source_entity VARCHAR,proof_id VARCHAR,PRIMARY KEY(kind,physical_entity,workspace_id,author_user_id));
CREATE TABLE IF NOT EXISTS attachment_bodies(attachment_id VARCHAR PRIMARY KEY,content_hash VARCHAR NOT NULL,size UINTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS embedding_state(singleton BOOLEAN PRIMARY KEY,profile JSON NOT NULL);
CREATE TABLE IF NOT EXISTS core_schema(singleton BOOLEAN PRIMARY KEY,version USMALLINT NOT NULL);
CREATE TABLE IF NOT EXISTS core_migrations(name VARCHAR PRIMARY KEY,state VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS archive_state(singleton BOOLEAN PRIMARY KEY,archive_id UUID NOT NULL,generation UBIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS archive_changes(kind VARCHAR,entity VARCHAR,generation UBIGINT,PRIMARY KEY(kind,entity));
"""
ARCHIVE_COLUMNS={"conversations":["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"],"messages":["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"],"tool_calls":["id","message_id","tool_name","input","output","status","duration_ms","created_at"],"attachments":["id","message_id","filename","mime_type","size","path","url","created_at"],"artifacts":["id","conversation_id","artifact_type","title","content","language","created_at","version"],"file_edits":["id","message_id","file_path","edit_type","content","created_at","old_content"]}
_MSG_UPDATES,_MSG_UPS,PROVENANCE_KINDS=(u:=",".join(f"{c}=excluded.{c}" for c in ARCHIVE_COLUMNS["messages"][1:])+",embedding=excluded.embedding"),f"INSERT INTO messages ({','.join(ARCHIVE_COLUMNS['messages'])},embedding) SELECT x.*,m.embedding FROM (VALUES ({','.join('?'*len(ARCHIVE_COLUMNS['messages']))})) x({','.join(ARCHIVE_COLUMNS['messages'])}) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content ON CONFLICT(id) DO UPDATE SET {u}",{"repository.observed","file.observed","file.version","edit.observed","git.checkpoint","checkpoint.link"}
_PROVENANCE_ROWS={"file.observed":(lambda p,v,m,o:p["id"]==provenance_digest({"repository":p["repository"],"path":p["path"]}),"provenance file identity mismatch","INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",lambda p,v,m,o:(p["id"],p["repository"],p["path"],p["kind"])),"file.version":(lambda p,v,m,o:p["id"]==provenance_digest({"file":p["file"],"content":p["content_hash"]}),"provenance version identity mismatch","INSERT OR IGNORE INTO provenance.file_versions VALUES (?,?,?,?)",lambda p,v,m,o:(p["id"],p["file"],p["content_hash"],o)),"git.checkpoint":(lambda p,v,m,o:p["id"]==provenance_digest({"repository":p["repository"],"head":p["head"],"state":p["state_hash"]}),"provenance checkpoint identity mismatch","INSERT OR IGNORE INTO provenance.git_checkpoints VALUES (?,?,?,?,?,?,?)",lambda p,v,m,o:(p["id"],p["repository"],p["head"],p["state_hash"],json.dumps(p["paths"]),o,p["capture_source"])),"checkpoint.link":(lambda p,v,m,o:v["entity"]==provenance_digest({"checkpoint":p["checkpoint"],"edit":p["edit"]}),"provenance checkpoint link mismatch","INSERT OR IGNORE INTO provenance.checkpoint_edits VALUES (?,?,?)",lambda p,v,m,o:(p["checkpoint"],m("file_edits",p["edit"]),p["evidence"]))}
def provenance_digest(v): return hashlib.sha256(v if isinstance(v,bytes) else json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()).hexdigest()
def remote_id(author,table,source): return provenance_digest(f"{author}:{table}:{source}")[:16] if source is not None else None
def _archive_touch(db,rows=()):
    generation=(db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone() or [0])[0]
    if rows: _insert_pages(db,"archive_changes",[(kind,entity,generation) for kind,entity in rows],mode=" OR REPLACE")
    return generation
def archive_changes(db,since): return db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],db.execute("SELECT kind,entity FROM archive_changes WHERE generation>?",(since,)).fetchall()
def archive_state(db):
    (archive_id,generation),local=db.execute("SELECT archive_id::VARCHAR,generation FROM archive_state WHERE singleton").fetchone(),sum(db.execute(f"SELECT COUNT(*) FROM {table} x WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name=? AND o.physical_row_id=x.id)",(table,)).fetchone()[0] for table in ARCHIVE_COLUMNS)
    return archive_id,generation,local
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
    p,port=(p:=__import__("urllib.parse").parse.urlparse(re.sub(r"^(?:[^/@]+@)?([^:/]+):",r"ssh://\1/",url) if "://" not in url else url)),p.port
    return f"https://{p.hostname.lower()}{f':{port}' if port and port!={'ssh':22,'https':443,'http':80}.get(p.scheme.lower()) else ''}/{p.path.strip('/').removesuffix('.git')}" if p.hostname and p.path else None
def _git_remotes(root): return sorted({remote for line in _git_run(root,"remote","-v").decode(errors="replace").splitlines() if "\t" in line and not (url:=line.split("\t",1)[1].rsplit(" ",1)[0]).startswith(("/","file:")) and (remote:=_remote(url))})
def repository_evidence(value): return provenance_digest({"lineage":value["lineage"],"remotes":value["remotes"]}) if value["lineage"] else None
def _checkout(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}" if (stat:=next((p.stat() for p in [Path(root)/'.git'] if p.exists()),None)) else str(root))[:32]
def _unborn(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}:{getattr(stat,'st_birthtime_ns',stat.st_ctime_ns)}" if (stat:=next((p.stat() for p in [Path(root)/'.git/config',Path(root)/'.git'] if p.exists()),None)) else str(root))
def _git_marker(path):
    p=p if (p:=Path(path)).is_dir() else p.parent
    return next(((str(root.resolve()),_checkout(root)) for root in (p,*p.parents) if (root/".git").exists()),(None,None))
@lru_cache(maxsize=256)
def _repository(root): return (lambda root,roots,remotes,lineage:dict(id=provenance_digest({"lineage":lineage,"remotes":remotes}) if lineage else _unborn(root),lineage=lineage,root=str(root),roots=roots,remotes=remotes,checkout=_checkout(root)))(root:=Path(root),roots:=sorted(_git_maybe(root,"rev-list","--max-parents=0","HEAD").decode().split()),_git_remotes(root),provenance_digest({"git_roots":roots}) if roots else None)
def repository_state(db): return {"roots":dict(db.execute("SELECT root,repository FROM provenance.repository_checkouts").fetchall()),"checkouts":dict(db.execute("SELECT id,repository FROM provenance.repository_checkouts").fetchall()),"checkout_roots":dict(db.execute("SELECT id,root FROM provenance.repository_checkouts").fetchall()),"lineages":dict(db.execute("SELECT id,lineage FROM provenance.repositories").fetchall()),"aliases":dict(db.execute("SELECT evidence,CASE WHEN COUNT(DISTINCT repository)=1 THEN MIN(repository) END FROM provenance.repository_aliases GROUP BY evidence").fetchall())}
def _refresh_repository(): (_git_root.cache_clear(),_repository.cache_clear())
def repository(path,known=None,refresh=True): return (lambda value,state,evidence,bound,resolved:{**value,"id":resolved or value["id"],"alias":None if resolved or not evidence else evidence})(value:={**_repository(str(root)),"head":_git_maybe(root,"rev-parse","--verify","HEAD").decode().strip(),"branch":_git_maybe(root,"symbolic-ref","--short","HEAD").decode().strip()},state:=repository_state(known) if known is not None and hasattr(known,"execute") else known or {"roots":{},"checkouts":{},"checkout_roots":{},"lineages":{},"aliases":{}},evidence:=repository_evidence(value),bound:=state["checkouts"].get(value["checkout"]),bound if value["lineage"] and state["lineages"].get(bound)==value["lineage"] else evidence and state["aliases"].get(evidence)) if (not refresh or _refresh_repository() is None) and (root:=_git_root(Path(path))) else None
def _cached_repository(cache,root,known): return cache[key] if (key:=str(root)) in cache else cache.setdefault(key,repository(root,known,False))
def _observe_checkout(db,repo):
    db.execute("DELETE FROM provenance.repository_checkouts WHERE root=? AND id<>?",(repo["root"],repo["checkout"]))
    db.execute("INSERT INTO provenance.repository_checkouts VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET repository=excluded.repository,root=excluded.root,branch=excluded.branch,head=excluded.head",(repo["checkout"],repo["id"],repo["root"],repo["branch"],repo["head"]))
    if repo["alias"]: db.execute("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",(repo["id"],repo["alias"]))
def capture_repository(path,db_path=None):
    with _core(db_path,ready=True) as db: known=repository_state(db)
    repo=repository(path,known)
    if not repo: return None
    record=_provenance_record("repository.observed",repo["id"],{k:repo[k] for k in ("id","lineage","roots","remotes","head")},datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    with _core(db_path) as db,_transaction(db):
        _observe_checkout(db,repo)
        project_provenance(db,record)
        db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",(record["kind"],record["entity"]))
    return repo
def _resolved(path,cwd=None): return str((Path(cwd)/p if not (p:=Path(path)).is_absolute() and cwd else p).expanduser().resolve())
def pending_scopes(conversations):
    return [(conversation,resolved,None,root,f"pending:{checkout}" if checkout else None,captured) for captured in [datetime.now(timezone.utc)] for conversation,cwd in conversations for resolved in [_resolved(cwd) if cwd else None] for root,checkout in [_git_marker(resolved) if resolved else (None,None)]]
def snapshot_scopes(conversations,known=None):
    _refresh_repository()
    return [(conversation,resolved,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,captured) for captured in [datetime.now(timezone.utc).isoformat().replace("+00:00","Z")] for conversation,cwd in conversations for resolved in [str(Path(cwd).expanduser().resolve()) if cwd else None] for repo in [repository(resolved,known,False) if resolved else None]]
def _provenance_where(path,cwd,cache,known=None,frozen=False):
    p,repo=(p:=Path(path) if frozen else Path(_resolved(path,cwd))),_cached_repository(cache,root,known) if (root:=_git_root(p)) else None
    return (repo,p.relative_to(repo["root"]).as_posix(),"repository") if repo else (None,f"external/{provenance_digest(str(p))[:24]}/{p.name}","external")
def pending_edit_scopes(edits):
    return [(e["id"],f"external/{provenance_digest(route)[:24]}/{Path(route).name}",None,root,f"pending:{checkout}" if checkout else None,route,captured) for captured in [datetime.now(timezone.utc)] for e in edits for route in [_resolved(e["path"],e["cwd"])] for root,checkout in [_git_marker(route)]]
def snapshot_edit_scopes(edits,known=None):
    _refresh_repository()
    captured,cache=datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),{}
    return [(e["id"],path,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,route,captured) for e in edits for route in [e.get("route") or _resolved(e["path"],e["cwd"])] for repo,path,_ in [_provenance_where(route,None,cache,known,True)]]
def edit_scope_inputs(result):
    conversations,messages={c["id"]:c["cwd"] for c in result.convs},{m["id"]:m["conversation_id"] for m in result.msgs}
    return [{"id":e["id"],"path":e["file_path"],"cwd":conversations.get(messages.get(e["message_id"]))} for e in result.edits]
def _checkpoint(repo,source): return (lambda paths,state:dict(id=provenance_digest({"repository":repo["id"],"head":repo["head"],"state":state}),repository=repo["id"],head=repo["head"],state_hash=state,paths=paths,capture_source=source))(sorted({x[3:].split(" -> ")[-1] for x in _git_run(repo["root"],"status","--porcelain=v1","-z").decode(errors="replace").split("\0") if len(x)>3}),provenance_digest((_git_maybe(repo["root"],"diff","--binary","HEAD")+_git_maybe(repo["root"],"diff","--binary","--cached","HEAD")) if repo["head"] else _git_run(repo["root"],"status","--porcelain=v1","-z")))
def _provenance_record(kind,entity,payload,observed_at): return dict(kind=kind,entity=entity,payload=payload,observed_at=observed_at)
def _repository_record(repo,observed): return _provenance_record("repository.observed",repo["id"],{k:repo[k] for k in ("id","lineage","roots","remotes","head")},observed)
def _checkpoint_records(repo,versions,source,observed):
    cp=_checkpoint(repo,source)
    return [_provenance_record("git.checkpoint",cp["id"],cp,observed),*[_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":full},observed) for (rid,fid),(edit,after,full,path) in versions.items() if rid==repo["id"] for vid in [provenance_digest({"file":fid,"content":full})]],*[_provenance_record("checkpoint.link",provenance_digest({"checkpoint":cp["id"],"edit":edit}),{"checkpoint":cp["id"],"edit":edit,"evidence":"full_content_match"},observed) for (rid,fid),(edit,after,full,path) in versions.items() if rid==repo["id"] and path not in cp["paths"] and after==full]]
def _provenance_edits(core,edit_ids=None):
    ids,selected,sql=(ids:=sorted(set(edit_ids or ())) if edit_ids is not None else None),(selected:=" AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_edit_id=fe.id)"+(f" AND fe.id IN ({','.join('?'*len(ids))})" if ids else " AND FALSE" if ids==[] else "")),"""SELECT fe.id,fe.file_path,fe.edit_type,fe.content,fe.old_content,CAST(fe.created_at AS VARCHAR),m.id,m.conversation_id,c.cwd,s.path,s.repository,s.root,s.checkout,s.route,CAST(s.observed_at AS VARCHAR) FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id LEFT JOIN provenance.file_edit_scopes s ON s.file_edit_id=fe.id WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id)"""+selected+" ORDER BY fe.created_at,fe.id"
    return [dict(zip(("id","path","type","content","old","ts","turn","conversation","cwd","scope_path","repository","root","checkout","route","scope_at"),r)) for r in core.execute(sql,ids or ()).fetchall()]
def _observe_provenance(edits,source="sync",known=None,conversations=(),cache=None):
    cache,captured,records,repos,repo_times,versions,fulls={} if cache is None and _refresh_repository() is None else cache,datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),[],{},{},{},{}
    for e in edits:
        rid,path,repo=e["repository"],e["scope_path"],_cached_repository(cache,e["root"],known) if e["root"] else None
        repo,kind,fid=repo if repo and (repo["id"],repo["checkout"])==(rid,e["checkout"]) else None,"repository" if rid else "external",provenance_digest({"repository":rid,"path":path})
        if repo: (repos.setdefault(rid,repo),repo_times.setdefault(rid,captured))
        target,key=(target:=Path(repo["root"],path) if repo else None),str(target) if target else None
        full=fulls[key] if key in fulls else fulls.setdefault(key,provenance_digest(target.read_bytes()) if target and target.is_file() else None)
        records += [_provenance_record("file.observed",fid,{"id":fid,"repository":rid,"path":path,"kind":kind},captured),_provenance_record("edit.observed",e["id"],{"id":e["id"],"turn":e["turn"],"file":fid,"repository":rid,"old_content_hash":provenance_digest((e["old"] or "").encode()) if e["old"] is not None else None,"new_content_hash":provenance_digest((e["content"] or "").encode()),"evidence":"captured_exact" if e["type"]=="write" or e["old"] is not None else "content_unknown"},captured)]
        if full: versions[(rid,fid)]=(e["id"],provenance_digest((e["content"] or "").encode()),full,path)
    for conversation,cwd,rid,root,checkout,observed in conversations:
        repo=_cached_repository(cache,root,known) if root else None
        if repo and (repo["id"],repo["checkout"])==(rid,checkout): (repos.setdefault(rid,repo),repo_times.setdefault(rid,observed))
    records[:0]=[_repository_record(repo,repo_times[rid]) for rid,repo in repos.items()]
    records.extend(record for repo in repos.values() for record in _checkpoint_records(repo,versions,source,captured))
    return records,repos
def observe_provenance(core): return _observe_provenance(_provenance_edits(core),known=repository_state(core))[0]
def provenance_records(db,only=None):
    ids=lambda kind:[entity for k,entity in only or () if k==kind]
    rows=lambda kind,sql,column="id":[] if only is not None and not ids(kind) else db.execute(sql+(f" WHERE {column} IN (SELECT UNNEST(?))" if only is not None else ""),[ids(kind)] if only is not None else []).fetchall()
    return [*[_provenance_record("repository.observed",r[0],dict(id=r[0],lineage=r[1],roots=json.loads(r[2]),remotes=json.loads(r[3]),head=r[4]),r[5]) for r in rows("repository.observed","SELECT id,lineage,CAST(roots AS VARCHAR),CAST(remotes AS VARCHAR),last_head,observed_at FROM provenance.repositories")],*[_provenance_record("file.observed",r[0],dict(zip(("id","repository","path","kind"),r)),None) for r in rows("file.observed","SELECT * FROM provenance.files")],*[_provenance_record("file.version",r[0],dict(zip(("id","file","content_hash"),r[:3])),r[3]) for r in rows("file.version","SELECT * FROM provenance.file_versions")],*[_provenance_record("edit.observed",r[0],dict(zip(("id","turn","file","repository","old_content_hash","new_content_hash","evidence"),r)),None) for r in rows("edit.observed","SELECT x.file_edit_id,fe.message_id,x.file_id,f.repository,x.old_content_hash,x.new_content_hash,x.evidence FROM provenance.file_edit_files x LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id JOIN file_edits fe ON fe.id=x.file_edit_id JOIN provenance.files f ON f.id=x.file_id AND (v.status='confirmed' OR EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.kind='edit.observed' AND o.physical_entity=x.file_edit_id))","x.file_edit_id")],*[_provenance_record("git.checkpoint",r[0],dict(id=r[0],repository=r[1],head=r[2],state_hash=r[3],paths=json.loads(r[4]),capture_source=r[6]),r[5]) for r in rows("git.checkpoint","SELECT id,repository,head,state_hash,CAST(paths AS VARCHAR),observed_at,capture_source FROM provenance.git_checkpoints")],*[_provenance_record("checkpoint.link",entity,dict(zip(("checkpoint","edit","evidence"),r)),None) for r in db.execute("SELECT * FROM provenance.checkpoint_edits").fetchall() for entity in [provenance_digest({"checkpoint":r[0],"edit":r[1]})] if only is None or entity in ids("checkpoint.link")]]
def project_provenance(db,value,map_id=lambda table,value:value,touch=True):
    p,k,observed=value["payload"],value["kind"],value["observed_at"]
    if k not in PROVENANCE_KINDS: return False
    if k!="checkpoint.link" and p["id"]!=value["entity"]: raise ValueError("provenance entity mismatch")
    if k=="repository.observed":
        if (old:=db.execute("SELECT lineage FROM provenance.repositories WHERE id=?",(p["id"],)).fetchone()) and old[0] and p["lineage"] and old[0]!=p["lineage"]: raise ValueError("repository lineage conflict")
        db.execute("INSERT INTO provenance.repositories VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lineage=excluded.lineage,roots=excluded.roots,remotes=excluded.remotes,last_head=COALESCE(excluded.last_head,repositories.last_head),observed_at=COALESCE(excluded.observed_at,repositories.observed_at)",(p["id"],p["lineage"],json.dumps(p["roots"]),json.dumps(p["remotes"]),p.get("head"),observed))
    elif k=="edit.observed":
        edit,turn=map_id("file_edits",p["id"]),map_id("messages",p["turn"])
        if not (row:=db.execute("SELECT message_id FROM file_edits WHERE id=?",(edit,)).fetchone()) or row[0]!=turn: raise ValueError("provenance edit/turn mismatch")
        if (old:=db.execute("SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id=?",(edit,)).fetchone()) and old[0]!=p["file"]: raise ValueError("provenance edit scope conflict")
        db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,?,?,?) ON CONFLICT(file_edit_id) DO UPDATE SET old_content_hash=excluded.old_content_hash,new_content_hash=excluded.new_content_hash,evidence=excluded.evidence",(edit,p["file"],p["old_content_hash"],p["new_content_hash"],p["evidence"]))
    else:
        check,error,sql,args=_PROVENANCE_ROWS[k]
        required(check(p,value,map_id,observed),ValueError(error))
        db.execute(sql,args(p,value,map_id,observed))
    return bool(_archive_touch(db,[(k,value["entity"])])) if touch else True
def capture_provenance(path=None,edit_ids=None,conversation_ids=None,source="sync"):
    targeted,eids,cids=edit_ids is not None or conversation_ids is not None,sorted(set(edit_ids or ())),sorted(set(conversation_ids or ()))
    with _core(path,True) as core:
        if eids: cids=sorted(set(cids)|{r[0] for r in core.execute("SELECT DISTINCT m.conversation_id FROM file_edits fe JOIN messages m ON m.id=fe.message_id WHERE fe.id IN (SELECT UNNEST(?))",[eids]).fetchall()})
        missing,missing_edits=core.execute("SELECT c.id,c.cwd FROM conversations c WHERE c.cwd IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) AND NOT EXISTS (SELECT 1 FROM provenance.conversation_scopes s WHERE s.conversation=c.id)"+(" AND c.id IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall(),[dict(id=r[0],path=r[1],cwd=r[2]) for r in core.execute("SELECT fe.id,fe.file_path,c.cwd FROM file_edits fe JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_scopes s WHERE s.file_edit_id=fe.id)"+(" AND fe.id IN (SELECT UNNEST(?))" if targeted else ""),[eids] if targeted else []).fetchall()]
    scopes,edit_scopes=pending_scopes(missing),pending_edit_scopes(missing_edits)
    if scopes or edit_scopes:
        with _core(path) as core,_transaction(core):
            (_insert_pages(core,"provenance.conversation_scopes",scopes,mode=" OR IGNORE"),_insert_pages(core,"provenance.file_edit_scopes",edit_scopes,("file_edit_id","path","repository","root","checkout","route","observed_at"),mode=" OR IGNORE"))
    with _core(path,True) as core: edits,known,conversations,files=_provenance_edits(core,edit_ids),repository_state(core),core.execute("SELECT s.conversation,s.cwd,s.repository,s.root,s.checkout,CAST(s.observed_at AS VARCHAR) FROM provenance.conversation_scopes s WHERE (s.checkout LIKE 'pending:%' OR s.repository IS NOT NULL AND NOT EXISTS (SELECT 1 FROM provenance.repository_checkouts c WHERE (c.id,c.repository,c.root)=(s.checkout,s.repository,s.root)))"+(" AND s.conversation IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall(),[] if targeted else core.execute("SELECT f.id,f.repository,f.path,c.root,c.id FROM provenance.files f JOIN provenance.repository_checkouts c ON c.repository=f.repository").fetchall()
    _refresh_repository()
    cache,scopes,conversations=(cache:={}), (scopes:=[(conversation,cwd,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,observed) for conversation,cwd,rid,root,checkout,observed in conversations if checkout and checkout.startswith("pending:") for marker in [checkout.removeprefix("pending:")] for repo in [_cached_repository(cache,root,known) if (root,marker)==_git_marker(cwd) else None]]),[{r[0]:r for r in scopes}.get(row[0],row) for row in conversations]
    edit_scopes,frozen,edits,(records,repos,captured)=(edit_scopes:=[(e["id"],relative,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,e["route"],e["scope_at"]) for e in edits if e["checkout"] and e["checkout"].startswith("pending:") for marker in [e["checkout"].removeprefix("pending:")] for repo,relative,_ in [_provenance_where(e["route"],None,cache,known,True) if (e["root"],marker)==_git_marker(e["route"]) else (None,e["scope_path"],"external")]]),(frozen:={r[0]:r for r in edit_scopes}),(edits:=[{**e,**dict(zip(("scope_path","repository","root","checkout","route","scope_at"),frozen[e["id"]][1:]))} if e["id"] in frozen else e for e in edits]),(*_observe_provenance(edits,source,known,conversations,cache),datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    repos.update({rid:repo for root,rid in (() if targeted else known["roots"].items()) if (repo:=_cached_repository(cache,root,known)) and repo["id"]==rid})
    records += [record for rid,repo in repos.items() if not any(r["kind"]=="git.checkpoint" and r["payload"]["repository"]==rid for r in records) for record in _checkpoint_records(repo,{},source,captured)]
    records += [_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":content},captured) for fid,rid,relative,root,checkout in files for target,repo in [(Path(root,relative),_cached_repository(cache,root,known))] if repo and (repo["id"],repo["checkout"])==(rid,checkout) and target.is_file() for content in [provenance_digest(target.read_bytes())] for vid in [provenance_digest({"file":fid,"content":content})]]
    stale,touched=[] if targeted else [root for root,rid in known["roots"].items() if not (repo:=_cached_repository(cache,root,known)) or repo["id"]!=rid],sorted({e["conversation"] for e in edits})
    with _core(path) as core,_transaction(core):
        if stale: core.execute("DELETE FROM provenance.repository_checkouts WHERE root IN (SELECT UNNEST(?))",[stale])
        if scopes: core.executemany("UPDATE provenance.conversation_scopes SET cwd=?,repository=?,root=?,checkout=?,observed_at=? WHERE conversation=? AND checkout LIKE 'pending:%'",[(cwd,rid,root,checkout,observed,conversation) for conversation,cwd,rid,root,checkout,observed in scopes])
        if edit_scopes: core.executemany("UPDATE provenance.file_edit_scopes SET path=?,repository=?,root=?,checkout=?,route=?,observed_at=? WHERE file_edit_id=? AND checkout LIKE 'pending:%'",[(relative,rid,root,checkout,route,observed,edit) for edit,relative,rid,root,checkout,route,observed in edit_scopes])
        for repo in repos.values(): _observe_checkout(core,repo)
        for record in records: project_provenance(core,record)
        if records: core.executemany("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",[(r["kind"],r["entity"]) for r in records])
        if scopes or touched: _archive_touch(core,[("conversations",r[0]) for r in scopes]+[("conversations",c) for c in touched])
    return records
def project_archive_row(db,table,columns,values,origin=None,touch=True):
    project_archive_rows(db,table,columns,[(values,origin)])
    if touch: _archive_touch(db,[(table,values[0])])
def _insert_pages(db,target,rows,columns=None,conflict="",mode="",embedding=False):
    schema,columns,shape,norm,extra,source,pages=(schema:={r[0]:r[1] for r in db.execute(f"DESCRIBE {target}").fetchall()}),(columns:=columns or list(schema)),(shape:=json.dumps([{c:schema[c] for c in columns}])),(norm:=lambda c,v:json.loads(v) if schema[c]=="JSON" and isinstance(v,str) else v),",embedding" if embedding else "","SELECT x.*,m.embedding FROM UNNEST(from_json(?,?)) t(x) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content" if embedding else "SELECT x.* FROM UNNEST(from_json(?,?)) t(x)",[(json.dumps([{c:norm(c,v) for c,v in zip(columns,row)} for row in rows[i:i+500]],default=str,ensure_ascii=True,allow_nan=False),shape) for i in range(0,len(rows),500)]
    if pages: db.executemany(f"INSERT{mode} INTO {target} ({','.join(columns)}{extra}) {source}{conflict}",pages)
def project_archive_rows(db,table,columns,rows):
    fields,values,origins,updates=(fields:=("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at")),[r[0] for r in rows],[(table,v[0],*(o[k] for k in fields),o.get("proof_id")) for v,o in rows if o],_MSG_UPDATES if table=="messages" else ','.join(f"{c}=excluded.{c}" for c in columns[1:])
    required(table in ARCHIVE_COLUMNS and columns==ARCHIVE_COLUMNS[table] and not any(len(v)!=len(columns) or o and set(o) not in (set(fields),set(fields)|{"proof_id"}) for v,o in rows),ValueError("record schema/entity mismatch"))
    _insert_pages(db,table,values,columns,f" ON CONFLICT(id) DO UPDATE SET {updates}",embedding=table=="messages")
    if table=="file_edits":
        for mode,foreign in ((" OR REPLACE",True),(" OR IGNORE",False)): _insert_pages(db,"provenance.file_edit_evidence",[(v[0],"unverified","signed_replica_missing_evidence",None) for v,o in rows if bool(o)==foreign],mode=mode)
    if origins: _insert_pages(db,"remote.row_origins",origins,mode=" OR REPLACE")
    if table in ("file_edits","tool_calls"): _apply_signed_edit_evidence(db)
def project_row_proofs(db,proofs,root_public,certificate):
    if not proofs: return []
    fields,expected,signer,packed,columns=(fields:=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")),{"v","kind",*fields},(proofs[0]["author_user_id"],proofs[0]["author_device_id"]),json.dumps(certificate,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False),("workspace_id","authorization_workspace_id","row_kind","source_row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    if any(set(proof)!=expected or proof["v"]!=1 or proof["kind"]!="row.proof" or (proof["author_user_id"],proof["author_device_id"])!=signer for proof in proofs) or set(certificate)!={"v","user","device","issued_at","signature"} or certificate["v"]!=1 or set(certificate["device"])!={"id","name","sign_public","box_public"} or (certificate["user"],certificate["device"]["id"])!=signer: raise ValueError("row proof storage schema mismatch")
    if (old:=db.execute("SELECT root_public,CAST(certificate AS VARCHAR) FROM remote.row_signers WHERE author_user_id=? AND author_device_id=?",signer).fetchone()) and (old[0]!=root_public or json.loads(old[1])["device"]!=certificate["device"]): raise ValueError("row signer conflict")
    ids=[provenance_digest(proof) for proof in proofs]
    db.execute("INSERT OR IGNORE INTO remote.row_signers VALUES (?,?,?,?)",(*signer,root_public,packed))
    _insert_pages(db,"remote.row_proofs",[(pid,*(proof[k] for k in fields)) for pid,proof in zip(ids,proofs)],("id",*columns),mode=" OR IGNORE")
    return ids
def project_row_proof(db,proof,root_public,certificate): return project_row_proofs(db,[proof],root_public,certificate)[0]
def _project_semantic_ancestors(db,kind,record):
    _insert_pages(db,"remote.semantic_ancestors",[(kind,record["workspace_id"],record["author_user_id"],record["object_id"],record["revision"],a) for a in record["proof"]["ancestors"]],mode=" OR IGNORE")
def project_provider_alias(db,record):
    fields=("workspace_id","author_user_id","object_id","revision","source","session_id","members","canonical_source_row_id","proof")
    required(set(record)==set(fields) and isinstance(p:=record["proof"],dict) and (p.get("workspace"),p.get("author_user_id"),p.get("object_id"),p.get("revision"))==tuple(record[k] for k in fields[:4]) and isinstance(p.get("ancestors"),list) and record["object_id"]=="provider-session:"+provenance_digest([record["source"],record["session_id"]]) and isinstance(record["members"],list) and record["members"] and all(isinstance(x,str) and x for x in record["members"]) and record["members"]==sorted(set(record["members"])) and record["canonical_source_row_id"]==record["members"][0],ValueError("provider alias storage schema mismatch"))
    _insert_pages(db,"remote.provider_session_aliases",[tuple(record[f] for f in fields)],fields,mode=" OR IGNORE")
    _project_semantic_ancestors(db,"provider.session",record)
def _apply_signed_edit_evidence(db):
    db.execute("""CREATE OR REPLACE TEMP TABLE core_signed_edit_evidence AS WITH leaves AS (SELECT p.*,count(*) OVER (PARTITION BY workspace_id,author_user_id,object_id) n FROM remote.file_edit_evidence_proofs p WHERE NOT EXISTS (SELECT 1 FROM remote.semantic_ancestors a WHERE a.object_kind='file-edit.evidence' AND (a.workspace_id,a.author_user_id,a.object_id,a.ancestor_revision)=(p.workspace_id,p.author_user_id,p.object_id,p.revision))), foreign_edits AS (SELECT fe.id file_edit_id,p.workspace_id,p.author_user_id,p.source_row_id,p.revision FROM file_edits fe JOIN remote.row_origins o ON o.table_name='file_edits' AND o.physical_row_id=fe.id JOIN remote.row_proofs p ON p.id=o.proof_id AND p.state='active'), matches AS (SELECT f.file_edit_id,s.revision semantic_revision,s.n,s.status,s.reason,tc.id tool_call_id FROM foreign_edits f JOIN leaves s ON (s.workspace_id,s.author_user_id,s.source_edit_id,s.edit_revision)=(f.workspace_id,f.author_user_id,f.source_row_id,f.revision) LEFT JOIN remote.row_proofs tp ON (tp.workspace_id,tp.author_user_id,tp.row_kind,tp.source_row_id,tp.revision,tp.state)=(s.workspace_id,s.author_user_id,'tool_calls',s.source_tool_call_id,s.tool_revision,'active') LEFT JOIN remote.row_origins tor ON tor.table_name='tool_calls' AND tor.proof_id=tp.id LEFT JOIN tool_calls tc ON tc.id=tor.physical_row_id WHERE s.source_tool_call_id IS NULL OR tc.id IS NOT NULL) SELECT f.file_edit_id,CASE WHEN max(m.n)>1 THEN 'unverified' WHEN count(m.semantic_revision)=1 THEN max(m.status) ELSE 'unverified' END status,CASE WHEN max(m.n)>1 THEN 'signed_evidence_conflict' WHEN count(m.semantic_revision)=1 THEN max(m.reason) ELSE 'signed_replica_missing_evidence' END reason,CASE WHEN max(m.n)=1 AND count(m.semantic_revision)=1 THEN max(m.tool_call_id) END tool_call_id FROM foreign_edits f LEFT JOIN matches m USING(file_edit_id) GROUP BY f.file_edit_id""")
    touched=db.execute("SELECT x.file_edit_id FROM core_signed_edit_evidence x LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id WHERE (v.status,v.reason,v.tool_call_id) IS DISTINCT FROM (x.status,x.reason,x.tool_call_id)").fetchall()
    if touched: (db.execute("INSERT OR REPLACE INTO provenance.file_edit_evidence SELECT * FROM core_signed_edit_evidence"),_archive_touch(db,[("file_edits",r[0]) for r in touched]))
def project_file_edit_evidence(db,record):
    fields=("workspace_id","author_user_id","object_id","revision","source_edit_id","edit_revision","status","reason","source_tool_call_id","tool_revision","proof")
    p=record.get("proof") if isinstance(record,dict) else None
    required(set(record)==set(fields) and isinstance(p,dict) and (p.get("workspace"),p.get("author_user_id"),p.get("object_id"),p.get("revision"))==tuple(record[k] for k in fields[:4]) and isinstance(p.get("ancestors"),list) and record["object_id"]=="file-edit-evidence:"+provenance_digest(record["source_edit_id"]) and record["status"] in {"confirmed","invalid","unknown","unverified"} and isinstance(record["reason"],str) and record["reason"] and isinstance(record["edit_revision"],str) and len(record["edit_revision"])==64 and ((record["source_tool_call_id"] is None and record["tool_revision"] is None) or all(isinstance(record[k],str) and record[k] for k in ("source_tool_call_id","tool_revision"))),ValueError("file edit evidence storage schema mismatch"))
    (_insert_pages(db,"remote.file_edit_evidence_proofs",[tuple(record[f] for f in fields)],fields,mode=" OR IGNORE"),_project_semantic_ancestors(db,"file-edit.evidence",record),_apply_signed_edit_evidence(db))
def project_provider_bindings(db,source,session,conversation,members):
    required(db.execute("SELECT 1 FROM conversations WHERE id=?",(conversation,)).fetchone() and all(isinstance(v,str) and v for v in (source,session,conversation,*members)),ValueError("provider binding target unavailable"))
    if members: db.execute("UPDATE provider_sessions SET conversation_id=? WHERE conversation_id IN (SELECT UNNEST(?))",(conversation,members))
    db.execute("INSERT OR REPLACE INTO provider_sessions VALUES (?,?,?)",(source,session,conversation))
def project_workspace_controls(db,controls):
    for value in controls:
        if not {"workspace","revision","epoch"}<=set(value) or not isinstance(value["revision"],int) or not isinstance(value["epoch"],int): raise ValueError("workspace control storage schema mismatch")
        raw,key,proof,old=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False),(key:=(value["workspace"],value["revision"])),provenance_digest(value),db.execute("SELECT state_hash FROM remote.workspace_controls WHERE workspace_id=? AND revision=?",key).fetchone()
        if old and old[0]!=proof: raise ValueError("workspace control conflict")
        db.execute("INSERT OR IGNORE INTO remote.workspace_controls VALUES (?,?,?,?,?)",(*key,value["epoch"],proof,raw))
    return len(controls)
def _logical_parts(row,proof,proof_id,native,parent_map):
    table,source,mapped,physical,origin=(table:=row["kind"]),(source:=row["id"]),(mapped:=lambda kind,value:(parent_map or {}).get((kind,value),value if native else remote_id(proof["author_user_id"],kind,value))),*((source,None) if native else (remote_id(proof["author_user_id"],table,source),{"workspace_id":proof["workspace"],"author_user_id":proof["author_user_id"],"author_device_id":proof["author_device_id"],"source_row_id":source,"source_event_id":proof["revision"],"content_key":f"{table}:{source}","observed_at":None,"proof_id":proof_id}))
    return table,source,mapped,physical,origin
def _logical_archive(row,proof,proof_id,native=False,parent_map=None):
    table,source,mapped,physical,origin=_logical_parts(row,proof,proof_id,native,parent_map)
    columns,data,parents=ARCHIVE_COLUMNS[table],{"id":source,**row["data"]},dict({"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}.get(table,()))
    return table,physical,[physical if c=="id" else mapped(parents[c],data[c]) if c in parents else json.dumps(data[c],sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False) if c in ("metadata","input","output") and data.get(c) is not None else data.get(c) for c in columns],origin,data
def project_logical_rows(db,items):
    delayed,logical,out=[item for item in items if item[0]["kind"] in PROVENANCE_KINDS or item[0]["state"]=="deleted"],(logical:=[_logical_archive(row,proof,pid,native,maps[0] if maps else None) for row,proof,pid,native,*maps in items if row["kind"] not in PROVENANCE_KINDS and row["state"]!="deleted"]),[(table,physical) for table,physical,values,origin,data in logical]
    for table in ARCHIVE_COLUMNS:
        if rows := [(values,origin) for kind,physical,values,origin,data in logical if kind==table]: project_archive_rows(db,table,ARCHIVE_COLUMNS[table],rows)
    if bodies:=[(physical,data["body_hash"],data["size"]) for table,physical,values,origin,data in logical if table=="attachments" and data["body_hash"]]: _insert_pages(db,"attachment_bodies",bodies,mode=" OR REPLACE")
    out.extend((item[0]["kind"],project_logical_row(db,*item[:4],touch=False,parent_map=item[4] if len(item)>4 else None)) for item in delayed)
    if out: _archive_touch(db,out)
    return out
def project_logical_row(db,row,proof,proof_id,native=False,touch=True,parent_map=None):
    table,source,mapped,physical,origin=_logical_parts(row,proof,proof_id,native,parent_map)
    if table in PROVENANCE_KINDS:
        data,value=(data:={"id":source,**row["data"]}),{"kind":table,"entity":source,"payload":data,"observed_at":data.pop("observed_at",None)}
        _,physical=project_provenance(db,value,mapped,False),mapped("file_edits",source) if table=="edit.observed" else provenance_digest({"checkpoint":data["checkpoint"],"edit":mapped("file_edits",data["edit"])}) if table=="checkpoint.link" else source
        if touch: _archive_touch(db,[(table,physical)])
        db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)" if native else "INSERT OR REPLACE INTO remote.provenance_origins VALUES (?,?,?,?,?,?)",(table,physical) if native else (table,physical,proof["workspace"],proof["author_user_id"],source,proof_id))
        return physical
    if row["state"]=="deleted":
        db.execute(f"DELETE FROM {table} WHERE id=?",(physical,))
        for related in {"attachments":("attachment_bodies WHERE attachment_id",),"file_edits":("provenance.file_edit_evidence WHERE file_edit_id",)}.get(table,()): db.execute(f"DELETE FROM {related}=?",(physical,))
        (origin and db.execute("INSERT OR REPLACE INTO remote.row_origins VALUES (?,?,?,?,?,?,?,?,?,?)",(table,physical,*(origin[k] for k in ("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at","proof_id")))),table in ("file_edits","tool_calls") and _apply_signed_edit_evidence(db),touch and _archive_touch(db,[(table,physical)]))
        return physical
    table,physical,values,origin,data=_logical_archive(row,proof,proof_id,native,parent_map)
    (project_archive_row(db,table,ARCHIVE_COLUMNS[table],values,origin,touch),table=="attachments" and data["body_hash"] and db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(physical,data["body_hash"],data["size"])))
    return physical
def set_attachment_path(db,row_id,path): db.execute("UPDATE attachments SET path=? WHERE id=?",(str(path),row_id))
def index_attachment_body(db,row_id,path,size=None):
    path,actual=(path:=Path(path)),path.stat().st_size if path.is_file() and not path.is_symlink() else -1
    if actual<0 or actual>ATTACHMENT_LIMIT or size is not None and actual!=size: return None
    with path.open("rb") as source: body_hash=hashlib.file_digest(source,"sha256").hexdigest()
    old=db.execute("SELECT content_hash,size FROM attachment_bodies WHERE attachment_id=?",(row_id,)).fetchone()
    if size is None: db.execute("UPDATE attachments SET size=? WHERE id=? AND size IS NULL",(actual,row_id))
    if old!=(body_hash,actual): (db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(row_id,body_hash,actual)),_archive_touch(db,[("attachments",row_id)]))
    return body_hash
def project_attachment_body(db_path,data,body_hash):
    required(provenance_digest(data)==body_hash,ValueError("attachment body hash mismatch"))
    path=attachment_body(data,Path(db_path).parent)
    with _core(db_path) as db:
        rows=db.execute("SELECT b.attachment_id,a.path FROM attachment_bodies b JOIN attachments a ON a.id=b.attachment_id WHERE b.content_hash=?",(body_hash,)).fetchall()
        if rows and path and (ids:=[row_id for row_id,old in rows if old!=str(path)]):
            with _transaction(db): db.executemany("UPDATE attachments SET path=? WHERE id=?",[(str(path),row_id) for row_id in ids])
        return len(rows)
def _backup_copy(source,target):
    command=("cp","-c",str(source),str(target)) if sys.platform=="darwin" else ("cp","--reflink=auto",str(source),str(target)) if sys.platform.startswith("linux") else None
    if not command or subprocess.run(command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode: shutil.copyfile(source,target)
def _file_sha256(path):
    with Path(path).open("rb") as source: return hashlib.file_digest(source,"sha256").hexdigest()
def _check_archive(path):
    with contextlib.closing(duckdb.connect(str(path),read_only=True)) as db: db.execute("SELECT COUNT(*) FROM conversations").fetchone()
def _migration_backup(conn,version=1):
    tables,current=(tables:={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}),(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    if "conversations" not in tables or isinstance(version,int) and current>=version: return None
    path,label,backup=(path:=Path(next((r[2] for r in conn.execute("PRAGMA database_list").fetchall() if r[2]),""))),(label:=f"v{version}" if isinstance(version,int) else version),path.with_name(f"{path.name}.pre-{label}.bak") if path.name else None
    required(isinstance(label,str) and label and all(c.isalnum() or c in "-_" for c in label),ValueError("invalid backup label"))
    if not backup: return None
    conn.execute("CHECKPOINT")
    if backup.exists():
        if backup.is_symlink() or not backup.is_file(): raise ValueError("core migration backup path is unsafe")
        _,source=_check_archive(backup),(source:=_file_sha256(path))
        if source==_file_sha256(backup): return backup
        backup=backup.with_name(f"{backup.name}.{source[:12]}")
    return (atomic_publish(backup,lambda tmp:(_backup_copy(path,tmp),_check_archive(tmp))),backup)[-1]
def _repository_alias_migration(conn):
    _refresh_repository()
    ambiguous={rid for rid, in conn.execute("SELECT DISTINCT repository FROM provenance.repository_checkouts").fetchall() if len({repository_evidence(value) for root, in conn.execute("SELECT root FROM provenance.repository_checkouts WHERE repository=?",(rid,)).fetchall() if (git_root:=_git_root(root)) and (value:=_repository(str(git_root)))["lineage"]})>1}
    return [(rid,repository_evidence({"lineage":lineage,"remotes":json.loads(remotes)})) for rid,lineage,remotes in conn.execute("SELECT id,lineage,CAST(remotes AS VARCHAR) FROM provenance.repositories WHERE lineage IS NOT NULL").fetchall() if rid not in ambiguous],ambiguous
def session_bindings(conn): return {(source,session):cid for source,session,cid in conn.execute("SELECT source,session_id,conversation_id FROM provider_sessions").fetchall()}
def _schema_migrate(conn,version,fn):
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<version:
        with _transaction(conn): fn()
def init_schema(conn):
    tables,current=(tables:={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}),(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    scope,_=remote_id_migration_scope(conn,remote_id) if current<2 else set(),_migration_backup(conn)
    current==1 and scope and ("core_migrations" not in tables or not conn.execute("SELECT 1 FROM core_migrations WHERE name='remote_ids'").fetchone()) and _migration_backup(conn,2)
    v3_backup=_migration_backup(conn,3) if current==2 else None
    if current==2 and (foreign:=[r[0] for r in conn.execute("SELECT x.file_edit_id FROM (SELECT file_edit_id FROM provenance.file_edit_files GROUP BY file_edit_id HAVING count(*)>1) x LEFT JOIN provenance.local_facts l ON (l.kind,l.entity)=('edit.observed',x.file_edit_id) WHERE l.entity IS NULL OR EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE (o.kind,o.physical_entity)=('edit.observed',x.file_edit_id))").fetchall()]): raise ValueError(f"v3 migration refused to rewrite ambiguous foreign signed edit {foreign[0]}; archive preserved and backed up at {v3_backup}")
    3<=current<8 and _migration_backup(conn,current+1)
    conn.execute("""CREATE TABLE IF NOT EXISTS conversations (
        id VARCHAR PRIMARY KEY, source VARCHAR NOT NULL, title VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP,
        model VARCHAR, cwd VARCHAR, git_branch VARCHAR, project_id VARCHAR, metadata JSON);
        CREATE TABLE IF NOT EXISTS messages (
        id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, role VARCHAR NOT NULL, content VARCHAR,
        thinking VARCHAR, created_at TIMESTAMP, model VARCHAR, metadata JSON, embedding FLOAT[]);
        CREATE TABLE IF NOT EXISTS tool_calls (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, tool_name VARCHAR, input JSON, output JSON,
        status VARCHAR, duration_ms INTEGER, created_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS attachments (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, filename VARCHAR, mime_type VARCHAR,
        size INTEGER, path VARCHAR, url VARCHAR, created_at TIMESTAMP);
        CREATE TABLE IF NOT EXISTS artifacts (
        id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, artifact_type VARCHAR, title VARCHAR,
        content TEXT, language VARCHAR, created_at TIMESTAMP, version INTEGER);
        CREATE TABLE IF NOT EXISTS file_edits (
        id VARCHAR PRIMARY KEY, message_id VARCHAR NOT NULL, file_path VARCHAR, edit_type VARCHAR,
        content TEXT, created_at TIMESTAMP)""")
    if not conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone(): conn.execute("ALTER TABLE messages ADD COLUMN embedding FLOAT[]")
    conn.execute("ALTER TABLE file_edits ADD COLUMN IF NOT EXISTS old_content TEXT; ALTER TABLE messages ADD COLUMN IF NOT EXISTS parent_id VARCHAR")  # ALTER keeps fresh and migrated column order identical
    local_facts=bool(conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='provenance' AND table_name='local_facts'").fetchone())
    conn.execute(_PROVENANCE_SCHEMA)
    with _transaction(conn):
        cols={r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='provenance' AND table_name='file_edit_files'").fetchall()}
        if renames:=[f"ALTER TABLE provenance.file_edit_files RENAME COLUMN {old} TO {new}" for old,new in (("before_hash","old_content_hash"),("after_hash","new_content_hash")) if old in cols and new not in cols]: conn.execute(";".join(renames))
        conn.execute("ALTER TABLE provenance.conversation_scopes ADD COLUMN IF NOT EXISTS checkout VARCHAR; ALTER TABLE provenance.file_edit_scopes ADD COLUMN IF NOT EXISTS route VARCHAR; ALTER TABLE provenance.git_checkpoints ADD COLUMN IF NOT EXISTS capture_source VARCHAR; ALTER TABLE remote.row_origins ADD COLUMN IF NOT EXISTS proof_id VARCHAR; ALTER TABLE remote.row_proofs ADD COLUMN IF NOT EXISTS authorization_workspace_id VARCHAR; UPDATE remote.row_proofs SET authorization_workspace_id=workspace_id WHERE authorization_workspace_id IS NULL; DROP TABLE IF EXISTS provenance.assertions; DROP TABLE IF EXISTS provenance.capture_gaps; INSERT INTO core_schema SELECT TRUE,1 WHERE NOT EXISTS (SELECT 1 FROM core_schema WHERE singleton)")
        for row in conn.execute("SELECT id,path,size FROM attachments WHERE path IS NOT NULL AND id NOT IN (SELECT attachment_id FROM attachment_bodies)").fetchall(): index_attachment_body(conn,*row)
        foreign,facts=(foreign:=set(conn.execute("SELECT kind,physical_entity FROM remote.provenance_origins").fetchall())),[(r["kind"],r["entity"]) for r in provenance_records(conn) if (r["kind"],r["entity"]) not in foreign] if not local_facts else []
        if facts: conn.executemany("INSERT OR IGNORE INTO provenance.local_facts VALUES (?,?)",facts)
    conn.execute("INSERT INTO archive_state SELECT TRUE,uuid(),0 WHERE NOT EXISTS (SELECT 1 FROM archive_state)")
    migration=(conn.execute("SELECT state FROM core_migrations WHERE name='remote_ids'").fetchone() or [None])[0]
    with migration_memory(conn) if current<2 else contextlib.nullcontext():
        if current<2 and (scope or migration and (migration.startswith("data") or migration=="changes")):
            with _transaction(conn): result,rebuild,_=(result:=migrate_remote_data(conn,ARCHIVE_COLUMNS,migration=="data_direct") if migration and migration.startswith("data") else migrate_remote_changes(conn) if migration=="changes" else migrate_remote_ids(conn,ARCHIVE_COLUMNS)),(rebuild:=fts_needs_rebuild(conn) if migration else result[1]),conn.execute(f"INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','{'fts' if rebuild else 'done'}'); INSERT OR REPLACE INTO core_schema SELECT TRUE,2 WHERE {not rebuild}; DELETE FROM core_migrations WHERE name='remote_ids' AND {not rebuild}")
            pending,current=rebuild,current if rebuild else 2
        else: pending=bool(conn.execute("SELECT 1 FROM core_migrations WHERE name='remote_ids' AND state='fts'").fetchone())
        if current<2 and not scope and not pending and fts_needs_rebuild(conn): pending=bool(conn.execute("INSERT OR REPLACE INTO core_migrations VALUES ('remote_ids','fts')"))
        conn.execute("INSTALL fts; LOAD fts")
        if pending:
            rebuild_fts_index(conn)
            with _transaction(conn): conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,2); DELETE FROM core_migrations WHERE name='remote_ids'")
        elif current<2 and not scope: conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,2)")
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<3:
        (aliases,ambiguous),duplicates,unknown,legacy_edits=_repository_alias_migration(conn),(duplicates:=[r[0] for r in conn.execute("SELECT file_edit_id FROM provenance.file_edit_files GROUP BY file_edit_id HAVING COUNT(*)>1").fetchall()]),[(edit,provenance_digest({"repository":None,"path":(path:=f"external/{provenance_digest(edit)[:24]}/unknown")}),None,None,"legacy_scope_conflict",None,path,"external") for edit in duplicates],[(r[0],f"external/{provenance_digest(r[0])[:24]}/unknown",None,None,None,None,None) for r in conn.execute("SELECT id FROM file_edits fe WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id)").fetchall()]
        _schema_migrate(conn,3,lambda:(conn.execute("CREATE TABLE provenance.file_edit_files_v3(file_edit_id VARCHAR PRIMARY KEY,file_id VARCHAR,old_content_hash VARCHAR,new_content_hash VARCHAR,evidence VARCHAR); INSERT INTO provenance.file_edit_files_v3 SELECT * FROM provenance.file_edit_files QUALIFY count(*) OVER (PARTITION BY file_edit_id)=1"),unknown and conn.executemany("INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",[(fid,repo,path,kind) for edit,fid,old,new,evidence,repo,path,kind in unknown]),unknown and conn.executemany("INSERT INTO provenance.file_edit_files_v3 VALUES (?,?,?,?,?)",[(edit,fid,old,new,evidence) for edit,fid,old,new,evidence,repo,path,kind in unknown]),conn.execute("DROP TABLE provenance.file_edit_files; ALTER TABLE provenance.file_edit_files_v3 RENAME TO file_edit_files"),ambiguous and (conn.execute("DELETE FROM provenance.repository_checkouts WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)]),conn.execute("DELETE FROM provenance.repository_aliases WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)])),aliases and conn.executemany("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",aliases),legacy_edits and conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",legacy_edits),conn.execute("INSERT OR IGNORE INTO provenance.conversation_scopes SELECT c.id,NULL,NULL,NULL,NULL,NULL FROM conversations c WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); INSERT OR REPLACE INTO core_schema VALUES (TRUE,3)")))
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<4:
        metadata,recoveries=(metadata:=conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='conversations' AND column_name='metadata'").fetchone()),(conn.execute("SELECT 'conversations',c.id FROM conversations c WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) UNION ALL SELECT 'messages',m.id FROM messages m WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)").fetchall() if metadata else [])
        _schema_migrate(conn,5,lambda:(conn.execute("CREATE TABLE IF NOT EXISTS provider_sessions(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); DELETE FROM provider_sessions"+("; UPDATE conversations c SET metadata=json_merge_patch(c.metadata,'{\"capture_mode\":\"history\"}') WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); UPDATE messages m SET metadata=json_merge_patch(m.metadata,CASE json_extract_string(m.metadata,'$.recovered') WHEN 'history.jsonl' THEN '{\"capture_mode\":\"history\"}' ELSE '{\"capture_mode\":\"recovery\"}' END) WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)" if metadata else "")),conn.execute((("INSERT INTO provider_sessions SELECT source,json_extract_string(metadata,'$.session_id'),min(id) FROM conversations c WHERE json_extract_string(metadata,'$.session_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) GROUP BY source,json_extract_string(metadata,'$.session_id');" if metadata else "")+"INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)")),recoveries and _archive_touch(conn,recoveries)))
    with _transaction(conn) if (binding_v5:=conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]==4) else contextlib.nullcontext(): binding_v5 and conn.execute("CREATE TABLE provider_sessions_v5(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); INSERT INTO provider_sessions_v5 SELECT * FROM provider_sessions; DROP TABLE provider_sessions; ALTER TABLE provider_sessions_v5 RENAME TO provider_sessions; INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)")
    _schema_migrate(conn,6,lambda:((legacy:=conn.execute("SELECT id FROM file_edits fe WHERE NOT EXISTS (SELECT 1 FROM provenance.file_edit_evidence v WHERE v.file_edit_id=fe.id)").fetchall()),legacy and conn.executemany("INSERT INTO provenance.file_edit_evidence VALUES (?,'unverified','source_unavailable',NULL)",legacy),legacy and _archive_touch(conn,[("file_edits",r[0]) for r in legacy]),conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,6)")))
    _schema_migrate(conn,7,lambda:conn.execute("CREATE TABLE provenance.file_edit_evidence_v7(file_edit_id VARCHAR PRIMARY KEY,status VARCHAR NOT NULL CHECK(status IN ('confirmed','invalid','unknown','unverified')),reason VARCHAR NOT NULL,tool_call_id VARCHAR); INSERT INTO provenance.file_edit_evidence_v7 SELECT file_edit_id,CASE status WHEN 'legacy_unverified' THEN 'unverified' ELSE status END,reason,tool_call_id FROM provenance.file_edit_evidence; DROP TABLE provenance.file_edit_evidence; ALTER TABLE provenance.file_edit_evidence_v7 RENAME TO file_edit_evidence; INSERT OR REPLACE INTO core_schema VALUES (TRUE,7)"))
    _schema_migrate(conn,8,lambda:conn.execute("""ALTER TABLE messages ALTER embedding TYPE FLOAT[]; INSERT OR IGNORE INTO remote.semantic_ancestors SELECT kind,workspace_id,author_user_id,object_id,revision,json_extract_string(a.value,'$') FROM (SELECT 'provider.session' kind,workspace_id,author_user_id,object_id,revision,proof FROM remote.provider_session_aliases UNION ALL SELECT 'file-edit.evidence',workspace_id,author_user_id,object_id,revision,proof FROM remote.file_edit_evidence_proofs),json_each(proof,'$.ancestors') a; INSERT OR REPLACE INTO core_schema VALUES (TRUE,8); INSERT OR REPLACE INTO embedding_state SELECT TRUE,? WHERE EXISTS (SELECT 1 FROM messages WHERE embedding IS NOT NULL)""",(json.dumps(_EPROFILES["llama"],sort_keys=True,separators=(",",":")),)))
    with _transaction(conn): conn.execute("""CREATE OR REPLACE TEMP TABLE core_legacy_conflicts AS SELECT x.file_edit_id edit,x.file_id old_id,f.path,sha256(json_object('path',f.path,'repository',NULL)) new_id FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id WHERE x.evidence='legacy_scope_conflict' AND EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='edit.observed' AND l.entity=x.file_edit_id) AND NOT EXISTS(SELECT 1 FROM remote.provenance_origins o WHERE o.kind='edit.observed' AND o.physical_entity=x.file_edit_id) AND (x.file_id<>sha256(json_object('path',f.path,'repository',NULL)) OR NOT EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='file.observed' AND l.entity=sha256(json_object('path',f.path,'repository',NULL)))); INSERT OR IGNORE INTO provenance.files SELECT new_id,NULL,path,'external' FROM core_legacy_conflicts; UPDATE provenance.file_edit_files x SET file_id=c.new_id FROM core_legacy_conflicts c WHERE x.file_edit_id=c.edit; INSERT OR IGNORE INTO provenance.local_facts SELECT 'file.observed',new_id FROM core_legacy_conflicts; DELETE FROM provenance.local_facts l USING core_legacy_conflicts c WHERE l.kind='file.observed' AND l.entity=c.old_id AND c.old_id<>c.new_id; DELETE FROM provenance.files f USING core_legacy_conflicts c WHERE f.id=c.old_id AND c.old_id<>c.new_id AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_versions v WHERE v.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.kind='file.observed' AND o.physical_entity=f.id); UPDATE archive_state SET generation=generation+1 WHERE singleton AND EXISTS(SELECT 1 FROM core_legacy_conflicts); INSERT OR REPLACE INTO archive_changes SELECT kind,entity,generation FROM archive_state,(SELECT 'file_edits' kind,edit entity FROM core_legacy_conflicts UNION ALL SELECT 'edit.observed',edit FROM core_legacy_conflicts UNION ALL SELECT 'file.observed',new_id FROM core_legacy_conflicts) WHERE singleton; DROP TABLE core_legacy_conflicts""")

def counts_by_source(conn):
    queries,rows=(queries:=[("conversations","source"),("messages m JOIN conversations c ON c.id=m.conversation_id","c.source"),("tool_calls tc JOIN messages m ON tc.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source"),("attachments a JOIN messages m ON a.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source"),("file_edits fe JOIN messages m ON fe.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source")]),[(source,i,n) for i,(table,column) in enumerate(queries) for source,n in conn.execute(f"SELECT {column},COUNT(*) FROM {table} GROUP BY {column}").fetchall()]
    return {source:[next((n for s,j,n in rows if (s,j)==(source,i)),0) for i in range(5)] for source in {r[0] for r in rows}}

def load_fts(conn, allow_install: bool = False):
    try:
        if allow_install: conn.execute("INSTALL fts")
        conn.execute("LOAD fts")
    except Exception as e: raise ValueError("FTS extension not available. Run `convos init` once with network access.") from e

def ensure_fts_index(conn):
    if not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'fts_main_messages'").fetchone(): conn.execute("PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1)")

def rebuild_fts_index(conn): conn.execute("PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1)")

def ensure_db_ready(conn):
    if conn.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'messages'").fetchone(): return True
    typer.echo("Database not initialized. Run `convos init` or `convos sync`.")
    return False

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
    return {"text":"\n".join(b.get("text","") or b.get("thinking","") if b.get("type") in ("text",None) else "" for b in blocks).strip() or "\n".join(str(b) for b in content if isinstance(b,str)).strip(),"thinking":"\n".join(b["thinking"] for b in blocks if b.get("type")=="thinking" and b.get("thinking")).strip() or None,"tools":[{"name":b["name"],"input":b.get("input",{}),"id":b.get("id")} for b in blocks if b.get("type")=="tool_use"]+[{"id":b.get("tool_use_id"),"output":b.get("content","") ,"error":b.get("is_error",False)} for b in blocks if b.get("type")=="tool_result"],"attachments":[{"filename":b.get("name",b.get("file_name")),"mime_type":b.get("content_type",b.get("file_type")),"size":b.get("size",b.get("file_size")),"url":b.get("asset_pointer",b.get("url"))} for b in blocks if b.get("type") in ("image_asset_pointer","file") or b.get("content_type") in ("image_asset_pointer","file")]}

def _safari_records():
    paths=(Path.home()/"Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies",Path.home()/"Library/Cookies/Cookies.binarycookies")
    if not (path:=next((p for p in paths if p.exists()),None)): return
    data=path.read_bytes()
    if data[:4]!=b"cook": return
    count=struct.unpack(">I",data[4:8])[0]
    sizes=[struct.unpack(">I",data[8+i*4:12+i*4])[0] for i in range(count)]
    pages=[data[start:start+size] for start,size in zip(itertools.accumulate([8+4*count,*sizes]),sizes)]
    yield from ((text(fields[0]),text(fields[1]),text(struct.unpack("<I",page[off+28:off+32])[0])) for page in pages if page[:4]==b"\0\0\1\0" for i in range(struct.unpack("<I",page[4:8])[0]) for off in [struct.unpack("<I",page[8+i*4:12+i*4])[0]] for fields,text in [(struct.unpack("<III",page[off+16:off+28]),lambda pos,page=page,off=off:page[off+pos:page.find(b"\0",off+pos)].decode(errors="ignore"))])
def read_safari_cookies(domain: str) -> dict[str,str]:
    target=domain.lstrip(".").lower()
    try: return {name:value for host,name,value in _safari_records() or () if target in (clean:=host.lstrip(".").lower()) or clean in target or clean.endswith(target) or target.endswith(clean)}
    except PermissionError as e: raise ValueError("Safari cookies are not readable. Grant Full Disk Access to your terminal or use -b chrome.") from e

def _chrome_path(profile=None):
    root=Path.home()/"Library/Application Support/Google/Chrome"/(profile or os.environ.get("CONVOS_CHROME_PROFILE","Default"))
    return next((path for path in (root/"Cookies",root/"Network/Cookies") if path.exists()),None)

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:
    if not (db_path:=_chrome_path(profile)): return {}
    result = subprocess.run(["security", "find-generic-password", "-w", "-a", "Chrome", "-s", "Chrome Safe Storage"], capture_output=True, text=True, timeout=10)
    if result.returncode != 0: return {}
    key=hashlib.pbkdf2_hmac('sha1',result.stdout.strip().encode(),b'saltysalt',1003,16)
    with contextlib.closing(sqlite3.connect(f"file:{db_path}?mode=ro&nolock=1",uri=True)) as db:
        return {name:(value[32:] if not value[:32].isascii() else value).decode(errors="ignore") for name,encrypted,host in db.execute("SELECT name,encrypted_value,host_key FROM cookies WHERE host_key LIKE ?",(f"%{domain}%",)) if encrypted[:3]==b"v10" for decryptor in [Cipher(algorithms.AES(key),modes.CBC(b" "*16)).decryptor()] for decrypted in [decryptor.update(encrypted[3:])+decryptor.finalize()] for value in [decrypted[:-decrypted[-1]]]}

def get_cookies(domain: str, browser: str = "safari", profile: str | None = None) -> dict[str, str]: return read_safari_cookies(domain) if browser == "safari" else read_chrome_cookies(domain, profile=profile)

def get_cookies_any(domains: list[str], browser: str = "safari", profile: str | None = None) -> dict[str, str]: return {name:value for domain in domains for name,value in get_cookies(domain,browser,profile=profile).items()}

def safari_cookie_domains(): return {host for host,_,_ in _safari_records() or ()}

def chrome_cookie_domains(profile: str | None = None):
    if not (path:=_chrome_path(profile)): return set()
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro&nolock=1",uri=True)) as db: return {r[0] for r in db.execute("SELECT DISTINCT host_key FROM cookies")}

def chrome_profiles() -> list[str]: return [p.name for p in base.iterdir() if p.is_dir() and ((p/"Cookies").exists() or (p/"Network/Cookies").exists())] if (base:=Path.home()/"Library/Application Support/Google/Chrome").exists() else []

def chatgpt_profiles(browser: str) -> list[str | None]: return [None] if browser!="chrome" else [prof] if (prof:=os.environ.get("CONVOS_CHROME_PROFILE")) else chrome_profiles() or [None]

def chatgpt_cookie_base(browser: str, hosts: list[tuple[str, list[str]]], profile: str | None):
    if found:=next(((cookies,url) for url,domains in hosts if (cookies:=get_cookies_any(domains,browser,profile=profile))),None): return found
    raise ValueError(f"No ChatGPT cookies found in {browser}" + (f" profile {profile}" if profile else ""))
def claude_listing(browser):
    cookies=required(get_cookies("claude.ai",browser),ValueError(f"No Claude cookies found in {browser}"))
    org=required(fetch_json("https://claude.ai/api/organizations",cookies,_CLAUDE_HEADERS),ValueError("Could not get Claude org ID"))[0]["uuid"]
    return cookies,org,fetch_json(f"https://claude.ai/api/organizations/{org}/chat_conversations",cookies,_CLAUDE_HEADERS)

def chatgpt_headers(cookies, base, ua, debug_profile: str | None = None):
    headers={"Origin":base,"Referer":f"{base}/","User-Agent":ua,"Accept":"application/json","Accept-Language":"en-US,en;q=0.9","Sec-Fetch-Site":"same-origin","Sec-Fetch-Mode":"cors","Sec-Fetch-Dest":"empty"}
    with contextlib.suppress(Exception):
        session = fetch_json(f"{base}/api/auth/session", cookies, headers, timeout=10, retries=0, rate_limit_backoff=300)
        headers.update({**({"Authorization":f"Bearer {token}"} if (token:=session.get("accessToken")) else {}),**({"ChatGPT-Account-ID":aid} if (aid:=session.get("account",{}).get("id")) else {})})
        if debug_profile: typer.echo(f"  chatgpt chrome profile={debug_profile} user={session.get('user', {}).get('email')}", flush=True)
    return headers

def fetch_json(url: str, cookies: dict[str, str], headers: dict = None, timeout: int = 15, retries: int = 1, before_request=None, rate_limit_backoff=None) -> dict:
    parts=[s for k,v in cookies.items() for s in [f"{k}={v}"] if all(ord(c)<256 for c in s)]
    cookie_str,hdrs,req=(cookie_str:="; ".join(parts)),(hdrs:={"Cookie":cookie_str,"User-Agent":"Mozilla/5.0","Accept":"application/json",**(headers or {})}),urllib.request.Request(url,headers=hdrs)
    for i in range(retries+1):
        before_request and before_request()
        try:
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=timeout) as resp: return json.loads(resp.read())
        except Exception as e:
            if i == retries: raise
            code,retry,delay=(code:=getattr(e,"code",None)),(retry:=(getattr(e,"headers",None) or {}).get("Retry-After","")),max(rate_limit_backoff or 30*(i+1),int(retry)) if code==429 and str(retry).isdigit() else rate_limit_backoff or 30*(i+1) if code==429 else 1+i
            if code == 429: typer.echo(f"  rate limited; retrying in {delay}s", err=True)
            time.sleep(delay)

@dataclass(slots=True)
class ParseResult:
    convs: list = field(default_factory=list)
    msgs: list = field(default_factory=list)
    tools: list = field(default_factory=list)
    attachs: list = field(default_factory=list)
    artifacts: list = field(default_factory=list)
    edits: list = field(default_factory=list)
    edit_evidence: list = field(default_factory=list)
    scopes: list | None = None
    edit_scopes: list | None = None
    provenance_edits: set = field(default_factory=set)
    provenance_conversations: set = field(default_factory=set)

    def __iadd__(self,other):
        for name in ("convs","msgs","tools","attachs","artifacts","edits","edit_evidence"): getattr(self,name).extend(getattr(other,name))
        return self
    def __add__(self,other): return ParseResult(**{name:[*getattr(self,name),*getattr(other,name)] for name in ("convs","msgs","tools","attachs","artifacts","edits","edit_evidence")})

def log_parse_error(context: str, err: Exception): typer.echo(f"  parse error ({context}): {type(err).__name__}: {err}", err=True)

def safe_parse(context: str, fn, *args, **kwargs):
    try: return fn(*args, **kwargs)
    except Exception as e:
        log_parse_error(context,e)
        return None

def parse_source(path: Path, source: str|None = None, bindings=None) -> ParseResult:
    parsers = {"chatgpt": parse_chatgpt, "claude": parse_claude, "claude-code": parse_claude_code, "codex": parse_codex}
    src = source or detect_source(path)
    if src not in parsers: raise ValueError(f"Unknown source: {src}")
    return parsers[src](path,bindings=bindings) if src in ("claude-code","codex") else parsers[src](path)

def chatgpt_mapping(cid: str, mapping: dict) -> tuple[list, list, list]:
    tree_path=lambda nid:(*tree_path(parent),(mapping[parent].get("children") or [x for x in mapping if mapping[x].get("parent")==parent]).index(nid)) if (parent:=mapping[nid].get("parent")) in mapping else (tuple(mapping).index(nid),)
    def pmid(nid):  # nearest ancestor that carries a message (roots are message-less)
        while (nid := mapping[nid].get("parent")) and not mapping[nid].get("message"): pass
        return gen_id("chatgpt", f"{cid}:{nid}") if nid else None
    nodes=[(provider_index,nid,msg,gen_id("chatgpt",f"{cid}:{nid}"),msg.get("author",{}).get("role","unknown"),(meta:=msg.get("metadata",{})),ts_any(msg.get("create_time")),meta.get("model_slug"),(parts:=(msg.get("content") or {}).get("parts",[])),"\n".join(dict.fromkeys(p.strip() for p in [*parts,(msg.get("content") or {}).get("text")] if isinstance(p,str) and p.strip()))) for provider_index,nid in enumerate(sorted(mapping,key=tree_path)) if (msg:=mapping[nid].get("message"))]
    msgs=[dict(id=mid,conversation_id=cid,role=role,content=text,thinking=None,created_at=ts,model=model,metadata=json.dumps({**meta,"provider_index":provider_index}),parent_id=pmid(nid)) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes]
    tools=[dict(id=gen_id("chatgpt",f"tool:{mid}"),message_id=mid,tool_name=meta.get("invoked_plugin",{}).get("namespace",role),input=json.dumps(meta.get("args",{})),output=json.dumps(msg.get("content",{})),status="complete",duration_ms=None,created_at=ts) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes if role=="tool" or meta.get("invoked_plugin")]
    attachs=[dict(id=gen_id("chatgpt",f"attach:{mid}:{i}"),message_id=mid,filename=p.get("name",""),mime_type=p.get("content_type"),size=p.get("size"),path=None,url=p.get("asset_pointer"),created_at=ts) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes for i,p in enumerate(parts) if isinstance(p,dict) and p.get("content_type") in ("image_asset_pointer","file")]
    return msgs,tools,attachs

def fetch_chatgpt(browser: str = "safari", limit: int = 0, profiles: list[str | None] | None = None, known: dict | None = None, legacy: set | None = None, frontiers: dict | None = None, sink=None) -> ParseResult:
    debug,known,legacy,frontiers,limiters=os.environ.get("CONVOS_CHATGPT_DEBUG"),known or {},legacy or set(),frontiers or {},{}

    def fetch_with_profile(profile: str | None) -> ParseResult:
        cookies,base=chatgpt_cookie_base(browser,_CHATGPT_HOSTS,profile)
        headers,key,account,r,saved,matched,frontier,boundary,bucket=(headers:=chatgpt_headers(cookies,base,_BROWSER_UA[browser],debug_profile=profile if debug else None)),(key:=profile or "default"),(account:=headers.get("ChatGPT-Account-ID")),ParseResult(),(saved:=frontiers.get(key,{})),(matched:=account and isinstance(saved,dict) and saved.get("account")==account),*((ts_any(saved.get("updated")),saved.get("id")) if matched else (None,None)),limiters.setdefault(("account",account) if account else ("profile",browser,key),[CHATGPT_BURST,time.monotonic()])
        def pace():
            now,credit=time.monotonic(),min(CHATGPT_BURST,bucket[0]+(time.monotonic()-bucket[1])*CHATGPT_RATE)
            if delay:=max(0,(1-credit)/CHATGPT_RATE): time.sleep(delay)
            bucket[:]=[max(0,credit-1),time.monotonic() if delay else now]
        def parse_item_raw(item):
            cid,gizmo,conv,(msgs,tools,attachs),times,current=(cid:=gen_id("chatgpt",item["id"])),item.get("gizmo_id"),(conv:=fetch_json(f"{base}/backend-api/conversation/{item['id']}",cookies,headers,timeout=20,retries=2,before_request=pace,rate_limit_backoff=300)),(parsed:=chatgpt_mapping(cid,conv.get("mapping",{}))),[m["created_at"] for m in parsed[0] if m["created_at"]],(conv.get("mapping",{}).get(conv.get("current_node")) or {}).get("message")
            return dict(conv=dict(id=cid,source="chatgpt",title=item.get("title"),created_at=ts_any(conv.get("create_time") or item.get("create_time")) or min(times,key=datetime.timestamp,default=None),updated_at=ts_any(conv.get("update_time") or item.get("update_time")) or max(times,key=datetime.timestamp,default=None),model=item.get("model"),cwd=None,git_branch=None,project_id=gizmo,metadata=json.dumps({"session_id":item["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"api","remote_update_time":conv.get("update_time") or item.get("update_time"),**({"remote_complete":current.get("metadata",{}).get("is_complete",current.get("author",{}).get("role")=="assistant" and current.get("status") not in ("in_progress","running"))} if current else {}),**({"gizmo_id":gizmo} if gizmo else {})})),msgs=msgs,tools=tools,attachs=attachs)
        listed, seen, tail, offset, fetched, total = [], set(), set(), 0, 0, None
        while True:
            data = fetch_json(f"{base}/backend-api/conversations?offset={offset}&limit=100&order=updated", cookies, headers, timeout=20, retries=1, before_request=pace, rate_limit_backoff=300)
            raw, reported, keys = data.get("items", []), data.get("total"), ",".join(data.keys())
            if debug: print(f"  chatgpt page offset={offset} items={len(raw)} total={reported} keys={keys}", flush=True)
            if total is not None and reported is not None and reported < total: raise RuntimeError(f"unstable list total {total}->{reported}")
            total = reported if reported is not None else total
            if not raw:
                required(reported is None or offset>=reported,RuntimeError(f"incomplete list at {offset}/{reported}"))
                break
            ids = [it["id"] for it in raw]
            if offset and not tail.intersection(ids): raise RuntimeError(f"unstable list at offset {offset}")
            tail,items=set(ids[-20:]),[it for it in {it["id"]:it for it in raw}.values() if it["id"] not in seen]
            seen,cut,stop=seen|{it["id"] for it in items},len(items),bool(frontier and any((updated:=ts_any(it.get("update_time"))) and updated.timestamp()<frontier.timestamp() for it in items))
            if not frontier and boundary and (at := next((i for i, it in enumerate(items) if it["id"] == boundary), None)) is not None: items, cut, stop = items[:at+1]+[it for it in items[at+1:] if gen_id("chatgpt", it["id"]) not in known], len(items), True
            listed += items[:cut]
            if stop or reported is not None and offset+len(raw) >= reported: break
            offset += max(1, len(raw)-20)
        page = [it for it in listed if (cid := gen_id("chatgpt", it["id"])) not in known or known[cid] is None or (updated := ts_any(it.get("update_time"))) is None or updated.timestamp() > (frontier.timestamp() if frontier and cid in legacy else known[cid]+(5 if cid in legacy else 0))][:limit or len(listed)]
        if len(page) > CHATGPT_BURST: typer.echo(f"  chatgpt pacing bulk fetch ({CHATGPT_BURST} burst, then {int(CHATGPT_RATE*300)} requests/5m)")
        for at in range(0, len(page), 20):
            try: results=[parse_item_raw(item) for item in page[at:at+20]]
            except Exception as e: raise RuntimeError(f"detail fetch failed: {e}") from e
            (sink or r.__iadd__)(ParseResult([x["conv"] for x in results],[m for x in results for m in x["msgs"]],[t for x in results for t in x["tools"]],[a for x in results for a in x["attachs"]]))
            typer.echo(f"  chatgpt details {(fetched:=fetched+len(results))}")
        return r
    out, errs = ParseResult(), []
    for profile in profiles if profiles is not None else chatgpt_profiles(browser):
        try: out+=fetch_with_profile(profile)
        except Exception as e: errs.append(f"{profile or 'default'}: {e}")
    if errs and (profiles is not None or not out.convs or any("detail fetch failed" in e for e in errs)): raise ValueError("ChatGPT fetch failed -- " + " | ".join(errs))
    return out

def fetch_claude(browser: str = "safari", limit: int = 0, since: datetime = None) -> ParseResult:
    print("  claude listing...", flush=True)
    cookies,org_id,data=claude_listing(browser)
    listed=data if limit==0 else data[:limit]
    items=[item for item in listed if not since or not (updated:=ts_from_iso(item.get("updated_at") or item.get("created_at"))) or updated>since]
    if listed: print(f"  claude listed {len(listed)}; fetching {len(items)}", flush=True)
    def parse_message(cid,m):
        mid,ts,blocks,tools,attachs,text,msgs=(mid:=gen_id("claude",f"{cid}:{m.get('uuid','')}")),(ts:=ts_from_iso(m.get("created_at"))),(blocks:=m.get("content",[]) if isinstance(m.get("content"),list) else [{"type":"text","text":m.get("text","")}]),(tools:=[dict(id=gen_id("claude",f"{'tool' if b.get('type')=='tool_use' else 'toolres'}:{mid}:{i}"),message_id=mid,tool_name=b.get("name") if b.get("type")=="tool_use" else b.get("tool_use_id"),input=json.dumps(b.get("input",{})) if b.get("type")=="tool_use" else "{}",output="{}" if b.get("type")=="tool_use" else json.dumps(b.get("content","")),status="pending" if b.get("type")=="tool_use" else "complete",duration_ms=None,created_at=ts) for i,b in enumerate(blocks) if isinstance(b,dict) and b.get("type") in ("tool_use","tool_result")]),(attachs:=[dict(id=gen_id("claude",f"attach:{mid}:{i}"),message_id=mid,filename=a.get("file_name"),mime_type=a.get("file_type"),size=a.get("file_size"),path=None,url=a.get("url"),created_at=ts) for i,a in enumerate(m.get("attachments",[]))]),(text:="\n".join(b.get("text","") if isinstance(b,dict) and b.get("type")=="text" else b if isinstance(b,str) else "" for b in blocks).strip()),[dict(id=mid,conversation_id=cid,role="user" if m.get("sender")=="human" else m.get("sender","unknown"),content=text,thinking=None,created_at=ts,model=m.get("model"),metadata="{}",parent_id=None)] if text or tools or attachs else []
        return ParseResult(msgs=msgs,tools=tools,attachs=attachs)
    def parse_item(item):
        cid,project,conv,detail=(cid:=gen_id("claude",item["uuid"])),(project:=item.get("project_uuid")),dict(id=cid,source="claude",title=item.get("name"),created_at=ts_from_iso(item.get("created_at")),updated_at=ts_from_iso(item.get("updated_at")),model=item.get("model"),cwd=None,git_branch=None,project_id=project,metadata=json.dumps({"session_id":item["uuid"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"api",**({"project_uuid":project} if project else {})})),fetch_json(f"https://claude.ai/api/organizations/{org_id}/chat_conversations/{item['uuid']}",cookies,_CLAUDE_HEADERS)
        return sum((parse_message(cid,m) for m in detail.get("chat_messages",[])),ParseResult(convs=[conv]))
    fetched,step,r=0,max(1,len(items)//10),ParseResult()
    for idx, item in enumerate(items):
        r,fetched=r+parse_item(item),fetched+1
        if idx == len(items)-1 or (idx+1) % step == 0: print(f"  claude fetched {fetched}/{len(items)}", flush=True)
    return r

def parse_chatgpt(path: Path) -> ParseResult:
    data=json.load(zipfile.ZipFile(path).open("conversations.json")) if path.suffix==".zip" else json.loads(path.read_text())
    def parse_conv(c):
        cid, gizmo = gen_id("chatgpt", c.get("id", "")), c.get("gizmo_id")
        conv=dict(id=cid,source="chatgpt",title=c.get("title"),created_at=ts_any(c.get("create_time")),updated_at=ts_any(c.get("update_time")),model=c.get("default_model_slug"),cwd=None,git_branch=None,project_id=gizmo,metadata=json.dumps({"session_id":c.get("id", ""),"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export",**({"gizmo_id":gizmo} if gizmo else {})}))
        msgs, tools, attachs = chatgpt_mapping(cid, c.get("mapping", {}))
        return ParseResult(convs=[conv],msgs=msgs,tools=tools,attachs=attachs)
    return sum((parsed for idx,c in enumerate(data) if (parsed:=safe_parse(f"chatgpt export conv {c.get('id') if isinstance(c,dict) else idx}",parse_conv,c))),ParseResult())

def parse_claude(path: Path) -> ParseResult:
    data = json.loads(path.read_text())
    def parse_conv(c):
        cid = gen_id("claude", c["uuid"] if "uuid" in c else c["id"])
        msgs_data = c.get("chat_messages", [])
        parsed=[(i,m,gen_id("claude",f"{cid}:{m['uuid'] if 'uuid' in m else m['id']}"),extract_content(m.get("text") or m.get("content", ""))) for i,m in enumerate(msgs_data)]
        results={t["id"]:t for i,m,mid,ec in parsed for t in ec["tools"] if "output" in t and t.get("id")}
        return {"conv":dict(id=cid,source="claude",title=c.get("name") or c.get("title"),created_at=ts_from_iso(c.get("created_at")),updated_at=ts_from_iso(c.get("updated_at")),model=c.get("model"),cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"session_id":c["uuid"] if "uuid" in c else c["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export"})),"msgs":[dict(id=mid,conversation_id=cid,role="user" if m.get("sender")=="human" else m.get("sender","unknown"),content=ec["text"],thinking=ec["thinking"],created_at=ts_from_iso(m.get("created_at")),model=m.get("model"),metadata=json.dumps({"provider_index":i}),parent_id=None) for i,m,mid,ec in parsed if ec["text"] or ec["tools"] or ec["attachments"] or m.get("attachments")],"tools":[dict(id=gen_id("claude",f"tool:{mid}:{t.get('id') or j}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((results.get(t.get("id")) or {}).get("output","")),status="failed" if (results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in results else "pending",duration_ms=None,created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,t in enumerate(ec["tools"]) if "name" in t],"attachs":[dict(id=gen_id("claude",f"attach:{mid}:{j}"),message_id=mid,filename=a.get("name",a.get("file_name")),mime_type=a.get("content_type",a.get("file_type")),size=a.get("size",a.get("file_size")),path=None,url=a.get("asset_pointer",a.get("url")),created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,a in enumerate([*ec["attachments"],*m.get("attachments",[])])]}
    parsed=[p for idx,c in enumerate(data) if (p:=safe_parse(f"claude export conv {c.get('uuid') if isinstance(c,dict) else idx}",parse_conv,c))]
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
    agent_id,src,kind,parent,cid=(agent_id:=explicit_agent or (jsonl.stem if "subagents" in jsonl.parts else None)),"claude-code","subagent" if agent_id or sidechain else "main",root_session if agent_id and agent_id!=root_session else None,(cid:=(bindings or {}).get(("claude-code",agent_id or root_session),gen_id("claude-code",str(jsonl))))
    timestamps,msg_events,tool_results,uuid2id=[ts_from_iso(e["timestamp"]) for e in events if "timestamp" in e],(msg_events:=[(i,e) for i,e in enumerate(events) if "message" in e]),{t["id"]:t for e in events if "message" in e for t in extract_content(e["message"].get("content",[]))["tools"] if "output" in t and t.get("id")},{e["uuid"]:gen_id(src,f"{cid}:{idx}") for idx,(i,e) in enumerate(msg_events) if "uuid" in e}

    turns=[(idx,i,e,(c:=extract_content(e["message"].get("content",e["message"].get("text","")))),ts_from_iso(e.get("timestamp")),gen_id(src,f"{cid}:{idx}")) for idx,(i,e) in enumerate(msg_events)]
    msgs=[dict(id=mid,conversation_id=cid,role="user" if e["type"] in ("human","user") else e["type"],content=c["text"],thinking=c["thinking"],created_at=ts,model=e["message"].get("model") if e["type"]=="assistant" else None,metadata=json.dumps({"provider_index":i}),parent_id=uuid2id.get(e.get("parentUuid"))) for idx,i,e,c,ts,mid in turns if c["text"] or c["tools"]]  # keep tool-only turns: tools/edits reference them
    msgs=[{**m,"parent_id":m["parent_id"] if m["parent_id"] in mids else None} for m in msgs] if (mids:={x["id"] for x in msgs}) else []
    if not msgs: return None
    tools=[dict(id=gen_id(src,f"tool:{cid}:{t.get('id') or f'{idx}:{j}'}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((tool_results.get(t.get("id")) or {}).get("output","")),status="failed" if (tool_results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in tool_results else "pending",duration_ms=None,created_at=ts) for idx,i,e,c,ts,mid in turns for j,t in enumerate(c["tools"]) if "name" in t]
    edit_calls=[(idx,ts,mid,j,t,tool_results.get(t.get("id"))) for idx,i,e,c,ts,mid in turns for j,t in enumerate(c["tools"]) if t.get("name") in ("Write","Edit","MultiEdit") and t.get("input",{}).get("file_path")]
    edits=[dict(id=gen_id(src,f"edit:{cid}:{idx}:{j}"),message_id=mid,file_path=t["input"]["file_path"],edit_type=t["name"].lower(),content=t["input"].get("content") or t["input"].get("new_string",""),created_at=ts,old_content=t["input"].get("old_string")) for idx,ts,mid,j,t,result in edit_calls if result is not None and not result.get("error")]
    evidence=[dict(file_edit_id=gen_id(src,f"edit:{cid}:{idx}:{j}"),status=status,reason=reason,tool_call_id=gen_id(src,f"tool:{cid}:{t.get('id') or f'{idx}:{j}'}")) for idx,ts,mid,j,t,result in edit_calls for status,reason in [(('unknown','result_missing') if result is None else ('invalid','provider_failure') if result.get('error') else ('confirmed','provider_success'))]]
    return {"conv":dict(id=cid,source=src,title=f"{jsonl.parent.name.replace('-Users-','~/').replace('-','/')} ({jsonl.stem[:8]})",created_at=timestamps[0] if timestamps else None,updated_at=timestamps[-1] if timestamps else None,model=next((m["model"] for m in msgs if m["model"] and m["model"]!="<synthetic>"),None),cwd=system.get("cwd") or next((e.get("cwd") for e in events if e.get("cwd")),None),git_branch=system.get("gitBranch") or next((e.get("gitBranch") for e in events if e.get("gitBranch")),None),project_id=None,metadata=json.dumps({k:v for k,v in {"session_id":agent_id or root_session,"parent_session_id":parent,"session_kind":kind,"session_kind_evidence":"exact" if explicit_agent or sidechain else "inferred","agent_id":agent_id,"agent_name":next((e.get("agentName") for e in events if e.get("agentName")),None),"agent_role":next((e.get("agentType") for e in events if e.get("agentType")),None),"agent_depth":next((e.get("agentDepth") for e in events if e.get("agentDepth") is not None),None),"originator":system.get("entrypoint"),"client_version":system.get("version"),"capture_mode":"transcript"}.items() if v is not None})),"msgs":msgs,"tools":tools,"attachs":[],"edits":edits,"edit_evidence":evidence}

def _parse_sessions(paths,parser,bindings):
    bound={} if bindings is None else bindings
    sessions=[(bound.setdefault((s["conv"]["source"],m["session_id"]),s["conv"]["id"]),s)[1] if (m:=json.loads(s["conv"]["metadata"] or "{}")).get("session_id") else s for path in sorted(paths) if (s:=safe_parse(f"{parser.__name__.removeprefix('parse_').replace('_session','').replace('_','-')} session {path}",parser,path,bound))]
    return ParseResult(convs=[s["conv"] for s in sessions],msgs=[m for s in sessions for m in s["msgs"]],tools=[t for s in sessions for t in s["tools"]],attachs=[a for s in sessions for a in s["attachs"]],edits=[e for s in sessions for e in s["edits"]],edit_evidence=[v for s in sessions for v in s["edit_evidence"]])
def parse_claude_code(projects_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult: return _parse_sessions(files or projects_dir.rglob("*.jsonl"),parse_claude_code_session,bindings)

def parse_codex_session(jsonl: Path, bindings=None) -> dict | None:
    events,timestamps=[],{}
    for i,e in iter_jsonl(jsonl):
        if "timestamp" in e: timestamps[i]=ts_from_iso(e["timestamp"])
        if e.get("type") in ("session_meta","turn_context") or e.get("type")=="response_item" and isinstance(e.get("payload"),dict) and e["payload"].get("type") in ("message","function_call","function_call_output","custom_tool_call","custom_tool_call_output"): events.append((i,e))
    if not events: return None
    meta,contexts,subagent,spawn,provider_id,src,items,cid=(meta:=next((e["payload"] for _,e in events if e.get("type")=="session_meta"),{})),(contexts:=[(i,e["payload"].get("model")) for i,e in events if e.get("type")=="turn_context" and e.get("payload",{}).get("model")]),(subagent:=(meta.get("source") or {}).get("subagent") if isinstance(meta.get("source"),dict) else None),(spawn:=subagent.get("thread_spawn",{}) if isinstance(subagent,dict) else {}),(provider_id:=meta.get("id")),"codex",[(i,e["payload"]) for i,e in events if e.get("type")=="response_item"],((bindings or {}).get(("codex",provider_id)) if provider_id else None) or gen_id("codex",str(jsonl))
    extract_msg_text,model_at=lambda p:"\n".join(b["text"] for b in p.get("content",[]) if isinstance(b,dict) and b.get("type") in ("input_text","output_text","text") and b.get("text")),lambda i:next((m for j,m in reversed(contexts) if j<=i),None)
    def image(i,j,b):
        url,data,head,encoded=(url:=b["image_url"]),(data:=url.startswith("data:")),*(url.split(",",1) if data else ("",""))
        mime=head[5:-7] if head.startswith("data:image/") and head.endswith(";base64") else b.get("mime_type")
        required(not data or mime,ValueError("invalid Codex image data URL"))
        size,body,path,ext=len(encoded.rstrip("="))*3//4 if data else None,(body:=base64.b64decode(encoded,validate=True) if data and len(encoded)<=4*((ATTACHMENT_LIMIT+2)//3) else None),attachment_body(body) if body is not None else None,{"image/jpeg":"jpg"}.get(mime,mime.rsplit("/",1)[-1] if mime and re.fullmatch(r"image/[a-z0-9.+-]+",mime) else "bin")
        return dict(id=gen_id(src,f"attach:{cid}:{i}:{j}"),message_id=gen_id(src,f"{cid}:{i}"),filename=f"image-{j+1}.{ext}",mime_type=mime,size=len(body) if body is not None else size,path=str(path) if path else None,url=None if data else url,created_at=timestamps.get(i))
    def norm_args(p): return json.loads(a) if isinstance((a := p.get("arguments", {})), str) else a

    mitems = [(i, p, t) for i, p in items if p.get("type") == "message" and ((t := extract_msg_text(p)) or any(isinstance(b,dict) and b.get("type")=="input_image" for b in p.get("content",[])))]
    if not (msgs := [dict(id=gen_id(src, f"{cid}:{i}"), conversation_id=cid, role=p["role"], content=t.strip(), thinking=None, created_at=timestamps.get(i), model=model_at(i), metadata=json.dumps({"provider_index":i}), parent_id=None)
                     for i, p, t in mitems]): return None
    anchor = lambda k: gen_id(src, f"{cid}:{next((i for i, _, _ in reversed(mitems) if i <= k), mitems[0][0])}")  # function_call items are not messages; attach to nearest preceding one

    function_out={p.get("call_id"):p.get("output","") for _,p in items if p.get("type")=="function_call_output"}
    def edit_result(call,outputs,outer=None):
        if call not in outputs: return "unknown","result_missing"
        raw=json.dumps(outputs[call]).lower().replace("\\","")
        if any(x in raw for x in ("timed out","timeout","script failed","verification failed","is_error\": true","<tool_use_error>","aborted","rejected","denied","cancelled","canceled")) or re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+-?[1-9]\d*",raw): return "invalid","provider_failure"
        if "process running with session id" in raw or "script running with cell id" in raw: return "unknown","nonterminal_result"
        if not str(outputs[call]).strip() or str(outputs[call]).strip().lower() in ("ok","{}") or re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+0\b|script completed|success\. updated",raw): return "confirmed","provider_success"
        return ("confirmed","provider_success") if outer=="completed" and not raw.strip('"') else ("unknown","inconclusive_result")
    failed=lambda out:edit_result("result",{"result":out})[0]=="invalid"
    tools=[dict(id=gen_id(src,f"tool:{cid}:{p.get('call_id') or i}"),message_id=anchor(i),tool_name=p["name"],input=json.dumps(args),output=json.dumps(function_out.get(p.get("call_id"),"")),status="failed" if p.get("call_id") in function_out and failed(function_out[p.get("call_id")]) else "complete" if p.get("call_id") in function_out else "pending",duration_ms=None,created_at=timestamps.get(i)) for i,p in items if p.get("type")=="function_call" and (args:=norm_args(p)) is not None]
    custom_out = {p.get("call_id"):p.get("output", "") for _, p in items if p.get("type") == "custom_tool_call_output"}
    tools += [dict(id=gen_id(src, f"custom:{cid}:{i}"), message_id=anchor(i), tool_name=p["name"], input=json.dumps({"code":p.get("input", "")}), output=json.dumps(custom_out.get(p.get("call_id"), "")), status="failed" if any(x in json.dumps(custom_out.get(p.get("call_id"), "")).lower() for x in ("script failed","verification failed")) or re.search(r"exit code: [1-9]\d*",json.dumps(custom_out.get(p.get("call_id"), "")).lower()) else "complete" if p.get("call_id") in custom_out or p.get("status") == "completed" else p.get("status", "pending"), duration_ms=None, created_at=timestamps.get(i)) for i, p in items if p.get("type") == "custom_tool_call"]

    def patch_edits(args):
        """Exact file edits from patches/heredocs; retain commands for redirects with unknown content."""
        cmd,root=" ".join(cmd) if isinstance((cmd:=args.get("cmd") or args.get("command") or ""),list) else str(cmd),args.get("workdir") or meta.get("cwd") or ""
        if "*** Begin Patch" not in cmd:
            head,heredoc,target=cmd.split("\n",1)[0],re.search(r"<<-?\s*'?(\w+)'?",cmd.split("\n",1)[0]),re.search(r"(?:(?<![0-9&])>{1,2}\s*|\btee\s+(?:-a\s+)?)([^\s;|&<>'\"]+)",cmd.split("\n",1)[0])
            if heredoc and target and target.group(1)!="/dev/null" and (body:=re.search(rf"\n(.*)\n{heredoc.group(1)}\s*$",cmd,re.S)): return [(os.path.join(root,target.group(1)),"write",body.group(1),None)]
            if (target:=re.search(r"(?<![0-9&])>{1,2}\s*([^\s;|&<>'\"]+\.[A-Za-z]{1,5})\b",head)) and target.group(1)!="/dev/null": return [(os.path.join(root,target.group(1)),"shell",cmd,None)]
            return []
        parts=re.split(r"(?m)^\*\*\* (Update|Add|Delete) File: (.+)\n",cmd.split("*** Begin Patch",1)[1].split("*** End Patch",1)[0])
        def hunk(text):
            lines=[line for line in text.splitlines() if not line.startswith("***")]
            return [line[1:] if line.startswith((" ","-")) else line for line in lines if not line.startswith("+")],[line[1:] if line.startswith((" ","+")) else line for line in lines if not line.startswith("-")]
        return [(os.path.join(root,path.strip()),op,"\n".join(new),"\n".join(old) or None) for kind,path,body in zip(parts[1::3],parts[2::3],parts[3::3]) for op in [{"Update":"edit","Add":"write","Delete":"delete"}[kind]] for text in re.split(r"(?m)^@@.*(?:\n|$)",body) for old,new in [hunk(text)] if old or new or op!="edit"]

    def custom_edits(p):
        code,names=(code:=p.get("input","")),re.findall(r"await\s+tools\.apply_patch\(\s*(\w+)\s*\)",code)
        if p.get("name") == "apply_patch": return patch_edits({"cmd":code})
        def json_string(value):
            try: return json.loads(value)
            except json.JSONDecodeError: return None
        vals,patches=(vals:={n:v for n,s in re.findall(r"(?:const|let|var)\s+(\w+)\s*=\s*(\"(?:\\.|[^\"\\])*\")",code,re.S) if n in names and (v:=json_string(s)) is not None}),[vals[n] for n in names if n in vals]+[p for s in re.findall(r"await\s+tools\.apply_patch\(\s*(\"(?:\\.|[^\"\\])*\")\s*\)",code,re.S) if (p:=json_string(s)) is not None]
        return [e for patch in patches if "*** Begin Patch" in patch for e in patch_edits({"cmd":patch})]

    candidates,evidence,edits=(candidates:=[(i,p,gen_id(src,f"tool:{cid}:{p.get('call_id') or i}"),edit_result(p.get("call_id"),function_out),patch_edits(args)) for i,p in items if p.get("type")=="function_call" and p.get("name") in ("exec_command","shell_command","shell") and (args:=norm_args(p))]+[(i,p,gen_id(src,f"custom:{cid}:{i}"),edit_result(p.get("call_id"),custom_out,p.get("status")),custom_edits(p)) for i,p in items if p.get("type")=="custom_tool_call"]),[dict(file_edit_id=gen_id(src,f"edit:{cid}:{i}:{j}"),status=status,reason=reason,tool_call_id=tool) for i,p,tool,(status,reason),rows in candidates for j,row in enumerate(rows)],[dict(id=gen_id(src,f"edit:{cid}:{i}:{j}"),message_id=anchor(i),file_path=fp,edit_type=op,content=c,created_at=timestamps.get(i),old_content=o) for i,p,tool,(status,reason),rows in candidates if status=="confirmed" for j,(fp,op,c,o) in enumerate(rows)]

    return {"conv":dict(id=cid,source=src,title=meta.get("cwd") or jsonl.stem,created_at=min(timestamps.values(),default=None),updated_at=max(timestamps.values(),default=None),model=next((m["model"] for m in msgs if m["role"]=="assistant" and m["model"]),None),cwd=meta.get("cwd"),git_branch=(meta.get("git") or {}).get("branch"),project_id=None,metadata=json.dumps({k:v for k,v in {"session_id":provider_id,"parent_session_id":spawn.get("parent_thread_id") or meta.get("parent_thread_id"),"session_kind":"subagent" if subagent is not None else "main","session_kind_evidence":"exact" if subagent is not None else "inferred","agent_name":spawn.get("agent_nickname") or meta.get("agent_nickname"),"agent_role":spawn.get("agent_role") or (subagent if isinstance(subagent,str) else meta.get("agent_role")),"agent_depth":spawn.get("depth"),"originator":meta.get("originator"),"client_version":meta.get("cli_version"),"capture_mode":"transcript","git_repository":(meta.get("git") or {}).get("repository_url"),"git_commit":(meta.get("git") or {}).get("commit_hash"),"forked_from_id":meta.get("forked_from_id"),"thread_source":meta.get("thread_source")}.items() if v is not None})),"msgs":msgs,"tools":tools,"attachs":[image(i,j,b) for i,p,t in mitems for j,b in enumerate(x for x in p.get("content",[]) if isinstance(x,dict) and x.get("type")=="input_image")],"edits":edits,"edit_evidence":evidence}

def parse_codex(codex_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult:
    if not (sessions_dir:=codex_dir/"sessions").exists(): return ParseResult()
    return _parse_sessions(files or sessions_dir.rglob("*.jsonl"),parse_codex_session,bindings)

_CONV_UPS = "INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET source=excluded.source,title=excluded.title,created_at=CASE WHEN conversations.created_at IS NULL OR excluded.created_at < conversations.created_at THEN excluded.created_at ELSE conversations.created_at END,updated_at=CASE WHEN conversations.updated_at IS NULL OR excluded.updated_at > conversations.updated_at THEN excluded.updated_at ELSE conversations.updated_at END,model=COALESCE(excluded.model,conversations.model),cwd=COALESCE(excluded.cwd,conversations.cwd),git_branch=COALESCE(excluded.git_branch,conversations.git_branch),project_id=COALESCE(excluded.project_id,conversations.project_id),metadata=excluded.metadata"

def _id_conflict(rows,fields): return next((rid for rid in {r["id"] for r in rows} if len({tuple(str(r[k]) for k in fields) for r in rows if r["id"]==rid})>1),None)
def _parse_result_refs(conn,r):
    incoming={"conversations":{v["id"] for v in r.convs},"messages":{v["id"] for v in r.msgs},"tool_calls":{v["id"] for v in r.tools},"file_edits":{v["id"] for v in r.edits}}
    for table,needed in (("conversations",{v["conversation_id"] for v in r.msgs}|{v["conversation_id"] for v in r.artifacts}),("messages",{v["message_id"] for v in [*r.tools,*r.attachs,*r.edits]}|{v["parent_id"] for v in r.msgs if v["parent_id"]}),("file_edits",{v["file_edit_id"] for v in r.edit_evidence}),("tool_calls",{v["tool_call_id"] for v in r.edit_evidence if v["tool_call_id"]})):
        existing={v[0] for v in conn.execute(f"SELECT id FROM {table} WHERE id IN (SELECT UNNEST(?))",(list(missing),)).fetchall()} if (missing:=needed-incoming[table]) else set()
        required(not (unavailable:=missing-existing),ValueError(f"parse result reference unavailable: {table}:{next(iter(sorted(unavailable)), '')}"))
def _rows_by_id(conn,table,ids): return {row[0]:row for row in conn.execute(f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",[list(ids)]).fetchall()} if ids else {}
def _history_row(table,row,payload):
    historical=list(row)
    historical[0]=gen_id("history",f"{table}:{row[0]}:{json.dumps(payload,default=str)}")
    if table=="messages": historical[7]=json.dumps({**json.loads(historical[7] or "{}"),"history_of":row[0],"superseded_at":datetime.now().isoformat()})
    return historical
def upsert(conn, r: ParseResult):
    required(not (conflict:=_id_conflict(r.convs,("source","cwd","git_branch","project_id","metadata")) or _id_conflict(r.msgs,("conversation_id","role","content","thinking","created_at","model","metadata","parent_id"))),ValueError(f"divergent provider session in import batch: {conflict}"))
    if uncertain:={v["file_edit_id"] for v in r.edit_evidence if v["status"]!="confirmed"}: r.edit_evidence=[v for v in r.edit_evidence if v["status"]=="confirmed" or v["file_edit_id"] in {e["id"] for e in r.edits}|{x[0] for x in conn.execute("SELECT id FROM file_edits WHERE id IN (SELECT UNNEST(?))",(list(uncertain),)).fetchall()}]
    _parse_result_refs(conn,r)
    quarantined={c["id"] for c in r.convs if c["source"]=="codex" and (meta:=json.loads(c["metadata"] or "{}")).get("session_kind")=="main" and not meta.get("parent_session_id") and any(m["conversation_id"]==c["id"] and m["role"]=="user" for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="user" and not re.fullmatch(_INJECTED_RE,m["content"]) for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="assistant" for m in r.msgs) and not any(x.get("conversation_id")==c["id"] or x.get("message_id") in {m["id"] for m in r.msgs if m["conversation_id"]==c["id"]} for x in [*r.tools,*r.attachs,*r.artifacts,*r.edits])}
    r.convs=[{**c,"metadata":json.dumps({**json.loads(c["metadata"] or "{}"),"capture_mode":"startup-stub-candidate"})} if c["id"] in quarantined else c for c in r.convs]
    cids,mids,bindings=[c["id"] for c in r.convs],[m["id"] for m in r.msgs],[(c["source"],meta["session_id"],c["id"]) for c in r.convs if (meta:=json.loads(c["metadata"] or "{}")).get("session_id")]
    required(not (conflict:=conn.execute("SELECT p.source,p.session_id,p.conversation_id,json_extract_string(j.value,'$.conversation_id') FROM provider_sessions p JOIN json_each(?) j ON p.source=json_extract_string(j.value,'$.source') AND p.session_id=json_extract_string(j.value,'$.session_id') WHERE p.conversation_id<>json_extract_string(j.value,'$.conversation_id') LIMIT 1",(json.dumps([dict(source=s,session_id=i,conversation_id=c) for s,i,c in bindings]),)).fetchone() if bindings else None),ValueError(f"provider session identity conflict: {conflict}"))
    old_convs,old_msgs,new_convs,changed_rows,changed_msgs,updated,changed_conversations=(old_convs:=_rows_by_id(conn,"conversations",cids)),(old_msgs:=_rows_by_id(conn,"messages",mids)),(new_convs:=set(cids)-set(old_convs)),(changed_rows:={m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][:8]+old_msgs[m["id"]][9:]!=tuple(m.values())}),(changed_msgs:={m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][2:5]!=tuple(m[k] for k in ("role","content","thinking"))}),{m["conversation_id"] for m in r.msgs if m["id"] in changed_msgs}-new_convs,[list(c.values()) for c in r.convs if old_convs.get(c["id"])!=tuple(c.values())]
    if changed_conversations: conn.executemany(_CONV_UPS,changed_conversations)
    if bindings: conn.executemany("INSERT INTO provider_sessions VALUES (?,?,?) ON CONFLICT(source,session_id) DO NOTHING",list(dict.fromkeys(bindings)))
    frozen=r.scopes if r.scopes is not None else pending_scopes([(c["id"],c["cwd"]) for c in r.convs])
    if frozen: conn.executemany("INSERT OR IGNORE INTO provenance.conversation_scopes VALUES (?,?,?,?,?,?)",frozen)
    message_history=[_history_row("messages",old,old[2:5]) for m in r.msgs if (old:=old_msgs.get(m["id"])) and old[2:5]!=tuple(m[k] for k in ("role","content","thinking"))]
    changed_msgs|={row[0] for row in message_history}
    if message_history: conn.executemany("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",message_history)
    if changed_rows: conn.executemany(_MSG_UPS,[list(m.values()) for m in r.msgs if m["id"] in changed_rows])
    def replace_preserving(table, rows):
        if not rows: return []
        old,skip,payload,changed,histories=(old:=_rows_by_id(conn,table,[r["id"] for r in rows])),(skip:={"tool_calls":7,"attachments":7,"artifacts":6,"file_edits":5}[table]),(payload:=lambda values:tuple(v for i,v in enumerate(values) if i not in (0,skip))),(changed:=[(list(row.values()),old.get(row["id"])) for row in rows if not old.get(row["id"]) or payload(old[row["id"]])!=payload(tuple(row.values()))]),[_history_row(table,previous,payload(previous)) for values,previous in changed if previous]
        if histories: conn.executemany(f"INSERT INTO {table} VALUES ({','.join('?'*len(histories[0]))}) ON CONFLICT DO NOTHING",histories)
        if changed: conn.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({','.join('?'*len(changed[0][0]))})",[values for values,previous in changed])
        return [(table,values[0]) for values,previous in changed]+[(table,row[0]) for row in histories]
    historical=[item for table,rows in (("tool_calls",r.tools),("attachments",r.attachs),("artifacts",r.artifacts),("file_edits",r.edits)) for item in replace_preserving(table,rows)]
    evidence_row,evidence,classified,incoming=(evidence_row:=lambda v:(v["file_edit_id"],v["status"],v["reason"],v["tool_call_id"])),{v[0]:v for v in conn.execute("SELECT * FROM provenance.file_edit_evidence WHERE file_edit_id IN (SELECT UNNEST(?))",[[v["file_edit_id"] for v in r.edit_evidence]+[e["id"] for e in r.edits]]).fetchall()} if r.edit_evidence or r.edits else {},(classified:={v["file_edit_id"] for v in r.edit_evidence}),[*r.edit_evidence,*(dict(file_edit_id=e["id"],status="unknown",reason="unclassified_input",tool_call_id=None) for e in r.edits if e["id"] not in classified)]
    required(all(set(v)=={"file_edit_id","status","reason","tool_call_id"} and v["status"] in {"confirmed","invalid","unknown","unverified"} and v["file_edit_id"] and v["reason"] for v in incoming),ValueError("invalid file edit evidence"))
    tool_ids={v["tool_call_id"] for v in incoming if v["tool_call_id"]}
    required(not tool_ids or conn.execute("SELECT COUNT(*) FROM tool_calls WHERE id IN (SELECT UNNEST(?))",[list(tool_ids)]).fetchone()[0]==len(tool_ids),ValueError("file edit evidence tool call unavailable"))
    if incoming: conn.executemany("INSERT OR REPLACE INTO provenance.file_edit_evidence VALUES (?,?,?,?)",list(map(evidence_row,incoming)))
    evidence_changed={v["file_edit_id"] for v in incoming if evidence.get(v["file_edit_id"])!=evidence_row(v) and conn.execute("SELECT 1 FROM file_edits WHERE id=?",[v["file_edit_id"]]).fetchone()}
    statuses={v["file_edit_id"]:v["status"] for v in incoming}
    (frozen_edits:=r.edit_scopes) is not None or (frozen_edits:=pending_edit_scopes(edit_scope_inputs(r)))
    if frozen_edits: conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",frozen_edits)
    for attachment in (a for a in r.attachs if a.get("path")): index_attachment_body(conn,attachment["id"],attachment["path"],attachment.get("size"))
    changed_convs={row_id for row_id,row in _rows_by_id(conn,"conversations",cids).items() if old_convs.get(row_id)!=row}
    archive_msgs=changed_rows|changed_msgs-set(mids)
    if changed_convs or archive_msgs or historical or evidence_changed: _archive_touch(conn,[("conversations",x) for x in changed_convs]+[("messages",x) for x in archive_msgs]+historical+[("file_edits",x) for x in evidence_changed])
    r.provenance_edits,r.provenance_conversations={x["id"] for x in r.edits if statuses.get(x["id"])=="confirmed"}&({i for t,i in historical if t=="file_edits"}|evidence_changed),changed_convs
    return len(r.convs), len(r.msgs), len(r.tools), len(r.attachs), len(r.edits), len(new_convs), len(updated), changed_msgs

def hook_root(source): return Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"projects" if source == "claude-code" else Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"sessions"
def hook_result(source,path,bindings=None):
    session=(parse_claude_code_session if source=="claude-code" else parse_codex_session)(path,bindings)
    return ParseResult(convs=[session["conv"]],msgs=session["msgs"],tools=session["tools"],attachs=session["attachs"],edits=session["edits"],edit_evidence=session["edit_evidence"]) if session else ParseResult()
def enqueue_hook(source, payload):
    path,root=Path(payload["transcript_path"]).expanduser().resolve(),hook_root(source).expanduser().resolve()
    if source not in ("claude-code", "codex") or path.suffix != ".jsonl" or not path.is_relative_to(root): raise ValueError(f"Invalid {source} transcript path")
    st,key=path.stat(),gen_id("hook",f"{source}:{path}")
    atomic_json(HOOK_DIR/f"{key}.json",dict(source=source,path=str(path),mtime=st.st_mtime_ns,size=st.st_size))
    subprocess.Popen([sys.executable, "-m", "ai_convos", "drain-hooks", "--no-block"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
def retry_hook(work, force=False):
    q,target=(q:=work.with_suffix(".json")),q if q.exists() else work
    if force: atomic_json(target,{**json.loads(target.read_text()),"retry":True})
    work.unlink(missing_ok=True) if q.exists() else os.replace(work, q)
def merge_embed_dirty(ids):
    old=set(json.loads(HOOK_EMBED_DIRTY.read_text())) if HOOK_EMBED_DIRTY.exists() else set()
    atomic_json(HOOK_EMBED_DIRTY,sorted(old|set(ids)))
def mark_dirty(ids):
    if not ids: return
    with _locked(HOOK_DIR/".lock"): (HOOK_FTS_DIRTY.touch(),merge_embed_dirty(ids))
def drain_hooks(embed=False, local_only=False,block=False):
    HOOK_DIR.mkdir(parents=True,exist_ok=True)
    done,claims,failed,started=[],[],0,time.monotonic()
    with (HOOK_DIR/".drain.lock").open("w") as drain:
        try: fcntl.flock(drain,fcntl.LOCK_EX|(0 if block else fcntl.LOCK_NB))
        except BlockingIOError: return 0
        with _locked(HOOK_DIR/".lock"):
            state=json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {}
            [done.append((work,work.stem,event["snap"],set(event["changed"]))) if "changed" in event else retry_hook(work,True) for work in HOOK_DIR.glob("*.work") for event in [json.loads(work.read_text())]]
            claims=[work for queue in sorted(HOOK_DIR.glob("*.json"),key=lambda p:(p.stat().st_mtime_ns,p.name))[:HOOK_DRAIN_EVENTS] for work in [queue.with_suffix(".work")] if os.replace(queue,work) is None]
        if claims:
            with _core(ready=True) as conn: bindings=session_bindings(conn)
        for n,work in enumerate(claims):
            if n and time.monotonic()-started>=HOOK_DRAIN_SECONDS: break
            try:
                e,path,key,st,snap=(e:=json.loads(work.read_text())),(path:=Path(e["path"])),work.stem,(st:=path.stat()),[st.st_mtime_ns,st.st_size]
                if state.get(key)==snap:
                    work.unlink()
                    continue
                r,st2=hook_result(e["source"],path,bindings),path.stat()
                if snap!=[st2.st_mtime_ns,st2.st_size]:
                    with _locked(HOOK_DIR/".lock"): retry_hook(work)
                    continue
                if not r.convs:
                    done.append((work,key,snap,set()))
                    continue
                with _core(ready=True) as conn,_transaction(conn): changed=upsert(conn,r)[-1]|({m["id"] for m in r.msgs} if e.get("retry") else set())
                capture_provenance(edit_ids=[x["id"] for x in r.edits],conversation_ids=[x["id"] for x in r.convs],source=f"{e['source']}.hook")
                atomic_json(work,{**e,"snap":snap,"changed":sorted(changed)})
                done.append((work,key,snap,changed))
            except FileNotFoundError: work.unlink(missing_ok=True)
            except Exception as error:
                with _locked(HOOK_DIR/".lock"): retry_hook(work)
                failed+=1
                log_parse_error(f"hook inbox {work}",error)
        if done:
            with _locked(HOOK_DIR/".lock"):
                dirty=set().union(*(d for _,_,_,d in done))
                if dirty: (HOOK_FTS_DIRTY.touch(),merge_embed_dirty(dirty))
                state.update((key,snap) for _,key,snap,_ in done)
                atomic_json(HOOK_STATE,state)
                for work,_,_,_ in done: work.unlink(missing_ok=True)
    atomic_json(HOOK_PROGRESS,dict(completed_at=time.time_ns(),processed=len(done),failed=failed,pending=len(pending:=[*HOOK_DIR.glob("*.json"),*HOOK_DIR.glob("*.work")]),oldest=min((p.stat().st_mtime_ns for p in pending),default=None)))
    if pending and len(pending)>failed: subprocess.Popen([sys.executable,"-m","ai_convos","drain-hooks","--no-block"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    if embed and semantic_enabled():
        try: embed_hook_pending(local_only=local_only)
        except Exception as e: log_parse_error("hook embeddings", e)
    return len(done)

def flush_fts():
    HOOK_DIR.mkdir(parents=True, exist_ok=True)
    with _locked(HOOK_DIR/".fts.lock"):
        with _locked(HOOK_DIR/".lock"):
            claims=list(DATA_DIR.glob(f".{HOOK_FTS_DIRTY.name}.*"))
            if claims: (HOOK_FTS_DIRTY.touch(),[path.unlink(missing_ok=True) for path in claims])
            if not HOOK_FTS_DIRTY.exists(): return False
            claim=HOOK_FTS_DIRTY.with_name(f".{HOOK_FTS_DIRTY.name}.{os.getpid()}")
            os.replace(HOOK_FTS_DIRTY,claim)
        try:
            with _core(wait=0) as conn: rebuild_fts_index(conn)
        except BaseException:
            HOOK_FTS_DIRTY.touch()
            raise
        finally: claim.unlink(missing_ok=True)
    return True

_MODELS,_MCFG,_LLAMA_LOG,_SEMANTIC_INSTALL={},dict(repo_id="ggml-org/embeddinggemma-300m-qat-q8_0-GGUF",filename="embeddinggemma-300m-qat-q8_0.gguf",revision="66f974f8cd48cc3b9c41c516b95508e75b4bee64",artifact_sha256="6fa0c02a9c302be6f977521d399b4de3a46310a4f2621ee0063747881b673f67",embedding=True,n_ctx=16384,n_batch=2048,n_ubatch=2048,n_seq_max=8,n_gpu_layers=-1),None,"Semantic runtime unavailable. macOS includes it; elsewhere install `convos[semantic]`, set CONVOS_SEMANTIC=llama, then run `convos embed`. Literal `convos search` needs no model."
def semantic_enabled(): return (mode:=os.environ.get("CONVOS_SEMANTIC","auto").lower()) not in ("0","false","no","off") and (mode!="auto" or sys.platform=="darwin")
def semantic_backend():
    mode=os.environ.get("CONVOS_SEMANTIC","auto").lower()
    required(mode not in ("0","false","no","off"),ValueError("Semantic retrieval is disabled by CONVOS_SEMANTIC=0; use `convos search` for literal retrieval."))
    required(mode in ("auto","1","true","yes","on","llama"),ValueError("CONVOS_SEMANTIC must be auto, llama, or 0"))
    required(mode!="auto" or sys.platform=="darwin",ValueError(_SEMANTIC_INSTALL))
    return "llama"
_EPROFILES={"llama":{"backend":"llama.cpp","model":_MCFG["repo_id"],"revision":_MCFG["revision"],"artifact":_MCFG["filename"],"artifact_sha256":_MCFG["artifact_sha256"],"dimensions":768,"pooling":"model","normalization":"l2","character_limit":1600,"token_limit":_MCFG["n_ctx"],"query_prefix":"task: search result | query: ","document_prefix":"task: search result | document: "}}
def embedding_profile(): return _EPROFILES[semantic_backend()]
def embedding_model_path(local_only=False):
    try: from huggingface_hub import hf_hub_download
    except ImportError as e: raise ValueError(_SEMANTIC_INSTALL) from e
    path=hf_hub_download(_MCFG["repo_id"],_MCFG["filename"],revision=_MCFG["revision"],local_files_only=local_only)
    return required(_file_sha256(path)==_MCFG["artifact_sha256"],ValueError("Semantic model artifact hash mismatch")) and path
def _llama(local_only=False):
    global _LLAMA_LOG
    if "llama" not in _MODELS:
        try:
            Llama,lc=__import__("llama_cpp").Llama,__import__("llama_cpp.llama_cpp",fromlist=["*"])
        except ImportError as e: raise ValueError(_SEMANTIC_INSTALL) from e
        __import__("warnings").filterwarnings("ignore",message="The `local_dir_use_symlinks` argument is deprecated.*",category=UserWarning)
        if _LLAMA_LOG is None: lc.llama_log_set((_LLAMA_LOG:=lc.llama_log_callback(lambda *_:None)),None)
        nseq,cfg=_MCFG["n_seq_max"],{k:v for k,v in _MCFG.items() if k not in ("repo_id","filename","revision","artifact_sha256","n_seq_max")}
        if nseq: lc.llama_context_default_params=lambda o=(orig:=lc.llama_context_default_params),n=nseq:(setattr(p:=o(),"n_seq_max",n) or p)
        try: _MODELS["llama"] = Llama(model_path=embedding_model_path(local_only), **cfg, verbose=False)
        finally: lc.llama_context_default_params=orig if nseq else lc.llama_context_default_params
    return _MODELS["llama"]
def _activate_embedding_profile():
    profile=embedding_profile()
    with _core(ready=True) as conn:
        old,changed,wrong=(old:=(conn.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state WHERE singleton").fetchone() or [None])[0]),old is not None and json.loads(old)!=profile,conn.execute("SELECT COUNT(*) FROM messages WHERE embedding IS NOT NULL AND len(embedding)<>?",(profile["dimensions"],)).fetchone()[0]
        with _transaction(conn):
            if changed or wrong: conn.execute("UPDATE messages SET embedding=NULL WHERE embedding IS NOT NULL AND (? OR len(embedding)<>?)",(changed,profile["dimensions"]))
            conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",(json.dumps(profile,sort_keys=True,separators=(",",":")),))
    return profile,changed or bool(wrong)
def embed_texts(ss: list[str], doc: bool = False, local_only=False) -> list[list[float]]:
    profile=embedding_profile()
    texts=[profile["document_prefix" if doc else "query_prefix"]+(s or "")[:profile["character_limit"]] for s in ss]
    vectors=[d["embedding"] for d in _llama(local_only).create_embedding(texts)["data"]]
    required(all(len(v)==profile["dimensions"] for v in vectors),ValueError("Embedding runtime returned the wrong dimensions"))
    return vectors
def embed_text(s: str, doc: bool = False, local_only=False) -> list[float]: return embed_texts([s], doc, local_only)[0]
def embed_pending(batch: int = 32, ids=None, local_only=False):
    if ids == []: return
    profile,reset=_activate_embedding_profile()
    ids,Q,ps=(ids:=None if reset else ids),(Q:="FROM messages WHERE embedding IS NULL AND content IS NOT NULL AND content != ''"+_NOISE+(f" AND id IN ({','.join(['?']*len(ids))})" if ids is not None else "")),ids or []
    with _core(read_only=True) as conn: n=conn.execute(f"SELECT COUNT(*) {Q}",ps).fetchone()[0]
    if not n: return
    typer.echo(f"Embedding {n} messages...",err=True)
    done=0
    while True:
        with _core(read_only=True) as conn: rows=conn.execute(f"SELECT id,content {Q} ORDER BY LEAST(length(content),1600) LIMIT ?",ps+[batch]).fetchall()
        if not rows: break
        step=_MCFG["n_seq_max"] if profile["backend"]=="llama.cpp" else batch
        updates=[(vector,mid) for chunk in (rows[i:i+step] for i in range(0,len(rows),step)) for (mid,_),vector in zip(chunk,embed_texts([content for _,content in chunk],doc=True,local_only=local_only))]
        with _core() as conn: conn.executemany("UPDATE messages SET embedding=? WHERE id=? AND embedding IS NULL",updates)
        typer.echo(f"  {(done:=done+len(rows))}/{n}\r",nl=False,err=True)
    typer.echo(err=True)
def embed_hook_pending(all_msgs=False, batch=32, local_only=False):
    HOOK_DIR.mkdir(parents=True, exist_ok=True)
    with _locked(HOOK_DIR/".embed.lock"):
        claim=HOOK_EMBED_DIRTY.with_name(f".{HOOK_EMBED_DIRTY.name}.{os.getpid()}")
        with _locked(HOOK_DIR/".lock"):
            if HOOK_EMBED_DIRTY.exists(): os.replace(HOOK_EMBED_DIRTY, claim)
            elif not all_msgs: return
        ids = json.loads(claim.read_text()) if claim.exists() else []
        try: embed_pending(batch, None if all_msgs else ids, local_only)
        except BaseException:
            with _locked(HOOK_DIR/".lock"): merge_embed_dirty(ids)
            raise
        finally: claim.unlink(missing_ok=True)

def _ro():
    try: c=get_db(read_only=True)
    except ValueError as e: return typer.echo(str(e),err=True)
    if c is None: return typer.echo("Database not found. Run `convos init` or `convos sync`.")
    if not ensure_db_ready(c): return c.close()
    return c
def _hybrid_ro():
    if (c := _ro()) is None: return None
    if c.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone(): return c
    c.close()
    with _core(ready=True): pass
    return _ro()
def _fts_ro(hybrid=False):
    try: flush_fts()
    except Exception as e: typer.echo(f"FTS refresh failed; using last indexed snapshot: {e}", err=True)
    if (c := _hybrid_ro() if hybrid else _ro()) is None: return None
    try: load_fts(c)
    except ValueError as e: return typer.echo(str(e)) if c.close() is None else None
    if not c.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone():
        if HOOK_FTS_DIRTY.exists(): return typer.echo("FTS index unavailable until its first refresh can acquire the database",err=True) if c.close() is None else None
        c.close()
        with _core() as writer: load_fts(writer) or ensure_fts_index(writer)
        load_fts(c:=_ro())
    return c
def _filt(source, days, role, cwd=None, conversation=None):
    w,p=map(list,zip(*pairs)) if (pairs:=[(q,v) for v,q in ((source,"c.source = ?"),(datetime.now()-timedelta(days=days) if days else None,"m.created_at > ?"),(role,"m.role = ?")) if v]) else ([],[])
    if cwd:
        raw,resolved,w,p=(raw:=str(Path(cwd).expanduser().absolute())),(resolved:=str(Path(cwd).expanduser().resolve())),[*w,"(c.cwd=? OR starts_with(c.cwd,?) OR c.cwd=? OR starts_with(c.cwd,?))"],[*p,raw,raw.rstrip("/")+"/",resolved,resolved.rstrip("/")+"/"]
    conversation and (w.append("starts_with(c.id,?)"),p.append(conversation))
    return w, p
def _clip(s, n): return (s or "")[:n] + ("..." if s and len(s) > n else "")
def _fmt_hit(content, ts, role, title, src, cid, cwd, q, ctx, meta):
    p = _clip(content, ctx)
    for w in q.split(): p = re.sub(f"({re.escape(w)})", r"\033[1;33m\1\033[0m", p, flags=re.I)
    typer.echo(f"\n{'='*60}\n[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid[:8]})\n{role} @ {ts or '?'} ({meta})\n{'-'*40}\n{p}")

def emit(data,fmt): [typer.echo(json.dumps(row,default=str)) for row in data if fmt=="jsonl" and isinstance(data,list)] if fmt=="jsonl" and isinstance(data,list) else typer.echo(json.dumps(data,default=str))

def capture(source: str):
    try: enqueue_hook(source, json.loads(sys.stdin.read() or "{}"))
    except Exception as e: log_parse_error(f"{source} hook", e)

def drain_hooks_cmd(block:bool=typer.Option(False,"--block/--no-block",hidden=True)): drain_hooks(block=block)

def init():
    with _core(ready=True) as conn: rebuild_fts_index(conn)
    HOOK_FTS_DIRTY.unlink(missing_ok=True)
    sync(False,300,True,True,False,False,True)
    install_skills()
    install_hooks(False,False)
    for ep in entry_points(group="convos.init"): typer.echo(ep.load()())
    typer.echo(f"Database initialized at {DB_PATH}")

def search(query: str, source: str|None = typer.Option(None, "-s"), days: int|None = typer.Option(None, "-d"), role: str|None = typer.Option(None, "-r"), cwd: Path|None = typer.Option(None, "--cwd", "-w"), conversation: str|None = typer.Option(None, "--conversation"), thinking: bool = typer.Option(False, "--thinking", "-t"), limit: int = typer.Option(20, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _fts_ro()) is None: return
    with contextlib.closing(conn):
        w,p=_filt(source,days,role,cwd,conversation)
        results=conn.execute(f"""SELECT m.id, m.content, m.thinking, m.role, m.created_at, fts_main_messages.match_bm25(m.id, ?) as score, c.title, c.source, c.id, c.cwd
            FROM messages m JOIN conversations c ON m.conversation_id = c.id WHERE score IS NOT NULL{' AND ' + ' AND '.join(w) if w else ''}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY score DESC)=1 ORDER BY score DESC LIMIT ?""",[query]+p+[limit]).fetchall()
    if fmt!="text": return emit([dict(message_id=mid,role=r,content=_clip(content,context),thinking=_clip(think,context) if thinking and think else None,created_at=ts,score=score,title=title,source=src,conversation_id=cid,cwd=cwd) for mid,content,think,r,ts,score,title,src,cid,cwd in results],fmt)
    if not results: return typer.echo("No results")
    [(_fmt_hit(content,ts,role,title,source,cid,cwd,query,context,f"score: {score:.2f}"),thinking and think and typer.echo(f"\n[THINKING]\n{_clip(think,context)}")) for _,content,think,role,ts,score,title,source,cid,cwd in results]
    typer.echo(f"\n{len(results)} results")

def read_cmd(conversation: str, limit: int = typer.Option(20, "-n", min=1), context: int = typer.Option(2000, "-c", min=1), around: str|None = typer.Option(None, "--around", "-a"), thinking: bool = typer.Option(False, "--thinking", "-t"), fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _ro()) is None: return
    with contextlib.closing(conn):
        cs=conn.execute("SELECT id,title,source,cwd FROM conversations WHERE starts_with(id, ?) ORDER BY updated_at DESC NULLS LAST LIMIT 2",[conversation]).fetchall()
        if len(cs)!=1: raise typer.Exit(typer.echo("No matching conversation" if not cs else "Ambiguous prefix: "+", ".join(c[0] for c in cs),err=True) or 1)
        cid,title,src,cwd=cs[0]
        base=f"SELECT m.id,m.role,m.content,m.thinking,m.created_at,ROW_NUMBER() OVER (ORDER BY {MESSAGE_ORDER}) pos FROM messages m WHERE m.conversation_id=? AND json_extract_string(m.metadata,'$.history_of') IS NULL AND (COALESCE(m.content,'')!='' OR COALESCE(m.thinking,'')!='')"
        if around and len(mids:=conn.execute("SELECT id FROM messages WHERE conversation_id=? AND starts_with(id,?) AND json_extract_string(metadata,'$.history_of') IS NULL LIMIT 2",[cid,around]).fetchall())!=1: raise typer.Exit(typer.echo("No matching message" if not mids else "Ambiguous message prefix: "+", ".join(m[0] for m in mids),err=True) or 1)
        rows=conn.execute(f"WITH b AS ({base}),t AS (SELECT pos FROM b WHERE id=?) SELECT id,role,content,thinking,created_at FROM (SELECT b.*,abs(b.pos-t.pos) d FROM b,t ORDER BY d,b.pos LIMIT ?) ORDER BY pos",[cid,mids[0][0],limit]).fetchall() if around else conn.execute(f"SELECT id,role,content,thinking,created_at FROM ({base}) ORDER BY pos DESC LIMIT ?",[cid,limit]).fetchall()[::-1]
    data = [dict(id=mid, role=role, content=_clip(content, context), thinking=_clip(think, context) if thinking and think else None, created_at=ts) for mid, role, content, think, ts in rows]
    if fmt!="text": return emit(data,fmt)
    typer.echo(f"[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid})")
    for message in data: typer.echo(f"\n{message['role']} @ {message['created_at'] or '?'}\n{message['content']}{f'''\n[THINKING]\n{message['thinking']}''' if message['thinking'] else ''}")
    typer.echo(f"\n{len(data)} messages")

def hybrid_hits(q, source=None, days=None, role=None, limit=10, local_only=False, cwd=None, conversation=None):
    drain_hooks(embed=True,local_only=local_only)
    if DB_PATH.is_file(): _activate_embedding_profile()
    conn=_fts_ro(True)
    if conn is None: raise ValueError("Archive retrieval is unavailable")
    with contextlib.closing(conn):
        if not conn.execute("SELECT COUNT(*) FROM messages WHERE embedding IS NOT NULL").fetchone()[0]: raise ValueError("No embeddings yet. Run `convos embed`, or use `convos search` for BM25 only.")
        try: qv=embed_text(q,False,True) if local_only else embed_text(q,False)
        except Exception as e: raise ValueError(f"Hybrid embedding failed: {e}") from e
        w,p=_filt(source,days,role,cwd,conversation)
        rows=conn.execute(f"""WITH qe AS (SELECT ?::FLOAT[] AS v),
            base AS (SELECT m.id, m.embedding FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.content IS NOT NULL{_NOISE}{' AND ' + ' AND '.join(w) if w else ''}),
            fts AS (SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS r FROM (SELECT id, fts_main_messages.match_bm25(id, ?) AS score FROM base) s WHERE score IS NOT NULL LIMIT 50),
            vec AS (SELECT b.id, ROW_NUMBER() OVER (ORDER BY list_cosine_similarity(b.embedding, qe.v) DESC) AS r FROM base b, qe WHERE b.embedding IS NOT NULL AND len(b.embedding)=len(qe.v) LIMIT 50),
            fused AS (SELECT id, SUM(1.0/(60+r)) AS rrf FROM (SELECT id, r FROM fts UNION ALL SELECT id, r FROM vec) GROUP BY id)
            SELECT fused.rrf, m.id, m.role, m.content, m.created_at, c.title, c.source, c.id, c.cwd FROM fused JOIN messages m ON m.id = fused.id JOIN conversations c ON c.id = m.conversation_id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY fused.rrf DESC)=1 ORDER BY fused.rrf DESC LIMIT ?""",[qv]+p+[q,limit]).fetchall()
    return [dict(score=score,message_id=mid,role=r,content=content,created_at=ts,title=title,source=src,conversation_id=cid,cwd=cwd) for score,mid,r,content,ts,title,src,cid,cwd in rows]
def query_cmd(q: str, source: str|None = typer.Option(None, "-s"), days: int|None = typer.Option(None, "-d"), role: str|None = typer.Option(None, "-r"), cwd: Path|None = typer.Option(None, "--cwd", "-w"), conversation: str|None = typer.Option(None, "--conversation"), limit: int = typer.Option(10, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format")):
    try: rows = hybrid_hits(q, source, days, role, limit, cwd=cwd, conversation=conversation)
    except ValueError as e: return typer.echo(str(e),err=True)
    if not rows: return typer.echo("No results")
    if fmt!="text": return emit([{**r,"content":_clip(r["content"],context)} for r in rows],fmt)
    for x in rows: _fmt_hit(x["content"], x["created_at"], x["role"], x["title"], x["source"], x["conversation_id"], x["cwd"], q, context, f"score: {x['score']:.4f}")
    typer.echo(f"\n{len(rows)} results")

def embed_cmd(batch: int = typer.Option(32, "-b")):
    with _core(ready=True): pass
    try:
        embed_hook_pending(True,batch)
        typer.echo("Embeddings ready")
    except Exception as e: typer.echo(f"Embedding failed: {e}", err=True)

def doctor(verbose: bool = typer.Option(False, "-v")):
    typer.echo(f"convos: {version('convos')}")
    pending,state,progress,last,age,dirty,claims=(pending:=len(list(HOOK_DIR.glob("*.json")))+len(list(HOOK_DIR.glob("*.work")))),(state:=json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {}),(progress:=json.loads(HOOK_PROGRESS.read_text()) if HOOK_PROGRESS.exists() else {}),max((v[0] for v in state.values()),default=0),max(0,time.time_ns()-(progress.get("oldest") or time.time_ns()))/1e9 if pending else 0,len(json.loads(HOOK_EMBED_DIRTY.read_text())) if HOOK_EMBED_DIRTY.exists() else 0,len(list(DATA_DIR.glob(f".{HOOK_EMBED_DIRTY.name}.*")))
    typer.echo(f"ingest: pending={pending}, embedding_ids={dirty}, embedding_claims={claims}, last={datetime.fromtimestamp(last/1e9).isoformat(timespec='seconds') if last else 'never'}, oldest={age:.0f}s, last_batch={progress.get('processed',0)} ok/{progress.get('failed',0)} failed")
    if DB_PATH.exists():
        try:
            with _core(read_only=True) as conn: cols,required,missing,(convs,msgs,unembedded,latest),fts,evidence=(cols:=set(conn.execute("SELECT table_name,column_name FROM information_schema.columns").fetchall())),(required:={"conversations":("id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"),"messages":("id","conversation_id","role","content","thinking","created_at","model","metadata","embedding","parent_id"),"tool_calls":("id","message_id","tool_name","input","output","status","duration_ms","created_at"),"attachments":("id","message_id","filename","mime_type","size","path","url","created_at"),"artifacts":("id","conversation_id","artifact_type","title","content","language","created_at","version"),"file_edits":("id","message_id","file_path","edit_type","content","created_at","old_content"),"file_edit_evidence":("file_edit_id","status","reason","tool_call_id")}),(missing:=[f"{table}.{column}" for table,columns in required.items() for column in columns if (table,column) not in cols]),conn.execute(f"SELECT (SELECT COUNT(*) FROM conversations),(SELECT COUNT(*) FROM messages),(SELECT COUNT(*) FROM messages WHERE embedding IS NULL AND COALESCE(content,'')!=''{_NOISE}),(SELECT MAX(updated_at) FROM conversations)").fetchone() if not missing else (0,0,0,None),bool(conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()),dict(conn.execute("SELECT status,COUNT(*) FROM provenance.file_edit_evidence GROUP BY status").fetchall()) if not missing else {}
            typer.echo(f"archive: {convs} convs, {msgs} msgs, {unembedded} unembedded, {DB_PATH.stat().st_size/1024**3:.1f} GB, latest={latest or 'never'}, schema={'ready' if not missing else 'missing:' + ','.join(missing)}, fts={'yes' if fts else 'no'}, edit_evidence="+",".join(f"{k}:{evidence.get(k,0)}" for k in ("confirmed","unknown","invalid","unverified")))
            if missing or not fts: typer.echo("repair: convos init")
            elif unembedded: typer.echo("repair: convos embed")
        except Exception as e: typer.echo(f"archive: unavailable ({e})")
    else: typer.echo(f"archive: missing ({DB_PATH})\nrepair: convos init")
    paths,expected,current=(paths:=_skill_paths()),(expected:=paths[1].read_text() if paths[1].exists() else None),sum(expected is not None and p.is_file() and not p.is_symlink() and p.read_text()==expected for p in paths[3])
    typer.echo(f"skills: {current}/2 current")
    if current!=2: typer.echo("repair: convos install-skills")
    install_hooks(status=True)
    for ep in entry_points(group="convos.doctor"):
        try: typer.echo(ep.load()())
        except Exception as e: typer.echo(f"{ep.name}: unavailable ({e})")
    targets = ["chatgpt.com", "chat.openai.com", "openai.com", "claude.ai"]
    for name, getter in [("safari", safari_cookie_domains), ("chrome", chrome_cookie_domains)]:
        try: domains = getter()
        except PermissionError: domains=typer.echo(f"{name}: no access to cookies")
        if domains is None: continue
        typer.echo(f"{name}: "+", ".join(f"{target}={'yes' if any(target in domain or domain in target for domain in domains) else 'no'}" for target in targets))
        if verbose: typer.echo(f"{name}: chatgpt cookies={len(cg:=read_safari_cookies('chatgpt.com') if name=='safari' else read_chrome_cookies('chatgpt.com'))} keys={','.join(sig) if (sig:=[k for k in ['__Secure-next-auth.session-token','__Secure-next-auth.session-token.0','__Secure-next-auth.session-token.1','cf_clearance','__cf_bm'] if k in cg]) else 'none'}")

def _skill_paths():
    rel,shares,roots,skill,homes=(rel:=Path("skills")/"convos"/"SKILL.md"),(shares:=[Path(p)/"share"/"convos" for p in (sysconfig.get_paths().get("data",""),site.getuserbase())]),(roots:=[PROJECT_ROOT,Path(__file__).resolve().parents[2],*shares]),next((r/rel for r in roots if (r/rel).exists()),roots[-1]/rel),[Path(os.environ.get("CODEX_HOME",Path.home()/".codex")),Path(os.environ.get("CLAUDE_CONFIG_DIR",Path.home()/".claude"))]
    return rel,skill,homes,[home/rel for home in homes]
def install_skills():
    rel,skill,homes,dests=_skill_paths()
    olds=[home/"skills"/"agent-convos"/"SKILL.md" for home in homes]
    if not skill.exists(): raise typer.Exit(typer.echo(f"Missing skill: {skill}",err=True) or 1)
    text,legacy,resolved=(text:=skill.read_text()),text.replace("name: convos","name: agent-convos",1).replace("# Convos","# Agent Convos",1),[Path(os.path.realpath(p)) for p in dests]
    if unsafe:=next((p for home,p,target in zip(homes,dests,resolved) if p.is_symlink() or p.exists() and not p.is_file() or any(q.is_symlink() and resolved.count(target)<2 or q.exists() and not q.is_dir() for q in [home/Path(*rel.parts[:i]) for i in range(1,len(rel.parts))])),None): raise typer.Exit(typer.echo(f"Refusing unsafe managed file: {unsafe}",err=True) or 1)
    for dest,old in zip(dests,olds):
        if atomic_write(dest,text) is None: typer.echo(f"Installed {dest}")
        if old.is_file() and not old.is_symlink() and old.read_text()==legacy: typer.echo(f"Removed legacy {old}") if old.unlink() is None else None

def _capture_command(source):
    root=Path(os.environ.get("CONVOS_PROJECT_ROOT",PROJECT_ROOT)).expanduser().resolve()
    return f"{f'CONVOS_PROJECT_ROOT={shlex.quote(str(root))} ' if root!=Path.home()/'.convos' else ''}{shlex.quote(str(Path(sys.executable).with_name('convos')))} capture {source}"
def _managed_hook(h, source): return h.get("command", "").endswith("convos remote hook") or h.get("command", "").endswith((f" hook {source}", f" capture {source}")) and h.get("statusMessage") in ("Updating conversation archive", "Saving conversation to Convos")
def edit_hook_config(path, events, source, remove=False):
    data=json.loads(path.read_text()) if path.exists() else {}
    clean=lambda groups:[{**group,"hooks":kept} for group in groups for kept in [[h for h in group.get("hooks",[]) if not _managed_hook(h,source)]] if kept]
    hooks={event:kept for event,groups in data.get("hooks",{}).items() if (kept:=clean(groups))}
    data["hooks"]=hooks
    if not remove:
        cmd = _capture_command(source)
        for event in events: hooks.setdefault(event, []).append(dict(hooks=[dict(type="command", command=cmd, timeout=5, statusMessage="Saving conversation to Convos")]))
    return data, sum(_managed_hook(h, source) for gs in hooks.values() for g in gs for h in g.get("hooks", []))

def install_hooks(remove: bool = typer.Option(False, "--remove"), status: bool = typer.Option(False, "--status")):
    cfgs = [(Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"settings.json", ("Stop", "SessionEnd"), "claude-code"), (Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"hooks.json", ("Stop",), "codex")]
    if not status and (unsafe := next((p for p,_,_ in cfgs if p.is_symlink() or p.exists() and not p.is_file() or p.parent.exists() and not p.parent.is_dir()), None)): raise typer.Exit(typer.echo(f"Refusing unsafe managed file: {unsafe}",err=True) or 1)
    if not status: cfgs = [(path,events,source,*edit_hook_config(path, events, source, remove)) for path,events,source in cfgs]
    for path, events, source, *planned in cfgs:
        if status:
            expected,data,n=(expected:=_capture_command(source)),(data:=json.loads(path.read_text()) if path.exists() else {}),sum(sum(h.get("command")==expected and h.get("statusMessage")=="Saving conversation to Convos" for g in data.get("hooks",{}).get(event,[]) for h in g.get("hooks",[]))==1 for event in events)
            n*=sum(_managed_hook(h,source) for groups in data.get("hooks",{}).values() for group in groups for h in group.get("hooks",[]))==len(events)
        else:
            data,n=planned
            atomic_json(path,data)
        typer.echo(f"{source}: {n} hook{'s' if n != 1 else ''}{' installed' if not status and not remove else ''} ({path})" + ("; repair: convos install-hooks" if status and n != len(events) else ""))
    if not status and not remove: typer.echo("Start a new agent session; in Codex, review the user hook with `/hooks`.")

def export(output: Path, fmt: str = typer.Option("json", "-f"), source: str|None = typer.Option(None, "-s")):
    if (conn := _ro()) is None: return
    where,params=("WHERE c.source = ?",[source]) if source else ("",[])
    with contextlib.closing(conn):
        if fmt=="json":
            def build(row): return dict(id=row[0],source=row[1],title=row[2],created_at=str(row[3]) if row[3] else None,updated_at=str(row[4]) if row[4] else None,model=row[5],cwd=row[6],git_branch=row[7],project_id=row[8],messages=[dict(role=m[0],content=m[1],thinking=m[2],created_at=str(m[3]) if m[3] else None,model=m[4]) for m in conn.execute(f"SELECT m.role,m.content,m.thinking,m.created_at,m.model FROM messages m WHERE m.conversation_id=? ORDER BY {MESSAGE_ORDER}",[row[0]]).fetchall()],tool_calls=[dict(tool=t[0],input=json.loads(t[1]),output=json.loads(t[2]),status=t[3]) for t in conn.execute("SELECT tool_name,input,output,status FROM tool_calls tc JOIN messages m ON tc.message_id=m.id WHERE m.conversation_id=?",[row[0]]).fetchall()],file_edits=[dict(file=e[0],type=e[1],content=e[2],evidence_status=e[3],evidence_reason=e[4],tool_call_id=e[5]) for e in conn.execute("SELECT fe.file_path,fe.edit_type,fe.content,COALESCE(v.status,'unverified'),COALESCE(v.reason,'source_unavailable'),v.tool_call_id FROM file_edits fe JOIN messages m ON fe.message_id=m.id LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id WHERE m.conversation_id=? ORDER BY fe.created_at NULLS FIRST,fe.id",[row[0]]).fetchall()])
            rows=conn.execute(f"SELECT c.id,c.source,c.title,c.created_at,c.updated_at,c.model,c.cwd,c.git_branch,c.project_id FROM conversations c {where}",params).fetchall()
            output.write_text(json.dumps([build(row) for row in rows],indent=2))
        else:
            cur=conn.execute(f"SELECT c.id,c.source,c.title,c.cwd,m.role,m.content,m.created_at FROM conversations c JOIN messages m ON c.id=m.conversation_id {where} ORDER BY c.created_at,{MESSAGE_ORDER}",params)
            with output.open("w",newline="") as stream:
                csv.writer(stream).writerows([[d[0] for d in cur.description],*cur.fetchall()])
    typer.echo(f"Exported to {output}")

def backup():
    with _core(ready=True) as conn: path=_migration_backup(conn,datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ"))
    typer.echo(f"Archive backed up to {path}")

def _sync_leader(fn):
    DATA_DIR.mkdir(parents=True,exist_ok=True)
    with (DATA_DIR/".sync.lock").open("w") as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            typer.echo("Sync already running; no work was started")
            raise typer.Exit()
        return fn()

def sync(watch: bool = typer.Option(False, "-w"), interval: int = typer.Option(300, "-i"), claude_code: bool = True, codex: bool = True, full: bool = typer.Option(False, "--full", help="Re-parse/re-fetch all sources and reconcile all provenance"), verbose: bool = typer.Option(False, "-v", "--verbose"), local_only: bool = typer.Option(False, "--local-only", help="Import local agent sessions and configured exports without contacting web sources.")):
    if sys.argv[1:2] == ["sync"]: signal.signal(signal.SIGINT, signal.SIG_DFL)
    state,local,web,imports,chatgpt_ok,chatgpt_frontiers,offline,ready={},{},{},{},{},{},local_only is True,False
    def set_state(section,key,val): state.setdefault(section,{})[key]=val
    def plan_local(name, path, parser, bindings, sink):
        if not path.exists(): return None
        if name in ("codex", "claude-code"):
            prev,mt=local.get(name,{}).get("files",{}),{str(p):m for p in path.rglob("*.jsonl") if (m:=stat_mtime(p)) is not None}
            if not (chg:=list(map(Path,mt)) if full or local.get(name,{}).get("parser")!=PARSER_EPOCH else [Path(p) for p,m in mt.items() if m>prev.get(p,0)]): return None
            saved,run=[],lambda p=path,fs=chg:saved.extend(sink(parser(p,fs[i:i+20],bindings)) for i in range(0,len(fs),20)) or ParseResult()
            return dict(name=name,label=name.replace("-"," ").title(),source=name,func=run,saved=saved,state=("local",name,{"parser":PARSER_EPOCH,"files":mt}))
        mtime = latest_mtime(path)
        return None if not full and mtime<=local.get(name,{}).get("mtime",0) else dict(name=name,label=name.replace("-"," ").title(),source=name,func=lambda p=path:parser(p),state=("local",name,{"mtime":mtime}))
    def probe_chatgpt(browser):
        def one(profile):
            try:
                cookies,base=chatgpt_cookie_base(browser,_CHATGPT_HOSTS,profile)
                headers=chatgpt_headers(cookies,base,_BROWSER_UA[browser])
                items,account=fetch_json(f"{base}/backend-api/conversations?offset=0&limit=1&order=updated",cookies,headers,rate_limit_backoff=300)["items"],headers.get("ChatGPT-Account-ID")
                return profile,account,items[0] if items else None,None
            except Exception as error: return profile,None,None,error
        results,errors,accounts,valid=(results:=[one(profile) for profile in chatgpt_profiles(browser)]),[f"chatgpt.com{f'/{profile}' if profile else ''}: {error}" for profile,account,item,error in results if error],(accounts:=set()),[(profile,account,item) for profile,account,item,error in results if item and (not account or account not in accounts and not accounts.add(account))]
        required(valid,ValueError(f"ChatGPT request failed in {browser}: " + " | ".join(errors)) if errors else ValueError("ChatGPT request failed"))
        if errors: typer.echo("chatgpt profiles skipped: "+" | ".join(errors),err=True)
        chatgpt_ok[browser],chatgpt_frontiers[browser]=[profile for profile,account,item in valid],{profile or "default":{"account":account,"updated":item.get("update_time"),"id":item["id"]} for profile,account,item in valid}
        return "|".join(f"{profile or 'default'}:{item['id']}:{item.get('update_time')}" for profile,account,item in valid)
    def probe_claude(browser): return f"{(item:=items[0])['uuid']}:{item.get('updated_at') or item.get('created_at')}" if (items:=claude_listing(browser)[2]) else None
    def plan_web(name, fetcher, probe, known=None, sink=None, legacy=None, frontier_ok=True):
        pref,forced=web.get(name,{}),os.environ.get(f"CONVOS_{name.upper()}_BROWSER")
        order,errors=([forced] if forced else [pref.get("browser")]+[b for b in ("safari","chrome") if b!=pref.get("browser")]),[]
        for b in [x for x in order if x]:
            try:
                head=probe(b)
                lu,current=head.split(":",1)[1] if name=="claude" and head and ":" in head else None,{**(pref.get("frontiers",{}) if b==pref.get("browser") else {}),**chatgpt_frontiers.get(b,{})} if name=="chatgpt" else None
                st={"browser":b,"head":head,**({"frontiers":current} if name=="chatgpt" else {"last_updated":lu} if lu else {})}
                if name!="chatgpt" and head is not None and b==pref.get("browser") and head==pref.get("head") and not full: return set_state("web",name,st)
                last,saved,coverage,since=(last:=pref.get("last_updated")),None if name=="claude" else [],pref.get("coverage"),ts_from_iso(last) if name=="claude" and last and not full else None
                frontier=pref.get("frontiers") if frontier_ok and not full and b==pref.get("browser") and known and isinstance(coverage,list) and set(coverage)<=set(known) else None
                func=(lambda b=b,since=since:fetcher(b,since=since)) if name=="claude" else (lambda b=b,saved=saved:fetcher(b,profiles=chatgpt_ok[b],known=known,legacy=legacy,frontiers=frontier,sink=lambda r:saved.append(sink(r))))
                return dict(name=name,label=name.title(),source=name,func=func,state=("web",name,st),saved=saved)
            except Exception as e: errors.append(f"{b}: {e}")
        if errors: typer.echo(f"{name}: no cookies found -- skipped" if all("cookies" in e.lower() for e in errors) else f"{name} sync failed: " + " | ".join(errors))
    def plan_import(path: Path):
        if not path.exists(): return None
        mtime=latest_mtime(path) if path.is_dir() else path.stat().st_mtime
        return None if mtime<=imports.get(str(path),{}).get("mtime",0) else dict(name=f"import:{path}",label=f"import:{path}",func=lambda p=path:parse_source(p),state=("imports",str(path),{"mtime":mtime}))
    def run_sync():
        nonlocal state,local,web,imports,ready
        if not ready:
            with _core(ready=True): pass
            drain_hooks()
            ready=True
        state,before,t0=(state:=load_state()),json.dumps(state,sort_keys=True),time.perf_counter()
        local,web,imports=state.setdefault("local",{}),state.setdefault("web",{}),state.setdefault("imports",{})
        chatgpt_ok.clear() or chatgpt_frontiers.clear()
        total,changed,jobs,newc,updc,provenance_edits,provenance_conversations,repair_attempted=[0]*5,set(),[],0,0,set(),set(),set()
        def checkpoint(r):
            with _locked(HOOK_DIR/".lock"):
                if ids:=(set(m["id"] for m in r.msgs)): HOOK_FTS_DIRTY.touch() or merge_embed_dirty(ids)
                with _core() as conn,_transaction(conn): out=upsert(conn,r)
                known.update({c["id"]:(u.timestamp() if (u:=ts_any(json.loads(c["metadata"]).get("remote_update_time"))) else None) for c in r.convs if c["source"]=="chatgpt"})
                repair_attempted.update(c["id"] for c in r.convs if c["id"] in repair_order)
                return (*out,getattr(r,"provenance_edits",set()),getattr(r,"provenance_conversations",set()))
        with _core() as conn,_transaction(conn): (repaired:=[r[0] for r in conn.execute("UPDATE conversations c SET created_at=COALESCE(c.created_at,t.first_seen),updated_at=COALESCE(c.updated_at,t.last_seen) FROM (SELECT conversation_id,MIN(created_at) first_seen,MAX(created_at) last_seen FROM messages GROUP BY conversation_id) t WHERE c.id=t.conversation_id AND c.source='chatgpt' AND (c.created_at IS NULL OR c.updated_at IS NULL) RETURNING id").fetchall()]) and _archive_touch(conn,[("conversations",x) for x in repaired])
        with _core(read_only=True) as conn: cur,bindings,rows,candidates=counts_by_source(conn),session_bindings(conn),conn.execute(f"SELECT c.id,c.updated_at,json_extract_string(c.metadata,'$.remote_update_time'),json_extract_string(c.metadata,'$.remote_complete'),(SELECT role FROM messages m WHERE m.conversation_id=c.id ORDER BY {MESSAGE_ORDER_DESC} LIMIT 1) FROM conversations c WHERE source='chatgpt'").fetchall(),{r[0] for r in conn.execute("SELECT DISTINCT m.conversation_id FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.source='chatgpt' AND json_extract_string(m.metadata,'$.provider_index') IS NULL QUALIFY count(*) OVER (PARTITION BY m.conversation_id,m.created_at)>1").fetchall()}
        prior_order,updated,repair_order,known,legacy,fmt,start=(prior_order:=web.get("chatgpt",{}).get("order_repairs",{})),(updated:={cid:v.timestamp() if (v:=ts_any(raw)) else ts.timestamp() if ts else None for cid,ts,raw,_,_ in rows}),(repair_order:={cid for cid in candidates if prior_order.get(cid)!=updated[cid]}),{cid:None if cid in repair_order or (complete=="false" or complete is None and role=="tool") and (v:=ts_any(raw) or ts) and (datetime.now()-v).total_seconds()<900 else updated[cid] for cid,ts,raw,complete,role in rows},{cid for cid,_,raw,_,_ in rows if raw is None},(fmt:=lambda v:f"{v[0]} convs, {v[1]} msgs, {v[2]} tools, {v[3]} attachs, {v[4]} edits"),lambda label,src=None:typer.echo(f"Syncing {label}" if not src else f"Syncing {label} ({fmt(cur.setdefault(src,[0]*5))})")
        def schedule(job): jobs.extend([job] if job else [])
        if paths := [Path(p).expanduser() for p in os.environ.get("CONVOS_IMPORT_PATHS", "").split(",") if p.strip()]:
            jobs+=start("imports") or [j for p in paths if (j:=plan_import(p))]
        for name,label,enabled,p,parser in (("claude-code","Claude Code",claude_code,Path(os.environ.get("CLAUDE_CONFIG_DIR",Path.home()/".claude"))/"projects",parse_claude_code),("codex","Codex",codex,Path(os.environ.get("CODEX_HOME",Path.home()/".codex")),parse_codex)):
            if enabled and p.exists(): start(label,name) or schedule(plan_local(name,p,parser,bindings,checkpoint))
        if not offline:
            start("ChatGPT"+(f", provider order={len(candidates)-len(repair_order)} attempted/{len(candidates)} unresolved" if candidates else ""),"chatgpt")
            schedule(plan_web("chatgpt",fetch_chatgpt,probe_chatgpt,{} if full else known,checkpoint,legacy,not repair_order))
            start("Claude","claude")
            schedule(plan_web("claude",fetch_claude,probe_claude))
        verbose and typer.echo(f"Planning took {time.perf_counter()-t0:.2f}s")
        if jobs:
            with ThreadPoolExecutor(max_workers=min(4, len(jobs))) as ex:
                futs = {ex.submit(j["func"]): {**j, "t": time.perf_counter()} for j in jobs}
                for fut in as_completed(futs):
                    try: r=(j:=futs[fut]) and fut.result()
                    except Exception as e: r=typer.echo(f"{j['name']} failed: {e}")
                    if saved := j.get("saved"):
                        c,m,t,a,e,n,u,changed_ids,provenance_edits,provenance_conversations=(*[sum(s[i] for s in saved) for i in range(7)],set().union(*(s[7] for s in saved)),provenance_edits|set().union(*(s[8] for s in saved)),provenance_conversations|set().union(*(s[9] for s in saved)))
                    elif r is not None:
                        with _locked(HOOK_DIR/".lock"),_core() as conn,_transaction(conn): c,m,t,a,e,n,u,changed_ids=upsert(conn,r)
                    else: continue
                    total,newc,updc,changed=[total[i]+v for i,v in enumerate([c,m,t,a,e])],newc+n,updc+u,changed|changed_ids
                    if r is not None: provenance_edits,provenance_conversations=provenance_edits|getattr(r,"provenance_edits",set()),provenance_conversations|getattr(r,"provenance_conversations",set())
                    if r is not None and (st:=j.get("state")): (j["name"]=="chatgpt" and st[2].update(coverage=sorted(known),order_repairs={cid:known[cid] for cid in ({cid for cid in candidates if prior_order.get(cid)==updated[cid]}|repair_attempted)}),set_state(*st))
                    if j.get("source"): typer.echo(f"Updated {j['label']} ({n} new, {u} updated convs; {fmt([c, m, t, a, e])} processed){' before failure' if r is None else ''}{' in %.2fs' % (time.perf_counter()-j['t']) if verbose else ''}")
        capture_provenance() if full else capture_provenance(edit_ids=provenance_edits,conversation_ids=provenance_conversations)
        mark_dirty(changed)
        if before!=json.dumps(state,sort_keys=True): atomic_json(STATE_PATH,state)
        verbose and typer.echo(f"Total sync time {time.perf_counter()-t0:.2f}s")
        return total, newc, updc
    def do_sync(): return _sync_leader(run_sync)
    if watch: typer.echo(f"Daemon mode (interval: {interval}s)")
    while watch:
        r,n,u=do_sync()
        typer.echo(f"[{datetime.now().isoformat()}] {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits")
        time.sleep(interval)
    r,n,u=do_sync()
    typer.echo(f"Updated {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits processed")
    with _core(read_only=True) as conn: total=[conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("conversations","messages","tool_calls","attachments","file_edits")]
    typer.echo(f"Total: {', '.join(f'{n} {label}' for n,label in zip(total,('convs','msgs','tools','attachs','edits')))}")

def sql(query: str, fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _ro()) is None: return
    try:
        with contextlib.closing(conn): cols,rows=(cur:=conn.execute(query)) and [d[0] for d in cur.description],cur.fetchall()
    except Exception as e: return typer.echo(f"Query failed: {e}",err=True)
    if fmt!="text": return emit([dict(zip(cols,row)) for row in rows],fmt)
    typer.echo("\n".join([" | ".join(cols),*(" | ".join("" if value is None else str(value) for value in row) for row in rows),f"\n{len(rows)} rows"]))

for _fn,_name,_hidden in ((capture,"hook",True),(capture,"capture",True),(drain_hooks_cmd,"drain-hooks",True),(init,None,False),(search,None,False),(read_cmd,"read",False),(query_cmd,"query",False),(embed_cmd,"embed",False),(doctor,None,False),(install_skills,None,False),(install_hooks,"install-hooks",False),(export,None,False),(backup,None,False),(sync,None,False),(sql,None,False)): app.command(_name,hidden=_hidden)(_fn)
for _ep in entry_points(group="convos.commands"):
    try: _ep.load()(app)
    except Exception as _e: typer.echo(f"plugin {_ep.name} failed: {_e}", err=True)  # a broken plugin must not kill the CLI

if __name__ == "__main__": app()
