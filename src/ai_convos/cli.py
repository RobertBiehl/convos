#!/usr/bin/env python3
import base64, click, contextlib, csv, duckdb, fcntl, getpass, graphlib, hashlib, itertools, json, os, random, re, shlex, shutil, signal, site, sqlite3, ssl, struct, subprocess, sys, sysconfig, tempfile, threading, time, typer, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from importlib.metadata import entry_points, version
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from .migrations import fts_needs_rebuild, migrate_remote_changes, migrate_remote_data, migrate_remote_ids, migration_memory, remote_id_migration_scope

# Newer Typer vendors Click; command errors must use the active runtime.
click=getattr(typer,"_click",click)
app = typer.Typer(help="AI Conversations DB - searchable archive for Claude, ChatGPT, and Codex")
def find_root(): return Path(r).expanduser() if (r := os.environ.get("CONVOS_PROJECT_ROOT")) else Path.home()/".convos"
PROJECT_ROOT,DATA_DIR,DB_PATH,STATE_PATH=(root:=find_root()),(data:=root/"data"),data/"convos.db",data/"sync_state.json"
HOOK_DIR,HOOK_STATE,HOOK_PROGRESS,HOOK_EMBED_DIRTY,HOOK_FTS_DIRTY,_NOISE_RE,_NOISE,HOOK_DRAIN_EVENTS,HOOK_DRAIN_SECONDS,CHATGPT_BURST,CHATGPT_RATE,PARSER_EPOCH,CORE_VERSION,ATTACHMENT_LIMIT,_FTS_DEF=DATA_DIR/"hook_inbox",DATA_DIR/"hook_state.json",DATA_DIR/"hook_progress.json",DATA_DIR/"hook_embeddings_dirty",DATA_DIR/"hook_fts_dirty",(_NR:=r"^(Base directory for this skill:|# AGENTS\.md instructions for|<(codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)( |>))"),f" AND NOT regexp_matches(COALESCE(content,''),'{_NR}')",8,10,20,8/15,12,17,32*1024**2,hashlib.sha256(b"messages:id:content:thinking:active-v1").hexdigest()  # conservative web pacing stays below the observed ~200-detail failure point
_CHATGPT_HOSTS,_BROWSER_UA,_CLAUDE_HEADERS=(("https://chatgpt.com",("chatgpt.com",)),("https://chat.openai.com",("chat.openai.com","openai.com"))),(ua:={"safari":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15","chrome":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}),{"Origin":"https://claude.ai","Referer":"https://claude.ai/","User-Agent":ua["safari"],"Accept":"application/json","Accept-Language":"en-US,en;q=0.9","anthropic-client-sha":"unknown","anthropic-client-version":"unknown"}
_INJECTED_RE,MESSAGE_ORDER,MESSAGE_ORDER_DESC=r"(?s)(?:# AGENTS\.md instructions for [^\n]+\n\n<INSTRUCTIONS>\n.*\n</INSTRUCTIONS>|<(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)(?: [^>]*)?>.*</(?:codex_internal_context|environment_context|local-command-caveat|recommended_plugins|skill)>)\s*","m.created_at NULLS FIRST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) NULLS LAST,m.id","m.created_at DESC NULLS LAST,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) DESC NULLS LAST,m.id DESC"

def _open_db(path,read_only=False): return (path.parent.mkdir(parents=True,exist_ok=True),None if read_only and not path.exists() else duckdb.connect(str(path),read_only=read_only))[-1]
class LockBusy(click.ClickException,RuntimeError): pass
class ProvenanceChanged(ValueError): pass
def lock_owner(purpose,identity=None): return dict(identity or {},v=1,purpose=purpose,pid=os.getpid(),process=Path(sys.argv[0]).name,host=os.uname().nodename,os_user=getpass.getuser(),started_at=(now:=time.time()),heartbeat_at=now,stage="started")
def _lock_read(lock):
    try: return json.loads((lock.seek(0),lock.read())[-1] if hasattr(lock,"read") else Path(lock).read_text())
    except (OSError,TypeError,ValueError): return {}
def lock_holder(owner):
    if not owner: return "shared archive reader(s) or a pre-contract process; owner metadata unavailable"
    return (lambda remote,started,progress:f"{remote}OS user {owner.get('os_user','unknown')}@{owner.get('host','unknown')}; PID {owner.get('pid','unknown')} ({owner.get('process','unknown')}); purpose {owner.get('purpose','unknown')}; started {started}; last progress {progress} ({owner.get('stage','unknown')})")(f"remote user {owner['remote_user']} ({owner.get('user_id','unknown')}); device {owner.get('device','unknown')} ({owner.get('device_id','unknown')}); " if owner.get("remote_user") else "",time.strftime("%Y-%m-%d %H:%M:%S %Z",time.localtime(owner["started_at"])) if isinstance(owner.get("started_at"),(int,float)) else "unknown",f"{max(0,time.time()-owner['heartbeat_at']):.0f}s ago" if isinstance(owner.get("heartbeat_at"),(int,float)) else "unknown")
class _LockedDB(contextlib.AbstractContextManager):
    def __init__(self,db,purpose,read_only): self.db,self.audit=db,(purpose,read_only,time.monotonic())
    def __getattr__(self,name): return getattr(self.db,name)
    def __exit__(self,*_): self.close()
    def close(self): return ((db:=self.db),setattr(self,"db",None),db.close(),(held:=time.monotonic()-self.audit[2]),typer.echo(f"Database {'read' if self.audit[1] else 'write'} connection held {held:.1f}s: {self.audit[0]}",err=True) if held>5 else None)[-1] if self.db else None
def _lock_note(lock,value=""): return (lock.seek(0),lock.truncate(),value and lock.write(json.dumps(value,separators=(",",":")) if isinstance(value,dict) else value),lock.flush())[-1]
def _flock(lock,op,deadline,wait,purpose,identity=None):
    delay,progress=.01,None
    while True:
        try: return (fcntl.flock(lock,op|fcntl.LOCK_NB),(owner:=None if op!=fcntl.LOCK_EX else lock_owner(purpose,identity)),_lock_note(lock,owner or ""),owner)[-1]
        except BlockingIOError:
            owner,pulse=(owner:=_lock_read(lock)),owner.get("heartbeat_at")
            if pulse is not None and pulse!=progress: progress,deadline=pulse,time.monotonic()+wait
            if (remaining:=deadline-time.monotonic())<=0: raise LockBusy(f"Could not start {purpose}: another operation {'is already running' if not wait else f'made no recorded progress for {wait:g}s'}. Holder: {lock_holder(owner)}.")
            delay=(time.sleep(random.uniform((pause:=min(delay,remaining))/2,pause)),min(delay*2,5))[-1]
_DUCKDB_PID=re.compile(r"(?:\(PID |PID )(\d+)\)?")
def _duckdb_owner(path,error):
    text,pid,root=str(error),(int(found.group(1)) if (found:=_DUCKDB_PID.search(str(error))) else None),path.parent.parent if path.parent.name=="data" else path.parent
    return (lambda base:max([value for directory in (path.parent,path.parent/"hook_inbox",root/"remote") if directory.is_dir() for item in directory.glob("*.lock") if item!=path.with_name(f".{path.name}.lock") and (value:=_lock_read(item)) and value.get("pid")==pid and value.get("stage")!="finished"],key=lambda value:value.get("heartbeat_at",0),default=base))(dict(pid=pid,process=(Path(found.group(1)).name if (found:=re.search(r"held in (.+?) \(PID",text)) else "unknown"),os_user=(found.group(1) if (found:=re.search(r"by user ([^.\n]+)",text)) else "unknown"),host=os.uname().nodename,purpose="external DuckDB access",stage="unknown"))
def _waiter(path): return (lambda directory,lock:(os.chmod(directory,0o700),os.chmod(lock.name,0o600),fcntl.flock(lock,fcntl.LOCK_EX),lock)[-1])((directory:=path.with_name(f".{path.name}.waiters")),(directory.mkdir(exist_ok=True) or directory/f"{os.uname().nodename}.{os.getpid()}.{time.time_ns()}.lock").open("a+"))
def _waiter_close(lock): return (Path(lock.name).unlink(missing_ok=True),lock.close())[-1] if lock else None
def _waiting(marker):
    lock=marker.open("a+")
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: return lock.close() or True
    return (marker.unlink(missing_ok=True),lock.close(),False)[-1]
def archive_yield(path=None):
    directory,end=(path:=Path(path or DB_PATH).expanduser().resolve()).with_name(f".{path.name}.waiters"),time.monotonic()+1.1
    while time.monotonic()<end and any(map(_waiting,directory.glob("*.lock") if directory.is_dir() else ())): time.sleep(random.uniform(.01,.05))
def get_db(read_only:bool=False,wait=30,path=None,deadline=None,*,purpose="direct database access"):
    path,deadline=(path:=Path(path or DB_PATH).expanduser().resolve()),deadline if deadline is not None else time.monotonic()+wait
    if read_only and not path.exists(): return None
    if path.exists() and path.stat().st_nlink>1: raise ValueError(f"Database hardlink aliases are unsafe: {path}")
    delay,progress,notice,started,reported=.01,None,None,time.monotonic(),None
    try:
        while True:
            try: db=_open_db(path,read_only)
            except Exception as e:
                if "Conflicting lock is held" not in str(e): raise
                notice,owner=notice or _waiter(path),_duckdb_owner(path,e)
                if (pulse:=owner.get("heartbeat_at")) is not None and pulse!=progress: progress,deadline=pulse,time.monotonic()+wait
                if time.monotonic()-started>=1 and (identity:=(owner.get("pid"),owner.get("purpose")))!=reported: reported=(typer.echo(f"Waiting to start {purpose}. Holder: {lock_holder(owner)}.",err=True),identity)[-1]
                if (remaining:=deadline-time.monotonic())<=0: raise LockBusy(f"Could not start {purpose}: archive {'holder made no recorded progress' if pulse is not None else 'stayed busy'} for {wait:g}s. Holder: {lock_holder(owner)}. DuckDB: {e}") from None
                delay=(time.sleep(random.uniform((pause:=min(delay,remaining))/2,pause)),min(delay*2,1))[-1]
            else: return (_waiter_close(notice),_LockedDB(db,purpose,read_only) if db is not None else None)[-1]
    except BaseException as e: raise (_waiter_close(notice),e)[-1]
def open_db(path=None,read_only=False,wait=30,deadline=None,*,purpose="direct database access"): return get_db(read_only,wait,path,deadline,purpose=purpose)
@contextlib.contextmanager
def _core(path=None,read_only=False,ready=False,wait=30,*,purpose):
    with contextlib.closing(get_db(read_only,wait,path,purpose=purpose)) as db: (((ready) and (init_schema(db))),(yield db))
@contextlib.contextmanager
def operation_lock(path,purpose,wait=30,identity=None,mandatory=True):
    with (Path(path).parent.mkdir(parents=True,exist_ok=True) or Path(path).open("a+")) as lock:
        guard=(os.chmod(path,0o600),threading.Lock())[-1]
        try: owner=_flock(lock,fcntl.LOCK_EX,time.monotonic()+wait,wait,purpose,identity)
        except LockBusy as error: return (yield None) if not mandatory else (_ for _ in ()).throw(error)
        def pulse(stage):
            with guard: return owner.update(heartbeat_at=time.time(),stage=stage) or _lock_note(lock,owner)
        try: yield pulse
        finally: pulse("finished") or fcntl.flock(lock,fcntl.LOCK_UN)
@contextlib.contextmanager
def _transaction(db):
    db.execute("BEGIN")
    try: yield
    except BaseException as error: raise (db.execute("ROLLBACK"),error)[-1]
    else: db.execute("COMMIT")
def required(value,error): return value if value else (_ for _ in ()).throw(error)
def secure_dir(path,mode=0o700): return (required(not (path:=Path(path)).is_symlink(),ValueError("managed directory must not be a symlink")),path.mkdir(parents=True,exist_ok=True),os.chmod(path,mode),path)[-1]

def load_state():
    try: return json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    except Exception: return {}

def atomic_write(path: Path, text):
    if path.is_symlink() or path.exists() and not path.is_file(): raise typer.Exit(typer.echo(f"Refusing unsafe managed file: {path}",err=True) or 1)
    return (path.parent.mkdir(parents=True,exist_ok=True),atomic_publish(path,lambda tmp:tmp.write_text(text),path.stat().st_mode&0o777 if path.exists() else 0o600))[-1]
def atomic_json(path: Path, data): atomic_write(path, json.dumps(data))
def _fsync(path):
    fd=os.open(path,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
def durable_replace(tmp,path): return (_fsync(tmp:=Path(tmp)),os.replace(tmp,path:=Path(path)),_fsync(path.parent))[-1]
def atomic_publish(path,write,mode=0o600):
    path,(fd,tmp)=(path:=Path(path)),tempfile.mkstemp(prefix=f".{path.name}.",dir=path.parent)
    try: (os.close(fd),write(Path(tmp)),os.chmod(tmp,mode),durable_replace(tmp,path))
    finally: Path(tmp).unlink(missing_ok=True)
def attachment_body(data,root=None):
    if len(data)>ATTACHMENT_LIMIT: return None
    root,blob,path=(root:=secure_dir(Path(root or DATA_DIR)/"attachments")),(blob:=hashlib.sha256(data).hexdigest()),root/blob
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=len(data): raise ValueError("attachment body conflicts with content hash")
        return (required(_file_sha256(path)==blob,ValueError("attachment body conflicts with content hash")),os.chmod(path,0o600),path)[-1]
    return (atomic_publish(path,lambda tmp:tmp.write_bytes(data)),path)[-1]

def detect_source(path: Path): return "codex" if path.is_dir() and (path/"sessions").exists() else "claude-code" if path.is_dir() else "chatgpt" if path.suffix==".zip" or "chatgpt" in path.name.lower() else "claude" if "chat_messages" in (data:=required(json.loads(path.read_text()),ValueError(f"Empty export: {path}")))[0] else "chatgpt"

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
CREATE TABLE IF NOT EXISTS provenance.pending(kind VARCHAR,entity VARCHAR,generation UBIGINT NOT NULL,PRIMARY KEY(kind,entity));
CREATE SCHEMA IF NOT EXISTS remote;
CREATE TABLE IF NOT EXISTS remote.row_owners(kind VARCHAR,source VARCHAR,author VARCHAR,device VARCHAR,PRIMARY KEY(kind,source,author));
CREATE TABLE IF NOT EXISTS remote.derived_edits(physical_id VARCHAR PRIMARY KEY,source_id VARCHAR,user_id VARCHAR,parent_kind VARCHAR,parent_id VARCHAR);
CREATE TABLE IF NOT EXISTS remote.row_origins(table_name VARCHAR,physical_row_id VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,author_device_id VARCHAR,source_row_id VARCHAR,source_event_id VARCHAR,content_key VARCHAR,observed_at TIMESTAMP,proof_id VARCHAR,PRIMARY KEY(table_name,physical_row_id));
CREATE TABLE IF NOT EXISTS remote.row_signers(author_user_id VARCHAR,author_device_id VARCHAR,root_public VARCHAR,certificate JSON,PRIMARY KEY(author_user_id,author_device_id));
CREATE TABLE IF NOT EXISTS remote.workspace_controls(workspace_id VARCHAR,revision UINTEGER,epoch UINTEGER,state_hash VARCHAR,control JSON,PRIMARY KEY(workspace_id,revision));
CREATE TABLE IF NOT EXISTS remote.row_proofs(id VARCHAR PRIMARY KEY,workspace_id VARCHAR,authorization_workspace_id VARCHAR,row_kind VARCHAR,source_row_id VARCHAR,encoding_v USMALLINT,content_hash VARCHAR,revision VARCHAR,previous_revision VARCHAR,state VARCHAR,author_user_id VARCHAR,author_device_id VARCHAR,authorization_epoch UINTEGER,signature VARCHAR);
CREATE INDEX IF NOT EXISTS row_proofs_source ON remote.row_proofs(source_row_id);
CREATE TABLE IF NOT EXISTS remote.local_row_bases(kind VARCHAR,entity VARCHAR,author VARCHAR,revision VARCHAR,PRIMARY KEY(kind,entity,author));
CREATE TABLE IF NOT EXISTS remote.row_conflicts(proof_id VARCHAR PRIMARY KEY,body JSON);
CREATE TABLE IF NOT EXISTS remote.edit_dependencies(dependency_key VARCHAR,proof_id VARCHAR,PRIMARY KEY(dependency_key,proof_id));
CREATE INDEX IF NOT EXISTS edit_dependency_key ON remote.edit_dependencies(dependency_key);
CREATE INDEX IF NOT EXISTS edit_dependency_proof ON remote.edit_dependencies(proof_id);
CREATE TABLE IF NOT EXISTS remote.edit_ready(dependency_key VARCHAR PRIMARY KEY,after_proof_id VARCHAR NOT NULL);
INSERT OR IGNORE INTO remote.edit_ready VALUES ('','');
CREATE TABLE IF NOT EXISTS remote.row_references(table_name VARCHAR,physical_row_id VARCHAR,author_user_id VARCHAR,source_row_id VARCHAR,PRIMARY KEY(table_name,physical_row_id,author_user_id));
CREATE TABLE IF NOT EXISTS parser_tool_lineage(message_id VARCHAR,author VARCHAR,native BOOLEAN,old_source VARCHAR,old_hash VARCHAR,new_source VARCHAR,new_hash VARCHAR,old_id VARCHAR,new_id VARCHAR,PRIMARY KEY(message_id,author,old_source,old_hash,new_source,new_hash));
CREATE INDEX IF NOT EXISTS parser_tool_old ON parser_tool_lineage(old_source);
CREATE INDEX IF NOT EXISTS parser_tool_new ON parser_tool_lineage(new_source);
CREATE OR REPLACE VIEW parser_tool_history AS SELECT l.old_id,min(l.new_id) current_id FROM parser_tool_lineage l JOIN messages m ON m.id=l.message_id JOIN tool_calls new ON new.id=l.new_id GROUP BY l.old_id HAVING count(DISTINCT l.new_id)=1;
CREATE TABLE IF NOT EXISTS parser_edit_lineage(message_id VARCHAR,author VARCHAR,native BOOLEAN,old_source VARCHAR,old_hash VARCHAR,new_source VARCHAR,new_hash VARCHAR,old_id VARCHAR,new_id VARCHAR,PRIMARY KEY(message_id,author,old_source,old_hash,new_source,new_hash));
CREATE INDEX IF NOT EXISTS parser_edit_old ON parser_edit_lineage(old_source);
CREATE INDEX IF NOT EXISTS parser_edit_new ON parser_edit_lineage(new_source);
CREATE OR REPLACE VIEW parser_edit_history AS WITH RECURSIVE links AS (SELECT DISTINCT l.old_id,l.new_id FROM parser_edit_lineage l JOIN messages m ON m.id=l.message_id JOIN file_edits new ON new.id=l.new_id WHERE l.old_id<>l.new_id), paths(old_id,current_id,visited,cycle) AS (SELECT old_id,new_id,[old_id],FALSE FROM links UNION ALL SELECT p.old_id,l.new_id,list_append(p.visited,p.current_id),list_contains(p.visited,l.new_id) FROM paths p JOIN links l ON l.old_id=p.current_id WHERE NOT p.cycle AND len(p.visited)<64) SELECT old_id,min(current_id) current_id FROM paths p WHERE cycle OR len(visited)=64 OR NOT EXISTS(SELECT 1 FROM links l WHERE l.old_id=p.current_id) GROUP BY old_id HAVING NOT bool_or(cycle OR len(visited)=64) AND count(DISTINCT current_id)=1;
CREATE OR REPLACE VIEW current_file_edits AS SELECT * FROM file_edits;
CREATE TABLE IF NOT EXISTS parser_message_lineage(message_id VARCHAR,author VARCHAR,native BOOLEAN,old_source VARCHAR,old_hash VARCHAR,new_source VARCHAR,new_hash VARCHAR,old_id VARCHAR,new_id VARCHAR,PRIMARY KEY(message_id,author,old_source,old_hash,new_source,new_hash));
CREATE INDEX IF NOT EXISTS parser_message_old ON parser_message_lineage(old_source);
CREATE INDEX IF NOT EXISTS parser_message_new ON parser_message_lineage(new_source);
CREATE OR REPLACE VIEW parser_message_history AS SELECT l.old_id,min(l.new_id) current_id FROM parser_message_lineage l JOIN conversations c ON c.id=l.message_id JOIN messages new ON new.id=l.new_id GROUP BY l.old_id HAVING count(DISTINCT l.new_id)=1;
CREATE TABLE IF NOT EXISTS parser_retired_rows(kind VARCHAR,physical VARCHAR,source VARCHAR,author VARCHAR,content_hash VARCHAR,body JSON,projection JSON,replacement VARCHAR,PRIMARY KEY(kind,physical,content_hash));
CREATE INDEX IF NOT EXISTS parser_retired_physical ON parser_retired_rows(physical);
CREATE TABLE IF NOT EXISTS remote.provider_session_aliases(workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,revision VARCHAR,source VARCHAR,session_id VARCHAR,members JSON,canonical_source_row_id VARCHAR,proof JSON,PRIMARY KEY(author_user_id,object_id,revision));
CREATE TABLE IF NOT EXISTS remote.file_edit_evidence_proofs(workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,revision VARCHAR,source_edit_id VARCHAR,edit_revision VARCHAR,status VARCHAR,reason VARCHAR,source_tool_call_id VARCHAR,tool_revision VARCHAR,proof JSON,PRIMARY KEY(workspace_id,author_user_id,object_id,revision));
CREATE TABLE IF NOT EXISTS remote.semantic_ancestors(object_kind VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,object_id VARCHAR,child_revision VARCHAR,ancestor_revision VARCHAR,PRIMARY KEY(object_kind,workspace_id,author_user_id,object_id,child_revision,ancestor_revision));
CREATE OR REPLACE VIEW remote.file_edit_evidence_claims AS SELECT * FROM remote.file_edit_evidence_proofs WHERE reason NOT IN ('signed_replica_missing_evidence','signed_evidence_conflict');
CREATE OR REPLACE VIEW remote.file_edit_evidence_ancestors AS SELECT a.* FROM remote.semantic_ancestors a JOIN remote.file_edit_evidence_claims p ON (a.workspace_id,a.author_user_id,a.object_id,a.child_revision)=(p.workspace_id,p.author_user_id,p.object_id,p.revision) WHERE a.object_kind='file-edit.evidence';
CREATE TABLE IF NOT EXISTS remote.provenance_origins(kind VARCHAR,physical_entity VARCHAR,workspace_id VARCHAR,author_user_id VARCHAR,source_entity VARCHAR,proof_id VARCHAR,PRIMARY KEY(kind,physical_entity,workspace_id,author_user_id));
CREATE TABLE IF NOT EXISTS attachment_bodies(attachment_id VARCHAR PRIMARY KEY,content_hash VARCHAR NOT NULL,size UINTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS embedding_state(singleton BOOLEAN PRIMARY KEY,profile JSON NOT NULL);
CREATE TABLE IF NOT EXISTS core_schema(singleton BOOLEAN PRIMARY KEY,version USMALLINT NOT NULL);
CREATE TABLE IF NOT EXISTS core_migrations(name VARCHAR PRIMARY KEY,state VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS parser_id_bindings(source VARCHAR,generated VARCHAR,assigned VARCHAR,PRIMARY KEY(source,generated));
CREATE TABLE IF NOT EXISTS archive_sync(singleton BOOLEAN PRIMARY KEY,version USMALLINT NOT NULL,user_id VARCHAR NOT NULL,device_id VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS archive_state(singleton BOOLEAN PRIMARY KEY,archive_id UUID NOT NULL,generation UBIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS archive_changes(kind VARCHAR,entity VARCHAR,generation UBIGINT,PRIMARY KEY(kind,entity));
CREATE TABLE IF NOT EXISTS retrieval_state(singleton BOOLEAN PRIMARY KEY,messages_generation UBIGINT NOT NULL,fts_generation UBIGINT,fts_definition_hash VARCHAR);
"""
ARCHIVE_COLUMNS={"conversations":["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"],"messages":["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"],"tool_calls":["id","message_id","tool_name","input","output","status","duration_ms","created_at"],"attachments":["id","message_id","filename","mime_type","size","path","url","created_at"],"artifacts":["id","conversation_id","artifact_type","title","content","language","created_at","version"],"file_edits":["id","message_id","file_path","edit_type","content","created_at","old_content"]}
_MSG_UPDATES,_MSG_UPS,PROVENANCE_KINDS=(u:=",".join(f"{c}=excluded.{c}" for c in ARCHIVE_COLUMNS["messages"][1:])+",embedding=excluded.embedding"),f"INSERT INTO messages ({','.join(ARCHIVE_COLUMNS['messages'])},embedding) SELECT x.*,m.embedding FROM (VALUES ({','.join('?'*len(ARCHIVE_COLUMNS['messages']))})) x({','.join(ARCHIVE_COLUMNS['messages'])}) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content ON CONFLICT(id) DO UPDATE SET {u}",{"repository.observed","file.observed","file.version","edit.observed","git.checkpoint","checkpoint.link"}
_PROVENANCE_ROWS={"file.observed":(lambda p,v,m,o:p["id"]==provenance_digest({"repository":p["repository"],"path":p["path"]}),"provenance file identity mismatch","INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",lambda p,v,m,o:(p["id"],p["repository"],p["path"],p["kind"])),"file.version":(lambda p,v,m,o:p["id"]==provenance_digest({"file":p["file"],"content":p["content_hash"]}),"provenance version identity mismatch","INSERT OR IGNORE INTO provenance.file_versions VALUES (?,?,?,?)",lambda p,v,m,o:(p["id"],p["file"],p["content_hash"],o)),"git.checkpoint":(lambda p,v,m,o:p["id"]==provenance_digest({"repository":p["repository"],"head":p["head"],"state":p["state_hash"]}),"provenance checkpoint identity mismatch","INSERT OR IGNORE INTO provenance.git_checkpoints VALUES (?,?,?,?,?,?,?)",lambda p,v,m,o:(p["id"],p["repository"],p["head"],p["state_hash"],json.dumps(p["paths"]),o,p["capture_source"])),"checkpoint.link":(lambda p,v,m,o:v["entity"]==provenance_digest({"checkpoint":p["checkpoint"],"edit":p["edit"]}),"provenance checkpoint link mismatch","INSERT OR IGNORE INTO provenance.checkpoint_edits VALUES (?,?,?)",lambda p,v,m,o:(p["checkpoint"],m("file_edits",p["edit"]),p["evidence"]))}
def provenance_digest(v): return hashlib.sha256(v if isinstance(v,bytes) else json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()).hexdigest()
def remote_id(author,table,source): return provenance_digest(f"{author}:{table}:{source}")[:16] if source is not None else None
def edit_parents(db,ids): return db.execute("SELECT CASE WHEN v.tool_call_id IS NULL THEN 'messages' ELSE 'tool_calls' END,COALESCE(v.tool_call_id,e.message_id) FROM file_edits e LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id WHERE e.id IN (SELECT UNNEST(?))",[list(ids)]).fetchall() if ids else []
def _archive_touch(db,rows=()):
    rows=list(rows)
    rows += edit_parents(db,[entity for kind,entity in rows if kind in ('file_edits','edit.observed')])
    generation=(db.execute("UPDATE archive_state SET generation=generation+1 WHERE singleton RETURNING generation").fetchone() or [0])[0]
    (((rows) and (_insert_pages(db,"archive_changes",[(kind,entity,generation) for kind,entity in sorted(set(rows))],mode=" OR REPLACE"))),((any(kind=="messages" for kind,_ in rows)) and (db.execute("UPDATE retrieval_state SET messages_generation=messages_generation+1 WHERE singleton"))))
    return generation
def archive_changes(db,since): return db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],db.execute("SELECT kind,entity FROM archive_changes WHERE generation>?",(since,)).fetchall()
def archive_state(db): return (lambda state,local:(*state,local))(db.execute("SELECT archive_id::VARCHAR,generation FROM archive_state WHERE singleton").fetchone(),sum(db.execute(f"SELECT COUNT(*) FROM {table} x WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name=? AND o.physical_row_id=x.id)",(table,)).fetchone()[0] for table in ARCHIVE_COLUMNS))
def _git_run(root,*args): return subprocess.run(("git","-C",str(root),*args),capture_output=True,check=True).stdout
def _git_maybe(root,*args):
    try: return _git_run(root,*args)
    except (subprocess.CalledProcessError,FileNotFoundError): return b""
@lru_cache(maxsize=4096)
def _git_root(path):
    if not (probe:=(probe if (probe:=Path(path)).is_dir() else probe.parent)).exists(): return None
    try: return Path(_git_run(probe,"rev-parse","--show-toplevel").decode().strip()).resolve()
    except subprocess.CalledProcessError as e: return None if b"not a git repository" in e.stderr.lower() or not probe.exists() else (_ for _ in ()).throw(e)
def _remote(url): return (lambda p,port:f"https://{p.hostname.lower()}{f':{port}' if port and port!={'ssh':22,'https':443,'http':80}.get(p.scheme.lower()) else ''}/{p.path.strip('/').removesuffix('.git')}" if p.hostname and p.path else None)(p:=__import__("urllib.parse").parse.urlparse(re.sub(r"^(?:[^/@]+@)?([^:/]+):",r"ssh://\1/",url) if "://" not in url else url),p.port)
def _git_remotes(root): return sorted({remote for line in _git_run(root,"remote","-v").decode(errors="replace").splitlines() if "\t" in line and not (url:=line.split("\t",1)[1].rsplit(" ",1)[0]).startswith(("/","file:")) and (remote:=_remote(url))})
def repository_evidence(value): return provenance_digest({"lineage":value["lineage"],"remotes":value["remotes"]}) if value["lineage"] else None
def _checkout(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}" if (stat:=next((p.stat() for p in [Path(root)/'.git'] if p.exists()),None)) else str(root))[:32]
def _unborn(root): return provenance_digest(f"{stat.st_dev}:{stat.st_ino}:{getattr(stat,'st_birthtime_ns',stat.st_ctime_ns)}" if (stat:=next((p.stat() for p in [Path(root)/'.git/config',Path(root)/'.git'] if p.exists()),None)) else str(root))
def _git_marker(path): return next(((str(root.resolve()),_checkout(root)) for raw in [Path(path)] for p in [raw if raw.is_dir() else raw.parent] for root in (p,*p.parents) if (root/".git").exists()),(None,None))
@lru_cache(maxsize=256)
def _repository(root): return (lambda root,roots,remotes,lineage:dict(id=provenance_digest({"lineage":lineage,"remotes":remotes}) if lineage else _unborn(root),lineage=lineage,root=str(root),roots=roots,remotes=remotes,checkout=_checkout(root)))(root:=Path(root),roots:=sorted(_git_maybe(root,"rev-list","--max-parents=0","HEAD").decode().split()),_git_remotes(root),provenance_digest({"git_roots":roots}) if roots else None)
def repository_state(db): return {"roots":dict(db.execute("SELECT root,repository FROM provenance.repository_checkouts").fetchall()),"checkouts":dict(db.execute("SELECT id,repository FROM provenance.repository_checkouts").fetchall()),"checkout_roots":dict(db.execute("SELECT id,root FROM provenance.repository_checkouts").fetchall()),"lineages":dict(db.execute("SELECT id,lineage FROM provenance.repositories").fetchall()),"aliases":dict(db.execute("SELECT evidence,CASE WHEN COUNT(DISTINCT repository)=1 THEN MIN(repository) END FROM provenance.repository_aliases GROUP BY evidence").fetchall())}
def _refresh_repository(): (_git_root.cache_clear(),_repository.cache_clear())
def repository(path,known=None,refresh=True): return (lambda value,state,evidence,bound,resolved:{**value,"id":resolved or value["id"],"alias":None if resolved or not evidence else evidence})(value:={**_repository(str(root)),"head":_git_maybe(root,"rev-parse","--verify","HEAD").decode().strip(),"branch":_git_maybe(root,"symbolic-ref","--short","HEAD").decode().strip()},state:=repository_state(known) if known is not None and hasattr(known,"execute") else known or {"roots":{},"checkouts":{},"checkout_roots":{},"lineages":{},"aliases":{}},evidence:=repository_evidence(value),bound:=state["checkouts"].get(value["checkout"]),bound if value["lineage"] and state["lineages"].get(bound)==value["lineage"] else evidence and state["aliases"].get(evidence)) if (not refresh or _refresh_repository() is None) and Path(path).exists() and (root:=_git_root(Path(path))) and Path(path).exists() else None
def _cached_repository(cache,root,known): return cache[key] if (key:=str(root)) in cache else cache.setdefault(key,repository(root,known,False))
def _observe_checkout(db,repo): return (db.execute("DELETE FROM provenance.repository_checkouts WHERE root=? AND id<>?",(repo["root"],repo["checkout"])),db.execute("INSERT INTO provenance.repository_checkouts VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET repository=excluded.repository,root=excluded.root,branch=excluded.branch,head=excluded.head",(repo["checkout"],repo["id"],repo["root"],repo["branch"],repo["head"])),repo["alias"] and db.execute("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",(repo["id"],repo["alias"])))
def capture_repository(path,db_path=None):
    with _core(db_path,ready=True,purpose="schema.repository") as db: known=repository_state(db)
    repo=repository(path,known)
    if not repo: return None
    record=_provenance_record("repository.observed",repo["id"],{k:repo[k] for k in ("id","lineage","roots","remotes","head")},datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    with _core(db_path,purpose="provenance.repository") as db,_transaction(db): ((_observe_checkout(db,repo)),(project_native_provenance(db,[record])))
    return repo
def _resolved(path,cwd=None): return str((Path(cwd)/p if not (p:=Path(path)).is_absolute() and cwd else p).expanduser().resolve())
def pending_scopes(conversations): return [(conversation,resolved,None,root,f"pending:{checkout}" if checkout else None,captured) for captured in [datetime.now(timezone.utc)] for conversation,cwd in conversations for resolved in [_resolved(cwd) if cwd else None] for root,checkout in [_git_marker(resolved) if resolved and Path(resolved).is_dir() else (None,None)]]
def snapshot_scopes(conversations,known=None): return (_refresh_repository(),[(conversation,resolved,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,captured) for captured in [datetime.now(timezone.utc).isoformat().replace("+00:00","Z")] for conversation,cwd in conversations for resolved in [str(Path(cwd).expanduser().resolve()) if cwd else None] for repo in [repository(resolved,known,False) if resolved else None]])[1]
def _provenance_where(path,cwd,cache,known=None,frozen=False): return (repo,p.relative_to(repo["root"]).as_posix(),"repository") if (repo:=_cached_repository(cache,root,known) if (root:=_git_root(p:=Path(path) if frozen else Path(_resolved(path,cwd)))) else None) else (None,f"external/{provenance_digest(str(p))[:24]}/{p.name}","external")
def pending_edit_scopes(edits): return [(e["id"],f"external/{provenance_digest(route)[:24]}/{Path(route).name}",None,root,f"pending:{checkout}" if checkout else None,route,captured) for captured in [datetime.now(timezone.utc)] for e in edits for route in [_resolved(e["path"],e["cwd"])] for root,checkout in [_git_marker(route) if not e["cwd"] or Path(e["cwd"]).is_dir() else (None,None)]]
def snapshot_edit_scopes(edits,known=None): return (_refresh_repository(),[(e["id"],path,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,route,captured) for captured,cache in [(datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),{})] for e in edits for route in [e.get("route") or _resolved(e["path"],e["cwd"])] for repo,path,_ in [_provenance_where(route,None,cache,known,True)]])[1]
def edit_scope_inputs(result): return (lambda conversations,messages:[{"id":e["id"],"path":e["file_path"],"cwd":conversations.get(messages.get(e["message_id"]))} for e in result.edits])({c["id"]:c["cwd"] for c in result.convs},{m["id"]:m["conversation_id"] for m in result.msgs})
def _checkpoint(repo,source): return (lambda paths,state:dict(id=provenance_digest({"repository":repo["id"],"head":repo["head"],"state":state}),repository=repo["id"],head=repo["head"],state_hash=state,paths=paths,capture_source=source))(sorted({x[3:].split(" -> ")[-1] for x in _git_run(repo["root"],"status","--porcelain=v1","-z").decode(errors="replace").split("\0") if len(x)>3}),provenance_digest((_git_maybe(repo["root"],"diff","--binary","HEAD")+_git_maybe(repo["root"],"diff","--binary","--cached","HEAD")) if repo["head"] else _git_run(repo["root"],"status","--porcelain=v1","-z")))
def _provenance_record(kind,entity,payload,observed_at): return dict(kind=kind,entity=entity,payload=payload,observed_at=observed_at)
def _repository_record(repo,observed): return _provenance_record("repository.observed",repo["id"],{k:repo[k] for k in ("id","lineage","roots","remotes","head")},observed)
def _checkpoint_records(repo,versions,source,observed): return [_provenance_record("git.checkpoint",cp["id"],cp,observed),*[_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":full},observed) for (rid,fid),(edit,after,full,path) in versions.items() if rid==repo["id"] for vid in [provenance_digest({"file":fid,"content":full})]],*[_provenance_record("checkpoint.link",provenance_digest({"checkpoint":cp["id"],"edit":edit}),{"checkpoint":cp["id"],"edit":edit,"evidence":"full_content_match"},observed) for (rid,fid),(edit,after,full,path) in versions.items() if rid==repo["id"] and path not in cp["paths"] and after==full]] if (cp:=_checkpoint(repo,source)) else []
def _provenance_edits(core,edit_ids=None): return [dict(zip(("id","path","type","content","old","ts","turn","conversation","cwd","scope_path","repository","root","checkout","route","scope_at"),r)) for r in core.execute("""SELECT fe.id,fe.file_path,fe.edit_type,fe.content,fe.old_content,CAST(fe.created_at AS VARCHAR),m.id,m.conversation_id,c.cwd,COALESCE(f.path,s.path),CASE WHEN f.id IS NOT NULL THEN f.repository ELSE s.repository END,s.root,CASE WHEN f.id IS NOT NULL AND s.checkout LIKE 'pending:%' THEN NULL ELSE s.checkout END,s.route,CAST(s.observed_at AS VARCHAR) FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id LEFT JOIN provenance.file_edit_scopes s ON s.file_edit_id=fe.id LEFT JOIN provenance.file_edit_files captured ON captured.file_edit_id=fe.id LEFT JOIN provenance.files f ON f.id=captured.file_id AND captured.evidence IS DISTINCT FROM 'legacy_scope_conflict' AND f.id=sha256(json_object('path',f.path,'repository',f.repository)) WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id) AND NOT EXISTS(SELECT 1 FROM remote.derived_edits d WHERE d.physical_id=fe.id)"""+" AND (NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_edit_id=fe.id) OR EXISTS (SELECT 1 FROM provenance.pending p WHERE p.kind='file_edits' AND p.entity=fe.id))"+(f" AND fe.id IN ({','.join('?'*len(ids))})" if ids else " AND FALSE" if ids==[] else "")+" ORDER BY fe.created_at,fe.id",ids or ()).fetchall()] if (ids:=sorted(set(edit_ids or ())) if edit_ids is not None else None) is not False else []
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
    return [*[_repository_record(repo,repo_times[rid]) for rid,repo in repos.items()],*records,*(record for repo in repos.values() for record in _checkpoint_records(repo,versions,source,captured))],repos
# Include unsigned rows; explicit history markers do not prove that a row is disposable.
def archive_relationships(db,include_empty=False):
    history=bool(db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='parser_tool_lineage'").fetchone())
    return {f"{table}.{column}":value for table,refs in ARCHIVE_FKS.items() for column,parent in refs for value in [dict(zip(("rows","parent_ids","marked_history_rows"),db.execute(f"SELECT count(*),count(DISTINCT c.{column}),{"count(*) FILTER (WHERE json_extract_string(c.metadata,'$.history_of') IS NOT NULL)" if table=='messages' else "count(*) FILTER (WHERE EXISTS(SELECT 1 FROM parser_tool_history h WHERE h.old_id=c.id))" if table=='tool_calls' and history else '0'} FROM {table} c LEFT JOIN {parent} p ON p.id=c.{column} WHERE p.id IS NULL"+(f" AND c.{column} IS NOT NULL" if (table,column)==('messages','parent_id') else '')).fetchone()))] if include_empty or value["rows"]}
def provenance_records(db,only=None,historical=False):
    ids,rows=lambda kind:[entity for k,entity in only or () if k==kind],lambda kind,sql,column="id":[] if only is not None and not ids(kind) else db.execute(sql+(f" WHERE {column} IN (SELECT json_extract_string(value,'$') FROM json_each(?))" if only is not None else ""),[json.dumps(ids(kind),separators=(",",":"))] if only is not None else []).fetchall()
    return [*[_provenance_record("repository.observed",r[0],dict(id=r[0],lineage=r[1],roots=json.loads(r[2]),remotes=json.loads(r[3]),head=r[4]),r[5]) for r in rows("repository.observed","SELECT id,lineage,CAST(roots AS VARCHAR),CAST(remotes AS VARCHAR),last_head,observed_at FROM provenance.repositories")],*[_provenance_record("file.observed",r[0],dict(zip(("id","repository","path","kind"),r)),None) for r in rows("file.observed","SELECT * FROM provenance.files")],*[_provenance_record("file.version",r[0],dict(zip(("id","file","content_hash"),r[:3])),r[3]) for r in rows("file.version","SELECT * FROM provenance.file_versions")],*[_provenance_record("edit.observed",r[0],dict(zip(("id","turn","file","repository","old_content_hash","new_content_hash","evidence"),r)),None) for r in rows("edit.observed","SELECT x.file_edit_id,fe.message_id,x.file_id,f.repository,x.old_content_hash,x.new_content_hash,x.evidence FROM provenance.file_edit_files x LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id JOIN file_edits fe ON fe.id=x.file_edit_id JOIN provenance.files f ON f.id=x.file_id AND ("+("TRUE OR " if historical else "")+"v.status='confirmed' OR EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.kind='edit.observed' AND o.physical_entity=x.file_edit_id))","x.file_edit_id")],*[_provenance_record("git.checkpoint",r[0],dict(id=r[0],repository=r[1],head=r[2],state_hash=r[3],paths=json.loads(r[4]),capture_source=r[6]),r[5]) for r in rows("git.checkpoint","SELECT id,repository,head,state_hash,CAST(paths AS VARCHAR),observed_at,capture_source FROM provenance.git_checkpoints")],*[_provenance_record("checkpoint.link",provenance_digest({"checkpoint":r[0],"edit":r[1]}),dict(zip(("checkpoint","edit","evidence"),r)),None) for r in rows("checkpoint.link","SELECT * FROM provenance.checkpoint_edits","sha256(json_object('checkpoint',checkpoint_id,'edit',file_edit_id))")]]
def provenance_issue(db,value,map_id=lambda table,value:value,replace_file=False):
    p,k=value["payload"],value["kind"]
    if k=="repository.observed" and (old:=db.execute("SELECT lineage FROM provenance.repositories WHERE id=?",(p["id"],)).fetchone()) and old[0] and p["lineage"] and old[0]!=p["lineage"]: return f"repository lineage conflict: {p['id']} stored={old[0]} incoming={p['lineage']}"
    if k!="edit.observed": return None
    edit,turn=map_id("file_edits",p["id"]),map_id("messages",p["turn"])
    if not (row:=db.execute("SELECT message_id FROM file_edits WHERE id=?",(edit,)).fetchone()): return f"provenance dependency missing: edit={edit} logical={p['id']} turn={turn}"
    if row[0]!=turn: return f"provenance edit/turn mismatch: edit={edit} stored={row[0]} incoming={turn}"
    if not replace_file and (old:=db.execute("SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id=?",(edit,)).fetchone()) and old[0]!=p["file"]: return f"provenance edit scope conflict: edit={edit} stored={old[0]} incoming={p['file']}"
    return None
@contextlib.contextmanager
def preserve_fact_heads(db,keys,observed=()):
    # Fresh source writers may establish bases; receiving own replicas is not a source observation.
    keys=list(keys)
    keys+=edit_parents(db,[entity for kind,entity in keys if kind in ('file_edits','edit.observed')])
    keys,observed=(keys:={(k,entity) for kind,entity in keys for k in ((kind,"edit.observed" if kind=="file_edits" else "file_edits") if kind in ("file_edits","edit.observed") else (kind,))}),keys if observed is True else set(observed)
    rows,((found, paths))=(rows:=db.execute("WITH wanted AS (SELECT json_extract_string(value,'$[0]') kind,json_extract_string(value,'$[1]') entity FROM json_each(?)), origins AS (SELECT o.* FROM remote.provenance_origins o JOIN wanted w ON o.kind=w.kind AND o.physical_entity=w.entity UNION ALL SELECT o.table_name,o.physical_row_id,o.workspace_id,o.author_user_id,o.source_row_id,o.proof_id FROM remote.row_origins o JOIN wanted w ON o.table_name=w.kind AND o.physical_row_id=w.entity), proofs AS MATERIALIZED (SELECT p.* FROM remote.row_proofs p WHERE p.source_row_id IN (SELECT entity FROM wanted UNION SELECT source_entity FROM origins)), claims AS (SELECT p.id,p.row_kind,o.physical_entity,p.source_row_id,p.author_user_id,p.state,p.content_hash,p.revision FROM wanted w JOIN origins o ON (o.kind,o.physical_entity)=(w.kind,w.entity) JOIN proofs p ON (p.row_kind,p.source_row_id,p.author_user_id)=(o.kind,o.source_entity,o.author_user_id) UNION SELECT p.id,p.row_kind,p.source_row_id,p.source_row_id,p.author_user_id,p.state,p.content_hash,p.revision FROM wanted w JOIN proofs p ON (p.row_kind,p.source_row_id)=(w.kind,w.entity)) SELECT DISTINCT id,row_kind,physical_entity,source_row_id,author_user_id,state,content_hash,revision FROM claims p WHERE id IN (SELECT proof_id FROM origins) OR EXISTS (SELECT 1 FROM remote.local_row_bases b WHERE (b.kind,b.entity,b.author,b.revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) OR NOT EXISTS (SELECT 1 FROM proofs c WHERE (c.row_kind,c.source_row_id,c.author_user_id,c.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision))",[json.dumps(list(keys),separators=(",",":"))]).fetchall() if keys else []),(typed_logical_rows(db,[r[1:6] for r in rows],historical=True) if rows else {},captured_edit_paths(db,[r[2] for r in rows if r[1]=="file_edits"]))
    kept=[(r[0],r[6],r[1:6],body) for r in rows if (body:=matching_logical_row(found[r[1:6]],r[6],[paths[r[2]]] if r[1]=="file_edits" and r[2] in paths else ())) is not None]
    yield
    found,paths=typed_logical_rows(db,[claim for pid,expected,claim,body in kept]) if kept else {},captured_edit_paths(db,[claim[1] for pid,expected,claim,body in kept if claim[0]=="file_edits"])
    if lost:=[(pid,json.dumps(body,sort_keys=True,separators=(",",":"))) for pid,expected,claim,body in kept if matching_logical_row(found[claim],expected,[paths[claim[1]]] if claim[0]=="file_edits" and claim[1] in paths else ()) is None]: (_insert_pages(db,"remote.row_conflicts",lost,mode=" OR IGNORE"),(ids:={pid for pid,body in lost}),(bases:=[(r[1],r[3],r[4],r[7]) for r in rows if r[0] in ids and r[1] in ARCHIVE_COLUMNS and r[2]==r[3] and (r[1],r[2]) in observed]) and _insert_pages(db,"remote.local_row_bases",bases,mode=" OR IGNORE"))
def project_provenance(db,value,map_id=lambda table,value:value,touch=True,replace_file=False,preserve=True,native=False):
    p,k,observed=value["payload"],value["kind"],value["observed_at"]
    if k not in PROVENANCE_KINDS: return False
    if k!="checkpoint.link" and p["id"]!=value["entity"]: raise ValueError("provenance entity mismatch")
    if issue:=provenance_issue(db,value,map_id,replace_file): raise ValueError(issue)
    with preserve_fact_heads(db,[(k,map_id("file_edits",p["id"]) if k=="edit.observed" else value["entity"])]) if preserve and (native or k in ("repository.observed","edit.observed")) else contextlib.nullcontext():
        if k=="repository.observed": db.execute("INSERT INTO provenance.repositories VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET lineage=excluded.lineage,roots=excluded.roots,remotes=excluded.remotes,last_head=COALESCE(excluded.last_head,repositories.last_head),observed_at=COALESCE(excluded.observed_at,repositories.observed_at)",(p["id"],p["lineage"],json.dumps(p["roots"]),json.dumps(p["remotes"]),p.get("head"),observed))
        elif k=="edit.observed": db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,?,?,?) ON CONFLICT(file_edit_id) DO UPDATE SET file_id=excluded.file_id,old_content_hash=excluded.old_content_hash,new_content_hash=excluded.new_content_hash,evidence=excluded.evidence",(map_id("file_edits",p["id"]),p["file"],p["old_content_hash"],p["new_content_hash"],p["evidence"]))
        else:
            check,error,sql,args=_PROVENANCE_ROWS[k]
            ((required(check(p,value,map_id,observed),ValueError(error))),(db.execute(sql.replace("OR IGNORE","OR REPLACE") if native else sql,args(p,value,map_id,observed))))
    return bool(_archive_touch(db,[(k,value["entity"])])) if touch else True
def project_native_provenance(db,records):
    keys,local=(keys:=[(r["kind"],r["entity"]) for r in records]),set(db.execute("SELECT kind,entity FROM provenance.local_facts WHERE entity IN (SELECT UNNEST(?))",[[entity for kind,entity in keys]]).fetchall()) if keys else set()
    current={(r['kind'],r['entity']):logical_fact(r) for r in provenance_records(db,{key for key in keys if key[0] in ('file.observed','edit.observed','checkpoint.link')})}
    records,keys=(records:=[r for r in records if (key:=(r['kind'],r['entity'])) not in local or current.get(key)!=logical_fact(r)]),[(r['kind'],r['entity']) for r in records]
    with preserve_fact_heads(db,keys,observed=True): ([project_provenance(db,record,preserve=False,native=key not in local,touch=False) for record,key in zip(records,keys)],keys and _archive_touch(db,keys))
    _insert_pages(db,"provenance.local_facts",keys,mode=" OR IGNORE")
def repair_legacy_edit_scopes(db,apply=False):
    # Only migration placeholders: preserve the unique historical file, never reinterpret today's checkout.
    changes=[(r[:7],(r[0],r[8],r[9],*r[3:7])) for r in db.execute("SELECT s.*,f.id,f.path,f.repository FROM provenance.file_edit_scopes s JOIN file_edits fe ON fe.id=s.file_edit_id JOIN provenance.file_edit_files x ON x.file_edit_id=s.file_edit_id JOIN provenance.files f ON f.id=x.file_id WHERE s.repository IS NULL AND s.root IS NULL AND s.checkout IS NULL AND s.route IS NULL AND s.observed_at IS NULL AND x.evidence IS DISTINCT FROM 'legacy_scope_conflict' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=s.file_edit_id) ORDER BY s.file_edit_id").fetchall() if r[1]==f"external/{provenance_digest(r[0])[:24]}/unknown" and r[8] is not None and (r[8],r[9])!=(r[1],r[2]) and r[7]==provenance_digest({"repository":r[9],"path":r[8]})]
    return (apply and changes and (_insert_pages(db,"provenance.file_edit_scopes",[after for before,after in changes],mode=" OR REPLACE"),_archive_touch(db,[("file_edits",before[0]) for before,after in changes])),changes)[1]
def _provenance_generations(db,edits,conversations): return set(db.execute("SELECT kind,entity,generation FROM archive_changes WHERE kind='file_edits' AND entity IN (SELECT UNNEST(?)) OR kind='conversations' AND entity IN (SELECT UNNEST(?))",[edits,conversations]).fetchall())
def capture_provenance(path=None,edit_ids=None,conversation_ids=None,source="sync",strict=True):
    blocked=set()
    while True:
        try: return _capture_provenance(path,edit_ids,conversation_ids,source,blocked)
        except subprocess.CalledProcessError as error:
            root=str(Path(error.cmd[2]).resolve()) if len(error.cmd)>2 and error.cmd[:2] in (("git","-C"),["git","-C"]) else None
            if strict or root is None or root in blocked: raise
            blocked.add(root)
            log_parse_error(f"Git provenance pending for {root}: {(error.stderr or b'').decode(errors='replace').strip()}",error)
        except (OSError,ProvenanceChanged) as error:
            if strict: raise
            return log_parse_error('Provenance pending; captured conversations are committed',error) or []
def _capture_provenance(path=None,edit_ids=None,conversation_ids=None,source="sync",blocked=()):
    targeted,eids,cids=edit_ids is not None or conversation_ids is not None,sorted(set(edit_ids or ())),sorted(set(conversation_ids or ()))
    excluded=lambda value: bool(value) and any(Path(value).is_relative_to(root) for root in blocked)
    def snapshot(core):
        edits,known,conversations,files=_provenance_edits(core,eids if targeted else None),repository_state(core),core.execute("SELECT s.conversation,s.cwd,s.repository,s.root,s.checkout,CAST(s.observed_at AS VARCHAR) FROM provenance.conversation_scopes s WHERE (s.checkout LIKE 'pending:%' OR s.repository IS NOT NULL AND NOT EXISTS (SELECT 1 FROM provenance.repository_checkouts c WHERE (c.id,c.repository,c.root)=(s.checkout,s.repository,s.root)))"+(" AND s.conversation IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall(),[] if targeted else core.execute("SELECT f.id,f.repository,f.path,c.root,c.id FROM provenance.files f JOIN provenance.repository_checkouts c ON c.repository=f.repository").fetchall()
        edits,conversations,files=[e for e in edits if e['id'] not in failed_edits],[c for c in conversations if c[0] not in failed_convs],[f for f in files if not excluded(f[3])]
        edit_keys,conversation_keys=sorted(set(eids)|{e['id'] for e in edits}),sorted(set(cids)|{c[0] for c in conversations})
        return edits,known,conversations,files,edit_keys,conversation_keys,_provenance_generations(core,edit_keys,conversation_keys)
    with _core(path,True,purpose="provenance.plan") as core:
        failed_convs={cid for cid,root in core.execute('SELECT c.id,COALESCE(s.root,c.cwd) FROM conversations c LEFT JOIN provenance.conversation_scopes s ON s.conversation=c.id').fetchall() if excluded(root)} if blocked else set()
        failed_edits={eid for eid,cid,root in core.execute('SELECT e.id,m.conversation_id,COALESCE(s.root,s.route,e.file_path) FROM file_edits e LEFT JOIN messages m ON m.id=e.message_id LEFT JOIN provenance.file_edit_scopes s ON s.file_edit_id=e.id').fetchall() if cid in failed_convs or excluded(root)} if blocked else set()
        queued=core.execute("SELECT kind,entity FROM provenance.pending WHERE NOT (kind='conversations' AND entity IN (SELECT UNNEST(?)) OR kind='file_edits' AND entity IN (SELECT UNNEST(?))) ORDER BY generation,kind,entity"+(" LIMIT 500" if targeted else ""),[list(failed_convs),list(failed_edits)]).fetchall()
        selected=list(dict.fromkeys([*queued,*(('file_edits',e) for e in eids),*(('conversations',c) for c in cids)]))[:500 if targeted else None]
        eids,cids=[entity for kind,entity in selected if kind=='file_edits'],[entity for kind,entity in selected if kind=='conversations']
        eids,cids=sorted(set(eids)-failed_edits),sorted(set(cids)-failed_convs)
        if targeted and not eids and not cids: return []
        if eids: cids=sorted(set(cids)|{r[0] for r in core.execute("SELECT DISTINCT m.conversation_id FROM file_edits fe JOIN messages m ON m.id=fe.message_id WHERE fe.id IN (SELECT UNNEST(?))",[eids]).fetchall()})
        missing,missing_edits=core.execute("SELECT c.id,c.cwd FROM conversations c WHERE c.cwd IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) AND NOT EXISTS (SELECT 1 FROM provenance.conversation_scopes s WHERE s.conversation=c.id)"+(" AND c.id IN (SELECT UNNEST(?))" if targeted else ""),[cids] if targeted else []).fetchall(),[dict(id=r[0],path=r[1],cwd=r[2]) for r in core.execute("SELECT fe.id,fe.file_path,c.cwd FROM file_edits fe JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id) AND NOT EXISTS(SELECT 1 FROM remote.derived_edits d WHERE d.physical_id=fe.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_scopes s WHERE s.file_edit_id=fe.id)"+(" AND fe.id IN (SELECT UNNEST(?))" if targeted else ""),[eids] if targeted else []).fetchall()]
        missing,missing_edits=[c for c in missing if c[0] not in failed_convs],[e for e in missing_edits if e["id"] not in failed_edits]
        material=None if missing or missing_edits else snapshot(core)
    scopes,edit_scopes=pending_scopes(missing),pending_edit_scopes(missing_edits)
    if scopes or edit_scopes:
        with _core(path,purpose="provenance.scopes") as core,_transaction(core): (_insert_pages(core,"provenance.conversation_scopes",scopes,mode=" OR IGNORE"),_insert_pages(core,"provenance.file_edit_scopes",edit_scopes,("file_edit_id","path","repository","root","checkout","route","observed_at"),mode=" OR IGNORE"))
    if material is None:
        with _core(path,True,purpose="provenance.snapshot") as core: material=snapshot(core)
    edits,known,conversations,files,eids,cids,generations=material
    if targeted and not queued and not edits and not conversations: return []
    _refresh_repository()
    cache,scopes,conversations=(cache:={}), (scopes:=[(conversation,cwd,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,observed) for conversation,cwd,rid,root,checkout,observed in conversations if checkout and checkout.startswith("pending:") for marker in [checkout.removeprefix("pending:")] for repo in [_cached_repository(cache,root,known) if (root,marker)==_git_marker(cwd) else None]]),[{r[0]:r for r in scopes}.get(row[0],row) for row in conversations]
    edit_scopes,frozen,edits,(records,repos,captured)=(edit_scopes:=[(e["id"],relative,repo["id"] if repo else None,repo["root"] if repo else None,repo["checkout"] if repo else None,e["route"],e["scope_at"]) for e in edits if e["checkout"] and e["checkout"].startswith("pending:") for marker in [e["checkout"].removeprefix("pending:")] for repo,relative,_ in [_provenance_where(e["route"],None,cache,known,True) if (e["root"],marker)==_git_marker(e["route"]) else (None,e["scope_path"],"external")]]),(frozen:={r[0]:r for r in edit_scopes}),(edits:=[{**e,**dict(zip(("scope_path","repository","root","checkout","route","scope_at"),frozen[e["id"]][1:]))} if e["id"] in frozen else e for e in edits]),(*_observe_provenance(edits,source,known,conversations,cache),datetime.now(timezone.utc).isoformat().replace("+00:00","Z"))
    repos.update({rid:repo for root,rid in (() if targeted else known["roots"].items()) if not excluded(root) and (repo:=_cached_repository(cache,root,known)) and repo["id"]==rid})
    records += [record for rid,repo in repos.items() if not any(r["kind"]=="git.checkpoint" and r["payload"]["repository"]==rid for r in records) for record in _checkpoint_records(repo,{},source,captured)]
    records += [_provenance_record("file.version",vid,{"id":vid,"file":fid,"content_hash":content},captured) for fid,rid,relative,root,checkout in files for target,repo in [(Path(root,relative),_cached_repository(cache,root,known))] if repo and (repo["id"],repo["checkout"])==(rid,checkout) and target.is_file() for content in [provenance_digest(target.read_bytes())] for vid in [provenance_digest({"file":fid,"content":content})]]
    stale,touched=[] if targeted else [root for root,rid in known["roots"].items() if not excluded(root) and (not (repo:=_cached_repository(cache,root,known)) or repo["id"]!=rid)],sorted({e["conversation"] for e in edits})
    with _core(path,purpose="provenance.commit") as core,_transaction(core):
        ((((((required(generations==_provenance_generations(core,eids,cids),ProvenanceChanged("Provenance inputs changed during observation; committed conversations are preserved and provenance will retry."))),((stale) and (core.execute("DELETE FROM provenance.repository_checkouts WHERE root IN (SELECT UNNEST(?))",[stale]))))),((scopes) and (core.executemany("UPDATE provenance.conversation_scopes SET cwd=?,repository=?,root=?,checkout=?,observed_at=? WHERE conversation=? AND checkout LIKE 'pending:%'",[(cwd,rid,root,checkout,observed,conversation) for conversation,cwd,rid,root,checkout,observed in scopes]))))),((edit_scopes) and (core.executemany("UPDATE provenance.file_edit_scopes SET path=?,repository=?,root=?,checkout=?,route=?,observed_at=? WHERE file_edit_id=? AND checkout LIKE 'pending:%'",[(relative,rid,root,checkout,route,observed,edit) for edit,relative,rid,root,checkout,route,observed in edit_scopes]))))
        for repo in repos.values(): _observe_checkout(core,repo)
        ((((project_native_provenance(core,records)),((scopes or touched) and (_archive_touch(core,[("conversations",r[0]) for r in scopes]+[("conversations",c) for c in touched]))))),(core.execute("DELETE FROM provenance.pending WHERE kind='file_edits' AND entity IN (SELECT UNNEST(?)) OR kind='conversations' AND entity IN (SELECT UNNEST(?))",[eids,cids])))
    return records
def project_archive_row(db,table,columns,values,origin=None,touch=True,preserve=True): return (project_archive_rows(db,table,columns,[(values,origin)],preserve),touch and _archive_touch(db,[(table,values[0])]),None)[-1]
def _insert_pages(db,target,rows,columns=None,conflict="",mode="",embedding=False):
    schema,columns,shape,norm,extra,source,pages=(schema:={r[0]:r[1] for r in db.execute(f"DESCRIBE {target}").fetchall()}),(columns:=columns or list(schema)),(shape:=json.dumps([{c:schema[c] for c in columns}])),(norm:=lambda c,v:json.loads(v) if schema[c]=="JSON" and isinstance(v,str) else v),",embedding" if embedding else "","SELECT x.*,m.embedding FROM UNNEST(from_json(?,?)) t(x) LEFT JOIN messages m ON m.id=x.id AND m.content IS NOT DISTINCT FROM x.content" if embedding else "SELECT x.* FROM UNNEST(from_json(?,?)) t(x)",[(json.dumps([{c:norm(c,v) for c,v in zip(columns,row)} for row in rows[i:i+500]],default=str,ensure_ascii=True,allow_nan=False),shape) for i in range(0,len(rows),500)]
    return pages and db.executemany(f"INSERT{mode} INTO {target} ({','.join(columns)}{extra}) {source}{conflict}",pages)
def project_archive_rows(db,table,columns,rows,preserve=True):
    fields,values,origins,updates=(fields:=("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at")),[r[0] for r in rows],[(table,v[0],*(o[k] for k in fields),o.get("proof_id")) for v,o in rows if o],_MSG_UPDATES if table=="messages" else ','.join(f"{c}=excluded.{c}" for c in columns[1:])
    required(table in ARCHIVE_COLUMNS and columns==ARCHIVE_COLUMNS[table] and not any(len(v)!=len(columns) or o and set(o) not in (set(fields),set(fields)|{"proof_id"}) for v,o in rows),ValueError("record schema/entity mismatch"))
    ids,owned,occupied=(ids:=[v[0] for v,o in rows]),{r[0]:(r[1],r[2]) for r in db.execute("SELECT physical_row_id,author_user_id,source_row_id FROM remote.row_origins WHERE table_name=? AND physical_row_id IN (SELECT UNNEST(?))",(table,ids)).fetchall()},{r[0] for r in db.execute(f"SELECT id FROM {table} WHERE id IN (SELECT UNNEST(?))",(ids,)).fetchall()}
    required(not (conflict:=next((v[0] for v,o in rows if v[0] in owned and (not o or owned[v[0]]!=(o["author_user_id"],o["source_row_id"])) or o and v[0] in occupied and v[0] not in owned),None)),ValueError(f"archive ownership conflict: {table}:{conflict}"))
    with preserve_fact_heads(db,[(table,row_id) for row_id in ids if row_id in occupied],observed=[(table,v[0]) for v,o in rows if not o]) if preserve else contextlib.nullcontext():
        _insert_pages(db,table,values,columns,f" ON CONFLICT(id) DO UPDATE SET {updates}",embedding=table=="messages")
        if table=="file_edits":
            for mode,foreign in ((" OR REPLACE",True),(" OR IGNORE",False)): _insert_pages(db,"provenance.file_edit_evidence",[(v[0],"unverified","signed_replica_missing_evidence",None) for v,o in rows if bool(o)==foreign],mode=mode)
        (((origins) and ((_insert_pages(db,"remote.row_origins",origins,mode=" OR REPLACE"),db.execute("DELETE FROM remote.row_references r USING remote.row_origins o WHERE (r.table_name,r.physical_row_id,r.author_user_id,r.source_row_id)=(o.table_name,o.physical_row_id,o.author_user_id,o.source_row_id) AND r.table_name=? AND r.physical_row_id IN (SELECT UNNEST(?))",[table,ids])))),((table in ("file_edits","tool_calls")) and (_apply_signed_edit_evidence(db,[v[0] for v in values] if table=="file_edits" else (),[v[0] for v in values] if table=="tool_calls" else ()))))
def claim_row_owners(db,records):
    claims={(kind,entity,p['author_user_id'],p['author_device_id']) for row,p in records if row['kind'] in ARCHIVE_COLUMNS|{'edit.observed':(),'checkpoint.link':()} for kind,entity in ([(row['kind'],row['id']),*[(parent,row['data'][column]) for column,parent in ARCHIVE_FKS.get(row['kind'],()) if row['state']=='active' and row['data'][column]],*[(kind,entity) for e in (row['data'] or {}).get('edits',[]) for kind,entity in [('file_edits',e['id']),('messages',e['message_id'])]]] if row['kind'] in ARCHIVE_COLUMNS else [('file_edits',row['id']),('messages',row['data']['turn'])] if row['kind']=='edit.observed' else [('file_edits',row['data']['edit'])])}
    if not claims: return
    db.execute('CREATE OR REPLACE TEMP TABLE owner_claims(kind VARCHAR,source VARCHAR,author VARCHAR,device VARCHAR)')
    _insert_pages(db,'owner_claims',list(claims))
    required(not db.execute('SELECT 1 FROM (SELECT * FROM owner_claims UNION ALL SELECT o.* FROM remote.row_owners o JOIN owner_claims c USING(kind,source,author)) GROUP BY kind,source,author HAVING count(DISTINCT device)>1 LIMIT 1').fetchone(),ValueError('Source device ownership conflict; replica cannot revise this conversation'))
    db.execute('INSERT OR IGNORE INTO remote.row_owners SELECT * FROM owner_claims')
def project_row_proofs(db,proofs,root_public,certificate):
    if not proofs: return []
    fields,expected,signer,packed,columns=(fields:=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")),{"v","kind",*fields},(proofs[0]["author_user_id"],proofs[0]["author_device_id"]),json.dumps(certificate,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False),("workspace_id","authorization_workspace_id","row_kind","source_row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    if any(set(proof)!=expected or proof["v"]!=1 or proof["kind"]!="row.proof" or (proof["author_user_id"],proof["author_device_id"])!=signer for proof in proofs) or set(certificate)!={"v","user","device","issued_at","signature"} or certificate["v"]!=1 or set(certificate["device"])!={"id","name","sign_public","box_public"} or (certificate["user"],certificate["device"]["id"])!=signer: raise ValueError("row proof storage schema mismatch")
    if (old:=db.execute("SELECT root_public,CAST(certificate AS VARCHAR) FROM remote.row_signers WHERE author_user_id=? AND author_device_id=?",signer).fetchone()) and (old[0]!=root_public or json.loads(old[1])["device"]!=certificate["device"]): raise ValueError("row signer conflict")
    ids=[provenance_digest(proof) for proof in proofs]
    (db.execute("INSERT OR IGNORE INTO remote.row_signers VALUES (?,?,?,?)",(*signer,root_public,packed)),_insert_pages(db,"remote.row_proofs",[(pid,*(proof[k] for k in fields)) for pid,proof in zip(ids,proofs)],("id",*columns),mode=" OR IGNORE"))
    return ids
def project_row_proof(db,proof,root_public,certificate): return project_row_proofs(db,[proof],root_public,certificate)[0]
def project_attested_rows(db,records,root_public,certificate):
    claim_row_owners(db,records)
    proofs,pending,_=[proof for row,proof in records],set(),required(all((row["kind"],row["id"],row["v"],row["state"],provenance_digest(row))==(proof["row_kind"],proof["row_id"],proof["encoding_v"],proof["state"],proof["content_hash"]) for row,proof in records),ValueError("attestation snapshot/proof mismatch"))
    items=[(row,proof,pid,True) for (row,proof),pid in zip(records,project_row_proofs(db,proofs,root_public,certificate))]
    _retain_lossy_replicas(db,_protect_native_replicas(db,items,pending.add),pending.add)
    return (_insert_pages(db,"remote.row_conflicts",[(pid,json.dumps(row,sort_keys=True,separators=(",",":"))) for row,proof,pid,native in items if pid in pending],mode=" OR IGNORE"),(prior:=[(p["row_kind"],p["row_id"],p["author_user_id"],p["previous_revision"]) for p in proofs if p["previous_revision"]]) and retire_row_bodies(db,prior),record_local_row_bases(db,proofs),[repair_parent_links(db,author,{(row["kind"],row["id"]) for row,p in records}) for author in {p["author_user_id"] for p in proofs}])[-2]
def record_local_row_bases(db,proofs,seed=False):
    rows=db.execute("SELECT p.row_kind,p.source_row_id,p.author_user_id,p.revision FROM remote.row_proofs p WHERE p.id IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_conflicts c WHERE c.proof_id=p.id) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision))",[[provenance_digest(p) for p in proofs]]).fetchall() if seed else [(p["row_kind"],p["row_id"],p["author_user_id"],p["revision"]) for p in proofs]
    return _insert_pages(db,"remote.local_row_bases",rows,conflict=" ON CONFLICT(kind,entity,author) DO NOTHING" if seed else " ON CONFLICT(kind,entity,author) DO UPDATE SET revision=excluded.revision WHERE local_row_bases.revision IS DISTINCT FROM excluded.revision")
def captured_edit_paths(db,ids): return dict(db.execute("SELECT x.file_edit_id,CASE WHEN l.entity IS NOT NULL THEN f.path END FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id JOIN provenance.local_facts e ON (e.kind,e.entity)=('edit.observed',x.file_edit_id) LEFT JOIN provenance.local_facts l ON (l.kind,l.entity)=('file.observed',f.id) WHERE x.file_edit_id IN (SELECT json_extract_string(value,'$') FROM json_each(?))",[json.dumps(list(ids),separators=(",",":"))]).fetchall()) if ids else {}
ROW_FIELDS_V1={"conversations":("source","title","created_at","updated_at","model","project_id","metadata"),"messages":("conversation_id","role","content","thinking","created_at","model","metadata","parent_id"),"tool_calls":("message_id","tool_name","input","output","status","duration_ms","created_at"),"attachments":("message_id","filename","mime_type","size","body_hash","created_at"),"artifacts":("conversation_id","artifact_type","title","content","language","created_at","version"),"file_edits":("message_id","file_path","edit_type","content","created_at","old_content")}
ROW_FIELDS_V1={k:v+('edits',) if k in ('messages','tool_calls') else v for k,v in ROW_FIELDS_V1.items()}
PROVENANCE_FIELDS_V1={"repository.observed":("lineage","roots","remotes"),"file.observed":("repository","path","kind"),"file.version":("file","content_hash","observed_at"),"edit.observed":("turn","file","repository","old_content_hash","new_content_hash","evidence"),"git.checkpoint":("repository","head","state_hash","paths","observed_at","capture_source"),"checkpoint.link":("checkpoint","edit","evidence")}
ROW_JSON_V1,ROW_TIME_V1={"metadata","input","output"},{"created_at","updated_at"}
ARCHIVE_FKS={"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}
def logical_row(table,columns=(),values=(),identity=None,v=1,state="active"):
    if v!=1 or table not in ROW_FIELDS_V1 or state not in ("active","deleted") or state=="deleted" and (not identity or columns or values) or len(columns)!=len(values) or len(set(columns))!=len(columns): raise ValueError("invalid logical row schema")
    if state=="deleted": return {"v":v,"kind":table,"id":identity,"state":state,"data":None}
    row,required=({'edits':[]} if table in ('messages','tool_calls') else {})|dict(zip(columns,values)),{"id",*ROW_FIELDS_V1[table]}
    if not required<=set(row): raise ValueError("incomplete logical row")
    norm=lambda k,v: json.loads(v) if v is not None and k in ROW_JSON_V1 and isinstance(v,str) else v.isoformat(timespec="microseconds") if v is not None and k in ROW_TIME_V1 and isinstance(v,datetime) else v
    return {"v":v,"kind":table,"id":identity or row["id"],"state":state,"data":{k:norm(k,row[k]) for k in ROW_FIELDS_V1[table]}}
def logical_fact(record):
    kind,p=record["kind"],record["payload"]
    if kind not in PROVENANCE_FIELDS_V1 or record["entity"]!=(p.get("id") if kind!="checkpoint.link" else provenance_digest({"checkpoint":p["checkpoint"],"edit":p["edit"]})): raise ValueError("invalid provenance fact")
    return {"v":1,"kind":kind,"id":record["entity"],"state":"active","data":{k:(v.isoformat(timespec="microseconds") if isinstance(v,datetime) else v) for k in PROVENANCE_FIELDS_V1[kind] for v in [record["observed_at"] if k=="observed_at" else p[k]]}}
def matching_logical_row(row,expected,paths=()):
    if row is None or provenance_digest(row)==expected: return row
    candidates=[row,*([{**row,'data':{k:v for k,v in row['data'].items() if k!='edits'}}] if row['state']=='active' and 'edits' in row['data'] else [])]
    for field,value in (row["data"] or {}).items():
        if field!="file_path" and (field not in ("created_at","updated_at","observed_at") or not isinstance(value,str)): continue
        try: variants=paths if field=="file_path" else ((parsed:=datetime.fromisoformat(value)).isoformat(),parsed.isoformat(timespec="microseconds"))
        except ValueError: continue
        candidates += [{**candidate,"data":{**candidate["data"],field:variant}} for candidate in candidates for variant in variants if variant!=value]
    return next((candidate for candidate in candidates if provenance_digest(candidate)==expected),None)
def _apply_signed_edit_evidence(db,edits=(),tools=(),source=False):
    restored=[(physical,retired_projection("tool_calls",physical,body,json.loads(raw))) for physical,raw,encoded,stored,expected in db.execute("SELECT DISTINCT r.physical,r.projection,r.body,r.content_hash,p.content_hash FROM remote.file_edit_evidence_claims s JOIN remote.row_proofs p ON (p.workspace_id,p.author_user_id,p.row_kind,p.source_row_id,p.revision,p.state)=(s.workspace_id,s.author_user_id,'tool_calls',s.source_tool_call_id,s.tool_revision,'active') JOIN parser_retired_rows r ON r.kind='tool_calls' AND r.source=p.source_row_id AND (r.author=p.author_user_id OR r.author='') JOIN messages m ON m.id=json_extract_string(r.projection,'$.message_id') WHERE NOT EXISTS(SELECT 1 FROM remote.file_edit_evidence_ancestors a WHERE (a.workspace_id,a.author_user_id,a.object_id,a.ancestor_revision)=(s.workspace_id,s.author_user_id,s.object_id,s.revision)) AND (s.source_edit_id IN (SELECT UNNEST(?)) OR r.physical IN (SELECT UNNEST(?))) AND NOT EXISTS(SELECT 1 FROM tool_calls t WHERE t.id=r.physical) AND (EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='tool_calls' AND o.physical_row_id=r.physical AND o.proof_id=p.id) OR r.physical=p.source_row_id AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='tool_calls' AND o.physical_row_id=r.physical) AND EXISTS(SELECT 1 FROM remote.local_row_bases b WHERE (b.kind,b.entity,b.author,b.revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)))",[list(edits),list(tools)]).fetchall() if provenance_digest(body:=json.loads(encoded))==stored and matching_logical_row(body,expected) is not None]
    if restored: (_insert_pages(db,"tool_calls",[row for physical,row in restored],ARCHIVE_COLUMNS["tool_calls"],mode=" OR IGNORE"),_archive_touch(db,[("tool_calls",physical) for physical,row in restored]))
    where,args=(" AND p.source_row_id IN (SELECT UNNEST(?))",[list(edits)]) if edits and source else (" AND fe.id IN (SELECT UNNEST(?))",[list(edits)]) if edits else (" AND EXISTS (SELECT 1 FROM leaves s JOIN evidence_origins tor ON tor.table_name='tool_calls' AND (tor.author_user_id,tor.source_row_id)=(s.author_user_id,s.source_tool_call_id) WHERE (s.workspace_id,s.author_user_id,s.source_edit_id,s.edit_revision)=(p.workspace_id,p.author_user_id,p.source_row_id,p.revision) AND tor.physical_row_id IN (SELECT UNNEST(?)))",[list(tools)]) if tools else required(False,ValueError("signed edit evidence target required"))
    native=db.execute("SELECT b.kind,b.entity,b.author,p.id,p.content_hash FROM remote.local_row_bases b JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(b.kind,b.entity,b.author,b.revision) WHERE b.kind IN ('file_edits','tool_calls') AND (b.entity IN (SELECT UNNEST(?)) OR b.entity IN (SELECT source_tool_call_id FROM remote.file_edit_evidence_proofs WHERE source_edit_id IN (SELECT UNNEST(?))) OR b.entity IN (SELECT source_edit_id FROM remote.file_edit_evidence_proofs WHERE source_tool_call_id IN (SELECT UNNEST(?)))) AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE (o.table_name,o.physical_row_id)=(b.kind,b.entity))",[list(edits or tools),list(edits),list(tools)]).fetchall()
    claims,bodies=(claims:=[(kind,entity,entity,author,'active') for kind,entity,author,pid,expected in native]),typed_logical_rows(db,claims) if claims else {}
    db.execute("CREATE OR REPLACE TEMP TABLE core_evidence_origins AS SELECT table_name,physical_row_id,source_row_id,author_user_id,proof_id FROM remote.row_origins WHERE FALSE")
    _insert_pages(db,'core_evidence_origins',[(kind,entity,entity,author,pid) for (kind,entity,author,pid,expected),claim in zip(native,claims) if matching_logical_row(bodies[claim],expected) is not None])
    ((db.execute("""CREATE OR REPLACE TEMP TABLE core_signed_edit_evidence AS WITH evidence_origins AS (SELECT table_name,physical_row_id,source_row_id,author_user_id,proof_id FROM remote.row_origins UNION ALL SELECT * FROM core_evidence_origins), leaves AS (SELECT p.*,count(*) OVER (PARTITION BY workspace_id,author_user_id,object_id) n FROM remote.file_edit_evidence_claims p WHERE NOT EXISTS (SELECT 1 FROM remote.file_edit_evidence_ancestors a WHERE a.object_kind='file-edit.evidence' AND (a.workspace_id,a.author_user_id,a.object_id,a.ancestor_revision)=(p.workspace_id,p.author_user_id,p.object_id,p.revision))), foreign_edits AS (SELECT fe.id file_edit_id,p.workspace_id,p.author_user_id,p.source_row_id,p.revision FROM file_edits fe JOIN evidence_origins o ON o.table_name='file_edits' AND o.physical_row_id=fe.id JOIN remote.row_proofs p ON p.id=o.proof_id AND p.state='active' AND (EXISTS(SELECT 1 FROM remote.row_origins own WHERE own.table_name='file_edits' AND own.physical_row_id=fe.id) OR EXISTS(SELECT 1 FROM leaves s WHERE (s.workspace_id,s.author_user_id,s.source_edit_id,s.edit_revision)=(p.workspace_id,p.author_user_id,p.source_row_id,p.revision)))"""+where+" AND NOT EXISTS(SELECT 1 FROM remote.derived_edits d WHERE d.physical_id=fe.id)"+"""), matches AS (SELECT f.file_edit_id,s.revision semantic_revision,s.n,s.status,s.reason,tc.id tool_call_id FROM foreign_edits f JOIN leaves s ON (s.workspace_id,s.author_user_id,s.source_edit_id,s.edit_revision)=(f.workspace_id,f.author_user_id,f.source_row_id,f.revision) LEFT JOIN remote.row_proofs tp ON (tp.workspace_id,tp.author_user_id,tp.row_kind,tp.source_row_id,tp.revision,tp.state)=(s.workspace_id,s.author_user_id,'tool_calls',s.source_tool_call_id,s.tool_revision,'active') LEFT JOIN evidence_origins tor ON tor.table_name='tool_calls' AND tor.proof_id=tp.id LEFT JOIN tool_calls tc ON tc.id=tor.physical_row_id WHERE s.source_tool_call_id IS NULL OR tc.id IS NOT NULL) SELECT f.file_edit_id,CASE WHEN max(m.n)>1 THEN 'unverified' WHEN count(m.semantic_revision)=1 THEN max(m.status) ELSE 'unverified' END status,CASE WHEN max(m.n)>1 THEN 'signed_evidence_conflict' WHEN count(m.semantic_revision)=1 THEN max(m.reason) ELSE 'signed_replica_missing_evidence' END reason,CASE WHEN max(m.n)=1 AND count(m.semantic_revision)=1 THEN max(m.tool_call_id) END tool_call_id FROM foreign_edits f LEFT JOIN matches m USING(file_edit_id) GROUP BY f.file_edit_id""",args)),((touched:=db.execute("SELECT x.file_edit_id FROM core_signed_edit_evidence x LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id WHERE (v.status,v.reason,v.tool_call_id) IS DISTINCT FROM (x.status,x.reason,x.tool_call_id)").fetchall()) and ((db.execute("INSERT OR REPLACE INTO provenance.file_edit_evidence SELECT * FROM core_signed_edit_evidence"),_archive_touch(db,[("file_edits",r[0]) for r in touched])))))
def provider_session_key(source,session): return match[1] if source=='codex' and isinstance(session,str) and (match:=re.fullmatch(r'rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',session)) else session
def project_provider_bindings(db,source,session,conversation,members): return (required(db.execute("SELECT 1 FROM conversations WHERE id=?",(conversation,)).fetchone() and all(isinstance(v,str) and v for v in (source,session,conversation,*members)),ValueError("provider binding target unavailable")),members and db.execute("UPDATE provider_sessions SET conversation_id=? WHERE conversation_id IN (SELECT UNNEST(?))",(conversation,members)),db.executemany("INSERT OR REPLACE INTO provider_sessions VALUES (?,?,?)",[(source,key,conversation) for key in sorted({session,provider_session_key(source,session)})]),None)[-1]
def project_workspace_controls(db,controls):
    for value in controls:
        if not {"workspace","revision","epoch"}<=set(value) or not isinstance(value["revision"],int) or not isinstance(value["epoch"],int): raise ValueError("workspace control storage schema mismatch")
        raw,key,proof,old=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False),(key:=(value["workspace"],value["revision"])),provenance_digest(value),db.execute("SELECT state_hash FROM remote.workspace_controls WHERE workspace_id=? AND revision=?",key).fetchone()
        if old and old[0]!=proof: raise ValueError("workspace control conflict")
        db.execute("INSERT OR IGNORE INTO remote.workspace_controls VALUES (?,?,?,?,?)",(*key,value["epoch"],proof,raw))
    return len(controls)
def _logical_parts(row,proof,proof_id,native,parent_map):
    table,source,mapped,physical,origin=(table:=row["kind"]),(source:=row["id"]),(mapped:=lambda kind,value:(parent_map or {}).get((kind,value),value if native else remote_id(proof["author_user_id"],kind,value))),*((source,None) if native else (mapped(table,source),{"workspace_id":proof["workspace"],"author_user_id":proof["author_user_id"],"author_device_id":proof["author_device_id"],"source_row_id":source,"source_event_id":proof["revision"],"content_key":f"{table}:{source}","observed_at":None,"proof_id":proof_id}))
    return table,source,mapped,physical,origin
def _logical_archive(row,proof,proof_id,native=False,parent_map=None):
    table,source,mapped,physical,origin=_logical_parts(row,proof,proof_id,native,parent_map)
    columns,data,parents=ARCHIVE_COLUMNS[table],{"id":source,**row["data"]},dict(ARCHIVE_FKS.get(table,()))
    return table,physical,[physical if c=="id" else mapped(parents[c],data[c]) if c in parents else json.dumps(data[c],sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False) if c in ("metadata","input","output") and data.get(c) is not None else data.get(c) for c in columns],origin,data
def retired_projection(kind,physical,body,extra): return [json.dumps(value) if k in ('metadata','input','output') and k not in extra and value is not None else value for k in ARCHIVE_COLUMNS[kind] for value in [physical if k=='id' else extra[k] if k in extra else body['data'][k]]]
def repair_parent_links(db,local_user=None,parents=None):
    changed,available=0,{parent for parent in {p for refs in ARCHIVE_FKS.values() for column,p in refs} if parents is None or db.execute("SELECT 1 FROM remote.row_references WHERE table_name=? AND author_user_id=? AND physical_row_id IN (SELECT UNNEST(?)) LIMIT 1",[parent,local_user,[remote_id(local_user,kind,value) for kind,value in parents if kind==parent]]).fetchone()}
    for table,refs in ARCHIVE_FKS.items():
        for column,parent in refs:
            if parent not in available: continue
            after=""
            while rows:=db.execute(f"""SELECT c.id,c.{column},p.id,o.source_row_id,o.author_user_id,h.content_hash FROM {table} c JOIN remote.row_origins o ON o.table_name=? AND o.physical_row_id=c.id JOIN remote.row_proofs h ON h.id=o.proof_id AND h.state='active' AND h.row_kind=o.table_name AND h.source_row_id=o.source_row_id AND h.author_user_id=o.author_user_id JOIN remote.row_references r ON r.table_name=? AND r.physical_row_id=c.{column} AND r.author_user_id=o.author_user_id JOIN {parent} p ON p.id=r.source_row_id WHERE c.id>? AND NOT EXISTS(SELECT 1 FROM {parent} missing WHERE missing.id=c.{column}) AND NOT EXISTS(SELECT 1 FROM remote.row_origins foreign_parent WHERE foreign_parent.table_name=? AND foreign_parent.physical_row_id=p.id) AND r.physical_row_id=substr(sha256(to_json(r.author_user_id||':'||?||':'||r.source_row_id)),1,16) AND (o.author_user_id=? OR ? IS NULL AND EXISTS(SELECT 1 FROM remote.local_row_bases b WHERE b.kind=? AND b.entity=p.id AND b.author=o.author_user_id))"""+(" AND p.id IN (SELECT UNNEST(?))" if parents is not None else "")+" ORDER BY c.id LIMIT 500",[table,parent,after,parent,parent,local_user,local_user,parent]+([[value for kind,value in parents if kind==parent]] if parents is not None else [])).fetchall():
                claims,before,selected=(claims:=[(table,r[0],r[3],r[4],'active') for r in rows]),(before:=typed_logical_rows(db,claims)),[r for r,claim in zip(rows,claims) if matching_logical_row(before[claim],r[5]) is not None]
                if selected: (db.executemany(f"UPDATE {table} SET {column}=? WHERE id=?",[(r[2],r[0]) for r in selected]),required(before==typed_logical_rows(db,claims),ValueError("Parent repair changed signed logical content")),_archive_touch(db,[(table,r[0]) for r in selected]))
                changed,after=changed+len(selected),rows[-1][0]
    return changed
def _protect_native_replicas(db,items,defer):
    refs,wanted,existing=(refs:={"conversation_id":"conversations","message_id":"messages","parent_id":"messages","turn":"messages","edit":"file_edits"}),(wanted:={(row["kind"],row["id"]) for row,p,pid,native,*maps in items if native}|{(refs[key],value) for row,p,pid,native,*maps in items if native for key,value in (row["data"] or {}).items() if key in refs and value}),{table:{row[0]:row for row in db.execute(f"SELECT {','.join(ARCHIVE_COLUMNS[table])} FROM {table} WHERE id IN (SELECT json_extract_string(value,'$') FROM json_each(?))",[json.dumps(ids,separators=(",",":"))]).fetchall()} if (ids:=[value for kind,value in wanted if kind==table]) else {} for table in ARCHIVE_COLUMNS}
    received,occupied,bindings=(received:=db.execute("SELECT author_user_id,table_name,source_row_id,physical_row_id FROM remote.row_origins WHERE source_row_id IN (SELECT UNNEST(?)) OR physical_row_id IN (SELECT UNNEST(?))",[[value for kind,value in wanted]]*2).fetchall()),(occupied:={(kind,physical) for author,kind,source,physical in received}),{author:({(kind,source):physical for user,kind,source,physical in received if user==author and source not in existing[kind]}|{(kind,source):source for kind,rows in existing.items() for source in rows if (kind,source) not in occupied}) for author in {p["author_user_id"] for row,p,pid,native,*maps in items if native}}
    [bindings[p["author_user_id"]].setdefault((row["kind"],row["id"]),row["id"]) for row,p,pid,native,*maps in items if native and (row["kind"],row["id"]) not in occupied]
    paths,selected,unchanged=captured_edit_paths(db,list(existing["file_edits"])),[],[]
    embedded={(kind,entity):edits for kind in ('messages','tool_calls') for entity,edits in edit_metadata(db,kind,list(existing[kind])).items()}
    for item in items:
        row,p,pid,native,*maps=item
        bound=bindings[p["author_user_id"]] if native else {}
        if native: item=(row,p,pid,(native:=bound.get((row["kind"],row["id"]),row["id"])==row["id"]),bound|(maps[0] if maps else {}))
        if not native or row["kind"] not in existing or not (old:=existing[row["kind"]].get(row["id"])):
            selected.append(item)
            continue
        columns,raw,norm=(columns:=[*ARCHIVE_COLUMNS[row["kind"]]]),(raw:=dict(zip(columns,old))),lambda key,value:row["data"][key] if row["data"] and key in refs and value==bound.get((refs[key],row["data"][key]),row["data"][key]) else json.loads(value) if key in ("metadata","input","output") and isinstance(value,str) else value.isoformat() if isinstance(value,datetime) else value
        data={key:norm(key,raw.get(key)) for key in (row["data"] or {})}
        if row['kind'] in ('messages','tool_calls') and row['state']=='active': data['edits']=embedded.get((row['kind'],row['id']),[])
        if row["kind"]=="file_edits" and row["id"] in paths and row["data"] and row["data"]["file_path"]==paths[row["id"]]: data["file_path"]=paths[row["id"]]
        if row["kind"]=="attachments": data["body_hash"]=(db.execute("SELECT content_hash FROM attachment_bodies WHERE attachment_id=?",[row["id"]]).fetchone() or [None])[0]
        if row["state"]=="active" and matching_logical_row({**row,"data":data},p["content_hash"]) is not None:
            unchanged.append(p)
            continue
        defer(pid)
    return (record_local_row_bases(db,unchanged),selected)[-1]
def project_logical_rows(db,items,defer=False):
    evidence=items
    if defer: authors,parents,items={p["author_user_id"] for row,p,pid,native,*maps in items if native},{(row["kind"],row["id"]) for row,p,pid,native,*maps in items}|{(parent,value) for row,p,pid,native,*maps in items for column,parent in ARCHIVE_FKS.get(row["kind"],()) if (value:=(row["data"] or {}).get(column))},_protect_native_replicas(db,items,defer)
    delayed,logical,out=[item for item in items if item[0]["kind"] in PROVENANCE_KINDS or item[0]["state"]=="deleted"],(logical:=[_logical_archive(row,proof,pid,native,maps[0] if maps else None) for row,proof,pid,native,*maps in items if row["kind"] not in PROVENANCE_KINDS and row["state"]!="deleted"]),[(table,physical) for table,physical,values,origin,data in logical]
    local_columns={(table,r[0]):dict(zip(ARCHIVE_COLUMNS[table],r)) for table in ('conversations','attachments') for r in db.execute(f"SELECT {','.join(ARCHIVE_COLUMNS[table])} FROM {table} WHERE id IN (SELECT UNNEST(?))",[[physical for kind,physical,values,origin,data in logical if kind==table]]).fetchall()}
    logical=[(table,physical,[local_columns[(table,physical)][column] if (table,physical) in local_columns and column not in ('id',*ROW_FIELDS_V1[table]) else value for column,value in zip(ARCHIVE_COLUMNS[table],values)],origin,data) for table,physical,values,origin,data in logical]
    local=set(db.execute("SELECT kind,entity FROM provenance.local_facts WHERE entity IN (SELECT UNNEST(?))",[ids]).fetchall()) if (ids:=[source if table=="repository.observed" else mapped("file_edits",source) for row,p,pid,native,*maps in delayed for table,source,mapped,physical,origin in [_logical_parts(row,p,pid,native,maps[0] if maps else None)] if table in ("repository.observed","edit.observed")]) else set()
    with preserve_fact_heads(db,[(table,mapped("file_edits",source) if table=="edit.observed" else source if table=="repository.observed" else physical) for row,p,pid,native,*maps in items for table,source,mapped,physical,origin in [_logical_parts(row,p,pid,native,maps[0] if maps else None)] if table in ("repository.observed","edit.observed","file_edits") or native and table in ARCHIVE_COLUMNS]):
        for table in ARCHIVE_COLUMNS:
            if rows := [(values,origin) for kind,physical,values,origin,data in logical if kind==table]: project_archive_rows(db,table,ARCHIVE_COLUMNS[table],rows,False)
        if bodies:=[(physical,data["body_hash"],data["size"]) for table,physical,values,origin,data in logical if table=="attachments" and data["body_hash"]]: _insert_pages(db,"attachment_bodies",bodies,mode=" OR REPLACE")
        project_edit_metadata(db,items)
        facts=[(item,physical) for item in delayed if (physical:=project_logical_row(db,*item[:4],touch=False,parent_map=item[4] if len(item)>4 else None,defer=defer,preserve=False,local=local,record_origin=False)) is not None]
        (((out.extend((item[0]["kind"],physical) for item,physical in facts),_insert_pages(db,"remote.provenance_origins",[(item[0]["kind"],physical,item[1]["workspace"],item[1]["author_user_id"],item[0]["id"],item[2]) for item,physical in facts if item[0]["kind"] in PROVENANCE_KINDS],mode=" OR REPLACE"))),((defer) and ((_retain_lossy_replicas(db,items,defer),[repair_parent_links(db,author,parents) for author in authors]))))
    (record_local_row_bases(db,[p for row,p,pid,native,*maps in items if native and row['kind'] in ARCHIVE_COLUMNS]),[_apply_signed_edit_evidence(db,ids if kind=='file_edits' else (),ids if kind=='tool_calls' else ()) for kind in ('file_edits','tool_calls') if (ids:=[row['id'] for row,p,pid,native,*maps in items if native and row['kind']==kind])])
    return (out and _archive_touch(db,out),out)[-1]
def restore_signed_bodies(db,records):
    # Caller holds the core transaction; restore exact existing claims without changing their proofs or projections.
    proofs,existing,fresh=(proofs:={r[0]:r for r in db.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))",[[r["proof_id"] for r in records]]).fetchall()}),(existing:=dict(db.execute("SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))",[[r["proof_id"] for r in records]]).fetchall())),[r for r in records if required((p:=proofs.get(r["proof_id"])) is not None and tuple(r["proof_row"])==p and (r["body"]["kind"],r["body"]["id"],r["body"]["v"],r["body"]["state"],provenance_digest(r["body"]))==(p[3],p[4],p[5],p[9],p[6]) and (p[0] not in existing or provenance_digest(json.loads(existing[p[0]]))==p[6]),ValueError("Retained recovery body/proof mismatch")) and r["proof_id"] not in existing]
    return (required(all(r["body"]["kind"]!="attachments" or r["body"]["state"]!="active" or r["body"]["data"]["body_hash"] is None or attachment_index(blob:=Path(next((v[2] for v in db.execute("PRAGMA database_list").fetchall() if v[2]),"")).parent/"attachments"/(r["body"]["data"]["body_hash"] or ""),r["body"]["data"]["size"])==(r["body"]["data"]["body_hash"],r["body"]["data"]["size"]) and (_fsync(blob),_fsync(blob.parent),_fsync(blob.parent.parent),True)[-1] for r in fresh),ValueError("Restored attachment body is unavailable")),_insert_pages(db,"remote.row_conflicts",[(r["proof_id"],json.dumps(r["body"],sort_keys=True,separators=(",",":"))) for r in fresh],mode=" OR IGNORE"),_archive_touch(db,[("retained.body",r["proof_id"]) for r in fresh]),len(fresh))[-1] if fresh else 0
def restore_retired_proofs(db,targets=None): return (_insert_pages(db,'remote.row_conflicts',rows,mode=' OR IGNORE'),_archive_touch(db,[('retained.body',pid) for pid,body in rows])) if (rows:=[(pid,body) for pid,expected,raw in db.execute("SELECT p.id,p.content_hash,r.body FROM parser_retired_rows r JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id)=(r.kind,r.source) AND (r.author='' OR r.author=p.author_user_id) WHERE "+("r.physical IN (SELECT UNNEST(?))" if targets is not None else "NOT EXISTS(SELECT 1 FROM remote.row_conflicts c WHERE c.proof_id=p.id)"),[targets] if targets is not None else []).fetchall() if (body:=matching_logical_row(json.loads(raw),expected)) is not None]) else None
# A resolved fork still needs both signed branch bodies; a proof alone cannot recover their contents.
def retire_row_bodies(db,revisions): return db.execute("DELETE FROM remote.row_conflicts c USING remote.row_proofs p,(SELECT x.* FROM UNNEST(from_json(?,?)) t(x)) r WHERE c.proof_id=p.id AND NOT EXISTS(SELECT 1 FROM remote.row_proofs h WHERE (h.row_kind,h.source_row_id,h.author_user_id)=(p.row_kind,p.source_row_id,p.author_user_id) GROUP BY h.previous_revision HAVING count(DISTINCT h.revision)>1) AND NOT EXISTS(SELECT 1 FROM remote.local_row_bases b WHERE (b.kind,b.entity,b.author,b.revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) AND NOT EXISTS(SELECT 1 FROM parser_retired_rows h WHERE (h.kind,h.source)=(p.row_kind,p.source_row_id) AND (h.author='' OR h.author=p.author_user_id)) AND (p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(r.kind,r.source,r.author,r.revision) AND (NOT EXISTS (SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) OR NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.proof_id=p.id) AND NOT EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.proof_id=p.id))",(json.dumps([dict(kind=kind,source=source,author=author,revision=revision) for kind,source,author,revision in revisions],separators=(",",":")),'[{"kind":"VARCHAR","source":"VARCHAR","author":"VARCHAR","revision":"VARCHAR"}]'))
def project_edit_dependencies(db,waiting=(),arrived=(),processed=(),advanced=()):
    clean={key for key,pid in waiting}|{key for key,pid in advanced}|{r[0] for r in db.execute("SELECT dependency_key FROM remote.edit_dependencies WHERE proof_id IN (SELECT UNNEST(?))",[list(processed)]).fetchall()} if waiting or processed or advanced else set()
    (((((((((processed) and (db.execute("DELETE FROM remote.edit_dependencies WHERE proof_id IN (SELECT UNNEST(?))",[list(processed)]))),((waiting) and (_insert_pages(db,"remote.edit_dependencies",list(waiting),mode=" OR IGNORE"))))),((advanced) and (_insert_pages(db,"remote.edit_ready",list(advanced),mode=" OR REPLACE"))))),((arrived) and (db.execute("INSERT OR REPLACE INTO remote.edit_ready SELECT DISTINCT dependency_key,'' FROM remote.edit_dependencies WHERE dependency_key IN (SELECT UNNEST(?))",[list(arrived)]))))),((clean) and (db.execute("DELETE FROM remote.edit_ready r WHERE r.dependency_key<>'' AND r.dependency_key IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.edit_dependencies d WHERE d.dependency_key=r.dependency_key AND d.proof_id>r.after_proof_id)",[list(clean)]))))
    return bool(db.execute("SELECT 1 FROM remote.edit_ready WHERE dependency_key<>'' OR after_proof_id<>'done' LIMIT 1").fetchone())
def logical_references(db,ids,references=True):
    sql=("SELECT table_name,physical_row_id,author_user_id,source_row_id FROM remote.row_references WHERE physical_row_id IN (SELECT json_extract_string(value,'$') FROM json_each(?)) UNION ALL " if references else "")+"SELECT table_name,physical_row_id,author_user_id,source_row_id FROM remote.row_origins WHERE physical_row_id IN (SELECT json_extract_string(value,'$') FROM json_each(?))"
    return {(table,physical,user):source for table,physical,user,source in db.execute(sql,[json.dumps(list(ids),separators=(",",":"))]*(2 if references else 1)).fetchall()} if ids else {}
def edit_metadata(db,kind,ids):
    owner,where=('v.tool_call_id','v.tool_call_id IS NOT NULL') if kind=='tool_calls' else ('e.message_id','v.tool_call_id IS NULL')
    rows=db.execute(f"SELECT {owner},COALESCE(d.source_id,e.id),COALESCE(mo.source_row_id,e.message_id),CASE WHEN d.physical_id IS NOT NULL THEN e.file_path ELSE f.path END,e.edit_type,e.content,e.created_at,e.old_content,COALESCE(v.status,'unverified'),COALESCE(v.reason,'source_unavailable') FROM file_edits e LEFT JOIN remote.row_origins mo ON mo.table_name='messages' AND mo.physical_row_id=e.message_id LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id LEFT JOIN remote.derived_edits d ON d.physical_id=e.id LEFT JOIN provenance.file_edit_files ef ON ef.file_edit_id=e.id LEFT JOIN provenance.files f ON f.id=ef.file_id AND EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='file.observed' AND l.entity=f.id) WHERE {where} AND {owner} IN (SELECT UNNEST(?)) ORDER BY {owner},COALESCE(d.source_id,e.id)",[list(ids)]).fetchall() if kind in ('messages','tool_calls') and ids else []
    return {owner:[dict(zip(('id','message_id','file_path','edit_type','content','created_at','old_content','status','reason'),[v.isoformat(timespec='microseconds') if isinstance(v,datetime) else v for v in row[1:]])) for row in values] for owner,values in itertools.groupby(rows,key=lambda v:v[0])}
def project_edit_metadata(db,items):
    parents=[(row,p,native,*_logical_parts(row,p,pid,native,maps[0] if maps else None)[2:4]) for row,p,pid,native,*maps in items if row['kind'] in ('messages','tool_calls')]
    if not parents: return
    required(all(isinstance(edits:=(row['data'] or {}).get('edits',[]),list) and len({e['id'] for e in edits})==len(edits) and all(set(e)=={'id','message_id','file_path','edit_type','content','created_at','old_content','status','reason'} and e['status'] in ('confirmed','invalid','unknown','unverified') for e in edits) for row,p,native,mapped,physical in parents),ValueError('Invalid tool edit metadata'))
    rows=[(mapped('file_edits',e['id']),e['id'],p['author_user_id'],row['kind'],physical,mapped('messages',e['message_id']),e,native) for row,p,native,mapped,physical in parents for e in (row['data'] or {}).get('edits',[])]
    received={(kind,parent):{rid for rid,source,user,k,p,message,e,native in rows if (k,p)==(kind,parent)} for row,p,native,mapped,physical in parents if not native for kind,parent in [(row['kind'],physical)]}
    native_deleted={(row['kind'],physical) for row,p,native,mapped,physical in parents if native and row['state']=='deleted'}
    stale=[rid for rid,kind,parent in db.execute('SELECT physical_id,parent_kind,parent_id FROM remote.derived_edits WHERE parent_id IN (SELECT UNNEST(?))',[[physical for row,p,native,mapped,physical in parents]]).fetchall() if (kind,parent) in received and rid not in received[(kind,parent)]]
    stale += [rid for rid,kind,parent in db.execute("SELECT e.id,CASE WHEN v.tool_call_id IS NULL THEN 'messages' ELSE 'tool_calls' END,COALESCE(v.tool_call_id,e.message_id) FROM file_edits e LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id WHERE e.message_id IN (SELECT UNNEST(?)) OR v.tool_call_id IN (SELECT UNNEST(?))",[[parent for kind,parent in native_deleted]]*2).fetchall() if (kind,parent) in native_deleted] if native_deleted else []
    incoming={rid:(kind,parent) for rid,source,user,kind,parent,message,e,native in rows if not native}
    required(len({rid for rid,*rest in rows})==len(rows) and all((kind,parent)==incoming[rid] for rid,kind,parent in db.execute('SELECT e.id,d.parent_kind,d.parent_id FROM file_edits e LEFT JOIN remote.derived_edits d ON d.physical_id=e.id WHERE e.id IN (SELECT UNNEST(?))',[list(incoming)]).fetchall()),ValueError('Derived edit ownership conflict'))
    with preserve_fact_heads(db,[('file_edits',eid) for eid in stale]+[('checkpoint.link',provenance_digest(dict(checkpoint=c,edit=e))) for c,e in db.execute('SELECT checkpoint_id,file_edit_id FROM provenance.checkpoint_edits WHERE file_edit_id IN (SELECT UNNEST(?))',[stale]).fetchall()]):
        for table,column in [('file_edits','id'),*[(f'provenance.{t}','file_edit_id') for t in ('file_edit_evidence','file_edit_files','file_edit_scopes','checkpoint_edits')],('remote.derived_edits','physical_id')]:
            if stale: db.execute(f'DELETE FROM {table} WHERE {column} IN (SELECT UNNEST(?))',[stale])
    _insert_pages(db,'file_edits',[(rid,message,*[e[k] for k in ('file_path','edit_type','content','created_at','old_content')]) for rid,source,user,kind,parent,message,e,native in rows],mode=' OR REPLACE')
    _insert_pages(db,'provenance.file_edit_evidence',[(rid,e['status'],e['reason'],parent if kind=='tool_calls' else None) for rid,source,user,kind,parent,message,e,native in rows],mode=' OR REPLACE')
    _insert_pages(db,'remote.derived_edits',[(rid,source,user,kind,parent) for rid,source,user,kind,parent,message,e,native in rows if not native],mode=' OR REPLACE')
def typed_logical_rows(db,claims,references=True,historical=False):
    claims,found=tuple(claims),{}
    retired={(k,p,s,a):json.loads(body) for k,p,s,a,body in db.execute("SELECT kind,physical,source,author,body FROM parser_retired_rows WHERE physical IN (SELECT json_extract_string(value,'$') FROM json_each(?)) QUALIFY count(*) OVER(PARTITION BY kind,physical,source,author)=1",[json.dumps([c[1] for c in claims])]).fetchall()} if claims and db.execute("SELECT 1 FROM information_schema.tables WHERE table_name='parser_retired_rows'").fetchone() else {}
    for table in ARCHIVE_COLUMNS:
        if not (ids:=[physical for kind,physical,source,user,state in claims if kind==table and state=="active"]): continue
        cur=db.execute(f"SELECT * EXCLUDE (embedding) FROM {table} WHERE id IN (SELECT json_extract_string(value,'$') FROM json_each(?))",[json.dumps(ids,separators=(",",":"))]) if table=="messages" else db.execute("SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id WHERE a.id IN (SELECT json_extract_string(value,'$') FROM json_each(?))",[json.dumps(ids,separators=(",",":"))]) if table=="attachments" else db.execute(f"SELECT * FROM {table} WHERE id IN (SELECT json_extract_string(value,'$') FROM json_each(?))",[json.dumps(ids,separators=(",",":"))])
        found.update(((table,r[0]),(cols,r)) for cols in [[d[0] for d in cur.description]] for r in cur.fetchall())
        if table in ('messages','tool_calls'):
            edits=edit_metadata(db,table,ids)
            found.update({(table,rid):(cols+['edits'],[*raw,edits.get(rid,[])]) for (kind,rid),(cols,raw) in list(found.items()) if kind==table})
    facts,((mapped, out))=(facts:={(r["kind"],r["entity"]):r for r in provenance_records(db,{(kind,physical) for kind,physical,source,user,state in claims if kind in PROVENANCE_KINDS},historical)}),(logical_references(db,{raw[cols.index(column)] for (kind,physical),(cols,raw) in found.items() for column,parent in ARCHIVE_FKS.get(kind,()) if raw[cols.index(column)]}|{record["payload"][field] for record in facts.values() for field in ("turn","edit") if field in record["payload"]},references),{})
    for claim in claims:
        kind,physical,source,user,state=claim
        if kind in ARCHIVE_COLUMNS and state=="deleted": row=logical_row(kind,identity=source,state="deleted")
        elif kind in ARCHIVE_COLUMNS and (value:=found.get((kind,physical))):
            cols,raw,parents=*value,dict(ARCHIVE_FKS.get(kind,()))
            row=logical_row(kind,cols,[source if column=="id" else mapped.get((parents[column],item,user),item) if column in parents else item for column,item in zip(cols,raw)],source)
        elif kind in PROVENANCE_KINDS and (record:=facts.get((kind,physical))):
            payload={**record["payload"],**({"id":source} if kind!="checkpoint.link" else {})}
            for field,parent in (("turn","messages"),("edit","file_edits")):
                if field in payload: payload[field]=mapped.get((parent,payload[field],user),payload[field])
            try: row=logical_fact({**record,"entity":source,"payload":payload})
            except ValueError: row=None
        else: row=retired.get((kind,physical,source,user),retired.get((kind,physical,source,''))) if state=='active' else None
        out[claim]=row
    return out
def _retain_lossy_replicas(db,items,defer):
    refs,wanted,mapped=(refs:={"conversation_id":"conversations","message_id":"messages","parent_id":"messages","turn":"messages","edit":"file_edits"}),(wanted:={(refs[key],mapped(refs[key],value),p["author_user_id"]):value for row,p,pid,native,*maps in items for table,source,mapped,physical,origin in [_logical_parts(row,p,pid,native,maps[0] if maps else None)] for key,value in (row["data"] or {}).items() if key in refs and value}),logical_references(db,{physical for table,physical,user in wanted})
    if missing:={key:value for key,value in wanted.items() if key not in mapped and key[1]!=value}: _insert_pages(db,"remote.row_references",[(*key,value) for key,value in missing.items()],mode=" OR IGNORE")
    claims,found=(claims:=[(pid,p,(table,mapped("file_edits",source) if table=="edit.observed" else provenance_digest({"checkpoint":row["data"]["checkpoint"],"edit":mapped("file_edits",row["data"]["edit"])}) if table=="checkpoint.link" else source if table in PROVENANCE_KINDS else physical,source,p["author_user_id"],row["state"])) for row,p,pid,native,*maps in items for table,source,mapped,physical,origin in [_logical_parts(row,p,pid,native,maps[0] if maps else None)]]),typed_logical_rows(db,[claim for pid,p,claim in claims])
    [defer(pid) for pid,p,claim in claims if matching_logical_row(found[claim],p["content_hash"]) is None]
def project_logical_row(db,row,proof,proof_id,native=False,touch=True,parent_map=None,defer=False,preserve=True,local=None,record_origin=True):
    table,source,mapped,physical,origin=_logical_parts(row,proof,proof_id,native,parent_map)
    if table in PROVENANCE_KINDS:
        data,value=(data:={"id":source,**row["data"]}),{"kind":table,"entity":source,"payload":data,"observed_at":data.pop("observed_at",None)}
        replace_file=table=="edit.observed" and bool(db.execute("WITH RECURSIVE ancestors(revision) AS (SELECT CAST(? AS VARCHAR) UNION SELECT p.previous_revision FROM remote.row_proofs p JOIN ancestors a ON p.revision=a.revision WHERE p.row_kind=? AND p.source_row_id=? AND p.author_user_id=? AND p.previous_revision IS NOT NULL) SELECT 1 FROM remote.provenance_origins o JOIN remote.row_proofs p ON p.id=o.proof_id JOIN ancestors a ON a.revision=p.revision WHERE o.kind=? AND o.physical_entity=? AND o.author_user_id=? LIMIT 1",(proof["previous_revision"],table,source,proof["author_user_id"],table,mapped("file_edits",source),proof["author_user_id"])).fetchone())
        if defer and (issue:=provenance_issue(db,value,mapped,replace_file)): return (defer(proof_id),typer.echo(f"Remote fact retained: workspace={proof['workspace']} author={proof['author_user_id']} proof={proof_id}; {issue}",err=True))[1]
        physical=mapped("file_edits",source) if table=="edit.observed" else provenance_digest({"checkpoint":data["checkpoint"],"edit":mapped("file_edits",data["edit"])}) if table=="checkpoint.link" else source
        (((((table not in ("repository.observed","edit.observed") or not ((table,physical) in local if local is not None else db.execute("SELECT 1 FROM provenance.local_facts WHERE kind=? AND entity=?",[table,physical]).fetchone())) and (project_provenance(db,value,mapped,False,replace_file,preserve))),((touch) and (_archive_touch(db,[(table,physical)]))))),((record_origin) and (db.execute("INSERT OR REPLACE INTO remote.provenance_origins VALUES (?,?,?,?,?,?)",(table,physical,proof["workspace"],proof["author_user_id"],source,proof_id)))))
        return physical
    if row["state"]=="deleted":
        old=db.execute("SELECT author_user_id,source_row_id FROM remote.row_origins WHERE table_name=? AND physical_row_id=?",(table,physical)).fetchone()
        (required(not old or origin and old==(origin["author_user_id"],origin["source_row_id"]),ValueError(f"archive ownership conflict: {table}:{physical}")),required(not origin or old or not db.execute(f"SELECT 1 FROM {table} WHERE id=?",(physical,)).fetchone(),ValueError(f"archive ownership conflict: {table}:{physical}")))
        with preserve_fact_heads(db,[(table,physical)]) if preserve else contextlib.nullcontext():
            project_edit_metadata(db,[(row,proof,proof_id,native,parent_map)])
            db.execute(f"DELETE FROM {table} WHERE id=?",(physical,))
            for related in {"attachments":("attachment_bodies WHERE attachment_id",),"file_edits":("provenance.file_edit_evidence WHERE file_edit_id",)}.get(table,()): db.execute(f"DELETE FROM {related}=?",(physical,))
            (origin and db.execute("INSERT OR REPLACE INTO remote.row_origins VALUES (?,?,?,?,?,?,?,?,?,?)",(table,physical,*(origin[k] for k in ("workspace_id","author_user_id","author_device_id","source_row_id","source_event_id","content_key","observed_at","proof_id")))),table in ("file_edits","tool_calls") and _apply_signed_edit_evidence(db,[physical] if table=="file_edits" else (),[physical] if table=="tool_calls" else ()),touch and _archive_touch(db,[(table,physical)]))
        return physical
    table,physical,values,origin,data=_logical_archive(row,proof,proof_id,native,parent_map)
    with preserve_fact_heads(db,[(table,physical)]) if preserve else contextlib.nullcontext(): (project_archive_row(db,table,ARCHIVE_COLUMNS[table],values,origin,touch,False),project_edit_metadata(db,[(row,proof,proof_id,native,parent_map)]),table=="attachments" and data["body_hash"] and db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(physical,data["body_hash"],data["size"])))
    return physical
def set_attachment_path(db,row_id,path,expected=None): db.execute("UPDATE attachments SET path=? WHERE id=? AND (? OR path IS NULL AND EXISTS(SELECT 1 FROM attachment_bodies b WHERE b.attachment_id=attachments.id AND (b.content_hash,b.size)=(?,?)))",(str(path),row_id,expected is None,*(expected or (None,None))))
def attachment_index(path,size=None): return (lambda path,actual:None if actual<0 or actual>ATTACHMENT_LIMIT or size is not None and actual!=size else (_file_sha256(path),actual))(path:=Path(path),path.stat().st_size if path.is_file() and not path.is_symlink() else -1)
def index_attachment_body(db,row_id,path,size=None,prepared=None):
    if not (indexed:=prepared or attachment_index(path,size)): return None
    body_hash,actual,old,signed=(*indexed,db.execute("SELECT content_hash,size FROM attachment_bodies WHERE attachment_id=?",(row_id,)).fetchone(),db.execute("SELECT 1 FROM remote.row_origins WHERE table_name='attachments' AND physical_row_id=?",(row_id,)).fetchone())
    if signed: return required(old==(body_hash,actual) and (size is None or size==actual),ValueError("signed attachment body conflicts with retained proof")) and body_hash
    with preserve_fact_heads(db,[("attachments",row_id)] if size is None or old!=(body_hash,actual) else (),observed=True): ((updated:=size is None and db.execute("UPDATE attachments SET size=? WHERE id=? AND size IS NULL RETURNING id",(actual,row_id)).fetchone()),old!=(body_hash,actual) and db.execute("INSERT OR REPLACE INTO attachment_bodies VALUES (?,?,?)",(row_id,body_hash,actual)),(updated or old!=(body_hash,actual)) and _archive_touch(db,[("attachments",row_id)]))
    return body_hash
def project_attachment_body(db_path,data,body_hash):
    path=(required(provenance_digest(data)==body_hash,ValueError("attachment body hash mismatch")),attachment_body(data,Path(db_path).parent))[-1]
    with _core(db_path,purpose="attachment.project") as db:
        rows=db.execute("SELECT b.attachment_id,a.path FROM attachment_bodies b JOIN attachments a ON a.id=b.attachment_id WHERE b.content_hash=?",(body_hash,)).fetchall()
        if rows and path and (ids:=[row_id for row_id,old in rows if old!=str(path)]):
            with _transaction(db): db.executemany("UPDATE attachments SET path=? WHERE id=?",[(str(path),row_id) for row_id in ids])
        return len(rows)
def _backup_copy(source,target): return command and not subprocess.run(command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode or shutil.copyfile(source,target) if (command:=("cp","-c",str(source),str(target)) if sys.platform=="darwin" else ("cp","--reflink=auto",str(source),str(target)) if sys.platform.startswith("linux") else None) else shutil.copyfile(source,target)
def _file_sha256(path):
    with Path(path).open("rb") as source: return hashlib.file_digest(source,"sha256").hexdigest()
def _check_archive(path):
    with contextlib.closing(duckdb.connect(str(path),read_only=True)) as db: return db.execute("SELECT COUNT(*) FROM conversations").fetchone()
def _backup_rows(conn): return (lambda tables:(lambda rows:rows+(conn.execute("SELECT concat('conflict:',c.proof_id),json_extract_string(c.body,'$.data.body_hash'),CAST(json_extract_string(c.body,'$.data.size') AS UINTEGER),NULL FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.row_kind='attachments' AND p.state='active' AND json_extract_string(c.body,'$.data.body_hash') IS NOT NULL").fetchall() if {"row_conflicts","row_proofs"}<=tables else []))(conn.execute("SELECT concat('attachment:',b.attachment_id),b.content_hash,b.size,a.path FROM attachment_bodies b JOIN attachments a ON a.id=b.attachment_id").fetchall() if "attachment_bodies" in tables else [("attachment:"+row_id,None,size,path) for row_id,path,size in conn.execute("SELECT id,path,size FROM attachments WHERE path IS NOT NULL").fetchall()] if "attachments" in tables else []))({r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()})
def _bundle_valid(target,database,database_hash,rows):
    try: return target.is_dir() and not target.is_symlink() and (manifest:=json.loads((target/"manifest.json").read_text())).get("version")==1 and manifest.get("database")==database and manifest.get("database_sha256")==database_hash and set(refs:=manifest["references"])=={r[0] for r in rows} and all(isinstance(refs[ref],list) and len(refs[ref])==2 and (body_hash is None or refs[ref][0]==body_hash) and (size is None or refs[ref][1]==size) for ref,body_hash,size,path in rows) and manifest["attachments"]=={v[0]:v[1] for v in refs.values()} and all((target/h).is_file() and not (target/h).is_symlink() and (target/h).stat().st_size==size and _file_sha256(target/h)==h for h,size in manifest["attachments"].items())
    except (OSError,ValueError,KeyError,TypeError,AttributeError,json.JSONDecodeError): return False
def _backup_attachments(conn,source,backup,database_hash):
    target,rows=backup.with_name(f"{backup.name}.attachments"),_backup_rows(conn)
    if target.exists() and _bundle_valid(target,backup.name,database_hash,rows): return target
    bodies,refs,paths={},{},{h:[Path(p) for _,p in group] for h,group in itertools.groupby(sorted({(h,p) for _,h,_,p in rows if h and p}),key=lambda r:r[0])}
    for ref,body_hash,size,path in rows:
        candidates,(body)=(candidates:=paths.get(body_hash,[])+([Path(path)] if path and not body_hash else [])+([source.parent/"attachments"/body_hash] if body_hash else [])),(next((p for p in candidates if p.is_file() and not p.is_symlink() and (size is None or p.stat().st_size==size) and (not body_hash or _file_sha256(p)==body_hash)),None))
        required(body,ValueError(f"attachment body unavailable for backup: reference={ref} hash={body_hash} size={size} candidates={[str(p) for p in candidates]}"))
        body_hash,size=body_hash or _file_sha256(body),body.stat().st_size
        bodies[body_hash]=(required(body_hash not in bodies or bodies[body_hash][0]==size,ValueError("attachment body metadata conflict")) and size,body)
        refs[ref]=[body_hash,size]
    stage=required(not target.exists(),ValueError("attachment backup is incomplete")) and Path(tempfile.mkdtemp(prefix=f".{target.name}.",dir=target.parent))
    try:
        ([(_backup_copy(body,stage/body_hash),os.chmod(stage/body_hash,0o600),required((stage/body_hash).stat().st_size==size and _file_sha256(stage/body_hash)==body_hash,ValueError("attachment backup verification failed")),_fsync(stage/body_hash)) for body_hash,(size,body) in bodies.items()],atomic_json(stage/"manifest.json",dict(version=1,database=backup.name,database_sha256=database_hash,references=refs,attachments={h:s for h,(s,_) in bodies.items()})),(_fsync(stage),os.replace(stage,target),_fsync(target.parent)))
    except BaseException as e: raise (shutil.rmtree(stage,ignore_errors=True) or e)
    return target
def _migration_backup(conn,version=1):
    tables,current=(tables:={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}),(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    if "conversations" not in tables or isinstance(version,int) and current>=version: return None
    path,label,backup=(path:=Path(next((r[2] for r in conn.execute("PRAGMA database_list").fetchall() if r[2]),""))),(label:=f"v{version}" if isinstance(version,int) else version),path.with_name(f"{path.name}.pre-{label}.bak") if path.name else None
    required(isinstance(label,str) and label and all(c.isalnum() or c in "-_" for c in label),ValueError("invalid backup label"))
    if not backup: return None
    source=(conn.execute("CHECKPOINT"),_file_sha256(path))[1]
    if backup.exists() and (backup.is_symlink() or not backup.is_file()): raise ValueError("core migration backup path is unsafe")
    if backup.exists() and source==_file_sha256(backup) and _bundle_valid(backup.with_name(backup.name+".attachments"),backup.name,source,_backup_rows(conn)): return backup
    if not backup.exists() and _bundle_valid(backup.with_name(backup.name+".attachments"),backup.name,source,_backup_rows(conn)): return (atomic_publish(backup,lambda tmp:(_backup_copy(path,tmp),required(_file_sha256(tmp)==source,ValueError("archive backup verification failed")),_check_archive(tmp))),backup)[1]
    if backup.exists() or backup.with_name(backup.name+".attachments").exists(): backup=backup.with_name(f"{backup.name}.{source[:12]}")
    backup=backup.with_name(f"{backup.name}.{time.time_ns()}") if backup.exists() or backup.with_name(backup.name+".attachments").exists() else backup
    (_backup_attachments(conn,path,backup,source),atomic_publish(backup,lambda tmp:(_backup_copy(path,tmp),required(_file_sha256(tmp)==source,ValueError("archive backup verification failed")),_check_archive(tmp))))
    return backup
def migrate_provider_ids(db):
    sessions={(src,provider_session_key(src,session)):cid for src,session,cid in db.execute('SELECT source,session_id,conversation_id FROM provider_sessions').fetchall() if src in ('codex','claude-code')}
    if not sessions: return
    existing={table:{r[0] for r in db.execute(f'SELECT id FROM {table}').fetchall()} for table in ARCHIVE_COLUMNS}
    mapping,bindings,patches,edit_counts={table:{} for table in ARCHIVE_COLUMNS},{},{},dict(db.execute('SELECT tool_call_id,count(*) FROM provenance.file_edit_evidence GROUP BY tool_call_id').fetchall())
    def assign(src,table,old,new,provider=False):
        if old not in existing[table]: return
        target,mapping[table][old]=(target:=new if provider else old),required(old not in mapping[table] or mapping[table][old]==target,ValueError('Conflicting provider identity observations')) and target
        if target!=new: bindings[(src,new)]=target
    roots=[(src,hook_root(src)) for src in ('codex','claude-code')]+[('codex',hook_root('codex').parent/'archived_sessions')]
    for src,root in roots:
        for path in sorted(root.rglob('*.jsonl')) if root.is_dir() else []:
            events,sid=(events:=list(iter_jsonl(path))),next((e['payload'].get('id') for i,e in events if e.get('type')=='session_meta'),None) if src=='codex' else next((e.get('agentId') or e.get('sessionId') or e.get('session_id') for i,e in events if e.get('agentId') or e.get('sessionId') or e.get('session_id')),None)
            if src=='claude-code': sid=next((e['agentId'] for i,e in events if e.get('agentId')),None) or (path.stem if 'subagents' in path.parts else sid)
            if not sid or (src,sid) not in sessions: continue
            oldcid,newcid,_=(oldcid:=sessions[(src,sid)]),(newcid:=gen_id(src,sid)),assign(src,'conversations',oldcid,newcid,True)
            stored={int(index):mid for mid,index in db.execute("SELECT id,json_extract_string(metadata,'$.provider_index') FROM messages WHERE conversation_id=? AND TRY_CAST(json_extract_string(metadata,'$.provider_index') AS BIGINT)>=0",[oldcid]).fetchall()}
            messages=[(i,e if src=='claude-code' else e['payload']) for i,e in events if 'message' in e] if src=='claude-code' else [(i,e['payload']) for i,e in events if e.get('type')=='response_item' and e.get('payload',{}).get('type')=='message']
            coordinates={i:ordinal for ordinal,(i,e) in enumerate(events)}
            provided={uid:mid for mid,uid in db.execute("SELECT id,json_extract_string(metadata,'$.provider_id') FROM messages WHERE conversation_id=? AND json_extract_string(metadata,'$.provider_id') IS NOT NULL",[oldcid]).fetchall()}
            for ordinal,(i,e) in enumerate(messages):
                uid,old=(uid:=e.get('uuid') if src=='claude-code' else e.get('id')),provided.get(uid,stored.get(coordinates[i] if src=='claude-code' else i,gen_id(src,f'{oldcid}:{ordinal if src=="claude-code" else i}')))
                assign(src,'messages',old,gen_id(src,f'message:{newcid}:{uid}') if uid else gen_id(src,f'{newcid}:{i}'),bool(uid))
                if old in existing['messages']: patches[old]={'provider_index':i,**({'provider_id':uid} if uid else {})}
                if src=='claude-code':
                    for j,t in enumerate(extract_content(e['message'].get('content',[]))['tools']):
                        if 'name' not in t: continue
                        call=t.get('id')
                        assign(src,'tool_calls',gen_id(src,f'tool:{oldcid}:{call or f"{ordinal}:{j}"}'),gen_id(src,f'tool:{newcid}:{call or f"{i}:{j}"}'),bool(call))
                        oldtool,newtool=(oldtool:=gen_id(src,f'tool:{oldcid}:{call or f"{ordinal}:{j}"}')),mapping['tool_calls'].get(oldtool,gen_id(src,f'tool:{newcid}:{call or f"{i}:{j}"}'))
                        for oldedit in {gen_id(src,f'edit:{oldcid}:{ordinal}:{j}'),gen_id(src,f'edit:{oldtool}:0')}: assign(src,'file_edits',oldedit,gen_id(src,f'edit:{newtool}:0'))
                else:
                    for j,b in enumerate(b for b in e.get('content',[]) if isinstance(b,dict) and b.get('type')=='input_image'):
                        candidates=[r[0] for r in db.execute('SELECT id FROM attachments WHERE message_id=? AND starts_with(filename,?)',[old,f'image-{j+1}.']).fetchall()]
                        assign(src,'attachments',candidates[0] if len(candidates)==1 else gen_id(src,f'attach:{oldcid}:{i}:{j}'),gen_id(src,f'attach:{newcid}:{i}:{j}'))
            if src=='codex':
                for i,e in events:
                    if e.get('type')!='response_item' or (v:=e.get('payload',{})).get('type') not in ('function_call','custom_tool_call'): continue
                    call,custom=v.get('call_id'),v['type']=='custom_tool_call'
                    new=gen_id(src,f'tool:{newcid}:{call or i}') if call or not custom else gen_id(src,f'custom:{newcid}:{i}')
                    for old in {gen_id(src,f'tool:{oldcid}:{call or i}'),gen_id(src,f'custom:{oldcid}:{i}')} if custom else {gen_id(src,f'tool:{oldcid}:{call or i}')}: assign(src,'tool_calls',old,new,bool(call))
                    count=sum(edit_counts.get(t,0) for t in {gen_id(src,f'tool:{oldcid}:{call or i}'),gen_id(src,f'custom:{oldcid}:{i}')})
                    for j in range(count):
                        old=gen_id(src,f'edit:{oldcid}:{i}:{j}')
                        if old in existing['file_edits']: assign(src,'file_edits',old,gen_id(src,f'edit:{new if call else mapping["tool_calls"].get(gen_id(src,f"custom:{oldcid}:{i}") if custom else gen_id(src,f"tool:{oldcid}:{i}"),new)}:{j}'))
    if patches: db.execute("UPDATE messages m SET metadata=json_merge_patch(COALESCE(m.metadata,'{}'),j.value) FROM json_each(?) j WHERE m.id=j.key",[json.dumps(patches)])
    for table,ids in mapping.items():
        required(len(set(ids.values()))==len(ids) and not any(new in existing[table] and new!=old and new not in ids for old,new in ids.items()),ValueError(f'Provider identity collision in {table}; archive unchanged'))
        changed=[(old,new) for old,new in ids.items() if old!=new]
        if not changed: continue
        db.execute('CREATE OR REPLACE TEMP TABLE identity_map(old VARCHAR,new VARCHAR)')
        db.executemany('INSERT INTO identity_map VALUES (?,?)',changed)
        refs=[(t,column) for t,columns in ARCHIVE_FKS.items() for column,parent in columns if parent==table]+({'conversations':[('provider_sessions','conversation_id'),('provenance.conversation_scopes','conversation')],'tool_calls':[('provenance.file_edit_evidence','tool_call_id')],'attachments':[('attachment_bodies','attachment_id')]}.get(table,[]))
        for target,column in [(table,'id'),*refs]: db.execute(f'UPDATE {target} x SET {column}=m.new FROM identity_map m WHERE x.{column}=m.old')
        if table=='messages': db.execute("UPDATE messages x SET metadata=json_merge_patch(x.metadata,json_object('history_of',m.new)) FROM identity_map m WHERE json_extract_string(x.metadata,'$.history_of')=m.old")
    if bindings: db.executemany('INSERT OR REPLACE INTO parser_id_bindings VALUES (?,?,?)',[(src,new,old) for (src,new),old in bindings.items()])
    _archive_touch(db,[(table,new) for table,ids in mapping.items() for old,new in ids.items()])

def reset_archive_sync(path,user,device):
    if not Path(path).is_file(): return None
    with contextlib.closing(get_db(path=path,purpose='sync.cutover')) as db:
        init_schema(db,cutover=False)
        return reset_archive_sync_db(db,user,device)
def reset_archive_sync_db(db,user,device):
    if prior:=db.execute('SELECT version,user_id,device_id FROM archive_sync WHERE singleton').fetchone():
        required(prior==(2,user,device),ValueError('Archive sync identity or format does not match this device'))
        return None
    db.execute("CREATE OR REPLACE TEMP TABLE sync_received AS SELECT table_name,physical_row_id FROM remote.row_origins UNION SELECT 'file_edits',physical_id FROM remote.derived_edits")
    db.execute("CREATE OR REPLACE TEMP TABLE sync_local AS SELECT 'conversations' kind,c.id FROM conversations c JOIN provider_sessions s ON s.conversation_id=c.id WHERE NOT EXISTS(SELECT 1 FROM sync_received r WHERE r.table_name='conversations' AND r.physical_row_id=c.id) UNION SELECT 'messages',m.id FROM messages m JOIN provider_sessions s ON s.conversation_id=m.conversation_id UNION SELECT 'tool_calls',t.id FROM tool_calls t JOIN messages m ON m.id=t.message_id JOIN provider_sessions s ON s.conversation_id=m.conversation_id UNION SELECT 'file_edits',e.id FROM file_edits e JOIN messages m ON m.id=e.message_id JOIN provider_sessions s ON s.conversation_id=m.conversation_id")
    peers=db.execute("SELECT p.row_kind,p.source_row_id,list(DISTINCT p.content_hash) FROM remote.row_proofs p WHERE p.author_user_id=? AND p.author_device_id<>? AND p.row_kind IN (SELECT UNNEST(?)) AND NOT EXISTS(SELECT 1 FROM sync_local l WHERE (l.kind,l.id)=(p.row_kind,p.source_row_id)) AND NOT EXISTS(SELECT 1 FROM remote.row_proofs own WHERE (own.row_kind,own.source_row_id,own.author_user_id)=(p.row_kind,p.source_row_id,p.author_user_id) AND own.author_device_id=?) GROUP BY p.row_kind,p.source_row_id",[user,device,list(ARCHIVE_COLUMNS),device]).fetchall()
    for at in range(0,len(peers),500):
        claims=[(kind,source,source,user,'active') for kind,source,hashes in peers[at:at+500]]
        bodies=typed_logical_rows(db,claims)
        if received:=[(kind,source) for (kind,source,hashes),claim in zip(peers[at:at+500],claims) if any(matching_logical_row(bodies[claim],h) is not None for h in hashes)]: db.executemany('INSERT INTO sync_received VALUES (?,?)',received)
    db.execute('CREATE OR REPLACE TEMP TABLE sync_received AS SELECT DISTINCT * FROM sync_received')
    blocked=[f'{table}.{column}' for table,refs in ARCHIVE_FKS.items() for column,parent in refs if db.execute(f"SELECT 1 FROM {table} c JOIN sync_received p ON p.table_name=? AND p.physical_row_id=c.{column} WHERE NOT EXISTS(SELECT 1 FROM sync_received o WHERE o.table_name=? AND o.physical_row_id=c.id) LIMIT 1",[parent,table]).fetchone()]
    required(not blocked,ValueError(f'Cannot reset sync: owned rows reference received parents: {blocked}; archive unchanged'))
    backup=_migration_backup(db,'sync-v2')
    with _transaction(db):
        removed=sum(db.execute(f"SELECT count(*) FROM {table} t JOIN sync_received o ON o.table_name=? AND o.physical_row_id=t.id",[table]).fetchone()[0] for table in ARCHIVE_COLUMNS)
        changed=db.execute('SELECT table_name,physical_row_id FROM sync_received').fetchall()
        for table in reversed(ARCHIVE_COLUMNS): db.execute(f"DELETE FROM {table} t USING sync_received o WHERE o.table_name=? AND o.physical_row_id=t.id",[table])
        for table,column,parent in [('attachment_bodies','attachment_id','attachments'),('provider_sessions','conversation_id','conversations'),('provenance.conversation_scopes','conversation','conversations'),*[(f'provenance.{table}','file_edit_id','file_edits') for table in ('file_edit_scopes','file_edit_files','file_edit_evidence','checkpoint_edits')]]: db.execute(f'DELETE FROM {table} x WHERE NOT EXISTS(SELECT 1 FROM {parent} p WHERE p.id=x.{column})')
        for table,kind,retain in [('git_checkpoints','git.checkpoint','EXISTS(SELECT 1 FROM provenance.checkpoint_edits e WHERE e.checkpoint_id=x.id)'),('file_versions','file.version','FALSE'),('files','file.observed','EXISTS(SELECT 1 FROM provenance.file_edit_files e WHERE e.file_id=x.id) OR EXISTS(SELECT 1 FROM provenance.file_versions v WHERE v.file_id=x.id)'),('repositories','repository.observed','EXISTS(SELECT 1 FROM provenance.conversation_scopes c WHERE c.repository=x.id) OR EXISTS(SELECT 1 FROM provenance.file_edit_scopes e WHERE e.repository=x.id) OR EXISTS(SELECT 1 FROM provenance.files f WHERE f.repository=x.id) OR EXISTS(SELECT 1 FROM provenance.git_checkpoints g WHERE g.repository=x.id) OR EXISTS(SELECT 1 FROM provenance.repository_checkouts c WHERE c.repository=x.id)')]: db.execute(f'DELETE FROM provenance.{table} x WHERE EXISTS(SELECT 1 FROM remote.provenance_origins o WHERE o.kind=? AND o.physical_entity=x.id) AND NOT EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind=? AND l.entity=x.id) AND NOT ({retain})',[kind,kind])
        db.execute("DELETE FROM provenance.repository_aliases a WHERE NOT EXISTS(SELECT 1 FROM provenance.repositories r WHERE r.id=a.repository); DELETE FROM provenance.pending p USING sync_received o WHERE (p.kind,p.entity)=(o.table_name,o.physical_row_id)")
        [db.execute(f'DELETE FROM remote.{table}') for table, in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='remote' AND table_type='BASE TABLE'").fetchall()]
        migrate_provider_ids(db)
        db.execute("INSERT INTO remote.edit_ready VALUES ('',''); INSERT INTO archive_sync VALUES (TRUE,2,?,?)",[user,device])
        required(not (issues:=archive_relationships(db)),ValueError(f'Archive relationship validation failed during sync reset: {issues}'))
        _archive_touch(db,changed)
    return dict(removed=removed,backup=str(backup) if backup else None)

def merge_archive_backup(path,backup,page=500):
    def restore(db,table,rows,columns=None): return (((bodies:=dict(rows)),(proofs:=db.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))",[list(bodies)]).fetchall()),required(len(proofs)==len(bodies),ValueError("Recovery bodies require stored proofs")),restore_signed_bodies(db,[dict(proof_id=p[0],proof_row=p,body=json.loads(bodies[p[0]])) for p in proofs]))[-1] if table=="remote.row_conflicts" and rows else _insert_pages(db,table,rows,columns,mode=" OR IGNORE"),project_edit_dependencies(db,advanced=[('', '')]))[0]
    path,backup=Path(path).resolve(),Path(backup).resolve()
    required(path!=backup and backup.is_file(),ValueError("Repair donor must be a separate existing archive backup"))
    with contextlib.closing(open_db(backup,True,purpose="remote.repair.donor.read")) as donor:
        identity=donor.execute("SELECT archive_id::VARCHAR FROM archive_state WHERE singleton").fetchone()[0]
        with contextlib.closing(open_db(path,purpose="remote.repair.prepare")) as db:
            (init_schema(db),required(db.execute("SELECT archive_id::VARCHAR FROM archive_state WHERE singleton").fetchone()[0]==identity,ValueError("Repair backup belongs to a different archive")))
            targets={f"{schema}.{table}":[r[0] for r in db.execute(f"DESCRIBE {schema}.{table}").fetchall()] for schema,table in db.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND (table_schema IN ('provenance','remote') OR table_name IN (SELECT UNNEST(?)))",[list(ARCHIVE_COLUMNS)+["attachment_bodies","provider_sessions","parser_retired_rows","parser_tool_lineage","parser_edit_lineage","parser_message_lineage"]]).fetchall()}
        available,(bundle)=(available:={f"{schema}.{table}" for schema,table in donor.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE'").fetchall()}),(backup.with_name(backup.name+".attachments"))
        for ref,body_hash,size,original in _backup_rows(donor):
            candidates=([bundle/body_hash,backup.parent/"attachments"/body_hash] if body_hash else [])+([Path(original)] if original else [])
            if found:=next((p for p in candidates if p.is_file() and not p.is_symlink() and (size is None or p.stat().st_size==size) and (not body_hash or _file_sha256(p)==body_hash)),None):
                data=found.read_bytes()
                project_attachment_body(path,data,body_hash or _file_sha256(found))
        restored=0
        for table,columns in sorted(targets.items(),key=lambda item:(item[0] in ("provenance.local_facts","remote.row_conflicts"),item[0])):
            if table not in available: continue
            fields=[r[0] for r in donor.execute(f"DESCRIBE {table}").fetchall()]
            if not set(columns)<=set(fields): continue
            with contextlib.closing(donor.cursor()) as reader:
                reader.execute(f"SELECT {','.join(columns)} FROM {table}")
                while rows:=reader.fetchmany(page):
                    with contextlib.closing(open_db(path,purpose="remote.repair.merge")) as db,_transaction(db):
                        kind,(missing)=(kind:=table.split(".")[1]),({r[0] for r in rows}-set(_rows_by_id(db,kind,[r[0] for r in rows])) if kind in ARCHIVE_COLUMNS else set())
                        if table=="provenance.local_facts":
                            claims,((original, current))=(claims:=[(kind,entity,entity,"","active") for kind,entity in rows]),(typed_logical_rows(donor,claims,"remote.row_references" in available),typed_logical_rows(db,claims,"remote.row_references" in targets))
                            rows=[row for row,claim in zip(rows,claims) if original[claim] is not None and current[claim]==original[claim]]
                        ((restore(db,table,rows,columns)),((missing) and (_archive_touch(db,[(kind,row_id) for row_id in missing]))))
                    restored+=len(rows)
                    archive_yield(path)
        if "remote.row_bodies" in available:
            reader=donor.execute("SELECT b.proof_id,b.body,p.content_hash FROM remote.row_bodies b JOIN remote.row_proofs p ON p.id=b.proof_id")
            while rows:=reader.fetchmany(page):
                exact=[(pid,body) for pid,body,expected in rows if provenance_digest(json.loads(body))==expected]
                with contextlib.closing(open_db(path,purpose="remote.repair.bodies")) as db,_transaction(db): restore(db,"remote.row_conflicts",exact)
                archive_yield(path)
        if {"remote.row_proofs","remote.row_origins","remote.provenance_origins"}<=available:
            with contextlib.closing(donor.cursor()) as reader:
                reader.execute("""WITH origins AS (SELECT table_name kind,physical_row_id physical,source_row_id source_id,author_user_id author,proof_id FROM remote.row_origins UNION SELECT kind,physical_entity,source_entity,author_user_id,proof_id FROM remote.provenance_origins) SELECT DISTINCT p.id,p.row_kind,COALESCE(o.physical,p.source_row_id) physical,p.source_row_id,p.author_user_id,p.state,p.content_hash FROM remote.row_proofs p LEFT JOIN origins o ON (o.kind,o.source_id,o.author)=(p.row_kind,p.source_row_id,p.author_user_id) WHERE p.id=o.proof_id OR NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE (c.row_kind,c.source_row_id,c.author_user_id,c.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) ORDER BY p.id,physical""")
                while proofs:=reader.fetchmany(page):
                    claims,((found, paths))=(claims:=[tuple(row[1:-1]) for row in proofs]),(typed_logical_rows(donor,claims,"remote.row_references" in available,historical=True),captured_edit_paths(donor,[physical for kind,physical,source,user,state in claims if kind=="file_edits"]))
                    if exact:=[(pid,expected,claim,row) for (pid,*_,expected),claim in zip(proofs,claims) if (row:=matching_logical_row(found[claim],expected,[paths[claim[1]]] if paths.get(claim[1]) else ())) is not None]:
                        with contextlib.closing(open_db(path,purpose="remote.repair.bodies")) as db,_transaction(db):
                            current,paths=typed_logical_rows(db,[claim for pid,expected,claim,row in exact],"remote.row_references" in targets),captured_edit_paths(db,[claim[1] for pid,expected,claim,row in exact if claim[0]=="file_edits"])
                            restore(db,"remote.row_conflicts",[(pid,json.dumps(row,sort_keys=True,separators=(",",":"))) for pid,expected,claim,row in exact if matching_logical_row(current[claim],expected,[paths[claim[1]]] if paths.get(claim[1]) else ()) is None])
                    archive_yield(path)
    return restored
def _repository_alias_migration(conn):
    _refresh_repository()
    ambiguous={rid for rid, in conn.execute("SELECT DISTINCT repository FROM provenance.repository_checkouts").fetchall() if len({repository_evidence(value) for root, in conn.execute("SELECT root FROM provenance.repository_checkouts WHERE repository=?",(rid,)).fetchall() if (git_root:=_git_root(root)) and (value:=_repository(str(git_root)))["lineage"]})>1}
    return [(rid,repository_evidence({"lineage":lineage,"remotes":json.loads(remotes)})) for rid,lineage,remotes in conn.execute("SELECT id,lineage,CAST(remotes AS VARCHAR) FROM provenance.repositories WHERE lineage IS NOT NULL").fetchall() if rid not in ambiguous],ambiguous
def session_bindings(conn):
    return {(source,session):cid for source,session,cid in conn.execute("SELECT source,session_id,conversation_id FROM provider_sessions").fetchall()}|{(source,'id',generated):assigned for source,generated,assigned in conn.execute('SELECT * FROM parser_id_bindings').fetchall()}|{(source,cid,'message',index):mid for source,cid,index,mid in conn.execute("SELECT c.source,c.id,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT),m.id FROM conversations c JOIN messages m ON m.conversation_id=c.id WHERE TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT)>=0 AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) QUALIFY count(*) OVER(PARTITION BY c.id,json_extract_string(m.metadata,'$.provider_index'))=1").fetchall()}|{(source,cid,'provider-message',uid):mid for source,cid,uid,mid in conn.execute("SELECT c.source,c.id,json_extract_string(m.metadata,'$.provider_id'),m.id FROM conversations c JOIN messages m ON m.conversation_id=c.id WHERE json_extract_string(m.metadata,'$.provider_id') IS NOT NULL").fetchall()}|{(source,cid,'idless',index):mid for source,cid,index,mid in conn.execute("SELECT c.source,c.id,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT),m.id FROM conversations c JOIN messages m ON m.conversation_id=c.id WHERE json_extract_string(m.metadata,'$.provider_id') IS NULL").fetchall()}|{(source,cid,'title'):title for source,cid,title in conn.execute('SELECT source,id,title FROM conversations WHERE title IS NOT NULL').fetchall()}

def _schema_migrate(conn,version,fn):
    with _transaction(conn) if (needed:=conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<version) else contextlib.nullcontext(): needed and fn()
def init_schema(conn,cutover=True):
    tables,current=(tables:={r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}),(conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0] if "core_schema" in tables else 0
    scope,_=remote_id_migration_scope(conn,remote_id) if required(current<=CORE_VERSION,ValueError(f"Archive schema {current} is newer than this Convos build ({CORE_VERSION}); upgrade Convos before writing.")) and current<2 else set(),_migration_backup(conn)
    current==1 and scope and ("core_migrations" not in tables or not conn.execute("SELECT 1 FROM core_migrations WHERE name='remote_ids'").fetchone()) and _migration_backup(conn,2)
    v3_backup=_migration_backup(conn,3) if current==2 else None
    if current==2 and (foreign:=[r[0] for r in conn.execute("SELECT x.file_edit_id FROM (SELECT file_edit_id FROM provenance.file_edit_files GROUP BY file_edit_id HAVING count(*)>1) x LEFT JOIN provenance.local_facts l ON (l.kind,l.entity)=('edit.observed',x.file_edit_id) WHERE l.entity IS NULL OR EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE (o.kind,o.physical_entity)=('edit.observed',x.file_edit_id))").fetchall()]): raise ValueError(f"v3 migration refused to rewrite ambiguous foreign signed edit {foreign[0]}; archive preserved and backed up at {v3_backup}")
    3<=current<CORE_VERSION and _migration_backup(conn,current+1)
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
    (((not conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone()) and (conn.execute("ALTER TABLE messages ADD COLUMN embedding FLOAT[]"))),(conn.execute("ALTER TABLE file_edits ADD COLUMN IF NOT EXISTS old_content TEXT; ALTER TABLE messages ADD COLUMN IF NOT EXISTS parent_id VARCHAR")))  # ALTER keeps fresh and migrated column order identical
    local_facts=bool(conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='provenance' AND table_name='local_facts'").fetchone())
    conn.execute(_PROVENANCE_SCHEMA)
    with _transaction(conn):
        cols={r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='provenance' AND table_name='file_edit_files'").fetchall()}
        (((renames:=[f"ALTER TABLE provenance.file_edit_files RENAME COLUMN {old} TO {new}" for old,new in (("before_hash","old_content_hash"),("after_hash","new_content_hash")) if old in cols and new not in cols]) and (conn.execute(";".join(renames)))),(conn.execute("ALTER TABLE provenance.conversation_scopes ADD COLUMN IF NOT EXISTS checkout VARCHAR; ALTER TABLE provenance.file_edit_scopes ADD COLUMN IF NOT EXISTS route VARCHAR; ALTER TABLE provenance.git_checkpoints ADD COLUMN IF NOT EXISTS capture_source VARCHAR; ALTER TABLE remote.row_origins ADD COLUMN IF NOT EXISTS proof_id VARCHAR; ALTER TABLE remote.row_proofs ADD COLUMN IF NOT EXISTS authorization_workspace_id VARCHAR; UPDATE remote.row_proofs SET authorization_workspace_id=workspace_id WHERE authorization_workspace_id IS NULL; DROP TABLE IF EXISTS provenance.assertions; DROP TABLE IF EXISTS provenance.capture_gaps; INSERT INTO core_schema SELECT TRUE,1 WHERE NOT EXISTS (SELECT 1 FROM core_schema WHERE singleton)")))
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
        (aliases,ambiguous),duplicates,unknown,legacy_edits=_repository_alias_migration(conn),(duplicates:=[r[0] for r in conn.execute("SELECT file_edit_id FROM provenance.file_edit_files GROUP BY file_edit_id HAVING COUNT(*)>1").fetchall()]),[(edit,provenance_digest({"repository":None,"path":(path:=f"external/{provenance_digest(edit)[:24]}/unknown")}),None,None,"legacy_scope_conflict",None,path,"external") for edit in duplicates],[(r[0],f"external/{provenance_digest(r[0])[:24]}/unknown",None,None,None,None,None) for r in conn.execute("SELECT id FROM file_edits fe WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=fe.id) AND NOT EXISTS(SELECT 1 FROM remote.derived_edits d WHERE d.physical_id=fe.id)").fetchall()]
        _schema_migrate(conn,3,lambda:(conn.execute("CREATE TABLE provenance.file_edit_files_v3(file_edit_id VARCHAR PRIMARY KEY,file_id VARCHAR,old_content_hash VARCHAR,new_content_hash VARCHAR,evidence VARCHAR); INSERT INTO provenance.file_edit_files_v3 SELECT * FROM provenance.file_edit_files QUALIFY count(*) OVER (PARTITION BY file_edit_id)=1"),unknown and conn.executemany("INSERT OR IGNORE INTO provenance.files VALUES (?,?,?,?)",[(fid,repo,path,kind) for edit,fid,old,new,evidence,repo,path,kind in unknown]),unknown and conn.executemany("INSERT INTO provenance.file_edit_files_v3 VALUES (?,?,?,?,?)",[(edit,fid,old,new,evidence) for edit,fid,old,new,evidence,repo,path,kind in unknown]),conn.execute("DROP TABLE provenance.file_edit_files; ALTER TABLE provenance.file_edit_files_v3 RENAME TO file_edit_files"),ambiguous and (conn.execute("DELETE FROM provenance.repository_checkouts WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)]),conn.execute("DELETE FROM provenance.repository_aliases WHERE repository IN (SELECT UNNEST(?))",[list(ambiguous)])),aliases and conn.executemany("INSERT OR IGNORE INTO provenance.repository_aliases VALUES (?,?)",aliases),legacy_edits and conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",legacy_edits),repair_legacy_edit_scopes(conn,True),conn.execute("INSERT OR IGNORE INTO provenance.conversation_scopes SELECT c.id,NULL,NULL,NULL,NULL,NULL FROM conversations c WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); INSERT OR REPLACE INTO core_schema VALUES (TRUE,3)")))
    if conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]<4:
        metadata,recoveries=(metadata:=conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='conversations' AND column_name='metadata'").fetchone()),(conn.execute("SELECT 'conversations',c.id FROM conversations c WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) UNION ALL SELECT 'messages',m.id FROM messages m WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)").fetchall() if metadata else [])
        _schema_migrate(conn,5,lambda:(conn.execute("CREATE TABLE IF NOT EXISTS provider_sessions(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); DELETE FROM provider_sessions"+("; UPDATE conversations c SET metadata=json_merge_patch(c.metadata,'{\"capture_mode\":\"history\"}') WHERE json_extract_string(c.metadata,'$.recovered')='history.jsonl' AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id); UPDATE messages m SET metadata=json_merge_patch(m.metadata,CASE json_extract_string(m.metadata,'$.recovered') WHEN 'history.jsonl' THEN '{\"capture_mode\":\"history\"}' ELSE '{\"capture_mode\":\"recovery\"}' END) WHERE json_extract_string(m.metadata,'$.recovered') IN ('history.jsonl','id-inversion') AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)" if metadata else "")),conn.execute((("INSERT INTO provider_sessions SELECT source,json_extract_string(metadata,'$.session_id'),min(id) FROM conversations c WHERE json_extract_string(metadata,'$.session_id') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) GROUP BY source,json_extract_string(metadata,'$.session_id');" if metadata else "")+"INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)")),recoveries and _archive_touch(conn,recoveries)))
    with _transaction(conn) if (binding_v5:=conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone()[0]==4) else contextlib.nullcontext(): binding_v5 and conn.execute("CREATE TABLE provider_sessions_v5(source VARCHAR,session_id VARCHAR,conversation_id VARCHAR,PRIMARY KEY(source,session_id)); INSERT INTO provider_sessions_v5 SELECT * FROM provider_sessions; DROP TABLE provider_sessions; ALTER TABLE provider_sessions_v5 RENAME TO provider_sessions; INSERT OR REPLACE INTO core_schema VALUES (TRUE,5)")
    (((_schema_migrate(conn,6,lambda:((legacy:=conn.execute("SELECT id FROM file_edits fe WHERE NOT EXISTS (SELECT 1 FROM provenance.file_edit_evidence v WHERE v.file_edit_id=fe.id)").fetchall()),legacy and conn.executemany("INSERT INTO provenance.file_edit_evidence VALUES (?,'unverified','source_unavailable',NULL)",legacy),legacy and _archive_touch(conn,[("file_edits",r[0]) for r in legacy]),conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,6)"))),_schema_migrate(conn,7,lambda:conn.execute("CREATE TABLE provenance.file_edit_evidence_v7(file_edit_id VARCHAR PRIMARY KEY,status VARCHAR NOT NULL CHECK(status IN ('confirmed','invalid','unknown','unverified')),reason VARCHAR NOT NULL,tool_call_id VARCHAR); INSERT INTO provenance.file_edit_evidence_v7 SELECT file_edit_id,CASE status WHEN 'legacy_unverified' THEN 'unverified' ELSE status END,reason,tool_call_id FROM provenance.file_edit_evidence; DROP TABLE provenance.file_edit_evidence; ALTER TABLE provenance.file_edit_evidence_v7 RENAME TO file_edit_evidence; INSERT OR REPLACE INTO core_schema VALUES (TRUE,7)")),_schema_migrate(conn,8,lambda:conn.execute("""ALTER TABLE messages ALTER embedding TYPE FLOAT[]; INSERT OR IGNORE INTO remote.semantic_ancestors SELECT kind,workspace_id,author_user_id,object_id,revision,json_extract_string(a.value,'$') FROM (SELECT 'provider.session' kind,workspace_id,author_user_id,object_id,revision,proof FROM remote.provider_session_aliases UNION ALL SELECT 'file-edit.evidence',workspace_id,author_user_id,object_id,revision,proof FROM remote.file_edit_evidence_proofs),json_each(proof,'$.ancestors') a; INSERT OR REPLACE INTO core_schema VALUES (TRUE,8); INSERT OR REPLACE INTO embedding_state SELECT TRUE,? WHERE EXISTS (SELECT 1 FROM messages WHERE embedding IS NOT NULL)""",(json.dumps(_EPROFILES["llama"],sort_keys=True,separators=(",",":")),))))),([_schema_migrate(conn,v,fn) for v,fn in ((9,lambda:(conn.execute("INSERT OR REPLACE INTO retrieval_state SELECT TRUE,generation,NULL,? FROM archive_state",(_FTS_DEF,)),conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,9)"))),(10,lambda:conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,10)")),(11,lambda:conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,11)")),(12,lambda:conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,12)")))]))
    with migration_memory(conn) if current<CORE_VERSION else contextlib.nullcontext(): (_schema_migrate(conn,13,lambda:(repair_parent_links(conn),conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,13)"))),_schema_migrate(conn,14,lambda:(conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,14)"))),_schema_migrate(conn,15,lambda:(restore_retired_proofs(conn),conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,15)"))),_schema_migrate(conn,16,lambda:(conn.execute("INSERT OR REPLACE INTO core_schema VALUES (TRUE,16)"))))
    with _transaction(conn): conn.execute("""CREATE OR REPLACE TEMP TABLE core_legacy_conflicts AS SELECT x.file_edit_id edit,x.file_id old_id,f.path,sha256(json_object('path',f.path,'repository',NULL)) new_id FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id WHERE x.evidence='legacy_scope_conflict' AND EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='edit.observed' AND l.entity=x.file_edit_id) AND NOT EXISTS(SELECT 1 FROM remote.provenance_origins o WHERE o.kind='edit.observed' AND o.physical_entity=x.file_edit_id) AND (x.file_id<>sha256(json_object('path',f.path,'repository',NULL)) OR NOT EXISTS(SELECT 1 FROM provenance.local_facts l WHERE l.kind='file.observed' AND l.entity=sha256(json_object('path',f.path,'repository',NULL)))); INSERT OR IGNORE INTO provenance.files SELECT new_id,NULL,path,'external' FROM core_legacy_conflicts; UPDATE provenance.file_edit_files x SET file_id=c.new_id FROM core_legacy_conflicts c WHERE x.file_edit_id=c.edit; INSERT OR IGNORE INTO provenance.local_facts SELECT 'file.observed',new_id FROM core_legacy_conflicts; DELETE FROM provenance.local_facts l USING core_legacy_conflicts c WHERE l.kind='file.observed' AND l.entity=c.old_id AND c.old_id<>c.new_id; DELETE FROM provenance.files f USING core_legacy_conflicts c WHERE f.id=c.old_id AND c.old_id<>c.new_id AND NOT EXISTS (SELECT 1 FROM provenance.file_edit_files x WHERE x.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM provenance.file_versions v WHERE v.file_id=f.id) AND NOT EXISTS (SELECT 1 FROM remote.provenance_origins o WHERE o.kind='file.observed' AND o.physical_entity=f.id); UPDATE archive_state SET generation=generation+1 WHERE singleton AND EXISTS(SELECT 1 FROM core_legacy_conflicts); INSERT OR REPLACE INTO archive_changes SELECT kind,entity,generation FROM archive_state,(SELECT 'file_edits' kind,edit entity FROM core_legacy_conflicts UNION ALL SELECT 'edit.observed',edit FROM core_legacy_conflicts UNION ALL SELECT 'file.observed',new_id FROM core_legacy_conflicts) WHERE singleton; DROP TABLE core_legacy_conflicts""")
    if cutover and (config:=Path(next((r[2] for r in conn.execute('PRAGMA database_list').fetchall() if r[2]),'.')).parent.parent/'remote/config.json').is_file() and not conn.execute('SELECT 1 FROM archive_sync WHERE singleton').fetchone():
        identity=required((identity:=json.loads(config.read_text())).get('sync_version',1) in (1,2),ValueError('Remote configuration is newer than this Convos build')) and reset_archive_sync_db(conn,identity['user'],identity['device']['id'])
    _schema_migrate(conn,17,lambda:(migrate_provider_ids(conn) if cutover and not conn.execute('SELECT 1 FROM archive_sync UNION ALL SELECT TRUE FROM remote.row_proofs LIMIT 1').fetchone() else None,conn.execute('INSERT OR REPLACE INTO core_schema VALUES (TRUE,17)')))

def counts_by_source(conn):
    queries,rows=(queries:=[("conversations","source"),("messages m JOIN conversations c ON c.id=m.conversation_id","c.source"),("tool_calls tc JOIN messages m ON tc.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source"),("attachments a JOIN messages m ON a.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source"),("file_edits fe JOIN messages m ON fe.message_id=m.id JOIN conversations c ON c.id=m.conversation_id","c.source")]),[(source,i,n) for i,(table,column) in enumerate(queries) for source,n in conn.execute(f"SELECT {column},COUNT(*) FROM {table} GROUP BY {column}").fetchall()]
    return {source:[next((n for s,j,n in rows if (s,j)==(source,i)),0) for i in range(5)] for source in {r[0] for r in rows}}

def load_fts(conn, allow_install: bool = False):
    try:
        (((allow_install) and (conn.execute("INSTALL fts"))),(conn.execute("LOAD fts")))
    except Exception as e: raise ValueError("FTS extension not available. Run `convos init` once with network access.") from e

def rebuild_fts_index(conn): conn.execute("PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1); UPDATE retrieval_state SET fts_generation=messages_generation,fts_definition_hash=? WHERE singleton",(_FTS_DEF,))

def ensure_db_ready(conn): return (lambda tables:True if "messages" in tables and "core_schema" in tables and (conn.execute("SELECT version FROM core_schema WHERE singleton").fetchone() or [0])[0]==CORE_VERSION and not conn.execute("SELECT 1 FROM core_migrations").fetchone() else typer.echo("Database initialization or migration required. Run `convos sync`.",err=True))({r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()})

def gen_id(source: str, oid: str) -> str: return hashlib.sha256(f"{source}:{oid}".encode()).hexdigest()[:16]
def ts_from_epoch(t):
    try: return datetime.fromtimestamp(float(t),timezone.utc).replace(tzinfo=None) if t is not None and t!="" else None
    except Exception: return None
def ts_from_iso(t): return (lambda v:v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v)(datetime.fromisoformat(t.replace("Z", "+00:00"))) if t else None
def ts_any(t): return ts_from_epoch(t) or (ts_from_iso(t) if isinstance(t, str) else None)  # chatgpt list api sends iso, exports send epoch

def extract_content(content) -> dict:
    if not isinstance(content,list): return {"text":content if isinstance(content,str) else "","thinking":None,"tools":[],"attachments":[]}
    blocks = [b for b in content if isinstance(b, dict)]
    return {"text":"\n".join(b.get("text","") or b.get("thinking","") if b.get("type") in ("text",None) else "" for b in blocks).strip() or "\n".join(str(b) for b in content if isinstance(b,str)).strip(),"thinking":"\n".join(b["thinking"] for b in blocks if b.get("type")=="thinking" and b.get("thinking")).strip() or None,"tools":[{"name":b["name"],"input":b.get("input",{}),"id":b.get("id")} for b in blocks if b.get("type")=="tool_use"]+[{"id":b.get("tool_use_id"),"output":b.get("content","") ,"error":b.get("is_error",False)} for b in blocks if b.get("type")=="tool_result"],"attachments":[{"filename":b.get("name",b.get("file_name")),"mime_type":b.get("content_type",b.get("file_type")),"size":b.get("size",b.get("file_size")),"url":b.get("asset_pointer",b.get("url"))} for b in blocks if b.get("type") in ("image_asset_pointer","file") or b.get("content_type") in ("image_asset_pointer","file")]}

def _safari_records():
    if not (path:=next((p for p in (Path.home()/"Library/Containers/com.apple.Safari/Data/Library/Cookies/Cookies.binarycookies",Path.home()/"Library/Cookies/Cookies.binarycookies") if p.exists()),None)): return
    if (data:=path.read_bytes())[:4]!=b"cook": return
    sizes,pages=(sizes:=[struct.unpack(">I",data[8+i*4:12+i*4])[0] for i in range(struct.unpack(">I",data[4:8])[0])]),[data[start:start+size] for start,size in zip(itertools.accumulate([8+4*len(sizes),*sizes]),sizes)]
    yield from ((text(fields[0]),text(fields[1]),text(struct.unpack("<I",page[off+28:off+32])[0])) for page in pages if page[:4]==b"\0\0\1\0" for i in range(struct.unpack("<I",page[4:8])[0]) for off in [struct.unpack("<I",page[8+i*4:12+i*4])[0]] for fields,text in [(struct.unpack("<III",page[off+16:off+28]),lambda pos,page=page,off=off:page[off+pos:page.find(b"\0",off+pos)].decode(errors="ignore"))])
def read_safari_cookies(domain: str) -> dict[str,str]:
    target=domain.lstrip(".").lower()
    try: return {name:value for host,name,value in _safari_records() or () if target in (clean:=host.lstrip(".").lower()) or clean in target or clean.endswith(target) or target.endswith(clean)}
    except PermissionError as e: raise ValueError("Safari cookies are not readable. Grant Full Disk Access to your terminal or use -b chrome.") from e

def _chrome_path(profile=None): return (lambda root:next((path for path in (root/"Cookies",root/"Network/Cookies") if path.exists()),None))(Path.home()/"Library/Application Support/Google/Chrome"/(profile or os.environ.get("CONVOS_CHROME_PROFILE","Default")))

def read_chrome_cookies(domain: str, profile: str | None = None) -> dict[str, str]:
    if not (db_path:=_chrome_path(profile)): return {}
    if (result:=subprocess.run(["security","find-generic-password","-w","-a","Chrome","-s","Chrome Safe Storage"],capture_output=True,text=True,timeout=10)).returncode: return {}
    key=hashlib.pbkdf2_hmac('sha1',result.stdout.strip().encode(),b'saltysalt',1003,16)
    with contextlib.closing(sqlite3.connect(f"file:{db_path}?mode=ro&nolock=1",uri=True)) as db: return {name:(value[32:] if not value[:32].isascii() else value).decode(errors="ignore") for name,encrypted,host in db.execute("SELECT name,encrypted_value,host_key FROM cookies WHERE host_key LIKE ?",(f"%{domain}%",)) if encrypted[:3]==b"v10" for decryptor in [Cipher(algorithms.AES(key),modes.CBC(b" "*16)).decryptor()] for decrypted in [decryptor.update(encrypted[3:])+decryptor.finalize()] for value in [decrypted[:-decrypted[-1]]]}

def get_cookies(domain: str, browser: str = "safari", profile: str | None = None) -> dict[str, str]: return read_safari_cookies(domain) if browser == "safari" else read_chrome_cookies(domain, profile=profile)

def get_cookies_any(domains: list[str], browser: str = "safari", profile: str | None = None) -> dict[str, str]: return {name:value for domain in domains for name,value in get_cookies(domain,browser,profile=profile).items()}

def safari_cookie_domains(): return {host for host,_,_ in _safari_records() or ()}

def chrome_cookie_domains(profile: str | None = None):
    if not (path:=_chrome_path(profile)): return set()
    with contextlib.closing(sqlite3.connect(f"file:{path}?mode=ro&nolock=1",uri=True)) as db: return {r[0] for r in db.execute("SELECT DISTINCT host_key FROM cookies")}

def chrome_profiles() -> list[str]: return [p.name for p in base.iterdir() if p.is_dir() and ((p/"Cookies").exists() or (p/"Network/Cookies").exists())] if (base:=Path.home()/"Library/Application Support/Google/Chrome").exists() else []

def chatgpt_profiles(browser: str) -> list[str | None]: return [None] if browser!="chrome" else [prof] if (prof:=os.environ.get("CONVOS_CHROME_PROFILE")) else chrome_profiles() or [None]

def chatgpt_cookie_base(browser: str, hosts: list[tuple[str, list[str]]], profile: str | None): return required(next(((cookies,url) for url,domains in hosts if (cookies:=get_cookies_any(domains,browser,profile=profile))),None),ValueError(f"No ChatGPT cookies found in {browser}"+(f" profile {profile}" if profile else "")))
def claude_listing(browser):
    cookies,org=(cookies:=required(get_cookies("claude.ai",browser),ValueError(f"No Claude cookies found in {browser}"))),required(fetch_json("https://claude.ai/api/organizations",cookies,_CLAUDE_HEADERS),ValueError("Could not get Claude org ID"))[0]["uuid"]
    return cookies,org,fetch_json(f"https://claude.ai/api/organizations/{org}/chat_conversations",cookies,_CLAUDE_HEADERS)

def chatgpt_headers(cookies, base, ua, debug_profile: str | None = None):
    headers={"Origin":base,"Referer":f"{base}/","User-Agent":ua,"Accept":"application/json","Accept-Language":"en-US,en;q=0.9","Sec-Fetch-Site":"same-origin","Sec-Fetch-Mode":"cors","Sec-Fetch-Dest":"empty"}
    with contextlib.suppress(Exception): ((headers.update({**({"Authorization":f"Bearer {token}"} if (token:=(session:=fetch_json(f"{base}/api/auth/session", cookies, headers, timeout=10, retries=0, rate_limit_backoff=300)).get("accessToken")) else {}),**({"ChatGPT-Account-ID":aid} if (aid:=session.get("account",{}).get("id")) else {})})),((debug_profile) and (typer.echo(f"  chatgpt chrome profile={debug_profile} user={session.get('user', {}).get('email')}", flush=True))))
    return headers

def fetch_json(url: str, cookies: dict[str, str], headers: dict = None, timeout: int = 15, retries: int = 1, before_request=None, rate_limit_backoff=None) -> dict:
    parts,cookie_str,hdrs,req=(parts:=[s for k,v in cookies.items() for s in [f"{k}={v}"] if all(ord(c)<256 for c in s)]),(cookie_str:="; ".join(parts)),(hdrs:={"Cookie":cookie_str,"User-Agent":"Mozilla/5.0","Accept":"application/json",**(headers or {})}),urllib.request.Request(url,headers=hdrs)
    for i in range(retries+1):
        before_request and before_request()
        try:
            with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=timeout) as resp: return json.loads(resp.read())
        except Exception as e:
            if i == retries: raise
            code,retry,delay=(code:=getattr(e,"code",None)),(retry:=(getattr(e,"headers",None) or {}).get("Retry-After","")),max(rate_limit_backoff or 30*(i+1),int(retry)) if code==429 and str(retry).isdigit() else rate_limit_backoff or 30*(i+1) if code==429 else 1+i
            (((code == 429) and (typer.echo(f"  rate limited; retrying in {delay}s", err=True))),(time.sleep(delay)))

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
    attachment_indexes: dict = field(default_factory=dict)
    failed_inputs: list = field(default_factory=list)
    input_sessions: dict = field(default_factory=dict)

    def __iadd__(self,other): return ([getattr(self,name).extend(getattr(other,name)) for name in ("convs","msgs","tools","attachs","artifacts","edits","edit_evidence","failed_inputs")],self.input_sessions.update(other.input_sessions),self)[-1]
    def __add__(self,other): return ParseResult(**{name:[*getattr(self,name),*getattr(other,name)] for name in ("convs","msgs","tools","attachs","artifacts","edits","edit_evidence","failed_inputs")},input_sessions=self.input_sessions|other.input_sessions)

def log_parse_error(context: str, err: Exception): typer.echo(f"  parse error ({context}): {type(err).__name__}: {err}", err=True)
def _quarantine_stubs(r): return setattr(r,"convs",[{**c,"metadata":json.dumps({**json.loads(c["metadata"] or "{}"),"capture_mode":"startup-stub-candidate"})} if c["id"] in quarantined else c for c in r.convs]) if (quarantined:={c["id"] for c in r.convs if c["source"]=="codex" and (meta:=json.loads(c["metadata"] or "{}")).get("session_kind")=="main" and not meta.get("parent_session_id") and any(m["conversation_id"]==c["id"] and m["role"]=="user" for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="user" and not re.fullmatch(_INJECTED_RE,m["content"]) for m in r.msgs) and not any(m["conversation_id"]==c["id"] and m["role"]=="assistant" for m in r.msgs) and not any(x.get("conversation_id")==c["id"] or x.get("message_id") in {m["id"] for m in r.msgs if m["conversation_id"]==c["id"]} for x in [*r.tools,*r.attachs,*r.artifacts,*r.edits])}) else None
def prepare_result(r): return (_quarantine_stubs(r),setattr(r,"attachment_indexes",{a["id"]:attachment_index(a["path"],a.get("size")) for a in r.attachs if a.get("path")}),r.scopes is not None or setattr(r,"scopes",pending_scopes([(c["id"],c["cwd"]) for c in r.convs])),r.edit_scopes is not None or setattr(r,"edit_scopes",pending_edit_scopes(edit_scope_inputs(r))))[-1]

def safe_parse(context: str, fn, *args, **kwargs):
    try: return fn(*args, **kwargs)
    except Exception as e: return log_parse_error(context,e)

def parse_source(path: Path, source: str|None = None, bindings=None) -> ParseResult:
    parsers,src={"chatgpt":parse_chatgpt,"claude":parse_claude,"claude-code":parse_claude_code,"codex":parse_codex},source or detect_source(path)
    return (required(src in parsers,ValueError(f"Unknown source: {src}")),parsers[src](path,bindings=bindings) if src in ("claude-code","codex") else parsers[src](path))[1]

def chatgpt_mapping(cid: str, mapping: dict) -> tuple[list, list, list]:
    tree_path=lambda nid:(*tree_path(parent),(mapping[parent].get("children") or [x for x in mapping if mapping[x].get("parent")==parent]).index(nid)) if (parent:=mapping[nid].get("parent")) in mapping else (tuple(mapping).index(nid),)
    def pmid(nid):  # nearest ancestor that carries a message (roots are message-less)
        while (nid := mapping[nid].get("parent")) and not mapping[nid].get("message"): pass
        return gen_id("chatgpt", f"{cid}:{nid}") if nid else None
    nodes,(msgs)=(nodes:=[(provider_index,nid,msg,gen_id("chatgpt",f"{cid}:{nid}"),msg.get("author",{}).get("role","unknown"),(meta:=msg.get("metadata",{})),ts_any(msg.get("create_time")),meta.get("model_slug"),(parts:=(msg.get("content") or {}).get("parts",[])),"\n".join(dict.fromkeys(p.strip() for p in [*parts,(msg.get("content") or {}).get("text")] if isinstance(p,str) and p.strip()))) for provider_index,nid in enumerate(sorted(mapping,key=tree_path)) if (msg:=mapping[nid].get("message"))]),([dict(id=mid,conversation_id=cid,role=role,content=text,thinking=None,created_at=ts,model=model,metadata=json.dumps({**meta,"provider_index":provider_index}),parent_id=pmid(nid)) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes])
    tools,(attachs)=(tools:=[dict(id=gen_id("chatgpt",f"tool:{mid}"),message_id=mid,tool_name=meta.get("invoked_plugin",{}).get("namespace",role),input=json.dumps(meta.get("args",{})),output=json.dumps(msg.get("content",{})),status="complete",duration_ms=None,created_at=ts) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes if role=="tool" or meta.get("invoked_plugin")]),([dict(id=gen_id("chatgpt",f"attach:{mid}:{i}"),message_id=mid,filename=p.get("name",""),mime_type=p.get("content_type"),size=p.get("size"),path=None,url=p.get("asset_pointer"),created_at=ts) for provider_index,nid,msg,mid,role,meta,ts,model,parts,text in nodes for i,p in enumerate(parts) if isinstance(p,dict) and p.get("content_type") in ("image_asset_pointer","file")])
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
            return dict(conv=dict(id=cid,source="chatgpt",title=item.get("title"),created_at=ts_any(conv.get("create_time") or item.get("create_time")) or min(times,default=None),updated_at=ts_any(conv.get("update_time") or item.get("update_time")) or max(times,default=None),model=item.get("model"),cwd=None,git_branch=None,project_id=gizmo,metadata=json.dumps({"session_id":item["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"api","remote_update_time":conv.get("update_time") or item.get("update_time"),**({"remote_complete":current.get("metadata",{}).get("is_complete",current.get("author",{}).get("role")=="assistant" and current.get("status") not in ("in_progress","running"))} if current else {}),**({"gizmo_id":gizmo} if gizmo else {})})),msgs=msgs,tools=tools,attachs=attachs)
        listed, seen, tail, offset, fetched, total = [], set(), set(), 0, 0, None
        while True:
            data,((raw, reported, keys))=(data:=fetch_json(f"{base}/backend-api/conversations?offset={offset}&limit=100&order=updated", cookies, headers, timeout=20, retries=1, before_request=pace, rate_limit_backoff=300)),(data.get("items", []), data.get("total"), ",".join(data.keys()))
            if debug: print(f"  chatgpt page offset={offset} items={len(raw)} total={reported} keys={keys}", flush=True)
            if total is not None and reported is not None and reported < total: raise RuntimeError(f"unstable list total {total}->{reported}")
            total = reported if reported is not None else total
            if not raw:
                required(reported is None or offset>=reported,RuntimeError(f"incomplete list at {offset}/{reported}"))
                break
            ids = [it["id"] for it in raw]
            if offset and not tail.intersection(ids): raise RuntimeError(f"unstable list at offset {offset}")
            tail,items=set(ids[-20:]),[it for it in {it["id"]:it for it in raw}.values() if it["id"] not in seen]
            seen,cut,stop=seen|{it["id"] for it in items},len(items),bool(frontier and any((updated:=ts_any(it.get("update_time"))) and updated<frontier for it in items))
            if not frontier and boundary and (at := next((i for i, it in enumerate(items) if it["id"] == boundary), None)) is not None: items, cut, stop = items[:at+1]+[it for it in items[at+1:] if gen_id("chatgpt", it["id"]) not in known], len(items), True
            listed += items[:cut]
            if stop or reported is not None and offset+len(raw) >= reported: break
            offset += max(1, len(raw)-20)
        page = [it for it in listed if (cid := gen_id("chatgpt", it["id"])) not in known or known[cid] is None or (updated := ts_any(it.get("update_time"))) is None or updated.replace(tzinfo=timezone.utc).timestamp() > (frontier.replace(tzinfo=timezone.utc).timestamp() if frontier and cid in legacy else known[cid]+(5 if cid in legacy else 0))][:limit or len(listed)]
        if len(page) > CHATGPT_BURST: typer.echo(f"  chatgpt pacing bulk fetch ({CHATGPT_BURST} burst, then {int(CHATGPT_RATE*300)} requests/5m)")
        for at in range(0, len(page), 20):
            try: results=[parse_item_raw(item) for item in page[at:at+20]]
            except Exception as e: raise RuntimeError(f"detail fetch failed: {e}") from e
            ((sink or r.__iadd__)(ParseResult([x["conv"] for x in results],[m for x in results for m in x["msgs"]],[t for x in results for t in x["tools"]],[a for x in results for a in x["attachs"]])),typer.echo(f"  chatgpt details {(fetched:=fetched+len(results))}"))
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
    listed,(items)=(listed:=data if limit==0 else data[:limit]),([item for item in listed if not since or not (updated:=ts_from_iso(item.get("updated_at") or item.get("created_at"))) or updated>since])
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
        cid,gizmo,conv=(cid:=gen_id("chatgpt",c.get("id",""))),(gizmo:=c.get("gizmo_id")),dict(id=cid,source="chatgpt",title=c.get("title"),created_at=ts_any(c.get("create_time")),updated_at=ts_any(c.get("update_time")),model=c.get("default_model_slug"),cwd=None,git_branch=None,project_id=gizmo,metadata=json.dumps({"session_id":c.get("id",""),"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export",**({"gizmo_id":gizmo} if gizmo else {})}))
        return ParseResult(convs=[conv],msgs=(parsed:=chatgpt_mapping(cid,c.get("mapping",{})))[0],tools=parsed[1],attachs=parsed[2])
    return sum((parsed for idx,c in enumerate(data) if (parsed:=safe_parse(f"chatgpt export conv {c.get('id') if isinstance(c,dict) else idx}",parse_conv,c))),ParseResult())

def parse_claude(path: Path) -> ParseResult:
    data = json.loads(path.read_text())
    def parse_conv(c):
        cid,msgs_data,parsed,results=(cid:=gen_id("claude",c["uuid"] if "uuid" in c else c["id"])),(msgs_data:=c.get("chat_messages",[])),(parsed:=[(i,m,gen_id("claude",f"{cid}:{m['uuid'] if 'uuid' in m else m['id']}"),extract_content(m.get("text") or m.get("content",""))) for i,m in enumerate(msgs_data)]),{t["id"]:t for i,m,mid,ec in parsed for t in ec["tools"] if "output" in t and t.get("id")}
        return {"conv":dict(id=cid,source="claude",title=c.get("name") or c.get("title"),created_at=ts_from_iso(c.get("created_at")),updated_at=ts_from_iso(c.get("updated_at")),model=c.get("model"),cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"session_id":c["uuid"] if "uuid" in c else c["id"],"session_kind":"main","session_kind_evidence":"exact","capture_mode":"export"})),"msgs":[dict(id=mid,conversation_id=cid,role="user" if m.get("sender")=="human" else m.get("sender","unknown"),content=ec["text"],thinking=ec["thinking"],created_at=ts_from_iso(m.get("created_at")),model=m.get("model"),metadata=json.dumps({"provider_index":i}),parent_id=None) for i,m,mid,ec in parsed if ec["text"] or ec["tools"] or ec["attachments"] or m.get("attachments")],"tools":[dict(id=gen_id("claude",f"tool:{mid}:{t.get('id') or j}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((results.get(t.get("id")) or {}).get("output","")),status="failed" if (results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in results else "pending",duration_ms=None,created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,t in enumerate(ec["tools"]) if "name" in t],"attachs":[dict(id=gen_id("claude",f"attach:{mid}:{j}"),message_id=mid,filename=a.get("name",a.get("file_name")),mime_type=a.get("content_type",a.get("file_type")),size=a.get("size",a.get("file_size")),path=None,url=a.get("asset_pointer",a.get("url")),created_at=ts_from_iso(m.get("created_at"))) for i,m,mid,ec in parsed for j,a in enumerate([*ec["attachments"],*m.get("attachments",[])])]}
    return (lambda parsed:ParseResult(convs=[p["conv"] for p in parsed],msgs=[m for p in parsed for m in p["msgs"]],tools=[t for p in parsed for t in p["tools"]],attachs=[a for p in parsed for a in p["attachs"]]))([p for idx,c in enumerate(data) if (p:=safe_parse(f"claude export conv {c.get('uuid') if isinstance(c,dict) else idx}",parse_conv,c))])

def iter_jsonl(path: Path):
    for i, line in enumerate(path.open(), start=1):
        if line.strip():
            try: yield i-1,json.loads(line)
            except Exception as e: log_parse_error(f"jsonl {path} line {i}", e)

def parser_logical_row(kind,row): return logical_row(kind,list(row),[v.astimezone(timezone.utc).replace(tzinfo=None) if isinstance(v,datetime) and v.tzinfo else json.dumps(v) if k in ROW_JSON_V1 and isinstance(v,(dict,list)) else v for k,v in row.items()])

def provider_message_id(source,conversation,index,provider,bindings): return bindings.get((source,conversation,'provider-message',provider),bindings.get((source,conversation,'idless',index),gen_id(source,f'message:{conversation}:{provider}'))) if provider else bindings.get((source,conversation,'message',index),gen_id(source,f'{conversation}:{index}'))

def parse_claude_code_session(jsonl: Path, bindings=None) -> dict:
    indexed = list(iter_jsonl(jsonl))
    events = [e for _,e in indexed]
    if not events: return None
    system,root_session,explicit_agent,sidechain=next((e for e in events if e.get("type")=="system"),{}),next((e.get("sessionId") or e.get("session_id") for e in events if e.get("sessionId") or e.get("session_id")),jsonl.stem),next((e["agentId"] for e in events if e.get("agentId")),None),any(e.get("isSidechain") for e in events)
    agent_id,src,kind,parent,cid=(agent_id:=explicit_agent or (jsonl.stem if "subagents" in jsonl.parts else None)),"claude-code","subagent" if agent_id or sidechain else "main",root_session if agent_id and agent_id!=root_session else None,(cid:=(bindings or {}).get(("claude-code",agent_id or root_session),gen_id("claude-code",agent_id or root_session)))
    item_id=lambda recipe:(bindings or {}).get((src,'id',gen_id(src,recipe)),gen_id(src,recipe))
    event_ids={i:e['uuid'] for i,e in indexed if e.get('uuid')}
    message_id=lambda idx,i:provider_message_id(src,cid,i,event_ids.get(i),bindings or {})
    timestamps,msg_events,tool_results,uuid2id=[ts_from_iso(e["timestamp"]) for e in events if "timestamp" in e],(msg_events:=[(i,e) for i,e in indexed if "message" in e]),{t["id"]:t for e in events if "message" in e for t in extract_content(e["message"].get("content",[]))["tools"] if "output" in t and t.get("id")},{e["uuid"]:message_id(idx,i) for idx,(i,e) in enumerate(msg_events) if "uuid" in e}

    turns=[(idx,i,e,extract_content(e["message"].get("content",e["message"].get("text",""))),ts_from_iso(e.get("timestamp")),message_id(idx,i)) for idx,(i,e) in enumerate(msg_events)]
    msgs,(msgs)=(msgs:=[dict(id=mid,conversation_id=cid,role="user" if e["type"] in ("human","user") else e["type"],content=c["text"],thinking=c["thinking"],created_at=ts,model=e["message"].get("model") if e["type"]=="assistant" else None,metadata=json.dumps({"provider_index":i,**({"provider_id":e["uuid"]} if e.get("uuid") else {})}),parent_id=uuid2id.get(e.get("parentUuid"))) for idx,i,e,c,ts,mid in turns]),([{**m,"parent_id":m["parent_id"] if m["parent_id"] in mids else None} for m in msgs] if (mids:={x["id"] for x in msgs}) else [])  # retain every source message, including thinking-only turns
    if not msgs: return None
    edit_id=lambda idx,j,t:item_id(f"edit:{item_id(f'tool:{cid}:{t.get("id") or f"{idx}:{j}"}')}:{0}")
    tools,edit_calls,edits=(tools:=[dict(id=item_id(f"tool:{cid}:{t.get('id') or f'{i}:{j}'}"),message_id=mid,tool_name=t["name"],input=json.dumps(t.get("input",{})),output=json.dumps((tool_results.get(t.get("id")) or {}).get("output","")),status="failed" if (tool_results.get(t.get("id")) or {}).get("error") else "complete" if t.get("id") in tool_results else "pending",duration_ms=None,created_at=ts) for idx,i,e,c,ts,mid in turns for j,t in enumerate(c["tools"]) if "name" in t]),(edit_calls:=[(i,ts,mid,j,t,tool_results.get(t.get("id"))) for idx,i,e,c,ts,mid in turns for j,t in enumerate(c["tools"]) if t.get("name") in ("Write","Edit","MultiEdit") and t.get("input",{}).get("file_path")]),[dict(id=edit_id(idx,j,t),message_id=mid,file_path=t["input"]["file_path"],edit_type=t["name"].lower(),content=t["input"].get("content") or t["input"].get("new_string",""),created_at=ts,old_content=t["input"].get("old_string")) for idx,ts,mid,j,t,result in edit_calls if result is not None and not result.get("error")]
    evidence=[dict(file_edit_id=edit_id(idx,j,t),status=status,reason=reason,tool_call_id=item_id(f"tool:{cid}:{t.get('id') or f'{idx}:{j}'}")) for idx,ts,mid,j,t,result in edit_calls for status,reason in [(('unknown','result_missing') if result is None else ('invalid','provider_failure') if result.get('error') else ('confirmed','provider_success'))]]
    return {"conv":dict(id=cid,source=src,title=f"{jsonl.parent.name.replace('-Users-','~/').replace('-','/')} ({jsonl.stem[:8]})",created_at=timestamps[0] if timestamps else None,updated_at=timestamps[-1] if timestamps else None,model=next((m["model"] for m in msgs if m["model"] and m["model"]!="<synthetic>"),None),cwd=system.get("cwd") or next((e.get("cwd") for e in events if e.get("cwd")),None),git_branch=system.get("gitBranch") or next((e.get("gitBranch") for e in events if e.get("gitBranch")),None),project_id=None,metadata=json.dumps({k:v for k,v in {"session_id":agent_id or root_session,"parent_session_id":parent,"session_kind":kind,"session_kind_evidence":"exact" if explicit_agent or sidechain else "inferred","agent_id":agent_id,"agent_name":next((e.get("agentName") for e in events if e.get("agentName")),None),"agent_role":next((e.get("agentType") for e in events if e.get("agentType")),None),"agent_depth":next((e.get("agentDepth") for e in events if e.get("agentDepth") is not None),None),"originator":system.get("entrypoint"),"client_version":system.get("version"),"capture_mode":"transcript","timestamp_basis":"utc"}.items() if v is not None})),"msgs":msgs,"tools":tools,"attachs":[],"edits":edits,"edit_evidence":evidence}

def _parse_sessions(paths,parser,bindings):
    def one(path,bound):
        s=parser(path,bound)
        return [(bound.setdefault((s["conv"]["source"],m["session_id"]),s["conv"]["id"]),s)[1] if s and (m:=json.loads(s["conv"]["metadata"] or "{}")).get("session_id") else s]
    attempts,sessions=(attempts:=[(str(path),safe_parse(f"{parser.__name__.removeprefix('parse_').replace('_session','').replace('_','-')} session {path}",one,path,bound)) for bound in [{} if bindings is None else bindings] for path in sorted(paths)]),[s for _,result in attempts if result is not None for s in result if s]
    return ParseResult(convs=[s["conv"] for s in sessions],msgs=[m for s in sessions for m in s["msgs"]],tools=[t for s in sessions for t in s["tools"]],attachs=[a for s in sessions for a in s["attachs"]],edits=[e for s in sessions for e in s["edits"]],edit_evidence=[v for s in sessions for v in s["edit_evidence"]],failed_inputs=[path for path,result in attempts if result is None],input_sessions={path:next((json.loads(s["conv"]["metadata"]).get("session_id") for s in result if s),None) for path,result in attempts if result is not None})
def parse_claude_code(projects_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult: return _parse_sessions(files or projects_dir.rglob("*.jsonl"),parse_claude_code_session,bindings)

def parse_codex_session(jsonl: Path, bindings=None) -> dict | None:
    events,timestamps=[],{}
    for i,e in iter_jsonl(jsonl):
        if "timestamp" in e: timestamps[i]=ts_from_iso(e["timestamp"])
        if e.get("type") in ("session_meta","turn_context") or e.get("type")=="response_item" and isinstance(e.get("payload"),dict) and e["payload"].get("type") in ("message","function_call","function_call_output","custom_tool_call","custom_tool_call_output"): events.append((i,e))
    if not events: return None
    meta,contexts,subagent,spawn,provider_id,src,items,cid=(meta:=next((e["payload"] for _,e in events if e.get("type")=="session_meta"),{})),(contexts:=[(i,e["payload"].get("model")) for i,e in events if e.get("type")=="turn_context" and e.get("payload",{}).get("model")]),(subagent:=(meta.get("source") or {}).get("subagent") if isinstance(meta.get("source"),dict) else None),(spawn:=subagent.get("thread_spawn",{}) if isinstance(subagent,dict) else {}),(provider_id:=meta.get("id")),"codex",[(i,e["payload"]) for i,e in events if e.get("type")=="response_item"],((bindings or {}).get(("codex",provider_id)) if provider_id else None) or gen_id("codex",provider_id or jsonl.stem)
    item_id=lambda recipe:(bindings or {}).get((src,'id',gen_id(src,recipe)),gen_id(src,recipe))
    message_ids={i:p['id'] for i,p in items if p.get('type')=='message' and p.get('id')}
    message_id=lambda i:provider_message_id(src,cid,i,message_ids.get(i),bindings or {})
    extract_msg_text,model_at=lambda p:"\n".join(b["text"] for b in p.get("content",[]) if isinstance(b,dict) and b.get("type") in ("input_text","output_text","text") and b.get("text")),lambda i:next((m for j,m in reversed(contexts) if j<=i),None)
    def image(i,j,b):
        url,data,head,encoded=(url:=b["image_url"]),(data:=url.startswith("data:")),*(url.split(",",1) if data else ("",""))
        mime=head[5:-7] if head.startswith("data:image/") and head.endswith(";base64") else b.get("mime_type")
        required(not data or mime,ValueError("invalid Codex image data URL"))
        size,body,path,ext=len(encoded.rstrip("="))*3//4 if data else None,(body:=base64.b64decode(encoded,validate=True) if data and len(encoded)<=4*((ATTACHMENT_LIMIT+2)//3) else None),attachment_body(body) if body is not None else None,{"image/jpeg":"jpg"}.get(mime,mime.rsplit("/",1)[-1] if mime and re.fullmatch(r"image/[a-z0-9.+-]+",mime) else "bin")
        return dict(id=item_id(f"attach:{cid}:{i}:{j}"),message_id=message_id(i),filename=f"image-{j+1}.{ext}",mime_type=mime,size=len(body) if body is not None else size,path=str(path) if path else None,url=None if data else url,created_at=timestamps.get(i))
    def norm_args(p): return json.loads(a) if isinstance((a := p.get("arguments", {})), str) else a

    mitems = [(i, p, t) for i, p in items if p.get("type") == "message" and ((t := extract_msg_text(p)) or any(isinstance(b,dict) and b.get("type")=="input_image" for b in p.get("content",[])))]
    if not (msgs := [dict(id=message_id(i), conversation_id=cid, role=p["role"], content=t.strip(), thinking=None, created_at=timestamps.get(i), model=model_at(i), metadata=json.dumps({"provider_index":i,**({"provider_id":p["id"]} if p.get("id") else {})}), parent_id=None) for i, p, t in mitems]): return None
    anchor = lambda k: message_id(next((i for i, _, _ in reversed(mitems) if i <= k), mitems[0][0]))  # function_call items are not messages; attach to nearest preceding one

    function_out={p.get("call_id"):p.get("output","") for _,p in items if p.get("type")=="function_call_output"}
    def edit_result(call,outputs,outer=None):
        if call not in outputs: return "unknown","result_missing"
        raw=json.dumps(outputs[call]).lower().replace("\\","")
        if any(x in raw for x in ("timed out","timeout","script failed","verification failed","is_error\": true","<tool_use_error>","aborted","rejected","denied","cancelled","canceled")) or re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+-?[1-9]\d*",raw): return "invalid","provider_failure"
        if "process running with session id" in raw or "script running with cell id" in raw: return "unknown","nonterminal_result"
        if not str(outputs[call]).strip() or str(outputs[call]).strip().lower() in ("ok","{}") or re.search(r"(?:exit(?:ed with)? code|exit_code)[\"': ]+0\b|script completed|success\. updated",raw): return "confirmed","provider_success"
        return ("confirmed","provider_success") if outer=="completed" and not raw.strip('"') else ("unknown","inconclusive_result")
    failed,tools,custom_out=(failed:=lambda out:edit_result("result",{"result":out})[0]=="invalid"),(tools:=[dict(id=item_id(f"tool:{cid}:{p.get('call_id') or i}"),message_id=anchor(i),tool_name=p["name"],input=json.dumps(args),output=json.dumps(function_out.get(p.get("call_id"),"")),status="failed" if p.get("call_id") in function_out and failed(function_out[p.get("call_id")]) else "complete" if p.get("call_id") in function_out else "pending",duration_ms=None,created_at=timestamps.get(i)) for i,p in items if p.get("type")=="function_call" and (args:=norm_args(p)) is not None]),{p.get("call_id"):p.get("output", "") for _, p in items if p.get("type") == "custom_tool_call_output"}
    tools += [dict(id=item_id(f"tool:{cid}:{p['call_id']}") if p.get("call_id") else item_id(f"custom:{cid}:{i}"), message_id=anchor(i), tool_name=p["name"], input=json.dumps({"code":p.get("input", "")}), output=json.dumps(custom_out.get(p.get("call_id"), "")), status="failed" if any(x in json.dumps(custom_out.get(p.get("call_id"), "")).lower() for x in ("script failed","verification failed")) or re.search(r"exit code: [1-9]\d*",json.dumps(custom_out.get(p.get("call_id"), "")).lower()) else "complete" if p.get("call_id") in custom_out or p.get("status") == "completed" else p.get("status", "pending"), duration_ms=None, created_at=timestamps.get(i)) for i, p in items if p.get("type") == "custom_tool_call"]

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

    candidates,evidence,edits=(candidates:=[(i,p,item_id(f"tool:{cid}:{p.get('call_id') or i}"),edit_result(p.get("call_id"),function_out),patch_edits(args)) for i,p in items if p.get("type")=="function_call" and p.get("name") in ("exec_command","shell_command","shell") and (args:=norm_args(p))]+[(i,p,(item_id(f"tool:{cid}:{p['call_id']}") if p.get("call_id") else item_id(f"custom:{cid}:{i}")),edit_result(p.get("call_id"),custom_out,p.get("status")),custom_edits(p)) for i,p in items if p.get("type")=="custom_tool_call"]),[dict(file_edit_id=item_id(f"edit:{tool}:{j}"),status=status,reason=reason,tool_call_id=tool) for i,p,tool,(status,reason),rows in candidates for j,row in enumerate(rows)],[dict(id=item_id(f"edit:{tool}:{j}"),message_id=anchor(i),file_path=fp,edit_type=op,content=c,created_at=timestamps.get(i),old_content=o) for i,p,tool,(status,reason),rows in candidates if status=="confirmed" for j,(fp,op,c,o) in enumerate(rows)]

    return {"conv":dict(id=cid,source=src,title=(bindings or {}).get((src,cid,'title'),meta.get("cwd") or jsonl.stem),created_at=min(timestamps.values(),default=None),updated_at=max(timestamps.values(),default=None),model=next((m["model"] for m in msgs if m["role"]=="assistant" and m["model"]),None),cwd=meta.get("cwd"),git_branch=(meta.get("git") or {}).get("branch"),project_id=None,metadata=json.dumps({k:v for k,v in {"session_id":provider_id,"parent_session_id":spawn.get("parent_thread_id") or meta.get("parent_thread_id"),"session_kind":"subagent" if subagent is not None else "main","session_kind_evidence":"exact" if subagent is not None else "inferred","agent_name":spawn.get("agent_nickname") or meta.get("agent_nickname"),"agent_role":spawn.get("agent_role") or (subagent if isinstance(subagent,str) else meta.get("agent_role")),"agent_depth":spawn.get("depth"),"originator":meta.get("originator"),"client_version":meta.get("cli_version"),"capture_mode":"transcript","timestamp_basis":"utc","git_repository":(meta.get("git") or {}).get("repository_url"),"git_commit":(meta.get("git") or {}).get("commit_hash"),"forked_from_id":meta.get("forked_from_id"),"thread_source":meta.get("thread_source")}.items() if v is not None})),"msgs":msgs,"tools":tools,"attachs":[image(i,j,b) for i,p,t in mitems for j,b in enumerate(x for x in p.get("content",[]) if isinstance(x,dict) and x.get("type")=="input_image")],"edits":edits,"edit_evidence":evidence}

def parse_codex(codex_dir: Path, files: list[Path] | None = None, bindings=None) -> ParseResult: return _parse_sessions(files or sessions_dir.rglob("*.jsonl"),parse_codex_session,bindings) if (sessions_dir:=codex_dir/"sessions").exists() else ParseResult()

_CONV_UPS = "INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET source=excluded.source,title=excluded.title,created_at=CASE WHEN (excluded.source IN ('codex','claude-code') AND json_extract_string(excluded.metadata,'$.timestamp_basis')='utc' AND json_extract_string(conversations.metadata,'$.timestamp_basis') IS DISTINCT FROM 'utc') OR conversations.created_at IS NULL OR excluded.created_at < conversations.created_at THEN excluded.created_at ELSE conversations.created_at END,updated_at=CASE WHEN (excluded.source IN ('codex','claude-code') AND json_extract_string(excluded.metadata,'$.timestamp_basis')='utc' AND json_extract_string(conversations.metadata,'$.timestamp_basis') IS DISTINCT FROM 'utc') OR conversations.updated_at IS NULL OR excluded.updated_at > conversations.updated_at THEN excluded.updated_at ELSE conversations.updated_at END,model=COALESCE(excluded.model,conversations.model),cwd=COALESCE(excluded.cwd,conversations.cwd),git_branch=COALESCE(excluded.git_branch,conversations.git_branch),project_id=COALESCE(excluded.project_id,conversations.project_id),metadata=excluded.metadata"

def _id_conflict(rows,fields):
    seen={}
    return next((rid for r in rows if (value:=tuple(str(r[k]) for k in fields))!=seen.setdefault((rid:=r["id"]),value)),None)
def _validate_result(r): return required(not (conflict:=_id_conflict(r.convs,("source","cwd","git_branch","project_id","metadata")) or _id_conflict(r.msgs,("conversation_id","role","content","thinking","created_at","model","metadata","parent_id"))),ValueError(f"divergent provider session in import batch: {conflict}"))
def _parse_result_refs(conn,r):
    incoming={"conversations":{v["id"] for v in r.convs},"messages":{v["id"] for v in r.msgs},"tool_calls":{v["id"] for v in r.tools},"file_edits":{v["id"] for v in r.edits}}
    for table,needed in (("conversations",{v["conversation_id"] for v in r.msgs}|{v["conversation_id"] for v in r.artifacts}),("messages",{v["message_id"] for v in [*r.tools,*r.attachs,*r.edits]}|{v["parent_id"] for v in r.msgs if v["parent_id"]}),("file_edits",{v["file_edit_id"] for v in r.edit_evidence}),("tool_calls",{v["tool_call_id"] for v in r.edit_evidence if v["tool_call_id"]})):
        existing={v[0] for v in conn.execute(f"SELECT id FROM {table} WHERE id IN (SELECT UNNEST(?))",(list(missing),)).fetchall()} if (missing:=needed-incoming[table]) else set()
        required(not (unavailable:=missing-existing),ValueError(f"parse result reference unavailable: {table}:{next(iter(sorted(unavailable)), '')}"))
def _rows_by_id(conn,table,ids): return {row[0]:row for row in conn.execute(f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",[list(ids)]).fetchall()} if ids else {}
def _history_row(table,row,payload):
    historical=[gen_id("history",f"{table}:{row[0]}:{json.dumps(payload,default=str)}"),*row[1:]]
    if table=="messages": historical[7]=json.dumps({**json.loads(historical[7] or "{}"),"history_of":row[0],"superseded_at":datetime.now().isoformat()})
    return historical
def _prune_remote_rows(conn,r,conversations_only=False):
    candidates,owned=(candidates:={"conversations":{v["id"] for v in r.convs}|{v["conversation_id"] for v in [*r.msgs,*r.artifacts]},"messages":{v["id"] for v in r.msgs}|{v["parent_id"] for v in r.msgs if v["parent_id"]}|{v["message_id"] for v in [*r.tools,*r.attachs,*r.edits]},"tool_calls":{v["id"] for v in r.tools}|{v["tool_call_id"] for v in r.edit_evidence if v["tool_call_id"]},"attachments":{v["id"] for v in r.attachs},"artifacts":{v["id"] for v in r.artifacts},"file_edits":{v["id"] for v in r.edits}|{v["file_edit_id"] for v in r.edit_evidence}}),{table:{v[0] for v in conn.execute("SELECT UNNEST([physical_row_id,source_row_id]) FROM remote.row_origins WHERE table_name=? AND (physical_row_id IN (SELECT UNNEST(?)) OR source_row_id IN (SELECT UNNEST(?))) UNION SELECT o.source FROM remote.row_owners o,archive_sync s WHERE o.kind=? AND o.author=s.user_id AND o.device<>s.device_id AND o.source IN (SELECT UNNEST(?))",(table,list(ids),list(ids),table,list(ids))).fetchall()} if ids and (not conversations_only or table=="conversations") else set() for table,ids in candidates.items()}
    convs,msgs=owned["conversations"],owned["messages"]|{v["id"] for v in r.msgs if v["conversation_id"] in owned["conversations"]}
    while extra:={v["id"] for v in r.msgs if v["parent_id"] in msgs}-msgs: msgs|=extra
    tools,attachs,artifacts,edits=owned["tool_calls"]|{v["id"] for v in r.tools if v["message_id"] in msgs},owned["attachments"]|{v["id"] for v in r.attachs if v["message_id"] in msgs},owned["artifacts"]|{v["id"] for v in r.artifacts if v["conversation_id"] in convs},owned["file_edits"]|{v["id"] for v in r.edits if v["message_id"] in msgs}
    before,((r.convs, r.msgs, r.tools, r.attachs, r.artifacts, r.edits))=(before:=sum(map(len,(r.convs,r.msgs,r.tools,r.attachs,r.artifacts,r.edits,r.edit_evidence)))),([v for v in r.convs if v["id"] not in convs],[v for v in r.msgs if v["id"] not in msgs],[v for v in r.tools if v["id"] not in tools],[v for v in r.attachs if v["id"] not in attachs],[v for v in r.artifacts if v["id"] not in artifacts],[v for v in r.edits if v["id"] not in edits])
    r.edit_evidence=[v for v in r.edit_evidence if v["file_edit_id"] not in edits and v["tool_call_id"] not in tools]
    if skipped:=before-sum(map(len,(r.convs,r.msgs,r.tools,r.attachs,r.artifacts,r.edits,r.edit_evidence))): typer.echo(f"  skipped {skipped} local row(s) owned by signed Remote data",err=True)
def upsert(conn, r: ParseResult):
    (_validate_result(r),_prune_remote_rows(conn,r))
    if uncertain:={v["file_edit_id"] for v in r.edit_evidence if v["status"]!="confirmed"}: r.edit_evidence=[v for v in r.edit_evidence if v["status"]=="confirmed" or v["file_edit_id"] in {e["id"] for e in r.edits}|{x[0] for x in conn.execute("SELECT id FROM file_edits WHERE id IN (SELECT UNNEST(?))",(list(uncertain),)).fetchall()}]
    (_parse_result_refs(conn,r),_quarantine_stubs(r))
    stale={tid for tid,status in conn.execute("SELECT id,status FROM tool_calls WHERE id IN (SELECT UNNEST(?))",[[t['id'] for t in r.tools if t['status'] not in ('complete','failed')]+[v['tool_call_id'] for v in r.edit_evidence if v['status']=='unknown' and v['reason']=='result_missing' and v['tool_call_id']]]).fetchall() if status in ('complete','failed')}
    if stale:
        r.tools=[t for t in r.tools if t['id'] not in stale]
        r.edits=[e for e in r.edits if e['id'] not in {v['file_edit_id'] for v in r.edit_evidence if v['tool_call_id'] in stale}]
        r.edit_evidence=[v for v in r.edit_evidence if v['tool_call_id'] not in stale]
    cids,mids,bindings=[c["id"] for c in r.convs],[m["id"] for m in r.msgs],[(c["source"],meta["session_id"],c["id"]) for c in r.convs if (meta:=json.loads(c["metadata"] or "{}")).get("session_id")]
    required(not (conflict:=conn.execute("SELECT p.source,p.session_id,p.conversation_id,json_extract_string(j.value,'$.conversation_id') FROM provider_sessions p JOIN json_each(?) j ON p.source=json_extract_string(j.value,'$.source') AND p.session_id=json_extract_string(j.value,'$.session_id') WHERE p.conversation_id<>json_extract_string(j.value,'$.conversation_id') LIMIT 1",(json.dumps([dict(source=s,session_id=i,conversation_id=c) for s,i,c in bindings]),)).fetchone() if bindings else None),ValueError(f"provider session identity conflict: {conflict}"))
    old_convs,old_msgs,new_convs,changed_rows,changed_msgs,updated,changed_conversations=(old_convs:=_rows_by_id(conn,"conversations",cids)),(old_msgs:=_rows_by_id(conn,"messages",mids)),(new_convs:=set(cids)-set(old_convs)),(changed_rows:={m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][:8]+old_msgs[m["id"]][9:]!=tuple(m.values())}),(changed_msgs:={m["id"] for m in r.msgs if m["id"] not in old_msgs or old_msgs[m["id"]][2:5]!=tuple(m[k] for k in ("role","content","thinking"))}),{m["conversation_id"] for m in r.msgs if m["id"] in changed_msgs}-new_convs,[list(c.values()) for c in r.convs if old_convs.get(c["id"])!=tuple(c.values())]
    with preserve_fact_heads(conn,[("conversations",row[0]) for row in changed_conversations if row[0] in old_convs],observed=True): changed_conversations and conn.executemany(_CONV_UPS,changed_conversations)
    (((bindings) and (conn.executemany("INSERT INTO provider_sessions VALUES (?,?,?) ON CONFLICT(source,session_id) DO NOTHING",list(dict.fromkeys(bindings))))),((frozen:=r.scopes if r.scopes is not None else pending_scopes([(c["id"],c["cwd"]) for c in r.convs])) and (conn.executemany("INSERT OR IGNORE INTO provenance.conversation_scopes VALUES (?,?,?,?,?,?)",frozen))))
    message_history=[_history_row("messages",old,old[2:5]) for m in r.msgs if (old:=old_msgs.get(m["id"])) and old[2:5]!=tuple(m[k] for k in ("role","content","thinking"))]
    changed_msgs|={row[0] for row in message_history}
    if message_history: conn.executemany("INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",message_history)
    with preserve_fact_heads(conn,[("messages",row_id) for row_id in changed_rows if row_id in old_msgs],observed=True): changed_rows and conn.executemany(_MSG_UPS,[list(m.values()) for m in r.msgs if m["id"] in changed_rows])
    previous_tools,edit_paths,edit_routes=_rows_by_id(conn,"tool_calls",[t["id"] for t in r.tools]) if r.edits else {},captured_edit_paths(conn,[e['id'] for e in r.edits]),{eid:(route,cwd) for eid,route,cwd in conn.execute("SELECT e.id,s.route,c.cwd FROM file_edits e JOIN provenance.file_edit_scopes s ON s.file_edit_id=e.id JOIN messages m ON m.id=e.message_id JOIN conversations c ON c.id=m.conversation_id WHERE s.route IS NOT NULL AND e.id IN (SELECT UNNEST(?))",[[e['id'] for e in r.edits]]).fetchall()} if r.edits else {}
    def replace_preserving(table, rows):
        if not rows: return [],0
        old,skip,payload,canonical,changed,histories=(old:=_rows_by_id(conn,table,[r["id"] for r in rows])),(skip:={"tool_calls":7,"attachments":7,"artifacts":6,"file_edits":5}[table]),(payload:=lambda values:tuple(v for i,v in enumerate(values) if i not in ((0,skip,5) if table=="attachments" else (0,skip)))),(canonical:=lambda values:payload([edit_paths[values[0]] if table=="file_edits" and i==2 and edit_paths.get(values[0]) and (scope:=edit_routes.get(values[0])) and (v==edit_paths[values[0]] or _resolved(v,scope[1])==scope[0]) else json.dumps(json.loads(v),sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False) if table=="tool_calls" and i in (3,4) and isinstance(v,str) else v for i,v in enumerate(values)])),(changed:=[(list(row.values()),old.get(row["id"])) for row in rows if not old.get(row["id"]) or canonical(old[row["id"]])!=canonical(tuple(row.values())) or old[row["id"]][skip]!=list(row.values())[skip] or table=="attachments" and old[row["id"]][5]!=row["path"]]),[_history_row(table,previous,payload(previous)) for values,previous in changed if previous and canonical(previous)!=canonical(values)]
        with preserve_fact_heads(conn,[(table,values[0]) for values,previous in changed if previous]+[("messages",values[1]) for values,previous in changed if table=="file_edits"],observed=True):
            if histories: conn.executemany(f"INSERT INTO {table} VALUES ({','.join('?'*len(histories[0]))}) ON CONFLICT DO NOTHING",histories)
            if table=='file_edits' and histories:
                old_evidence={v[0]:v[1:] for v in conn.execute("SELECT * FROM provenance.file_edit_evidence WHERE file_edit_id IN (SELECT UNNEST(?))",[[previous[0] for values,previous in changed if previous]]).fetchall()}
                old_scopes={v[0]:v[1:] for v in conn.execute("SELECT * FROM provenance.file_edit_scopes WHERE file_edit_id IN (SELECT UNNEST(?))",[[previous[0] for values,previous in changed if previous]]).fetchall()}
                old_facts={record['entity']:record for record in provenance_records(conn,{('edit.observed',previous[0]) for values,previous in changed if previous},historical=True)}
                for history,previous in zip(histories,[previous for values,previous in changed if previous and canonical(previous)!=canonical(values)]):
                    if previous[0] in old_scopes: _insert_pages(conn,'provenance.file_edit_scopes',[(history[0],*old_scopes[previous[0]])],mode=' OR IGNORE')
                    if (fact:=old_facts.get(previous[0])) and not conn.execute('SELECT 1 FROM provenance.file_edit_files WHERE file_edit_id=?',[history[0]]).fetchone(): project_native_provenance(conn,[{**fact,'entity':history[0],'payload':{**fact['payload'],'id':history[0]}}])
                    if (e:=old_evidence.get(previous[0])) is None: continue
                    tool=previous_tools.get(e[2])
                    prior=_history_row('tool_calls',tool,tuple(v for i,v in enumerate(tool) if i not in (0,7))) if tool and tool!=_rows_by_id(conn,'tool_calls',[tool[0]]).get(tool[0]) else None
                    if prior: _insert_pages(conn,'tool_calls',[prior],mode=' OR IGNORE')
                    _insert_pages(conn,'provenance.file_edit_evidence',[(history[0],*e[:2],prior[0] if prior else e[2])],mode=' OR IGNORE')
            if changed: conn.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({','.join('?'*len(changed[0][0]))})",[values for values,previous in changed])
        return [(table,values[0]) for values,previous in changed]+[(table,row[0]) for row in histories],len(changed)
    replacements,historical=(replacements:=[replace_preserving(table,rows) for table,rows in (("tool_calls",r.tools),("attachments",r.attachs),("artifacts",r.artifacts),("file_edits",r.edits))]),[item for rows,count in replacements for item in rows]
    evidence_row,evidence,classified,incoming=(evidence_row:=lambda v:(v["file_edit_id"],v["status"],v["reason"],v["tool_call_id"])),{v[0]:v for v in conn.execute("SELECT * FROM provenance.file_edit_evidence WHERE file_edit_id IN (SELECT UNNEST(?))",[[v["file_edit_id"] for v in r.edit_evidence]+[e["id"] for e in r.edits]]).fetchall()} if r.edit_evidence or r.edits else {},(classified:={v["file_edit_id"] for v in r.edit_evidence}),[*r.edit_evidence,*(dict(file_edit_id=e["id"],status="unknown",reason="unclassified_input",tool_call_id=None) for e in r.edits if e["id"] not in classified)]
    required(all(set(v)=={"file_edit_id","status","reason","tool_call_id"} and v["status"] in {"confirmed","invalid","unknown","unverified"} and v["file_edit_id"] and v["reason"] for v in incoming),ValueError("invalid file edit evidence"))
    tool_ids={v["tool_call_id"] for v in incoming if v["tool_call_id"]}
    required(not tool_ids or conn.execute("SELECT COUNT(*) FROM tool_calls WHERE id IN (SELECT UNNEST(?))",[list(tool_ids)]).fetchone()[0]==len(tool_ids),ValueError("file edit evidence tool call unavailable"))
    if delta:=[v for v in incoming if evidence.get(v["file_edit_id"])!=evidence_row(v)]:
        with preserve_fact_heads(conn,[("edit.observed",v["file_edit_id"]) for v in delta]+[("tool_calls",v["tool_call_id"]) for v in delta if v["tool_call_id"]],observed=True): conn.executemany("INSERT OR REPLACE INTO provenance.file_edit_evidence VALUES (?,?,?,?)",list(map(evidence_row,delta)))
    evidence_changed,statuses=(evidence_changed:={v["file_edit_id"] for v in incoming if evidence.get(v["file_edit_id"])!=evidence_row(v) and conn.execute("SELECT 1 FROM file_edits WHERE id=?",[v["file_edit_id"]]).fetchone()}),{v["file_edit_id"]:v["status"] for v in incoming}
    (frozen_edits:=r.edit_scopes) is not None or (frozen_edits:=pending_edit_scopes(edit_scope_inputs(r)))
    if frozen_edits: conn.executemany("INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,repository,root,checkout,route,observed_at) VALUES (?,?,?,?,?,?,?)",frozen_edits)
    for attachment in (a for a in r.attachs if a.get("path")): index_attachment_body(conn,attachment["id"],attachment["path"],attachment.get("size"),r.attachment_indexes[attachment["id"]])
    changed_convs,archive_msgs=(changed_convs:={row_id for row_id,row in _rows_by_id(conn,"conversations",cids).items() if old_convs.get(row_id)!=row}),changed_rows|changed_msgs-set(mids)
    if changed_convs or archive_msgs or historical or evidence_changed: _archive_touch(conn,[("conversations",x) for x in changed_convs]+[("messages",x) for x in archive_msgs]+historical+[("file_edits",x) for x in evidence_changed])
    r.provenance_edits,r.provenance_conversations={x for x in {e["id"] for e in r.edits}|classified if statuses.get(x)=="confirmed"}&({i for t,i in historical if t=="file_edits"}|evidence_changed),changed_convs
    if targets:=[*(('file_edits',x) for x in r.provenance_edits),*(('conversations',x) for x in r.provenance_conversations)]: conn.execute("INSERT OR REPLACE INTO provenance.pending SELECT t.kind,t.entity,generation FROM (SELECT UNNEST(?) kind,UNNEST(?) entity) t,archive_state WHERE singleton AND (t.kind='file_edits' OR NOT EXISTS (SELECT 1 FROM provenance.conversation_scopes s WHERE s.conversation=t.entity) OR EXISTS (SELECT 1 FROM provenance.conversation_scopes s WHERE s.conversation=t.entity AND (s.checkout LIKE 'pending:%' OR s.repository IS NOT NULL AND NOT EXISTS (SELECT 1 FROM provenance.repository_checkouts c WHERE (c.id,c.repository,c.root)=(s.checkout,s.repository,s.root)))))",[[kind for kind,_ in targets],[entity for _,entity in targets]])
    return len(changed_conversations),len(changed_rows),replacements[0][1],replacements[1][1],replacements[3][1],len(new_convs),len(updated),changed_msgs
def ingest_parts(r,size=500):
    prepared,valid,blank,scopes,edit_scopes,chunks,byid=prepare_result(r),_validate_result(r),lambda **values:ParseResult(**{"scopes":[],"edit_scopes":[],**values}),{v[0]:v for v in r.scopes},{v[0]:v for v in r.edit_scopes},lambda rows:[rows[i:i+size] for i in range(0,len(rows),size)],{m["id"]:m for m in r.msgs}
    rank,messages=(rank:={mid:i for i,mid in enumerate(graphlib.TopologicalSorter({mid:{m["parent_id"]}&byid.keys() for mid,m in byid.items()}).static_order())}),sorted(r.msgs,key=lambda m:rank[m["id"]])
    if sum(map(len,(r.convs,r.msgs,r.tools,r.attachs,r.artifacts,r.edits,r.edit_evidence)))<=size: return (setattr(r,"msgs",messages),[r])[-1]
    return [*[blank(convs=v,scopes=[scopes[c["id"]] for c in v if c["id"] in scopes]) for v in chunks(r.convs)],*[blank(msgs=v) for v in chunks(messages)],*[blank(tools=v) for v in chunks(r.tools)],*[blank(attachs=v,attachment_indexes={a["id"]:r.attachment_indexes[a["id"]] for a in v if a.get("path")}) for v in chunks(r.attachs)],*[blank(artifacts=v) for v in chunks(r.artifacts)],*[blank(edits=v,edit_scopes=[edit_scopes[e["id"]] for e in v if e["id"] in edit_scopes]) for v in chunks(r.edits)],*[blank(edit_evidence=v) for v in chunks(r.edit_evidence)]]
def commit_result(r,purpose,progress=None):
    with (_validate_result(r),_core(read_only=True,purpose=purpose))[1] as conn: _prune_remote_rows(conn,r,True)
    total,changed,newids=[0]*7,set(),set()
    for page,part in enumerate((p for p in ingest_parts(r) if any((p.convs,p.msgs,p.tools,p.attachs,p.artifacts,p.edits,p.edit_evidence))),1):
        with _core(purpose=purpose) as conn,_transaction(conn): out=(newids.update({c["id"] for c in part.convs}-{x[0] for x in conn.execute("SELECT id FROM conversations WHERE id IN (SELECT UNNEST(?))",[[c["id"] for c in part.convs]]).fetchall()}) if part.convs else None,upsert(conn,part))[-1]
        ((total.__setitem__(slice(None),[total[i]+out[i] for i in range(7)]),changed.update(out[7]),total.__setitem__(slice(5,7),[len(newids),len({m["conversation_id"] for m in r.msgs if m["id"] in changed}-newids)]),r.provenance_edits.update(part.provenance_edits),r.provenance_conversations.update(part.provenance_conversations),progress and progress(f"{purpose} batch {page}, {sum(total[:5])} rows changed")),archive_yield(DB_PATH))
    return (*total,changed)

def hook_root(source): return Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"projects" if source == "claude-code" else Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"sessions"
def hook_result(source,path,bindings=None):
    session=(parse_claude_code_session if source=="claude-code" else parse_codex_session)(path,bindings)
    return ParseResult(convs=[session["conv"]],msgs=session["msgs"],tools=session["tools"],attachs=session["attachs"],edits=session["edits"],edit_evidence=session["edit_evidence"]) if session else ParseResult()
def wake_hooks(attempt=None,root=None):
    with operation_lock((Path(root)/"data/hook_inbox" if root is not None else HOOK_DIR)/".drain.lock","hooks.dispatch",0,mandatory=False) as available: pass
    if available: subprocess.Popen([sys.executable,"-m","ai_convos","drain-hooks","--no-block"],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True,env={**os.environ,**({"CONVOS_PROJECT_ROOT":str(root)} if root is not None else {}),**({"CONVOS_HOOK_ATTEMPT":attempt} if attempt else {})})
def enqueue_hook(source, payload):
    path,root=Path(payload["transcript_path"]).expanduser().resolve(),hook_root(source).expanduser().resolve()
    if source not in ("claude-code", "codex") or path.suffix != ".jsonl" or not path.is_relative_to(root): raise ValueError(f"Invalid {source} transcript path")
    st,key=path.stat(),gen_id("hook",f"{source}:{path}")
    if payload.get("hook_event_name", "Stop") not in ("Stop","SessionEnd"):
        with operation_lock(HOOK_DIR/".flush.lock","hooks.flush",0,mandatory=False) as pulse:
            if not pulse: return
            tick,now=HOOK_DIR/f"{key}.tick",time.time()
            if tick.exists() and now-json.loads(tick.read_text())<60: return
            atomic_json(tick,now)
    if HOOK_STATE.exists() and json.loads(HOOK_STATE.read_text()).get(key)==[st.st_mtime_ns,st.st_size] and not (HOOK_DIR/f"{key}.work").exists() and not (HOOK_DIR/f"{key}.json").exists(): return
    with operation_lock(HOOK_DIR/".lock","hooks.queue"): atomic_json(HOOK_DIR/f"{key}.json",dict(source=source,path=str(path),mtime=st.st_mtime_ns,size=st.st_size))
    wake_hooks()
def retry_hook(work, force=False):
    q,target=(q:=work.with_suffix(".json")),q if q.exists() else work
    if force: atomic_json(target,{**json.loads(target.read_text()),"retry":True})
    work.unlink(missing_ok=True) if q.exists() else os.replace(work, q)
def _hook_stamp(path): return [(st:=path.stat()).st_mtime_ns,st.st_ino,st.st_size]
def drain_hooks(embed=False, local_only=False,block=False,provenance=False):
    done,claims,failed,started,attempt,remaining,resume,provenance_error=[],[],0,time.monotonic(),os.environ.get("CONVOS_HOOK_ATTEMPT") or str(time.time_ns()),0,False,None
    with operation_lock(HOOK_DIR/".drain.lock","hooks.drain",30 if block else 0,mandatory=block) as pulse:
        if not pulse: return 0
        with operation_lock(HOOK_DIR/".lock","hooks.queue"):
            state,previous=json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {},json.loads(HOOK_PROGRESS.read_text()) if HOOK_PROGRESS.exists() else {}
            attempted=previous.get("failed_claims",{}) if previous.get("attempt")==attempt else {}
            [done.append((work,work.stem,event["snap"],set(event["changed"]))) if "changed" in event else retry_hook(work,True) for work in HOOK_DIR.glob("*.work") for event in [json.loads(work.read_text())]]
            queued,(claims)=(queued:={p:_hook_stamp(p) for p in HOOK_DIR.glob("*.json")}),([(work,queued[queue]) for queue in [p for p in sorted(queued,key=lambda p:(queued[p][0],p.name)) if attempted.get(p.stem)!=queued[p]][:HOOK_DRAIN_EVENTS] for work in [queue.with_suffix(".work")] if not work.exists() and os.replace(queue,work) is None])
        bindings=None
        for n,(work,stamp) in enumerate(claims):
            if n and time.monotonic()-started>=HOOK_DRAIN_SECONDS: break
            try:
                pulse(f"processing {work.name}")
                e,path,key,st,snap=(e:=json.loads(work.read_text())),(path:=Path(e["path"])),work.stem,(st:=path.stat()),[st.st_mtime_ns,st.st_size]
                if state.get(key)==snap:
                    work.unlink()
                    continue
                if bindings is None:
                    with _core(ready=True,purpose="hooks.schema") as conn: bindings=session_bindings(conn)
                r,st2=hook_result(e["source"],path,bindings),path.stat()
                if snap!=[st2.st_mtime_ns,st2.st_size]:
                    with operation_lock(HOOK_DIR/".lock","hooks.queue"): retry_hook(work)
                    continue
                if not r.convs:
                    done.append((work,key,snap,set()))
                    continue
                changed=commit_result(r,purpose="hooks.ingest",progress=pulse)[-1]|({m["id"] for m in r.msgs} if e.get("retry") else set())
                (atomic_json(work,{**e,"snap":snap,"changed":sorted(changed)}),done.append((work,key,snap,changed)))
            except Exception as error:
                with operation_lock(HOOK_DIR/".lock","hooks.queue"): retry_hook(work)
                attempted[work.stem]=stamp
                failed+=1
                log_parse_error(f"hook inbox {work}",error)
        if done:
            with operation_lock(HOOK_DIR/".lock","hooks.queue"):
                (state.update((key,snap) for _,key,snap,_ in done),atomic_json(HOOK_STATE,state))
                for work,_,_,_ in done: work.unlink(missing_ok=True)
        if DB_PATH.exists() and (done or provenance or previous.get("provenance_pending")):
            with _core(read_only=True,purpose="hooks.provenance.plan") as conn: before=conn.execute("SELECT count(*) FROM provenance.pending").fetchone()[0]
            try:
                if before: (pulse("capturing provenance batch"),capture_provenance(edit_ids=[],conversation_ids=[],source="hooks",strict=False))
            except Exception as error: (log_parse_error("Provenance pending; hook imports are committed",error),(provenance_error:=f"{type(error).__name__}: {error}"))
            with _core(read_only=True,purpose="hooks.provenance.progress") as conn: remaining=conn.execute("SELECT count(*) FROM provenance.pending").fetchone()[0]
            resume=0<remaining<before
        pending={p:_hook_stamp(p) for p in [*HOOK_DIR.glob("*.json"),*HOOK_DIR.glob("*.work")]}
        atomic_json(HOOK_PROGRESS,dict(completed_at=time.time_ns(),processed=len(done),failed=failed,pending=len(pending),oldest=min((s[0] for s in pending.values()),default=None),attempt=attempt,failed_claims={p.stem:s for p,s in pending.items() if attempted.get(p.stem)==s},provenance_pending=remaining,provenance_error=provenance_error))
    with operation_lock(HOOK_DIR/".lock","hooks.queue"):
        if resume or any(attempted.get(p.stem)!=_hook_stamp(p) for p in [*HOOK_DIR.glob("*.json"),*HOOK_DIR.glob("*.work")]): wake_hooks(attempt)
    return len(done)

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
def _activate_embedding_profile(limit=None):
    profile=embedding_profile()
    with _core(read_only=True,purpose="embedding.profile.read") as conn: old,wrong=(conn.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state WHERE singleton").fetchone() or [None])[0],conn.execute("SELECT COUNT(*) FROM messages WHERE embedding IS NOT NULL AND len(embedding)<>?",(profile["dimensions"],)).fetchone()[0]
    changed=old is not None and json.loads(old)!=profile
    if limit and (changed or wrong): raise ValueError("Embedding profile reset requires full maintenance; rerun `convos embed` without --limit")
    with _core(ready=True,purpose="embedding.profile.write") as conn,_transaction(conn):
        if changed or wrong: conn.execute("UPDATE messages SET embedding=NULL WHERE embedding IS NOT NULL AND (? OR len(embedding)<>?)",(changed,profile["dimensions"]))
        conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",(json.dumps(profile,sort_keys=True,separators=(",",":")),))
    return profile,changed or bool(wrong)
def embed_texts(ss: list[str], doc: bool = False, local_only=False) -> list[list[float]]:
    profile,(texts)=(profile:=embedding_profile()),([profile["document_prefix" if doc else "query_prefix"]+(s or "")[:profile["character_limit"]] for s in ss])
    vectors=[d["embedding"] for d in _llama(local_only).create_embedding(texts)["data"]]
    required(all(len(v)==profile["dimensions"] for v in vectors),ValueError("Embedding runtime returned the wrong dimensions"))
    return vectors
def embed_text(s: str, doc: bool = False, local_only=False) -> list[float]: return embed_texts([s], doc, local_only)[0]
def embed_pending(batch: int = 32, ids=None, local_only=False,limit=None,progress=None):
    if ids == []: return
    profile,reset=(progress and progress("checking profile"),_activate_embedding_profile(limit))[-1]
    ids,Q,ps=(ids:=None if reset else ids),(Q:="FROM messages WHERE embedding IS NULL AND content IS NOT NULL AND content != '' AND json_extract_string(metadata,'$.history_of') IS NULL"+_NOISE+(f" AND id IN ({','.join(['?']*len(ids))})" if ids is not None else "")),ids or []
    with _core(read_only=True,purpose="embedding.count") as conn: n=min(conn.execute(f"SELECT COUNT(*) {Q}",ps).fetchone()[0],limit) if limit else conn.execute(f"SELECT COUNT(*) {Q}",ps).fetchone()[0]
    if not n: return
    typer.echo(f"Embedding {n} messages...",err=True)
    done=0
    while done<n:
        with _core(read_only=True,purpose="embedding.batch.read") as conn: rows=conn.execute(f"SELECT id,left(content,?),sha256(content) {Q} ORDER BY LEAST(length(content),?) LIMIT ?",[profile["character_limit"],*ps,profile["character_limit"],min(batch,n-done)]).fetchall()
        if not rows: break
        step,(updates)=(step:=_MCFG["n_seq_max"] if profile["backend"]=="llama.cpp" else batch),((progress and progress(f"embedding {done}/{n}"),[(vector,mid,content_hash) for chunk in (rows[i:i+step] for i in range(0,len(rows),step)) for (mid,_,content_hash),vector in zip(chunk,embed_texts([content for _,content,_ in chunk],doc=True,local_only=local_only))])[-1])
        with _core(purpose="embedding.batch.write") as conn,_transaction(conn): conn.executemany("UPDATE messages SET embedding=? WHERE id=? AND sha256(content)=? AND embedding IS NULL",updates)
        archive_yield(DB_PATH)
        typer.echo(f"  {(done:=done+len(rows))}/{n}\r",nl=False,err=True)
    with _core(read_only=True,purpose="embedding.remaining") as conn: remaining=conn.execute(f"SELECT COUNT(*) {Q}",ps).fetchone()[0]
    if remaining: typer.echo(f"{remaining} messages remain unembedded; rerun `convos embed` or use lexical `convos search`.",err=True)
    typer.echo(err=True)
    return done
def _ro():
    try: c=get_db(read_only=True,purpose="retrieval")
    except ValueError as e: raise typer.Exit(typer.echo(str(e),err=True) or 1)
    if c is None: raise typer.Exit(typer.echo("Database not found. Run `convos init` or `convos sync`.",err=True) or 1)
    if not ensure_db_ready(c): raise typer.Exit(c.close() or 1)
    return c
def _fts_ro():
    c,(state)=(c:=(c:=_ro()).execute("BEGIN TRANSACTION") and c),(c.execute("SELECT messages_generation,fts_generation,fts_definition_hash FROM retrieval_state WHERE singleton").fetchone())
    current=bool(state and state[0]==state[1] and state[2]==_FTS_DEF and c.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone())
    try: current and load_fts(c)
    except ValueError: current=False
    return c,"bm25" if current else "scan"
def _filt(source, days, role, cwd=None, conversation=None):
    w,p=map(list,zip(*pairs)) if (pairs:=[(q,v) for v,q in ((source,"c.source = ?"),(datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(days=days) if days else None,"m.created_at > ?"),(role,"m.role = ?")) if v]) else ([],[])
    if cwd: raw,resolved,w,p=(raw:=str(Path(cwd).expanduser().absolute())),(resolved:=str(Path(cwd).expanduser().resolve())),[*w,"(c.cwd=? OR starts_with(c.cwd,?) OR c.cwd=? OR starts_with(c.cwd,?))"],[*p,raw,raw.rstrip("/")+"/",resolved,resolved.rstrip("/")+"/"]
    conversation and (w.append("starts_with(c.id,?)"),p.append(conversation))
    return w, p
def _clip(s, n): return (s or "")[:n] + ("..." if s and len(s) > n else "")
def _lexical_ids(conn,q,w,p,mode,n=50):
    where=f"json_extract_string(m.metadata,'$.history_of') IS NULL AND (m.content IS NOT NULL OR m.thinking IS NOT NULL){_NOISE}{' AND ' + ' AND '.join(w) if w else ''}"
    if mode=="bm25":
        rows=conn.execute(f"WITH scored AS (SELECT m.id,m.conversation_id,fts_main_messages.match_bm25(m.id,?) score FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE {where}),ranked AS (SELECT *,ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY score DESC,id) pick FROM scored WHERE score IS NOT NULL) SELECT id,score FROM ranked WHERE pick=1 ORDER BY score DESC LIMIT ?",[q]+p+[n]).fetchall()
        return (rows,"bm25") if rows else _lexical_ids(conn,q,w,p,"scan",n)
    terms,(contains)=(terms:=list(dict.fromkeys(re.findall(r"\w+",q.lower())))),(" AND contains(lower(coalesce(m.content,'')||' '||coalesce(m.thinking,'')),?)"*len(terms))
    return (conn.execute(f"SELECT id,score FROM (SELECT m.id,m.conversation_id,CAST(1 AS DOUBLE) score,ROW_NUMBER() OVER (PARTITION BY m.conversation_id ORDER BY m.created_at DESC NULLS LAST,m.id) pick FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE {where}{contains}) WHERE pick=1 ORDER BY id LIMIT ?",p+terms+[n]).fetchall() if terms else []),"scan"
def _hit_rows(conn,ranked,limit):
    order,scores,rows=(order:={mid:i for i,(mid,_) in enumerate(ranked)}),dict(ranked),conn.execute("SELECT m.id,m.role,m.content,m.thinking,m.created_at,c.title,c.source,c.id,c.cwd FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.id IN (SELECT UNNEST(?))",([*order],)).fetchall() if order else []
    seen=set()
    return [(scores[mid],mid,role,content,thinking,created,title,source,cid,cwd) for mid,role,content,thinking,created,title,source,cid,cwd in sorted(rows,key=lambda r:order[r[0]]) if cid not in seen and not seen.add(cid)][:limit]
def _fmt_hit(content, ts, role, title, src, cid, cwd, q, ctx, meta):
    p = _clip(content, ctx)
    for w in q.split(): p = re.sub(f"({re.escape(w)})", r"\033[1;33m\1\033[0m", p, flags=re.I)
    typer.echo(f"\n{'='*60}\n[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid[:8]})\n{role} @ {ts or '?'} ({meta})\n{'-'*40}\n{p}")

def emit(data,fmt): [typer.echo(json.dumps(row,default=str)) for row in data if fmt=="jsonl" and isinstance(data,list)] if fmt=="jsonl" and isinstance(data,list) else typer.echo(json.dumps(data,default=str))

def capture(source: str):
    try: enqueue_hook(source, json.loads(sys.stdin.read() or "{}"))
    except Exception as e: log_parse_error(f"{source} hook", e)

def drain_hooks_cmd(block:bool=typer.Option(False,"--block/--no-block",hidden=True)): drain_hooks(block=block,provenance=True)

def init():
    with _core(ready=True,purpose="maintenance.fts") as conn: rebuild_fts_index(conn)
    (HOOK_FTS_DIRTY.unlink(missing_ok=True),sync(False,300,True,True,False,False,True),install_skills(),install_hooks(False,False),[typer.echo(ep.load()()) for ep in entry_points(group="convos.init")],typer.echo(f"Database initialized at {DB_PATH}"))

def fts():
    with _core(ready=True,purpose="maintenance.fts") as conn: rebuild_fts_index(conn)
    (HOOK_FTS_DIRTY.unlink(missing_ok=True),typer.echo("FTS index ready"))

def search(query: str, source: str|None = typer.Option(None, "-s"), days: int|None = typer.Option(None, "-d"), role: str|None = typer.Option(None, "-r"), cwd: Path|None = typer.Option(None, "--cwd", "-w"), conversation: str|None = typer.Option(None, "--conversation"), thinking: bool = typer.Option(False, "--thinking", "-t"), limit: int = typer.Option(20, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format")):
    conn,mode=_fts_ro()
    with contextlib.closing(conn):
        w,p=_filt(source,days,role,cwd,conversation)
        results,mode=(lambda ranked:(_hit_rows(conn,ranked[0],limit),ranked[1]))(_lexical_ids(conn,query,w,p,mode,limit))
    if mode=="scan": typer.echo("FTS index is stale; using a complete lexical scan. Run `convos fts` to restore BM25 ranking.",err=True)
    if fmt!="text": return emit([dict(message_id=mid,role=r,content=_clip(content,context),thinking=_clip(think,context) if thinking and think else None,created_at=ts,score=score,title=title,source=src,conversation_id=cid,cwd=cwd) for score,mid,r,content,think,ts,title,src,cid,cwd in results],fmt)
    if not results: return typer.echo("No results")
    [(_fmt_hit(content,ts,role,title,source,cid,cwd,query,context,f"score: {score:.2f}; lexical: {mode}"),thinking and think and typer.echo(f"\n[THINKING]\n{_clip(think,context)}")) for score,_,role,content,think,ts,title,source,cid,cwd in results]
    typer.echo(f"\n{len(results)} results")

def read_cmd(conversation: str, limit: int = typer.Option(20, "-n", min=1), context: int = typer.Option(2000, "-c", min=1), around: str|None = typer.Option(None, "--around", "-a"), thinking: bool = typer.Option(False, "--thinking", "-t"), fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    with contextlib.closing(_ro()) as conn:
        cs=conn.execute("SELECT id,title,source,cwd FROM conversations WHERE starts_with(id, ?) ORDER BY updated_at DESC NULLS LAST LIMIT 2",[conversation]).fetchall()
        if len(cs)!=1: raise typer.Exit(typer.echo("No matching conversation" if not cs else "Ambiguous prefix: "+", ".join(c[0] for c in cs),err=True) or 1)
        cid,title,src,cwd=cs[0]
        base=f"SELECT m.id,m.role,m.content,m.thinking,m.created_at,ROW_NUMBER() OVER (ORDER BY {MESSAGE_ORDER}) pos FROM messages m WHERE m.conversation_id=? AND json_extract_string(m.metadata,'$.history_of') IS NULL AND (COALESCE(m.content,'')!='' OR COALESCE(m.thinking,'')!='')"
        if around and len(mids:=conn.execute("SELECT id FROM messages WHERE conversation_id=? AND starts_with(id,?) AND json_extract_string(metadata,'$.history_of') IS NULL LIMIT 2",[cid,around]).fetchall())!=1: raise typer.Exit(typer.echo("No matching message" if not mids else "Ambiguous message prefix: "+", ".join(m[0] for m in mids),err=True) or 1)
        rows=conn.execute(f"WITH b AS ({base}),t AS (SELECT pos FROM b WHERE id=?) SELECT id,role,content,thinking,created_at FROM (SELECT b.*,abs(b.pos-t.pos) d FROM b,t ORDER BY d,b.pos LIMIT ?) ORDER BY pos",[cid,mids[0][0],limit]).fetchall() if around else conn.execute(f"SELECT id,role,content,thinking,created_at FROM ({base}) ORDER BY pos DESC LIMIT ?",[cid,limit]).fetchall()[::-1]
    data = [dict(id=mid, role=role, content=_clip(content, context), thinking=_clip(think, context) if thinking and think else None, created_at=ts) for mid, role, content, think, ts in rows]
    if fmt!="text": return emit(data,fmt)
    typer.echo(f"[{src}] {title or 'Untitled'}{f' @ {cwd}' if cwd else ''} ({cid})")
    for message in data: typer.echo("\n{} @ {}\n{}{}".format(message["role"],message["created_at"] or "?",message["content"],"\n[THINKING]\n{}".format(message["thinking"]) if message["thinking"] else ""))
    typer.echo(f"\n{len(data)} messages")

def hybrid_hits(q,source=None,days=None,role=None,limit=10,local_only=False,cwd=None,conversation=None,status=None,require_complete=False):
    w,p=_filt(source,days,role,cwd,conversation)
    profile,stored,error=None,None,None
    try: profile=embedding_profile()
    except ValueError as e: error=str(e)
    with contextlib.closing(_ro()) as conn:
        stored,(where)=(stored:=(conn.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state WHERE singleton").fetchone() or [None])[0]),(f"FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE m.content IS NOT NULL AND m.content!='' AND json_extract_string(m.metadata,'$.history_of') IS NULL{_NOISE}{' AND ' + ' AND '.join(w) if w else ''}")
        useful=bool(profile and stored and json.loads(stored)==profile and conn.execute(f"SELECT 1 {where} AND m.embedding IS NOT NULL AND len(m.embedding)=? LIMIT 1",p+[profile["dimensions"]]).fetchone())
    qv=None
    if useful:
        try: qv=embed_text(q,False,local_only)
        except Exception as e: error=f"Hybrid embedding unavailable: {e}"
    conn,mode=_fts_ro()
    stored,(eligible)=(stored:=(conn.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state WHERE singleton").fetchone() or [None])[0]),(conn.execute(f"SELECT COUNT(*) {where}",p).fetchone()[0])
    valid=conn.execute(f"SELECT COUNT(*) {where} AND m.embedding IS NOT NULL AND len(m.embedding)=?",p+[profile["dimensions"]]).fetchone()[0] if profile and stored and json.loads(stored)==profile else 0
    if valid and qv is None and not error:
        conn.close()
        try: qv=embed_text(q,False,local_only)
        except Exception as e: error=f"Hybrid embedding unavailable: {e}"
        conn,mode=_fts_ro()
        stored,eligible=(conn.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state WHERE singleton").fetchone() or [None])[0],conn.execute(f"SELECT COUNT(*) {where}",p).fetchone()[0]
        valid=conn.execute(f"SELECT COUNT(*) {where} AND m.embedding IS NOT NULL AND len(m.embedding)=?",p+[profile["dimensions"]]).fetchone()[0] if profile and stored and json.loads(stored)==profile else 0
    with contextlib.closing(conn):
        lexical,mode=_lexical_ids(conn,q,w,p,mode,limit)
        vectors,(mids)=(vectors:=conn.execute(f"WITH scored AS (SELECT m.id,m.conversation_id,list_cosine_similarity(m.embedding,?::FLOAT[]) score {where} AND m.embedding IS NOT NULL AND len(m.embedding)=?),ranked AS (SELECT *,ROW_NUMBER() OVER (PARTITION BY conversation_id ORDER BY score DESC,id) pick FROM scored) SELECT id,score FROM ranked WHERE pick=1 ORDER BY score DESC LIMIT ?",[qv]+p+[len(qv),limit]).fetchall() if qv and valid else []),([x[0] for ranked in (lexical,vectors) for x in ranked])
        conversations,(scores)=(conversations:=dict(conn.execute("SELECT id,conversation_id FROM messages WHERE id IN (SELECT UNNEST(?))",[mids]).fetchall()) if mids else {}),({cid:sum(1/(60+rank) for ranked in (lexical,vectors) for rank,(mid,_) in enumerate(ranked,1) if conversations[mid]==cid) for cid in set(conversations.values())})
        ranked,(rows)=(ranked:=[(min((mid for source in (lexical,vectors) for mid,_ in source if conversations[mid]==cid),key=lambda mid:min(i for source in (lexical,vectors) for i,(candidate,_) in enumerate(source) if candidate==mid)),score) for cid,score in sorted(scores.items(),key=lambda x:-x[1])]),(_hit_rows(conn,ranked,limit))
    meta=dict(lexical_mode=mode,semantic_eligible=eligible,semantic_valid=valid,semantic_coverage=valid/eligible if eligible else 1.0,semantic_error=error)
    status is not None and status.update(meta)
    if require_complete and (mode!="bm25" or valid<eligible or error): raise ValueError(f"Retrieval is incomplete: lexical={mode}, semantic={valid}/{eligible}")
    return [dict(score=score,message_id=mid,role=role,content=content,created_at=created,title=title,source=source,conversation_id=cid,cwd=cwd) for score,mid,role,content,_,created,title,source,cid,cwd in rows]
def query_cmd(q: str, source: str|None = typer.Option(None, "-s"), days: int|None = typer.Option(None, "-d"), role: str|None = typer.Option(None, "-r"), cwd: Path|None = typer.Option(None, "--cwd", "-w"), conversation: str|None = typer.Option(None, "--conversation"), limit: int = typer.Option(10, "-n"), context: int = typer.Option(300, "-c"), fmt: str = typer.Option("text", "-f", "--format"), require_complete: bool = typer.Option(False,"--require-complete")):
    try: rows = hybrid_hits(q,source,days,role,limit,cwd=cwd,conversation=conversation,status=(status:={}),require_complete=require_complete)
    except ValueError as e: raise typer.Exit(typer.echo(str(e),err=True) or 1)
    [typer.echo(message,err=True) for condition,message in ((status["lexical_mode"]=="scan","FTS index is stale; using a complete lexical scan. Run `convos fts` to restore BM25 ranking."),(status["semantic_valid"]<status["semantic_eligible"],f"Semantic coverage is partial ({status['semantic_valid']}/{status['semantic_eligible']}); lexical recall remains complete. Run `convos embed` for full hybrid ranking."),(status["semantic_error"],status["semantic_error"])) if condition]
    if fmt!="text": return emit([{**r,"content":_clip(r["content"],context)} for r in rows],fmt)
    if not rows: return typer.echo("No results")
    for x in rows: _fmt_hit(x["content"], x["created_at"], x["role"], x["title"], x["source"], x["conversation_id"], x["cwd"], q, context, f"score: {x['score']:.4f}")
    typer.echo(f"\n{len(rows)} results")

def embed_cmd(batch: int = typer.Option(32, "-b",min=1),limit: int|None = typer.Option(None,"--limit",min=1)):
    with _core(ready=True,purpose="schema.embedding"): pass
    try:
        with operation_lock(DATA_DIR/".embed.lock","embedding",wait=0) as pulse: done=embed_pending(batch,limit=limit,progress=pulse)
        typer.echo(f"Processed {done or 0} embedding candidates" if limit else "Embeddings ready")
    except Exception as e: raise typer.Exit(typer.echo(f"Embedding failed: {e}",err=True) or 1)

def doctor(verbose: bool = typer.Option(False, "-v")):
    typer.echo(f"convos: {version('convos')}")
    pending,state,progress,last,age=(pending:=len(list(HOOK_DIR.glob("*.json")))+len(list(HOOK_DIR.glob("*.work")))),(state:=json.loads(HOOK_STATE.read_text()) if HOOK_STATE.exists() else {}),(progress:=json.loads(HOOK_PROGRESS.read_text()) if HOOK_PROGRESS.exists() else {}),max((v[0] for v in state.values()),default=0),max(0,time.time_ns()-(progress.get("oldest") or time.time_ns()))/1e9 if pending else 0
    typer.echo(f"ingest: pending={pending}, last={datetime.fromtimestamp(last/1e9).isoformat(timespec='seconds') if last else 'never'}, oldest={age:.0f}s, last_batch={progress.get('processed',0)} ok/{progress.get('failed',0)} failed")
    if progress.get("provenance_error"): typer.echo(f"provenance last error: {progress['provenance_error']}")
    local=load_state().get("local",{})
    known={key:'present' if any(v[2] for v in values) else 'missing' for key,values in itertools.groupby(sorted((source,provider_session_key(source,value[0]),Path(path).is_file()) for source,checkpoint in local.items() for path,value in checkpoint.get('bindings',{}).items() if value[0]),key=lambda v:v[:2])}
    for source,checkpoint in local.items():
        if "files" in checkpoint: typer.echo(f"inputs {source}: tracked={len(checkpoint['files'])}, missing_locally={len(checkpoint.get('missing',[]))}, retry={len(checkpoint.get('failed',[]))}, current_parser={sum(checkpoint.get('epochs',{}).get(p,checkpoint.get('parser'))==PARSER_EPOCH for p in checkpoint['files'])}")
    if DB_PATH.exists():
        try:
            with _core(read_only=True,purpose="doctor") as conn:
                cols,required,missing,(convs,msgs,unembedded,latest),fts_schema,fts,evidence,relationships=(cols:=set(conn.execute("SELECT table_name,column_name FROM information_schema.columns").fetchall())),(required:={"conversations":("id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"),"messages":("id","conversation_id","role","content","thinking","created_at","model","metadata","embedding","parent_id"),"tool_calls":("id","message_id","tool_name","input","output","status","duration_ms","created_at"),"attachments":("id","message_id","filename","mime_type","size","path","url","created_at"),"artifacts":("id","conversation_id","artifact_type","title","content","language","created_at","version"),"file_edits":("id","message_id","file_path","edit_type","content","created_at","old_content"),"file_edit_evidence":("file_edit_id","status","reason","tool_call_id")}),(missing:=[f"{table}.{column}" for table,columns in required.items() for column in columns if (table,column) not in cols]),conn.execute(f"SELECT (SELECT COUNT(*) FROM conversations),(SELECT COUNT(*) FROM messages),(SELECT COUNT(*) FROM messages WHERE embedding IS NULL AND COALESCE(content,'')!=''{_NOISE}),(SELECT MAX(updated_at) FROM conversations)").fetchone() if not missing else (0,0,0,None),(fts_schema:=bool(conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone())),bool(fts_schema and ("retrieval_state","messages_generation") in cols and (rs:=conn.execute("SELECT messages_generation,fts_generation,fts_definition_hash FROM retrieval_state WHERE singleton").fetchone()) and rs[0]==rs[1] and rs[2]==_FTS_DEF),dict(conn.execute("SELECT status,COUNT(*) FROM provenance.file_edit_evidence GROUP BY status").fetchall()) if not missing else {},archive_relationships(conn,True) if not missing else {}
                provenance_pending=conn.execute("SELECT count(*) FROM provenance.pending").fetchone()[0] if ("pending","entity") in cols else None
                coverage=[(source,known.get((source,provider_session_key(source,session)),'untracked')) for source,session in conn.execute("SELECT c.source,json_extract_string(c.metadata,'$.session_id') FROM conversations c WHERE c.source IN ('codex','claude-code') AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id)").fetchall()] if not missing else []
            typer.echo(f"archive: {convs} convs, {msgs} msgs, {unembedded} unembedded, {DB_PATH.stat().st_size/1024**3:.1f} GB, latest={latest or 'never'}, schema={'ready' if not missing else 'missing:' + ','.join(missing)}, fts={'current' if fts else 'stale' if fts_schema else 'missing'}, edit_evidence="+",".join(f"{k}:{evidence.get(k,0)}" for k in ("confirmed","unknown","invalid","unverified"))+("\nmissing physical parents: unavailable (schema incomplete)" if missing else "\nmissing physical parents:\n"+"\n".join(f"  {key}: rows={value['rows']}, parent_ids={value['parent_ids']}"+(f", marked_history_rows={value['marked_history_rows']}" if key.startswith('messages.') or value['marked_history_rows'] else "") for key,value in relationships.items())))
            typer.echo(f"provenance: pending={provenance_pending if provenance_pending is not None else 'unavailable'}")
            for source in sorted({s for s,status in coverage}): typer.echo(f"local sources {source}: "+', '.join(f'{status}={sum((s,value)==(source,status) for s,value in coverage)}' for status in ('present','missing','untracked'))+' (native conversations)')
            if any(status=='untracked' for source,status in coverage): typer.echo('untracked: no recorded local input association; source availability on other devices is unknown')
            if missing or not fts: typer.echo("repair: convos init" if missing else "repair: convos fts")
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
    if not skill.exists(): raise typer.Exit(typer.echo(f"Missing skill: {skill}",err=True) or 1)
    text,legacy,resolved=(text:=skill.read_text()),text.replace("name: convos","name: agent-convos",1).replace("# Convos","# Agent Convos",1),[Path(os.path.realpath(p)) for p in dests]
    if unsafe:=next((p for home,p,target in zip(homes,dests,resolved) if p.is_symlink() or p.exists() and not p.is_file() or any(q.is_symlink() and resolved.count(target)<2 or q.exists() and not q.is_dir() for q in [home/Path(*rel.parts[:i]) for i in range(1,len(rel.parts))])),None): raise typer.Exit(typer.echo(f"Refusing unsafe managed file: {unsafe}",err=True) or 1)
    for dest,old in zip(dests,(home/"skills"/"agent-convos"/"SKILL.md" for home in homes)):
        if atomic_write(dest,text) is None: typer.echo(f"Installed {dest}")
        if old.is_file() and not old.is_symlink() and old.read_text()==legacy: typer.echo(f"Removed legacy {old}") if old.unlink() is None else None

def _capture_command(source): return f"{f'CONVOS_PROJECT_ROOT={shlex.quote(str(root))} ' if (root:=Path(os.environ.get('CONVOS_PROJECT_ROOT',PROJECT_ROOT)).expanduser().resolve())!=Path.home()/'.convos' else ''}{shlex.quote(str(Path(sys.executable).with_name('convos')))} capture {source}"
def _managed_hook(h, source): return h.get("command", "").endswith("convos remote hook") or h.get("command", "").endswith((f" hook {source}", f" capture {source}")) and h.get("statusMessage") in ("Updating conversation archive", "Saving conversation to Convos")
def edit_hook_config(path, events, source, remove=False):
    data,(clean)=(data:=json.loads(path.read_text()) if path.exists() else {}),(lambda groups:[{**group,"hooks":kept} for group in groups for kept in [[h for h in group.get("hooks",[]) if not _managed_hook(h,source)]] if kept])
    hooks,(cmd)=(hooks:={event:kept for event,groups in data.get("hooks",{}).items() if (kept:=clean(groups))}),(_capture_command(source) if not remove else None)
    for event in (() if remove else events): hooks.setdefault(event, []).append(dict(hooks=[dict(type="command", command=cmd, timeout=5, statusMessage="Saving conversation to Convos")]))
    return {**data,"hooks":hooks}, sum(_managed_hook(h, source) for gs in hooks.values() for g in gs for h in g.get("hooks", []))

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
    where,params=("WHERE c.source = ?",[source]) if source else ("",[])
    with contextlib.closing(_ro()) as conn:
        if fmt=="json":
            rows,grouped=(rows:=conn.execute(f"SELECT c.id,c.source,c.title,c.created_at,c.updated_at,c.model,c.cwd,c.git_branch,c.project_id FROM conversations c {where}",params).fetchall()),lambda values:{cid:[v[1:] for v in group] for cid,group in itertools.groupby(values,key=lambda v:v[0])}
            ids,((messages, tools, edits))=(ids:=[r[0] for r in rows]),(grouped(conn.execute(f"SELECT m.conversation_id,m.role,m.content,m.thinking,m.created_at,m.model FROM messages m WHERE m.conversation_id IN (SELECT UNNEST(?)) ORDER BY m.conversation_id,{MESSAGE_ORDER}",[ids]).fetchall()),grouped(conn.execute("SELECT m.conversation_id,tc.tool_name,tc.input,tc.output,tc.status FROM tool_calls tc JOIN messages m ON tc.message_id=m.id WHERE m.conversation_id IN (SELECT UNNEST(?)) AND NOT EXISTS(SELECT 1 FROM parser_tool_history h WHERE h.old_id=tc.id) ORDER BY m.conversation_id,tc.id",[ids]).fetchall()),grouped(conn.execute("SELECT m.conversation_id,fe.file_path,fe.edit_type,fe.content,COALESCE(v.status,'unverified'),COALESCE(v.reason,'source_unavailable'),v.tool_call_id FROM current_file_edits fe JOIN messages m ON fe.message_id=m.id LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id WHERE m.conversation_id IN (SELECT UNNEST(?)) ORDER BY m.conversation_id,fe.created_at NULLS FIRST,fe.id",[ids]).fetchall()))
        else:
            cur,((header, data))=(cur:=conn.execute(f"SELECT c.id,c.source,c.title,c.cwd,m.role,m.content,m.created_at FROM conversations c JOIN messages m ON c.id=m.conversation_id {where} ORDER BY c.created_at,{MESSAGE_ORDER}",params)),([d[0] for d in cur.description],cur.fetchall())
    if fmt=="json":
        def build(row): return dict(id=row[0],source=row[1],title=row[2],created_at=str(row[3]) if row[3] else None,updated_at=str(row[4]) if row[4] else None,model=row[5],cwd=row[6],git_branch=row[7],project_id=row[8],messages=[dict(role=m[0],content=m[1],thinking=m[2],created_at=str(m[3]) if m[3] else None,model=m[4]) for m in messages.get(row[0],[])],tool_calls=[dict(tool=t[0],input=json.loads(t[1]),output=json.loads(t[2]),status=t[3]) for t in tools.get(row[0],[])],file_edits=[dict(file=e[0],type=e[1],content=e[2],evidence_status=e[3],evidence_reason=e[4],tool_call_id=e[5]) for e in edits.get(row[0],[])])
        output.write_text(json.dumps([build(row) for row in rows],indent=2))
    else:
        with output.open("w",newline="") as stream: csv.writer(stream).writerows([header,*data])
    typer.echo(f"Exported to {output}")

def backup():
    if not DB_PATH.exists() or DB_PATH.is_symlink() or not DB_PATH.is_file(): raise typer.Exit(typer.echo("Archive not found; nothing was backed up.",err=True) or 1)
    with _core(purpose="maintenance.backup") as conn: path=_migration_backup(conn,datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ"))
    typer.echo(f"Archive backed up to {path} with attachments at {path}.attachments")

def _sync_leader(fn,full=False):
    with operation_lock(DATA_DIR/".sync.lock","sync",wait=0) as pulse,(operation_lock(HOOK_DIR/".drain.lock","sync.full.capture",wait=30) if full else contextlib.nullcontext()) as capture: return fn(lambda stage:(pulse(stage),capture and capture(stage),sys.stderr.isatty() and (now:=time.monotonic())-getattr(pulse,"shown",0)>=1 and (setattr(pulse,"shown",now),typer.echo(f"  Local sync: {stage}",err=True)))[0])

def sync(watch: bool = typer.Option(False, "-w"), interval: int = typer.Option(300, "-i"), claude_code: bool = True, codex: bool = True, full: bool = typer.Option(False, "--full", help="Re-parse/re-fetch all sources and reconcile all provenance"), verbose: bool = typer.Option(False, "-v", "--verbose"), local_only: bool = typer.Option(False, "--local-only", help="Import local agent sessions and configured exports without contacting web sources.")):
    if sys.argv[1:2] == ["sync"]: signal.signal(signal.SIGINT, signal.SIG_DFL)
    state,local,web,imports,chatgpt_ok,chatgpt_frontiers,offline,ready,checkpoint_lock={},{},{},{},{},{},local_only is True,False,threading.RLock()
    def set_state(section,key,val):
        with checkpoint_lock: (state.setdefault(section,{}).__setitem__(key,val),atomic_json(STATE_PATH,state))
    def plan_local(name, path, parser, bindings, sink, progress):
        if name in ("codex", "claude-code"):
            previous,prev,mt=(previous:=local.get(name,{})),previous.get("files",{}),{str(p):m for p in path.rglob("*.jsonl") if (m:=stat_mtime(p)) is not None}
            epochs,missing,dependencies={p:previous.get("epochs",{}).get(p,previous.get("parser")) for p in prev},sorted(set(prev)-mt.keys()),previous.get("bindings",{})
            if missing!=previous.get("missing",[]): set_state("local",name,{**previous,"missing":missing})
            if missing: typer.echo(f"{name}: {len(missing)} previously imported transcript(s) missing locally; archived conversations retained.",err=True)
            if not (chg:=[Path(p) for p,m in mt.items() if full or p not in prev or m!=prev[p] or epochs.get(p)!=PARSER_EPOCH or p in previous.get("failed",[]) or p not in dependencies or dependencies[p][1]!=bindings.get((name,dependencies[p][0]),gen_id(name,dependencies[p][0]))]): return None
            saved,checkpoint,rejected=[],dict(parser=PARSER_EPOCH,files=dict(prev),epochs=epochs,missing=missing,failed=previous.get("failed",[]),bindings=dict(dependencies)),[]
            typer.echo(f"{name}: importing {len(chg)} transcript(s); progress is saved after each batch.",err=True)
            def run():
                for i in range(0,len(chg),20):
                    parsed=parser(path,chg[i:i+20],bindings)
                    saved.append(sink(parsed))
                    failed,finished=(failed:={str(p) for p in parsed.failed_inputs}),{str(p):mt[str(p)] for p in chg[i:i+20] if str(p) not in failed and stat_mtime(p)==mt[str(p)]}
                    rejected.extend(str(p) for p in chg[i:i+20] if str(p) not in finished)
                    with checkpoint_lock:
                        checkpoint["files"].update(finished)
                        checkpoint["epochs"].update({p:PARSER_EPOCH for p in finished})
                        checkpoint["bindings"].update({p:[session,bindings.get((name,session),gen_id(name,session))] for p in finished for session in [parsed.input_sessions.get(p)]})
                        checkpoint["failed"]=sorted((set(checkpoint["failed"])-finished.keys())|failed|{str(p) for p in chg[i:i+20] if str(p) not in finished})
                        set_state("local",name,checkpoint)
                    progress(f"parsing {name} {min(i+20,len(chg))}/{len(chg)}")
                return ParseResult()
            return dict(name=name,label=name.replace("-"," ").title(),source=name,func=run,saved=saved,failed=rejected)
        if not path.exists(): return None
        mtime = latest_mtime(path)
        return None if not full and mtime<=local.get(name,{}).get("mtime",0) else dict(name=name,label=name.replace("-"," ").title(),source=name,func=lambda p=path:parser(p),state=("local",name,{"mtime":mtime}))
    def probe_chatgpt(browser):
        def one(profile):
            try:
                cookies,base=chatgpt_cookie_base(browser,_CHATGPT_HOSTS,profile)
                headers,((items, account))=(headers:=chatgpt_headers(cookies,base,_BROWSER_UA[browser])),(fetch_json(f"{base}/backend-api/conversations?offset=0&limit=1&order=updated",cookies,headers,rate_limit_backoff=300)["items"],headers.get("ChatGPT-Account-ID"))
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
                head,((lu, current))=(head:=probe(b)),(head.split(":",1)[1] if name=="claude" and head and ":" in head else None,{**(pref.get("frontiers",{}) if b==pref.get("browser") else {}),**chatgpt_frontiers.get(b,{})} if name=="chatgpt" else None)
                st={"browser":b,"head":head,**({"frontiers":current} if name=="chatgpt" else {"last_updated":lu} if lu else {})}
                if name!="chatgpt" and head is not None and b==pref.get("browser") and head==pref.get("head") and not full: return set_state("web",name,st)
                last,saved,coverage,since=(last:=pref.get("last_updated")),None if name=="claude" else [],pref.get("coverage"),ts_from_iso(last) if name=="claude" and last and not full else None
                frontier,(func)=(frontier:=pref.get("frontiers") if frontier_ok and not full and b==pref.get("browser") and known and isinstance(coverage,list) and set(coverage)<=set(known) else None),((lambda b=b,since=since:fetcher(b,since=since)) if name=="claude" else (lambda b=b,saved=saved:fetcher(b,profiles=chatgpt_ok[b],known=known,legacy=legacy,frontiers=frontier,sink=lambda r:saved.append(sink(r)))))
                return dict(name=name,label=name.title(),source=name,func=func,state=("web",name,st),saved=saved)
            except Exception as e: errors.append(f"{b}: {e}")
        if errors: typer.echo(f"{name}: no cookies found -- skipped" if all("cookies" in e.lower() for e in errors) else f"{name} sync failed: " + " | ".join(errors))
    def plan_import(path: Path):
        if not path.exists(): return None
        mtime=latest_mtime(path) if path.is_dir() else path.stat().st_mtime
        return None if mtime<=imports.get(str(path),{}).get("mtime",0) else dict(name=f"import:{path}",label=f"import:{path}",func=lambda p=path:parse_source(p),state=("imports",str(path),{"mtime":mtime}))
    def run_sync(pulse):
        nonlocal state,local,web,imports,ready
        if not ready:
            with _core(ready=True,purpose="sync.schema"): pass
            drain_hooks()
            ready=True
        state,before,t0=(state:=pulse("planning") or load_state()),json.dumps(state,sort_keys=True),time.perf_counter()
        local,web,imports=state.setdefault("local",{}),state.setdefault("web",{}),state.setdefault("imports",{})
        chatgpt_ok.clear() or chatgpt_frontiers.clear()
        total,changed,jobs,newc,updc,provenance_edits,provenance_conversations,repair_attempted,failed_sources=[0]*5,set(),[],0,0,set(),set(),set(),set()
        def checkpoint(r,source):
            with checkpoint_lock:
                out=commit_result(r,purpose="sync.ingest",progress=lambda stage:pulse(f"ingesting {source} {stage.removeprefix('sync.ingest ')}"))
                (known.update({c["id"]:(u.replace(tzinfo=timezone.utc).timestamp() if (u:=ts_any(json.loads(c["metadata"]).get("remote_update_time"))) else None) for c in r.convs if c["source"]=="chatgpt"}),repair_attempted.update(c["id"] for c in r.convs if c["id"] in repair_order))
                return (*out,getattr(r,"provenance_edits",set()),getattr(r,"provenance_conversations",set()))
        with _core(read_only=True,purpose="sync.chatgpt.repair.plan") as conn: repairs=conn.execute("SELECT c.id,MIN(m.created_at),MAX(m.created_at) FROM conversations c JOIN messages m ON m.conversation_id=c.id WHERE c.source='chatgpt' AND (c.created_at IS NULL OR c.updated_at IS NULL) AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) GROUP BY c.id").fetchall()
        if repairs:
            with _core(purpose="sync.chatgpt.repair.write") as conn,_transaction(conn),preserve_fact_heads(conn,[("conversations",cid) for cid,_,_ in repairs],observed=True): (conn.executemany("UPDATE conversations SET created_at=COALESCE(created_at,?),updated_at=COALESCE(updated_at,?) WHERE id=?",[(first,last,cid) for cid,first,last in repairs]),_archive_touch(conn,[("conversations",cid) for cid,_,_ in repairs]))
        with _core(read_only=True,purpose="sync.plan") as conn: cur,bindings,rows,candidates=counts_by_source(conn),session_bindings(conn),conn.execute(f"SELECT c.id,c.updated_at,json_extract_string(c.metadata,'$.remote_update_time'),json_extract_string(c.metadata,'$.remote_complete'),(SELECT role FROM messages m WHERE m.conversation_id=c.id ORDER BY {MESSAGE_ORDER_DESC} LIMIT 1) FROM conversations c WHERE source='chatgpt'").fetchall(),{r[0] for r in conn.execute("SELECT DISTINCT m.conversation_id FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.source='chatgpt' AND json_extract_string(m.metadata,'$.provider_index') IS NULL QUALIFY count(*) OVER (PARTITION BY m.conversation_id,m.created_at)>1").fetchall()}
        prior_order,updated,repair_order,known,legacy,fmt,start=(prior_order:=web.get("chatgpt",{}).get("order_repairs",{})),(updated:={cid:v.replace(tzinfo=timezone.utc).timestamp() if (v:=ts_any(raw)) else ts.timestamp() if ts else None for cid,ts,raw,_,_ in rows}),(repair_order:={cid for cid in candidates if prior_order.get(cid)!=updated[cid]}),{cid:None if cid in repair_order or (complete=="false" or complete is None and role=="tool") and (v:=ts_any(raw) or (ts.astimezone(timezone.utc).replace(tzinfo=None) if ts else None)) and (datetime.now(timezone.utc).replace(tzinfo=None)-v).total_seconds()<900 else updated[cid] for cid,ts,raw,complete,role in rows},{cid for cid,_,raw,_,_ in rows if raw is None},(fmt:=lambda v:f"{v[0]} convs, {v[1]} msgs, {v[2]} tools, {v[3]} attachs, {v[4]} edits"),lambda label,src=None:typer.echo(f"Syncing {label}" if not src else f"Syncing {label} ({fmt(cur.setdefault(src,[0]*5))})")
        def schedule(job): jobs.extend([job] if job else [])
        if paths := [Path(p).expanduser() for p in os.environ.get("CONVOS_IMPORT_PATHS", "").split(",") if p.strip()]:
            jobs+=start("imports") or [j for p in paths if (j:=plan_import(p))]
        for name,label,enabled,p,parser in (("claude-code","Claude Code",claude_code,Path(os.environ.get("CLAUDE_CONFIG_DIR",Path.home()/".claude"))/"projects",parse_claude_code),("codex","Codex",codex,Path(os.environ.get("CODEX_HOME",Path.home()/".codex")),parse_codex)):
            if enabled: start(label,name) or schedule(plan_local(name,p,parser,bindings,lambda r,name=name:checkpoint(r,name),pulse))
        if not offline:
            start("ChatGPT"+(f", provider order={len(candidates)-len(repair_order)} attempted/{len(candidates)} unresolved" if candidates else ""),"chatgpt")
            schedule(plan_web("chatgpt",fetch_chatgpt,probe_chatgpt,{} if full else known,lambda r:checkpoint(r,"chatgpt"),legacy,not repair_order))
            start("Claude","claude")
            schedule(plan_web("claude",fetch_claude,probe_claude))
        verbose and typer.echo(f"Planning took {time.perf_counter()-t0:.2f}s")
        if jobs:
            with ThreadPoolExecutor(max_workers=min(4, len(jobs))) as ex:
                futs = {ex.submit(j["func"]): {**j, "t": time.perf_counter()} for j in jobs}
                for fut in as_completed(futs):
                    try: r=(pulse(f"receiving {(j:=futs[fut])['name']}"),fut.result())[-1]
                    except Exception as e: r=typer.echo(f"{j['name']} failed: {e}")
                    if r is None or j.get("failed"): failed_sources.add(j["name"])
                    if saved := j.get("saved"): c,m,t,a,e,n,u,changed_ids,provenance_edits,provenance_conversations=(*[sum(s[i] for s in saved) for i in range(7)],set().union(*(s[7] for s in saved)),provenance_edits|set().union(*(s[8] for s in saved)),provenance_conversations|set().union(*(s[9] for s in saved)))
                    elif r is not None: c,m,t,a,e,n,u,changed_ids,*_=checkpoint(r,j["name"])
                    else: continue
                    total,newc,updc,changed=[total[i]+v for i,v in enumerate([c,m,t,a,e])],newc+n,updc+u,changed|changed_ids
                    if r is not None: provenance_edits,provenance_conversations=provenance_edits|getattr(r,"provenance_edits",set()),provenance_conversations|getattr(r,"provenance_conversations",set())
                    if r is not None and (st:=j.get("state")): (j["name"]=="chatgpt" and st[2].update(coverage=sorted(known),order_repairs={cid:known[cid] for cid in ({cid for cid in candidates if prior_order.get(cid)==updated[cid]}|repair_attempted)}),set_state(*st))
                    if j.get("source"): typer.echo(f"Updated {j['label']} ({n} new, {u} updated convs; {fmt([c, m, t, a, e])} changed){' before failure' if r is None else ''}{' in %.2fs' % (time.perf_counter()-j['t']) if verbose else ''}")
        if before!=json.dumps(state,sort_keys=True): atomic_json(STATE_PATH,state)
        (pulse("capturing provenance"),capture_provenance(strict=False) if full else capture_provenance(edit_ids=provenance_edits,conversation_ids=provenance_conversations,strict=False))
        with _core(read_only=True,purpose="sync.provenance.progress") as conn: pending=conn.execute("SELECT count(*) FROM provenance.pending").fetchone()[0]
        if pending: (typer.echo(f"Provenance: {pending} pending; conversations committed, enrichment continues in the background."),wake_hooks())
        if failed_sources: raise click.ClickException(f"Sync incomplete: {', '.join(sorted(failed_sources))}; completed import checkpoints retained.")
        verbose and typer.echo(f"Total sync time {time.perf_counter()-t0:.2f}s")
        return total, newc, updc
    def do_sync():
        try: return _sync_leader(run_sync,full is True)
        except LockBusy as error: raise typer.Exit(typer.echo(str(error),err=True) or 1)
    if watch: typer.echo(f"Daemon mode (interval: {interval}s)")
    while watch:
        r,n,u=do_sync()
        typer.echo(f"[{datetime.now().isoformat()}] {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits")
        time.sleep(interval)
    r,n,u=do_sync()
    typer.echo(f"Updated {n} new, {u} updated convs; {r[1]} msgs, {r[2]} tools, {r[3]} attachs, {r[4]} edits changed")
    with _core(read_only=True,purpose="sync.summary") as conn: total=[conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("conversations","messages","tool_calls","attachments","file_edits")]
    typer.echo(f"Total: {', '.join(f'{n} {label}' for n,label in zip(total,('convs','msgs','tools','attachs','edits')))}")

def sql(query: str, fmt: str = typer.Option("text", "-f", "--format")):
    drain_hooks()
    if (conn := _ro()) is None: return
    try:
        with contextlib.closing(conn): cols,rows=(cur:=conn.execute(query)) and [d[0] for d in cur.description],cur.fetchall()
    except Exception as e: raise typer.Exit(typer.echo(f"Query failed: {e}",err=True) or 1)
    if fmt!="text": return emit([dict(zip(cols,row)) for row in rows],fmt)
    typer.echo("\n".join([" | ".join(cols),*(" | ".join("" if value is None else str(value) for value in row) for row in rows),f"\n{len(rows)} rows"]))

for _fn,_name,_hidden in ((capture,"hook",True),(capture,"capture",True),(drain_hooks_cmd,"drain-hooks",True),(init,None,False),(fts,None,False),(search,None,False),(read_cmd,"read",False),(query_cmd,"query",False),(embed_cmd,"embed",False),(doctor,None,False),(install_skills,None,False),(install_hooks,"install-hooks",False),(export,None,False),(backup,None,False),(sync,None,False),(sql,None,False)): app.command(_name,hidden=_hidden)(_fn)
for _ep in entry_points(group="convos.commands"):
    try: _ep.load()(app)
    except Exception as _e: typer.echo(f"plugin {_ep.name} failed: {_e}", err=True)  # a broken plugin must not kill the CLI

if __name__ == "__main__": app()
