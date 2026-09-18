"""Portable record/event projection. The immutable relay ledger can rebuild every local view."""
import contextlib, duckdb, hashlib, itertools, json, os, re, shutil, sqlite3, time
from datetime import date, datetime
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path

from ai_convos.cli import ARCHIVE_COLUMNS as COLUMNS, ARCHIVE_FKS as FKS, PROVENANCE_KINDS as PROVENANCE, _insert_pages, _migration_backup, _transaction, archive_relationships, archive_yield, captured_edit_paths, gen_id, index_attachment_body, init_schema, matching_logical_row, open_db, operation_lock, preserve_fact_heads, project_attested_rows, project_edit_dependencies, project_file_edit_evidence_many, project_logical_rows, project_provenance, project_provider_bindings, project_row_proofs, project_workspace_controls, provider_session_key, provenance_records, record_local_row_bases, required, retire_row_bodies, set_attachment_path, typed_logical_rows, wake_hooks
from .control import verify_state
from .migrations import migrate_state
from .protocol import _seal, canon, digest, fingerprint, logical_fact, logical_row, replica_compression, row_proof, row_signing_key, seal_blob, seal_replica, semantic_proof, verify_row_proof, verify_row_proof_header, verify_semantic_proof

STATE_VERSION,ALIAS_VERSION,ALIAS_WRITE_PAGE="4",15,250
REPLICA_VERSIONS=(16,15)  # Schema 16 only rebuilds derived edit lineage locally; projection changes must invalidate these epochs.
STATE = """
CREATE TABLE IF NOT EXISTS outbox(workspace TEXT,event TEXT,entity TEXT,revision TEXT,author TEXT,seq INT,epoch INT,kind TEXT,payload_v INT,status TEXT,path TEXT,size INT,PRIMARY KEY(workspace,event)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS receipts(workspace TEXT,event TEXT,cursor INT,author TEXT,seq INT,epoch INT,kind TEXT,payload_v INT,entity TEXT,revision TEXT,status TEXT,PRIMARY KEY(workspace,event)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS receipt_cursor ON receipts(workspace,cursor);
CREATE TABLE IF NOT EXISTS publication_heads(workspace TEXT,owner TEXT,entity TEXT,revision TEXT,event TEXT,PRIMARY KEY(workspace,owner,entity)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cursors(workspace TEXT PRIMARY KEY,cursor INT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS lazy_events(workspace TEXT,event TEXT,cursor INT,size INT,PRIMARY KEY(workspace,event)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS deferred_events(workspace TEXT,event TEXT,cursor INT,kind TEXT,payload_v INT,required INT,PRIMARY KEY(workspace,event)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS event_sequences(workspace TEXT,author TEXT,seq INT,event TEXT,PRIMARY KEY(workspace,author,seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sequence_gaps(workspace TEXT,author TEXT,seq INT,parents TEXT,PRIMARY KEY(workspace,author,seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS replica_receipts(workspace TEXT,replica TEXT,epoch INT,cursor INT,PRIMARY KEY(workspace,replica,epoch)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS blob_outbox(workspace TEXT,blob TEXT,epoch INT,uploader TEXT,path TEXT,size INT,PRIMARY KEY(workspace,blob,epoch,uploader)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS blob_receipts(workspace TEXT,blob TEXT,epoch INT,cursor INT,PRIMARY KEY(workspace,blob,epoch)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS origin_bindings(workspace TEXT,origin TEXT,bundle TEXT,epoch INT,cursor INT,PRIMARY KEY(workspace,origin)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS control_dependencies(workspace TEXT,origin TEXT,bundle TEXT,epoch INT,cursor INT,PRIMARY KEY(workspace,origin)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS policies(workspace TEXT,owner TEXT,kind TEXT,value TEXT,evidence TEXT,PRIMARY KEY(workspace,owner,kind,value)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS policy_proofs(workspace TEXT,owner TEXT,value TEXT,proof TEXT,PRIMARY KEY(workspace,owner,value)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sharing_preferences(workspace TEXT,user TEXT,revision TEXT,auto_contribute INT,match TEXT,proof TEXT,PRIMARY KEY(workspace,user,revision)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS team_scopes(workspace TEXT,conversation TEXT,PRIMARY KEY(workspace,conversation)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sync_states(workspace TEXT PRIMARY KEY,lifecycle TEXT NOT NULL,tail INT NOT NULL DEFAULT 0,floor INT NOT NULL DEFAULT 0,error TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID;
"""
STATE_TABLES={"outbox","receipts","publication_heads","cursors","lazy_events","deferred_events","event_sequences","sequence_gaps","replica_receipts","blob_outbox","blob_receipts","origin_bindings","control_dependencies","policies","policy_proofs","sharing_preferences","team_scopes","sync_states","meta"}
STATE_FORBIDDEN={"published","event_log","history_material","history_outbox","history_queue","attachment_chunks","imported_rows","raw_events","repositories","files","file_versions","changesets","edits","checkpoints","assertions","gaps","boundaries","sharing_boundaries"}
TABLES={"conversation.record":"conversations","message.record":"messages","tool.record":"tool_calls","attachment.record":"attachments","artifact.record":"artifacts","file_edit.record":"file_edits"}
CORE_EVENTS,SIGNED={(kind,1) for kind in {"workspace.policy","workspace.preference","workspace.membership","workspace.device"}}|{("workspace.policy",2)},set(TABLES)|PROVENANCE
PROOF_FIELDS=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
TEXT_IDS="SELECT json_extract_string(value,'$') FROM json_each(?)"
def packed(values): return json.dumps(list(values),separators=(",",":"))

def _connect(path,journal="WAL"):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path)
    os.chmod(path,0o600)
    db.row_factory=sqlite3.Row
    db.executescript(f"PRAGMA journal_mode={journal};PRAGMA secure_delete=ON;"+STATE)
    return db
def _fsync(path):
    fd=os.open(path,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
def inspect_state(path,verify=False):
    path=Path(path)
    base={"path":str(path),"bytes":path.stat().st_size if path.exists() and path.is_file() else 0,"version":None}
    if not path.exists(): return base|{"status":"absent"}
    if path.is_symlink() or not path.is_file(): return base|{"status":"invalid","error":"state path is not a regular file"}
    try:
        with contextlib.closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            integrity=db.execute("PRAGMA quick_check").fetchone()[0] if verify else "ok"
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            try: version=(db.execute("SELECT value FROM meta WHERE key='state_schema'").fetchone() or [None])[0]
            except sqlite3.Error: version=None
            status="current" if version==STATE_VERSION and STATE_TABLES<=tables and not STATE_FORBIDDEN&tables and integrity=="ok" else "invalid" if version==STATE_VERSION or integrity!="ok" else "incompatible"
            return base|{"status":status,"version":version,"error":None if status!="invalid" else "schema or integrity check failed"}
    except sqlite3.Error as e: return base|{"status":"invalid","error":str(e)}
def read_state(path):
    info=inspect_state(path)
    if info["status"]!="current": raise ValueError(f"remote state is {info['status']}")
    db=sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True)
    db.row_factory=sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db
def cutover_state(path):
    path,info=(path:=Path(path)),inspect_state(path,True)
    if info["status"] not in ("incompatible","invalid") or path.is_symlink() or not path.is_file(): raise ValueError(f"remote state cannot be rebuilt ({info['status']})")
    backups=path.parent/"backups"
    if backups.is_symlink(): raise ValueError("remote state backup directory must not be a symlink")
    backups.mkdir(parents=True,exist_ok=True)
    os.chmod(backups,0o700)
    name=f"state-{info['version'] or 'legacy'}-{time.time_ns()}"
    stage,target,fresh=backups/f".{name}.{os.getpid()}",backups/name,path.with_name(f".{path.name}.v{STATE_VERSION}.{os.getpid()}.{time.time_ns()}")
    stage.mkdir(mode=0o700)
    files=[p for p in (path,Path(str(path)+"-wal"),Path(str(path)+"-shm")) if p.exists()]
    saved={}
    try:
        if info["status"]=="incompatible":
            copy=stage/path.name
            with contextlib.closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)) as source,contextlib.closing(sqlite3.connect(copy)) as destination: source.backup(destination)
            os.chmod(copy,0o600)
            saved[path.name]={"bytes":copy.stat().st_size,"sha256":file_hash(copy)}
            migration_source,staged=copy,[copy]
        else:
            for source in files:
                if source.is_symlink() or not source.is_file(): raise ValueError("remote state backup source must be a regular file")
                copy=stage/source.name
                shutil.copyfile(source,copy)
                os.chmod(copy,0o600)
                saved[source.name]={"bytes":copy.stat().st_size,"sha256":file_hash(copy)}
                if file_hash(source)!=saved[source.name]["sha256"]: raise ValueError("remote state backup verification failed")
            migration_source,staged=stage/path.name,[stage/p.name for p in files]
        report={"from":info["version"] or "legacy","to":int(STATE_VERSION),"backup":str(target),"files":saved}
        manifest=stage/"manifest.json"
        manifest.write_text(json.dumps(report,sort_keys=True,indent=2))
        os.chmod(manifest,0o600)
        [_fsync(p) for p in [*staged,manifest]]
        _fsync(stage)
        new=_connect(fresh,"DELETE")
        try:
            migrate=migrate_state(migration_source,new,info["version"])
            new.execute("INSERT INTO meta VALUES ('state_schema',?),('state_cutover',?)",(STATE_VERSION,json.dumps({**report,"preserved":migrate},sort_keys=True)))
            new.commit()
            valid=new.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        finally: new.close()
        if not valid: raise ValueError("fresh remote state validation failed")
        os.replace(stage,target)
        _fsync(backups)
        [p.unlink(missing_ok=True) for p in (Path(str(path)+"-wal"),Path(str(path)+"-shm"))]
        os.replace(fresh,path)
        os.chmod(path,0o600)
        _fsync(path)
        _fsync(path.parent)
        return report
    except BaseException:
        fresh.unlink(missing_ok=True)
        Path(str(fresh)+"-journal").unlink(missing_ok=True)
        if stage.exists(): shutil.rmtree(stage)
        raise
def connect(path):
    path,info=(path:=Path(path)),inspect_state(path)
    if info["status"]=="incompatible": raise ValueError(f"remote state rebuild required ({info['version'] or 'legacy'} -> {STATE_VERSION}); run `convos remote sync`")
    if info["status"]=="invalid": raise ValueError(f"invalid remote state: {info['error']}")
    db=_connect(path)
    if info["status"]=="absent":
        db.execute("INSERT INTO meta VALUES ('state_schema',?)",(STATE_VERSION,))
        db.commit()
    return db
@lru_cache(maxsize=1)
def bridges():
    if (result:=[entry.load()() for entry in entry_points(group="convos.remote")]) and any(not {"v","schema","objects","records","accept"}<=set(b)<={"v","schema","objects","records","accept","accept_many","delta","source"} or b["v"]!=3 or isinstance(b["v"],bool) or not isinstance(b["schema"],int) or isinstance(b["schema"],bool) or b["schema"]<1 or b.get("source") not in (None,"archive") or any(not callable(b[k]) for k in ("records","accept")+tuple(k for k in ("accept_many","delta") if k in b)) or not isinstance(b["objects"],set) or not b["objects"] or any(not isinstance(v,str) or not v for v in b["objects"]) for b in result) or len([v for b in result for v in b["objects"]])!=len({v for b in result for v in b["objects"]}): raise ValueError("Unsupported remote bridge")
    return result
def control_chain(controls):
    ordered,previous=sorted(controls,key=lambda c:c["revision"]),None
    if not ordered or [c["revision"] for c in ordered]!=list(range(1,len(ordered)+1)) or len({c["workspace"] for c in ordered})!=1: raise ValueError("invalid origin control chain")
    for value in ordered:
        verify_state(value,previous)
        previous=value
    return ordered
def stored_controls(db_path,origins):
    if not origins or not Path(db_path).is_file(): return []
    with open_db(db_path,True,purpose="remote.controls.read") as db: return [json.loads(r[0]) for r in db.execute(f"SELECT CAST(control AS VARCHAR) FROM remote.workspace_controls WHERE workspace_id IN ({','.join('?'*len(origins))}) ORDER BY workspace_id,revision",list(origins)).fetchall()]
def audit_rows(db_path,page=5000,progress=None,local_user=None,on_unavailable=None):
    # Capture stays durable in the inbox; page readers still release DuckDB for unrelated work.
    with operation_lock(Path(db_path).parent/".sync.lock","remote.audit.local",30) as local,operation_lock(Path(db_path).parent/"hook_inbox/.drain.lock","remote.audit.capture",30) as hooks:
        return _audit_rows(db_path,page,lambda stage:(local(stage),hooks(stage),progress and progress(stage)),local_user,on_unavailable)
def _audit_rows(db_path,page=5000,progress=None,local_user=None,on_unavailable=None):
    sql="SELECT * FROM (SELECT o.table_name kind,o.physical_row_id physical,o.source_row_id,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id UNION ALL SELECT o.kind,o.physical_entity,o.source_entity,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.provenance_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id) WHERE kind>? OR kind=? AND (physical>? OR physical=? AND COALESCE(proof_id,'')>?) ORDER BY kind,physical,COALESCE(proof_id,'') LIMIT ?"
    origins,after,generation=[],("","",""),None
    while True:
        with contextlib.closing(open_db(db_path,True,purpose="remote.audit.rows")) as db:
            current,generation=(current:=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]),current if generation is None else (required(current==generation,RuntimeError("Archive changed during Remote audit; retry")),generation)[1]
            rows=db.execute(sql,(after[0],after[0],after[1],after[1],after[2],page)).fetchall()
        if not rows: break
        origins,after=origins+rows,(rows[-1][0],rows[-1][1],rows[-1][4] or "")
        (progress and progress(f"audit inventory {len(origins)}"),archive_yield(db_path))
    if local_user is not None:
        cursor,seen="",{r[4] for r in origins}
        while True:
            with contextlib.closing(open_db(db_path,True,purpose="remote.audit.requirements")) as db: heads=db.execute("SELECT p.row_kind,p.source_row_id,p.author_user_id,p.id,p.content_hash,p.state FROM remote.row_proofs p WHERE p.id>? AND NOT EXISTS(SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision) ORDER BY p.id LIMIT ?",[cursor,page]).fetchall()
            if not heads: break
            origins += [(kind,source if user==local_user or kind in PROVENANCE-{'edit.observed','checkpoint.link'} else foreign_id(user,"file_edits" if kind=="edit.observed" else kind,source),source,user,pid,expected,state) for kind,source,user,pid,expected,state in heads if pid not in seen]
            cursor=heads[-1][3]
            archive_yield(db_path)
    tables,examples={},[]
    for at in range(0,len(origins),page):
        batch=origins[at:at+page]
        with contextlib.closing(open_db(db_path,True,purpose="remote.audit.rows")) as db:
            required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during Remote audit; retry"))
            found=typed_logical_rows(db,[(kind,physical,source,user,state) for kind,physical,source,user,pid,expected,state in batch])
            retained,paths={pid:json.loads(body) for pid,body in db.execute("SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))",[[r[4] for r in batch]]).fetchall()},captured_edit_paths(db,[r[1] for r in batch if r[0]=="file_edits"])
        for kind,physical,source,user,pid,expected,state in batch:
            row=found[(kind,physical,source,user,state)]
            projection=row is not None and expected is not None and matching_logical_row(row,expected,[paths[physical]] if kind=="file_edits" and physical in paths else ()) is not None
            kept=pid in retained and digest(retained[pid])==expected
            stat=tables.setdefault(kind,dict(origins=0,projection_match=0,projection_mismatch=0,projection_missing=0,proof_missing=0,retained_variants=0,unavailable=0))
            stat["retained_variants"]+=int(kept and not projection)
            stat["unavailable"]+=int(not projection and not kept)
            if not projection and not kept and on_unavailable: on_unavailable(dict(kind=kind,physical=physical,source=source,author=user,proof=pid,expected=expected,state=state))
            for key,value in (("origins",1),("proof_missing",expected is None),("projection_missing",row is None),("projection_match",projection),("projection_mismatch",row is not None and expected is not None and not projection)): stat[key]+=value
            if len(examples)<20 and not projection: examples.append(dict(kind=kind,id=source,projection="missing" if row is None else "mismatch"))
        (progress and progress(f"audit rows {min(at+page,len(origins))}"),archive_yield(db_path))
    with contextlib.closing(open_db(db_path,True,purpose="remote.audit.relationships")) as db:
        required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during Remote audit; retry"))
        relationships=archive_relationships(db)
    return (lambda keys:dict(totals={key:sum(value[key] for value in tables.values()) for key in keys},tables=tables,examples=examples,relationships=relationships,archive_generation=generation))(next(iter(tables.values())).keys() if tables else ())
def event_support(value):
    if not isinstance(kind:=value["kind"],str) or not isinstance(version:=value["payload_v"],int) or isinstance(version,bool) or version<1: raise ValueError("invalid event schema")
    return "supported" if (kind,version) in CORE_EVENTS else "required"
def bridge_records(root,cfg,workspace,kind,archive=True,changes=None): return [record for bridge in bridges() if archive or bridge.get("source")!="archive" for record in (bridge["delta"](root,cfg["user"],workspace,kind,changes) if changes is not None and "delta" in bridge else bridge["records"](root,cfg["user"],workspace,kind))]
def bridge_stamps(root): return tuple(digest({"core":version,"bridges":{kind:(bridge["v"],bridge["schema"]) for bridge in bridges() for kind in bridge["objects"]}}) for version in REPLICA_VERSIONS)
def bridge_stamp(root): return bridge_stamps(root)[0]
def bridge_state(root,cfg,workspace,kind,generation): return digest((generation,bridge_stamp(root),bridge_records(root,cfg,workspace,kind,False)))
def bridge_accept_many(root,values,project=True):
    groups=[(bridge,selected) for bridge in bridges() if (selected:=[(i,value) for i,value in enumerate(values) if value[0]["kind"] in bridge["objects"]])]
    batches=[(selected,bridge["accept_many"](root,[value for _,value in selected],project) if "accept_many" in bridge else [bridge["accept"](root,*value,project) for _,value in selected]) for bridge,selected in groups]
    mapped=(required(all(len(selected)==len(answers) for selected,answers in batches),ValueError("Remote bridge batch result mismatch")),{i:bool(answer) for selected,answers in batches for (i,_),answer in zip(selected,answers)})[-1]
    return [mapped.get(i,False) for i in range(len(values))]
def bridge_replicas(root,cfg,workspace,kind,key_,known=(),inventory=None,archive=True,changes=None):
    values=bridge_records(root,cfg,workspace,kind,archive,changes)
    fresh=[(value["row"],value["proof"]) for value in values if value["proof"] is None and not value.__setitem__("proof",semantic_proof(cfg["root"],cfg["user"],cfg["device"]["id"],workspace,cfg["workspaces"][workspace]["epoch"],value["row"],value["previous"]))]
    bridge_accept_many(root,fresh,False)
    values=[(value,fingerprint(key_,digest(value["proof"]))) for value in values if value["proof"]]
    present=set(inventory([(replica,cfg["workspaces"][workspace]["epoch"]) for value,replica in values])) if inventory else set(known)
    return [seal_replica(value["row"],value["proof"],workspace,cfg["workspaces"][workspace]["epoch"],key_,cfg["device"]["id"],compression=replica_compression(cfg)) for value,replica in values if replica not in present]
def clean(v):
    if isinstance(v,datetime): return v.isoformat()
    if isinstance(v,date): return v.isoformat()
    if isinstance(v,dict): return {k:clean(x) for k,x in v.items()}
    if isinstance(v,(list,tuple)): return [clean(x) for x in v]
    return v
def file_hash(path):
    with Path(path).open("rb") as source: return hashlib.file_digest(source,"sha256").hexdigest()
def relocate_attachments(db_path,remote_root):
    db_path,remote_root=Path(db_path),Path(remote_root)
    if not db_path.is_file() or not remote_root.exists(): return 0
    if remote_root.is_symlink() or not remote_root.is_dir(): raise ValueError("legacy attachment root must be a regular directory")
    with contextlib.closing(open_db(db_path,True,purpose="remote.attachments.plan")) as db: rows=db.execute("SELECT id,path,size FROM attachments WHERE path IS NOT NULL").fetchall()
    moved,target_root=[],db_path.parent/"attachments"
    for row_id,value,size in rows:
        source=Path(value)
        if not source.is_absolute(): continue
        if not source.is_relative_to(remote_root.absolute()): continue
        if source.is_symlink() or not source.is_file() or source.resolve()!=source or size is not None and source.stat().st_size!=size: raise ValueError("legacy attachment body is unsafe or inconsistent")
        if target_root.is_symlink(): raise ValueError("archive attachment root must not be a symlink")
        target_root.mkdir(parents=True,exist_ok=True)
        os.chmod(target_root,0o700)
        blob=file_hash(source)
        target=target_root/blob
        if target.exists():
            if target.is_symlink() or not target.is_file() or target.stat().st_size!=source.stat().st_size or file_hash(target)!=blob: raise ValueError("archive attachment body conflicts")
        else:
            tmp=target.with_name(f".{target.name}.{os.getpid()}")
            shutil.copyfile(source,tmp)
            os.chmod(tmp,0o600)
            _fsync(tmp)
            os.replace(tmp,target)
            _fsync(target_root)
        os.chmod(target,0o600)
        moved.append((row_id,source,target,blob,source.stat().st_size,size))
    if moved:
        with contextlib.closing(open_db(db_path,purpose="remote.attachments.write")) as db,_transaction(db):
            current={r[0]:r[1:] for r in db.execute("SELECT id,path,size FROM attachments WHERE id IN (SELECT UNNEST(?))",[[r[0] for r in moved]]).fetchall()}
            required(all(current.get(row_id)==(str(source),size) for row_id,source,target,blob,actual,size in moved),RuntimeError("Archive changed during attachment relocation; retry"))
            [(set_attachment_path(db,row_id,target),index_attachment_body(db,row_id,target,size,(blob,actual))) for row_id,source,target,blob,actual,size in moved]
    [source.unlink(missing_ok=True) for row_id,source,target,blob,actual,size in moved if source!=target]
    return len(moved)
def _records(core,state,blobs=True,changes=None):
    out=[]
    for kind,table in TABLES.items():
        wanted=[entity for changed,entity in changes or () if changed==table]
        if changes is not None and not wanted: continue
        target,imported=(target:="a.id" if table=="attachments" else "x.id"),{r[0] for r in core.execute(f"SELECT physical_row_id FROM remote.row_origins WHERE table_name=? AND physical_row_id IN ({TEXT_IDS}) UNION SELECT physical FROM parser_retired_rows WHERE kind=? AND physical IN ({TEXT_IDS})",(table,packed(wanted),table,packed(wanted))).fetchall()} if changes is not None else set()
        where=" WHERE "+(f"{target} IN ({TEXT_IDS}) AND " if changes is not None else "")+f"NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='{table}' AND o.physical_row_id={target})"
        cur=core.execute(("SELECT x.* EXCLUDE (embedding) FROM messages x" if table=="messages" else "SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id" if table=="attachments" else f"SELECT x.* FROM {table} x")+where,[packed(wanted)] if changes is not None else [])
        cols=[d[0] for d in cur.description]
        records=[dict(kind=kind,entity=f"{table}:{row['id']}",payload=dict(table=table,columns=cols,row=[row[c] for c in cols])) for values in cur.fetchall() for row in [dict(zip(cols,map(clean,values)))|({"cwd":None} if table=="conversations" else {})|({"path":None} if table=="attachments" else {})]]
        found={r["payload"]["row"][0] for r in records}
        out += records+[dict(kind=kind,entity=f"{table}:{row_id}",payload=dict(table=table,state="deleted",id=row_id)) for row_id in wanted if row_id not in found and row_id not in imported]
    return out
def signed_row(record): return logical_row(p["table"],identity=p["id"],state="deleted") if (p:=record["payload"]).get("state")=="deleted" else logical_row(p["table"],p["columns"],p["row"])
def _logical_record(record,aliases):
    if record["kind"] not in TABLES or (payload:=record["payload"]).get("state")=="deleted": return record
    table=payload["table"]
    parents=dict(FKS.get(table,()))
    return {**record,"payload":{**payload,"row":[aliases.get((parents[column],value),value) if column in parents else value for column,value in zip(payload["columns"],payload["row"])]}}
def logical_records(db,records,author):
    aliases={(table,physical):source for table,physical,source in db.execute(f"SELECT table_name,physical_row_id,source_row_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN ({TEXT_IDS})",(author,packed(refs))).fetchall()} if (refs:={value for record in records if record["kind"] in TABLES and record["payload"].get("state")!="deleted" for column,value in zip(record["payload"]["columns"],record["payload"]["row"]) if column in dict(FKS.get(record["payload"]["table"],())) and value is not None}) else {}
    return [_logical_record(record,aliases) for record in records]
def sharing(state,workspace,user):
    rows=state.execute("SELECT revision,auto_contribute,match,proof FROM sharing_preferences WHERE workspace=? AND user=?",(workspace,user)).fetchall()
    proofs=[json.loads(r[3]) for r in rows]
    ancestors={a for p in proofs for a in p["ancestors"]}
    leaves=[(r,p) for r,p in zip(rows,proofs) if r[0] not in ancestors]
    values=[dict(auto_contribute=None if r[1] is None else bool(r[1]),match=json.loads(r[2])) for r,p in leaves] or [dict(auto_contribute=None,match=["cwd","edit"])]
    autos=[v["auto_contribute"] for v in values]
    match=[m for m in ("cwd","edit") if all(m in v["match"] for v in values)]
    configured=False if False in autos else True if autos and all(v is True for v in autos) else None
    return dict(auto_contribute=configured,effective_auto_contribute=configured is not False,match=match,proofs=[p for r,p in leaves],conflict=len(leaves)>1)
def sharing_object(state,workspace,row,proof,authors):
    user=proof["author_user_id"]
    verify_semantic_proof(proof,row,user)
    if proof["workspace"]!=workspace or authors.get(proof["author_device_id"])!=user: raise ValueError("sharing proof authorization mismatch")
    data=row["data"]
    if row["kind"]=="sharing.preference":
        match=data["match"]
        if row["id"]!=f"sharing:{workspace}:{user}" or row["state"]!="active" or set(data)!={"auto_contribute","match"} or data["auto_contribute"] is not None and not isinstance(data["auto_contribute"],bool) or not isinstance(match,list) or match!=[m for m in ("cwd","edit") if m in match]: raise ValueError("invalid sharing preference")
        state.execute("INSERT OR REPLACE INTO sharing_preferences VALUES (?,?,?,?,?,?)",(workspace,user,proof["revision"],data["auto_contribute"],json.dumps(match),json.dumps(proof)))
    elif row["kind"]=="repository.policy":
        evidence=data["evidence"]
        if row["id"]!=f"repository:{workspace}:{data['value']}" or row["state"]!="active" or set(data)!={"value","evidence"} or not isinstance(data["value"],str) or set(evidence)!={"lineage","remotes"} or not isinstance(evidence["lineage"],str) or not isinstance(evidence["remotes"],list) or not all(isinstance(r,str) for r in evidence["remotes"]): raise ValueError("invalid repository policy")
        old=state.execute("SELECT evidence FROM policies WHERE workspace=? AND owner=? AND kind='repository' AND value=?",(workspace,user,data["value"])).fetchone()
        encoded=json.dumps(evidence,sort_keys=True)
        if old and old[0] is not None and json.loads(old[0])!=evidence: raise ValueError("repository policy evidence conflict")
        state.execute("INSERT OR REPLACE INTO policies VALUES (?,?,?,?,?)",(workspace,user,"repository",data["value"],encoded))
        state.execute("INSERT OR REPLACE INTO policy_proofs VALUES (?,?,?,?)",(workspace,user,data["value"],json.dumps(proof)))
    else: raise ValueError("invalid sharing object")
    state.execute("DELETE FROM meta WHERE key=?",(f"core_generation:{workspace}",))
    return True
def _team_scope(core,provenance,repositories,roots,candidates=None,match=("cwd","edit")):
    roots=[Path(p).expanduser() for p in roots]
    where=f" WHERE c.id IN ({TEXT_IDS})" if candidates is not None else ""
    args=[packed(candidates)] if candidates is not None else []
    rows=core.execute("SELECT s.route,m.conversation_id,s.repository FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN provenance.file_edit_scopes s ON s.file_edit_id=fe.id JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id"+where,args).fetchall()
    cwd_rows=core.execute("SELECT c.id,s.cwd,s.repository FROM conversations c JOIN provenance.conversation_scopes s ON s.conversation=c.id"+where,args).fetchall()
    return ({cid for route,cid,repo in rows if repo in repositories or route and any(Path(route).is_relative_to(root) for root in roots)} if "edit" in match else set())|({cid for cid,cwd,repo in cwd_rows if repo in repositories or cwd and any(Path(cwd).is_relative_to(root) for root in roots)} if "cwd" in match else set())
def scan(core,graph,kind="personal",repositories=(),roots=(),changes=None,workspace=None,new_scope=None,match=("cwd","edit"),user=None,selected=None):
    local={tuple(r) for r in core.execute("SELECT kind,entity FROM provenance.local_facts"+(f" WHERE entity IN ({TEXT_IDS})" if facts is not None else ""),[packed(r[1] for r in facts)] if facts is not None else []).fetchall()} if (facts:=None if changes is None else {r for r in changes if r[0] in PROVENANCE}) is None or facts else set()
    all_provenance=[clean(r) for r in provenance_records(core,facts) if (r["kind"],r["entity"]) in local] if local else []
    changed={table:{entity for name,entity in changes or () if name==table} for table in TABLES.values()}
    edit_ids,file_ids,repo_ids=changed["file_edits"]|{r["payload"]["id"] if r["kind"]=="edit.observed" else r["payload"]["edit"] for r in all_provenance if r["kind"] in ("edit.observed","checkpoint.link")},{r["payload"]["id"] if r["kind"]=="file.observed" else r["payload"]["file"] for r in all_provenance if r["kind"] in ("file.observed","file.version")},{r["payload"]["id"] if r["kind"]=="repository.observed" else r["payload"]["repository"] for r in all_provenance if r["kind"] in ("repository.observed","git.checkpoint")}
    linked=core.execute(f"SELECT fe.id,m.conversation_id,x.file_id,f.repository FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN messages m ON m.id=fe.message_id LEFT JOIN provenance.file_edit_files x ON x.file_edit_id=fe.id LEFT JOIN provenance.files f ON f.id=x.file_id"+(f" WHERE fe.id IN ({TEXT_IDS}) OR x.file_id IN ({TEXT_IDS}) OR f.repository IN ({TEXT_IDS})" if changes is not None else ""),[packed(ids) for ids in (edit_ids,file_ids,repo_ids)] if changes is not None else []).fetchall() if kind=="team" and selected is None and (changes is None or edit_ids or file_ids or repo_ids) else []
    candidates=changed["conversations"]|{r[1] for r in linked}|{r[0] for r in core.execute(f"SELECT conversation_id FROM messages WHERE id IN ({TEXT_IDS}) UNION SELECT m.conversation_id FROM messages m JOIN (SELECT message_id FROM tool_calls WHERE id IN ({TEXT_IDS}) UNION SELECT message_id FROM attachments WHERE id IN ({TEXT_IDS}) UNION SELECT message_id FROM file_edits WHERE id IN ({TEXT_IDS})) x ON x.message_id=m.id UNION SELECT conversation_id FROM artifacts WHERE id IN ({TEXT_IDS})",[packed(changed[table]) for table in ("messages","tool_calls","attachments","file_edits","artifacts")]).fetchall()} if kind=="team" and changes is not None and selected is None else None
    prior={r[0] for r in graph.execute("SELECT conversation FROM team_scopes WHERE workspace=?"+(" AND conversation IN (SELECT value FROM json_each(?))" if candidates is not None else ""),(workspace,*([packed(candidates)] if candidates is not None else []))).fetchall()} if kind=="team" and workspace and changes is not None and selected is None else set()
    admitted={r[0] for r in core.execute("SELECT source_row_id FROM remote.row_proofs WHERE authorization_workspace_id=? AND author_user_id=? AND row_kind='conversations' AND state='active'"+(f" AND source_row_id IN ({TEXT_IDS})" if candidates is not None else ""),(workspace,user,*([packed(candidates)] if candidates is not None else []))).fetchall()} if kind=="team" and workspace and user and selected is None else set()
    convs=set(selected) if selected is not None else (admitted|prior|_team_scope(core,all_provenance,set(repositories),roots,candidates,match)) if kind=="team" else set()
    if workspace and new_scope is not None: new_scope.update(convs-prior)
    provenance=[r for r in all_provenance if changes is None or (r["kind"],r["entity"]) in changes]
    records=_records(core,graph,kind=="personal",changes)
    if user and records:
        pending={tuple(r) for r in core.execute("SELECT DISTINCT p.row_kind,p.source_row_id FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id LEFT JOIN remote.local_row_bases b ON (b.kind,b.entity,b.author)=(p.row_kind,p.source_row_id,p.author_user_id) WHERE p.author_user_id=? AND p.source_row_id IN (SELECT UNNEST(?)) AND b.revision IS DISTINCT FROM p.revision AND NOT EXISTS (SELECT 1 FROM remote.row_proofs n WHERE n.row_kind=p.row_kind AND n.source_row_id=p.source_row_id AND n.author_user_id=p.author_user_id AND n.previous_revision=p.revision)",[user,[r["payload"].get("id") or r["payload"]["row"][0] for r in records]]).fetchall()}
        records=[r for r in records if (r["payload"]["table"],r["payload"].get("id") or r["payload"]["row"][0]) not in pending]
    edit_paths=captured_edit_paths(core,[r["payload"]["row"][0] for r in records if r["kind"]=="file_edit.record" and r["payload"].get("state")!="deleted"])
    for r in records:
        if r["kind"]=="file_edit.record" and r["payload"].get("state")!="deleted": r["payload"]["row"][2]=edit_paths.get(r["payload"]["row"][0],r["payload"]["row"][2] if kind=="personal" else None)
    if kind=="personal" or selected is not None: return records+provenance
    keep=[]
    parents={r["payload"]["row"][1] for r in records if r["payload"]["table"] in ("tool_calls","attachments","file_edits") and r["payload"].get("state")!="deleted"}
    msg_convs=dict(core.execute("SELECT id,conversation_id FROM messages WHERE id IN (SELECT UNNEST(?))",[list(parents)]).fetchall()) if parents else {}
    edits,allowed_files,allowed_repos=({r[column] for r in linked if r[1] in convs} for column in (0,2,3))
    shared=set(core.execute(f"SELECT row_kind,source_row_id FROM remote.row_proofs WHERE authorization_workspace_id=? AND source_row_id IN ({TEXT_IDS})",(workspace,packed(deleted))).fetchall()) if workspace and (deleted:={r["payload"]["id"] for r in records if r["payload"].get("state")=="deleted"}) else set()
    for r in records:
        table,row=r["payload"]["table"],r["payload"]["row"] if "row" in r["payload"] else [r["payload"]["id"]]
        if r["payload"].get("state")=="deleted" and (table,row[0]) in shared or table=="conversations" and row[0] in convs or len(row)>1 and (table=="messages" and row[1] in convs or table in ("tool_calls","attachments") and msg_convs.get(row[1]) in convs or table=="file_edits" and row[0] in edits or table=="artifacts" and row[1] in convs): keep.append(r)
    for r in provenance:
        p,k=r["payload"],r["kind"]
        if k=="edit.observed" and p["id"] in edits or k=="file.observed" and p["id"] in allowed_files or k=="file.version" and p["file"] in allowed_files or k in ("repository.observed","git.checkpoint") and p.get("repository",p.get("id")) in allowed_repos or k=="checkpoint.link" and p["edit"] in edits: keep.append(r)
    return keep
def _team_page(core,conversations,after,page):
    convs=packed(conversations)
    query="""WITH scoped AS (SELECT json_extract_string(value,'$') id FROM json_each(?)), edits AS (SELECT fe.id,x.file_id,f.repository FROM file_edits fe JOIN messages m ON m.id=fe.message_id JOIN scoped s ON s.id=m.conversation_id JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' LEFT JOIN provenance.file_edit_files x ON x.file_edit_id=fe.id LEFT JOIN provenance.files f ON f.id=x.file_id), selected AS (SELECT 'conversations' kind,c.id entity FROM conversations c JOIN scoped s ON s.id=c.id UNION SELECT 'messages',m.id FROM messages m JOIN scoped s ON s.id=m.conversation_id UNION SELECT 'tool_calls',x.id FROM tool_calls x JOIN messages m ON m.id=x.message_id JOIN scoped s ON s.id=m.conversation_id UNION SELECT 'attachments',x.id FROM attachments x JOIN messages m ON m.id=x.message_id JOIN scoped s ON s.id=m.conversation_id UNION SELECT 'file_edits',e.id FROM edits e UNION SELECT 'artifacts',x.id FROM artifacts x JOIN scoped s ON s.id=x.conversation_id UNION SELECT 'edit.observed',l.entity FROM provenance.local_facts l JOIN edits e ON l.kind='edit.observed' AND l.entity=e.id UNION SELECT 'file.observed',l.entity FROM provenance.local_facts l JOIN edits e ON l.kind='file.observed' AND l.entity=e.file_id UNION SELECT 'file.version',l.entity FROM provenance.local_facts l JOIN provenance.file_versions v ON l.kind='file.version' AND l.entity=v.id JOIN edits e ON e.file_id=v.file_id UNION SELECT 'repository.observed',l.entity FROM provenance.local_facts l JOIN edits e ON l.kind='repository.observed' AND l.entity=e.repository UNION SELECT 'git.checkpoint',l.entity FROM provenance.local_facts l JOIN provenance.git_checkpoints c ON l.kind='git.checkpoint' AND l.entity=c.id JOIN edits e ON e.repository=c.repository UNION SELECT 'checkpoint.link',l.entity FROM provenance.local_facts l JOIN provenance.checkpoint_edits c ON l.kind='checkpoint.link' AND l.entity=sha256(json_object('checkpoint',c.checkpoint_id,'edit',c.file_edit_id)) JOIN edits e ON e.id=c.file_edit_id) SELECT kind,entity FROM selected WHERE entity IS NOT NULL AND (kind>? OR kind=? AND entity>?) ORDER BY kind,entity LIMIT ?"""
    return core.execute(query,(convs,after[0],after[0],after[1],page)).fetchall()
def scan_archive(db_path,graph,kind="personal",repositories=(),roots=(),workspace=None,new_scope=None,match=("cwd","edit"),user=None,generation=None,progress=None,page=2500,since=None):
    sources=required(generation is not None,ValueError("archive scan requires a generation watermark")) and ([*((f"SELECT '{table}' kind,id entity FROM {table} x WHERE NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='{table}' AND o.physical_row_id=x.id) AND NOT EXISTS (SELECT 1 FROM archive_changes c WHERE (c.kind,c.entity)=('{table}',x.id) AND c.generation>?)",(generation,)) for table in TABLES.values()),("SELECT kind,entity FROM provenance.local_facts x WHERE NOT EXISTS (SELECT 1 FROM archive_changes c WHERE (c.kind,c.entity)=(x.kind,x.entity) AND c.generation>?)",(generation,))] if since is None else [("SELECT kind,entity FROM archive_changes WHERE generation>? AND generation<=?",(since,generation))])
    out,done,scope=[],0,new_scope if new_scope is not None else set()
    for query,args in sources:
        after,limit=("",""),page
        while True:
            started=time.monotonic()
            with contextlib.closing(open_db(db_path,True,purpose="remote.scan.page")) as core:
                changes=core.execute(f"SELECT kind,entity FROM ({query}) WHERE kind>? OR kind=? AND entity>? ORDER BY kind,entity LIMIT ?",(*args,after[0],after[0],after[1],limit)).fetchall()
                batch=scan(core,graph,kind,repositories,roots,set(changes),workspace,scope,match,user) if changes else []
            out+=batch
            if not changes: break
            after,done,limit,_=changes[-1],done+len(changes),max(100,min(5000,int(limit*.5/max(time.monotonic()-started,.001)))),(progress and progress(f"scanning archive {done+len(changes)}"),archive_yield(db_path))
    if scope:
        after=("","")
        while True:
            with contextlib.closing(open_db(db_path,True,purpose="remote.scan.team-page")) as core:
                changes=_team_page(core,scope,after,page)
                batch=scan(core,graph,kind,repositories,roots,set(changes),workspace,None,match,user,scope) if changes else []
            out+=batch
            if not changes: break
            after=changes[-1]
            progress and progress(f"scanning admitted conversations {after[0]}")
            archive_yield(db_path)
    return list({(r["kind"],r["entity"]):r for r in out}.values())
def _store_proofs(db_path,records,signer,controls):
    with contextlib.closing(open_db(db_path,purpose="remote.attest.write")) as db,_transaction(db):
        project_workspace_controls(db,controls)
        project_attested_rows(db,records,signer["root_public"],signer["certificate"])
    archive_yield(db_path)
def attest_rows(db_path,cfg,workspace,records,origins=()):
    controls,device,signer=next(w["controls"] for w in cfg["server_state"]["workspaces"] if w["id"]==workspace),cfg["device"],cfg["controls"][workspace]["devices"][cfg["device"]["id"]]
    wanted={value for r in records if r["kind"] in TABLES and r["payload"].get("state")!="deleted" for column,value in zip(r["payload"]["columns"],r["payload"]["row"]) if value is not None and (column=="id" or column in dict(FKS.get(r["payload"]["table"],())))}
    with contextlib.closing(open_db(db_path,True,purpose="remote.attest.plan")) as db:
        aliases={(table,physical):source for table,physical,source in db.execute(f"SELECT table_name,physical_row_id,source_row_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN ({TEXT_IDS})",(cfg["user"],packed(wanted))).fetchall()}
        selected=[_logical_record(r,aliases) for r in records if r["kind"] in SIGNED]
        rows=[signed_row(r) if r["kind"] in TABLES else logical_fact(r) for r in selected]
        scopes,ids=(workspace,*origins),[r["id"] for r in rows]
        found=db.execute(f"SELECT DISTINCT p.workspace_id,p.row_kind,p.source_row_id,p.revision,p.content_hash FROM remote.row_proofs p WHERE p.workspace_id IN ({','.join('?'*len(scopes))}) AND p.author_user_id=? AND p.source_row_id IN ({TEXT_IDS}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision)",(*scopes,cfg["user"],packed(ids))).fetchall() if rows else []
        bases={(entity):(revision,body) for entity,revision,expected,raw in db.execute(f"SELECT b.entity,b.revision,p.content_hash,c.body FROM remote.local_row_bases b JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(b.kind,b.entity,b.author,b.revision) JOIN remote.row_conflicts c ON c.proof_id=p.id WHERE b.kind='edit.observed' AND b.author=? AND b.entity IN ({TEXT_IDS})",(cfg["user"],packed(ids))).fetchall() if digest(body:=json.loads(raw))==expected} if rows else {}
    heads={}
    [heads.setdefault((r[1],r[2]),{}).setdefault(r[3],(r[0],r[3],r[4])) for r in sorted(found,key=lambda r:(r[0]!=workspace,r[0]))]
    snapshots,signing_key=[],None
    for row in rows:
        prior,current=list(heads.get((row["kind"],row["id"]),{}).values()),digest(row)
        if any(h[2]==current for h in prior): continue
        if row["kind"]=="edit.observed" and len(prior)>1 and (base:=bases.get(row["id"])) and base[0] in {h[1] for h in prior} and all(base[1][k]==row[k] for k in ("kind","id")) and all(base[1]["data"][k]==row["data"][k] for k in ("turn","file","repository")): prior=[h for h in prior if h[1]==base[0]]
        if len({h[2] for h in prior})>1: raise ValueError(f"row revision conflict: {row['kind']}:{row['id']}")
        required(len(prior)<=500,ValueError('row revision fanout exceeds atomic attestation limit'))
        snapshots.extend((row,row_proof(device,cfg["user"],ws,cfg["workspaces"][workspace]["epoch"],row,revision,workspace,current,signing_key=(signing_key:=signing_key or row_signing_key(device)))) for ws,revision,_ in prior or [(workspace,None,None)])
    pages=[[]]
    for key,group in itertools.groupby(snapshots,key=lambda item:(item[0]['kind'],item[0]['id'])):
        group=list(group)
        if len(pages[-1])+len(group)>500: pages.append([])
        pages[-1].extend(group)
    [_store_proofs(db_path,page,signer,controls) for page in pages if page]
    return len(snapshots)
def retained_proof_pages(db_path,workspace,origins=(),author=None,page=5000):
    scopes,after,marks=(scopes:=(workspace,*origins)),"",','.join('?'*len(scopes))
    sql=f"SELECT id FROM (SELECT p.id FROM remote.row_proofs p WHERE p.workspace_id IN ({marks}) AND p.author_user_id=? AND p.state='deleted' AND p.row_kind IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision) UNION SELECT p.id FROM remote.row_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.table_name,o.source_row_id,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision) UNION SELECT p.id FROM remote.provenance_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.kind,o.source_entity,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision) UNION SELECT p.id FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.workspace_id IN ({marks})) retained WHERE id>? ORDER BY id LIMIT ?"
    while True:
        with contextlib.closing(open_db(db_path,True,purpose="remote.replicas.page")) as db: rows=db.execute(sql,(*scopes,author,list(TABLES.values()),*scopes,*scopes,*scopes,after,page)).fetchall()
        if not rows: return
        yield (after:=rows[-1][0]) and {r[0] for r in rows}
        archive_yield(db_path)
def row_replicas(db_path,cfg,workspace,records,keys,known=(),origins=(),origin_epochs=None,inventory=None,retained=True,blocked=None):
    if retained is True: return list({(env["replica"],env["epoch"]):env for env in [*row_replicas(db_path,cfg,workspace,records,keys,known,origins,origin_epochs,inventory,False,blocked),*(env for page in retained_proof_pages(db_path,workspace,origins,cfg["user"]) for env in row_replicas(db_path,cfg,workspace,[],keys,known,origins,origin_epochs,inventory,page,blocked))]}.values())
    fields=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    db=open_db(db_path,True,purpose="remote.replicas.read")
    records,bodies,only,only_sql=logical_records(db,records,cfg["user"]),{},list(retained) if retained else [],f" AND p.id IN ({TEXT_IDS})" if retained else ""
    proof=lambda values:{"v":1,"kind":"row.proof",**dict(zip(fields,values))}
    keep=lambda row,p,content_hash=None:bodies.update({digest(p):(row,p,content_hash)}) if digest(p) not in bodies or digest(row)==p["content_hash"] else None
    try:
        scopes=(workspace,*origins)
        marks=','.join('?'*len(scopes))
        prepared=[(signed_row(r) if r["kind"] in TABLES else logical_fact(r)) for r in records if r["kind"] in SIGNED]
        local={}
        [local.setdefault((r[2],r[3],r[5]),{}).setdefault(r[6],proof(r)) for r in sorted(db.execute(f"SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE workspace_id IN ({marks}) AND author_user_id=? AND source_row_id IN ({TEXT_IDS}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision)",(*scopes,cfg["user"],packed(row["id"] for row in prepared))).fetchall() if prepared else [],key=lambda r:(r[0]!=workspace,r[0],r[10],r[12]))]
        for row in prepared:
            values=list(local.get((row["kind"],row["id"],digest(row)),{}).values())
            if not values: raise ValueError(f"current row proof unavailable: {row['kind']}:{row['id']}")
            [keep(row,p,p["content_hash"]) for p in values]
        for values in (db.execute(f"SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE workspace_id IN ({marks}) AND author_user_id=? AND state='deleted' AND row_kind IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,cfg["user"],list(TABLES.values()),*([packed(only)] if only else []))).fetchall() if retained else []):
            p=proof(values)
            keep(logical_row(p["row_kind"],identity=p["row_id"],state="deleted"),p)
        imported=db.execute(f"SELECT o.table_name,o.physical_row_id,o.source_row_id,o.author_user_id,p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature FROM remote.row_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.table_name,o.source_row_id,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,*([packed(only)] if only else []))).fetchall() if retained else []
        imported_facts=db.execute(f"SELECT o.kind,o.physical_entity,o.source_entity,o.author_user_id,p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature FROM remote.provenance_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.kind,o.source_entity,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,*([packed(only)] if only else []))).fetchall() if retained else []
        claims=[(proof(values),(kind,physical,source,user,values[8])) for kind,physical,source,user,*values in [*imported,*imported_facts]]
        found=typed_logical_rows(db,[claim for p,claim in claims])
        for p,claim in claims: keep(matching_logical_row(found[claim],p["content_hash"]) or found[claim],p)
        for raw,*values in (db.execute(f"SELECT CAST(c.body AS VARCHAR),p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.workspace_id IN ({marks}){only_sql}",(*scopes,*([packed(only)] if only else []))).fetchall() if retained else []): keep(json.loads(raw),proof(values))
        delivery=lambda p:p["authorization_epoch"] if p["authorization_workspace"]==workspace else (origin_epochs or {})[p["workspace"]]
        candidates=[(row,p,content_hash,epoch,fingerprint(keys[epoch],digest(p))) for row,p,content_hash in bodies.values() for epoch in [delivery(p)] if epoch in keys]
        db.close()
        archive_yield(db_path)
        known=set(inventory([(r[4],r[3]) for r in candidates])) if inventory else known if isinstance(known,set) else set(known)
        candidates=[r for r in candidates if r[4] not in known]
        if not candidates: return []
        heads={(p["workspace"],p["row_kind"],p["row_id"],p["author_user_id"]):p for row,p,content_hash,epoch,replica in candidates if p["kind"]=="row.proof"}
        ids,users,histories={k[2] for k in heads},{k[3] for k in heads},{}
        with contextlib.closing(open_db(db_path,True,purpose="remote.replicas.lineage")) as lineage_db: [histories.setdefault((p["workspace"],p["row_kind"],p["row_id"],p["author_user_id"],p["revision"]),p) for values in (lineage_db.execute(f"SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs WHERE workspace_id IN ({marks}) AND source_row_id IN ({TEXT_IDS}) AND author_user_id IN ({TEXT_IDS})",(*scopes,packed(ids),packed(users))).fetchall() if heads else []) for p in [proof(values)]]
        archive_yield(db_path)
        def lineage(p):
            out,revision,seen=[],p["previous_revision"],set()
            while revision:
                parent=histories.get((p["workspace"],p["row_kind"],p["row_id"],p["author_user_id"],revision))
                if not parent or revision in seen: raise ValueError(f"row proof lineage unavailable: {p['row_kind']}:{p['row_id']}")
                out.append(parent)
                seen.add(revision)
                revision=parent["previous_revision"]
            return out
        def seal(row,p,content_hash,epoch):
            if content_hash is None and digest(row)!=p["content_hash"]:
                if blocked is not None: return blocked.append((p["row_kind"],p["row_id"]))
                raise ValueError(f"typed projection differs from author proof: {p['row_kind']}:{p['row_id']}")
            return seal_replica(row,p,workspace,epoch,keys[epoch],cfg["device"]["id"],content_hash,lineage(p) if p["kind"]=="row.proof" else (),replica_compression(cfg))
        return [env for row,p,content_hash,epoch,replica in candidates if (env:=seal(row,p,content_hash,epoch))]
    finally: db and db.close()
def _proof(values):
    return {"v":1,"kind":"row.proof",**dict(zip(PROOF_FIELDS,values))}
REPACK_PROOF_COLUMNS="workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature"
def repack_index(db_path,state,cfg,workspace,keys,root,origins=()):
    state.execute("CREATE TEMP TABLE compact_local(semantic INT,replica TEXT,epoch INT,proof_id TEXT,payload BLOB,PRIMARY KEY(semantic,replica,epoch)) WITHOUT ROWID")
    after,scopes="",[workspace,*origins]
    while Path(db_path).is_file():
        with contextlib.closing(open_db(db_path,True,purpose="remote.compact.index")) as db: rows=db.execute(f"SELECT id,{REPACK_PROOF_COLUMNS} FROM remote.row_proofs WHERE id>? AND workspace_id IN (SELECT UNNEST(?)) ORDER BY id LIMIT 5000",[after,scopes]).fetchall()
        if not rows: break
        state.executemany("INSERT OR IGNORE INTO compact_local VALUES (0,?,?,?,NULL)",[(fingerprint(key_,digest(_proof(values))),epoch,pid) for pid,*values in rows for epoch,key_ in keys.items()])
        after=rows[-1][0]
        archive_yield(db_path)
    for value in bridge_records(root,cfg,workspace,cfg["workspaces"][workspace]["kind"]):
        if (p:=value["proof"]) is None: continue
        raw=canon({"row":value["row"],"proof":p,"lineage":[]})
        state.executemany("INSERT OR IGNORE INTO compact_local VALUES (1,?,?,NULL,?)",[(fingerprint(key_,digest(p)),epoch,raw) for epoch,key_ in keys.items()])
    state.commit()
def local_repack_envelopes(db_path,state,cfg,page,keys):
    entries=[(item,entry) for item in page if (entry:=state.execute("SELECT proof_id,payload FROM compact_local WHERE semantic=? AND replica=? AND epoch=?",(item["semantic"],item["header"]["replica"],item["header"]["epoch"])).fetchone())]
    bodies={item["cursor"]:entry[1] for item,entry in entries if entry[1] is not None}
    pids=[entry[0] for item,entry in entries if entry[0] is not None]
    if pids:
        with contextlib.closing(open_db(db_path,True,purpose="remote.compact.bodies")) as db:
            proofs={pid:_proof(values) for pid,*values in db.execute(f"SELECT id,{REPACK_PROOF_COLUMNS} FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))",[pids]).fetchall()}
            physical=dict(db.execute("SELECT proof_id,physical_row_id FROM remote.row_origins WHERE proof_id IN (SELECT UNNEST(?)) UNION ALL SELECT proof_id,physical_entity FROM remote.provenance_origins WHERE proof_id IN (SELECT UNNEST(?))",[pids,pids]).fetchall())
            claims={pid:(p["row_kind"],physical.get(pid,p["row_id"] if p["author_user_id"]==cfg["user"] or p["row_kind"] in PROVENANCE-{'edit.observed','checkpoint.link'} else foreign_id(p["author_user_id"],"file_edits" if p["row_kind"]=="edit.observed" else p["row_kind"],p["row_id"])),p["row_id"],p["author_user_id"],p["state"]) for pid,p in proofs.items()}
            found=typed_logical_rows(db,claims.values())
            paths=captured_edit_paths(db,[c[1] for c in claims.values() if c[0]=="file_edits"])
            retained={pid:json.loads(raw) for pid,raw in db.execute("SELECT proof_id,CAST(body AS VARCHAR) FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))",[pids]).fetchall()}
            parents=[p for p in proofs.values() if p["previous_revision"]]
            history={(v[0],v[2],v[3],v[9],v[6]):_proof(v) for v in db.execute(f"SELECT {REPACK_PROOF_COLUMNS} FROM remote.row_proofs WHERE workspace_id IN (SELECT UNNEST(?)) AND source_row_id IN (SELECT UNNEST(?)) AND author_user_id IN (SELECT UNNEST(?))",[[p["workspace"] for p in parents],[p["row_id"] for p in parents],[p["author_user_id"] for p in parents]]).fetchall()} if parents else {}
            for item,entry in entries:
                if entry[0] not in proofs: continue
                p,claim=proofs[entry[0]],claims[entry[0]]
                row=retained.get(entry[0])
                if row is None or digest(row)!=p["content_hash"]: row=matching_logical_row(found[claim],p["content_hash"],[paths[claim[1]]] if claim[1] in paths else ())
                if row is None or digest(row)!=p["content_hash"]: continue
                lineage,revision,seen=[],p["previous_revision"],set()
                while revision:
                    parent=history.get((p["workspace"],p["row_kind"],p["row_id"],p["author_user_id"],revision))
                    if revision in seen or parent is None: break
                    lineage.append(parent)
                    seen.add(revision)
                    revision=parent["previous_revision"]
                if revision is None: bodies[item["cursor"]]=canon({"row":row,"proof":p,"lineage":lineage})
        archive_yield(db_path)
    # The old nonce is used only for local equality checks; replacements use a fresh nonce.
    return {item["cursor"]:_seal(item["header"],bodies[item["cursor"]],keys[item["header"]["epoch"]]) for item in page if item["cursor"] in bodies}
def _heads(db,user,ids,all_heads=False):
    out={}
    for table,sources in ids.items():
        if not sources: continue
        rows=db.execute("SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE author_user_id=? AND row_kind=? AND source_row_id IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE (c.row_kind,c.source_row_id,c.author_user_id)=(p.row_kind,p.source_row_id,p.author_user_id) AND c.previous_revision=p.revision) ORDER BY workspace_id",(user,table,list(sources))).fetchall()
        for values in rows:
            out.setdefault((table,values[3]),{})[values[6]]=_proof(values)
    missing=[(table,source) for table,sources in ids.items() for source in sources if (table,source) not in out]
    forks=[key for key,values in out.items() if len({p["content_hash"] for p in values.values()})!=1]
    if missing or forks: raise ValueError(f"provider alias row proof unavailable or forked: {(missing or forks)[0]}")
    return {key:list(values.values()) if all_heads else values[min(values)] for key,values in out.items()}
# Preserve the single-parent wire format by advancing every equal-valued branch.
def _alias_proofs(db,cfg,workspace,records):
    heads=_heads(db,cfg['user'],{kind:{r['id'] for r,p in records if r['kind']==kind} for kind in {r['kind'] for r,p in records}},True)
    required(all(p['previous_revision'] in {h['revision'] for h in heads[(r['kind'],r['id'])]} for r,p in records),RuntimeError('Row heads changed during provider alias reconciliation; retry'))
    extras=[row_proof(cfg['device'],cfg['user'],h['workspace'],cfg['workspaces'][workspace]['epoch'],r,h['revision'],workspace) for r,p in records for h in heads[(r['kind'],r['id'])] if h['revision']!=p['previous_revision']]
    signer=cfg['controls'][workspace]['devices'][cfg['device']['id']]
    return project_row_proofs(db,[p for r,p in records]+extras,signer['root_public'],signer['certificate'])[:len(records)]
def _alias_members(db,user,source,session,members,canonical):
    member_heads,origin_rows=_heads(db,user,{"conversations":set(members)}),db.execute("SELECT physical_row_id,source_row_id FROM remote.row_origins WHERE table_name='conversations' AND author_user_id=? AND source_row_id IN (SELECT UNNEST(?))",(user,members)).fetchall()
    origin_members={}
    [origin_members.setdefault(logical,set()).add(physical) for physical,logical in origin_rows]
    present={r[0] for r in db.execute("SELECT id FROM conversations WHERE id IN (SELECT UNNEST(?))",[sorted(set(members)|{p for p,m in origin_rows})]).fetchall()}
    local,member_physical=(local:={r[0] for r in db.execute("SELECT id FROM conversations c WHERE id IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id)",[members]).fetchall()}),{member:required(next(iter(found)) if len(found)==1 else None,ValueError(f"provider alias member projection is ambiguous: {member}")) for member in members for found in [(origin_members.get(member,set())&present)|({member} if member in local else set())] if found}
    active=(required(canonical in (active:={member for member in members if member_heads[("conversations",member)]["state"]=="active"}) and active<=set(member_physical),ValueError("provider alias active body unavailable")),active)[-1]
    required(all(actual==source and isinstance(metadata:=json.loads(raw) if raw else None,dict) and provider_session_key(source,metadata.get("session_id"))==provider_session_key(source,session) for actual,raw in db.execute("SELECT source,metadata FROM conversations WHERE id IN (SELECT UNNEST(?))",[[member_physical[m] for m in active]]).fetchall()),ValueError("provider alias exact evidence conflicts"))
    member_physical|={member:foreign_id(user,'conversations',member) if member in origin_members else member for member in set(members)-set(member_physical)}
    return member_heads,member_physical,active,{member:member_physical[member]==member and member in local or member not in origin_members and member not in present for member in members},not (binding:=db.execute("SELECT conversation_id FROM provider_sessions WHERE source=? AND session_id=?",(source,provider_session_key(source,session))).fetchone()) or binding[0]!=member_physical[canonical]
def _lineage_union(values):
    required(all(isinstance(v,dict) and set(v)=={'v','records'} and type(v['v']) is int and v['v']==1 and isinstance(v['records'],list) and all(isinstance(r,dict) and set(r)=={'old_id','old_hash','current_id','current_hash'} and all(isinstance(r[k],str) and re.fullmatch('[0-9a-f]{'+str(64 if k.endswith('hash') else 16)+'}',r[k]) for k in r) for r in v['records']) for v in values),ValueError('provider alias lineage merge is invalid'))
    return dict(v=1,records=sorted({canon(r):r for v in values for r in v['records']}.values(),key=canon))
def _alias_merge_native(db,row,head):
    if row is None: return None
    records=db.execute("WITH RECURSIVE ancestors(revision) AS (SELECT CAST(? AS VARCHAR) UNION SELECT p.previous_revision FROM remote.row_proofs p JOIN ancestors a ON p.revision=a.revision WHERE (p.row_kind,p.source_row_id,p.author_user_id)=(?,?,?) AND p.previous_revision IS NOT NULL) SELECT DISTINCT 0 slot,p.content_hash,c.body FROM remote.local_row_bases b JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(b.kind,b.entity,b.author,b.revision) JOIN ancestors a ON a.revision=b.revision JOIN remote.row_conflicts c ON c.proof_id=p.id WHERE (b.kind,b.entity,b.author)=(?,?,?) UNION ALL SELECT 1 slot,p.content_hash,c.body FROM remote.row_proofs p JOIN remote.row_conflicts c ON c.proof_id=p.id WHERE p.id=? ORDER BY slot",[head['revision'],row['kind'],row['id'],head['author_user_id'],row['kind'],row['id'],head['author_user_id'],digest(head)]).fetchall()
    if row['kind'] in ('conversations','messages') and len(current:=[json.loads(raw) for slot,expected,raw in records if slot==1 and digest(json.loads(raw))==expected])==1:
        try: return _parser_metadata_join([row,*current])
        except ValueError: pass  # Other independent changes still require a shared signed base.
    if len(records)!=2 or any(digest(json.loads(raw))!=expected for slot,expected,raw in records): return None
    base,remote=(json.loads(raw) for slot,expected,raw in records)
    if any(v['state']!='active' or (v['kind'],v['id'])!=(row['kind'],row['id']) for v in (base,remote)): return None
    absent=object()
    equal=lambda a,b:a is b if a is absent or b is absent else canon(a)==canon(b)
    def merge(old,local,new,key=''):
        if key in ('created_at','updated_at'):
            old,local,new=(datetime.fromisoformat(v).isoformat(timespec='microseconds') if isinstance(v,str) else v for v in (old,local,new))
        if equal(local,old): return new
        if equal(new,old) or equal(local,new): return local
        if key in ('convos_message_lineage','convos_tool_lineage','convos_edit_lineage'):
            values=[v for v in (old,local,new) if v is not absent]
            return _lineage_union(values)
        if key in ('','metadata') and all(isinstance(v,dict) for v in (old,local,new)):
            values={k:merge(old.get(k,absent),local.get(k,absent),new.get(k,absent),k) for k in old.keys()|local.keys()|new.keys()}
            return {k:v for k,v in values.items() if v is not absent}
        raise ValueError('provider alias concurrent content changes require resolution')
    return {**row,'data':merge(base['data'],row['data'],remote['data'])}
def _alias_page(db,user,member_physical,after,page=500):
    members,query=list(member_physical.values()),"""WITH selected AS (SELECT 'conversations' kind,id FROM conversations WHERE id IN (SELECT UNNEST(?)) UNION SELECT 'messages',id FROM messages WHERE conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'tool_calls',x.id FROM tool_calls x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'attachments',x.id FROM attachments x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'file_edits',x.id FROM file_edits x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'artifacts',id FROM artifacts WHERE conversation_id IN (SELECT UNNEST(?))) SELECT kind,id FROM selected WHERE kind>? OR kind=? AND id>? ORDER BY kind,id LIMIT ?"""
    keys=db.execute(query,(*([members]*6),after[0],after[0],after[1],page)).fetchall()
    if not keys: return [],after
    selected,raws=((selected:={table:{row_id for kind,row_id in keys if kind==table} for table in COLUMNS}),{(table,values[0]):(columns,list(map(clean,values))) for table,physical_ids in selected.items() if physical_ids for cur in [db.execute("SELECT * EXCLUDE (embedding) FROM messages WHERE id IN (SELECT UNNEST(?))" if table=="messages" else "SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id WHERE a.id IN (SELECT UNNEST(?))" if table=="attachments" else f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",(list(physical_ids),))] for columns in [[d[0] for d in cur.description]] for values in cur.fetchall()})
    origins={(table,physical):(logical,workspace) for table,physical,logical,workspace in db.execute("SELECT table_name,physical_row_id,source_row_id,workspace_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN (SELECT UNNEST(?))",(user,[row_id for kind,row_id in keys])).fetchall()}
    reverse={(table,physical):logical for (table,physical),(logical,workspace) in origins.items()}|{("conversations",physical):logical for logical,physical in member_physical.items()}
    refs={(parent,value) for (table,physical),(columns,values) in raws.items() for column,value in zip(columns,values) for parent in [dict(FKS.get(table,())).get(column)] if parent and value is not None}
    reverse|={(table,physical):logical for table,physical,logical in db.execute("SELECT table_name,physical_row_id,source_row_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN (SELECT UNNEST(?))",(user,[value for table,value in refs])).fetchall()}
    reverse|={ref:ref[1] for ref in refs if ref not in reverse}
    physical_by_source={(table,logical):physical for (table,physical),logical in reverse.items()}|{("conversations",logical):physical for logical,physical in member_physical.items()}
    paths=captured_edit_paths(db,selected.get("file_edits",set()))
    lineage=db.execute("SELECT DISTINCT l.old_source,l.new_source,l.old_id,l.new_id,l.old_hash,l.new_hash FROM parser_message_lineage l JOIN messages old ON old.id=l.old_id JOIN messages new ON new.id=l.new_id WHERE (l.author=? OR l.native) AND old.conversation_id IN (SELECT UNNEST(?)) AND new.conversation_id IN (SELECT UNNEST(?)) AND (old.id IN (SELECT UNNEST(?)) OR new.id IN (SELECT UNNEST(?)))",[user,list(member_physical.values()),list(member_physical.values()),[v for k,v in refs if k=='messages'],[v for k,v in refs if k=='messages']]).fetchall()
    claims=[('messages',physical,source,user,'active') for old,new,op,np,oh,nh in lineage for source,physical in ((old,op),(new,np))]
    bodies=typed_logical_rows(db,claims) if claims else {}
    equivalent={}
    for old,new,op,np,oh,nh in lineage:
        if matching_logical_row(bodies[('messages',op,old,user,'active')],oh) is not None and matching_logical_row(bodies[('messages',np,new,user,'active')],nh) is not None:
            equivalent.setdefault(old,set()).add(new)
            equivalent.setdefault(new,set()).add(old)
    heads=_heads(db,user,{table:{reverse.get((table,physical),physical) for physical in values} for table,values in selected.items()})
    out=[]
    for table,physical in keys:
        columns,values=raws[(table,physical)]
        source,parents=reverse.get((table,physical),physical),dict(FKS.get(table,()))
        head=heads[(table,source)]
        if table=='conversations' and head['state']=='deleted': continue
        row=logical_row(table,columns,[reverse.get((parents[column],value),value) if column in parents else value for column,value in zip(columns,values)],source)
        matched=matching_logical_row(row,head['content_hash'],[paths[physical]] if table=='file_edits' and physical in paths else ())
        # Re-import may restore an obsolete conversation binding; signed aliases prove this reference repair only.
        if matched is None and table in ('messages','artifacts') and row['data']['conversation_id'] in member_physical:
            matched=next(({**candidate,'data':{**candidate['data'],'conversation_id':row['data']['conversation_id']}} for member in member_physical if (candidate:=matching_logical_row({**row,'data':{**row['data'],'conversation_id':member}},head['content_hash'])) is not None),None)
        if matched is None:
            variants=[row]
            for column,parent in FKS.get(table,()):
                if parent=='messages' and row['data'][column] in equivalent: variants=[{**value,'data':{**value['data'],column:key}} for value in variants for key in {row['data'][column],*equivalent[row['data'][column]]}]
            candidate=next((matched_variant for variant in variants if (matched_variant:=matching_logical_row(variant,head['content_hash'])) is not None),None)
            if candidate: matched={**candidate,'data':{**candidate['data'],**{column:row['data'][column] for column,parent in FKS.get(table,()) if parent=='messages'}}}
        if matched is None and (table,physical) not in origins: matched=_alias_merge_native(db,row,head)
        required(matched is not None,ValueError(f'provider alias body/proof mismatch: {table}:{source}'))
        out.append((matched,head,(table,physical) not in origins,physical_by_source,values[columns.index('path')] if table=='attachments' else None))
    return out,keys[-1]
def _alias_pages(db_path,user,member_physical,page=500,progress=None):
    after=("","")
    while True:
        with contextlib.closing(open_db(db_path,True,purpose="remote.alias.page")) as db: generation,values=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],_alias_page(db,user,member_physical,after,page)
        rows,after=values
        if not rows: return
        yield (progress and progress(f"provider alias page {after[0]}:{after[1]}"),generation,rows)[1:]
        archive_yield(db_path)
def _alias_terminal(links,key):
    seen=set()
    while key in links and key not in seen:
        seen.add(key)
        key=links[key]
    return key if key is not None and key not in seen else None

def _alias_message_plan(rows,source,members,history=()):
    # Authenticated provider event indices and complete payloads prove equivalence.
    messages={row['id']:row for row,*_ in rows if row['kind']=='messages'}
    records=[record for row,*_ in rows if row['kind']=='conversations' for meta in [row['data']['metadata']] if isinstance(meta,dict) for lineage in [meta.get('convos_message_lineage')] if isinstance(lineage,dict) and type(lineage.get('v')) is int and lineage['v']==1 and isinstance(lineage.get('records'),list) for record in lineage['records'] if isinstance(record,dict) and set(record)=={'old_id','old_hash','current_id','current_hash'} and all(isinstance(record[k],str) and re.fullmatch('[0-9a-f]{'+str(64 if k.endswith('hash') else 16)+'}',record[k]) for k in record)]
    declared={key:next(iter(targets)) if len(targets:={r['current_id'] for r in group})==1 else None for key,group in itertools.groupby(sorted(records,key=lambda r:r['old_id']),key=lambda r:r['old_id'])}
    records=[r for r in records if _alias_terminal(declared,r['old_id']) is not None]
    bodies={key:[row] for key,row in messages.items()}
    for row in history: bodies.setdefault(row['id'],[]).append(row)
    equivalent,pairs={},set()
    def available(row):
        if row['id'] not in messages or row is messages[row['id']]: return True
        current=messages[row['id']]
        if current['data']['parent_id'] not in {row['data']['parent_id'],*equivalent.get(row['data']['parent_id'],())}: return False
        try: return bool(_parser_metadata_join([current,{**row,'data':{**row['data'],'parent_id':current['data']['parent_id']}}]))
        except ValueError: return False
    while True:
        fresh={(r['old_id'],r['current_id']) for r in records if r['old_id']!=r['current_id'] and all(any(matching_logical_row({**row,'data':{**row['data'],'parent_id':parent}},expected) is not None for row in bodies.get(key,[]) if available(row) for parent in {row['data']['parent_id'],*equivalent.get(row['data']['parent_id'],())}) for key,expected in ((r['old_id'],r['old_hash']),(r['current_id'],r['current_hash'])))}-pairs
        if not fresh: break
        pairs|=fresh
        for old,new in fresh:
            merged={old,new,*equivalent.get(old,()),*equivalent.get(new,())}
            for key in merged: equivalent[key]=merged
    pairs=sorted(pairs)
    links={key:next(iter(targets)) for key,group in itertools.groupby(pairs,key=lambda pair:pair[0]) if len(targets:={target for _,target in group})==1}
    normalized={key:target for key in links if key in messages and (target:=_alias_terminal(links,key)) in messages}
    # Only exact, source-backed lineage can replace an older parser's payload or local timestamp.
    messages={key:{**row,'data':messages[normalized[key]]['data']} if key in normalized else row for key,row in messages.items()}
    indices={key:meta['provider_index'] for key,row in messages.items() for meta in [row['data']['metadata']] if isinstance(meta,dict) and type(meta.get('provider_index')) is int and meta['provider_index']>=0}
    lineages={(label,key):value['records'] for row,*_ in rows if row['kind']=='messages' for key in [row['id']] for label in ('convos_tool_lineage','convos_edit_lineage') for value in [(row['data']['metadata'] or {}).get(label)] if isinstance(value,dict) and set(value)=={'v','records'} and type(value['v']) is int and value['v']==1 and isinstance(value['records'],list) and all(isinstance(r,dict) and set(r)=={'old_id','old_hash','current_id','current_hash'} and all(isinstance(r[k],str) and re.fullmatch('[0-9a-f]{'+str(64 if k.endswith('hash') else 16)+'}',r[k]) for k in r) for r in value['records'])}
    signatures,groups={},{}
    for key in indices:
        chain,visiting=[],set()
        current=key
        while current in indices and current not in signatures and current not in visiting:
            chain.append(current)
            visiting.add(current)
            current=messages[current]['data']['parent_id']
        if current in visiting:
            for member in chain: signatures[member]=digest(['unresolved-cycle',member])
        for member in reversed(chain):
            if member not in signatures:
                data=messages[member]['data']
                metadata={k:v for k,v in data['metadata'].items() if k not in ('convos_tool_lineage','convos_edit_lineage') or (k,member) not in lineages and (k,normalized.get(member)) not in lineages}
                signatures[member]=digest({**data,'created_at':datetime.fromisoformat(data['created_at']).isoformat(timespec='microseconds') if data['created_at'] else None,'metadata':metadata,'parent_id':signatures.get(data['parent_id'],data['parent_id'])})
        groups.setdefault((indices[key],signatures[key]),[]).append(key)
    replacements={old:min(keys) for keys in groups.values() for old in keys if old!=min(keys)}
    revised=[({**row,'data':{**row['data'],**{column:replacements[row['data'][column]] for column,parent in FKS.get(row['kind'],()) if parent=='messages' and row['data'][column] in replacements}}},head,native,parents,path) for original,head,native,parents,path in rows for row in [messages[original['id']] if original['kind']=='messages' else original]]
    carriers={min(keys):{label:{'v':1,'records':sorted({canon(record):record for key in keys for record in lineages.get((label,key),[])}.values(),key=canon)} for label in ('convos_tool_lineage','convos_edit_lineage') if any((label,key) in lineages for key in keys)} for keys in groups.values() if len(keys)>1}
    revised=[({**row,'data':{**row['data'],'metadata':{**row['data']['metadata'],**carriers[row['id']]}}} if row['kind']=='messages' and carriers.get(row['id']) else row,head,native,parents,path) for row,head,native,parents,path in revised]
    current={row['id']:row for row,*_ in revised if row['kind']=='messages'}
    records=[dict(old_id=old,old_hash=digest(current[old]),current_id=new,current_hash=digest(current[new])) for old,new in sorted(replacements.items())]
    return [value for value,prior in zip(revised,rows) if value[0]!=prior[0]],records

def _alias_edit_successors(db,user,rows):
    edits=[(row,head,native,parents,path) for row,head,native,parents,path in rows if row['kind']=='file_edits']
    existing={r[0] for r in db.execute("SELECT DISTINCT source_row_id FROM remote.row_proofs WHERE row_kind='edit.observed' AND author_user_id=? AND source_row_id IN (SELECT UNNEST(?))",[user,[r['id'] for r,*_ in edits]]).fetchall()}
    heads=_heads(db,user,{'edit.observed':existing})
    claims=[('edit.observed',parents.get(('file_edits',row['id']),row['id']),row['id'],user,'active') for row,head,native,parents,path in edits if row['id'] in existing]
    bodies=typed_logical_rows(db,claims)
    return [({**fact,'data':{**fact['data'],'turn':row['data']['message_id']}},heads[('edit.observed',row['id'])],native,parents,None) for row,head,native,parents,path in edits if row['id'] in existing for claim in [('edit.observed',parents.get(('file_edits',row['id']),row['id']),row['id'],user,'active')] for fact in [required(matching_logical_row(bodies[claim],heads[('edit.observed',row['id'])]['content_hash']) or (native and _alias_merge_native(db,bodies[claim],heads[('edit.observed',row['id'])])),ValueError('provider alias edit provenance body/proof mismatch'))]]
def _alias_evidence_successors(db,cfg,revisions,proofs):
    from . import _edit_evidence_value
    changes={(row['kind'],row['id']):(head,proof) for (row,head,*_),proof in zip(revisions,proofs) if row['kind'] in ('file_edits','tool_calls')}
    values=[_edit_evidence_value(r) for r in db.execute("SELECT workspace_id,author_user_id,object_id,revision,source_edit_id,edit_revision,status,reason,source_tool_call_id,tool_revision,CAST(proof AS VARCHAR) FROM remote.file_edit_evidence_claims p WHERE author_user_id=? AND (source_edit_id IN (SELECT UNNEST(?)) OR source_tool_call_id IN (SELECT UNNEST(?))) AND NOT EXISTS(SELECT 1 FROM remote.file_edit_evidence_ancestors a WHERE a.object_kind='file-edit.evidence' AND (a.workspace_id,a.author_user_id,a.object_id,a.ancestor_revision)=(p.workspace_id,p.author_user_id,p.object_id,p.revision))",[cfg['user'],[key for kind,key in changes if kind=='file_edits'],[key for kind,key in changes if kind=='tool_calls']]).fetchall()]
    records=[]
    for value in values:
        row,data=value['row'],value['row']['data']
        related=[(field,changes[(kind,data[key])]) for kind,key,field in (('file_edits','edit','edit_revision'),('tool_calls','tool_call','tool_revision')) if (kind,data[key]) in changes]
        if sum(v['row']['id']==row['id'] for v in values)!=1 or not all(data[field]==old['revision'] for field,(old,new) in related): continue
        revised={**row,'data':{**data,**{field:new['revision'] for field,(old,new) in related}}}
        records.append((value['workspace'],revised,value['proof']))
    return records
def _alias_fingerprints(db,user,groups):
    members=[dict(alias='provider-session:'+digest([source,session]),member=member) for (source,session),aliases in groups.items() for member in sorted({m for oid,leaves in aliases for value in leaves for m in value[3]})]
    if not members: return {}
    # One shared dependency scan replaces thousands of repeated conversation-tree scans.
    query="""WITH members AS (SELECT x.* FROM UNNEST(from_json(?,'[{"alias":"VARCHAR","member":"VARCHAR"}]')) t(x)), roots AS (
      SELECT alias,member physical FROM members UNION SELECT m.alias,o.physical_row_id FROM members m JOIN remote.row_origins o ON o.table_name='conversations' AND o.author_user_id=? AND o.source_row_id=m.member), selected AS (
      SELECT alias,'conversations' kind,physical FROM roots UNION SELECT r.alias,'messages',p.physical FROM roots r JOIN parser_retired_rows p ON p.kind='messages' AND json_extract_string(p.body,'$.data.conversation_id')=r.physical UNION SELECT r.alias,'messages',m.id FROM roots r JOIN messages m ON m.conversation_id=r.physical
      UNION SELECT r.alias,'artifacts',a.id FROM roots r JOIN artifacts a ON a.conversation_id=r.physical
      UNION SELECT r.alias,'tool_calls',x.id FROM roots r JOIN messages m ON m.conversation_id=r.physical JOIN tool_calls x ON x.message_id=m.id
      UNION SELECT r.alias,'attachments',x.id FROM roots r JOIN messages m ON m.conversation_id=r.physical JOIN attachments x ON x.message_id=m.id
      UNION SELECT r.alias,'file_edits',x.id FROM roots r JOIN messages m ON m.conversation_id=r.physical JOIN file_edits x ON x.message_id=m.id
      UNION SELECT r.alias,'edit.observed',x.id FROM roots r JOIN messages m ON m.conversation_id=r.physical JOIN file_edits x ON x.message_id=m.id
      UNION SELECT r.alias,'file.observed',v.file_id FROM roots r JOIN messages m ON m.conversation_id=r.physical JOIN file_edits x ON x.message_id=m.id JOIN provenance.file_edit_files v ON v.file_edit_id=x.id), inventory AS (
      SELECT * FROM selected UNION SELECT s.alias,'messages',m.parent_id FROM selected s JOIN messages m ON s.kind='messages' AND m.id=s.physical WHERE m.parent_id IS NOT NULL), origins AS (
      SELECT table_name,physical_row_id,source_row_id,author_user_id,proof_id FROM remote.row_origins UNION ALL SELECT kind,physical_entity,source_entity,author_user_id,proof_id FROM remote.provenance_origins)
      SELECT s.alias,sha256(string_agg(to_json([s.kind,s.physical,CAST(c.generation AS VARCHAR),o.source_row_id,o.author_user_id,o.proof_id,p.id,rc.proof_id,b.revision]),'' ORDER BY s.kind,s.physical,p.id))
      FROM inventory s LEFT JOIN archive_changes c ON (c.kind,c.entity)=(s.kind,s.physical)
      LEFT JOIN origins o ON (o.table_name,o.physical_row_id)=(s.kind,s.physical)
      LEFT JOIN remote.row_proofs p ON p.row_kind=s.kind AND p.source_row_id=COALESCE(o.source_row_id,s.physical) AND p.author_user_id=? LEFT JOIN remote.row_conflicts rc ON rc.proof_id=p.id LEFT JOIN remote.local_row_bases b ON (b.kind,b.entity,b.author)=(p.row_kind,p.source_row_id,p.author_user_id) GROUP BY s.alias"""
    inputs=dict(db.execute(query,[json.dumps(members,separators=(',',':')),user,user]).fetchall())
    bindings=db.execute('SELECT source,session_id,conversation_id FROM provider_sessions ORDER BY source,session_id').fetchall()
    bound={}
    for source,session,physical in bindings: bound.setdefault((source,provider_session_key(source,session)),[]).append((session,physical))
    archive=db.execute('SELECT archive_id::VARCHAR,(SELECT version FROM core_schema WHERE singleton) FROM archive_state WHERE singleton').fetchone()
    return {'provider-session:'+digest([source,session]):digest([ALIAS_VERSION,archive,aliases,bound.get((source,session),[]),inputs.get('provider-session:'+digest([source,session]))]) for (source,session),aliases in groups.items()}
def _parser_metadata_join(rows):
    kind=rows[0]['kind']
    required(all(r['kind']==kind and r['state']=='active' and r['id']==rows[0]['id'] and isinstance(r['data']['metadata'],dict) and (r['data']['source'] in ('codex','claude-code') and r['data']['metadata'].get('session_id') and r['data']['metadata'].get('timestamp_basis')=='utc' if kind=='conversations' else kind=='messages' and type(r['data']['metadata'].get('provider_index')) is int and r['data']['metadata']['provider_index']>=0) for r in rows),ValueError('parser fork lacks exact current provider metadata'))
    lineage={'convos_tool_lineage','convos_edit_lineage'}|({'convos_message_lineage'} if kind=='conversations' else set())
    ordinary=[{**r,'data':{k:({name:value for name,value in v.items() if name not in lineage} if k=='metadata' else datetime.fromisoformat(v).isoformat(timespec='microseconds') if k=='created_at' and v else v) for k,v in r['data'].items() if k!='updated_at'}} for r in rows]
    required(len({digest(r) for r in ordinary})==1,ValueError('parser content conflict requires resolution'))
    stamps=[datetime.fromisoformat(r['data']['updated_at']) for r in rows if kind=='conversations' and r['data']['updated_at'] is not None]
    required(all(t.tzinfo is None for t in stamps),ValueError('conversation fork timestamp representation is not canonical UTC'))
    metadata={**ordinary[0]['data']['metadata'],**{key:_lineage_union([r['data']['metadata'][key] for r in rows if key in r['data']['metadata']]) for key in sorted(lineage) if any(key in r['data']['metadata'] for r in rows)}}
    return {**ordinary[0],'data':{**ordinary[0]['data'],**({'updated_at':max(stamps).isoformat(timespec='microseconds') if stamps else None} if kind=='conversations' else {}),'metadata':metadata}}
def _message_history(db,user,owners): return [body for raw,expected in db.execute("SELECT body,content_hash FROM parser_retired_rows WHERE kind='messages' AND (author=? OR author='') AND json_extract_string(body,'$.data.conversation_id') IN (SELECT UNNEST(?)) UNION SELECT c.body,p.content_hash FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.row_kind='messages' AND p.author_user_id=? AND json_extract_string(c.body,'$.data.conversation_id') IN (SELECT UNNEST(?))",[user,list(owners),user,list(owners)]).fetchall() if digest(body:=json.loads(raw))==expected]
def _source_message_join(db,rows,user):
    owners={r['data']['conversation_id'] for r in rows}
    claims=[(kind,entity,entity,user,'active') for kind,entity in db.execute("SELECT 'conversations',id FROM conversations c WHERE id IN (SELECT UNNEST(?)) AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='conversations' AND o.physical_row_id=c.id) UNION ALL SELECT 'messages',id FROM messages m WHERE conversation_id IN (SELECT UNNEST(?)) AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='messages' AND o.physical_row_id=m.id)",[list(owners),list(owners)]).fetchall()]
    current,history=list(typed_logical_rows(db,claims).values()),_message_history(db,user,owners)
    required(sum(r['kind']=='conversations' for r in current)==len(owners) and all(r['data']['source'] in ('codex','claude-code') for r in current if r['kind']=='conversations'),ValueError('source-lineage carrier is not a native provider conversation'))
    normalized=[next((value for value,*_ in revisions if value['kind']=='messages' and value['id']==row['id']),row) for row in rows for revisions,lineage in [_alias_message_plan([(v,{},True,{},None) for v in [*[r for r in current if (r['kind'],r['id'])!=('messages',row['id'])],row]],next(r['data']['source'] for r in current if r['kind']=='conversations'),owners,history)]]
    return _parser_metadata_join(normalized)
def _reconcile_parser_heads(db_path,cfg,workspace,progress):
    user,after,blocked,changed=cfg['user'],('',''),{},0
    while True:
        with contextlib.closing(open_db(db_path,True,purpose='remote.parser.plan')) as db:
            ids=db.execute("SELECT p.row_kind,p.source_row_id FROM remote.row_proofs p JOIN (SELECT 'conversations' kind,id FROM conversations UNION ALL SELECT 'messages',m.id FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.source IN ('codex','claude-code')) c ON (c.kind,c.id)=(p.row_kind,p.source_row_id) WHERE p.author_user_id=? AND p.workspace_id=? AND (p.row_kind,p.source_row_id)>(?,?) AND NOT EXISTS(SELECT 1 FROM remote.row_origins o WHERE (o.table_name,o.physical_row_id)=(c.kind,c.id)) AND NOT EXISTS(SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) GROUP BY p.row_kind,p.source_row_id HAVING count(DISTINCT p.content_hash)>1 ORDER BY p.row_kind,p.source_row_id LIMIT 20",[user,workspace,*after]).fetchall()
            if not ids: return changed,blocked
            generation=db.execute('SELECT generation FROM archive_state WHERE singleton').fetchone()[0]
            values=db.execute("SELECT "+','.join('p.'+name for name in ('workspace_id','authorization_workspace_id','row_kind','source_row_id','encoding_v','content_hash','revision','previous_revision','state','author_user_id','author_device_id','authorization_epoch','signature'))+",c.body FROM remote.row_proofs p LEFT JOIN remote.row_conflicts c ON c.proof_id=p.id WHERE (p.row_kind,p.source_row_id) IN (SELECT json_extract_string(value,'$[0]'),json_extract_string(value,'$[1]') FROM json_each(?)) AND p.author_user_id=? AND NOT EXISTS(SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) QUALIFY row_number() OVER(PARTITION BY p.row_kind,p.source_row_id ORDER BY p.revision)<=65 ORDER BY p.row_kind,p.source_row_id,p.revision",[json.dumps(ids),user]).fetchall()
            groups,bodies,plans={key:list(group) for key,group in itertools.groupby(values,key=lambda v:(v[2],v[3]))},typed_logical_rows(db,[(kind,entity,entity,user,'active') for kind,entity in ids]),{}
            for kind,entity in ids:
                try:
                    values=groups[(kind,entity)]
                    required(len(values)<=64,ValueError('parser fork exceeds automatic reconciliation limit'))
                    native=bodies[(kind,entity,entity,user,'active')]
                    rows=[required(row if row is not None and digest(row)==v[5] else None,ValueError('parser fork body unavailable')) for v in values for row in [json.loads(v[-1]) if v[-1] else matching_logical_row(native,v[5])]]
                    try: merged=_parser_metadata_join([native,*rows])
                    except ValueError:
                        if kind!='messages': raise
                        merged=_source_message_join(db,[native,*rows],user)
                    plans[(kind,entity)]=(merged,[row_proof(cfg['device'],user,v[0],cfg['workspaces'][workspace]['epoch'],merged,v[6],workspace) for v in values])
                except ValueError as e: blocked[kind+':'+entity]=str(e)
                progress('planning '+kind+' metadata '+entity)
        if plans:
            with contextlib.closing(open_db(db_path,purpose='remote.parser.merge')) as db,_transaction(db),preserve_fact_heads(db,list(plans)):
                required(db.execute('SELECT generation FROM archive_state WHERE singleton').fetchone()[0]==generation,RuntimeError('Archive changed during parser metadata reconciliation; retry'))
                signer=cfg['controls'][workspace]['devices'][cfg['device']['id']]
                project_workspace_controls(db,next(w['controls'] for w in cfg['server_state']['workspaces'] if w['id']==workspace))
                project_attested_rows(db,[(row,proof) for row,proofs in plans.values() for proof in proofs],signer['root_public'],signer['certificate'])
                project_logical_rows(db,[(row,proof,digest(proof),True) for row,proofs in plans.values() for proof in [min(proofs,key=lambda p:p['revision'])]])
            changed+=len(plans)
            progress('reconciled parser metadata batch '+str(len(plans)))
        archive_yield(db_path)
        after=ids[-1]
def reconcile_provider_aliases(db_path,cfg,workspace,progress=None,state=None):
    data=Path(db_path).parent
    try:
        with operation_lock(data/".sync.lock","remote.alias.local",30) as local,operation_lock(data/"hook_inbox/.drain.lock","remote.alias.capture",30) as hooks:
            return _reconcile_provider_aliases(db_path,cfg,workspace,lambda stage:(local(stage),hooks(stage),progress and progress(stage)),state)
    finally:
        if data.name=='data' and Path(db_path).name=='convos.db' and any(p for pattern in ('*.json','*.work') for p in (data/'hook_inbox').glob(pattern)): wake_hooks(root=data.parent)
def _reconcile_provider_aliases(db_path,cfg,workspace,progress,state=None):
    groups,objects,user={},{},cfg["user"]
    with contextlib.closing(open_db(db_path,purpose="remote.alias.schema")) as db: init_schema(db)
    with contextlib.closing(open_db(db_path,True,purpose="remote.alias.plan")) as db: stored=[(oid,source,session,json.loads(members),canonical,json.loads(proof)) for oid,source,session,members,canonical,proof in db.execute("SELECT object_id,source,session_id,CAST(members AS VARCHAR),canonical_source_row_id,CAST(proof AS VARCHAR) FROM remote.provider_session_aliases WHERE author_user_id=? ORDER BY object_id,revision",(user,)).fetchall()]
    [objects.setdefault(row[0],[]).append(row) for row in stored]
    for object_id,values in objects.items():
        ancestors={a for value in values for a in value[5]['ancestors']}
        leaves=[value for value in values if value[5]['revision'] not in ancestors]
        key=(values[0][1],provider_session_key(values[0][1],values[0][2]))
        groups.setdefault(key,[]).append((object_id,leaves))
    result,controls,signer,backed_up={"changed":0,"settled":0,"blocked":{}},next(w["controls"] for w in cfg["server_state"]["workspaces"] if w["id"]==workspace),cfg["controls"][workspace]["devices"][cfg["device"]["id"]],False
    result["changed"],result["blocked"]=_reconcile_parser_heads(db_path,cfg,workspace,progress)
    cache_key=f'provider_alias_cache:{workspace}:{user}'
    cached=json.loads(row[0]) if state is not None and (row:=state.execute('SELECT value FROM meta WHERE key=?',[cache_key]).fetchone()) else {}
    with contextlib.closing(open_db(db_path,True,purpose='remote.alias.dependencies')) as db: fingerprints=_alias_fingerprints(db,user,groups) if state is not None else {}
    outcomes,checkpoint={},False
    for at,((source,session),aliases) in enumerate(groups.items()):
        if state is not None and checkpoint: (state.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',[cache_key,json.dumps(cached|outcomes,sort_keys=True,separators=(',',':'))]),state.commit())
        progress(f"provider aliases {at}/{len(groups)}")
        object_id,checkpoint='provider-session:'+digest([source,session]),False
        token=digest([fingerprints.get(object_id),controls,cfg['device']['id']])
        if state is not None and object_id in cached and cached[object_id][0]==token:
            previous=cached[object_id][1]
            if previous is None: result['settled']+=1
            else: result['blocked'][object_id]=previous
            outcomes[object_id]=cached[object_id]
            continue
        if any(len(leaves)!=1 for oid,leaves in aliases):
            result["blocked"][object_id]="provider alias proof is not converged"
            continue
        members=sorted({member for oid,leaves in aliases for member in leaves[0][3]})
        canonical=members[0]
        try:
            with contextlib.closing(open_db(db_path,True,purpose="remote.alias.members")) as db: member_heads,member_physical,active,member_native,binding=_alias_members(db,user,source,session,members,canonical)
            with contextlib.closing(open_db(db_path,True,purpose='remote.alias.present')) as db:
                present={m for m,p in member_physical.items() if db.execute('SELECT 1 FROM conversations WHERE id=?',[p]).fetchone()}
                history=_message_history(db,user,members)
            losers,moving,message_rows,attachment_paths=set(members)-{canonical},False,[],[]
            for generation,rows in _alias_pages(db_path,user,member_physical,progress=progress):
                for row,head,native,parent_map,path in rows:
                    if row["kind"]=="conversations" and row["id"] in active:
                        required(row["data"]["source"]==source and isinstance(metadata:=row["data"]["metadata"],dict) and provider_session_key(source,metadata.get("session_id"))==session,ValueError("provider alias exact evidence conflicts"))
                    if row['kind']=='attachments' and row['data']['body_hash']:
                        body=Path(path) if path else Path(db_path).parent/'attachments'/row['data']['body_hash']
                        required(body.is_file() and not body.is_symlink() and file_hash(body)==row['data']['body_hash'],ValueError(f"provider alias attachment body unavailable: {row['id']}"))
                        if not path: attachment_paths.append((parent_map.get(('attachments',row['id']),row['id'] if native else foreign_id(user,'attachments',row['id'])),body))
                    moving|=row["kind"] in ("messages","artifacts") and row["data"]["conversation_id"] in losers or digest(row)!=head["content_hash"]
                    if row['kind'] in ('conversations','messages'): message_rows.append((row,head,native,parent_map,path))
        except ValueError as e:
            result["blocked"][object_id]=str(e)
            continue
        if not moving and present=={canonical} and not binding and not attachment_paths and not _alias_message_plan(message_rows,source,members,history)[1]:
            result['settled'],outcomes[object_id],checkpoint=result['settled']+1,[token,None],True
            continue
        if not backed_up:
            with contextlib.closing(open_db(db_path,purpose="maintenance.remote.alias-backup")) as db: _migration_backup(db,"provider-alias-reconciliation")
            backed_up=True
        try:
            changed=bool(attachment_paths)
            if attachment_paths:
                with contextlib.closing(open_db(db_path,purpose='remote.alias.attachment-paths')) as db,_transaction(db): [set_attachment_path(db,physical,path) for physical,path in attachment_paths]
            for generation,values in _alias_pages(db_path,user,member_physical,progress=progress):
                rows=[({**row,"data":{**row["data"],"conversation_id":canonical}} if row["kind"] in ("messages","artifacts") else row,head,native,parent_map) for row,head,native,parent_map,path in values if row["kind"] in ("messages","artifacts") and row["data"]["conversation_id"] in losers or digest(row)!=head["content_hash"]]
                if not rows: continue
                proofs=[row_proof(cfg["device"],user,head["workspace"],cfg["workspaces"][workspace]["epoch"],row,head["revision"],workspace) for row,head,native,parent_map in rows]
                with contextlib.closing(open_db(db_path,purpose="remote.alias.write-page")) as db,_transaction(db),preserve_fact_heads(db,[(row["kind"],row["id"]) for row,head,native,parent_map in rows if native]):
                    (required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during provider alias reconciliation; retry")),project_workspace_controls(db,controls),project_logical_rows(db,[(row,proof,pid,native,parent_map) for (row,head,native,parent_map),proof,pid in zip(rows,proofs,_alias_proofs(db,cfg,workspace,[(row,proof) for (row,*_),proof in zip(rows,proofs)]))]))
                changed=True
            current=[item for generation,values in _alias_pages(db_path,user,member_physical,progress=progress) for item in values]
            revisions,lineage=_alias_message_plan(current,source,members,history)
            if lineage:
                row,head,native,parent_map,path=next(item for item in current if item[0]['kind']=='conversations' and item[0]['id']==canonical)
                metadata=row['data']['metadata']
                prior=metadata.get('convos_message_lineage',{}).get('records',[])
                replacements={value['old_id']:value['current_id'] for value in lineage}
                # Supersede reverse directions only within freshly proven equivalent groups; signed ancestors remain retained.
                prior=[value for value in prior if value['current_id'] not in replacements or replacements.get(value['old_id'],value['old_id'])!=replacements[value['current_id']]]
                records=sorted({canon(value):value for value in [*prior,*lineage]}.values(),key=lambda v:(v['old_id'],v['old_hash'],v['current_id'],v['current_hash']))
                carrier={**row,'data':{**row['data'],'metadata':{**metadata,'convos_message_lineage':{'v':1,'records':records}}}}
                with contextlib.closing(open_db(db_path,True,purpose='remote.alias.message-plan')) as db:
                    generation=db.execute('SELECT generation FROM archive_state WHERE singleton').fetchone()[0]
                    revisions+=_alias_edit_successors(db,user,revisions)
                revisions.append((carrier,head,native,parent_map,path))
                proofs=[row_proof(cfg['device'],user,head['workspace'],cfg['workspaces'][workspace]['epoch'],row,head['revision'],workspace) for row,head,native,parents,path in revisions]
                from . import _edit_evidence_project
                with contextlib.closing(open_db(db_path,True,purpose='remote.alias.message-plan')) as db: evidence_plan=_alias_evidence_successors(db,cfg,revisions,proofs)
                edit_evidence=[_edit_evidence_project(row,semantic_proof(cfg['root'],user,cfg['device']['id'],ws,cfg['workspaces'][ws]['epoch'],row,previous)) for ws,row,previous in evidence_plan]
                children=[(row,proof) for row,proof in zip(revisions,proofs) if row[0]['kind'] not in ('conversations','edit.observed')]
                facts={row[0]['id']:(row,proof) for row,proof in zip(revisions,proofs) if row[0]['kind']=='edit.observed'}
                # Publish retirement only after every reference and its signed evidence is durable.
                pages=[([*part,*[facts[row['id']] for (row,*_),proof in part if row['kind']=='file_edits' and row['id'] in facts]],[]) for i in range(0,len(children),ALIAS_WRITE_PAGE) for part in [children[i:i+ALIAS_WRITE_PAGE]]]+[([],edit_evidence[i:i+500]) for i in range(0,len(edit_evidence),500)]+[([(revisions[-1],proofs[-1])],[])]
                for index,(page,evidence) in enumerate(pages):
                    progress(f"provider message repair {index}/{len(pages)}")
                    with contextlib.closing(open_db(db_path,purpose='remote.alias.message-lineage')) as db,_transaction(db),preserve_fact_heads(db,[(row['kind'],parents.get(('file_edits' if row['kind']=='edit.observed' else row['kind'],row['id']),row['id'])) for (row,head,native,parents,path),proof in page]):
                        required(db.execute('SELECT generation FROM archive_state WHERE singleton').fetchone()[0]==generation,RuntimeError('Archive changed during provider message reconciliation; retry'))
                        pids=_alias_proofs(db,cfg,workspace,[(row[0],proof) for row,proof in page])
                        if page: project_logical_rows(db,[(row,proof,pid,native,parents) for ((row,head,native,parents,path),proof),pid in zip(page,pids)])
                        if evidence: project_file_edit_evidence_many(db,evidence)
                        generation=db.execute('SELECT generation FROM archive_state WHERE singleton').fetchone()[0]
                    archive_yield(db_path)
                changed=True
            with contextlib.closing(open_db(db_path,True,purpose="remote.alias.finish-plan")) as db:
                generation,(member_heads,member_physical,active,member_native,binding)=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],_alias_members(db,user,source,session,members,canonical)
                required(not db.execute("SELECT 1 FROM (SELECT conversation_id FROM messages UNION ALL SELECT conversation_id FROM artifacts) WHERE conversation_id IN (SELECT UNNEST(?)) LIMIT 1",([member_physical[m] for m in set(members)-{canonical}],)).fetchone(),RuntimeError("Provider alias rows remain; retry"))
            rows,proofs=(rows:=[(logical_row("conversations",identity=member,state="deleted"),member_heads[("conversations",member)],member_native[member],{("conversations",source_id):physical for source_id,physical in member_physical.items()}) for member in present-{canonical}]),[row_proof(cfg["device"],user,head["workspace"],cfg["workspaces"][workspace]["epoch"],row,head["revision"],workspace) for row,head,native,parent_map in rows]
            with contextlib.closing(open_db(db_path,purpose="remote.alias.finish")) as db,_transaction(db),preserve_fact_heads(db,[(row["kind"],row["id"]) for row,head,native,parent_map in rows if native]):
                (required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during provider alias reconciliation; retry")),project_workspace_controls(db,controls),project_logical_rows(db,[(row,proof,pid,native,parent_map) for (row,head,native,parent_map),proof,pid in zip(rows,proofs,_alias_proofs(db,cfg,workspace,[(row,proof) for (row,*_),proof in zip(rows,proofs)]))]),project_provider_bindings(db,source,session,member_physical[canonical],list(member_physical.values())))
            result["changed"]+=bool(changed or rows or binding)
        except duckdb.InterruptException: raise
        except Exception as e: result["blocked"][object_id]=str(e)
    if state is not None:
        for object_id,token in fingerprints.items():
            if object_id in outcomes: continue
            error=result['blocked'].get(object_id)
            if error and 'attachment body unavailable' in error: continue
            # Changed groups are rechecked once; generation-dependent results never survive a concurrent writer.
            if error or result['changed']==0: outcomes[object_id]=[digest([token,controls,cfg['device']['id']]),error]
        state.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',[cache_key,json.dumps(outcomes,sort_keys=True,separators=(',',':'))])
        state.commit()
    progress(f"provider aliases {len(groups)}/{len(groups)}")
    return result
def blob_replicas(db_path,cfg,workspace,records,keys,known=(),origins=(),origin_epochs=None,retained=True):
    if cfg["workspaces"][workspace]["kind"]!="personal": return []
    allowed={dict(zip(r["payload"]["columns"],r["payload"]["row"])).get("body_hash") for r in records if r["kind"]=="attachment.record" and r["payload"].get("state")!="deleted"}
    scopes=(workspace,*origins)
    marks=','.join('?'*len(scopes))
    with contextlib.closing(open_db(db_path,True,purpose="remote.blobs.read")) as db:
        rows=db.execute(f"SELECT a.path,b.content_hash,b.size,p.workspace_id,p.authorization_workspace_id,p.authorization_epoch,o.proof_id IS NOT NULL FROM attachments a JOIN attachment_bodies b ON b.attachment_id=a.id LEFT JOIN remote.row_origins o ON o.table_name='attachments' AND o.physical_row_id=a.id LEFT JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON p.row_kind='attachments' AND p.source_row_id=COALESCE(o.source_row_id,a.id) AND p.author_user_id=COALESCE(o.author_user_id,?) AND (o.proof_id IS NULL OR p.content_hash=q.content_hash) WHERE a.path IS NOT NULL AND p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision)",(cfg["user"],*scopes)).fetchall()
        if retained: rows+=db.execute(f"SELECT concat(?,'/',json_extract_string(c.body,'$.data.body_hash')),json_extract_string(c.body,'$.data.body_hash'),CAST(json_extract_string(c.body,'$.data.size') AS UINTEGER),p.workspace_id,p.authorization_workspace_id,p.authorization_epoch,TRUE FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.workspace_id IN ({marks}) AND p.row_kind='attachments' AND p.state='active'",(str(Path(db_path).parent/"attachments"),*scopes)).fetchall()
    out,seen=[],set()
    for path,body_hash,size,origin,authorization,authorized_epoch,imported in rows:
        if not retained and (imported or body_hash not in allowed) or not imported and body_hash not in allowed: continue
        epoch=authorized_epoch if authorization==workspace else (origin_epochs or {})[origin]
        if epoch not in keys: continue
        if (body_hash,epoch) in seen or seen.add((body_hash,epoch)): continue
        path=Path(path)
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=size or size>32*1024**2 or file_hash(path)!=body_hash: raise ValueError("retained attachment body is inconsistent")
        out.append(seal_blob(path.read_bytes(),workspace,epoch,keys[epoch],cfg["device"]["id"]))
    return out
def proof_signer(proof,workspace,controls):
    allowed={workspace,*[c["workspace"] for c in controls]}
    candidates=[{k:r[k] for k in ("user","root_public","device","certificate")} for c in controls if proof["workspace"] in allowed and c["workspace"]==proof["authorization_workspace"] and c["epoch"]==proof["authorization_epoch"] and proof["author_device_id"] in c["devices"] for r in [c["devices"][proof["author_device_id"]]]]
    return required(candidates[0] if candidates and all(c==candidates[0] for c in candidates) else None,ValueError("row proof authorization unavailable"))
def verified_replica(body,workspace,controls,user):
    row,proof,lineage=body["row"],body["proof"],body.get("lineage")
    if proof.get("kind")=="semantic.proof":
        if lineage: raise ValueError("semantic proof has row lineage")
        verify_semantic_proof(proof,row,proof["author_user_id"])
        authorized=[device for control in controls if (control["workspace"],control["epoch"])==(proof["workspace"],proof["authorization_epoch"]) for device in [control["devices"].get(proof["author_device_id"])] if device and (device["user"],device["root_public"])==(proof["author_user_id"],proof["root_public"])]
        required(authorized,ValueError("semantic proof authorization unavailable"))
        return row,proof,None,[]
    signer_=proof_signer(proof,workspace,controls)
    verify_row_proof(proof,row,signer_["certificate"],signer_["root_public"])
    verified=[]
    if lineage is not None:
        if not isinstance(lineage,list) or len(lineage)!=len({p.get("revision") for p in lineage}): raise ValueError("invalid row proof lineage")
        expected=proof["previous_revision"]
        for parent in lineage:
            parent_signer=proof_signer(parent,workspace,controls)
            verify_row_proof_header(parent,parent_signer["certificate"],parent_signer["root_public"])
            if (parent["revision"],parent["workspace"],parent["row_kind"],parent["row_id"],parent["author_user_id"])!=(expected,proof["workspace"],proof["row_kind"],proof["row_id"],proof["author_user_id"]): raise ValueError("invalid row proof lineage")
            verified.append((parent,parent_signer))
            expected=parent["previous_revision"]
        if expected is not None: raise ValueError("incomplete row proof lineage")
    return row,proof,signer_,verified
def temp_rows(db,name,columns,rows): db.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT x.* FROM UNNEST(from_json(?,?)) t(x)",(json.dumps([dict(zip(columns,row)) for row in rows]),json.dumps([{c:"VARCHAR" for c in columns}])))
def _edit_retry_work(db):
    seed=db.execute("SELECT after_proof_id FROM remote.edit_ready WHERE dependency_key=''").fetchone()[0]
    if seed!='done':
        rows=db.execute("SELECT p.id,'' FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.row_kind='edit.observed' AND p.id>? ORDER BY p.id LIMIT 500",[seed]).fetchall()
        if rows: return rows,[('',rows[-1][0] if len(rows)==500 else 'done')]
    rows=db.execute("SELECT d.proof_id,r.dependency_key FROM remote.edit_ready r JOIN remote.edit_dependencies d ON d.dependency_key=r.dependency_key WHERE r.dependency_key<>'' AND d.proof_id>r.after_proof_id ORDER BY r.dependency_key,d.proof_id LIMIT 500").fetchall()
    return rows,([('','done')] if seed!='done' else [])+list({key:pid for pid,key in rows}.items())
def retry_edit_replicas(db_path,local_user,local_device=None,root=None,progress=lambda stage:None):
    done=0
    while count:=apply_row_replicas(db_path,[],None,[],local_user=local_user,root=root,ready=False,local_device=local_device,retry=True):
        done+=count
        progress(f"retrying edit facts {done}")
        archive_yield(db_path)
def retry_own_replicas(db_path,local_user,local_device=None,progress=lambda stage:None):
    after=''
    while True:
        with contextlib.closing(open_db(db_path,True,purpose='remote.rows.retry-own')) as db:
            rows=db.execute("SELECT p.id,'' FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id LEFT JOIN remote.local_row_bases b ON (b.kind,b.entity,b.author)=(p.row_kind,p.source_row_id,p.author_user_id) WHERE p.author_user_id=? AND p.state='active' AND p.id>? AND b.revision IS DISTINCT FROM p.revision AND NOT EXISTS (SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision)) ORDER BY p.id LIMIT 500",[local_user,after]).fetchall()
        if not rows: return
        apply_row_replicas(db_path,[],None,[],local_user=local_user,local_device=local_device,ready=False,retry=rows)
        after=rows[-1][0]
        progress('retrying retained own updates')
        archive_yield(db_path)
def apply_row_replicas(db_path,bodies,workspace,controls,recover=None,local_user=None,db=None,root=None,ready=True,local_device=None,retry=False):
    if not bodies and not retry: return []
    if len(bodies)>500: return [value for at in range(0,len(bodies),500) for value in apply_row_replicas(db_path,bodies[at:at+500],workspace,controls,recover,local_user,db,root,ready,local_device)]
    values=[verified_replica(body,workspace,controls,local_user) for body in bodies]
    from ai_convos.cli import attachment_index
    attachment_paths={key:path for key in {(r['data']['body_hash'],r['data']['size']) for r,p,*_ in values if r['kind']=='attachments' and r['state']=='active' and r['data']['body_hash']} for path in [Path(db_path).parent/'attachments'/key[0]] if attachment_index(path,key[1])==key}
    if ready and Path(db_path).is_file() and any(value[1]["kind"]=="semantic.proof" for value in values):
        with contextlib.closing(open_db(db_path,purpose="schema.remote.rows")) as schema: ready=(init_schema(schema),False)[-1]
    semantic_ids,semantic=(semantic_ids:=[i for i,value in enumerate(values) if value[1]["kind"]=="semantic.proof"]),dict(zip(semantic_ids,bridge_accept_many(root,[(values[i][0],values[i][1]) for i in semantic_ids])))
    indexes=[i for i in range(len(values)) if i not in semantic]
    if not indexes and not retry: return list(semantic.values())
    names=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    proof=lambda values:{"v":1,"kind":"row.proof",**dict(zip(names,values))}
    own=db is None
    db=db or open_db(db_path,purpose="remote.rows.project")
    if own and ready and not semantic_ids: init_schema(db)
    try:
        with _transaction(db):
            items=[values[i] for i in indexes]
            work,advanced=(retry,[]) if isinstance(retry,list) else _edit_retry_work(db) if retry else ([],[])
            columns=("workspace","kind","row_id","author","revision")
            temp_rows(db,"incoming",columns,[tuple(p[k] for k in ("workspace","row_kind","row_id","author_user_id","revision")) for row,p,signer_,lineage in items])
            if work: db.execute("INSERT INTO incoming SELECT workspace_id,row_kind,source_row_id,author_user_id,revision FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))",[[pid for pid,key in work]])
            old={tuple(r) for r in db.execute("SELECT p.workspace_id,p.row_kind,p.source_row_id,p.author_user_id,p.revision FROM remote.row_proofs p JOIN incoming i ON (p.workspace_id,p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(i.workspace,i.kind,i.row_id,i.author,i.revision)").fetchall()}
            record_local_row_bases(db,[p for row,p,signer_,lineage in items if p["author_user_id"]==local_user and p["author_device_id"]==local_device],True)
            project_workspace_controls(db,controls)
            groups={}
            for row,head,head_signer,lineage in items:
                for p,signer_ in [*lineage,(head,head_signer)]: groups.setdefault((p["author_user_id"],p["author_device_id"]),(signer_,[]))[1].append(p)
            [project_row_proofs(db,proofs,signer_["root_public"],signer_["certificate"]) for signer_,proofs in groups.values()]
            _insert_pages(db,"remote.row_conflicts",[(digest(p),json.dumps(row,sort_keys=True,separators=(",",":"))) for row,p,signer_,lineage in items],("proof_id","body"),mode=" OR IGNORE")
            projected,chosen=[],{}
            fields="p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature"
            chains={}
            for r in db.execute(f"SELECT p.id,CAST(c.body AS VARCHAR),{fields} FROM remote.row_proofs p JOIN (SELECT DISTINCT kind,row_id,author FROM incoming) i ON (p.row_kind,p.source_row_id,p.author_user_id)=(i.kind,i.row_id,i.author) LEFT JOIN remote.row_conflicts c ON c.proof_id=p.id").fetchall(): chains.setdefault((r[4],r[5],r[11]),{})[r[8]]=(r[0],json.loads(r[1]) if r[1] else None,proof(r[2:]))
            bases={(kind,entity,author):revision for kind,entity,author,revision in db.execute("SELECT b.* FROM remote.local_row_bases b JOIN (SELECT DISTINCT kind,row_id,author FROM incoming) i ON (b.kind,b.entity,b.author)=(i.kind,i.row_id,i.author)").fetchall()}
            for scope,nodes in chains.items():
                leaves=set(nodes)-{v[2]["previous_revision"] for v in nodes.values() if v[2]["previous_revision"]}
                if leaves and len({nodes[r][2]['content_hash'] for r in leaves})==1:
                    def follows(revision):
                        seen=set()
                        while revision in nodes and revision not in seen:
                            if revision==bases.get(scope): return True
                            seen.add(revision)
                            revision=nodes[revision][2]['previous_revision']
                        return False
                    revision=min(leaves,key=lambda r:(not follows(r),nodes[r][1] is None,r))
                    pid,row,p=nodes[revision]
                    chosen[scope]=revision
                    if row is not None: projected.append((row,p,pid,p["author_user_id"]==local_user))
            pending=set()
            project_logical_rows(db,projected,defer=pending.add,advance_native=local_device if not recover else None)
            [set_attachment_path(db,row['id'] if p['author_user_id']==local_user else foreign_id(p['author_user_id'],'attachments',row['id']),attachment_paths[key],key) for row,p,*_ in values if row['kind']=='attachments' and row['state']=='active' and (key:=(row['data']['body_hash'],row['data']['size'])) in attachment_paths]
            resolved=[(row["kind"],row["id"],p["author_user_id"]) for row,p,pid,native in projected if pid not in pending]
            missing={scope:chains[scope][revision] for scope,revision in chosen.items() if chains[scope][revision][1] is None}
            physical=dict(db.execute("SELECT proof_id,physical_row_id FROM remote.row_origins WHERE proof_id IN (SELECT UNNEST(?)) UNION ALL SELECT proof_id,physical_entity FROM remote.provenance_origins WHERE proof_id IN (SELECT UNNEST(?))",[[pid for pid,row,p in missing.values()]]*2).fetchall()) if missing else {}
            claims={scope:(kind,physical.get(pid,source if user==local_user or kind in PROVENANCE-{'edit.observed','checkpoint.link'} else foreign_id(user,'file_edits' if kind=='edit.observed' else kind,source)),source,user,p['state']) for scope,(pid,row,p) in missing.items() for kind,source,user in [scope]}
            found=typed_logical_rows(db,claims.values()) if claims else {}
            resolved += [scope for scope,claim in claims.items() if matching_logical_row(found[claim],missing[scope][2]['content_hash']) is not None]
            if resolved:
                retired=[]
                for scope in resolved:
                    revision=chosen[scope]
                    while revision in chains[scope]:
                        retired.append((*scope,revision))
                        revision=chains[scope][revision][2]["previous_revision"]
                retire_row_bodies(db,retired)
            dependency=lambda kind,entity,author:digest([kind,author if kind=='file_edits' else None,entity])
            waiting=[(dependency(kind,entity,p['author_user_id']),pid) for row,p,pid,native in projected if pid in pending and row['kind']=='edit.observed' for kind,entity in (('file_edits',row['id']),('file.observed',row['data']['file']))]
            again=project_edit_dependencies(db,waiting,[dependency(row['kind'],row['id'],p['author_user_id']) for row,p,pid,native in projected if pid not in pending and row['kind'] in ('file_edits','file.observed')],[pid for pid,key in work]+[digest(p) for row,p,signer_,lineage in items if row['kind']=='edit.observed'],advanced)
        results=[(*((p[k] for k in ("workspace","row_kind","row_id","author_user_id"))),p["revision"]) not in old and chosen.get(tuple(p[k] for k in ("row_kind","row_id","author_user_id")))==p["revision"] for row,p,signer_,lineage in items]
        accepted=semantic|dict(zip(indexes,results))
        result=len(work) if retry else [accepted[i] for i in range(len(values))]
    finally:
        if own: db.close()
    if own and not retry and again: apply_row_replicas(db_path,[],None,[],local_user=local_user,root=root,ready=False,local_device=local_device,retry=True)
    return result
def author_user(value,authors): return required((authors or {}).get(value["author"]),ValueError("verified author user required"))
def foreign_id(author_user,table,old): return digest(f"{author_user}:{table}:{old}")[:16] if old else old
def sequence(state,workspace,value):
    old=state.execute("SELECT event FROM event_sequences WHERE workspace=? AND author=? AND seq=?",(workspace,value["author"],value["seq"])).fetchone()
    if old and old[0]!=value["id"]: raise ValueError("device sequence replay")
    if old: return True
    before=state.execute("SELECT event FROM event_sequences WHERE workspace=? AND author=? AND seq=?",(workspace,value["author"],value["seq"]-1)).fetchone()
    after=state.execute("SELECT event FROM event_sequences WHERE workspace=? AND author=? AND seq=?",(workspace,value["author"],value["seq"]+1)).fetchone()
    gap=after and state.execute("SELECT parents FROM sequence_gaps WHERE workspace=? AND author=? AND seq=?",(workspace,value["author"],value["seq"]+1)).fetchone()
    if value["seq"]==1 and value["parents"] or before and before[0] not in value["parents"] or after and (not gap or value["id"] not in json.loads(gap[0])): raise ValueError("device event chain mismatch")
    state.execute("INSERT INTO event_sequences VALUES (?,?,?,?)",(workspace,value["author"],value["seq"],value["id"]))
    if value["seq"]>1 and not before: state.execute("INSERT INTO sequence_gaps VALUES (?,?,?,?)",(workspace,value["author"],value["seq"],json.dumps(value["parents"])))
    if after: state.execute("DELETE FROM sequence_gaps WHERE workspace=? AND author=? AND seq=?",(workspace,value["author"],value["seq"]+1))
    return True
def reset_history(state,workspace,boundary):
    [state.execute(f"DELETE FROM {table} WHERE workspace=?",(workspace,)) for table in ("receipts","cursors","lazy_events","deferred_events","event_sequences","sequence_gaps")]
    [state.execute("INSERT INTO event_sequences VALUES (?,?,?,?)",(workspace,author,head["seq"],head["event"])) for author,head in boundary["heads"].items()]
def verify_history(state,workspace,controls,start):
    gaps=state.execute("SELECT author,seq FROM sequence_gaps WHERE workspace=? ORDER BY author,seq LIMIT 1",(workspace,)).fetchone()
    if gaps: raise ValueError(f"required event sequence is incomplete at {gaps[0]}:{gaps[1]}")
    if deferred:=state.execute("SELECT kind,payload_v FROM deferred_events WHERE workspace=? AND required=1 ORDER BY cursor LIMIT 1",(workspace,)).fetchone(): raise ValueError(f"required event is unsupported: {deferred[0]} payload_v={deferred[1]}")
    for control in controls:
        if control["boundary"]["epoch"]>=start:
            for author,head in control["boundary"]["heads"].items():
                if (state.execute("SELECT event FROM event_sequences WHERE workspace=? AND author=? AND seq=?",(workspace,author,head["seq"])).fetchone() or [None])[0]!=head["event"]: raise ValueError("signed history checkpoint is incomplete")
def project(db_path,state,value,workspace,local_device=None,db=None,root=None,batch=False,authors=None,recover=None,local_user=None):
    if event_support(value)!="supported": return False
    if value["kind"]=="workspace.policy":
        p=value["payload"]
        if value["payload_v"]==2:
            if set(p)!={"row","proof"} or value["entity"]!=f"policy:repository:{p['row']['data']['value']}": raise ValueError("invalid repository policy")
            sharing_object(state,workspace,p["row"],p["proof"],authors)
        else:
            if set(p)!={"kind","value"} or not all(isinstance(p[k],str) for k in p): raise ValueError("invalid workspace policy")
            state.execute("INSERT OR REPLACE INTO policies VALUES (?,?,?,?,?)",(workspace,author_user(value,authors),p["kind"],p["value"],None))
            state.execute("DELETE FROM meta WHERE key=?",(f"core_generation:{workspace}",))
        batch or state.commit()
        return True
    if value["kind"]=="workspace.preference":
        p=value["payload"]
        if set(p)!={"row","proof"} or value["entity"]!=p["row"]["id"]: raise ValueError("invalid sharing preference")
        sharing_object(state,workspace,p["row"],p["proof"],authors)
        batch or state.commit()
        return True
    if value["kind"] not in PROVENANCE: return False
    user=author_user(value,authors)
    owned,native=bool(recover and user==local_user),bool(recover and user==local_user and recover=="native")
    if owned and recover=="adopt": return True
    if value["author"]==local_device and not owned: return True
    own=db is None
    if own:
        Path(db_path).parent.mkdir(parents=True,exist_ok=True)
        db=open_db(db_path,purpose="remote.provenance.project")
        init_schema(db)
    try:
        if own:
            with _transaction(db): return project_provenance(db,value,lambda table,old:old if native else foreign_id(user,table,old))
        return project_provenance(db,value,lambda table,old:old if native else foreign_id(user,table,old))
    finally:
        if own: db.close()
def project_many(db_path,state,items,local_device=None,root=None,commit=True,authors=None,recover=None,local_user=None,ready=True):
    records=any(v["kind"] in PROVENANCE and (v["author"]!=local_device or recover and author_user(v,authors)==local_user) for _,v in items)
    db=None
    if records:
        Path(db_path).parent.mkdir(parents=True,exist_ok=True)
        db=open_db(db_path,purpose="remote.provenance.project_many")
        if ready: init_schema(db)
    try:
        with _transaction(db) if db else contextlib.nullcontext(): [project(db_path,state,v,ws,local_device,db,root,True,authors,recover,local_user) for ws,v in items]
        commit and state.commit()
    except BaseException:
        state.rollback()
        raise
    finally:
        if db: db.close()
    return len(items)
