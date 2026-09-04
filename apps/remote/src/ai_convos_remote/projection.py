"""Portable record/event projection. The immutable relay ledger can rebuild every local view."""
import contextlib, duckdb, hashlib, json, os, shutil, sqlite3, time
from datetime import date, datetime
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path

from ai_convos.cli import ARCHIVE_COLUMNS as COLUMNS, PROVENANCE_KINDS as PROVENANCE, _insert_pages, _migration_backup, _transaction, archive_yield, index_attachment_body, init_schema, open_db, project_logical_rows, project_provenance, project_provider_bindings, project_row_proofs, project_workspace_controls, provenance_records, required, set_attachment_path
from .control import verify_state
from .migrations import migrate_state
from .protocol import digest, fingerprint, logical_fact, logical_row, row_proof, seal_blob, seal_replica, semantic_proof, verify_row_proof, verify_row_proof_header, verify_semantic_proof

STATE_VERSION="4"
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
FKS={"messages":(("conversation_id","conversations"),("parent_id","messages")),"tool_calls":(("message_id","messages"),),"attachments":(("message_id","messages"),),"artifacts":(("conversation_id","conversations"),),"file_edits":(("message_id","messages"),)}
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
def audit_rows(db_path,page=5000,progress=None):
    sql="SELECT * FROM (SELECT o.table_name kind,o.physical_row_id physical,o.source_row_id,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id UNION ALL SELECT o.kind,o.physical_entity,o.source_entity,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.provenance_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id) WHERE kind>? OR kind=? AND (physical>? OR physical=? AND COALESCE(proof_id,'')>?) ORDER BY kind,physical,COALESCE(proof_id,'') LIMIT ?"
    origins,after,generation=[],("","",""),None
    while True:
        with contextlib.closing(open_db(db_path,True,purpose="remote.audit.rows")) as db:
            current,generation=(current:=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]),current if generation is None else (required(current==generation,RuntimeError("Archive changed during Remote audit; retry")),generation)[1]
            rows=db.execute(sql,(after[0],after[0],after[1],after[1],after[2],page)).fetchall()
        if not rows: break
        origins,after=origins+rows,(rows[-1][0],rows[-1][1],rows[-1][4] or "")
        (progress and progress(f"audit inventory {len(origins)}"),archive_yield(db_path))
    mapped,tables,examples={(kind,physical,user):source for kind,physical,source,user,pid,expected,state in origins if kind in COLUMNS},{},[]
    for at in range(0,len(origins),page):
        batch,found,facts=origins[at:at+page],{},{}
        with contextlib.closing(open_db(db_path,True,purpose="remote.audit.rows")) as db:
            required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during Remote audit; retry"))
            for table in COLUMNS:
                if not (ids:=[physical for kind,physical,source,user,pid,expected,state in batch if kind==table]): continue
                cur=db.execute(f"SELECT * EXCLUDE (embedding) FROM {table} WHERE id IN (SELECT UNNEST(?))",[ids]) if table=="messages" else db.execute("SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id WHERE a.id IN (SELECT UNNEST(?))",[ids]) if table=="attachments" else db.execute(f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",[ids])
                cols=[d[0] for d in cur.description]
                found.update(((table,r[0]),(cols,list(map(clean,r)))) for r in cur.fetchall())
            facts={(r["kind"],r["entity"]):r for r in provenance_records(db,{(kind,physical) for kind,physical,source,user,pid,expected,state in batch if kind in PROVENANCE})}
        for kind,physical,source,user,pid,expected,state in batch:
            if kind in COLUMNS and state=="deleted": row=logical_row(kind,identity=source,state="deleted")
            elif kind in COLUMNS and (value:=found.get((kind,physical))):
                cols,raw=value
                parents=dict(FKS.get(kind,()))
                row=logical_row(kind,cols,[source if column=="id" else mapped.get((parents[column],item,user),item) if column in parents else item for column,item in zip(cols,raw)],source)
            elif kind in PROVENANCE and (record:=facts.get((kind,physical))):
                payload={**record["payload"],**({"id":source} if kind!="checkpoint.link" else {})}
                for field,parent in (("turn","messages"),("edit","file_edits")):
                    if field in payload and (source_id:=mapped.get((parent,payload[field],user))): payload[field]=source_id
                row=logical_fact({**record,"entity":source,"payload":payload})
            else: row=None
            projection=row is not None and expected is not None and digest(row)==expected
            stat=tables.setdefault(kind,dict(origins=0,projection_match=0,projection_mismatch=0,projection_missing=0,proof_missing=0))
            for key,value in (("origins",1),("proof_missing",expected is None),("projection_missing",row is None),("projection_match",projection),("projection_mismatch",row is not None and expected is not None and not projection)): stat[key]+=value
            if len(examples)<20 and not projection: examples.append(dict(kind=kind,id=source,projection="missing" if row is None else "mismatch"))
        (progress and progress(f"audit rows {min(at+page,len(origins))}"),archive_yield(db_path))
    return (lambda keys:dict(totals={key:sum(value[key] for value in tables.values()) for key in keys},tables=tables,examples=examples))(next(iter(tables.values())).keys() if tables else ())
def event_support(value):
    if not isinstance(kind:=value["kind"],str) or not isinstance(version:=value["payload_v"],int) or isinstance(version,bool) or version<1: raise ValueError("invalid event schema")
    return "supported" if (kind,version) in CORE_EVENTS else "required"
def bridge_records(root,cfg,workspace,kind,archive=True,changes=None): return [record for bridge in bridges() if archive or bridge.get("source")!="archive" for record in (bridge["delta"](root,cfg["user"],workspace,kind,changes) if changes is not None and "delta" in bridge else bridge["records"](root,cfg["user"],workspace,kind))]
def bridge_stamp(root): return digest({kind:(bridge["v"],bridge["schema"]) for bridge in bridges() for kind in bridge["objects"]})
def bridge_state(root,cfg,workspace,kind,generation): return digest((generation,bridge_stamp(root),bridge_records(root,cfg,workspace,kind,False)))
def bridge_accept(root,row,proof,project=True): return bool((found:=[bridge for bridge in bridges() if row["kind"] in bridge["objects"]]) and found[0]["accept"](root,row,proof,project))
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
    return [seal_replica(value["row"],value["proof"],workspace,cfg["workspaces"][workspace]["epoch"],key_,cfg["device"]["id"]) for value,replica in values if replica not in present]
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
        target,imported=(target:="a.id" if table=="attachments" else "x.id"),{r[0] for r in core.execute(f"SELECT physical_row_id FROM remote.row_origins WHERE table_name=? AND physical_row_id IN ({TEXT_IDS})",(table,packed(wanted))).fetchall()} if changes is not None else set()
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
def _under(path,cwd,roots):
    p=Path(path)
    p=(Path(cwd)/p if not p.is_absolute() and cwd else p).expanduser().resolve()
    return any(p.is_relative_to(root) for root in roots)
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
    where=" WHERE c.id IN (SELECT UNNEST(?))" if candidates is not None else ""
    args=[list(candidates)] if candidates is not None else []
    rows=core.execute("SELECT s.route,m.conversation_id,s.repository FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN provenance.file_edit_scopes s ON s.file_edit_id=fe.id JOIN messages m ON m.id=fe.message_id JOIN conversations c ON c.id=m.conversation_id"+where,args).fetchall()
    cwd_rows=core.execute("SELECT c.id,s.cwd,s.repository FROM conversations c JOIN provenance.conversation_scopes s ON s.conversation=c.id"+where,args).fetchall()
    return ({cid for route,cid,repo in rows if repo in repositories or route and any(Path(route).is_relative_to(root) for root in roots)} if "edit" in match else set())|({cid for cid,cwd,repo in cwd_rows if repo in repositories or cwd and any(Path(cwd).is_relative_to(root) for root in roots)} if "cwd" in match else set())
def scan(core,graph,kind="personal",repositories=(),roots=(),changes=None,workspace=None,new_scope=None,match=("cwd","edit"),user=None,selected=None):
    local={tuple(r) for r in core.execute("SELECT kind,entity FROM provenance.local_facts"+(f" WHERE entity IN ({TEXT_IDS})" if facts is not None else ""),[packed(r[1] for r in facts)] if facts is not None else []).fetchall()} if (facts:=None if changes is None else {r for r in changes if r[0] in PROVENANCE}) is None or facts else set()
    all_provenance=[clean(r) for r in provenance_records(core,facts) if (r["kind"],r["entity"]) in local] if local else []
    prior={r[0] for r in graph.execute("SELECT conversation FROM team_scopes WHERE workspace=?",(workspace,)).fetchall()} if kind=="team" and workspace and changes is not None else set()
    admitted={r[0] for r in core.execute("SELECT source_row_id FROM remote.row_proofs WHERE authorization_workspace_id=? AND author_user_id=? AND row_kind='conversations' AND state='active'",(workspace,user)).fetchall()} if kind=="team" and workspace and user else set()
    changed={table:{entity for name,entity in changes or () if name==table} for table in TABLES.values()}
    candidates=changed["conversations"]|{r[0] for r in core.execute("SELECT conversation_id FROM messages WHERE id IN (SELECT UNNEST(?))",[list(changed["messages"])]).fetchall()}|{r[0] for r in core.execute("SELECT m.conversation_id FROM messages m JOIN (SELECT message_id FROM tool_calls WHERE id IN (SELECT UNNEST(?)) UNION SELECT message_id FROM attachments WHERE id IN (SELECT UNNEST(?)) UNION SELECT message_id FROM file_edits WHERE id IN (SELECT UNNEST(?))) x ON x.message_id=m.id",[list(changed["tool_calls"]),list(changed["attachments"]),list(changed["file_edits"])]).fetchall()}|{r[0] for r in core.execute("SELECT conversation_id FROM artifacts WHERE id IN (SELECT UNNEST(?))",[list(changed["artifacts"])]).fetchall()} if kind=="team" and changes is not None else None
    convs=set(selected) if selected is not None else (admitted|prior|_team_scope(core,all_provenance,set(repositories),roots,candidates,match)) if kind=="team" else set()
    if workspace and new_scope is not None: new_scope.update(convs)
    provenance=[r for r in all_provenance if changes is None or (r["kind"],r["entity"]) in changes]
    records=_records(core,graph,kind=="personal",changes)
    edit_paths={r["payload"]["id"]:r["payload"]["file"] for r in provenance if r["kind"]=="edit.observed"} if kind=="personal" else {r[0]:r[1] for r in core.execute("SELECT x.file_edit_id,f.path FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id JOIN provenance.local_facts e ON (e.kind,e.entity)=('edit.observed',x.file_edit_id) JOIN provenance.local_facts l ON (l.kind,l.entity)=('file.observed',f.id) WHERE x.file_edit_id IN (SELECT UNNEST(?))",[[r["payload"]["row"][0] for r in records if r["kind"]=="file_edit.record" and r["payload"].get("state")!="deleted"]]).fetchall()}
    file_paths={r["payload"]["id"]:r["payload"]["path"] for r in provenance if r["kind"]=="file.observed"}
    for r in records:
        if r["kind"]=="file_edit.record" and r["payload"].get("state")!="deleted": r["payload"]["row"][2]=edit_paths.get(r["payload"]["row"][0]) if kind=="team" else file_paths.get(fid) if (fid:=edit_paths.get(r["payload"]["row"][0])) else r["payload"]["row"][2]
    if kind=="personal" or selected is not None: return records+provenance
    keep=[]
    parents={r["payload"]["row"][1] for r in records if r["payload"]["table"] in ("tool_calls","attachments","file_edits") and r["payload"].get("state")!="deleted"}
    msg_convs=dict(core.execute("SELECT id,conversation_id FROM messages WHERE id IN (SELECT UNNEST(?))",[list(parents)]).fetchall()) if parents else {}
    edits={r[0] for r in core.execute("SELECT fe.id FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN messages m ON m.id=fe.message_id WHERE m.conversation_id IN (SELECT UNNEST(?))",[list(convs)]).fetchall()}
    shared=set(core.execute("SELECT row_kind,source_row_id FROM remote.row_proofs WHERE authorization_workspace_id=?",(workspace,)).fetchall()) if workspace else set()
    for r in records:
        table,row=r["payload"]["table"],r["payload"]["row"] if "row" in r["payload"] else [r["payload"]["id"]]
        if r["payload"].get("state")=="deleted" and (table,row[0]) in shared or table=="conversations" and row[0] in convs or len(row)>1 and (table=="messages" and row[1] in convs or table in ("tool_calls","attachments") and msg_convs.get(row[1]) in convs or table=="file_edits" and row[0] in edits or table=="artifacts" and row[1] in convs): keep.append(r)
    allowed_files={r["payload"]["file"] for r in all_provenance if r["kind"]=="edit.observed" and r["payload"]["id"] in edits}
    allowed_repos={r["payload"]["repository"] for r in all_provenance if r["kind"]=="edit.observed" and r["payload"]["id"] in edits}
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
    out,done,prior,scope=[],0,{r[0] for r in graph.execute("SELECT conversation FROM team_scopes WHERE workspace=?",(workspace,)).fetchall()} if kind=="team" and workspace else set(),new_scope if new_scope is not None else set()
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
    if new:=scope-prior:
        after=("","")
        while True:
            with contextlib.closing(open_db(db_path,True,purpose="remote.scan.team-page")) as core:
                changes=_team_page(core,new,after,page)
                batch=scan(core,graph,kind,repositories,roots,set(changes),workspace,None,match,user,new) if changes else []
            out+=batch
            if not changes: break
            after=changes[-1]
            progress and progress(f"scanning admitted conversations {after[0]}")
            archive_yield(db_path)
    return list({(r["kind"],r["entity"]):r for r in out}.values())
def _store_proofs(db_path,proofs,signer,controls):
    with contextlib.closing(open_db(db_path,purpose="remote.attest.write")) as db,_transaction(db):
        project_workspace_controls(db,controls)
        project_row_proofs(db,proofs,signer["root_public"],signer["certificate"])
    archive_yield(db_path)
def attest_rows(db_path,cfg,workspace,records,origins=()):
    controls,device,signer=next(w["controls"] for w in cfg["server_state"]["workspaces"] if w["id"]==workspace),cfg["device"],cfg["controls"][workspace]["devices"][cfg["device"]["id"]]
    wanted={value for r in records if r["kind"] in TABLES and r["payload"].get("state")!="deleted" for column,value in zip(r["payload"]["columns"],r["payload"]["row"]) if value is not None and (column=="id" or column in dict(FKS.get(r["payload"]["table"],())))}
    with contextlib.closing(open_db(db_path,True,purpose="remote.attest.plan")) as db: aliases={(table,physical):source for table,physical,source in db.execute(f"SELECT table_name,physical_row_id,source_row_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN ({TEXT_IDS})",(cfg["user"],packed(wanted))).fetchall()}
    selected=[_logical_record(r,aliases) for r in records if r["kind"] in SIGNED]
    rows=[signed_row(r) if r["kind"] in TABLES else logical_fact(r) for r in selected]
    scopes,ids=(workspace,*origins),[r["id"] for r in rows]
    with contextlib.closing(open_db(db_path,True,purpose="remote.attest.heads")) as db: found=db.execute(f"SELECT DISTINCT p.workspace_id,p.row_kind,p.source_row_id,p.revision,p.content_hash FROM remote.row_proofs p WHERE p.workspace_id IN ({','.join('?'*len(scopes))}) AND p.author_user_id=? AND p.source_row_id IN ({TEXT_IDS}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision)",(*scopes,cfg["user"],packed(ids))).fetchall() if rows else []
    heads={}
    [heads.setdefault((r[1],r[2]),{}).setdefault(r[3],(r[0],r[3],r[4])) for r in sorted(found,key=lambda r:(r[0]!=workspace,r[0]))]
    proofs=[]
    for row in rows:
        prior,current=list(heads.get((row["kind"],row["id"]),{}).values()),digest(row)
        if any(h[2]==current for h in prior): continue
        if len(prior)>1: raise ValueError(f"row revision conflict: {row['kind']}:{row['id']}")
        proofs.append(row_proof(device,cfg["user"],prior[0][0] if prior else workspace,cfg["workspaces"][workspace]["epoch"],row,prior[0][1] if prior else None,workspace,current))
    [_store_proofs(db_path,proofs[i:i+500],signer,controls) for i in range(0,len(proofs),500)]
    return len(proofs)
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
    keep=lambda row,p,content_hash=None:bodies.setdefault(digest(p),(row,p,content_hash))
    try:
        scopes=(workspace,*origins)
        marks=','.join('?'*len(scopes))
        prepared=[(signed_row(r) if r["kind"] in TABLES else logical_fact(r)) for r in records if r["kind"] in SIGNED]
        local={}
        [local.setdefault((r[2],r[3],r[5]),{}).setdefault(r[6],proof(r)) for r in sorted(db.execute(f"SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE workspace_id IN ({marks}) AND author_user_id=? AND source_row_id IN ({TEXT_IDS}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision)",(*scopes,cfg["user"],packed(row["id"] for row in prepared))).fetchall() if prepared else [],key=lambda r:(r[0]!=workspace,r[0],r[10],r[12]))]
        for row in prepared:
            values=list(local.get((row["kind"],row["id"],digest(row)),{}).values())
            if len(values)!=1: raise ValueError(f"current row proof unavailable: {row['kind']}:{row['id']}")
            keep(row,values[0],values[0]["content_hash"])
        for values in (db.execute(f"SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE workspace_id IN ({marks}) AND author_user_id=? AND state='deleted' AND row_kind IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,cfg["user"],list(TABLES.values()),*([packed(only)] if only else []))).fetchall() if retained else []):
            p=proof(values)
            keep(logical_row(p["row_kind"],identity=p["row_id"],state="deleted"),p)
        imported=db.execute(f"SELECT o.table_name,o.physical_row_id,o.source_row_id,o.author_user_id,p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature FROM remote.row_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.table_name,o.source_row_id,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,*([packed(only)] if only else []))).fetchall() if retained else []
        mapped={(r[0],r[1],r[3]):r[2] for r in imported}
        for table in TABLES.values():
            selected=[r for r in imported if r[0]==table]
            ids=[r[1] for r in selected]
            cur=db.execute(f"SELECT * EXCLUDE (embedding) FROM {table} WHERE id IN (SELECT UNNEST(?))",[ids]) if table=="messages" else db.execute("SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id WHERE a.id IN (SELECT UNNEST(?))",[ids]) if table=="attachments" else db.execute(f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",[ids])
            cols=[d[0] for d in cur.description]
            raws={r[0]:list(map(clean,r)) for r in cur.fetchall()}
            for column,parent in FKS.get(table,()):
                if refs:={(raw[cols.index(column)]) for raw in raws.values() if raw[cols.index(column)] is not None}: mapped.update({(parent,physical,user):source for physical,source,user in db.execute("SELECT physical_row_id,source_row_id,author_user_id FROM remote.row_origins WHERE table_name=? AND physical_row_id IN (SELECT UNNEST(?))",(parent,list(refs))).fetchall()})
            for name,physical,source,user,*values in selected:
                p,raw=proof(values),raws.get(physical)
                if p["state"]=="deleted": row=logical_row(table,identity=source,state="deleted")
                elif raw:
                    parents=dict(FKS.get(table,()))
                    raw=[source if column=="id" else mapped.get((parents[column],value,user),value) if column in parents else value for column,value in zip(cols,raw)]
                    row=logical_row(table,cols,raw,source)
                else: continue
                keep(row,p)
        imported_facts=db.execute(f"SELECT o.kind,o.physical_entity,o.source_entity,o.author_user_id,p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature FROM remote.provenance_origins o JOIN remote.row_proofs q ON q.id=o.proof_id JOIN remote.row_proofs p ON (p.row_kind,p.source_row_id,p.author_user_id,p.content_hash)=(o.kind,o.source_entity,o.author_user_id,q.content_hash) WHERE p.workspace_id IN ({marks}) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision){only_sql}",(*scopes,*([packed(only)] if only else []))).fetchall() if retained else []
        facts={(r["kind"],r["entity"]):r for r in provenance_records(db,{(r[0],r[1]) for r in imported_facts})}
        for field,parent in (("turn","messages"),("edit","file_edits")):
            if refs:={record["payload"][field] for kind,physical,source,user,*values in imported_facts if (record:=facts.get((kind,physical))) and field in record["payload"]}: mapped.update({(parent,physical,user):source for physical,source,user in db.execute("SELECT physical_row_id,source_row_id,author_user_id FROM remote.row_origins WHERE table_name=? AND physical_row_id IN (SELECT UNNEST(?))",(parent,list(refs))).fetchall()})
        for kind,physical,source,user,*values in imported_facts:
            p,record=proof(values),facts.get((kind,physical))
            if not record: continue
            payload={**record["payload"],**({"id":source} if kind!="checkpoint.link" else {})}
            for field,parent in (("turn","messages"),("edit","file_edits")):
                if field in payload and (source_id:=mapped.get((parent,payload[field],user))): payload[field]=source_id
            keep(logical_fact({**record,"entity":source,"payload":payload}),p)
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
            return seal_replica(row,p,workspace,epoch,keys[epoch],cfg["device"]["id"],content_hash,lineage(p) if p["kind"]=="row.proof" else ())
        return [env for row,p,content_hash,epoch,replica in candidates if (env:=seal(row,p,content_hash,epoch))]
    finally: db and db.close()
def _proof(values):
    return {"v":1,"kind":"row.proof",**dict(zip(PROOF_FIELDS,values))}
def _heads(db,user,ids):
    out={}
    for table,sources in ids.items():
        if not sources: continue
        rows=db.execute("SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs p WHERE author_user_id=? AND row_kind=? AND source_row_id IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE (c.row_kind,c.source_row_id,c.author_user_id)=(p.row_kind,p.source_row_id,p.author_user_id) AND c.previous_revision=p.revision) ORDER BY workspace_id",(user,table,list(sources))).fetchall()
        for values in rows:
            out.setdefault((table,values[3]),{})[values[6]]=_proof(values)
    missing=[(table,source) for table,sources in ids.items() for source in sources if (table,source) not in out]
    forks=[key for key,values in out.items() if len(values)!=1]
    if missing or forks: raise ValueError(f"provider alias row proof unavailable or forked: {(missing or forks)[0]}")
    return {key:next(iter(values.values())) for key,values in out.items()}
def _alias_members(db,user,source,session,members,canonical):
    member_heads,origin_rows=_heads(db,user,{"conversations":set(members)}),db.execute("SELECT physical_row_id,source_row_id FROM remote.row_origins WHERE table_name='conversations' AND author_user_id=? AND source_row_id IN (SELECT UNNEST(?))",(user,members)).fetchall()
    origin_members={}
    [origin_members.setdefault(logical,set()).add(physical) for physical,logical in origin_rows]
    local,member_physical=(local:={r[0] for r in db.execute("SELECT id FROM conversations WHERE id IN (SELECT UNNEST(?))",(members,)).fetchall()}),{member:required(next(iter(found)) if len(found)==1 else None,ValueError(f"provider alias member projection is ambiguous: {member}")) for member in members for found in [{*origin_members.get(member,set()),*({member} if member in local else set())}] if found}
    active=(required(canonical in (active:={member for member in members if member_heads[("conversations",member)]["state"]=="active"}) and active<=set(member_physical),ValueError("provider alias active body unavailable")),active)[-1]
    member_physical|={member:next(iter(origin_members.get(member,{member}))) for member in set(members)-set(member_physical)}
    return member_heads,member_physical,active,{member:member not in origin_members for member in members},not (binding:=db.execute("SELECT conversation_id FROM provider_sessions WHERE source=? AND session_id=?",(source,session)).fetchone()) or binding[0]!=member_physical[canonical]
def _alias_page(db,user,member_physical,after,page=500):
    members,query=list(member_physical.values()),"""WITH selected AS (SELECT 'conversations' kind,id FROM conversations WHERE id IN (SELECT UNNEST(?)) UNION SELECT 'messages',id FROM messages WHERE conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'tool_calls',x.id FROM tool_calls x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'attachments',x.id FROM attachments x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'file_edits',x.id FROM file_edits x JOIN messages m ON m.id=x.message_id WHERE m.conversation_id IN (SELECT UNNEST(?)) UNION SELECT 'artifacts',id FROM artifacts WHERE conversation_id IN (SELECT UNNEST(?))) SELECT kind,id FROM selected WHERE kind>? OR kind=? AND id>? ORDER BY kind,id LIMIT ?"""
    keys=db.execute(query,(*([members]*6),after[0],after[0],after[1],page)).fetchall()
    if not keys: return [],after
    selected,raws=((selected:={table:{row_id for kind,row_id in keys if kind==table} for table in COLUMNS}),{(table,values[0]):(columns,values) for table,physical_ids in selected.items() if physical_ids for cur in [db.execute("SELECT * EXCLUDE (embedding) FROM messages WHERE id IN (SELECT UNNEST(?))" if table=="messages" else "SELECT a.*,b.content_hash body_hash FROM attachments a LEFT JOIN attachment_bodies b ON b.attachment_id=a.id WHERE a.id IN (SELECT UNNEST(?))" if table=="attachments" else f"SELECT * FROM {table} WHERE id IN (SELECT UNNEST(?))",(list(physical_ids),))] for columns in [[d[0] for d in cur.description]] for values in cur.fetchall()})
    origins={(table,physical):(logical,workspace) for table,physical,logical,workspace in db.execute("SELECT table_name,physical_row_id,source_row_id,workspace_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN (SELECT UNNEST(?))",(user,[row_id for kind,row_id in keys])).fetchall()}
    reverse={(table,physical):logical for (table,physical),(logical,workspace) in origins.items()}|{("conversations",physical):logical for logical,physical in member_physical.items()}
    refs={(parent,value) for (table,physical),(columns,values) in raws.items() for column,value in zip(columns,values) for parent in [dict(FKS.get(table,())).get(column)] if parent and value is not None}
    reverse|={(table,physical):logical for table,physical,logical in db.execute("SELECT table_name,physical_row_id,source_row_id FROM remote.row_origins WHERE author_user_id=? AND physical_row_id IN (SELECT UNNEST(?))",(user,[value for table,value in refs])).fetchall()}
    reverse|={ref:ref[1] for ref in refs if ref not in reverse}
    physical_by_source={(table,logical):physical for (table,physical),logical in reverse.items()}|{("conversations",logical):physical for logical,physical in member_physical.items()}
    heads,out=(heads:=_heads(db,user,{table:{reverse.get((table,physical),physical) for physical in values} for table,values in selected.items()})),[(row,heads[(table,logical_id)],(table,physical) not in origins,physical_by_source,values[columns.index("path")] if table=="attachments" else None) for table,physical in keys for columns,values in [raws[(table,physical)]] for logical_id in [reverse.get((table,physical),physical)] for parents in [dict(FKS.get(table,()))] for row in [logical_row(table,columns,[reverse.get((parents[column],value),value) if column in parents else value for column,value in zip(columns,values)],logical_id)] if required(digest(row)==heads[(table,logical_id)]["content_hash"],ValueError(f"provider alias body/proof mismatch: {table}:{logical_id}"))]
    return out,keys[-1]
def _alias_pages(db_path,user,member_physical,page=500):
    after=("","")
    while True:
        with contextlib.closing(open_db(db_path,True,purpose="remote.alias.page")) as db: generation,values=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],_alias_page(db,user,member_physical,after,page)
        rows,after=values
        if not rows: return
        yield generation,rows
        archive_yield(db_path)
def reconcile_provider_aliases(db_path,cfg,workspace):
    user=cfg["user"]
    with contextlib.closing(open_db(db_path,purpose="remote.alias.schema")) as db: init_schema(db)
    with contextlib.closing(open_db(db_path,True,purpose="remote.alias.plan")) as db: stored=[(oid,source,session,json.loads(members),canonical,json.loads(proof)) for oid,source,session,members,canonical,proof in db.execute("SELECT object_id,source,session_id,CAST(members AS VARCHAR),canonical_source_row_id,CAST(proof AS VARCHAR) FROM remote.provider_session_aliases WHERE author_user_id=? ORDER BY object_id,revision",(user,)).fetchall()]
    groups={}
    [groups.setdefault(row[0],[]).append(row) for row in stored]
    result,controls,signer,backed_up={"changed":0,"settled":0,"blocked":{}},next(w["controls"] for w in cfg["server_state"]["workspaces"] if w["id"]==workspace),cfg["controls"][workspace]["devices"][cfg["device"]["id"]],False
    for object_id,values in groups.items():
        ancestors={a for value in values for a in value[5]["ancestors"]}
        leaves=[value for value in values if value[5]["revision"] not in ancestors]
        if len(leaves)!=1:
            result["blocked"][object_id]="provider alias proof is not converged"
            continue
        _,source,session,members,canonical,_=leaves[0]
        try:
            with contextlib.closing(open_db(db_path,True,purpose="remote.alias.members")) as db: member_heads,member_physical,active,member_native,binding=_alias_members(db,user,source,session,members,canonical)
            losers,moving=set(members)-{canonical},False
            for generation,rows in _alias_pages(db_path,user,member_physical):
                for row,head,native,parent_map,path in rows:
                    if row["kind"]=="conversations" and row["id"] in active:
                        required(row["data"]["source"]==source and isinstance(metadata:=row["data"]["metadata"],dict) and metadata.get("session_id")==session,ValueError("provider alias exact evidence conflicts"))
                    if row["kind"]=="attachments" and row["data"]["body_hash"]: required(path and (path:=Path(path)).is_file() and not path.is_symlink() and file_hash(path)==row["data"]["body_hash"],ValueError(f"provider alias attachment body unavailable: {row['id']}"))
                    moving|=row["kind"] in ("messages","artifacts") and row["data"]["conversation_id"] in losers
        except ValueError as e:
            result["blocked"][object_id]=str(e)
            continue
        if not moving and active=={canonical} and not binding:
            result["settled"]+=1
            continue
        if not backed_up:
            with contextlib.closing(open_db(db_path,purpose="maintenance.remote.alias-backup")) as db: _migration_backup(db,"provider-alias-reconciliation")
            backed_up=True
        try:
            changed=False
            for generation,values in _alias_pages(db_path,user,member_physical):
                rows=[({**row,"data":{**row["data"],"conversation_id":canonical}},head,native,parent_map) for row,head,native,parent_map,path in values if row["kind"] in ("messages","artifacts") and row["data"]["conversation_id"] in losers]
                if not rows: continue
                proofs=[row_proof(cfg["device"],user,head["workspace"],cfg["workspaces"][workspace]["epoch"],row,head["revision"],workspace) for row,head,native,parent_map in rows]
                with contextlib.closing(open_db(db_path,purpose="remote.alias.write-page")) as db,_transaction(db):
                    (required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during provider alias reconciliation; retry")),project_workspace_controls(db,controls),project_logical_rows(db,[(row,proof,pid,native,parent_map) for (row,head,native,parent_map),proof,pid in zip(rows,proofs,project_row_proofs(db,proofs,signer["root_public"],signer["certificate"]))]))
                changed=True
            with contextlib.closing(open_db(db_path,True,purpose="remote.alias.finish-plan")) as db:
                generation,(member_heads,member_physical,active,member_native,binding)=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],_alias_members(db,user,source,session,members,canonical)
                required(not db.execute("SELECT 1 FROM (SELECT conversation_id FROM messages UNION ALL SELECT conversation_id FROM artifacts) WHERE conversation_id IN (SELECT UNNEST(?)) LIMIT 1",([member_physical[m] for m in set(members)-{canonical}],)).fetchone(),RuntimeError("Provider alias rows remain; retry"))
            rows,proofs=(rows:=[(logical_row("conversations",identity=member,state="deleted"),member_heads[("conversations",member)],member_native[member],{("conversations",source_id):physical for source_id,physical in member_physical.items()}) for member in active-{canonical}]),[row_proof(cfg["device"],user,head["workspace"],cfg["workspaces"][workspace]["epoch"],row,head["revision"],workspace) for row,head,native,parent_map in rows]
            with contextlib.closing(open_db(db_path,purpose="remote.alias.finish")) as db,_transaction(db):
                (required(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation,RuntimeError("Archive changed during provider alias reconciliation; retry")),project_workspace_controls(db,controls),project_logical_rows(db,[(row,proof,pid,native,parent_map) for (row,head,native,parent_map),proof,pid in zip(rows,proofs,project_row_proofs(db,proofs,signer["root_public"],signer["certificate"]))]),project_provider_bindings(db,source,session,member_physical[canonical],list(member_physical.values())))
            result["changed"]+=bool(changed or rows or binding)
        except duckdb.InterruptException: raise
        except Exception as e: result["blocked"][object_id]=str(e)
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
def apply_row_replicas(db_path,bodies,workspace,controls,recover=None,local_user=None,db=None,root=None,ready=True):
    if not bodies: return []
    values=[verified_replica(body,workspace,controls,local_user) for body in bodies]
    if ready and Path(db_path).is_file() and any(value[1]["kind"]=="semantic.proof" for value in values):
        with contextlib.closing(open_db(db_path,purpose="schema.remote.rows")) as schema: ready=(init_schema(schema),False)[-1]
    semantic_ids,semantic=(semantic_ids:=[i for i,value in enumerate(values) if value[1]["kind"]=="semantic.proof"]),dict(zip(semantic_ids,bridge_accept_many(root,[(values[i][0],values[i][1]) for i in semantic_ids])))
    indexes=[i for i in range(len(values)) if i not in semantic]
    if not indexes: return list(semantic.values())
    names=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature")
    proof=lambda values:{"v":1,"kind":"row.proof",**dict(zip(names,values))}
    own=db is None
    db=db or open_db(db_path,purpose="remote.rows.project")
    if own and ready and not semantic_ids: init_schema(db)
    try:
        with _transaction(db):
            items=[values[i] for i in indexes]
            columns=("workspace","kind","row_id","author","revision")
            temp_rows(db,"incoming",columns,[tuple(p[k] for k in ("workspace","row_kind","row_id","author_user_id","revision")) for row,p,signer_,lineage in items])
            old={tuple(r) for r in db.execute("SELECT p.workspace_id,p.row_kind,p.source_row_id,p.author_user_id,p.revision FROM remote.row_proofs p JOIN incoming i ON (p.workspace_id,p.row_kind,p.source_row_id,p.author_user_id,p.revision)=(i.workspace,i.kind,i.row_id,i.author,i.revision)").fetchall()}
            project_workspace_controls(db,controls)
            groups={}
            for row,head,head_signer,lineage in items:
                for p,signer_ in [*lineage,(head,head_signer)]: groups.setdefault((p["author_user_id"],p["author_device_id"]),(signer_,[]))[1].append(p)
            [project_row_proofs(db,proofs,signer_["root_public"],signer_["certificate"]) for signer_,proofs in groups.values()]
            _insert_pages(db,"remote.row_conflicts",[(digest(p),json.dumps(row,sort_keys=True,separators=(",",":"))) for row,p,signer_,lineage in items],("proof_id","body"),mode=" OR IGNORE")
            projected,chosen,resolved=[],{},[]
            fields="p.workspace_id,p.authorization_workspace_id,p.row_kind,p.source_row_id,p.encoding_v,p.content_hash,p.revision,p.previous_revision,p.state,p.author_user_id,p.author_device_id,p.authorization_epoch,p.signature"
            chains={}
            for r in db.execute(f"SELECT p.id,CAST(c.body AS VARCHAR),{fields} FROM remote.row_proofs p JOIN (SELECT DISTINCT kind,row_id,author FROM incoming) i ON (p.row_kind,p.source_row_id,p.author_user_id)=(i.kind,i.row_id,i.author) LEFT JOIN remote.row_conflicts c ON c.proof_id=p.id").fetchall(): chains.setdefault((r[4],r[5],r[11]),{})[r[8]]=(r[0],json.loads(r[1]) if r[1] else None,proof(r[2:]))
            for scope,nodes in chains.items():
                leaves=set(nodes)-{v[2]["previous_revision"] for v in nodes.values() if v[2]["previous_revision"]}
                if len(leaves)==1:
                    revision=leaves.pop()
                    pid,row,p=nodes[revision]
                    chosen[scope]=revision
                    resolved.append(scope)
                    if row is not None and not (p["author_user_id"]==local_user and recover=="adopt"): projected.append((row,p,pid,p["author_user_id"]==local_user and recover=="native"))
            if resolved:
                temp_rows(db,"resolved_scopes",columns[1:4],resolved)
                db.execute("DELETE FROM remote.row_conflicts c USING remote.row_proofs p,resolved_scopes r WHERE c.proof_id=p.id AND (p.row_kind,p.source_row_id,p.author_user_id)=(r.kind,r.row_id,r.author)")
            project_logical_rows(db,projected)
        results=[(*((p[k] for k in ("workspace","row_kind","row_id","author_user_id"))),p["revision"]) not in old and chosen.get(tuple(p[k] for k in ("row_kind","row_id","author_user_id")))==p["revision"] for row,p,signer_,lineage in items]
        accepted=semantic|dict(zip(indexes,results))
        return [accepted[i] for i in range(len(values))]
    finally:
        if own: db.close()
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
