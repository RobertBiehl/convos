"""Client-side enrollment, E2EE keyring, automatic sync, membership, and local queries."""
import fcntl, json, os, sqlite3, time, traceback, urllib.error, urllib.parse, urllib.request
from contextlib import closing, contextmanager
from functools import wraps
from pathlib import Path; from typing import Optional

import typer
from ai_convos_redact import protect_all
_pending=[]
def register(app): _pending.append(app) if "remote" not in globals() else app.add_typer(remote,name="remote")
from ai_convos.cli import PROJECT_ROOT, archive_changes as core_archive_changes, archive_state as core_archive_state, capture_repository as core_capture_repository, drain_hooks, durable_replace, init_schema, install_hooks, open_db, project_attachment_body, project_provider_alias, project_workspace_controls, repository as core_repository, repository_evidence, repository_state as core_repository_state, required
from .control import CONTROL_V, approved, electorate, proposal as device_proposal, record as control_record, sign as control_sign, state_hash, verify_proposal, verify_state, vote as device_vote
from .projection import SIGNED, TABLES, apply_row_replicas, attest_rows, blob_replicas, bridge_replicas, bridge_stamp, connect, control_chain, cutover_state, event_support, inspect_state, project, project_many, read_state, reconcile_provider_aliases, relocate_attachments, reset_history, row_replicas, scan, sequence, sharing, stored_controls, verify_history
from .protocol import (b64, certificate, digest, event, fingerprint, identity, open_blob, open_event, open_key, open_origin, open_replica, public, public_id, recover,
                       recovery_bundle, registration_proof, seal_event, seal_key, seal_origin, seal_replica, semantic_proof, sign_control, signer, unb64, verify_certificate, verify_semantic_proof)
from .service import edit_hooks, enable

remote=typer.Typer(help="End-to-end encrypted personal and team synchronization")
def local_root(root=None): return Path(root or os.environ.get("CONVOS_PROJECT_ROOT",PROJECT_ROOT))
def paths(root=None): return (base:=local_root(root)/"remote"),base/"config.json",base/"state.db"
def core_path(root=None): return local_root(root)/"data"/"convos.db"
def archive_info(root=None,create=False):
    if not (path:=core_path(root)).exists() and not create: return None
    path.parent.mkdir(parents=True,exist_ok=True); db=open_db(path); init_schema(db)
    try: return core_archive_state(db)
    finally: db.close()
def load(root=None):
    if not (path:=paths(root)[1]).exists(): raise ValueError("Remote is not configured. Run `convos remote setup`.")
    return json.loads(path.read_text())
def provider_alias_id(source,session): return "provider-session:"+digest([source,session])
def _provider_alias_value(row):
    workspace,author,object_id,revision,source,session,members,canonical,proof=row
    members=json.loads(members)
    proof=json.loads(proof)
    body={"v":1,"kind":"provider.session","id":object_id,"state":"active","data":{"source":source,"session_id":session,"members":members,"canonical":canonical}}
    return dict(row=body,proof=proof,previous=None,workspace=workspace,author=author)
def provider_alias_records(root,user,workspace,kind):
    if kind!="personal" or not core_path(root).is_file(): return []
    db=open_db(core_path(root))
    init_schema(db)
    stored=[_provider_alias_value(r) for r in db.execute("SELECT workspace_id,author_user_id,object_id,revision,source,session_id,CAST(members AS VARCHAR),canonical_source_row_id,CAST(proof AS VARCHAR) FROM remote.provider_session_aliases WHERE author_user_id=? ORDER BY object_id,revision",(user,)).fetchall()]
    ancestors={a for value in stored for a in value["proof"]["ancestors"]}
    leaves=[value for value in stored if value["proof"]["revision"] not in ancestors]
    groups={(source,session):set(members) for source,session,members in db.execute("SELECT source,session_id,list(member ORDER BY member) FROM (SELECT DISTINCT c.source,json_extract_string(c.metadata,'$.session_id') session_id,COALESCE(o.source_row_id,c.id) member,COALESCE(o.author_user_id,?) author FROM conversations c LEFT JOIN remote.row_origins o ON o.table_name='conversations' AND o.physical_row_id=c.id WHERE session_id IS NOT NULL) WHERE author=? GROUP BY source,session_id HAVING count(*)>1 ORDER BY source,session_id",(user,user)).fetchall()}
    db.close()
    for value in leaves: groups.setdefault((value["row"]["data"]["source"],value["row"]["data"]["session_id"]),set()).update(value["row"]["data"]["members"])
    records=[{k:v[k] for k in ("row","proof","previous")} for v in leaves]
    for (source,session),members in sorted(groups.items()):
        members=sorted(members)
        row={"v":1,"kind":"provider.session","id":provider_alias_id(source,session),"state":"active","data":{"source":source,"session_id":session,"members":members,"canonical":members[0]}}
        matching=[v for v in leaves if v["row"]["id"]==row["id"]]
        if len(matching)!=1 or matching[0]["row"]!=row: records.append(dict(row=row,proof=None,previous=[v["proof"] for v in matching] if len(matching)>1 else matching[0]["proof"] if matching else None))
    return records
def provider_alias_accept(root,row,proof,project=True):
    data=row.get("data")
    members=data.get("members") if isinstance(data,dict) else None
    author=proof.get("author_user_id")
    verify_semantic_proof(proof,row,author)
    if set(row)!={"v","kind","id","state","data"} or row["v"]!=1 or row["kind"]!="provider.session" or row["state"]!="active" or set(data)!={"source","session_id","members","canonical"} or not all(isinstance(data[k],str) and data[k] for k in ("source","session_id","canonical")) or not isinstance(members,list) or len(members)<2 or members!=sorted(set(members)) or data["canonical"]!=members[0] or row["id"]!=provider_alias_id(data["source"],data["session_id"]): raise ValueError("Malformed provider session alias")
    db=open_db(core_path(root))
    init_schema(db)
    project_provider_alias(db,dict(workspace_id=proof["workspace"],author_user_id=author,object_id=row["id"],revision=proof["revision"],source=data["source"],session_id=data["session_id"],members=members,canonical_source_row_id=data["canonical"],proof=proof))
    values=[_provider_alias_value(r) for r in db.execute("SELECT workspace_id,author_user_id,object_id,revision,source,session_id,CAST(members AS VARCHAR),canonical_source_row_id,CAST(proof AS VARCHAR) FROM remote.provider_session_aliases WHERE author_user_id=? AND object_id=?",(author,row["id"])).fetchall()]
    db.close()
    ancestors={a for value in values for a in value["proof"]["ancestors"]}
    return len([value for value in values if value["proof"]["revision"] not in ancestors])==1
def provider_alias_token(root):
    cfg=load(root)
    return digest([(v["row"],v["proof"]["revision"] if v["proof"] else None) for v in provider_alias_records(root,cfg["user"],"","personal")])
def provider_alias_bridge(): return dict(v=2,objects={"provider.session"},records=provider_alias_records,accept=provider_alias_accept,token=provider_alias_token)
def save(cfg,root=None):
    base,path,_=paths(root); base.mkdir(parents=True,exist_ok=True); os.chmod(base,0o700); tmp=path.with_name(f".{path.name}.{os.getpid()}"); tmp.write_text(json.dumps(cfg)); os.chmod(tmp,0o600); durable_replace(tmp,path)
def save_watch_status(value,root=None):
    path=paths(root)[0]/"watch.json"
    path.parent.mkdir(parents=True,exist_ok=True)
    os.chmod(path.parent,0o700)
    tmp=path.with_name(f".{path.name}.{os.getpid()}")
    tmp.write_text(json.dumps(value))
    os.chmod(tmp,0o600)
    durable_replace(tmp,path)
def rescue_bindings(cfg,state_path,root=None):
    db=sqlite3.connect(Path(state_path).resolve().as_uri()+"?mode=ro",uri=True)
    try: rows=db.execute(f"SELECT workspace,value,{('local_root' if 'local_root' in columns else 'NULL')} FROM policies WHERE kind='path'").fetchall() if {"workspace","kind","value"}<=(columns:={r[1] for r in db.execute("PRAGMA table_info(policies)").fetchall()}) else []
    finally: db.close()
    changed=0
    for ws,value,stored in rows:
        old,new,path=(old:=f"{ws}:{value}"),policy_binding(ws,"path",value),stored or cfg.get("bindings",{}).get(old)
        if path is None: continue
        if not isinstance(path,str) or not Path(path).is_absolute(): raise ValueError("legacy path binding is invalid")
        if (current:=cfg.setdefault("bindings",{}).get(new)) is not None and binding_path(current)!=path: raise ValueError("legacy path binding conflicts")
        cfg["bindings"][new]=path
        cfg["bindings"].pop(old,None)
        changed+=1
    if changed: save(cfg,root)
    return changed
def encrypted_file(root,event,value):
    path=paths(root)[0]/"outbox"/f"{event}.json"; path.parent.mkdir(parents=True,exist_ok=True)
    if path.parent.is_symlink() or path.is_symlink(): raise ValueError("encrypted outbox path must not be a symlink")
    os.chmod(path.parent,0o700); raw=json.dumps(value,separators=(",",":")).encode(); tmp=path.with_name(f".{path.name}.{os.getpid()}"); handle=tmp.open("xb"); os.chmod(tmp,0o600); handle.write(raw); handle.close(); durable_replace(tmp,path); return path,len(raw)
def receipt(state,ws,value,cursor,epoch): state.execute("INSERT OR REPLACE INTO receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",(ws,value["id"],cursor,value["author"],value["seq"],epoch,value["kind"],value["payload_v"],value["entity"],value["revision"],value["payload"].get("status") if isinstance(value["payload"],dict) and value["payload"].get("status") in ("active","deleted") else None))
def safe_url(url): parsed=urllib.parse.urlparse(url); required(parsed.scheme=="https" or parsed.hostname in ("127.0.0.1","localhost","::1") or os.environ.get("CONVOS_REMOTE_INSECURE")=="1",ValueError("Remote URL must use HTTPS (set CONVOS_REMOTE_INSECURE=1 only on a trusted test network)"))
def request(cfg,body,auth=True):
    safe_url(cfg["url"])
    headers={"Content-Type":"application/json"};
    if auth: headers["Authorization"]="Bearer "+cfg["token"]
    req=urllib.request.Request(cfg["url"].rstrip("/")+"/v1",data=json.dumps(body).encode(),headers=headers,method="POST")
    try: return json.loads(urllib.request.urlopen(req,timeout=10).read())
    except urllib.error.HTTPError as e: raise ValueError(json.loads(e.read())["error"]) from e
def health(cfg): safe_url(cfg["url"]); result=json.loads(urllib.request.urlopen(cfg["url"].rstrip("/")+"/v1/health",timeout=3).read()); required(result.get("version")==1,ValueError("relay protocol v1 required")); return result
@contextmanager
def local_lock(root,name="mutation",blocking=True):
    path=paths(root)[0]/f"{name}.lock"
    path.parent.mkdir(parents=True,exist_ok=True)
    handle=path.open("a+")
    os.chmod(path,0o600)
    try: fcntl.flock(handle,fcntl.LOCK_EX|(0 if blocking else fcntl.LOCK_NB))
    except BlockingIOError:
        handle.close()
        raise RuntimeError(f"Remote {name} already running") from None
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        yield
    finally:
        fcntl.flock(handle,fcntl.LOCK_UN)
        handle.close()
def mutation_lock(root): return local_lock(root,"mutation",False)
def sync_run(root): return local_lock(root,"sync",False)
def locked(fn):
    @wraps(fn)
    def call(*args,**kwargs):
        with mutation_lock(None): return fn(*args,**kwargs)
    return call
def workspace(cfg,value):
    if len(hits:=[k for k,v in cfg["workspaces"].items() if k.startswith(value) or v["name"]==value])!=1: raise ValueError(f"Workspace must match exactly one of: {', '.join(v['name'] for v in cfg['workspaces'].values())}")
    return hits[0]
def key(cfg,ws,epoch): return unb64(cfg["keys"][f"{ws}:{epoch}"])
def trusted(devices):
    for d in devices: required(d["certificate"],ValueError("device certificate missing")); body=verify_certificate(json.loads(d["certificate"]),d["root_public"]); signer({d["id"]:d},d["id"]); required(public_id(d["root_public"])==d["user_id"] and body["user"]==d["user_id"] and body["device"]==public(d),ValueError("device certificate mismatch"))
    return devices
def server_record(d,history=True): return control_record(d["user_id"],d["root_public"],d,json.loads(d["certificate"]) if isinstance(d["certificate"],str) else d["certificate"],history)
def own_record(cfg,history=True): return control_record(cfg["user"],cfg["root"]["sign_public"],cfg["device"],certificate(cfg["root"],cfg["user"],cfg["device"]),history)
def control_body(cfg,previous,key_,action,members=None,devices=None,removed=None,approval=None,boundary=None):
    advance=action not in ("history","history_activate"); boundary=required(boundary,ValueError("new workspace epoch requires a signed history boundary")) if advance else previous["boundary"]; return control_sign(cfg["device"],{"v":CONTROL_V,"kind":"workspace.state","workspace":previous["workspace"],"scope":previous["scope"],"revision":previous["revision"]+1,"prev":state_hash(previous),"epoch":previous["epoch"]+advance,"boundary":boundary,"key_commitment":digest(key_),"members":members or previous["members"],"devices":devices or previous["devices"],"removed":removed if removed is not None else previous["removed"],"action":action,"approval":approval,"approved_at":time.time()})
def access_from(cfg,ws):
    head=cfg["controls"][ws]; device=cfg["device"]["id"]; first=min(value["epoch"] for remote in cfg["server_state"]["workspaces"] if remote["id"]==ws for value in remote["controls"] if device in value["devices"]); return head["members"][cfg["user"]]["history_from"] if head["devices"][device]["history"] else first
def sequence_heads(state,ws): return {author:{"seq":seq,"event":event} for author,seq,event in state.execute("SELECT s.author,s.seq,s.event FROM event_sequences s JOIN (SELECT author,MAX(seq) seq FROM event_sequences WHERE workspace=? GROUP BY author) h ON h.author=s.author AND h.seq=s.seq WHERE s.workspace=?",(ws,ws)).fetchall()}
def prepare_archive(cfg,state,root=None,create=True):
    info=archive_info(root); empty=not info or info[1:]==(0,0); cached=[(state.execute("SELECT value FROM meta WHERE key=?",(key,)).fetchone() or [None])[0] for key in ("archive_id","archive_generation")]; proofs=[p for p in (cfg.get("archive"),{"id":cached[0],"generation":int(cached[1])} if all(cached) else None) if p]; existing={r[0].split(":",1)[1]:r[1] for r in state.execute("SELECT key,value FROM meta WHERE key LIKE 'archive_mode:%'").fetchall()}; bases={r[0].split(":",1)[1]:json.loads(r[1]) for r in state.execute("SELECT key,value FROM meta WHERE key LIKE 'archive_basis:%'").fetchall()}; valid=info and all(ws in bases and bases[ws]["id"]==info[0] and info[1]>=bases[ws]["generation"] for ws in existing); fresh=not state.execute("SELECT 1 FROM sync_states LIMIT 1").fetchone(); mode=None
    if existing and not valid: state.execute("DELETE FROM meta WHERE key LIKE 'archive_mode:%' OR key LIKE 'archive_basis:%'"); existing={}
    if existing and not empty: return existing
    if not existing and not proofs: mode="native" if empty else "adopt"
    elif not existing and (not info or any(info[0]!=p["id"] or info[1]<p["generation"] for p in proofs)): mode="native" if empty else "import"
    elif not existing and fresh: mode="adopt"
    if not existing and not mode: return None
    if not info and not create: return existing or mode
    if create and not info: info=archive_info(root,True)
    active={w["id"] for w in cfg["server_state"]["workspaces"] if w["device_authorized"]}
    for ws in active:
        personal=cfg["workspaces"][ws]["kind"]=="personal"; selected=existing.get(ws,"native" if personal else "adopt") if existing else mode if personal else "adopt"; selected="native" if existing and selected=="import" else selected; state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?)",(f"archive_mode:{ws}",selected,f"archive_basis:{ws}",json.dumps({"id":info[0],"generation":info[1]}))); state.execute("DELETE FROM meta WHERE key=?",(f"boundary:{ws}",)); state.execute("INSERT OR REPLACE INTO sync_states VALUES (?,'rebaselining',COALESCE((SELECT tail FROM sync_states WHERE workspace=?),0),COALESCE((SELECT floor FROM sync_states WHERE workspace=?),0),NULL)",(ws,ws,ws))
    state.commit(); return existing or mode
def remember_archive(cfg,state,root=None):
    if info:=archive_info(root): cfg["archive"]={"id":info[0],"generation":info[1]}; save(cfg,root); state.execute("INSERT OR REPLACE INTO meta VALUES ('archive_id',?),('archive_generation',?)",(info[0],str(info[1]))); state.execute("DELETE FROM meta WHERE key LIKE 'archive_mode:%' OR key LIKE 'archive_basis:%'")
def next_boundary(cfg,ws,root=None):
    if cfg["device"]["id"] in cfg["controls"][ws]["devices"]:
        state=connect(paths(root)[2])
        try: upload(cfg,state,root,{ws}); prepare_archive(cfg,state,root,False); pull(cfg,state,root); local=sequence_heads(state,ws); remote=request(cfg,{"op":"ledger","workspace":ws})
        finally: state.close()
        if local!=remote["heads"]: raise ValueError("relay ledger changed while creating history boundary")
    else: remote=request(cfg,{"op":"ledger","workspace":ws})
    return {"epoch":cfg["controls"][ws]["epoch"]+1,**remote}
def directory_user(found,user): field="id" if len(user)==32 and all(c in "0123456789abcdef" for c in user) else "name"; values=[v for v in found["users"] if v[field]==user]; return required(values[0] if len(values)==1 and public_id(values[0]["root_public"])==values[0]["id"] else None,ValueError("directory user mismatch"))
def update_recovery(cfg,root=None):
    personal={ws for ws,v in cfg["workspaces"].items() if v["kind"]=="personal"}; keys={name:value for name,value in cfg["keys"].items() if name.rsplit(":",1)[0] in personal}; _,bundle=recovery_bundle({"root":cfg["root"],"keys":keys,"workspaces":cfg["workspaces"],"controls":{ws:v for ws,v in cfg["controls"].items() if ws in personal},"sharing":cfg.get("sharing",{})},unb64(cfg["recovery"])); request(cfg,sign_control(cfg["device"],{"op":"recovery","bundle":bundle})); save(cfg,root)
def refresh(cfg,root=None):
    state=request(cfg,{"op":"state"}); cfg["server_state"]=state; changed=False; seen=set()
    if (state["user"],state["device"])!=(cfg["user"],cfg["device"]["id"]): raise ValueError("relay identity metadata mismatch")
    for ws in state["workspaces"]:
        if ws["id"] in seen: raise ValueError("duplicate relay workspace")
        seen.add(ws["id"])
        previous=None
        for value in ws["controls"]: verify_state(value,previous); previous=value
        if not previous or (ws["id"],ws["kind"],ws["epoch"])!=(previous["workspace"],previous["scope"],previous["epoch"]): raise ValueError("relay workspace metadata does not match signed state")
        member=previous["members"].get(cfg["user"]); authorized=cfg["device"]["id"] in previous["devices"] and cfg["device"]["id"] not in previous["removed"]
        if not member or (ws["role"],ws["history_from"],bool(ws["device_authorized"]))!=(member["role"],member["history_from"],authorized): raise ValueError("relay access metadata does not match signed state")
        if (pinned:=cfg["controls"].get(ws["id"])) and (not previous or pinned["revision"]>previous["revision"] or state_hash(pinned) not in {state_hash(v) for v in ws["controls"]}): raise ValueError("workspace control rollback or fork")
        cfg["controls"][ws["id"]]=previous; cfg["workspaces"].setdefault(ws["id"],{"name":ws["id"][:8]}); cfg["workspaces"][ws["id"]].update(kind=previous["scope"],epoch=previous["epoch"]); ws.update(kind=previous["scope"],epoch=previous["epoch"],role=member["role"],history_from=member["history_from"],device_authorized=authorized)
        first=min((v["epoch"] for v in ws["controls"] if cfg["device"]["id"] in v["devices"]),default=previous["epoch"]); start=member["history_from"] if authorized and previous["devices"][cfg["device"]["id"]]["history"] else first; epochs=[int(v["epoch"]) for v in ws["keys"]]
        if authorized and (len(epochs)!=len(set(epochs)) or any(not start<=epoch<=previous["epoch"] for epoch in epochs)): raise ValueError("relay key envelope exceeds signed entitlement")
        for wrapped in ws["keys"] if authorized else []:
            name=f"{ws['id']}:{wrapped['epoch']}"
            if name not in cfg["keys"]:
                opened=open_key(json.loads(wrapped["envelope"]),cfg["device"]["box_private"],f"workspace:{ws['id']}:epoch:{wrapped['epoch']}"); controls=[v for v in ws["controls"] if v["epoch"]==wrapped["epoch"]]
                if not controls or digest(opened)!=controls[-1]["key_commitment"]: raise ValueError("workspace epoch key commitment mismatch")
                cfg["keys"][name]=b64(opened); changed=True
    save(cfg,root)
    if changed: update_recovery(cfg,root)
    return state
def create(cfg,name,kind="team",root=None):
    ws,key_=digest(os.urandom(32))[:32],os.urandom(32); entry=own_record(cfg); control=control_sign(cfg["device"],{"v":CONTROL_V,"kind":"workspace.state","workspace":ws,"scope":kind,"revision":1,"prev":None,"epoch":1,"boundary":{"epoch":1,"tail":0,"heads":{}},"key_commitment":digest(key_),"members":{cfg["user"]:{"role":"admin","joined":1,"history_from":1}},"devices":{cfg["device"]["id"]:entry},"removed":[],"action":"create","approval":None,"approved_at":time.time()}); env=seal_key(key_,cfg["device"]["box_public"],f"workspace:{ws}:epoch:1"); request(cfg,sign_control(cfg["device"],{"op":"create","workspace":ws,"kind":kind,"control":control,"envelopes":{cfg["device"]["id"]:env}})); cfg["workspaces"][ws]={"name":name,"kind":kind,"epoch":1}; cfg["keys"][f"{ws}:1"]=b64(key_); cfg["controls"][ws]=control; update_recovery(cfg,root); membership_event(cfg,ws,1,{cfg["user"]:"admin"},root); return ws
def enroll(url,name,root,device,recovery=None):
    cert=certificate(root,root["id"],device); base={"root_public":root["sign_public"],"certificate":cert}; challenge=request({"url":url},{"op":"register_challenge",**base},False)["challenge"]; return request({"url":url},{"op":"register","user_name":name,**base,"challenge":challenge,"proof":registration_proof(device,challenge,root["sign_public"],cert),**({"recovery":recovery} if recovery else {})},False)
def setup_client(url,user,device="computer",recovery=None,root=None):
    if recovery:
        bundle=request({"url":url},{"op":"recovery_fetch","user":user},False)["bundle"]; recovered=recover(bundle,recovery); root_id=recovered["root"]; keys,workspaces,controls,sharing=recovered["keys"],recovered["workspaces"],recovered.get("controls",{}),recovered.get("sharing",{})
    else: root_id,keys,workspaces,controls,sharing=identity(user+" root"),{},{},{},{}
    dev=identity(device); uid=root_id["id"]
    if not recovery: recovery,bundle=recovery_bundle({"root":root_id,"keys":keys,"workspaces":workspaces})
    registered=enroll(url,user,root_id,dev,bundle if not workspaces else None)
    cfg={"url":url,"name":user,"user":uid,"token":registered["token"],"root":root_id,"device":dev,"recovery":recovery,"keys":keys,"workspaces":workspaces,"controls":controls,"bindings":{},"sharing":sharing,"server_state":{}}; save(cfg,root)
    if not workspaces: create(cfg,"Personal","personal",root)
    else:
        state=refresh(cfg,root)
        for ws in [w for w in state["workspaces"] if cfg["controls"][w["id"]]["members"][uid]["role"]=="admin" and cfg["controls"][w["id"]]["scope"]=="personal"]:
            rotate(cfg,ws["id"],{u:m["role"] for u,m in cfg["controls"][ws["id"]]["members"].items()},[],root=root); grant_all(cfg,ws["id"],uid,root)
    return cfg,recovery
def rehome_client(cfg,url,root=None):
    recovery,bundle=recovery_bundle({"root":cfg["root"],"keys":{},"workspaces":{}}); registered=enroll(url,cfg["name"],cfg["root"],cfg["device"],bundle); fresh={**cfg,"url":url,"token":registered["token"],"recovery":recovery,"keys":{},"workspaces":{},"controls":{},"bindings":{},"server_state":{}}; save(fresh,root); create(fresh,"Personal","personal",root); return fresh,recovery
def rotate(cfg,ws,members,devices,deactivate=(),root=None):
    state=refresh(cfg,root); previous=cfg["controls"][ws]; epoch=previous["epoch"]+1; boundary=next_boundary(cfg,ws,root); new=os.urandom(32); devices=trusted(devices); old=previous["members"]; meta={u:old.get(u,{"joined":epoch,"history_from":epoch})|{"role":role} for u,role in members.items()}; removed=sorted(set(previous["removed"])|set(deactivate)|{d for d,r in previous["devices"].items() if r["user"] not in members}); records={d:r for d,r in previous["devices"].items() if r["user"] in members and d not in deactivate}
    if cfg["device"]["id"] not in previous["devices"] and previous["scope"]=="personal":
        entry=own_record(cfg); req=device_proposal(cfg["device"],ws,previous,{**entry,"history":True},time.time()+300); records|={cfg["device"]["id"]:{**entry,"history":True}}; action,approval="personal_recover",{"proposal":req,"votes":[]}
    else:
        action,approval=("remove",None) if deactivate else ("membership",None); new_users=set(members)-set(old)
        records|={d["id"]:server_record(d) for d in devices if d["user_id"] in new_users and d["id"] not in removed and d.get("active",1) and d.get("allowed",1)}
    control=control_body(cfg,previous,new,action,meta,records,removed,approval,boundary); envs={d:seal_key(new,r["device"]["box_public"],f"workspace:{ws}:epoch:{epoch}") for d,r in records.items()}; request(cfg,sign_control(cfg["device"],{"op":"rotate","workspace":ws,"control":control,"envelopes":envs})); cfg["keys"][f"{ws}:{epoch}"]=b64(new); cfg["workspaces"][ws]["epoch"]=epoch; cfg["controls"][ws]=control; update_recovery(cfg,root)
    if cfg["device"]["id"] in records: membership_event(cfg,ws,epoch,members,root); retain_sharing(cfg,ws,root)
    return epoch
def publish(cfg,state,ws,record,root=None,defer=False,heads=None,force=False):
    if cfg["workspaces"][ws]["kind"]=="team":
        if not (records:=protect_all([record],root,ws)): return None
        record=records[0]
    payload_v=record.get("payload_v",1)
    if event_support({"kind":record["kind"],"payload_v":payload_v})!="supported": raise ValueError(f"unsupported event schema: {record['kind']} payload_v={payload_v}")
    revision=digest(record["payload"]); old=heads.get(record["entity"]) if heads is not None else state.execute("SELECT revision,event FROM publication_heads WHERE workspace=? AND owner=? AND entity=?",(ws,cfg["user"],record["entity"])).fetchone()
    if not force and heads is not None and old==revision: return None
    if not force and heads is None and old and old["revision"]==revision: return old["event"]
    seq=int((state.execute("SELECT value FROM meta WHERE key=?",(f"seq:{ws}",)).fetchone() or ["0"])[0])+1; prev=(state.execute("SELECT value FROM meta WHERE key=?",(f"prev:{ws}",)).fetchone() or [None])[0]; value=event(cfg["device"],seq,record["kind"],record["entity"],record["payload"],[prev] if prev else (),record.get("observed_at"),payload_v); epoch=cfg["workspaces"][ws]["epoch"]; env=seal_event(value,ws,epoch,key(cfg,ws,epoch)); path,size=encrypted_file(root,value["id"],env); status=record["payload"].get("status") if isinstance(record["payload"],dict) and record["payload"].get("status") in ("active","deleted") else None
    state.execute("INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",(ws,value["id"],record["entity"],revision,value["author"],seq,epoch,record["kind"],payload_v,status,str(path),size)); state.execute("INSERT OR REPLACE INTO publication_heads VALUES (?,?,?,?,?)",(ws,cfg["user"],record["entity"],revision,value["id"])); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?)",(f"seq:{ws}",str(seq),f"prev:{ws}",value["id"])); sequence(state,ws,value)
    if not defer: state.commit()
    if heads is not None: heads[record["entity"]]=revision
    project(core_path(root),state,value,ws,cfg["device"]["id"],root=root,authors={cfg["device"]["id"]:cfg["user"]}); return value["id"]
def configure_sharing(cfg,state,ws,auto_contribute,match,root=None):
    row={"v":1,"kind":"sharing.preference","id":f"sharing:{ws}:{cfg['user']}","state":"active","data":{"auto_contribute":auto_contribute,"match":match}}
    proof=semantic_proof(cfg["root"],cfg["user"],cfg["device"]["id"],ws,cfg["workspaces"][ws]["epoch"],row,sharing(state,ws,cfg["user"])["proofs"] or None)
    publish(cfg,state,ws,{"kind":"workspace.preference","entity":row["id"],"payload":{"row":row,"proof":proof}},root)
    upload(cfg,state,root); cfg.setdefault("sharing",{})[ws]={"auto_contribute":auto_contribute,"match":match}; update_recovery(cfg,root)
    return sharing(state,ws,cfg["user"])
def repository_policy(cfg,state,ws,value,evidence):
    row={"v":1,"kind":"repository.policy","id":f"repository:{ws}:{value}","state":"active","data":{"value":value,"evidence":evidence}}
    old=state.execute("SELECT proof FROM policy_proofs WHERE workspace=? AND owner=? AND value=?",(ws,cfg["user"],value)).fetchone(); proof=json.loads(old[0]) if old else semantic_proof(cfg["root"],cfg["user"],cfg["device"]["id"],ws,cfg["workspaces"][ws]["epoch"],row)
    return {"kind":"workspace.policy","entity":f"policy:repository:{value}","payload":{"row":row,"proof":proof},"payload_v":2}
def retained_sharing(state,ws,epoch=None):
    records=[{"kind":"workspace.policy","entity":f"policy:repository:{value}","payload":{"row":{"v":1,"kind":"repository.policy","id":f"repository:{ws}:{value}","state":"active","data":{"value":value,"evidence":json.loads(evidence)}},"proof":json.loads(proof)},"payload_v":2} for value,evidence,proof in state.execute("SELECT p.value,p.evidence,x.proof FROM policies p JOIN policy_proofs x USING(workspace,owner,value) WHERE p.workspace=? AND p.kind='repository'",(ws,)).fetchall()]
    for user, in state.execute("SELECT DISTINCT user FROM sharing_preferences WHERE workspace=?",(ws,)).fetchall():
        for proof in sharing(state,ws,user)["proofs"]:
            auto,match=state.execute("SELECT auto_contribute,match FROM sharing_preferences WHERE workspace=? AND user=? AND revision=?",(ws,user,proof["revision"])).fetchone(); row={"v":1,"kind":"sharing.preference","id":f"sharing:{ws}:{user}","state":"active","data":{"auto_contribute":None if auto is None else bool(auto),"match":json.loads(match)}}; records.append({"kind":"workspace.preference","entity":row["id"],"payload":{"row":row,"proof":proof}})
    return [r for r in records if epoch is None or not state.execute("SELECT 1 FROM outbox WHERE workspace=? AND entity=? AND revision=? AND epoch=? UNION SELECT 1 FROM receipts WHERE workspace=? AND entity=? AND revision=? AND epoch=?",(ws,r["entity"],digest(r["payload"]),epoch,ws,r["entity"],digest(r["payload"]),epoch)).fetchone()]
def retain_sharing(cfg,ws,root=None,state=None):
    if cfg["workspaces"][ws]["kind"]!="team": return 0
    owned=state is None; state=state or connect(paths(root)[2]); records=[r for r in retained_sharing(state,ws,cfg["workspaces"][ws]["epoch"]) if r["payload"]["proof"]["author_user_id"]==cfg["user"]]; [publish(cfg,state,ws,r,root,force=True) for r in records]; records and upload(cfg,state,root,{ws}); owned and state.close(); return len(records)
def settle_sharing(cfg,state,ready,root=None):
    changed=False
    for ws in ready&set(cfg["workspaces"]):
        if cfg["workspaces"][ws]["kind"]!="team": continue
        if state.execute("SELECT 1 FROM sharing_preferences WHERE workspace=? AND user=?",(ws,cfg["user"])).fetchone():
            value={k:sharing(state,ws,cfg["user"])[k] for k in ("auto_contribute","match")}; changed|=cfg.setdefault("sharing",{}).get(ws)!=value; cfg["sharing"][ws]=value
        elif (value:=cfg.get("sharing",{}).get(ws)): configure_sharing(cfg,state,ws,value["auto_contribute"],value["match"],root)
    if changed: update_recovery(cfg,root)
def policy_binding(ws,kind,value): return f"{ws}:{kind}:{value}"
def binding_path(value): return value.get("path") if isinstance(value,dict) else value
def sharing_routes(state,ws,user,bindings,core):
    pref=sharing(state,ws,user); known=core_repository_state(core) if hasattr(core,"execute") else core
    policies=state.execute("SELECT owner,kind,value,evidence FROM policies WHERE workspace=?",(ws,)).fetchall(); active=[p for p in policies if p[1]=="repository" and (p[0]==user or pref["effective_auto_contribute"])]
    def resolve(p):
        if not p[3]: return None
        binding=bindings.get(policy_binding(ws,p[1],p[2])) if p[0]==user else None; path=binding_path(binding) if binding else None
        expected=binding["repository"] if isinstance(binding,dict) else known["aliases"].get(repository_evidence(json.loads(p[3])))
        if path and (repo:=core_repository(path,known,True)) and repo["id"]==expected: return expected
        for checkout,rid in known["checkouts"].items():
            if rid==expected and (repo:=core_repository(known.get("checkout_roots",{}).get(checkout),known,True)) and (repo["id"],repo["checkout"])==(expected,checkout): return expected
        return None
    return sorted({repo for p in active if (repo:=resolve(p))}),[binding_path(bindings[key]) for p in policies if p[0]==user and p[1]=="path" and (key:=policy_binding(ws,p[1],p[2])) in bindings],pref["match"]
def upgrade_repository_policies(cfg,state,core,root=None):
    changed=False
    for ws,owner,value in state.execute("SELECT workspace,owner,value FROM policies WHERE kind='repository' AND evidence IS NULL").fetchall():
        if owner!=cfg["user"] or ws not in cfg["workspaces"] or cfg["workspaces"][ws]["kind"]!="team": continue
        repos={repository_evidence({k:repo[k] for k in ("lineage","remotes")}):(path,repo) for checkout,rid in core["checkouts"].items() if rid==value and (path:=core["checkout_roots"].get(checkout)) and (repo:=core_repository(path,core,True)) and repo["id"]==rid and repo["lineage"]}
        if len(repos)!=1: continue
        path,repo=next(iter(repos.values())); binding={"path":path,"repository":repo["id"],"checkout":repo["checkout"]}; key=policy_binding(ws,"repository",value)
        if (old:=cfg.setdefault("bindings",{}).get(key)) and old!=binding: continue
        cfg["bindings"][key]=binding; save(cfg,root); publish(cfg,state,ws,repository_policy(cfg,state,ws,value,{k:repo[k] for k in ("lineage","remotes")}),root); changed=True
    return changed
def promote_paths(cfg,state,core,root=None):
    changed=False
    for ws,value in state.execute("SELECT workspace,value FROM policies WHERE owner=? AND kind='path'",(cfg["user"],)).fetchall():
        binding=cfg.get("bindings",{}).get(policy_binding(ws,"path",value)); path=binding_path(binding) if binding else None; repo=core_repository(path,core,True) if path else None
        if not repo or not repo["lineage"] or Path(repo["root"])!=Path(path).resolve(): continue
        promotions,key=cfg.setdefault("promotions",{}),policy_binding(ws,"path",value); promotion=promotions.get(key)
        if promotion and (set(promotion)!={"grant","path","repository","checkout","evidence"} or promotion["path"]!=str(Path(path).resolve()) or promotion["checkout"]!=repo["checkout"] or promotion["evidence"]["lineage"]!=repo["lineage"]): continue
        grants=[r[0] for r in state.execute("SELECT value FROM policies WHERE workspace=? AND owner=? AND kind='repository'",(ws,cfg["user"])).fetchall()]
        dirty=False
        if not promotion: promotion={"grant":digest(os.urandom(32))[:32],"path":str(Path(path).resolve()),"repository":repo["id"],"checkout":repo["checkout"],"evidence":{k:repo[k] for k in ("lineage","remotes")}}; promotions[key]=promotion; dirty=True
        grant,bound=promotion["grant"],{k:promotion[k] for k in ("path","repository","checkout")}; grant_binding=policy_binding(ws,"repository",grant)
        if (old:=cfg.setdefault("bindings",{}).get(grant_binding)) and old!=bound: continue
        if not old: cfg["bindings"][grant_binding]=bound; dirty=True
        if dirty: save(cfg,root); changed=True
        if grant not in grants: publish(cfg,state,ws,repository_policy(cfg,state,ws,grant,promotion["evidence"]),root)
    return changed
def membership_event(cfg,ws,epoch,members,root=None):
    state=connect(paths(root)[2]); publish(cfg,state,ws,{"kind":"workspace.membership","entity":f"membership:{epoch}","payload":{"epoch":epoch,"members":members}},root); upload(cfg,state,root); state.close()
def control_event(cfg,ws,action,target,root=None):
    state=connect(paths(root)[2]); value=cfg["controls"][ws]; publish(cfg,state,ws,{"kind":"workspace.device","entity":f"device:{target}:{value['revision']}","payload":{"action":action,"device":target,"revision":value["revision"],"state":state_hash(value)}},root); upload(cfg,state,root); state.close()
def _upload_batches(rows,limit=8*1024*1024,measure=lambda row:row["size"]):
    batch,size=[],0
    for row in rows:
        if (amount:=measure(row))>limit: raise ValueError("encrypted upload item exceeds byte limit")
        if batch and (len(batch)==500 or size+amount>limit): yield batch; batch,size=[],0
        batch.append(row); size+=amount
    if batch: yield batch
def upload(cfg,state,root=None,workspaces=None):
    active={w["id"] for w in refresh(cfg,root)["workspaces"] if workspaces is None or w["id"] in workspaces}
    rows=[r for r in state.execute("SELECT * FROM outbox ORDER BY workspace,seq").fetchall() if r["workspace"] in active]
    for batch in _upload_batches(rows):
        prepared=[]; acknowledged=[]
        for row in batch:
            expected=paths(root)[0]/"outbox"/f"{row['event']}.json"
            if Path(row["path"])!=expected or expected.is_symlink(): raise ValueError("invalid encrypted outbox path")
            env=json.loads(expected.read_text()); current=cfg["workspaces"][row["workspace"]]["epoch"]
            if env["epoch"]!=current:
                try: found=request(cfg,{"op":"fetch","workspace":row["workspace"],"event":row["event"]})
                except ValueError as e:
                    if str(e)!="event not found": raise
                    value=open_event(env,key(cfg,row["workspace"],env["epoch"]),cfg["device"]["sign_public"]); env=seal_event(value,row["workspace"],current,key(cfg,row["workspace"],current)); path,size=encrypted_file(root,row["event"],env); state.execute("UPDATE outbox SET epoch=?,path=?,size=? WHERE workspace=? AND event=?",(current,str(path),size,row["workspace"],row["event"]))
                else:
                    if found["envelope"]!=env or not isinstance(found["cursor"],int) or isinstance(found["cursor"],bool): raise ValueError("relay upload acknowledgement mismatch")
                    acknowledged.append((row,env,expected,{"cursor":found["cursor"]})); continue
            prepared.append((row,env,expected))
        state.commit(); result=request(cfg,{"op":"upload_many","envelopes":[p[1] for p in prepared]})["events"] if prepared else []
        if len(result)!=len(prepared) or any(not isinstance(r.get("cursor"),int) or isinstance(r["cursor"],bool) or r["cursor"]<1 for r in result): raise ValueError("relay upload acknowledgement mismatch")
        for row,env,path,ack in acknowledged+[(row,env,path,ack) for (row,env,path),ack in zip(prepared,result)]:
            state.execute("INSERT OR REPLACE INTO receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",(row["workspace"],row["event"],ack["cursor"],row["author"],row["seq"],env["epoch"],row["kind"],row["payload_v"],row["entity"],row["revision"],row["status"])); state.execute("DELETE FROM outbox WHERE workspace=? AND event=?",(row["workspace"],row["event"]))
        state.commit(); [path.unlink(missing_ok=True) for row,env,path,ack in acknowledged+[(row,env,path,ack) for (row,env,path),ack in zip(prepared,result)]]
    upload_blobs(cfg,state,root,active)
def reconcile_replicas(cfg,state,root,ws,envelopes):
    for page in _upload_batches(envelopes,48*1024**2,lambda env:len(json.dumps(env,separators=(",",":")).encode())):
        present=request(cfg,{"op":"replica_reconcile","workspace":ws,"replicas":(ids:=[e["replica"] for e in page])})["present"]
        if not isinstance(present,dict) or not set(present)<=set(ids) or any(not isinstance(v,int) or isinstance(v,bool) or v<1 for v in present.values()): raise ValueError("relay replica inventory mismatch")
        missing=[env for env in page if env["replica"] not in present]; uploaded=request(cfg,{"op":"replica_upload_many","envelopes":missing})["replicas"] if missing else []
        if len(uploaded)!=len(missing) or any(not isinstance(r.get("cursor"),int) or isinstance(r["cursor"],bool) or r["cursor"]<1 for r in uploaded): raise ValueError("relay replica acknowledgement mismatch")
        cursors={**present,**{env["replica"]:ack["cursor"] for env,ack in zip(missing,uploaded)}}; state.executemany("INSERT OR REPLACE INTO replica_receipts VALUES (?,?,?,?)",[(ws,env["replica"],env["epoch"],cursors[env["replica"]]) for env in page]); state.commit()
def replica_inventory(cfg,state,ws,candidates):
    found={}
    for rows in (candidates[i:i+500] for i in range(0,len(candidates),500)):
        present=request(cfg,{"op":"replica_reconcile","workspace":ws,"replicas":(page:=[r[0] for r in rows])})["present"]
        if not isinstance(present,dict) or not set(present)<=set(page) or any(not isinstance(v,int) or isinstance(v,bool) or v<1 for v in present.values()): raise ValueError("relay replica inventory mismatch")
        found.update(present); available=[(ws,replica,epoch,present[replica]) for replica,epoch in rows if replica in present]; missing=[(ws,replica,epoch) for replica,epoch in rows if replica not in present]; available and state.executemany("INSERT OR REPLACE INTO replica_receipts VALUES (?,?,?,?)",available); missing and state.executemany("DELETE FROM replica_receipts WHERE workspace=? AND replica=? AND epoch=?",missing)
    state.commit(); return set(found)
def reconcile_blobs(cfg,state,root,ws,envelopes):
    for page in (envelopes[i:i+500] for i in range(0,len(envelopes),500)):
        present=request(cfg,{"op":"blob_reconcile","workspace":ws,"blobs":(ids:=[e["blob"] for e in page])})["present"]
        if not isinstance(present,dict) or not set(present)<=set(ids) or any(not isinstance(v,int) or isinstance(v,bool) or v<1 for v in present.values()): raise ValueError("relay blob inventory mismatch")
        for env in page:
            if env["blob"] in present: state.execute("INSERT OR REPLACE INTO blob_receipts VALUES (?,?,?,?)",(ws,env["blob"],env["epoch"],present[env["blob"]]))
            else:
                state.execute("DELETE FROM blob_receipts WHERE workspace=? AND blob=? AND epoch=?",(ws,env["blob"],env["epoch"]));
                if not state.execute("SELECT 1 FROM blob_outbox WHERE workspace=? AND blob=? AND epoch=?",(ws,env["blob"],env["epoch"])).fetchone(): path,size=encrypted_file(root,"blob-"+digest((ws,env["blob"],env["epoch"],env["uploader"])),env); state.execute("INSERT INTO blob_outbox VALUES (?,?,?,?,?,?)",(ws,env["blob"],env["epoch"],env["uploader"],str(path),size))
def upload_blobs(cfg,state,root,workspaces):
    for row in [r for r in state.execute("SELECT * FROM blob_outbox ORDER BY workspace,blob").fetchall() if r["workspace"] in workspaces]:
        path=Path(row["path"]); env=json.loads(path.read_text())
        if path.is_symlink() or (env["workspace"],env["blob"],env["epoch"],env["uploader"])!=(row["workspace"],row["blob"],row["epoch"],row["uploader"]): raise ValueError("invalid blob outbox")
        ack=request(cfg,{"op":"blob_upload","envelope":env}); state.execute("INSERT OR REPLACE INTO blob_receipts VALUES (?,?,?,?)",(row["workspace"],row["blob"],row["epoch"],ack["cursor"])); state.execute("DELETE FROM blob_outbox WHERE workspace=? AND blob=? AND epoch=? AND uploader=?",(row["workspace"],row["blob"],row["epoch"],row["uploader"])); state.commit(); path.unlink(missing_ok=True)
def pull_origins(cfg,state,root,ws):
    sid=ws["id"]; values=request(cfg,{"op":"origin_pull","workspace":sid})["origins"]
    if not values: return {r[0] for r in state.execute("SELECT origin FROM origin_bindings WHERE workspace=?",(sid,)).fetchall()}
    valid={}; invalid={}
    for item in values:
        env=item["envelope"]; identity=(env["origin"],env["epoch"])
        if env["workspace"]!=sid or not isinstance(item["cursor"],int) or isinstance(item["cursor"],bool) or not access_from(cfg,sid)<=env["epoch"]<=cfg["controls"][sid]["epoch"]: raise ValueError("origin bundle response mismatch")
        if identity in valid: continue
        try: body=open_origin(env,key(cfg,sid,env["epoch"])); controls=control_chain(body["controls"]); origin=controls[0]["workspace"]
        except ValueError: invalid[identity]=item["cursor"]
        else:
            if origin==sid: raise ValueError("origin bundle response mismatch")
            valid[identity]=(item,body,controls,origin); invalid.pop(identity,None)
    if invalid: raise ValueError(f"invalid origin bundle: no valid delivery copy for {len(invalid)} object(s)")
    core=open_db(core_path(root)); init_schema(core); core.execute("BEGIN")
    try:
        for item,body,controls,origin in valid.values():
            env=item["envelope"]; project_workspace_controls(core,controls); state.execute(f"INSERT OR REPLACE INTO {'origin_bindings' if body['rows'] else 'control_dependencies'} VALUES (?,?,?,?,?)",(sid,origin,env["origin"],env["epoch"],item["cursor"]))
        state.commit(); core.execute("COMMIT"); return {r[0] for r in state.execute("SELECT origin FROM origin_bindings WHERE workspace=?",(sid,)).fetchall()}
    except BaseException: core.execute("ROLLBACK"); state.rollback(); raise
    finally: core.close()
def local_replica_ids(path,cfg,workspace,epochs):
    if not path.is_file(): return set()
    fields=("workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature"); core=open_db(path,True); cursor=core.execute("SELECT workspace_id,authorization_workspace_id,row_kind,source_row_id,encoding_v,content_hash,revision,previous_revision,state,author_user_id,author_device_id,authorization_epoch,signature FROM remote.row_proofs"); found=set()
    while rows:=cursor.fetchmany(5000): found.update((fingerprint(key(cfg,workspace,epoch),digest({"v":1,"kind":"row.proof",**dict(zip(fields,row))})),epoch) for row in rows for epoch in epochs)
    core.close(); return found
def pull_row_replicas(cfg,state,root,ws,recover=None,origins=()):
    sid,stamp=ws["id"],bridge_stamp(root); saved=(state.execute("SELECT value FROM meta WHERE key=?",(f"replica_projection:{sid}",)).fetchone() or [None])[0]; reset=saved!=stamp; after=0 if reset else int((state.execute("SELECT value FROM meta WHERE key=?",(f"replica_cursor:{sid}",)).fetchone() or [0])[0]); cursor,total=after,0; dependencies={r[0] for r in state.execute("SELECT origin FROM control_dependencies WHERE workspace=?",(sid,)).fetchall()}; controls=ws["controls"]+stored_controls(core_path(root),set(origins)|dependencies); known=set() if reset else {(r[0],r[1]) for r in state.execute("SELECT replica,epoch FROM replica_receipts WHERE workspace=?",(sid,)).fetchall()}; valid=set(known); invalid={}; checked=not reset and (recover is None or not known)
    while True:
        result=request(cfg,{"op":"replica_pull","workspace":sid,"after":cursor,"limit":500}); floor,tail=result["floor"],result["tail"]
        if not all(isinstance(v,int) and not isinstance(v,bool) and v>=0 for v in (floor,tail)) or floor>tail and tail: raise ValueError("relay replica cursor window is invalid")
        if cursor>tail: cursor=after=0; known=set(); valid=set(); invalid={}; state.execute("DELETE FROM meta WHERE key=?",(f"replica_cursor:{sid}",)); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(f"replica_repair:{sid}","1")); state.commit(); continue
        if not checked and result["replicas"]:
            epochs={r[1] for r in known} or {r["epoch"] for r in ws["keys"]}; local=local_replica_ids(core_path(root),cfg,sid,epochs); known=known&local if known else local; valid=set(known); checked=True
        opened=[]; received=[]
        for item in result["replicas"]:
            env=item["envelope"]
            if not isinstance(item["cursor"],int) or isinstance(item["cursor"],bool) or not cursor<item["cursor"]<=tail or env["workspace"]!=sid or not access_from(cfg,sid)<=env["epoch"]<=cfg["controls"][sid]["epoch"]: raise ValueError("row replica envelope response mismatch")
            cursor=item["cursor"]; received.append(item); identity=(env["replica"],env["epoch"])
            if identity not in valid:
                try: body=open_replica(env,key(cfg,sid,env["epoch"]))
                except ValueError: invalid.setdefault(identity,item["cursor"])
                else: opened.append((item,body)); valid.add(identity); invalid.pop(identity,None)
        accepted=apply_row_replicas(core_path(root),[body for item,body in opened],sid,controls,recover,cfg["user"],root=root); accepted_keys={(item["envelope"]["replica"],item["envelope"]["epoch"]) for (item,body),ok in zip(opened,accepted) if body["proof"]["kind"]=="row.proof" or ok}
        known|=accepted_keys
        receipts=[(sid,item["envelope"]["replica"],item["envelope"]["epoch"],item["cursor"]) for item in received if (item["envelope"]["replica"],item["envelope"]["epoch"]) in known|accepted_keys]; receipts and state.executemany("INSERT OR REPLACE INTO replica_receipts VALUES (?,?,?,?)",receipts)
        after=min(invalid.values())-1 if invalid else cursor; total+=len(received); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?)",(f"replica_cursor:{sid}",str(after),f"replica_projection:{sid}",stamp)); state.commit()
        if cursor>=tail:
            if invalid: raise ValueError(f"invalid row replica: no valid delivery copy for {len(invalid)} object(s)")
            return total
        if not result["replicas"]: raise ValueError("relay replica tail cannot be reached")
def pull_blobs(cfg,state,root,ws,recover=None):
    sid=ws["id"]; after=int((state.execute("SELECT value FROM meta WHERE key=?",(f"blob_cursor:{sid}",)).fetchone() or [0])[0]); cursor,total=after,0; known={(r[0],r[1]) for r in state.execute("SELECT blob,epoch FROM blob_receipts WHERE workspace=?",(sid,)).fetchall()}; valid=set(known); invalid={}; checked=recover is None or not known
    while True:
        result=request(cfg,{"op":"blob_pull","workspace":sid,"after":cursor,"limit":20}); floor,tail=result["floor"],result["tail"]
        if not all(isinstance(v,int) and not isinstance(v,bool) and v>=0 for v in (floor,tail)) or floor>tail and tail: raise ValueError("relay blob cursor window is invalid")
        if cursor>tail: cursor=after=0; known=set(); valid=set(); invalid={}; state.execute("DELETE FROM meta WHERE key=?",(f"blob_cursor:{sid}",)); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(f"replica_repair:{sid}","1")); state.commit(); continue
        if not checked and result["blobs"]:
            core=open_db(core_path(root),True); hashes=[r[0] for r in core.execute("SELECT content_hash FROM attachment_bodies").fetchall()]; core.close(); epochs={r[1] for r in known}; known&={(fingerprint(key(cfg,sid,epoch),body_hash),epoch) for body_hash in hashes for epoch in epochs}; valid=set(known); checked=True
        received=[]
        for item in result["blobs"]:
            env=item["envelope"]
            if not isinstance(item["cursor"],int) or isinstance(item["cursor"],bool) or not cursor<item["cursor"]<=tail or env["workspace"]!=sid or not access_from(cfg,sid)<=env["epoch"]<=cfg["controls"][sid]["epoch"]: raise ValueError("blob replica envelope response mismatch")
            cursor=item["cursor"]; received.append(item); identity=(env["blob"],env["epoch"])
            if identity not in valid:
                try: data,body_hash=open_blob(env,key(cfg,sid,env["epoch"]))
                except ValueError: invalid.setdefault(identity,item["cursor"])
                else: project_attachment_body(core_path(root),data,body_hash); known.add(identity); valid.add(identity); invalid.pop(identity,None)
        for item in received:
            env=item["envelope"]; identity=(env["blob"],env["epoch"])
            if identity in valid: state.execute("INSERT OR REPLACE INTO blob_receipts VALUES (?,?,?,?)",(sid,*identity,item["cursor"]))
        after=min(invalid.values())-1 if invalid else cursor; total+=len(received); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(f"blob_cursor:{sid}",str(after))); state.commit()
        if cursor>=tail:
            if invalid: raise ValueError(f"invalid blob replica: no valid delivery copy for {len(invalid)} object(s)")
            return total
        if not result["blobs"]: raise ValueError("relay blob tail cannot be reached")
def pull(cfg,state,root=None):
    root=local_root(root)
    server=refresh(cfg,root)
    summary={}
    for ws in server["workspaces"]:
        if not ws["device_authorized"]: continue
        sid=ws["id"]; current=state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(sid,)).fetchone(); lifecycle=current[0] if current else "rebaselining"; state.execute("INSERT OR REPLACE INTO sync_states VALUES (?,?,COALESCE((SELECT tail FROM sync_states WHERE workspace=?),0),COALESCE((SELECT floor FROM sync_states WHERE workspace=?),0),NULL)",(sid,lifecycle,sid,sid)); state.commit()
        records={r["device"]["id"]:r for control in ws["controls"] for r in control["devices"].values()}; devices={device:r["device"] for device,r in records.items()}; authors={device:r["user"] for device,r in records.items()}
        after=(state.execute("SELECT cursor FROM cursors WHERE workspace=?",(sid,)).fetchone() or [0])[0]; seen=(state.execute("SELECT value FROM meta WHERE key=?",(f"history_from:{sid}",)).fetchone() or [str(ws["history_from"])])[0]; earliest=min([k["epoch"] for k in ws["keys"]],default=ws["epoch"]); old_key=int((state.execute("SELECT value FROM meta WHERE key=?",(f"key_from:{sid}",)).fetchone() or [earliest])[0]); start=access_from(cfg,sid); candidates=[control["boundary"] for control in ws["controls"] if control["boundary"]["epoch"]==start]; boundary=required(candidates[0] if candidates and all(value==candidates[0] for value in candidates) else None,ValueError("signed history boundary is ambiguous")); proof=digest(boundary); old_proof=(state.execute("SELECT value FROM meta WHERE key=?",(f"boundary:{sid}",)).fetchone() or [None])[0]; mode=(state.execute("SELECT value FROM meta WHERE key=?",(f"archive_mode:{sid}",)).fetchone() or [None])[0]; recover=mode if mode=="adopt" or ws["kind"]=="personal" and mode in ("native","import") else None
        deferred=state.execute("SELECT kind,payload_v,required FROM deferred_events WHERE workspace=?",(sid,)).fetchall(); reclassify=any(event_support(r)!=("required" if r["required"] else "optional") for r in deferred)
        if not current or ws["history_from"]<int(seen) or earliest<old_key or proof!=old_proof or reclassify: after=0; reset_history(state,sid,boundary); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?),(?,?)",(f"boundary:{sid}",proof,f"replica_cursor:{sid}","0",f"blob_cursor:{sid}","0")); state.commit()
        try:
            total=0; origins=pull_origins(cfg,state,root,ws); replicas=pull_row_replicas(cfg,state,root,ws,recover,origins); blobs=pull_blobs(cfg,state,root,ws,recover)
            while True:
                result=request(cfg,{"op":"pull","workspace":sid,"after":after,"limit":500}); floor,tail=result["floor"],result["tail"]
                if not all(isinstance(v,int) and not isinstance(v,bool) and v>=0 for v in (floor,tail)) or floor>tail and tail or after>tail: raise ValueError("relay cursor window is invalid")
                incoming=[]
                for item in result["events"]:
                    env=request(cfg,{"op":"fetch","workspace":sid,"event":item["event"]})["envelope"] if item.get("lazy") else item["envelope"]
                    if (env["workspace"],env["event"])!=(sid,item.get("event",env["event"])) or not access_from(cfg,sid)<=env["epoch"]<=cfg["controls"][sid]["epoch"]: raise ValueError("event envelope response mismatch")
                    value=open_event(env,key(cfg,sid,env["epoch"]),signer(devices,env["author"])); support=event_support(value); sequence(state,sid,value); receipt(state,sid,value,item["cursor"],env["epoch"])
                    if authors[value["author"]]==cfg["user"]:
                        state.execute("INSERT OR REPLACE INTO publication_heads VALUES (?,?,?,?,?)",(sid,cfg["user"],value["entity"],value["revision"],value["id"]))
                    if value["author"]==cfg["device"]["id"]:
                        if value["seq"]>=int((state.execute("SELECT value FROM meta WHERE key=?",(f"seq:{sid}",)).fetchone() or ["0"])[0]): state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?)",(f"seq:{sid}",str(value["seq"]),f"prev:{sid}",value["id"]))
                    if support!="supported": state.execute("INSERT OR REPLACE INTO deferred_events VALUES (?,?,?,?,?,?)",(sid,value["id"],item["cursor"],value["kind"],value["payload_v"],support=="required"))
                    else: incoming.append((sid,value))
                    after=max(after,item["cursor"])
                project_many(core_path(root),state,incoming,cfg["device"]["id"],root,False,authors,recover,cfg["user"]); total+=len(result["events"]); state.execute("INSERT OR REPLACE INTO cursors VALUES (?,?)",(sid,after)); state.execute("INSERT OR REPLACE INTO meta VALUES (?,?),(?,?)",(f"history_from:{sid}",str(ws["history_from"]),f"key_from:{sid}",str(earliest))); state.commit()
                if after>=tail: break
                if not result["events"]: raise ValueError("relay tail cannot be reached")
            verify_history(state,sid,ws["controls"],start)
            if mode=="import": raise ValueError("archive rollback or replacement recovered additively; publication remains blocked")
            state.execute("INSERT OR REPLACE INTO sync_states VALUES (?,'ready',?,?,NULL)",(sid,tail,floor)); state.commit(); summary[sid]={"events":total,"replicas":replicas,"blobs":blobs,"cursor":after,"tail":tail,"floor":floor}
        except Exception as e:
            state.rollback(); state.execute("INSERT OR REPLACE INTO sync_states VALUES (?,'blocked',COALESCE((SELECT tail FROM sync_states WHERE workspace=?),0),COALESCE((SELECT floor FROM sync_states WHERE workspace=?),0),?)",(sid,sid,sid,str(e))); state.commit(); raise
    return summary
def fetch_lazy(cfg,state,event_id=None,root=None):
    root=local_root(root)
    server=refresh(cfg,root); controls={ws["id"]:ws["controls"] for ws in server["workspaces"]}; sql="SELECT workspace,event,cursor FROM lazy_events"+(" WHERE event=?" if event_id else ""); rows=[r for r in state.execute(sql,(event_id,) if event_id else ()).fetchall() if r[0] in controls]
    for ws,eid,cursor in rows:
        records={r["device"]["id"]:r for control in controls[ws] for r in control["devices"].values()}; devices={device:r["device"] for device,r in records.items()}; authors={device:r["user"] for device,r in records.items()}
        env=request(cfg,{"op":"fetch","workspace":ws,"event":eid})["envelope"]
        if (env["workspace"],env["event"])!=(ws,eid) or not access_from(cfg,ws)<=env["epoch"]<=cfg["controls"][ws]["epoch"]: raise ValueError("lazy event response mismatch")
        value=open_event(env,key(cfg,ws,env["epoch"]),signer(devices,env["author"])); support=event_support(value); sequence(state,ws,value); receipt(state,ws,value,cursor,env["epoch"]); support=="supported" and project(core_path(root),state,value,ws,cfg["device"]["id"],root=root,batch=True,authors=authors); support!="supported" and state.execute("INSERT OR REPLACE INTO deferred_events VALUES (?,?,?,?,?,?)",(ws,value["id"],cursor,value["kind"],value["payload_v"],support=="required")); state.execute("DELETE FROM lazy_events WHERE event=?",(eid,))
    state.commit(); return len(rows)
def sync_once(root=None,force=False):
    root=local_root(root)
    with sync_run(root):
        cfg=load(root); _,_,state_path=paths(root); info=inspect_state(state_path,force); cutover=None
        relocate_attachments(core_path(root),paths(root)[0]/"attachments")
        if info["status"] in ("incompatible","invalid"): refresh(cfg,root); info["status"]=="incompatible" and rescue_bindings(cfg,state_path,root); cutover=cutover_state(state_path)
        state=connect(state_path)
        try:
            drain_hooks(); refresh(cfg,root); ready={r[0] for r in state.execute("SELECT workspace FROM sync_states WHERE lifecycle='ready'").fetchall()}
            if force:
                for ws in ready: state.execute("INSERT OR REPLACE INTO meta VALUES (?,'1')",(f"replica_repair:{ws}",))
            state.commit(); upload(cfg,state,root,ready); prepare_archive(cfg,state,root); baseline=archive_info(root); bound={r[0] for r in state.execute("SELECT DISTINCT workspace FROM origin_bindings").fetchall()}
            pull(cfg,state,root)
            ready={r[0] for r in state.execute("SELECT workspace FROM sync_states WHERE lifecycle='ready'").fetchall()}; authorized={w["id"] for w in cfg["server_state"]["workspaces"] if w["device_authorized"]}
            aliases={ws:reconcile_provider_aliases(core_path(root),cfg,ws) for ws in ready&authorized if cfg["workspaces"].get(ws,{}).get("kind")=="personal"}
            [state.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(f"provider_aliases:{ws}",json.dumps(value,sort_keys=True))) for ws,value in aliases.items()]
            state.commit()
            settle_sharing(cfg,state,ready&authorized,root)
            path,active,generation=(path:=core_path(root)),authorized,archive_info(root)[1] if path.is_file() else 0
            known=core_repository_state(core) if (core:=open_db(path,True) if path.is_file() else None) else {"roots":{},"checkouts":{},"checkout_roots":{},"lineages":{},"aliases":{}}
            (core and core.close(),promote_paths(cfg,state,known,root),upgrade_repository_policies(cfg,state,known,root),[retain_sharing(cfg,ws,root,state) for ws in ready&active])
            if baseline and baseline[2]==0:
                for ws in ready&active-bound: state.execute("INSERT OR IGNORE INTO meta VALUES (?,?)",(f"core_generation:{ws}",str(generation)))
            scans=[(ws,meta,state.execute("SELECT value FROM meta WHERE key=?",(f"core_generation:{ws}",)).fetchone()) for ws,meta in cfg["workspaces"].items() if path.is_file() and ws in ready and ws in active and f"{ws}:{meta['epoch']}" in cfg["keys"]]
            scans=[(ws,meta,prior) for ws,meta,prior in scans if prior is None or int(prior[0])!=generation or state.execute("SELECT 1 FROM meta WHERE key=?",(f"replica_repair:{ws}",)).fetchone()]
            if scans:
                routes={ws:sharing_routes(state,ws,cfg["user"],cfg.get("bindings",{}),known) for ws,meta,prior in scans}; core=open_db(path,True); batches=[]
                for ws,meta,prior in scans:
                    repos,roots,match=routes[ws]
                    changes=None if prior is None or state.execute("SELECT 1 FROM meta WHERE key=?",(f"replica_repair:{ws}",)).fetchone() else set(core_archive_changes(core,int(prior[0]))[1])
                    scope=set()
                    records=scan(core,state,meta["kind"],repos,roots,changes,ws,scope,match=match,user=cfg["user"])
                    records=protect_all(records,root,ws) if meta["kind"]=="team" else records
                    batches.append((ws,records,scope,prior is None))
                core.close()
                for ws,records,scope,initial in batches:
                    bindings={r[0]:r[1] for r in state.execute("SELECT origin,epoch FROM origin_bindings WHERE workspace=?",(ws,)).fetchall()}; origins=set(bindings); repair=initial or bool(state.execute("SELECT 1 FROM meta WHERE key=?",(f"replica_repair:{ws}",)).fetchone()); attest_rows(path,cfg,ws,records,origins)
                    keys={epoch:key(cfg,ws,epoch) for epoch in range(access_from(cfg,ws),cfg["workspaces"][ws]["epoch"]+1) if f"{ws}:{epoch}" in cfg["keys"]}; known={r[0] for r in state.execute("SELECT replica FROM replica_receipts WHERE workspace=?",(ws,)).fetchall()}
                    for i in range(0,max(len(records),1),5000): reconcile_replicas(cfg,state,root,ws,row_replicas(path,cfg,ws,records[i:i+5000],keys,known,origins,bindings,lambda ids:replica_inventory(cfg,state,ws,ids),repair and i==0))
                    known_blobs={r[0] for r in state.execute("SELECT blob FROM blob_receipts WHERE workspace=? UNION SELECT blob FROM blob_outbox WHERE workspace=?",(ws,ws)).fetchall()}; reconcile_blobs(cfg,state,root,ws,blob_replicas(path,cfg,ws,records,keys,known_blobs,origins,bindings,repair))
                    heads={r[0]:r[1] for r in state.execute("SELECT entity,revision FROM publication_heads WHERE workspace=? AND owner=?",(ws,cfg["user"])).fetchall()}
                    for record in records:
                        if record["kind"] not in SIGNED: publish(cfg,state,ws,record,root,True,heads)
                    state.execute("DELETE FROM team_scopes WHERE workspace=?",(ws,)); state.executemany("INSERT INTO team_scopes VALUES (?,?)",[(ws,c) for c in scope])
                [state.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(f"core_generation:{ws}",str(generation))) for ws,meta,prior in scans]
            for ws,meta in cfg["workspaces"].items():
                if ws in ready and ws in active and meta["kind"]=="personal" and f"{ws}:{meta['epoch']}" in cfg["keys"]:
                    known={r[0] for r in state.execute("SELECT replica FROM replica_receipts WHERE workspace=?",(ws,)).fetchall()}; repair=bool(state.execute("SELECT 1 FROM meta WHERE key=?",(f"replica_repair:{ws}",)).fetchone()); reconcile_replicas(cfg,state,root,ws,bridge_replicas(root,cfg,ws,meta["kind"],key(cfg,ws,meta["epoch"]),known,lambda ids:replica_inventory(cfg,state,ws,ids) if repair else set(known)))
            [state.execute("DELETE FROM meta WHERE key=?",(f"replica_repair:{ws}",)) for ws in ready&active]
            state.commit(); upload(cfg,state,root,ready)
            pull(cfg,state,root); remember_archive(cfg,state,root); state.execute("INSERT OR REPLACE INTO meta VALUES ('last_sync',?)",(str(time.time()),)); state.commit()
        finally: state.close()
        return cutover
def bind_origin(cfg,state,ws,origin,root=None):
    root=local_root(root); db=open_db(core_path(root),True); exists=db.execute("SELECT 1 FROM remote.row_proofs WHERE workspace_id=?",(origin,)).fetchone(); dependencies={r[0] for r in db.execute("SELECT DISTINCT authorization_workspace_id FROM remote.row_proofs WHERE workspace_id=? AND authorization_workspace_id<>workspace_id",(origin,)).fetchall()}; controls=stored_controls(core_path(root),{origin}|dependencies); db.close(); current=cfg["workspaces"][ws]["epoch"]
    if origin==ws or not exists: raise ValueError("origin has no retained row proofs")
    for target,rows in [(origin,True),*((d,False) for d in dependencies)]:
        chain=control_chain([c for c in controls if c["workspace"]==target]); env=seal_origin(chain,ws,current,key(cfg,ws,current),cfg["device"]["id"],rows); ack=request(cfg,{"op":"origin_upload","envelope":env}); required(isinstance(ack.get("cursor"),int) and not isinstance(ack["cursor"],bool),ValueError("origin upload acknowledgement mismatch")); state.execute(f"INSERT OR REPLACE INTO {'origin_bindings' if rows else 'control_dependencies'} VALUES (?,?,?,?,?)",(ws,target,env["origin"],current,ack["cursor"]))
    state.execute("DELETE FROM meta WHERE key=?",(f"core_generation:{ws}",)); state.commit(); return len(controls)
def add_member(cfg,ws,user,remove=False,root=None):
    refresh(cfg,root); members={u:m["role"] for u,m in cfg["controls"][ws]["members"].items()}; devices=[]
    if remove:
        found=request(cfg,{"op":"directory","user":user}); target=directory_user(found,user) if found["users"] else {"id":user}; members.pop(target["id"],None)
    else:
        found=request(cfg,{"op":"directory","user":user}); trusted(found["devices"]); target=directory_user(found,user); members[target["id"]]="member"; devices+=found["devices"]
    return rotate(cfg,ws,members,[d for d in {d["id"]:d for d in devices}.values() if d["user_id"] in members],root=root)
def grant_all(cfg,ws,user,root=None):
    found=request(cfg,{"op":"directory","user":user}); target=directory_user(found,user); refresh(cfg,root); previous=cfg["controls"][ws]; devices=[r["device"] for r in previous["devices"].values() if r["user"]==target["id"]]; envelopes={}
    for name,value in cfg["keys"].items():
        if name.startswith(ws+":"):
            epoch=int(name.rsplit(":",1)[1]); envelopes[str(epoch)]={d["id"]:seal_key(unb64(value),d["box_public"],f"workspace:{ws}:epoch:{epoch}") for d in devices}
    members={**previous["members"],target["id"]:{**previous["members"][target["id"]],"history_from":1}}; records={d:{**r,"history":True} if r["user"]==target["id"] else r for d,r in previous["devices"].items()}; control=control_body(cfg,previous,key(cfg,ws,previous["epoch"]),"history",members,records); request(cfg,sign_control(cfg["device"],{"op":"grant_all","workspace":ws,"user":target["id"],"control":control,"envelopes":envelopes})); cfg["controls"][ws]=control; save(cfg,root); return len(envelopes)
def remove_device(cfg,ws,device_id,root=None):
    refresh(cfg,root); members={u:m["role"] for u,m in cfg["controls"][ws]["members"].items()}; return rotate(cfg,ws,members,[],[device_id],root)
def request_device(cfg,ws,root=None,delay=3600):
    refresh(cfg,root); base=cfg["controls"][ws]
    if cfg["user"] not in base["members"] or cfg["device"]["id"] in base["devices"] or cfg["device"]["id"] in base["removed"]: raise ValueError("device is not pending for this workspace")
    target=own_record(cfg,False); voters=electorate(base,cfg["user"]); not_before=time.time()+delay if len(voters)==1 and not any(d["user"]==cfg["user"] for d in base["devices"].values()) else time.time(); value=device_proposal(cfg["device"],ws,base,target,time.time()+86400,not_before); request(cfg,{"op":"propose","proposal":value}); return value
def proposals(cfg,ws): return request(cfg,{"op":"proposals","workspace":ws})["proposals"]
def pending(cfg,ws,device_id,kind):
    if len(found:=[p for p in proposals(cfg,ws) if p["proposal"]["kind"]==kind and p["proposal"]["target"]["device"]["id"]==device_id and p["proposal"]["base"]==state_hash(base:=cfg["controls"][ws])])!=1: raise ValueError("pending device proposal not found")
    return base,found[0]
def approve_device(cfg,ws,device_id,approve=True,root=None):
    refresh(cfg,root); base,item=pending(cfg,ws,device_id,"device.proposal"); target=item["proposal"]["target"]; same=cfg["user"]==target["user"] and cfg["device"]["id"] in base["devices"]
    if same and not approve:
        request(cfg,sign_control(cfg["device"],{"op":"reject","workspace":ws,"proposal":state_hash(item["proposal"])})); return {"approved":False,"rejected":True}
    if same: verify_proposal(base,item["proposal"])
    if not same:
        request(cfg,{"op":"vote","vote":device_vote(cfg["device"],cfg["user"],item["proposal"],approve)}); item=next(p for p in proposals(cfg,ws) if state_hash(p["proposal"])==state_hash(item["proposal"]))
        yes=len({v["voter"] for v in item["votes"] if v["approve"]}); needed=len(electorate(base,target["user"]))//2+1
        if not approve or yes<needed: return {"approved":False,"votes":yes,"needed":needed}
        approved(base,item["proposal"],item["votes"])
    new,epoch,boundary=os.urandom(32),base["epoch"]+1,next_boundary(cfg,ws,root); inherit=base["devices"][cfg["device"]["id"]]["history"] if same else False; entry={**target,"history":inherit}; records={**base["devices"],device_id:entry}; action="self_approve" if same else "quorum_approve"; proof={"proposal":item["proposal"],"votes":item["votes"]}; control=control_body(cfg,base,new,action,devices=records,approval=proof,boundary=boundary); envs={d:seal_key(new,r["device"]["box_public"],f"workspace:{ws}:epoch:{epoch}") for d,r in records.items()}; start=base["members"][target["user"]]["history_from"]; history={name.rsplit(":",1)[1]:seal_key(unb64(value),entry["device"]["box_public"],f"workspace:{ws}:epoch:{name.rsplit(':',1)[1]}") for name,value in cfg["keys"].items() if inherit and name.startswith(ws+":") and int(name.rsplit(":",1)[1])>=start}
    body={"op":"rotate","workspace":ws,"control":control,"envelopes":envs}; history and body.update(history_envelopes={device_id:history})
    request(cfg,sign_control(cfg["device"],body)); cfg["keys"][f"{ws}:{epoch}"]=b64(new); cfg["workspaces"][ws]["epoch"]=epoch; cfg["controls"][ws]=control; update_recovery(cfg,root); control_event(cfg,ws,action,device_id,root); retain_sharing(cfg,ws,root); return {"approved":True,"epoch":epoch,"history":len(history)}
def request_history(cfg,ws,root=None,delay=3600):
    refresh(cfg,root); base=cfg["controls"][ws]; current=base["devices"].get(cfg["device"]["id"])
    if not current or current["history"]: raise ValueError("device does not need history approval")
    voters=electorate(base,cfg["user"]); value=device_proposal(cfg["device"],ws,base,{**current,"history":True},time.time()+86400,time.time()+delay if len(voters)==1 else time.time(),kind="history.proposal"); request(cfg,{"op":"propose","proposal":value}); return value
def approve_history(cfg,ws,device_id,approve=True,root=None):
    refresh(cfg,root); base,item=pending(cfg,ws,device_id,"history.proposal"); target=item["proposal"]["target"]; request(cfg,{"op":"vote","vote":device_vote(cfg["device"],cfg["user"],item["proposal"],approve)}); item=next(p for p in proposals(cfg,ws) if state_hash(p["proposal"])==state_hash(item["proposal"])); yes=len({v["voter"] for v in item["votes"] if v["approve"]}); needed=len(electorate(base,target["user"]))//2+1
    if not approve or yes<needed: return {"approved":False,"votes":yes,"needed":needed}
    approved(base,item["proposal"],item["votes"],kind="history.proposal"); device=target["device"]["id"]; records={**base["devices"],device:{**base["devices"][device],"history":True}}; proof={"proposal":item["proposal"],"votes":item["votes"]}; control=control_body(cfg,base,key(cfg,ws,base["epoch"]),"history_activate",devices=records,approval=proof); start=base["members"][target["user"]]["history_from"]; envs={str(epoch):seal_key(key(cfg,ws,epoch),target["device"]["box_public"],f"workspace:{ws}:epoch:{epoch}") for epoch in range(start,base["epoch"]+1)}
    request(cfg,sign_control(cfg["device"],{"op":"history_activate","workspace":ws,"control":control,"envelopes":envs})); cfg["controls"][ws]=control; save(cfg,root); control_event(cfg,ws,"history_activate",device,root); return {"approved":True,"history":len(envs)}

@remote.command("setup")
@locked
def setup_cmd(url:str,user:str,device:str=typer.Option("computer","--device")): cfg,recovery=setup_client(url,user,device); typer.echo(f"Personal workspace ready. User ID: {cfg['user']}. Recovery key (store offline): {recovery}")
@remote.command("recover")
@locked
def recover_cmd(url:str,user:str,device:str=typer.Option("computer","--device"),recovery:Optional[str]=typer.Option(None,"--recovery")): setup_client(url,user,device,recovery or typer.prompt("Recovery key",hide_input=True)); typer.echo("Device enrolled and keys rotated")
@remote.command("rehome")
@locked
def rehome_cmd(url:str): cfg,recovery=rehome_client(load(),url); typer.echo(f"Fresh relay ready. User ID retained: {cfg['user']}. New recovery key (store offline): {recovery}")
@remote.command("workspace")
@locked
def workspace_cmd(name:str): cfg=load(); typer.echo(create(cfg,name,"team"))
@remote.command("invite")
@locked
def invite_cmd(space:str,user:str): cfg=load(); typer.echo(f"epoch {add_member(cfg,workspace(cfg,space),user)}")
@remote.command("remove")
@locked
def remove_cmd(space:str,user:str): cfg=load(); typer.echo(f"epoch {add_member(cfg,workspace(cfg,space),user,True)}")
@remote.command("grant-all")
@locked
def grant_all_cmd(space:str,user:str): cfg=load(); typer.echo(f"Granted {grant_all(cfg,workspace(cfg,space),user)} epochs")
@remote.command("refound")
def refound_cmd(space:str,origin_workspace_id:str):
    with mutation_lock(None): cfg=load(); state=connect(paths()[2]); count=bind_origin(cfg,state,workspace(cfg,space),origin_workspace_id); state.close()
    sync_once(force=True); typer.echo(f"Bound {count} signed origin controls")
@remote.command("origins")
def origins_cmd(): db=open_db(core_path(),True); typer.echo(json.dumps([{"workspace":r[0],"rows":r[1]} for r in db.execute("SELECT workspace_id,COUNT(*) FROM remote.row_proofs GROUP BY workspace_id ORDER BY workspace_id").fetchall()])); db.close()
@remote.command("remove-device")
@locked
def remove_device_cmd(space:str,device_id:str): cfg=load(); typer.echo(f"epoch {remove_device(cfg,workspace(cfg,space),device_id)}")
@remote.command("request-device")
@locked
def request_device_cmd(space:str): cfg=load(); value=request_device(cfg,workspace(cfg,space)); typer.echo(f"Pending device {value['target']['device']['id']}; certificate {value['certificate_hash']}")
@remote.command("approve-device")
@locked
def approve_device_cmd(space:str,device_id:str,reject:bool=typer.Option(False,"--reject")): cfg=load(); typer.echo(json.dumps(approve_device(cfg,workspace(cfg,space),device_id,not reject)))
@remote.command("approvals")
def approvals_cmd(space:str): cfg=load(); typer.echo(json.dumps(proposals(cfg,workspace(cfg,space))))
@remote.command("request-history")
@locked
def request_history_cmd(space:str): cfg=load(); value=request_history(cfg,workspace(cfg,space)); typer.echo(f"Pending history approval for {value['target']['device']['id']}")
@remote.command("approve-history")
@locked
def approve_history_cmd(space:str,device_id:str,reject:bool=typer.Option(False,"--reject")): cfg=load(); typer.echo(json.dumps(approve_history(cfg,workspace(cfg,space),device_id,not reject)))
@remote.command("link")
@locked
def link_cmd(path:Path,space:str):
    ws=workspace(cfg:=load(),space); state=connect(paths()[2]); resolved=path.resolve(); repo=core_capture_repository(resolved,core_path()); kind="repository" if repo and repo["lineage"] else "path"; existing=[p for p in state.execute("SELECT value,evidence FROM policies WHERE workspace=? AND owner=? AND kind=?",(ws,cfg["user"],kind)).fetchall() if binding_path(cfg.get("bindings",{}).get(policy_binding(ws,kind,p[0])))==str(resolved)]; pending=[key.rsplit(":",1)[1] for key,bound in cfg.get("bindings",{}).items() if key.startswith(f"{ws}:{kind}:") and binding_path(bound)==str(resolved)]; value=existing[0][0] if existing else pending[0] if len(pending)==1 else digest(os.urandom(32))[:32]
    evidence=json.loads(existing[0][1]) if existing and existing[0][1] else {k:repo[k] for k in ("lineage","remotes")} if repo else None; record=repository_policy(cfg,state,ws,value,evidence) if repo else {"kind":"workspace.policy","entity":f"policy:path:{value}","payload":{"kind":"path","value":value}}
    bound=cfg.get("bindings",{}).get(policy_binding(ws,kind,value)); cfg.setdefault("bindings",{})[policy_binding(ws,kind,value)]=bound or ({"path":str(resolved),"repository":repo["id"],"checkout":repo["checkout"]} if repo and kind=="repository" else str(resolved)); save(cfg); publish(cfg,state,ws,record); upload(cfg,state)
    typer.echo(f"{kind} {value} -> {cfg['workspaces'][ws]['name']}")
@remote.command("config")
@locked
def config_cmd(space:str,auto_contribute:Optional[bool]=typer.Option(None,"--auto-contribute/--no-auto-contribute"),inherit:bool=typer.Option(False,"--inherit"),match:Optional[str]=typer.Option(None,"--match",help="cwd,edit; cwd; edit; or none")):
    ws=workspace(cfg:=load(),space)
    if cfg["workspaces"][ws]["kind"]!="team": raise typer.BadParameter("sharing policy applies only to team workspaces","space")
    state=connect(paths()[2]); current=sharing(state,ws,cfg["user"])
    requested=[] if match=="none" else [m.strip() for m in match.split(",") if m.strip()] if match is not None else current["match"]
    if match is not None and (len(requested)!=len(set(requested)) or not set(requested)<={"cwd","edit"}): raise typer.BadParameter("must be cwd,edit; cwd; edit; or none","--match")
    if inherit and auto_contribute is not None: raise typer.BadParameter("cannot combine with auto-contribute override","--inherit")
    result=configure_sharing(cfg,state,ws,None if inherit else auto_contribute if auto_contribute is not None else current["auto_contribute"],[m for m in ("cwd","edit") if m in requested]) if inherit or auto_contribute is not None or match is not None else current
    state.close()
    typer.echo(json.dumps({k:result[k] for k in ("auto_contribute","effective_auto_contribute","match","conflict")}))
@remote.command("sync")
def sync_cmd(): result=sync_once(force=True); typer.echo("Remote synchronized"+(f"; previous state preserved at {result['backup']}" if result else ""))
@remote.command("fetch")
@locked
def fetch_cmd(event_id:Optional[str]=None):
    with closing(connect(paths()[2])) as state: typer.echo(f"Fetched {fetch_lazy(load(),state,event_id)} lazy events")
@remote.command("watch")
def watch(interval:int=typer.Option(2,"--interval")):
    failures=0
    while True:
        try: sync_once()
        except Exception as e:
            failures+=1
            delay=min(300,interval*2**min(failures-1,9))
            base=paths()[0]
            base.mkdir(parents=True,exist_ok=True)
            error=base/"last_error"
            error.write_text(traceback.format_exc())
            os.chmod(error,0o600)
            failed_at=time.time()
            watch_status={"failures":failures,"failed_at":failed_at,"next_retry":failed_at+delay,"error":f"{type(e).__name__}: {e}"}
            save_watch_status(watch_status)
            typer.echo(f"Remote sync failed ({watch_status['error']}); retry in {delay}s",err=True)
        else:
            delay=interval
            if failures: typer.echo("Remote sync recovered")
            failures=0
            (paths()[0]/"last_error").unlink(missing_ok=True)
            save_watch_status({"failures":0,"succeeded_at":time.time()})
        time.sleep(delay)
@remote.command("enable")
def enable_cmd(remove:bool=typer.Option(False,"--remove")): not remove and install_hooks(False,False); typer.echo(enable(paths()[0],remove))
def doctor_status():
    try:
        cfg=load(); info=inspect_state(paths()[2],True)
        try: online="reachable" if health(cfg)["ok"] else "error"
        except Exception: online="error"
        if info["status"]!="current": return f"remote: {online}, state={info['status']}, schema={info['version'] or 'unknown'}, next_sync={'backup+rebaseline' if info['status'] in ('incompatible','invalid') else 'initialize'}"
        state=read_state(paths()[2])
        try:
            pending=state.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
            lazy=state.execute("SELECT COUNT(*) FROM lazy_events").fetchone()[0]
            deferred,required=state.execute("SELECT COUNT(*),COALESCE(SUM(required),0) FROM deferred_events").fetchone()
            lifecycle=",".join(f"{r[0][:8]}:{r[1]}" for r in state.execute("SELECT workspace,lifecycle FROM sync_states ORDER BY workspace").fetchall()) or "uninitialized"
            last=(state.execute("SELECT value FROM meta WHERE key='last_sync'").fetchone() or ["never"])[0]
            backup=(state.execute("SELECT value FROM meta WHERE key='state_cutover'").fetchone() or [None])[0]
            alias_blocked=sum(len(json.loads(r[0])["blocked"]) for r in state.execute("SELECT value FROM meta WHERE key LIKE 'provider_aliases:%'").fetchall())
        finally:
            state.close()
        watch_path=paths()[0]/"watch.json"
        watch=json.loads(watch_path.read_text()) if watch_path.exists() else {"failures":0}
        retry=f", next_retry={watch['next_retry']}" if watch["failures"] else ""
        return f"remote: {online}, user={cfg['user'][:8]}, device={cfg['device']['id'][:8]}, workspaces={len(cfg['workspaces'])}, epochs={len(cfg['keys'])}, lifecycle={lifecycle}, pending={pending}, lazy={lazy}, deferred={deferred}, required={required}, alias_blocked={alias_blocked}, watch_failures={watch['failures']}{retry}, last={last}"+(f", backup={json.loads(backup)['backup']}" if backup else "")
    except Exception as e: return f"remote: unavailable ({e})"
@remote.command("doctor")
def doctor_cmd(): typer.echo(doctor_status())
[register(app) for app in _pending]; _pending.clear()
