"""Opaque self-hosted relay. It authorizes envelopes but never receives content keys."""
import argparse, base64, hashlib, hmac, json, os, secrets, sqlite3, time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

V=CONTROL_V=1
APPROVAL_DELAY=int(os.environ.get("CONVOS_REMOTE_APPROVAL_DELAY","3600"))
REPLICA_QUOTA=int(os.environ.get("CONVOS_REMOTE_REPLICA_QUOTA",str(10*1024**3)))
BLOB_LIMIT,BLOB_QUOTA=32*1024**2,int(os.environ.get("CONVOS_REMOTE_BLOB_QUOTA",str(10*1024**3)))
CLOCK_SKEW,REGISTRATION_TTL=30,300
def canon(v): return json.dumps(v,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()
def unb64(v): return base64.urlsafe_b64decode(v+"="*(-len(v)%4))
def b64(v): return base64.urlsafe_b64encode(v).decode().rstrip("=")
def digest(v): return hashlib.sha256(v if isinstance(v,bytes) else canon(v)).hexdigest()
def public_id(value): return digest(unb64(value))[:32]
def verify_certificate(cert,root_public):
    sig,body=unb64(cert["signature"]),{k:v for k,v in cert.items() if k!="signature"}
    Ed25519PublicKey.from_public_bytes(unb64(root_public)).verify(sig,canon(body))
    if body["v"]!=V: raise ValueError(f"Unsupported certificate version {body['v']}")
    return body

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY,name TEXT UNIQUE,root_public TEXT NOT NULL,recovery TEXT,created REAL);
CREATE TABLE IF NOT EXISTS devices(id TEXT PRIMARY KEY,user_id TEXT NOT NULL,name TEXT,sign_public TEXT NOT NULL,box_public TEXT NOT NULL,token_hash TEXT UNIQUE NOT NULL,active INT NOT NULL DEFAULT 1,created REAL);
CREATE TABLE IF NOT EXISTS device_certificates(device TEXT PRIMARY KEY,certificate TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS relay_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS registration_uses(challenge TEXT PRIMARY KEY,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY,kind TEXT NOT NULL,epoch INT NOT NULL,created_by TEXT NOT NULL,created REAL);
CREATE TABLE IF NOT EXISTS members(workspace TEXT,user_id TEXT,role TEXT,active INT,joined_epoch INT,history_from INT,PRIMARY KEY(workspace,user_id));
CREATE TABLE IF NOT EXISTS key_envelopes(workspace TEXT,epoch INT,device TEXT,envelope TEXT,PRIMARY KEY(workspace,epoch,device));
CREATE TABLE IF NOT EXISTS workspace_device_exclusions(workspace TEXT,device TEXT,PRIMARY KEY(workspace,device));
CREATE TABLE IF NOT EXISTS ledger_cursors(cursor INTEGER PRIMARY KEY AUTOINCREMENT);
CREATE TABLE IF NOT EXISTS events(cursor INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT,event TEXT,author TEXT,epoch INT,seq INT,envelope TEXT,wire_hash TEXT,created REAL,UNIQUE(workspace,event));
CREATE UNIQUE INDEX IF NOT EXISTS event_author_sequence ON events(workspace,author,seq);
CREATE INDEX IF NOT EXISTS event_workspace_cursor ON events(workspace,cursor);
CREATE TABLE IF NOT EXISTS row_replicas(cursor INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT,replica TEXT,epoch INT,uploader TEXT,envelope TEXT,wire_hash TEXT,created REAL,updated REAL,UNIQUE(workspace,replica,epoch,uploader));
CREATE INDEX IF NOT EXISTS replica_workspace_cursor ON row_replicas(workspace,cursor);
CREATE TABLE IF NOT EXISTS semantic_replicas(cursor INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT,replica TEXT,epoch INT,uploader TEXT,envelope TEXT,wire_hash TEXT,created REAL,updated REAL,UNIQUE(workspace,replica,epoch,uploader));
CREATE INDEX IF NOT EXISTS semantic_workspace_cursor ON semantic_replicas(workspace,cursor);
CREATE TABLE IF NOT EXISTS replica_usage(workspace TEXT,uploader TEXT,bytes INT NOT NULL,PRIMARY KEY(workspace,uploader));
CREATE TABLE IF NOT EXISTS blob_replicas(cursor INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT,blob TEXT,epoch INT,uploader TEXT,size INT,nonce TEXT,ciphertext BLOB,created REAL,updated REAL,UNIQUE(workspace,blob,epoch,uploader));
CREATE INDEX IF NOT EXISTS blob_workspace_cursor ON blob_replicas(workspace,cursor);
CREATE TABLE IF NOT EXISTS origin_bundles(cursor INTEGER PRIMARY KEY AUTOINCREMENT,workspace TEXT,origin TEXT,epoch INT,uploader TEXT,envelope TEXT,wire_hash TEXT,created REAL,updated REAL,UNIQUE(workspace,origin,epoch,uploader));
CREATE INDEX IF NOT EXISTS origin_workspace_cursor ON origin_bundles(workspace,cursor);
CREATE TABLE IF NOT EXISTS workspace_controls(workspace TEXT,revision INT,state_hash TEXT UNIQUE,state TEXT,PRIMARY KEY(workspace,revision));
CREATE TABLE IF NOT EXISTS device_proposals(id TEXT PRIMARY KEY,workspace TEXT,base TEXT,target_user TEXT,target_device TEXT,proposal TEXT,not_before REAL,expires REAL,active INT);
CREATE TABLE IF NOT EXISTS device_votes(proposal TEXT,voter_user TEXT,voter_device TEXT,approve INT,vote TEXT,PRIMARY KEY(proposal,voter_user));
"""

def connect(path):
    existed=Path(path).exists() and Path(path).stat().st_size>0
    db=sqlite3.connect(path)
    db.row_factory=sqlite3.Row
    old=existed and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'").fetchone()
    if old and (db.execute("PRAGMA user_version").fetchone()[0]!=1 or db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_purges'").fetchone()):
        db.close()
        raise ValueError("relay database is incompatible; create a fresh relay")
    db.executescript("PRAGMA journal_mode=WAL;PRAGMA foreign_keys=ON;PRAGMA secure_delete=ON;PRAGMA busy_timeout=30000;"+SCHEMA+"PRAGMA user_version=1;")
    db.execute("INSERT OR IGNORE INTO relay_meta VALUES ('registration_secret',?)",(b64(os.urandom(32)),))
    db.commit()
    return db
def token_hash(token): return hashlib.sha256(token.encode()).hexdigest()
def auth(db, token):
    row = db.execute("SELECT * FROM devices WHERE token_hash=? AND active=1", (token_hash(token or ""),)).fetchone()
    if not row: raise PermissionError("invalid device token")
    return dict(row)
def member(db, workspace, user, role=None):
    row = db.execute("SELECT * FROM members WHERE workspace=? AND user_id=? AND active=1", (workspace,user)).fetchone()
    if not row or role == "admin" and row["role"] != "admin": raise PermissionError("workspace access denied")
    return dict(row)
def device_member(db,workspace,actor):
    result=member(db,workspace,actor["user_id"])
    epoch=db.execute("SELECT epoch FROM workspaces WHERE id=?",(workspace,)).fetchone()[0]
    if db.execute("SELECT 1 FROM workspace_device_exclusions WHERE workspace=? AND device=?",(workspace,actor["id"])).fetchone() or not db.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=? AND device=?",(workspace,epoch,actor["id"])).fetchone(): raise PermissionError("device is not authorized for current workspace epoch")
    return result
def verify_request(actor,req):
    try: Ed25519PublicKey.from_public_bytes(unb64(actor["sign_public"])).verify(unb64(req["control_signature"]),canon({k:v for k,v in req.items() if k!="control_signature"}))
    except (InvalidSignature,KeyError,TypeError,ValueError) as e: raise PermissionError("invalid control signature") from e
def certify(db,actor,req):
    root=db.execute("SELECT root_public FROM users WHERE id=?",(actor["user_id"],)).fetchone()[0]
    body=verify_certificate(req["certificate"],root)
    target=db.execute("SELECT * FROM devices WHERE id=? AND user_id=?",(body["device"]["id"],actor["user_id"])).fetchone()
    expected={k:target[k] for k in ("id","name","sign_public","box_public")} if target else None
    if body["user"]!=actor["user_id"] or body["device"]!=expected: raise PermissionError("device certificate does not match account")
    db.execute("INSERT OR REPLACE INTO device_certificates VALUES (?,?)",(body["device"]["id"],json.dumps(req["certificate"])))
    db.commit()
    return {"certified":body["device"]["id"]}
def rows(db, sql, args=()): return [dict(r) for r in db.execute(sql, args).fetchall()]
def cursor_bounds(db,table,access,args): return tuple((db.execute(f"SELECT cursor FROM {table} WHERE {access} ORDER BY cursor {direction} LIMIT 1",args).fetchone() or [0])[0] for direction in ("","DESC"))
def sync_tails(db,workspace,history,device):
    args,access=(workspace,history,device),"x.workspace=? AND x.epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=x.workspace AND k.epoch=x.epoch AND k.device=?)"
    tail=lambda table:(db.execute(f"SELECT cursor FROM {table} x WHERE {access} ORDER BY cursor DESC LIMIT 1",args).fetchone() or [0])[0]
    return {"events":tail("events"),"replicas":max(tail("row_replicas"),tail("semantic_replicas")),"blobs":tail("blob_replicas"),"origins":tail("origin_bundles")}
def verify_signed(value,sign_public):
    signature=unb64(value["signature"])
    body={k:v for k,v in value.items() if k!="signature"}
    Ed25519PublicKey.from_public_bytes(unb64(sign_public)).verify(signature,canon(body))
    return value
def verify_record(value):
    body=verify_certificate(value["certificate"],value["root_public"])
    device=value["device"]
    if public_id(value["root_public"])!=value["user"] or public_id(device["sign_public"])!=device["id"] or body["user"]!=value["user"] or body["device"]!=device: raise ValueError("device record mismatch")
    return value
def control_hash(value): return digest(value)
def ledger_state(db,ws):
    values=rows(db,"SELECT cursor,event,author,seq FROM events WHERE workspace=?",(ws,))
    heads={author:{"seq":row["seq"],"event":row["event"]} for author in {r["author"] for r in values} for row in [max((r for r in values if r["author"]==author),key=lambda r:r["seq"])]}
    return {"tail":max((r["cursor"] for r in values),default=0),"heads":heads}
def current_control(db,ws):
    row=db.execute("SELECT state FROM workspace_controls WHERE workspace=? ORDER BY revision DESC LIMIT 1",(ws,)).fetchone()
    return json.loads(row[0]) if row else None
def electorate(state,target): return sorted({d["user"] for d in state["devices"].values() if d["user"]!=target})
def verify_proposal(previous,request,now=None,kind="device.proposal"):
    target=verify_record(request["target"])
    verify_signed(request,target["device"]["sign_public"])
    moment=time.time() if now is None else float(now)
    if request["v"]!=V or request["kind"]!=kind or request["base"]!=control_hash(previous) or request["workspace"]!=previous["workspace"] or request["epoch"]!=previous["epoch"] or request["certificate_hash"]!=digest(target["certificate"]) or not request["not_before"]<=moment<request["expires"]: raise ValueError("proposal state mismatch")
    return target
def verify_approval(previous,approval,now=None,kind="device.proposal"):
    request,votes=approval["proposal"],approval.get("votes",[])
    target=verify_proposal(previous,request,now,kind)
    if kind=="device.proposal" and any(d["user"]==target["user"] for d in previous["devices"].values()): raise ValueError("target user can self approve")
    eligible,by_user=set(electorate(previous,target["user"])),{}
    for value in votes:
        record=previous["devices"].get(value["author"])
        if value["v"]!=V or value["kind"]!="device.vote" or type(value["approve"]) is not bool or not record or record["user"]!=value["voter"] or value["voter"] not in eligible: raise ValueError("ineligible vote")
        verify_signed(value,record["device"]["sign_public"])
        if (value["proposal"],value["workspace"],value["base"])!=(control_hash(request),request["workspace"],request["base"]): raise ValueError("vote proposal mismatch")
        if value["voter"] in by_user and by_user[value["voter"]]!=value["approve"]: raise ValueError("conflicting user votes")
        by_user[value["voter"]]=value["approve"]
    needed=len(eligible)//2+1
    if not eligible or sum(v is True for v in by_user.values())<needed: raise ValueError(f"approval requires {needed} of {len(eligible)} votes")
def verify_window(db,request,now):
    row=db.execute("SELECT not_before,expires,active FROM device_proposals WHERE id=?",(control_hash(request),)).fetchone()
    if not row or not row["active"] or not row["not_before"]<=now<row["expires"]: raise ValueError("proposal is not active by relay clock")
def verify_control(db,actor,value,previous=None):
    if value["v"]!=CONTROL_V or value["kind"]!="workspace.state": raise ValueError("unsupported workspace state")
    boundary=value["boundary"]
    heads=boundary["heads"]
    if value["scope"] not in ("personal","team") or len(value["key_commitment"])!=64 or set(boundary)!={"epoch","tail","heads"} or boundary["epoch"]!=value["epoch"] or not isinstance(boundary["tail"],int) or isinstance(boundary["tail"],bool) or boundary["tail"]<0 or not isinstance(heads,dict) or any(set(h)!={"seq","event"} or not isinstance(h["seq"],int) or isinstance(h["seq"],bool) or h["seq"]<1 or not isinstance(h["event"],str) or len(h["event"])!=64 for h in heads.values()) or any(set(m)!={"role","joined","history_from"} or m["role"] not in ("admin","member") or any(not isinstance(m[k],int) or isinstance(m[k],bool) or not 1<=m[k]<=value["epoch"] for k in ("joined","history_from")) for m in value["members"].values()) or any(d["user"] not in value["members"] for d in value["devices"].values()) or set(value["devices"])&set(value["removed"]): raise ValueError("invalid workspace state")
    author=(value["devices"] if previous is None else previous["devices"]).get(value["author"])
    if value["action"]=="personal_recover" and previous is not None: author=value["devices"].get(value["author"])
    if not author or actor["id"]!=value["author"]: raise PermissionError("state author is not authorized")
    verify_signed(value,verify_record(author)["device"]["sign_public"])
    for device in value["devices"].values(): verify_record(device)
    if previous is None:
        if value["revision"]!=1 or value["prev"] is not None or value["epoch"]!=1 or boundary!={"epoch":1,"tail":0,"heads":{}} or value["author"] not in value["devices"] or value["action"]!="create" or value["members"][author["user"]]["role"]!="admin" or set(value["members"])!={author["user"]} or set(value["devices"])!={value["author"]} or value["removed"]: raise ValueError("invalid genesis state")
        return value
    if (value["workspace"],value["scope"])!=(previous["workspace"],previous["scope"]) or value["revision"]!=previous["revision"]+1 or value["prev"]!=control_hash(previous): raise ValueError("workspace state chain mismatch")
    if value["epoch"]==previous["epoch"] and boundary!=previous["boundary"] or value["epoch"]>previous["epoch"] and boundary!={"epoch":value["epoch"],**ledger_state(db,value["workspace"])}: raise ValueError("workspace history boundary mismatch")
    action=value["action"]
    previous_author=previous["devices"].get(value["author"])
    admin=bool(previous_author and previous["members"][previous_author["user"]]["role"]=="admin")
    if action in ("membership","remove","history") and not admin: raise PermissionError("admin control required")
    if action in ("self_approve","quorum_approve","personal_recover"):
        now=time.time()
        if abs(float(value["approved_at"])-now)>CLOCK_SKEW: raise ValueError("approval clock mismatch")
        if value["members"]!=previous["members"] or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"]+1: raise ValueError("approval changed workspace policy")
        added=set(value["devices"])-set(previous["devices"])
        if len(added)!=1 or set(previous["devices"])-set(value["devices"]): raise ValueError("approval must add exactly one device")
        target=value["approval"]["proposal"]["target"]
        device=next(iter(added))
        if device!=target["device"]["id"] or {k:v for k,v in value["devices"][device].items() if k!="history"}!={k:v for k,v in target.items() if k!="history"} or device in previous["removed"]: raise ValueError("approval target mismatch")
        if action=="self_approve" and (previous_author["user"]!=target["user"] or value["devices"][device]["history"]!=previous_author["history"]): raise PermissionError("self approval permission mismatch")
        if action in ("self_approve","personal_recover"): verify_proposal(previous,value["approval"]["proposal"],now)
        if action!="personal_recover": verify_window(db,value["approval"]["proposal"],now)
        if action=="quorum_approve" and value["devices"][device]["history"] is not False: raise ValueError("quorum approval must be future-only")
        if action=="quorum_approve": verify_approval(previous,value["approval"],now)
        if action=="personal_recover" and (previous["scope"]!="personal" or target["user"] not in previous["members"]): raise PermissionError("personal recovery mismatch")
    elif action=="history":
        if set(value["devices"])!=set(previous["devices"]) or any({k:v for k,v in d.items() if k!="history"}!={k:v for k,v in previous["devices"][i].items() if k!="history"} or previous["devices"][i]["history"] and not d["history"] for i,d in value["devices"].items()) or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"] or value["key_commitment"]!=previous["key_commitment"] or set(value["members"])!=set(previous["members"]) or any((m["role"],m["joined"])!=(previous["members"][u]["role"],previous["members"][u]["joined"]) or m["history_from"]>previous["members"][u]["history_from"] for u,m in value["members"].items()): raise ValueError("invalid history transition")
    elif action=="history_activate":
        now=time.time()
        if abs(float(value["approved_at"])-now)>CLOCK_SKEW: raise ValueError("approval clock mismatch")
        target=value["approval"]["proposal"]["target"]
        device=target["device"]["id"]
        expected={**previous["devices"],device:{**previous["devices"][device],"history":True}}
        if target["history"] is not True or previous["devices"].get(device,{}).get("history") is not False or value["members"]!=previous["members"] or value["devices"]!=expected or value["removed"]!=previous["removed"] or value["epoch"]!=previous["epoch"] or value["key_commitment"]!=previous["key_commitment"]: raise ValueError("invalid history activation")
        verify_approval(previous,value["approval"],now,"history.proposal")
        verify_window(db,value["approval"]["proposal"],now)
    elif action=="remove":
        removed=set(previous["devices"])-set(value["devices"])
        if value["members"]!=previous["members"] or value["epoch"]!=previous["epoch"]+1 or not removed or not removed<=set(value["removed"]) or not set(previous["removed"])<=set(value["removed"]) or any(value["devices"].get(d)!=r for d,r in previous["devices"].items() if d not in removed): raise ValueError("invalid device removal")
    elif action=="membership":
        added_users=set(value["members"])-set(previous["members"])
        removed_users=set(previous["members"])-set(value["members"])
        added=set(value["devices"])-set(previous["devices"])
        removed=set(previous["devices"])-set(value["devices"])
        if value["epoch"]!=previous["epoch"]+1 or value["scope"]=="personal" and value["members"]!=previous["members"] or set(value["devices"])&set(value["removed"]) or any(r["user"] not in added_users for d,r in value["devices"].items() if d in added) or any(r["user"] not in removed_users for d,r in previous["devices"].items() if d in removed) or any(value["devices"].get(d)!=r for d,r in previous["devices"].items() if r["user"] not in removed_users) or any((m["joined"],m["history_from"])!=(previous["members"][u]["joined"],previous["members"][u]["history_from"]) for u,m in value["members"].items() if u not in added_users) or any((m["joined"],m["history_from"])!=(value["epoch"],value["epoch"]) for u,m in value["members"].items() if u in added_users) or not set(previous["removed"])<=set(value["removed"]) or not removed<=set(value["removed"]): raise ValueError("invalid membership transition")
    else: raise ValueError("unknown workspace action")
    return value
def apply_control(db,value,envelopes):
    ws=value["workspace"]
    previous=current_control(db,ws)
    if set(envelopes)!=set(value["devices"]): raise ValueError("one key envelope required for every authorized device")
    for old in rows(db,"SELECT user_id FROM members WHERE workspace=?",(ws,)):
        if old["user_id"] not in value["members"]: db.execute("UPDATE members SET active=0 WHERE workspace=? AND user_id=?",(ws,old["user_id"]))
    for user,meta in value["members"].items(): db.execute("INSERT OR REPLACE INTO members VALUES (?,?,?,?,?,?)",(ws,user,meta["role"],1,meta["joined"],meta["history_from"]))
    db.execute("DELETE FROM workspace_device_exclusions WHERE workspace=?",(ws,))
    db.executemany("INSERT INTO workspace_device_exclusions VALUES (?,?)",[(ws,d) for d in value["removed"]])
    db.executemany("INSERT INTO key_envelopes VALUES (?,?,?,?)",[(ws,value["epoch"],d,json.dumps(env)) for d,env in envelopes.items()])
    db.execute("UPDATE workspaces SET epoch=? WHERE id=?",(value["epoch"],ws))
    db.execute("INSERT INTO workspace_controls VALUES (?,?,?,?)",(ws,value["revision"],control_hash(value),json.dumps(value)))
    return {"workspace":ws,"epoch":value["epoch"],"control":control_hash(value)}
def apply_history(db,value):
    db.executemany("UPDATE members SET history_from=? WHERE workspace=? AND user_id=?",[(m["history_from"],value["workspace"],u) for u,m in value["members"].items()])
    db.execute("INSERT INTO workspace_controls VALUES (?,?,?,?)",(value["workspace"],value["revision"],control_hash(value),json.dumps(value)))
    return {"workspace":value["workspace"],"control":control_hash(value)}

def registration_identity(req):
    cert,root=req["certificate"],req["root_public"]
    body=verify_certificate(cert,root)
    user,dev=body["user"],body["device"]
    if user!=public_id(root) or dev["id"]!=public_id(dev["sign_public"]): raise ValueError("identity id does not match public key")
    return cert,root,user,dev
def registration_challenge(db,req):
    cert,root,user,dev=registration_identity(req)
    body={"v":V,"kind":"registration.challenge","user":user,"device":dev["id"],"certificate":digest(cert),"expires":int(time.time())+REGISTRATION_TTL,"nonce":b64(os.urandom(24))}
    secret=unb64(db.execute("SELECT value FROM relay_meta WHERE key='registration_secret'").fetchone()[0])
    return {**body,"mac":b64(hmac.new(secret,canon(body),hashlib.sha256).digest())}
def verify_registration_challenge(db,value,cert,user,dev):
    try:
        body={k:value[k] for k in ("v","kind","user","device","certificate","expires","nonce")}
        now=time.time()
        secret=unb64(db.execute("SELECT value FROM relay_meta WHERE key='registration_secret'").fetchone()[0])
        valid=set(value)==set(body)|{"mac"} and (body["v"],body["kind"],body["user"],body["device"],body["certificate"])==(V,"registration.challenge",user,dev["id"],digest(cert)) and type(body["expires"]) is int and now<=body["expires"]<=now+REGISTRATION_TTL+CLOCK_SKEW and isinstance(body["nonce"],str) and len(unb64(body["nonce"]))==24 and hmac.compare_digest(unb64(value["mac"]),hmac.new(secret,canon(body),hashlib.sha256).digest())
    except (KeyError,TypeError,ValueError): valid=False
    if not valid: raise PermissionError("invalid registration challenge")
    return digest(value)
def register(db,req):
    cert,root,user,dev=registration_identity(req)
    challenge=verify_registration_challenge(db,req["challenge"],cert,user,dev)
    proof=req["proof"]
    expected={"v":V,"kind":"device.registration","challenge":challenge,"root_public":root,"certificate":digest(cert),"user":user,"device":dev["id"]}
    try:
        if set(proof)!=set(expected)|{"signature"} or {k:proof[k] for k in expected}!=expected: raise ValueError
        Ed25519PublicKey.from_public_bytes(unb64(dev["sign_public"])).verify(unb64(proof["signature"]),canon(expected))
    except (InvalidSignature,KeyError,TypeError,ValueError) as e: raise PermissionError("invalid registration proof") from e
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("DELETE FROM registration_uses WHERE expires<?",(time.time(),))
        if db.execute("SELECT 1 FROM registration_uses WHERE challenge=?",(challenge,)).fetchone(): raise PermissionError("registration challenge was already used")
        db.execute("INSERT INTO registration_uses VALUES (?,?)",(challenge,req["challenge"]["expires"]))
        old=db.execute("SELECT root_public FROM users WHERE id=?",(user,)).fetchone()
        if old and old[0]!=root: raise PermissionError("user root mismatch")
        if not old: db.execute("INSERT INTO users VALUES (?,?,?,?,?)",(user,req["user_name"],root,json.dumps(req.get("recovery")),time.time()))
        token=secrets.token_urlsafe(32)
        db.execute("INSERT INTO devices VALUES (?,?,?,?,?,?,?,?)",(dev["id"],user,dev["name"],dev["sign_public"],dev["box_public"],token_hash(token),1,time.time()))
        db.execute("INSERT INTO device_certificates VALUES (?,?)",(dev["id"],json.dumps(cert)))
        db.commit()
        return dict(user=user,device=dev["id"],token=token)
    except BaseException:
        db.rollback()
        raise

def rotate(db, actor, req):
    db.execute("BEGIN IMMEDIATE")
    previous=current_control(db,req["workspace"])
    if not previous: raise ValueError("workspace control state is not initialized")
    if actor["id"] not in previous["devices"] and req.get("control",{}).get("action")!="personal_recover": raise PermissionError("device is not authorized for current workspace epoch")
    verify_control(db,actor,req["control"],previous)
    history=req.get("history_envelopes",{})
    added=set(req["control"]["devices"])-set(previous["devices"])
    if set(history)-added: raise ValueError("history envelopes may target only a newly approved device")
    for device,epochs in history.items():
        record=req["control"]["devices"][device]
        start=req["control"]["members"][record["user"]]["history_from"]
        if not record["history"] or any(not start<=int(epoch)<req["control"]["epoch"] for epoch in epochs): raise ValueError("history target is not entitled")
    if req["control"]["action"]=="self_approve" and (device:=next(iter(added))) and req["control"]["devices"][device]["history"] and {int(e) for e in history.get(device,{})}!=set(range(req["control"]["members"][req["control"]["devices"][device]["user"]]["history_from"],previous["epoch"]+1)): raise ValueError("inherited history does not cover the signed entitlement")
    result=apply_control(db,req["control"],req["envelopes"])
    for device,epochs in history.items():
        db.executemany("INSERT OR REPLACE INTO key_envelopes VALUES (?,?,?,?)",[(req["workspace"],int(epoch),device,json.dumps(env)) for epoch,env in epochs.items()])
    if approval:=req["control"].get("approval"): db.execute("UPDATE device_proposals SET active=0 WHERE id=?",(control_hash(approval["proposal"]),))
    db.commit()
    return result
def store_event(db,actor,env):
    ws=env["workspace"]
    device_member(db,ws,actor)
    epoch=db.execute("SELECT epoch FROM workspaces WHERE id=?",(ws,)).fetchone()[0]
    if env["author"]!=actor["id"]: raise PermissionError("event author rejected")
    if set(env)!={"v","workspace","epoch","event","author","seq","parents","nonce","ciphertext"} or env["v"]!=V or not isinstance(env["seq"],int) or isinstance(env["seq"],bool) or env["seq"]<1 or not isinstance(env["parents"],list) or len(env["parents"])>32 or len(set(env["parents"]))!=len(env["parents"]) or any(not isinstance(p,str) or len(p)!=64 for p in env["parents"]): raise PermissionError("event envelope rejected")
    wire=digest(env)
    old=db.execute("SELECT wire_hash,cursor FROM events WHERE workspace=? AND event=?",(ws,env["event"])).fetchone()
    if old:
        if old["wire_hash"]!=wire: raise ValueError("event id already has different ciphertext")
        return {"cursor":old["cursor"],"created":False}
    if env["epoch"]!=epoch: raise PermissionError("event epoch rejected")
    cursor=db.execute("INSERT INTO ledger_cursors DEFAULT VALUES").lastrowid
    db.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)",(cursor,ws,env["event"],env["author"],env["epoch"],env["seq"],json.dumps(env),wire,time.time()))
    return {"cursor":cursor,"created":True}
def store_replica(db,actor,env,semantic=False):
    table="semantic_replicas" if semantic else "row_replicas"
    fields={"v","kind","workspace","replica","epoch","uploader","nonce","ciphertext"}
    ws=env["workspace"]
    member=device_member(db,ws,actor)
    current=db.execute("SELECT epoch FROM workspaces WHERE id=?",(ws,)).fetchone()[0]
    if set(env)!=fields or env["v"]!=1 or env["kind"]!="row.replica" or env["uploader"]!=actor["id"] or not member["history_from"]<=env["epoch"]<=current or not db.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=? AND device=?",(ws,env["epoch"],actor["id"])).fetchone() or not isinstance(env["replica"],str) or len(env["replica"])!=64 or any(c not in "0123456789abcdef" for c in env["replica"]): raise PermissionError("row replica envelope rejected")
    key=(ws,env["replica"],env["epoch"],actor["id"])
    wire=digest(env)
    old=db.execute(f"SELECT cursor,wire_hash,envelope FROM {table} WHERE workspace=? AND replica=? AND epoch=? AND uploader=?",key).fetchone()
    now,size=time.time(),len(canon(env))
    used=(db.execute("SELECT bytes FROM replica_usage WHERE workspace=? AND uploader=?",(ws,actor["id"])).fetchone() or [0])[0]
    total=used-(len(canon(json.loads(old["envelope"]))) if old else 0)+size
    if size>48*1024**2: raise ValueError("row replica exceeds 48 MiB")
    if total>REPLICA_QUOTA: raise ValueError("row replica quota exceeded")
    db.execute("INSERT OR REPLACE INTO replica_usage VALUES (?,?,?)",(ws,actor["id"],total))
    if old:
        db.execute(f"UPDATE {table} SET envelope=?,wire_hash=?,updated=? WHERE workspace=? AND replica=? AND epoch=? AND uploader=?",(json.dumps(env),wire,now,*key))
        return {"cursor":old["cursor"],"created":False,"replaced":old["wire_hash"]!=wire}
    cursor=db.execute("INSERT INTO ledger_cursors DEFAULT VALUES").lastrowid
    db.execute(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?)",(cursor,*key,json.dumps(env),wire,now,now))
    return {"cursor":cursor,"created":True,"replaced":False}
def store_origin(db,actor,env):
    fields={"v","kind","workspace","origin","epoch","uploader","nonce","ciphertext"}
    ws=env["workspace"]
    device_member(db,ws,actor)
    current=db.execute("SELECT epoch FROM workspaces WHERE id=?",(ws,)).fetchone()[0]
    if set(env)!=fields or env["v"]!=1 or env["kind"]!="origin.bundle" or env["uploader"]!=actor["id"] or env["epoch"]!=current or not isinstance(env["origin"],str) or len(env["origin"])!=64 or any(c not in "0123456789abcdef" for c in env["origin"]): raise PermissionError("origin bundle rejected")
    key=(ws,env["origin"],env["epoch"],actor["id"])
    wire=digest(env)
    old=db.execute("SELECT cursor,wire_hash,LENGTH(envelope) size FROM origin_bundles WHERE workspace=? AND origin=? AND epoch=? AND uploader=?",key).fetchone()
    now,size=time.time(),len(canon(env))
    used=db.execute("SELECT COALESCE(SUM(LENGTH(envelope)),0) FROM origin_bundles WHERE workspace=? AND uploader=?",(ws,actor["id"])).fetchone()[0]
    if size>4*1024**2 or used-(old["size"] if old else 0)+size>64*1024**2: raise ValueError("origin bundle quota exceeded")
    if old:
        db.execute("UPDATE origin_bundles SET envelope=?,wire_hash=?,updated=? WHERE workspace=? AND origin=? AND epoch=? AND uploader=?",(json.dumps(env),wire,now,*key))
        return {"cursor":old["cursor"],"created":False,"replaced":old["wire_hash"]!=wire}
    cursor=db.execute("INSERT INTO ledger_cursors DEFAULT VALUES").lastrowid
    db.execute("INSERT INTO origin_bundles VALUES (?,?,?,?,?,?,?,?,?)",(cursor,*key,json.dumps(env),wire,now,now))
    return {"cursor":cursor,"created":True,"replaced":False}
def store_blob(db,actor,env):
    fields={"v","kind","workspace","blob","epoch","uploader","size","nonce","ciphertext"}
    if not isinstance(env,dict) or set(env)!=fields or env["v"]!=1 or env["kind"]!="blob.replica" or not isinstance(env["workspace"],str) or not isinstance(env["epoch"],int) or isinstance(env["epoch"],bool) or env["uploader"]!=actor["id"] or not isinstance(env["blob"],str) or len(env["blob"])!=64 or any(c not in "0123456789abcdef" for c in env["blob"]) or not isinstance(env["size"],int) or isinstance(env["size"],bool) or not 0<=env["size"]<=BLOB_LIMIT or not all(isinstance(env[k],str) for k in ("nonce","ciphertext")): raise PermissionError("blob replica envelope rejected")
    try: raw,nonce=unb64(env["ciphertext"]),unb64(env["nonce"])
    except Exception as e: raise PermissionError("blob replica envelope rejected") from e
    ws=env["workspace"]
    m=device_member(db,ws,actor)
    current=db.execute("SELECT epoch FROM workspaces WHERE id=?",(ws,)).fetchone()[0]
    if not m["history_from"]<=env["epoch"]<=current or not db.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=? AND device=?",(ws,env["epoch"],actor["id"])).fetchone() or len(nonce)!=12 or len(raw)!=env["size"]+16: raise PermissionError("blob replica envelope rejected")
    key=(ws,env["blob"],env["epoch"],actor["id"])
    old=db.execute("SELECT cursor,LENGTH(ciphertext) size FROM blob_replicas WHERE workspace=? AND blob=? AND epoch=? AND uploader=?",key).fetchone()
    used=db.execute("SELECT COALESCE(SUM(LENGTH(ciphertext)),0) FROM blob_replicas WHERE workspace=? AND uploader=?",(ws,actor["id"])).fetchone()[0]
    if used-(old["size"] if old else 0)+len(raw)>BLOB_QUOTA: raise ValueError("blob replica quota exceeded")
    if old:
        db.execute("UPDATE blob_replicas SET size=?,nonce=?,ciphertext=?,updated=? WHERE workspace=? AND blob=? AND epoch=? AND uploader=?",(env["size"],env["nonce"],raw,time.time(),*key))
        return {"cursor":old["cursor"],"created":False}
    cursor=db.execute("INSERT INTO ledger_cursors DEFAULT VALUES").lastrowid
    db.execute("INSERT INTO blob_replicas VALUES (?,?,?,?,?,?,?,?,?,?)",(cursor,*key,env["size"],env["nonce"],raw,time.time(),time.time()))
    return {"cursor":cursor,"created":True}
def blob_envelope(row): return {"v":1,"kind":"blob.replica","workspace":row["workspace"],"blob":row["blob"],"epoch":row["epoch"],"uploader":row["uploader"],"size":row["size"],"nonce":row["nonce"],"ciphertext":b64(row["ciphertext"])}
def bounded(values,size,limit=48*1024**2):
    out,used=[],0
    for value in values:
        if out and used+size(value)>limit: break
        out.append(value)
        used+=size(value)
    return out
def immediate(db,fn):
    db.execute("BEGIN IMMEDIATE")
    try:
        value=fn()
        db.commit()
        return value
    except BaseException:
        db.rollback()
        raise
def action(db, req, token=None):
    op = req["op"]
    if op == "register_challenge": return {"challenge":registration_challenge(db,req)}
    if op == "register": return register(db, req)
    if op == "recovery_fetch":
        row = db.execute("SELECT recovery FROM users WHERE id=? OR name=?", (req["user"],req["user"])).fetchone()
        if not row or not row[0]: raise ValueError("recovery bundle not found")
        return {"bundle":json.loads(row[0])}
    actor = auth(db, token)
    if op == "certify": return certify(db,actor,req)
    if op in ("create","rotate","grant_all","history_activate","reject","recovery"): verify_request(actor,req)
    if op == "create":
        ws,control=req["workspace"],req["control"]
        verify_control(db,actor,control)
        if (control["workspace"],control["scope"])!=(ws,req["kind"]): raise ValueError("workspace create scope mismatch")
        db.execute("INSERT INTO workspaces VALUES (?,?,?,?,?)",(ws,req["kind"],1,actor["user_id"],time.time()))
        result=apply_control(db,control,req["envelopes"])
        db.commit()
        return result
    if op == "rotate":
        try: return rotate(db,actor,req)
        except BaseException:
            db.rollback()
            raise
    if op == "propose":
        request=req["proposal"]
        previous=current_control(db,request["workspace"])
        target=verify_record(request["target"])
        verify_signed(request,target["device"]["sign_public"])
        pending=request["kind"]=="device.proposal" and target["history"] is False and actor["id"] not in previous["devices"] and actor["id"] not in previous["removed"]
        history=request["kind"]=="history.proposal" and actor["id"] in previous["devices"] and previous["devices"][actor["id"]]["history"] is False and target=={**previous["devices"][actor["id"]],"history":True}
        now=time.time()
        delay=APPROVAL_DELAY if len(electorate(previous,target["user"]))==1 and (history or not any(d["user"]==target["user"] for d in previous["devices"].values())) else 0
        active=max(request["not_before"],now+delay)
        if request["v"]!=V or request["certificate_hash"]!=digest(target["certificate"]) or actor["id"]!=request["author"] or target["device"]["id"]!=actor["id"] or target["user"]!=actor["user_id"] or request["base"]!=control_hash(previous) or request["epoch"]!=previous["epoch"] or actor["user_id"] not in previous["members"] or not (pending or history) or not active<request["expires"]<=now+86400+CLOCK_SKEW: raise PermissionError("invalid device proposal")
        pid=control_hash(request)
        db.execute("INSERT INTO device_proposals VALUES (?,?,?,?,?,?,?,?,1)",(pid,request["workspace"],request["base"],actor["user_id"],actor["id"],json.dumps(request),active,request["expires"]))
        db.commit()
        return {"proposal":pid}
    if op == "reject":
        row=db.execute("SELECT proposal,active FROM device_proposals WHERE id=? AND workspace=?",(req["proposal"],req["workspace"])).fetchone()
        previous=current_control(db,req["workspace"])
        record=previous["devices"].get(actor["id"])
        proposal=json.loads(row["proposal"]) if row else None
        if not row or not row["active"] or not record or record["user"]!=actor["user_id"] or proposal["target"]["user"]!=actor["user_id"] or proposal["base"]!=control_hash(previous): raise PermissionError("proposal rejection denied")
        db.execute("UPDATE device_proposals SET active=0 WHERE id=?",(req["proposal"],))
        db.commit()
        return {"rejected":True}
    if op == "vote":
        value=req["vote"]
        proposal_row=db.execute("SELECT proposal,workspace,base,expires,active FROM device_proposals WHERE id=?",(value["proposal"],)).fetchone()
        if not proposal_row or not proposal_row["active"] or proposal_row["expires"]<=time.time(): raise ValueError("proposal is not active")
        previous=current_control(db,proposal_row["workspace"])
        record=previous["devices"].get(actor["id"])
        if value["v"]!=V or value["kind"]!="device.vote" or value["workspace"]!=proposal_row["workspace"] or value["voter"]!=actor["user_id"] or value["author"]!=actor["id"] or type(value["approve"]) is not bool or not record or record["user"]!=actor["user_id"] or actor["user_id"]==json.loads(proposal_row["proposal"])["target"]["user"] or value["base"]!=control_hash(previous): raise PermissionError("ineligible vote")
        verify_signed(value,record["device"]["sign_public"])
        old=db.execute("SELECT approve FROM device_votes WHERE proposal=? AND voter_user=?",(value["proposal"],actor["user_id"])).fetchone()
        if old and bool(old[0])!=value["approve"]: raise ValueError("conflicting user vote")
        db.execute("INSERT OR REPLACE INTO device_votes VALUES (?,?,?,?,?)",(value["proposal"],actor["user_id"],actor["id"],value["approve"],json.dumps(value)))
        db.commit()
        return {"recorded":True}
    if op == "proposals":
        member(db,req["workspace"],actor["user_id"])
        out=[]
        for row in rows(db,"SELECT * FROM device_proposals WHERE workspace=? AND active=1 AND expires>? ORDER BY expires",(req["workspace"],time.time())):
            out.append({"proposal":json.loads(row["proposal"]),"votes":[json.loads(v[0]) for v in db.execute("SELECT vote FROM device_votes WHERE proposal=? ORDER BY voter_user",(row["id"],)).fetchall()]})
        return {"proposals":out}
    if op == "directory":
        return {"users":rows(db, "SELECT id,name,root_public FROM users WHERE name=? OR id=?", (req["user"],req["user"])), "devices":rows(db, "SELECT d.id,d.user_id,d.name,d.sign_public,d.box_public,c.certificate,u.root_public FROM devices d JOIN users u ON u.id=d.user_id LEFT JOIN device_certificates c ON c.device=d.id WHERE d.active=1 AND d.user_id IN (SELECT id FROM users WHERE name=? OR id=?)", (req["user"],req["user"]))}
    if op == "state":
        memberships = rows(db, "SELECT w.id,w.kind,w.epoch,m.role,m.joined_epoch,m.history_from FROM workspaces w JOIN members m ON w.id=m.workspace WHERE m.user_id=? AND m.active=1", (actor["user_id"],))
        for w in memberships:
            w["keys"]=rows(db,"SELECT epoch,envelope FROM key_envelopes WHERE workspace=? AND device=? ORDER BY epoch",(w["id"],actor["id"]))
            w["devices"]=rows(db,"SELECT DISTINCT d.id,d.user_id,d.name,d.sign_public,d.box_public,d.active,c.certificate,u.root_public,NOT EXISTS(SELECT 1 FROM workspace_device_exclusions x WHERE x.workspace=? AND x.device=d.id) allowed,EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=? AND k.epoch=? AND k.device=d.id) authorized FROM devices d JOIN users u ON u.id=d.user_id LEFT JOIN device_certificates c ON c.device=d.id JOIN members m ON d.user_id=m.user_id WHERE m.workspace=?",(w["id"],w["id"],w["epoch"],w["id"]))
            w["members"]=rows(db,"SELECT user_id,role,active,joined_epoch,history_from FROM members WHERE workspace=?",(w["id"],))
            w["device_authorized"]=bool(db.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=? AND device=?",(w["id"],w["epoch"],actor["id"])).fetchone()) and not bool(db.execute("SELECT 1 FROM workspace_device_exclusions WHERE workspace=? AND device=?",(w["id"],actor["id"])).fetchone())
            w["sync"]=sync_tails(db,w["id"],w["history_from"],actor["id"]) if w["device_authorized"] else None
            w["controls"]=[json.loads(r[0]) for r in db.execute("SELECT state FROM workspace_controls WHERE workspace=? ORDER BY revision",(w["id"],)).fetchall()]
        return {"user":actor["user_id"],"device":actor["id"],"workspaces":memberships,"capabilities":{"replica_reconcile_limit":2500,"replica_pull_limit":2500,"sync_tails":1}}
    if op == "ledger":
        member(db,req["workspace"],actor["user_id"])
        return ledger_state(db,req["workspace"])
    if op == "upload":
        result=store_event(db,actor,req["envelope"])
        db.commit()
        return result
    if op == "upload_many":
        if len(req["envelopes"])>500: raise ValueError("upload batch limit is 500")
        result=[store_event(db,actor,env) for env in req["envelopes"]]
        db.commit()
        return {"events":result}
    if op == "replica_upload_many":
        if not isinstance(req["envelopes"],list) or not 1<=len(req["envelopes"])<=500: raise ValueError("replica upload batch limit is 1 to 500")
        semantic=req.get("semantic",False)
        if not isinstance(semantic,bool): raise ValueError("replica channel is invalid")
        return immediate(db,lambda:{"replicas":[store_replica(db,actor,env,semantic) for env in req["envelopes"]]})
    if op == "blob_upload": return immediate(db,lambda:store_blob(db,actor,req["envelope"]))
    if op == "blob_reconcile":
        ws=req["workspace"]
        m=device_member(db,ws,actor)
        ids=req["blobs"]
        if not isinstance(ids,list) or not 1<=len(ids)<=500 or len(ids)!=len(set(ids)) or any(not isinstance(v,str) or len(v)!=64 for v in ids): raise ValueError("blob inventory is invalid")
        found=rows(db,f"SELECT blob,MAX(cursor) cursor FROM blob_replicas WHERE workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=blob_replicas.workspace AND k.epoch=blob_replicas.epoch AND k.device=?) AND blob IN ({','.join('?'*len(ids))}) GROUP BY blob",(ws,m["history_from"],actor["id"],*ids))
        return {"present":{r["blob"]:r["cursor"] for r in found}}
    if op == "blob_pull":
        ws=req["workspace"]
        m=device_member(db,ws,actor)
        after,limit=req.get("after",0),req.get("limit",20)
        if not isinstance(after,int) or isinstance(after,bool) or after<0 or not isinstance(limit,int) or isinstance(limit,bool) or not 1<=limit<=20: raise ValueError("blob page is invalid")
        access="workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=blob_replicas.workspace AND k.epoch=blob_replicas.epoch AND k.device=?)"
        args=(ws,m["history_from"],actor["id"])
        floor,tail=cursor_bounds(db,"blob_replicas",access,args)
        values=bounded(db.execute(f"SELECT * FROM blob_replicas WHERE {access} AND cursor>? ORDER BY cursor LIMIT ?",args+(after,limit)),lambda r:4*((len(r["ciphertext"])+2)//3)+1024)
        return {"floor":floor,"tail":tail,"blobs":[{"cursor":r["cursor"],"envelope":blob_envelope(r)} for r in values]}
    if op == "origin_upload": return immediate(db,lambda:store_origin(db,actor,req["envelope"]))
    if op == "origin_pull":
        ws=req["workspace"]
        m=device_member(db,ws,actor)
        values=rows(db,"SELECT cursor,envelope FROM origin_bundles WHERE workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=origin_bundles.workspace AND k.epoch=origin_bundles.epoch AND k.device=?) ORDER BY cursor",(ws,m["history_from"],actor["id"]))
        return {"origins":[{"cursor":r["cursor"],"envelope":json.loads(r["envelope"])} for r in values]}
    if op == "replica_reconcile":
        ws=req["workspace"]
        device_member(db,ws,actor)
        ids=req["replicas"]
        semantic=req.get("semantic",False)
        table="semantic_replicas" if semantic else "row_replicas"
        if not isinstance(semantic,bool): raise ValueError("replica channel is invalid")
        if not isinstance(ids,list) or not 1<=len(ids)<=2500 or len(ids)!=len(set(ids)) or any(not isinstance(v,str) or len(v)!=64 for v in ids): raise ValueError("replica inventory is invalid")
        found=rows(db,f"SELECT replica,MAX(cursor) cursor FROM {table} x WHERE workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=x.workspace AND k.epoch=x.epoch AND k.device=?) AND replica IN ({','.join('?'*len(ids))}) GROUP BY replica",(ws,member(db,ws,actor["user_id"])["history_from"],actor["id"],*ids))
        return {"present":{r["replica"]:r["cursor"] for r in found}}
    if op == "replica_pull":
        ws=req["workspace"]
        m=device_member(db,ws,actor)
        after,limit=req.get("after",0),req.get("limit",500)
        semantic=req.get("semantic",False)
        table="(SELECT * FROM row_replicas UNION ALL SELECT * FROM semantic_replicas)" if semantic else "row_replicas"
        if not isinstance(semantic,bool): raise ValueError("replica channel is invalid")
        if not isinstance(after,int) or isinstance(after,bool) or after<0 or not isinstance(limit,int) or isinstance(limit,bool) or not 1<=limit<=2500: raise ValueError("replica page is invalid")
        access="x.workspace=? AND x.epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=x.workspace AND k.epoch=x.epoch AND k.device=?)"
        args=(ws,m["history_from"],actor["id"])
        floor,tail=cursor_bounds(db,table+" x",access,args)
        values=bounded(db.execute(f"SELECT cursor,envelope FROM {table} x WHERE {access} AND cursor>? ORDER BY cursor LIMIT ?",args+(after,limit)),lambda r:len(r["envelope"]))
        return {"floor":floor,"tail":tail,"replicas":[{"cursor":r["cursor"],"envelope":json.loads(r["envelope"])} for r in values]}
    if op == "pull":
        ws=req["workspace"]
        m=device_member(db,ws,actor)
        limit=req.get("limit",500)
        if not isinstance(limit,int) or isinstance(limit,bool) or not 1<=limit<=500: raise ValueError("pull limit must be 1 to 500")
        access="workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=x.workspace AND k.epoch=x.epoch AND k.device=?)"
        args=(ws,m["history_from"],actor["id"])
        floor,tail=cursor_bounds(db,"events x",access,args)
        out=rows(db,f"SELECT cursor,event,envelope,LENGTH(envelope) size FROM events x WHERE {access} AND cursor>? ORDER BY cursor LIMIT ?",args+(req.get("after",0),limit))
        return {"floor":floor,"tail":tail,"events":[{"cursor":r["cursor"],**({"lazy":True,"event":r["event"],"size":r["size"]} if r["size"]>65536 else {"envelope":json.loads(r["envelope"])})} for r in out]}
    if op == "fetch":
        m=device_member(db,req["workspace"],actor)
        row=db.execute("SELECT cursor,envelope FROM events WHERE workspace=? AND event=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace=events.workspace AND k.epoch=events.epoch AND k.device=?)",(req["workspace"],req["event"],m["history_from"],actor["id"])).fetchone()
        if not row: raise ValueError("event not found")
        return {"cursor":row["cursor"],"envelope":json.loads(row["envelope"])}
    if op == "grant_all":
        if device_member(db,req["workspace"],actor)["role"]!="admin": raise PermissionError("workspace access denied")
        previous=current_control(db,req["workspace"])
        verify_control(db,actor,req["control"],previous)
        member(db,req["workspace"],req["user"])
        current=previous["epoch"]
        expected={d for d,r in previous["devices"].items() if r["user"]==req["user"]}
        epochs={int(epoch) for epoch in req["envelopes"]}
        if len(epochs)!=len(req["envelopes"]) or any(not 1<=epoch<=current for epoch in epochs): raise ValueError("history grant epoch is outside the workspace history")
        if any(set(values)!=expected for values in req["envelopes"].values()): raise ValueError("one key envelope required for every authorized target device")
        existing={(r[0],r[1]) for r in db.execute("SELECT epoch,device FROM key_envelopes WHERE workspace=?",(req["workspace"],)).fetchall()}
        provided={(int(epoch),dev) for epoch,values in req["envelopes"].items() for dev in values}
        needed={(epoch,dev) for epoch in range(1,current+1) for dev in expected}
        if not expected or not needed<=existing|provided: raise ValueError("history grant does not cover every workspace epoch")
        if req["control"]["members"][req["user"]]["history_from"]!=1 or any(not req["control"]["devices"][d]["history"] for d in expected): raise ValueError("history control does not grant all")
        apply_history(db,req["control"])
        db.executemany("INSERT OR REPLACE INTO key_envelopes VALUES (?,?,?,?)",[(req["workspace"],int(epoch),dev,json.dumps(env)) for epoch,values in req["envelopes"].items() for dev,env in values.items()])
        db.commit()
        return {"granted":"all"}
    if op == "history_activate":
        previous=current_control(db,req["workspace"])
        verify_control(db,actor,req["control"],previous)
        target=req["control"]["approval"]["proposal"]["target"]
        device=target["device"]["id"]
        start=previous["members"][target["user"]]["history_from"]
        expected=set(range(start,previous["epoch"]+1))
        epochs={int(e) for e in req["envelopes"]}
        if epochs!=expected: raise ValueError("history activation does not cover the entitlement")
        apply_history(db,req["control"])
        db.executemany("INSERT OR REPLACE INTO key_envelopes VALUES (?,?,?,?)",[(req["workspace"],int(epoch),device,json.dumps(env)) for epoch,env in req["envelopes"].items()])
        db.execute("UPDATE device_proposals SET active=0 WHERE id=?",(control_hash(req["control"]["approval"]["proposal"]),))
        db.commit()
        return {"activated":device}
    if op == "recovery":
        db.execute("UPDATE users SET recovery=? WHERE id=?",(json.dumps(req["bundle"]),actor["user_id"]))
        db.commit()
        return {"updated":True}
    raise ValueError(f"unknown operation {op}")

DB = None
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass
    def send(self, status, value):
        body=canon(value)
        self.send_response(status)
        self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self): self.send(200,{"ok":True,"version":1}) if self.path == "/v1/health" else self.send(404,{"error":"not found"})
    def do_POST(self):
        if self.path!="/v1":
            self.send(404,{"error":"protocol v1 endpoint required"})
            return
        try:
            length=int(self.headers.get("Content-Length","0"))
            if length>64*1024*1024: raise ValueError("request exceeds 64 MiB")
            req=json.loads(self.rfile.read(length) or b"{}")
            token=self.headers.get("Authorization","").removeprefix("Bearer ")
            with closing(connect(DB)) as db: out=action(db,req,token)
            self.send(200,out)
        except PermissionError as e: self.send(403,{"error":str(e)})
        except (ValueError,KeyError,sqlite3.IntegrityError) as e: self.send(400,{"error":str(e)})
        except Exception as e: self.send(500,{"error":str(e)})

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument("command",choices=("serve","backup"))
    p.add_argument("--db",default=os.environ.get("CONVOS_SERVER_DB","convos-server.db"))
    p.add_argument("--host",default="127.0.0.1")
    p.add_argument("--port",type=int,default=8787)
    p.add_argument("--output")
    a=p.parse_args(argv)
    Path(a.db).parent.mkdir(parents=True,exist_ok=True)
    with closing(connect(a.db)): pass
    if a.command == "backup":
        if not a.output: p.error("backup requires --output")
        Path(a.output).parent.mkdir(parents=True,exist_ok=True)
        with closing(sqlite3.connect(a.db)) as src,closing(sqlite3.connect(a.output)) as dst: src.backup(dst)
        print(a.output)
        return
    global DB
    DB=a.db
    print(f"convos-server http://{a.host}:{a.port}",flush=True)
    ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
