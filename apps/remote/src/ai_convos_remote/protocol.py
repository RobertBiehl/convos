"""Canonical signed/encrypted event protocol. Wire format v1; server sees envelopes only."""
import base64, hashlib, hmac, json, os
from datetime import datetime, timezone
from functools import lru_cache

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from ai_convos.cli import required

V = 1
ROW_FIELDS_V1={"conversations":("source","title","created_at","updated_at","model","project_id","metadata"),"messages":("conversation_id","role","content","thinking","created_at","model","metadata","parent_id"),"tool_calls":("message_id","tool_name","input","output","status","duration_ms","created_at"),"attachments":("message_id","filename","mime_type","size","body_hash","created_at"),"artifacts":("conversation_id","artifact_type","title","content","language","created_at","version"),"file_edits":("message_id","file_path","edit_type","content","created_at","old_content")}
PROVENANCE_FIELDS_V1={"repository.observed":("lineage","roots","remotes"),"file.observed":("repository","path","kind"),"file.version":("file","content_hash","observed_at"),"edit.observed":("turn","file","repository","old_content_hash","new_content_hash","evidence"),"git.checkpoint":("repository","head","state_hash","paths","observed_at","capture_source"),"checkpoint.link":("checkpoint","edit","evidence")}; SEMANTIC_FIELDS_V1=ROW_FIELDS_V1|PROVENANCE_FIELDS_V1
ROW_JSON_V1={"metadata","input","output"}; ROW_TIME_V1={"created_at","updated_at"}
ROW_PROOF_FIELDS={"v","kind","workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature"}
SEMANTIC_PROOF_FIELDS={"v","kind","workspace","object_kind","object_id","encoding_v","content_hash","revision","previous_revision","ancestors","state","author_user_id","author_device_id","authorization_epoch","root_public","signature"}
def canon(v): return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
def b64(v): return base64.urlsafe_b64encode(v).decode().rstrip("=")
def unb64(v): return base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
def digest(v): return hashlib.sha256(v if isinstance(v, bytes) else canon(v)).hexdigest()
def public_id(value): return digest(unb64(value))[:32]
def now(): return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
def logical_row(table,columns=(),values=(),identity=None,v=1,state="active"):
    if v!=1 or table not in ROW_FIELDS_V1 or state not in ("active","deleted") or state=="deleted" and (not identity or columns or values) or len(columns)!=len(values) or len(set(columns))!=len(columns): raise ValueError("invalid logical row schema")
    if state=="deleted": return {"v":v,"kind":table,"id":identity,"state":state,"data":None}
    row=dict(zip(columns,values)); required={"id",*ROW_FIELDS_V1[table]}
    if not required<=set(row): raise ValueError("incomplete logical row")
    norm=lambda k,v: json.loads(v) if v is not None and k in ROW_JSON_V1 and isinstance(v,str) else v.isoformat(timespec="microseconds") if v is not None and k in ROW_TIME_V1 and isinstance(v,datetime) else v
    return {"v":v,"kind":table,"id":identity or row["id"],"state":state,"data":{k:norm(k,row[k]) for k in ROW_FIELDS_V1[table]}}
def logical_fact(record):
    kind,p=record["kind"],record["payload"]
    if kind not in PROVENANCE_FIELDS_V1 or record["entity"]!=(p.get("id") if kind!="checkpoint.link" else digest({"checkpoint":p["checkpoint"],"edit":p["edit"]})): raise ValueError("invalid provenance fact")
    return {"v":1,"kind":kind,"id":record["entity"],"state":"active","data":{k:(v.isoformat(timespec="microseconds") if isinstance(v,datetime) else v) for k in PROVENANCE_FIELDS_V1[kind] for v in [record["observed_at"] if k=="observed_at" else p[k]]}}
def _priv(cls, value): return cls.from_private_bytes(unb64(value))
def _pub(cls, value): return cls.from_public_bytes(unb64(value))
def _raw(k): return b64(k.private_bytes_raw() if hasattr(k, "private_bytes_raw") else k.public_bytes_raw())

def identity(name="device"):
    sign, box = Ed25519PrivateKey.generate(), X25519PrivateKey.generate(); sp, bp = sign.public_key(), box.public_key()
    return dict(id=digest(sp.public_bytes_raw())[:32], name=name, sign_private=_raw(sign), sign_public=_raw(sp), box_private=_raw(box), box_public=_raw(bp))

def public(identity): return {k:identity[k] for k in ("id", "name", "sign_public", "box_public")}

def certificate(root, user, device):
    body = dict(v=V, user=user, device=public(device), issued_at=now()); body["signature"] = b64(_priv(Ed25519PrivateKey, root["sign_private"]).sign(canon(body)))
    return body

def verify_certificate(cert, root_public):
    sig, body = unb64(cert["signature"]), {k:v for k,v in cert.items() if k != "signature"}; _pub(Ed25519PublicKey, root_public).verify(sig, canon(body))
    if body["v"] != V: raise ValueError(f"Unsupported certificate version {body['v']}")
    return body
@lru_cache(maxsize=256)
def verified_certificate(raw,root_public): return verify_certificate(json.loads(raw),root_public)
def registration_proof(device,challenge,root_public,cert):
    body={"v":V,"kind":"device.registration","challenge":digest(challenge),"root_public":root_public,"certificate":digest(cert),"user":public_id(root_public),"device":device["id"]}; body["signature"]=b64(_priv(Ed25519PrivateKey,device["sign_private"]).sign(canon(body))); return body

def row_proof(device,user,workspace,epoch,row,previous=None,authorization_workspace=None,content_hash=None):
    claim={"row_kind":row["kind"],"row_id":row["id"],"encoding_v":row["v"],"content_hash":content_hash if content_hash is not None else digest(row),"previous_revision":previous,"state":row["state"]}; body={"v":1,"kind":"row.proof","workspace":workspace,"authorization_workspace":authorization_workspace or workspace,**claim,"revision":digest({"v":1,**claim}),"author_user_id":user,"author_device_id":device["id"],"authorization_epoch":epoch}; body["signature"]=b64(_priv(Ed25519PrivateKey,device["sign_private"]).sign(canon(body))); return body

def verify_row_proof_header(value,cert,root_public):
    try:
        signed={k:v for k,v in value.items() if k!="signature"}; device=verified_certificate(canon(cert),root_public)["device"]; previous=value["previous_revision"]
        claim={k:value[k] for k in ("row_kind","row_id","encoding_v","content_hash","previous_revision","state")}
        hex64=lambda v:isinstance(v,str) and len(v)==64 and not any(c not in "0123456789abcdef" for c in v)
        if set(value)!=ROW_PROOF_FIELDS or value["v"]!=1 or value["kind"]!="row.proof" or any(not isinstance(value[k],str) or not value[k] for k in ("workspace","authorization_workspace","row_id")) or value["row_kind"] not in SEMANTIC_FIELDS_V1 or value["encoding_v"]!=1 or not hex64(value["content_hash"]) or value["state"] not in ("active","deleted") or value["row_kind"] in PROVENANCE_FIELDS_V1 and value["state"]!="active" or value["revision"]!=digest({"v":1,**claim}) or previous is not None and (not hex64(previous) or previous==value["revision"]) or not isinstance(value["authorization_epoch"],int) or isinstance(value["authorization_epoch"],bool) or value["authorization_epoch"]<1 or (cert["user"],device["id"],public_id(root_public),public_id(device["sign_public"]))!=(value["author_user_id"],value["author_device_id"],value["author_user_id"],value["author_device_id"]): raise ValueError
        _pub(Ed25519PublicKey,device["sign_public"]).verify(unb64(value["signature"]),canon(signed)); return value
    except (InvalidSignature,KeyError,TypeError,ValueError) as e: raise ValueError("invalid row proof") from e

def verify_row_proof(value,row,cert,root_public):
    try:
        verify_row_proof_header(value,cert,root_public)
        if set(row)!={"v","kind","id","state","data"} or row["kind"] not in SEMANTIC_FIELDS_V1 or not isinstance(row["id"],str) or not row["id"] or row["v"]!=1 or row["state"] not in ("active","deleted") or row["kind"] in PROVENANCE_FIELDS_V1 and row["state"]!="active" or row["data"] is not None and (row["state"]!="active" or set(row["data"])!=set(SEMANTIC_FIELDS_V1[row["kind"]])) or row["state"]=="deleted" and row["data"] is not None or (value["row_kind"],value["row_id"],value["encoding_v"],value["content_hash"],value["state"])!=(row["kind"],row["id"],row["v"],digest(row),row["state"]): raise ValueError
        return value
    except (KeyError,TypeError,ValueError) as e: raise ValueError("invalid row proof") from e

def semantic_proof(root,user,device,workspace,epoch,row,previous=None):
    parents=[] if previous is None else previous if isinstance(previous,list) else [previous]; ancestors=[] if not parents else [parents[0]["revision"],*sorted({a for p in parents for a in [p["revision"],*p["ancestors"]]}-{parents[0]["revision"]})]; claim={"object_kind":row["kind"],"object_id":row["id"],"encoding_v":row["v"],"content_hash":digest(row),"previous_revision":ancestors[0] if ancestors else None,"ancestors":ancestors,"state":row["state"]}; body={"v":1,"kind":"semantic.proof","workspace":workspace,**claim,"revision":digest({"v":1,**claim}),"author_user_id":user,"author_device_id":device,"authorization_epoch":epoch,"root_public":root["sign_public"]}; body["signature"]=b64(_priv(Ed25519PrivateKey,root["sign_private"]).sign(canon(body))); return body

def verify_semantic_proof(value,row,user):
    try:
        claim={k:value[k] for k in ("object_kind","object_id","encoding_v","content_hash","previous_revision","ancestors","state")}; previous,ancestors=value["previous_revision"],value["ancestors"]
        if set(row)!={"v","kind","id","state","data"} or row["v"]!=1 or not isinstance(row["kind"],str) or not isinstance(row["id"],str) or not row["id"] or row["state"] not in ("active","deleted") or row["state"]=="active" and not isinstance(row["data"],dict) or row["state"]=="deleted" and row["data"] is not None: raise ValueError
        if set(value)!=SEMANTIC_PROOF_FIELDS or value["v"]!=1 or value["kind"]!="semantic.proof" or value["author_user_id"]!=user or public_id(value["root_public"])!=user or not isinstance(value["author_device_id"],str) or len(value["author_device_id"])!=32 or (value["object_kind"],value["object_id"],value["encoding_v"],value["content_hash"],value["state"])!=(row["kind"],row["id"],row["v"],digest(row),row["state"]) or value["revision"]!=digest({"v":1,**claim}) or not isinstance(ancestors,list) or len(ancestors)!=len(set(ancestors)) or any(not isinstance(a,str) or len(a)!=64 or any(c not in "0123456789abcdef" for c in a) for a in ancestors) or previous!=(ancestors[0] if ancestors else None) or value["revision"] in ancestors or not isinstance(value["authorization_epoch"],int) or isinstance(value["authorization_epoch"],bool) or value["authorization_epoch"]<1: raise ValueError
        _pub(Ed25519PublicKey,value["root_public"]).verify(unb64(value["signature"]),canon({k:v for k,v in value.items() if k!="signature"})); return value
    except (InvalidSignature,KeyError,TypeError,ValueError) as e: raise ValueError("invalid semantic proof") from e

def event(device, seq, kind, entity, payload, parents=(), observed_at=None, payload_v=1):
    body = dict(v=V, kind=kind, entity=entity, revision=digest(payload), author=device["id"], seq=seq, parents=list(parents), observed_at=observed_at or now(), payload_v=payload_v, payload=payload)
    body["id"] = digest(body); body["signature"] = b64(_priv(Ed25519PrivateKey, device["sign_private"]).sign(canon(body)))
    return body

def verify_event(value, sign_public):
    if value["v"] != V: raise ValueError(f"Unsupported event version {value['v']}")
    sig, signed = unb64(value["signature"]), {k:v for k,v in value.items() if k != "signature"}; _pub(Ed25519PublicKey, sign_public).verify(sig, canon(signed))
    body = {k:v for k,v in signed.items() if k != "id"}
    if digest(body) != value["id"] or digest(value["payload"]) != value["revision"]: raise ValueError("Invalid event digest")
    return value
def signer(devices,author): value=devices[author]["sign_public"]; return required(value if public_id(value)==author else None,ValueError("device signing key mismatch"))
def sign_control(device,body): return {**body,"control_signature":b64(_priv(Ed25519PrivateKey,device["sign_private"]).sign(canon(body)))}

def seal_event(value, workspace, epoch, key):
    nonce = os.urandom(12); header = dict(v=V, workspace=workspace, epoch=epoch, event=value["id"], author=value["author"], seq=value["seq"], parents=value["parents"], nonce=b64(nonce))
    return {**header, "ciphertext":b64(AESGCM(key).encrypt(nonce, canon(value), canon(header)))}

def open_event(envelope, key, sign_public):
    if envelope["v"] != V: raise ValueError(f"Unsupported envelope version {envelope['v']}")
    header = {k:envelope[k] for k in ("v", "workspace", "epoch", "event", "author", "seq", "parents", "nonce")}; value = json.loads(AESGCM(key).decrypt(unb64(header["nonce"]), unb64(envelope["ciphertext"]), canon(header)))
    verify_event(value, sign_public)
    if (value["id"], value["author"], value["seq"], value["parents"]) != (header["event"], header["author"], header["seq"], header["parents"]): raise ValueError("Envelope header mismatch")
    return value

def seal_replica(row,proof,workspace,epoch,key,uploader,content_hash=None,lineage=()):
    if proof["content_hash"]!=(content_hash if content_hash is not None else digest(row)): raise ValueError("row replica proof mismatch")
    nonce=os.urandom(12); header={"v":1,"kind":"row.replica","workspace":workspace,"replica":fingerprint(key,digest(proof)),"epoch":epoch,"uploader":uploader,"nonce":b64(nonce)}; return {**header,"ciphertext":b64(AESGCM(key).encrypt(nonce,canon({"row":row,"proof":proof,"lineage":list(lineage)}),canon(header)))}

def open_replica(value,key):
    try:
        if set(value)!={"v","kind","workspace","replica","epoch","uploader","nonce","ciphertext"} or value["v"]!=1 or value["kind"]!="row.replica": raise ValueError
        header={k:value[k] for k in ("v","kind","workspace","replica","epoch","uploader","nonce")}; body=json.loads(AESGCM(key).decrypt(unb64(value["nonce"]),unb64(value["ciphertext"]),canon(header)))
        if set(body)!={"row","proof","lineage"} or not isinstance(body["lineage"],list) or value["replica"]!=fingerprint(key,digest(body["proof"])) or body["proof"]["content_hash"]!=digest(body["row"]): raise ValueError
        return body
    except (InvalidTag,KeyError,TypeError,ValueError) as e: raise ValueError("invalid row replica") from e

def seal_origin(controls,workspace,epoch,key,uploader,rows=True):
    body={"controls":controls,"rows":rows}; nonce=os.urandom(12); header={"v":1,"kind":"origin.bundle","workspace":workspace,"origin":fingerprint(key,digest(body)),"epoch":epoch,"uploader":uploader,"nonce":b64(nonce)}; return {**header,"ciphertext":b64(AESGCM(key).encrypt(nonce,canon(body),canon(header)))}
def open_origin(value,key):
    try: header={k:value[k] for k in ("v","kind","workspace","origin","epoch","uploader","nonce")}; body=json.loads(AESGCM(key).decrypt(unb64(value["nonce"]),unb64(value["ciphertext"]),canon(header))); return required(body if set(value)==set(header)|{"ciphertext"} and value["v"]==1 and value["kind"]=="origin.bundle" and set(body)=={"controls","rows"} and isinstance(body["controls"],list) and body["controls"] and isinstance(body["rows"],bool) and value["origin"]==fingerprint(key,digest(body)) else None,ValueError())
    except (InvalidTag,KeyError,TypeError,ValueError) as e: raise ValueError("invalid origin bundle") from e

def seal_blob(data,workspace,epoch,key,uploader):
    if len(data)>32*1024**2: raise ValueError("attachment body exceeds 32 MiB")
    nonce=os.urandom(12); body_hash=digest(data); header={"v":1,"kind":"blob.replica","workspace":workspace,"blob":fingerprint(key,body_hash),"epoch":epoch,"uploader":uploader,"size":len(data),"nonce":b64(nonce)}; return {**header,"ciphertext":b64(AESGCM(key).encrypt(nonce,data,canon(header)))}
def open_blob(value,key):
    try: header={k:value[k] for k in ("v","kind","workspace","blob","epoch","uploader","size","nonce")}; data=AESGCM(key).decrypt(unb64(value["nonce"]),unb64(value["ciphertext"]),canon(header)); body_hash=digest(data); return required((data,body_hash) if set(value)==set(header)|{"ciphertext"} and value["v"]==1 and value["kind"]=="blob.replica" and 0<=value["size"]<=32*1024**2 and len(data)==value["size"] and value["blob"]==fingerprint(key,body_hash) else None,ValueError())
    except (InvalidTag,KeyError,TypeError,ValueError) as e: raise ValueError("invalid blob replica") from e

def _wrap_key(shared, context): return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"convos-key-v1:" + context.encode()).derive(shared)
def seal_key(key, recipient_public, context):
    ephemeral, nonce = X25519PrivateKey.generate(), os.urandom(12); shared = ephemeral.exchange(_pub(X25519PublicKey, recipient_public)); aad = canon(dict(v=V, context=context, ephemeral=_raw(ephemeral.public_key()), nonce=b64(nonce)))
    return dict(v=V, context=context, ephemeral=_raw(ephemeral.public_key()), nonce=b64(nonce), ciphertext=b64(AESGCM(_wrap_key(shared, context)).encrypt(nonce, key, aad)))

def open_key(value, recipient_private, context=None):
    if value["v"] != V or context is not None and value["context"]!=context: raise ValueError("Unsupported or mismatched key envelope")
    shared = _priv(X25519PrivateKey, recipient_private).exchange(_pub(X25519PublicKey, value["ephemeral"])); aad = canon({k:value[k] for k in ("v", "context", "ephemeral", "nonce")})
    return AESGCM(_wrap_key(shared, value["context"])).decrypt(unb64(value["nonce"]), unb64(value["ciphertext"]), aad)

def recovery_bundle(payload, recovery=None):
    key, nonce = recovery or os.urandom(32), os.urandom(12); header = dict(v=V, kdf="raw-256", nonce=b64(nonce)); header["ciphertext"] = b64(AESGCM(key).encrypt(nonce, canon(payload), canon(header)))
    return b64(key), header

def recover(value, recovery):
    if value["v"] != V or value["kdf"] != "raw-256": raise ValueError("Unsupported recovery bundle")
    header = {k:value[k] for k in ("v", "kdf", "nonce")}; return json.loads(AESGCM(unb64(recovery)).decrypt(unb64(value["nonce"]), unb64(value["ciphertext"]), canon(header)))

def fingerprint(key, value): return hmac.new(key, value if isinstance(value, bytes) else canon(value), hashlib.sha256).hexdigest()
