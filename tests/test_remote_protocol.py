import copy, json
from datetime import datetime

import pytest
from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from ai_convos_remote.protocol import (b64, certificate, digest, event, fingerprint, identity, logical_fact, logical_row, open_blob, open_event, open_key, open_origin, open_replica, public_id, row_proof,
                                       public, recover, recovery_bundle, seal_event, seal_key, semantic_proof,
                                       seal_blob, seal_origin, seal_replica, verify_certificate, verify_event, verify_row_proof, verify_semantic_proof)

def fixed_identity():
    sign, box = Ed25519PrivateKey.from_private_bytes(bytes(range(32))), X25519PrivateKey.from_private_bytes(bytes(range(32, 64)))
    return dict(id=digest(sign.public_key().public_bytes_raw())[:32], name="laptop", sign_private=b64(sign.private_bytes_raw()),
                sign_public=b64(sign.public_key().public_bytes_raw()), box_private=b64(box.private_bytes_raw()), box_public=b64(box.public_key().public_bytes_raw()))

def test_identity_certificate_and_event_vector():
    root, device = identity("root"), fixed_identity()
    cert = certificate(root, "u1", device)
    assert verify_certificate(cert, root["sign_public"])["device"] == public(device)
    value = event(device, 1, "message.record", "m1", {"content":"hello"}, [], "2026-01-01T00:00:00.000000Z")
    assert value["id"] == "6ee3c8b0416343604b05cad40bed9f9b1d5ebde1af2fbe76f08ac3731d7da1d2"
    assert verify_event(value, device["sign_public"])["payload"]["content"] == "hello"


def test_logical_row_vector_ignores_storage_layout_and_local_fields():
    columns=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata","embedding"]; values=["physical","codex","Hello",datetime(2026,1,2,3,4,5,6),None,"gpt","/one","main",None,'{"z":1,"a":[2,1]}',[1.0]]
    row=logical_row("conversations",columns,values,"stable"); shuffled=list(reversed(list(zip(columns,values))))
    assert digest(row)=="84d853103ea3ca17ff92d876f9ff5f736b1322da7a027189f4ff40a331b5326a" and row==logical_row("conversations",[x[0] for x in shuffled],[x[1] for x in shuffled],"stable")
    changed=[*values]; changed[6:8]=["/other","feature"]; changed[9]='{"a":[2,1],"z":1}'; changed[-1]=[9.0]; assert logical_row("conversations",columns,changed,"stable")==row
    changed[2]="Changed"; assert digest(logical_row("conversations",columns,changed,"stable"))!=digest(row)
    assert logical_row("conversations",columns,values)["id"]=="physical" and logical_row("conversations",columns,values,"stable")["id"]=="stable" and logical_row("messages",identity="m",state="deleted")["data"] is None


def test_logical_attachment_excludes_body_location_and_temporary_url():
    columns=["id","message_id","filename","mime_type","size","path","url","created_at","body_hash"]; values=["a","m","x.png","image/png",3,"/one","https://temporary/one",datetime(2026,1,1),"a"*64]
    other=[*values]; other[5:7]=["/two","https://temporary/two"]
    assert logical_row("attachments",columns,values)==logical_row("attachments",columns,other)
    other[-1]="b"*64; assert logical_row("attachments",columns,values)!=logical_row("attachments",columns,other)
    with pytest.raises(ValueError,match="logical row"): logical_row("messages",identity="m",state="unknown")


def test_logical_provenance_fact_signs_semantics_not_checkout_observation():
    record={"kind":"repository.observed","entity":"r","payload":{"id":"r","lineage":"l","roots":["portable"],"remotes":[],"head":"local-head"},"observed_at":"2026-01-01T00:00:00Z"}; row=logical_fact(record); assert row["data"]=={"lineage":"l","roots":["portable"],"remotes":[]} and row==logical_fact({**record,"payload":{**record["payload"],"head":"other-head"},"observed_at":"2027-01-01T00:00:00Z"})
    root,device=identity("root"),fixed_identity(); user=public_id(root["sign_public"]); proof=row_proof(device,user,"w",1,row); assert verify_row_proof(proof,row,certificate(root,user,device),root["sign_public"])
    with pytest.raises(ValueError,match="row proof"): verify_row_proof(proof,{**row,"data":{**row["data"],"lineage":"changed"}},certificate(root,user,device),root["sign_public"])
    deleted=logical_fact({**record,"payload":{"id":"r","state":"deleted"}})
    retirement=row_proof(device,user,"w",1,deleted,proof['revision'])
    assert deleted['data'] is None and verify_row_proof(retirement,deleted,certificate(root,user,device),root['sign_public'])
    with pytest.raises(ValueError,match="row proof"): verify_row_proof(proof,deleted,certificate(root,user,device),root['sign_public'])


def test_row_proof_binds_origin_revision_predecessor_and_deletion():
    root,device=identity("root"),fixed_identity(); user=public_id(root["sign_public"]); cert=certificate(root,user,device); active=logical_row("messages",["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"],["m","c","user","hello",None,datetime(2026,1,1),None,"{}",None]); first=row_proof(device,user,"origin",2,active)
    assert first==row_proof(device,user,"origin",2,active,content_hash=digest(active)) and first["previous_revision"] is None and set(first)=={"v","kind","workspace","authorization_workspace","row_kind","row_id","encoding_v","content_hash","revision","previous_revision","state","author_user_id","author_device_id","authorization_epoch","signature"} and verify_row_proof(first,active,cert,root["sign_public"])==first
    deleted=logical_row("messages",identity="m",state="deleted"); tombstone=row_proof(device,user,"origin",3,deleted,first["revision"]); restored=row_proof(device,user,"origin",4,active,tombstone["revision"]); assert tombstone["revision"]!=first["revision"] and tombstone["previous_revision"]==first["revision"] and restored["content_hash"]==first["content_hash"] and restored["revision"]!=first["revision"] and verify_row_proof(tombstone,deleted,cert,root["sign_public"])
    for changed in ({**first,"workspace":"other"},{**first,"authorization_workspace":"other"},{**first,"previous_revision":"f"*64},{**first,"author_user_id":"0"*32},{**first,"authorization_epoch":3}):
        with pytest.raises(ValueError,match="row proof"): verify_row_proof(changed,active,cert,root["sign_public"])


def test_row_proof_rejects_wrong_body_signer_and_self_predecessor():
    root,device=identity("root"),fixed_identity(); user=public_id(root["sign_public"]); cert=certificate(root,user,device); row=logical_row("attachments",identity="a",state="deleted"); proof=row_proof(device,user,"origin",1,row)
    with pytest.raises(ValueError,match="row proof"): verify_row_proof(proof,logical_row("attachments",identity="b",state="deleted"),cert,root["sign_public"])
    with pytest.raises(ValueError,match="row proof"): verify_row_proof(proof,row,certificate(root,user,identity("other")),root["sign_public"])
    with pytest.raises(ValueError,match="row proof"): verify_row_proof({**proof,"previous_revision":proof["revision"]},row,cert,root["sign_public"])


def test_delivery_replica_separates_origin_author_from_uploader():
    root,author,uploader=identity("root"),fixed_identity(),identity("peer"); user=public_id(root["sign_public"]); cert=certificate(root,user,author); row=logical_row("messages",identity="m",state="deleted"); proof=row_proof(author,user,"origin",2,row,"a"*64); key=bytes(range(32)); env=seal_replica(row,proof,"replacement",1,key,uploader["id"]); opened=open_replica(env,key)
    assert env["uploader"]==uploader["id"]!=proof["author_device_id"] and env["replica"]!=proof["revision"] and opened["lineage"]==[] and verify_row_proof(opened["proof"],opened["row"],cert,root["sign_public"])
    with pytest.raises(ValueError,match="replica"): open_replica({**env,"replica":"0"*64},key)
    with pytest.raises(ValueError,match="proof mismatch"): seal_replica(row,proof,"replacement",1,key,uploader["id"],"0"*64)
    assert open_replica(seal_replica(row,proof,"replacement",1,key,uploader["id"],digest(row)),key)["row"]==row


def test_origin_bundle_is_encrypted_once_and_bound_to_destination_epoch():
    key=bytes(range(32)); controls=[{"workspace":"origin-workspace","revision":1}]; env=seal_origin(controls,"replacement",3,key,"peer",False); assert open_origin(env,key)=={"controls":controls,"rows":False} and env["workspace"]=="replacement" and "origin-workspace" not in json.dumps({k:v for k,v in env.items() if k!="ciphertext"})
    with pytest.raises(ValueError,match="origin bundle"): open_origin({**env,"epoch":4},key)


def test_blob_replica_is_bounded_content_addressed_and_epoch_bound():
    key=bytes(range(32)); data=b"retained body"; env=seal_blob(data,"team",2,key,"peer"); assert open_blob(env,key)==(data,digest(data)) and "retained body" not in json.dumps(env)
    with pytest.raises(ValueError,match="blob replica"): open_blob({**env,"epoch":3},key)
    with pytest.raises(ValueError,match="32 MiB"): seal_blob(b"x"*(32*1024**2+1),"team",2,key,"peer")


def test_event_encryption_tamper_signature_and_header_binding():
    device, other, key = identity(), identity(), bytes(range(32))
    value = event(device, 7, "conversation.record", "c1", {"title":"private"}, [], "2026-01-01T00:00:00Z")
    envelope = seal_event(value, "w1", 3, key)
    assert "private" not in json.dumps(envelope) and open_event(envelope, key, device["sign_public"]) == value
    bad = copy.deepcopy(envelope); bad["workspace"] = "w2"
    with pytest.raises(InvalidTag): open_event(bad, key, device["sign_public"])
    with pytest.raises(InvalidSignature): open_event(envelope, key, other["sign_public"])


def test_key_envelope_recovery_and_private_fingerprint():
    device, key = identity(), bytes(reversed(range(32)))
    wrapped = seal_key(key, device["box_public"], "workspace:w1:epoch:2")
    assert open_key(wrapped, device["box_private"]) == key
    with pytest.raises(ValueError,match="mismatched"): open_key(wrapped,device["box_private"],"workspace:w2:epoch:2")
    recovery, bundle = recovery_bundle({"workspace_keys":{"w1:2":key.hex()}}, bytes([9])*32)
    assert recover(bundle, recovery)["workspace_keys"]["w1:2"] == key.hex()
    assert fingerprint(key, "https://example/repo") == "07e9b5f5727490c3d14b5ed15cdfe6f9bb3c83a04995f9e8081a5b8fa2eb6413"


def test_replay_under_another_identity_and_payload_mutation_rejected():
    a, b = identity("a"), identity("b")
    value = event(a, 1, "x.future", "x", {"unknown":True}, [], "2026-01-01T00:00:00Z")
    forged = {**value, "author":b["id"]}
    with pytest.raises((InvalidSignature, ValueError)): verify_event(forged, a["sign_public"])
    changed = copy.deepcopy(value); changed["payload"]["unknown"] = False
    with pytest.raises((InvalidSignature, ValueError)): verify_event(changed, a["sign_public"])


def test_semantic_proof_is_self_contained_and_compacts_bodyless_ancestry():
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); active={"v":1,"kind":"memory.canonical","id":"memory:global:m","state":"active","data":{"content":"one"}}; first=semantic_proof(root,user,device["id"],"personal",2,active); deleted={**active,"state":"deleted","data":None}; tombstone=semantic_proof(root,user,device["id"],"personal",3,deleted,first); fork=semantic_proof(root,user,device["id"],"personal",3,{**active,"data":{"content":"fork"}},first); merged=semantic_proof(root,user,device["id"],"personal",4,{**active,"data":{"content":"merged"}},[tombstone,fork])
    assert verify_semantic_proof(first,active,user)==first and verify_semantic_proof(tombstone,deleted,user)==tombstone and tombstone["ancestors"]==[first["revision"]] and tombstone["previous_revision"]==first["revision"] and {tombstone["revision"],fork["revision"],first["revision"]}<=set(merged["ancestors"])
    for changed in ({**tombstone,"workspace":"other"},{**tombstone,"author_user_id":"0"*32},{**tombstone,"ancestors":[]},{**tombstone,"content_hash":"0"*64}):
        with pytest.raises(ValueError,match="semantic proof"): verify_semantic_proof(changed,deleted,user)
