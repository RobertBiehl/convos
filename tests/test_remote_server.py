import copy, json, sqlite3, threading

import pytest
import ai_convos_remote_server as server_module
from ai_convos_remote.control import CONTROL_V, record, sign, state_hash
from ai_convos_remote.protocol import certificate, digest, event, identity, logical_row, open_blob, registration_proof, row_proof, seal_blob, seal_event, seal_key, seal_replica, sign_control
from ai_convos_remote_server import action, bounded, connect, ledger_state


def register_device(db,name,root,dev,recovery=None):
    cert=certificate(root,root["id"],dev); base={"root_public":root["sign_public"],"certificate":cert}; challenge=action(db,{"op":"register_challenge",**base})["challenge"]; return action(db,{"op":"register","user_name":name,**base,"challenge":challenge,"proof":registration_proof(dev,challenge,root["sign_public"],cert),**({"recovery":recovery} if recovery else {})})
def account(db, name):
    root, dev = identity(name + " root"), identity(name + " laptop"); user = root["id"]
    result = register_device(db,name,root,dev,{"ciphertext":"opaque"})
    return {"root":root,"device":dev,"user":user,"token":result["token"]}
def device_record(a): return record(a["user"],a["root"]["sign_public"],a["device"],certificate(a["root"],a["user"],a["device"]))
def create_ws(db,a,ws,key,kind):
    state=sign(a["device"],{"v":CONTROL_V,"kind":"workspace.state","workspace":ws,"scope":kind,"revision":1,"prev":None,"epoch":1,"boundary":{"epoch":1,"tail":0,"heads":{}},"key_commitment":digest(key),"members":{a["user"]:{"role":"admin","joined":1,"history_from":1}},"devices":{a["device"]["id"]:device_record(a)},"removed":[],"action":"create","approval":None,"approved_at":0})
    action(db,sign_control(a["device"],{"op":"create","workspace":ws,"kind":kind,"control":state,"envelopes":{a["device"]["id"]:seal_key(key,a["device"]["box_public"],f"workspace:{ws}:epoch:1")}}),a["token"]); return state
def rotate_ws(db,a,previous,key,people,boundary=None):
    epoch=previous["epoch"]+1; members={p["user"]:{"role":role,"joined":previous["members"].get(p["user"],{"joined":epoch})["joined"],"history_from":previous["members"].get(p["user"],{"history_from":epoch})["history_from"]} for p,role in people}; devices={p["device"]["id"]:previous["devices"].get(p["device"]["id"],device_record(p)) for p,_ in people}; state=sign(a["device"],{"v":CONTROL_V,"kind":"workspace.state","workspace":previous["workspace"],"scope":previous["scope"],"revision":previous["revision"]+1,"prev":state_hash(previous),"epoch":epoch,"boundary":boundary or {"epoch":epoch,**ledger_state(db,previous["workspace"])},"key_commitment":digest(key),"members":members,"devices":devices,"removed":sorted(set(previous["removed"])|set(previous["devices"])-set(devices)),"action":"membership","approval":None,"approved_at":0}); envs={p["device"]["id"]:seal_key(key,p["device"]["box_public"],f"workspace:{previous['workspace']}:epoch:{epoch}") for p,_ in people}; action(db,sign_control(a["device"],{"op":"rotate","workspace":previous["workspace"],"control":state,"envelopes":envs}),a["token"]); return state
def history_ws(a,previous,user):
    members={**previous["members"],user:{**previous["members"][user],"history_from":1}}; return sign(a["device"],{**{k:v for k,v in previous.items() if k not in ("signature","author")},"revision":previous["revision"]+1,"prev":state_hash(previous),"members":members,"action":"history","approval":None,"approved_at":0})


def test_incompatible_relay_database_is_rejected_instead_of_mutated(tmp_path):
    path=tmp_path/"server.db"; db=sqlite3.connect(path); db.execute("CREATE TABLE events(cursor INTEGER PRIMARY KEY)"); db.close()
    with pytest.raises(ValueError,match="incompatible"): connect(path)


def test_paged_ledgers_seek_workspace_cursor_indexes(tmp_path):
    db=connect(tmp_path/"server.db"); tables={"events":"event_workspace_cursor","row_replicas":"replica_workspace_cursor","semantic_replicas":"semantic_workspace_cursor","blob_replicas":"blob_workspace_cursor","origin_bundles":"origin_workspace_cursor"}
    for table,index in tables.items():
        assert tuple(r[2] for r in db.execute(f"PRAGMA index_info({index})"))==("workspace","cursor")
        for direction in ("","DESC"):
            plan=" ".join(str(v) for row in db.execute(f"EXPLAIN QUERY PLAN SELECT cursor FROM {table} WHERE workspace=? AND epoch>=? AND EXISTS(SELECT 1 FROM key_envelopes k WHERE k.workspace={table}.workspace AND k.epoch={table}.epoch AND k.device=?) ORDER BY cursor {direction} LIMIT 1",("w",1,"d")) for v in row); assert index in plan and "TEMP B-TREE" not in plan


def test_personal_workspace_idempotency_and_ciphertext_only(tmp_path):
    db = connect(tmp_path/"server.db"); a = account(db,"alice"); key = bytes(range(32)); ws = "personal-alice"
    create_ws(db,a,ws,key,"personal")
    value = event(a["device"],1,"message.record","m1",{"content":"server must not see this"},[],"2026-01-01T00:00:00Z"); envelope = seal_event(value,ws,1,key)
    first = action(db,{"op":"upload","envelope":envelope},a["token"]); second = action(db,{"op":"upload","envelope":envelope},a["token"])
    assert first["created"] and not second["created"] and first["cursor"] == second["cursor"]
    assert "server must not see this" not in (tmp_path/"server.db").read_bytes().decode(errors="ignore")
    pulled=action(db,{"op":"pull","workspace":ws,"after":0},a["token"]); assert pulled["events"][0]["envelope"]==envelope and (pulled["floor"],pulled["tail"])==(first["cursor"],first["cursor"])
    status=action(db,{"op":"state"},a["token"]); assert status["capabilities"]["sync_tails"]==1 and status["workspaces"][0]["sync"]=={"events":first["cursor"],"replicas":0,"blobs":0,"origins":0}
    bad = copy.deepcopy(envelope); bad["ciphertext"] = bad["ciphertext"][:-1] + ("A" if bad["ciphertext"][-1] != "A" else "B")
    with pytest.raises(ValueError,match="different ciphertext"): action(db,{"op":"upload","envelope":bad},a["token"])
    same_seq = seal_event(event(a["device"],1,"message.record","m2",{"content":"different"},[],"2026-01-02T00:00:00Z"),ws,1,key)
    with pytest.raises(Exception): action(db,{"op":"upload","envelope":same_seq},a["token"])


def test_repairable_replica_is_uploader_bounded_and_replaceable(tmp_path,monkeypatch):
    db=connect(tmp_path/"server.db"); a,b=account(db,"alice"),account(db,"bob"); ws,key="team",bytes([3])*32; state=create_ws(db,a,ws,key,"team"); rotate_ws(db,a,state,bytes([4])*32,((a,"admin"),(b,"member"))); key=bytes([4])*32; row=logical_row("messages",identity="m",state="deleted"); proof=row_proof(a["device"],a["user"],ws,2,row,"a"*64); env=seal_replica(row,proof,ws,2,key,b["device"]["id"])
    first=action(db,{"op":"replica_upload_many","envelopes":[env]},b["token"])["replicas"][0]; same=action(db,{"op":"replica_upload_many","envelopes":[env]},b["token"])["replicas"][0]; replacement=seal_replica(row,proof,ws,2,key,b["device"]["id"]); changed=action(db,{"op":"replica_upload_many","envelopes":[replacement]},b["token"])["replicas"][0]; page=action(db,{"op":"replica_pull","workspace":ws,"after":0},a["token"])
    present=action(db,{"op":"replica_reconcile","workspace":ws,"replicas":[env["replica"]]},a["token"]); missing=action(db,{"op":"replica_reconcile","workspace":ws,"replicas":["f"*64]},a["token"])
    assert first["created"] and not same["created"] and not same["replaced"] and changed["replaced"] and first["cursor"]==same["cursor"]==changed["cursor"] and page["replicas"]==[{"cursor":first["cursor"],"envelope":replacement}] and present=={"present":{env["replica"]:first["cursor"]}} and missing=={"present":{}} and db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1 and db.execute("SELECT bytes FROM replica_usage").fetchone()[0]==len(server_module.canon(replacement))
    semantic=action(db,{"op":"replica_upload_many","envelopes":[replacement],"semantic":True},b["token"])["replicas"][0]; old=action(db,{"op":"replica_pull","workspace":ws,"after":0},a["token"]); current=action(db,{"op":"replica_pull","workspace":ws,"after":0,"semantic":True},a["token"]); semantic_present=action(db,{"op":"replica_reconcile","workspace":ws,"replicas":[env["replica"]],"semantic":True},a["token"])
    assert old["replicas"]==page["replicas"] and len(current["replicas"])==2 and semantic["cursor"]>first["cursor"] and semantic_present=={"present":{env["replica"]:semantic["cursor"]}} and db.execute("SELECT COUNT(*) FROM semantic_replicas").fetchone()[0]==1
    with pytest.raises(PermissionError,match="rejected"): action(db,{"op":"replica_upload_many","envelopes":[{**replacement,"uploader":a["device"]["id"]}]},b["token"])
    monkeypatch.setattr(server_module,"REPLICA_QUOTA",1)
    with pytest.raises(ValueError,match="quota"): action(db,{"op":"replica_upload_many","envelopes":[replacement]},b["token"])


def test_replica_quota_is_atomic_across_connections(tmp_path,monkeypatch):
    path=tmp_path/"server.db"; db=connect(path); a=account(db,"alice"); ws,key="personal",bytes([3])*32; create_ws(db,a,ws,key,"personal"); rows=[logical_row("messages",identity=str(i),state="deleted") for i in range(2)]; envs=[seal_replica(row,row_proof(a["device"],a["user"],ws,1,row),ws,1,key,a["device"]["id"]) for row in rows]; monkeypatch.setattr(server_module,"REPLICA_QUOTA",len(server_module.canon(envs[0]))+10); db.close(); barrier=threading.Barrier(2); results=[]
    def upload(env):
        conn=connect(path); barrier.wait()
        try: action(conn,{"op":"replica_upload_many","envelopes":[env]},a["token"]); results.append("ok")
        except ValueError as error: results.append(str(error))
        finally: conn.close()
    threads=[threading.Thread(target=upload,args=(env,)) for env in envs]; [thread.start() for thread in threads]; [thread.join() for thread in threads]; db=connect(path); assert results.count("ok")==1 and sum("quota" in result for result in results)==1 and db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1


def test_response_byte_bound_stops_before_materializing_rest():
    seen=[]
    def values():
        for value in ("aaa","bbb","should-not-be-read"): seen.append(value); yield value
    assert bounded(values(),len,5)==["aaa"] and seen==["aaa","bbb"]


def test_blob_replica_is_raw_bounded_repairable_and_history_scoped(tmp_path,monkeypatch):
    db=connect(tmp_path/"server.db"); a,b=account(db,"alice"),account(db,"bob"); ws,k1,k2="team",bytes([1])*32,bytes([2])*32; state=create_ws(db,a,ws,k1,"team"); env=seal_blob(b"body",ws,1,k1,a["device"]["id"]); state=rotate_ws(db,a,state,k2,((a,"admin"),(b,"member"))); ack=action(db,{"op":"blob_upload","envelope":env},a["token"]); assert db.execute("SELECT LENGTH(ciphertext) FROM blob_replicas").fetchone()[0]==20 and action(db,{"op":"blob_pull","workspace":ws},b["token"])["blobs"]==[]
    state=history_ws(a,state,b["user"]); action(db,sign_control(a["device"],{"op":"grant_all","workspace":ws,"user":b["user"],"control":state,"envelopes":{"1":{b["device"]["id"]:seal_key(k1,b["device"]["box_public"],f"workspace:{ws}:epoch:1")},"2":{b["device"]["id"]:seal_key(k2,b["device"]["box_public"],f"workspace:{ws}:epoch:2")}}}),a["token"]); pulled=action(db,{"op":"blob_pull","workspace":ws},b["token"])["blobs"][0]; assert pulled["cursor"]==ack["cursor"] and open_blob(pulled["envelope"],k1)[0]==b"body"
    monkeypatch.setattr(server_module,"BLOB_QUOTA",1)
    with pytest.raises(ValueError,match="quota"): action(db,{"op":"blob_upload","envelope":env},a["token"])


def test_team_add_default_history_grant_remove_and_rotation(tmp_path):
    db = connect(tmp_path/"server.db"); a, b = account(db,"alice"), account(db,"bob"); ws = "team"; k1,k2,k3 = bytes([1])*32,bytes([2])*32,bytes([3])*32
    state=create_ws(db,a,ws,k1,"team")
    old = seal_event(event(a["device"],1,"message.record","old",{"content":"before bob"},[],"2026-01-01T00:00:00Z"),ws,1,k1); action(db,{"op":"upload","envelope":old},a["token"]); row=logical_row("messages",identity="old",state="deleted"); replica=seal_replica(row,row_proof(a["device"],a["user"],ws,1,row),ws,1,k1,a["device"]["id"])
    state=rotate_ws(db,a,state,k2,((a,"admin"),(b,"member")))
    action(db,{"op":"replica_upload_many","envelopes":[replica]},a["token"]); assert action(db,{"op":"pull","workspace":ws,"after":0},b["token"])["events"] == [] and action(db,{"op":"replica_pull","workspace":ws,"after":0},b["token"])["replicas"]==[] and action(db,{"op":"replica_reconcile","workspace":ws,"replicas":[replica["replica"]]},b["token"])["present"]=={}
    current = seal_event(event(a["device"],2,"message.record","new",{"content":"after bob"},[old["event"]],"2026-01-02T00:00:00Z"),ws,2,k2); action(db,{"op":"upload","envelope":current},a["token"])
    assert [x["envelope"]["event"] for x in action(db,{"op":"pull","workspace":ws,"after":0},b["token"])["events"]] == [current["event"]]
    old_for_b = seal_key(k1,b["device"]["box_public"],f"workspace:{ws}:epoch:1")
    state=history_ws(a,state,b["user"]); action(db,sign_control(a["device"],{"op":"grant_all","workspace":ws,"user":b["user"],"control":state,"envelopes":{"1":{b["device"]["id"]:old_for_b},"2":{b["device"]["id"]:seal_key(k2,b["device"]["box_public"],f"workspace:{ws}:epoch:2")}}}),a["token"])
    assert len(action(db,{"op":"pull","workspace":ws,"after":0},b["token"])["events"]) == 2 and action(db,{"op":"replica_pull","workspace":ws,"after":0},b["token"])["replicas"][0]["envelope"]==replica
    rotate_ws(db,a,state,k3,((a,"admin"),))
    with pytest.raises(PermissionError): action(db,{"op":"pull","workspace":ws,"after":0},b["token"])


def test_rotation_rejects_a_stale_or_invented_signed_history_boundary(tmp_path):
    db=connect(tmp_path/"server.db"); a,b=account(db,"alice"),account(db,"bob"); ws,key="team",bytes([1])*32; state=create_ws(db,a,ws,key,"team"); env=seal_event(event(a["device"],1,"x","x",{},[]),ws,1,key); action(db,{"op":"upload","envelope":env},a["token"])
    with pytest.raises(ValueError,match="history boundary"): rotate_ws(db,a,state,bytes([2])*32,((a,"admin"),(b,"member")),{"epoch":2,"tail":0,"heads":{}})


def test_device_certificate_recovery_and_author_acl(tmp_path):
    db = connect(tmp_path/"server.db"); a = account(db,"alice")
    with pytest.raises(PermissionError,match="signature"): action(db,{"op":"create","workspace":"stolen","kind":"team","envelope":{}},a["token"])
    with pytest.raises(PermissionError,match="signature"): action(db,{"op":"recovery","bundle":{}},a["token"])
    assert action(db,{"op":"recovery_fetch","user":"alice"})["bundle"] == {"ciphertext":"opaque"}
    second = identity("desktop"); registered = register_device(db,"alice",a["root"],second)
    assert registered["device"] == second["id"]
    ws,key = "personal",bytes([5])*32; create_ws(db,a,ws,key,"personal")
    forged = seal_event(event(second,1,"x.future","x",{},[],"2026-01-01T00:00:00Z"),ws,1,key)
    with pytest.raises(PermissionError,match="author"): action(db,{"op":"upload","envelope":forged},a["token"])


def test_large_events_are_manifested_then_fetched(tmp_path):
    db=connect(tmp_path/"server.db"); a=account(db,"alice"); ws,key="personal",bytes([7])*32; create_ws(db,a,ws,key,"personal")
    env=seal_event(event(a["device"],1,"future.large","large",{"blob":"x"*70000},[],"2026-01-01T00:00:00Z"),ws,1,key); action(db,{"op":"upload","envelope":env},a["token"]); item=action(db,{"op":"pull","workspace":ws,"after":0},a["token"])["events"][0]
    assert item["lazy"] and "envelope" not in item and action(db,{"op":"fetch","workspace":ws,"event":env["event"]},a["token"])["envelope"]==env


def test_relay_has_no_memory_specific_purge_ledger(tmp_path):
    db=connect(tmp_path/"server.db"); a=account(db,"alice"); ws,key="personal",bytes([9])*32; create_ws(db,a,ws,key,"personal")
    assert not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_purges'").fetchone()
    with pytest.raises(ValueError,match="unknown operation"): action(db,{"op":"purge","workspace":ws},a["token"])


def test_legacy_purge_relay_requires_clean_cutover(tmp_path):
    path=tmp_path/"legacy.db"; db=sqlite3.connect(path); db.execute("CREATE TABLE events(id TEXT)"); db.execute("CREATE TABLE event_purges(id TEXT)"); db.execute("PRAGMA user_version=1"); db.commit(); db.close()
    with pytest.raises(ValueError,match="fresh relay"): connect(path)


def test_restart_preserves_events_tokens_and_idempotency(tmp_path):
    path=tmp_path/"server.db"; db=connect(path); a=account(db,"alice"); ws,key="personal",bytes([8])*32; create_ws(db,a,ws,key,"personal"); env=seal_event(event(a["device"],1,"x","x",{},[],"2026-01-01T00:00:00Z"),ws,1,key); action(db,{"op":"upload","envelope":env},a["token"]); db.close(); db=connect(path)
    assert action(db,{"op":"upload","envelope":env},a["token"])["created"] is False and action(db,{"op":"pull","workspace":ws,"after":0},a["token"])["events"][0]["envelope"]==env


def test_registration_rejects_public_key_identity_mismatch(tmp_path):
    db=connect(tmp_path/"server.db"); root,device=identity("root"),identity("device"); cert=certificate(root,"not-root-id",device)
    with pytest.raises(ValueError,match="identity id"): action(db,{"op":"register_challenge","root_public":root["sign_public"],"certificate":cert})


def test_registration_requires_fresh_device_key_proof_and_consumes_challenge(tmp_path):
    path=tmp_path/"server.db"; db=connect(path); root,device,attacker=identity("root"),identity("device"),identity("attacker"); cert=certificate(root,root["id"],device); base={"root_public":root["sign_public"],"certificate":cert}; challenge=action(db,{"op":"register_challenge",**base})["challenge"]; proof=registration_proof(device,challenge,root["sign_public"],cert); forged={**registration_proof(attacker,challenge,root["sign_public"],cert),"device":device["id"]}; request={"op":"register","user_name":"alice",**base,"challenge":challenge}
    with pytest.raises(PermissionError,match="registration proof"): action(db,{**request,"proof":forged})
    db.close(); db=connect(path)
    assert action(db,{**request,"proof":proof})["device"]==device["id"]
    db.close(); db=connect(path)
    with pytest.raises(PermissionError,match="already used"): action(db,{**request,"proof":proof})
