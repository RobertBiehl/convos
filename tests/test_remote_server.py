import copy, json, sqlite3, threading, time, tracemalloc
from contextlib import closing

import pytest
import ai_convos_remote_server as server_module
from ai_convos_remote.control import CONTROL_V, proposal, record, sign, state_hash, verify_state, vote
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
    assert first["created"] and not same["created"] and not same["replaced"] and changed["replaced"] and first["cursor"]==same["cursor"]==changed["cursor"] and page["replicas"]==[{"cursor":first["cursor"],"envelope":replacement}] and present=={"present":{env["replica"]:first["cursor"]}} and missing=={"present":{}} and db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1
    assert db.execute("SELECT bytes FROM replica_usage").fetchone()[0]==db.execute("SELECT LENGTH(CAST(envelope AS BLOB))+LENGTH(ciphertext) FROM row_replicas").fetchone()[0]<len(server_module.canon(replacement))
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


@pytest.mark.parametrize('approval',['self_approve','quorum_approve','personal_recover'])
def test_device_approval_preserves_existing_authorization_records(tmp_path,approval):
    with closing(connect(tmp_path/'server.db')) as db:
        alice,bob,carol=account(db,'alice'),account(db,'bob'),account(db,'carol')
        base=create_ws(db,alice,'w',bytes(32),'personal' if approval=='personal_recover' else 'team')
        if approval!='personal_recover': base=rotate_ws(db,alice,base,bytes([1])*32,((alice,'admin'),(bob,'member'),(carol,'member')))
        def advance(actor,action,devices,removed=None,approval=None):
            return sign(actor['device'],{'v':1,'kind':'workspace.state','workspace':'w','scope':base['scope'],'revision':base['revision']+1,'prev':state_hash(base),'epoch':base['epoch']+1,'boundary':{'epoch':base['epoch']+1,**ledger_state(db,'w')},'key_commitment':digest(bytes([2])*32),'members':base['members'],'devices':devices,'removed':base['removed'] if removed is None else removed,'action':action,'approval':approval,'approved_at':time.time()})
        def rotate_request(actor,state,history=None):
            envelopes={key:seal_key(bytes([2])*32,entry['device']['box_public'],f'workspace:w:epoch:{state["epoch"]}') for key,entry in state['devices'].items()}
            return sign_control(actor['device'],{'op':'rotate','workspace':'w','control':state,'envelopes':envelopes,'history_envelopes':history or {}})
        if approval=='quorum_approve':
            removal=advance(alice,'remove',{key:value for key,value in base['devices'].items() if key!=bob['device']['id']},[bob['device']['id']])
            action(db,rotate_request(alice,removal),alice['token']); base=removal
        owner=alice if approval=='personal_recover' else bob
        target=identity('second-device'); registered=register_device(db,owner['user'],owner['root'],target)
        entry=record(owner['user'],owner['root']['sign_public'],target,certificate(owner['root'],owner['user'],target),False)
        request=proposal(target,'w',base,entry,time.time()+60)
        if approval!='personal_recover': action(db,{'op':'propose','proposal':request},registered['token'])
        author=carol if approval=='quorum_approve' else bob if approval=='self_approve' else {'device':target,'token':registered['token']}
        votes=[vote(person['device'],person['user'],request) for person in (alice,carol)] if approval=='quorum_approve' else []
        devices={**base['devices'],target['id']:{**entry,'history':approval!='quorum_approve'}}
        clean=advance(author,approval,devices,approval={'proposal':request,'votes':votes})
        history={target['id']:{str(epoch):seal_key(bytes([1])*32,target['box_public'],f'workspace:w:epoch:{epoch}') for epoch in range(base['members'][owner['user']]['history_from'],clean['epoch'])}} if approval=='self_approve' else {}
        changed=copy.deepcopy(devices); changed[alice['device']['id']]['history']=False
        corrupted=advance(author,approval,changed,approval={'proposal':request,'votes':votes})
        with pytest.raises(ValueError,match='preserve existing devices'): verify_state(corrupted,base)
        with pytest.raises(ValueError,match='preserve existing devices'): action(db,rotate_request(author,corrupted,history),author['token'])
        assert action(db,{'op':'state'},alice['token'])['workspaces'][0]['controls'][-1]==base
        assert verify_state(clean,base)==clean
        action(db,rotate_request(author,clean,history),author['token'])
        stored=action(db,{'op':'state'},alice['token'])['workspaces'][0]['controls'][-1]
        assert stored==clean and all(stored['devices'][key]==value for key,value in base['devices'].items())


def test_device_removal_cannot_approve_a_replacement(tmp_path):
    with closing(connect(tmp_path/'server.db')) as db:
        alice,bob=account(db,'alice'),account(db,'bob')
        base=create_ws(db,alice,'w',bytes(32),'team'); base=rotate_ws(db,alice,base,bytes([1])*32,((alice,'admin'),(bob,'member')))
        target=identity('unapproved-bob'); register_device(db,'bob',bob['root'],target)
        entry=record(bob['user'],bob['root']['sign_public'],target,certificate(bob['root'],bob['user'],target))
        def removal(devices):
            return sign(alice['device'],{'v':1,'kind':'workspace.state','workspace':'w','scope':'team','revision':base['revision']+1,'prev':state_hash(base),'epoch':base['epoch']+1,'boundary':{'epoch':base['epoch']+1,**ledger_state(db,'w')},'key_commitment':digest(bytes([2])*32),'members':base['members'],'devices':devices,'removed':[bob['device']['id']],'action':'remove','approval':None,'approved_at':time.time()})
        def request(state):
            envelopes={key:seal_key(bytes([2])*32,value['device']['box_public'],f'workspace:w:epoch:{state["epoch"]}') for key,value in state['devices'].items()}
            return sign_control(alice['device'],{'op':'rotate','workspace':'w','control':state,'envelopes':envelopes})
        kept={alice['device']['id']:base['devices'][alice['device']['id']]}; corrupted=removal(kept|{target['id']:entry})
        with pytest.raises(ValueError,match='invalid device removal'): verify_state(corrupted,base)
        with pytest.raises(ValueError,match='invalid device removal'): action(db,request(corrupted),alice['token'])
        assert action(db,{'op':'state'},alice['token'])['workspaces'][0]['controls'][-1]==base
        clean=removal(kept); assert verify_state(clean,base)==clean
        action(db,request(clean),alice['token'])
        assert action(db,{'op':'state'},alice['token'])['workspaces'][0]['controls'][-1]==clean


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


def test_failed_event_batch_rolls_back_before_connection_reuse(tmp_path):
    with closing(connect(tmp_path/"server.db")) as db:
        a=account(db,"alice"); ws,key="personal",bytes(32); create_ws(db,a,ws,key,"personal")
        env=seal_event(event(a["device"],1,"message.record","one",{},[]),ws,1,key)
        with pytest.raises(PermissionError): action(db,{"op":"upload_many","envelopes":[env,{**env,"author":"wrong"}]},a["token"])
        assert not db.in_transaction
        action(db,sign_control(a["device"],{"op":"recovery","bundle":{"ciphertext":"updated"}}),a["token"])
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0]==db.execute("SELECT COUNT(*) FROM ledger_cursors").fetchone()[0]==0


def test_event_upload_and_signed_rotation_are_serialized(tmp_path,monkeypatch):
    path=tmp_path/"server.db"
    with closing(connect(path)) as db:
        a=account(db,"alice"); ws,key="personal",bytes(32); state=create_ws(db,a,ws,key,"personal")
        env=seal_event(event(a["device"],1,"message.record","one",{},[]),ws,1,key)
        checked,resume=threading.Event(),threading.Event(); outcomes=[]; real=server_module.digest
        def pause(value):
            if value is env:
                checked.set()
                assert resume.wait(5)
            return real(value)
        def upload():
            with closing(connect(path)) as conn:
                try: outcomes.append(action(conn,{"op":"upload","envelope":env},a["token"]))
                except BaseException as error: outcomes.append(error)
        monkeypatch.setattr(server_module,"digest",pause)
        worker=threading.Thread(target=upload,name="paused-upload"); worker.start()
        try:
            assert checked.wait(5)
            db.execute("PRAGMA busy_timeout=0")
            with pytest.raises(sqlite3.OperationalError,match="locked"): rotate_ws(db,a,state,bytes([1])*32,((a,"admin"),))
            assert not db.in_transaction
        finally:
            resume.set(); worker.join(5)
        assert not worker.is_alive() and len(outcomes)==1 and isinstance(outcomes[0],dict), outcomes
        rotated=rotate_ws(db,a,state,bytes([1])*32,((a,"admin"),))
        assert rotated["boundary"]=={"epoch":2,"tail":outcomes[0]["cursor"],"heads":{a["device"]["id"]:{"seq":1,"event":env["event"]}}}


def test_state_is_one_snapshot_while_a_rotation_commits(tmp_path,monkeypatch):
    path=tmp_path/"server.db"
    with closing(connect(path)) as db,closing(connect(path,False)) as writer:
        a=account(db,"alice"); ws,key="personal",bytes(32); state=create_ws(db,a,ws,key,"personal"); real=server_module.rows; rotations=[]
        def rotate_after_membership(conn,sql,args=()):
            values=real(conn,sql,args)
            if sql.startswith("SELECT w.id") and not rotations: rotations.append(rotate_ws(writer,a,state,bytes([1])*32,((a,"admin"),)))
            return values
        monkeypatch.setattr(server_module,"rows",rotate_after_membership)
        snapshot=action(db,{"op":"state"},a["token"])["workspaces"][0]
        assert rotations and snapshot["epoch"]==snapshot["controls"][-1]["epoch"]==max(k["epoch"] for k in snapshot["keys"])==1
        assert not db.in_transaction and action(db,{"op":"state"},a["token"])["workspaces"][0]["epoch"]==2


def test_request_connection_reads_during_writer_and_does_not_create_missing_db(tmp_path):
    path=tmp_path/"relay #1.db"
    with closing(connect(path)) as writer:
        a=account(writer,"alice")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE users SET recovery='{}'")
        with closing(connect(path,False)) as reader:
            assert reader.execute("PRAGMA foreign_keys").fetchone()[0]==reader.execute("PRAGMA secure_delete").fetchone()[0]==1
            assert reader.execute("PRAGMA busy_timeout").fetchone()[0]==30000
            assert action(reader,{"op":"recovery_fetch","user":a["user"]})=={"bundle":{"ciphertext":"opaque"}}
        writer.rollback()
    with pytest.raises(sqlite3.OperationalError): connect(tmp_path/"missing.db",False)
    assert not (tmp_path/"missing.db").exists()


def test_concurrent_event_retries_keep_one_cursor(tmp_path):
    path=tmp_path/"server.db"
    with closing(connect(path)) as db:
        a=account(db,"alice"); ws,key="personal",bytes(32); create_ws(db,a,ws,key,"personal")
        env=seal_event(event(a["device"],1,"message.record","one",{},[]),ws,1,key)
        barrier=threading.Barrier(4); outcomes=[]
        def upload():
            with closing(connect(path,False)) as conn:
                barrier.wait(5)
                try: outcomes.append(action(conn,{"op":"upload","envelope":env},a["token"]))
                except BaseException as error: outcomes.append(error)
        workers=[threading.Thread(target=upload) for _ in range(4)]
        for worker in workers: worker.start()
        for worker in workers: worker.join(5)
        assert len(outcomes)==4 and all(isinstance(value,dict) for value in outcomes), outcomes
        assert sum(value["created"] for value in outcomes)==1 and len({value["cursor"] for value in outcomes})==1
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0]==db.execute("SELECT COUNT(*) FROM ledger_cursors").fetchone()[0]==1


def test_ledger_heads_use_each_authors_sequence_and_workspace_tail(tmp_path):
    with closing(connect(tmp_path/"server.db")) as db:
        values=[(ws,f"event-{i}",author,seq) for i,(ws,author,seq) in enumerate((("w","a",7),("w","b",2),("w","a",1),("other","a",100),("w","b",3),("w","b",1)))]
        db.executemany("INSERT INTO events(workspace,event,author,seq) VALUES (?,?,?,?)",values)
        assert ledger_state(db,"w")=={"tail":6,"heads":{"a":{"seq":7,"event":"event-0"},"b":{"seq":3,"event":"event-4"}}}
        assert ledger_state(db,"absent")=={"tail":0,"heads":{}}


def test_lazy_event_page_does_not_materialize_large_bodies(tmp_path):
    with closing(connect(tmp_path/"server.db")) as db:
        a=account(db,"alice"); ws,key="personal",bytes(32); create_ws(db,a,ws,key,"personal")
        envelopes=[seal_event(event(a["device"],i+1,"future.large",str(i),{"body":"x"*1024**2},[]),ws,1,key) for i in range(8)]
        action(db,{"op":"upload_many","envelopes":envelopes},a["token"])
        tracemalloc.start()
        try:
            result=action(db,{"op":"pull","workspace":ws},a["token"])
            peak=tracemalloc.get_traced_memory()[1]
        finally: tracemalloc.stop()
        assert len(result["events"])==8 and all(value["lazy"] and "envelope" not in value for value in result["events"])
        assert peak<2*1024**2
        assert action(db,{"op":"fetch","workspace":ws,"event":envelopes[-1]["event"]},a["token"])["envelope"]==envelopes[-1]
