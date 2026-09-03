import json, os, subprocess
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
import ai_convos.cli as core_module
from ai_convos.cli import ARCHIVE_COLUMNS, capture_provenance, init_schema, project_attachment_body, project_row_proof, provenance_digest, repository
import ai_convos_remote as remote_client
import ai_convos_remote.projection as projection_module
from ai_convos_remote import edit_evidence_accept, edit_evidence_bridge, edit_evidence_records, promote_paths, provider_alias_accept, provider_alias_bridge, provider_alias_id, provider_alias_records, publish, sharing_routes
from ai_convos_remote.projection import apply_row_replicas, attest_rows, audit_rows, blob_replicas, bridge_replicas, bridge_stamp, bridges, connect, cutover_state, event_support, foreign_id, inspect_state, project, project_many, reconcile_provider_aliases, relocate_attachments, row_replicas, scan, sequence, sharing
from ai_convos_remote.protocol import b64, certificate, digest, event, identity, logical_row, open_blob, open_replica, public, public_id, row_proof, seal_replica, semantic_proof


def git(path,*args): return subprocess.run(("git","-C",str(path),*args),check=True,capture_output=True).stdout.decode().strip()
def source(tmp_path):
    repo=tmp_path/"repo"; repo.mkdir(); git(repo,"init","-q"); git(repo,"config","user.email","a@b.c"); git(repo,"config","user.name","A"); (repo/"a.py").write_text("new\n"); git(repo,"add","."); git(repo,"commit","-qm","init")
    path=tmp_path/"source.db"; db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','title','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(repo)]); db.execute("INSERT INTO messages VALUES ('u','c','user','change it',NULL,'2026-01-01 00:00:00','m','{}',NULL,NULL),('m','c','assistant','done',NULL,'2026-01-01 00:00:01','m','{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','new\n','2026-01-01 00:00:01',NULL)",[str(repo/'a.py')]); db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL)"); db.close(); capture_provenance(path); return repo,duckdb.connect(str(path))

def signed_edit_graph():
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}
    rows={"conversations":logical_row("conversations",ARCHIVE_COLUMNS["conversations"],["c","codex","title","2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]),"messages":logical_row("messages",ARCHIVE_COLUMNS["messages"],["m","c","assistant","done",None,"2026-01-01T00:00:00",None,"{}",None]),"tool_calls":logical_row("tool_calls",ARCHIVE_COLUMNS["tool_calls"],["t","m","write","{}","{}","complete",None,"2026-01-01T00:00:00"]),"file_edits":logical_row("file_edits",ARCHIVE_COLUMNS["file_edits"],["e","m","a.py","write","one","2026-01-01T00:00:00",None])}
    proofs={kind:row_proof(device,user,"w",1,row) for kind,row in rows.items()}; bodies=[{"row":rows[k],"proof":proofs[k]} for k in rows]; evidence=lambda edit,tool,status="confirmed",reason="provider_success":{"v":1,"kind":"file-edit.evidence","id":"file-edit-evidence:"+core_module.provenance_digest("e"),"state":"active","data":{"edit":"e","edit_revision":edit,"status":status,"reason":reason,"tool_call":"t","tool_revision":tool}}
    return root,device,user,control,rows,proofs,bodies,evidence


def test_personal_scan_strips_local_roots_and_projects_duckdb(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); raw=json.dumps(records)
    assert str(repo) not in raw and len(records)>3; root,device=identity("root"),identity("remote"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}; core.close(); attest_rows(tmp_path/"source.db",cfg,"personal",records)
    def inventory(_):
        with core_module.open_db(tmp_path/"source.db",wait=0) as writer: writer.execute("UPDATE conversations SET title='title'")
        return []
    envs=row_replicas(tmp_path/"source.db",cfg,"personal",records,{1:bytes(range(32))},inventory=inventory); apply_row_replicas(tmp_path/"target.db",[open_replica(e,bytes(range(32))) for e in envs],"personal",[control])
    target=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert target.execute("SELECT title,cwd FROM conversations").fetchone()==("title",None); assert target.execute("SELECT content FROM messages WHERE role='user'").fetchone()[0]=="change it"; assert target.execute("SELECT file_path FROM file_edits").fetchone()[0]=="a.py" and target.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone()==("unverified","signed_replica_missing_evidence"); before=target.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]; assert target.execute("SELECT x.file_edit_id=fe.id FROM provenance.file_edit_files x JOIN file_edits fe ON fe.id=x.file_edit_id").fetchone()[0]; assert target.execute("SELECT COUNT(*) FROM remote.provenance_origins").fetchone()[0]>=before; target.close()
    fresh=connect(tmp_path/"fresh-state.db"); imported=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert scan(imported,fresh)==[]; imported.close(); fresh.close()
    old={"raw_events","repositories","files","file_versions","changesets","edits","changeset_repositories","checkpoints","checkpoint_changesets","assertions","gaps","boundaries"}; assert not old&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert not {"event_log","history_material","history_outbox","history_queue","sharing_boundaries","attachment_chunks","imported_rows"}&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}

def test_signed_edit_evidence_preserves_status_and_tool_binding_across_replica(tmp_path):
    sender=tmp_path/"sender"; path=sender/"data/convos.db"; path.parent.mkdir(parents=True); repo,source_db=source(tmp_path); source_db.execute("INSERT INTO tool_calls VALUES ('t','m','write','{}','{}','complete',NULL,'2026-01-01 00:00:01'); UPDATE provenance.file_edit_evidence SET tool_call_id='t' WHERE file_edit_id='e'"); source_db.close(); (tmp_path/"source.db").replace(path)
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:{"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}}}; cfg={"user":user,"root":root,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}
    (sender/"remote").mkdir(); (sender/"remote/config.json").write_text(json.dumps({"user":user})); core=duckdb.connect(str(path)); state=connect(tmp_path/"state.db"); records=scan(core,state); core.close(); state.close(); attest_rows(path,cfg,"personal",records); pending=edit_evidence_records(sender,user,"personal","personal"); assert len(pending)==1 and pending[0]["row"]["data"]["status"]=="confirmed" and pending[0]["row"]["data"]["tool_call"]=="t"; proof=semantic_proof(root,user,device["id"],"personal",1,pending[0]["row"]); assert edit_evidence_accept(sender,pending[0]["row"],proof,False)
    key_=bytes(range(32)); rows=[open_replica(e,key_) for e in row_replicas(path,cfg,"personal",records,{1:key_})]; semantic=open_replica(seal_replica(pending[0]["row"],proof,"personal",1,key_,device["id"]),key_); receiver=tmp_path/"receiver"; (receiver/"remote").mkdir(parents=True); (receiver/"remote/config.json").write_text(json.dumps({"user":"receiver"})); attacker,attacker_device=identity("attacker"),identity("attacker-device"); unauthorized=semantic_proof(attacker,public_id(attacker["sign_public"]),attacker_device["id"],"personal",1,pending[0]["row"])
    with pytest.raises(ValueError,match="authorization unavailable"): apply_row_replicas(receiver/"data/convos.db",[open_replica(seal_replica(pending[0]["row"],unauthorized,"personal",1,key_,attacker_device["id"]),key_)],"personal",[control],local_user="receiver",root=receiver)
    apply_row_replicas(receiver/"data/convos.db",[semantic,*rows],"personal",[control],local_user="receiver",root=receiver)
    target=duckdb.connect(str(receiver/"data/convos.db"),read_only=True); assert target.execute("SELECT v.status,v.reason,v.tool_call_id=t.id FROM provenance.file_edit_evidence v JOIN tool_calls t ON t.id=v.tool_call_id").fetchone()==("confirmed","test_fixture",True); target.close()
    core=duckdb.connect(str(path)); core.execute("UPDATE provenance.file_edit_evidence SET status='invalid',reason='provider_failure' WHERE file_edit_id='e'"); core.close(); update=next(v for v in edit_evidence_records(sender,user,"personal","personal") if v["proof"] is None); revised=semantic_proof(root,user,device["id"],"personal",1,update["row"],update["previous"]); semantic=open_replica(seal_replica(update["row"],revised,"personal",1,key_,device["id"]),key_); apply_row_replicas(receiver/"data/convos.db",[semantic],"personal",[control],local_user="receiver",root=receiver)
    target=duckdb.connect(str(receiver/"data/convos.db"),read_only=True); assert target.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone()==("invalid","provider_failure") and target.execute("SELECT count(*) FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed'").fetchone()[0]==0; target.close()

def test_edit_evidence_inventory_is_set_based_not_per_edit(tmp_path,monkeypatch):
    root=tmp_path/"archive"; path=root/"data/convos.db"; db=core_module.open_db(path,purpose="fixture"); init_schema(db); rows=[(f"p{i}","w","w","file_edits",f"e{i}",1,"h",f"{i:064x}",None,"active","u","d",1,"s") for i in range(40)]; db.executemany("INSERT INTO remote.row_proofs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",rows); db.executemany("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','test',NULL)",[(f"e{i}",) for i in range(40)]); db.close(); opened=[]; real=remote_client._core
    @contextmanager
    def counted(*args,purpose,**kwargs):
        opened.append(purpose)
        with real(*args,purpose=purpose,**kwargs) as db: yield db
    monkeypatch.setattr(remote_client,"_core",counted); assert len(edit_evidence_records(root,"u","w","personal"))==40 and opened.count("remote.edit_evidence.read")==1

def test_signed_edit_evidence_tracks_exact_row_heads_reorder_tombstones_and_forks(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); archive=tmp_path/"archive"; path=archive/"data/convos.db"; apply=lambda values:apply_row_replicas(path,values,"w",[control],local_user="receiver",root=archive); state=lambda:duckdb.connect(str(path),read_only=True).execute("SELECT status,reason,tool_call_id IS NOT NULL FROM provenance.file_edit_evidence").fetchone()
    assert apply(bodies)==[True]*4 and state()==("unverified","signed_replica_missing_evidence",False)
    s1=semantic_proof(root,user,device["id"],"w",1,evidence(proofs["file_edits"]["revision"],proofs["tool_calls"]["revision"])); assert apply([{"row":evidence(proofs["file_edits"]["revision"],proofs["tool_calls"]["revision"]),"proof":s1}])==[True] and state()==("confirmed","provider_success",True)
    edit2=logical_row("file_edits",ARCHIVE_COLUMNS["file_edits"],["e","m","a.py","write","two","2026-01-02",None]); e2=row_proof(device,user,"w",1,edit2,proofs["file_edits"]["revision"]); apply([{"row":edit2,"proof":e2}]); assert state()==("unverified","signed_replica_missing_evidence",False)
    row2=evidence(e2["revision"],proofs["tool_calls"]["revision"]); s2=semantic_proof(root,user,device["id"],"w",1,row2,s1); apply([{"row":row2,"proof":s2}]); assert state()==("confirmed","provider_success",True)
    tool2=logical_row("tool_calls",ARCHIVE_COLUMNS["tool_calls"],["t","m","write","{}",'{"ok":true}',"complete",None,"2026-01-02"]); t2=row_proof(device,user,"w",1,tool2,proofs["tool_calls"]["revision"]); apply([{"row":tool2,"proof":t2}]); assert state()==("unverified","signed_replica_missing_evidence",False)
    row3=evidence(e2["revision"],t2["revision"]); s3=semantic_proof(root,user,device["id"],"w",1,row3,s2); apply([{"row":row3,"proof":s3}]); assert state()==("confirmed","provider_success",True)
    deleted=logical_row("file_edits",identity="e",state="deleted"); tomb=row_proof(device,user,"w",1,deleted,e2["revision"]); apply([{"row":deleted,"proof":tomb}]); db=duckdb.connect(str(path),read_only=True); assert not db.execute("SELECT 1 FROM file_edits").fetchone() and not db.execute("SELECT 1 FROM provenance.file_edit_evidence").fetchone(); db.close()
    edit3=logical_row("file_edits",ARCHIVE_COLUMNS["file_edits"],["e","m","a.py","write","three","2026-01-03",None]); e3=row_proof(device,user,"w",1,edit3,tomb["revision"]); apply([{"row":edit3,"proof":e3}]); assert state()==("unverified","signed_replica_missing_evidence",False)
    row4=evidence(e3["revision"],t2["revision"]); s4=semantic_proof(root,user,device["id"],"w",1,row4,s3); apply([{"row":row4,"proof":s4}]); assert state()==("confirmed","provider_success",True)
    edit4=logical_row("file_edits",ARCHIVE_COLUMNS["file_edits"],["e","m","a.py","write","four","2026-01-04",None]); e4=row_proof(device,user,"w",1,edit4,e3["revision"]); row5=evidence(e4["revision"],t2["revision"]); s5=semantic_proof(root,user,device["id"],"w",1,row5,s4); apply([{"row":row5,"proof":s5}]); assert state()==("unverified","signed_replica_missing_evidence",False); apply([{"row":edit4,"proof":e4}]); assert state()==("confirmed","provider_success",True)
    branches=[]
    for status,reason in (("confirmed","branch-a"),("invalid","branch-b"),("unknown","branch-c")):
        row=evidence(e4["revision"],t2["revision"],status,reason); branches.append((row,semantic_proof(root,user,device["id"],"w",1,row,s5)))
    apply([{"row":row,"proof":proof} for row,proof in branches]); assert state()==("unverified","signed_evidence_conflict",False)
    merged=evidence(e4["revision"],t2["revision"]); merge=semantic_proof(root,user,device["id"],"w",1,merged,[proof for row,proof in branches]); assert apply([{"row":merged,"proof":merge}])==[True] and state()==("confirmed","provider_success",True); assert apply([{"row":merged,"proof":merge}])==[True] and state()==("confirmed","provider_success",True)
    db=duckdb.connect(str(path),read_only=True); leaves=db.execute("SELECT count(*) FROM remote.file_edit_evidence_proofs p WHERE NOT EXISTS (SELECT 1 FROM remote.semantic_ancestors a WHERE a.object_kind='file-edit.evidence' AND a.ancestor_revision=p.revision)").fetchone()[0]; assert leaves==1 and db.execute("SELECT count(*) FROM remote.semantic_ancestors WHERE child_revision=?",(merge["revision"],)).fetchone()[0]>=len(branches); db.close()

def test_signed_evidence_never_binds_to_native_rows_by_source_id(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); archive=tmp_path/"native"; path=archive/"data/convos.db"; apply_row_replicas(path,bodies,"w",[control],recover="native",local_user=user,root=archive); row=evidence(proofs["file_edits"]["revision"],proofs["tool_calls"]["revision"]); proof=semantic_proof(root,user,device["id"],"w",1,row); apply_row_replicas(path,[{"row":row,"proof":proof}],"w",[control],recover="native",local_user=user,root=archive); db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT status,reason,tool_call_id FROM provenance.file_edit_evidence").fetchone()==("unverified","signed_replica_missing_evidence",None) and not db.execute("SELECT 1 FROM remote.row_origins WHERE table_name='file_edits'").fetchone(); db.close()

def test_retained_foreign_semantic_proof_is_reuploadable_after_author_departure(tmp_path,monkeypatch):
    root_a,device_a,user_a,control1,rows,proofs,bodies,evidence=signed_edit_graph(); holder=tmp_path/"holder"; path=holder/"data/convos.db"; apply_row_replicas(path,bodies,"w",[control1],local_user="holder",root=holder); row=evidence(proofs["file_edits"]["revision"],proofs["tool_calls"]["revision"]); proof=semantic_proof(root_a,user_a,device_a["id"],"w",1,row); apply_row_replicas(path,[{"row":row,"proof":proof}],"w",[control1],local_user="holder",root=holder)
    root_b,device_b=identity("root-b"),identity("device-b"); user_b=public_id(root_b["sign_public"]); root_c,device_c=identity("root-c"),identity("device-c"); user_c=public_id(root_c["sign_public"]); members={d["id"]:{"user":u,"root_public":r["sign_public"],"device":public(d),"certificate":certificate(r,u,d),"history":True} for r,d,u in ((root_b,device_b,user_b),(root_c,device_c,user_c))}; control2={"workspace":"w","revision":2,"epoch":2,"devices":members}; cfg={"user":user_b,"root":root_b,"device":device_b,"workspaces":{"w":{"kind":"personal","epoch":2}}}; monkeypatch.setattr(projection_module,"bridges",lambda:[edit_evidence_bridge()]); key_=bytes(range(32)); envelopes=bridge_replicas(holder,cfg,"w","personal",key_); assert len(envelopes)==1 and envelopes[0]["uploader"]==device_b["id"]
    repaired=open_replica(envelopes[0],key_); assert repaired["proof"]==proof and repaired["row"]==row
    fresh=tmp_path/"fresh"; fresh_path=fresh/"data/convos.db"; apply_row_replicas(fresh_path,bodies,"w",[control1,control2],local_user=user_c,root=fresh); apply_row_replicas(fresh_path,[repaired],"w",[control1,control2],local_user=user_c,root=fresh); db=duckdb.connect(str(fresh_path),read_only=True); assert db.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone()==("confirmed","provider_success"); db.close()

def test_semantic_data_changes_do_not_reset_the_replica_cursor(tmp_path,monkeypatch):
    archive=tmp_path/"archive"; path=archive/"data/convos.db"; path.parent.mkdir(parents=True); db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}'); INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','assistant','{}'); INSERT INTO file_edits VALUES ('e','m','a.py','write','one',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','first',NULL)"); db.close(); stamp=bridge_stamp(archive); db=duckdb.connect(str(path)); db.execute("UPDATE provenance.file_edit_evidence SET reason='second'"); db.close(); assert bridge_stamp(archive)==stamp
    state=connect(tmp_path/"state.db"); state.execute("INSERT INTO meta VALUES ('replica_cursor:w','941'),('replica_projection:w',?)",(stamp,)); state.commit(); requests=[]; monkeypatch.setattr(remote_client,"request",lambda cfg,payload:requests.append(payload) or {"floor":0,"tail":941,"replicas":[]}); assert remote_client.pull_row_replicas({"user":"receiver"},state,archive,{"id":"w","controls":[],"keys":[]})==0 and requests==[{"op":"replica_pull","workspace":"w","after":941,"limit":500,"semantic":True}]; state.close()

def test_personal_retains_but_team_excludes_unconfirmed_edits(tmp_path):
    repo,core=source(tmp_path); core.execute("INSERT INTO file_edits VALUES ('bad','m',?,'write','bad','2026-01-01 00:00:02',NULL)",[str(repo/'bad.py')]); core.execute("INSERT INTO provenance.file_edit_evidence VALUES ('bad','invalid','provider_failure',NULL)"); state=connect(tmp_path/"state.db"); personal=scan(core,state); rid=next(r["payload"]["id"] for r in personal if r["kind"]=="repository.observed"); team=scan(core,state,"team",[rid],[])
    assert {r["payload"]["row"][0] for r in personal if r["kind"]=="file_edit.record"}=={"e","bad"} and {r["payload"]["row"][0] for r in team if r["kind"]=="file_edit.record"}=={"e"}


def test_old_state_inspection_is_read_only_and_cutover_preserves_exact_backup(tmp_path):
    path=tmp_path/"state.db"; db=__import__("sqlite3").connect(path); db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)"); db.execute("CREATE TABLE legacy_payload(value TEXT)"); db.execute("INSERT INTO meta VALUES ('state_schema','2')"); db.execute("INSERT INTO legacy_payload VALUES ('only in old state')"); db.commit(); db.close(); before=(path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in tmp_path.iterdir()})
    assert inspect_state(path)["status"]=="incompatible"
    with pytest.raises(ValueError,match="rebuild required"): connect(path)
    assert (path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in tmp_path.iterdir()})==before
    report=cutover_state(path); backup=Path(report["backup"]); old=__import__("sqlite3").connect(backup/"state.db"); assert old.execute("SELECT value FROM legacy_payload").fetchone()[0]=="only in old state"; old.close()
    state=connect(path); assert state.execute("SELECT value FROM meta WHERE key='state_schema'").fetchone()[0]=="4" and json.loads(state.execute("SELECT value FROM meta WHERE key='state_cutover'").fetchone()[0])["backup"]==str(backup); state.close(); assert inspect_state(path)["status"]=="current" and os.stat(backup).st_mode&0o777==0o700 and os.stat(backup/"state.db").st_mode&0o777==0o600


def test_known_state_upgrade_preserves_replica_anchors_and_replays_policy_events(tmp_path):
    path=tmp_path/"state.db"; state=connect(path); state.executescript("INSERT INTO replica_receipts VALUES ('w','r',1,7); INSERT INTO blob_receipts VALUES ('w','b',1,8); INSERT INTO receipts VALUES ('w','event',9,'device',1,1,'workspace.policy',1,'policy','revision',NULL); INSERT INTO publication_heads VALUES ('w','user','policy','revision','event'); INSERT INTO cursors VALUES ('w',9); INSERT INTO sync_states VALUES ('w','ready',9,1,NULL); INSERT INTO meta VALUES ('replica_cursor:w','7'),('blob_cursor:w','8'),('replica_projection:w','stamp'),('boundary:w','boundary'),('core_generation:w','99'); DROP TABLE policy_proofs; DROP TABLE sharing_preferences; DROP TABLE policies; CREATE TABLE policies(workspace TEXT,kind TEXT,value TEXT,PRIMARY KEY(workspace,kind,value)) WITHOUT ROWID; UPDATE meta SET value='1' WHERE key='state_schema';"); state.commit(); state.close(); report=cutover_state(path); state=connect(path)
    assert json.loads(state.execute("SELECT value FROM meta WHERE key='state_cutover'").fetchone()[0])["preserved"] and tuple(state.execute("SELECT replica,cursor FROM replica_receipts").fetchone())==("r",7) and tuple(state.execute("SELECT blob,cursor FROM blob_receipts").fetchone())==("b",8) and state.execute("SELECT cursor FROM receipts").fetchone()[0]==9 and state.execute("SELECT lifecycle FROM sync_states").fetchone()[0]=="ready" and state.execute("SELECT value FROM meta WHERE key='replica_cursor:w'").fetchone()[0]=="7" and not state.execute("SELECT 1 FROM cursors").fetchone() and not state.execute("SELECT 1 FROM policies").fetchone() and not state.execute("SELECT 1 FROM meta WHERE key='core_generation:w'").fetchone(); state.close(); assert Path(report["backup"]).is_dir()


def test_cutover_recovers_corrupt_regular_state_but_refuses_symlink(tmp_path):
    path=tmp_path/"state.db"; path.write_bytes(b"corrupt but preserved"); report=cutover_state(path); assert (Path(report["backup"])/"state.db").read_bytes()==b"corrupt but preserved" and inspect_state(path)["status"]=="current"
    target=tmp_path/"target.db"; target.write_bytes(b"do not touch"); link=tmp_path/"link.db"; link.symlink_to(target)
    with pytest.raises(ValueError,match="cannot be rebuilt"): cutover_state(link)
    assert target.read_bytes()==b"do not touch"


def test_cutover_install_failure_keeps_old_state_and_verified_backup(tmp_path,monkeypatch):
    path=tmp_path/"state.db"; db=__import__("sqlite3").connect(path); db.execute("CREATE TABLE legacy(value TEXT)"); db.execute("INSERT INTO legacy VALUES ('still here')"); db.commit(); db.close(); original=projection_module.os.replace; failed=[False]
    def replace(source,target):
        if Path(target)==path and not failed[0]: failed[0]=True; raise OSError("install failed")
        return original(source,target)
    monkeypatch.setattr(projection_module.os,"replace",replace)
    with pytest.raises(OSError,match="install failed"): cutover_state(path)
    old=__import__("sqlite3").connect(path); assert old.execute("SELECT value FROM legacy").fetchone()[0]=="still here"; old.close(); backup=next((tmp_path/"backups").iterdir()); saved=__import__("sqlite3").connect(backup/"state.db"); assert saved.execute("SELECT value FROM legacy").fetchone()[0]=="still here"; saved.close()
    monkeypatch.setattr(projection_module.os,"replace",original); cutover_state(path); assert inspect_state(path)["status"]=="current"


def test_unchanged_provenance_does_not_republish_but_file_change_does(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}
    first=scan(core,state); timed=[r for r in first if r["kind"] in ("git.checkpoint","file.version")]; assert timed and all("observed_at" in r and "observed_at" not in r["payload"] for r in timed); core.close(); assert attest_rows(tmp_path/"source.db",cfg,"personal",first)>4 and attest_rows(tmp_path/"source.db",cfg,"personal",first)==0
    (repo/"a.py").write_text("changed\n"); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db"),read_only=True); changed=scan(core,state); core.close(); assert attest_rows(tmp_path/"source.db",cfg,"personal",changed)==2


def test_row_attestation_survives_state_loss_and_tracks_change_and_reversion(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); core.close(); state.close(); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":cert,"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"w":{"kind":"personal","epoch":1}},"controls":{"w":control},"server_state":{"workspaces":[{"id":"w","controls":[control]}]}}
    assert attest_rows(tmp_path/"source.db",cfg,"w",records)>4 and attest_rows(tmp_path/"source.db",cfg,"w",records)==0; Path(tmp_path/"state.db").unlink(); db=duckdb.connect(str(tmp_path/"source.db")); db.execute("UPDATE conversations SET title='changed'"); db.close(); state=connect(tmp_path/"new-state.db"); core=duckdb.connect(str(tmp_path/"source.db"),read_only=True); changed=scan(core,state); core.close(); assert attest_rows(tmp_path/"source.db",cfg,"w",changed)==1; db=duckdb.connect(str(tmp_path/"source.db")); db.execute("UPDATE conversations SET title='title'"); db.close(); core=duckdb.connect(str(tmp_path/"source.db"),read_only=True); reverted=scan(core,state); core.close(); assert attest_rows(tmp_path/"source.db",cfg,"w",reverted)==1
    db=duckdb.connect(str(tmp_path/"source.db"),read_only=True); rows=db.execute("SELECT content_hash,revision,previous_revision FROM remote.row_proofs WHERE row_kind='conversations' ORDER BY previous_revision NULLS FIRST").fetchall(); assert len(rows)==3 and rows[0][0]==rows[2][0] and len({r[1] for r in rows})==3 and rows[1][2]==rows[0][1] and rows[2][2]==rows[1][1] and db.execute("SELECT COUNT(*) FROM remote.workspace_controls").fetchone()[0]==1; db.close()


def test_refounded_successor_keeps_origin_and_separates_current_authorization(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); core.close(); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":cert,"history":True}; control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg=lambda ws:{"user":user,"device":device,"workspaces":{ws:{"kind":"team","epoch":1}},"controls":{ws:control(ws)},"server_state":{"workspaces":[{"id":ws,"controls":[control(ws)]}]}}; assert attest_rows(tmp_path/"source.db",cfg("origin"),"origin",records)==len(records); db=duckdb.connect(str(tmp_path/"source.db")); old=db.execute("SELECT revision FROM remote.row_proofs WHERE row_kind='conversations'").fetchone()[0]; db.execute("UPDATE conversations SET title='after refound'"); db.close(); core=duckdb.connect(str(tmp_path/"source.db"),read_only=True); changed=scan(core,state); core.close(); assert attest_rows(tmp_path/"source.db",cfg("replacement"),"replacement",changed,{"origin"})==1
    db=duckdb.connect(str(tmp_path/"source.db"),read_only=True); assert db.execute("SELECT workspace_id,authorization_workspace_id,previous_revision FROM remote.row_proofs WHERE previous_revision IS NOT NULL").fetchone()==("origin","replacement",old); db.close()


def test_duplicate_logical_roots_across_workspaces_converge_before_successor(tmp_path):
    repo,core=source(tmp_path)
    state=connect(tmp_path/"state.db")
    records=scan(core,state)
    core.close()
    state.close()
    root,device=identity("root"),identity("device")
    user=public_id(root["sign_public"])
    cert=certificate(root,user,device)
    entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":cert,"history":True}
    control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}
    cfg=lambda ws:{"user":user,"device":device,"workspaces":{ws:{"kind":"team","epoch":1}},"controls":{ws:control(ws)},"server_state":{"workspaces":[{"id":ws,"controls":[control(ws)]}]}}
    assert attest_rows(tmp_path/"source.db",cfg("origin-a"),"origin-a",records)==len(records) and attest_rows(tmp_path/"source.db",cfg("origin-b"),"origin-b",records)==len(records)
    db=duckdb.connect(str(tmp_path/"source.db"))
    db.execute("UPDATE conversations SET title='successor'")
    db.close()
    fresh=connect(tmp_path/"fresh.db")
    core=duckdb.connect(str(tmp_path/"source.db"),read_only=True)
    changed=scan(core,fresh)
    core.close()
    fresh.close()
    current=cfg("replacement")
    current["server_state"]["workspaces"][0]["controls"]=[control("replacement")]
    assert attest_rows(tmp_path/"source.db",current,"replacement",changed,{"origin-a","origin-b"})==1
    envs=row_replicas(tmp_path/"source.db",current,"replacement",changed,{1:bytes(32)},origins={"origin-a","origin-b"},origin_epochs={"origin-a":1,"origin-b":1})
    bodies=[open_replica(env,bytes(32)) for env in envs]
    rows=[body for body in bodies if body["row"]["kind"]=="conversations"]
    assert len(rows)==1 and rows[0]["row"]["data"]["title"]=="successor" and len(rows[0]["lineage"])==1
    assert any(apply_row_replicas(tmp_path/"target.db",bodies,"replacement",[control(ws) for ws in ("origin-a","origin-b","replacement")],local_user=user))
    target=duckdb.connect(str(tmp_path/"target.db"),read_only=True)
    assert target.execute("SELECT title FROM conversations").fetchone()[0]=="successor"
    target.close()


def test_row_attestation_refuses_to_guess_between_concurrent_heads(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); core.close(); state.close(); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":cert,"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"w":{"kind":"personal","epoch":1}},"controls":{"w":control},"server_state":{"workspaces":[{"id":"w","controls":[control]}]}}; attest_rows(tmp_path/"source.db",cfg,"w",records)
    db=duckdb.connect(str(tmp_path/"source.db")); base=db.execute("SELECT revision FROM remote.row_proofs WHERE row_kind='conversations'").fetchone()[0]; row=lambda title:projection_module.logical_row("conversations",["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"],["c","codex",title,"2026-01-01","2026-01-01","m",str(repo),None,None,"{}"]); [project_row_proof(db,projection_module.row_proof(device,user,"w",1,row(title),base),root["sign_public"],cert) for title in ("branch-a","branch-b")]; db.execute("UPDATE conversations SET title='third'"); db.close(); fresh=connect(tmp_path/"fresh.db"); core=duckdb.connect(str(tmp_path/"source.db"),read_only=True); current=scan(core,fresh); core.close(); fresh.close()
    with pytest.raises(ValueError,match="row revision conflict"): attest_rows(tmp_path/"source.db",cfg,"w",current)


def test_row_dag_converges_across_arrival_order_and_preserves_true_fork(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=lambda title:logical_row("conversations",fields,["c","codex",title,"2026-01-01","2026-01-01",None,None,None,None,"{}"]); parent=row_proof(device,user,"w",1,row("parent")); child=row_proof(device,user,"w",1,row("child"),parent["revision"]); body=lambda value,proof:{"row":value,"proof":proof}
    assert apply_row_replicas(tmp_path/"late",[body(row("child"),child)],"w",[control])==[True] and apply_row_replicas(tmp_path/"late",[body(row("parent"),parent)],"w",[control])==[False]; assert duckdb.connect(str(tmp_path/"late"),read_only=True).execute("SELECT title FROM conversations").fetchone()[0]=="child"
    assert apply_row_replicas(tmp_path/"batch",[body(row("child"),child),body(row("parent"),parent)],"w",[control])==[True,False] and duckdb.connect(str(tmp_path/"batch"),read_only=True).execute("SELECT title FROM conversations").fetchone()[0]=="child"
    shared=duckdb.connect(str(tmp_path/"shared")); init_schema(shared); assert apply_row_replicas(tmp_path/"shared",[body(row("parent"),parent)],"w",[control],db=shared)==[True] and apply_row_replicas(tmp_path/"shared",[body(row("child"),child)],"w",[control],db=shared)==[True]; shared.close()
    fork=row_proof(device,user,"w",1,row("fork"),parent["revision"]); apply_row_replicas(tmp_path/"fork",[body(row("parent"),parent)],"w",[control]); assert apply_row_replicas(tmp_path/"fork",[body(row("child"),child),body(row("fork"),fork)],"w",[control])==[False,False]; db=duckdb.connect(str(tmp_path/"fork"),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="parent" and db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==2; db.close()


def test_current_replica_carries_signed_lineage_and_rejects_late_stale_ancestor(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=lambda title:logical_row("conversations",fields,["c","codex",title,"2026-01-01","2026-01-01",None,None,None,None,"{}"]); rows=[row("one"),row("two"),logical_row("conversations",identity="c",state="deleted")]; proofs=[row_proof(device,user,"w",1,rows[0])]; proofs+=[row_proof(device,user,"w",1,rows[1],proofs[0]["revision"])]; proofs+=[row_proof(device,user,"w",1,rows[2],proofs[1]["revision"])]; source=tmp_path/"source"
    [apply_row_replicas(source,[{"row":value,"proof":proof}],"w",[control]) for value,proof in zip(rows,proofs)]; cfg={"user":user,"device":device,"workspaces":{"w":{"kind":"personal"}}}; body=open_replica(row_replicas(source,cfg,"w",[],{1:bytes(32)})[0],bytes(32)); assert [p["revision"] for p in body["lineage"]]==[proofs[1]["revision"],proofs[0]["revision"]]
    target=tmp_path/"target"; assert apply_row_replicas(target,[body],"w",[control])==[True] and apply_row_replicas(target,[{"row":rows[0],"proof":proofs[0],"lineage":[]}],"w",[control])==[False]; db=duckdb.connect(str(target),read_only=True); assert not db.execute("SELECT 1 FROM conversations").fetchone() and db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0]==3 and db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==0; db.close()


def test_row_replica_rejects_incomplete_or_unrelated_signed_lineage(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; parent=logical_row("messages",identity="m",state="deleted"); first=row_proof(device,user,"w",1,parent); child=row_proof(device,user,"w",1,parent,first["revision"]); unrelated=row_proof(device,user,"w",1,logical_row("messages",identity="other",state="deleted"))
    with pytest.raises(ValueError,match="incomplete row proof lineage"): apply_row_replicas(tmp_path/"missing",[{"row":parent,"proof":child,"lineage":[]}],"w",[control])
    with pytest.raises(ValueError,match="invalid row proof lineage"): apply_row_replicas(tmp_path/"unrelated",[{"row":parent,"proof":child,"lineage":[unrelated]}],"w",[control])


def test_native_tombstone_and_author_heads_are_repairable_from_duckdb(tmp_path):
    roots,devices=[identity("root-a"),identity("root-b")],[identity("device-a"),identity("device-b")]; users=[public_id(r["sign_public"]) for r in roots]; entries=[{"user":u,"root_public":r["sign_public"],"device":public(d),"certificate":certificate(r,u,d),"history":True} for r,d,u in zip(roots,devices,users)]; control={"workspace":"w","revision":1,"epoch":1,"devices":{d["id"]:e for d,e in zip(devices,entries)}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; values=["c","codex","kept","2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]; active=logical_row("conversations",fields,values); deleted=logical_row("conversations",identity="c",state="deleted"); pa=row_proof(devices[0],users[0],"w",1,active); tomb=row_proof(devices[0],users[0],"w",1,deleted,pa["revision"]); pb=row_proof(devices[1],users[1],"w",1,active); path=tmp_path/"db"; apply_row_replicas(path,[{"row":active,"proof":pa},{"row":deleted,"proof":tomb},{"row":active,"proof":pb}],"w",[control]); cfg={"user":users[0],"device":devices[0],"workspaces":{"w":{"kind":"personal"}}}; envs=row_replicas(path,cfg,"w",[],{1:bytes(32)}); bodies=[open_replica(e,bytes(32)) for e in envs]
    assert any(b["row"]["state"]=="deleted" and b["proof"]["author_user_id"]==users[0] for b in bodies); cfg["user"],cfg["device"]=users[1],devices[1]; bodies=[open_replica(e,bytes(32)) for e in row_replicas(path,cfg,"w",[dict(kind="conversation.record",entity="conversations:c",payload=dict(table="conversations",columns=fields,row=values))],{1:bytes(32)})]; assert sum(b["row"]["state"]=="active" and b["proof"]["author_user_id"]==users[1] for b in bodies)==1


def test_conflicting_attachment_bodies_remain_repairable(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","message_id","filename","mime_type","size","path","url","created_at","body_hash"]; data=[b"one",b"two"]; hash_=lambda value:__import__("hashlib").sha256(value).hexdigest(); hashes=[hash_(value) for value in data]; row=lambda name,body:logical_row("attachments",fields,["a","m",name,None,len(body),None,None,"2026-01-01T00:00:00",hash_(body)]); base=row("base",b"old"); parent=row_proof(device,user,"w",1,base); children=[row_proof(device,user,"w",1,row(str(i),body),parent["revision"]) for i,body in enumerate(data)]; path=tmp_path/"data/convos.db"; apply_row_replicas(path,[{"row":base,"proof":parent}],"w",[control]); apply_row_replicas(path,[{"row":row(str(i),body),"proof":proof} for i,(body,proof) in enumerate(zip(data,children))],"w",[control]); [project_attachment_body(path,body,body_hash) for body,body_hash in zip(data,hashes)]; cfg={"user":user,"device":device,"workspaces":{"w":{"kind":"personal"}}}; blobs=blob_replicas(path,cfg,"w",[],{1:bytes(32)}); recovered={open_blob(env,bytes(32))[0] for env in blobs}
    assert recovered==set(data) and duckdb.connect(str(path),read_only=True).execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==2


def test_row_replica_page_is_atomic(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":cert,"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=logical_row("conversations",fields,["c","codex","valid","2026-01-01","2026-01-01",None,None,None,None,"{}"]); proof=row_proof(device,user,"w",1,row); bad={**row,"data":{**row["data"],"title":"tampered"}}
    with pytest.raises(ValueError,match="invalid row proof"): apply_row_replicas(tmp_path/"db",[{"row":row,"proof":proof},{"row":bad,"proof":proof}],"w",[control])
    assert not (tmp_path/"db").exists()


def test_row_projection_preserves_heterogeneous_json_shapes(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"]; rows=[logical_row("messages",fields,[str(i),"c","user","x",None,"2026-01-01",None,json.dumps(value),None]) for i,value in enumerate(({"ref_index":1,"ref_type":"x","turn_index":2},"hidden"))]; bodies=[{"row":row,"proof":row_proof(device,user,"w",1,row)} for row in rows]; assert apply_row_replicas(tmp_path/"db",bodies,"w",[control])==[True,True]
    db=duckdb.connect(str(tmp_path/"db"),read_only=True); physical={r[0] for r in db.execute("SELECT id FROM messages").fetchall()}; assert {json.dumps(json.loads(r[0]),sort_keys=True) for r in db.execute("SELECT CAST(metadata AS VARCHAR) FROM messages").fetchall()}=={json.dumps({"ref_index":1,"ref_type":"x","turn_index":2},sort_keys=True),json.dumps("hidden")} and {r[0] for r in db.execute("SELECT entity FROM archive_changes WHERE kind='messages'").fetchall()}==physical and not physical&{"0","1"}; db.close()


def test_equal_revisions_from_different_authors_project_independently(tmp_path):
    roots,devices=[identity("root-a"),identity("root-b")],[identity("device-a"),identity("device-b")]; users=[public_id(r["sign_public"]) for r in roots]; entries=[{"user":u,"root_public":r["sign_public"],"device":public(d),"certificate":certificate(r,u,d),"history":True} for r,d,u in zip(roots,devices,users)]; control={"workspace":"w","revision":1,"epoch":1,"devices":{d["id"]:e for d,e in zip(devices,entries)}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=logical_row("conversations",fields,["c","codex","same","2026-01-01","2026-01-01",None,None,None,None,"{}"]) ; bodies=[{"row":row,"proof":row_proof(d,u,"w",1,row)} for d,u in zip(devices,users)]
    assert bodies[0]["proof"]["revision"]==bodies[1]["proof"]["revision"] and apply_row_replicas(tmp_path/"db",bodies,"w",[control])==[True,True]; db=duckdb.connect(str(tmp_path/"db"),read_only=True); assert db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0]==2; db.close()


def test_same_authored_row_has_one_identity_across_workspaces(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=lambda title:logical_row("conversations",fields,["c","codex",title,"2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]); base=row("base"); proofs={ws:row_proof(device,user,ws,1,base) for ws in ("a","b")}; path=tmp_path/"db"
    assert apply_row_replicas(path,[{"row":base,"proof":proofs["a"]}],"a",[control("a")])==[True] and apply_row_replicas(path,[{"row":base,"proof":proofs["b"]}],"b",[control("b")])==[True]
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchall()==[(foreign_id(user,"conversations","c"),"base")] and db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==1 and {r[0] for r in db.execute("SELECT workspace_id FROM remote.row_proofs").fetchall()}=={"a","b"}; db.close()
    db=duckdb.connect(str(path)); db.execute("UPDATE conversations SET title='local drift'"); db.close(); newer=row("newer"); pa=row_proof(device,user,"a",1,newer,proofs["a"]["revision"]); assert apply_row_replicas(path,[{"row":newer,"proof":pa}],"a",[control("a")])==[True] and audit_rows(path)["totals"]["projection_mismatch"]==0; branch=row("branch"); pb=row_proof(device,user,"b",1,branch,proofs["b"]["revision"]); assert apply_row_replicas(path,[{"row":branch,"proof":pb}],"b",[control("b")])==[False]
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="newer" and db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==1; db.close()

def test_retained_replica_inventories_before_skipping_a_drifted_projection(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"archive.db"; assert apply_row_replicas(path,bodies,"w",[control],local_user="receiver")==[True]*4
    db=duckdb.connect(str(path)); db.execute("UPDATE tool_calls SET output='\"later\"',status='complete'"); db.close(); assert audit_rows(path)["totals"]["projection_mismatch"]==1
    cfg={"user":"receiver","device":identity("holder"),"workspaces":{"w":{"kind":"personal","epoch":1}}}; key_=bytes(range(32)); seen=[]
    assert row_replicas(path,cfg,"w",[],{1:key_},inventory=lambda ids:seen.extend(ids) or {r[0] for r in ids})==[] and seen
    blocked=[]; repaired=[open_replica(env,key_) for env in row_replicas(path,cfg,"w",[],{1:key_},inventory=lambda ids:set(),blocked=blocked)]
    assert blocked==[("tool_calls","t")] and {body["row"]["kind"] for body in repaired}=={"conversations","messages","file_edits"}

def test_remote_audit_releases_the_archive_between_pages(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"archive.db"; assert apply_row_replicas(path,bodies,"w",[control],local_user="receiver")==[True]*4; db=core_module.open_db(path,purpose="fixture"); db.execute("INSERT INTO provenance.files VALUES ('f',NULL,'x','external')"); db.executemany("INSERT INTO remote.row_proofs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[(f"p{i}",f"w{i}",f"w{i}","file.observed",f"s{i}",1,"h",f"r{i}",None,"active",f"u{i}","d",1,"s") for i in range(2)]); db.executemany("INSERT INTO remote.provenance_origins VALUES ('file.observed','f',?,?,?,?)",[(f"w{i}",f"u{i}",f"s{i}",f"p{i}") for i in range(2)]); db.close(); stages=[]
    def progress(stage):
        with core_module.open_db(path,wait=0,purpose="concurrent audit probe"): pass
        stages.append(stage)
    result=audit_rows(path,1,progress)
    assert result["totals"]["origins"]==6 and len(stages)==12

def test_retained_replica_pages_release_the_archive_reader(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"archive.db"; assert apply_row_replicas(path,bodies,"w",[control],local_user="receiver")==[True]*4; pages=iter(projection_module.retained_proof_pages(path,"w",author="receiver",page=1)); found=set(next(pages))
    with core_module.open_db(path,wait=0,purpose="test.writer") as writer: writer.execute("SELECT 1")
    assert len(found|set().union(*pages))==4


def test_legacy_physical_archive_migrates_then_accepts_unchanged_v1_relay_replica(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=logical_row("conversations",fields,["c","codex","legacy","2026-01-01","2026-01-01",None,None,None,None,"{}"]); proofs={ws:row_proof(device,user,ws,1,row) for ws in ("old-workspace","relay-workspace")}; path=tmp_path/"db"; db=duckdb.connect(str(path)); init_schema(db); pid=project_row_proof(db,proofs["old-workspace"],root["sign_public"],entry["certificate"]); old=provenance_digest(f"old-workspace:{user}:conversations:c")[:16]; db.execute("INSERT INTO conversations VALUES (?,'codex','legacy','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')",[old]); db.execute("INSERT INTO remote.row_origins VALUES ('conversations',?,'old-workspace',?,'device','c',?,'conversations:c',NULL,?)",[old,user,proofs["old-workspace"]["revision"],pid]); db.execute("UPDATE core_schema SET version=1"); db.close(); db=duckdb.connect(str(path)); init_schema(db); db.close()
    assert apply_row_replicas(path,[{"row":row,"proof":proofs["relay-workspace"]}],"relay-workspace",[control("relay-workspace")])==[True]; db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchall()==[(foreign_id(user,"conversations","c"),"legacy")] and {r[0] for r in db.execute("SELECT workspace_id FROM remote.row_proofs").fetchall()}=={"old-workspace","relay-workspace"}; db.close()


def test_optional_projection_bridge_contract_fails_closed(monkeypatch):
    class Entry:
        def load(self): return lambda:{"v":2,"objects":{"x"},"records":lambda *_:[],"accept":lambda *_:None}
    bridges.cache_clear(); monkeypatch.setattr(projection_module,"entry_points",lambda **_:[Entry()])
    with pytest.raises(ValueError,match="Unsupported remote bridge"): bridges()
    bridges.cache_clear()

def test_bridge_collection_runs_once_and_persists_new_proofs_in_bulk(tmp_path,monkeypatch):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); calls={"records":0,"accepted":[]}
    def records(*_): calls["records"]+=1; return [dict(row={"v":1,"kind":"x","id":str(i),"state":"active","data":{}},proof=None,previous=None) for i in range(2)]
    def accept_many(_,values,project): calls["accepted"].append((values,project)); return [True]*len(values)
    monkeypatch.setattr(projection_module,"bridges",lambda:[{"v":3,"schema":1,"objects":{"x"},"records":records,"accept":lambda *_:None,"accept_many":accept_many}]); cfg={"root":root,"user":user,"device":device,"workspaces":{"w":{"epoch":1}}}
    assert len(bridge_replicas(tmp_path,cfg,"w","personal",bytes(range(32))))==2 and calls["records"]==1 and len(calls["accepted"])==1 and len(calls["accepted"][0][0])==2 and calls["accepted"][0][1] is False


def test_provider_session_alias_converges_one_author_and_separates_authors(tmp_path):
    archive=tmp_path/"archive"; path=archive/"data/convos.db"; path.parent.mkdir(parents=True); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); db=duckdb.connect(str(path)); init_schema(db); remote=foreign_id(user,"conversations","b"); metadata='{"session_id":"same","session_kind":"main","session_kind_evidence":"exact"}'; db.executemany("INSERT INTO conversations(id,source,title,metadata) VALUES (?, 'codex','T',?)",[("a",metadata),(remote,metadata)]); db.execute("INSERT INTO remote.row_origins VALUES ('conversations',?,'personal',?,'other-device','b','event','conversations:b',NULL,NULL)",(remote,user)); db.close()
    pending=[v for v in provider_alias_records(archive,user,"personal","personal") if v["proof"] is None]; assert len(pending)==1 and pending[0]["row"]["data"]=={"source":"codex","session_id":"same","members":["a","b"],"canonical":"a"} and pending[0]["row"]["id"]==provider_alias_id("codex","same")
    first=semantic_proof(root,user,device["id"],"personal",1,pending[0]["row"]); assert provider_alias_accept(archive,pending[0]["row"],first,False); assert len(provider_alias_records(archive,user,"personal","personal"))==1
    right={**pending[0]["row"],"data":{**pending[0]["row"]["data"],"members":["a","c"],"canonical":"a"}}; fork=semantic_proof(root,user,identity("other")["id"],"personal",1,right); assert not provider_alias_accept(archive,right,fork)
    merge=next(v for v in provider_alias_records(archive,user,"personal","personal") if v["proof"] is None); assert merge["row"]["data"]["members"]==["a","b","c"] and {first["revision"],fork["revision"]}=={p["revision"] for p in merge["previous"]}; merged=semantic_proof(root,user,device["id"],"personal",1,merge["row"],merge["previous"]); assert provider_alias_accept(archive,merge["row"],merged) and len(provider_alias_records(archive,user,"personal","personal"))==1
    other_root=identity("other-root"); other_user=public_id(other_root["sign_public"]); other=semantic_proof(other_root,other_user,identity("other-device")["id"],"personal",1,pending[0]["row"]); assert provider_alias_accept(archive,pending[0]["row"],other); db=duckdb.connect(str(path),read_only=True); assert {r[0] for r in db.execute("SELECT author_user_id FROM remote.provider_session_aliases").fetchall()}=={user,other_user}; db.close(); bridges.cache_clear(); assert len(provider_alias_records(archive,user,"team","team"))==0 and provider_alias_bridge()["objects"]=={"provider.session"} and "provider.session" in set().union(*(b["objects"] for b in bridges())); bridges.cache_clear()


def test_provider_session_alias_rejects_noncanonical_members(tmp_path):
    archive=tmp_path/"archive"; path=archive/"data/convos.db"; path.parent.mkdir(parents=True); db=duckdb.connect(str(path)); init_schema(db); db.close(); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); row={"v":1,"kind":"provider.session","id":provider_alias_id("codex","s"),"state":"active","data":{"source":"codex","session_id":"s","members":["b","a"],"canonical":"a"}}; proof=semantic_proof(root,user,device["id"],"personal",1,row)
    with pytest.raises(ValueError,match="Malformed provider session alias"): provider_alias_accept(archive,row,proof)


@pytest.mark.parametrize(("native_id","imported_id"),[("a","b"),("b","a")])
def test_provider_alias_reconciliation_signs_successors_and_replays_mixed_native_imported_rows(tmp_path,native_id,imported_id):
    archive=tmp_path/"archive"
    path=archive/"data/convos.db"
    path.parent.mkdir(parents=True)
    root,device,peer=identity("root"),identity("device"),identity("peer")
    user=public_id(root["sign_public"])
    entry=lambda value:{"user":user,"root_public":root["sign_public"],"device":public(value),"certificate":certificate(root,user,value),"history":True}
    control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry(device),peer["id"]:entry(peer)}}
    cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}
    metadata='{"session_id":"same","session_kind":"main","session_kind_evidence":"exact"}'
    native_message,native_artifact=f"message-{native_id}",f"artifact-{native_id}"
    imported_message,imported_artifact=f"message-{imported_id}",f"artifact-{imported_id}"
    db=duckdb.connect(str(path))
    init_schema(db)
    db.execute("INSERT INTO conversations(id,source,title,metadata) VALUES (?,'codex','native',?)",(native_id,metadata))
    db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES (?,?,'user','native','{}')",(native_message,native_id))
    db.execute("INSERT INTO artifacts(id,conversation_id,artifact_type,title,content) VALUES (?,?,'text','native','body')",(native_artifact,native_id))
    db.execute("INSERT INTO provider_sessions VALUES ('codex','same',?),('codex','legacy',?)",(native_id,native_id))
    state=connect(tmp_path/"state.db")
    records=scan(db,state)
    db.close()
    assert attest_rows(path,cfg,"personal",records)==3
    conversation=logical_row("conversations",ARCHIVE_COLUMNS["conversations"],[imported_id,"codex","imported",None,None,None,None,None,None,metadata])
    message=logical_row("messages",ARCHIVE_COLUMNS["messages"],[imported_message,imported_id,"assistant","imported",None,None,None,"{}",None])
    artifact=logical_row("artifacts",ARCHIVE_COLUMNS["artifacts"],[imported_artifact,imported_id,"text","imported","body",None,None,None])
    imported=[{"row":row,"proof":row_proof(peer,user,"personal",1,row)} for row in (conversation,message,artifact)]
    assert apply_row_replicas(path,imported,"personal",[control])==[True,True,True]
    db=duckdb.connect(str(path))
    remote_conversation=foreign_id(user,"conversations",imported_id)
    db.execute("UPDATE provider_sessions SET conversation_id=?",(remote_conversation,))
    initial={(kind,row_id):revision for kind,row_id,revision in db.execute("SELECT row_kind,source_row_id,revision FROM remote.row_proofs").fetchall()}
    db.close()
    alias={"v":1,"kind":"provider.session","id":provider_alias_id("codex","same"),"state":"active","data":{"source":"codex","session_id":"same","members":["a","b"],"canonical":"a"}}
    assert provider_alias_accept(archive,alias,semantic_proof(root,user,device["id"],"personal",1,alias))
    first=reconcile_provider_aliases(path,cfg,"personal")
    assert first=={"changed":1,"settled":0,"blocked":{}}
    db=duckdb.connect(str(path),read_only=True)
    canonical="a" if native_id=="a" else foreign_id(user,"conversations","a")
    loser_message=native_message if native_id=="b" else imported_message
    loser_artifact=native_artifact if native_id=="b" else imported_artifact
    assert db.execute("SELECT id FROM conversations").fetchall()==[(canonical,)]
    assert {r[0] for r in db.execute("SELECT conversation_id FROM messages").fetchall()}=={canonical}
    assert {r[0] for r in db.execute("SELECT conversation_id FROM artifacts").fetchall()}=={canonical}
    assert set(db.execute("SELECT source,session_id,conversation_id FROM provider_sessions").fetchall())=={("codex","same",canonical),("codex","legacy",canonical)}
    heads=db.execute("SELECT row_kind,source_row_id,state,previous_revision FROM remote.row_proofs p WHERE source_row_id IN (SELECT UNNEST(?)) AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.row_kind=p.row_kind AND c.source_row_id=p.source_row_id AND c.author_user_id=p.author_user_id AND c.previous_revision=p.revision) ORDER BY row_kind",(["b",loser_message,loser_artifact],)).fetchall()
    assert heads==[("artifacts",loser_artifact,"active",initial[("artifacts",loser_artifact)]),("conversations","b","deleted",initial[("conversations","b")]),("messages",loser_message,"active",initial[("messages",loser_message)])]
    generation=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]
    db.close()
    assert (path.with_name(path.name+".pre-provider-alias-reconciliation.bak")).is_file()
    assert reconcile_provider_aliases(path,cfg,"personal")=={"changed":0,"settled":1,"blocked":{}}
    db=duckdb.connect(str(path),read_only=True)
    assert db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==generation
    replay_records=scan(db,state)
    db.close()
    key=bytes(range(32))
    bodies=[open_replica(value,key) for value in row_replicas(path,cfg,"personal",replay_records,{1:key})]
    target=tmp_path/"target.db"
    assert any(apply_row_replicas(target,bodies,"personal",[control]))
    replay=duckdb.connect(str(target),read_only=True)
    replay_canonical=foreign_id(user,"conversations","a")
    assert replay.execute("SELECT id FROM conversations").fetchall()==[(replay_canonical,)]
    assert {r[0] for r in replay.execute("SELECT conversation_id FROM messages").fetchall()}=={replay_canonical}
    assert {r[0] for r in replay.execute("SELECT conversation_id FROM artifacts").fetchall()}=={replay_canonical}
    replay.close()
    state.close()


def test_provider_alias_reconciliation_blocks_before_mutation_when_attachment_body_is_missing(tmp_path):
    archive=tmp_path/"archive"
    path=archive/"data/convos.db"
    path.parent.mkdir(parents=True)
    root,device=identity("root"),identity("device")
    user=public_id(root["sign_public"])
    entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}
    control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}
    cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}
    metadata='{"session_id":"same","session_kind":"main","session_kind_evidence":"exact"}'
    db=duckdb.connect(str(path))
    init_schema(db)
    db.executemany("INSERT INTO conversations(id,source,title,metadata) VALUES (?,'codex','T',?)",[("a",metadata),("b",metadata)])
    db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('m','b','user','file','{}')")
    db.execute("INSERT INTO attachments(id,message_id,filename,size) VALUES ('attachment','m','missing.bin',7)")
    db.execute("INSERT INTO attachment_bodies VALUES ('attachment','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',7)")
    db.execute("INSERT INTO provider_sessions VALUES ('codex','same','b')")
    state=connect(tmp_path/"state.db")
    records=scan(db,state)
    before=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]
    db.close()
    assert attest_rows(path,cfg,"personal",records)==4
    alias={"v":1,"kind":"provider.session","id":provider_alias_id("codex","same"),"state":"active","data":{"source":"codex","session_id":"same","members":["a","b"],"canonical":"a"}}
    assert provider_alias_accept(archive,alias,semantic_proof(root,user,device["id"],"personal",1,alias))
    result=reconcile_provider_aliases(path,cfg,"personal")
    assert result["changed"]==result["settled"]==0 and "attachment body unavailable" in next(iter(result["blocked"].values()))
    db=duckdb.connect(str(path),read_only=True)
    assert db.execute("SELECT id FROM conversations ORDER BY id").fetchall()==[("a",),("b",)]
    assert db.execute("SELECT conversation_id FROM messages").fetchone()[0]=="b"
    assert db.execute("SELECT conversation_id FROM provider_sessions").fetchone()[0]=="b"
    assert db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==before
    db.close()
    assert not path.with_name(path.name+".pre-provider-alias-reconciliation.bak").exists()
    state.close()


def _provider_alias_archive(tmp_path,session_b="same"):
    archive=tmp_path/"archive"
    path=archive/"data/convos.db"
    path.parent.mkdir(parents=True)
    root,device=identity("root"),identity("device")
    user=public_id(root["sign_public"])
    entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}
    control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}
    cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}
    metadata=lambda session:json.dumps({"session_id":session,"session_kind":"main","session_kind_evidence":"exact"})
    db=duckdb.connect(str(path))
    init_schema(db)
    db.executemany("INSERT INTO conversations(id,source,title,metadata) VALUES (?,'codex',?,?)",[("a","A",metadata("same")),("b","B",metadata(session_b))])
    db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('message-b','b','user','B','{}')")
    db.execute("INSERT INTO provider_sessions VALUES ('codex','same','b')")
    state=connect(tmp_path/"state.db")
    records=scan(db,state)
    db.close()
    state.close()
    assert attest_rows(path,cfg,"personal",records)==3
    alias={"v":1,"kind":"provider.session","id":provider_alias_id("codex","same"),"state":"active","data":{"source":"codex","session_id":"same","members":["a","b"],"canonical":"a"}}
    assert provider_alias_accept(archive,alias,semantic_proof(root,user,device["id"],"personal",1,alias))
    return archive,path,root,device,user,cfg,entry


def test_provider_alias_reconciliation_blocks_on_row_fork(tmp_path):
    archive,path,root,device,user,cfg,entry=_provider_alias_archive(tmp_path)
    db=duckdb.connect(str(path))
    base=db.execute("SELECT revision FROM remote.row_proofs WHERE row_kind='conversations' AND source_row_id='b'").fetchone()[0]
    values=lambda title:["b","codex",title,None,None,None,None,None,None,'{"session_id":"same","session_kind":"main","session_kind_evidence":"exact"}']
    rows=[logical_row("conversations",ARCHIVE_COLUMNS["conversations"],values(title)) for title in ("fork one","fork two")]
    [project_row_proof(db,row_proof(device,user,"personal",1,row,base),root["sign_public"],entry["certificate"]) for row in rows]
    before=db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]
    db.close()
    result=reconcile_provider_aliases(path,cfg,"personal")
    assert result["changed"]==0 and "forked" in next(iter(result["blocked"].values()))
    db=duckdb.connect(str(path),read_only=True)
    assert db.execute("SELECT id FROM conversations ORDER BY id").fetchall()==[("a",),("b",)]
    assert db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]==before
    db.close()
    assert not path.with_name(path.name+".pre-provider-alias-reconciliation.bak").exists()


def test_provider_alias_reconciliation_blocks_on_conflicting_exact_evidence(tmp_path):
    archive,path,root,device,user,cfg,entry=_provider_alias_archive(tmp_path,"different")
    result=reconcile_provider_aliases(path,cfg,"personal")
    assert result["changed"]==0 and "exact evidence conflicts" in next(iter(result["blocked"].values()))
    db=duckdb.connect(str(path),read_only=True)
    assert db.execute("SELECT id FROM conversations ORDER BY id").fetchall()==[("a",),("b",)]
    assert db.execute("SELECT conversation_id FROM provider_sessions").fetchone()[0]=="b"
    db.close()


def test_provider_alias_reconciliation_rolls_back_interrupted_projection(tmp_path,monkeypatch):
    archive,path,root,device,user,cfg,entry=_provider_alias_archive(tmp_path)
    db=duckdb.connect(str(path),read_only=True)
    before=(db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0])
    db.close()
    monkeypatch.setattr(projection_module,"project_provider_bindings",lambda *_:(_ for _ in ()).throw(RuntimeError("interrupted")))
    result=reconcile_provider_aliases(path,cfg,"personal")
    assert result["changed"]==0 and next(iter(result["blocked"].values()))=="interrupted"
    db=duckdb.connect(str(path),read_only=True)
    assert db.execute("SELECT id FROM conversations ORDER BY id").fetchall()==[("a",),("b",)]
    assert db.execute("SELECT conversation_id FROM messages").fetchone()[0]=="b"
    assert db.execute("SELECT conversation_id FROM provider_sessions").fetchone()[0]=="b"
    assert (db.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0],db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0])==before
    db.close()
    assert path.with_name(path.name+".pre-provider-alias-reconciliation.bak").is_file()


def test_provider_alias_reconciliation_takes_one_backup_for_a_batch(tmp_path,monkeypatch):
    archive,path,root,device,user,cfg,entry=_provider_alias_archive(tmp_path)
    metadata='{"session_id":"second","session_kind":"main","session_kind_evidence":"exact"}'
    db=duckdb.connect(str(path))
    db.executemany("INSERT INTO conversations(id,source,title,metadata) VALUES (?,'codex',?,?)",[("c","C",metadata),("d","D",metadata)])
    db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('message-d','d','user','D','{}')")
    db.execute("INSERT INTO provider_sessions VALUES ('codex','second','d')")
    state=connect(tmp_path/"second-state.db")
    records=scan(db,state)
    db.close()
    state.close()
    assert attest_rows(path,cfg,"personal",records)==3
    alias={"v":1,"kind":"provider.session","id":provider_alias_id("codex","second"),"state":"active","data":{"source":"codex","session_id":"second","members":["c","d"],"canonical":"c"}}
    assert provider_alias_accept(archive,alias,semantic_proof(root,user,device["id"],"personal",1,alias))
    backups=[]
    original=projection_module._migration_backup
    def backup(*args):
        backups.append(1)
        return original(*args)
    monkeypatch.setattr(projection_module,"_migration_backup",backup)
    assert reconcile_provider_aliases(path,cfg,"personal")=={"changed":2,"settled":0,"blocked":{}}
    assert len(backups)==1


def test_event_support_is_exact_and_unknowns_fail_closed(monkeypatch):
    monkeypatch.setattr(projection_module,"bridges",lambda:[]); classify=lambda kind,version:event_support({"kind":kind,"payload_v":version}); assert classify("workspace.policy",1)==classify("workspace.policy",2)==classify("workspace.preference",1)=="supported" and classify("conversation.record",1)==classify("conversation.record",2)==classify("future.opaque",1)==classify("memory.canonical",1)=="required"


def test_member_sharing_preference_is_root_signed_and_defaults_on(tmp_path):
    repo_root,core=source(tmp_path); state=connect(tmp_path/"state.db"); repo=repository(repo_root,core); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); evidence={k:repo[k] for k in ("lineage","remotes")}; state.executemany("INSERT INTO policies VALUES (?,?,?,?,?)",[("w","teammate","repository","theirs",json.dumps(evidence)),("w",user,"path","opaque",None)]); state.execute("INSERT INTO meta VALUES ('core_generation:w','7')"); assert sharing(state,"w",user)|{"proofs":[]}=={"auto_contribute":None,"effective_auto_contribute":True,"match":["cwd","edit"],"proofs":[],"conflict":False} and sharing_routes(state,"w",user,{"w:path:opaque":"/bound"},core)==([repo["id"]],["/bound"],["cwd","edit"])
    row={"v":1,"kind":"sharing.preference","id":f"sharing:w:{user}","state":"active","data":{"auto_contribute":False,"match":["edit"]}}; proof=semantic_proof(root,user,device["id"],"w",1,row); value=event(device,1,"workspace.preference",row["id"],{"row":row,"proof":proof}); project(tmp_path/"core.db",state,value,"w",authors={device["id"]:user},local_user=user)
    assert sharing(state,"w",user)|{"proofs":[]}=={"auto_contribute":False,"effective_auto_contribute":False,"match":["edit"],"proofs":[],"conflict":False} and sharing_routes(state,"w",user,{"w:path:opaque":"/bound"},core)==([],["/bound"],["edit"]) and not state.execute("SELECT 1 FROM meta WHERE key='core_generation:w'").fetchone()


def test_incoming_policy_invalidates_cached_team_scope(tmp_path):
    state=connect(tmp_path/"state.db"); state.execute("INSERT INTO meta VALUES ('core_generation:w','7')"); device=identity("device"); value=event(device,1,"workspace.policy","policy:path:grant",{"kind":"path","value":"grant"}); project(tmp_path/"core.db",state,value,"w",authors={device["id"]:"member"}); assert not state.execute("SELECT 1 FROM meta WHERE key='core_generation:w'").fetchone()


def test_repository_grant_token_uses_evidence_and_bound_checkout_can_go_dormant(tmp_path):
    root,core=source(tmp_path); state=connect(tmp_path/"state.db"); user="member"; repo=repository(root,core); evidence={k:repo[k] for k in ("lineage","remotes")}; state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",("w",user,"repository","grant",json.dumps(evidence))); bindings={"w:repository:grant":{"path":str(root),"repository":repo["id"],"checkout":repo["checkout"]}}
    assert sharing_routes(state,"w",user,bindings,core)[0]==[repo["id"]]; git(root,"remote","add","origin","git@github.com:acme/renamed.git"); assert sharing_routes(state,"w",user,bindings,core)[0]==[repo["id"]]
    __import__("shutil").rmtree(root/".git"); core.close(); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db")); assert sharing_routes(state,"w",user,bindings,core)[0]==[]


def test_path_grant_promotes_only_when_its_exact_root_becomes_git(tmp_path,monkeypatch):
    root=tmp_path/"repo"; root.mkdir(); core=duckdb.connect(); init_schema(core); state=connect(tmp_path/"state.db"); state.execute("INSERT INTO policies VALUES ('w','member','path','path-grant',NULL)"); signer=identity("member"); cfg={"user":public_id(signer["sign_public"]),"root":signer,"device":identity("device"),"workspaces":{"w":{"epoch":1}},"bindings":{"w:path:path-grant":str(root)},"promotions":{}}; state.execute("UPDATE policies SET owner=?",(cfg["user"],)); events=[]; saved=[]
    def emit(cfg,state,ws,record,root=None): p=record["payload"]; events.append(record); state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",(ws,cfg["user"],"repository",p["row"]["data"]["value"],json.dumps(p["row"]["data"]["evidence"])))
    monkeypatch.setattr(remote_client,"publish",emit); monkeypatch.setattr(remote_client,"save",lambda *args:saved.append(1))
    assert not promote_paths(cfg,state,core); git(root,"init","-q"); git(root,"config","user.email","a@b.c"); git(root,"config","user.name","A"); (root/"a.py").write_text("new\n"); git(root,"add","."); git(root,"commit","-qm","init")
    assert promote_paths(cfg,state,core) and not promote_paths(cfg,state,core) and len(events)==len(saved)==1 and events[0]["payload_v"]==2 and events[0]["payload"]["row"]["data"]["value"]==cfg["promotions"]["w:path:path-grant"]["grant"] and cfg["bindings"][f"w:repository:{events[0]['payload']['row']['data']['value']}"]["path"]==str(root)
    state.execute("INSERT INTO policies VALUES ('w',?,'path','second-path',NULL)",(cfg["user"],)); cfg["bindings"]["w:path:second-path"]=str(root); assert promote_paths(cfg,state,core) and len(events)==len(saved)==2 and cfg["promotions"]["w:path:second-path"]["grant"]!=events[0]["payload"]["row"]["data"]["value"]


def test_path_promotion_never_retargets_after_in_place_checkout_replacement(tmp_path,monkeypatch):
    root=tmp_path/"repo"; root.mkdir(); git(root,"init","-q"); git(root,"config","user.email","a@b.c"); git(root,"config","user.name","A"); (root/"a").write_text("one"); git(root,"add","."); git(root,"commit","-qm","one"); core=duckdb.connect(); init_schema(core); state=connect(tmp_path/"state.db"); signer=identity("member"); cfg={"user":public_id(signer["sign_public"]),"root":signer,"device":identity("device"),"workspaces":{"w":{"epoch":1}},"bindings":{"w:path:path-grant":str(root)},"promotions":{}}; state.execute("INSERT INTO policies VALUES ('w',?,'path','path-grant',NULL)",(cfg["user"],)); events=[]
    def emit(cfg,state,ws,record,root=None): events.append(record); state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",(ws,cfg["user"],"repository",record["payload"]["row"]["data"]["value"],json.dumps(record["payload"]["row"]["data"]["evidence"])))
    monkeypatch.setattr(remote_client,"publish",emit); monkeypatch.setattr(remote_client,"save",lambda *args:None); assert promote_paths(cfg,state,core); frozen=json.loads(json.dumps(cfg["promotions"]))
    for child in (root/".git").iterdir(): __import__("shutil").rmtree(child) if child.is_dir() else child.unlink()
    git(root,"init","-q"); git(root,"config","user.email","a@b.c"); git(root,"config","user.name","A"); (root/"a").write_text("two"); git(root,"add","."); git(root,"commit","-qm","two"); assert not promote_paths(cfg,state,core) and cfg["promotions"]==frozen and len(events)==1


def test_grant_binding_kind_and_owner_prevent_repository_to_path_downgrade(tmp_path):
    root,core=source(tmp_path); state=connect(tmp_path/"state.db"); repo=repository(root,core); evidence={k:repo[k] for k in ("lineage","remotes")}; state.executemany("INSERT INTO policies VALUES (?,?,?,?,?)",[("w","member","repository","shared",json.dumps(evidence)),("w","attacker","path","shared",None)]); repos,roots,_=sharing_routes(state,"w","member",{"w:repository:shared":{"path":str(root),"repository":repo["id"],"checkout":repo["checkout"]}},core); assert repos==[repo["id"]] and roots==[]


def test_repository_policy_evidence_is_root_signed_and_immutable(tmp_path):
    state=connect(tmp_path/"state.db"); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); row=lambda lineage:{"v":1,"kind":"repository.policy","id":"repository:w:grant","state":"active","data":{"value":"grant","evidence":{"lineage":lineage,"remotes":[]}}}; wrap=lambda value,seq:event(device,seq,"workspace.policy","policy:repository:grant",{"row":value,"proof":semantic_proof(root,user,device["id"],"w",1,value)},payload_v=2); authors={device["id"]:user}; first=wrap(row("a"),1); assert project(tmp_path/"core.db",state,first,"w",authors=authors) and project(tmp_path/"core.db",state,first,"w",authors=authors)
    with pytest.raises(ValueError,match="evidence conflict"): project(tmp_path/"core.db",state,wrap(row("b"),2),"w",authors=authors)
    assert json.loads(state.execute("SELECT evidence FROM policies").fetchone()[0])["lineage"]=="a"


def test_team_match_modes_are_independent_and_empty_is_passive(tmp_path):
    repo,core=source(tmp_path); core.execute("INSERT INTO conversations VALUES ('cwd','codex','cwd only','2026-01-02','2026-01-02','m',?,NULL,NULL,'{}')",[str(repo)]); core.execute("INSERT INTO messages VALUES ('cwd-m','cwd','user','inspect',NULL,'2026-01-02','m','{}',NULL,NULL)"); core.close(); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db")); state=connect(tmp_path/"state.db"); rid=next(r["payload"]["id"] for r in scan(core,state) if r["kind"]=="repository.observed")
    selected=lambda mode:{r["payload"]["row"][0] for r in scan(core,state,"team",[rid],[],match=mode) if r["kind"]=="conversation.record"}
    assert selected(["cwd"])=={"c","cwd"} and selected(["edit"])=={"c"} and selected([])==set()


def test_durable_row_proof_keeps_only_its_authors_admitted_conversation_after_state_rebuild(tmp_path):
    repo,core=source(tmp_path); alice_root,alice_device=identity("alice-root"),identity("alice-device"); bob_root,bob_device=identity("bob-root"),identity("bob-device"); alice,bob=public_id(alice_root["sign_public"]),public_id(bob_root["sign_public"]); row=logical_row("conversations",ARCHIVE_COLUMNS["conversations"],list(core.execute("SELECT * FROM conversations WHERE id='c'").fetchone())); project_row_proof(core,row_proof(bob_device,bob,"w",1,row),bob_root["sign_public"],certificate(bob_root,bob,bob_device)); state=connect(tmp_path/"fresh-state.db"); scope=set(); assert not scan(core,state,"team",[],[],None,"w",scope,match=[],user=alice) and not scope
    project_row_proof(core,row_proof(alice_device,alice,"w",1,row),alice_root["sign_public"],certificate(alice_root,alice,alice_device)); records=scan(core,state,"team",[],[],None,"w",scope,match=[],user=alice); assert "c" in scope and any(r["kind"]=="conversation.record" and r["payload"]["row"][0]=="c" for r in records)


def test_team_scope_includes_prompt_turn_and_linked_repo_only(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); personal=scan(core,state); rid=next(r["payload"]["id"] for r in personal if r["kind"]=="repository.observed"); team=scan(core,state,"team",[rid],[])
    kinds=[r["kind"] for r in team]; assert kinds.count("conversation.record")==1 and kinds.count("message.record")==2 and "file_edit.record" in kinds and "edit.observed" in kinds and "changeset.observed" not in kinds


def test_scan_redacts_edit_path_when_local_file_fact_is_missing(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); fid=core.execute("SELECT file_id FROM provenance.file_edit_files").fetchone()[0]; core.execute("DELETE FROM provenance.local_facts WHERE kind='file.observed' AND entity=?",[fid]); records=scan(core,state); edit=next(r for r in records if r["kind"]=="file_edit.record"); assert edit["payload"]["row"][2] is None and str(repo) not in json.dumps(records); core.close(); state.close()


def test_team_incremental_uses_changed_rows_after_scope_seed(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); rid=next(r["payload"]["id"] for r in scan(core,state) if r["kind"]=="repository.observed"); seeded=set(); first=scan(core,state,"team",[rid],[],None,"w",seeded); state.executemany("INSERT INTO team_scopes VALUES (?,?)",[("w",c) for c in seeded]); changed=scan(core,state,"team",[rid],[],{("messages","u")},"w",set()); state.execute("DELETE FROM team_scopes"); reentered=scan(core,state,"team",[rid],[],{("conversations","c")},"w",set())
    assert seeded=={"c"} and len(first)>len(changed)==1 and changed[0]["kind"]=="message.record" and changed[0]["payload"]["row"][0]=="u" and len(reentered)==len(first)


def test_repository_policy_does_not_reclassify_uncaptured_live_worktree(tmp_path):
    repo,core=source(tmp_path); worktree=tmp_path/"worktree"; git(repo,"worktree","add","-qb","worktree-test",str(worktree)); core.execute("INSERT INTO conversations VALUES ('w','codex','worktree','2026-01-02','2026-01-02','m',?,NULL,NULL,'{}')",[str(worktree)]); core.execute("INSERT INTO messages VALUES ('wm','w','user','inspect',NULL,'2026-01-02','m','{}',NULL,NULL)"); state=connect(tmp_path/"state.db"); rid=next(r["payload"]["id"] for r in scan(core,state) if r["kind"]=="repository.observed"); team=scan(core,state,"team",[rid],[])
    assert {r["payload"]["row"][0] for r in team if r["kind"]=="conversation.record"}=={"c"}


def test_team_projection_never_reads_attachment_bodies(tmp_path,monkeypatch):
    repo,core=source(tmp_path); body=tmp_path/"secret.bin"; body.write_bytes(b"secret"); core.execute("INSERT INTO attachments (id,message_id,filename,path) VALUES ('a','m','secret.bin',?)",[str(body)]); state=connect(tmp_path/"state.db"); personal=scan(core,state); rid=next(r["payload"]["id"] for r in personal if r["kind"]=="repository.observed")
    original=Path.read_bytes; monkeypatch.setattr(Path,"read_bytes",lambda path:pytest.fail("team projection read attachment body") if path==body else original(path))
    records=scan(core,state,"team",[rid],[])
    assert any(r["kind"]=="attachment.record" for r in records) and not any(r["kind"]=="attachment.chunk" for r in records) and blob_replicas(tmp_path/"source.db",{"workspaces":{"team":{"kind":"team"}}},"team",records,{})==[]


def test_team_policy_routes_complete_cross_repo_conversation(tmp_path):
    first,core=source(tmp_path); second=tmp_path/"second"; second.mkdir(); git(second,"init","-q"); git(second,"config","user.email","a@b.c"); git(second,"config","user.name","A"); (second/"private.py").write_text("private\n"); git(second,"add","."); git(second,"commit","-qm","init"); core.execute("INSERT INTO file_edits VALUES ('private','m',?,'write','private\n','2026-01-01 00:00:01',NULL)",[str(second/'private.py')]); core.execute("INSERT INTO provenance.file_edit_evidence VALUES ('private','confirmed','test_fixture',NULL)"); core.close(); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db")); state=connect(tmp_path/"state.db"); all_records=scan(core,state); repos={r["payload"]["remotes"][0] if r["payload"]["remotes"] else r["payload"]["id"]:r["payload"]["id"] for r in all_records if r["kind"]=="repository.observed"}; first_id=next(r["payload"]["id"] for r in all_records if r["kind"]=="repository.observed" and r["payload"]["head"]==git(first,"rev-parse","HEAD"))
    routed=scan(core,state,"team",[first_id],[]); assert sum(r["kind"]=="file_edit.record" for r in routed)==sum(r["kind"]=="edit.observed" for r in routed)==2 and {r["kind"] for r in routed}>={"conversation.record","message.record","repository.observed"} and not any(r["kind"]=="turn.boundary" for r in routed)


def test_path_policy_match_routes_complete_conversation(tmp_path):
    allowed,private=tmp_path/"project",tmp_path/"project-private"; allowed.mkdir(); private.mkdir(); (allowed/"a.py").write_text("a"); (private/"b.py").write_text("b"); core=duckdb.connect(str(tmp_path/"core.db")); init_schema(core); core.execute("INSERT INTO conversations VALUES ('c','codex','paths','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(tmp_path)]); core.execute("INSERT INTO messages VALUES ('m','c','assistant','done',NULL,'2026-01-01','m','{}',NULL,NULL)"); core.execute("INSERT INTO file_edits VALUES ('a','m',?,'write','a','2026-01-01',NULL),('b','m',?,'write','b','2026-01-01',NULL)",[str(allowed/'a.py'),str(private/'b.py')]); core.execute("INSERT INTO provenance.file_edit_evidence VALUES ('a','confirmed','test_fixture',NULL),('b','confirmed','test_fixture',NULL)"); state=connect(tmp_path/"state.db")
    core.close(); capture_provenance(tmp_path/"core.db"); core=duckdb.connect(str(tmp_path/"core.db")); records=scan(core,state,"team",[],[str(allowed)]); assert sum(r["kind"]=="file_edit.record" for r in records)==2 and not any(r["kind"]=="turn.boundary" for r in records) and str(private) not in json.dumps(records)


def test_edit_policy_uses_captured_resolved_route_after_symlink_retarget(tmp_path):
    a,b=tmp_path/"a",tmp_path/"b"; a.mkdir(); b.mkdir(); link=tmp_path/"current"; link.symlink_to(a); path=tmp_path/"core.db"; core=duckdb.connect(str(path)); init_schema(core); result=core_module.ParseResult(convs=[dict(id="c",source="codex",title="route",created_at=None,updated_at=None,model=None,cwd=str(tmp_path),git_branch=None,project_id=None,metadata="{}")],msgs=[dict(id="m",conversation_id="c",role="assistant",content="done",thinking=None,created_at=None,model=None,metadata="{}",parent_id=None)],edits=[dict(id="e",message_id="m",file_path=str(link/"x.py"),edit_type="write",content="x",created_at=None,old_content=None)],edit_evidence=[dict(file_edit_id="e",status="confirmed",reason="test_fixture",tool_call_id=None)]); core_module.upsert(core,result); core.close(); link.unlink(); link.symlink_to(b); capture_provenance(path); core=duckdb.connect(str(path)); state=connect(tmp_path/"state.db"); selected=lambda root:{r["payload"]["row"][0] for r in scan(core,state,"team",[],[str(root)],match=["edit"]) if r["kind"]=="conversation.record"}; assert selected(a)=={"c"} and selected(b)==set()


def test_per_workspace_device_chain_accepts_reorder_and_rejects_replay_or_bad_parent(tmp_path):
    state=connect(tmp_path/"state.db"); device=identity(); first=event(device,1,"x","1",{},[],"2026-01-01T00:00:00Z"); second=event(device,2,"x","2",{},[first["id"]],"2026-01-01T00:00:01Z"); assert sequence(state,"team",second) and sequence(state,"team",first)
    assert state.execute("SELECT COUNT(*) FROM event_sequences").fetchone()[0]==2 and state.execute("SELECT COUNT(*) FROM sequence_gaps").fetchone()[0]==0
    bad=event(device,3,"x","3",{},["wrong"],"2026-01-01T00:00:02Z")
    import pytest
    with pytest.raises(ValueError,match="chain"): sequence(state,"team",bad)
    replay=event(device,2,"x","other",{},[first["id"]],"2026-01-01T00:00:03Z")
    with pytest.raises(ValueError,match="replay"): sequence(state,"team",replay)
    assert sequence(state,"personal",replay)


def test_completed_remote_attachment_is_rescued_into_archive_storage(tmp_path):
    db_path=tmp_path/"data/convos.db"; db_path.parent.mkdir(); db=duckdb.connect(str(db_path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','attachment','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); db.execute("INSERT INTO messages VALUES ('m','c','user','file',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); old=tmp_path/"remote/attachments/w/blob"; old.parent.mkdir(parents=True); old.write_bytes(b"canonical"); db.execute("INSERT INTO attachments (id,message_id,filename,size,path) VALUES ('a','m','a.bin',?,?)",(old.stat().st_size,str(old))); db.close()
    assert relocate_attachments(db_path,tmp_path/"remote/attachments")==1 and not old.exists() and relocate_attachments(db_path,tmp_path/"remote/attachments")==0
    db=duckdb.connect(str(db_path),read_only=True); path=Path(db.execute("SELECT path FROM attachments WHERE id='a'").fetchone()[0]); db.close(); assert path.parent==tmp_path/"data/attachments" and path.read_bytes()==b"canonical" and os.stat(path).st_mode&0o777==0o600
