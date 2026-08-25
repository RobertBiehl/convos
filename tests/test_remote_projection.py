import json, os, subprocess
from pathlib import Path

import duckdb
import pytest
from ai_convos.cli import ARCHIVE_COLUMNS, capture_provenance, init_schema, project_attachment_body, project_row_proof, provenance_digest, repository
import ai_convos_remote as remote_client
import ai_convos_remote.projection as projection_module
from ai_convos_remote import promote_paths, publish, sharing_routes
from ai_convos_remote.projection import apply_row_replicas, attest_rows, blob_replicas, bridges, connect, cutover_state, event_support, foreign_id, inspect_state, project, project_many, relocate_attachments, row_replicas, scan, sequence, sharing
from ai_convos_remote.protocol import b64, certificate, digest, event, identity, logical_row, open_blob, open_replica, public, public_id, row_proof, semantic_proof


def git(path,*args): return subprocess.run(("git","-C",str(path),*args),check=True,capture_output=True).stdout.decode().strip()
def source(tmp_path):
    repo=tmp_path/"repo"; repo.mkdir(); git(repo,"init","-q"); git(repo,"config","user.email","a@b.c"); git(repo,"config","user.name","A"); (repo/"a.py").write_text("new\n"); git(repo,"add","."); git(repo,"commit","-qm","init")
    path=tmp_path/"source.db"; db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','title','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(repo)]); db.execute("INSERT INTO messages VALUES ('u','c','user','change it',NULL,'2026-01-01 00:00:00','m','{}',NULL,NULL),('m','c','assistant','done',NULL,'2026-01-01 00:00:01','m','{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','new\n','2026-01-01 00:00:01',NULL)",[str(repo/'a.py')]); db.close(); capture_provenance(path); return repo,duckdb.connect(str(path))


def test_personal_scan_strips_local_roots_and_projects_duckdb(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); raw=json.dumps(records)
    assert str(repo) not in raw and len(records)>3; root,device=identity("root"),identity("remote"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}; core.close(); attest_rows(tmp_path/"source.db",cfg,"personal",records); envs=row_replicas(tmp_path/"source.db",cfg,"personal",records,{1:bytes(range(32))}); apply_row_replicas(tmp_path/"target.db",[open_replica(e,bytes(range(32))) for e in envs],"personal",[control])
    target=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert target.execute("SELECT title,cwd FROM conversations").fetchone()==("title",None); assert target.execute("SELECT content FROM messages WHERE role='user'").fetchone()[0]=="change it"; assert target.execute("SELECT file_path FROM file_edits").fetchone()[0]=="a.py"; before=target.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]; assert target.execute("SELECT x.file_edit_id=fe.id FROM provenance.file_edit_files x JOIN file_edits fe ON fe.id=x.file_edit_id").fetchone()[0]; assert target.execute("SELECT COUNT(*) FROM remote.provenance_origins").fetchone()[0]>=before; target.close()
    fresh=connect(tmp_path/"fresh-state.db"); imported=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert scan(imported,fresh)==[]; imported.close(); fresh.close()
    old={"raw_events","repositories","files","file_versions","changesets","edits","changeset_repositories","checkpoints","checkpoint_changesets","assertions","gaps","boundaries"}; assert not old&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert not {"event_log","history_material","history_outbox","history_queue","sharing_boundaries","attachment_chunks","imported_rows"}&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_old_state_inspection_is_read_only_and_cutover_preserves_exact_backup(tmp_path):
    path=tmp_path/"state.db"; db=__import__("sqlite3").connect(path); db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)"); db.execute("CREATE TABLE legacy_payload(value TEXT)"); db.execute("INSERT INTO meta VALUES ('state_schema','2')"); db.execute("INSERT INTO legacy_payload VALUES ('only in old state')"); db.commit(); db.close(); before=(path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in tmp_path.iterdir()})
    assert inspect_state(path)["status"]=="incompatible"
    with pytest.raises(ValueError,match="rebuild required"): connect(path)
    assert (path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in tmp_path.iterdir()})==before
    report=cutover_state(path); backup=Path(report["backup"]); old=__import__("sqlite3").connect(backup/"state.db"); assert old.execute("SELECT value FROM legacy_payload").fetchone()[0]=="only in old state"; old.close()
    state=connect(path); assert state.execute("SELECT value FROM meta WHERE key='state_schema'").fetchone()[0]=="3" and json.loads(state.execute("SELECT value FROM meta WHERE key='state_cutover'").fetchone()[0])["backup"]==str(backup); state.close(); assert inspect_state(path)["status"]=="current" and os.stat(backup).st_mode&0o777==0o700 and os.stat(backup/"state.db").st_mode&0o777==0o600


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
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=lambda title:logical_row("conversations",fields,["c","codex",title,"2026-01-01","2026-01-01",None,None,None,None,"{}"]); base=row("base"); proofs={ws:row_proof(device,user,ws,1,base) for ws in ("a","b")}; path=tmp_path/"db"
    assert apply_row_replicas(path,[{"row":base,"proof":proofs["a"]}],"a",[control("a")])==[True] and apply_row_replicas(path,[{"row":base,"proof":proofs["b"]}],"b",[control("b")])==[True]
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchall()==[(foreign_id(user,"conversations","c"),"base")] and db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==1 and {r[0] for r in db.execute("SELECT workspace_id FROM remote.row_proofs").fetchall()}=={"a","b"}; db.close()
    newer=row("newer"); pa=row_proof(device,user,"a",1,newer,proofs["a"]["revision"]); assert apply_row_replicas(path,[{"row":newer,"proof":pa}],"a",[control("a")])==[True]; branch=row("branch"); pb=row_proof(device,user,"b",1,branch,proofs["b"]["revision"]); assert apply_row_replicas(path,[{"row":branch,"proof":pb}],"b",[control("b")])==[False]
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="newer" and db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==1; db.close()


def test_legacy_physical_archive_migrates_then_accepts_unchanged_v1_relay_replica(tmp_path):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control=lambda ws:{"workspace":ws,"revision":1,"epoch":1,"devices":{device["id"]:entry}}; fields=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]; row=logical_row("conversations",fields,["c","codex","legacy","2026-01-01","2026-01-01",None,None,None,None,"{}"]); proofs={ws:row_proof(device,user,ws,1,row) for ws in ("old-workspace","relay-workspace")}; path=tmp_path/"db"; db=duckdb.connect(str(path)); init_schema(db); pid=project_row_proof(db,proofs["old-workspace"],root["sign_public"],entry["certificate"]); old=provenance_digest(f"old-workspace:{user}:conversations:c")[:16]; db.execute("INSERT INTO conversations VALUES (?,'codex','legacy','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')",[old]); db.execute("INSERT INTO remote.row_origins VALUES ('conversations',?,'old-workspace',?,'device','c',?,'conversations:c',NULL,?)",[old,user,proofs["old-workspace"]["revision"],pid]); db.execute("UPDATE core_schema SET version=1"); db.close(); db=duckdb.connect(str(path)); init_schema(db); db.close()
    assert apply_row_replicas(path,[{"row":row,"proof":proofs["relay-workspace"]}],"relay-workspace",[control("relay-workspace")])==[True]; db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchall()==[(foreign_id(user,"conversations","c"),"legacy")] and {r[0] for r in db.execute("SELECT workspace_id FROM remote.row_proofs").fetchall()}=={"old-workspace","relay-workspace"}; db.close()


def test_optional_projection_bridge_contract_fails_closed(monkeypatch):
    class Entry:
        def load(self): return lambda:{"v":2,"objects":{"x"},"records":lambda *_:[],"accept":lambda *_:None}
    bridges.cache_clear(); monkeypatch.setattr(projection_module,"entry_points",lambda **_:[Entry()])
    with pytest.raises(ValueError,match="Unsupported remote bridge"): bridges()
    bridges.cache_clear()


def test_event_support_is_exact_and_unknowns_fail_closed(monkeypatch):
    monkeypatch.setattr(projection_module,"bridges",lambda:[]); classify=lambda kind,version:event_support({"kind":kind,"payload_v":version}); assert classify("workspace.policy",1)==classify("workspace.policy",2)==classify("workspace.preference",1)=="supported" and classify("conversation.record",1)==classify("conversation.record",2)==classify("future.opaque",1)==classify("memory.canonical",1)=="required"


def test_member_sharing_preference_is_root_signed_and_defaults_on(tmp_path):
    state=connect(tmp_path/"state.db"); core=duckdb.connect(); init_schema(core); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); state.executemany("INSERT INTO policies VALUES (?,?,?,?,?)",[("w",user,"repository","mine",None),("w","teammate","repository","theirs",None),("w","teammate","path","opaque",None)]); state.execute("INSERT INTO meta VALUES ('core_generation:w','7')"); assert sharing(state,"w",user)|{"proofs":[]}=={"auto_contribute":None,"effective_auto_contribute":True,"match":["cwd","edit"],"proofs":[],"conflict":False} and sharing_routes(state,"w",user,{"w:opaque":"/bound"},core)==(["mine","theirs"],["/bound"],["cwd","edit"])
    row={"v":1,"kind":"sharing.preference","id":f"sharing:w:{user}","state":"active","data":{"auto_contribute":False,"match":["edit"]}}; proof=semantic_proof(root,user,device["id"],"w",1,row); value=event(device,1,"workspace.preference",row["id"],{"row":row,"proof":proof}); project(tmp_path/"core.db",state,value,"w",authors={device["id"]:user},local_user=user)
    assert sharing(state,"w",user)|{"proofs":[]}=={"auto_contribute":False,"effective_auto_contribute":False,"match":["edit"],"proofs":[],"conflict":False} and sharing_routes(state,"w",user,{"w:opaque":"/bound"},core)==(["mine"],["/bound"],["edit"]) and not state.execute("SELECT 1 FROM meta WHERE key='core_generation:w'").fetchone()


def test_incoming_policy_invalidates_cached_team_scope(tmp_path):
    state=connect(tmp_path/"state.db"); state.execute("INSERT INTO meta VALUES ('core_generation:w','7')"); device=identity("device"); value=event(device,1,"workspace.policy","policy:path:grant",{"kind":"path","value":"grant"}); project(tmp_path/"core.db",state,value,"w",authors={device["id"]:"member"}); assert not state.execute("SELECT 1 FROM meta WHERE key='core_generation:w'").fetchone()


def test_repository_grant_token_uses_evidence_and_bound_checkout_can_go_dormant(tmp_path):
    root,core=source(tmp_path); state=connect(tmp_path/"state.db"); user="member"; repo=repository(root,core); evidence={k:repo[k] for k in ("lineage","remotes")}; state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",("w",user,"repository","grant",json.dumps(evidence))); bindings={"w:grant":str(root)}
    assert sharing_routes(state,"w",user,bindings,core)[0]==[repo["id"]]; git(root,"remote","add","origin","git@github.com:acme/renamed.git"); assert sharing_routes(state,"w",user,bindings,core)[0]==[repo["id"]]
    __import__("shutil").rmtree(root/".git"); core.close(); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db")); assert sharing_routes(state,"w",user,bindings,core)[0]==[]


def test_path_grant_promotes_only_when_its_exact_root_becomes_git(tmp_path,monkeypatch):
    root=tmp_path/"repo"; root.mkdir(); core=duckdb.connect(); init_schema(core); state=connect(tmp_path/"state.db"); state.execute("INSERT INTO policies VALUES ('w','member','path','path-grant',NULL)"); cfg={"user":"member","bindings":{"w:path-grant":str(root)},"promotions":{}}; events=[]; saved=[]
    def emit(cfg,state,ws,record,root=None): p=record["payload"]; events.append(record); state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",(ws,cfg["user"],p["kind"],p["value"],json.dumps(p["evidence"])))
    monkeypatch.setattr(remote_client,"publish",emit); monkeypatch.setattr(remote_client,"save",lambda *args:saved.append(1))
    assert not promote_paths(cfg,state,core); git(root,"init","-q"); git(root,"config","user.email","a@b.c"); git(root,"config","user.name","A"); (root/"a.py").write_text("new\n"); git(root,"add","."); git(root,"commit","-qm","init")
    assert promote_paths(cfg,state,core) and not promote_paths(cfg,state,core) and len(events)==len(saved)==1 and events[0]["payload_v"]==2 and events[0]["payload"]["value"]==cfg["promotions"]["w:path-grant"] and cfg["bindings"][f"w:{events[0]['payload']['value']}"]==str(root)
    state.execute("INSERT INTO policies VALUES ('w','member','path','second-path',NULL)"); cfg["bindings"]["w:second-path"]=str(root); assert promote_paths(cfg,state,core) and len(events)==1 and len(saved)==2 and cfg["promotions"]["w:second-path"]==events[0]["payload"]["value"]


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
    first,core=source(tmp_path); second=tmp_path/"second"; second.mkdir(); git(second,"init","-q"); git(second,"config","user.email","a@b.c"); git(second,"config","user.name","A"); (second/"private.py").write_text("private\n"); git(second,"add","."); git(second,"commit","-qm","init"); core.execute("INSERT INTO file_edits VALUES ('private','m',?,'write','private\n','2026-01-01 00:00:01',NULL)",[str(second/'private.py')]); core.close(); capture_provenance(tmp_path/"source.db"); core=duckdb.connect(str(tmp_path/"source.db")); state=connect(tmp_path/"state.db"); all_records=scan(core,state); repos={r["payload"]["remotes"][0] if r["payload"]["remotes"] else r["payload"]["id"]:r["payload"]["id"] for r in all_records if r["kind"]=="repository.observed"}; first_id=next(r["payload"]["id"] for r in all_records if r["kind"]=="repository.observed" and r["payload"]["head"]==git(first,"rev-parse","HEAD"))
    routed=scan(core,state,"team",[first_id],[]); assert sum(r["kind"]=="file_edit.record" for r in routed)==sum(r["kind"]=="edit.observed" for r in routed)==2 and {r["kind"] for r in routed}>={"conversation.record","message.record","repository.observed"} and not any(r["kind"]=="turn.boundary" for r in routed)


def test_path_policy_match_routes_complete_conversation(tmp_path):
    allowed,private=tmp_path/"project",tmp_path/"project-private"; allowed.mkdir(); private.mkdir(); (allowed/"a.py").write_text("a"); (private/"b.py").write_text("b"); core=duckdb.connect(str(tmp_path/"core.db")); init_schema(core); core.execute("INSERT INTO conversations VALUES ('c','codex','paths','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(tmp_path)]); core.execute("INSERT INTO messages VALUES ('m','c','assistant','done',NULL,'2026-01-01','m','{}',NULL,NULL)"); core.execute("INSERT INTO file_edits VALUES ('a','m',?,'write','a','2026-01-01',NULL),('b','m',?,'write','b','2026-01-01',NULL)",[str(allowed/'a.py'),str(private/'b.py')]); state=connect(tmp_path/"state.db")
    core.close(); capture_provenance(tmp_path/"core.db"); core=duckdb.connect(str(tmp_path/"core.db")); records=scan(core,state,"team",[],[str(allowed)]); assert sum(r["kind"]=="file_edit.record" for r in records)==2 and not any(r["kind"]=="turn.boundary" for r in records) and str(private) not in json.dumps(records)


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
