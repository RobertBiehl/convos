import json, os, subprocess
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
import ai_convos.cli as core_module
from ai_convos.cli import ARCHIVE_COLUMNS, capture_provenance, init_schema, project_attachment_body, project_row_proof, provenance_digest, repository
import ai_convos_remote as remote_client
import ai_convos_remote.projection as projection_module
from ai_convos_remote import promote_paths, publish, sharing_routes
from ai_convos_remote.projection import apply_row_replicas, attest_rows, audit_rows, blob_replicas, bridge_replicas, bridge_stamp, bridges, connect, cutover_state, event_support, foreign_id, inspect_state, project, project_many, relocate_attachments, row_replicas, scan, sequence, sharing
from ai_convos_remote.protocol import b64, certificate, digest, event, identity, logical_row, open_blob, open_replica, public, public_id, row_proof, seal_replica, semantic_proof


def git(path,*args): return subprocess.run(("git","-C",str(path),*args),check=True,capture_output=True).stdout.decode().strip()
def source(tmp_path):
    repo=tmp_path/"repo"; repo.mkdir(); git(repo,"init","-q"); git(repo,"config","user.email","a@b.c"); git(repo,"config","user.name","A"); (repo/"a.py").write_text("new\n"); git(repo,"add","."); git(repo,"commit","-qm","init")
    path=tmp_path/"source.db"; db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','title','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(repo)]); db.execute("INSERT INTO messages VALUES ('u','c','user','change it',NULL,'2026-01-01 00:00:00','m','{}',NULL,NULL),('m','c','assistant','done',NULL,'2026-01-01 00:00:01','m','{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','new\n','2026-01-01 00:00:01',NULL)",[str(repo/'a.py')]); db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL)"); db.close(); capture_provenance(path); return repo,duckdb.connect(str(path))

def signed_edit_graph():
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:entry}}
    rows={"conversations":logical_row("conversations",ARCHIVE_COLUMNS["conversations"],["c","codex","title","2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]),"messages":logical_row("messages",ARCHIVE_COLUMNS["messages"],["m","c","assistant","done",None,"2026-01-01T00:00:00",None,"{}",None]),"tool_calls":logical_row("tool_calls",ARCHIVE_COLUMNS["tool_calls"],["t","m","write","{}","{}","complete",None,"2026-01-01T00:00:00"]),"file_edits":logical_row("file_edits",ARCHIVE_COLUMNS["file_edits"],["e","m","a.py","write","one","2026-01-01T00:00:00",None])}
    for kind in ("messages","tool_calls"): rows[kind]["data"].pop("edits")
    proofs={kind:row_proof(device,user,"w",1,row) for kind,row in rows.items()}; bodies=[{"row":rows[k],"proof":proofs[k]} for k in rows]; evidence=lambda edit,tool,status="confirmed",reason="provider_success":{"v":1,"kind":"file-edit.evidence","id":"file-edit-evidence:"+core_module.provenance_digest("e"),"state":"active","data":{"edit":"e","edit_revision":edit,"status":status,"reason":reason,"tool_call":"t","tool_revision":tool}}
    return root,device,user,control,rows,proofs,bodies,evidence

def metadata_edits(records): return [e for r in records if r['kind'] in ('message.record','tool.record') for e in dict(zip(r['payload']['columns'],r['payload']['row'])).get('edits',[])]

def test_personal_edit_body_is_independent_of_scan_batch(tmp_path):
    _,core=source(tmp_path)
    with core,connect(tmp_path/"state.db") as state:
        edit=lambda changes:metadata_edits(scan(core,state,changes=changes))
        assert edit(None)==edit({("file_edits","e")})==edit({("file_edits","e"),("edit.observed","e")})

def test_provenance_before_edit_is_retained_and_retried_across_calls(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    fid=digest(dict(repository=None,path="a.py"))
    row=dict(v=1,kind="edit.observed",id="e",state="active",data=dict(turn="m",file=fid,repository=None,old_content_hash=None,new_content_hash="h",evidence="captured_exact"))
    proof=row_proof(device,user,"w",1,row); path=tmp_path/"pending.db"
    apply_row_replicas(path,[dict(row=row,proof=proof)],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?",[digest(proof)]).fetchone()[0])==row
        assert db.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]==0
    apply_row_replicas(path,bodies,"w",[control],local_user="other")
    file=dict(v=1,kind="file.observed",id=fid,state="active",data=dict(repository=None,path="a.py",kind="external"))
    apply_row_replicas(path,[dict(row=file,proof=row_proof(device,user,"w",1,file))],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id=?",[foreign_id(user,"file_edits","e")]).fetchone()==(fid,)
        assert db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==0

def test_same_user_replay_preserves_native_ids_and_pending_local_content(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"native.db"
    apply_row_replicas(path,bodies,"w",[control],recover="native",local_user=user)
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE messages SET content='new local observation' WHERE id='m'")
        before={table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in rows}
    for _ in range(2): apply_row_replicas(path,bodies,"w",[control],local_user=user)
    with duckdb.connect(str(path),read_only=True) as db:
        assert {table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in rows}==before
        assert db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==0
        assert json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?",[digest(proofs["messages"])]).fetchone()[0])==rows["messages"]

def test_same_user_existing_received_binding_is_reused(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"bound.db"
    apply_row_replicas(path,bodies,"w",[control],local_user="previous")
    with duckdb.connect(str(path),read_only=True) as db: before={table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in rows}
    apply_row_replicas(path,bodies,"w",[control],local_user=user)
    with duckdb.connect(str(path),read_only=True) as db: assert {table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in rows}==before

def test_conflicting_provenance_preserves_both_facts_and_healthy_rows(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"conflict.db"
    row=dict(v=1,kind="edit.observed",id="e",state="active",data=dict(turn="m",file="f",repository=None,old_content_hash=None,new_content_hash="h",evidence="captured_exact")); proof=row_proof(device,user,"w",1,row)
    apply_row_replicas(path,[*bodies,dict(row=row,proof=proof)],"w",[control],local_user="other")
    revised={**row,"data":{**row["data"],"file":"another"}}; successor=row_proof(device,user,"w",1,revised)
    apply_row_replicas(path,[dict(row=revised,proof=successor)],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT file_id FROM provenance.file_edit_files").fetchone()==("f",)
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]==1
        assert json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?",[digest(successor)]).fetchone()[0])==revised

def test_author_successor_can_replace_provenance_association(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"successor.db"
    old,new=(digest(dict(repository=None,path=f)) for f in ("old","new"))
    row=dict(v=1,kind="edit.observed",id="e",state="active",data=dict(turn="m",file=old,repository=None,old_content_hash=None,new_content_hash="h",evidence="captured_exact")); proof=row_proof(device,user,"w",1,row)
    files=[dict(v=1,kind="file.observed",id=digest(dict(repository=None,path=f)),state="active",data=dict(repository=None,path=f,kind="external")) for f in ("old","new")]
    apply_row_replicas(path,[*bodies,*(dict(row=f,proof=row_proof(device,user,"w",1,f)) for f in files),dict(row=row,proof=proof)],"w",[control],local_user="other")
    revised={**row,"data":{**row["data"],"file":new}}; successor=row_proof(device,user,"w",1,revised,proof["revision"])
    apply_row_replicas(path,[dict(row=revised,proof=successor)],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT file_id FROM provenance.file_edit_files").fetchone()==(new,)
        assert db.execute("SELECT count(*) FROM remote.row_conflicts").fetchone()[0]==0

def test_received_proxy_rewritten_repository_facts_merge_into_the_configured_remote_id(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"receiver.db"
    loop,real=(dict(lineage="l",roots=[],remotes=[url]) for url in ("https://127.0.0.1:9/token/scm/acme/project","https://example.com/scm/acme/project"))
    old,new=(digest(dict(lineage="l",remotes=r["remotes"])) for r in (loop,real))
    fact=lambda kind,id,data:dict(row=(row:=dict(v=1,kind=kind,id=id,state="active",data=data)),proof=row_proof(device,user,"w",1,row))
    file=lambda repo,name:fact("file.observed",digest(dict(repository=repo,path=name)),dict(repository=repo,path=name,kind="tracked"))
    stale=[fact("repository.observed",old,loop),file(old,"a.py")]
    ssh=dict(lineage="l",roots=[],remotes=["https://example.com:7999/acme/project"]); sibling=digest(dict(lineage="l",remotes=ssh["remotes"]))
    apply_row_replicas(path,[*stale,fact("repository.observed",new,real),fact("repository.observed",sibling,ssh),file(new,"b.py")],"w",[control],local_user="other")
    apply_row_replicas(path,[file(old,"c.py"),*stale],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert sorted(db.execute("SELECT id,CAST(remotes AS VARCHAR) FROM provenance.repositories").fetchall())==sorted([(new,json.dumps(real["remotes"])),(sibling,json.dumps(ssh["remotes"]))])
        assert sorted(db.execute("SELECT id,repository,path FROM provenance.files").fetchall())==sorted((digest(dict(repository=new,path=n)),new,n) for n in ("a.py","b.py","c.py"))
        assert db.execute("SELECT count(*) FROM remote.row_conflicts").fetchone()[0]==3
    assert audit_rows(path,page=1,local_user="other")["totals"]["unavailable"]==0
    keys={1:os.urandom(32)}; exported=[open_replica(env,keys[1]) for env in row_replicas(path,dict(user="other",device=device),"w",[],keys)]
    assert {row["id"] for item in exported for row in [item["row"]] if row["kind"]=="repository.observed"}=={old,new,sibling}
    replay=tmp_path/"replay.db"; apply_row_replicas(replay,exported,"w",[control],local_user="third")
    with duckdb.connect(str(replay),read_only=True) as db:
        assert {r[0] for r in db.execute("SELECT id FROM provenance.repositories").fetchall()}=={new,sibling}
        assert {r[0] for r in db.execute("SELECT path FROM provenance.files").fetchall()}=={"a.py","b.py","c.py"}

def test_proxy_merge_retains_a_colliding_signed_destination_body(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"receiver.db"
    lineage="l"; loop="https://127.0.0.1:9/token/acme/project"; real="https://example.com/acme/project"
    old,new=(digest(dict(lineage=lineage,remotes=[url])) for url in (loop,real))
    fact=lambda rid,remotes:(lambda row:dict(row=row,proof=row_proof(device,user,"w",1,row)))(dict(v=1,kind="repository.observed",id=rid,state="active",data=dict(lineage=lineage,roots=[],remotes=remotes)))
    destination= fact(new,[real,"https://example.com/other/repository"]); proxy=fact(old,[loop])
    apply_row_replicas(path,[destination,proxy],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT CAST(remotes AS VARCHAR) FROM provenance.repositories WHERE id=?",[new]).fetchone()==(json.dumps([real]),)
        assert json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?",[digest(destination["proof"])]).fetchone()[0])==destination["row"]
    keys={1:os.urandom(32)}; exported=[open_replica(env,keys[1]) for env in row_replicas(path,dict(user="other",device=device),"w",[],keys)]
    assert destination["row"] in [item["row"] for item in exported] and proxy["row"] in [item["row"] for item in exported]
    replay=tmp_path/"replay.db"; apply_row_replicas(replay,exported,"w",[control],local_user="third")
    with duckdb.connect(str(replay),read_only=True) as db: assert db.execute("SELECT id FROM provenance.repositories").fetchall()==[(new,)]

def test_audit_detects_missing_body_even_without_surviving_origin(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"lost.db"
    apply_row_replicas(path,bodies,"w",[control],local_user="other")
    with duckdb.connect(str(path)) as db: db.execute("DELETE FROM messages; DELETE FROM remote.row_origins WHERE table_name='messages'")
    audit=audit_rows(path,page=1,local_user="other")
    assert audit["tables"]["messages"]["unavailable"]==1
    apply_row_replicas(path,[bodies[1]],"w",[control],local_user="other")
    assert audit_rows(path,page=1,local_user="other")["totals"]["unavailable"]==0

@pytest.mark.parametrize("timestamp",["2026-01-01T00:00:00","2026-01-01T00:00:00.000000","2026-01-01T00:00:00.123456"])
def test_historical_timestamp_reconstructs_exact_proof_without_resigning(tmp_path,timestamp):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"encoding.db"
    row={**rows["messages"],"data":{**rows["messages"]["data"],"created_at":timestamp}}; proof=row_proof(device,user,"w",1,row)
    apply_row_replicas(path,[bodies[0],dict(row=row,proof=proof)],"w",[control],local_user="other")
    audit=audit_rows(path,local_user="other"); assert audit["totals"]["projection_mismatch"]==0 and audit["totals"]["unavailable"]==0
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute("SELECT content_hash FROM remote.row_proofs WHERE id=?",[digest(proof)]).fetchone()[0]==digest(row)

def test_sql_conversion_cannot_discard_the_exact_verified_body(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/"lossy.db"
    row={**rows["messages"],"data":{**rows["messages"]["data"],"created_at":"2026-01-01T00:00:00+02:00"}}; proof=row_proof(device,user,"w",1,row)
    apply_row_replicas(path,[bodies[0],dict(row=row,proof=proof)],"w",[control],local_user="other")
    with duckdb.connect(str(path),read_only=True) as db: assert json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?",[digest(proof)]).fetchone()[0])==row
    audit=audit_rows(path,local_user="other"); assert audit["totals"]["unavailable"]==0 and audit["totals"]["retained_variants"]==1
    keys={1:os.urandom(32)}; replicas=row_replicas(path,dict(user="other",device=device),"w",[],keys)
    assert row in [open_replica(env,keys[1])["row"] for env in replicas]



def test_personal_scan_strips_local_roots_and_projects_duckdb(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); records=scan(core,state); raw=json.dumps(records)
    assert str(repo) not in raw and len(records)>3; root,device=identity("root"),identity("remote"); user=public_id(root["sign_public"]); entry={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"personal","revision":1,"epoch":1,"devices":{device["id"]:entry}}; cfg={"user":user,"device":device,"workspaces":{"personal":{"kind":"personal","epoch":1}},"controls":{"personal":control},"server_state":{"workspaces":[{"id":"personal","controls":[control]}]}}; core.close(); attest_rows(tmp_path/"source.db",cfg,"personal",records)
    def inventory(_):
        with core_module.open_db(tmp_path/"source.db",wait=0) as writer: writer.execute("UPDATE conversations SET title='title'")
        return []
    envs=row_replicas(tmp_path/"source.db",cfg,"personal",records,{1:bytes(range(32))},inventory=inventory); apply_row_replicas(tmp_path/"target.db",[open_replica(e,bytes(range(32))) for e in envs],"personal",[control])
    target=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert target.execute("SELECT title,cwd FROM conversations").fetchone()==("title",None); assert target.execute("SELECT content FROM messages WHERE role='user'").fetchone()[0]=="change it"; assert target.execute("SELECT file_path FROM file_edits").fetchone()[0]=="a.py" and target.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone()==("confirmed","test_fixture"); before=target.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]; assert target.execute("SELECT x.file_edit_id=fe.id FROM provenance.file_edit_files x JOIN file_edits fe ON fe.id=x.file_edit_id").fetchone()[0]; assert target.execute("SELECT COUNT(*) FROM remote.provenance_origins").fetchone()[0]>=before; target.close()
    fresh=connect(tmp_path/"fresh-state.db"); imported=duckdb.connect(str(tmp_path/"target.db"),read_only=True); assert scan(imported,fresh)==[]; imported.close(); fresh.close()
    old={"raw_events","repositories","files","file_versions","changesets","edits","changeset_repositories","checkpoints","checkpoint_changesets","assertions","gaps","boundaries"}; assert not old&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert not {"event_log","history_material","history_outbox","history_queue","sharing_boundaries","attachment_chunks","imported_rows"}&{r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}











def test_semantic_data_changes_do_not_reset_the_replica_cursor(tmp_path,monkeypatch):
    archive=tmp_path/"archive"; path=archive/"data/convos.db"; path.parent.mkdir(parents=True); db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}'); INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','assistant','{}'); INSERT INTO file_edits VALUES ('e','m','a.py','write','one',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','first',NULL)"); db.close(); stamp=bridge_stamp(archive); db=duckdb.connect(str(path)); db.execute("UPDATE provenance.file_edit_evidence SET reason='second'"); db.close(); assert bridge_stamp(archive)==stamp
    state=connect(tmp_path/"state.db"); state.execute("INSERT INTO meta VALUES ('replica_cursor:w','941'),('replica_projection:w',?)",(stamp,)); state.commit(); requests=[]; monkeypatch.setattr(remote_client,"request",lambda cfg,payload:requests.append(payload) or {"floor":0,"tail":941,"replicas":[]}); assert remote_client.pull_row_replicas({"user":"receiver"},state,archive,{"id":"w","controls":[],"keys":[]})==0 and requests==[{"op":"replica_pull","workspace":"w","after":941,"limit":500,"semantic":True}]; state.close()

def test_personal_and_team_retain_results_but_only_confirmed_edits_have_graph_edges(tmp_path):
    repo,core=source(tmp_path); core.execute("INSERT INTO file_edits VALUES ('bad','m',?,'write','bad','2026-01-01 00:00:02',NULL)",[str(repo/'bad.py')]); core.execute("INSERT INTO provenance.file_edit_evidence VALUES ('bad','invalid','provider_failure',NULL)"); state=connect(tmp_path/"state.db"); personal=scan(core,state); rid=next(r["payload"]["id"] for r in personal if r["kind"]=="repository.observed"); team=scan(core,state,"team",[rid],[])
    assert {e["id"] for e in metadata_edits(personal)}=={e["id"] for e in metadata_edits(team)}=={"e","bad"}
    assert {r["entity"] for r in team if r["kind"]=="edit.observed"}=={"e"}


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
    fork=row_proof(device,user,"w",1,row("fork"),parent["revision"]); apply_row_replicas(tmp_path/"fork",[body(row("parent"),parent)],"w",[control]); assert apply_row_replicas(tmp_path/"fork",[body(row("child"),child),body(row("fork"),fork)],"w",[control])==[False,False]; db=duckdb.connect(str(tmp_path/"fork"),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="parent" and db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==3; db.close()


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


@pytest.mark.parametrize('body_first',[False,True])
@pytest.mark.parametrize('native',[False,True])
def test_attachment_rows_and_shared_bytes_can_arrive_in_either_order(tmp_path,body_first,native):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    path=tmp_path/'data/convos.db'; data=b'one shared attachment'; hash_=core_module.provenance_digest(data)
    apply=lambda values:apply_row_replicas(path,values,'w',[control],local_user=user if native else 'receiver')
    apply(bodies)
    if body_first: project_attachment_body(path,data,hash_)
    for name in ('a','b'):
        row=logical_row('attachments',[*ARCHIVE_COLUMNS['attachments'],'body_hash'],[name,'m','image.png','image/png',len(data),None,None,None,hash_])
        apply([dict(row=row,proof=row_proof(device,user,'w',1,row))])
    if not body_first: project_attachment_body(path,data,hash_)
    with duckdb.connect(str(path),read_only=True) as db:
        paths=db.execute('SELECT path FROM attachments').fetchall()
        assert len(paths)==2 and all(value and Path(value).read_bytes()==data for value, in paths)


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

def test_bridge_delta_receives_exact_archive_changes(tmp_path,monkeypatch):
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); changes=[{"kind":"file_edit.record","payload":{"row":["e"]}}]; calls=[]; row={"v":1,"kind":"x","id":"1","state":"active","data":{}}; bridge={"v":3,"schema":1,"objects":{"x"},"records":lambda *_:(_ for _ in ()).throw(AssertionError("full inventory used")),"delta":lambda *args:calls.append(args[-1]) or [dict(row=row,proof=None,previous=None)],"accept":lambda *_:True}; monkeypatch.setattr(projection_module,"bridges",lambda:[bridge]); cfg={"root":root,"user":user,"device":device,"workspaces":{"w":{"epoch":1}}}
    assert len(bridge_replicas(tmp_path,cfg,"w","personal",bytes(range(32)),changes=changes))==1 and calls==[changes]






















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
    kinds=[r["kind"] for r in team]; assert kinds.count("conversation.record")==1 and kinds.count("message.record")==2 and metadata_edits(team) and "file_edit.record" not in kinds and "edit.observed" in kinds and "changeset.observed" not in kinds


def test_scan_redacts_edit_path_when_local_file_fact_is_missing(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); fid=core.execute("SELECT file_id FROM provenance.file_edit_files").fetchone()[0]; core.execute("DELETE FROM provenance.local_facts WHERE kind='file.observed' AND entity=?",[fid]); records=scan(core,state); edit=metadata_edits(records)[0]; assert edit["file_path"] is None and str(repo) not in json.dumps(records); core.close(); state.close()


def test_team_incremental_uses_changed_rows_after_scope_seed(tmp_path):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); rid=next(r["payload"]["id"] for r in scan(core,state) if r["kind"]=="repository.observed"); seeded=set(); first=scan(core,state,"team",[rid],[],None,"w",seeded); state.executemany("INSERT INTO team_scopes VALUES (?,?)",[("w",c) for c in seeded]); changed=scan(core,state,"team",[rid],[],{("messages","u")},"w",set()); state.execute("DELETE FROM team_scopes"); reentered=scan(core,state,"team",[rid],[],{("conversations","c")},"w",set())
    assert seeded=={"c"} and len(first)>len(changed)==1 and changed[0]["kind"]=="message.record" and changed[0]["payload"]["row"][0]=="u" and len(reentered)==1


@pytest.mark.parametrize("kind",sorted(projection_module.PROVENANCE))
def test_team_provenance_delta_uses_unchanged_scope_dependencies(tmp_path,kind):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); rid=core.execute("SELECT id FROM provenance.repositories").fetchone()[0]
    record=next(r for r in scan(core,state,"team",[rid],[],workspace="w") if r["kind"]==kind)
    state.execute("INSERT INTO team_scopes VALUES ('w','c')"); state.commit(); scope=set(); change={(kind,record["entity"])}
    expected=scan(core,state,"team",[],[],change,"w",scope,match=[])
    assert [r for r in expected if r["kind"] in core_module.PROVENANCE_KINDS]==[record] and not scope
    assert bool(metadata_edits(expected))==(kind=="edit.observed")
    assert scan(core,state,"team",[],[],change,"unshared",set(),match=[])==[]
    before=core.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]; core_module._archive_touch(core,change); generation=core.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]; core.close()
    assert projection_module.scan_archive(tmp_path/"source.db",state,"team",workspace="w",new_scope=set(),match=[],generation=generation,since=before,page=1)==expected
    state.close()


def test_team_message_delta_reads_only_its_scope_and_emits_no_scope_rewrite(tmp_path):
    _,core=source(tmp_path); state=connect(tmp_path/"state.db")
    core.execute("INSERT INTO conversations SELECT 'extra-c'||i,'codex','extra',NULL,NULL,NULL,NULL,NULL,NULL,'{}' FROM range(1000) r(i)")
    core.execute("INSERT INTO messages SELECT 'extra-m'||i,'extra-c'||i,'assistant','extra',NULL,NULL,NULL,'{}',NULL,NULL FROM range(1000) r(i)")
    core.execute("INSERT INTO file_edits SELECT 'extra-e'||i,'extra-m'||i,'extra.py','write','extra',NULL,NULL FROM range(1000) r(i)")
    core.execute("INSERT INTO provenance.file_edit_evidence SELECT 'extra-e'||i,'confirmed','fixture',NULL FROM range(1000) r(i)")
    state.executemany("INSERT INTO team_scopes VALUES ('w',?)",[("c",),*((f"extra-c{i}",) for i in range(1000))]); state.commit()
    class Bounded:
        def __init__(self,db): self.db=db
        def execute(self,*args): self.cursor=self.db.execute(*args); return self
        @property
        def description(self): return self.cursor.description
        def fetchall(self):
            rows=self.cursor.fetchall(); assert len(rows)<=10, "one-message delta materialized unrelated archive inventory"; return rows
    scope=set(); records=scan(Bounded(core),Bounded(state),"team",[],[],{("messages","u")},"w",scope,match=[],user="local")
    assert len(records)==1 and records[0]["payload"]["row"][0]=="u" and not scope
    core.close(); state.close()


def test_team_admission_expands_in_bounded_reopenable_pages(tmp_path,monkeypatch):
    repo,core=source(tmp_path); state=connect(tmp_path/"state.db"); rid=next(r["payload"]["id"] for r in scan(core,state) if r["kind"]=="repository.observed"); expected=scan(core,state,"team",[rid],[]); generation=core.execute("SELECT generation FROM archive_state WHERE singleton").fetchone()[0]; core.close(); scope,pages,probes=set(),[],[]; real_page=projection_module._team_page
    def page(*args): rows=real_page(*args); pages.append(len(rows)); return rows
    def progress(stage):
        if stage.startswith("scanning admitted"):
            with core_module.open_db(tmp_path/"source.db",wait=0,purpose="team page probe"): pass
            probes.append(1)
    monkeypatch.setattr(projection_module,"_team_page",page)
    actual=projection_module.scan_archive(tmp_path/"source.db",state,"team",[rid],[],"w",scope,("cwd","edit"),None,generation,progress,page=1)
    assert {(r["kind"],r["entity"]):r for r in actual}=={(r["kind"],r["entity"]):r for r in expected} and scope=={"c"} and probes and max(pages)<=1


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
    routed=scan(core,state,"team",[first_id],[]); assert len(metadata_edits(routed))==sum(r["kind"]=="edit.observed" for r in routed)==2 and {r["kind"] for r in routed}>={"conversation.record","message.record","repository.observed"} and not any(r["kind"]=="turn.boundary" for r in routed)


def test_path_policy_match_routes_complete_conversation(tmp_path):
    allowed,private=tmp_path/"project",tmp_path/"project-private"; allowed.mkdir(); private.mkdir(); (allowed/"a.py").write_text("a"); (private/"b.py").write_text("b"); core=duckdb.connect(str(tmp_path/"core.db")); init_schema(core); core.execute("INSERT INTO conversations VALUES ('c','codex','paths','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(tmp_path)]); core.execute("INSERT INTO messages VALUES ('m','c','assistant','done',NULL,'2026-01-01','m','{}',NULL,NULL)"); core.execute("INSERT INTO file_edits VALUES ('a','m',?,'write','a','2026-01-01',NULL),('b','m',?,'write','b','2026-01-01',NULL)",[str(allowed/'a.py'),str(private/'b.py')]); core.execute("INSERT INTO provenance.file_edit_evidence VALUES ('a','confirmed','test_fixture',NULL),('b','confirmed','test_fixture',NULL)"); state=connect(tmp_path/"state.db")
    core.close(); capture_provenance(tmp_path/"core.db"); core=duckdb.connect(str(tmp_path/"core.db")); records=scan(core,state,"team",[],[str(allowed)]); assert len(metadata_edits(records))==2 and not any(r["kind"]=="turn.boundary" for r in records) and str(private) not in json.dumps(records)


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
