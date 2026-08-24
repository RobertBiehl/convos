import json, subprocess, sys

import duckdb
import pytest
import ai_convos.cli as core_module
from ai_convos.cli import ARCHIVE_COLUMNS, archive_changes, archive_state, capture_provenance as capture, init_schema, project_archive_row, project_provenance as project, project_row_proof, project_row_proofs, project_workspace_controls, provenance_digest as digest, rebuild_fts_index, remote_id, repository
from ai_convos_changegraph.provenance import query
from ai_convos_remote.protocol import certificate, event, identity, logical_row, public_id, row_proof


def git(path,*args): return subprocess.run(("git","-C",str(path),*args),check=True,capture_output=True).stdout.decode().strip()
def repo(path,name="x.py",content="one\n"):
    path.mkdir(); git(path,"init","-q"); git(path,"config","user.email","test@example.com"); git(path,"config","user.name","Test"); (path/name).write_text(content); git(path,"add","."); git(path,"commit","-qm","initial"); return path
def core(path,cwd,edits):
    db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','cross repo','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(cwd)]); db.execute("INSERT INTO messages VALUES ('u','c','user','make the cross-repo change',NULL,'2025-12-31 23:59:59','m','{}',NULL,NULL),('m','c','assistant','done',NULL,'2026-01-01','m','{}',NULL,NULL)")
    for i,(file,kind,content,old) in enumerate(edits): db.execute("INSERT INTO file_edits VALUES (?,?,?,?,?,'2026-01-01',?)",[f'e{i}','m',str(file),kind,content,old])
    return db
def graph(path):
    db=duckdb.connect(str(path)); init_schema(db); return db


def test_path_independent_repo_cross_repo_changeset_and_canonical_schema(tmp_path):
    a,b=repo(tmp_path/"a",content="new a\n"),repo(tmp_path/"b",content="new b\n"); clone=tmp_path/"clone"; subprocess.run(("git","clone","-q",str(a),str(clone)),check=True); assert repository(a)["id"]==repository(clone)["id"]
    path=tmp_path/"core.db"; db=core(path,a,[(a/"x.py","write","new a\n",None),(b/"x.py","write","new b\n",None)]); db.close(); records=capture(path); db=duckdb.connect(str(path)); wire=json.dumps(records); assert str(a) not in wire and str(b) not in wire and not any(r["kind"]=="changeset.observed" for r in records)
    assert len({r["payload"]["id"] for r in records if r["kind"]=="repository.observed"})==2 and {r["payload"]["id"] for r in records if r["kind"]=="edit.observed"}=={"e0","e1"}
    observed=next(r for r in records if r["kind"]=="repository.observed"); head=db.execute("SELECT last_head FROM provenance.repositories WHERE id=?",[observed["entity"]]).fetchone()[0]; project(db,{**observed,"payload":{k:v for k,v in observed["payload"].items() if k!="head"},"observed_at":None}); assert db.execute("SELECT last_head FROM provenance.repositories WHERE id=?",[observed["entity"]]).fetchone()[0]==head
    row=query(db,"conversation_changes","c")[0]; assert row["repositories"]==2 and row["files"]==2 and row["prompt"]=="make the cross-repo change" and row["changeset_id"]=="m"
    assert len(query(db,"changeset_files","m"))==2 and query(db,"current_activity",str(a))[0]["repository"]==repository(a)["id"]
    tables={r[0] for r in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='provenance'").fetchall()}; assert tables=={"repositories","repository_checkouts","files","file_versions","file_edit_files","git_checkpoints","checkpoint_edits","local_facts"}
    columns={r[0] for r in db.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='provenance'").fetchall()}; assert not columns&{"prompt","content","payload","workspace","author"}


def test_core_schema_upgrade_adds_canonical_schemas_without_rewriting_archive(tmp_path):
    db=duckdb.connect(str(tmp_path/"core.db")); init_schema(db); identity=archive_state(db)[0]; db.execute("INSERT INTO conversations VALUES ('keep','codex','preserved','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); db.execute("DROP SCHEMA provenance CASCADE"); db.execute("DROP SCHEMA remote CASCADE"); init_schema(db)
    assert db.execute("SELECT title FROM conversations WHERE id='keep'").fetchone()[0]=="preserved" and db.execute("SELECT COUNT(*) FROM provenance.repositories").fetchone()[0]==db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==0 and archive_state(db)[0]==identity


def test_existing_core_is_checkpointed_once_before_automatic_migration(tmp_path):
    path=tmp_path/"legacy.db"; db=duckdb.connect(str(path)); db.execute("CREATE TABLE conversations(id VARCHAR,title VARCHAR)"); db.execute("INSERT INTO conversations VALUES ('keep','preserved')"); db.close(); db=duckdb.connect(str(path)); init_schema(db); db.close(); backup=path.with_name("legacy.db.pre-v1.bak"); check=duckdb.connect(str(backup),read_only=True); assert check.execute("SELECT * FROM conversations").fetchall()==[("keep","preserved")]; check.close(); stamp=backup.stat().st_mtime_ns; db=duckdb.connect(str(path)); init_schema(db); assert db.execute("SELECT version FROM core_schema").fetchone()[0]==2; db.close(); assert backup.stat().st_mtime_ns==stamp and backup.stat().st_mode&0o777==0o600


def test_stale_fixed_migration_backup_is_preserved_not_reused(tmp_path):
    path=tmp_path/"legacy.db"; stale=path.with_name("legacy.db.pre-v1.bak"); db=duckdb.connect(str(stale)); db.execute("CREATE TABLE conversations(id VARCHAR,title VARCHAR)"); db.execute("INSERT INTO conversations VALUES ('old','stale')"); db.close(); db=duckdb.connect(str(path)); db.execute("CREATE TABLE conversations(id VARCHAR,title VARCHAR)"); db.execute("INSERT INTO conversations VALUES ('new','current')"); db.close(); db=duckdb.connect(str(path)); init_schema(db); db.close(); backups=list(tmp_path.glob("legacy.db.pre-v1.bak.*")); assert len(backups)==1 and duckdb.connect(str(stale),read_only=True).execute("SELECT id FROM conversations").fetchone()[0]=="old" and duckdb.connect(str(backups[0]),read_only=True).execute("SELECT id FROM conversations").fetchone()[0]=="new"


def test_v2_unaffected_archive_skips_backup_and_rewrite(tmp_path):
    path=tmp_path/"ready.db"; db=graph(path); user="author"; row_id=remote_id(user,"conversations","c"); db.execute("INSERT INTO conversations VALUES (?,'codex','ready',NULL,NULL,NULL,NULL,NULL,NULL,'{}')",[row_id]); db.execute("INSERT INTO remote.row_origins VALUES ('conversations',?,'w',?,'d','c','e','conversations:c',NULL,NULL)",[row_id,user]); before=archive_state(db)[1]; db.execute("UPDATE core_schema SET version=1"); db.close(); db=duckdb.connect(str(path)); init_schema(db); assert db.execute("SELECT id FROM conversations").fetchone()[0]==row_id and archive_state(db)[1]==before and db.execute("SELECT version FROM core_schema").fetchone()[0]==2; db.close(); assert not path.with_name("ready.db.pre-v2.bak").exists()


def test_v2_migrates_workspace_scoped_rows_and_unblocks_provenance_replay(tmp_path,monkeypatch):
    path=tmp_path/"archive.db"; db=graph(path); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); workspace="workspace"; old=lambda table,source:digest(f"{workspace}:{user}:{table}:{source}")[:16]; source={"conversations":["c","codex","legacy","2026-01-01","2026-01-01",None,None,None,None,"{}"],"messages":["m","c","user","findable",None,"2026-01-01",None,"{}",None],"file_edits":["e","m","x.py","write","new","2026-01-01","old"]}; pids={}
    for table,values in source.items():
        row=logical_row(table,ARCHIVE_COLUMNS[table],values); proof=row_proof(device,user,workspace,1,row); pids[table]=pid=project_row_proof(db,proof,root["sign_public"],cert); physical=values.copy(); physical[0]=old(table,values[0]); physical[1]=old("conversations" if table=="messages" else "messages",values[1]) if table!="conversations" else physical[1]; origin={"workspace_id":workspace,"author_user_id":user,"author_device_id":device["id"],"source_row_id":values[0],"source_event_id":proof["revision"],"content_key":f"{table}:{values[0]}","observed_at":None,"proof_id":pid}; project_archive_row(db,table,ARCHIVE_COLUMNS[table],physical,origin)
    db.execute("INSERT INTO provenance.files VALUES ('f',NULL,'x.py','external')"); event={"kind":"edit.observed","entity":"e","payload":{"id":"e","turn":"m","file":"f","repository":None,"old_content_hash":"old","new_content_hash":"new","evidence":"captured_exact"},"observed_at":None}; mapper=lambda table,value:remote_id(user,table,value); legacy=lambda table,value:old(table,value); assert project(db,event,legacy); old_link=digest({"checkpoint":"cp","edit":old("file_edits","e")}); db.execute("INSERT INTO provenance.checkpoint_edits VALUES ('cp',?,'full_content_match')",[old("file_edits","e")]); db.execute("INSERT INTO remote.provenance_origins VALUES ('edit.observed',?,?,?,?,?)",(old("file_edits","e"),workspace,user,"e",pids["file_edits"])); db.execute("INSERT INTO remote.provenance_origins VALUES ('checkpoint.link',?,?,?,?,?)",(old_link,workspace,user,digest({"checkpoint":"cp","edit":"e"}),None))
    with pytest.raises(ValueError,match="edit/turn mismatch"): project(db,event,mapper)
    db.execute("UPDATE core_schema SET version=1"); rebuild_fts_index(db); docid=db.execute("SELECT docid FROM fts_main_messages.docs WHERE name=?",[old("messages","m")]).fetchone()[0]; before=archive_state(db)[1]; db.close(); monkeypatch.setattr(core_module,"rebuild_fts_index",lambda _:(_ for _ in ()).throw(AssertionError("FTS rebuild was unnecessary"))); db=duckdb.connect(str(path)); init_schema(db); assert path.with_name("archive.db.pre-v2.bak").is_file() and db.execute("SELECT version FROM core_schema").fetchone()[0]==2 and db.execute("SELECT id FROM conversations").fetchone()[0]==remote_id(user,"conversations","c") and db.execute("SELECT id,conversation_id FROM messages").fetchone()==(remote_id(user,"messages","m"),remote_id(user,"conversations","c")) and db.execute("SELECT id,message_id FROM file_edits").fetchone()==(remote_id(user,"file_edits","e"),remote_id(user,"messages","m")) and archive_state(db)[1]>before
    new_edit=remote_id(user,"file_edits","e"); new_link=digest({"checkpoint":"cp","edit":new_edit}); assert project(db,event,mapper) and db.execute("SELECT file_edit_id FROM provenance.file_edit_files").fetchone()[0]==new_edit and db.execute("SELECT file_edit_id FROM provenance.checkpoint_edits").fetchone()[0]==new_edit and {r[0] for r in db.execute("SELECT physical_entity FROM remote.provenance_origins").fetchall()}=={new_edit,new_link} and db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==3 and db.execute("SELECT docid FROM fts_main_messages.docs WHERE name=?",[remote_id(user,"messages","m")]).fetchone()[0]==docid and db.execute("SELECT fts_main_messages.match_bm25(?, 'findable')",[remote_id(user,"messages","m")]).fetchone()[0] is not None; db.close()


def test_v2_resumes_required_fts_rebuild_after_data_commit(tmp_path,monkeypatch):
    path=tmp_path/"resume.db"; db=graph(path); user="author"; old=lambda table,source:digest(f"workspace:{user}:{table}:{source}")[:16]; rows=(("conversations",old("conversations","c"),"c"),("messages",old("messages","m"),"m")); db.execute("INSERT INTO conversations VALUES (?,'codex','legacy',NULL,NULL,NULL,NULL,NULL,NULL,'{}')",[rows[0][1]]); db.execute("INSERT INTO messages VALUES (?,?,'user','findable',NULL,NULL,NULL,'{}',NULL,NULL)",[rows[1][1],rows[0][1]]); db.executemany("INSERT INTO remote.row_origins VALUES (?,?,'workspace',?,'device',?,'event','key',NULL,NULL)",[(table,physical,user,source) for table,physical,source in rows]); rebuild_fts_index(db); db.execute("DELETE FROM fts_main_messages.docs WHERE name=?",[rows[1][1]]); db.execute("UPDATE core_schema SET version=1"); db.close(); real=core_module.rebuild_fts_index; monkeypatch.setattr(core_module,"rebuild_fts_index",lambda _:(_ for _ in ()).throw(RuntimeError("interrupted"))); db=duckdb.connect(str(path))
    with pytest.raises(RuntimeError,match="interrupted"): init_schema(db)
    assert db.execute("SELECT version FROM core_schema").fetchone()[0]==1 and db.execute("SELECT state FROM core_migrations WHERE name='remote_ids'").fetchone()[0]=="fts" and db.execute("SELECT id FROM messages").fetchone()[0]==remote_id(user,"messages","m")
    db.execute("DELETE FROM core_migrations")  # simulate interruption under the migration build that lacked a durable FTS marker
    with pytest.raises(RuntimeError,match="interrupted"): init_schema(db)
    assert db.execute("SELECT state FROM core_migrations WHERE name='remote_ids'").fetchone()[0]=="fts"
    monkeypatch.setattr(core_module,"rebuild_fts_index",real); init_schema(db); assert db.execute("SELECT version FROM core_schema").fetchone()[0]==2 and not db.execute("SELECT 1 FROM core_migrations").fetchone() and db.execute("SELECT fts_main_messages.match_bm25(?, 'findable')",[remote_id(user,"messages","m")]).fetchone()[0] is not None; db.close()


def test_v2_mixed_archive_keeps_unique_latest_author_scoped_revision(tmp_path):
    path=tmp_path/"mixed.db"; db=graph(path); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); workspace="w"; fields=ARCHIVE_COLUMNS["conversations"]; row=lambda title:logical_row("conversations",fields,["c","codex",title,"2026-01-01","2026-01-01",None,None,None,None,"{}"]); base,new=row("base"),row("new"); proofs=[row_proof(device,user,workspace,1,base),None]; proofs[1]=row_proof(device,user,workspace,1,new,proofs[0]["revision"]); physical=[digest(f"{workspace}:{user}:conversations:c")[:16],remote_id(user,"conversations","c")]
    for identity_,logical,proof in zip(physical,(base,new),proofs):
        pid=project_row_proof(db,proof,root["sign_public"],cert); values=[identity_,*(["codex",logical["data"]["title"],"2026-01-01","2026-01-01",None,None,None,None,"{}"])]; origin={"workspace_id":workspace,"author_user_id":user,"author_device_id":device["id"],"source_row_id":"c","source_event_id":proof["revision"],"content_key":"conversations:c","observed_at":None,"proof_id":pid}; project_archive_row(db,"conversations",fields,values,origin)
    db.execute("UPDATE core_schema SET version=1"); db.close(); db=duckdb.connect(str(path)); init_schema(db); assert db.execute("SELECT id,title FROM conversations").fetchall()==[(physical[1],"new")] and db.execute("SELECT physical_row_id,proof_id FROM remote.row_origins").fetchone()==(physical[1],digest(proofs[1])) and db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0]==2; db.close()


def test_v2_collapses_duplicate_messages_without_retokenizing_or_losing_embedding(tmp_path,monkeypatch):
    path=tmp_path/"messages.db"; db=graph(path); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); workspace="w"; old=digest(f"{workspace}:{user}:messages:m")[:16]; new=remote_id(user,"messages","m"); db.execute("INSERT INTO conversations VALUES ('c','codex','x',NULL,NULL,NULL,NULL,NULL,NULL,'{}')"); values=["m","c","user","findable",None,None,None,"{}",None]; logical=logical_row("messages",ARCHIVE_COLUMNS["messages"],values); proof=row_proof(device,user,workspace,1,logical); pid=project_row_proof(db,proof,root["sign_public"],cert); db.execute("INSERT INTO messages VALUES (?,'c','user','findable',NULL,NULL,NULL,'{}',?,NULL),(?,'c','user','findable',NULL,NULL,NULL,'{}',NULL,NULL)",[old,[1.0]*768,new]); origins=[{"workspace_id":workspace,"author_user_id":user,"author_device_id":device["id"],"source_row_id":"m","source_event_id":proof["revision"],"content_key":"messages:m","observed_at":None,"proof_id":pid} for _ in range(2)]; [project_archive_row(db,"messages",ARCHIVE_COLUMNS["messages"],row,origin) for row,origin in zip(([old,*values[1:]],[new,*values[1:]]),origins)]; db.execute("UPDATE messages SET embedding=? WHERE id=?",[[1.0]*768,old]); rebuild_fts_index(db); db.execute("UPDATE core_schema SET version=1"); db.close(); monkeypatch.setattr(core_module,"rebuild_fts_index",lambda _:(_ for _ in ()).throw(AssertionError("FTS rebuild was unnecessary"))); db=duckdb.connect(str(path)); init_schema(db)
    assert db.execute("SELECT id,embedding[1] FROM messages").fetchone()==(new,1.0) and db.execute("SELECT count(*) FROM fts_main_messages.docs").fetchone()[0]==db.execute("SELECT num_docs FROM fts_main_messages.stats").fetchone()[0]==1 and db.execute("SELECT max(df) FROM fts_main_messages.dict").fetchone()[0]==1 and db.execute("SELECT fts_main_messages.match_bm25(?,'findable')",[new]).fetchone()[0] is not None; db.close()


def test_provenance_upgrade_preserves_edit_evidence_and_removes_unused_graph_tables(tmp_path):
    db=graph(tmp_path/"graph.db"); db.execute("ALTER TABLE provenance.file_edit_files RENAME COLUMN old_content_hash TO before_hash"); db.execute("ALTER TABLE provenance.file_edit_files RENAME COLUMN new_content_hash TO after_hash"); db.execute("ALTER TABLE provenance.git_checkpoints DROP COLUMN capture_source"); db.execute("CREATE TABLE provenance.assertions(id VARCHAR)"); db.execute("CREATE TABLE provenance.capture_gaps(id VARCHAR)"); db.execute("INSERT INTO provenance.repositories VALUES ('r','l','[]','[]',NULL,'2026-01-01')"); db.execute("INSERT INTO provenance.file_edit_files VALUES ('e','f','old','new','captured_exact')"); db.execute("DROP TABLE provenance.local_facts"); init_schema(db)
    assert db.execute("SELECT old_content_hash,new_content_hash FROM provenance.file_edit_files").fetchone()==("old","new") and db.execute("SELECT * FROM provenance.local_facts").fetchone()==("repository.observed","r") and not {"assertions","capture_gaps"}&{r[0] for r in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='provenance'").fetchall()}


def test_archive_generation_is_transactional_and_counts_only_owned_rows(tmp_path):
    db=duckdb.connect(str(tmp_path/"core.db")); init_schema(db); initial=archive_state(db); row=["c","codex","one","2026-01-01","2026-01-01",None,None,None,None,"{}"]
    db.execute("BEGIN"); project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],row); assert archive_state(db)[1:]==(initial[1]+1,1) and archive_changes(db,initial[1])[1]==[("conversations","c")]; db.execute("ROLLBACK"); assert archive_state(db)==initial and archive_changes(db,initial[1])[1]==[]


def test_mixed_message_projection_preserves_only_current_embeddings(tmp_path):
    db=graph(tmp_path/"core.db"); db.execute("INSERT INTO conversations VALUES ('c','codex','x',NULL,NULL,NULL,NULL,NULL,NULL,'{}')"); db.executemany("INSERT INTO messages VALUES (?, 'c','user','old',NULL,NULL,NULL,'{}',?,NULL)",[("changed",[1.0]*768),("same",[2.0]*768)]); row=lambda mid,content:([mid,"c","user",content,None,None,None,"{}",None],None)
    expected=[("changed","new",True,None),("same","old",False,2.0)]; core_module.project_archive_rows(db,"messages",ARCHIVE_COLUMNS["messages"],[row("changed","new"),row("same","old")]); assert db.execute("SELECT id,content,embedding IS NULL,embedding[1] FROM messages ORDER BY id").fetchall()==expected
    db.execute("DELETE FROM messages"); db.executemany("INSERT INTO messages VALUES (?, 'c','user','old',NULL,NULL,NULL,'{}',?,NULL)",[("changed",[1.0]*768),("same",[2.0]*768)]); core_module.upsert(db,core_module.ParseResult(msgs=[dict(zip(ARCHIVE_COLUMNS["messages"],r[0])) for r in (row("changed","new"),row("same","old"))])); assert db.execute("SELECT id,content,embedding IS NULL,embedding[1] FROM messages WHERE id IN ('changed','same') ORDER BY id").fetchall()==expected


def test_row_proofs_and_signers_are_durable_idempotent_core_metadata(tmp_path):
    db=graph(tmp_path/"graph.db"); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); cert=certificate(root,user,device); row=logical_row("messages",identity="m",state="deleted"); proof=row_proof(device,user,"origin",2,row); other=row_proof(device,user,"origin",2,logical_row("messages",identity="n",state="deleted")); generation=archive_state(db)[1]; db.execute("BEGIN"); pid=project_row_proof(db,proof,root["sign_public"],cert); assert project_row_proof(db,proof,root["sign_public"],cert)==pid and project_row_proofs(db,[proof,other],root["sign_public"],cert)==[pid,digest(other)]; db.execute("COMMIT")
    assert db.execute("SELECT workspace_id,row_kind,source_row_id,state,revision,previous_revision FROM remote.row_proofs WHERE source_row_id='m'").fetchone()==("origin","messages","m","deleted",proof["revision"],None) and db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0]==2 and db.execute("SELECT COUNT(*) FROM remote.row_signers").fetchone()[0]==1 and archive_state(db)[1]==generation and project_row_proof(db,proof,root["sign_public"],certificate(root,user,device))==pid
    with pytest.raises(ValueError,match="signer conflict"): project_row_proof(db,proof,identity("other")["sign_public"],cert)


def test_core_upgrade_adds_origin_proof_link_without_losing_attribution(tmp_path):
    db=graph(tmp_path/"graph.db"); db.execute("ALTER TABLE remote.row_origins DROP COLUMN proof_id"); db.execute("INSERT INTO remote.row_origins VALUES ('messages','p','w','u','d','s','e','k','2026-01-01')"); init_schema(db); assert db.execute("SELECT workspace_id,author_user_id,proof_id FROM remote.row_origins").fetchone()==("w","u",None)


def test_core_upgrade_backfills_proof_authorization_workspace(tmp_path):
    db=graph(tmp_path/"graph.db"); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); proof=row_proof(device,user,"origin",1,logical_row("messages",identity="m",state="deleted")); cert=certificate(root,user,device); project_row_proof(db,proof,root["sign_public"],cert); db.execute("ALTER TABLE remote.row_proofs DROP COLUMN authorization_workspace_id"); init_schema(db); project_row_proof(db,row_proof(device,user,"origin",1,logical_row("messages",identity="n",state="deleted")),root["sign_public"],cert); assert db.execute("SELECT workspace_id,authorization_workspace_id FROM remote.row_proofs ORDER BY source_row_id").fetchall()==[("origin","origin"),("origin","origin")]


def test_core_upgrade_indexes_existing_retained_attachment_body(tmp_path):
    body=tmp_path/"body"; body.write_bytes(b"retained"); db=graph(tmp_path/"graph.db"); db.execute("INSERT INTO conversations VALUES ('c','codex','x','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); db.execute("INSERT INTO messages VALUES ('m','c','user','x',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.execute("INSERT INTO attachments VALUES ('a','m','body',NULL,NULL,?,NULL,'2026-01-01')",[str(body)]); init_schema(db); assert db.execute("SELECT content_hash,size FROM attachment_bodies").fetchone()==(digest(b"retained"),8) and db.execute("SELECT size FROM attachments").fetchone()[0]==8


def test_workspace_authorization_chain_is_normalized_and_conflicts_fail(tmp_path):
    db=graph(tmp_path/"graph.db"); controls=[{"workspace":"w","revision":1,"epoch":1,"members":{"u":"admin"}},{"workspace":"w","revision":2,"epoch":2,"members":{"u":"admin","v":"member"}}]; assert project_workspace_controls(db,controls)==project_workspace_controls(db,controls)==2 and db.execute("SELECT revision,epoch FROM remote.workspace_controls ORDER BY revision").fetchall()==[(1,1),(2,2)]
    with pytest.raises(ValueError,match="control conflict"): project_workspace_controls(db,[{**controls[1],"members":{"u":"member"}}])


def test_git_checkpoint_is_capture_observation_not_dirty_path_gap(tmp_path):
    root=repo(tmp_path/"repo",content="old\n"); (root/"x.py").write_text("new\n"); git(root,"add","x.py"); git(root,"commit","-qm","agent edit"); (root/"manual.py").write_text("outside capture\n")
    path=tmp_path/"core.db"; db=core(path,root,[(root/"x.py","write","new\n",None)]); db.close(); capture(path); db=duckdb.connect(str(path))
    checkpoint=query(db,"checkpoint_states")[0]; assert "manual.py" in json.loads(checkpoint["paths"]) and checkpoint["capture_source"]=="sync" and query(db,"commit_conversations",git(root,"rev-parse","HEAD"))[0]["conversation"]=="c"
    assert query(db,"file_history","x.py")[0]["evidence"]=="captured_exact" and query(db,"file_history","x.py")[0]["prompt"]=="make the cross-repo change"


def test_provenance_failure_rolls_back_only_enrichment(tmp_path,monkeypatch):
    root=repo(tmp_path/"repo"); path=tmp_path/"core.db"; db=core(path,root,[(root/"x.py","write","one\n",None)]); write=core_module.project_provenance; generation=archive_state(db)[1]; db.close()
    def fail(conn,value,*args):
        if value["kind"]=="file.observed": raise OSError("git evidence failed")
        return write(conn,value,*args)
    monkeypatch.setattr(core_module,"project_provenance",fail)
    with pytest.raises(OSError,match="evidence"): capture(path)
    db=duckdb.connect(str(path))
    assert db.execute("SELECT COUNT(*) FROM file_edits").fetchone()[0]==1 and db.execute("SELECT COUNT(*) FROM provenance.repositories").fetchone()[0]==0 and db.execute("SELECT COUNT(*) FROM provenance.repository_checkouts").fetchone()[0]==0 and archive_state(db)[1]==generation


def test_checkpoint_diff_uses_local_git_evidence(tmp_path):
    root=repo(tmp_path/"repo",content="one\n"); path=tmp_path/"core.db"; db=core(path,root,[(root/"x.py","write","one\n",None)]); db.close(); first=capture(path); cp1=next(r["payload"]["id"] for r in first if r["kind"]=="git.checkpoint"); (root/"x.py").write_text("two\n"); git(root,"add","x.py"); git(root,"commit","-qm","second"); second=capture(path); cp2=next(r["payload"]["id"] for r in second if r["kind"]=="git.checkpoint"); db=duckdb.connect(str(path))
    result=query(db,"checkpoint_diff",f"{cp1}..{cp2}")[0]; assert result["head_before"]!=result["head_after"] and result["changed"]==["M\tx.py"]


def test_concurrent_edits_remain_version_branches(tmp_path):
    db=graph(tmp_path/"graph.db"); db.execute("INSERT INTO conversations VALUES ('c2','codex','a','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}'),('c3','codex','b','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); db.execute("INSERT INTO messages VALUES ('m2','c2','assistant','',NULL,'2026-01-01',NULL,'{}',NULL,NULL),('m3','c3','assistant','',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e2','m2','shared.py','edit','a','2026-01-01','base'),('e3','m3','shared.py','edit','b','2026-01-01','base')")
    a,b=identity("a"),identity("b"); file_id=digest({"repository":None,"path":"shared.py"}); project(db,event(a,1,"file.observed",file_id,{"id":file_id,"repository":None,"path":"shared.py","kind":"external"},[],"2026-01-01T00:00:00Z"))
    for i,(device,after) in enumerate(((a,"after-a"),(b,"after-b")),2): payload={"id":f"e{i}","turn":f"m{i}","file":file_id,"repository":None,"old_content_hash":"same-base","new_content_hash":after,"evidence":"captured_exact"}; project(db,event(device,i,"edit.observed",payload["id"],payload,[],"2026-01-01T00:00:00Z"))
    rows=query(db,"file_history","shared.py"); assert len(rows)==2 and {r["new_content_hash"] for r in rows}=={"after-a","after-b"} and {r["old_content_hash"] for r in rows}=={"same-base"}


def test_repository_identity_distinguishes_fork_but_preserves_lineage_and_unborn_repo(tmp_path):
    source=repo(tmp_path/"source"); git(source,"remote","add","origin","https://example.com/acme/repo.git"); clone=tmp_path/"clone"; subprocess.run(("git","clone","-q",str(source),str(clone)),check=True); git(clone,"remote","set-url","origin","https://example.com/acme/repo.git"); fork=tmp_path/"fork"; subprocess.run(("git","clone","-q",str(source),str(fork)),check=True); git(fork,"remote","set-url","origin","https://example.com/other/fork.git")
    a,b,c=repository(source),repository(clone),repository(fork); assert a["id"]==b["id"]!=c["id"] and a["lineage"]==b["lineage"]==c["lineage"]
    empty=tmp_path/"empty"; empty.mkdir(); git(empty,"init","-q"); (empty/"new.py").write_text("new\n"); observed=repository(empty); assert observed["head"]=="" and observed["lineage"] is None
    path=tmp_path/"empty-core.db"; db=core(path,empty,[(empty/"new.py","write","new\n",None)]); db.close(); records=capture(path); assert any(r["kind"]=="git.checkpoint" and r["payload"]["head"]=="" for r in records)


def test_semantic_capture_ids_are_existing_archive_identities(tmp_path):
    root=repo(tmp_path/"repo"); path=tmp_path/"core.db"; db=core(path,root,[(root/"x.py","write","one\n",None)]); db.close(); records=capture(path); one=lambda kind:next(r["payload"]["id"] for r in records if r["kind"]==kind)
    assert one("edit.observed")=="e0" and one("file.observed")==digest({"repository":repository(root)["id"],"path":"x.py"})


def test_provenance_git_observation_does_not_hold_duckdb_lock(tmp_path,monkeypatch):
    root=repo(tmp_path/"repo"); path=tmp_path/"core.db"; db=core(path,root,[(root/"x.py","write","one\n",None)]); db.close(); run=core_module._git_run
    def unlocked(repo,*args): subprocess.run((sys.executable,"-c","import duckdb,sys; duckdb.connect(sys.argv[1]).close()",str(path)),check=True); return run(repo,*args)
    monkeypatch.setattr(core_module,"_git_run",unlocked); capture(path)
