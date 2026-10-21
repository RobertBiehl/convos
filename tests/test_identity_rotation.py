"""A new sync identity on an archive synced by a retired identity starts clean instead of auditing stale signatures."""

import shutil, sqlite3

import duckdb, pytest
import ai_convos.cli as core
from ai_convos_remote import repull_once, setup_client, sync_once
from ai_convos_remote.projection import audit_rows
from ai_convos_remote_server import connect as server_connect
from tests.test_remote_client import transport
from tests.test_request_blobs import events, ingest, repo, write

ROWS="SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM messages),(SELECT count(*) FROM tool_calls),(SELECT count(*) FROM file_edits)"
def query(path,sql):
    with duckdb.connect(str(path),read_only=True) as db: return db.execute(sql).fetchall()
def ready(root): return {l for l,e in sqlite3.connect(root/"remote/state.db").execute("SELECT lifecycle,error FROM sync_states").fetchall()}=={"ready"}
def synced_archive(tmp_path,monkeypatch):
    monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None)
    monkeypatch.setattr("ai_convos_remote.request",transport(server_connect(tmp_path/"old.db")))
    a,session,rows=tmp_path/"a",tmp_path/"s1.jsonl",events(repo(tmp_path))
    old,recovery=setup_client("http://old","robert","laptop",root=a)
    (a/"data").mkdir(parents=True); write(session,rows)
    with duckdb.connect(str(a/"data/convos.db")) as db: core.init_schema(db); ingest(db,session)
    sync_once(a,True)
    return a,old,recovery,session,rows

def test_new_identity_retires_previous_sync_metadata(tmp_path,monkeypatch):
    a,old,_,session,rows=synced_archive(tmp_path,monkeypatch); path,b=a/"data/convos.db",tmp_path/"b"
    before=query(path,ROWS)
    assert query(path,"SELECT user_id FROM archive_sync")==[(old["user"],)] and query(path,"SELECT count(*) FROM remote.row_proofs")[0][0]
    shutil.move(a/"remote",tmp_path/"retired-remote")
    monkeypatch.setattr("ai_convos_remote.request",transport(relay:=server_connect(tmp_path/"new.db")))
    new,recovery=setup_client("http://new","robert","laptop",root=a)
    assert query(path,"SELECT user_id,device_id FROM archive_sync")==[(new["user"],new["device"]["id"])] and query(path,ROWS)==before
    assert not query(path,f"SELECT 1 FROM remote.row_proofs WHERE author_user_id='{old['user']}' UNION ALL SELECT 1 FROM remote.row_owners WHERE author='{old['user']}' UNION ALL SELECT 1 FROM remote.local_row_bases WHERE author='{old['user']}'")
    assert path.with_name(f"convos.db.pre-identity-{old['user']}.bak").is_file()
    sync_once(a,True); repull_once(a)
    assert ready(a) and query(path,ROWS)==before and relay.execute("SELECT count(*) FROM row_replicas").fetchone()[0]
    assert not (audit:=audit_rows(path,local_user=new["user"]))["totals"].get("unavailable",0) and not audit["totals"].get("retained_variants",0)
    setup_client("http://new","robert","desktop",recovery,root=b); sync_once(b,True)
    assert query(b/"data/convos.db",ROWS)==before
    write(session,rows+[{"type":"assistant","uuid":"a3","parentUuid":"a2","timestamp":"2026-10-16T10:00:05Z","message":{"role":"assistant","model":"m","content":[{"type":"text","text":"more"}]}}])
    with duckdb.connect(str(path)) as db: ingest(db,session)
    sync_once(a,True); sync_once(b,True)
    assert query(path,ROWS)[0][1]==query(b/"data/convos.db",ROWS)[0][1]==before[0][1]+1 and ready(a) and ready(b)

def test_new_identity_refuses_rows_received_from_previous_identity_devices(tmp_path,monkeypatch):
    a,old,recovery,_,_=synced_archive(tmp_path,monkeypatch); b=tmp_path/"b"; path=b/"data/convos.db"
    setup_client("http://old","robert","desktop",recovery,root=b); sync_once(b,True)
    pinned,proofs=query(path,"SELECT user_id,device_id FROM archive_sync"),query(path,"SELECT count(*) FROM remote.row_proofs")
    assert query(path,"SELECT count(*) FROM remote.row_origins")[0][0]
    shutil.move(b/"remote",tmp_path/"retired-remote")
    with pytest.raises(ValueError,match="recover that identity instead"): setup_client("http://old","robert","desktop",root=b)
    assert query(path,"SELECT user_id,device_id FROM archive_sync")==pinned and query(path,"SELECT count(*) FROM remote.row_proofs")==proofs

def test_cli_tracebacks_never_print_locals():
    assert core.app.pretty_exceptions_show_locals is False
