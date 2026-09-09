"""Binary relay migrations preserve encrypted history and publish verified copies only."""
import json, sqlite3, stat
from contextlib import closing

import pytest
import ai_convos_remote_server as server
from ai_convos_remote.protocol import canon, event, logical_row, row_proof, seal_event, seal_origin, seal_replica
from tests.test_remote_server import account, create_ws


def legacy_relay(path):
    with closing(server.connect(path)) as db:
        author=account(db,"alice")
        key,workspace=bytes(range(32)),"personal"
        control=create_ws(db,author,workspace,key,"personal")
        for table in server.ENCRYPTED_TABLES:
            db.execute(f"ALTER TABLE {table} DROP COLUMN ciphertext")
            db.execute(f"ALTER TABLE {table} DROP COLUMN wire_size")
        db.execute("PRAGMA user_version=1")
        row=logical_row("messages",identity="message",state="deleted")
        proof=row_proof(author["device"],author["user"],workspace,1,row)
        replica=seal_replica(row,proof,workspace,1,key,author["device"]["id"],compression="none")
        envelopes={"events":seal_event(event(author["device"],1,"test","message",{"text":"private data"*1000}),workspace,1,key),
                   "row_replicas":replica,"semantic_replicas":replica,
                   "origin_bundles":seal_origin([control],workspace,1,key,author["device"]["id"])}
        for table,env in envelopes.items():
            cursor=db.execute("INSERT INTO ledger_cursors DEFAULT VALUES").lastrowid
            prefix=(cursor,workspace,env["event"],env["author"],1,env["seq"]) if table=="events" else (cursor,workspace,env["origin"] if table=="origin_bundles" else env["replica"],1,env["uploader"])
            values=(*prefix,json.dumps(env),server.digest(env),123.0,*(() if table=="events" else (124.0,)))
            db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in values)})",values)
        db.execute("INSERT INTO replica_usage VALUES (?,?,?)",(workspace,author["device"]["id"],2*len(canon(replica))))
        db.commit()
        db.execute("PRAGMA journal_mode=DELETE")
    return author,envelopes


def test_binary_migration_preserves_every_envelope_cursor_and_source(tmp_path,capsys):
    source,target=tmp_path/"old.db",tmp_path/"binary.db"
    author,envelopes=legacy_relay(source)
    original,modified=source.read_bytes(),source.stat().st_mtime_ns
    server.main(["migrate","--db",str(source),"--output",str(target)])
    assert capsys.readouterr().out.strip()==str(target)
    assert source.read_bytes()==original and source.stat().st_mtime_ns==modified
    assert stat.S_IMODE(target.stat().st_mode)==0o600
    with closing(server.connect(target)) as db,closing(sqlite3.connect(source)) as old:
        assert db.execute("PRAGMA user_version").fetchone()[0]==2
        for table,env in envelopes.items():
            row=db.execute(f"SELECT * FROM {table}").fetchone()
            assert isinstance(row["ciphertext"],bytes) and "ciphertext" not in json.loads(row["envelope"])
            assert server.stored_envelope(row)==env and row["wire_hash"]==server.digest(env)
            assert row["wire_size"]==len(canon(env))
            assert tuple(row[k] for k in row.keys() if k not in ("envelope","ciphertext","wire_size"))==tuple(v for k,v in zip([d[0] for d in old.execute(f"SELECT * FROM {table}").description],old.execute(f"SELECT * FROM {table}").fetchone()) if k!="envelope")
        tables=[r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'") if r[0] not in (*server.ENCRYPTED_TABLES,"replica_usage")]
        for table in tables: assert [tuple(r) for r in db.execute(f"SELECT * FROM {table}")]==old.execute(f"SELECT * FROM {table}").fetchall()
        stored=db.execute("SELECT SUM(LENGTH(CAST(envelope AS BLOB))+LENGTH(ciphertext)) FROM (SELECT envelope,ciphertext FROM row_replicas UNION ALL SELECT envelope,ciphertext FROM semantic_replicas)").fetchone()[0]
        assert db.execute("SELECT bytes FROM replica_usage").fetchone()[0]==stored
        token=author["token"]
        assert server.action(db,{"op":"pull","workspace":"personal"},token)["events"][0]["envelope"]==envelopes["events"]
        assert server.action(db,{"op":"replica_pull","workspace":"personal","semantic":True},token)["replicas"]==[{"cursor":2,"envelope":envelopes["row_replicas"]},{"cursor":3,"envelope":envelopes["semantic_replicas"]}]
        assert server.action(db,{"op":"origin_pull","workspace":"personal"},token)["origins"]==[{"cursor":4,"envelope":envelopes["origin_bundles"]}]
        assert not server.action(db,{"op":"upload","envelope":envelopes["events"]},token)["created"]
    again=tmp_path/"binary-again.db"
    server.main(["migrate","--db",str(target),"--output",str(again)])
    with closing(server.connect(again)) as db:
        for table,env in envelopes.items(): assert server.stored_envelope(db.execute(f"SELECT * FROM {table}").fetchone())==env


@pytest.mark.parametrize("damage",["wire_hash","ciphertext"])
def test_migration_rejects_corruption_without_publishing_a_partial_copy(tmp_path,damage):
    source,target=tmp_path/"old.db",tmp_path/"binary.db"
    legacy_relay(source)
    with closing(sqlite3.connect(source)) as db:
        if damage=="wire_hash": db.execute("UPDATE row_replicas SET wire_hash=?",("0"*64,))
        else:
            env=json.loads(db.execute("SELECT envelope FROM row_replicas").fetchone()[0])
            env["ciphertext"]+="="
            db.execute("UPDATE row_replicas SET envelope=?,wire_hash=?",(json.dumps(env),server.digest(env)))
        db.commit()
    original=source.read_bytes()
    with pytest.raises(ValueError): server.main(["migrate","--db",str(source),"--output",str(target)])
    assert source.read_bytes()==original and not target.exists()
    assert not list(tmp_path.glob(".binary.db.*"))


def test_migration_never_overwrites_existing_output_and_old_server_state_is_explicit(tmp_path):
    source,target=tmp_path/"old.db",tmp_path/"binary.db"
    legacy_relay(source)
    original=source.read_bytes()
    with pytest.raises(ValueError,match="migration required"): server.connect(source)
    assert source.read_bytes()==original
    target.write_bytes(b"existing backup")
    with pytest.raises(SystemExit): server.main(["migrate","--db",str(source),"--output",str(target)])
    assert target.read_bytes()==b"existing backup" and source.read_bytes()==original


def test_migration_never_overwrites_output_created_during_conversion(tmp_path,monkeypatch):
    source,target=tmp_path/"old.db",tmp_path/"binary.db"
    legacy_relay(source)
    original,migrate=source.read_bytes(),server.migrate_storage
    def concurrent_output(db):
        migrate(db)
        target.write_bytes(b"concurrently published backup")
    monkeypatch.setattr(server,"migrate_storage",concurrent_output)
    with pytest.raises(FileExistsError): server.main(["migrate","--db",str(source),"--output",str(target)])
    assert target.read_bytes()==b"concurrently published backup" and source.read_bytes()==original
    assert not list(tmp_path.glob(".binary.db.*"))


def test_migration_accounting_does_not_sort_ciphertext(tmp_path):
    source=tmp_path/"legacy.db"
    author,envelopes=legacy_relay(source)
    env={**envelopes["row_replicas"],"ciphertext":server.b64(b"x"*(128*1024))}
    with closing(sqlite3.connect(source)) as db:
        db.execute("UPDATE row_replicas SET envelope=?,wire_hash=?",(json.dumps(env),server.digest(env)))
        db.commit()
        expected=sum(len(server.split_envelope(e)[0].encode())+len(server.split_envelope(e)[1]) for e in (env,envelopes["semantic_replicas"]))
        # A diagnostic limit at accounting time rejects payload-sized sorter records.
        db.set_trace_callback(lambda sql:db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,64*1024) if sql.startswith("INSERT INTO replica_usage") else None)
        server.migrate_storage(db)
        assert db.execute("SELECT bytes FROM replica_usage WHERE uploader=?",(author["device"]["id"],)).fetchone()[0]==expected
