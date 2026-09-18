"""The one-time relay boundary resets old replicas; explicit backups preserve the old file."""
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


@pytest.mark.parametrize('version',[1,2])
def test_upgrade_resets_encrypted_history_once_with_optional_explicit_backup(tmp_path,version):
    source,target=tmp_path/'old.db',tmp_path/'backup.db'
    author,envelopes=legacy_relay(source)
    with closing(sqlite3.connect(source)) as db:
        db.execute(f'PRAGMA user_version={version}')
        db.commit()
    server.main(['backup','--db',str(source),'--output',str(target)])
    assert stat.S_IMODE(target.stat().st_mode)==0o600
    with closing(sqlite3.connect(target)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0]==version
        assert db.execute('SELECT count(*) FROM events').fetchone()[0]==1
    with closing(server.connect(source)) as db:
        generation=server.action(db,{'op':'status'})['generation']
        assert db.execute('PRAGMA user_version').fetchone()[0]==server.STORAGE_VERSION
        assert all(db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]==0 for table in (*server.ENCRYPTED_TABLES,'users','devices','workspaces'))
        fresh=account(db,'alice')
    with closing(server.connect(source)) as db:
        assert server.action(db,{'op':'status'})['generation']==generation
        assert server.auth(db,fresh['token'])['id']==fresh['device']['id']
        with pytest.raises(PermissionError): server.auth(db,author['token'])


def test_interrupted_relay_reset_rolls_back_all_old_data(tmp_path,monkeypatch):
    source=tmp_path/'old.db'
    author,envelopes=legacy_relay(source)
    def fail(db): raise KeyboardInterrupt
    with monkeypatch.context() as patch:
        patch.setattr(server,'binary_schema',fail)
        with pytest.raises(KeyboardInterrupt): server.connect(source)
    with closing(sqlite3.connect(source)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM users').fetchone()[0]==1
        assert json.loads(db.execute('SELECT envelope FROM events').fetchone()[0])==envelopes['events']
    with closing(server.connect(source)) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0]==server.STORAGE_VERSION
        assert db.execute('SELECT count(*) FROM users').fetchone()[0]==0
