"""Relay backup preserves the source and publishes only complete private snapshots."""
import os, sqlite3, stat
from contextlib import closing

import pytest
import ai_convos_remote_server as server


def create_database(path,mode="DELETE"):
    db=sqlite3.connect(path)
    db.execute(f"PRAGMA journal_mode={mode}")
    db.execute("PRAGMA wal_autocheckpoint=0")
    db.executescript("CREATE TABLE payload(id INTEGER PRIMARY KEY,body BLOB); PRAGMA user_version=77;")
    db.execute("INSERT INTO payload VALUES (?,?)",(1,b"opaque\x00payload\xff"))
    db.commit()
    return db


def test_missing_backup_source_does_not_create_database_or_directories(tmp_path):
    source,output=tmp_path/"missing"/"source.db",tmp_path/"backups"/"copy.db"
    with pytest.raises(SystemExit) as error: server.main(["backup","--db",str(source),"--output",str(output)])
    assert error.value.code==2
    assert not source.parent.exists() and not output.parent.exists()


@pytest.mark.parametrize("alias",["same","symlink","hardlink"])
def test_backup_rejects_source_alias_without_modifying_it(tmp_path,alias):
    source=tmp_path/"source.db"
    create_database(source).close()
    output=source if alias=="same" else tmp_path/"alias.db"
    if alias=="symlink": output.symlink_to(source)
    if alias=="hardlink": os.link(source,output)
    before,modified=source.read_bytes(),source.stat().st_mtime_ns
    with pytest.raises(SystemExit) as error: server.main(["backup","--db",str(source),"--output",str(output)])
    assert error.value.code==2
    assert source.read_bytes()==before and source.stat().st_mtime_ns==modified
    assert source.samefile(output) and output.is_symlink()==(alias=="symlink")


@pytest.mark.parametrize("suffix",["-wal","-shm","-journal"])
def test_backup_does_not_replace_an_output_with_sqlite_sidecars(tmp_path,suffix):
    source,output=tmp_path/"source.db",tmp_path/"copy.db"
    create_database(source).close()
    output.write_bytes(b"old backup")
    sidecar=output.with_name(output.name+suffix)
    sidecar.write_bytes(b"old database state")
    before={p:p.read_bytes() for p in (source,output,sidecar)}
    with pytest.raises(SystemExit) as error: server.main(["backup","--db",str(source),"--output",str(output)])
    assert error.value.code==2
    assert all(p.read_bytes()==value for p,value in before.items())


@pytest.mark.parametrize("mode",["DELETE","WAL"])
def test_backup_is_read_only_complete_and_private(tmp_path,monkeypatch,capsys,mode):
    source,output=tmp_path/"relay #1?.db",tmp_path/"backups"/"copy.db"
    output.parent.mkdir()
    output.write_bytes(b"old backup")
    output.chmod(0o644)
    with closing(create_database(source,mode)) as live:
        before={p:p.read_bytes() for p in (source,source.with_name(source.name+"-wal")) if p.exists()}
        original_connect=sqlite3.connect
        class ReadOnlySource(sqlite3.Connection):
            def backup(self,destination):
                with pytest.raises(sqlite3.OperationalError,match="readonly"): self.execute("INSERT INTO payload VALUES (2,'unexpected write')")
                self.rollback()
                return super().backup(destination)
        monkeypatch.setattr(server.sqlite3,"connect",lambda *args,**kwargs:original_connect(*args,**(kwargs|{"factory":ReadOnlySource})))
        monkeypatch.setattr(server,"connect",lambda *args:pytest.fail("backup must not initialize the source"))
        server.main(["backup","--db",str(source),"--output",str(output)])
        assert all(p.read_bytes()==data for p,data in before.items())
        assert live.execute("SELECT * FROM payload").fetchall()==[(1,b"opaque\x00payload\xff")]
    assert capsys.readouterr().out.strip()==str(output)
    assert stat.S_IMODE(output.stat().st_mode)==0o600
    with closing(sqlite3.connect(output.as_uri()+"?mode=ro",uri=True)) as db:
        assert db.execute("PRAGMA quick_check").fetchall()==[("ok",)]
        assert db.execute("PRAGMA user_version").fetchone()==(77,)
        assert db.execute("SELECT * FROM payload").fetchall()==[(1,b"opaque\x00payload\xff")]
        assert db.execute("PRAGMA journal_mode").fetchone()==("delete",)
    assert list(output.parent.iterdir())==[output]


@pytest.mark.parametrize("failure",["copy","publish"])
def test_failed_backup_keeps_existing_destination_and_removes_stage(tmp_path,monkeypatch,failure):
    source,output=tmp_path/"source.db",tmp_path/"backups"/"copy.db"
    create_database(source).close()
    before=source.read_bytes()
    output.parent.mkdir()
    output.write_bytes(b"previous complete backup")
    original_connect=sqlite3.connect
    class BrokenSource(sqlite3.Connection):
        def backup(self,destination):
            destination.execute("CREATE TABLE partial(value TEXT)")
            destination.commit()
            raise OSError("injected copy failure")
    def connect(*args,**kwargs): return original_connect(*args,**(kwargs|({"factory":BrokenSource} if kwargs.get("uri") else {})))
    def replace(*args): raise OSError("injected publish failure")
    if failure=="copy": monkeypatch.setattr(server.sqlite3,"connect",connect)
    else: monkeypatch.setattr(server.os,"replace",replace)
    with pytest.raises(OSError,match="injected"): server.main(["backup","--db",str(source),"--output",str(output)])
    assert source.read_bytes()==before and output.read_bytes()==b"previous complete backup"
    assert list(output.parent.iterdir())==[output]
