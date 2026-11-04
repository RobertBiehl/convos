"""Schema 20 rewrites the archive into small row groups: disk use, refusal on a full disk, and crash safety."""
import contextlib, duckdb, os, signal, subprocess, sys, time
import pytest
from ai_convos import cli
SHAPE = """SELECT 'table',schema_name,table_name,sql FROM duckdb_tables() WHERE database_name=current_database() UNION ALL SELECT 'index',schema_name,index_name,sql FROM duckdb_indexes() WHERE database_name=current_database()
UNION ALL SELECT 'view',schema_name,view_name,sql FROM duckdb_views() WHERE database_name=current_database() AND NOT internal UNION ALL SELECT 'macro',schema_name,function_name,macro_definition FROM duckdb_functions() WHERE database_name=current_database() AND function_type LIKE '%macro%' ORDER BY ALL"""
class Killed(BaseException): pass  # process death: no handler runs

def v19(path, rows=60000):  # written without the row group size, as builds before schema 20 did, then grown by an update and a delete
    with contextlib.closing(duckdb.connect(str(path))) as db:
        cli.init_schema(db); db.execute(f"UPDATE core_schema SET version=19; INSERT INTO conversations(id,source) SELECT 'c'||range,'claude-code' FROM range(100); INSERT INTO messages(id,conversation_id,role,content) SELECT 'm'||range,'c'||(range%100),'user',repeat(md5(range::VARCHAR),8) FROM range({rows}); INSERT INTO tool_calls(id,message_id,output) SELECT 't'||range,'m'||range,to_json(repeat(md5((-range)::VARCHAR),8)) FROM range({rows})")
        db.execute("UPDATE messages SET thinking=md5(id) WHERE id LIKE '%7'; DELETE FROM tool_calls WHERE id LIKE '%3'; CREATE INDEX user_messages_role ON messages(role); CREATE VIEW user_long AS SELECT id FROM messages WHERE length(content)>100; PRAGMA create_fts_index('messages','id','content','thinking',overwrite=1); CHECKPOINT")

def state(path):  # everything a migration must keep: catalog SQL (tables, indexes, views, FTS macros) and every row of every table
    with contextlib.closing(cli.open_db(path, purpose="test.rewrite")) as db:
        tables = [f'"{s}"."{t}"' for s, t in db.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE database_name=current_database() AND table_name!='core_schema' ORDER BY ALL").fetchall()]
        return db.execute("SELECT version FROM core_schema").fetchone()[0], db.execute(SHAPE).fetchall(), {t: db.execute(f"SELECT count(*),bit_xor(hash(x)) FROM {t} x").fetchone() for t in tables}, db.execute("LOAD fts; SELECT count(*) FROM messages WHERE fts_main_messages.match_bm25(id,?) IS NOT NULL", [db.execute("SELECT md5('42')").fetchone()[0]]).fetchone()[0]

def migrate(path):
    with contextlib.closing(cli.open_db(path, purpose="test.rewrite")) as db: cli.init_schema(db)

def test_v20_leaves_a_compact_file_with_every_row_and_object(tmp_path, monkeypatch):
    v19(path := tmp_path / "convos.db"); version, *kept = state(path); size, peak, usage = path.stat().st_size, [0], cli.shutil.disk_usage
    sizes = lambda: sum(p.stat().st_size for p in tmp_path.iterdir() if p.is_file())
    monkeypatch.setattr(cli, "_fsync", (lambda fsync: lambda p: (peak.__setitem__(0, max(peak[0], sizes())), fsync(p))[1])(cli._fsync))  # sampled once the new file is complete
    migrate(path); new_version, *now = state(path)
    assert (version, new_version, now) == (19, 20, kept) and kept[2] > 0 and path.stat().st_size <= 1.2 * size and peak[0] <= 2.2 * size, (size, path.stat().st_size, peak)  # 4,096-row groups of random text compress up to ~15% worse
    assert state(tmp_path / "convos.db.pre-v20.bak")[0] == 19 and sorted(p.name for p in tmp_path.iterdir()) == ["convos.db", "convos.db.pre-v20.bak", "convos.db.pre-v20.bak.attachments"]
    with contextlib.closing(cli.open_db(path, purpose="test.rewrite")) as db:
        assert db.execute("SELECT max(count) FROM pragma_storage_info('messages')").fetchone()[0] <= cli.ROW_GROUP_ROWS and db.execute("SELECT free_blocks FROM pragma_database_size() WHERE database_name='convos'").fetchone()[0] == 0

def test_v20_refuses_up_front_when_free_space_is_short(tmp_path, monkeypatch):
    v19(path := tmp_path / "convos.db"); before = (path.stat().st_size, state(path))
    monkeypatch.setattr(cli.shutil, "disk_usage", lambda p: cli.shutil._ntuple_diskusage(10**12, 10**12 - 10**6, 10**6))
    with pytest.raises(ValueError, match=r"needs [\d.]+ GB free .* 0\.0 GB available"): migrate(path)
    assert (path.stat().st_size, state(path)) == before and sorted(p.name for p in tmp_path.iterdir()) == ["convos.db"]

def test_v20_failed_copy_leaves_no_partial_file(tmp_path, monkeypatch):  # e.g. the disk filled up anyway
    v19(path := tmp_path / "convos.db"); kept, attach = state(path), cli._attach
    monkeypatch.setattr(cli, "_attach", lambda db, p, name="convos", read_only=False: (attach(db, p, name, read_only), (_ for _ in ()).throw(duckdb.IOException("No space left on device")) if name == "core_rewrite" else None)[0])
    with pytest.raises(duckdb.IOException): migrate(path)
    monkeypatch.setattr(cli, "_attach", attach); assert state(path) == kept and sorted(p.name for p in tmp_path.iterdir()) == ["convos.db", "convos.db.pre-v20.bak.attachments"]

def test_v20_killed_while_writing_the_new_file_keeps_the_v19_archive(tmp_path):
    v19(path := tmp_path / "convos.db"); kept = state(path)
    child = subprocess.Popen([sys.executable, "-c", "import sys; from ai_convos import cli; cli.init_schema(cli.open_db(sys.argv[1],purpose='test.rewrite'))", str(path)])
    while not ((new := tmp_path / "convos.db.v20").exists() and new.stat().st_size > 1 << 20): assert child.poll() is None; time.sleep(.005)
    os.kill(child.pid, signal.SIGKILL); child.wait()
    assert state(path) == kept
    migrate(path); assert state(path)[1:] == kept[1:] and not new.exists()

def test_v20_killed_between_backup_link_and_swap_reopens_at_v19(tmp_path, monkeypatch):
    v19(path := tmp_path / "convos.db"); kept = state(path); db, replace = cli.open_db(path, purpose="test.rewrite"), os.replace
    with monkeypatch.context() as m, pytest.raises(Killed): m.setattr(cli.os, "replace", lambda src, dst: (_ for _ in ()).throw(Killed()) if dst == path else replace(src, dst)); cli.init_schema(db)
    db.close(); assert path.stat().st_nlink == 2  # the old file is both the archive and its backup
    assert state(path) == kept and path.stat().st_nlink == 1
    migrate(path); assert state(path)[1:] == kept[1:]
