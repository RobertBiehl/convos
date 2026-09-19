from types import SimpleNamespace

import duckdb
import pytest

import ai_convos.cli as core


def archive(path):
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,title,metadata) VALUES ('c','codex','healthy','{}')")
        db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('m','c','user','read every string','{}')")
    return path


def test_archive_verify_checks_indexes_and_variable_width_storage(tmp_path):
    result=core.verify_archive(archive(tmp_path/'archive.db'))
    assert result['tables']>=20 and result['keys']>=2 and result['strings']>=20


def test_archive_verify_names_the_last_isolated_check_on_process_crash(tmp_path,monkeypatch):
    monkeypatch.setattr(core.subprocess,'run',lambda *args,**kwargs:SimpleNamespace(returncode=-10,stdout='',stderr='index provenance.pending.kind\nstring provenance.pending.entity\n'))
    with pytest.raises(ValueError,match=r'provenance\.pending\.entity'): core.verify_archive(tmp_path/'archive.db')


def test_cutover_verifies_before_backup_or_mutation(tmp_path,monkeypatch):
    path=archive(tmp_path/'archive.db')
    monkeypatch.setattr(core,'verify_archive',lambda path:(_ for _ in ()).throw(ValueError('damaged index')))
    with pytest.raises(ValueError,match='damaged index'): core.reset_archive_sync(path,'user','device')
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT title FROM conversations").fetchone()==('healthy',)
        assert not db.execute("SELECT * FROM archive_sync").fetchall()
    assert not list(tmp_path.glob('archive.db.pre-*.bak*'))
