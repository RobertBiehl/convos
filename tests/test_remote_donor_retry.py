"""Donor pages must wake retained edit facts even after retry bootstrap completed."""
import json, shutil

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.protocol import digest
from tests.test_remote_edit_retry import graph


def archives(tmp_path,kind):
    path,donor=tmp_path/'current.db',tmp_path/'donor.bak'
    user,control,rows,bodies,file,fact,signed=graph()
    with duckdb.connect(str(path)) as db: core.init_schema(db)
    shutil.copyfile(path,donor)
    parents=[*bodies,signed(file,True)]
    projection.apply_row_replicas(path,parents,'w',[control],local_user='receiver')
    body=signed(fact('e'))
    projection.apply_row_replicas(donor,[*(parents if kind=='typed' else []),body],'w',[control],local_user='receiver')
    if kind=='typed':
        with duckdb.connect(str(path)) as db:
            db.execute("INSERT INTO provenance.file_edit_files VALUES (?, ?, NULL, 'older', 'captured_exact')",[projection.foreign_id(user,'file_edits','e'),file['id']])
        with duckdb.connect(str(donor),read_only=True) as db: assert not db.execute('SELECT 1 FROM remote.row_conflicts').fetchone()
    with duckdb.connect(str(donor)) as db:
        if kind=='legacy':
            db.execute('CREATE TABLE remote.row_bodies AS SELECT * FROM remote.row_conflicts; DELETE FROM remote.row_conflicts')
        db.execute('DROP TABLE remote.edit_dependencies; DROP TABLE remote.edit_ready')
    projection.retry_edit_replicas(path,'receiver')
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute("SELECT after_proof_id FROM remote.edit_ready WHERE dependency_key=''").fetchone()==('done',)
    return path,donor,user,file,body


@pytest.mark.parametrize('kind',['conflict','legacy','typed'])
@pytest.mark.parametrize('interleaved',[False,True])
def test_donor_restores_pending_edit_after_bootstrap_done_without_arrivals(tmp_path,monkeypatch,kind,interleaved):
    path,donor,user,file,body=archives(tmp_path,kind)
    before=core._file_sha256(donor)
    if interleaved:
        monkeypatch.setattr(core,'archive_yield',lambda _:projection.retry_edit_replicas(path,'receiver'))
    core.merge_archive_backup(path,donor,page=1)
    if not interleaved:
        with duckdb.connect(str(path),read_only=True) as db:
            assert db.execute("SELECT after_proof_id FROM remote.edit_ready WHERE dependency_key=''").fetchone()==('',)
            assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?',[digest(body['proof'])]).fetchone()[0])==body['row']
    projection.retry_edit_replicas(path,'receiver')
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT file_id,new_content_hash FROM provenance.file_edit_files WHERE file_edit_id=?',[projection.foreign_id(user,'file_edits','e')]).fetchone()==(file['id'],'h')
        assert not db.execute('SELECT 1 FROM remote.row_conflicts WHERE proof_id=?',[digest(body['proof'])]).fetchone()
        assert db.execute('SELECT * FROM remote.edit_ready').fetchall()==[('','done')]
    assert projection.audit_rows(path,local_user='receiver')['totals']['unavailable']==0
    assert core._file_sha256(donor)==before


def test_donor_body_and_retry_reset_roll_back_together(tmp_path,monkeypatch):
    path,donor,user,file,body=archives(tmp_path,'legacy')
    pid=digest(body['proof'])
    monkeypatch.setattr(core,'archive_yield',lambda _:projection.retry_edit_replicas(path,'receiver'))
    real=core.project_edit_dependencies
    def interrupted(db,*args,**kwargs):
        result=real(db,*args,**kwargs)
        if db.execute('SELECT 1 FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.id=?',[pid]).fetchone():
            raise RuntimeError('interrupted body page')
        return result
    monkeypatch.setattr(core,'project_edit_dependencies',interrupted)
    with pytest.raises(RuntimeError,match='interrupted body page'): core.merge_archive_backup(path,donor,page=1)
    with duckdb.connect(str(path),read_only=True) as db:
        assert not db.execute('SELECT 1 FROM remote.row_conflicts WHERE proof_id=?',[pid]).fetchone()
        assert db.execute("SELECT after_proof_id FROM remote.edit_ready WHERE dependency_key=''").fetchone()==('done',)
    monkeypatch.setattr(core,'project_edit_dependencies',real)
    core.merge_archive_backup(path,donor,page=1)
    projection.retry_edit_replicas(path,'receiver')
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT new_content_hash FROM provenance.file_edit_files WHERE file_edit_id=?',[projection.foreign_id(user,'file_edits','e')]).fetchone()==('h',)
