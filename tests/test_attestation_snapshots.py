"""Signing a scan snapshot must survive later source writes and a restart."""
import json

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.projection import attest_rows, audit_rows, row_replicas, signed_row
from ai_convos_remote.protocol import digest, open_replica, row_proof
from tests.test_native_provenance import people, scanned
from tests.test_remote_projection import git, source


def change_source(path,checkout,kind):
    if kind=='repository.observed':
        git(checkout,'remote','add','origin','https://example.test/newer/repo.git')
        core.capture_repository(checkout,path)
    else:
        with core.open_db(path,purpose='test.native.capture') as db,core._transaction(db):
            columns=core.ARCHIVE_COLUMNS['messages']; row=dict(zip(columns,db.execute('SELECT * EXCLUDE (embedding) FROM messages WHERE id=\'m\'').fetchone()))
            core.upsert(db,core.ParseResult(msgs=[row|{'content':'newer native source'}]))


def prepared(tmp_path,kind):
    checkout,db=source(tmp_path); db.close(); path=tmp_path/'source.db'; state=tmp_path/'state.db'; users,devices,control,cfg=people()
    records=scanned(path,state); selected=[r for r in records if r['kind']==kind or kind=='messages' and r['kind']=='message.record' and r['payload']['row'][0]=='m']
    assert len(selected)==1
    record=selected[0]; snapshot=core.logical_fact(record) if kind in core.PROVENANCE_KINDS else signed_row(record)
    change_source(path,checkout,kind)
    return path,state,cfg,selected,snapshot


@pytest.mark.parametrize('kind',['repository.observed','messages'])
def test_attested_snapshot_survives_source_change_and_restart(tmp_path,kind):
    path,state,cfg,records,snapshot=prepared(tmp_path,kind)
    before=scanned(path,state)
    assert attest_rows(path,cfg,'w',records)==1
    del records
    with duckdb.connect(str(path),read_only=True) as db:
        proof,body=db.execute('SELECT p.content_hash,CAST(c.body AS VARCHAR) FROM remote.row_proofs p LEFT JOIN remote.row_conflicts c ON c.proof_id=p.id').fetchone()
        assert body is not None and proof==digest(snapshot) and json.loads(body)==snapshot
    assert scanned(path,state)==before
    exported=[open_replica(env,bytes(32))['row'] for env in row_replicas(path,cfg,'w',[],{1:bytes(32)})]
    assert exported==[snapshot]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_attestation_snapshot_and_signature_rollback_together(tmp_path,monkeypatch):
    path,state,cfg,records,snapshot=prepared(tmp_path,'repository.observed'); before=scanned(path,state); write=core._insert_pages
    def fail(db,table,rows,*args,**kwargs):
        if table=='remote.row_conflicts' and rows: raise OSError('snapshot storage failed')
        return write(db,table,rows,*args,**kwargs)
    monkeypatch.setattr(core,'_insert_pages',fail)
    with pytest.raises(OSError,match='snapshot storage failed'): attest_rows(path,cfg,'w',records)
    with duckdb.connect(str(path),read_only=True) as db:
        assert all(db.execute('SELECT count(*) FROM remote.'+table).fetchone()[0]==0 for table in ('row_proofs','row_conflicts','local_row_bases'))
    assert scanned(path,state)==before


def test_unchanged_attestation_does_not_copy_archive_bodies(tmp_path):
    checkout,db=source(tmp_path); db.close(); path=tmp_path/'source.db'; users,devices,control,cfg=people(); records=scanned(path,tmp_path/'state.db')
    assert attest_rows(path,cfg,'w',records)>0
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==0
    assert attest_rows(path,cfg,'w',records)==0


def test_snapshot_writer_rejects_body_proof_mismatch_before_headers(tmp_path):
    path,state,cfg,records,snapshot=prepared(tmp_path,'messages'); signer=cfg['controls']['w']['devices'][cfg['device']['id']]
    proof=row_proof(cfg['device'],cfg['user'],'w',1,snapshot)
    with core.open_db(path,purpose='test.invalid.snapshot') as db,core._transaction(db):
        with pytest.raises(ValueError,match='snapshot/proof mismatch'):
            core.project_attested_rows(db,[(snapshot|{'data':snapshot['data']|{'content':'wrong body'}},proof)],signer['root_public'],signer['certificate'])
        assert db.execute('SELECT count(*) FROM remote.row_proofs').fetchone()[0]==0


def test_stale_attestation_snapshots_are_retained_in_bounded_pages(tmp_path,monkeypatch):
    path=tmp_path/'archive.db'; users,devices,control,cfg=people(); write=projection._store_proofs; sizes=[]
    with core.open_db(path,purpose='test.large.snapshot') as db:
        core.init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}')")
        db.executemany("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES (?,'c','assistant',?,'{}')",[(str(i),'before') for i in range(501)])
    records=[r for r in scanned(path,tmp_path/'state.db') if r['kind']=='message.record']
    with core.open_db(path,purpose='test.newer.snapshot') as db: db.execute("UPDATE messages SET content='after'")
    def store(path,records,*args):
        sizes.append(len(records))
        return write(path,records,*args)
    monkeypatch.setattr(projection,'_store_proofs',store)
    assert attest_rows(path,cfg,'w',records)==501 and sizes==[500,1]
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==501
        assert db.execute("SELECT count(*) FROM messages WHERE content='after'").fetchone()[0]==501
