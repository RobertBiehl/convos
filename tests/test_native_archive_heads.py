"""Native source writes retain the exact signed head until its successor is durable."""
import copy
import json
from datetime import datetime

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import attest_rows, audit_rows, row_replicas, signed_row
from ai_convos_remote.protocol import digest, open_replica, row_proof
from tests.test_native_provenance import people, scanned


def native_archive(tmp_path,monkeypatch):
    path=tmp_path/'archive.db'; body=tmp_path/'body'; body.write_bytes(b'original'); stamp=datetime(2026,1,1)
    monkeypatch.setattr(core,'DB_PATH',path)
    with core.open_db(path,purpose='test.native.init') as db: core.init_schema(db)
    result=core.ParseResult(
        convs=[dict(id='c',source='codex',title='original',created_at=stamp,updated_at=stamp,model=None,cwd=None,git_branch=None,project_id=None,metadata='{}')],
        msgs=[dict(id='m',conversation_id='c',role='assistant',content='original',thinking=None,created_at=stamp,model=None,metadata='{}',parent_id=None)],
        tools=[dict(id='t',message_id='m',tool_name='write',input='{}',output='"original"',status='complete',duration_ms=None,created_at=stamp)],
        attachs=[dict(id='a',message_id='m',filename='original',mime_type='text/plain',size=8,path=str(body),url=None,created_at=stamp)],
        artifacts=[dict(id='ar',conversation_id='c',artifact_type='code',title='original',content='original',language='python',created_at=stamp,version=1)],
        edits=[dict(id='e',message_id='m',file_path=str(tmp_path/'x.py'),edit_type='write',content='original',created_at=stamp,old_content=None)],
        edit_evidence=[dict(file_edit_id='e',status='confirmed',reason='provider_success',tool_call_id='t')])
    core.commit_result(copy.deepcopy(result),'test.native.ingest')
    core.capture_provenance(path)
    cfg=people()[-1]; state=tmp_path/'state.db'; records=scanned(path,state)
    return path,body,cfg,state,result,records


def archive_row(records,table):
    return next(signed_row(r) for r in records if r['kind'].endswith('.record') and r['payload']['table']==table and not r['payload']['row'][0].startswith('history:'))


def retained(path,cfg):
    return [open_replica(env,bytes(32)) for env in row_replicas(path,cfg,'w',[],{1:bytes(32)})]


@pytest.mark.parametrize('table,attribute,column,value',[
    ('conversations','convs','title','replacement'),('messages','msgs','content','replacement'),
    ('tool_calls','tools','output','"replacement"'),('attachments','attachs','filename','replacement'),
    ('artifacts','artifacts','content','replacement'),('file_edits','edits','content','replacement')])
def test_real_ingestion_preserves_signed_native_head_across_restart(tmp_path,monkeypatch,table,attribute,column,value):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch)
    before=archive_row(records,table)
    assert attest_rows(path,cfg,'w',records)>0
    getattr(result,attribute)[0][column]=value
    core.commit_result(result,'test.native.update')
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute(f'SELECT {column} FROM {table} WHERE id=?',[before['id']]).fetchone()[0]==value
    assert before in [v['row'] for v in retained(path,cfg)]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


@pytest.mark.parametrize('mode',['hash','size','size_only'])
def test_native_attachment_index_preserves_signed_joined_body(tmp_path,monkeypatch,mode):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch)
    if mode.startswith('size'):
        with core.open_db(path,purpose='test.native.unknown.size') as db: db.execute("UPDATE attachments SET size=NULL WHERE id='a'")
        records=scanned(path,state)
    before=archive_row(records,'attachments'); assert attest_rows(path,cfg,'w',records)>0
    content=b'original' if mode=='size_only' else b'replaced'; body.write_bytes(content)
    with core.open_db(path,purpose='test.native.attachment.index') as db,core._transaction(db):
        generation=db.execute('SELECT generation FROM archive_state').fetchone()[0]
        core.index_attachment_body(db,'a',body,None if mode.startswith('size') else 8)
        assert db.execute('SELECT generation FROM archive_state').fetchone()[0]>generation
    assert before in [v['row'] for v in retained(path,cfg)]
    assert archive_row(scanned(path,state),'attachments')['data']['body_hash']==digest(content)
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_codex_parser_update_keeps_original_signed_message(tmp_path,monkeypatch):
    path=tmp_path/'archive.db'; transcript=tmp_path/'session.jsonl'; monkeypatch.setattr(core,'DB_PATH',path)
    with core.open_db(path,purpose='test.native.init') as db: core.init_schema(db)
    def ingest(text):
        transcript.write_text('\n'.join(map(json.dumps,[{'type':'session_meta','timestamp':'2026-01-01T00:00:00Z','payload':{'id':'source-session'}},{'type':'response_item','timestamp':'2026-01-01T00:00:01Z','payload':{'type':'message','role':'assistant','content':[{'type':'output_text','text':text}]}}])))
        core.commit_result(core.hook_result('codex',transcript),'test.native.hook.ingest')
    ingest('original'); cfg=people()[-1]; records=scanned(path,tmp_path/'state.db'); before=archive_row(records,'messages')
    assert attest_rows(path,cfg,'w',records)>0
    ingest('replacement')
    assert before in [v['row'] for v in retained(path,cfg)]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_source_write_and_head_preservation_roll_back_together(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch); before=archive_row(records,'messages')
    assert attest_rows(path,cfg,'w',records)>0
    write=core._insert_pages
    def fail(db,table,rows,*args,**kwargs):
        if table=='remote.row_conflicts' and rows: raise OSError('retention interrupted')
        return write(db,table,rows,*args,**kwargs)
    monkeypatch.setattr(core,'_insert_pages',fail); result.msgs[0]['content']='replacement'
    with pytest.raises(OSError,match='retention interrupted'): core.commit_result(result,'test.native.rollback')
    assert archive_row(scanned(path,state),'messages')==before
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_new_attestation_retires_only_its_durable_predecessor(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch)
    assert attest_rows(path,cfg,'w',records)>0
    for value in ('second','third','fourth'):
        before=archive_row(scanned(path,state),'conversations'); result.convs[0]['title']=value
        core.commit_result(copy.deepcopy(result),'test.native.next')
        assert before in [v['row'] for v in retained(path,cfg)]
        records=scanned(path,state); assert attest_rows(path,cfg,'w',records)>0
        with duckdb.connect(str(path),read_only=True) as db: assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==0
        assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0
        assert archive_row(records,'conversations') in [open_replica(env,bytes(32))['row'] for env in row_replicas(path,cfg,'w',records,{1:bytes(32)})]


def test_source_update_and_attestation_keep_independent_forks(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch); before=archive_row(records,'messages')
    assert attest_rows(path,cfg,'w',records)>0
    fork=before|{'data':before['data']|{'content':'other branch'}}; proof=row_proof(cfg['device'],cfg['user'],'w',1,fork); signer=cfg['controls']['w']['devices'][cfg['device']['id']]
    with core.open_db(path,purpose='test.native.fork') as db,core._transaction(db): core.project_attested_rows(db,[(fork,proof)],signer['root_public'],signer['certificate'])
    result.msgs[0]['content']='replacement'; core.commit_result(result,'test.native.fork.update')
    assert {digest(before),digest(fork)}<={digest(v['row']) for v in retained(path,cfg)}
    with pytest.raises(ValueError,match='row revision conflict'): attest_rows(path,cfg,'w',scanned(path,state))
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


@pytest.mark.parametrize('batch',[False,True])
def test_core_native_deletion_keeps_signed_head(tmp_path,monkeypatch,batch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch); before=archive_row(records,'messages')
    assert attest_rows(path,cfg,'w',records)>0
    deleted=core.logical_row('messages',identity='m',state='deleted'); proof=row_proof(cfg['device'],cfg['user'],'w',1,deleted)
    with core.open_db(path,purpose='test.native.delete') as db,core._transaction(db):
        if batch: core.project_logical_rows(db,[(deleted,proof,digest(proof),True)])
        else: core.project_logical_row(db,deleted,proof,digest(proof),native=True)
    assert before in [v['row'] for v in retained(path,cfg)]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_unchanged_ingestion_never_looks_up_proof_heads(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch)
    assert attest_rows(path,cfg,'w',records)>0
    guard=core.preserve_fact_heads
    def unchanged(db,keys):
        keys=list(keys)
        assert not keys
        return guard(db,keys)
    monkeypatch.setattr(core,'preserve_fact_heads',unchanged)
    assert core.commit_result(result,'test.native.unchanged')[:5]==(0,0,0,0,0)


@pytest.mark.parametrize('writer',['archive','logical','logical_batch'])
def test_typed_native_writer_keeps_existing_signed_snapshot(tmp_path,monkeypatch,writer):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch); before=archive_row(records,'attachments')
    assert attest_rows(path,cfg,'w',records)>0
    changed=before|{'data':before['data']|{'filename':'newer','body_hash':digest(b'replaced')}}; proof=row_proof(cfg['device'],cfg['user'],'w',1,changed)
    with core.open_db(path,purpose='test.native.writer') as db,core._transaction(db):
        if writer=='archive':
            row=result.attachs[0]|{'filename':'newer'}
            core.project_archive_rows(db,'attachments',core.ARCHIVE_COLUMNS['attachments'],[(list(row.values()),None)])
        elif writer=='logical': core.project_logical_row(db,changed,proof,digest(proof),native=True)
        else: core.project_logical_rows(db,[(changed,proof,digest(proof),True)])
    assert before in [v['row'] for v in retained(path,cfg)]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_predecessor_retirement_rolls_back_with_failed_attestation(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch); before=archive_row(records,'conversations')
    assert attest_rows(path,cfg,'w',records)>0
    result.convs[0]['title']='newer'; core.commit_result(result,'test.native.next')
    def fail(*args,**kwargs): raise OSError('base storage failed')
    monkeypatch.setattr(core,'record_local_row_bases',fail)
    with pytest.raises(OSError,match='base storage failed'): attest_rows(path,cfg,'w',scanned(path,state))
    assert before in [v['row'] for v in retained(path,cfg)]
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_signed_chatgpt_timestamp_repair_preserves_original(tmp_path,monkeypatch):
    path,body,cfg,state,result,records=native_archive(tmp_path,monkeypatch)
    result.convs[0].update(source='chatgpt',created_at=None,updated_at=None)
    with core.open_db(path,purpose='test.native.legacy') as db,core._transaction(db):
        core.project_archive_rows(db,'conversations',core.ARCHIVE_COLUMNS['conversations'],[(list(result.convs[0].values()),None)])
    records=scanned(path,state); before=archive_row(records,'conversations'); assert attest_rows(path,cfg,'w',records)>0
    for key,value in [('DATA_DIR',tmp_path),('STATE_PATH',tmp_path/'sync_state.json'),('HOOK_DIR',tmp_path/'hooks'),('HOOK_STATE',tmp_path/'hook_state.json'),('HOOK_PROGRESS',tmp_path/'hook_progress.json')]: monkeypatch.setattr(core,key,value)
    monkeypatch.delenv('CONVOS_IMPORT_PATHS',raising=False)
    core.sync(False,300,False,False,False,False,True)
    assert before in [v['row'] for v in retained(path,cfg)]
    assert archive_row(scanned(path,state),'conversations')['data']['created_at'] is not None
    assert audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0
