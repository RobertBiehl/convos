"""Source-backed retirement removes obsolete projections without losing unique evidence."""
import copy, json

import pytest
from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.protocol import row_proof
from tests import legacy_parsers
from tests.test_remote_projection import signed_edit_graph


def archive(tmp_path,alter=None):
    transcript=tmp_path/'session.jsonl'
    event=lambda uid,parent,blocks,role:dict(type=role,uuid=uid,parentUuid=parent,sessionId='session',timestamp='2026-01-01T00:00:00Z',message=dict(role=role,content=blocks,model='claude'))
    transcript.write_text('\n'.join(map(json.dumps,[event('user',None,[dict(type='text',text='Unique query needle')],'user'),event('thinking','user',[dict(type='thinking',thinking='Preserved reasoning')],'assistant'),event('answer','thinking',[dict(type='text',text='Answer needle')],'assistant')])))
    old=legacy_parsers.parse_claude_thread_session(transcript)
    assert len(old['msgs'])==2 and old['msgs'][-1]['parent_id'] not in {m['id'] for m in old['msgs']}
    _,device,user,control,_,_,_,_=signed_edit_graph()
    if alter=='content': old['msgs'][-1]['content']='Unique old content'
    if alter=='parent': old['msgs'][-1]['parent_id']=core.gen_id('wrong','parent')
    if alter=='timestamp': old['msgs'][-1]['created_at']=core.ts_from_iso('2025-01-01T00:00:00')
    if alter=='metadata': old['msgs'][-1]['metadata']='{"unique":"annotation"}'
    values=[('conversations',old['conv']),*(('messages',m) for m in old['msgs'])]
    bodies=[dict(row=(row:=core.parser_logical_row(kind,value)),proof=row_proof(device,user,'w',1,row)) for kind,value in values]
    path=tmp_path/'data/convos.db'
    signer=control['devices'][device['id']]
    with core._core(path,purpose='test.old') as db:
        core.init_schema(db)
        with core._transaction(db):
            core.project_attested_rows(db,[(b['row'],b['proof']) for b in bodies],signer['root_public'],signer['certificate'])
            core.project_logical_rows(db,[(b['row'],b['proof'],core.provenance_digest(b['proof']),True) for b in bodies])
            db.execute('UPDATE core_schema SET version=12')
    return path,transcript,old,bodies,device,user,control


def import_current(db,transcript,size=500):
    current=core.parse_claude_code_session(transcript,{('claude-code','session'):core.gen_id('canonical','session')})
    result=core.ParseResult(convs=[current['conv']],**{k:current[k] for k in ('msgs','tools','attachs','edits','edit_evidence','tool_lineage','message_lineage')})
    for part in core.ingest_parts(result,size):
        with core._transaction(db): core.upsert(db,part)
    return current


@pytest.mark.parametrize('size',[1,500])
def test_old_copies_deleted_and_thinking_parent_preserved(tmp_path,size):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.upgrade') as db:
        core.init_schema(db)
        current=import_current(db,transcript,size)
        assert len(current['msgs'])==3
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
        assert db.execute('SELECT count(*) FROM conversations').fetchone()==(1,)
        assert db.execute('SELECT count(*) FROM parser_retired_rows WHERE kind=\'messages\'').fetchone()==(2,)
        assert db.execute("SELECT thinking FROM messages WHERE thinking IS NOT NULL AND thinking<>''").fetchall()==[('Preserved reasoning',)]
        assert not core.archive_relationships(db)
        for body in bodies[1:]:
            claim=('messages',body['row']['id'],body['row']['id'],user,'active')
            assert core.typed_logical_rows(db,[claim])[claim]==body['row']
        before={t:db.execute(f'SELECT * FROM {t} ORDER BY 1').fetchall() for t in ('messages','conversations','parser_retired_rows','archive_state')}
        import_current(db,transcript,size)
        assert before=={t:db.execute(f'SELECT * FROM {t} ORDER BY 1').fetchall() for t in before}
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0
    assert path.with_name(path.name+'.pre-v13.bak').is_file()


@pytest.mark.parametrize('alter',['content','parent','timestamp','metadata'])
def test_source_mismatch_keeps_unique_old_copy(tmp_path,alter):
    path,transcript,old,*_=archive(tmp_path,alter)
    with core._core(path,purpose='test.refuse') as db:
        core.init_schema(db)
        before=db.execute('SELECT * FROM messages WHERE id=?',[old['msgs'][-1]['id']]).fetchone()
        import_current(db,transcript)
        assert db.execute('SELECT * FROM messages WHERE id=?',[old['msgs'][-1]['id']]).fetchone()==before


@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('native',[False,True])
def test_replay_without_transcript_does_not_restore_obsolete_copies(tmp_path,reverse,native):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.sender') as db:
        core.init_schema(db)
        current=import_current(db,transcript)
        rows=[core.logical_row(kind,core.ARCHIVE_COLUMNS[kind],r) for kind in ('conversations','messages') for r in db.execute(f"SELECT {','.join(core.ARCHIVE_COLUMNS[kind])} FROM {kind} WHERE id IN (SELECT UNNEST(?))",[[current['conv']['id'],*[m['id'] for m in current['msgs']]]]).fetchall()]
    updates=[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows]
    transcript.unlink()
    receiver=tmp_path/'receiver.db'
    local=user if native else 'recipient'
    for b in reversed([*bodies,*updates]) if reverse else [*bodies,*updates]: projection.apply_row_replicas(receiver,[b],'w',[control],local_user=local)
    for _ in range(2):
        projection.apply_row_replicas(receiver,[*bodies,*updates],'w',[control],local_user=local)
        with core._core(receiver,True,purpose='test.receiver') as db:
            assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
            assert not core.archive_relationships(db)
        assert projection.audit_rows(receiver,local_user=local)['totals']['unavailable']==0


def test_late_unique_edit_restores_its_parent_and_blocks_retirement(tmp_path):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.sender') as db:
        core.init_schema(db)
        import_current(db,transcript)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
    mid=old['msgs'][0]['id']
    edit=core.logical_row('file_edits',core.ARCHIVE_COLUMNS['file_edits'],[core.gen_id('edit','unique'),mid,'x.txt','write','Unique historical edit',None,None])
    projection.apply_row_replicas(path,[dict(row=edit,proof=row_proof(device,user,'w',1,edit))],'w',[control],local_user=user)
    with core._core(path,True,purpose='test.dependencies') as db:
        assert db.execute('SELECT id FROM messages WHERE id=?',[mid]).fetchone()==(mid,)
        assert db.execute('SELECT content FROM file_edits').fetchone()==('Unique historical edit',)
        assert not core.archive_relationships(db)


def test_same_identity_reparse_restores_thinking_turn_without_aliases(tmp_path):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.same_identity') as db:
        core.init_schema(db)
        current=core.parse_claude_code_session(transcript)
        with core._transaction(db): core.upsert(db,core.ParseResult(convs=[current['conv']],msgs=current['msgs'],message_lineage=current['message_lineage']))
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
        assert not core.archive_relationships(db)
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_preexisting_unique_edit_prevents_parent_deletion(tmp_path):
    path,transcript,old,*_=archive(tmp_path)
    mid=old['msgs'][0]['id']
    with core._core(path,purpose='test.unique') as db:
        core.init_schema(db)
        db.execute("INSERT INTO file_edits(id,message_id,file_path,edit_type,content) VALUES ('unique',?,'x','write','Unique evidence')",[mid])
        import_current(db,transcript)
        assert db.execute('SELECT id FROM messages WHERE id=?',[mid]).fetchone()==(mid,)
        assert db.execute('SELECT content FROM file_edits').fetchone()==('Unique evidence',)
        assert not core.archive_relationships(db)


def test_two_late_dependencies_restore_one_exact_parent(tmp_path):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.clean') as db:
        core.init_schema(db)
        import_current(db,transcript)
    rows=[core.logical_row('file_edits',core.ARCHIVE_COLUMNS['file_edits'],[core.gen_id('unique',str(i)),old['msgs'][0]['id'],'x','write',f'Unique {i}',None,None]) for i in range(2)]
    projection.apply_row_replicas(path,[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows],'w',[control],local_user=user)
    with core._core(path,True,purpose='test.parents') as db:
        assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(2,)
        assert not core.archive_relationships(db)


def test_failed_retirement_rolls_back_then_resumes(tmp_path,monkeypatch):
    path,transcript,old,*_=archive(tmp_path)
    with core._core(path,purpose='test.interrupt') as db:
        core.init_schema(db)
        before={t:db.execute(f'SELECT * FROM {t} ORDER BY 1').fetchall() for t in ('messages','conversations','remote.row_proofs','remote.row_conflicts','parser_retired_rows','core_schema')}
        actual=core._insert_pages
        def fail(db,target,*args,**kwargs):
            if target=='parser_retired_rows': raise RuntimeError('interrupted retirement')
            return actual(db,target,*args,**kwargs)
        with monkeypatch.context() as patch:
            patch.setattr(core,'_insert_pages',fail)
            with pytest.raises(RuntimeError,match='interrupted retirement'): import_current(db,transcript)
        assert before=={t:db.execute(f'SELECT * FROM {t} ORDER BY 1').fetchall() for t in before}
        import_current(db,transcript)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)


def test_migration_rebuilds_received_lineage_without_source(tmp_path):
    path,transcript,old,*_=archive(tmp_path)
    with core._core(path,purpose='test.seed') as db:
        core.init_schema(db)
        current=import_current(db,transcript)
        # Model a pre-b11 receiver that stored portable metadata but did not interpret it.
        for kind in ('conversations','messages'):
            for physical,body,extra in db.execute('SELECT physical,body,projection FROM parser_retired_rows WHERE kind=?',[kind]).fetchall(): core._insert_pages(db,kind,[core.retired_projection(kind,physical,json.loads(body),json.loads(extra))],core.ARCHIVE_COLUMNS[kind])
        db.execute('DELETE FROM parser_retired_rows; DELETE FROM parser_message_lineage; UPDATE core_schema SET version=13')
        transcript.unlink()
        core.init_schema(db)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
        assert db.execute('SELECT count(*) FROM conversations').fetchone()==(1,)
        assert db.execute('SELECT version FROM core_schema').fetchone()==(core.CORE_VERSION,)
    assert path.with_name(path.name+'.pre-v14.bak').is_file()


def test_changed_body_under_a_retired_id_is_not_deleted(tmp_path):
    path,transcript,old,bodies,device,user,control=archive(tmp_path)
    with core._core(path,purpose='test.clean') as db:
        core.init_schema(db)
        import_current(db,transcript)
    previous=bodies[1]
    changed=copy.deepcopy(previous['row'])
    changed['data']['content']='New unique source content'
    projection.apply_row_replicas(path,[dict(row=changed,proof=row_proof(device,user,'w',1,changed,previous['proof']['revision']))],'w',[control],local_user=user)
    with core._core(path,True,purpose='test.new_revision') as db:
        assert db.execute('SELECT content FROM messages WHERE id=?',[changed['id']]).fetchone()==('New unique source content',)
        assert not core.archive_relationships(db)


@pytest.mark.parametrize('source',['claude-code','codex'])
def test_tool_cleanup_follows_a_rebound_session_after_source_move(tmp_path,source):
    from tests.test_parser_evolution import old_archive
    path,transcript,parser,old,old_tools,unique,bodies,device,user,control=old_archive(tmp_path,source)
    moved=tmp_path/'moved.jsonl'
    transcript.rename(moved)
    with core._core(path,purpose='test.rebound_tools') as db:
        core.init_schema(db)
        cid=core.gen_id('canonical',source)
        db.execute('INSERT OR REPLACE INTO provider_sessions VALUES (?,?,?)',[source,'session',cid])
        bindings=core.session_bindings(db)
        assert old['conv']['id'] in bindings[(source,'session','legacy')]
        current=parser(moved,bindings)
        result=core.ParseResult(convs=[current['conv']],**{k:current[k] for k in ('msgs','tools','attachs','edits','edit_evidence','tool_lineage','message_lineage')})
        with core._transaction(db): core.upsert(db,result)
        assert db.execute('SELECT count(*) FROM tool_calls').fetchone()==(1,)
        assert db.execute('SELECT content FROM file_edits').fetchone()==(unique['content'],)


def test_backup_merge_keeps_retirement_and_restores_late_dependencies(tmp_path):
    import shutil
    path,transcript,old,*_=archive(tmp_path)
    donor=tmp_path/'donor.db'
    shutil.copyfile(path,donor)
    with core._core(path,purpose='test.clean') as db:
        core.init_schema(db)
        import_current(db,transcript)
    core.merge_archive_backup(path,donor)
    with core._core(path,True,purpose='test.recovered') as db:
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(3,)
        assert db.execute('SELECT count(*) FROM conversations').fetchone()==(1,)
        assert not core.archive_relationships(db)


@pytest.mark.parametrize('unique',[False,True])
def test_long_chain_retires_in_batches_and_keeps_unique_ancestors(tmp_path,unique):
    transcript=tmp_path/'session.jsonl'
    transcript.write_text('\n'.join(json.dumps(dict(type='assistant',uuid=str(i),parentUuid=str(i-1) if i else None,sessionId='session',timestamp='2026-01-01T00:00:00Z',message=dict(role='assistant',content=[dict(type='text',text=f'Event {i}')],model='claude'))) for i in range(501)))
    old=legacy_parsers.parse_claude_thread_session(transcript)
    if unique: old['msgs'][400]['content']='Unique historical content'
    with core._core(tmp_path/'data/convos.db',purpose='test.chain') as db:
        core.init_schema(db)
        with core._transaction(db):
            core.upsert(db,core.ParseResult(convs=[old['conv']],msgs=old['msgs']))
            db.execute('DELETE FROM provider_sessions')  # Emulate the older archive before provider bindings existed.
        import_current(db,transcript)
        old_ids=[m['id'] for m in old['msgs']]
        kept={r[0] for r in db.execute('SELECT id FROM messages WHERE id IN (SELECT UNNEST(?))',[old_ids]).fetchall()}
        assert kept==(set(old_ids[:401]) if unique else set())
        assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='messages'").fetchone()==(100 if unique else 501,)
        assert not core.archive_relationships(db)
