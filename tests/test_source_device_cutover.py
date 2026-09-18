import json

import duckdb
import pytest

from ai_convos import cli as core


@pytest.mark.parametrize('source',['codex','claude-code'])
def test_identity_cutover_keeps_idless_rows_and_references_and_reimport_is_idempotent(tmp_path,source):
    root=core.hook_root(source)
    root.mkdir(parents=True)
    events=([{'type':'session_meta','payload':{'id':'session'}},
             {'type':'response_item','payload':{'type':'message','id':'provider-message','role':'user','content':[{'type':'input_text','text':'hello'}]}},
             {'type':'response_item','payload':{'type':'message','role':'assistant','content':[{'type':'output_text','text':'legacy'}]}},
             {'type':'response_item','payload':{'type':'function_call','call_id':'call','name':'test','arguments':'{}'}}]
            if source=='codex' else [{'type':'system','sessionId':'session'},
             {'type':'user','uuid':'provider-message','message':{'content':'hello'}},
             {'type':'assistant','parentUuid':'provider-message','message':{'content':[{'type':'text','text':'legacy'},{'type':'tool_use','id':'call','name':'test','input':{}}]}}])
    path=root/'session.jsonl'
    path.write_text('\n'.join(map(json.dumps,events)))
    parser=core.parse_codex if source=='codex' else core.parse_claude_code
    oldcid='legacy-conversation'
    bound={(source,oldcid,'provider-message','provider-message'):'legacy-message',(source,'session'):oldcid,(source,oldcid,'message',1):'legacy-message',(source,oldcid,'message',2):'idless-message'}
    archive=tmp_path/'archive.db'
    with duckdb.connect(str(archive)) as db:
        core.init_schema(db)
        core.upsert(db,parser(root.parent if source=='codex' else root,bindings=bound))
        db.execute("INSERT INTO attachments VALUES ('attachment','idless-message','a.txt','text/plain',3,NULL,NULL,NULL)")
        db.execute("INSERT INTO file_edits VALUES ('legacy-edit','idless-message','a.txt','write','abc',NULL,NULL)")
    result=core.reset_archive_sync(archive,'user','device')
    assert result['removed']==0 and result['backup']
    with duckdb.connect(str(archive)) as db:
        cid=core.gen_id(source,'session')
        mid=core.gen_id(source,f'message:{cid}:provider-message')
        assert db.execute('SELECT id FROM conversations').fetchall()==[(cid,)]
        assert set(db.execute('SELECT id,conversation_id FROM messages').fetchall())=={(mid,cid),('idless-message',cid)}
        assert db.execute('SELECT id,message_id FROM tool_calls').fetchall()==[(core.gen_id(source,f'tool:{cid}:call'),'idless-message')]
        before={table:db.execute(f'SELECT id FROM {table} ORDER BY id').fetchall() for table in core.ARCHIVE_COLUMNS}
        core.upsert(db,parser(root.parent if source=='codex' else root,bindings=core.session_bindings(db)))
        assert {table:db.execute(f'SELECT id FROM {table} ORDER BY id').fetchall() for table in core.ARCHIVE_COLUMNS}==before
        assert not core.archive_relationships(db)
    assert core.reset_archive_sync(archive,'user','device') is None


def test_cutover_collision_rolls_back_and_preserves_backup(tmp_path):
    root=core.hook_root('codex')
    root.mkdir(parents=True)
    (root/'session.jsonl').write_text(json.dumps({'type':'session_meta','payload':{'id':'session'}}))
    path=tmp_path/'archive.db'
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations (id,source,metadata) VALUES ('legacy','codex','{}'),(?,'codex','{}')",[core.gen_id('codex','session')])
        db.execute("INSERT INTO provider_sessions VALUES ('codex','session','legacy')")
    with pytest.raises(ValueError,match='identity collision'): core.reset_archive_sync(path,'user','device')
    with duckdb.connect(str(path)) as db:
        assert db.execute('SELECT count(*) FROM conversations').fetchone()==(2,)
        assert not db.execute('SELECT * FROM archive_sync').fetchall()


def signed_graph():
    from ai_convos_remote.protocol import identity,public_id,public,certificate,row_proof
    root,device,peer=identity('root'),identity('source'),identity('peer')
    user=public_id(root['sign_public'])
    entry=lambda d:dict(user=user,root_public=root['sign_public'],device=public(d),certificate=certificate(root,user,d),history=True)
    control=dict(workspace='w',revision=1,epoch=1,devices={d['id']:entry(d) for d in (device,peer)})
    rows=[core.logical_row('conversations',core.ARCHIVE_COLUMNS['conversations'],['c','codex','title',None,None,None,None,None,None,'{}']),core.logical_row('messages',core.ARCHIVE_COLUMNS['messages'],['m','c','assistant','done',None,None,None,'{}',None]),core.logical_row('tool_calls',core.ARCHIVE_COLUMNS['tool_calls'],['t','m','write','{}','{}','complete',None,None])]
    rows[-1]['data']['edits']=[dict(id='e',message_id='m',file_path=None,edit_type='write',content='one',created_at=None,old_content=None,status='confirmed',reason='provider_success')]
    bodies=[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows]
    return user,device,peer,control,bodies


def test_peer_receives_tool_owned_edits_and_cannot_revise_or_append(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas,foreign_id,audit_rows
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'peer.db'
    apply_row_replicas(path,bodies,'w',[control],local_user=user,local_device=peer['id'])
    with duckdb.connect(str(path)) as db:
        assert db.execute('SELECT count(*) FROM remote.row_origins').fetchone()==(3,)
        assert db.execute('SELECT content FROM file_edits').fetchone()==('one',)
        assert db.execute('SELECT status,tool_call_id FROM provenance.file_edit_evidence').fetchone()==('confirmed','t')
        assert not core.archive_relationships(db)
    assert audit_rows(path,local_user=user)['totals']['projection_mismatch']==0
    for row,previous in [(bodies[-1]['row'],bodies[-1]['proof']['revision']),({**bodies[1]['row'],'id':'unauthorized-child'},None)]:
        proof=row_proof(peer,user,'w',1,row,previous)
        with pytest.raises(ValueError,match='ownership conflict'): apply_row_replicas(path,[dict(row=row,proof=proof)],'w',[control],local_user=user,local_device=peer['id'])
    updated={**bodies[-1]['row'],'data':{**bodies[-1]['row']['data'],'edits':[{**bodies[-1]['row']['data']['edits'][0],'content':'two'}]}}
    proof=row_proof(device,user,'w',1,updated,bodies[-1]['proof']['revision'])
    apply_row_replicas(path,[dict(row=updated,proof=proof)],'w',[control],local_user=user,local_device=peer['id'])
    apply_row_replicas(path,bodies,'w',[control],local_user=user,local_device=peer['id'])
    with duckdb.connect(str(path)) as db: assert db.execute('SELECT content FROM file_edits').fetchall()==[('two',)]


def test_edit_delta_is_carried_by_its_parent_and_has_no_independent_replica(tmp_path):
    from ai_convos_remote.projection import scan,connect,signed_row
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'source.db'
    with duckdb.connect(str(path)) as db,connect(tmp_path/'state.db') as state:
        core.init_schema(db)
        core.project_logical_rows(db,[(b['row'],b['proof'],'proof',True) for b in bodies])
        generation=core._archive_touch(db,[('file_edits','e')])
        changes=set(db.execute('SELECT kind,entity FROM archive_changes WHERE generation=?',[generation]).fetchall())
        records=scan(db,state,changes=changes)
        assert [r['kind'] for r in records]==['tool.record']
        assert signed_row(records[0])['data']['edits']==bodies[-1]['row']['data']['edits']


def test_peer_local_import_cannot_overwrite_source_owned_rows(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'peer.db'
    apply_row_replicas(path,bodies,'w',[control],local_user=user,local_device=peer['id'])
    with duckdb.connect(str(path)) as db:
        core.upsert(db,core.ParseResult(convs=[dict(zip(core.ARCHIVE_COLUMNS['conversations'],['c','codex','peer overwrite',None,None,None,None,None,None,'{}']))]))
        assert db.execute('SELECT title FROM conversations').fetchone()==('title',)


def test_new_receiver_rejects_revision_lineage_that_changes_device(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    row={**bodies[0]['row'],'data':{**bodies[0]['row']['data'],'title':'peer revision'}}
    proof=row_proof(peer,user,'w',1,row,bodies[0]['proof']['revision'])
    with pytest.raises(ValueError,match='ownership conflict'):
        apply_row_replicas(tmp_path/'fresh.db',[dict(row=row,proof=proof,lineage=[bodies[0]['proof']])],'w',[control],local_user=user,local_device=peer['id'])


@pytest.mark.parametrize('chunked',[False,True])
def test_incomplete_local_tool_snapshot_does_not_erase_completed_output_or_edits(tmp_path,chunked):
    user,device,peer,control,bodies=signed_graph()
    with duckdb.connect(str(tmp_path/'source.db')) as db:
        core.init_schema(db)
        core.project_logical_rows(db,[(b['row'],b['proof'],'proof',True) for b in bodies])
        tool=dict(zip(core.ARCHIVE_COLUMNS['tool_calls'],['t','m','write','{}','""','pending',None,None]))
        result=core.ParseResult(tools=[tool],edit_evidence=[dict(file_edit_id='e',status='unknown',reason='result_missing',tool_call_id='t')])
        for part in core.ingest_parts(result,size=1) if chunked else [result]: core.upsert(db,part)
        assert db.execute('SELECT status,output FROM tool_calls').fetchone()==('complete','{}')
        assert db.execute('SELECT content FROM file_edits').fetchone()==('one',)
        assert db.execute('SELECT status FROM provenance.file_edit_evidence').fetchone()==('confirmed',)


def test_edit_metadata_removal_and_tombstone_clean_derived_graph(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'peer.db'
    apply=lambda values:apply_row_replicas(path,values,'w',[control],local_user=user,local_device=peer['id'])
    apply(bodies)
    row=core.logical_row('tool_calls',identity='t',state='deleted')
    proof=row_proof(device,user,'w',1,row,bodies[-1]['proof']['revision'])
    apply([dict(row=row,proof=proof)])
    with duckdb.connect(str(path)) as db:
        assert not db.execute('SELECT * FROM file_edits').fetchall()
        assert not db.execute('SELECT * FROM provenance.file_edit_evidence').fetchall()
        assert not db.execute('SELECT * FROM remote.derived_edits').fetchall()


def test_migration_preserves_legacy_custom_tool_edits_and_attachment_identity(tmp_path):
    root=core.hook_root('codex')
    root.mkdir(parents=True)
    path=root/'session.jsonl'
    events=[dict(type='session_meta',payload=dict(id='session',cwd=str(tmp_path))),dict(type='response_item',payload=dict(type='message',role='user',content=[dict(type='input_text',text='write'),dict(type='input_image',image_url='data:image/png;base64,YWJj')])),dict(type='response_item',payload=dict(type='custom_tool_call',call_id='call',name='apply_patch',input='*** Begin Patch\n*** Add File: a.py\n+one\n*** End Patch')),dict(type='response_item',payload=dict(type='custom_tool_call_output',call_id='call',output='Success. Updated the following files:\nA a.py'))]
    path.write_text('\n'.join(map(json.dumps,events)))
    archive=tmp_path/'archive.db'
    with duckdb.connect(str(archive)) as db:
        core.init_schema(db)
        result=core.parse_codex(root.parent,bindings={('codex','session'):'legacy'})
        core.prepare_result(result)
        core.upsert(db,result)
        tool,edit,attachment=[db.execute(f'SELECT id FROM {table}').fetchone()[0] for table in ('tool_calls','file_edits','attachments')]
        db.execute('UPDATE attachments SET id=? WHERE id=?',['legacy-remapped-attachment',attachment])
        attachment='legacy-remapped-attachment'
        oldtool,oldedit=core.gen_id('codex','custom:legacy:2'),core.gen_id('codex','edit:legacy:2:0')
        db.execute('UPDATE tool_calls SET id=? WHERE id=?',[oldtool,tool])
        db.execute('UPDATE provenance.file_edit_evidence SET tool_call_id=?,file_edit_id=? WHERE file_edit_id=?',[oldtool,oldedit,edit])
        db.execute('UPDATE file_edits SET id=? WHERE id=?',[oldedit,edit])
    core.reset_archive_sync(archive,'user','device')
    with duckdb.connect(str(archive)) as db:
        before={table:db.execute(f'SELECT id FROM {table} ORDER BY id').fetchall() for table in core.ARCHIVE_COLUMNS}
        result=core.parse_codex(root.parent,bindings=core.session_bindings(db))
        core.prepare_result(result)
        core.upsert(db,result)
        assert {table:db.execute(f'SELECT id FROM {table} ORDER BY id').fetchall() for table in core.ARCHIVE_COLUMNS}==before
        assert db.execute('SELECT id FROM file_edits').fetchall()==[(oldedit,)]
        assert db.execute('SELECT id FROM attachments').fetchall()==[(attachment,)]
        assert not core.archive_relationships(db)


def test_core_upgrade_migrates_before_local_import_without_contacting_relay(tmp_path):
    root=tmp_path/'archive'
    (root/'data').mkdir(parents=True)
    (root/'remote').mkdir()
    path=root/'data/convos.db'
    sessions=core.hook_root('codex')
    sessions.mkdir(parents=True)
    (sessions/'s.jsonl').write_text(json.dumps(dict(type='session_meta',payload=dict(id='session'))))
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('legacy','codex','{}')")
        db.execute("INSERT INTO provider_sessions VALUES ('codex','session','legacy'); UPDATE core_schema SET version=16")
    (root/'remote/config.json').write_text(json.dumps(dict(user='user',device=dict(id='device'),url='http://unreachable.invalid',sync_version=1)))
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        assert db.execute('SELECT id FROM conversations').fetchone()==(core.gen_id('codex','session'),)
        assert db.execute('SELECT version FROM archive_sync').fetchone()==(2,)
        assert db.execute('SELECT version FROM core_schema').fetchone()==(core.CORE_VERSION,)
        core.init_schema(db)
        assert db.execute('SELECT count(*) FROM conversations').fetchone()==(1,)


def test_single_row_projection_applies_and_removes_embedded_edits(tmp_path):
    user,device,peer,control,bodies=signed_graph()
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        for b in bodies: core.project_logical_row(db,b['row'],b['proof'],'proof')
        assert db.execute('SELECT content FROM file_edits').fetchall()==[('one',)]
        core.project_logical_row(db,core.logical_row('tool_calls',identity='t',state='deleted'),bodies[-1]['proof'],'deleted')
        assert not db.execute('SELECT * FROM file_edits').fetchall()
        assert not db.execute('SELECT * FROM remote.derived_edits').fetchall()


def test_cutover_retains_locally_imported_parent_even_if_only_peer_signed_it(tmp_path):
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'archive.db'
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        core.project_logical_rows(db,[(b['row'],b['proof'],'proof',True) for b in bodies])
        db.execute("INSERT INTO provider_sessions VALUES ('codex','missing-source-file','c')")
        db.execute("INSERT INTO messages(id,conversation_id,parent_id,metadata,role) VALUES ('child','c','m','{}','assistant')")
        signer=control['devices'][device['id']]
        core.project_attested_rows(db,[(b['row'],b['proof']) for b in bodies],signer['root_public'],signer['certificate'])
    core.reset_archive_sync(path,user,peer['id'])
    with duckdb.connect(str(path)) as db:
        assert db.execute('SELECT id FROM messages ORDER BY id').fetchall()==[('child',),('m',)]
        assert not core.archive_relationships(db)


def test_removing_tool_edits_preserves_signed_provenance_history(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas,audit_rows
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    path=tmp_path/'peer.db'
    file=core.provenance_digest(dict(repository=None,path='a.txt'))
    facts=[core._provenance_record('file.observed',file,dict(id=file,repository=None,path='a.txt',kind='external'),None),core._provenance_record('edit.observed','e',dict(id='e',turn='m',file=file,repository=None,old_content_hash=None,new_content_hash=core.provenance_digest(b'one'),evidence='captured_exact'),None)]
    bodies += [dict(row=row,proof=row_proof(device,user,'w',1,row)) for fact in facts for row in [core.logical_fact(fact)]]
    apply=lambda rows:apply_row_replicas(path,rows,'w',[control],local_user=user,local_device=peer['id'])
    apply(bodies)
    assert audit_rows(path,local_user=user)['totals']['unavailable']==0
    row={**bodies[2]['row'],'data':{**bodies[2]['row']['data'],'edits':[]}}
    apply([dict(row=row,proof=row_proof(device,user,'w',1,row,bodies[2]['proof']['revision']))])
    assert audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_attachment_cache_relocation_does_not_create_a_historical_duplicate(tmp_path):
    user,device,peer,control,bodies=signed_graph()
    paths=[tmp_path/name/'attachment.bin' for name in ('old','new')]
    for p in paths:
        p.parent.mkdir()
        p.write_bytes(b'unchanged')
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        core.project_logical_rows(db,[(b['row'],b['proof'],'proof',True) for b in bodies])
        for p in paths: core.upsert(db,core.ParseResult(attachs=[dict(id='a',message_id='m',filename='attachment.bin',mime_type='application/octet-stream',size=9,path=str(p),url=None,created_at=None)],attachment_indexes={'a':(core.provenance_digest(b'unchanged'),9)}))
        assert db.execute('SELECT id,path FROM attachments').fetchall()==[('a',str(paths[1]))]
        assert db.execute('SELECT count(*) FROM attachment_bodies').fetchone()==(1,)


def test_embedded_historical_edit_keeps_its_original_message(tmp_path):
    from ai_convos_remote.projection import apply_row_replicas,audit_rows
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,bodies=signed_graph()
    row={**bodies[1]['row'],'id':'later-message'}
    bodies.append(dict(row=row,proof=row_proof(device,user,'w',1,row)))
    row={**bodies[2]['row'],'data':{**bodies[2]['row']['data'],'message_id':'later-message'}}
    bodies[2]=dict(row=row,proof=row_proof(device,user,'w',1,row))
    path=tmp_path/'peer.db'
    apply_row_replicas(path,bodies,'w',[control],local_user=user,local_device=peer['id'])
    with duckdb.connect(str(path)) as db:
        assert db.execute('SELECT message_id FROM file_edits').fetchone()==('m',)
        assert db.execute('SELECT message_id FROM tool_calls').fetchone()==('later-message',)
    assert audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_pending_signed_owner_claim_prevents_peer_evidence_import(tmp_path):
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        db.execute("INSERT INTO archive_sync VALUES (TRUE,2,'user','peer')")
        db.execute("INSERT INTO remote.row_owners VALUES ('file_edits','pending-edit','user','source')")
        core.upsert(db,core.ParseResult(edit_evidence=[dict(file_edit_id='pending-edit',status='confirmed',reason='provider_success',tool_call_id='pending-tool')]))
        assert not db.execute('SELECT * FROM provenance.file_edit_evidence').fetchall()


def test_local_only_upgrade_maps_claude_subagent_and_legacy_blank_line_coordinates(tmp_path):
    root=core.hook_root('claude-code')/'project/subagents'
    root.mkdir(parents=True)
    events=[dict(type='system',sessionId='root-session'),dict(type='assistant',agentId='agent',uuid='provider',message=dict(content='one')),dict(type='user',agentId='agent',message=dict(content='two'))]
    (root/'agent-file.jsonl').write_text('\n\n'.join(map(json.dumps,events)))
    path=tmp_path/'archive.db'
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('legacy','claude-code','{}')")
        db.execute("INSERT INTO provider_sessions VALUES ('claude-code','agent','legacy'); UPDATE core_schema SET version=16")
        db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('old-provider','legacy','assistant','one','{\"provider_index\":1}'),('idless','legacy','user','two','{\"provider_index\":2}')")
        core.init_schema(db)
        cid=core.gen_id('claude-code','agent')
        assert db.execute('SELECT id FROM conversations').fetchone()==(cid,)
        assert set(db.execute('SELECT id FROM messages').fetchall())=={(core.gen_id('claude-code',f'message:{cid}:provider'),),('idless',)}
        core.upsert(db,core.parse_claude_code(core.hook_root('claude-code'),bindings=core.session_bindings(db)))
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(2,)
        assert not core.archive_relationships(db)


def test_chunked_peer_reimport_skips_children_whose_bodies_have_not_arrived():
    from ai_convos_remote.projection import apply_row_replicas
    user,device,peer,control,bodies=signed_graph()
    apply_row_replicas(core.DB_PATH,bodies[:2],'w',[control],local_user=user,local_device=peer['id'])
    messages=[dict(id=f'pending-{i}',conversation_id='c',role='assistant',content='pending',thinking=None,created_at=None,model=None,metadata='{}',parent_id=None) for i in range(501)]
    result=core.ParseResult(msgs=messages,tools=[dict(id='pending-tool',message_id=messages[-1]['id'],tool_name='write',input='{}',output='{}',status='complete',duration_ms=None,created_at=None)],edits=[dict(id='pending-edit',message_id=messages[-1]['id'],file_path='a.txt',edit_type='write',content='pending',created_at=None,old_content=None)],edit_evidence=[dict(file_edit_id='pending-edit',status='confirmed',reason='provider_success',tool_call_id='pending-tool')])
    assert core.commit_result(result,purpose='test.peer.partial')[:7]==(0,0,0,0,0,0,0)
    with core.open_db(core.DB_PATH,read_only=True) as db:
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(1,)
        assert not db.execute('SELECT * FROM file_edits').fetchall()


def test_unchanged_received_source_is_not_reparsed_on_every_sync(monkeypatch):
    from ai_convos_remote.projection import apply_row_replicas
    from ai_convos_remote.protocol import row_proof
    user,device,peer,control,_=signed_graph()
    root=core.hook_root('codex')
    root.mkdir(parents=True)
    (root/'session.jsonl').write_text('\n'.join(map(json.dumps,[dict(type='session_meta',payload=dict(id='session')),dict(type='response_item',payload=dict(type='message',role='user',content=[dict(type='input_text',text='hello')]))])))
    parsed=core.parse_codex(root.parent)
    rows=[core.logical_row(kind,list(row),list(row.values())) for kind,records in [('conversations',parsed.convs),('messages',parsed.msgs)] for row in records]
    apply_row_replicas(core.DB_PATH,[dict(row=row,proof=row_proof(device,user,'w',1,row)) for row in rows],'w',[control],local_user=user,local_device=peer['id'])
    core.sync(False,300,False,True,False,False,True)
    monkeypatch.setattr(core,'parse_codex',lambda *args:pytest.fail('unchanged peer source was parsed again'))
    core.sync(False,300,False,True,False,False,True)
