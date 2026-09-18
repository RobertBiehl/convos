"""Source event identities distinguish duplicate projections from repeated edits."""
import json

import pytest
from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.protocol import row_proof
from tests.test_remote_projection import signed_edit_graph


def fixture(tmp_path,source):
    path=tmp_path/'session.jsonl'
    stamp='2026-01-01T00:00:00Z'
    if source=='codex':
        patch='*** Begin Patch\n*** Update File: x.py\n@@\n-old\n+new\n@@\n-old\n+new\n*** End Patch'
        events=[dict(type='session_meta',timestamp=stamp,payload=dict(id='session',cwd=str(tmp_path))),
                dict(type='response_item',timestamp=stamp,payload=dict(type='message',role='user',content=[dict(type='input_text',text='apply both edits')])),
                dict(type='response_item',timestamp=stamp,payload=dict(type='custom_tool_call',name='apply_patch',call_id='call',input=patch,status='completed')),
                dict(type='response_item',timestamp=stamp,payload=dict(type='custom_tool_call_output',call_id='call',output='Success. Updated'))]
    else:
        events=[dict(type='system',timestamp=stamp,sessionId='session',cwd=str(tmp_path)),
                dict(type='assistant',timestamp=stamp,message=dict(content=[dict(type='tool_use',id=f'call-{i}',name='Edit',input=dict(file_path=str(tmp_path/'x.py'),old_string='old',new_string='new')) for i in range(2)])),
                dict(type='user',timestamp=stamp,message=dict(content=[dict(type='tool_result',tool_use_id=f'call-{i}',content='ok') for i in range(2)]))]
    path.write_text('\n'.join(map(json.dumps,events)))
    parser=core.parse_codex_session if source=='codex' else core.parse_claude_code_session
    old=parser(path)
    canonical=core.gen_id('test','canonical')
    bound={(source,'session'):canonical,(source,'session','legacy'):[old['conv']['id']]}
    bound.update({(source,canonical,'message',json.loads(m['metadata'])['provider_index']):m['id'] for m in old['msgs']})
    return path,parser,old,bound,canonical


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('alter',[False,True])
def test_alias_reimport_marks_only_exact_edit_copies_and_retains_every_body(tmp_path,source,alter):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    dbpath=tmp_path/'data/convos.db'
    with core._core(dbpath,purpose='test.edit-alias') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],**{k:old[k] for k in ('msgs','tools','edits','edit_evidence')}))
        db.execute('UPDATE conversations SET id=?',[canonical])
        db.execute('UPDATE messages SET conversation_id=?',[canonical])
        db.execute('UPDATE provider_sessions SET conversation_id=?',[canonical])
        if alter: db.execute('UPDATE file_edits SET content=? WHERE id=?',['unique evidence',old['edits'][0]['id']])
        before={r[0]:r for r in db.execute('SELECT * FROM file_edits').fetchall()}
        parsed=parser(path,bound)
        result=core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')})
        for part in core.ingest_parts(result,1):
            with core._transaction(db): core.upsert(db,part)
        assert len(parsed['edits'])==2
        current=db.execute('SELECT id FROM file_edits WHERE id NOT IN (SELECT old_id FROM parser_edit_history) ORDER BY id').fetchall()
        assert current==sorted([(e['id'],) for e in parsed['edits']]+([(old['edits'][0]['id'],)] if alter else []))
        assert all(db.execute('SELECT * FROM file_edits WHERE id=?',[key]).fetchone()==value for key,value in before.items())
        assert not core.archive_relationships(db)


@pytest.mark.parametrize('source',['codex','claude-code'])
def test_received_pre_lineage_message_preserves_pending_native_classification(tmp_path,source):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    dbpath=tmp_path/'data/convos.db'
    with core._core(dbpath,purpose='test.edit-dirty-native') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],**{k:old[k] for k in ('msgs','tools','edits','edit_evidence')}))
        db.execute('UPDATE conversations SET id=?',[canonical])
        db.execute('UPDATE messages SET conversation_id=?',[canonical])
        db.execute('UPDATE provider_sessions SET conversation_id=?',[canonical])
        parsed=parser(path,bound)
        core.upsert(db,core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')}))
        before=db.execute('SELECT id,metadata FROM messages ORDER BY id').fetchall()
        current=db.execute('SELECT id FROM current_file_edits ORDER BY id').fetchall()
        rows=[core.logical_row('messages',core.ARCHIVE_COLUMNS['messages'],row) for row in db.execute('SELECT '+','.join(core.ARCHIVE_COLUMNS['messages'])+' FROM messages').fetchall()]
    assert len(current)==2
    _,device,user,control,*_=signed_edit_graph()
    for row in rows:
        row['data']['metadata']={k:v for k,v in row['data']['metadata'].items() if k not in ('convos_edit_lineage','convos_tool_lineage')}
    projection.apply_row_replicas(dbpath,[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows],'w',[control],local_user=user)
    with core._core(dbpath,True,purpose='test.edit-dirty-preserved') as db:
        assert db.execute('SELECT id,metadata FROM messages ORDER BY id').fetchall()==before
        assert db.execute('SELECT id FROM current_file_edits ORDER BY id').fetchall()==current
        assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(4,)


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('normalized',[False,True])
@pytest.mark.parametrize('hook',[False,True])
def test_reparented_edit_snapshots_keep_their_original_identity(tmp_path,source,normalized,hook):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    if normalized and source=='codex': path.write_text(path.read_text().replace('File: x.py','File: '+str(tmp_path/'x.py')))
    first=parser(path,{key:value for key,value in bound.items() if len(key)!=4})
    parsed=parser(path,bound)
    result=lambda value:core.ParseResult(convs=[value['conv']],**{k:value[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')})
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-snapshot-parent') as db:
        core.init_schema(db)
        core.upsert(db,result(first))
        core.upsert(db,result(parsed))
        assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(4,)
        for before,after in zip(first['msgs'],parsed['msgs']): db.execute('UPDATE file_edits SET message_id=? WHERE message_id=?',[after['id'],before['id']])
        if normalized:
            fid=core.provenance_digest(dict(repository=None,path='x.py'))
            db.execute("INSERT OR IGNORE INTO provenance.files VALUES (?,NULL,'x.py','external')",[fid])
            db.execute("UPDATE file_edits SET file_path='x.py'")
            for edit in parsed['edits']:
                db.execute('UPDATE provenance.file_edit_scopes SET route=? WHERE file_edit_id=?',[str(tmp_path/'x.py'),edit['id']])
                db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,NULL,NULL,'captured_exact')",[edit['id'],fid])
                db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES ('edit.observed',?),('file.observed',?)",[edit['id'],fid])
        originals=db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()
        core.upsert(db,core.hook_result(source,path,bound) if hook else result(parser(path,bound)))
        assert db.execute('SELECT count(*) FROM current_file_edits').fetchone()==(2,)
        assert db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()==originals


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('normalized',[False,True])
@pytest.mark.parametrize('alter',[False,True])
def test_exact_source_lineage_classifies_legacy_children_arriving_after_source_is_gone(tmp_path,source,normalized,alter):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    parsed=parser(path,bound)
    dbpath=tmp_path/'data/convos.db'
    with core._core(dbpath,purpose='test.late-alias-source') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')}))
        if normalized:
            fid=core.provenance_digest(dict(repository=None,path='x.py'))
            db.execute("INSERT OR IGNORE INTO provenance.files VALUES (?,NULL,'x.py','external')",[fid])
            for edit in old['edits']: db.execute('INSERT OR IGNORE INTO provenance.file_edit_scopes(file_edit_id,path,route) VALUES (?,?,?)',[edit['id'],'external/pending/x.py',str(tmp_path/'x.py')])
            for edit in parsed['edits']:
                db.execute('UPDATE provenance.file_edit_scopes SET route=? WHERE file_edit_id=?',[str(tmp_path/'x.py'),edit['id']])
                db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,NULL,NULL,'captured_exact')",[edit['id'],fid])
                db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES ('edit.observed',?),('file.observed',?)",[edit['id'],fid])
            parsed=parser(path,bound)
            core.upsert(db,core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')}))
            old['edits']=[{**edit,'file_path':'x.py'} for edit in old['edits']]
        before_count=db.execute('SELECT count(*) FROM file_edits').fetchone()[0]
    path.unlink()
    if alter: old['edits'][0]['content']='unique late evidence'
    _,device,user,control,*_=signed_edit_graph()
    rows=[core.parser_logical_row(kind,row) for kind,values in [('tool_calls',old['tools']),('file_edits',old['edits'])] for row in values]
    bodies=[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows]
    for _ in range(2):
        projection.apply_row_replicas(dbpath,bodies,'w',[control],local_user=user)
        with core._core(dbpath,True,purpose='test.late-alias-result') as db:
            assert db.execute('SELECT count(*) FROM current_file_edits').fetchone()==(len(parsed['edits'])+int(alter),)
            assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(len(old['edits'])+before_count,)
            assert db.execute('SELECT count(*) FROM tool_calls WHERE id NOT IN (SELECT old_id FROM parser_tool_history)').fetchone()==(len(parsed['tools']),)
            claims=[(r['kind'],r['id'],r['id'],user,'active') for r in rows]
            assert list(core.typed_logical_rows(db,claims).values())==rows
        assert projection.audit_rows(dbpath,local_user=user)['totals']['unavailable']==0


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('fault',['none','path','content','route'])
@pytest.mark.parametrize('snapshot_owner',['original','current'])
def test_edit_history_recaptured_after_path_normalization_preserves_every_binding(tmp_path,source,fault,snapshot_owner):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    result=lambda value:core.ParseResult(convs=[value['conv']],**{k:value[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')})
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-recaptured-path') as db:
        core.init_schema(db)
        core.upsert(db,result(old))
        db.execute('UPDATE conversations SET id=?',[canonical])
        db.execute('UPDATE messages SET conversation_id=?',[canonical])
        db.execute('UPDATE provider_sessions SET conversation_id=?',[canonical])
        parsed=parser(path,bound)
        core.upsert(db,result(parsed))
        snapshots=[]
        for before,current in zip(old['edits'],parsed['edits']):
            first,second='external/first/x.py','external/second/x.py'
            normalized={**(before if snapshot_owner=='original' else current),'message_id':current['message_id'],'file_path':first}
            snapshot=core._history_row('file_edits',list(normalized.values()),tuple(v for i,v in enumerate(normalized.values()) if i not in (0,5)))
            snapshot[2]=second
            db.execute('INSERT INTO file_edits VALUES (?,?,?,?,?,?,?)',snapshot)
            snapshots.append(snapshot[0])
            db.execute('UPDATE file_edits SET message_id=?,file_path=? WHERE id=?',[current['message_id'],first,before['id']])
            db.execute('UPDATE provenance.file_edit_scopes SET path=?,route=? WHERE file_edit_id=?',[first,core._resolved(current['file_path'],str(tmp_path)),current['id']])
            db.execute('UPDATE provenance.file_edit_scopes SET path=?,route=? WHERE file_edit_id=?',[second,core._resolved(first if fault!='route' else 'unrelated.py',str(tmp_path)),before['id']])
        if fault in ('path','content'): db.execute('UPDATE file_edits SET '+('file_path' if fault=='path' else 'content')+'=? WHERE id=?',['unique evidence',snapshots[0]])
        original=db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()
        scopes=db.execute('SELECT * FROM provenance.file_edit_scopes ORDER BY file_edit_id').fetchall()
        for part in core.ingest_parts(result(parser(path,bound)),1): core.upsert(db,part)
        assert db.execute('SELECT count(*) FROM current_file_edits').fetchone()==(2+(2 if fault=='route' else int(fault!='none')),)
        assert db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()==original
        assert db.execute('SELECT * FROM provenance.file_edit_scopes ORDER BY file_edit_id').fetchall()==scopes


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('observed',[False,True])
def test_normalized_reimport_and_real_edit_revision_keep_the_captured_file(tmp_path,source,observed):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    result=lambda value:core.ParseResult(convs=[value['conv']],**{k:value[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')})
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-snapshot-scope') as db:
        core.init_schema(db)
        core.upsert(db,result(old))
        fid=core.provenance_digest(dict(repository=None,path='external/captured/x.py'))
        db.execute("INSERT INTO provenance.files VALUES (?,NULL,'external/captured/x.py','external')",[fid])
        for edit in old['edits']:
            db.execute("UPDATE file_edits SET file_path='external/captured/x.py' WHERE id=?",[edit['id']])
            db.execute("UPDATE provenance.file_edit_scopes SET path='external/captured/x.py',observed_at=? WHERE file_edit_id=?",['2026-01-01' if observed else None,edit['id']])
            db.execute("INSERT INTO provenance.file_edit_files VALUES (?,?,NULL,NULL,'captured_exact')",[edit['id'],fid])
            db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES ('edit.observed',?),('file.observed',?)",[edit['id'],fid])
        before=db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()
        core.upsert(db,result(parser(path)))
        assert db.execute('SELECT * FROM file_edits ORDER BY id').fetchall()==before
        changed=parser(path)
        changed['edits']=[{**edit,'content':'a real new revision'} for edit in changed['edits']]
        core.upsert(db,result(changed))
        current={e['id'] for e in old['edits']}
        histories=[r for r in db.execute('SELECT * FROM file_edits').fetchall() if r[0] not in current]
        assert len(histories)==2
        for history in histories:
            assert db.execute('SELECT path,route FROM provenance.file_edit_scopes WHERE file_edit_id=?',[history[0]]).fetchone()==('external/captured/x.py',core._resolved(old['edits'][0]['file_path'],str(tmp_path)))
            assert db.execute('SELECT file_id,evidence FROM provenance.file_edit_files WHERE file_edit_id=?',[history[0]]).fetchone()==(fid,'captured_exact')
            assert db.execute("SELECT 1 FROM provenance.local_facts WHERE kind='edit.observed' AND entity=?",[history[0]]).fetchone()==(1,)


@pytest.mark.parametrize('source',['codex','claude-code'])
@pytest.mark.parametrize('native',[False,True])
@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('upgrade',[False,True])
def test_edit_lineage_replays_without_source_files_and_keeps_original_rows(tmp_path,source,native,reverse,upgrade):
    path,parser,old,bound,canonical=fixture(tmp_path,source)
    sender=tmp_path/'sender.db'
    with core._core(sender,purpose='test.edit-source') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],**{k:old[k] for k in ('msgs','tools','edits','edit_evidence')}))
        db.execute('UPDATE conversations SET id=?',[canonical])
        db.execute('UPDATE messages SET conversation_id=?',[canonical])
        db.execute('UPDATE provider_sessions SET conversation_id=?',[canonical])
        parsed=parser(path,bound)
        core.upsert(db,core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')}))
        rows=[core.logical_row(kind,core.ARCHIVE_COLUMNS[kind],r) for kind in core.ARCHIVE_COLUMNS for r in db.execute('SELECT '+','.join(core.ARCHIVE_COLUMNS[kind])+' FROM '+kind).fetchall()]
    path.unlink()
    _,device,user,control,*_=signed_edit_graph()
    bodies=[dict(row=r,proof=row_proof(device,user,'w',1,r)) for r in rows]
    receiver=tmp_path/'receiver.db'
    local=user if native else 'recipient'
    for body in reversed(bodies) if reverse else bodies: projection.apply_row_replicas(receiver,[body],'w',[control],local_user=local)
    if upgrade:
        with core._core(receiver,purpose='test.edit-migration') as db:
            snapshot=lambda:{kind:db.execute('SELECT * FROM '+kind+' ORDER BY id').fetchall() for kind in (*core.ARCHIVE_COLUMNS,'remote.row_proofs')}
            before=snapshot()
            tool_history=db.execute('SELECT * FROM parser_tool_history ORDER BY old_id').fetchall()
            db.execute('DELETE FROM parser_tool_lineage')
            db.execute('DROP VIEW current_file_edits')
            db.execute('DROP VIEW parser_edit_history')
            db.execute('DROP TABLE parser_edit_lineage')
            db.execute('UPDATE core_schema SET version=15')
            core.init_schema(db)
            assert snapshot()==before
            assert db.execute('SELECT * FROM parser_tool_history ORDER BY old_id').fetchall()==tool_history
            assert db.execute('SELECT count(*) FROM current_file_edits').fetchone()==(2,)
        assert receiver.with_suffix('.db.pre-v16.bak').is_file()
    for _ in range(2):
        projection.apply_row_replicas(receiver,bodies,'w',[control],local_user=local)
        with core._core(receiver,True,purpose='test.edit-received') as db:
            assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(4,)
            assert db.execute('SELECT count(*) FROM file_edits WHERE id NOT IN (SELECT old_id FROM parser_edit_history)').fetchone()==(2,)
            claims=[('file_edits',r['id'] if native else core.remote_id(user,'file_edits',r['id']),r['id'],user,'active') for r in rows if r['kind']=='file_edits']
            assert list(core.typed_logical_rows(db,claims).values())==[r for r in rows if r['kind']=='file_edits']
            assert not core.archive_relationships(db)
        assert projection.audit_rows(receiver,local_user=local)['totals']['unavailable']==0


@pytest.mark.parametrize('route_matches',[False,True])
def test_normalized_path_history_requires_the_captured_route(tmp_path,route_matches):
    path,parser,old,bound,canonical=fixture(tmp_path,'codex')
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-path-history') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],**{k:old[k] for k in ('msgs','tools','edits','edit_evidence')}))
        for edit in old['edits']:
            normalized={**edit,'file_path':'x.py'}
            body=core._history_row('file_edits',list(normalized.values()),tuple(v for i,v in enumerate(normalized.values()) if i not in (0,5)))
            db.execute('INSERT INTO file_edits VALUES (?,?,?,?,?,?,?)',body)
            db.execute('UPDATE provenance.file_edit_scopes SET route=? WHERE file_edit_id=?',[str(tmp_path/('x.py' if route_matches else 'different.py')),edit['id']])
            fid=core.provenance_digest(dict(repository=None,path='x.py'))
            db.execute("INSERT OR IGNORE INTO provenance.files VALUES (?,NULL,'x.py','external')",[fid])
            db.execute('INSERT INTO provenance.file_edit_files VALUES (?,?,NULL,NULL,?)',[edit['id'],fid,'captured_exact'])
            db.execute("INSERT OR IGNORE INTO provenance.local_facts VALUES ('edit.observed',?),('file.observed',?)",[edit['id'],fid])
        parsed=parser(path)
        core.upsert(db,core.ParseResult(convs=[parsed['conv']],**{k:parsed[k] for k in ('msgs','tools','edits','edit_evidence','tool_lineage','edit_lineage')}))
        assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(4,)
        assert db.execute('SELECT count(*) FROM parser_edit_history').fetchone()==(2 if route_matches else 0,)


@pytest.mark.parametrize('links,expected',[
    ([(0,1),(1,2)],[(0,2),(1,2)]),
    ([(0,1),(1,2),(2,0)],[]),
    ([(0,1),(0,2)],[]),
    ([(0,1),(0,2),(1,2)],[(0,2),(1,2)]),
])
def test_edit_history_requires_an_unambiguous_terminal_identity(tmp_path,links,expected):
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-lineage-graph') as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source) VALUES ('c','codex')")
        db.execute("INSERT INTO messages(id,conversation_id,role) VALUES ('m','c','user')")
        db.executemany("INSERT INTO file_edits(id,message_id) VALUES (?,'m')",[(str(i),) for i in range(3)])
        db.executemany("INSERT INTO parser_edit_lineage VALUES ('m','',TRUE,?,'old',?,'new',?,?)",[(str(a),str(b),str(a),str(b)) for a,b in links])
        assert db.execute('SELECT old_id,current_id FROM parser_edit_history ORDER BY old_id').fetchall()==[(str(a),str(b)) for a,b in expected]
        assert db.execute('SELECT count(*) FROM file_edits').fetchone()==(3,)


def test_current_edit_view_composes_with_parent_queries_in_bounded_memory(tmp_path):
    with core._core(tmp_path/'data/convos.db',purpose='test.edit-view-cost') as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}')")
        db.execute("INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','user','{}')")
        db.execute("INSERT INTO file_edits(id,message_id) SELECT 'edit-'||range,'m' FROM range(32000)")
        db.execute("INSERT INTO parser_edit_lineage SELECT 'm','',TRUE,'edit-'||range,'old','edit-'||(range+16000),'new','edit-'||range,'edit-'||(range+16000) FROM range(16000)")
        db.execute("SET memory_limit='128MB'")
        db.execute('SET threads=1')
        timer=core.threading.Timer(10,db.interrupt)
        timer.start()
        try: assert db.execute("WITH owners AS MATERIALIZED (SELECT id,source,json_extract_string(metadata,'$.session_id') AS provider_session FROM conversations) SELECT c.source,c.provider_session,count(*) FROM current_file_edits t JOIN messages m ON m.id=t.message_id JOIN owners c ON c.id=m.conversation_id WHERE json_extract_string(m.metadata,'$.history_of') IS NULL GROUP BY 1,2").fetchall()==[('codex',None,16000)]
        finally: timer.cancel()
