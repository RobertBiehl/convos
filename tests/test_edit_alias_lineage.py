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
@pytest.mark.parametrize('native',[False,True])
@pytest.mark.parametrize('reverse',[False,True])
def test_edit_lineage_replays_without_source_files_and_keeps_original_rows(tmp_path,source,native,reverse):
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
