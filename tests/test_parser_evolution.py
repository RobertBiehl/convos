"""Historical parser identities reconcile through exact portable message evidence."""
import copy, json

import pytest
from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.protocol import row_proof
from tests.test_remote_projection import signed_edit_graph
from tests import legacy_parsers


def fixture(tmp_path,source):
    path=tmp_path/"session.jsonl"
    stamp="2026-01-01T00:00:00.000000"
    if source=="codex":
        events=[dict(type="session_meta",timestamp=stamp,payload=dict(id="session")),
                dict(type="response_item",timestamp=stamp,payload=dict(type="message",role="user",content=[dict(type="input_text",text="inspect")])),
                dict(type="response_item",timestamp=stamp,payload=dict(type="function_call",name="Read",call_id="call-1",arguments='{"path":"x.txt"}')),
                dict(type="response_item",timestamp=stamp,payload=dict(type="function_call_output",call_id="call-1",output="contents"))]
        old_ids=["tool:{cid}:2","toolout:{cid}:3"]
        parents=[2,3]
    else:
        events=[dict(type="system",sessionId="session"),
                dict(type="user",timestamp=stamp,message=dict(content="inspect")),
                dict(type="assistant",timestamp=stamp,message=dict(content=[dict(type="tool_use",id="call-1",name="Read",input=dict(path="x.txt"))])),
                dict(type="user",timestamp=stamp,message=dict(content=[dict(type="tool_result",tool_use_id="call-1",content="contents")]))]
        old_ids=["tool:{cid}:1:0","tool:{cid}:2:0"]
        parents=[1,2]
    path.write_text("\n".join(map(json.dumps,events)))
    parser=core.parse_codex_session if source=="codex" else core.parse_claude_code_session
    session=parser(path)
    cid=session["conv"]["id"]
    # Frozen old-parser output: indexed tools/results with their own missing parents.
    old=[dict(id=core.gen_id(source,pattern.format(cid=cid)),message_id=core.gen_id(source,f"{cid}:{parent}"),tool_name="Read" if i==0 else "call-1",input='{"path":"x.txt"}' if i==0 else '{}',output='{}' if i==0 else '"contents"',status="pending" if i==0 else "complete",duration_ms=None,created_at=core.ts_from_iso(stamp)) for i,(pattern,parent) in enumerate(zip(old_ids,parents))]
    historical=(legacy_parsers.parse_codex_session if source=='codex' else legacy_parsers.parse_claude_code_session)(path)
    assert [core.logical_row('tool_calls',list(r),list(r.values())) for r in historical['tools']]==[core.logical_row('tool_calls',list(r),list(r.values())) for r in old]
    assert {r['message_id'] for r in historical['tools']}.isdisjoint({m['id'] for m in historical['msgs']})
    old=historical['tools']
    unique=dict(id=core.gen_id(source,"unique-edit"),message_id=old[0]["message_id"],file_path="x.txt",edit_type="write",content="unique old evidence",created_at=core.ts_from_iso(stamp),old_content=None)
    return path,parser,session,old,unique


def old_archive(tmp_path,source):
    transcript,parser,session,old,unique=fixture(tmp_path,source)
    _,device,user,control,_,_,_,_=signed_edit_graph()
    path=tmp_path/"data/convos.db"
    values=[("conversations",session["conv"]),("messages",session["msgs"][0]),*(('tool_calls',r) for r in old),('file_edits',unique)]
    bodies=[dict(row=(row:=core.logical_row(kind,list(value),list(value.values()))),proof=row_proof(device,user,"w",1,row)) for kind,value in values]
    signer=control["devices"][device["id"]]
    with core._core(path,purpose="test.legacy") as db:
        core.init_schema(db)
        with core._transaction(db):
            for body in bodies:
                core.project_attested_rows(db,[(body['row'],body['proof'])],signer['root_public'],signer['certificate'])
                core.project_logical_row(db,body['row'],body['proof'],core.provenance_digest(body['proof']),native=True)
            db.execute("UPDATE core_schema SET version=12")
    return path,transcript,parser,session,old,unique,bodies,device,user,control


def reimport(db,parser,transcript,size=500):
    session=parser(transcript)
    result=core.ParseResult(convs=[session['conv']],**{k:session[k] for k in ('msgs','tools','attachs','edits','edit_evidence','tool_lineage')})
    for part in core.ingest_parts(result,size):
        with core._transaction(db): core.upsert(db,part)
    return session


@pytest.mark.parametrize("source",["codex","claude-code"])
@pytest.mark.parametrize("size",[1,500])
def test_upgrade_reimport_retains_legacy_rows_and_unique_edit(tmp_path,source,size):
    path,transcript,parser,session,old,unique,bodies,device,user,control=old_archive(tmp_path,source)
    with core._core(path,purpose="test.upgrade") as db:
        before=db.execute("SELECT * FROM file_edits").fetchall()
        proofs=db.execute("SELECT * FROM remote.row_proofs").fetchall()
        core.init_schema(db)
        reimport(db,parser,transcript,size)
        assert db.execute("SELECT old_id,current_id FROM parser_tool_history ORDER BY old_id").fetchall()==sorted((r['id'],session['tools'][0]['id']) for r in old)
        assert db.execute("SELECT * FROM file_edits").fetchall()==before
        assert db.execute("SELECT * FROM remote.row_proofs").fetchall()==proofs
        for row in old:
            claim=('tool_calls',row['id'],row['id'],user,'active')
            assert core.typed_logical_rows(db,[claim])[claim]==next(b['row'] for b in bodies if b['row']['id']==row['id'])
        snapshot={t:db.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in ('messages','tool_calls','file_edits','parser_tool_lineage','archive_state')}
        reimport(db,parser,transcript,size)
        assert {t:db.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in snapshot}==snapshot
        gaps=core.archive_relationships(db)
        if source=='codex':
            assert 'tool_calls.message_id' not in gaps
            assert db.execute('SELECT count(*) FROM tool_calls').fetchone()==(1,)
            assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='tool_calls'").fetchone()==(2,)
            assert gaps['file_edits.message_id']['rows']==1
    assert path.with_name(path.name+'.pre-v13.bak').is_file()
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


@pytest.mark.parametrize("source",["codex","claude-code"])
@pytest.mark.parametrize("reverse",[False,True])
def test_fresh_receiver_reconciles_signed_history_without_transcript(tmp_path,source,reverse):
    path,transcript,parser,session,old,unique,bodies,device,user,control=old_archive(tmp_path,source)
    with core._core(path,purpose="test.reimport") as db:
        core.init_schema(db)
        reimport(db,parser,transcript)
        current=[core.logical_row(table,core.ARCHIVE_COLUMNS[table],r) for table in ('messages','tool_calls') for r in db.execute(f"SELECT {','.join(core.ARCHIVE_COLUMNS[table])} FROM {table}").fetchall() if r[0] not in {t['id'] for t in old}]
    previous={b['row']['id']:b['proof']['revision'] for b in bodies}
    updates=[dict(row=r,proof=row_proof(device,user,'w',1,r,previous.get(r['id']))) for r in current]
    transcript.unlink()
    receiver=tmp_path/'receiver/data/convos.db'
    for body in list(reversed([*bodies,*updates])) if reverse else [*bodies,*updates]: projection.apply_row_replicas(receiver,[body],'w',[control],local_user='recipient')
    for _ in range(2):
        projection.apply_row_replicas(receiver,[*bodies,*updates],'w',[control],local_user='recipient')
        with core._core(receiver,True,purpose="test.received") as db:
            assert db.execute("SELECT old_id,current_id FROM parser_tool_history ORDER BY old_id").fetchall()==sorted((core.remote_id(user,'tool_calls',r['id']),core.remote_id(user,'tool_calls',session['tools'][0]['id'])) for r in old)
            assert db.execute("SELECT content FROM file_edits").fetchone()==(unique['content'],)
        assert projection.audit_rows(receiver,local_user='recipient')['totals']['unavailable']==0


@pytest.mark.parametrize("alter",["old_content","current_content","different_author","missing_current"])
def test_lineage_cannot_classify_changed_or_foreign_tools(tmp_path,alter):
    path,transcript,parser,session,old,unique,bodies,device,user,control=old_archive(tmp_path,'codex')
    with core._core(path,purpose="test.candidates") as db:
        core.init_schema(db)
        reimport(db,parser,transcript)
        message=db.execute("SELECT metadata FROM messages WHERE id=?",[session['msgs'][0]['id']]).fetchone()[0]
        assert len(json.loads(message)['convos_tool_lineage']['records'])==2
        if alter=='old_content': core._insert_pages(db,'tool_calls',[core.retired_projection('tool_calls',old[0]['id'],*map(json.loads,db.execute("SELECT body,projection FROM parser_retired_rows WHERE physical=?",[old[0]['id']]).fetchone()))],core.ARCHIVE_COLUMNS['tool_calls'])
        if alter=='old_content': db.execute("UPDATE tool_calls SET output='\"changed\"' WHERE id=?",[old[0]['id']])
        if alter=='current_content': db.execute("UPDATE tool_calls SET output='\"changed\"' WHERE id=?",[session['tools'][0]['id']])
        if alter=='different_author': db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id,source_row_id,author_user_id) VALUES ('tool_calls',?,?,'someone-else')",[old[0]['id'],old[0]['id']])
        if alter=='missing_current': db.execute("DELETE FROM tool_calls WHERE id=?",[session['tools'][0]['id']])
        core.reconcile_tool_lineage(db,[session['msgs'][0]['id']])
        assert db.execute("SELECT count(*) FROM parser_tool_history").fetchone()[0]==(1 if alter in ('old_content','different_author') else 0)


def test_fresh_import_has_no_unused_lineage_metadata(tmp_path):
    transcript,parser,session,old,unique=fixture(tmp_path,'codex')
    with core._core(tmp_path/'db',purpose="test.fresh") as db:
        core.init_schema(db)
        reimport(db,parser,transcript)
        assert all('convos_tool_lineage' not in json.loads(meta) for meta, in db.execute('SELECT metadata FROM messages').fetchall())
        assert db.execute('SELECT count(*) FROM parser_tool_lineage').fetchone()==(0,)


def test_multiple_authors_with_identical_source_ids_remain_independent(tmp_path):
    path,transcript,parser,session,old,unique,_,device,user,control=old_archive(tmp_path,'codex')
    with core._core(path,purpose='test.two_authors') as db:
        core.init_schema(db)
        reimport(db,parser,transcript)
        rows=[core.logical_row(table,core.ARCHIVE_COLUMNS[table],r) for table in ('conversations','messages','tool_calls') for r in db.execute(f"SELECT {','.join(core.ARCHIVE_COLUMNS[table])} FROM {table}").fetchall()]
        rows += [json.loads(body) for body, in db.execute('SELECT body FROM parser_retired_rows').fetchall()]
    _,device2,user2,control2,_,_,_,_=signed_edit_graph()
    combined={**control,'devices':control['devices']|control2['devices']}
    bodies=[dict(row=r,proof=row_proof(dev,author,'w',1,r)) for dev,author in ((device,user),(device2,user2)) for r in rows]
    target=tmp_path/'two-authors.db'
    projection.apply_row_replicas(target,bodies,'w',[combined],local_user='reader')
    with core._core(target,True,purpose='test.independence') as db: assert db.execute('SELECT count(*) FROM parser_tool_history').fetchone()==(4,)
    current=next(b for b in bodies if b['row']['id']==session['tools'][0]['id'] and b['proof']['author_user_id']==user)
    revised=copy.deepcopy(current['row'])
    revised['data']['output']='different result'
    projection.apply_row_replicas(target,[dict(row=revised,proof=row_proof(device,user,'w',1,revised,current['proof']['revision']))],'w',[combined],local_user='reader')
    with core._core(target,True,purpose='test.independence') as db:
        assert {r[0] for r in db.execute('SELECT old_id FROM parser_tool_history').fetchall()}=={core.remote_id(user2,'tool_calls',r['id']) for r in old}


def test_current_invocation_with_different_json_whitespace_is_not_duplicated(tmp_path):
    path,transcript,parser,session,old,unique,_,device,user,control=old_archive(tmp_path,'codex')
    with core._core(path,purpose='test.existing_current') as db:
        core.init_schema(db)
        with core._transaction(db): core.project_archive_row(db,'tool_calls',core.ARCHIVE_COLUMNS['tool_calls'],list(session['tools'][0].values()))
        assert db.execute('SELECT count(*) FROM tool_calls').fetchone()==(3,)
        reimport(db,parser,transcript)
        assert db.execute('SELECT count(*) FROM tool_calls').fetchone()==(1,)
        assert db.execute('SELECT count(*) FROM tool_calls WHERE id NOT IN (SELECT old_id FROM parser_tool_history)').fetchone()==(1,)


def test_tool_json_comparison_preserves_boolean_and_numeric_distinctions(tmp_path):
    transcript,parser,session,old,unique=fixture(tmp_path,'codex')
    with core._core(tmp_path/'db',purpose='test.json_types') as db:
        core.init_schema(db)
        reimport(db,parser,transcript)
        current=copy.deepcopy(session['tools'][0])
        for value in (True,1,1.0):
            current['input']=json.dumps({'value':value})
            with core._transaction(db): core.upsert(db,core.ParseResult(tools=[current]))
            actual=db.execute('SELECT input FROM tool_calls WHERE id=?',[current['id']]).fetchone()[0]
            assert json.dumps(json.loads(actual))==current['input']
        assert db.execute('SELECT count(*) FROM tool_calls').fetchone()==(4,)


@pytest.mark.parametrize('blank_lines',[False,True])
@pytest.mark.parametrize('changed',[False,True])
def test_codex_legacy_messages_require_exact_transcript_backed_evidence(tmp_path,blank_lines,changed):
    transcript=tmp_path/'rollout-2026-01-01T00-00-00-019a2f3d-9455-7820-b4f6-0beeb2bf1f6f.jsonl'
    rows=[dict(type='session_meta',payload=dict(id='019a2f3d-9455-7820-b4f6-0beeb2bf1f6f')),
          *[dict(type='response_item',timestamp=f'2026-01-01T00:00:0{i}Z',payload=dict(type='message',role='user',content=[dict(type='input_text',text=f'turn {i}')])) for i in range(1,4)]]
    transcript.write_text(('\n\n' if blank_lines else '\n').join(map(json.dumps,rows)))
    old=legacy_parsers.parse_codex_session(transcript)
    cid=core.gen_id('codex','canonical')
    current=core.parse_codex_session(transcript,{('codex',rows[0]['payload']['id']):cid})
    with core._core(tmp_path/'db',purpose='test.codex_message_upgrade') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],msgs=[{**m,'parent_id':None} for m in old['msgs']]))
        if changed: db.execute("UPDATE messages SET content='unique local evidence' WHERE id=?",[old['msgs'][0]['id']])
        result=core.ParseResult(convs=[current['conv']],msgs=current['msgs'],message_lineage=current['message_lineage'])
        core.upsert(db,result)
        assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='messages'").fetchone()==(2 if changed else 3,)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(4 if changed else 3,)
        if changed: assert db.execute('SELECT content FROM messages WHERE id=?',[old['msgs'][0]['id']]).fetchone()==('unique local evidence',)
        assert not core.archive_relationships(db)


@pytest.mark.parametrize('reverse',[False,True])
def test_codex_source_backed_retirement_replays_without_transcript(tmp_path,reverse):
    transcript,parser,_,_,_=fixture(tmp_path,'codex')
    transcript=transcript.rename(transcript.with_name('rollout-legacy.jsonl'))
    old=legacy_parsers.parse_codex_session(transcript)
    current=parser(transcript,{('codex','session'):core.gen_id('codex','canonical')})
    path=tmp_path/'sender.db'
    with core._core(path,purpose='test.codex_lineage') as db:
        core.init_schema(db)
        core.upsert(db,core.ParseResult(convs=[old['conv']],msgs=[{**m,'parent_id':None} for m in old['msgs']]))
        core.upsert(db,core.ParseResult(convs=[current['conv']],msgs=current['msgs'],message_lineage=current['message_lineage']))
        retained=[json.loads(v[0]) for v in db.execute('SELECT body FROM parser_retired_rows').fetchall()]
        rows=retained+[core.logical_row(kind,core.ARCHIVE_COLUMNS[kind],v) for kind in ('conversations','messages') for v in db.execute(f"SELECT {','.join(core.ARCHIVE_COLUMNS[kind])} FROM {kind}").fetchall()]
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(1,)
    _,device,user,control,*_=signed_edit_graph()
    bodies=[dict(row=row,proof=row_proof(device,user,'w',1,row)) for row in rows]
    transcript.unlink()
    target=tmp_path/'receiver.db'
    for body in reversed(bodies) if reverse else bodies: projection.apply_row_replicas(target,[body],'w',[control],local_user='reader')
    with core._core(target,True,purpose='test.codex_replay') as db:
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(1,)
        assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='messages'").fetchone()==(1,)
        assert not core.archive_relationships(db)
    assert projection.audit_rows(target,local_user='reader')['totals']['unavailable']==0


def test_changed_edit_preserves_its_original_provider_result(tmp_path):
    import duckdb
    db=duckdb.connect()
    core.init_schema(db)
    conv=dict(id='c',source='claude-code',title='edit',created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata='{}')
    message=dict(id='m',conversation_id='c',role='assistant',content='',thinking=None,created_at=None,model=None,metadata='{}',parent_id=None)
    tool=dict(id='t',message_id='m',tool_name='Write',input='{}',output='"first result"',status='complete',duration_ms=None,created_at=None)
    edit=dict(id='e',message_id='m',file_path='a.txt',edit_type='write',content='first contents',created_at=None,old_content=None)
    evidence=dict(file_edit_id='e',status='confirmed',reason='provider_success',tool_call_id='t')
    core.upsert(db,core.ParseResult(convs=[conv],msgs=[message],tools=[tool],edits=[edit],edit_evidence=[evidence]))
    core.upsert(db,core.ParseResult(tools=[{**tool,'output':'"second result"'}],edits=[{**edit,'content':'second contents'}],edit_evidence=[evidence]))
    assert db.execute('SELECT e.content,v.status,t.output FROM file_edits e JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id JOIN tool_calls t ON t.id=v.tool_call_id ORDER BY e.content').fetchall()==[('first contents','confirmed','"first result"'),('second contents','confirmed','"second result"')]
    assert not core.archive_relationships(db)


def test_upgrade_recovers_each_exact_signed_retired_variant_and_keeps_it(tmp_path):
    _,device,user,control,rows,proofs,bodies,_=signed_edit_graph()
    first=rows['tool_calls']
    second={**first,'data':{**first['data'],'output':'later result'}}
    p1=proofs['tool_calls']
    p2=row_proof(device,user,'w',1,second,p1['revision'])
    path=tmp_path/'retired.db'
    signer=control['devices'][device['id']]
    with core._core(path,purpose='test.retired_encodings') as db:
        core.init_schema(db)
        for body,proof in ((first,p1),(second,p2)):
            core.project_row_proof(db,proof,signer['root_public'],signer['certificate'])
            variant={**body,'data':{**body['data'],'created_at':str(body['data']['created_at'])+'.000000'}}
            core._insert_pages(db,'parser_retired_rows',[('tool_calls','t','t',user,core.provenance_digest(variant),variant,{'id':'t','message_id':'m'},'current-tool')])
        db.execute('UPDATE core_schema SET version=14')
        core.init_schema(db)
        restored=dict(db.execute('SELECT proof_id,body FROM remote.row_conflicts').fetchall())
        assert {pid:json.loads(body) for pid,body in restored.items()}=={core.provenance_digest(p1):first,core.provenance_digest(p2):second}
        core.retire_row_bodies(db,[('tool_calls','t',user,p['revision']) for p in (p1,p2)])
        core.init_schema(db)
        assert dict(db.execute('SELECT proof_id,body FROM remote.row_conflicts').fetchall())==restored
    assert path.with_name(path.name+'.pre-v15.bak').is_file()


def test_source_replay_repairs_all_event_timestamps_without_content_history(tmp_path):
    import duckdb
    stamp=core.ts_from_iso('2026-01-01T00:00:00Z')
    conv=dict(id='c',source='claude-code',title='source',created_at=stamp,updated_at=stamp,model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps(dict(capture_mode='transcript',timestamp_basis='utc')))
    msg=dict(id='m',conversation_id='c',role='assistant',content='',thinking=None,created_at=stamp,model=None,metadata='{}',parent_id=None)
    tool=dict(id='t',message_id='m',tool_name='Write',input='{}',output='"written"',status='complete',duration_ms=None,created_at=stamp)
    edit=dict(id='e',message_id='m',file_path='x.txt',edit_type='write',content='source text',created_at=stamp,old_content=None)
    evidence=dict(file_edit_id='e',status='confirmed',reason='provider_success',tool_call_id='t')
    result=core.ParseResult([conv],[msg],tools=[tool],edits=[edit],edit_evidence=[evidence])
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        core.upsert(db,copy.deepcopy(result))
        for table in ('conversations','messages','tool_calls','file_edits'): db.execute(f"UPDATE {table} SET created_at=created_at-INTERVAL '8 hours'")
        db.execute("UPDATE conversations SET updated_at=updated_at+INTERVAL '8 hours',metadata=CAST(? AS JSON)",[json.dumps(dict(capture_mode='transcript'))])
        core.upsert(db,copy.deepcopy(result))
        for table in ('conversations','messages','tool_calls','file_edits'): assert db.execute(f'SELECT created_at FROM {table}').fetchall()==[(stamp,)]
        assert db.execute('SELECT updated_at FROM conversations').fetchall()==[(stamp,)]
        assert db.execute('SELECT * FROM provenance.file_edit_evidence').fetchall()==[('e','confirmed','provider_success','t')]
        before=core.archive_state(db)
        core.upsert(db,copy.deepcopy(result))
        assert core.archive_state(db)==before
