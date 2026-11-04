"""Claude Code transcripts resumed in Claude Desktop re-append events under the same uuid (#173)."""
import json

import duckdb

from ai_convos import cli as core

A,B='aaaaaaaa-0000-4000-8000-000000000001','bbbbbbbb-0000-4000-8000-000000000002'
uid=lambda i:f'00000000-0000-4000-8000-{i:012d}'


def ev(i,sid,**extra):
    role='user' if i%2 else 'assistant'
    message=(dict(role='user',content=[dict(type='tool_result',tool_use_id='call',content='written')] if i==5 else f'question {i}') if role=='user' else
             dict(id=f'msg_{i}',type='message',role='assistant',model='claude-x',content=[dict(type='text',text=f'answer {i}'),*([dict(type='tool_use',id='call',name='Write',input=dict(file_path='/tmp/demo/a.txt',content='x'))] if i==4 else [])]))
    return dict(dict(uuid=uid(i),parentUuid=uid(i-1) if i>1 else None,sessionId=sid,timestamp=f'2026-11-01T10:{i:02d}:00.000Z',type=role,cwd='/tmp/demo',slug='calm-blue-fox',message=message,
                     **(dict(promptId='p1') if role=='user' else {}),**(dict(toolUseResult=dict(type='create',filePath='/tmp/demo/a.txt')) if i==5 else {})),**extra)


def desktop_resumed_session():
    """Events 3-6 re-appended with the same uuid and message but other metadata, plus Desktop's second file that copies the first line for line."""
    first=[ev(i,A) for i in range(1,7)]
    dups=[dict(e,slug='other-green-owl',cwd='/tmp/demo/sub',**(dict(promptId='p2') if e['type']=='user' else {}),**(dict(toolUseResult=dict(type='update')) if 'toolUseResult' in e else {})) for e in first[2:]]
    a,b=first+dups+[ev(i,A) for i in (7,8)],first+dups+[ev(i,A) for i in (7,8)]+[ev(i,B) for i in (9,10)]
    (root:=core.hook_root('claude-code')/'-tmp-demo').mkdir(parents=True)
    [(root/f'{sid}.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events)) for sid,events in ((A,a),(B,b))]
    return b


def assert_one_conversation(db,events,edits=1):
    cid,first=core.gen_id('claude-code',A),{e['uuid']:i for i,e in reversed(list(enumerate(events)))}
    assert db.execute('SELECT id FROM conversations').fetchall()==[(cid,)]
    assert sorted(db.execute("SELECT id,json_extract(metadata,'$.provider_index')::INT FROM messages").fetchall())==sorted((core.gen_id('claude-code',f'message:{cid}:{u}'),i) for u,i in first.items())
    assert db.execute('SELECT output,status FROM tool_calls').fetchall()==[('"written"','complete')]
    assert db.execute('SELECT count(DISTINCT message_id),count(*) FROM file_edits').fetchone()==(1,edits)
    assert not core.archive_relationships(db)


def test_fresh_import_of_desktop_resumed_session_keeps_first_occurrence(tmp_path):
    events=desktop_resumed_session()
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        core.upsert(db,core.parse_claude_code(core.hook_root('claude-code'),bindings=core.session_bindings(db)))
        assert_one_conversation(db,events)
        rows=lambda:[db.execute(f'SELECT {",".join(core.ARCHIVE_COLUMNS[table])} FROM {table} ORDER BY id').fetchall() for table in ('messages','tool_calls','file_edits')]
        before=rows()
        core.upsert(db,core.parse_claude_code(core.hook_root('claude-code'),bindings=core.session_bindings(db)))
        assert rows()==before


def test_schema_16_archive_with_repeated_uuids_upgrades(tmp_path):
    events=desktop_resumed_session()
    old=lambda idx:core.gen_id('claude-code',f'legacy:{idx}')
    last={e['uuid']:old(idx) for idx,e in enumerate(events)}
    with duckdb.connect(str(tmp_path/'archive.db')) as db:
        core.init_schema(db)
        # Schema 16 parsers stored one row per transcript line: ids and provider_index by line position, parents by the last line of a uuid.
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('legacy','claude-code','{}')")
        db.execute("INSERT INTO provider_sessions VALUES ('claude-code',?,'legacy')",[A])
        db.execute('UPDATE core_schema SET version=16')
        db.executemany('INSERT INTO messages(id,conversation_id,role,content,metadata,parent_id) VALUES (?,?,?,?,?,?)',[(old(idx),'legacy',e['type'],e['message']['content'] if e['type']=='user' and isinstance(e['message']['content'],str) else e['message']['content'][0].get('text',''),json.dumps(dict(provider_index=idx)),last.get(e['parentUuid'])) for idx,e in enumerate(events)])
        db.execute("INSERT INTO tool_calls VALUES (?,?,'Write',?,'\"written\"','complete',NULL,NULL)",[tool:=core.gen_id('claude-code','tool:legacy:call'),last[uid(4)],json.dumps(dict(file_path='/tmp/demo/a.txt',content='x'))])
        edits=[(core.gen_id('claude-code',f'edit:legacy:{idx}:0'),old(idx)) for idx,e in enumerate(events) if e['uuid']==uid(4)]
        db.executemany("INSERT INTO file_edits VALUES (?,?,'/tmp/demo/a.txt','write','x',NULL,NULL)",edits)
        db.executemany("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','provider_success',?)",[(edit,tool) for edit,_ in edits])
        core.init_schema(db)
        assert db.execute('SELECT version FROM core_schema').fetchone()==(core.CORE_VERSION,)
        assert_one_conversation(db,events,edits=2)  # the legacy parser's second edit row stays, now on the merged message
        core.upsert(db,core.parse_claude_code(core.hook_root('claude-code'),bindings=core.session_bindings(db)))
        assert_one_conversation(db,events,edits=2)
