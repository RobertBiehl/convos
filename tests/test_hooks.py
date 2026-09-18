import contextlib, json, os, signal, subprocess, sys, threading, time
from pathlib import Path
import duckdb, pytest, typer
from typer.testing import CliRunner
from ai_convos import cli
POPEN=subprocess.Popen

def test_ingest_chunk_identity_work_is_bounded_and_parents_precede_children():
    reads=[]
    class Message(dict):
        def __getitem__(self,key):
            if key=='id': reads.append(key)
            return super().__getitem__(key)
    rows=[Message(id=str(i),conversation_id='conversation',role='user',content=f'turn {i}',thinking=None,created_at=None,model=None,parent_id=str(i-1) if i else None,metadata='{}') for i in range(1000)]
    result=cli.ParseResult(msgs=list(reversed(rows)),scopes=[],edit_scopes=[])
    parts=cli.ingest_parts(result,size=100)
    assert len(reads)<20*len(rows), 'Chunk planning must not rebuild identities for every chunk'
    assert [m for part in parts for m in part.msgs]==rows
    assert all(len(part.msgs)<=100 for part in parts)

def test_unchanged_ingest_reports_committed_progress_without_dirtying_archive():
    conv=dict(id='c',source='codex',title='source',created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata='{}')
    msgs=[dict(id=str(i),conversation_id='c',role='user',content=str(i),thinking=None,created_at=None,model=None,metadata='{}',parent_id=None) for i in range(501)]
    with cli._core(purpose='test.noop.init') as db: cli.init_schema(db)
    cli.commit_result(cli.ParseResult(convs=[conv],msgs=msgs),purpose='test.noop.seed')
    with cli._core(read_only=True,purpose='test.noop.before') as db: before=cli.archive_state(db)
    progress=[]
    def pulse(stage):
        with cli._core(read_only=True,purpose='test.noop.progress') as db: assert db.execute('SELECT count(*) FROM messages').fetchone()==(501,)
        progress.append(stage)
    result=cli.commit_result(cli.ParseResult(convs=[conv],msgs=msgs),purpose='test.noop.ingest',progress=pulse)
    with cli._core(read_only=True,purpose='test.noop.after') as db: assert cli.archive_state(db)==before
    assert result[:7]==(0,0,0,0,0,0,0)
    assert len(progress)==3 and len(set(progress))==3

def test_doctor_reports_native_conversations_without_recorded_local_sources(tmp_path,capsys):
    present,missing=tmp_path/'present.jsonl',tmp_path/'missing.jsonl'
    present.write_text('{}\n')
    session='019a2f3d-9455-7820-b4f6-0beeb2bf1f6f'
    with cli._core(purpose='test.coverage.init') as db:
        cli.init_schema(db)
        db.executemany("INSERT INTO conversations(id,source,metadata) VALUES (?,'codex',?)",[(cid,json.dumps(dict(session_id=sid))) for cid,sid in [('present','available'),('missing','rollout-2025-10-29T10-12-36-'+session),('unknown','untracked'),('foreign','untracked')]])
        db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id,source_row_id,author_user_id) VALUES ('conversations','foreign','foreign','another-user')")
    cli.atomic_json(cli.STATE_PATH,dict(local=dict(codex=dict(parser=cli.PARSER_EPOCH,files={str(present):0,str(missing):0},bindings={str(present):['available',['present',[]]],str(missing):[session,['missing',[]]],str(tmp_path/'also-missing.jsonl'):['available',['present',[]]]}))))
    cli.doctor(False)
    output=capsys.readouterr().out
    assert 'local sources codex: present=1, missing=1, untracked=1 (native conversations)' in output

@pytest.fixture
def hooks(tmp_path, monkeypatch):
    data, codex = tmp_path/"data", tmp_path/".codex"; sessions = codex/"sessions"; sessions.mkdir(parents=True)
    for k, v in (("DATA_DIR", data), ("DB_PATH", data/"convos.db"), ("HOOK_DIR", data/"hook_inbox"), ("HOOK_STATE", data/"hook_state.json"), ("HOOK_PROGRESS", data/"hook_progress.json"), ("HOOK_EMBED_DIRTY", data/"hook_embeddings_dirty"), ("HOOK_FTS_DIRTY", data/"hook_fts_dirty")): monkeypatch.setattr(cli, k, v)
    monkeypatch.setenv("CODEX_HOME", str(codex)); monkeypatch.setattr(cli.subprocess, "Popen", lambda *a, **k: None if a[0][1:4] == ["-m", "ai_convos", "drain-hooks"] else POPEN(*a, **k))
    return sessions, data

def transcript(path, user="remember alpha", assistant=None):
    rows = [{"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":user}]}}]
    if assistant: rows.append({"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":assistant}]}})
    path.write_text("\n".join(json.dumps(x) for x in rows))

def enqueue(path,command="capture"):
    r = CliRunner().invoke(cli.app, [command, "codex"], input=json.dumps({"transcript_path":str(path), "cwd":"/private", "session_id":"secret"}))
    assert r.exit_code == 0

def test_hook_is_nonblocking_coalesced_and_private(hooks, monkeypatch):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); launched=[]; monkeypatch.setattr(cli.subprocess,"Popen",lambda args,**kwargs:launched.append(args))
    monkeypatch.setattr(cli, "get_db", lambda *a, **k: (_ for _ in ()).throw(AssertionError("hook touched db")))
    enqueue(path); enqueue(path,"hook")
    queued = list((data/"hook_inbox").glob("*.json")); assert len(queued) == 1
    raw = queued[0].read_text(); assert "remember alpha" not in raw and "secret" not in raw and set(json.loads(raw)) == {"source", "path", "mtime", "size"} and all(args[-1]=="--no-block" for args in launched)

def test_explicit_drain_is_nonblocking_unless_requested(hooks,monkeypatch):
    calls=[]; monkeypatch.setattr(cli,"drain_hooks",lambda **kwargs:calls.append(kwargs)); runner=CliRunner(); assert runner.invoke(cli.app,["drain-hooks"]).exit_code==runner.invoke(cli.app,["drain-hooks","--block"]).exit_code==0 and calls==[{"block":False,"provenance":True},{"block":True,"provenance":True}]

def test_active_drainer_buffers_and_coalesces_hooks_without_spawning(hooks,monkeypatch):
    sessions,_=hooks
    transcript(path:=sessions/'active.jsonl')
    launched=[]
    monkeypatch.setattr(cli.subprocess,'Popen',lambda *args,**kwargs:launched.append(args))
    with cli.operation_lock(cli.HOOK_DIR/'.drain.lock','test.active'):
        for _ in range(10): cli.enqueue_hook('codex',dict(transcript_path=str(path)))
    assert not launched and len(list(cli.HOOK_DIR.glob('*.json')))==1
    assert cli.drain_hooks()==1

def test_enqueue_during_drainer_exit_cannot_lose_wakeup(hooks,monkeypatch):
    sessions,_=hooks
    transcript(first:=sessions/'first.jsonl')
    transcript(later:=sessions/'later.jsonl','later')
    enqueue(first)
    launched=[]
    real=cli.operation_lock
    @contextlib.contextmanager
    def exiting(path,purpose,*args,**kwargs):
        with real(path,purpose,*args,**kwargs) as pulse:
            yield pulse
            if purpose=='hooks.drain': cli.enqueue_hook('codex',dict(transcript_path=str(later)))
    monkeypatch.setattr(cli.subprocess,'Popen',lambda *args,**kwargs:launched.append(args))
    monkeypatch.setattr(cli,'operation_lock',exiting)
    assert cli.drain_hooks()==1
    assert len(launched)==1 and len(list(cli.HOOK_DIR.glob('*.json')))==1

def test_intermediate_capture_minute_limit_and_completion_bypass(hooks,monkeypatch):
    sessions,_=hooks; transcript(path:=sessions/"rate.jsonl"); launched=[]; now=[1000.0]
    monkeypatch.setattr(cli.time,"time",lambda:now[0]); monkeypatch.setattr(cli.subprocess,"Popen",lambda *a,**k:launched.append(a))
    event=lambda name:cli.enqueue_hook("codex",dict(transcript_path=str(path),hook_event_name=name))
    event("PostToolUse")
    for offset in (1,20,59.99): now[0]=1000+offset; event("PostToolUse")
    assert len(launched)==1
    event("Stop"); event("SessionEnd"); assert len(launched)==3
    now[0]=1060; event("PostToolUse"); assert len(launched)==4

def test_unchanged_capture_and_queued_duplicate_do_not_open_archive(hooks,monkeypatch):
    sessions,data=hooks; transcript(path:=sessions/"duplicate.jsonl"); enqueue(path); assert cli.drain_hooks()==1
    launched=[]; monkeypatch.setattr(cli.subprocess,"Popen",lambda *a,**k:launched.append(a)); monkeypatch.setattr(cli,"_core",lambda **k:pytest.fail("unchanged hook opened archive"))
    enqueue(path); assert not launched and not list(cli.HOOK_DIR.glob("*.json"))
    st=path.stat(); cli.atomic_json(cli.HOOK_DIR/f"{cli.gen_id('hook',f'codex:{path}')}.json",dict(source="codex",path=str(path),mtime=st.st_mtime_ns,size=st.st_size))
    assert cli.drain_hooks()==0 and not list(cli.HOOK_DIR.glob("*.json"))

def test_incidental_drain_and_manual_sync_do_not_wait_for_worker(hooks, monkeypatch):
    _,data=hooks; (data/"hook_inbox").mkdir(parents=True); hold=POPEN([sys.executable,"-c","import fcntl,sys; f=open(sys.argv[1],'w'); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); input()",str(data/"hook_inbox/.drain.lock")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True); monkeypatch.setattr(cli,"capture_provenance",lambda *a,**k:[]); threads=[]
    try:
        assert hold.stdout.readline().strip()=="ready"; done=threading.Event(); results=[]; threads.append(threading.Thread(target=lambda:(results.append(cli.drain_hooks()),cli.sync(False,300,False,False,False,False,True),done.set()))); threads[-1].start(); assert done.wait(10) and results==[0]; threads[-1].join()
        done=threading.Event(); threads.append(threading.Thread(target=lambda:(cli.drain_hooks(block=True),done.set()))); threads[-1].start(); assert not done.wait(.1); hold.stdin.write("\n"); hold.stdin.flush(); assert done.wait(5); threads[-1].join()
    finally:
        if hold.poll() is None: hold.stdin.write("\n"); hold.stdin.flush()
        hold.wait(timeout=5); [thread.join(5) for thread in threads]

def test_drain_releases_inbox_lock_before_parsing(hooks, monkeypatch):
    sessions,data=hooks; path=sessions/"s.jsonl"; transcript(path); enqueue(path); parse=cli.hook_result
    def unlocked(*args):
        result=subprocess.run([sys.executable,"-c","import fcntl,sys; f=open(sys.argv[1],'w'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)",str(data/"hook_inbox/.lock")]); assert result.returncode==0; return parse(*args)
    monkeypatch.setattr(cli,"hook_result",unlocked); assert cli.drain_hooks()==1

def test_drain_bounds_batch_records_progress_and_hands_off(hooks,monkeypatch):
    sessions,data=hooks; launched=[]; monkeypatch.setattr(cli,"HOOK_DRAIN_EVENTS",2); monkeypatch.setattr(cli.subprocess,"Popen",lambda args,**kwargs:launched.append(args))
    for i in range(5): transcript(path:=sessions/f"{i}.jsonl",f"event {i}"); enqueue(path)
    launched.clear(); assert cli.drain_hooks()==2 and len(list((data/"hook_inbox").glob("*.json")))==3 and len(launched)==1
    progress=json.loads((data/"hook_progress.json").read_text()); assert (progress["processed"],progress["failed"],progress["pending"],bool(progress["oldest"]))==(2,0,3,True)

def test_drain_stops_between_events_at_time_budget(hooks,monkeypatch):
    sessions,data=hooks; launched=[]; monkeypatch.setattr(cli,"HOOK_DRAIN_SECONDS",-1); monkeypatch.setattr(cli.subprocess,"Popen",lambda args,**kwargs:launched.append(args))
    for i in range(3): transcript(path:=sessions/f"timed-{i}.jsonl",f"timed {i}"); enqueue(path)
    launched.clear(); assert cli.drain_hooks()==1 and len([*list((data/"hook_inbox").glob("*.json")),*list((data/"hook_inbox").glob("*.work"))])==2 and len(launched)==1
    monkeypatch.setattr(cli,"HOOK_DRAIN_SECONDS",10); assert cli.drain_hooks()==2 and not [*list((data/"hook_inbox").glob("*.json")),*list((data/"hook_inbox").glob("*.work"))]

def test_failed_only_drain_does_not_respawn_tight_loop(hooks,monkeypatch):
    sessions,data=hooks; path=sessions/"bad.jsonl"; transcript(path); enqueue(path); launched=[]; monkeypatch.setattr(cli.subprocess,"Popen",lambda args,**kwargs:launched.append(args)); monkeypatch.setattr(cli,"hook_result",lambda *_:(_ for _ in ()).throw(ValueError("bad transcript")))
    assert cli.drain_hooks()==0 and not launched and json.loads((data/"hook_progress.json").read_text())["failed"]==1

def test_failed_prefix_does_not_starve_healthy_capture_or_spin(hooks,monkeypatch):
    sessions,_=hooks; launched=[]; parse=cli.hook_result; monkeypatch.setattr(cli,"HOOK_DRAIN_EVENTS",2)
    for name in ("bad-0","bad-1","bad-2","good"): transcript(path:=sessions/f"{name}.jsonl",name); enqueue(path)
    def broken(source,path,bindings):
        if path.stem.startswith("bad"): raise ValueError("bad transcript")
        return parse(source,path,bindings)
    monkeypatch.setattr(cli,"hook_result",broken); monkeypatch.setattr(cli.subprocess,"Popen",lambda args,**kwargs:launched.append(kwargs))
    assert cli.drain_hooks()==0 and len(launched)==1
    monkeypatch.setenv("CONVOS_HOOK_ATTEMPT",launched[0]["env"]["CONVOS_HOOK_ATTEMPT"])
    assert cli.drain_hooks()==1 and len(launched)==1
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert db.execute("SELECT content FROM messages").fetchall()==[("good",)]
    assert len(list(cli.HOOK_DIR.glob("*.json")))==3
    monkeypatch.setattr(cli,"hook_result",parse)
    monkeypatch.delenv("CONVOS_HOOK_ATTEMPT")
    assert cli.drain_hooks()==2 and cli.drain_hooks()==1

def test_replaced_failed_capture_is_eligible_in_same_drain_attempt(hooks,monkeypatch):
    sessions,_=hooks; transcript(path:=sessions/"retry.jsonl"); enqueue(path); parse=cli.hook_result
    monkeypatch.setattr(cli,"hook_result",lambda *_:(_ for _ in ()).throw(ValueError("partial transcript")))
    assert cli.drain_hooks()==0
    monkeypatch.setenv("CONVOS_HOOK_ATTEMPT",json.loads(cli.HOOK_PROGRESS.read_text())["attempt"])
    monkeypatch.setattr(cli,"hook_result",parse); transcript(path,"completed transcript"); enqueue(path)
    assert cli.drain_hooks()==1 and not list(cli.HOOK_DIR.glob("*.json"))
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert db.execute("SELECT content FROM messages").fetchall()==[("completed transcript",)]

def test_provenance_backlog_drains_without_another_hook_and_stops_on_failure(hooks,monkeypatch):
    root=hooks[0].parent
    subprocess.run(['git','-C',str(root),'init','-q'],check=True)
    rows=[dict(id=f'c{i}',source='codex',title='pending',created_at=None,updated_at=None,model=None,cwd=str(root),git_branch=None,project_id=None,metadata='{}') for i in range(501)]
    with cli._core(ready=True,purpose='test.schema'): pass
    cli.commit_result(cli.ParseResult(convs=rows),purpose='test.backlog')
    launched=[]
    monkeypatch.setattr(cli,'wake_hooks',lambda *a:launched.append(a))
    capture=cli.capture_provenance
    monkeypatch.setattr(cli,'capture_provenance',lambda **kw:(_ for _ in ()).throw(ValueError('conflicting provenance')))
    assert cli.drain_hooks(provenance=True)==0 and not launched
    assert json.loads(cli.HOOK_PROGRESS.read_text())['provenance_pending']==501
    monkeypatch.setattr(cli,'capture_provenance',capture)
    assert cli.drain_hooks()==0 and len(launched)==1
    assert json.loads(cli.HOOK_PROGRESS.read_text())['provenance_pending']==1
    assert cli.drain_hooks()==0 and len(launched)==1
    assert json.loads(cli.HOOK_PROGRESS.read_text())['provenance_pending']==0

def test_drain_publishes_progress_before_releasing_worker_lease(hooks,monkeypatch):
    write=cli.atomic_json; observed=[]
    def publish(path,value):
        if path==cli.HOOK_PROGRESS:
            with cli.operation_lock(cli.HOOK_DIR/".drain.lock","fixture.probe",0,mandatory=False) as acquired: observed.append(acquired is None)
        return write(path,value)
    monkeypatch.setattr(cli,"atomic_json",publish)
    assert cli.drain_hooks()==0 and observed==[True]

def test_concurrent_sync_exits_immediately_and_explicitly(hooks,capsys):
    _,data=hooks; data.mkdir(); hold=POPEN([sys.executable,"-c","import sys; from pathlib import Path; from ai_convos.cli import operation_lock\nwith operation_lock(Path(sys.argv[1]),'sync'): print('ready',flush=True); input()",str(data/".sync.lock")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True); done=threading.Event(); result=[]
    def attempt():
        try: cli.sync(False,300,False,False,False,False,True)
        except BaseException as error: result.append(error)
        finally: done.set()
    try:
        assert hold.stdout.readline().strip()=="ready"; thread=threading.Thread(target=attempt); thread.start(); assert done.wait(5); thread.join(); assert len(result)==1 and isinstance(result[0],typer.Exit)
        assert not (data/"convos.db").exists() and "another operation is already running" in capsys.readouterr().err and result[0].exit_code==1
    finally: hold.stdin.write("\n"); hold.stdin.flush(); hold.wait(timeout=5)

def test_concurrent_sync_cli_error_has_no_traceback(hooks):
    _,data=hooks; data.mkdir()
    with cli.operation_lock(data/".sync.lock","sync"):
        result=CliRunner().invoke(cli.app,["sync","--local-only"])
    assert result.exit_code==1 and "Could not start sync" in result.output and "PID " in result.output and "Traceback" not in result.output and "LockBusy" not in result.output

def test_parallel_sync_checkpoints_keep_queue_lease_alive(hooks,tmp_path,monkeypatch):
    sessions,_=hooks; transcript(sessions/"codex.jsonl"); claude=tmp_path/"claude/projects"; claude.mkdir(parents=True); (claude/"claude.jsonl").write_text("fixture"); monkeypatch.setenv("CLAUDE_CONFIG_DIR",str(claude.parent)); barrier=threading.Barrier(2); parse=lambda *_:(barrier.wait(),cli.ParseResult())[-1]
    monkeypatch.setattr(cli,"parse_codex",parse); monkeypatch.setattr(cli,"parse_claude_code",parse); monkeypatch.setattr(cli,"drain_hooks",lambda:0); monkeypatch.setattr(cli,"capture_provenance",lambda **_:None); monkeypatch.setattr(cli,"ingest_parts",lambda _:[]); real=cli.operation_lock; completed=[]
    @contextlib.contextmanager
    def quick(path,purpose,wait=30,identity=None,mandatory=True):
        with real(path,purpose,.03 if purpose=="hooks.queue" else wait,identity,mandatory) as pulse: yield pulse
    def commit(r,purpose,progress=None,parts=None):
        completed.append(threading.get_ident())
        for i in range(5): time.sleep(.02); progress(f"{purpose} {i}")
        return (*[0]*7,set())
    monkeypatch.setattr(cli,"operation_lock",quick); monkeypatch.setattr(cli,"commit_result",commit); cli.sync(False,300,True,True,False,False,True)
    assert len(completed)==2

def test_sync_ingestion_leaves_hook_inbox_available(hooks,monkeypatch):
    sessions,data=hooks
    transcript(sessions/'sync.jsonl')
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    real=cli.commit_result
    def ingest(*args,**kwargs):
        with cli.operation_lock(cli.HOOK_DIR/'.lock','test.enqueue',wait=0): pass
        return real(*args,**kwargs)
    monkeypatch.setattr(cli,'commit_result',ingest)
    cli.sync(False,300,False,True,False,False,True)
    assert str(sessions/'sync.jsonl') in cli.load_state()['local']['codex']['files']

def test_sync_checkpoints_import_before_provenance_failure(hooks,monkeypatch):
    sessions,data=hooks
    transcript(sessions/'saved.jsonl')
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    def fail(**kwargs): raise ValueError('provenance failure after committed import')
    monkeypatch.setattr(cli,'capture_provenance',fail)
    with pytest.raises(ValueError,match='after committed import'): cli.sync(False,300,False,True,False,False,True)
    assert str(sessions/'saved.jsonl') in cli.load_state()['local']['codex']['files']
    monkeypatch.setattr(cli,'capture_provenance',lambda **kwargs:None)
    monkeypatch.setattr(cli,'parse_codex',lambda *args:pytest.fail('completed import was repeated'))
    cli.sync(False,300,False,True,False,False,True)

@pytest.mark.parametrize('upgrade',['parser','input_bindings'])
def test_parser_upgrade_checkpoints_batches_and_resumes_unfinished_inputs(hooks,monkeypatch,upgrade):
    sessions,data=hooks
    for i in range(25): transcript(sessions/f'{i:02}.jsonl',f'turn {i}')
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    cli.sync(False,300,False,True,False,False,True)
    old=cli.load_state()
    if upgrade=='parser':
        old['local']['codex'].pop('epochs')
        old['local']['codex']['parser']=cli.PARSER_EPOCH-1
    else: old['local']['codex'].pop('bindings')
    cli.atomic_json(cli.STATE_PATH,old)
    real=cli.parse_codex
    parsed=[]
    def interrupted(path,files,bindings):
        if parsed: raise RuntimeError('interrupted second batch')
        parsed.extend(map(str,files))
        return real(path,files,bindings)
    monkeypatch.setattr(cli,'parse_codex',interrupted)
    with pytest.raises(cli.click.ClickException,match='Sync incomplete'): cli.sync(False,300,False,True,False,False,True)
    state=cli.load_state()['local']['codex']
    assert (sum(v==cli.PARSER_EPOCH for v in state['epochs'].values()) if upgrade=='parser' else len(state['bindings']))==20
    resumed=[]
    monkeypatch.setattr(cli,'parse_codex',lambda path,files,bindings:resumed.extend(map(str,files)) or real(path,files,bindings))
    cli.sync(False,300,False,True,False,False,True)
    assert len(resumed)==5 and set(resumed).isdisjoint(parsed)
    assert set(cli.load_state()['local']['codex']['epochs'].values())=={cli.PARSER_EPOCH}

def test_missing_transcript_keeps_last_successful_input_and_archive(hooks,monkeypatch,capsys):
    sessions,data=hooks
    transcript(path:=sessions/'removed.jsonl')
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    cli.sync(False,300,False,True,False,False,True)
    before=cli.load_state()['local']['codex']
    path.unlink()
    cli.sync(False,300,False,True,False,False,True)
    state=cli.load_state()['local']['codex']
    assert state['files']==before['files'] and state['epochs']==before['epochs'] and state['missing']==[str(path)]
    assert 'missing locally' in capsys.readouterr().err
    with cli._core(read_only=True,purpose='test.retained') as db: assert db.execute('SELECT count(*) FROM messages').fetchone()==(1,)

def test_explicit_drain_is_idempotent_and_preserves_truncated_rewritten_history(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; runner = CliRunner(); transcript(path); enqueue(path)
    assert "Database not found" in runner.invoke(cli.app,["search","remember alpha"]).output and cli.drain_hooks()==1
    assert json.loads(runner.invoke(cli.app, ["search", "remember alpha", "-f", "json"]).stdout)[0]["source"] == "codex"
    transcript(path, assistant="second answer"); enqueue(path); assert cli.drain_hooks()==1
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2; updated = conn.execute("SELECT updated_at FROM conversations").fetchone()[0]; conn.close()
    transcript(path); enqueue(path); assert cli.drain_hooks()==1
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2; assert conn.execute("SELECT updated_at FROM conversations").fetchone()[0] == updated; conn.close()
    transcript(path, user="rewritten alpha"); enqueue(path); assert cli.drain_hooks()==1
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3; assert conn.execute("SELECT COUNT(*) FROM messages WHERE content IN ('remember alpha','rewritten alpha')").fetchone()[0] == 2; meta = json.loads(conn.execute("SELECT metadata FROM messages WHERE content='remember alpha'").fetchone()[0]); assert meta["history_of"] and meta["superseded_at"]; conn.close()
    enqueue(path); assert cli.drain_hooks()==0; conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3; conn.close()

def test_changed_mtime_without_changed_rows_does_not_touch_archive(hooks):
    sessions,data=hooks; path=sessions/"same.jsonl"; transcript(path); enqueue(path); assert cli.drain_hooks()==1
    conn=duckdb.connect(str(data/"convos.db")); generation=cli.archive_state(conn)[1]; conn.close(); os.utime(path,ns=(path.stat().st_atime_ns,path.stat().st_mtime_ns+1)); enqueue(path); assert cli.drain_hooks()==1
    conn=duckdb.connect(str(data/"convos.db")); assert cli.archive_state(conn)[1]==generation; conn.close()

def test_enqueue_during_drain_survives_for_next_worker(hooks, monkeypatch):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); original, raced = cli.upsert, {"done":False}
    def upsert(conn, result):
        out = original(conn, result)
        if not raced["done"]: raced["done"] = True; transcript(path, "newer alpha"); enqueue(path)
        return out
    monkeypatch.setattr(cli, "upsert", upsert); assert cli.drain_hooks() == 1
    assert len(list((data/"hook_inbox").glob("*.json"))) == 1
    monkeypatch.setattr(cli, "upsert", original); assert cli.drain_hooks() == 1
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages WHERE content IN ('remember alpha','newer alpha')").fetchone()[0] == 2; conn.close()

def test_failed_parse_returns_claim_to_queue(hooks, monkeypatch):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); original = cli.hook_result
    monkeypatch.setattr(cli, "hook_result", lambda *_: (_ for _ in ()).throw(ValueError("partial transcript"))); assert cli.drain_hooks() == 0
    assert len(list((data/"hook_inbox").glob("*.json"))) == 1 and not list((data/"hook_inbox").glob("*.work"))
    monkeypatch.setattr(cli, "hook_result", original); assert cli.drain_hooks() == 1

def test_missing_parser_dependency_does_not_discard_hook_input(hooks,monkeypatch):
    sessions,_=hooks; transcript(path:=sessions/"missing.jsonl"); enqueue(path)
    monkeypatch.setattr(cli,"hook_result",lambda *_:(_ for _ in ()).throw(FileNotFoundError("parser dependency unavailable")))
    assert cli.drain_hooks()==0 and json.loads(cli.HOOK_PROGRESS.read_text())["failed"]==1
    assert len(list(cli.HOOK_DIR.glob("*.json")))==1

def test_hook_retries_provenance_after_ingestion_committed(hooks,tmp_path,monkeypatch):
    sessions,_=hooks; root=tmp_path/"repo"; root.mkdir(); subprocess.run(["git","-C",str(root),"init","-q"],check=True)
    transcript(path:=sessions/"git-retry.jsonl"); path.write_text(path.read_text().replace('"/repo"',json.dumps(str(root)))); enqueue(path); run=cli._git_run
    monkeypatch.setattr(cli,"_git_run",lambda *args:(_ for _ in ()).throw(subprocess.CalledProcessError(1,args,stderr=b"temporary git failure")))
    assert cli.drain_hooks()==1
    with cli.open_db(read_only=True,purpose="fixture.read") as db:
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]==1
        assert db.execute("SELECT checkout LIKE 'pending:%' FROM provenance.conversation_scopes").fetchone()[0]
    assert not [*cli.HOOK_DIR.glob('*.json'),*cli.HOOK_DIR.glob('*.work')]
    assert json.loads(cli.HOOK_PROGRESS.read_text())['provenance_error']
    monkeypatch.setattr(cli,'hook_result',lambda *_:pytest.fail('Committed capture was parsed again for provenance'))
    monkeypatch.setattr(cli,"_git_run",run); assert cli.drain_hooks()==0
    with cli.open_db(read_only=True,purpose="fixture.read") as db:
        assert not db.execute("SELECT checkout LIKE 'pending:%' FROM provenance.conversation_scopes").fetchone()[0]
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]==1

def test_local_sync_retries_failed_unchanged_transcript(hooks,monkeypatch):
    sessions,_=hooks; parse=cli.parse_codex_session
    for name in ("good","bad"): transcript(sessions/f"{name}.jsonl",name)
    def broken(path,bindings=None):
        if path.stem=="bad": raise ValueError("temporary parser failure")
        return parse(path,bindings)
    monkeypatch.setattr(cli,"parse_codex_session",broken)
    with pytest.raises(cli.click.ClickException,match="Sync incomplete"): cli.sync(False,300,False,True,False,False,True)
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert db.execute("SELECT content FROM messages").fetchall()==[("good",)]
    assert str(sessions/"bad.jsonl") not in cli.load_state()["local"]["codex"]["files"]
    monkeypatch.setattr(cli,"parse_codex_session",parse); cli.sync(False,300,False,True,False,False,True)
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert set(db.execute("SELECT content FROM messages").fetchall())=={("good",),("bad",)}

def test_local_sync_notices_backward_transcript_mtime(hooks):
    sessions,_=hooks; transcript(path:=sessions/"restored.jsonl","original"); cli.sync(False,300,False,True,False,False,True); previous=path.stat()
    transcript(path,"restored evidence"); os.utime(path,ns=(previous.st_atime_ns,previous.st_mtime_ns-60_000_000_000)); cli.sync(False,300,False,True,False,False,True)
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert set(db.execute("SELECT content FROM messages").fetchall())=={("original",),("restored evidence",)}

def test_stable_metadata_only_session_completes_until_changed(hooks):
    sessions, data = hooks; path = sessions/"empty.jsonl"; path.write_text(json.dumps({"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}})); enqueue(path)
    assert cli.drain_hooks() == 1 and not list((data/"hook_inbox").glob("*.json"))
    state = json.loads((data/"hook_state.json").read_text()); assert len(state) == 1 and next(iter(state.values())) == [path.stat().st_mtime_ns, path.stat().st_size]

def test_orphaned_claim_leaves_authoritative_fts_state_stale(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); q = next((data/"hook_inbox").glob("*.json")); work = q.with_suffix(".work"); q.replace(work)
    conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); cli.upsert(conn, cli.hook_result("codex", path)); conn.close()
    assert cli.drain_hooks() == 1
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT fts_generation IS NULL OR messages_generation<>fts_generation FROM retrieval_state").fetchone()[0]; conn.close()

def test_recovered_completion_preserves_newer_queued_capture_at_time_budget(hooks,monkeypatch):
    sessions,_=hooks; transcript(path:=sessions/"recovered.jsonl"); enqueue(path); queue=next(cli.HOOK_DIR.glob("*.json")); event=json.loads(queue.read_text()); assert cli.drain_hooks()==1
    cli.atomic_json(queue.with_suffix(".work"),{**event,"snap":[event["mtime"],event["size"]],"changed":[]})
    transcript(other:=sessions/"other.jsonl","other"); enqueue(other); transcript(path,"newer recovered evidence"); enqueue(path)
    monkeypatch.setattr(cli,"HOOK_DRAIN_SECONDS",-1); assert cli.drain_hooks()==2
    assert [*cli.HOOK_DIR.glob("*.json"),*cli.HOOK_DIR.glob("*.work")], "newer queued capture was erased by recovered completion"
    monkeypatch.setattr(cli,"HOOK_DRAIN_SECONDS",10); assert cli.drain_hooks()==1
    with cli.open_db(read_only=True,purpose="fixture.read") as db: assert db.execute("SELECT content FROM messages WHERE content='newer recovered evidence'").fetchone()

def test_hook_defers_fts_and_search_does_not_rebuild(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); assert cli.drain_hooks() == 1
    conn = duckdb.connect(str(data/"convos.db")); assert not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone(); conn.close()
    result=CliRunner().invoke(cli.app,["search","remember alpha","-f","json"]); hits=json.loads(result.stdout)
    assert hits[0]["content"]=="remember alpha" and "complete lexical scan" in result.stderr
    conn=duckdb.connect(str(data/"convos.db")); assert not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone(); conn.close()

def test_search_scans_stale_archive_while_external_reader_is_open(hooks):
    sessions,data=hooks; path=sessions/"s.jsonl"; transcript(path); enqueue(path); assert cli.drain_hooks()==1; runner=CliRunner()
    hold=POPEN([sys.executable,"-c","import duckdb,sys; c=duckdb.connect(sys.argv[1],read_only=True); print('ready',flush=True); sys.stdin.read()",str(data/"convos.db")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    try: assert hold.stdout.readline().strip()=="ready"; result=runner.invoke(cli.app,["search","remember alpha","-f","json"])
    finally: hold.stdin.close(); hold.wait(timeout=5)
    assert result.exit_code==0 and json.loads(result.stdout)[0]["content"]=="remember alpha" and "complete lexical scan" in result.stderr

def test_core_connections_share_process_lock(hooks):
    _,data=hooks; data.mkdir(); db=duckdb.connect(str(data/"convos.db")); cli.init_schema(db); db.close(); env={**os.environ,"CONVOS_PROJECT_ROOT":str(data.parent)}; hold=POPEN([sys.executable,"-c","from ai_convos.cli import get_db; c=get_db(); print('ready',flush=True); input(); c.close()"],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True); done=threading.Event()
    try:
        assert hold.stdout.readline().strip()=="ready"; started=time.monotonic()
        with pytest.raises(cli.LockBusy,match="0.05s"): cli.get_db(True,wait=.05)
        assert time.monotonic()-started<.5; waiter=threading.Thread(target=lambda:(cli.get_db(True).close(),done.set())); waiter.start(); assert not done.wait(.1); hold.stdin.write("\n"); hold.stdin.flush(); assert done.wait(5); waiter.join()
    finally: hold.stdin.close(); hold.wait(timeout=5)

def test_sync_defers_fts_and_embeddings(hooks, tmp_path, monkeypatch):
    _, data = hooks; src = tmp_path/"import.json"; src.write_text("[]"); monkeypatch.setenv("CONVOS_IMPORT_PATHS", str(src)); monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json")
    result = cli.ParseResult(convs=[dict(id="sync-c", source="chatgpt", title="T", created_at=None, updated_at=None, model=None, cwd=None, git_branch=None, project_id=None, metadata="{}")], msgs=[dict(id="sync-m", conversation_id="sync-c", role="user", content="alpha", thinking=None, created_at=None, model=None, metadata="{}", parent_id=None)])
    monkeypatch.setattr(cli, "parse_source", lambda _: result); monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: []); monkeypatch.setattr(cli, "get_cookies", lambda *_: {})
    fail = lambda *_: (_ for _ in ()).throw(AssertionError("foreground consolidation")); monkeypatch.setattr(cli, "rebuild_fts_index", fail); monkeypatch.setattr(cli, "embed_pending", fail); old = signal.getsignal(signal.SIGINT)
    try: cli.sync(False, 300, False, False, False, False); assert signal.getsignal(signal.SIGINT) == old
    finally: signal.signal(signal.SIGINT, old)
    conn=duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT fts_generation IS NULL OR messages_generation<>fts_generation FROM retrieval_state").fetchone()[0] and conn.execute("SELECT embedding IS NULL FROM messages WHERE id='sync-m'").fetchone()[0]; conn.close()

def test_sync_targets_provenance_but_full_reconciles_all(hooks, monkeypatch):
    _,data=hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); calls=[]; monkeypatch.setattr(cli,"capture_provenance",lambda *a,**k:calls.append((a,k)) or [])
    cli.sync(False,300,False,False,False,False,True); cli.sync(False,300,False,False,True,False,True)
    assert calls==[((),{"edit_ids":set(),"conversation_ids":set(),"strict":False}),((),{"strict":False})]

def test_full_sync_refreshes_the_capture_lease_with_committed_progress(hooks):
    def work(progress):
        progress('committed batch 7')
        for path in (cli.DATA_DIR/'.sync.lock',cli.HOOK_DIR/'.drain.lock'):
            assert json.loads(path.read_text())['stage']=='committed batch 7'
    cli._sync_leader(work,True)

def test_local_only_sync_imports_configured_agent_roots_without_web(hooks, tmp_path, monkeypatch):
    sessions, data = hooks; transcript(sessions/"local.jsonl", "offline codex history"); (sessions/"gone.jsonl").symlink_to(tmp_path/"missing-codex.jsonl"); claude=tmp_path/"claude"; project=claude/"projects"/"-repo"; project.mkdir(parents=True); (project/"local.jsonl").write_text("\n".join([json.dumps({"type":"system","timestamp":"2026-01-01T00:00:00Z","cwd":"/repo"}),json.dumps({"type":"human","timestamp":"2026-01-01T00:00:01Z","message":{"content":"offline claude history"}})])); (project/"gone.jsonl").symlink_to(tmp_path/"missing-claude.jsonl"); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude)); monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json"); blocked = lambda *_a,**_k: (_ for _ in ()).throw(AssertionError("local-only sync touched web"))
    monkeypatch.setattr(cli, "chatgpt_profiles", blocked); monkeypatch.setattr(cli, "get_cookies", blocked); first = CliRunner().invoke(cli.app, ["sync","--local-only"]); second = CliRunner().invoke(cli.app, ["sync","--local-only"])
    db=duckdb.connect(str(data/"convos.db"),read_only=True); rows=db.execute("SELECT source,content FROM conversations c JOIN messages m ON m.conversation_id=c.id").fetchall(); db.close(); state=json.loads(cli.STATE_PATH.read_text()); state["local"]["codex"]["parser"]=cli.PARSER_EPOCH-1; state["local"]["codex"].pop("epochs"); cli.atomic_json(cli.STATE_PATH,state); real,calls=cli.parse_codex,[]; monkeypatch.setattr(cli,"parse_codex",lambda *a,**k:calls.append(a) or real(*a,**k)); third=CliRunner().invoke(cli.app,["sync","--local-only"]); fourth=CliRunner().invoke(cli.app,["sync","--local-only"])
    assert first.exit_code == second.exit_code == third.exit_code == fourth.exit_code == 0 and set(rows) == {("codex","offline codex history"),("claude-code","offline claude history")} and "2 new, 0 updated" in first.output and "0 new, 0 updated" in second.output and len(calls)==1 and json.loads(cli.STATE_PATH.read_text())["local"]["codex"]["parser"]==cli.PARSER_EPOCH

@pytest.mark.parametrize("stamp", [None, 100])
def test_sync_rechecks_chatgpt_unchanged_head(hooks, monkeypatch, stamp):
    _, data = hooks; monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json"); cli.atomic_json(cli.STATE_PATH, {"web":{"chatgpt":{"browser":"safari","head":f"default:c1:{stamp}"}}}); called = []
    monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: [None]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a: ({}, "https://chatgpt.com")); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli, "fetch_json", lambda *a, **k: {"items":[{"id":"c1","update_time":stamp}]}); monkeypatch.setattr(cli, "fetch_chatgpt", lambda *a, **k: called.append(k) or cli.ParseResult()); monkeypatch.setattr(cli, "get_cookies", lambda *_: {})
    cli.sync(False, 300, False, False, False, False)
    assert called and called[0]["profiles"] == [None]

def test_sync_repairs_legacy_chatgpt_timestamps_before_comparison(hooks, monkeypatch):
    _, data = hooks; data.mkdir(); monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json"); cid = cli.gen_id("chatgpt","legacy"); when = cli.ts_from_epoch(200)
    conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); cli.upsert(conn, cli.ParseResult([dict(id=cid,source="chatgpt",title="T",created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata="{}")],[dict(id="legacy-m",conversation_id=cid,role="user",content="old",thinking=None,created_at=when,model=None,metadata="{}",parent_id=None)])); conn.close()
    cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":{"browser":"safari","head":"default:old:100"}}}); captured = []
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"new","update_time":300}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:captured.append(k) or cli.ParseResult()); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); conn = duckdb.connect(str(data/"convos.db"),read_only=True); times = conn.execute("SELECT created_at,updated_at FROM conversations WHERE id=?",[cid]).fetchone(); conn.close()
    assert times == (when,when) and captured[0]["known"][cid] == when.timestamp() and cid in captured[0]["legacy"]

def test_sync_refetches_tool_ended_chatgpt_at_same_timestamp(hooks,monkeypatch):
    _,data=hooks; data.mkdir(); monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); cid=cli.gen_id("chatgpt","active"); now=time.time(); when=cli.ts_any(now); conv=dict(id=cid,source="chatgpt",title="T",created_at=when,updated_at=when,model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"remote_update_time":now})); msg=dict(id="tool",conversation_id=cid,role="tool",content="running",thinking=None,created_at=when,model=None,metadata="{}",parent_id=None); db=duckdb.connect(str(data/"convos.db")); cli.init_schema(db); cli.upsert(db,cli.ParseResult([conv],[msg])); db.close(); captured=[]
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"active","update_time":100}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:captured.append(k) or cli.ParseResult()); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); assert captured[0]["known"][cid] is None

def test_sync_refetches_tied_chatgpt_without_provider_order(hooks,monkeypatch):
    _,data=hooks; data.mkdir(); monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); cid=cli.gen_id("chatgpt","tied"); when=cli.ts_any(100); conv=dict(id=cid,source="chatgpt",title="T",created_at=when,updated_at=when,model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"remote_update_time":100,"remote_complete":True})); msgs=[dict(id=f"m{i}",conversation_id=cid,role="assistant",content=str(i),thinking=None,created_at=when,model=None,metadata="{}",parent_id=None) for i in range(2)]; db=duckdb.connect(str(data/"convos.db")); cli.init_schema(db); cli.upsert(db,cli.ParseResult([conv],msgs)); db.close(); cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":{"browser":"safari","frontiers":{"default":{"account":"acct","updated":100}},"coverage":[cid]}}}); captured=[]
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"tied","update_time":100}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:(captured.append({**k,"known":dict(k["known"])}),k["sink"](cli.ParseResult([conv],[])),cli.ParseResult())[-1]); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); cli.sync(False,300,False,False,False,False); assert captured[0]["known"][cid] is None and captured[0]["frontiers"] is None and captured[1]["known"][cid]==100 and captured[1]["frontiers"] is not None

def test_read_uses_provider_order_for_tied_timestamps(hooks):
    _,data=hooks; data.mkdir(); db=duckdb.connect(str(data/"convos.db")); cli.init_schema(db); db.execute("INSERT INTO conversations(id,source,title,metadata) VALUES ('ordered','chatgpt','T','{}')"); db.executemany("INSERT INTO messages(id,conversation_id,role,content,created_at,metadata) VALUES (?,?,?,?,?,?)",[("z-start","ordered","user","start","2026-01-01",'{"provider_index":0}'),("z-middle","ordered","tool","middle","2026-01-01",'{"provider_index":1}'),("a-final","ordered","assistant","verdict","2026-01-01",'{"provider_index":2}')]); db.close()
    rows=json.loads(CliRunner().invoke(cli.app,["read","ordered","-n","2","-f","json"]).output); assert [r["id"] for r in rows]==["z-middle","a-final"]

def test_sync_disables_frontier_when_saved_ids_are_missing(hooks, monkeypatch):
    _, data = hooks; data.mkdir(); monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); cid = cli.gen_id("chatgpt","present"); missing = cli.gen_id("chatgpt","missing"); conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); cli.upsert(conn,cli.ParseResult([dict(id=cid,source="chatgpt",title="T",created_at=None,updated_at=cli.ts_any(100),model=None,cwd=None,git_branch=None,project_id=None,metadata="{}")],[])); conn.close(); cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":{"browser":"safari","head":"default:old:100","frontiers":{"default":{"account":"acct","updated":100}},"coverage":[missing]}}}); captured = []
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"new","update_time":200}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:captured.append(k) or cli.ParseResult()); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); assert captured[0]["frontiers"] is None

def test_sync_deduplicates_profiles_for_same_chatgpt_account(hooks, monkeypatch):
    _, data = hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); captured = []
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:["A","B"]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"c1","update_time":100}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:captured.append(k) or cli.ParseResult()); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); assert captured[0]["profiles"]==["A"]

def test_sync_checkpoints_chatgpt_pages_and_retries_only_unfinished(hooks, monkeypatch):
    _, data = hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); cid0 = cli.gen_id("chatgpt","ok0"); mid0 = cli.gen_id("chatgpt",f"{cid0}:m"); old = {"browser":"safari","head":"default:old:100","frontiers":{"default":{"account":"acct","updated":100}},"coverage":[cid0]}; cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":old}}); fail, details = {"bad":True}, []
    conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); cli.upsert(conn,cli.ParseResult([dict(id=cid0,source="chatgpt",title="T",created_at=cli.ts_any(100),updated_at=cli.ts_any(100),model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"remote_update_time":100}))],[dict(id=mid0,conversation_id=cid0,role="user",content="ok0",thinking=None,created_at=cli.ts_any(100),model=None,metadata="{}",parent_id=None)])); conn.close()
    items = [{"id":f"ok{i}","create_time":300-i,"update_time":300-i} for i in range(20)]+[{"id":"bad","create_time":200,"update_time":200}]
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"get_cookies",lambda *_:{}); monkeypatch.setattr(cli.time,"sleep",lambda _:None)
    def fetch(url,*a,**k):
        if "limit=1&order=updated" in url: return {"items":[items[0]]}
        if "/conversations?" in url:
            offset = int(url.split("offset=")[1].split("&")[0])
            return {"items":items if offset==0 else [],"total":len(items)}
        name = url.rsplit("/",1)[-1]; details.append(name)
        if name=="bad" and fail["bad"]: raise TimeoutError("detail timeout")
        when = next(x["update_time"] for x in items if x["id"]==name)
        return {"mapping":{"m":{"parent":None,"message":{"author":{"role":"user"},"content":{"parts":[name]},"create_time":when}}}}
    monkeypatch.setattr(cli,"fetch_json",fetch)
    with pytest.raises(cli.click.ClickException,match="Sync incomplete"): cli.sync(False,300,False,False,False,False)
    conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert {r[0] for r in conn.execute("SELECT content FROM messages").fetchall()}=={f"ok{i}" for i in range(20)} and conn.execute("SELECT fts_generation IS NULL OR messages_generation<>fts_generation FROM retrieval_state").fetchone()[0]; conn.close(); assert json.loads(cli.STATE_PATH.read_text())["web"]["chatgpt"]==old
    fail["bad"] = False; cli.sync(False,300,False,False,False,False); conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert {r[0] for r in conn.execute("SELECT content FROM messages").fetchall()}=={*(f"ok{i}" for i in range(20)),"bad"}; conn.close()
    saved = json.loads(cli.STATE_PATH.read_text())["web"]["chatgpt"]
    assert details==[*(f"ok{i}" for i in range(20)),"bad","bad"] and saved["frontiers"]=={"default":{"account":"acct","updated":300,"id":"ok0"}} and len(saved["coverage"])==21 and cid0 in saved["coverage"]

def test_sync_serializes_streamed_checkpoint_and_completed_source(hooks, tmp_path, monkeypatch):
    _, data = hooks; src = tmp_path/"import.json"; src.write_text("[]"); monkeypatch.setenv("CONVOS_IMPORT_PATHS",str(src)); monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json")
    row=lambda cid,mid,text:cli.ParseResult([dict(id=cid,source="chatgpt",title="T",created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata="{}")],[dict(id=mid,conversation_id=cid,role="user",content=text,thinking=None,created_at=None,model=None,metadata="{}",parent_id=None)])
    local,web=row("import-c","import-m","local"),row("web-c","web-m","web"); started,active,attempted,overlap=(threading.Event() for _ in range(4)); real,flock=cli.upsert,cli.fcntl.flock
    def guarded(conn,result):
        if result.convs and result.convs[0]["id"]=="web-c":
            active.set()
            try: out=real(conn,result); started.set(); assert attempted.wait(2); time.sleep(.05); return out
            finally: active.clear()
        if active.is_set(): overlap.set(); attempted.set()
        return real(conn,result)
    def serialized(fd,op):
        if active.is_set() and threading.current_thread() is threading.main_thread(): attempted.set()
        return flock(fd,op)
    monkeypatch.setattr(cli,"upsert",guarded); monkeypatch.setattr(cli.fcntl,"flock",serialized); monkeypatch.setattr(cli,"parse_source",lambda _:(started.wait(2),attempted.set(),local)[-1]); monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"web","update_time":1}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:(k["sink"](web),cli.ParseResult())[-1]); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); db=duckdb.connect(str(data/"convos.db"),read_only=True); assert not overlap.is_set() and set(db.execute("SELECT content FROM messages").fetchall())=={("local",),("web",)}; db.close()

def test_sync_rolls_back_interrupted_chatgpt_checkpoint(hooks, monkeypatch):
    _, data = hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); old = {"browser":"safari","head":"default:old:100","frontiers":{"default":{"account":"acct","updated":100}},"coverage":[]}; cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":old}}); cid = cli.gen_id("chatgpt","c1"); mid = cli.gen_id("chatgpt","m1")
    result = cli.ParseResult([dict(id=cid,source="chatgpt",title="T",created_at=cli.ts_any(300),updated_at=cli.ts_any(300),model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"remote_update_time":300}))],[dict(id=mid,conversation_id=cid,role="user",content="atomic",thinking=None,created_at=cli.ts_any(300),model=None,metadata="{}",parent_id=None)])
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"c1","update_time":300}]}); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    def fetched(*a,**k): k["sink"](result); return cli.ParseResult()
    monkeypatch.setattr(cli,"fetch_chatgpt",fetched); real = cli.upsert
    def interrupted(conn,r): real(conn,r); raise RuntimeError("mid-upsert")
    monkeypatch.setattr(cli,"upsert",interrupted)
    with pytest.raises(cli.click.ClickException,match="Sync incomplete"): cli.sync(False,300,False,False,False,False)
    conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==0 and conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]==0; conn.close(); assert json.loads(cli.STATE_PATH.read_text())["web"]["chatgpt"]==old
    monkeypatch.setattr(cli,"upsert",real); cli.sync(False,300,False,False,False,False); conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert conn.execute("SELECT content FROM messages").fetchall()==[("atomic",)]; conn.close()

def test_sync_sigint_exits_during_blocked_source(tmp_path):
    src, blocked, ready, done = tmp_path/"import.json", tmp_path/"blocked.json", tmp_path/"ready", tmp_path/"done"; src.write_text("[]"); blocked.write_text("[]")
    code = '''import hashlib,os,sys
from pathlib import Path
from ai_convos import cli
class C:
 def close(self): pass
 def execute(self,*_): return self
 def fetchone(self): return [0]
 def fetchall(self): return []
cli.get_db=lambda *a,**k:C(); cli.init_schema=lambda _:None; cli.drain_hooks=lambda *a,**k:cli.HOOK_DIR.mkdir(parents=True,exist_ok=True) or 0; cli.counts_by_source=lambda _:{}
cli.chatgpt_profiles=lambda _:[]; cli.get_cookies=lambda *_:{}
def parsed(path):
 if path.name=="blocked.json" and os.environ.get("BLOCK")!="0": Path(os.environ["READY"]).touch(); hashlib.pbkdf2_hmac("sha256",b"x",b"y",500_000_000)
 if path.name!="blocked.json": Path(os.environ["DONE"]).touch()
 return cli.ParseResult()
cli.parse_source=parsed; sys.argv[1:]=["sync"]; cli.sync(False,300,False,False,False,False)'''
    root = tmp_path/"archive"; (root/"data").mkdir(parents=True); (root/"data/sync_state.json").write_text('{"sentinel":1}'); env = {**os.environ, "CONVOS_PROJECT_ROOT":str(root), "CONVOS_IMPORT_PATHS":f"{src},{blocked}", "READY":str(ready), "DONE":str(done)}; p = subprocess.Popen([sys.executable, "-c", code], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 5
        while not (ready.exists() and done.exists()) and p.poll() is None and time.monotonic() < deadline: time.sleep(.02)
        assert ready.exists() and done.exists(), f"sync sources did not start (exit={p.poll()})"; time.sleep(.1); p.send_signal(signal.SIGINT)
        try: p.wait(timeout=2)
        except subprocess.TimeoutExpired: p.kill(); p.wait(); pytest.fail("sync ignored Ctrl-C for more than 2 seconds")
        state=json.loads((root/"data/sync_state.json").read_text())
        assert p.returncode == -signal.SIGINT and state["sentinel"]==1 and set(state["imports"])=={str(src)}
        assert subprocess.run([sys.executable, "-c", code], env={**env, "BLOCK":"0"}, capture_output=True).returncode == 0
        state = json.loads((root/"data/sync_state.json").read_text()); assert len(state["imports"]) == 2
    finally:
        if p.poll() is None: p.kill(); p.wait()

def test_explicit_fts_rebuild_makes_generation_current_and_clears_legacy_hint(hooks):
    _,data=hooks; data.mkdir(); conn=duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test')"); conn.execute("INSERT INTO messages(id,conversation_id,role,content) VALUES ('m','c','user','needle')"); cli._archive_touch(conn,[("messages","m")]); conn.close(); (data/"hook_fts_dirty").touch()
    result=CliRunner().invoke(cli.app,["fts"]); assert result.exit_code==0 and not (data/"hook_fts_dirty").exists()
    conn=duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT messages_generation=fts_generation FROM retrieval_state").fetchone()[0]; conn.close()

def test_init_sets_up_core_and_installed_products(hooks, monkeypatch):
    called = []; monkeypatch.setattr(cli, "sync", lambda *args: called.append(("sync",args))); monkeypatch.setattr(cli, "install_skills", lambda: called.append("skills")); monkeypatch.setattr(cli, "install_hooks", lambda remove,status: called.append(("hooks",remove,status))); monkeypatch.setattr(cli, "entry_points", lambda group: [type("EP",(),{"load":lambda self:lambda: called.append("product") or "Product ready"})()] if group == "convos.init" else [])
    first, second = (CliRunner().invoke(cli.app, ["init"]) for _ in range(2))
    assert first.exit_code == second.exit_code == 0 and "Product ready" in first.output and called == [("sync",(False,300,True,True,False,False,True)),"skills",("hooks",False,False),"product"]*2

@pytest.mark.parametrize("unsafe_name,unsafe_kind", [(name,kind) for name in ("codex","claude") for kind in ("file","directory","non-directory")])
def test_managed_files_are_atomic_private_and_reject_skill_symlinks(tmp_path, monkeypatch, unsafe_name, unsafe_kind):
    managed, fresh = tmp_path/"managed.json", tmp_path/"fresh.json"; managed.write_text("{}"); os.chmod(managed, 0o640); cli.atomic_json(managed, {"ready":True}); cli.atomic_json(fresh, {})
    assert json.loads(managed.read_text()) == {"ready":True} and os.stat(managed).st_mode&0o777 == 0o640 and os.stat(fresh).st_mode&0o777 == 0o600
    codex, claude, outside = tmp_path/"codex", tmp_path/"claude", tmp_path/"outside"; targets = {"codex":codex/"skills"/"convos"/"SKILL.md","claude":claude/"skills"/"convos"/"SKILL.md"}; unsafe, safe = targets[unsafe_name], targets[{"codex":"claude","claude":"codex"}[unsafe_name]]; safe.parent.mkdir(parents=True); safe.write_text("unchanged")
    if unsafe_kind == "file": unsafe.parent.mkdir(parents=True); outside.write_text("sentinel"); unsafe.symlink_to(outside)
    else: unsafe.parent.parent.mkdir(parents=True); outside.mkdir(); unsafe.parent.symlink_to(outside) if unsafe_kind == "directory" else unsafe.parent.write_text("blocker")
    monkeypatch.setenv("CODEX_HOME", str(codex)); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude)); result = CliRunner().invoke(cli.app, ["install-skills"])
    assert result.exit_code == 1 and "Refusing unsafe managed file" in result.output and "Traceback" not in result.output and safe.read_text() == "unchanged" and (outside.read_text() == "sentinel" if outside.is_file() else not (outside/"SKILL.md").exists())

def test_install_skills_allows_one_shared_declared_destination(tmp_path, monkeypatch):
    codex, claude = tmp_path/"codex", tmp_path/"claude"; (codex/"skills").mkdir(parents=True); claude.mkdir(); (claude/"skills").symlink_to(codex/"skills", target_is_directory=True); monkeypatch.setenv("CODEX_HOME", str(codex)); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude))
    result = CliRunner().invoke(cli.app, ["install-skills"]); a, b = codex/"skills"/"convos"/"SKILL.md", claude/"skills"/"convos"/"SKILL.md"
    assert result.exit_code == 0 and result.output.count("Installed ") == 2 and a.samefile(b) and a.read_text() == b.read_text()

def test_install_skills_replaces_only_exact_legacy_skill(tmp_path, monkeypatch):
    codex,claude=tmp_path/"codex",tmp_path/"claude"; monkeypatch.setenv("CODEX_HOME",str(codex)); monkeypatch.setenv("CLAUDE_CONFIG_DIR",str(claude)); source=(Path(__file__).resolve().parents[1]/"skills/convos/SKILL.md").read_text(); legacy=source.replace("name: convos","name: agent-convos",1).replace("# Convos","# Agent Convos",1); exact,modified=codex/"skills/agent-convos/SKILL.md",claude/"skills/agent-convos/SKILL.md"
    exact.parent.mkdir(parents=True); exact.write_text(legacy); modified.parent.mkdir(parents=True); modified.write_text(legacy+"\ncustom\n"); result=CliRunner().invoke(cli.app,["install-skills"])
    assert result.exit_code==0 and not exact.exists() and modified.read_text().endswith("custom\n") and (codex/"skills/convos/SKILL.md").read_text()==source and (claude/"skills/convos/SKILL.md").read_text()==source

def test_doctor_reports_archive_ingest_and_hook_health(hooks, monkeypatch):
    _, data = hooks; claude = data/"claude"; claude.mkdir(parents=True); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude)); monkeypatch.setattr(cli, "safari_cookie_domains", lambda: []); monkeypatch.setattr(cli, "chrome_cookie_domains", lambda: [])
    conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); conn.execute("INSERT INTO conversations VALUES ('c','codex','T',NULL,'2026-01-01',NULL,NULL,NULL,NULL,NULL)"); conn.execute("INSERT INTO messages VALUES ('m','c','user','hello',NULL,NULL,NULL,NULL,NULL,NULL),('noise','c','user','# AGENTS.md instructions for /repo',NULL,NULL,NULL,NULL,NULL,NULL)"); cli.rebuild_fts_index(conn); conn.close()
    cli.atomic_json(data/"hook_state.json", {"x":[1767225600000000000,1]}); cli.atomic_json(data/"hook_embeddings_dirty", ["m"]); cli.atomic_json(data/"hook_inbox/q.json", {"source":"codex"})
    r = CliRunner().invoke(cli.app, ["doctor"]); assert r.exit_code == 0
    assert "convos:" in r.output and "archive: 1 convs, 2 msgs, 1 unembedded" in r.output and "schema=ready, fts=current" in r.output and "repair: convos embed" in r.output
    assert "ingest: pending=1, last=2026-01-01" in r.output and "skills: 0/2 current" in r.output and "repair: convos install-skills" in r.output and "codex: 0 hooks" in r.output and "repair: convos install-hooks" in r.output

def test_doctor_detects_current_stale_and_symlinked_skills(hooks, tmp_path, monkeypatch):
    _, data = hooks; codex,claude=Path(os.environ["CODEX_HOME"]),tmp_path/"claude"; claude.mkdir(); monkeypatch.setenv("CLAUDE_CONFIG_DIR",str(claude)); monkeypatch.setattr(cli,"entry_points",lambda **_:[]); monkeypatch.setattr(cli,"safari_cookie_domains",lambda:[]); monkeypatch.setattr(cli,"chrome_cookie_domains",lambda:[]); runner=CliRunner()
    missing=runner.invoke(cli.app,["doctor"]).output; assert "skills: 0/2 current" in missing and "repair: convos install-skills" in missing
    assert runner.invoke(cli.app,["install-skills"]).exit_code==0; current=runner.invoke(cli.app,["doctor"]).output; assert "skills: 2/2 current" in current and "repair: convos install-skills" not in current
    target=claude/"skills/convos/SKILL.md"; target.write_text("stale"); assert "skills: 1/2 current" in runner.invoke(cli.app,["doctor"]).output
    target.unlink(); target.symlink_to(codex/"skills/convos/SKILL.md"); linked=runner.invoke(cli.app,["doctor"]).output; assert "skills: 1/2 current" in linked and "repair: convos install-skills" in linked

def test_doctor_surfaces_schema_skew(hooks, monkeypatch):
    _, data = hooks; data.mkdir(); monkeypatch.setattr(cli, "safari_cookie_domains", lambda: []); monkeypatch.setattr(cli, "chrome_cookie_domains", lambda: [])
    conn = duckdb.connect(str(data/"convos.db")); conn.execute("CREATE TABLE messages (id VARCHAR, content VARCHAR)"); conn.close()
    r = CliRunner().invoke(cli.app, ["doctor"]); assert r.exit_code == 0
    assert "schema=missing:" in r.output and "messages.embedding" in r.output and "fts=missing" in r.output and "repair: convos init" in r.output

def test_hook_rejects_paths_outside_provider_root(hooks):
    _, data = hooks; data.mkdir(); path = data/"outside.jsonl"; transcript(path)
    with pytest.raises(ValueError, match="Invalid codex transcript path"): cli.enqueue_hook("codex", {"transcript_path":str(path)})

def test_install_status_reinstall_and_remove_hooks(tmp_path, monkeypatch):
    claude, codex, archive = tmp_path/"claude", tmp_path/"codex", tmp_path/"archive root"; claude.mkdir(); codex.mkdir(); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude)); monkeypatch.setenv("CODEX_HOME", str(codex)); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(archive))
    (claude/"settings.json").write_text(json.dumps({"x":1,"hooks":{"Stop":[{"hooks":[{"type":"command","command":"keep me"},{"type":"command","command":"other hook claude-code"},{"type":"command","command":"/old/convos hook claude-code","statusMessage":"Updating conversation archive"},{"type":"command","command":"touch /tmp/wake # ai-convos remote hook"}]}]}})); runner = CliRunner()
    first = runner.invoke(cli.app, ["install-hooks"]); second = runner.invoke(cli.app, ["install-hooks"]); assert first.exit_code == second.exit_code == 0 and "`/hooks`" in first.output
    c, x = json.loads((claude/"settings.json").read_text()), json.loads((codex/"hooks.json").read_text())
    handler=x["hooks"]["Stop"][0]["hooks"][0]; assert c["x"] == 1 and sum(len(g["hooks"]) for g in c["hooks"]["Stop"]) == 3 and len(c["hooks"]["SessionEnd"]) == 1 and len(x["hooks"]["Stop"]) == 1 and handler["command"]==cli._capture_command("codex") and handler["timeout"]==5 and handler["statusMessage"]=="Saving conversation to Convos"
    status = runner.invoke(cli.app, ["install-hooks", "--status"]).output; assert "claude-code: 2 hooks" in status and "codex: 1 hook" in status and "repair:" not in status and "`/hooks`" not in status
    x["hooks"]["Stop"][0]["hooks"].append({"type":"command","command":"/old/convos capture codex","statusMessage":"Saving conversation to Convos"}); (codex/"hooks.json").write_text(json.dumps(x)); duplicate=runner.invoke(cli.app,["install-hooks","--status"]).output; assert "codex: 0 hooks" in duplicate and "repair: convos install-hooks" in duplicate
    assert runner.invoke(cli.app,["install-hooks"]).exit_code==0; x=json.loads((codex/"hooks.json").read_text())
    c["hooks"]["Stop"]+=c["hooks"].pop("SessionEnd"); (claude/"settings.json").write_text(json.dumps(c)); misplaced=runner.invoke(cli.app,["install-hooks","--status"]).output; assert "claude-code: 0 hooks" in misplaced and "repair: convos install-hooks" in misplaced
    assert runner.invoke(cli.app,["install-hooks"]).exit_code==0
    x["hooks"]["Stop"][0]["hooks"][0]["command"]="/old/convos capture codex"; (codex/"hooks.json").write_text(json.dumps(x)); stale=runner.invoke(cli.app,["install-hooks","--status"]).output; assert "codex: 0 hooks" in stale and "repair: convos install-hooks" in stale
    assert runner.invoke(cli.app,["install-hooks"]).exit_code==0 and json.loads((codex/"hooks.json").read_text())["hooks"]["Stop"][0]["hooks"][0]["command"]==cli._capture_command("codex")
    assert runner.invoke(cli.app, ["install-hooks", "--remove"]).exit_code == 0
    c, x = json.loads((claude/"settings.json").read_text()), json.loads((codex/"hooks.json").read_text())
    assert c["hooks"] == {"Stop":[{"hooks":[{"type":"command","command":"keep me"},{"type":"command","command":"other hook claude-code"}]}]} and x["hooks"] == {}

@pytest.mark.parametrize("unsafe_name,unsafe_kind", [(name,kind) for name in ("claude","codex") for kind in ("symlink","malformed","shape","parent")])
@pytest.mark.parametrize("remove", [False,True])
def test_install_hooks_preflights_every_config_before_writing(tmp_path, monkeypatch, unsafe_name, unsafe_kind, remove):
    homes = {"claude":tmp_path/"claude","codex":tmp_path/"codex"}; paths = {"claude":homes["claude"]/"settings.json","codex":homes["codex"]/"hooks.json"}; unsafe, safe = paths[unsafe_name], paths[{"claude":"codex","codex":"claude"}[unsafe_name]]; safe.parent.mkdir(); safe.write_text('{"keep":1}'); outside = tmp_path/"outside"
    if unsafe_kind == "parent": unsafe.parent.write_text("blocker")
    else: unsafe.parent.mkdir(); outside.write_text("sentinel"); unsafe.symlink_to(outside) if unsafe_kind == "symlink" else unsafe.write_text("{" if unsafe_kind == "malformed" else '{"hooks":[]}')
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(homes["claude"])); monkeypatch.setenv("CODEX_HOME", str(homes["codex"])); result = CliRunner().invoke(cli.app, ["install-hooks",*(["--remove"] if remove else [])])
    assert result.exit_code == 1 and safe.read_text() == '{"keep":1}' and "installed" not in result.output.lower() and (outside.read_text() == "sentinel" if outside.exists() else True)


def test_dispatch_releases_worker_lease_before_child_can_start(hooks,monkeypatch):
    _,data=hooks
    launched=[]
    def start(args,**kwargs):
        with cli.operation_lock(data/'hook_inbox/.drain.lock','test.child',0): launched.append(kwargs['env']['CONVOS_PROJECT_ROOT'])
    monkeypatch.setattr(cli.subprocess,'Popen',start)
    cli.wake_hooks(root=data.parent)
    assert launched==[str(data.parent)]


def test_unchanged_source_reparses_only_after_its_provider_binding_changes(hooks,monkeypatch):
    sessions,data=hooks
    for name in ('affected','unrelated'):
        path=sessions/(name+'.jsonl'); transcript(path)
        events=list(map(json.loads,path.read_text().splitlines())); events[0]['payload']['id']=name
        path.write_text('\n'.join(map(json.dumps,events)))
    monkeypatch.setattr(cli,'STATE_PATH',data/'sync_state.json')
    cli.sync(False,300,False,True,False,False,True)
    with cli._core(purpose='test.source.alias') as db,cli._transaction(db):
        cid=cli.gen_id('codex','affected')
        session=json.loads(db.execute('SELECT metadata FROM conversations WHERE id=?',[cid]).fetchone()[0])['session_id']
        db.execute("INSERT INTO conversations SELECT 'new-canonical',* EXCLUDE(id) FROM conversations WHERE id=?",[cid])
        cli.project_provider_bindings(db,'codex',session,'new-canonical',[cid])
    real,parsed=cli.parse_codex,[]
    monkeypatch.setattr(cli,'parse_codex',lambda path,files,bindings:parsed.extend(map(str,files)) or real(path,files,bindings))
    cli.sync(False,300,False,True,False,False,True)
    assert parsed==[str(sessions/'affected.jsonl')]
    parsed.clear()
    cli.sync(False,300,False,True,False,False,True)
    assert parsed==[]
