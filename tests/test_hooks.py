import json, os, signal, subprocess, sys, threading, time
from pathlib import Path
import duckdb, pytest
from typer.testing import CliRunner
from ai_convos import cli
POPEN=subprocess.Popen

@pytest.fixture
def hooks(tmp_path, monkeypatch):
    data, codex = tmp_path/"data", tmp_path/".codex"; sessions = codex/"sessions"; sessions.mkdir(parents=True)
    for k, v in (("DATA_DIR", data), ("DB_PATH", data/"convos.db"), ("HOOK_DIR", data/"hook_inbox"), ("HOOK_STATE", data/"hook_state.json"), ("HOOK_EMBED_DIRTY", data/"hook_embeddings_dirty"), ("HOOK_FTS_DIRTY", data/"hook_fts_dirty")): monkeypatch.setattr(cli, k, v)
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
    calls=[]; monkeypatch.setattr(cli,"drain_hooks",lambda **kwargs:calls.append(kwargs)); runner=CliRunner(); assert runner.invoke(cli.app,["drain-hooks"]).exit_code==runner.invoke(cli.app,["drain-hooks","--block"]).exit_code==0 and calls==[{"block":False},{"block":True}]

def test_incidental_drain_and_manual_sync_do_not_wait_for_worker(hooks, monkeypatch):
    _,data=hooks; (data/"hook_inbox").mkdir(parents=True); hold=POPEN([sys.executable,"-c","import fcntl,sys; f=open(sys.argv[1],'w'); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); input()",str(data/"hook_inbox/.drain.lock")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True); monkeypatch.setattr(cli,"capture_provenance",lambda *a,**k:[])
    try:
        assert hold.stdout.readline().strip()=="ready"; started=time.monotonic(); assert cli.drain_hooks()==0; cli.sync(False,300,False,False,False,False,True); assert time.monotonic()-started<1
        done=threading.Event(); waiter=threading.Thread(target=lambda:(cli.drain_hooks(block=True),done.set())); waiter.start(); assert not done.wait(.1); hold.stdin.write("\n"); hold.stdin.flush(); assert done.wait(5); waiter.join()
    finally:
        if hold.poll() is None: hold.stdin.write("\n"); hold.stdin.flush()
        hold.wait(timeout=5)

def test_drain_releases_inbox_lock_before_parsing(hooks, monkeypatch):
    sessions,data=hooks; path=sessions/"s.jsonl"; transcript(path); enqueue(path); parse=cli.hook_result
    def unlocked(*args):
        result=subprocess.run([sys.executable,"-c","import fcntl,sys; f=open(sys.argv[1],'w'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)",str(data/"hook_inbox/.lock")]); assert result.returncode==0; return parse(*args)
    monkeypatch.setattr(cli,"hook_result",unlocked); assert cli.drain_hooks()==1

def test_concurrent_sync_exits_immediately_and_explicitly(hooks,capsys):
    _,data=hooks; data.mkdir(); hold=POPEN([sys.executable,"-c","import fcntl,sys; f=open(sys.argv[1],'w'); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); input()",str(data/".sync.lock")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    try:
        assert hold.stdout.readline().strip()=="ready"; started=time.monotonic()
        with pytest.raises(cli.typer.Exit): cli.sync(False,300,False,False,False,False,True)
        assert time.monotonic()-started<1 and "Sync already running; no work was started" in capsys.readouterr().out
    finally: hold.stdin.write("\n"); hold.stdin.flush(); hold.wait(timeout=5)

def test_retrieval_drains_idempotently_and_preserves_truncated_rewritten_history(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; runner = CliRunner(); transcript(path); enqueue(path)
    assert json.loads(runner.invoke(cli.app, ["search", "remember alpha", "-f", "json"]).output)[0]["source"] == "codex"
    transcript(path, assistant="second answer"); enqueue(path); runner.invoke(cli.app, ["search", "second answer", "-f", "json"])
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2; updated = conn.execute("SELECT updated_at FROM conversations").fetchone()[0]; conn.close()
    transcript(path); enqueue(path); runner.invoke(cli.app, ["search", "remember alpha"])
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 2; assert conn.execute("SELECT updated_at FROM conversations").fetchone()[0] == updated; conn.close()
    transcript(path, user="rewritten alpha"); enqueue(path); runner.invoke(cli.app, ["search", "rewritten alpha"])
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3; assert conn.execute("SELECT COUNT(*) FROM messages WHERE content IN ('remember alpha','rewritten alpha')").fetchone()[0] == 2; meta = json.loads(conn.execute("SELECT metadata FROM messages WHERE content='remember alpha'").fetchone()[0]); assert meta["history_of"] and meta["superseded_at"]; conn.close()
    enqueue(path); runner.invoke(cli.app, ["search", "rewritten alpha"]); conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 3; conn.close()

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

def test_stable_metadata_only_session_completes_until_changed(hooks):
    sessions, data = hooks; path = sessions/"empty.jsonl"; path.write_text(json.dumps({"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}})); enqueue(path)
    assert cli.drain_hooks() == 1 and not list((data/"hook_inbox").glob("*.json"))
    state = json.loads((data/"hook_state.json").read_text()); assert len(state) == 1 and next(iter(state.values())) == [path.stat().st_mtime_ns, path.stat().st_size]

def test_orphaned_claim_forces_reindex_after_committed_upsert(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); q = next((data/"hook_inbox").glob("*.json")); work = q.with_suffix(".work"); q.replace(work)
    conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); cli.upsert(conn, cli.hook_result("codex", path)); conn.close()
    assert cli.drain_hooks() == 1
    assert (data/"hook_fts_dirty").exists(); assert cli.flush_fts()
    conn = duckdb.connect(str(data/"convos.db")); assert conn.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()[0] == 1; conn.close()

def test_hook_defers_fts_until_fresh_search(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); assert cli.drain_hooks() == 1
    conn = duckdb.connect(str(data/"convos.db")); assert not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone(); conn.close(); assert (data/"hook_fts_dirty").exists()
    hits = json.loads(CliRunner().invoke(cli.app, ["search", "remember alpha", "-f", "json"]).output)
    assert hits[0]["content"] == "remember alpha" and not (data/"hook_fts_dirty").exists()

def test_search_uses_last_fts_snapshot_when_refresh_is_locked(hooks):
    sessions, data = hooks; path = sessions/"s.jsonl"; transcript(path); enqueue(path); runner = CliRunner(); assert json.loads(runner.invoke(cli.app,["search","remember alpha","-f","json"]).output)
    hold=POPEN([sys.executable,"-c","import duckdb,sys; c=duckdb.connect(sys.argv[1],read_only=True); print('ready',flush=True); sys.stdin.read()",str(data/"convos.db")],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    try: assert hold.stdout.readline().strip()=="ready"; (data/"hook_fts_dirty").touch(); result=runner.invoke(cli.app,["search","remember alpha","-f","json"])
    finally: hold.stdin.close(); hold.wait(timeout=5)
    assert result.exit_code == 0 and json.loads(result.stdout)[0]["content"] == "remember alpha" and "using last indexed snapshot" in result.stderr and (data/"hook_fts_dirty").exists()

def test_core_connections_share_process_lock(hooks):
    _,data=hooks; data.mkdir(); db=duckdb.connect(str(data/"convos.db")); cli.init_schema(db); db.close(); env={**os.environ,"CONVOS_PROJECT_ROOT":str(data.parent)}; hold=POPEN([sys.executable,"-c","from ai_convos.cli import get_db; c=get_db(); print('ready',flush=True); input(); c.close()"],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True); done=threading.Event()
    try:
        assert hold.stdout.readline().strip()=="ready"; (data/"hook_fts_dirty").touch(); started=time.monotonic()
        with pytest.raises(ValueError,match="0 seconds"): cli.flush_fts()
        assert time.monotonic()-started<.5 and (data/"hook_fts_dirty").exists(); started=time.monotonic()
        with pytest.raises(ValueError,match="0.05 seconds"): cli.get_db(True,wait=.05)
        assert time.monotonic()-started<.5; waiter=threading.Thread(target=lambda:(cli.get_db(True).close(),done.set())); waiter.start(); assert not done.wait(.1); hold.stdin.write("\n"); hold.stdin.flush(); assert done.wait(5); waiter.join()
    finally: hold.stdin.close(); hold.wait(timeout=5)

def test_sync_defers_fts_and_embeddings(hooks, tmp_path, monkeypatch):
    _, data = hooks; src = tmp_path/"import.json"; src.write_text("[]"); monkeypatch.setenv("CONVOS_IMPORT_PATHS", str(src)); monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json")
    result = cli.ParseResult(convs=[dict(id="sync-c", source="chatgpt", title="T", created_at=None, updated_at=None, model=None, cwd=None, git_branch=None, project_id=None, metadata="{}")], msgs=[dict(id="sync-m", conversation_id="sync-c", role="user", content="alpha", thinking=None, created_at=None, model=None, metadata="{}", parent_id=None)])
    monkeypatch.setattr(cli, "parse_source", lambda _: result); monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: []); monkeypatch.setattr(cli, "get_cookies", lambda *_: {})
    fail = lambda *_: (_ for _ in ()).throw(AssertionError("foreground consolidation")); monkeypatch.setattr(cli, "flush_fts", fail); monkeypatch.setattr(cli, "embed_hook_pending", fail); old = signal.getsignal(signal.SIGINT)
    try: cli.sync(False, 300, False, False, False, False); assert signal.getsignal(signal.SIGINT) == old
    finally: signal.signal(signal.SIGINT, old)
    assert (data/"hook_fts_dirty").exists() and json.loads((data/"hook_embeddings_dirty").read_text()) == ["sync-m"]

def test_sync_targets_provenance_but_full_reconciles_all(hooks, monkeypatch):
    _,data=hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); calls=[]; monkeypatch.setattr(cli,"capture_provenance",lambda *a,**k:calls.append((a,k)) or [])
    cli.sync(False,300,False,False,False,False,True); cli.sync(False,300,False,False,True,False,True)
    assert calls==[((),{"edit_ids":set(),"conversation_ids":set()}),((),{})]

def test_local_only_sync_imports_configured_agent_roots_without_web(hooks, tmp_path, monkeypatch):
    sessions, data = hooks; transcript(sessions/"local.jsonl", "offline codex history"); (sessions/"gone.jsonl").symlink_to(tmp_path/"missing-codex.jsonl"); claude=tmp_path/"claude"; project=claude/"projects"/"-repo"; project.mkdir(parents=True); (project/"local.jsonl").write_text("\n".join([json.dumps({"type":"system","timestamp":"2026-01-01T00:00:00Z","cwd":"/repo"}),json.dumps({"type":"human","timestamp":"2026-01-01T00:00:01Z","message":{"content":"offline claude history"}})])); (project/"gone.jsonl").symlink_to(tmp_path/"missing-claude.jsonl"); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude)); monkeypatch.setattr(cli, "STATE_PATH", data/"sync_state.json"); blocked = lambda *_a,**_k: (_ for _ in ()).throw(AssertionError("local-only sync touched web"))
    monkeypatch.setattr(cli, "chatgpt_profiles", blocked); monkeypatch.setattr(cli, "get_cookies", blocked); first = CliRunner().invoke(cli.app, ["sync","--local-only"]); second = CliRunner().invoke(cli.app, ["sync","--local-only"])
    db=duckdb.connect(str(data/"convos.db"),read_only=True); rows=db.execute("SELECT source,content FROM conversations c JOIN messages m ON m.conversation_id=c.id").fetchall(); db.close()
    assert first.exit_code == second.exit_code == 0 and set(rows) == {("codex","offline codex history"),("claude-code","offline claude history")} and "2 new, 0 updated" in first.output and "0 new, 0 updated" in second.output

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
    monkeypatch.setattr(cli,"fetch_json",fetch); cli.sync(False,300,False,False,False,False)
    conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert {r[0] for r in conn.execute("SELECT content FROM messages").fetchall()}=={f"ok{i}" for i in range(20)}; conn.close(); assert json.loads(cli.STATE_PATH.read_text())["web"]["chatgpt"]==old and (data/"hook_fts_dirty").exists() and len(json.loads((data/"hook_embeddings_dirty").read_text()))==20
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
            try: out=real(conn,result); started.set(); assert attempted.wait(2); return out
            finally: active.clear()
        if active.is_set(): overlap.set(); attempted.set()
        return real(conn,result)
    def serialized(fd,op):
        if active.is_set() and threading.current_thread() is threading.main_thread(): attempted.set()
        return flock(fd,op)
    monkeypatch.setattr(cli,"upsert",guarded); monkeypatch.setattr(cli.fcntl,"flock",serialized); monkeypatch.setattr(cli,"parse_source",lambda _:started.wait(2) and local); monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"web","update_time":1}]}); monkeypatch.setattr(cli,"fetch_chatgpt",lambda *a,**k:k["sink"](web) and cli.ParseResult()); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    cli.sync(False,300,False,False,False,False); db=duckdb.connect(str(data/"convos.db"),read_only=True); assert not overlap.is_set() and set(db.execute("SELECT content FROM messages").fetchall())=={("local",),("web",)}; db.close()

def test_sync_rolls_back_interrupted_chatgpt_checkpoint(hooks, monkeypatch):
    _, data = hooks; monkeypatch.setattr(cli,"STATE_PATH",data/"sync_state.json"); old = {"browser":"safari","head":"default:old:100","frontiers":{"default":{"account":"acct","updated":100}},"coverage":[]}; cli.atomic_json(cli.STATE_PATH,{"web":{"chatgpt":old}}); cid = cli.gen_id("chatgpt","c1"); mid = cli.gen_id("chatgpt","m1")
    result = cli.ParseResult([dict(id=cid,source="chatgpt",title="T",created_at=cli.ts_any(300),updated_at=cli.ts_any(300),model=None,cwd=None,git_branch=None,project_id=None,metadata=json.dumps({"remote_update_time":300}))],[dict(id=mid,conversation_id=cid,role="user",content="atomic",thinking=None,created_at=cli.ts_any(300),model=None,metadata="{}",parent_id=None)])
    monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); monkeypatch.setattr(cli,"fetch_json",lambda *a,**k:{"items":[{"id":"c1","update_time":300}]}); monkeypatch.setattr(cli,"get_cookies",lambda *_:{})
    def fetched(*a,**k): k["sink"](result); return cli.ParseResult()
    monkeypatch.setattr(cli,"fetch_chatgpt",fetched); real = cli.upsert
    def interrupted(conn,r): real(conn,r); raise RuntimeError("mid-upsert")
    monkeypatch.setattr(cli,"upsert",interrupted)
    cli.sync(False,300,False,False,False,False)
    conn = duckdb.connect(str(data/"convos.db"),read_only=True); assert conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==0 and conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]==0; conn.close(); assert json.loads(cli.STATE_PATH.read_text())["web"]["chatgpt"]==old and (data/"hook_fts_dirty").exists() and json.loads((data/"hook_embeddings_dirty").read_text())==[mid]
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
 return cli.ParseResult()
def upsert(*_): Path(os.environ["DONE"]).touch(); return 0,0,0,0,0,0,0,{"m"}
cli.parse_source=parsed; cli.upsert=upsert; sys.argv[1:]=["sync"]; cli.sync(False,300,False,False,False,False)'''
    root = tmp_path/"archive"; (root/"data").mkdir(parents=True); (root/"data/sync_state.json").write_text('{"sentinel":1}'); env = {**os.environ, "CONVOS_PROJECT_ROOT":str(root), "CONVOS_IMPORT_PATHS":f"{src},{blocked}", "READY":str(ready), "DONE":str(done)}; p = subprocess.Popen([sys.executable, "-c", code], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 5
        while not (ready.exists() and done.exists()) and p.poll() is None and time.monotonic() < deadline: time.sleep(.02)
        assert ready.exists() and done.exists(), f"sync sources did not start (exit={p.poll()})"; time.sleep(.1); p.send_signal(signal.SIGINT)
        try: p.wait(timeout=2)
        except subprocess.TimeoutExpired: p.kill(); p.wait(); pytest.fail("sync ignored Ctrl-C for more than 2 seconds")
        assert p.returncode == -signal.SIGINT and json.loads((root/"data/sync_state.json").read_text()) == {"sentinel":1}
        assert subprocess.run([sys.executable, "-c", code], env={**env, "BLOCK":"0"}, capture_output=True).returncode == 0
        state = json.loads((root/"data/sync_state.json").read_text()); assert len(state["imports"]) == 2 and (root/"data/hook_fts_dirty").exists() and json.loads((root/"data/hook_embeddings_dirty").read_text()) == ["m"]
    finally:
        if p.poll() is None: p.kill(); p.wait()

def test_fts_claim_preserves_new_work(hooks, monkeypatch):
    _, data = hooks; (data/"hook_inbox").mkdir(parents=True); (data/"hook_fts_dirty").touch(); conn = duckdb.connect(str(data/"convos.db")); cli.init_schema(conn); conn.close()
    monkeypatch.setattr(cli, "rebuild_fts_index", lambda _: (data/"hook_fts_dirty").touch()); assert cli.flush_fts()
    assert (data/"hook_fts_dirty").exists() and not list(data.glob(".hook_fts_dirty.*"))

@pytest.mark.parametrize("error", [RuntimeError("index failed"), KeyboardInterrupt()])
def test_failed_fts_claim_is_restored(hooks, monkeypatch, error):
    _, data = hooks; (data/"hook_inbox").mkdir(parents=True); (data/"hook_fts_dirty").touch()
    monkeypatch.setattr(cli, "rebuild_fts_index", lambda _: (_ for _ in ()).throw(error))
    with pytest.raises(type(error)): cli.flush_fts()
    assert (data/"hook_fts_dirty").exists() and not list(data.glob(".hook_fts_dirty.*"))

def test_orphaned_fts_claim_is_retried(hooks, monkeypatch):
    _, data = hooks; (data/"hook_inbox").mkdir(parents=True); (data/".hook_fts_dirty.dead").touch(); seen = []
    monkeypatch.setattr(cli, "rebuild_fts_index", lambda _: seen.append(True)); assert cli.flush_fts()
    assert seen == [True] and not (data/"hook_fts_dirty").exists() and not list(data.glob(".hook_fts_dirty.*"))

def test_embedding_claim_is_scoped_and_preserves_new_work(hooks, monkeypatch):
    _, data = hooks; (data/"hook_inbox").mkdir(parents=True); cli.atomic_json(data/"hook_embeddings_dirty", ["old"]); seen = {}
    def embed(batch, ids, local_only): seen.update(batch=batch, ids=ids, local_only=local_only); cli.atomic_json(data/"hook_embeddings_dirty", ["new"])
    monkeypatch.setattr(cli, "embed_pending", embed); cli.embed_hook_pending(local_only=True)
    assert seen == {"batch":32, "ids":["old"], "local_only":True} and json.loads((data/"hook_embeddings_dirty").read_text()) == ["new"]

@pytest.mark.parametrize("error", [RuntimeError("model failed"), KeyboardInterrupt()])
def test_failed_embedding_restores_claimed_ids(hooks, monkeypatch, error):
    _, data = hooks; (data/"hook_inbox").mkdir(parents=True); cli.atomic_json(data/"hook_embeddings_dirty", ["old"])
    monkeypatch.setattr(cli, "embed_pending", lambda *_: (_ for _ in ()).throw(error))
    with pytest.raises(type(error)): cli.embed_hook_pending()
    assert json.loads((data/"hook_embeddings_dirty").read_text()) == ["old"]

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
    assert "convos:" in r.output and "archive: 1 convs, 2 msgs, 1 unembedded" in r.output and "schema=ready, fts=yes" in r.output and "repair: convos embed" in r.output
    assert "ingest: pending=1, embedding_ids=1, embedding_claims=0, last=2026-01-01" in r.output and "skills: 0/2 current" in r.output and "repair: convos install-skills" in r.output and "codex: 0 hooks" in r.output and "repair: convos install-hooks" in r.output

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
    assert "schema=missing:" in r.output and "messages.embedding" in r.output and "fts=no" in r.output and "repair: convos init" in r.output

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
