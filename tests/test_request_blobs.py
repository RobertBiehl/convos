"""Claude Code api-request-blob events are request-history copies, not conversation turns."""

import json, os, subprocess

import duckdb
import ai_convos.cli as core
from ai_convos.cli import ParseResult, gen_id, parse_claude_code_session, upsert
from ai_convos_remote.projection import apply_row_replicas, attest_rows, connect, row_replicas, scan
from ai_convos_remote.protocol import open_replica
from tests.test_remote_projection import signed_edit_graph

CALLS=[{"type":"tool_use","id":"toolu_write","name":"Write","input":{"file_path":"REPO/a.py","content":"x=1\n"}},{"type":"tool_use","id":"toolu_ls","name":"Bash","input":{"command":"ls"}}]
RESULTS=[{"type":"tool_result","tool_use_id":"toolu_write","content":"ok"},{"type":"tool_result","tool_use_id":"toolu_ls","content":"a.py"}]

def events(repo,blob_type="api-request-blob",calls=CALLS):
    calls=json.loads(json.dumps(calls).replace("REPO",str(repo)))
    return [{"type":"user","uuid":"u1","sessionId":"s1","cwd":str(repo),"timestamp":"2026-10-16T10:00:00Z","message":{"role":"user","content":"write it"}},
            {"type":"assistant","uuid":"a1","parentUuid":"u1","timestamp":"2026-10-16T10:00:01Z","message":{"role":"assistant","model":"m","content":calls}},
            {"type":"user","uuid":"u2","parentUuid":"a1","timestamp":"2026-10-16T10:00:02Z","message":{"role":"user","content":RESULTS}},
            {"type":blob_type,"sessionId":"s1","hash":"h1","message":{"role":"system","content":"Today's date is 2026-10-16."}},
            {"type":blob_type,"sessionId":"s1","hash":"h2","message":{"role":"assistant","content":calls}},
            {"type":blob_type,"sessionId":"s1","hash":"h3","message":{"role":"user","content":RESULTS}},
            {"type":"api-request","sessionId":"s1","timestamp":"2026-10-16T10:00:03Z","tail":["h1","h2","h3"]},
            {"type":"assistant","uuid":"a2","parentUuid":"u2","timestamp":"2026-10-16T10:00:04Z","message":{"role":"assistant","model":"m","content":[{"type":"text","text":"done"}]}}]

def write(path,rows): path.parent.mkdir(parents=True,exist_ok=True); path.write_text("\n".join(map(json.dumps,rows)))
def ingest(db,path): s=parse_claude_code_session(path,core.session_bindings(db)); return upsert(db,ParseResult(convs=[s["conv"]],msgs=s["msgs"],tools=s["tools"],attachs=s["attachs"],edits=s["edits"],edit_evidence=s["edit_evidence"]))
def repo(tmp_path):
    root=tmp_path/"repo"; root.mkdir(); git=lambda *a:subprocess.run(("git","-C",str(root),*a),check=True,capture_output=True)
    (git("init","-q"),git("config","user.email","a@b.c"),git("config","user.name","A"),(root/"a.py").write_text("x=1\n"),git("add","."),git("commit","-qm","init"))
    return root

def test_request_blobs_are_not_turns_and_reingest_is_stable(tmp_path):
    path=tmp_path/"s1.jsonl"; write(path,events(tmp_path))
    s=parse_claude_code_session(path)
    assert [m["role"] for m in s["msgs"]]==["user","assistant","user","assistant"]
    assert sorted(t["id"] for t in s["tools"])==sorted({t["id"] for t in s["tools"]}) and len(s["tools"])==2
    assert {t["message_id"] for t in s["tools"]}=={s["msgs"][1]["id"]} and all(t["status"]=="complete" and t["created_at"] for t in s["tools"])
    assert len(s["edits"])==1 and s["edits"][0]["message_id"]==s["msgs"][1]["id"]
    db=duckdb.connect(); core.init_schema(db)
    ingest(db,path)
    tools,edits=db.execute("SELECT * FROM tool_calls ORDER BY id").fetchall(),db.execute("SELECT * FROM file_edits").fetchall()
    assert ingest(db,path)[-1]==set() and ingest(db,path)[-1]==set()
    assert db.execute("SELECT * FROM tool_calls ORDER BY id").fetchall()==tools and db.execute("SELECT * FROM file_edits").fetchall()==edits

def test_schema_18_retires_blob_session_without_edits(tmp_path,monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR",str(tmp_path/".claude"))
    db,cid,legacy=duckdb.connect(),gen_id("claude-code","s1"),tmp_path/"legacy.jsonl"; core.init_schema(db); write(legacy,events(tmp_path,"assistant",CALLS[1:])); ingest(db,legacy)
    db.execute("UPDATE messages SET role='api-request-blob' WHERE id IN (SELECT UNNEST(?))",[[gen_id("claude-code",f"{cid}:{i}") for i in (3,4,5)]])
    write(core.hook_root("claude-code")/"-repo"/"s1.jsonl",events(tmp_path,calls=CALLS[1:])); db.execute("UPDATE core_schema SET version=17"); core.init_schema(db)
    assert not db.execute("SELECT 1 FROM messages WHERE role='api-request-blob'").fetchall()
    assert db.execute("SELECT message_id,input::JSON->>'$.command' FROM tool_calls").fetchall()==[(gen_id("claude-code","message:"+cid+":a1"),"ls")]

def test_schema_18_retires_request_blob_rows_with_signed_deletes(tmp_path,monkeypatch):
    monkeypatch.setenv("CLAUDE_CONFIG_DIR",str(tmp_path/".claude"))
    checkout,path=repo(tmp_path),tmp_path/"data/convos.db"
    transcript=core.hook_root("claude-code")/"-repo"/"s1.jsonl"
    legacy=tmp_path/"legacy/s1.jsonl"; write(legacy,events(checkout,"assistant"))
    root,device,user,control,*_=signed_edit_graph()
    cfg=dict(user=user,device=device,workspaces={"w":dict(kind="personal",epoch=1)},controls={"w":control},server_state=dict(workspaces=[dict(id="w",controls=[control])]))
    with core._core(path,ready=True,purpose="test.legacy") as db:
        db.execute("INSERT INTO archive_sync VALUES (TRUE,2,?,?)",[user,device["id"]])
        for _ in range(3):
            with core._transaction(db): ingest(db,legacy)
        cid=gen_id("claude-code","s1"); blobs=[gen_id("claude-code",f"{cid}:{i}") for i in (3,4,5)]
        with core._transaction(db): db.execute("UPDATE messages SET role='api-request-blob' WHERE id IN (SELECT UNNEST(?))",[blobs])
    core.capture_provenance(path)
    with duckdb.connect(str(path)) as db:
        assert db.execute("SELECT count(*) FROM tool_calls WHERE message_id IN (SELECT UNNEST(?))",[blobs]).fetchone()[0]>=2
        assert db.execute("SELECT count(*) FROM provenance.local_facts WHERE kind='edit.observed'").fetchone()[0]>=2
        with connect(tmp_path/"state.db") as state: initial=scan(db,state)
        generation=core.archive_state(db)[1]
    keys={1:os.urandom(32)}
    attest_rows(path,cfg,"w",initial)
    receiver=tmp_path/"receiver.db"
    apply_row_replicas(receiver,[open_replica(env,keys[1]) for env in row_replicas(path,cfg,"w",initial,keys)],"w",[control],local_user=user,local_device="another-device")
    write(transcript,events(checkout))
    with core._core(path,purpose="test.schema") as db:
        db.execute("UPDATE core_schema SET version=17")
        core.init_schema(db)
        assert db.execute("SELECT version FROM core_schema").fetchone()==(core.CORE_VERSION,)
        assert not db.execute("SELECT 1 FROM messages WHERE role='api-request-blob' OR id IN (SELECT UNNEST(?))",[blobs]).fetchall()
        assistant=gen_id("claude-code","message:"+cid+":a1")
        assert db.execute("SELECT DISTINCT message_id FROM tool_calls UNION SELECT DISTINCT message_id FROM file_edits").fetchall()==[(assistant,)]
        assert db.execute("SELECT count(*),count(DISTINCT input) FROM tool_calls").fetchone()==(2,2) and db.execute("SELECT count(*) FROM file_edits").fetchone()==(1,)
        assert not db.execute("SELECT 1 FROM tool_calls WHERE created_at IS NULL UNION ALL SELECT 1 FROM file_edits WHERE created_at IS NULL").fetchall()
        assert db.execute("SELECT count(*) FROM provenance.local_facts WHERE kind='edit.observed'").fetchone()==(1,)
        assert not db.execute("SELECT 1 FROM provenance.file_edit_evidence v WHERE NOT EXISTS (SELECT 1 FROM file_edits e WHERE e.id=v.file_edit_id) UNION ALL SELECT 1 FROM provenance.file_edit_files x WHERE NOT EXISTS (SELECT 1 FROM file_edits e WHERE e.id=x.file_edit_id)").fetchall()
        after=core.archive_state(db)[1]; core.init_schema(db)
        assert core.archive_state(db)[1]==after
        with core._transaction(db): assert ingest(db,transcript)[-1]==set()
    assert path.with_name(path.name+".pre-v18.bak").is_file()
    with duckdb.connect(str(path)) as db,connect(tmp_path/"state.db") as state: changed=scan(db,state,changes=set(db.execute("SELECT kind,entity FROM archive_changes WHERE generation>?",[generation]).fetchall()))
    deleted={r["entity"] for r in changed if r["payload"].get("state")=="deleted"}
    assert {f"messages:{m}" for m in blobs}<=deleted and any(e.startswith("tool_calls:") for e in deleted) and any(r["kind"]=="edit.observed" for r in changed if r["payload"].get("state")=="deleted")
    attest_rows(path,cfg,"w",changed)
    apply_row_replicas(receiver,[open_replica(env,keys[1]) for env in row_replicas(path,cfg,"w",changed,keys)],"w",[control],local_user=user,local_device="another-device")
    with duckdb.connect(str(path),read_only=True) as source,duckdb.connect(str(receiver),read_only=True) as copy:
        for sql in ("SELECT id,role,content,created_at FROM messages ORDER BY id","SELECT id,message_id,tool_name,input::JSON->>'$.command',input::JSON->>'$.content',output,status,created_at FROM tool_calls ORDER BY id","SELECT id,message_id,content,created_at FROM file_edits ORDER BY id","SELECT file_edit_id FROM provenance.file_edit_files ORDER BY 1"): assert copy.execute(sql).fetchall()==source.execute(sql).fetchall()
        assert not source.execute("SELECT 1 FROM remote.row_conflicts").fetchall() and not copy.execute("SELECT 1 FROM remote.row_conflicts").fetchall()
