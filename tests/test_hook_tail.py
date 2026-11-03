import json, subprocess
from datetime import datetime, timedelta, timezone
import duckdb, pytest
from typer.testing import CliRunner
from ai_convos import cli
POPEN = subprocess.Popen

def event(i, role, content, parent=None):
    stamp = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"type": role, "uuid": f"u{i}", "parentUuid": parent, "sessionId": "tail-session", "timestamp": stamp, "message": {"role": role, "content": content}}

def turns(start, stop, text="turn"):
    return [event(i, "user" if i % 2 == 0 else "assistant", f"{text} {i}" if i % 2 == 0 else [{"type": "text", "text": f"{text} {i}"}], f"u{i-1}" if i else None) for i in range(start, stop)]

def write(path, events, mode="w"):
    with path.open(mode) as f: f.writelines(json.dumps(e) + "\n" for e in events)

@pytest.fixture
def session(tmp_path, monkeypatch):
    data = tmp_path / "data"
    for k, v in (("DATA_DIR", data), ("DB_PATH", data/"convos.db"), ("HOOK_DIR", data/"hook_inbox"), ("HOOK_STATE", data/"hook_state.json"), ("HOOK_TAIL", data/"hook_tails.json"), ("HOOK_PROGRESS", data/"hook_progress.json"), ("HOOK_EMBED_DIRTY", data/"hook_embeddings_dirty"), ("HOOK_FTS_DIRTY", data/"hook_fts_dirty")): monkeypatch.setattr(cli, k, v)
    monkeypatch.setattr(cli.subprocess, "Popen", lambda *a, **k: None if a[0][1:4] == ["-m", "ai_convos", "drain-hooks"] else POPEN(*a, **k))
    root = tmp_path / "claude"; path = root / "projects" / "-repo" / "tail-session.jsonl"; path.parent.mkdir(parents=True)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(root)); committed = []; commit = cli.commit_result
    monkeypatch.setattr(cli, "commit_result", lambda r, **k: (committed.append((len(r.msgs), len(r.tools))), commit(r, **k))[1])
    return path, data, committed

def drain(path):
    assert CliRunner().invoke(cli.app, ["capture", "claude-code"], input=json.dumps({"transcript_path": str(path), "hook_event_name": "Stop"})).exit_code == 0
    return cli.drain_hooks()

def archive(data):
    db = duckdb.connect(str(data / "convos.db"), read_only=True)
    try: return sorted(db.execute("SELECT id, role, content, parent_id FROM messages").fetchall()), sorted(db.execute("SELECT id, status, output FROM tool_calls").fetchall())
    finally: db.close()

def fresh(path, data):
    db = duckdb.connect(str(data / "convos.db"), read_only=True); bindings = cli.session_bindings(db); db.close()
    s = cli.parse_claude_code_session(path, bindings)
    return sorted((m["id"], m["role"], m["content"], m["parent_id"]) for m in s["msgs"]), sorted((t["id"], t["status"], t["output"]) for t in s["tools"])

def current(path, data):  # every row of a from-scratch parse is stored as-is (the archive also keeps history rows of replaced versions)
    stored, parsed = archive(data), fresh(path, data)
    return all(m in stored[0] for m in parsed[0]) and all(t in stored[1] for t in parsed[1])

def test_appended_turns_commit_only_the_tail_and_match_a_full_parse(session):
    path, data, committed = session; write(path, turns(0, 5000)); assert drain(path) == 1 and committed[-1][0] == 5000
    write(path, turns(5000, 5010), "a"); assert drain(path) == 1
    assert committed[-1][0] < 40, committed
    assert archive(data) == fresh(path, data) and len(archive(data)[0]) == 5010

def test_tool_result_arriving_after_the_anchor_completes_the_call(session):
    path, data, committed = session; use = event(5000, "assistant", [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}], "u4999")
    write(path, turns(0, 5000) + [use]); assert drain(path) == 1 and [t[1] for t in archive(data)[1]] == ["pending"]
    write(path, [event(5001, "user", [{"type": "tool_result", "tool_use_id": "t1", "content": "done"}], "u5000")] + turns(5002, 5004), "a"); assert drain(path) == 1
    assert committed[-1][0] < 40 and current(path, data) and sorted(t[1] for t in archive(data)[1]) == ["complete", "pending"]  # the pending version stays as history

def test_rewritten_history_goes_back_to_the_last_matching_chunk(session):
    path, data, committed = session; write(path, turns(0, 5000)); assert drain(path) == 1
    write(path, turns(0, 3000) + turns(3000, 5100, "rewritten")); assert drain(path) == 1
    assert 2000 < committed[-1][0] < 5100, committed  # resumes at the chunk boundary before the rewrite, not at the end and not at zero
    assert current(path, data) and any(m[2] == "rewritten 3001" for m in archive(data)[0])

def test_unchanged_prefix_is_never_reparsed_into_the_archive_twice(session):
    path, data, committed = session; write(path, turns(0, 3000)); assert drain(path) == 1; write(path, turns(3000, 3002), "a"); assert drain(path) == 1; write(path, turns(3002, 3004), "a"); assert drain(path) == 1
    assert [c[0] for c in committed][1:] == [committed[1][0]] * 2 and committed[1][0] < 40 and current(path, data) and len(archive(data)[0]) == 3004
