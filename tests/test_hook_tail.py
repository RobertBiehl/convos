import json, subprocess
from datetime import datetime, timedelta, timezone
import duckdb, pytest
from typer.testing import CliRunner
from ai_convos import cli
POPEN, TAIL_CUT = subprocess.Popen, cli.tail_cut

def event(i, role, content, parent=None):
    stamp = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"type": role, "uuid": f"u{i}", "parentUuid": parent, "sessionId": "tail-session", "timestamp": stamp, "message": {"role": role, "content": content}}

def turns(start, stop, text="turn"):
    return [event(i, "user" if i % 2 == 0 else "assistant", f"{text} {i}" if i % 2 == 0 else [{"type": "text", "text": f"{text} {i}"}], f"u{i-1}" if i else None) for i in range(start, stop)]

def write(path, events, mode="w"):
    with path.open(mode) as f: f.writelines(json.dumps(e) + "\n" for e in events)

def use(monkeypatch, data):
    for k, v in (("DATA_DIR", data), ("DB_PATH", data/"convos.db"), ("HOOK_DIR", data/"hook_inbox"), ("HOOK_STATE", data/"hook_state.json"), ("HOOK_TAIL", data/"hook_tails.json"), ("HOOK_PROGRESS", data/"hook_progress.json"), ("HOOK_EMBED_DIRTY", data/"hook_embeddings_dirty"), ("HOOK_FTS_DIRTY", data/"hook_fts_dirty")): monkeypatch.setattr(cli, k, v)

@pytest.fixture
def session(tmp_path, monkeypatch):
    use(monkeypatch, data := tmp_path / "data")
    monkeypatch.setattr(cli.subprocess, "Popen", lambda *a, **k: None if a[0][1:4] == ["-m", "ai_convos", "drain-hooks"] else POPEN(*a, **k))
    root = tmp_path / "claude"; path = root / "projects" / "-repo" / "tail-session.jsonl"; path.parent.mkdir(parents=True)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(root)); committed = []; commit = cli.commit_result
    monkeypatch.setattr(cli, "commit_result", lambda r, **k: (committed.append((len(r.msgs), len(r.tools))), commit(r, **k))[1])
    return path, data, committed

def capture(path): assert CliRunner().invoke(cli.app, ["capture", "claude-code"], input=json.dumps({"transcript_path": str(path), "hook_event_name": "Stop"})).exit_code == 0
def drain(path): return (capture(path), cli.drain_hooks())[1]

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

def test_rewritten_history_falls_back_to_a_full_ingest(session):
    path, data, committed = session; write(path, turns(0, 5000)); assert drain(path) == 1
    write(path, turns(0, 3000) + turns(3000, 5100, "rewritten")); assert drain(path) == 1
    assert committed[-1][0] == 5100, committed  # a tool row before any cut can depend on a rewritten result line
    assert current(path, data) and any(m[2] == "rewritten 3001" for m in archive(data)[0])

def test_unchanged_prefix_is_never_reparsed_into_the_archive_twice(session):
    path, data, committed = session; write(path, turns(0, 3000)); assert drain(path) == 1; write(path, turns(3000, 3002), "a"); assert drain(path) == 1; write(path, turns(3002, 3004), "a"); assert drain(path) == 1
    assert [c[0] for c in committed][1:] == [committed[1][0]] * 2 and committed[1][0] < 40 and current(path, data) and len(archive(data)[0]) == 3004

def rows(data):  # every column of every archive table the hook writes
    db = duckdb.connect(str(data / "convos.db"), read_only=True)
    try: return {t: sorted((dict(zip([d[0] for d in c.description], r)) for r in c.fetchall()), key=repr) for t in ("conversations", "messages", "tool_calls", "attachments", "file_edits", "provenance.file_edit_evidence") for c in [db.execute(f"SELECT * FROM {t}")]}
    finally: db.close()

def cols(rows, *names): return [tuple(r[n] for n in names) for r in rows]

def step(monkeypatch, data, path, snapshot, full=False):  # full=True: an archive that always ingests the full parse
    use(monkeypatch, data); monkeypatch.setattr(cli, "tail_cut", (lambda *a: None) if full else TAIL_CUT)
    write(path, snapshot); assert drain(path) == 1
    return rows(data)

def replay(session, monkeypatch, snapshots):  # after every snapshot the tail archive equals the full-ingest archive
    path, data, _ = session
    for s in snapshots: assert (got := step(monkeypatch, data, path, s)) == step(monkeypatch, data.parent / "full", path, s, True)
    return got

def call(i, parent, *tools): return event(i, "assistant", [{"type": "tool_use", "id": t, "name": "Edit" if t.startswith("e") else "Bash", "input": {"file_path": "/repo/x.py", "old_string": "a", "new_string": t} if t.startswith("e") else {"command": "ls"}} for t in tools], parent)
def result(i, parent, tool, out="ok", error=False): return event(i, "user", [{"type": "tool_result", "tool_use_id": tool, "content": out, **({"is_error": True} if error else {})}], parent)

def test_partial_tool_results_keep_the_other_call_pending(session, monkeypatch):
    base = [event(0, "user", "go"), call(1, "u0", "t1", "e2")]
    got = replay(session, monkeypatch, [base, base + [result(2, "u1", "t1")], base + [result(2, "u1", "t1"), result(3, "u2", "e2")]])
    assert {("Bash", "complete"), ("Edit", "complete")} <= set(cols(got["tool_calls"], "tool_name", "status")) and len(got["file_edits"]) == 1 and cols(got["provenance.file_edit_evidence"], "status", "reason") == [("confirmed", "provider_success")]

def test_result_for_an_earlier_call_arriving_after_later_prompts_completes_it(session, monkeypatch):
    base = [event(0, "user", "go"), call(1, "u0", "e1")] + turns(2, 60)
    got = replay(session, monkeypatch, [base, base + [result(60, "u59", "e1"), *turns(61, 63)]])
    assert ("Edit", "complete") in cols(got["tool_calls"], "tool_name", "status") and len(got["file_edits"]) == 1 and cols(got["provenance.file_edit_evidence"], "status", "reason") == [("confirmed", "provider_success")]

def test_rewritten_result_reaches_the_tool_call_before_the_cut(session, monkeypatch):
    got = replay(session, monkeypatch, [[call(0, None, "t1"), result(1, "u0", "t1", "old")], [call(0, None, "t1"), result(1, "u0", "t1", "new")]])
    assert '"new"' in [t["output"] for t in got["tool_calls"]]

def test_failed_edit_evidence_reaches_the_archive(session, monkeypatch):
    base = [event(0, "user", "go"), call(1, "u0", "e1")]
    got = replay(session, monkeypatch, [base + [result(2, "u1", "e1")], base + [result(2, "u1", "e1", "boom", True)]])
    assert ("Edit", "failed") in cols(got["tool_calls"], "tool_name", "status") and cols(got["provenance.file_edit_evidence"], "status", "reason") == [("invalid", "provider_failure")]

def test_second_result_for_a_completed_call_reaches_the_archive(session, monkeypatch):
    s0 = [call(0, None, "e1"), result(1, "u0", "e1", "boom", True)]; s1 = s0 + [result(2, "u1", "e1")]
    got = replay(session, monkeypatch, [s0, s1, s1 + [result(3, "u2", "e1", "boom", True)]])  # failed -> succeeded -> failed again
    assert len(got["file_edits"]) == 1 and cols(got["provenance.file_edit_evidence"], "status", "reason") == [("invalid", "provider_failure")]

def test_rewrite_between_previous_drain_ends_is_detected(session, monkeypatch):
    done = event(2, "assistant", [{"type": "text", "text": "done"}], "u1")
    got = replay(session, monkeypatch, [[call(0, None, "t1"), result(1, "u0", "t1", "old"), done], [call(0, None, "t1"), result(1, "u0", "t1", "new"), done, *turns(3, 4)]])
    assert '"new"' in [t["output"] for t in got["tool_calls"]]

def test_appended_parent_reaches_its_earlier_child(session, monkeypatch):
    child = event(1, "user", "child", "u0")
    got = replay(session, monkeypatch, [[child], [child, event(0, "assistant", [{"type": "text", "text": "parent"}])]])
    assert all(m["parent_id"] for m in got["messages"] if m["content"] == "child")

def test_late_agent_identity_reingests_the_prefix(session, monkeypatch):
    got = replay(session, monkeypatch, [turns(0, 3), turns(0, 3) + [{**event(3, "user", "late"), "agentId": "a1"}]])
    assert len(got["messages"]) == 3 + 4

OK, BAD, MORE = [call(0, None, "e1"), result(1, "u0", "e1")], [call(0, None, "e1"), result(1, "u0", "e1", "boom", True)], turns(2, 3)
class Killed(BaseException): pass  # process death: no except handler in drain_hooks runs

def crash(session, monkeypatch, fail, error=OSError):  # S0 succeeds; S1 rewrites the result to a failure and dies where fail(path) says
    path, data, _ = session; save = cli.atomic_json
    step(monkeypatch, data, path, OK); monkeypatch.setattr(cli, "atomic_json", lambda p, v: (_ for _ in ()).throw(error()) if fail(p) else save(p, v))
    write(path, BAD)
    try: outcome = drain(path)
    except error: outcome = "raised"
    monkeypatch.setattr(cli, "atomic_json", save)
    return outcome

def restored(session, monkeypatch, between=()):  # S0 returns with one appended turn; the archive equals full ingests of the same snapshots and the edit is confirmed again
    path, data, _ = session; write(path, OK + MORE); assert drain(path) == 1
    assert (got := rows(data)) == [step(monkeypatch, data.parent / "full", path, s, True) for s in [OK, BAD, *between, OK + MORE]][-1]
    assert cols(got["provenance.file_edit_evidence"], "status", "reason") == [("confirmed", "provider_success")]

def test_recovered_journal_restores_the_tail_of_its_commit(session, monkeypatch):  # crash after the completion journal, before hook_tails.json
    assert crash(session, monkeypatch, lambda p: p == cli.HOOK_TAIL) == "raised" and cli.drain_hooks() == 1
    restored(session, monkeypatch)

def test_failed_completion_marker_invalidates_the_tail(session, monkeypatch):  # commit succeeded, writing the completion journal failed
    assert crash(session, monkeypatch, lambda p: p.suffix == ".work") == 0
    restored(session, monkeypatch)

def test_unfinished_work_recovered_against_an_empty_transcript_invalidates_the_tail(session, monkeypatch):
    assert crash(session, monkeypatch, lambda p: p.suffix == ".work", Killed) == "raised"
    write(session[0], []); assert cli.drain_hooks() == 1  # the forced retry finds no conversation
    restored(session, monkeypatch, [[]])

def test_unfinished_work_whose_retry_coalesces_invalidates_the_tail(session, monkeypatch):
    path, parse = session[0], cli.hook_result
    assert crash(session, monkeypatch, lambda p: p.suffix == ".work", Killed) == "raised"
    def changing(*a):  # the transcript changes while the forced retry parses, and a fresh ordinary event is queued
        monkeypatch.setattr(cli, "hook_result", parse); write(path, OK + MORE); capture(path); return parse(*a)
    monkeypatch.setattr(cli, "hook_result", changing); assert cli.drain_hooks() == 0
    restored(session, monkeypatch)
