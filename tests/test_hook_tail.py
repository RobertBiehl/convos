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
    with path.open(mode) as f: f.write(events if isinstance(events, str) else "".join(json.dumps(e) + "\n" for e in events))  # str: raw transcript text

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

def test_claims_left_at_the_time_budget_keep_their_tail(session, monkeypatch):  # an unprocessed claim is not crashed work
    path, data, committed = session; paths = [path, path.with_name("other.jsonl")]; other = lambda es: [{**e, "sessionId": "other"} for e in es]
    write(paths[0], turns(0, 200)); write(paths[1], other(turns(0, 200))); [capture(p) for p in paths]; assert cli.drain_hooks() == 2
    write(paths[0], turns(200, 202), "a"); write(paths[1], other(turns(200, 202)), "a"); [capture(p) for p in paths]
    monkeypatch.setattr(cli, "HOOK_DRAIN_SECONDS", -1); assert cli.drain_hooks() == 1
    queue, progress = sorted((p.suffix, json.loads(p.read_text()).get("retry")) for p in [*cli.HOOK_DIR.glob("*.json"), *cli.HOOK_DIR.glob("*.work")]), json.loads(cli.HOOK_PROGRESS.read_text())
    monkeypatch.setattr(cli, "HOOK_DRAIN_SECONDS", 10); assert cli.drain_hooks() == 1
    assert committed[-2:] == [(2, 0), (2, 0)] and current(paths[1], data), committed  # both commit only their appended turns; a retry would re-commit all 202
    assert queue == [(".json", None)] and (progress["failed"], progress["pending"], progress["failed_claims"]) == (0, 1, {})

def rows(data):  # every column of every archive table the hook writes
    db = duckdb.connect(str(data / "convos.db"), read_only=True)
    try: return {t: sorted((dict(zip([d[0] for d in c.description], r)) for r in c.fetchall()), key=repr) for t in ("conversations", "messages", "tool_calls", "attachments", "file_edits", "provenance.file_edit_evidence") for c in [db.execute(f"SELECT * FROM {t}")]}
    finally: db.close()

def cols(rows, *names): return [tuple(r[n] for n in names) for r in rows]

def step(monkeypatch, data, path, snapshot, full=False):  # full=True: an archive that keeps no tail, so every drain ingests the full parse
    use(monkeypatch, data); full and cli.HOOK_TAIL.unlink(missing_ok=True)
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

def resumed(session, monkeypatch, snapshots, path=None):  # replay, recording per drain whether it resumed from a tail and how many messages it parsed
    seen, parse = [], cli.hook_result
    monkeypatch.setattr(cli, "hook_result", lambda s, p, b=None, t=None: (r := parse(s, p, b, t), cli.HOOK_TAIL.parent == session[1] and seen.append((t is not None, len(r.msgs))))[0])
    return replay((path or session[0], *session[1:]), monkeypatch, snapshots), seen

def test_growing_transcript_resumes_and_matches_full_ingests(session, monkeypatch):  # a call completed a turn later, parents before the offset (a message, a system line), a sidechain line
    base = turns(0, 40) + [call(40, "u39", "e1"), {"type": "system", "uuid": "s1", "parentUuid": "u40", "timestamp": "2026-01-01T00:01:00Z"}]
    s1 = base + [result(41, "s1", "e1"), *turns(42, 44)]; s2 = s1 + [{**event(44, "user", "side", "u43"), "isSidechain": True, "agentName": "helper"}]; s3 = s2 + [event(45, "user", "late", "s1"), *turns(46, 47)]
    got, seen = resumed(session, monkeypatch, [base, s1, s2, s3])
    assert [t for t, _ in seen] == [False, True, True, True] and [n for _, n in seen][1:] == [4, 1, 2], seen  # s1 re-parses the pending call's message
    assert ("Edit", "complete") in cols(got["tool_calls"], "tool_name", "status") and len(got["file_edits"]) == 1 and '"session_kind": "subagent"' in got["conversations"][0]["metadata"]
    assert json.loads(cli.HOOK_TAIL.read_text())[cli.gen_id("hook", f"claude-code:{session[0].resolve()}")]["dangling"] == []  # s1 found its parent before the offset

def test_subagent_transcript_resumes_and_matches_full_ingests(session, monkeypatch):
    path = session[0].parent / "tail-session" / "subagents" / "agent-a1.jsonl"; path.parent.mkdir(parents=True)
    side = lambda es: [{**e, "isSidechain": True, "agentId": "a1"} for e in es]
    got, seen = resumed(session, monkeypatch, [side(turns(0, 30)), side(turns(0, 33)), side(turns(0, 35))], path)
    assert [t for t, _ in seen] == [False, True, True] and json.loads(got["conversations"][0]["metadata"])["parent_session_id"] == "tail-session"

def test_parallel_calls_completing_across_drains_do_not_force_a_full_parse(session, monkeypatch):  # a result re-read below the old end is not new
    s0 = [event(0, "user", "go"), call(1, "u0", "t1"), call(2, "u1", "t2"), result(3, "u2", "t1")]
    got, seen = resumed(session, monkeypatch, [s0, s0 + [result(4, "u3", "t2")]])
    assert seen[1][0] and seen[1][1] == 3 and sorted(cols(got["tool_calls"], "status")) == [("complete",), ("complete",), ("pending",)]

def test_truncated_or_rewritten_transcript_falls_back_to_a_full_parse(session, monkeypatch):  # replaced rows become history stamped with wall time, so this checks a fresh parse is stored
    path, data, committed = session; parse, seen = cli.hook_result, []
    monkeypatch.setattr(cli, "hook_result", lambda s, p, b=None, t=None: (seen.append(t is not None), parse(s, p, b, t))[1])
    for snapshot in (turns(0, 300), turns(0, 300) + turns(300, 302), turns(0, 200), turns(0, 150) + turns(150, 320, "regrown")): write(path, snapshot); assert drain(path) == 1 and current(path, data)
    assert seen == [False, True, False, False] and [n for n, _ in committed] == [300, 2, 200, 320]

def test_rebinding_the_session_between_drains_matches_full_ingests(session, monkeypatch):  # a provider binding moves the session to another conversation
    path, data, _ = session; s0, s1 = turns(0, 30), turns(0, 34)
    for d in (data, data.parent / "full"): step(monkeypatch, d, path, s0, d != data)
    for d in (data, data.parent / "full"):
        with duckdb.connect(str(d / "convos.db")) as db: db.execute("INSERT INTO conversations(id,source) VALUES ('moved','claude-code')"); cli.project_provider_bindings(db, "claude-code", "tail-session", "moved", [])
    assert (got := step(monkeypatch, data, path, s1)) == step(monkeypatch, data.parent / "full", path, s1, True) and sum(m["conversation_id"] == "moved" for m in got["messages"]) == 34

def test_narrow_bindings_are_the_full_bindings_of_that_session(session):
    path, data, _ = session; other = path.with_name("other.jsonl"); write(path, turns(0, 20)); write(other, [{**e, "sessionId": "other"} for e in turns(0, 10)]); [capture(p) for p in (path, other)]; assert cli.drain_hooks() == 2
    with duckdb.connect(str(data / "convos.db"), read_only=True) as db: full, narrow = cli.session_bindings(db), cli.session_bindings(db, [("claude-code", "tail-session")])
    mine = lambda b: {k: v for k, v in b.items() if len(k) < 4 or k[1] == cli.gen_id("claude-code", "tail-session")}
    assert mine(full) == mine(narrow) == narrow and len(narrow) > 40 and len(full) > len(narrow)

def test_desktop_resume_reappending_uuids_after_the_offset_matches_full_ingests(session, monkeypatch):  # #173: a repeated uuid is one message, from its first line, even when that line is before the offset
    first = [event(0, "user", "go"), call(1, "u0", "e1"), result(2, "u1", "e1"), *turns(3, 6)]
    events = first + [{**e, "cwd": "/repo/sub", "slug": "other"} for e in first[1:]] + turns(6, 9)  # Desktop re-appends the same uuids and messages with other metadata
    got, seen = resumed(session, monkeypatch, [events[:k] for k in range(1, len(events) + 1)])
    assert len(got["messages"]) == 9 and ("Edit", "complete") in cols(got["tool_calls"], "tool_name", "status") and seen[-2:] == [(True, 1), (True, 1)], seen  # turns after the block resume again

def test_parent_in_another_file_of_the_session_stays_unlinked(session, monkeypatch):  # a full parse of this file cannot see it either
    path, data, _ = session; other = path.with_name("other.jsonl"); write(other, turns(0, 6))
    for d in (data, data.parent / "full"): use(monkeypatch, d); assert drain(other) == 1
    b0 = [event(100, "user", "start"), event(101, "assistant", [{"type": "text", "text": "b"}], "u100")]
    got, seen = resumed(session, monkeypatch, [b0, b0 + [event(102, "user", "branch", "u5")]])
    assert seen[1] == (True, 1) and [m["parent_id"] for m in got["messages"] if m["content"] == "branch"] == [None]

def test_last_line_without_newline_keeps_line_indexes(session, monkeypatch):  # the offset stops before a line that may still grow
    text = "".join(json.dumps(e) + "\n" for e in turns(0, 10))
    got, seen = resumed(session, monkeypatch, [text[:-1], text + "".join(json.dumps(e) + "\n" for e in turns(10, 14))])
    assert seen[1][0] and sorted(json.loads(m["metadata"])["provider_index"] for m in got["messages"]) == list(range(14))

def test_call_appended_after_its_logged_result_completes(session, monkeypatch):  # out-of-order records: the result is before the offset
    got = replay(session, monkeypatch, [[result(0, None, "t1")], [result(0, None, "t1"), call(1, "u0", "t1")]])
    assert cols(got["tool_calls"], "status", "output") == [("complete", '"ok"')]
