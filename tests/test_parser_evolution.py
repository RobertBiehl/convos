"""Reimport stability across parser evolution: current parsers round-trip without duplication, and changed messages preserve history."""
import json

import pytest
from ai_convos import cli as core

TABLES = ("conversations", "messages", "tool_calls", "attachments", "artifacts", "file_edits", "provider_sessions")


def fixture(path, source):
    stamp = "2026-01-01T00:00:00.000000"
    if source == "codex":
        events = [dict(type="session_meta", timestamp=stamp, payload=dict(cwd="/repo", id="session")),
                  dict(type="response_item", timestamp=stamp, payload=dict(type="message", role="user", content=[dict(type="input_text", text="inspect")])),
                  dict(type="response_item", timestamp=stamp, payload=dict(type="function_call", name="Read", call_id="call-1", arguments='{"path":"x.txt"}')),
                  dict(type="response_item", timestamp=stamp, payload=dict(type="function_call_output", call_id="call-1", output="contents"))]
    else:
        events = [dict(type="system", sessionId="session"),
                  dict(type="user", timestamp=stamp, message=dict(content="inspect")),
                  dict(type="assistant", timestamp=stamp, message=dict(content=[dict(type="tool_use", id="call-1", name="Read", input=dict(path="x.txt"))])),
                  dict(type="user", timestamp=stamp, message=dict(content=[dict(type="tool_result", tool_use_id="call-1", content="contents")]))]
    path.write_text("\n".join(map(json.dumps, events)))
    return (core.parse_codex_session if source == "codex" else core.parse_claude_code_session)(path)


def result(session):
    return core.ParseResult(convs=[session["conv"]], msgs=session["msgs"], tools=session["tools"], attachs=session["attachs"],
                            edits=session.get("edits", []), edit_evidence=session.get("edit_evidence", []))


def snapshot(db):
    return {t: db.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in TABLES}


@pytest.mark.parametrize("source", ["codex", "claude-code"])
def test_reimport_is_stable_and_idempotent(tmp_path, source):
    session = fixture(tmp_path / "session.jsonl", source)
    assert [(t["tool_name"], t["status"]) for t in session["tools"]] == [("Read", "complete")]
    with core._core(tmp_path / "db", purpose="test.reimport") as db:
        core.init_schema(db)
        with core._transaction(db): core.upsert(db, result(session))
        before = snapshot(db)
        assert db.execute("SELECT tool_name,status,COUNT(*) FROM tool_calls GROUP BY 1,2").fetchall() == [("Read", "complete", 1)]
        with core._transaction(db): core.upsert(db, result(session))
        assert snapshot(db) == before
        assert core.archive_relationships(db) == {}


@pytest.mark.parametrize("source", ["codex", "claude-code"])
def test_changed_message_reimport_preserves_history(tmp_path, source):
    path = tmp_path / "session.jsonl"
    session = fixture(path, source)
    old, new = session["msgs"][-1]["content"], "CHANGED CONTENT"
    assert old != new
    with core._core(tmp_path / "db", purpose="test.history") as db:
        core.init_schema(db)
        with core._transaction(db): core.upsert(db, result(session))
        before = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        session["msgs"][-1] = {**session["msgs"][-1], "content": new}
        with core._transaction(db): core.upsert(db, result(session))
        rows = db.execute("SELECT content,metadata FROM messages WHERE conversation_id=?", [session["conv"]["id"]]).fetchall()
        assert len(rows) == before + 1
        assert [(content, "history_of" in (meta or "")) for content, meta in rows].count((old, True)) == 1
        assert [(content, "history_of" in (meta or "")) for content, meta in rows].count((new, False)) == 1
        assert core.archive_relationships(db) == {}
