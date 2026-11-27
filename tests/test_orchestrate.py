"""High-value tests for Antigravity protocol & runtime invariants in Convos Orchestrate."""
import json, queue
from typer.testing import CliRunner

from ai_convos_orchestrate import app as orch_app, runtime, store, transcripts


def _ag_transcript(app_dir, sid, events):
    p = app_dir / "brain" / sid / ".system_generated" / "logs" / "transcript_full.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(map(json.dumps, events)) + "\n")
    return p


def test_antigravity_models_cache_and_command_invariants(tmp_path, monkeypatch):
    raw = (
        "\r\x1b[32mgemini-4-neon-high             Gemini 4 Neon (High)\x1b[0m\n"
        "gemini-4-neon-xhigh            Gemini 4 Neon (XHigh)\n"
        "gemini-4-neon-low              Gemini 4 Neon (Low)\n"
        "gemini-pro-orchestrator        Orchestrator (Carbon)\n"
    )
    fake_agy = tmp_path / "agy"
    fake_agy.write_text("#!/bin/sh\n")
    calls = []
    monkeypatch.setattr(runtime.shutil, "which", lambda name: str(fake_agy) if name == "agy" else None)
    monkeypatch.setattr(runtime.subprocess, "run", lambda *a, **k: (calls.append(a), type("R", (), {"stdout": raw})())[1])
    runtime.SCANNED.clear()
    parsed = runtime.antigravity_models()
    assert parsed == [
        {"id": "gemini-4-neon", "efforts": ["low", "high", "xhigh"], "effort": "high"},
        {"id": "gemini-pro-orchestrator", "efforts": [], "effort": None},
    ]
    runtime.SCANNED.clear()
    assert runtime.antigravity_models() == parsed and len(calls) == 1
    sid = "oabcdef01234"
    meta = dict(id=sid, title="T", provider="antigravity", provider_session=None, model="gemini-4-neon-xhigh", effort="high", permission="accept-edits", cwd=str(tmp_path), origin="gui", skills=["orchestrate"])
    cmd = runtime.command(sid, meta, resume=False)
    assert cmd[cmd.index("--model") + 1] == "gemini-4-neon" and cmd[cmd.index("--effort") + 1] == "high"
    assert "--conversation" not in cmd and "--sandbox" in cmd and runtime.MARK.search(f"99 /opt/homebrew/Caskroom/antigravity-cli/1.5.18/antigravity --log-file {cmd[cmd.index('--log-file') + 1]}")[1] == sid
    cmd_resume = runtime.command(sid, {**meta, "provider_session": "uuid-1", "permission": "always-proceed"}, resume=True)
    assert "--conversation" in cmd_resume and "--dangerously-skip-permissions" in cmd_resume and "--sandbox" not in cmd_resume
    monkeypatch.setenv("ANTIGRAVITY_CSRF_TOKEN", "secret")
    assert not any(k.startswith("ANTIGRAVITY_") for k in runtime.child_env(sid))


def test_antigravity_stream_json_send_read_and_permit(tmp_path, monkeypatch):
    sid = "oabcdef01235"
    meta = dict(id=sid, title="T", provider="antigravity", provider_session=None, model="gemini-4-neon", effort="high", permission="accept-edits", cwd=str(tmp_path), origin="gui", parent=None, skills=["orchestrate"])
    store.emit(sid, "open", "user", "hello", session=meta)
    q = queue.SimpleQueue()
    live_proc = type("P", (), {"poll": lambda self: None, "wait": lambda self: 0})()
    done_proc = type("P", (), {"poll": lambda self: 0, "wait": lambda self: 0})()
    p = dict(pid=99999, proc=live_proc, busy=False, stdin=object(), outbox=q, given={}, provider="antigravity")
    runtime.PROCS[sid] = p
    try:
        runtime.send(sid, "ping")
        assert json.loads(q.get_nowait().decode()) == {"event": "user", "message": {"content": "ping"}} and p["busy"] is True
        assert runtime.permit(sid, "always-proceed") == "after turn"
        assert p.get("relaunch") == "switched this session's permission" and q.empty()
    finally:
        runtime.PROCS.pop(sid, None)
    (store.sdir(sid) / "stdout.jsonl").write_text(
        json.dumps({"event": "init", "conversation_id": "33333333-4444-5555-6666-777777777777"}) + "\n"
        + json.dumps({"event": "result", "result": {"status": "SUCCESS", "num_turns": 2}}) + "\n"
    )
    states, real_set = [], runtime.set_runtime
    monkeypatch.setattr(runtime, "set_runtime", lambda s, st, **kw: (states.append((st, kw)), real_set(s, st, **kw))[1])
    p2 = dict(pid=99999, proc=done_proc, busy=True, stdin=object(), outbox=queue.SimpleQueue(), given={}, provider="antigravity")
    runtime.read(sid, p2, 0)
    reloaded = store.load(sid)
    assert reloaded["session"]["provider_session"] == "33333333-4444-5555-6666-777777777777" and p2["busy"] is False
    assert ("idle", {"started": True, "pid": 99999, "turns": 2}) in states and states[-1][0] == "stopped"


def test_antigravity_join_transcripts_and_plan_move(tmp_path, monkeypatch):
    ag_app, sid_uuid = tmp_path / "ag", "44444444-5555-6666-7777-888888888888"
    p = _ag_transcript(ag_app, sid_uuid, [
        {"step_index": 0, "type": "USER_INPUT", "status": "DONE", "created_at": "2026-11-27T10:00:00Z", "content": "hi"},
        {"step_index": 1, "type": "PLANNER_RESPONSE", "status": "DONE", "created_at": "2026-11-27T10:00:01Z", "content": "ok", "model": "gemini-4-neon-xhigh"},
    ])
    db = ag_app / "conversations" / f"{sid_uuid}.db"; db.parent.mkdir(parents=True); db.write_text("")
    monkeypatch.setenv("ANTIGRAVITY_APP_DATA_DIR", str(ag_app))
    monkeypatch.setenv("ANTIGRAVITY_CONVERSATION_ID", sid_uuid)
    for k in ("CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "MUSE_SESSION_ID", "CONVOS_ORCHESTRATE_SESSION"): monkeypatch.delenv(k, raising=False)
    transcripts.FOUND.clear(); transcripts.STATE.clear(); transcripts.SEEN.clear()
    res = CliRunner().invoke(orch_app, ["join", "--title", "AG task", "--json"])
    assert res.exit_code == 0, res.output
    s = store.load(json.loads(res.output)["session"])
    osid = s["session"]["id"]
    assert (s["session"]["provider"], s["session"]["provider_session"], s["session"]["model"]) == ("antigravity", sid_uuid, "gemini-4-neon-xhigh")
    # Delivered message recognition + quota limit detection/clearing
    p.write_text(p.read_text() + json.dumps({"step_index": 2, "type": "USER_INPUT", "status": "DONE", "created_at": "2026-11-27T10:00:02Z", "content": f"[orchestrate] 1 new from the user (session {osid}):\n- c1 user comment: check"}) + "\n" + json.dumps({"step_index": 3, "type": "PLANNER_RESPONSE", "status": "ERROR", "created_at": "2026-11-27T10:01:00Z", "error": "RESOURCE_EXHAUSTED: quota exceeded"}) + "\n")
    assert transcripts.arrived(s["session"]) == {"c1"} and "quota exceeded" in transcripts.limit(s["session"])["text"]
    p.write_text(p.read_text() + json.dumps({"step_index": 4, "type": "PLANNER_RESPONSE", "status": "DONE", "created_at": "2026-11-27T10:02:00Z", "content": "back", "model": "gemini-4-neon-xhigh"}) + "\n")
    assert transcripts.limit(s["session"]) is None
    # plan_move: interactive TUI detected; headless holder blocked
    monkeypatch.setattr(runtime.subprocess, "run", lambda *a, **k: type("R", (), {"stdout": "54321\n"})())
    monkeypatch.setattr(runtime, "commands", lambda pids: {54321: "/opt/homebrew/Caskroom/antigravity-cli/1.5.18/antigravity --mode plan"})
    monkeypatch.setattr(runtime, "antigravity_version", lambda: "1.5.18")
    plan = runtime.plan_move(s)
    assert plan["blocked"] is None and (plan["pid"], plan["model"], plan["effort"], plan["permission"]) == (54321, "gemini-4-neon", "xhigh", "plan")
    monkeypatch.setattr(runtime, "commands", lambda pids: {54321: "agy --input-format stream-json"})
    assert "headless" in runtime.plan_move(s)["blocked"]
