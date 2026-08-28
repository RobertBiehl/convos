"""Tests for local file format parsers."""

import pytest, json, tempfile, zipfile
from pathlib import Path
from datetime import datetime


# ---- Claude Code JSONL Parser Tests ----

class TestClaudeCodeParser:
    """Tests for parsing Claude Code session JSONL files."""

    def test_parse_basic_session(self, tmp_path):
        """Parse a minimal Claude Code session."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test-project"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session-123.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "system", "timestamp": "2024-01-01T00:00:00Z", "sessionId":"session-123", "cwd": "/test", "gitBranch": "main", "version":"2.1.9", "entrypoint":"cli"}),
            json.dumps({"type": "human", "timestamp": "2024-01-01T00:00:01Z", "message": {"content": "Hello"}}),
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:00:02Z", "message": {"model":"claude-opus-4-8", "content": [{"type": "text", "text": "Hi there!"}]}}),
        ]))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        assert len(result.convs) == 1
        assert len(result.msgs) == 2
        assert result.convs[0]["cwd"] == "/test"
        assert result.convs[0]["git_branch"] == "main"
        assert result.msgs[0]["role"] == "user"
        assert result.msgs[1]["role"] == "assistant" and result.msgs[1]["model"] == "claude-opus-4-8"
        assert result.convs[0]["model"] == "claude-opus-4-8"
        assert json.loads(result.convs[0]["metadata"]) == {"session_id":"session-123","session_kind":"main","session_kind_evidence":"inferred","originator":"cli","client_version":"2.1.9","capture_mode":"transcript"}

    def test_subagent_session_metadata_is_normalized(self,tmp_path):
        from ai_convos.cli import parse_claude_code
        sessions=tmp_path/".claude/projects/-test/root/subagents"; sessions.mkdir(parents=True); (sessions/"agent-child.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"system","timestamp":"2026-01-01T00:00:00Z","sessionId":"root","agentId":"child","isSidechain":True,"cwd":"/repo","gitBranch":"main","version":"2.1.9"},
            {"type":"user","timestamp":"2026-01-01T00:00:01Z","message":{"content":"inspect"}},
            {"type":"assistant","timestamp":"2026-01-01T00:00:02Z","message":{"model":"claude-opus-4-8","content":[{"type":"text","text":"done"}]}}]))
        conv=parse_claude_code(tmp_path/".claude/projects").convs[0]; meta=json.loads(conv["metadata"])
        assert (conv["cwd"],conv["git_branch"],conv["model"]) == ("/repo","main","claude-opus-4-8") and meta == {"session_id":"child","parent_session_id":"root","session_kind":"subagent","session_kind_evidence":"exact","agent_id":"child","client_version":"2.1.9","capture_mode":"transcript"}

    def test_parent_thread_tree(self, tmp_path):
        """parentUuid chains become parent_id links; roots and unknown parents stay NULL."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)
        (session_dir / "s.jsonl").write_text("\n".join([
            json.dumps({"type": "human", "uuid": "u-1", "parentUuid": None, "timestamp": "2024-01-01T00:00:00Z", "message": {"content": "Hello"}}),
            json.dumps({"type": "assistant", "uuid": "a-1", "parentUuid": "u-1", "timestamp": "2024-01-01T00:00:01Z", "message": {"content": [{"type": "text", "text": "Hi"}]}}),
            json.dumps({"type": "human", "uuid": "u-2", "parentUuid": "a-1", "timestamp": "2024-01-01T00:00:02Z", "message": {"content": "Branch A"}}),
            json.dumps({"type": "human", "uuid": "u-3", "parentUuid": "a-1", "timestamp": "2024-01-01T00:00:03Z", "message": {"content": "Branch B (regenerated)"}}),
        ]))

        msgs = parse_claude_code(tmp_path / ".claude" / "projects").msgs
        by_content = {m["content"]: m for m in msgs}
        assert by_content["Hello"]["parent_id"] is None
        assert by_content["Hi"]["parent_id"] == by_content["Hello"]["id"]
        assert by_content["Branch A"]["parent_id"] == by_content["Hi"]["id"]
        assert by_content["Branch B (regenerated)"]["parent_id"] == by_content["Hi"]["id"]

    def test_parse_thinking_blocks(self, tmp_path):
        """Parse session with thinking blocks."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:00:00Z", "message": {"content": [
                {"type": "thinking", "thinking": "Let me think about this..."},
                {"type": "text", "text": "Here's my answer."}
            ]}}),
        ]))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        assert len(result.msgs) == 1
        assert result.msgs[0]["thinking"] == "Let me think about this..."
        assert result.msgs[0]["content"] == "Here's my answer."

    def test_parse_tool_calls(self, tmp_path):
        """Parse session with tool calls."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:00:00Z", "message": {"content": [
                {"type": "tool_use", "name": "Read", "input": {"file_path": "/test.py"}},
                {"type": "text", "text": "Let me read that file."}
            ]}}),
        ]))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        assert len(result.tools) == 1
        assert result.tools[0]["tool_name"] == "Read"

    def test_tool_results_merge_into_their_invocation(self,tmp_path):
        from ai_convos.cli import parse_claude_code
        session=tmp_path/".claude/projects/-test"; session.mkdir(parents=True); (session/"s.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"assistant","timestamp":"2026-01-01T00:00:00Z","message":{"content":[{"type":"tool_use","id":"call-1","name":"Read","input":{"file_path":"x"}}]}},
            {"type":"user","timestamp":"2026-01-01T00:00:01Z","message":{"content":[{"type":"tool_result","tool_use_id":"call-1","content":"body"}]}}]))
        result=parse_claude_code(tmp_path/".claude/projects"); assert len(result.tools)==1 and result.tools[0]["status"]=="complete" and json.loads(result.tools[0]["output"])=="body"

    def test_parse_file_edits(self, tmp_path):
        """Parse session with file edits."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:00:00Z", "message": {"content": [
                {"type": "tool_use", "id":"write-1", "name": "Write", "input": {"file_path": "/test.py", "content": "print('hello')"}},
                {"type": "text", "text": "Created file."}
            ]}}),
            json.dumps({"type":"user","timestamp":"2024-01-01T00:00:30Z","message":{"content":[{"type":"tool_result","tool_use_id":"write-1","content":"ok","is_error":False}]}}),
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:01:00Z", "message": {"content": [
                {"type": "tool_use", "id":"edit-1", "name": "Edit", "input": {"file_path": "/test.py", "old_string": "print('hello')", "new_string": "print('world')"}},
                {"type": "text", "text": "Updated file."}
            ]}}), json.dumps({"type":"user","timestamp":"2024-01-01T00:01:30Z","message":{"content":[{"type":"tool_result","tool_use_id":"edit-1","content":"ok","is_error":False}]}}),
        ]))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        assert len(result.edits) == 2
        assert result.edits[0]["file_path"] == "/test.py"
        assert result.edits[0]["edit_type"] == "write"
        assert result.edits[0]["old_content"] is None
        assert result.edits[1]["edit_type"] == "edit"
        assert result.edits[1]["content"] == "print('world')"
        assert result.edits[1]["old_content"] == "print('hello')"
        assert {v["status"] for v in result.edit_evidence} == {"confirmed"}
        assert {v["tool_call_id"] for v in result.edit_evidence} == {t["id"] for t in result.tools if t["tool_name"] in ("Write","Edit")}

    def test_tool_only_turns_keep_message_rows(self, tmp_path):
        """Tool-only assistant turns produce message rows so tools/edits are not orphaned."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "user", "timestamp": "2024-01-01T00:00:00Z", "message": {"content": "edit the file"}}),
            json.dumps({"type": "assistant", "timestamp": "2024-01-01T00:00:01Z", "message": {"content": [
                {"type": "tool_use", "name": "Edit", "input": {"file_path": "/t.py", "old_string": "a", "new_string": "b"}}
            ]}}),
        ]))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        msg_ids = {m["id"] for m in result.msgs}
        assert len(result.msgs) == 2  # tool-only assistant turn included despite empty text
        assert all(t["message_id"] in msg_ids for t in result.tools)
        assert result.edits == []

    def test_failed_edit_result_retains_tool_but_not_canonical_edit(self,tmp_path):
        from ai_convos.cli import parse_claude_code
        session=tmp_path/".claude/projects/-test"; session.mkdir(parents=True); (session/"s.jsonl").write_text("\n".join(map(json.dumps,[{"type":"assistant","message":{"content":[{"type":"tool_use","id":"e1","name":"Edit","input":{"file_path":"x.py","old_string":"a","new_string":"b"}}]}},{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"e1","content":"permission denied","is_error":True}]}}])))
        result=parse_claude_code(tmp_path/".claude/projects"); assert len(result.tools)==1 and result.tools[0]["status"]=="failed" and result.edits==[]

    def test_empty_session_skipped(self, tmp_path):
        """Empty sessions (no messages) are skipped."""
        from ai_convos.cli import parse_claude_code

        session_dir = tmp_path / ".claude" / "projects" / "-test"
        session_dir.mkdir(parents=True)

        jsonl = session_dir / "session.jsonl"
        jsonl.write_text(json.dumps({"type": "system", "timestamp": "2024-01-01T00:00:00Z"}))

        result = parse_claude_code(tmp_path / ".claude" / "projects")

        assert len(result.convs) == 0


# ---- Codex Parser Tests ----

class TestCodexParser:
    """Tests for parsing Codex session JSONL files."""

    def test_parse_basic_session(self, tmp_path):
        """Parse a minimal Codex session."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions" / "2024" / "01"
        sessions_dir.mkdir(parents=True)

        jsonl = sessions_dir / "session-123.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {"id":"provider-123", "cwd": "/test", "model_provider": "openai", "originator":"codex-tui", "cli_version":"0.116.0", "git":{"branch":"main","commit_hash":"abc","repository_url":"https://example.com/repo.git"}}}),
            json.dumps({"type":"turn_context","timestamp":"2024-01-01T00:00:00Z","payload":{"model":"gpt-5.6-sol"}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:01Z", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Hello"}]}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:02Z", "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Hi!"}]}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        assert len(result.convs) == 1
        assert len(result.msgs) == 2
        assert result.convs[0]["cwd"] == "/test"
        assert (result.convs[0]["model"],result.convs[0]["git_branch"]) == ("gpt-5.6-sol","main")
        assert {m["model"] for m in result.msgs} == {"gpt-5.6-sol"}
        assert json.loads(result.convs[0]["metadata"]) == {"session_id":"provider-123","session_kind":"main","session_kind_evidence":"inferred","originator":"codex-tui","client_version":"0.116.0","capture_mode":"transcript","git_repository":"https://example.com/repo.git","git_commit":"abc"}

    def test_subagent_session_metadata_is_normalized(self,tmp_path):
        from ai_convos.cli import parse_codex
        sessions=tmp_path/".codex/sessions"; sessions.mkdir(parents=True); spawn={"thread_spawn":{"parent_thread_id":"root","agent_nickname":"Ada","agent_role":"explorer","depth":1}}
        (sessions/"child.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"id":"child","cwd":"/repo","source":{"subagent":spawn},"cli_version":"0.116.0"}},
            {"type":"turn_context","timestamp":"2026-01-01T00:00:00Z","payload":{"model":"gpt-5.6-luna"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect"}]}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}]))
        conv=parse_codex(tmp_path/".codex").convs[0]; meta=json.loads(conv["metadata"])
        assert conv["model"] == "gpt-5.6-luna" and meta == {"session_id":"child","parent_session_id":"root","session_kind":"subagent","session_kind_evidence":"exact","agent_name":"Ada","agent_role":"explorer","agent_depth":1,"client_version":"0.116.0","capture_mode":"transcript"}

    def test_review_subagent_string_is_normalized(self,tmp_path):
        from ai_convos import cli
        session=tmp_path/".codex/sessions/review.jsonl"; session.parent.mkdir(parents=True); session.write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","payload":{"id":"review","cwd":"/repo","source":{"subagent":"review"}}},
            {"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"review"}]}},
            {"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}]))
        meta=json.loads(cli.parse_codex(tmp_path/".codex").convs[0]["metadata"]); assert meta["session_id"]=="review" and meta["session_kind"]=="subagent" and meta["agent_role"]=="review"

    def test_irrelevant_events_are_streamed_without_shifting_ids_or_timestamps(self,tmp_path):
        from ai_convos import cli
        session=tmp_path/".codex/sessions/stream.jsonl"; session.parent.mkdir(parents=True); session.write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"id":"stream"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"reasoning","summary":[],"content":"x"*10000}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:03Z","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}},
            {"type":"event_msg","timestamp":"2026-01-01T00:00:04Z","payload":{"type":"token_count","info":"ignored"}}]))
        result=cli.parse_codex(tmp_path/".codex"); assert [m["created_at"].second for m in result.msgs]==[1,3] and result.convs[0]["updated_at"].second==4
        assert [m["id"] for m in result.msgs]==[cli.gen_id("codex",f"{result.convs[0]['id']}:{i}") for i in (1,3)]

    def test_native_binding_survives_transcript_move(self,tmp_path):
        import duckdb
        from ai_convos import cli
        root=tmp_path/".codex"; a,b=root/"sessions/a.jsonl",root/"sessions/b.jsonl"; a.parent.mkdir(parents=True); a.write_text("\n".join(json.dumps(x) for x in [{"type":"session_meta","payload":{"id":"native","cwd":"/repo"}},{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}},{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}])); first=cli.parse_codex(root,files=[a]); db=duckdb.connect(); cli.init_schema(db); cli.upsert(db,first); b.write_text(a.read_text()); moved=cli.parse_codex(root,files=[b],bindings=cli.session_bindings(db)); assert ([c["id"] for c in moved.convs],[m["id"] for m in moved.msgs])==([c["id"] for c in first.convs],[m["id"] for m in first.msgs]); cli.upsert(db,moved); assert db.execute("SELECT (SELECT COUNT(*) FROM conversations),(SELECT COUNT(*) FROM messages)").fetchone()==(1,2)

    def test_legacy_filename_and_exact_native_ids_alias_one_conversation(self,tmp_path):
        import duckdb
        from ai_convos import cli
        root=tmp_path/".codex"; path=root/"sessions/rollout-native.jsonl"; path.parent.mkdir(parents=True); path.write_text("\n".join(map(json.dumps,[{"type":"session_meta","payload":{"id":"native"}},{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}]))); parsed=cli.parse_codex(root,files=[path]); cid=parsed.convs[0]["id"]; db=duckdb.connect(); cli.init_schema(db); db.execute("INSERT INTO provider_sessions VALUES ('codex','rollout-native',?)",[cid]); cli.upsert(db,parsed); assert set(db.execute("SELECT session_id,conversation_id FROM provider_sessions").fetchall())=={("rollout-native",cid),("native",cid)} and db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==1

    def test_same_batch_native_session_keeps_one_complete_transcript(self,tmp_path):
        import duckdb
        from ai_convos import cli
        root=tmp_path/".codex"; a,b=root/"sessions/a.jsonl",root/"sessions/b.jsonl"; a.parent.mkdir(parents=True); events=[{"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"id":"same","cwd":"/repo"}},{"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}},{"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}]; a.write_text("\n".join(map(json.dumps,events[:2]))); b.write_text("\n".join(map(json.dumps,events))); parsed=cli.parse_codex(root); assert len(parsed.convs)==2; db=duckdb.connect(); cli.init_schema(db); cli.upsert(db,parsed); assert db.execute("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM messages),(SELECT count(*) FROM provider_sessions)").fetchone()==(1,2,1)

    def test_same_batch_divergent_native_session_fails_before_mutation(self,tmp_path):
        import duckdb,pytest
        from ai_convos import cli
        root=tmp_path/".codex"; sessions=root/"sessions"; sessions.mkdir(parents=True); base=lambda text:[{"type":"session_meta","payload":{"id":"same","cwd":"/repo"}},{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":text}]}}]; (sessions/"a.jsonl").write_text("\n".join(map(json.dumps,base("alpha")))); (sessions/"b.jsonl").write_text("\n".join(map(json.dumps,base("beta")))); db=duckdb.connect(); cli.init_schema(db)
        with pytest.raises(ValueError,match="divergent provider session"): cli.upsert(db,cli.parse_codex(root))
        assert db.execute("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM messages),(SELECT count(*) FROM provider_sessions)").fetchone()==(0,0,0)

    def test_admission_quarantines_only_exact_main_startup_wrapper_after_parsing(self,tmp_path):
        import duckdb
        from ai_convos import cli
        root=tmp_path/".codex"; sessions=root/"sessions"; sessions.mkdir(parents=True); wrapper="# AGENTS.md instructions for /repo\n\n<INSTRUCTIONS>\nrules\n</INSTRUCTIONS>"; event=lambda sid,text:[{"type":"session_meta","payload":{"id":sid}},{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":text}]}}]; (sessions/"noise.jsonl").write_text("\n".join(map(json.dumps,event("noise",wrapper)))); (sessions/"prompt.jsonl").write_text("\n".join(map(json.dumps,event("prompt",wrapper+"\nactual request")))); parsed=cli.parse_codex(root); db=duckdb.connect(); cli.init_schema(db); cli.upsert(db,parsed); assert db.execute("SELECT (SELECT COUNT(*) FROM conversations),(SELECT COUNT(*) FROM messages),(SELECT COUNT(*) FROM provider_sessions)").fetchone()==(2,2,2); assert db.execute("SELECT json_extract_string(metadata,'$.capture_mode') FROM conversations WHERE json_extract_string(metadata,'$.session_id')='noise'").fetchone()[0]=="startup-stub-candidate" and db.execute("SELECT json_extract_string(metadata,'$.capture_mode') FROM conversations WHERE json_extract_string(metadata,'$.session_id')='prompt'").fetchone()[0]=="transcript"

    def test_upsert_provenance_targets_only_changed_rows(self):
        import duckdb
        from ai_convos import cli
        conv=dict(id="c",source="codex",title="t",created_at=None,updated_at=None,model=None,cwd="/repo",git_branch=None,project_id=None,metadata="{}"); msg=dict(id="m",conversation_id="c",role="assistant",content="done",thinking=None,created_at=None,model=None,metadata="{}",parent_id=None); edit=dict(id="e",message_id="m",file_path="x.py",edit_type="write",content="x",created_at=None,old_content=None); result=cli.ParseResult([conv],[msg],edits=[edit],edit_evidence=[dict(file_edit_id="e",status="confirmed",reason="test_fixture",tool_call_id=None)]); db=duckdb.connect(); cli.init_schema(db)
        cli.upsert(db,result); assert result.provenance_conversations=={"c"} and result.provenance_edits=={"e"}; cli.upsert(db,result); assert not result.provenance_conversations and not result.provenance_edits

    def test_upsert_reclassifies_historical_edit_without_deleting_raw_row(self):
        import duckdb
        from ai_convos import cli
        db=duckdb.connect(); cli.init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}'); INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','assistant','{}'); INSERT INTO tool_calls(id,message_id,input,output) VALUES ('t','m','{}','{}'); INSERT INTO file_edits VALUES ('e','m','x.py','write','x',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('e','legacy_unverified','source_unavailable',NULL)"); before=cli.archive_state(db)[1]; result=cli.ParseResult(edit_evidence=[dict(file_edit_id="e",status="invalid",reason="provider_failure",tool_call_id="t")]); cli.upsert(db,result)
        assert db.execute("SELECT COUNT(*) FROM file_edits WHERE id='e'").fetchone()[0]==1 and db.execute("SELECT status,reason,tool_call_id FROM provenance.file_edit_evidence WHERE file_edit_id='e'").fetchone()==("invalid","provider_failure","t") and cli.archive_state(db)[1]>before
        generation=cli.archive_state(db)[1]; cli.upsert(db,result); assert cli.archive_state(db)[1]==generation

    def test_input_images_become_bounded_durable_attachments(self,tmp_path,monkeypatch):
        import ai_convos.cli as cli
        raw=b"\x89PNG\r\n\x1a\ncapture"; monkeypatch.setattr(cli,"DATA_DIR",tmp_path/"archive"); monkeypatch.setattr(cli,"ATTACHMENT_LIMIT",len(raw)); sessions=tmp_path/".codex/sessions"; sessions.mkdir(parents=True); session=sessions/"image.jsonl"
        session.write_text(json.dumps({"type":"response_item","timestamp":"2026-01-01T00:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,"+__import__("base64").b64encode(raw).decode()},{"type":"input_image","image_url":"data:image/png;base64,"+__import__("base64").b64encode(raw+b"x").decode()}]}}))
        result=cli.parse_codex(tmp_path/".codex"); assert len(result.msgs)==1 and result.msgs[0]["content"]=="" and len(result.attachs)==2
        saved,large=result.attachs; path=Path(saved["path"]); assert (saved["message_id"],saved["filename"],saved["mime_type"],saved["size"],path.read_bytes())==(result.msgs[0]["id"],"image-1.png","image/png",len(raw),raw)
        assert path.parent==tmp_path/"archive/attachments" and path.stat().st_mode&0o777==0o600 and path.parent.stat().st_mode&0o777==0o700 and large["path"] is None and large["size"]==len(raw)+1 and not saved["url"]
        assert len(cli.hook_result("codex",session).attachs)==2 and len(list(path.parent.iterdir()))==1

    def test_parse_function_calls(self, tmp_path):
        """Parse Codex session with function calls."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions"
        sessions_dir.mkdir(parents=True)

        jsonl = sessions_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:01Z", "payload": {"type": "function_call", "name": "shell", "arguments": {"command": "ls -la"}}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:02Z", "payload": {"type": "message", "role": "assistant", "content": [{"type": "text", "text": "Done"}]}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        assert len(result.tools) == 1
        assert result.tools[0]["tool_name"] == "shell"
        assert result.tools[0]["message_id"] in {m["id"] for m in result.msgs}  # leading call anchors forward to the first message

    def test_parse_empty_function_call_arguments(self, tmp_path):
        from ai_convos.cli import parse_codex
        sessions = tmp_path / ".codex" / "sessions"; sessions.mkdir(parents=True)
        (sessions / "session.jsonl").write_text("\n".join([
            json.dumps({"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "list"}]}}),
            json.dumps({"type": "response_item", "payload": {"type": "function_call", "name": "list_dir", "arguments": "{}"}})]))
        assert parse_codex(tmp_path / ".codex").tools[0]["input"] == "{}"

    def test_function_calls_anchor_to_preceding_message(self, tmp_path):
        """Tools and shell edits attach to the nearest preceding message, never to a phantom id."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions"
        sessions_dir.mkdir(parents=True)

        jsonl = sessions_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:01Z", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "fix it"}]}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:02Z", "payload": {"type": "function_call", "name": "shell", "call_id": "c1", "arguments": {"command": "echo x > /out.txt"}}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:03Z", "payload": {"type": "function_call_output", "call_id": "c1", "output": "ok"}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:04Z", "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        user_id = next(m["id"] for m in result.msgs if m["role"] == "user")
        assert len(result.tools) == 1 and result.tools[0]["status"] == "complete" and json.loads(result.tools[0]["output"]) == "ok"
        assert all(t["message_id"] == user_id for t in result.tools)
        assert len(result.edits) == 1  # redirect target is exact; content stays the command (unknown effect)
        assert result.edits[0]["file_path"] == "/out.txt"
        assert result.edits[0]["edit_type"] == "shell"
        assert result.edits[0]["old_content"] is None

    def test_heredoc_write_edits(self, tmp_path):
        """cat > file <<EOF heredocs yield write edits with the exact full content."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions"
        sessions_dir.mkdir(parents=True)

        cmd = "cat > src/x.py <<'EOF'\nprint('a')\nprint('b')\nEOF"
        jsonl = sessions_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {"cwd": "/repo"}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:01Z", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "write it"}]}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:02Z", "payload": {"type": "function_call", "name": "exec_command", "call_id":"write", "arguments": json.dumps({"cmd": cmd})}}),
            json.dumps({"type":"response_item","timestamp":"2024-01-01T00:00:03Z","payload":{"type":"function_call_output","call_id":"write","output":"Process exited with code 0"}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:04Z", "payload": {"type": "function_call", "name": "exec_command", "arguments": json.dumps({"cmd": "python3 - <<'PY'\nprint(1)\nPY"})}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        assert len(result.edits) == 1  # interpreter heredocs (no redirect) are not edits
        assert result.edits[0]["file_path"] == "/repo/src/x.py"
        assert result.edits[0]["edit_type"] == "write"
        assert result.edits[0]["content"] == "print('a')\nprint('b')"

    def test_apply_patch_edits(self, tmp_path):
        """exec_command apply_patch heredocs yield exact per-hunk edits with before/after text."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions"
        sessions_dir.mkdir(parents=True)

        patch = ("apply_patch <<'PATCH'\n*** Begin Patch\n"
                 "*** Update File: src/app.py\n@@\n ctx\n-old line\n+new line\n@@\n+pure add\n"
                 "*** Add File: docs/new.md\n+hello\n+world\n*** End of File\n"
                 "*** End Patch\nPATCH")
        jsonl = sessions_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {"cwd": "/repo"}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:01Z", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "patch it"}]}}),
            json.dumps({"type": "response_item", "timestamp": "2024-01-01T00:00:02Z", "payload": {"type": "function_call", "name": "exec_command", "call_id":"patch", "arguments": json.dumps({"cmd": patch, "workdir": "/repo"})}}),
            json.dumps({"type":"response_item","timestamp":"2024-01-01T00:00:03Z","payload":{"type":"function_call_output","call_id":"patch","output":"Exit code: 0"}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        e = result.edits
        assert [(x["file_path"], x["edit_type"]) for x in e] == \
               [("/repo/src/app.py", "edit"), ("/repo/src/app.py", "edit"), ("/repo/docs/new.md", "write")]
        assert e[0]["old_content"] == "ctx\nold line" and e[0]["content"] == "ctx\nnew line"
        assert e[1]["old_content"] is None and e[1]["content"] == "pure add"  # insert-only hunk: no anchor
        assert e[2]["old_content"] is None and e[2]["content"] == "hello\nworld"
        assert all(x["message_id"] == result.msgs[0]["id"] for x in e)

    def test_custom_exec_tools_and_apply_patch_edits(self, tmp_path):
        from ai_convos.cli import parse_codex
        sessions = tmp_path/".codex"/"sessions"; sessions.mkdir(parents=True); patch = "*** Begin Patch\n*** Update File: src/app.py\n@@\n-old\n+new\n*** End Patch"
        code = rf'const unrelated = "\x1b"; const patch = {json.dumps(patch)}; text(await tools.apply_patch(patch));'
        (sessions/"session.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fix"}]}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"custom_tool_call","name":"exec","call_id":"c1","status":"completed","input":code}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:03Z","payload":{"type":"custom_tool_call_output","call_id":"c1","output":[{"type":"input_text","text":"Script completed\nWall time 0.0 seconds\nOutput:\n"},{"type":"input_text","text":"{}"}]}}]))
        result = parse_codex(tmp_path/".codex"); assert len(result.tools) == 1 and result.tools[0]["tool_name"] == "exec" and result.tools[0]["status"] == "complete" and json.loads(result.tools[0]["input"])["code"] == code and "Script completed" in result.tools[0]["output"]
        assert len(result.edits) == 1 and (result.edits[0]["file_path"], result.edits[0]["old_content"], result.edits[0]["content"]) == ("/repo/src/app.py", "old", "new")

    def test_failed_missing_and_timed_out_function_edits_are_not_facts(self,tmp_path):
        from ai_convos.cli import parse_codex
        sessions=tmp_path/".codex/sessions"; sessions.mkdir(parents=True); call=lambda cid,path:{"type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":cid,"arguments":json.dumps({"cmd":f"echo x > {path}"})}}; output=lambda cid,text:{"type":"response_item","payload":{"type":"function_call_output","call_id":cid,"output":text}}; events=[{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"try"}]}},call("failed","a.py"),output("failed","Process exited with code 2"),call("negative","b.py"),output("negative",'{"exit_code":-1}'),call("rejected","c.py"),output("rejected","Rejected by user"),call("running","d.py"),output("running","Process running with session ID 42"),call("missing","e.py")]; (sessions/"s.jsonl").write_text("\n".join(map(json.dumps,events))); result=parse_codex(tmp_path/".codex")
        assert len(result.tools)==5 and {t["status"] for t in result.tools}=={"failed","complete","pending"} and result.edits==[]
        assert [(v["status"],v["reason"]) for v in result.edit_evidence]==[("invalid","provider_failure")]*3+[("unknown","nonterminal_result"),("unknown","result_missing")]

    def test_direct_custom_apply_patch_captures_raw_patch_without_scanning_its_code(self, tmp_path, capsys):
        from ai_convos.cli import parse_codex
        sessions = tmp_path/".codex"/"sessions"; sessions.mkdir(parents=True); patch = "*** Begin Patch\n*** Update File: src/app.py\n@@\n-old\n+new\n*** Update File: tests/test_app.py\n@@\n-code = r'await tools.apply_patch(\"\\s\")'\n+code = \"safe\"\n*** End Patch"
        (sessions/"session.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"fix both"}]}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"custom_tool_call","name":"apply_patch","call_id":"c1","input":patch}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:03Z","payload":{"type":"custom_tool_call_output","call_id":"c1","output":"Exit code: 0\nWall time: 0 seconds\nOutput:\nSuccess. Updated"}}]))
        result = parse_codex(tmp_path/".codex")
        assert [(e["file_path"],e["old_content"],e["content"]) for e in result.edits] == [("/repo/src/app.py","old","new"),("/repo/tests/test_app.py",'code = r\'await tools.apply_patch("\\s")\'','code = "safe"')]
        assert {v["status"] for v in result.edit_evidence}=={"confirmed"} and {v["tool_call_id"] for v in result.edit_evidence}=={result.tools[0]["id"]}
        assert len(result.tools) == 1 and result.tools[0]["tool_name"] == "apply_patch" and not capsys.readouterr().err

    def test_custom_exec_patch_text_is_not_an_edit(self, tmp_path):
        from ai_convos.cli import parse_codex
        sessions = tmp_path/".codex"/"sessions"; sessions.mkdir(parents=True)
        code = 'const sample = "await tools.apply_patch(patch)"; text(sample);'
        (sessions/"session.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"session_meta","timestamp":"2026-01-01T00:00:00Z","payload":{"cwd":"/repo"}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect"}]}},
            {"type":"response_item","timestamp":"2026-01-01T00:00:02Z","payload":{"type":"custom_tool_call","name":"exec","call_id":"c1","input":code}}]))
        result = parse_codex(tmp_path/".codex"); assert len(result.tools) == 1 and result.edits == []

    def test_failed_custom_edits_do_not_create_file_edits_or_parse_noise(self, tmp_path, capsys):
        from ai_convos.cli import parse_codex
        sessions = tmp_path/".codex"/"sessions"; sessions.mkdir(parents=True)
        malformed = r'const patch = "*** Begin Patch\n*** Update File: x.py\n\-old\n+new\n*** End Patch"; await tools.apply_patch(patch);'; patch = "*** Begin Patch\n*** Update File: y.py\n@@\n-old\n+new\n*** End Patch"
        (sessions/"session.jsonl").write_text("\n".join(json.dumps(x) for x in [
            {"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"keep me"}]}},
            {"type":"response_item","payload":{"type":"custom_tool_call","name":"exec","call_id":"c1","input":malformed}},
            {"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"c1","output":[{"type":"input_text","text":"Script failed"},{"type":"input_text","text":"apply_patch verification failed"}]}},
            {"type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","call_id":"c2","input":patch}},
            {"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"c2","output":"Exit code: 2\ninvalid patch input"}}]))
        result = parse_codex(tmp_path/".codex")
        assert [m["content"] for m in result.msgs] == ["keep me"] and len(result.tools) == 2 and {t["status"] for t in result.tools} == {"failed"} and result.edits == []
        assert not capsys.readouterr().err

    def test_preserve_system_messages(self, tmp_path):
        """System and developer evidence is preserved for query-time filtering."""
        from ai_convos.cli import parse_codex

        sessions_dir = tmp_path / ".codex" / "sessions"
        sessions_dir.mkdir(parents=True)

        jsonl = sessions_dir / "session.jsonl"
        jsonl.write_text("\n".join([
            json.dumps({"type": "session_meta", "timestamp": "2024-01-01T00:00:00Z", "payload": {}}),
            json.dumps({"type": "response_item", "payload": {"type": "message", "role": "developer", "content": [{"type": "text", "text": "System prompt"}]}}),
            json.dumps({"type": "response_item", "payload": {"type": "message", "role": "system", "content": [{"type": "text", "text": "Instructions"}]}}),
            json.dumps({"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Hello"}]}}),
        ]))

        result = parse_codex(tmp_path / ".codex")

        assert [m["role"] for m in result.msgs] == ["developer","system","user"]


# ---- ChatGPT Export Parser Tests ----

class TestChatGPTExportParser:
    """Tests for parsing ChatGPT export files."""

    def test_parse_json_export(self, tmp_path):
        """Parse ChatGPT JSON export."""
        from ai_convos.cli import parse_chatgpt

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "id": "conv-123",
            "title": "Test Chat",
            "create_time": 1704067200,
            "update_time": 1704067200,
            "mapping": {
                "node1": {"message": {"author": {"role": "user"}, "content": {"parts": ["Hello"]}, "create_time": 1704067200}},
                "node2": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["Hi!"]}, "create_time": 1704067201}},
            }
        }]))

        result = parse_chatgpt(export)

        assert len(result.convs) == 1
        assert result.convs[0]["title"] == "Test Chat"
        assert len(result.msgs) == 2

    def test_parse_zip_export(self, tmp_path):
        """Parse ChatGPT ZIP export."""
        from ai_convos.cli import parse_chatgpt

        export_data = json.dumps([{
            "id": "conv-456",
            "title": "Zipped Chat",
            "mapping": {
                "n1": {"message": {"author": {"role": "user"}, "content": {"parts": ["Test"]}}}
            }
        }])

        zip_path = tmp_path / "export.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("conversations.json", export_data)

        result = parse_chatgpt(zip_path)

        assert len(result.convs) == 1
        assert result.convs[0]["title"] == "Zipped Chat"

    def test_parse_with_attachments(self, tmp_path):
        """Parse ChatGPT export with image attachments."""
        from ai_convos.cli import parse_chatgpt

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "id": "conv-789",
            "mapping": {
                "n1": {"message": {
                    "author": {"role": "user"},
                    "content": {"parts": [
                        {"content_type": "image_asset_pointer", "asset_pointer": "file://image.png", "name": "screenshot.png"}
                    ]}
                }}
            }
        }]))

        result = parse_chatgpt(export)

        assert len(result.attachs) == 1
        assert result.attachs[0]["filename"] == "screenshot.png"

    def test_parse_with_gizmo(self, tmp_path):
        """Parse ChatGPT export with custom GPT (gizmo)."""
        from ai_convos.cli import parse_chatgpt

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "id": "conv-gizmo",
            "gizmo_id": "g-abc123",
            "mapping": {}
        }]))

        result = parse_chatgpt(export)

        assert result.convs[0]["project_id"] == "g-abc123"

    def test_tool_only_node_kept_and_threaded(self, tmp_path):
        """Tool-role nodes without text parts still produce a message row (tool_calls reference it);
        parent_id walks the mapping tree past message-less roots."""
        from ai_convos.cli import parse_chatgpt

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "id": "conv-tool",
            "mapping": {
                "root": {"message": None, "parent": None},
                "n1": {"message": {"author": {"role": "user"}, "content": {"parts": ["search the web"]}}, "parent": "root"},
                "n2": {"message": {"author": {"role": "tool"}, "content": {"content_type": "execution_output", "text": "raw output"}}, "parent": "n1"},
                "n3": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["Found it."]}}, "parent": "n2"},
            }
        }]))

        result = parse_chatgpt(export)
        by_id = {m["id"]: m for m in result.msgs}
        assert len(result.msgs) == 3  # tool node kept despite empty text
        assert len(result.tools) == 1
        assert result.tools[0]["message_id"] in by_id  # no orphan
        tool_msg = by_id[result.tools[0]["message_id"]]
        assert tool_msg["role"] == "tool" and tool_msg["content"] == "raw output"
        user_msg = next(m for m in result.msgs if m["content"] == "search the web")
        assert user_msg["parent_id"] is None  # walks past the message-less root
        assert tool_msg["parent_id"] == user_msg["id"]
        assert next(m for m in result.msgs if m["content"] == "Found it.")["parent_id"] == tool_msg["id"]

    def test_tree_order_is_explicit_when_timestamps_tie(self,tmp_path):
        from ai_convos.cli import parse_chatgpt
        export=tmp_path/"conversations.json"; same=100; export.write_text(json.dumps([{"id":"ordered","mapping":{"root":{"message":None,"parent":None,"children":["user"]},"final":{"message":{"author":{"role":"assistant"},"content":{"parts":["verdict"]},"create_time":same},"parent":"tool","children":[]},"tool":{"message":{"author":{"role":"tool"},"content":{},"create_time":same},"parent":"user","children":["final"]},"user":{"message":{"author":{"role":"user"},"content":{"parts":["question"]},"create_time":same},"parent":"root","children":["tool"]}}}]))
        result=parse_chatgpt(export); assert [(m["role"],json.loads(m["metadata"])["provider_index"]) for m in result.msgs]==[("user",1),("tool",2),("assistant",3)] and result.msgs[-1]["content"]=="verdict"

    def test_iso_timestamps(self, tmp_path):
        """create_time as ISO string (web list api format) parses instead of becoming NULL."""
        from ai_convos.cli import parse_chatgpt, ts_any

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "id": "conv-iso", "create_time": "2024-03-01T12:00:00.000000+00:00", "update_time": 1709294400,
            "mapping": {"n1": {"message": {"author": {"role": "user"}, "content": {"parts": ["hi"]}, "create_time": 1709294400}}}
        }]))

        conv = parse_chatgpt(export).convs[0]
        assert conv["created_at"] is not None and conv["created_at"].year == 2024
        assert conv["updated_at"] is not None  # epoch still works
        assert ts_any(None) is None and ts_any("") is None


# ---- Claude Export Parser Tests ----

class TestClaudeExportParser:
    """Tests for parsing Claude.ai export files."""

    def test_parse_basic_export(self, tmp_path):
        """Parse Claude JSON export."""
        from ai_convos.cli import parse_claude

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "uuid": "conv-123",
            "name": "Test Chat",
            "created_at": "2024-01-01T00:00:00Z",
            "chat_messages": [
                {"uuid": "msg-1", "sender": "human", "text": "Hello"},
                {"uuid": "msg-2", "sender": "assistant", "model":"claude-opus-4-8", "text": "Hi there!"},
            ]
        }]))

        result = parse_claude(export)

        assert len(result.convs) == 1
        assert result.convs[0]["title"] == "Test Chat"
        assert len(result.msgs) == 2
        assert [m["role"] for m in result.msgs] == ["user","assistant"]
        assert result.msgs[1]["model"] == "claude-opus-4-8"
        assert json.loads(result.convs[0]["metadata"]) == {"session_id":"conv-123","session_kind":"main","session_kind_evidence":"exact","capture_mode":"export"}

    def test_parse_with_attachments(self, tmp_path):
        """Parse Claude export with attachments."""
        from ai_convos.cli import parse_claude

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "uuid": "conv-456",
            "chat_messages": [
                {
                    "uuid": "msg-1",
                    "sender": "human",
                    "text": "Here's a file",
                    "attachments": [
                        {"file_name": "doc.pdf", "file_type": "application/pdf", "file_size": 1024}
                    ]
                }
            ]
        }]))

        result = parse_claude(export)

        assert len(result.attachs) == 1
        assert result.attachs[0]["filename"] == "doc.pdf"

    def test_attachment_and_tool_only_turns_keep_parent_messages(self,tmp_path):
        from ai_convos.cli import parse_claude
        export=tmp_path/"claude.json"; export.write_text(json.dumps([{"uuid":"c","chat_messages":[{"uuid":"a","sender":"human","content":[],"attachments":[{"file_name":"x.pdf","file_type":"application/pdf"}]},{"uuid":"t","sender":"assistant","content":[{"type":"tool_use","id":"call","name":"lookup","input":{"q":"x"}}]},{"uuid":"r","sender":"human","content":[{"type":"tool_result","tool_use_id":"call","content":"found"}]}]}])); result=parse_claude(export); mids={m["id"] for m in result.msgs}
        assert len(result.msgs)==3 and len(result.attachs)==len(result.tools)==1 and result.attachs[0]["message_id"] in mids and result.tools[0]["message_id"] in mids and result.tools[0]["status"]=="complete" and json.loads(result.tools[0]["output"])=="found"

    def test_parse_content_blocks(self, tmp_path):
        """Parse Claude export with content blocks format."""
        from ai_convos.cli import parse_claude

        export = tmp_path / "conversations.json"
        export.write_text(json.dumps([{
            "uuid": "conv-789",
            "chat_messages": [
                {
                    "uuid": "msg-1",
                    "sender": "assistant",
                    "content": [
                        {"type": "text", "text": "Part 1"},
                        {"type": "text", "text": "Part 2"},
                    ]
                }
            ]
        }]))

        result = parse_claude(export)

        # Content blocks should be joined
        assert "Part 1" in result.msgs[0]["content"]


# ---- ID Generation Tests ----

class TestIDGeneration:
    """Tests for consistent ID generation."""

    def test_id_length(self):
        """Generated IDs are 16 characters."""
        from ai_convos.cli import gen_id
        assert len(gen_id("test", "123")) == 16

    def test_id_hex(self):
        """Generated IDs are valid hex."""
        from ai_convos.cli import gen_id
        id_ = gen_id("test", "123")
        int(id_, 16)  # should not raise

    def test_id_consistent_across_syncs(self):
        """Same file path produces same conversation ID."""
        from ai_convos.cli import gen_id

        # Simulating multiple syncs of same session
        path = "/Users/test/.claude/projects/-test/session-abc.jsonl"
        id1 = gen_id("claude-code", path)
        id2 = gen_id("claude-code", path)
        assert id1 == id2


def test_parse_failures_are_visible(capsys):
    from ai_convos.cli import safe_parse
    assert safe_parse("broken fixture", lambda: 1/0) is None
    assert "parse error (broken fixture): ZeroDivisionError" in capsys.readouterr().err


def test_latest_mtime_includes_export_formats(tmp_path):
    from ai_convos.cli import latest_mtime
    (tmp_path / "export.json").write_text("[]"); (tmp_path/"gone.json").symlink_to(tmp_path/"missing.json")
    assert latest_mtime(tmp_path) == (tmp_path / "export.json").stat().st_mtime


# ---- Timestamp Parsing Tests ----

class TestTimestampParsing:
    """Tests for timestamp parsing utilities."""

    def test_epoch_to_datetime(self):
        """Parse Unix epoch timestamp."""
        from ai_convos.cli import ts_from_epoch
        dt = ts_from_epoch(1704067200)  # 2024-01-01 00:00:00 UTC
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_epoch_none(self):
        """None epoch returns None."""
        from ai_convos.cli import ts_from_epoch
        assert ts_from_epoch(None) is None

    def test_iso_to_datetime(self):
        """Parse ISO format timestamp."""
        from ai_convos.cli import ts_from_iso
        dt = ts_from_iso("2024-01-01T12:00:00Z")
        assert dt.year == 2024
        assert dt.hour == 12

    def test_iso_none(self):
        """None ISO returns None."""
        from ai_convos.cli import ts_from_iso
        assert ts_from_iso(None) is None


class TestInstallSkills:
    def test_writes_bundled_skill_to_both_targets(self, tmp_path, monkeypatch):
        """install_skills resolves the bundled SKILL.md and installs it to codex + claude dirs."""
        from ai_convos.cli import install_skills
        src = Path(__file__).resolve().parents[1] / "skills" / "convos" / "SKILL.md"
        assert src.exists(), "bundled skill missing from repo source tree"
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setenv("CODEX_HOME", str(tmp_path / ".codex"))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "custom-claude"))
        install_skills()
        dests = [tmp_path / ".codex" / "skills" / "convos" / "SKILL.md",
                 tmp_path / "custom-claude" / "skills" / "convos" / "SKILL.md"]
        assert all(d.read_text() == src.read_text() for d in dests)
