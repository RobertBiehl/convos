"""Run a synthetic, isolated cross-agent recall demo against the installed CLI."""

import json
import os
import subprocess
import tempfile
from pathlib import Path


def main():
    with tempfile.TemporaryDirectory(prefix="convos-demo-") as directory:
        root = Path(directory)
        project = root / "sample-project"
        codex = root / "codex" / "sessions"
        claude = root / "claude" / "projects" / "sample-project"
        for path in (project, codex, claude): path.mkdir(parents=True)
        (project / "README.md").write_text("# Sample project\n\nA worker with a duplicate-job bug.\n")
        decision = "The first fix only deduped jobs in memory. Restart brought the bug back, so persist idempotency keys in SQLite."
        (claude / "decision.jsonl").write_text("\n".join(json.dumps(event) for event in [
            {"type": "system", "sessionId": "demo-claude-decision", "timestamp": "2026-01-10T09:00:00Z", "cwd": str(project)},
            {"type": "user", "sessionId": "demo-claude-decision", "uuid": "demo-question", "timestamp": "2026-01-10T09:00:01Z", "message": {"content": "Wasn't the duplicate-job bug already fixed?"}},
            {"type": "assistant", "sessionId": "demo-claude-decision", "uuid": "demo-decision", "parentUuid": "demo-question", "timestamp": "2026-01-10T09:00:02Z", "message": {"content": decision}},
        ]) + "\n")
        (codex / "implementation.jsonl").write_text("\n".join(json.dumps(event) for event in [
            {"type": "session_meta", "timestamp": "2026-01-11T10:00:00Z", "payload": {"id": "demo-codex-implementation", "cwd": str(project)}},
            {"type": "response_item", "timestamp": "2026-01-11T10:00:01Z", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Implement persisted idempotency keys for the duplicate-job bug after restart."}]}},
            {"type": "response_item", "timestamp": "2026-01-11T10:00:02Z", "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "I will persist idempotency keys in SQLite; the previous agent's in-memory fix disappears on restart."}]}},
        ]) + "\n")
        env = {**os.environ, "CONVOS_PROJECT_ROOT": str(root / "archive"), "CODEX_HOME": str(codex.parent), "CLAUDE_CONFIG_DIR": str(claude.parent.parent), "MUSE_HOME": str(root / "muse")}
        env["CONVOS_SEMANTIC"] = "0"

        def convos(*args):
            result = subprocess.run(["convos", *args], env=env, text=True, capture_output=True, check=False)
            if result.returncode: raise RuntimeError(f"convos {' '.join(args)} failed:\n{result.stdout}{result.stderr}")
            return result.stdout

        convos("init")
        convos("fts")
        question = "duplicate-job bug after restart"
        hits = json.loads(convos("query", question, "--cwd", str(project), "-f", "json"))
        assert {hit["source"] for hit in hits} == {"claude-code", "codex"}, hits
        decision_hit = next(hit for hit in hits if hit["source"] == "claude-code")
        turns = json.loads(convos("read", decision_hit["conversation_id"], "--around", decision_hit["message_id"], "-f", "json"))
        assert any(turn["content"] == decision for turn in turns), turns
        print("Question: Wasn't the duplicate-job bug fixed two agents ago?")
        print(f"convos query '{question}' -> " + ", ".join(hit["source"] for hit in hits))
        print(f"convos read {decision_hit['conversation_id']} --around {decision_hit['message_id']}")
        print("Source turn: " + decision)
        print("All records came from synthetic local Claude Code and Codex sessions; no web provider or remote service is configured.")


if __name__ == "__main__": main()
