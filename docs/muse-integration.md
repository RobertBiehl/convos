---
summary: "Muse integration: local session files, JSONL event format, and Stop-hook capture."
read_when:
  - Syncing Muse sessions
  - Understanding Muse session log format
  - Installing the Muse Stop hook
  - Debugging Muse sync issues
---

# Muse Integration

Parses local Muse session files from `~/.local/share/muse/sessions/`
(`MUSE_HOME` overrides the store root). Canonical field semantics and
subagent relationships are defined in
[Provider conversation contract](provider-conversation-contract.md).

Native-session sync checkpoints only successfully parsed files. Parser failures
remain eligible for the next sync even when the transcript mtime has not changed;
valid empty sessions are accounted for without creating a conversation.

## Session Files

Muse stores one event log per session, sharded by local date, plus one log
per delegated subagent session:

```
~/.local/share/muse/sessions/
└── 2026/
    └── 09/
        └── 20/
            └── <session-id>/
                ├── session.jsonl
                └── subagent/
                    └── <child-session-id>/
                        └── session.jsonl
```

Each `session.jsonl` line is an event-log envelope (`schema_version`, `id`,
`stream`, `sequence`, `recorded_at` microseconds, `payload_type`, `payload`).
The first line may be a `retained_frame` wrapper whose `children` carry
`record_json` envelopes; the parser unpacks those. Full tool outputs that
overflow the log live under `tool-outputs/` and are not parsed (results join
from `tool_result_batch_committed` instead).

## Extracted Data

User prompts come from run `started` events; every provider commit becomes
one stable assistant message keyed by its committed `message_id`: one per
tool-call batch, one per `assistant_message_committed` text, and one per
`reasoning_summary_committed` plaintext summary (which carries the thinking;
`response_id` is association only, never identity). Raw
`reasoning_committed` blobs are encrypted and never stored. Because identity
never depends on transcript completeness, re-syncing a grown transcript adds
rows without duplicating or moving earlier ones. Tool calls come from
`assistant_tool_calls_committed` and join outputs from
`tool_result_batch_committed`; a typed failed `tool_batch.effect.terminal`
outcome marks the call failed, and calls with no result yet stay pending.
The conversation model is the first `model_completed` model, and each
assistant message records the model active at its sequence. Conversation
bounds span all decoded envelopes, and per-message `provider_index` is the
zero-based order of envelope sequences (native `sequence` is ordering only).

### Conversations

- `id`: Opaque stable physical ID; native identity is `source` plus
  `metadata.session_id` (the stream session UUID)
- `metadata.session_id`: Envelope stream ID, or the child directory name for
  a `subagent/` log
- `metadata.parent_session_id`: Enclosing session directory for a subagent log
- `metadata.session_kind`: `main` or `subagent`
- `source`: `muse`
- `title`: First user prompt clipped to 80 chars, else `muse (<id prefix>)`
- `cwd`: `route_facts` working directory; `git_branch` from the workspace
  branch observation

### Messages and Tool Calls

- User message per run `started` prompt, keyed by the stable run ID
- One assistant message per provider commit, keyed by its committed
  `message_id`: tool-call batches, committed texts, and reasoning summaries
  each keep a stable row (in-progress turns archive batches and summaries;
  completed text arrives as its own row, never merged into an earlier one)
- `thinking` lives on the summary's own row; per-message `model` is
  the latest completed model at that sequence
- Tool `input` is the provider args JSON, `output` the committed result text

### File edits

`write_file` (`{path, content}`) maps to a `write` edit holding the full new
content; `edit_file` (`{path, find, replace}`) maps to an `edit` holding the
replacement as content and the matched text as old content. A typed failed
terminal outcome records `invalid` evidence first; otherwise rows are emitted
only with positive success evidence in the committed result (`wrote ` for
writes, `edited` for edits, exactly as observed) as
`confirmed`/`provider_success`. Calls with no result record `unknown`/
`result_missing`, and results with unrecognized text record `unknown`/
`unconfirmed_result`, both without rows — result arrival alone never confirms
a mutation.

### Not captured in v1

- Attachments: the wire schema defines image-attachment metadata, but no
  session-log sample has been observed yet, so no `attachments` rows are
  emitted (user text is still archived).
- `import` paths must point at a session directory or rely on sync; a bare
  store root is only picked up by `convos sync`.

## Live Capture via Stop Hook

Muse has no hand-editable hooks file; hooks are native-plugin capabilities
(`.muse-plugin/plugin.json`, installed with `muse plugins install` plus
`muse plugins approve`, currently behind `MUSE_EXPERIMENTAL_PLUGINS=1`).
The wire contract is Claude Code's hook schema: snake_case JSON on stdin
(`hook_event_name`, `session_id`, `cwd`, `permission_mode`; Muse sends
`transcript_path: null`, so convos resolves the session ID to its
`session.jsonl` under the store). Register a `Stop` hook (plus
`SubagentStop` for subagent sessions) running `convos capture muse`:

```json
{
  "schemaVersion": 1,
  "name": "convos",
  "displayName": "Convos",
  "version": "0.1.0",
  "description": "Archive Muse sessions to Convos on stop.",
  "compat": {"source": "native", "manifestDir": ".muse-plugin"},
  "capabilities": {
    "skills": [],
    "commands": [],
    "hooks": [
      {"id": "capture-stop", "event": "Stop", "command": ["/path/to/convos", "capture", "muse"], "timeoutMs": 5000, "statusMessage": "Saving conversation to Convos"},
      {"id": "capture-subagent-stop", "event": "SubagentStop", "command": ["/path/to/convos", "capture", "muse"], "timeoutMs": 5000, "statusMessage": "Saving conversation to Convos"}
    ],
    "mcpServers": [],
    "reminders": []
  }
}
```

```bash
MUSE_EXPERIMENTAL_PLUGINS=1 muse plugins install ./convos-plugin --scope user
MUSE_EXPERIMENTAL_PLUGINS=1 muse plugins approve convos
```

Use the absolute `convos` entrypoint path: hooks run with a cleared
environment, so non-default roots need a wrapper such as
`["sh", "-c", "CONVOS_PROJECT_ROOT='<root>' MUSE_HOME='<store>' exec '/path/to/convos' capture muse"]`.
Without `MUSE_HOME`, the hook searches the default store. Resolved hook
paths are canonicalized before the store-containment check, so a symlink
planted in the store cannot escape it. Unknown or malformed session IDs are
rejected before anything is queued.

## Usage

```bash
# Sync all Muse sessions (also picked up by plain `convos sync`)
uv run convos sync

# Sync only Muse
uv run convos sync --no-claude-code --no-codex --local-only

# Search Muse history
uv run convos search "auth bug" -s muse

# Inspect a session
uv run convos sql "SELECT tool_name, status, COUNT(*) FROM tool_calls GROUP BY tool_name, status" -f jsonl
```

## Troubleshooting

**Empty sync:**
- Check `~/.local/share/muse/sessions/` exists (or `MUSE_HOME` points at
  the store) and holds `session.jsonl` files.

**Missing assistant text:**
- Text commits when the turn completes; in-progress turns archive prompts,
  tool calls, and reasoning summaries, and the next sync fills in the text.

**Hook never fires:**
- The plugin must be both installed and approved; the management CLI still
  needs `MUSE_EXPERIMENTAL_PLUGINS=1` on current builds.
