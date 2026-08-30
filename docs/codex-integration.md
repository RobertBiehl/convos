---
summary: "Codex CLI integration: session files, JSONL format, tool calls, and file edits."
read_when:
  - Syncing Codex sessions
  - Understanding Codex log format
  - Debugging Codex sync issues
---

# Codex Integration

Parses local Codex CLI session files from `~/.codex/sessions/`.
Canonical field semantics and subagent relationships are defined in
[Provider conversation contract](provider-conversation-contract.md).

## Session Files

Codex stores sessions organized by date:

```
~/.codex/
└── sessions/
    └── 2024/
        └── 01/
            └── 16/
                ├── rollout-2024-01-16T00-01-52-uuid.jsonl
                └── rollout-2024-01-16T12-30-00-uuid.jsonl
```

## JSONL Format

Each line is a JSON object with `type` field.

### Session Metadata

First line contains session info:

```json
{
  "timestamp": "2024-01-16T00:01:52.679Z",
  "type": "session_meta",
  "payload": {
    "id": "uuid",
    "timestamp": "2024-01-16T00:01:52.655Z",
    "cwd": "/Users/name/project",
    "originator": "codex_cli_rs",
    "cli_version": "0.85.0",
    "model_provider": "openai"
  }
}
```

### Response Items

Messages and function calls:

**User message:**
```json
{
  "timestamp": "2024-01-16T00:01:53Z",
  "type": "response_item",
  "payload": {
    "type": "message",
    "role": "user",
    "content": [
      {"type": "input_text", "text": "Fix the bug"}
    ]
  }
}
```

**Assistant message:**
```json
{
  "type": "response_item",
  "payload": {
    "type": "message",
    "role": "assistant",
    "content": [
      {"type": "output_text", "text": "I'll fix that."}
    ]
  }
}
```

**Function call:**
```json
{
  "type": "response_item",
  "payload": {
    "type": "function_call",
    "name": "shell",
    "arguments": {
      "command": "cat file.py"
    }
  }
}
```

**Function output (legacy format):**
```json
{
  "type": "response_item",
  "payload": {
    "type": "function_call_output",
    "call_id": "call_xyz",
    "output": "file contents..."
  }
}
```

Current Codex versions emit custom tool calls. The `exec` input is JavaScript
which can call nested tools such as `apply_patch`; the matching output carries
the same `call_id`:

```json
{"type":"response_item","payload":{"type":"custom_tool_call","name":"exec","call_id":"call_xyz","status":"completed","input":"..."}}
{"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"call_xyz","output":"..."}}
```

## Extracted Data

### Conversations

- `id`: Opaque stable physical ID; native identity is `source` plus `metadata.session_id`
- `metadata.session_id`: Exact first `session_meta.payload.id`
- `metadata.session_kind`: `main` or `subagent`
- `metadata.parent_session_id`: Explicit spawned-parent thread ID
- `source`: `codex`
- `title`: Working directory or session stem
- `cwd`: From session_meta payload
- `git_branch`: From session metadata Git observation
- `model`: First exact assistant model from turn context
- `metadata`: Agent name/role/depth, originator, client version, capture mode,
  repository URL, commit, and documented provider extensions when recorded

### Messages

- User/assistant messages from `response_item` with `type: message`
- Developer and system messages are preserved for query-time filtering
- Content extracted from `input_text`, `output_text`, or `text` blocks
- `model` records the exact active turn model when available

### Attachments

- Inline `input_image` data URLs become attachment rows on their user message
- Bodies up to 32 MiB are decoded into private content-addressed files under
  `data/attachments/`; larger images retain metadata without a body
- Re-syncing is idempotent, and image-only user turns are retained
- A local path mentioned in text is not an attachment and is never read

### Tool Calls

Legacy function calls and current custom calls are mapped to `tool_calls`:
- `tool_name`: Function name (for example, `shell` or `exec`)
- `input`: Function arguments or custom tool code
- `output`: Function or custom tool output

### File Edits

Extracted from legacy shell commands and current custom `exec` calls:
- `apply_patch` calls produce exact per-hunk old/new content
- Heredoc redirects produce exact writes
- Plain redirects retain the modifying command when content is unknown

## Usage

```bash
# Sync all Codex sessions
uv run convos sync

# Import specific Codex directory
CONVOS_IMPORT_PATHS="~/.codex" uv run convos sync

# Search Codex sessions only
uv run convos search "python" -s codex

# List by source
uv run convos sql "SELECT id, title, created_at FROM conversations WHERE source='codex' ORDER BY created_at DESC LIMIT 20" -f json
```

## Differences from Claude Code

| Aspect | Claude Code | Codex |
|--------|-------------|-------|
| Provider | Anthropic | OpenAI |
| Event types | system, human, assistant | session_meta, response_item |
| Tool format | tool_use/tool_result blocks | function or custom tool call/output |
| Thinking | Explicit thinking blocks | Not available |
| File edits | Write/Edit tools | Shell commands and nested `apply_patch` calls |

## Troubleshooting

**No sessions found:**
- Check `~/.codex/sessions/` exists
- Codex may store sessions elsewhere on some systems

**Missing function outputs:**
- Some function calls may not have recorded output
- Status shows as "pending" for incomplete calls

**Duplicate sessions:**
- Re-syncing the same path updates existing records today
- Native session identity is retained in metadata and indexed independently of the
  stable physical ID
