---
summary: "Claude Code integration: local session files, JSONL format, and metadata."
read_when:
  - Syncing Claude Code sessions
  - Understanding Claude Code log format
  - Extracting thinking blocks and tool calls
  - Debugging Claude Code sync issues
---

# Claude Code Integration

Parses local Claude Code session files from `~/.claude/projects/`.
Canonical field semantics and subagent relationships are defined in
[Provider conversation contract](provider-conversation-contract.md).

## Session Files

Claude Code stores sessions as JSONL files:

```
~/.claude/projects/
├── -Users-name-project1/
│   ├── session-uuid-1.jsonl
│   └── session-uuid-2.jsonl
└── -Users-name-project2/
    └── session-uuid-3.jsonl
```

Directory names are path-encoded: `-Users-name-project` = `/Users/name/project`

## JSONL Format

Each line is a JSON object with `type` field.

### System Event

Session metadata at start:

```json
{
  "type": "system",
  "timestamp": "2024-01-01T00:00:00Z",
  "cwd": "/Users/name/project",
  "gitBranch": "main",
  "version": "2.1.9"
}
```

### Human Message

User input:

```json
{
  "type": "human",
  "timestamp": "2024-01-01T00:00:01Z",
  "message": {
    "content": "Fix the bug in auth.py"
  }
}
```

### Assistant Message

Claude response with possible thinking and tool use:

```json
{
  "type": "assistant",
  "timestamp": "2024-01-01T00:00:02Z",
  "message": {
    "content": [
      {
        "type": "thinking",
        "thinking": "Let me analyze the auth module..."
      },
      {
        "type": "text",
        "text": "I'll fix that bug."
      },
      {
        "type": "tool_use",
        "id": "tool-123",
        "name": "Read",
        "input": {"file_path": "/path/to/auth.py"}
      }
    ]
  }
}
```

### Tool Result

Result of tool execution:

```json
{
  "type": "assistant",
  "message": {
    "content": [
      {
        "type": "tool_result",
        "tool_use_id": "tool-123",
        "content": "file contents..."
      }
    ]
  }
}
```

## Extracted Data

### Conversations

- `id`: Internal physical ID (path-based until the 0.11 identity migration)
- `metadata.session_id`: Main `sessionId`, or `agentId` for a subagent
- `metadata.parent_session_id`: Root `sessionId` for a subagent
- `metadata.session_kind`: `main` or `subagent`
- `source`: `claude-code`
- `title`: Derived from directory name and session ID
- `cwd`: Working directory from system event
- `git_branch`: Git branch from system event
- `model`: First exact non-synthetic assistant model
- `metadata`: Agent, entrypoint, client version, and capture mode when recorded

### Messages

- User messages from `human` events
- Assistant messages from `assistant` events
- `thinking` column populated from thinking blocks
- Legacy `human` roles normalize to `user`
- Assistant `model` retains the exact provider model

### Tool Calls

Extracted from `tool_use` and `tool_result` blocks:
- `tool_name`: Read, Write, Edit, Bash, etc.
- `input`: Tool parameters
- `output`: Tool result

### File Edits

Special handling for file-modifying tools:
- `Write`: New file content
- `Edit`: String replacement
- `MultiEdit`: Multiple edits

## Usage

```bash
# Sync all Claude Code sessions
uv run convos sync

# Import specific project directory
CONVOS_IMPORT_PATHS="~/.claude/projects/-Users-name-project" uv run convos sync

# Search with thinking included
uv run convos search "auth bug" --thinking

# File edits touching a path
uv run convos sql "SELECT file_path, edit_type, created_at FROM file_edits WHERE file_path LIKE '%auth.py' ORDER BY created_at DESC" -f jsonl
```

## Claude Code Web

Claude Code web (claude.ai/code) stores sessions on Anthropic's servers, not locally.

**Current status:** No public API documented. Options:
1. Use `/teleport` command to pull sessions to local (requires Claude Code 2.1.0+)
2. Manual export from web UI

**Teleport (when available):**
```bash
claude --teleport           # interactive picker
claude --teleport <id>      # specific session
```

Teleported sessions appear in `~/.claude/projects/` and sync normally.

## Troubleshooting

**Empty sync:**
- Check `~/.claude/projects/` exists and has `.jsonl` files
- Sessions may be in different location on some systems

**Missing thinking:**
- Thinking blocks only present with extended thinking enabled
- Older sessions may not have thinking

**Duplicate sessions:**
- Re-syncing the same path updates existing records today
- Native session identity is retained in metadata and becomes physical identity in
  the 0.11 identity migration
