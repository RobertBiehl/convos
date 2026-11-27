---
summary: "Antigravity integration: local session transcripts, SQLite metadata, Stop-hook capture, and skills."
read_when:
  - Syncing Antigravity CLI sessions
  - Understanding Antigravity transcript_full.jsonl format
  - Installing the Antigravity Stop hook or skills
  - Debugging Antigravity sync issues
---

# Antigravity Integration

Parses local Antigravity CLI (`agy`) sessions from
`~/.gemini/antigravity-cli/brain/` (`ANTIGRAVITY_APP_DATA_DIR` overrides the
app data root; `ANTIGRAVITY_CONFIG_DIR` overrides `~/.gemini/config` for
hooks and skills). Canonical field semantics and subagent relationships are
defined in [Provider conversation contract](provider-conversation-contract.md).

Native-session sync checkpoints only successfully parsed files. Parser failures
remain eligible for the next sync even when the transcript mtime has not
changed; valid empty sessions are accounted for without creating a
conversation.

## Session Files

Antigravity stores one transcript directory per conversation under `brain/`,
plus read-only session metadata in `conversation_summaries.db` (with fallbacks
in `cache/conversation_metadata.json` and `annotations/<conversation-id>.pbtxt`):

```
~/.gemini/antigravity-cli/
├── conversation_summaries.db
├── cache/
│   └── conversation_metadata.json
├── annotations/
│   └── <conversation-id>.pbtxt
└── brain/
    └── <conversation-id>/
        └── .system_generated/
            └── logs/
                ├── transcript_full.jsonl
                └── transcript.jsonl
```

`transcript_full.jsonl` holds the complete untruncated step log and is the
canonical input for `convos sync` and hook capture. `transcript.jsonl` is the
compact companion log (which may truncate long fields and double-encode JSON
string arguments); when passed directly via `import`, the parser decodes those
string arguments, and when passed via a hook, capture upgrades it to the
sibling `transcript_full.jsonl` when present.

Each JSONL line is a step object (`step_index`, `type`, `status`, `created_at`,
plus type-specific fields). Duplicate `step_index` lines are deduplicated by
keeping the latest step record for each index.

## Extracted Data

- `USER_INPUT` -> `user` message
- `PLANNER_RESPONSE` -> `assistant` message with `content`, `thinking`, exact
  `model`, and attached `tool_calls`
- `SYSTEM_MESSAGE` -> `system` message
- Tool results (`GENERIC` / `MODEL` steps immediately following a
  `PLANNER_RESPONSE`) pair positionally with `PLANNER_RESPONSE.tool_calls[j]`
  at `step_index = s + 1 + j`. Step `status` maps `DONE` -> `complete`,
  `ERROR` / `CANCELED` -> `failed`, else `pending`.

### Conversations

- `id`: Opaque stable physical ID; native identity is `source` (`antigravity`)
  plus `metadata.session_id` (the conversation UUID)
- `metadata.session_id`: Conversation UUID from the `brain/<uuid>` directory
  (or filename stem when imported standalone)
- `metadata.parent_session_id`: `parent_conversation_id` from
  `conversation_summaries.db` when non-empty
- `metadata.session_kind`: `subagent` when a parent conversation ID or positive
  `nesting_depth` is recorded, else `main`
- `metadata.session_kind_evidence`: `exact` when summary metadata confirms the
  record, else `inferred`
- `metadata.agent_name`, `metadata.agent_depth`, `metadata.originator`:
  populated sparsely when recorded
- `title`: Summary title from `conversation_summaries.db` / metadata JSON /
  annotation pbtxt, falling back to the first user message clipped to 80 chars,
  else `antigravity (<id prefix>)`
- `cwd`: First `file://` URI decoded from `workspace_uris`
- `project_id`: `project_id` from summary metadata when recorded
- `model`: First exact non-synthetic `PLANNER_RESPONSE.model`

### File edits

`write_file` / `write_to_file` (`{Path|TargetFile, Content|CodeContent}`) map
to a `write` edit holding the written content; `edit_file` /
`replace_file_content` (`{Path|TargetFile, Target|TargetContent,
Replacement|ReplacementContent}`) map to an `edit` holding replacement and old
content. A failed or canceled tool result records `invalid` evidence; rows are
emitted only when the paired result step reached `DONE` without an error field
(`confirmed`/`provider_success`). Calls with no result record
`unknown`/`result_missing`, and unfinished results record
`unknown`/`unconfirmed_result`, both without rows.

## Live Capture via Stop Hook

`convos init` and `convos install-hooks` install a named `Stop` hook in
`~/.gemini/config/hooks.json` (overridable via `ANTIGRAVITY_CONFIG_DIR`):

```json
{
  "convos-capture": {
    "Stop": [
      {
        "type": "command",
        "command": "/path/to/convos capture antigravity",
        "timeout": 5
      }
    ]
  }
}
```

`convos capture antigravity` reads Antigravity's camelCase hook payload on
stdin (`transcriptPath`, `conversationId`, `hookEventName`), upgrades
`transcript.jsonl` to its sibling `transcript_full.jsonl` when present,
canonicalizes and verifies containment under `brain/`, enqueues the transcript
non-blockingly, and prints `{}` on stdout so Antigravity allows stopping cleanly.

`convos install-skills` installs the bundled `convos` skill to
`~/.gemini/config/skills/convos/SKILL.md` (making `/convos` available across
Antigravity CLI, App, and IDE).

## Usage

```bash
# Install skills and Stop hook (also run by `convos init`)
convos install-skills
convos install-hooks

# Sync all local sources including Antigravity
convos sync --local-only

# Sync only Antigravity
convos sync --no-claude-code --no-codex --no-muse --local-only

# Search Antigravity history
convos search "orchestrate" -s antigravity
```

## Troubleshooting

**Empty sync:**
- Check `~/.gemini/antigravity-cli/brain/` exists (or `ANTIGRAVITY_APP_DATA_DIR`
  points at the app data dir) and contains `.system_generated/logs/transcript_full.jsonl`.

**Hook status reports 0 hooks:**
- Run `convos install-hooks --status` and repair with `convos install-hooks`.
  Existing user hooks in `~/.gemini/config/hooks.json` are preserved.
