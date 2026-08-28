# Provider conversation contract

Status: frozen for the 0.11 schema epoch, 2026-08-28

This contract is the normalized union of evidence available from Codex, Claude
Code, ChatGPT, and Claude. It distinguishes canonical fields from provider-only
metadata and never invents facts a source did not record.

## Identity and relationships

- `conversations.id` is the internal physical identifier. The identity migration
  derives it from author, source, and the exact provider session identity; a local
  transcript path is evidence location, not logical identity.
- `metadata.session_id` is the exact provider-native session identity. For a Claude
  Code subagent this is its `agentId`; the root `sessionId` becomes its parent.
- `metadata.parent_session_id` is present only when the provider explicitly names a
  parent session.
- `metadata.session_kind` is `main` or `subagent`. It is never inferred merely from
  a shared cwd or repository.
- Missing parents remain missing relationships. A child may retain its explicit
  parent identity even when the parent transcript is unavailable.

## Canonical conversation fields

| Field | Meaning |
| --- | --- |
| `source` | Provider/integration namespace: `codex`, `claude-code`, `chatgpt`, or `claude`. |
| `title` | Display label only; never identity. |
| `created_at`, `updated_at` | Earliest and latest recorded event timestamps. |
| `model` | First exact non-synthetic assistant model recorded for the session; NULL when unavailable. |
| `cwd` | Provider-recorded working directory; NULL when unavailable. |
| `git_branch` | Provider-recorded branch at capture; NULL when unavailable. |
| `project_id` | Provider-native project/gizmo identity when applicable. |
| `metadata` | Stable optional session fields below plus explicitly documented provider extensions. |

`messages.model` stores the exact model active for that message. A provider label
such as `openai` or `claude` is not a model name. Claude's legacy `human` role is
normalized to `user`; provider spellings may be retained in message metadata when
they carry additional meaning.

## Stable session metadata

| Key | Meaning |
| --- | --- |
| `session_id` | Exact provider-native identity for this main or subagent session. |
| `parent_session_id` | Explicit native parent identity. |
| `session_kind` | `main` or `subagent`. |
| `agent_id` | Provider-native subagent identity when distinct from the session identity. |
| `agent_name` | Provider-recorded agent name or nickname. |
| `agent_role` | Provider-recorded agent role/type. |
| `agent_depth` | Provider-recorded nesting depth. |
| `originator` | Client/entrypoint that created the session. |
| `client_version` | Provider client version that wrote the transcript. |
| `capture_mode` | Evidence source such as `transcript`, `history`, `api`, or `export`. |
| `git_repository` | Exact provider-recorded repository URL. |
| `git_commit` | Exact provider-recorded commit. |

Sparse metadata omits unavailable keys rather than storing guessed values or NULL
members. Codex currently also retains `forked_from_id` and `thread_source` as
documented provider extensions; they do not replace the explicit parent relation.

## Provider mapping

| Canonical evidence | Codex | Claude Code |
| --- | --- | --- |
| Session identity | first `session_meta.payload.id` | main `sessionId`; subagent `agentId` |
| Parent | `source.subagent.thread_spawn.parent_thread_id` | subagent root `sessionId` |
| Kind | presence of explicit `source.subagent` | `agentId`, `isSidechain`, or provider subagent path |
| Agent | spawn nickname, role, depth | agent ID/name/type/depth when recorded |
| cwd | first session metadata | system/event cwd |
| Git | branch, commit, repository URL | branch; other fields remain absent |
| Model | active `turn_context.payload.model` | assistant `message.model` |
| Client | originator and CLI version | entrypoint and version |

Repeated Codex `session_meta` records inside one rollout are forked context, not new
conversations. The first record names the rollout whose UUID is in the filename;
later parent metadata must not overwrite it.

## Messages, tools, attachments, and edits

- Message trees preserve explicit provider parent IDs; absence does not imply a
  linear conversation.
- System, developer, user, assistant, and tool evidence is parsed before retrieval
  filters are applied.
- One logical tool invocation retains its provider call identity, input, output,
  status, and timestamps. Results are joined to calls rather than counted as a
  second invocation.
- Attachments retain provider metadata and bounded local bodies without reading
  arbitrary paths mentioned in text.
- File edits retain the modifying turn, provider path, operation, exact captured
  before/after material, and evidence quality. No file-system state is fabricated
  for historical sessions.

## Admission and recovery

A coding-agent transcript is an admitted conversation only when it contains a real
user/delegation prompt. Injected instructions, environment context, and startup
metadata do not satisfy admission. Recovered prompt history is retained with
`capture_mode=history`; it is not represented as a complete transcript and cannot
prove that tools, edits, or assistant turns were absent.

## 2026-08-28 local evidence audit

- Codex: 1,112 files; 1,111 exact provider session IDs; 386 subagent files; all
  session metadata records had cwd; 1,216 recorded Git observations; 27,979 exact
  turn-model observations; 116 files had only injected/noise user blocks.
- Claude Code: 17 surviving transcript files with 17 distinct `sessionId` values;
  current samples recorded cwd, branch, and exact assistant models.
- Archive: all 1,109 Codex rows had cwd, while the 9,647 additional Claude Code rows
  were explicitly tagged `history.jsonl` recoveries created by the old reversible
  orphan-recovery script. They are not evidence of current-parser duplication.

This PR records and begins emitting the normalized contract without changing
physical conversation IDs. The dependent identity/migration PR performs dedupe,
admission cleanup, recovery classification, obsolete split-result cleanup, and
backed-up ID replacement together.
