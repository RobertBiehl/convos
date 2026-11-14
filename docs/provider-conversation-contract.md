# Provider conversation contract

Status: frozen for the 0.11 schema epoch, 2026-08-28

This contract is the normalized union of evidence available from Codex, Claude
Code, ChatGPT, and Claude. It distinguishes canonical fields from provider-only
metadata and never invents facts a source did not record.

## Identity and relationships

- `conversations.id` is an opaque internal physical identifier. Once published in
  a signed row it remains stable. Native import identity is the pair of `source`
  and exact `metadata.session_id`; a local transcript path is evidence location,
  not logical identity.
- `metadata.session_id` is the exact provider-native session identity. For a Claude
  Code subagent this is its `agentId`; the root `sessionId` becomes its parent.
- Local bindings may retain a legacy filename-derived alias and the later exact
  provider ID for one conversation; `(source, session_id)` remains unique and can
  never name two conversations.
- Cross-device equivalence is represented only by the
  [author-scoped signed provider-session alias](provider-session-aliases.md).
  Exact provider evidence may add a logical conversation row ID to that alias;
  local bindings, content similarity, cwd, and repository evidence cannot.
- `metadata.parent_session_id` is present only when the provider explicitly names a
  parent session.
- `metadata.session_kind` is the normalized `main` or `subagent` classification.
  `metadata.session_kind_evidence` is `exact` when the provider format defines the
  unit or explicitly marks a child, and `inferred` when absence of child evidence
  is the only basis for `main`. It is never inferred from a shared cwd or repository.
- Missing parents remain missing relationships. A child may retain its explicit
  parent identity even when the parent transcript is unavailable.

## Canonical conversation fields

| Field | Meaning |
| --- | --- |
| `source` | Provider/integration namespace: `codex`, `claude-code`, `chatgpt`, `claude`, or `perplexity`. |
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

`messages.metadata.provider_index` is the zero-based provider record order. Codex
uses the JSONL event index, Claude Code uses transcript event order, Claude export
uses message-array order, and ChatGPT uses parent-before-child tree order with the
provider's child ordering. It breaks timestamp ties for `read`, export, resume,
replay, and last-role decisions; timestamps never substitute for it.

## Stable session metadata

| Key | Meaning |
| --- | --- |
| `session_id` | Exact provider-native identity for this main or subagent session. |
| `parent_session_id` | Explicit native parent identity. |
| `session_kind` | `main` or `subagent`. |
| `session_kind_evidence` | `exact` or `inferred`; how `session_kind` was established. |
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

A Codex subagent spawned with forked context replays its parent's history before
its own work. Replayed items keep the parent's turn ids, so `fork_boundary` records
the `provider_index` of the first `turn_context` whose turn id is absent from the
parent rollout named by `forked_from_id`. Messages before it are inherited copies:
they stay stored and readable, but discovery ranks only the copy in the session that
authored it. The key is omitted when the parent rollout is not available locally,
so nothing is hidden without that evidence. A fork without its own turn yet has a
boundary past its last item.

Discovery ranks session families. A conversation's root follows exact
`parent_session_id` links within one source; `search` and `query` return one row per
root with up to three conversations of that family (best message each), so subagent
hits appear under the session that spawned them. `--flat` restores one row per
conversation. Injected `developer` and `system` prompts leave default ranking and
stay available with `-r developer`.

Normalized values have four distinct evidence states: exact values copied from the
provider, absent values the provider did not supply, explicit provider `unknown`
values, and inferred classifications carrying an evidence marker. Convos never
promotes absent or inferred evidence to exact.

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
  result-arrival status, and timestamps. Results are joined to calls rather than
  counted as a second invocation; a returned result is not by itself proof that a
  mutating operation succeeded.
- Attachments retain provider metadata and bounded local bodies without reading
  arbitrary paths mentioned in text.
- File edits are emitted only after a matching provider result confirms success.
  Failed, timed-out, missing-result, or indeterminate attempts remain tool calls but
  never become canonical edit facts. No file-system state is fabricated for
  historical sessions.

## Admission and recovery

A structurally exact wrapper-only main may be marked
`capture_mode=startup-stub-candidate`; it is retained rather than destructively
deleted. A wrapper followed by real prompt text and every explicit subagent remain
ordinary conversations. Recovered prompt history is retained with
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

This PR emits the normalized contract without changing physical conversation IDs
and advances a parser epoch so unchanged raw transcripts are reconsidered. The
dependent migration adds native-identity indexing and owned-row recovery
classification while preserving released signed row identities.
