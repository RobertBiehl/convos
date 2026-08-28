# Reliability and data stewardship plan

Status: active, 2026-08-28

Convos is a durable archive. A successful command is not sufficient evidence of
correctness: every source input must be accounted for, migrations must preserve
meaning, incremental work must be bounded by changed data, and released clients
and relay replicas must remain interoperable for their documented lifetime.

## Work items

Only concrete deliverables receive task status here. The requirements and release
gate below are acceptance criteria, not additional tasks.

| Status | Task |
| --- | --- |
| Done in PR #94 | Restore capture and manual-sync availability: use short inbox critical sections, make competing drains and syncs nonblocking, cap each drain worker by event count and elapsed time with visible progress and safe handoff, bound ordinary provenance work to touched conversations, edits, and repositories, preserve full reconciliation behind `sync --full`, and add concurrency and archive-size regressions. |
| Done in stacked PR #95 | Freeze the cross-provider conversation contract and audit real Codex and Claude imports. Define transcript and session identity, main-versus-subagent relationships, authorship, cwd, Git observations, model evidence, tool calls, attachments, edits, and provider-only metadata without deriving unavailable facts. |
| Pending | Ship one backed-up, resumable, bounded-memory identity and ingestion migration. Dedupe within author and provider transcript identity, never by root session alone and never across relay authors. Re-read surviving raw inputs only after native identity bindings are installed, clean promptless startup stubs, and prove interruption recovery. |
| Pending | Harden remote synchronization: fix duplicate-root proof-chain convergence, add observable exponential backoff, narrow local mutation locks, cover interruption recovery, and prove retained signed logical replicas remain ingestible. |
| Pending | Build and qualify the Titan multi-user testbed with a fresh synthetic lane and a persistent upgrade canary before declaring a stable release. |

## Production evidence register

The 2026-08-28 two-user deployment report established these findings. This is an
evidence register; remediation is represented by the work items above.

| Finding | Current outcome |
| --- | --- |
| v3 migration omitted `local_facts` | Fixed in 0.10.1; regression retained. |
| Projection failed on legacy rows | Fixed in 0.10.1; regression retained. |
| State rebaseline used excessive time and memory | Fixed in 0.10.1; release benchmark retained. |
| Attestation failed on duplicate-root proof chains | Open; remote-hardening task. |
| Remote watch swallowed repeated failures without backoff | Open; remote-hardening task. |
| Remote synchronization held coarse locks around unrelated work | Open; remote-hardening task. |
| Commands blocked behind a running hook drain | Fixed in current PR. |
| One hook event performed archive-sized work | Fixed in current PR. |
| One winning hook drain claimed the entire backlog | Fixed in current PR; bounded workers hand remaining backlog to a successor. |
| Imported cwd, Git, model, tool, and edit evidence was incomplete | Fixed in stacked PR #95; parser and live-sample regressions retained. |
| Provider transcripts could be imported more than once | Open; identity-migration task. |
| Promptless startup stubs became conversations | Open; identity-migration task. |

## Acceptance criteria

### Input accountability

- Every discovered input has an exact outcome: imported, intentionally skipped with
  a reason, or failed visibly.
- Re-importing unchanged input produces no logical changes.
- Parser version and input content identity make required reprocessing discoverable.
- Missing capture is never presented as proof that an action did not occur.

### Schema and migrations

- Package and archive schema versions are separate.
- Ordinary releases prefer additive fields or provider metadata. Semantic schema
  changes are grouped into explicit, reviewed schema epochs rather than recurring
  release-by-release churn.
- Released migrations are append-only, backed up, resumable, and tested from every
  released on-disk version still in use.
- Validation covers conversations, messages, tools, attachments, edits, embeddings,
  provenance, remote origins, signed proofs, FTS, and change tracking.
- Interrupted migration plus resume produces the same semantic result as an
  uninterrupted migration under the 1.5 GiB memory ceiling.

### Incremental performance and concurrency

- Hook capture cost is bounded by the changed transcript, touched rows, and touched
  repositories, not total archive or provenance size.
- Each worker has an event cap and a between-event time cap; pending age and the
  previous batch outcome are visible in `doctor`, and failed-only queues do not spin.
- Read-only commands and manual sync do not wait for an already running incidental
  hook drain.
- No-op sync, one-event capture, relay paging, and migration have recorded budgets
  that fail on regression.

### Multi-user Titan testbed

- A fresh lane tests installation, enrollment, personal and team bootstrap, and full
  synchronization using synthetic data.
- A persistent canary lane is upgraded across releases to expose migration holes,
  accumulated duplicates, schema drift, and size-dependent performance.
- At least two isolated POSIX users, multiple devices, different checkout paths for
  one repository, concurrent/offline updates, device recovery/removal, relay restart,
  backup/restore, and old/new client interoperability are exercised.
- The evidence bundle records exact commits and versions, semantic counts and hashes,
  input outcomes, timings, peak memory, retries, conflicts, failures, doctor output,
  and a relay plaintext-canary scan.

## Stable release gate

A stable release requires all eleven findings to be closed or explicitly blocked
with user agreement, their regressions passing, complete input accounting, zero
unexplained duplicate transcripts or orphaned rows, idempotent second sync, verified
migration interruption recovery, bounded performance, a passing fresh Titan run, a
passing persistent-canary upgrade, CI/build/install checks, and verification of the
exact released commit and installed executable.
