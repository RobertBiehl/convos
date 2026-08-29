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
| In review in PR #105 | Restore capture and manual-sync availability: use short inbox critical sections, make competing drains and syncs nonblocking, cap each drain worker by event count and elapsed time with visible progress and safe handoff, bound ordinary provenance work to touched conversations, edits, and repositories, and preserve full reconciliation behind `sync --full`. |
| In review in PR #105 | Freeze the cross-provider conversation contract and audit real Codex and Claude imports. Define transcript and session identity, main-versus-subagent relationships, cwd, Git and model evidence, tools, attachments, and confirmed edits without deriving unavailable facts. |
| In review in PR #105 | Install backed-up local native-session bindings without rewriting released physical IDs; preserve safe filename-to-native aliases, make a batch converge or fail before mutation, preserve foreign signed rows, structurally quarantine startup stubs, and keep migration commit independent of FTS rebuild. |
| In review in PR #105 | Make ordinary imports complete and bounded: stream relevant Codex records, retain current ChatGPT content forms, recheck incomplete and tool-ended web sessions, checkpoint chunks, and keep parse failures visible. |
| In review in PR #105 | Add explicit provider turn ordering so timestamp ties cannot hide the terminal message from bounded `read`, export, replay, resume, or last-role logic, and selectively refetch affected ChatGPT conversations. |
| In review in PR #105 | Freeze the signed author-scoped provider-session alias and tombstone design: exact-evidence membership, deterministic multi-ancestor convergence, author separation, retained v1 row identities, backed-up migration, and fail-before-mutation reconciliation. |
| In review in PR #105 | Add the core-owned provider-session proof ledger and personal-workspace bridge; derive aliases only for actual exact-evidence conflicts, converge concurrent same-author leaves, and keep different authors separate. |
| In review in PR #105 | Replace the blocking pre-existing-duplicate migration with a backed-up, non-destructive admission: retain every row, bind future parsing deterministically, and let the signed alias ledger own convergence. |
| In review in PR #105 | Implement backed-up alias reconciliation and prove pre-existing and cross-device duplicate row successors/tombstones converge with retained v1 replicas. |
| In review in PR #105 | Add an auditable file-edit evidence ledger, reclassify surviving provider transcripts, retain raw uncertain history, and exclude anything except confirmed edits from exact provenance and team sharing. |
| In review in PR #105 | Harden remote synchronization: converge duplicate-root proof chains without hiding true forks, add observable exponential backoff, separate nonblocking sync and control-mutation leases, cover interruption recovery, and prove retained signed logical replicas remain ingestible. |
| In review in PR #105 | Build and qualify the isolated Titan multi-user testbed with a destructive fresh synthetic lane and a persistent upgrade canary before declaring a stable release. |

## Production evidence register

The 2026-08-28 two-user deployment report established these findings. This is an
evidence register; remediation is represented by the work items above.

| Finding | Current outcome |
| --- | --- |
| v3 migration omitted `local_facts` | Fixed in 0.10.1; regression retained. |
| Projection failed on legacy rows | Fixed in 0.10.1; regression retained. |
| State rebaseline used excessive time and memory | Fixed in 0.10.1; release benchmark retained. |
| Attestation failed on duplicate-root proof chains | Fixed in PR #105; duplicate-root, true-fork, and retained-v1 regressions retained. |
| Remote watch swallowed repeated failures without backoff | Fixed in PR #105; failures, retry time, recovery, and capped exponential delays are visible. |
| Remote synchronization held coarse locks around unrelated work | Fixed in PR #105; sync and explicit control mutations use separate nonblocking leases. |
| Recovered-device approval re-emitted another member's signed preference under the approver's event identity | Fixed in PR #105; epoch retention republishes only objects signed by the local user. |
| A removed device's persisted `READY` state attempted publication without the withheld epoch key | Fixed in PR #105; signed device authorization gates settlement, scanning, retention, and publication. |
| Commands blocked behind a running hook drain | Fixed in PR #105. |
| One hook event performed archive-sized work | Fixed in PR #105. |
| One winning hook drain claimed the entire backlog | Fixed in PR #105; bounded workers hand remaining backlog to a successor. |
| Imported cwd, Git, model, tool, and edit evidence was incomplete | Fixed in PR #105; parser and live-sample regressions retained. |
| Provider transcripts could be imported more than once | Fixed in PR #105 through exact native bindings and signed cross-device alias reconciliation; every physical row remains retained. |
| Promptless startup stubs became conversations | Exact wrapper-only candidates are retained and marked in PR #105; ownership-safe cleanup remains open. |
| Completed ChatGPT verdict was absent from bounded `read` | Fixed in PR #105 with canonical provider order and selective repair of affected ChatGPT conversations. |
| A clean Ubuntu 24.04 install of core plus remote required an undocumented native compiler toolchain for `llama-cpp-python` | Fixed in PR #105 and narrowed before release: Linux installs compiler-free core, literal retrieval, and relay support without a default semantic runtime; macOS retains llama.cpp. |
| A confirmed local edit lost its evidence classification through a signed replica | Fixed in PR #105 without changing signed-v1 rows; an authorized root-signed evidence object binds the classification to exact edit/tool revisions, and old rows remain `unverified` until that evidence arrives. |

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

A stable release requires every registered finding to be closed or explicitly blocked
with user agreement, their regressions passing, complete input accounting, zero
unexplained duplicate transcripts or orphaned rows, idempotent second sync, verified
migration interruption recovery, bounded performance, a passing fresh Titan run, a
passing persistent-canary upgrade, CI/build/install checks, and verification of the
exact released commit and installed executable.
