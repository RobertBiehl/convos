# Reliability and data stewardship plan

Status: active, 2026-08-28

Convos is a durable archive. A successful command is not sufficient evidence of
correctness: every source input must be accounted for, migrations must preserve
meaning, incremental work must be bounded by changed data, and released clients
and relay replicas must remain interoperable for their documented lifetime.

## Production findings

The 2026-08-28 two-user deployment report is tracked as eleven concrete issues:

| Finding | Work | Status |
| --- | --- | --- |
| 1 | v3 migration omitted `local_facts` | fixed in 0.10.1; retain regression coverage |
| 2 | Projection failed on legacy rows | fixed in 0.10.1; retain regression coverage |
| 3 | State rebaseline used excessive time and memory | fixed in 0.10.1; retain release benchmark |
| 4 | Attestation failed on duplicate-root proof chains | pending upstream fix and regression |
| 5 | Remote watch swallowed repeated failures without backoff | pending |
| 6 | Remote synchronization held coarse locks around unrelated work | pending |
| 7 | Commands blocked behind a running hook drain | current priority |
| 8 | One hook event performed archive-sized work | current priority |
| 9 | Imported cwd, Git, model, tool, and edit evidence was incomplete | pending canonical importer work |
| 10 | Provider transcripts could be imported more than once | pending identity migration |
| 11 | Promptless startup stubs became conversations | pending admission and cleanup migration |

## Work order

1. Restore capture and manual-sync availability. Incidental drains must never wait
   behind another drain, and one changed transcript must do work proportional to
   that transcript. Add lock, progress, archive-size, and retry regressions.
2. Freeze the cross-provider identity and data contract before backfill. Preserve
   exact transcript, owning session, explicit parent, author, cwd, Git observation,
   model, tool invocation, attachment, and edit evidence without deriving facts the
   providers did not supply.
3. Ship one backed-up, resumable, bounded-memory identity and ingestion migration.
   Dedupe within author and provider transcript identity, never by root session alone
   and never across relay authors. Re-read surviving raw files only after canonical
   identity is installed.
4. Harden remote synchronization: proof-chain convergence, observable exponential
   backoff, narrow local mutation locks, interruption recovery, and mixed-version
   receive-path coverage.
5. Qualify a release candidate in the Titan multi-user testbed before declaring a
   stable release.

## Permanent quality gates

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
- Interrupted migration plus resume must produce the same semantic result as an
  uninterrupted migration under the 1.5 GiB memory ceiling.

### Incremental performance and concurrency

- Hook capture cost is bounded by the changed transcript, touched rows, and touched
  repositories, not total archive or provenance size.
- Read-only commands and manual sync do not wait for an already running incidental
  hook drain.
- Tests measure forward progress and phase timing; CPU utilization alone is never
  treated as evidence of useful progress.
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
