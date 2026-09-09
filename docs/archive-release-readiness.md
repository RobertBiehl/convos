# Archive integrity release review

Release: **0.11.6b9**, based on published b8. Do not replace b8's tag
or artifacts. This is a client integrity and recovery release; complete recovery
of the two reported production archives has not been demonstrated.

Oracle reviewed candidate `ca2f8f2` against b8 and returned a no-go.
The release includes corrections and executable regressions for all four
findings; the earlier Oracle verdict is not approval of this final source.

## What is fixed

- Legacy placeholder scope correction reuses existing historical file identity.
- Attestation can extend a recorded, current, hash-valid edit branch while
  preserving competing heads. It does not choose a universal fork winner.
- Origins protect predecessor bodies from successor retirement.
- Historical reconstruction preserves exactly signed failed-edit bodies without
  promoting their evidence status or weakening ordinary validity filtering.
- Full import, audit, and alias reconciliation coordinate with hook draining.
  Capture remains queued; unrelated mutations still invalidate paginated audits.
- Doctor and audit report exact missing-parent counts, distinct parent IDs, and
  explicitly marked message history, including unsigned rows.
- Restored history is published under its original proof and author on the next
  ordinary sync. A retained-proof change marker triggers one full authorized
  scan; it cannot be mistaken for a native deletion.
- Repair durably publishes its verified snapshot before mutation and stages
  exact attachment bytes before committing references, including simulations.

The supplied reproducer established three failures on released b8. Independent
regressions cover their invariants and refusal cases. The placeholder seeding
entered in `cf1d41a` on August 26; a release where a bug was reported is not a
reliable name for its cause.

## Migration and implementation size

There is **no new core schema version, table, column, signed encoding, or physical
identity recipe**. Core remains schema 12. The existing v3 migration gains one
corrective call. Already-migrated archives need explicit repair.

Using the repository's token-aware code-line counter:

- Legacy scope repair: 3 counted lines, plus its call in the existing migration.
- Exact signed-body restoration writer: 5 counted lines. This is a reusable
  archive writer, not another schema migration.
- Total core: 1,499, unchanged from b8. The strict limit is below 1,500, leaving
  no spare counted line. Compactness does not make the integrity logic trivial.
- Total Remote: 2,539, up from b8's 2,520, below its existing 2,600 limit.

Physical patch counts relative to b8, excluding documentation:

| Area | Added | Removed |
| --- | ---: | ---: |
| Core and Remote runtime | 71 | 47 |
| Two recovery tools | 186 | 0 |
| Obsolete orphan-stub script | 0 | 73 |
| Regression tests | 913 | 7 |

The 186 tool lines are localized under `scripts/archive_recovery`: 149 for
repair and 37 for diagnosis. Core owns canonical mutation and durable migration;
operator backup, planning, and private reports stay outside the product runtime.
There are no new dependencies in this patch.

B8's already-released relay storage conversion is separate: its schema helper
and conversion function total 23 counted lines, with additional shared CLI
backup/publication plumbing. This patch changes none of that server code.

## Recovery tooling contract

Diagnosis exports the canonical audit's exact unavailable claims and proofs,
bound to an archive identity and generation. It is offline and read-only.

Repair defaults to an isolated copy. Live apply requires ownership and a verified
attachment backup. A stale plan, wrong archive, changed proof, mismatched body,
or unexpected change to a protected table fails the transaction. Protected
row multisets are compared with SQL EXCEPT ALL; aggregate fingerprints are not
the integrity test. No import, signing experiment, repull, or command forwarding
is hidden in repair. Normal product commands perform those operations separately.

See [the procedure](archive-recovery.md). The scripts ship in the tagged repository
source, not the package wheel or sdist. GitHub's matching source archive is the
appropriate delivery mechanism; a reporter-specific patch bundler is unnecessary.

## Remaining uncertainty

The production report recorded 22,902 messages with missing conversation parents
across 2,620 parent IDs on dev, and 29,900 across 688 IDs on aiagent. These are
report-snapshot measurements, not current live counts. Their complete origin and
repairability remain unproved. Preserved signed bodies do not imply that
conversation joins work; missing parents can affect retrieval.

Neither upgrading, full local sync, nor repull guarantees that every gap resolves.
Recovery depends on exact existing aliases, authorized parents, donor bodies,
or original transcripts. Deleting rows to obtain matching totals is not repair.

Doctor's additional exact scans and repair's full protected-table comparison
have not been timed on these production archives. Those costs need measurement
on private copies. An older client can reintroduce the corrected failures even
though there is no schema bump; update background writers as well as shells.

## Oracle findings and validation

All four concerns were confirmed against the candidate. DuckDB regression tests
reproduced the false native deletion and the missing database fsync. New tests
exercise exact attachment restoration and subsequent complete backup, failure
on missing/corrupt/symlinked bytes, and reconstruction of an invalid edit from a
donor with no retained JSON. Both simulation and owner-local apply use temporary
archives. A two-user in-process relay test verifies original-author publication,
no cross-workspace upload, no native tombstone, and a subsequent idle sync.
A separate relay test preserves and publishes an invalid native edit without
confirming its evidence status.

Oracle inspected exact-ref source through GitHub and verified the supplied
package manifests and synthetic signatures. It could not install DuckDB or
obtain full checkouts; its own probes used database doubles. The executable
Convos tests described here were run locally, not by Oracle.

The focused final regression pass completed with **284 passed**. All eight
products built as wheels and sdists; package budgets, connection checks, and
isolated install verification passed. The complete final suite contains 929
non-integration tests and excludes 9 live integration tests; PR and post-merge
CI must pass before publication. Validation
uses Python 3.14.2 and DuckDB 1.4.3 locally; Linux CI independently runs the suite
and a binary-only wheel smoke. All eight versions and internal bounds are b9.

A fresh macOS Python 3.14 environment installs all four local public wheels and
runs their CLI help entry points. A binary-only macOS Python 3.12 install fails
because PyPI has no usable llama-cpp-python wheel; this reproduces on published
b8. The existing macOS installation contract permits local compilation, as the
README states. This release does not claim compiler-free macOS installation.

## Publication and owner rollout

1. Require PR CI, merge, verify post-merge CI, and tag that exact merge commit.
2. Publish a GitHub prerelease. Trusted publishers release four distributions:
   convos, convos-redact, convos-remote, and convos-remote-server. Verify every
   publication, fresh public-index installs, metadata, and CLI entry points.
3. Before applying production repair, simulate on fresh copies of both affected
   archives. Inspect exact plans, remaining gaps, and production-scale runtime.
4. Update both owners' foreground and background clients. Run reviewed local
   repairs and full local imports, then exchange records: first owner, second
   owner, first owner again, with further exchange driven by actual new records.
5. Audit body availability, physical references, shared logical IDs/content,
   and authorization scope separately before resuming background operation.

An already b8-compatible relay needs no new deployment for these fixes. If the
relay is still pre-b8, follow b8's separate stopped-relay storage migration and
receiver-upgrade procedure before enabling compression. No live archive repair
or production-scale timing is claimed by these release tests.
