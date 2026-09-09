# Archive integrity release review

Recommended release: **0.11.6b9**, based on published b8. Do not replace b8's tag
or artifacts. This is a client integrity and recovery release; complete recovery
of the two reported production archives has not been demonstrated.

The runtime and operator tooling reviewed here are committed at `daffb57`.
Subsequent documentation changes do not change that runtime.

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
- Total Remote: 2,538, up from b8's 2,520, below its existing 2,600 limit.

Physical patch counts relative to b8, excluding documentation:

| Area | Added | Removed |
| --- | ---: | ---: |
| Core and Remote runtime | 63 | 40 |
| Two recovery tools | 171 | 0 |
| Obsolete orphan-stub script | 0 | 73 |
| Regression tests | 699 | 1 |

The 171 tool lines are localized under `scripts/archive_recovery`: 134 for
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

## Release sequence

Final local validation of `daffb57`: **914 passed, 9 integration tests deselected**
in 265.43 seconds. The focused integrity/tooling review passed 96 tests, including
rollback after an unexpected content write, stale-plan refusal, donor/diagnostic
identity mismatches, source preservation, and idempotent owner-local apply on
temporary archives. Both tools' command-line entry points were checked.

All eight products built as wheels and sdists. The built core and Remote source
was compared byte-for-byte with the reviewed checkout. Validation used Python
3.14.2 and DuckDB 1.4.3. Linux CI, public-install verification, production-copy
simulation, and production-scale timing remain release gates. No live archive
was repaired and no new release was published during this work.

1. Finish source validation and simulate independently on fresh copies of both
   affected archives. Inspect exact plans, remaining gaps, and runtime.
2. Align all eight product versions and internal minimums to b9; update the
   version-alignment test, lockfile, and changelog. Current build metadata still
   says b8 and must not be uploaded as a replacement.
3. Review the final PR; require Linux CI and its isolated wheel smoke. Merge,
   verify post-merge CI, and create the tag from that exact merge commit.
4. Publish a GitHub prerelease. The current workflow builds all eight products
   and publishes four to PyPI: convos, convos-redact, convos-remote, and
   convos-remote-server. Verify every publication and a fresh installation.
5. Update both owners' foreground and background clients. Run reviewed local
   repairs and full local imports, then exchange records: first owner, second
   owner, first owner again, with further exchange driven by actual new records.
6. Audit body availability, physical references, shared logical IDs/content,
   and authorization scope separately before resuming background operation.

An already b8-compatible relay needs no new deployment for these fixes. If the
relay is still pre-b8, follow b8's separate stopped-relay storage migration and
receiver-upgrade procedure before enabling compression.
