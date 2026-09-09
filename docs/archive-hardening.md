# Archive integrity changes after b8

Release `0.11.6b8` is already published at
`d7ad83446cb1fe80d8d6a93bcf9757bc2b2ecd54`. The proposed next prerelease is b9.
Its client fixes retain b8's storage and compression behavior; no new relay
migration or wire change is required by this patch.

## Invariants and implementation

| Invariant | Failure | Correction |
| --- | --- | --- |
| A known historical file identity survives migration | The v3 initializer creates a conflicting placeholder scope | Reuse an unambiguous hash-valid mapping; use the same narrow core repair for already-migrated archives |
| New observations extend a known causal branch | Multiple heads prevent attestation despite a recorded local base | Continue only a current, retained, hash-valid base with matching author, turn, file, and repository; preserve competing heads |
| A proof referenced by an origin retains an available body | Successor signing retires the origin's predecessor body | Exclude origin-referenced predecessors from retirement |
| Validity filtering cannot erase signed history | A failed edit disappears from the current reader despite a reconstructible signed body | Use historical reconstruction only in preservation/recovery paths, with exact signed-hash checks; preserve failed evidence status |
| Audits describe one archive generation | Hook draining changes the archive between audit pages | Coordinate maintenance leases, keep capture queued, and reject other concurrent mutations |
| Body availability and relationship health are distinct | Repull completion conceals missing physical parents | Report exact relationship counts in audit and doctor, including unsigned rows |

The supplied minimal report reproduces the first three failures on released b8.
That establishes affected behavior, not when the defects were introduced.
It does not establish the origin of the production orphan-message population.
Missing conversation parents affect conversation joins and retrieval even when
message bodies remain preserved. Diagnose exact IDs before selecting a repair.

## Maintained tooling

[Archive recovery](archive-recovery.md) is implemented by two source-checkout
operator tools under [scripts/archive_recovery](../scripts/archive_recovery/README.md).
Diagnosis uses the canonical audit, including exact proof claims. Repair binds
its plan to archive identity and generation, validates donor/current/diagnostic
proof equality, compares protected tables exactly, and commits through core.

Import, Remote synchronization, and signing are separate product operations.
There is no reporter-specific package generator, credential-backed signing
experiment, bundled fixture generator, or command-forwarding layer. The old
orphan-stub script is removed. Runtime products never import recovery tooling.

## Release gates

1. Validate the final source with all non-integration tests, budget checks,
   all-package builds, Linux CI, and isolated wheel installation.
2. Simulate on fresh copies of both affected owner archives. Check plans,
   retained-body availability, reference gaps, idempotence, and production-scale
   runtime. No production recovery or large-archive timing is established yet.
3. Align all eight product versions and internal dependencies for b9, update the
   lockfile and changelog, review the PR, and publish from the exact tested merge.
4. Verify all four public distributions, fresh installation, and actual
   foreground/background client versions. Distribute recovery tools through the
   matching tagged source; wheels and sdists do not include them.
5. Apply reviewed owner-local repairs, then import and exchange records. Check
   body preservation, physical references, and shared logical identities/content
   separately. Document remaining gaps; do not manufacture equality by deletion.

The core schema remains 12, with no new tables or columns. The existing v3
migration gains one corrective call. Core owns this durable upgrade behavior
and the minimal transactional writers. Operator workflow remains outside core.
