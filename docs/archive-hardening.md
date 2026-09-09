# Archive hardening after b8

## Release boundary

`v0.11.6b8` is published at `d7ad83446cb1fe80d8d6a93bcf9757bc2b2ecd54`.
Its binary relay storage, verified migration, compression, and conditional
replica replacement remain the base for this work. Do not move that tag or
replace its published packages. Integrate the client recovery fixes in a new
PR and release, proposed as `0.11.6b9` after validation.

The supplied minimal fixture reproduces all three failures on unmodified b8:
legacy scope migration blocks capture, edit-head forks block attestation, and
successor attestation can retire an origin-referenced body. The same fixture
passes with this branch. Its input is synthetic; it does not establish the
cause of the production orphan messages.

## Changes included

- Repair only exact migration-generated scope placeholders with an unambiguous,
  hash-valid historical file mapping. Existing v12 archives need explicit repair;
  correcting the old migration does not rerun it on an already upgraded archive.
- Extend an edit branch only from its recorded, current, hash-verified predecessor
  with the same turn, file, and repository. Preserve competing heads and proofs.
- Protect predecessor bodies while origins reference them. Keep current-validity
  reads separate from historical reconstruction, and hash-check reconstructed
  failed-edit bodies before retaining them. Failed evidence stays failed.
- Restore historical bodies through core, with proof/body precondition checks,
  archive generation advancement, and change tracking for subsequent sync.
- Coordinate hook draining with full local import, audit, and alias reconciliation.
  New captures stay queued. Release maintenance leases on failure or cancellation;
  the next hook drain can process them. Other archive changes still invalidate a
  paginated audit rather than produce a result from mixed generations.
- Include exact missing physical references in audit, including unsigned rows.
  Report body preservation separately from relationship completeness. Repull
  completion is not a convergence assertion.
- Show the same relationship counts in `convos doctor`, including healthy zeros,
  distinct missing-parent IDs, and explicitly marked message history. This uses
  the existing read-only archive snapshot and works without the Remote product.
  An incomplete schema is reported as unavailable, never as zero orphan rows.

## How serious are orphan messages?

A message whose physical conversation parent is absent cannot join that
conversation in retrieval. This is a functional integrity problem even when
the message's signed body is preserved. The new audit distinguishes messages
with an explicit `history_of` marker from unmarked messages, without treating
either category as safe to delete. Unmarked does not prove current/native.

The incident's missing-parent totals do not establish permanent content loss.
For each parent identity, establish whether a physical alias, exact signed body,
backup donor, or original provider transcript can supply the missing relationship.
An authorized signed parent arriving after its child is covered in both legacy
and compressed transport tests. That verifies the supported recovery case; it
does not explain the existing incident population.

Do not make zero physical row-count differences a release gate: independent
devices can retain different local/history rows or have different authorization.
Do require every remaining gap to be reported and every claimed repair to be
verified against exact identities and preserved original proofs.

## Incorporation and rollout

1. Review the port and upstream changes together with the synthetic reproducer,
   regression tests, and repository constraints. Test b8 storage/compression
   alongside the recovery changes.
2. Simulate repair independently on fresh copies of both affected archives.
   Keep verified backups and exact preimages. Review unexplained scope conflicts
   and the remaining parent-reference inventory before any live repair.
3. After the new client release is verified, update both owners' foreground and
   background clients. Running a patched checkout alone does not update a service.
4. Apply the verified, owner-run repair on each archive, then import local sources
   and exchange remote records. Use the same fixed code for all writers. These
   fixes require no additional relay wire change beyond b8.
5. Audit both archives at stable generations. Check body availability, physical
   relationships, signed logical identities/content, and authorization scope
   independently. Document unresolved historical variants instead of deleting them.

See [the repair procedure](b7-archive-recovery.md) for simulation and recovery
tools. Live archive repair, relay deployment, and publication are separate from
the code and synthetic validation in this branch.

## Local validation

- Unmodified b8 reproduces all three supplied synthetic failures; the patched
  branch passes all three without changing the supplied fixture.
- Initial hardening (`1adcfbc`): `uv run pytest -m 'not integration' -q`:
  908 passed, 9 deselected.
- Doctor statistics follow-up: 124 targeted tests passed, covering doctor,
  capture, Remote operations, recovery, database access, and line budgets.
- `uv build --all-packages`: all eight distributions build as wheels and sdists.
- Core: 1,499 token-counted lines. Existing budgets are unchanged.
- Runtime: Python 3.14.2, DuckDB 1.4.3. No affected production archive was repaired
  or used to infer convergence in this validation.
