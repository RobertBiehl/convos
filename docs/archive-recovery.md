# Archive recovery

Recovery has three independent questions: can every required signed body be
read, do physical parent relationships resolve, and can native capture continue?
A successful repull answers only part of the first question. Upgrading prevents
known failures from recurring; it does not recreate missing history or repair
metadata already migrated by older code.

## Supported operations

`convos doctor` reports exact missing-parent row and distinct-parent counts for
messages, reply parents, tool calls, attachments, artifacts, and file edits.
Explicit message history markers are counted separately. Neither an orphan
count nor a history marker identifies data that is safe to delete.

`convos remote audit` verifies signed-body availability and reports relationships.
For an offline copy without account configuration, use `diagnose.py` with its
owner's account ID. Both use the same audit implementation and generation guard.
Diagnosis exits 1 for unresolved bodies or references, after saving its report.

`repair.py` supports two evidence-backed changes:

1. Correct an exact migration-generated edit-scope placeholder when the archive
   already contains one unambiguous, hash-valid historical file mapping. Real
   captured scopes, foreign ownership, and ambiguous mappings are excluded.
2. Restore missing retained bodies from an earlier backup of the same archive.
   The diagnosis, current archive, and donor must have identical proof records;
   each body must match the recorded version, identity, state, and content hash.
   Missing proofs and corrupt existing bodies are refused, not overwritten.

These operations leave conversation content, evidence status, and signatures
unchanged. They do not resolve arbitrary forks or invent missing parent rows.
The old `scripts/recover_orphans.py` stub-writing path was removed because it
bypassed the current canonical writer and signed-history rules.

## Procedure

Use the reviewed release checkout and its dependencies (`uv sync --extra dev`).
Run each owner's recovery separately. Pause background writers for live repair;
capture can continue queuing. The tooling also takes the existing maintenance
leases. All output directories and report files must be new.

```sh
# Inventory a private snapshot. ACCOUNT_ID is the archive owner's account ID.
uv run python scripts/archive_recovery/diagnose.py \
  --database /private/snapshot/convos.db --user-id ACCOUNT_ID \
  --output /private/diagnosis.json

# Default: verified backup, exact plan, and repair of an isolated copy.
uv run python scripts/archive_recovery/repair.py \
  --root /path/to/convos-root --output /private/repair-simulation

# Optional missing-body restoration, also simulated by default.
uv run python scripts/archive_recovery/repair.py \
  --root /path/to/convos-root --output /private/body-simulation \
  --donor /private/earlier-backup/convos.db --diagnosis /private/diagnosis.json

# Audit the simulated target with the same read-only diagnosis tool.
uv run python scripts/archive_recovery/diagnose.py \
  --database /private/repair-simulation/simulation/data/convos.db \
  --user-id ACCOUNT_ID --output /private/simulation-audit.json
```

Review `plan.json`, `report.json`, and the resulting audit. Then run the same
repair invocation with a new output directory and `--apply` as the archive's OS
owner. `--database-only` is available solely for simulations whose attachments
are inaccessible; live application always requires the complete attachment
backup. If the source still has a WAL, run `convos backup` as its owner first.

The transaction rejects stale plans and compares every protected table's complete
row multiset against the backup with SQL `EXCEPT ALL`. This includes proofs,
conversation content, and evidence. Any difference aborts the transaction.
The repair must be idempotent before commit. Reports distinguish a committed
repair from failure; the backup and plan survive a failed transaction.
Private plans can contain signed bodies and conversation text. Keep them private.
`success` in the repair report means the requested narrow repair committed;
use diagnosis to assess remaining body and relationship gaps.

After both owners have the fixed client and have completed their own local
repair, run the regular product commands under each owner's environment:

```sh
convos sync --full --local-only
convos remote sync
convos remote audit
convos doctor
```

Publish from the first owner, sync the second owner, then sync the first again.
Audit both, and continue only as needed for newly exchanged records. Account
scope and unresolved signed branches matter; equal total row counts are not a
convergence test. Resume background jobs with the upgraded interpreter after
validation. Do not delete history to make an audit count reach zero.

## Compatibility

The corrective path changes no schema version, signed encoding, or physical-ID
recipe. Archives still on the old migration path receive the corrected scope
initialization. Archives already at schema 12 require explicit repair.

The bugs were reproduced on released b7 and b8. Their introduction date is not
established by that observation. The supplied report and synthetic reproducer
provided evidence; maintained regression tests exercise scope eligibility,
branch continuity, retained history, and maintenance behavior directly.
