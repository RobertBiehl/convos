# Archive recovery for b7 and b8

For a tiny, fully synthetic before/after database, see
[the public fixture](b7-minimal-fixture.md): one conversation, one message, one
edit, three proofs; no production data required.

## Failure and bounded repair

**Bug:** schema v3 seeded `external/<hash(edit)>/unknown` scopes even when an
edit already had a unique historical file mapping (`src/ai_convos/cli.py`,
`init_schema`). Later confirmed-source capture derives a different file ID
from that placeholder; `provenance_issue` correctly rejects the rebind.

**Fix:** reuse the existing, hash-valid file mapping for those exact migration
placeholders. Fix v3 initialization and expose the same core-owned repair for
already-migrated v12 archives. No schema bump is needed for this metadata repair.
Do not weaken the scope-conflict guard or resolve paths using today's checkout.

**Evidence:** the old v3 migration produces a mismatched scope in the regression
fixture; the corrected migration preserves the historical file identity. An
already-migrated fixture fails full capture before repair, succeeds afterwards,
then captures no further edits. Signed-row audit remains complete. Tests also
reject genuine scopes, captured routes, foreign-owned edits, ambiguous mappings,
and invalid file hashes, and preserve invalid/unknown/unverified evidence status.

Eligible scopes must have the exact migration-generated path, no repository,
root, checkout, route, or observation time, and a unique hash-valid historical
file mapping not marked `legacy_scope_conflict`. Updating the scope does not
change the edit, its file association, its evidence status, or any signature.

Full local sync, remote audit, and provider-alias reconciliation now hold the
hook-drain lease while they need stable input. Capture still queues durably.
Audit and aliases also exclude concurrent local sync; they continue releasing
DuckDB between pages and still reject unrelated archive mutations. Leases
release on cancellation. Ordinary incremental sync retains its nonblocking
behavior around an existing hook worker. Remote sync reports processed/total
alias groups; a processed group can be settled, changed, or blocked.

For edit observations, attestation can use the recorded `remote.local_row_bases`
predecessor when it is still a current head and its hash-verified retained body
has the same turn, file, and repository. This permits a fresh native observation
to extend that branch without choosing a winner for the fork. Missing, stale,
wrong-author, unretained, or differently scoped bases still fail closed.
Ordinary archive-row forks retain their existing refusal behavior. Existing proof
rows and competing heads are unchanged. Predecessor bodies remain available
while either origin table still references their proofs; unreferenced bodies
can retire after a durable successor. Verified current-projection copies still
retire normally. Full backups retain the original bodies.
Contradictory alias session metadata is rejected before expanding dependent
bodies, while the existing per-page proof and metadata checks remain in place.

## Operator tool

Use the installed Convos Python. The scripts load this checkout's core and Remote
code explicitly; no installed packages, release tags, or Koder pins are changed.
A live repair must run as the archive's OS owner. Until upstream ships the fix,
the `convos` forwarding mode runs commands with this checkout's maintenance
coordination, without requiring another fork release.

```sh
# Run in the patched checkout. Adjust this Python path for your installation.
convos_python="$HOME/.local/pipx/venvs/convos/bin/python"

# Default: simulate in a new isolated copy, including both provenance passes.
"$convos_python" scripts/repair_b7_archive.py --output "$HOME/convos-b7-simulation" --capture
# Optional isolated test of recorded-branch attestation, then signed-body audit.
"$convos_python" scripts/verify_b7_attestation.py "$HOME/convos-b7-simulation/report.json"
"$convos_python" scripts/verify_b7_recovery.py "$HOME/convos-b7-simulation/report.json"

# Apply only after simulation. Backup/export precedes the transactional repair.
# Runs actual full local-only sync twice and saves each command's output.
"$convos_python" scripts/repair_b7_archive.py --output "$HOME/convos-b7-repaired" --apply --sync-full
# Use the same tested client code for the final relay operations.
"$convos_python" scripts/repair_b7_archive.py convos remote sync
"$convos_python" scripts/verify_b7_recovery.py "$HOME/convos-b7-repaired/report.json" --repull
```

Output directories must be new. `--root` selects another archive for simulation.
`--database-only` permits a simulation when another account's attachments are
inaccessible; it is explicitly forbidden with `--apply`. `--sync-full` requires
the source owner's identity because provider roots are account-specific.
Verification is offline unless `--repull` is explicit; that option accepts only
the owned live target and saves the actual repull audit in `audit.json`.

Each run keeps a SHA-verified database snapshot, verified attachment bundle
(except explicitly database-only simulation), exact affected-row exports,
scope preimages, before/after full-row fingerprints, and a JSON report. The
fingerprints are non-cryptographic change detectors; the complete backup has a
SHA-256 checksum. All repair artifacts are private. They include conversation
text and signed history: do not attach the raw directory to a public bug report.

The script refuses unexplained active scope conflicts, changed preconditions,
unexpected table mutations, or a non-idempotent repair. After successful scope
repair, later sync failure does not undo committed ingestion or erase backups.
Inspect the saved log and report; rerunning the repair is safe and creates a new
backup. Neither the script nor the maintenance locks delete queued captures.

## Verification boundaries

Publication exposed a separate retention bug: a signed successor retired its
predecessor body even while an origin still required it. Regression cases cover
message rows, edit observations, file versions, and checkpoints; they fail with
old retirement and pass when both origin tables protect referenced predecessors.
Retirement still works after the origin reference moves. Existing signatures
are unchanged, and no origin is silently rebound to make an audit pass.

For an archive already affected, diagnose a private snapshot and restore only
the exact bodies from an earlier backup. Use a new output directory each time:

```sh
"$convos_python" scripts/diagnose_b7_rows.py --database /private/snapshot/convos.db --user-id ACCOUNT_ID --output /private/unavailable.json
# Default simulation; live apply also requires the owner's full fresh backup.
"$convos_python" scripts/repair_b7_archive.py --output "$HOME/convos-b7-history-simulation" --restore-history /private/earlier/convos.db --restore-claims /private/unavailable.json
"$convos_python" scripts/verify_b7_recovery.py "$HOME/convos-b7-history-simulation/report.json"
```

Restoration uses the core writer and advances archive generation and affected-row change tracking. Message restoration conservatively invalidates retrieval freshness.

The recovery checks complete original/current proof rows and each body's signed
hash, refuses corrupt existing bodies, exports exact private preimages, and
verifies that only permitted tables changed. Repeating the restoration is a
no-op. It does not synthesize a body, regenerate a proof, or rewrite an origin.
The audit also counts missing physical parent references, including unsigned messages, and identifies explicitly marked message history separately. A successful repull can still have relationship gaps; its output reports them. JSON audit output includes `relationships` and `archive_generation`. These counts do not establish permanent data loss or a safe deletion set.

The diagnostic output contains exact proof rows; it is private, not an upload.

A complete audit means every required signed row body remains available, either
in the current projection or retained history. It does not mean all historical
forks are merged, orphan messages are resolved, or physical row counts match
across devices. Do not delete those records to force count equality.

Relay wire format and server behavior are unchanged by this client repair.
No relay redeployment is required. Keep background sync paused until both
devices have the upstream fixes installed and complete their owner-run import
and Remote checks. Running patched commands does not update installed jobs.
