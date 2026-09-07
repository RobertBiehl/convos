# Convos prevention and existing-database recovery plan

**The recovery release needs more than the timestamp patch.** I would replace destructive repull with an additive, resumable reconciliation path, fix identity independently of recovery mode, and make verified fact retention independent of whether its current projection succeeds.

There is also an additional mechanism worth investigating before accepting the report’s classification of the remaining 177 mismatches: **personal `scan()` can produce different signed `file_path` values for the same database contents depending on which provenance records happen to be in its input batch.** That can create a proof for a published body that differs from the native typed row without an importer overwriting that row.

## Review scope

I resolved and inspected **`db592a0cc666f03ec34f66f218e07ab7cd5f0e44`**. Its parent is the supplied baseline, **`1d2cfc1008aacbbed9fbd8f13c6e8dcb4ff45223`**. The review commit changes hook handling, hook documentation/tests, and the core line-budget test—not the Remote recovery implementation. The mechanisms below therefore remain in this candidate.

I accessed the exact files through GitHub’s connector. Container DNS prevented cloning, so **I did not execute the repository test suite**. I inspected relevant source and tests, but the reported production counts and “76 targeted tests passed” are not independently reproduced results. No personal archive was accessed or modified.

## 1. Causal assessment

### What is confirmed, and what the report overstates

| Area                                                        | Assessment                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **67 timestamp mismatches**                                 | **Confirmed mechanism.** `_alias_page` feeds raw datetimes into `logical_row`, whereas scanning and received-row reconstruction use `clean()`. Whole-second datetimes consequently acquire different string representations. The report’s 67-row measurement is credible field evidence, but I cannot verify that population without its database/proofs. Fixing the comparison does not require rewriting those rows or proofs. |
| **Own-history duplication after replay**                    | **Confirmed mechanism, incomplete proposed fix.** `apply_row_replicas` makes native projection depend on `recover == "native"`. However, missing `archive_mode` is not itself corruption: `remember_archive` deliberately deletes it after convergence. Persisting `"native"` indiscriminately would confuse identity with rollback/adoption policy.                                                                             |
| **Failed repull leaves a degraded archive**                 | **Confirmed.** SQLite reset and canonical deletion precede network replay. Failure retains a backup but leaves already committed changes in place. The audit cannot establish completeness because its inventory comes from surviving origins.                                                                                                                                                                                   |
| **Provenance blocks unrelated work**                        | **Confirmed.** Missing edit and wrong turn share one exception; conflicting file associations also raise. Delaying provenance within one apply batch does not solve cross-page ordering. The resulting workspace block gates publication.                                                                                                                                                                                        |
| **177 mismatches prove unavailable second-revision bodies** | **Not established.** Those measurements establish that two tested reconstructions did not match the selected proof. They do not establish that no exact reconstruction or retained copy exists, nor when a body disappeared. Batch-dependent path rendering, prepared publication snapshots, adoption behavior, historical copies, and migration 11 all require investigation.                                                   |
| **Blocked aliases cause repeated expensive sync**           | **Confirmed scheduling defect.** Any cached blocked alias makes `_settled` dirty. The reported 19-minute duration is a field measurement, not a benchmark reproduced here.                                                                                                                                                                                                                                                       |

These conclusions follow from the exact alias/replay code, recovery-mode lifecycle, repull implementation, provenance projectors, and settled predicate.

### Additional findings that materially affect the plan

**A. Personal edit serialization depends on batch composition.**

In `projection.py::scan`, personal `edit_paths` and `file_paths` are derived from the provenance records selected for that particular call. A call containing only a changed `file_edits` row does not load its unchanged provenance dependencies.

For an edit whose native path is `/checkout/a.py` and whose captured portable path is `a.py`:

```text
scan(..., changes=None)                    can publish file_path="a.py"
scan(..., changes={("file_edits", "e")})     publishes file_path="/checkout/a.py"
```

`scan_archive` also divides work into pages and separately selected sources. Meanwhile, `_alias_page` compares the stored path without reconstructing the publication transformation. This is a **concrete additional serialization defect**, and a plausible explanation for some file-edit proof/body discrepancies—not a verified diagnosis of the reported 162 edits.

**B. Proof existence is currently mistaken for sufficient local retention.**

`local_replica_ids` inventories proof headers, not recoverable bodies. Repair can therefore regard an identity as locally available when its current body is missing. Additionally, `replica_inventory` writes receipts based on **relay presence**; that is not evidence of successful local materialization. These meanings must be separated.

**C. Reset is broader than “delete received rows.”**

`reset_remote_projection` expands its deletion set through descendants without requiring an origin on each descendant. It also clears all conflict bodies and is not scoped to the set of workspaces that the subsequent replay can currently access. Consequently, “has a received parent” is being treated as evidence of disposability, and retained data from an inaccessible workspace can be removed without a corresponding replay source.

The existing repull test explicitly expects an origin-less child named `derived` and a relay orphan to disappear. That fixture does not prove those classes are generally reconstructible or disposable. The failed-repull test checks a **set of titles** after retry; that assertion can hide duplicate physical conversations carrying the same title.

**D. The importer-overwrite explanation needs qualification.**

Current ingestion explicitly excludes origin-owned rows and their incoming subtrees. Native self-authored rows do not receive that protection, but “the importer overwrote foreign data” is not established by this report. Also, `attest_rows` signs prepared records, which can differ from the current native row because of serialization, team redaction, or a concurrent local change. Migration 11 then drops `remote.row_bodies` without checking replacement materialization. Investigate these separately rather than selecting one theory prematurely.

**E. “Missing means ordering” and “repository means better” are both too strong.**

A missing edit may arrive later, but it may also be unavailable, outside entitlement, or never published. Defer it first; classify its eventual availability separately.

A repository observation may be more useful than an external observation, but that does not prove file identity equivalence. Also, the report’s concrete example is an **incoming degraded external observation**, despite its “rejects the better fact” heading. Neither timestamp precedence nor repository preference is a sufficient identity rule.

---

## 2. Prevention implementation, in order

The smallest coherent design is **one verified-row reconciliation path used by ordinary sync and explicit repair**. Keep existing proof storage and reuse the existing conflict-body facility for exceptional retained bodies. Add only the compact metadata needed to distinguish native source observations, materialized revisions, and pending work.

### P1. Make signed-body construction independent of batching

**Callsites:** `projection.py::{scan, _alias_page, audit_rows, row_replicas, attest_rows, _store_proofs}`.

First apply the narrow correction in `_alias_page`:

```python
(columns, list(map(clean, values)))
```

Then fix the larger representation inconsistency:

* Resolve captured edit-path dependencies by the **selected edit IDs**, not by whichever provenance records happen to share their batch.
* Share the same reconstruction logic between comparison, audit, and re-publication.
* Keep `logical_row` v1 and its released signature/hash rules unchanged.
* Verify incoming bodies **before** any SQL conversion. A successful SQL round trip must not be assumed to preserve every valid signed representation.

For historical data, reconstruct the known released representations and require an **exact match to the existing proof hash**. This includes the clean/raw timestamp distinction and native versus captured portable edit paths. Do not accept approximate timestamp equivalence, arbitrary timezone shifts, or “normalized hashes.”

When an already verified body cannot be reconstructed exactly from typed storage, retain that body as an exception. Do not discard it merely because some typed projection exists. This preserves old encodings without restoring a permanent second body copy for every healthy row.

A useful regression, using the existing helper and imports in `tests/test_remote_projection.py`, is:

```python
def test_personal_edit_body_does_not_depend_on_scan_batch(tmp_path):
    _, core = source(tmp_path)
    state = connect(tmp_path / "state.db")
    try:
        def edit(records):
            return next(r for r in records if r["kind"] == "file_edit.record")

        whole = edit(scan(core, state))
        isolated = edit(scan(core, state, changes={("file_edits", "e")}))

        assert projection_module.signed_row(whole) == \
               projection_module.signed_row(isolated)
    finally:
        state.close()
        core.close()
```

This targets the batch-dependent path mechanism; it is a proposed regression, not a test I executed. The existing protocol vectors must remain unchanged.

### P2. Separate identity, source observation, and update permission

**Callsites:** core `_logical_parts`, `project_logical_rows`, `project_archive_rows`, `upsert`; Remote `apply_row_replicas`, `prepare_archive`, `remember_archive`, `attest_rows`.

Use the already documented logical identity:

```text
(author_user_id, row_kind, source_row_id)
```

A device signs a revision. A workspace supplies an authorization/delivery path. Neither creates another logical identity. Multiple valid workspace proofs for one revision remain retained authorization evidence, not additional rows.

Implement the physical resolver with these rules:

1. An established, unambiguous binding wins.
2. Proven locally owned identities use their native IDs; foreign identities use the existing `remote_id` recipe.
3. A legacy same-user received binding is recognized as the same logical identity—not treated as a new foreign author.
4. Two existing physical candidates become a repair item. Ordinary replay must not create a third.
5. Recovery mode controls **what may be changed or published**, not the identity recipe.

Persist native ownership/basis information in core, not in disposable receipt state. A compact native-state record should distinguish:

```text
logical identity / physical binding
currently materialized proof
last locally observed source payload and its causal base
pending local change, if any
```

This last distinction is important. **A full reread of an older transcript is not automatically a newly authored revision.** If Remote advances a native row from A to B, rereading an unchanged local source that still contains A must not manufacture a new B→A revision. Conversely, a genuinely observed A→B→A source transition must remain expressible.

Record the relevant source observation/base atomically with capture. Do not use `HOOK_STATE`, wall-clock recency, or whichever proof happens to be newest at attestation time as a substitute for that causal information. On legacy or ambiguous inputs, preserve the incoming local variant rather than inventing ancestry.

For application:

| Relationship                                                  | Action                                                                                      |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Same logical revision/body                                    | Idempotent acceptance; no new row or signature.                                             |
| Incoming verified descendant of a clean materialized revision | Advance to it.                                                                              |
| Incoming verified ancestor                                    | Keep the current descendant.                                                                |
| Independent valid branches                                    | Retain both; expose conflict.                                                               |
| Incoming revision versus a genuinely pending local change     | Preserve both; do not overwrite or silently attach the local change to the incoming head.   |
| Suspect rollback/replacement archive                          | Retain inputs additively; publication requires a verified basis for each affected identity. |

`native=True` must therefore **never imply unconditional overwrite**. In particular, a team publication transformation must not destructively replace lossless local source content merely because its proof belongs to the same user. Preserve the source variant and exact signed publication body where they differ.

### P3. Commit verified facts even when their projection is unresolved

**Callsites:** `projection.py::apply_row_replicas`; core `project_logical_rows`, `project_provenance`, proof/body mutation helpers.

Replace the current boolean-style result with explicit dispositions, for example:

```text
materialized
already_materialized
superseded
retained_pending_dependency
retained_conflict
rejected
```

The central invariant is:

> Every accepted current signed revision has either an exact reconstructible materialization or a durably retained exact body.

Reuse `remote.row_conflicts` for bodies that cannot currently be materialized: forks, unresolved dependencies, protected local variants, and non-round-trippable representations. Add reason/dependency metadata rather than another healthy-row body store.

Move the canonical insert/delete operations behind a typed core primitive. Remote verifies signatures and authorization and submits typed records; core commits proofs, required body retention, and safe projections together. In particular, **do not delete the retained body before establishing that its replacement representation is sufficient**. The current resolved-scope cleanup is too coarse for this contract.

For provenance, separate three cases:

**Missing dependency.** Store the verified fact and mark the referenced edit/turn/file as pending. Retry when that dependency changes or arrives. Passing the relay tail does not turn absence into a contradiction; it changes the diagnostic to an unresolved availability problem.

**A valid newer observation.** When the same logical observation has a verified causal successor, update the derived association from that successor. Do not retain an older association merely because the new observation looks less useful.

**Incompatible active observations.** Preserve every observation. Derive an unambiguous association only when the active evidence supports one. Otherwise expose ambiguity rather than choosing whichever arrived first, whichever has a later timestamp, or whichever says `repository`.

A fact incompatible with the current edit may also be historically valid. Describe it as incompatible with the selected projection unless the evidence establishes a stronger contradiction.

Pending indexes can live in rebuildable SQLite because the facts/bodies remain canonical. After state loss, rebuild those indexes from retained unresolved records. Dependency resolution should query changed keys and their dependents—not re-run all provenance.

Errors should identify the workspace, author, proof/revision, logical edit, physical edit, expected/stored turn, and competing file/fact IDs. Put concise diagnostics on stderr; do not alter existing archive query row schemas.

### P4. Separate authorization readiness from projection completeness

**Callsites:** Remote `pull`, `pull_row_replicas`, `local_replica_ids`, `replica_inventory`, `sync_once`, `_settled`.

Keep two explicit dimensions:

```text
authorization/history basis: verified or blocked
data reconciliation: complete or incomplete, with itemized dispositions
```

Invalid controls, rollback, unavailable authorization, and required signed-history failures remain security barriers. A valid fact with an unresolved edit reference is not the same category.

Once the authorization basis is valid, healthy rows with established ownership and causal bases should continue uploading and downloading around unrelated data conflicts. This breaks the report’s plausible cycle:

```text
provenance problem → workspace publication disabled
→ missing author bodies never reach relay → repair cannot fetch them
```

The current ready-workspace gate confirms that such a cycle can occur, but does not independently prove the origin of the reported 45 missing relay replicas.

Also separate receipt meanings:

* **Relay presence** supports upload deduplication.
* **Canonical acceptance** means the exact body was materialized or durably retained.
* **Projection completion** is a separate result.

These can be columns rather than three competing subsystems. Remove proof-header inventory as a reason to skip a required body download. Explicit repair must be able to bypass misleading old receipt state, including while `sync_states.lifecycle == "blocked"`.

Do not catch every exception and call it a benign conflict. Validation failures need their own rejected-item accounting; security failures must retain their security semantics.

### P5. Retry blocked aliases only when their blockers change

**Callsites:** `_settled`, `reconcile_provider_aliases`, `provider_alias_records`, and the existing core change-feed consumers.

A cached unresolved alias is a **status**, not necessarily runnable work.

Persist the alias revision, projector version, blocking keys, and last relevant input version. Retry when an implicated proof/body, member binding, required attachment, or relevant observation changes—or when explicitly requested.

Use alias membership plus targeted relationship queries to connect changed rows to affected aliases. Do not build another full per-row archive mirror. New rows in unrelated conversations must not reactivate every blocked alias.

A settled-but-incomplete sync can then do the ordinary cheap state check, report the same unresolved items, and avoid repeating their scans. It must not report full synchronization merely because no runnable work remains.

There are two adjacent incremental costs to remove as part of this work: `provider_alias_records` scans conversation groups, and `archive_info` uses an archive-state function that counts native rows across tables. Ordinary changed sync should use the cheap identity/generation marker and targeted work; full counts belong in explicit status/audit operations.

---

## 3. Existing-database recovery implementation

### R1. Start a durable repair job without deleting anything

**Replace the implementation of `repull_once`; do not wrap its current reset in more diagnostics.**

Keep `repull` as a compatibility entry point to the new non-destructive engine. Let `sync --repair` enter that engine even from an existing data-blocked state.

The job records its archive identity, verified owner/control basis, selected workspaces, input backups, enumeration boundaries, expected items, progress, and unresolved outcomes. Keep the manifest outside disposable `state.db`, with private permissions. It must survive a working-state rebuild.

Discover compatible retained repull/migration backups as candidate inputs. Do not blindly select “the latest backup”: repeated failed repulls may have produced newer but less complete copies.

For a partially completed old repull, the inputs are:

```text
current live archive
+ surviving proof headers and conflict bodies
+ compatible retained pre-repull/migration backups
+ currently accessible relay replicas
```

The backup is a **read-only donor**, not a replacement for the live database.

### R2. Build an expected set independent of surviving projections

Use separate identities for logical revisions and proof evidence:

```text
K = (author_user_id, kind, source_row_id)
R = (K, revision)
P = exact signed proof identity
```

Several proofs can authorize one logical revision. Retain all required proofs, but do not require duplicate body copies for each authorization path.

Inventory:

* Known current revision requirements from live and backup proof graphs—not merely origins.
* Native rows and locally retained variants, including unsigned work.
* Retained pending/conflicting bodies.
* Verified authorized relay rows and applicable semantic objects.
* Referenced retained attachment bytes.

Reduce revision requirements only through verified causal relationships. A child absent from a shorter transcript is **not** a deletion.

Every expected item must receive an explicit disposition:

| Disposition                  | Required evidence                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------------- |
| Current materialization      | Exact reconstructed body hash and valid proof.                                     |
| Retained conflict/dependency | Exact body is durable, with explicit unresolved projection status.                 |
| Superseded                   | A verified descendant explains why this is no longer the current body requirement. |
| Deleted                      | An applicable valid author-signed tombstone.                                       |
| Intentionally not projected  | A recorded, justified policy/dependency reason—not simply relay absence.           |
| Missing/unverifiable         | An unresolved recovery failure, naming the item.                                   |

A verified orphan row can be retained independently. An unavailable parent does not prove its child is corrupt, nor does a reference alone prove that the parent was ever included in the recoverable set.

The relay provides pagination and presence queries, not a signed inventory proving all historical row availability. Freeze the job’s traversal boundary, record the actual enumeration, and reconcile it against the independent expected set. **State the scope of completeness precisely:** surviving known requirements and the authorized inputs actually enumerated. Neither a tail nor an origin count proves recovery of data never represented in any surviving inventory.

### R3. Fetch, verify, and merge in bounded batches

Use the existing row-replica pagination and reconciliation operations. Event `fetch` is not a row-replica fetch API. A future direct row-fetch operation can optimize this; it is not necessary to make recovery safe.

Stage/download and verify outside DuckDB. Then apply bounded typed batches through the same primitive used by ordinary sync.

**Transaction sketch:**

```text
read a bounded input batch
verify signatures, authorization, exact bodies and lineage outside DuckDB
read the affected live bindings, revisions and local-source bases
prepare a reconciliation plan

inside one short core transaction:
    recheck only the affected inputs
    retain proofs and bodies that must survive
    apply safe projections / exact duplicate consolidation
    update native bases and the existing change feed
commit

only then commit SQLite receipts and repair progress
yield before the next batch
```

Use record and byte ceilings as well as an adaptive elapsed-time target. A page size alone does not bound work when a row is large. Do not hold an archive connection during relay requests, cryptographic verification, attachment copying, or backup hashing.

Revalidation must be **per affected key/component**, not “the global archive generation changed, restart the whole repair.” A local write in another conversation should not invalidate the batch.

The repair run should also service already-authorized healthy publication between repair batches; repair completion must not become a new workspace-wide upload barrier.

### R4. Consolidate accidental duplicates without discarding unique children

Distinguish two operations.

**Same logical identity, two physical rows.** This is the reported native/received fork. Consolidate the physical bindings and dependent references without changing signed logical IDs or re-signing foreign content.

**Different logical conversation IDs grouped by a provider alias.** Changing a signed child’s logical `conversation_id` changes its signed body. Use the existing author-authorized alias/revision mechanism; do not disguise this as a physical rewrite. The current reconciliation already creates author-signed successors for such moves.

For the report’s **22-message native / 11-message received** pair:

1. Establish that the parent rows represent the same logical identity.
2. Compare children by their logical identities and revisions.
3. Preserve the union of distinct children.
4. Apply verified descendants over ancestors.
5. Retain incomparable bodies as conflicts.
6. Remove only physical duplicates whose content has a proven retained destination.

If the 11 are a subset of the 22, the result is 22—not 33 and not 11. If either side has unique children, retain those too. Message counts alone cannot select a winner.

Move children in bounded batches while both parent slots remain usable. Retire the redundant parent only after checking all referencing tables and retained variants. Include message parents, tools, edits, artifacts, attachments/body indexes, provenance links, provider bindings, and retrieval invalidation.

Coordinate provider bindings with capture commits. A parser using a stale binding must retry or be safely remapped; it must not lose its captured input. Add an unchanged-transcript reimport assertion after consolidation to ensure repair does not trigger another round of duplicates.

### R5. Recover proof/body mismatches by evidence, not speculative rewriting

For each unresolved proof, produce a compact diagnostic record:

```text
logical identity; proof ID; author device
origin and authorization workspaces
revision / predecessor / selected heads
raw, clean, and applicable released-representation hashes
matching retained revision, if any
candidate body sources and exact-match result
relay availability
```

Search donors in this order:

1. Current typed data using exact released representations.
2. Captured provenance needed to reconstruct published edit paths.
3. Retained conflict bodies and local history rows.
4. Compatible pre-repull and pre-v11 migration backups, including old `row_bodies` where present.
5. Verified relay copies.
6. Another authorized holder.

A history row with a different storage ID can be a body donor: reconstruct using the **known proof’s logical ID and references**, then require the exact proof hash. That does not authorize deleting the donor or inferring identity from general content similarity.

Investigate the batch-dependent path mechanism before concluding the 162 second revisions are locally unrecoverable. Separately inspect adoption skips, prepared publication snapshots, redaction, native ingestion history, and migration chronology. No production cause should be assigned solely because the current body matches the root revision.

**Author device unavailable:** an authorized holder of the intact body and original proof can repair delivery without the original device’s private key. That capability already exists in the protocol.

**Only a proof/hash survives:** report the body as unavailable. Do not label an older body as the latest, invent a tombstone, or re-sign a receiver’s approximation. Keep useful older material visibly stale and let unrelated work proceed. An authorized author may later issue an intentional successor, but that is a new authored revision—not recovery of missing historical bytes.

### R6. Reconcile SQLite and finish with truthful accounting

Do not repeatedly clear global state on each retry. The job owns its own replay progress. At completion, rebuild the applicable incoming working-state checkpoints from the durable outcomes.

Preserve keys, pinned controls, outbound envelopes, sequence identity, and non-rebuildable bindings. In particular, a relay-presence receipt must not become a local-retention receipt simply because an old header survived.

A crash after core commit but before SQLite commit repeats an idempotent batch. A crash before core commit cannot publish a completed receipt. An archive replacement or rollback invalidates the corresponding working-state basis; it does not change logical identity.

Successful **preservation** and successful **projection** are distinct. A retained conflicting body can satisfy preservation while the repair remains projection-incomplete. Report both; do not turn `doctor ready` into a substitute for the expected-set audit.

---

## 4. Why neither proposed “atomic repull” fix is sufficient

**Automatic whole-database restore is unsafe during ordinary operation.** The Remote run lease is separate from local archive mutation. Restoring an old DuckDB after network failure can discard local captures committed during repair, and restoring DuckDB alone leaves SQLite ahead. An offline, explicitly requested whole-backup restore is a different operation; it should not be the failure handler here.

**A transaction spanning reset and network refill violates the lock requirement.** It would also not magically make the separate SQLite state atomic.

**A full shadow archive followed by a file swap has the same concurrent-write problem** unless it adds change capture, catch-up, and a final coordinated barrier. That is more machinery than this archive needs.

Stage **inputs and reconciliation decisions**, not a replacement live database. Preserve existing rows while verified replacements arrive.

For backup cost, move verification/copying work off the live archive lock once an immutable snapshot is available. Do not claim a hard short-lock guarantee for a large non-reflink file copy. Where a short consistent clone is unavailable, the additive repair should use durable affected-row preimages rather than require a giant replacement snapshot before doing useful work.

### Backup deletion rule

Delete a temporary backup only after a backup-specific manifest proves that every unique row/variant, proof, and retained attachment it contributes is:

* preserved durably elsewhere in the archive, or
* covered by a justified duplicate/supersession/deletion disposition.

Also ensure that no pending job or outbound body depends on it.

A clean current-origin audit, a matching row count, or a successful relay request is insufficient. An unresolved job normally retains its inputs; a backup proven to contain no remaining unique input need not be kept forever merely because another unrelated item is unresolved.

---

## 5. Supported recovery UX

The following is a **proposed interface**, not functionality present in `db592a0`:

```bash
convos remote repair --plan --workspace Personal
convos remote repair --plan --workspace Personal --from-backup /path/to/backup.bak
convos remote repair --apply JOB_ID
convos remote repair --resume JOB_ID
convos remote repair --status JOB_ID
```

The plan should discover matching retained backups, show the intended affected identities, and distinguish safe automatic actions from unresolved choices.

`remote repull` should enter this engine rather than delete projections. `remote sync --repair` should use the same repair primitives and work when an earlier data failure left the workspace blocked.

A status report should separate, for example:

```text
Authorization basis: verified
Healthy synchronization: continuing
Expected current revisions: ...
Materialized and verified: ...
Bodies retained with unresolved projection: ...
Missing bodies: ...
Duplicate physical rows safely consolidated: ...
Local variants preserved: ...
Backup cleanup: not yet eligible
```

Return nonzero for unresolved required recovery failures, with a resumable job ID. Repeating the command must not create another destructive reset or make another complete backup of an already degraded archive.

**For the reported affected archive, the current destructive `repull` should not be run again as the recovery strategy. Its retained backups are valuable repair inputs.**

---

## 6. Required regression tests

Use two users **Alice and Bob**, plus two independent Alice devices. Otherwise “two devices” can accidentally test only foreign-user namespacing.

The acceptance assertions must compare exact identities, revision sets, body hashes, signatures, and relationships—not only successful exit, title sets, or lifecycle flags.

| Test                             | Required assertions                                                                                                                                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Released encodings**           | Whole-second, subsecond, null and previously accepted timestamp encodings round-trip or retain their exact body. Existing protocol vectors and proof signatures remain unchanged.                                |
| **Batch-independent edit paths** | Full scan, edit-only delta, provenance-only changes, and page splits produce identical logical bodies for unchanged facts. No spurious revision is signed.                                                       |
| **Same-user receipt reset**      | Alice’s two overlapping archives retain one physical projection per resolved logical identity after repeated receipt/state resets. No extra conversations, messages, edits or tools appear.                      |
| **Personal/team delivery**       | The same identity received through multiple authorized workspaces does not fork physically; Bob’s same source ID remains distinct. Transformed team content cannot destroy lossless source data.                 |
| **Newer versus older**           | Valid descendant advances; replayed ancestor cannot revert it. A real A→B→A source transition works, while rereading an unchanged older source does not manufacture that transition.                             |
| **Pending local change**         | Remote replay preserves both the pending local observation and the valid incoming body; it does not invent causal ancestry.                                                                                      |
| **Header without body**          | A retained proof with no materialization/body is downloaded during repair despite old receipts. It cannot count as recovered merely because its header exists.                                                   |
| **Dependency inversion**         | Deliver provenance before its edit across multiple core batches and relay pages, then restart. The fact survives, resolves once, and does not block unrelated uploads.                                           |
| **Dependency never arrives**     | The fact remains retained and explicitly unresolved; repeated settled sync does not rescan the archive or call it complete.                                                                                      |
| **Conflicting observations**     | Causal successor updates the association. Incomparable repository/external observations remain preserved and ambiguous in either arrival order. No repository identity is inferred from preference.              |
| **Unequal duplicate trees**      | Test 22/11 subset and non-subset cases. Assert the exact union of message/tool/edit/attachment identities and bodies, not merely the parent count.                                                               |
| **Retained inaccessible data**   | Repairing Personal leaves retained rows/proofs/bodies from an inaccessible team unchanged and does not upload them without authorization.                                                                        |
| **Old aborted repull**           | Start with missing projections and advanced SQLite state. Merge from the pre-repull backup while preserving new local writes absent from that backup.                                                            |
| **Missing relay rows**           | Distinguish a tombstone, a legacy orphan, an inaccessible epoch, and a genuinely missing expected head. None becomes silently “clean.”                                                                           |
| **Unavailable author**           | Bob repairs Alice’s intact signed row without invoking Alice’s signer. A proof-only row remains a named unresolved requirement.                                                                                  |
| **Process interruption**         | Kill before/after staging durability, core commit, SQLite commit, duplicate retirement and cleanup. Resume preserves the exact expected content/proof sets. Include SIGKILL, not just caught exceptions.         |
| **Concurrent writers**           | Local writes during network requests succeed. Unrelated writes do not restart repair; changes to affected keys are re-planned rather than overwritten.                                                           |
| **Settled performance**          | With a large archive and unchanged blocked aliases, the next sync performs no archive-wide row/body/alias scan. One unrelated changed row causes only delta work. One blocker change retries only affected work. |
| **Pre-v11 upgrade**              | Needed bodies in old `row_bodies` survive upgrade until exact replacement retention is verified. Healthy rows do not acquire permanent duplicate body copies.                                                    |
| **Post-repair reimport**         | Re-reading unchanged provider transcripts produces zero accidental duplicates and preserves established bindings.                                                                                                |

Extend the existing tests rather than discarding their useful coverage. In particular, strengthen `test_failed_repull_retains_backup_and_is_retryable`, replace the unsafe generalization in `test_repull_replaces_received_rows_and_preserves_local_rows`, and add timestamp-bearing alias fixtures; the existing alias helper often uses null timestamps.

Run the complete ordinary suite in isolated temporary homes:

```bash
uv run pytest -m 'not integration'
```

Add query/work-count instrumentation alongside timing tests. A fast small fixture is not proof that blocked sync remains incremental at archive scale.

---

## 7. Runtime, storage, migration, and release boundaries

### Permanent cost

The permanent additions should be limited to:

* Compact native ownership/source-base metadata.
* Reason/dependency information for existing exceptional retained bodies.
* Rebuildable keyed work indexes for pending projection and alias retries.

Healthy signed rows should retain one reconstructible canonical representation plus existing proof metadata. Full extra bodies are justified only where a distinct required variant or non-reconstructible signed representation actually exists.

Normal work should be proportional to changed rows and their actual dependents. Explicit repair may scan the archive and backups, but must stream them and remain resumable.

The **1500-line core and 2400-line Remote limits remain acceptance gates**. Replace the destructive path and duplicated reconstruction logic; do not retain two recovery implementations or move canonical mutations into an optional package to satisfy the count. Actual line and storage budgets must be measured after implementation—I have not established that a particular patch fits them.

### Migration strategy

Use one additive schema transition for the ownership/retention metadata. Backfill affected identities lazily or through explicit repair, not by a full scan on every startup.

For archives still containing `remote.row_bodies`, prevent the unconditional drop until necessary bodies have been preserved. A legacy table can remain a protected repair input temporarily; it need not remain a permanent runtime store. Archives already at schema 11 recover from current data, retained backups, and other holders.

Do not add separate permanent migrations for “67 timestamps,” “162 edits,” or this particular incident. These are applications of the same reconstruction and repair primitives.

### Adjacent hook work

The new throttle principally helps custom intermediate hooks, not ordinary changed Stop/SessionEnd captures. Parsing is already outside DuckDB, but the time budget is checked between transcripts, so one large transcript can exceed the nominal worker budget. Full-result ingestion still does substantial locked work.

An inexpensive improvement is to replace `_id_conflict`’s quadratic repeated scanning with a linear dictionary pass while retaining its current comparison semantics:

```python
def _id_conflict(rows, fields):
    seen = {}
    for row in rows:
        key = row["id"]
        value = tuple(str(row[field]) for field in fields)
        if key in seen and seen[key] != value:
            return key
        seen[key] = value
    return None
```

Read/sql/resume and memory paths that drain captures need a separate freshness contract: a committed-snapshot read with a visible pending-capture watermark, and an explicit bounded flush when freshness is required. Do not quietly remove flushing and introduce unexplained stale reads. Incremental transcript parsing and changing read defaults can be a separate work item; they should not delay data recovery.

### Release blockers versus deferrable work

**Release blockers:** consistent signed-body reconstruction, mode-independent identity with safe native update semantics, durable retention of unresolved facts/bodies, blocked-mode recovery, non-destructive merge from existing backups, independent expected-set accounting, and keyed retries that let healthy work continue.

**Deferrable:** a direct replica-fetch optimization, stronger relay inventory facilities, full incremental transcript parsers, richer conflict-resolution UI, and further metadata compression.

No user choice is needed to fix false comparisons, retain facts, recover exact proof-matching bodies, or remove proven physical duplicates. User input is genuinely necessary only when evidence cannot choose between incompatible authored/local variants, when someone proposes discarding unprovable leftovers, or when an author must intentionally supply a new value after the last recoverable body has disappeared.

**The implementation should leave an affected user with a usable archive, preserved evidence, continued healthy synchronization, and an exact list of unresolved items—not another reset, another global block, or a “clean” audit over whatever happened to survive.**

