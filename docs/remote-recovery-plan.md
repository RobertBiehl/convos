# Remote recovery implementation

Review: `db592a0cc666f03ec34f66f218e07ab7cd5f0e44`, Oracle session
`convos-recovery-plan-review`, GPT-6 Pro, completed 2026-09-07.
See [the verdict](oracle-recovery-review-2026-09-07.md). Production counts in
the supplied report have not been independently audited.

- [x] Fix timestamp comparison and batch-independent captured edit paths without changing released encodings; preserve missing-file-fact redaction.
- [x] Share exact-hash timestamp reconstruction across audit, aliases and retained publication; aliases/audit also try captured edit paths. Keep non-round-trippable bodies.
- [x] Preserve existing pre-v11 row_bodies as repair donors during upgrade; do not create that table for new archives.
- [x] Stable self-authored identity independent of recovery mode; preserve native differences and reuse existing received bindings. Local publication bases distinguish an old upload echo from a remote conflict.
- [x] Durable unresolved provenance/body retention and edit/turn/file dependency retries, separate from authorization failures; author-signed causal successors may update file associations.
- [x] Non-destructive resumable repull with independent current-proof-head accounting; no network-spanning live archive lock or automatic whole-database rollback.
- [x] Additive missing-row recovery from same-archive read-only backup donors; preserve unique descendants, newer local changes, available attachment bodies, and exact legacy body donors.
- [x] Explicit unavailable-body versus retained-variant accounting, including signed heads with missing origin mappings.
- [x] Unchanged alias blocks no longer prevent the no-op fast path; doctor retains their status. Sync reports exceptional retained bodies.
- [x] Add cross-device replay, interrupted repull, rollback-mode exit, legacy upgrade, dependency ordering, old-encoding, and exact-preservation regressions.
- [ ] Automatically consolidate existing duplicate identities only with exact proof and descendant accounting; do not delete ambiguous copies.
- [ ] Full native source-observation causal reconciliation: current conservative behavior retains native/remote conflicts instead of overwriting originals.
- [ ] Dependency-keyed alias retries; current retry triggers are changed sync state and explicit repair.
- [ ] Recover and audit the reporter's actual archive with their backup donors; do not infer production repair counts from synthetic tests.
- [ ] Complete b6 ordinary suite, release CI, publication, and public-index fresh installation.

No report-specific repair has been run on personal data. Hook throttling is
separate completed work and does not resolve these defects. Schema additions
must be compact and reusable; avoid incident-specific migration chains.

Pending edit facts now use core-owned `remote.edit_dependencies` and `remote.edit_ready` metadata. Archive edit dependencies include the author; shared file dependencies do not. An arriving parent queues its key, and each retry transaction handles at most 500 dependency/proof entries. The retry cursor commits with projection and body cleanup. Incoming relay receipts follow the core commit that durably retains the body and dependency metadata. Sync drains ready work after restart even when no new replicas arrive, and the settled check includes readiness without scanning retained bodies.

The empty dependency key tracks a one-time, paginated bootstrap of legacy retained edit facts; `done` completes that bootstrap. Unready facts remain retained without making every no-op sync scan their bodies. Cleanup removes a safely projected head and only its proven causal ancestors, preserving unresolved forks and scope conflicts.
