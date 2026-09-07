# Remote recovery implementation

Review: `db592a0cc666f03ec34f66f218e07ab7cd5f0e44`, Oracle session
`convos-recovery-plan-review`, GPT-6 Pro, completed 2026-09-07.
See [the verdict](oracle-recovery-review-2026-09-07.md). Production counts in
the supplied report have not been independently audited.

- [x] Fix timestamp comparison and batch-independent captured edit paths without changing released encodings; preserve missing-file-fact redaction.
- [ ] Complete exact historical signed-body reconstruction across audit, aliases and retained publication.
- [x] Preserve existing pre-v11 row_bodies as repair donors during upgrade; do not create that table for new archives.
- [ ] Stable self-authored identity independent of recovery mode, with safe revision and local-source update semantics.
- [ ] Durable unresolved provenance/body retention and dependency retries, separate from authorization failures.
- [ ] Non-destructive resumable repull with independent expected-set accounting; no network-spanning archive lock or automatic whole-database rollback.
- [ ] Existing archive recovery from read-only backup donors; consolidate only proven duplicate identities and preserve unique descendants/local changes.
- [ ] Exact proof-matching body recovery and explicit unavailable-body accounting.
- [ ] Keyed alias retries and truthful settled-but-incomplete status without repeated full scans.
- [ ] Isolated cross-device, crash, concurrent-write, old-encoding and exact-preservation acceptance tests; ordinary suite and line budgets.

No report-specific repair has been run on personal data. Hook throttling is
separate completed work and does not resolve these defects. Schema additions
must be compact and reusable; avoid incident-specific migration chains.
