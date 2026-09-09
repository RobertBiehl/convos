# Changelog

## 0.11.6b10

- Preserve metadata-only signed attachment history without requiring nonexistent
  files during backup or recovery. Resolve claimed attachment bytes through all
  indexed paths for the same hash; keep missing/corrupt blobs fatal and identify
  the blocked reference, expected hash/size, and candidate paths in the error.

## 0.11.6b9

- Repair proven legacy edit-scope placeholders without changing signed history or evidence status; provide backup-first simulation and targeted recovery tools.
- Preserve origin-referenced historical bodies when signing successors, and reconstruct exact failed-edit bodies from backup without treating failed edits as successful.
- Coordinate capture with full import, audit, and alias reconciliation; preserve queued input on interruption. Continue only verified recorded edit branches.
- Report missing physical parent references separately from signed-body availability, including unsigned messages and explicitly marked history.
- Include exact orphan-reference counts and distinct missing-parent IDs in `convos doctor`, even without Remote installed.
- Keep offline diagnosis and explicit repair in `scripts/archive_recovery`; use canonical audit results, exact protected-row comparison, and archive-bound donor proofs. Remove the obsolete direct-write orphan-stub script.

- Publish restored history with its original proof and author on the next ordinary sync, without generating native deletion records.
- Flush verified repair snapshots durably before mutation; stage exact attachment bytes before restoring references, including isolated simulations.

Core stays at schema 12; no new signed encoding, identity recipe, or relay migration.
Already-migrated archives need explicit evidence-backed repair. See the
[recovery procedure](docs/archive-recovery.md) and [release report](docs/archive-release-readiness.md).

## 0.11.6b8

- Store bulk relay ciphertext as binary with accurate storage quotas. Add a
  verified, copy-only migration that preserves the source database and checks
  exact wire reconstruction before publishing the converted copy.
- Compress retained replicas before encryption with Zstd level 1 by default
  on supporting relays; keep incompressible payloads in the legacy envelope.
  Authenticate codec metadata and bound decompression size and framing.
- Compact owned retained replicas across authorized workspaces by default.
  Skip compressed bodies at the relay, reuse byte-verified local payloads, and
  download only missing historical payloads. Preserve signed logical rows,
  cursors, and concurrent replacements through conditional updates.

Upgrade all receiving clients before enabling compression on the relay. Stop
the relay before taking the final migration snapshot and switching databases.
See [compression and measurements](docs/remote-compression.md) and
[verified storage migration](docs/relay-storage-validation.md).

## 0.11.6b7

- Preserve exact signed archive rows, repository variants, and joined edit facts
  across native capture, attachment indexing, foreign projection, reparenting,
  deletion, timestamp repair, and backup recovery.
  Receiving a replica no longer establishes independent local provenance.
  Attestation retains a scanned snapshot atomically if source capture changes
  its typed row before the signature is stored, and retires the direct
  predecessor body only after its replacement is durable.
- Keep durable reverse references when a child arrives before its parent; audit
  and retained publication share core's released logical-row encoders.
- Resume missing edit dependencies in bounded, durable pages after restart,
  including shared files observed by another author. Preserve unresolved forks.
- Keep failed provenance enrichment queued separately from committed ingestion;
  fix hook handoff races and checkpoint only successfully parsed source files.
- Keep relay authorization and writes in the same transaction; avoid schema
  initialization in requests, bound HTTP framing and reads, and refuse redirects
  that could forward client credentials. Backups require an existing read-only
  source and publish a verified snapshot atomically.
- Reject device approvals that alter existing devices and removals that add
  unapproved replacements, in both the relay and client validators.
- Remove quadratic ingestion validation and archive-sized team scope/receipt
  inventories. Bound proof preservation through an indexed source-history lookup.
- Align all products and dependency minimums so the Remote client receives the
  core API it requires. Require DuckDB 1.4.3 because the former 1.2 minimum cannot
  initialize the existing schema. Schema v12 adds recovery metadata after a verified backup;
  released signed v1 encodings and physical identity recipes stay unchanged.

Existing ambiguous native markers and historical bodies already absent from
both the archive and relay still require evidence-based reconciliation.
Automatic live database compaction is deferred until concurrent replacement is
safe. See [b7 validation](docs/b7-validation.md) for measured scope and limits.

## 0.11.6b6

- Throttle intermediate hook capture to at most once per minute per transcript;
  turn/session completion bypasses that throttle. Unchanged captures skip workers.
- Fix batch-dependent edit paths and reconstruct historical timestamp/path
  encodings only when they exactly match the existing signed proof.
- Keep same-user replay identity stable, preserve native differences without
  duplicate conversations, and distinguish local publication from remote variants.
- Retain verified provenance when dependencies are missing or facts conflict;
  retry affected dependencies without blocking healthy rows.
- Replace destructive repull with resumable reconciliation and optional additive
  recovery from a same-archive backup. Preserve existing rows, legacy body donors,
  and concurrent local captures; report unavailable bodies and retained variants.
- Do not treat stored proof headers as proof that repair bodies are present;
  audit current proof heads independently of surviving origin mappings.
- Stop unchanged blocked aliases from forcing repeated full syncs.

This beta does not automatically resolve ambiguous existing duplicates or
conflicting native/remote versions. Keep recovery donors until those differences
have been accounted for. Production repair counts remain unverified.

## 0.8.1

- Publish Convos, Redact, Remote, and Remote Server through isolated trusted
  publisher environments with independently staged wheel and source artifacts.
- Make partial PyPI release retries safe and keep unreleased workspace products
  out of the public upload set and core package extras.
- Remove Remote's incidental Changegraph dependency and diagnostic command;
  encrypted synchronization and local recall remain independent product pillars.

## 0.8.0

- Rename the public product, repository, primary distribution, companion
  distributions, and bundled skill to the `Convos` / `convos` family while
  retaining stable internal Python import namespaces.
- Make PyPI the primary installation path, include semantic retrieval in the
  default distribution, and verify a clean wheel through an installed hybrid
  query smoke test.
- Lead the README with an agent-copyable setup prompt and the daily
  Capture -> Recall -> Continue workflow.

- Add explicit fresh-relay rehoming and team re-founding with retained signing
  identities, one shared encrypted origin-control bundle, portable row proofs,
  fresh membership and keys, and excluded-member denial.
- Add flat opaque row-replica reconciliation so state loss uploads only missing
  proofs and any authorized holder can repair an imported row without the
  original author's private key.
- Keep repair replicas at their original delivery epoch so rotations preserve
  future-only history boundaries.
- Make retained personal attachment bodies bounded, signed-hash-associated blob
  replicas that any authorized holder can repair without duplicating bytes in
  DuckDB or settled `state.db`.
- Checkpoint and validate a private one-time backup before automatically
  migrating an existing core DuckDB.
- Make provenance canonical core facts with logical-row proofs, so Remote is a
  read-only consumer and another authorized holder can repair the facts with
  their original authorship after relay loss.
- Add a content-free core change-generation index and per-workspace cursor so
  normal personal and team replication reads only changed rows, including
  signed tombstones and complete expansion when a conversation enters scope.
- Bound proof transactions, replica sealing, reconciliation, verification, and
  projection; retries inventory deterministic replica IDs before encryption,
  acknowledged local copies are not reprocessed, and relay quota accounting is
  constant-time.
- Simplify team history to future-only or complete-history access, remove
  per-row grants and carrier events, and define the first client/server
  protocol, signed workspace state, rebuildable state schema, optional-app
  bridge, and fresh relay schema as v1.

## 0.7.0

- Replace relay-authored memory purge markers with deterministic author-signed
  per-event certificates that bind exact envelope history to a retained signed
  deletion event; clients verify proofs before sequence or receipt mutation,
  selected history remains permanently protected, fresh state recovery closes
  purged sequence slots without ciphertext, and normal forget/sync usage is
  unchanged.
- Avoid downloading legacy ChatGPT conversations already covered by a completed
  account-scoped sync frontier; normal sync now fetches their details only after
  new activity moves them above that frontier.
- Capture direct Codex `apply_patch` custom-tool events as raw patches instead
  of scanning their diff text as JavaScript. Custom edits are recorded only
  after definite tool success; failed attempts remain auditable tool calls with
  failed status, without fabricated file edits or repeated parse noise.
- Stop treating dead lifecycle commands as healthy hooks: capture installation
  now pins the `convos` executable beside the running interpreter and the
  resolved custom archive root, while status requires that exact command once
  in every required event. Old executable paths, duplicates, and misplaced
  Claude `Stop`/`SessionEnd` handlers report `convos install-hooks` and are
  repaired idempotently.
- Detect agent-policy drift directly in `convos doctor`: Codex and Claude skill
  copies are compared byte-for-byte with the bundled distribution, unsafe file
  symlinks remain unhealthy, shared declared parent destinations still work,
  and missing or stale copies report `convos install-skills` as the exact
  repair without modifying user state.
- Make first-run setup actually complete: `convos init` now installs core
  capture hooks, imports existing local Codex and Claude Code sessions, and
  invokes strictly local, idempotent initializers from installed products; the
  Memory extra becomes ready in the same command without repeating skill
  installation. The reusable `sync --local-only` path honors configured agent
  roots and configured exports while skipping every web probe; downloads,
  remotes, credentials, network enrollment, and destructive actions remain
  explicit.
- Make conflict review readable without exposing the resolution engine: `memory review` now shows friendly providers and before/new/current text, labels unavailable origins explicitly, gives the conversational next step, and omits source IDs, locators, canonical IDs, hashes, and protocol action names while the JSON plan remains unchanged.
- Make the normal Memory surface speak in memories, projects, delivery, changes, decisions, and backups: bare health now gives plain readiness and exact next actions, human status omits engine counts, current hides source IDs and locators behind JSON, public commands use `--project` while hidden protocol keeps `--scope`, routine help drops ledger and canonical language, and doctor retains complete forensic diagnostics.
- Let people address memories by displayed first-line title or unique literal text for revision, history, and preview-first deletion, with deterministic ID precedence, project scoping, fail-closed ambiguity, readable `current` headings, and stable IDs retained for automation.
- Add content-free `convos memory audit` for current or all scopes, covering current and historical revision evidence with verified/changed/missing/unavailable counts, exact read pivots, unique-prefix reverse lookup from an archive message to its supported memories, and corrected archive-level unavailability semantics.
- Add revision-scoped memory evidence with repeatable `remember --from MESSAGE_ID`, exact local archive pivots, live verified/changed/missing status, scope checks, content-free ledger records, backup and deletion coverage, schema-v5 migration, and a hard boundary that excludes conversation provenance from remote sync.
- Add direct `--cwd` and `--conversation` filters to literal and hybrid retrieval, replacing the deferred custom query language, plus a development-only offline JSONL exact-ID evaluation harness for literal/hybrid comparison, hit@k, MRR, content-free exact read pivots, and threshold-based CI failure.
- Add the optional `ai-convos-resume` product: deterministic project handoff combines live Git branch, HEAD, and bounded status with path-isolated recent sessions, exact last-turn IDs, touched files, tool statuses, secret-scrubbed bounded excerpts, explicit untrusted-evidence labeling, JSON output, and exact `read --around` pivots; `convos replay` exposes bounded exact messages, tool inputs/outputs, statuses, durations, and before/after file edits without generation, network access, or inferred completion claims.
- Add the standalone `ai-convos-redact` product and make it a mandatory remote-client dependency: local scans report only secret type and exact record location, every team event is scrubbed inside the pre-encryption publish boundary, personal sync stays lossless, redaction markers contain no secret-derived fingerprint, binary attachments are omitted without being read, and a private value-free audit ledger records automatic removals.
- Add the optional read-only `ai-convos-explore` product with local semantic `convos related` pivots and bounded cycle-free multi-hop `convos trail` graphs from exact conversation or message IDs, with conversation fingerprints, strongest-turn evidence, text/JSON/JSONL/DOT output, injected-scaffolding exclusion, and exact-duplicate collapse; hybrid indexing and health counts now skip that scaffolding too.
- Synchronize canonical memory automatically across configured personal devices through the existing signed end-to-end encrypted remote, with versioned optional package discovery, watcher-safe root resolution, path-free repository scope, bounded non-lazy encrypted parts for large records, out-of-order and replay convergence, tombstones, and conflict-preserving three-way resolution instead of last-write-wins.
- Propagate user-owned memory deletion through personal sync with safe remote-only canonical and decrypted-event purging, author-bound relay ciphertext removal, permanent signed replay-denial proofs, conflict preservation for local/provider/projected state, recreation support, and explicit historical-backup retention boundaries.
- Restore supported older private memory snapshots through a validated in-memory migration without modifying the source snapshot, while continuing to reject malformed tables, triggers, cross-scope links, failed integrity checks, and unknown newer schemas.
- Deliver synchronized memory automatically through Claude Code and Codex `SessionStart` hooks, with nullable Codex transcript handling, bounded live Codex trust diagnostics, one-time trust guidance, dual health reporting, and preflighted atomic-per-file configuration that prevents partial installs or removals.
- Make bare `convos memory` a delivery-aware project health surface, keep the default command list human-sized while retaining callable agent protocol commands, and render irregular memory plurals correctly.
- Render `memory current` as readable canonical content and exact provenance by default, including missing and pending source state, with explicit `--json` for agents and automation.
- Give `remember`, `forget`, and `history` readable defaults with exact IDs, scope, source, revision counts, confirmation guidance, and chronological content while preserving structured `--json` modes.
- Add atomic private full-ledger snapshots plus preview-first, schema-validated restore with an automatic pre-restore rescue snapshot.
- Carry memory scopes across clones, worktrees, restored machines, and SSH/HTTPS origin forms through private Git evidence while preserving fork isolation, migrating v1 ledgers without rewriting identities, and failing closed with preview-first explicit adoption for ambiguous legacy splits.
- Preflight both agent hook configurations before `disable --remove-projection` can delete its owned Claude artifact, preventing partial destructive failure on malformed settings.
- Add the independently versioned, one-spec-installable `ai-convos-memory` product with a bounded core `memory` extra, complete wheel metadata and public command help, fail-closed one-time global enable with optional counts-only all-project warm-up, idempotent disable with preflighted alias-aware atomic skill delivery, live provider-aware doctor diagnostics, private symlink-safe versioned ledger creation, custom Codex/Claude home support, permission-preserving fail-closed agent settings updates, Git-root-normalized multi-scope provider refresh and migration-safe metadata-derived Claude scopes, direct revisioned user-owned `remember`, `current --owned` auditing and counts, plus dry-run and provider/projection-safe `forget`, project-safe semantic defaults and concise projection previews, human-first scoped and global-safe `sync`, per-project status, and three-way review, readable precedence-safe exact-ID-addressable explicit and automatic bounded indexes plus full small-scope first-session context with explicit misses, traceback-free shape-validated agent injection, exact Codex/Claude revision tracking with live structured source provenance and self-describing history, race-tolerant automatic read/delivery-time lone-source bootstrapping and exact-match linking, versioned transactionally previewable scope-isolated semantic reconciliation, engine-owned identities, stale-aware context, and reversible session-derived Claude projection with drift and symlink protection.

## 0.6.0

- Add signed workspace-state chains for client-verifiable membership, roles, devices, removals, history entitlements, and epoch keys.
- Let an authorized device approve another device for the same user with matching access, including selected-history inheritance.
- Add strict-majority team recovery and separate history activation with one vote per represented user.
- Enforce proposal timing with the server clock and harden relay metadata, proposal, vote, rejection, and history-envelope validation.

## 0.5.0

- Add optional self-hosted end-to-end encrypted personal and team synchronization.
- Add immutable signed events, device enrollment, recovery, epoch rotation, history grants, and opaque server storage.
- Add path-independent Git provenance, cross-repository changesets, checkpoint gaps, and typed local graph views.

## 0.4.0
- Add bundled Codex and Claude Code skills plus just-in-time lifecycle hooks with crash-safe, idempotent queue draining.
- Make literal and hybrid retrieval conversation-first, bound structured output, expose exact message IDs, and add `convos read` for recent or hit-centered context.
- Simplify hybrid ranking to BM25 + embedding Reciprocal Rank Fusion, removing the second local reranker model and reducing cold query latency.
- Add `convos doctor` archive, schema, FTS, embedding, hook, queue, and browser-cookie health checks with concrete repair commands.
- Capture current Codex custom tool calls, outputs, and exact `apply_patch` hunks for local code provenance.
- Add the optional changegraph commands: `blame`, `timeline`, `at`, `graph`, and `browse`.
- Keep DuckDB locks short, wait for active writers, defer FTS rebuilding until retrieval, and preserve superseded message/tool/edit payloads as history.
- Add full sync reconciliation, ChatGPT thread-parent metadata, and incremental local/web import improvements.

## 0.2.0
- Add hybrid semantic search with `convos query`: BM25 + local embeddings + Qwen3 reranking.
- Add `convos embed` to backfill embeddings without fetching new web conversations.
- Preserve existing embeddings during sync unless message content changes.
- Document hybrid search setup and database schema.

## 0.1.3
- Auto-discover Chrome profiles for ChatGPT sync.

## 0.1.2
- Fix ChatGPT web sync for workspace accounts.
- Add optional parse error logging and Chrome profile selection.

## 0.1.1
- Sync output: per-service updated/new convo counts, totals, and timings with -v.
- Fix Claude no-op sync to avoid full re-fetch when unchanged.
- Improve local sync to only reparse changed Codex/Claude Code sessions.
- Add repo-local UV cache wrapper and install script cache default.
- README install command and headings cleanup.
