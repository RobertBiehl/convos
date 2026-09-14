# Parser evolution and archive cleanup

Implementation: core schema 14 and parser epoch 6, released in v0.11.6b11.
These changes require updated clients; they introduce no relay protocol or
signed logical-row encoding change. The [product invariants](invariants.md#parser-evolution)
remain the release gate.

## Mixed native and received rows

The b10 receiver could reuse a received child's identity without preserving its
native parent binding. Parent resolution now preserves each known binding
independently of the child's physical identity, including parents arriving in
the same batch. Bindings are author-scoped; another author's source ID cannot
select a native row. No parser or provider format change is required to trigger
this replay defect.

The backed-up schema migration repairs dangling physical references only when:

- The child's origin and active proof agree, and its body matches that proof.
- An explicit, author-scoped reference maps the missing physical parent to its
  logical source ID using the released author/kind/source physical-ID recipe.
- That source ID names an existing native parent. At migration time a local
  attestation base establishes the parent's author; during receive the
  authenticated local account supplies that scope.
- The complete reconstructed logical child body remains unchanged by relinking.

Repair operates in pages inside the core transaction and updates change tracking.
Receive also revisits affected parents so delivery order and replay cannot undo
repair. Unproven references remain visible gaps.

## Source-backed parser lineage

The parser reconstructs supported historical representations from provider events
and matches their complete normalized logical-body hashes to existing locally
owned rows. Similar text or timestamps alone never establish replacement.

Tool recipes cover former Claude Code indexed call/result rows and former Codex
indexed function-call/result rows, including known parent attribution and
timestamp variants. Provider call identity ties each candidate to the current
invocation; ambiguous reused call IDs do not qualify. Known old session bindings
are considered even after the transcript moved paths.

Claude Code message recipes cover the historical indexed parser, including its
omission of thinking-only parents and known model, metadata, parent, and timestamp
representations. Source events, the old session identity, and complete body
hashes establish which current message replaces an old child. Current parsing
retains thinking-only and empty source message events so thread topology survives.
Unsupported historical recipes remain unresolved.

Verified pairs are carried in ordinary metadata with this versioned shape:

```json
{"v":1,"records":[{"old_id":"<logical row ID>","old_hash":"<SHA-256>",
 "current_id":"<logical row ID>","current_hash":"<SHA-256>"}]}
```

Tool evidence uses `metadata.convos_tool_lineage` on the owning message. Message
evidence uses `metadata.convos_message_lineage` on the owning conversation, keeping
the message hash independent of its lineage carrier. Fresh imports without
matching legacy rows add no lineage claims. Core preserves already signed
carrier revisions before recording successor metadata.

Parser epoch 6 makes unchanged local transcripts eligible for reprocessing.
Without those source files, a client cannot discover new native replacement
claims. A receiver can apply an author's signed claims without the transcript or
private key. Core materializes `parser_tool_lineage` and `parser_message_lineage`,
checks full old and current bodies under the evidence author's identity, and
rejects missing, changed, cross-author, or ambiguous matches. Delivery of either
the carrier or its referenced rows rechecks the claims. Migration also rebuilds
claims already present in stored metadata.

## Removing obsolete active rows

Verified old tools and messages are physically deleted from their active tables
when the current replacement has its physical parents and no active dependent
needs the old row. Core then removes an empty obsolete conversation associated
with those verified message replacements and updates its provider bindings.
Unique messages, edits, attachments, artifacts, and tool evidence block the
relevant deletion; they are never dropped merely to equalize counts.

`parser_retired_rows` retains the historical logical body once, its hash and
replacement, and the small physical-ID/local-field mapping needed for exact
restoration. Signed proofs and required historical bodies remain auditable and
repairable. This is historical evidence outside the active conversation tables,
not a second searchable conversation copy. `parser_tool_history` and
`parser_message_history` expose unambiguous matches while their current carriers
and replacements remain present.

A replay of the exact retired body is removed again, including after a later
replacement revision or a backup merge. A changed body under the old identity is
preserved. If a unique late child needs an old parent, core restores the exact
retained parent and its ancestors; that dependency then prevents deletion. A
hash mismatch during restoration rolls back the transaction.

Schema upgrades use the existing private backup and memory-limit machinery. A
failed transaction preserves the previous schema version for retry. Cleanup and
its durable evidence are core-owned and included in backup recovery; they do not
require an optional product to remain installed.

## Evidence and remaining scope

The September 14 v2 maintainer report attributes 1,860 Claude Code thread gaps
to thinking-only parent turns omitted by the historical parser. It reports exact
child ID, text/thinking hash, timestamp, and parent ID checks against available
source events. Original production transcripts are excluded from that package;
this population attribution is supplied evidence, not an independently repeated
census.

An invented three-event transcript independently reproduces the old omission
and dangling answer parent. Rebinding the provider session produces different
child IDs and previously left old copies alongside the current representation.
b11 retains the thinking parent and reconciles verified historical copies across
those bindings. No provider file-format change is needed to reproduce this case.

The supplied report leaves 8,675 other Claude Code and 739 ChatGPT thread gaps
unclassified. A historical ChatGPT code-node omission is independently
reproducible and already fixed in b10, but the affected production mappings are
absent. All 11,274 reported gaps already exist in the September 5 backup according
to the supplied comparison. These facts do not establish the cause of the
unclassified rows, and b11 does not blanket-delete them.

## Validation

`tests/test_parent_reconciliation.py` covers mixed archive child types, signed
successors, migration backups, transaction interruption, idempotence, and unsafe
binding refusal. `tests/test_parser_evolution.py` covers both providers, old-output
upgrade, chunked ingestion, signed replay without transcripts, delivery order,
changed bodies, and author isolation. `tests/test_parser_retirement.py` covers
thinking parents, rebound and moved sessions, actual active-row deletion,
re-import, replay, unique dependencies, late parent restoration, migration retry,
and backup merge. The full non-integration suite and clean package installation
remain required release checks.
