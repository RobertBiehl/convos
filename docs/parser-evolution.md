# Parser evolution and mixed-parent repair

Implementation: core schema 13 and parser epoch 5. These changes require updated
clients; they introduce no relay protocol or signed logical-row encoding change.
The [product invariants](invariants.md#parser-evolution) remain the release gate.

## Mixed native and received rows

The b10 receiver reused a received child's identity without preserving its native
parent binding. Parent resolution now preserves each known binding independently
of the child's physical identity, including parents arriving in the same batch.
Bindings are author-scoped; another author's source ID cannot select a native row.

The backed-up schema migration repairs dangling physical references only when:

- The child's origin and active proof agree, and its body matches that proof.
- An explicit, author-scoped reference maps the missing physical parent to its
  logical source ID, using the released author/kind/source physical-ID recipe.
- That source ID names an existing native parent. At schema migration time a
  local attestation base establishes the parent's author; during receive the
  authenticated local account supplies that scope.
- The complete reconstructed logical child body remains unchanged by relinking.

Repair operates in pages inside the core transaction and updates change tracking.
The migration uses the existing private backup and memory-limit machinery. A
failed transaction preserves the previous schema version for retry. Receive also
revisits affected parents so delivery order and later replay cannot undo repair.
No parent, message, or signed body is invented. Unproven references remain gaps.

## Historical tool representations

The supported recipes are the former Claude Code indexed call/result rows and
the former Codex indexed function-call/result rows, including their known parent
attribution and timestamp variants. The parser reconstructs candidate old bodies
from provider events and associates them with the current invocation by provider
call identity. Text or timestamp similarity alone does not establish lineage.

During native re-import, a candidate is retained only if an existing, locally
owned legacy tool matches its complete normalized logical-body hash. The owning
message receives a versioned `metadata.convos_tool_lineage` object:

```json
{"v":1,"records":[{"old_id":"<logical tool ID>","old_hash":"<SHA-256>",
 "current_id":"<logical tool ID>","current_hash":"<SHA-256>"}]}
```

Hashes cover the complete logical tool row, including its parent, input, output,
status, and timestamp. Fresh archives with no matching legacy rows store no
lineage metadata. Parser epoch 5 makes unchanged local transcripts eligible for
reprocessing. Existing signed message revisions are preserved by core before the
new metadata is attested as a successor.

The message carries that evidence through ordinary signed row replication. A
receiver needs neither the original transcript nor the author's private key. Core
projects the evidence into `parser_tool_lineage` and verifies the complete legacy
and current tool bodies under the evidence author's identity. Missing, changed,
cross-author, or ambiguously bound tools do not qualify. Tool arrival or revision
rechecks affected evidence, including when tools arrive after the message.

`parser_tool_history` exposes only unambiguous, fully matched replacements.
Original tool rows and proofs remain intact and queryable; conversation export
uses the current invocation. Relationship audits retain the physical gap count
and identify matching historical tools in `marked_history_rows`, so retained
history is distinguishable from an unexplained broken projection. A history
classification is not deletion permission.

Unique legacy edits, attachments, and unsupported identity recipes remain
preserved. This repair does not infer their content or upgrade their evidence
quality. Without source-derived lineage from an authorized author, older signed
rows remain ingestible and their unresolved relationships remain visible.
Thread-parent reconciliation has additional requirements described below.

## Thread-parent follow-up

The September 14 v2 maintainer report attributes 1,860 Claude Code thread gaps
to thinking-only parent turns omitted by the historical parser. It reports exact
child ID, text/thinking hash, timestamp, and parent ID checks against available
source events. Original production transcripts are excluded from that package;
the population attribution is supplied evidence, not a locally repeated census.

The invented three-event transcript independently reproduces the mechanism:
the historical parser emits the user and answer, omits the thinking-only parent,
and leaves the answer pointing at its absent ID. Both b10 and this branch still
omit that parent, but clear the answer's reference. Changing the provider-session
binding emits different child IDs, leaving the old representation unreconciled.
No relay or provider file-format change is required to reproduce these results.

The tool lineage implemented above does not reconcile these legacy message and
conversation identities. Completing this scope requires retaining thinking-only
turns and source-backed reconciliation across old and current session bindings,
while preserving signed message bodies and proofs. Nulling legacy parent fields
or deleting old children does not satisfy the invariant.

The report leaves 8,675 other Claude Code and 739 ChatGPT thread gaps unclassified.
The historical ChatGPT code-node omission is also reproducible, and b10 already
retains that synthetic parent, but the affected production mappings are absent.
All 11,274 reported gaps already exist in the September 5 backup according to the
supplied comparison. Neither that timing nor a synthetic mechanism attributes the
unclassified rows to a particular parser defect.

Validation used the v2 package's five thread evidence tests against exact b10
and repeated its parser reproduction against commit `942c831`. All five tests
passed; the current branch retains the limitations stated here. Validation used
temporary outputs and left the supplied archives and live installation unchanged.

## Validation

`tests/test_parent_reconciliation.py` covers mixed archive child types, signed
successors, migration backups, transaction interruption, idempotence, and unsafe
binding refusal. `tests/test_parser_evolution.py` covers both providers, old-output
upgrade and re-import, chunked ingestion, signed replay without transcripts,
delivery order, changed bodies, and author isolation. The broader parser and
Remote suites remain required before release.
