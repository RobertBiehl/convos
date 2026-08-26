---
summary: "Normative product invariants for archive ownership, recovery, replication, and provenance."
read_when:
  - Changing core ingestion or the DuckDB schema
  - Changing Remote state, relay recovery, or replication
  - Changing file-edit or provenance capture
  - Reviewing an architectural tradeoff
status: normative working agreement (2026-08-13)
---

# Product invariants

These are product boundaries, not descriptions of incidental implementation.
New behavior must preserve them or change this document deliberately. Open
questions do not become invariants by implication.

## Authority and ownership

> Core DuckDB is the canonical archive. It owns retained conversations,
> messages, tools, attachments, artifacts, file edits, and provenance facts.

> Ingestion, hooks, normalization, and canonical writes belong to core.
> Applications consume core data read-only. An optional application must never
> be required to prevent future data loss.

> `state.db` contains rebuildable synchronization metadata and bounded working
> memory, not canonical content. Losing it may cost time and bandwidth, but must
> never change authorship, publish foreign rows, or lose canonical user content.

> Local absolute paths describe where an identity was observed. They are not
> portable identity and are never required for remote reconstruction.

## Upgrade and recovery

> Core DuckDB upgrades are automatic and frictionless. Before the first
> migration of an existing archive to schema N, Convos checkpoints and
> validates a private `<database>.pre-vN.bak`. A failed upgrade must leave a
> usable original or recovery copy.

> A physical archive migration and relay compatibility are two halves of one
> upgrade. Retained signed logical encodings remain readable at ingestion;
> physical-ID changes repair already projected rows without changing relay
> ciphertext, logical row IDs, proofs, or signatures. Workspace identity is
> authorization evidence, never a serialized physical-ID recipe.
> Core migrations cap DuckDB at 1.5 GiB without raising a lower user limit and
> restore the connection setting only after the active transaction has ended.

> Rebuildable remote state may be discarded and recreated. During the current
> pre-establishment phase, Relay and `state.db` require no compatibility
> migration; protocol migrations may be introduced later when real deployments
> require them.

> Relay, `state.db`, or a core replica may disappear independently. The union of
> surviving authorized peers should restore currently retained semantic
> objects, original authorship, retained attachment bodies, and permanent
> deletion proofs. A replacement workspace deliberately establishes new
> controls and grants. Superseded revisions survive only when explicitly
> retained or pinned.

Current implementation status: archive rows, provenance, attachment blobs, and
personal-memory canonicals use peer-held repair proofs. Memory tombstones are
self-contained semantic descendants retained with the current object state;
they do not depend on relay or `state.db` history.

> Any currently authorized holder of an object may repair its relay replica
> using the original author's signed proof. The repairing peer never needs and
> never fabricates the author's private key.

> Every remotely repairable semantic object retains a compact, independently
> verifiable origin proof binding its workspace, semantic identity, revision or
> content-manifest hash, original publishing user and device, authorization
> epoch, and signature. The proof never duplicates the semantic body.

> A currently authorized peer may create a new delivery envelope around an
> intact object and origin proof. Delivery authority never permits replacing,
> reinterpreting, or forging the original signature.

> Randomized re-encryption creates a delivery replica, not new semantic
> content. The relay attributes replicas to their authenticated uploader and
> bounds them to one current envelope per `(row revision, epoch, uploader)`, so
> one peer cannot overwrite another peer's valid copy or amplify storage
> without limit.

> Clients evaluate uploader copies independently. One malformed delivery copy
> cannot mask a later valid copy of the same semantic object; readiness blocks
> only when every accessible copy fails decryption or verification.

> Relay loss does not require automatic reconstruction of its former authority.
> A personal owner may create a replacement workspace; a team may explicitly
> re-found one and invite the members it currently trusts. The replacement uses
> fresh access credentials, authorization, and workspace keys.

> A replacement workspace may accept portable intact row proofs without
> rewriting their original signatures. Original publication context and current
> destination authorization remain separate. An excluded member cannot access
> new data, although no protocol can erase data that member already decrypted.

> A row lineage keeps one immutable origin workspace. Every revision separately
> names the workspace and epoch that authorized its signer; after explicit
> re-founding, a valid successor may retain the old origin and predecessor while
> using the fresh workspace as its authorization context.

> A retained current-row replica carries the exact signed proof-header chain
> back to its root, but no superseded row bodies. Receivers verify every
> signature and direct predecessor link before using that chain to reject a
> stale ancestor or identify a true fork.

> A replacement relay rebuilds from the union of surviving authorized replicas.
> Peers advertise flat, paginated logical identities, revisions, deletion
> states, proofs, and blob hashes; the relay requests what it lacks. Repair is
> idempotent and may carry an intact original proof from any authorized holder.

> Reconciliation monotonically adds verified knowledge. Known descendants
> defeat stale ancestors, incomparable revisions remain visible, and later
> peers may fill earlier holes without requiring the original publisher to
> return. Data absent from every surviving replica is unrecoverable.

> Bloom filters, Merkle structures, and other reconciliation accelerators are
> added only after flat pagination crosses a measured performance limit.

> Automatic quorum recovery or preservation of the old workspace security
> identity is deferred until a concrete product need justifies its ceremony and
> protocol complexity.

> A team workspace is a uniform content-access boundary. Every member can read
> every retained row shared into it; roles govern workspace administration, not
> per-row visibility. Different audiences use different workspaces.

> New members receive either data from membership onward or all retained
> workspace history. Per-row selected-history grants are excluded until a
> demonstrated product need justifies their linear per-row and per-device
> cryptographic and operational cost.

> A repair replica remains encrypted under the earliest workspace epoch that
> authorized its delivery. Rotation never silently moves old rows across a
> future-only boundary; only an explicit all-history grant exposes old epochs.

> Repository and local-path policies route complete conversations into a
> workspace. A conversation matching multiple workspaces may be shared in each;
> work that must remain within one audience uses a separate conversation.

> A team repository policy is attributed to the member who linked it. Other
> members inherit automatic contribution by default and may disable it without
> disabling their own explicit links. Captured starting-directory and edit
> matching are independent member controls, both enabled by default. Local-path
> bindings remain device-local and never activate merely because another member
> published an opaque path token. Durable conversation admission is keyed by
> `(author_user_id, row_kind, source_row_id)`; another member's proof cannot
> admit a same-ID local row.

> `author_user_id` identifies the user under whose identity the immutable event
> or object proof was originally signed. Message role and provider source
> describe content authorship separately; the authenticated relay request
> identifies the current delivery peer. Existing signed fields are not renamed
> or rewritten to express this distinction.

> A remote logical row is identified by author, semantic kind, and source row.
> Workspaces authorize delivery of that identity and never participate in its
> physical archive ID. Equivalent access through another workspace adds a
> proof; an incomparable revision remains conflicted instead of creating a
> duplicate row or winning by arrival order.

## Retained semantic state

> Recovery preserves the newest valid causal revision of mutable semantic
> state, not every historical row revision. Relay arrival order and untrusted
> wall-clock time do not decide which revision wins. Concurrent incomparable
> revisions remain detectable.

> A retained conversation is an open-ended set of related logical rows. Every
> row remains independently useful and repairable; synchronization monotonically
> fills known data without requiring a declaration that a conversation is
> complete.

> Relationships remain explicit: messages reference conversations and optional
> parents, while tools, edits, attachments, and other children reference their
> owning row. A known missing reference may be shown as unavailable; unrelated
> rows are never hidden while repair proceeds.

> Attachment metadata is canonical. A retained attachment body is stored once
> in the content-addressed body store and referenced by the archive; database
> rows do not duplicate its bytes. The signed attachment row includes the
> expected body hash. Body ingestion is bounded by a 32 MiB per-body limit
> unless policy deliberately changes it.

## Signed replication units

> The independently signed content unit is a canonical logical row revision,
> never DuckDB's physical row encoding. There is no signed conversation
> inventory or atomic-completeness gate.

> The new replication protocol starts with a clean relay cutover and does not
> preserve today's relay event format. From that boundary onward, every signed
> semantic encoding version is immutable and remains verifiable for as long as
> a proof using it is retained.

> Physical DuckDB migrations never change signed meaning. A semantic field or
> meaning change introduces a new encoding version; old revisions keep their
> original bytes and signatures, while new revisions use the new version.

> A changed logical row names the revision it directly replaced. First
> revisions have no predecessor, unchanged rows create no revision, and
> naturally append-only rows normally never need a predecessor.

> A revision supersedes another only through this explicit causal link. Relay
> arrival order, capture time, and event ID never allow a stale revision to
> replace a known descendant. Two revisions replacing the same predecessor are
> a detectable conflict and neither is silently discarded.

> Local removal affects only one replica. Workspace deletion is a signed,
> bodyless `deleted` revision of the same logical row, linked to the active
> revision it replaces. Empty semantic content is not deletion.

> Peers retain and may repair the compact deleted revision after discarding the
> body. It defeats stale active revisions wherever at least one informed copy
> survives. Perfect no-resurrection is not claimed after every copy of the
> deletion proof is lost.

> Any authorized peer may deliver another user's immutable revision, including
> a deletion proof. Only that user's authorized devices may create a successor
> revision, including an undelete. Possession and workspace administration do
> not grant mutation or impersonation authority.

> Signing, reconciliation, transport, and blob storage are separate layers.
> Transport may compress and batch independently signed rows without changing
> their proofs. Attachment bodies remain separately content-addressed blobs.

> Peers may advertise flat row identities and revisions for anti-entropy repair.
> Such inventories are rebuildable sync indexes, not canonical conversation
> objects. Chunking or Merkle structures require measured evidence that a real
> size or reconciliation-cost limit was crossed.

> Compact recovery proofs target at most 10 percent additional storage relative
> to canonical archive content and must not exceed 15 percent without an
> explicit product decision based on measured data.

## Release gates

> The protocol ships only after a representative live archive measures proof,
> DuckDB, wire, relay, and `state.db` storage plus initial signing, incremental
> sync, verification, interruption recovery, and empty-relay rebuild time.

> Normal incremental work remains proportional to changed rows. `state.db`
> stays content-free except for bounded encrypted pending uploads. Flat
> reconciliation remains the design until measurement proves it inadequate.

> Acceptance tests destroy and rebuild `state.db` and the relay; repair one
> user's row from another user's intact proof; reject forgery and stale
> resurrection; preserve conflicts; restore retained attachments; retain origin
> attribution; and exclude removed members from a replacement workspace.

> Core DuckDB receives an automatic, backed-up migration. The present relay and
> `state.db` protocol are discarded at cutover. Locally owned rows receive new
> proofs; imported rows without an original proof cannot acquire cryptographic
> origin retroactively.

> Correctness and recovery claims require destructive tests and measured limits,
> not architectural intent.

## File edits

> Every file edit associated with a retained conversation is durable semantic
> data. Preserve exactly what the source exposed and explicitly describe the
> evidence quality; never invent a diff, line range, or file effect.

> Convos records observed edits, not an implicit repository backup. It does not
> copy unchanged or unobserved repository state.

> A full write provides complete new-file content. An edit or patch may provide
> exact old and new fragments without providing either complete file version.
> Arbitrary commands have no file provenance unless the source exposes a
> concrete file operation.

## Repository and file identity

> Repository and file identities survive machines and worktrees. A repository
> is identified from durable Git evidence; a repository file is identified by
> repository plus normalized relative path. A checkout is an observation of a
> repository, never the repository identity itself.

> Different worktrees may have different branches and heads while referring to
> the same repository and file identities. Files outside a resolved repository
> receive opaque identities; equal-looking local paths are not assumed equal.

> A repository sharing grant has its own stable opaque identity. Git lineage and
> normalized remotes are matching evidence, not the grant identity. An exact
> local checkout binding survives mutable remote metadata; an unbound checkout
> must match the original evidence, and an ambiguous or unrelated fork does not
> inherit the grant. Replacing a checkout in place does not transfer its binding;
> legacy identities with conflicting live evidence are quarantined.

> Conversation starting scope and edit repository scope are capture-time facts.
> Later `git init`, remote changes, moves, nesting, or `.git` removal do not
> retroactively reclassify old conversations. A removed repository binding is
> dormant, not an implicit recursive path policy.

> Archive ingestion commits before Git enrichment. A delayed enrichment may
> retry only while the captured checkout marker still identifies the same
> filesystem object; otherwise the pending scope resolves to unknown/external.

> Core has no generic identity-assertion relation. Explicit moves remain typed
> captured operations; likely renames or equivalence remain read-only hints.
> A new canonical relationship requires a concrete product use case and a
> deliberately typed schema. Similar names or content never establish identity.

## File versions and repository observations

> A file version is a cryptographic hash of complete file bytes that were
> actually observed. Hashes of edit fragments are edit evidence, not file
> versions. A hash proves equality but is not a stored backup.

> Identical `(file identity, content hash)` versions may be deduplicated. The
> corresponding content is not duplicated when it is already retained by an
> edit, attachment body, or another canonical store.

> Convos records exact file states and Git anchors actually observed around
> edits. It does not require full pre-turn repository snapshots and does not
> need to know in advance which files or repositories a turn will touch.

> A turn touching multiple repositories produces independent repository
> observations discovered from its edits. A checkpoint may group observations
> made together, but never implies complete repository capture. A whole-tree
> diff hash is supplementary evidence, not the provenance foundation.

## Evidence, hashes, and gaps

> Provenance states positive knowledge and the strength of its evidence. Lack
> of evidence limits a claim; it does not by itself create a capture gap.

> Evidence has three explicit levels: `verified` for an exact mathematical or
> authenticated relationship, `observed` for a fact directly seen at a stated
> time and place, and `inferred` for a useful interpretation of observations.
> Inference is never silently promoted to observation or verification.

> Core stores compact source observations, including checkout, head, branch,
> capture source, event time, capture time, and exact hashes where available.
> Read-only applications may derive and improve labeled hints without rewriting
> canonical evidence.

> No capture gap exists without a concrete expected observation. Gaps represent
> detected failures such as an interrupted known edit capture or a failed
> verification across an explicitly captured continuous boundary. Arbitrary
> commands, unrelated dirty files, and theoretical side effects do not create
> gaps.

> A hash mismatch is a gap only when Convos captured both sides of a boundary
> that was expected to be continuous. A later filesystem observation may
> legitimately differ because more edits occurred; that mismatch is not a gap.

> Edit-to-version links exist only when exact material permits verification. A
> missing complete before- or after-version is ordinary evidence quality, not a
> failed verification.

## Git association

> `HEAD` is qualified by when and where it was observed. A head seen during
> later ingestion is not presented as the turn's starting commit.

> Git and time correlation may produce useful, explicitly labeled hints. Only
> exact repository, path, and content relationships produce verified
> associations. A commit may be verified when its blob contains a complete
> observed file result.

> Inferred relationships may improve retrieval and explanation, but never
> control authorship, replication, deletion, authorization, or another safety
> decision.

> The tool record establishes the observed edit. A matching Git blob establishes
> that the result appears in a commit; it does not claim that the conversation
> authored every other part of that commit.
