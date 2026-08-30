---
summary: "Specification for self-hosted, end-to-end encrypted personal and team synchronization over a global provenance graph."
read_when:
  - Implementing the remote protocol, server, client, or provenance projection
  - Reviewing encryption, membership, recovery, or sharing behavior
  - Verifying the remote synchronization Definition of Done
status: accepted (2026-07-10)
---

# Self-hosted encrypted synchronization

## Product goal

Convos synchronizes a global provenance graph across a person's computers and
authorized team members without giving the server plaintext. Normal use is
automatic: local hooks capture work, a background client publishes signed
events, and other clients update local search and graph projections. Explicit
sync exists only for repair and backfill.

The graph is global in model, not globally readable. A client assembles the
union of personal, team, and eventually public subgraphs it is authorized to
decrypt. A workspace is an encryption and access-control projection, never the
owner or boundary of a conversation, changeset, repository, or file lineage.

## Architecture and trust boundary

```text
provider transcripts -> canonical convos DuckDB -> proof/replica encoder -> encrypted outbox
                               |                                  |              |
                       typed graph views                       HTTPS relay
                                                                    |
                                                        opaque delivery cache
                                                                    |
             canonical convos DuckDB <- verified projector <- encrypted replicas
```

- DuckDB is the canonical local archive and typed provenance store, not a wire
  format.
- Changegraph is a read-only typed view over DuckDB; there is no second graph
  database.
- Logical-row and personal-memory origin proofs are durable peer-held evidence.
  Workspace controls remain signed events.
- The server stores ciphertext, public device records, workspace ACLs, key
  envelopes, opaque event headers, cursors, invitation state, and quotas.
- The server accelerates delivery and recovery but is not canonical archive
  storage. Authorized peers can rebuild retained rows, proofs, and blobs.
- Semantic search, embeddings, Git inspection, and graph queries execute only
  on authorized clients.

A database, backup, or passive relay compromise can disclose accounts,
workspace membership, device public keys, event authors, epochs, timing, sizes,
IP addresses, and access patterns. It does not disclose conversation text, tool
data, paths, repository URLs, project names, embeddings, file contents, Git
fingerprints, or workspace keys. An actively malicious relay can deny service,
roll back state, or manipulate ACL and membership views before a later signed
rotation; v1 does not provide a transparency log or MLS consensus. The protocol
also does not hide ciphertext length or prevent traffic analysis, endpoint
compromise, or an authorized recipient from retaining plaintext.

## Identities

- `user_id`: server account and human membership identity.
- `device_id`: one installation, with independent authentication token,
  Ed25519 signing key, and X25519 encryption key.
- `workspace_id`: personal or team encryption and ACL scope.
- `epoch`: monotonically increasing workspace-key generation.
- `event_id`: SHA-256 of the canonical signed event body.
- `entity_id`: author-scoped immutable origin identity; never assumed globally
  equivalent to an existing local conversation or provider id.
- `repository_id`, `file_id`, `changeset_id`: graph entities asserted by events,
  independent of a workspace and allowed to cross repository boundaries.

Absolute checkout roots and device filesystem identities are local-only. A
checkout maps a local root to repository evidence. Shared paths are repository
relative or opaque external-file identities.

## Cryptographic profile v1

Use `cryptography` recipes and primitives, never local crypto implementations:

- Ed25519 signs device certificates and event bodies.
- X25519 + HKDF-SHA256 + AES-256-GCM seals workspace keys to devices.
- AES-256-GCM encrypts signed event bodies with a fresh 96-bit random nonce.
- SHA-256 identifies canonical event bodies and ciphertext blobs.
- A random 256-bit recovery key encrypts the user root and personal-workspace
  keyring. Team keys require administrator-approved device envelopes.

Canonical JSON uses UTF-8, sorted keys, compact separators, no NaN, and integer
protocol versions. The envelope header is AEAD associated data. It binds the
protocol version, workspace, epoch, event id, author device, and device
sequence. The decrypted event author and id must equal the header. The event
signature must verify against the registered author key.

Before sealing any key to a device, clients verify that the device certificate
is signed by the root key committed to by its user ID and binds the device ID,
Ed25519 key, and X25519 key. Team invitations use a user ID exchanged through
an authenticated channel; relay-provided display-name lookup is not an
authenticated identity-discovery mechanism.

This profile provides authenticated encryption, authorship, tamper detection,
and future-key exclusion after epoch rotation. It does not provide message
ratcheting or forward secrecy after compromise of an old workspace key. MLS is
a possible later key-management profile, not a v1 dependency.

## Event format

```json
{
  "v": 1,
  "id": "sha256(canonical body without id/signature)",
  "kind": "conversation.record",
  "entity": "origin-scoped id",
  "revision": "sha256(canonical payload)",
  "author": "device id",
  "seq": 42,
  "parents": ["previous device event", "causal input event"],
  "observed_at": "RFC3339 UTC timestamp",
  "payload_v": 1,
  "payload": {}
}
```

The signature covers every field except `signature`; the event id covers the
body before `id` and `signature` are added. Client dispatch is exact on
`(kind, payload_v)`: unknown event semantics block readiness until upgraded.
Events are never edited. Corrections, key changes, and control updates are new
events. Semantic archive and personal-memory objects use the independent proof
format below rather than this event chain.

Required initial semantic kinds:

- `conversation.record`, `message.record`, `tool.record`, `attachment.record`,
  `artifact.record`, `file_edit.record`
- `repository.observed`, `git.checkpoint`, `file.version`, `edit.observed`
- `checkpoint.link`, `workspace.policy`, `workspace.membership`

Archive rows and provenance facts carry independently signed logical-row
proofs. Workspace policy and membership remain signed events. Provenance facts
carry semantic references, not copied content: edit identity is
`file_edits.id`, changeset identity is the existing message/turn, and prompt
text is resolved from `messages` after projection.

Personal-memory canonicals carry user-root-signed semantic proofs. Each proof
binds object kind and ID, encoding version, content hash, active/deleted state,
author user and device, personal workspace, authorization epoch, direct
predecessor, and the complete ancestor-revision set. The proof is independent
of delivery encryption and can be retained or re-uploaded by any authorized
holder without the author's private key. Only the author root can sign a new
revision. A deleted revision has no semantic body and defeats all ancestors
named in its proof.

Provider-session aliases use the same root-signed, multi-ancestor proof envelope.
They union exact provider-session membership for one author and select the
lexicographically first logical conversation source-row ID. The alias is identity
authorization only: row bodies and deletions still require ordinary row-proof
successors. See [Author-scoped provider session aliases](../provider-session-aliases.md).

## Envelopes and idempotency

Canonical archive rows and provenance facts carry independently signed origin
proofs and travel in separate repairable delivery replicas. A proof records both its immutable
origin workspace and the workspace/epoch that authorized that revision. They
are normally identical; after explicit team re-founding, a successor keeps its
old origin and predecessor while naming the fresh authorization workspace.
The old signed control chain is encrypted once per destination as a shared
origin bundle rather than copied into every row replica.

An event is signed, then encrypted independently under its workspace epoch key.
The server accepts `PUT(workspace_id, event_id)` only from an active device in
that workspace. Repeating the identical upload succeeds without allocating a
new cursor. A different ciphertext for an existing event id is rejected.

The server assigns an increasing delivery cursor after atomically persisting an
envelope. Pull is `events after cursor`, may return duplicates or out of order,
and carries no semantic ordering guarantee. Clients deduplicate by event id,
verify before persistence, and project in deterministic `(observed_at, id)`
order where no causal relationship exists. Per-workspace, per-device `seq` and
previous-event parents detect replay and interior gaps in either arrival order;
clocks never establish causality.

For personal-memory deletion, the author publishes a bodyless signed semantic
descendant. Recipients verify its root signature and ancestry, then remove an
unchanged remote-only memory and its local revisions. A locally changed memory
or one with another provider origin or active projection survives, with the
remote origin marked missing. The relay retains opaque replicas and need not
understand deletion. Existing peers and backups may retain older ciphertext;
the tombstone governs convergent current state rather than proving erasure.

Large auxiliary event bodies use encrypted lazy events. Personal attachments
use independently repairable, content-addressed blob replicas capped at 32 MiB;
their signed row carries the expected hash. Pull validates and atomically places
the body in archive-owned storage. Team workspaces publish an explicit
attachment placeholder and never read or upload the binary body.

## Membership, devices, and recovery

The first device creates a user root, personal workspace, epoch key, and random
recovery key. Server login tokens authorize API access but cannot sign events or
decrypt data. The user root signs device certificates. An existing device or a
recovered user root can enroll another device.

Workspace admins sign membership events and key-control requests. Every add or
removal advances the epoch and creates key envelopes for every currently
authorized device. Removed devices receive no new envelope and cannot decrypt
future events. They retain all plaintext and old keys already obtained.

Every epoch-advancing control also signs the exact relay ledger boundary:
workspace epoch and tail plus each author's latest sequence and event ID. The
relay verifies that snapshot atomically with the control update. Full-history
recovery validates chains from genesis; future-only recovery starts from the
signed boundary and requires every subsequent sequence to be contiguous.
Missing interior events or signed checkpoints block publication. A malicious
relay can still hide a newest suffix after the latest signed boundary because
v1 has no gossip or transparency log.

New members receive only the new epoch by default. Complete-history grant seals
all retained old epoch keys to their devices. Same-user device approval rewraps
those epoch keys to the new device. Work requiring a different audience uses a
different workspace; the protocol has no per-row history grants.

The recovery bundle contains the user root private material and personal
workspace keyring, encrypted by the recovery key and stored as an opaque server
blob. Team keys are excluded: a newly recovered team device must receive the
current epoch through an administrator-approved rotation. Loss of every
enrolled device and the recovery key is permanent personal-workspace data loss.

## Automatic personal and team projection

Personal policy is `all`: every captured conversation record and provenance
event is encrypted to the personal workspace without path or repository
allowlists. Users can opt out with local exclusions, but never need to opt in.

Team policy routes whole conversations by stable repository identity or an
opaque machine-local path binding. A match by conversation working directory or
any captured edit selects the conversation atomically: every turn, tool, edit,
artifact, and provenance fact belonging to it is published. Policies never
slice a turn or materialize a conversation with silent holes. Textual secret
redactions and unsafe binary attachment omissions remain explicit typed markers,
so the receiver can distinguish protected content from absent data.

## Git and file evidence

Git is a durable checkpoint layer below the temporal resolution of edit events.
A repository observation contains encrypted normalized remote fingerprints,
root/anchor commit ids, and ancestry evidence. Logical repository identity uses
normalized non-local remotes when available, while a separate lineage id uses
root commits. Clones can match; forks remain distinct repositories connected by
shared lineage. URLs and absolute roots are never server-visible.

`git.checkpoint` records HEAD, an index/worktree digest, dirty paths, capture
source, and observation time. It is capture-time context, not a complete
repository snapshot or inferred turn baseline. An edit links to it only when
complete captured content exactly matches the observed file version. Unrelated
dirty paths and theoretical tool side effects never become capture gaps.

File identity is repository plus normalized relative path, or an opaque
external identity. Explicit moves remain captured operations; equivalence and
likely renames remain read-only hints until a concrete use case earns a typed
canonical relation.

## Stable application contract

Applications consume typed projection APIs/views, not envelopes. Initial views:

- `file_history`
- `changeset_files`
- `conversation_changes`
- `commit_conversations`
- `repository_activity`

There are two installable packages. The remote client contains internal modules
for canonical encoding, crypto, enrollment, keyring, synchronization,
background work, DuckDB projection, Git inspection, and typed graph views. The
remote server contains opaque storage, ACLs, and the minimal certificate
validation required at its trust boundary. None belongs in the 1,000-line core
CLI.

## Operational invariants

- Hooks enqueue only local identifiers and return without network or Git work.
- Background workers scan, enrich, encrypt, upload, pull, verify, and project.
- Retrieval never waits for the network.
- Explicit sync inventories retained row proofs, memory semantic proofs, and
  blobs against the relay; controls reconcile through the event ledger.
- Server database and blob directory may be backed up together from a
  consistent snapshot for fast restoration, but are not canonical row
  authority.
- Forget does not rewrite historical client or relay backups. Operators must
  choose and enforce backup retention appropriate to their deletion policy.
- Real archives and evaluation datasets remain local and untracked.

## Completion evidence

Automated acceptance must run one server, two devices for one user, and a second
user with two devices. It must prove enrollment, automatic personal delivery,
team policy projection, default-future-only invitation, explicit all-history
history, removal and rotation, tamper rejection, retry idempotency, offline and
out-of-order convergence, crash recovery, backup/restore, empty projection
rebuild, cross-repository changesets, checkpoint gaps, local-only queries, lazy
blobs, and hook p95 below 100 ms. The test uses synthetic fixtures; a separate
local-only benchmark may use the real archive and must publish no content.

## Non-goals

No Git hosting, server plaintext search, searchable encryption, enclave compute,
perfect attribution of unobserved changes, automatic resolution of every
identity ambiguity, public federation, mobile/web client, generalized graph
database, or claim of production-grade cryptographic assurance without an
independent review.
