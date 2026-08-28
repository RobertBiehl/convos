# Author-scoped provider session aliases

Status: accepted design for the 0.11 schema epoch, 2026-08-28

## Outcome

Two devices belonging to one user must converge when they imported the same exact
provider session under different conversation row IDs. The same provider session
ID imported by different users must remain two unrelated origins. Convergence must
not rewrite a released physical-ID recipe or infer identity from content, cwd,
repository, title, timestamps, or transcript similarity.

The durable identity assertion is a user-root-signed `provider.session` semantic
object. Ordinary conversation, message, tool, attachment, artifact, and edit rows
remain device-signed logical rows. The semantic object authorizes reconciliation;
it never replaces a row proof or changes the body authenticated by one.

## Object

An active object has this exact body:

```json
{
  "v": 1,
  "kind": "provider.session",
  "id": "provider-session:<sha256(canonical JSON [source, session_id])>",
  "state": "active",
  "data": {
    "source": "codex",
    "session_id": "provider-native-id",
    "members": ["source-conversation-row-id-a", "source-conversation-row-id-b"],
    "canonical": "source-conversation-row-id-a"
  }
}
```

`members` is a sorted, unique, non-empty list of logical conversation source-row
IDs. `canonical` is exactly its lexicographically first member. Only exact
`metadata.session_id` evidence from the provider contract may create membership;
legacy filename aliases and inferred relationships are ineligible.

The proof is the existing user-root-signed semantic proof. Storage and comparison
are keyed by `(author_user_id, object_id)`, so equal provider IDs from different
authors never meet. Concurrent leaves merge by set union, with every leaf revision
named as an ancestor. Union and the canonical choice are deterministic, associative,
commutative, and idempotent. A device must retain every leaf until a signed merged
descendant covers it.

Alias replicas travel only through the author's personal workspace. A team may
receive the resulting row successors and tombstones under its existing policy, but
team membership never creates or merges provider identity.

Core owns an additive durable proof ledger:

```text
remote.provider_session_aliases(
  author_user_id, object_id, revision, source, session_id,
  members JSON, canonical_source_row_id, proof JSON,
  PRIMARY KEY(author_user_id, object_id, revision)
)
```

Only leaf proofs and the complete ancestor set needed to verify them must remain
materialized. `provider_sessions` remains a local binding from exact or legacy
provider locators to physical conversation IDs; it is neither replicated nor used
as wire identity.

## Reconciliation

Reconciliation starts only after the merged alias proof and every named member's
current row proof and body are present. It computes the complete dependency closure
before mutation: conversation, messages, message parents, tools, attachments,
artifacts, edits, and attachment bodies.

For each non-canonical member, the local device creates ordinary signed successors:

1. rows whose conversation reference changes retain their logical row ID and get a
   new body and proof extending their one current head;
2. the losing conversation gets a bodyless deleted successor extending its current
   head;
3. old proofs remain retained and ingestible, while current projection resolves all
   live dependents to the canonical conversation;
4. local provider bindings point to the canonical physical projection only after
   the signed successors project successfully.

No content row is mutated under an old proof. No device may reconcile through an
incomparable row-proof fork, a missing body, an unavailable dependency, conflicting
exact provider evidence, or a different author. Those cases remain visible and
block that alias without blocking unrelated ingestion.

The canonical member is a logical source-row ID, not a physical ID recipe. Foreign
projection continues to use the released `(author, table, source_row_id)` mapping.
New clients therefore ingest every retained v1 logical replica exactly as before.

## Migration and interruption

The local duplicate migration is a separate, backed-up step after the alias ledger
exists. It records every exact duplicate group as one pending alias and performs no
destructive cleanup. Reconciliation then uses the same signed path as cross-device
convergence. Commit order is:

```text
backup -> install additive ledger -> sign/receive merged alias
       -> stage complete row successors -> project one core transaction
       -> publish replicas -> mark alias settled
```

An interruption before projection leaves the archive unchanged. An interruption
after projection is replayable from core change tracking and retained proofs.
Retrying any stage is idempotent. FTS and embeddings are repaired after the archive
transaction and cannot roll it back.

## Required tests

- two devices, one author, two row IDs, either arrival order;
- same provider ID under two authors remains separate;
- concurrent alias leaves merge with all ancestors retained;
- copied legacy filename alias does not become semantic membership;
- pre-existing local duplicates migrate from a verified backup without row loss;
- missing dependency, row fork, conflicting provider evidence, and interrupted
  projection each fail before partial archive mutation;
- loser successors and tombstones re-ingest on a fresh device using v1 row encoding;
- a second sync emits no alias, row, or migration changes.
