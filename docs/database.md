---
summary: "Database schema: tables, columns, relationships, and FTS setup."
read_when:
  - Writing queries against the database
  - Adding new fields or tables
  - Understanding data model
  - Debugging search issues
---

# Database Schema

DuckDB database at `<root>/data/convos.db`. Default root is `~/.convos` (override with `CONVOS_PROJECT_ROOT`).

Core owns the `provenance` schema in this same DuckDB. It stores canonical
repository, file, hash, checkpoint, and edit-link facts while joining
prompts, conversations, and edits from the existing archive tables. Remote
submits verified facts through the core projector. Transport cursors and
device/workspace activity remain in `<root>/remote/state.db`; durable imported
row authorship and compact signed proofs live in the `remote` DuckDB schema. The main-schema
`archive_state` gives the file a stable identity and monotonic generation;
`archive_changes` keeps only each logical row's latest generation so normal
replication reads changed identifiers without retaining content history.

## Tables

### conversations

Primary record for each conversation session.

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash of `source:original_id` |
| source | VARCHAR | `chatgpt`, `claude`, `claude-code`, `codex` |
| title | VARCHAR | Conversation title or derived name |
| created_at | TIMESTAMP | First message timestamp |
| updated_at | TIMESTAMP | Last message timestamp |
| model | VARCHAR | Primary model used |
| cwd | VARCHAR | Working directory (CLI tools only) |
| git_branch | VARCHAR | Git branch (CLI tools only) |
| project_id | VARCHAR | Project/gizmo ID if applicable |
| metadata | JSON | Stable normalized session fields plus documented provider extensions; see the [provider conversation contract](provider-conversation-contract.md) |

### messages

Individual messages within conversations. Has FTS index.

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash |
| conversation_id | VARCHAR FK | References conversations.id |
| role | VARCHAR | Canonical `user`, `assistant`, `system`, `developer`, or `tool` |
| content | VARCHAR | Message text content |
| thinking | VARCHAR | Extended thinking/reasoning (Claude) |
| created_at | TIMESTAMP | Message timestamp |
| model | VARCHAR | Model for this specific message |
| metadata | JSON | Source-specific extra fields |
| embedding | FLOAT[768] | embeddinggemma vector for hybrid search (NULL until embedded) |

### tool_calls

Tool/function invocations and results.

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash |
| message_id | VARCHAR FK | References messages.id |
| tool_name | VARCHAR | Tool/function name |
| input | JSON | Tool input parameters |
| output | JSON | Tool output/result |
| status | VARCHAR | `pending`, `complete`, `error` |
| duration_ms | INTEGER | Execution time if available |
| created_at | TIMESTAMP | Invocation timestamp |

### attachments

File and image attachments.

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash |
| message_id | VARCHAR FK | References messages.id |
| filename | VARCHAR | Original filename |
| mime_type | VARCHAR | MIME type |
| size | INTEGER | File size in bytes |
| path | VARCHAR | Local path if downloaded |
| url | VARCHAR | Remote URL if available |
| created_at | TIMESTAMP | Attachment timestamp |

`attachment_bodies` maps an attachment ID to the SHA-256 and bounded size of a
retained body. The bytes live once under `data/attachments/<sha256>`; this table
preserves the canonical association when the local body is temporarily absent.

### artifacts

Claude artifacts (code, documents, etc.).

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash |
| conversation_id | VARCHAR FK | References conversations.id |
| artifact_type | VARCHAR | `code`, `document`, etc. |
| title | VARCHAR | Artifact title |
| content | TEXT | Full artifact content |
| language | VARCHAR | Programming language if code |
| created_at | TIMESTAMP | Creation timestamp |
| version | INTEGER | Version number |

### file_edits

File modifications from CLI tools.

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR PK | Deterministic hash |
| message_id | VARCHAR FK | References messages.id |
| file_path | VARCHAR | Absolute file path |
| edit_type | VARCHAR | `write`, `edit`, `multiedit`, `shell` |
| content | TEXT | New content or edit description |
| created_at | TIMESTAMP | Edit timestamp |
| old_content | TEXT | Replaced text (`old_string` for `edit`; NULL for `write`/`shell`) |

### provenance schema

The schema is initialized and written by core because it is part of the archive
contract. Capture runs on hooks and ordinary sync; the installable changegraph
product is strictly read-only.

| Relation | Canonical facts |
|----------|-----------------|
| `repositories` | Repository identity, lineage, roots, remotes, and last observed head |
| `repository_checkouts` | Local-only checkout path, branch, and head |
| `repository_aliases` | Local exact Git evidence used to reattach a repository without making remotes its identity |
| `conversation_scopes` | Capture-time resolved cwd, repository, root, and checkout marker |
| `files` | Repository-relative or opaque external file identity |
| `file_versions` | Observed full-content hashes |
| `file_edit_scopes` | Immutable capture-time repository-relative path, resolved filesystem route, root, and checkout marker for each local edit |
| `file_edit_files` | Edit-to-file edges plus hashes of captured old/new edit material and evidence quality |
| `git_checkpoints` | Git head plus capture-time working-tree hash, changed paths, and capture source |
| `checkpoint_edits` | Checkpoint-to-`file_edits.id` evidence |
| `local_facts` | Content-free marker that this archive independently observed a fact and may sign it locally |

There are intentionally no copied prompts, message bodies, changesets,
file-edit bodies, raw remote payloads, workspace IDs, or device IDs in this
schema.

The `remote` schema is the separate identifier-only exception for remotely
projected archive rows. Core writes it atomically with each imported row so
deleting synchronization state cannot change authorship or make a foreign row
publishable. Imported physical IDs use the verified author user, table, and
exact source row; workspace proofs remain access paths. The signing device is
separate provenance and does not change semantic row identity after recovery.

| Relation | Durable facts |
|----------|---------------|
| `row_origins` | Author-scoped physical-to-source identity and currently materialized proof |
| `provenance_origins` | Imported provenance fact attribution and original proof link |
| `row_proofs` | Bodyless signed revision, content hash, predecessor, state, author, and authorization epoch |
| `row_signers` | One normalized root key and device certificate per author device |
| `workspace_controls` | Signed origin-workspace authorization chain, once per control revision |
| `row_conflicts` | Canonical logical body for a rare verified incomparable head not selected as the main row |

Proof rows never duplicate conversation content. Reissuing an equivalent
certificate for the same certified device keys does not create another signer.
Sync creates proofs only after portable-path normalization and mandatory team
redaction. Unchanged rows reuse their existing proof; a changed or reverted row
names its one known head as predecessor. Multiple unmatched heads block
automatic publication instead of guessing ancestry.
On receive, a causal successor updates the ordinary archive table, a stale
ancestor is ignored, and an incomparable signed body is retained in
`row_conflicts` rather than winning by relay arrival time.

### archive_state

| Column | Type | Description |
|--------|------|-------------|
| singleton | BOOLEAN PK | Enforces exactly one archive identity row |
| archive_id | UUID | Stable identity created when this DuckDB is initialized |
| generation | UBIGINT | Monotonic archive/provenance write generation |

The row is added automatically to existing DuckDB archives. Generation changes
in the same transaction as core projection, provenance capture, retained
attachment-body indexing, or ingestion, so a failed transaction cannot advance
the proof. It is a rollback detector, not a content revision or remote cursor.
`archive_changes(kind, entity, generation)` is a compact current-generation
index: one content-free row per touched logical identity, overwritten on later
changes. Missing marked rows produce tombstones. Losing Remote state resets its
cursor and causes a deterministic full scan; no canonical data depends on this
index.

Existing core archives migrate automatically. Before schema N first mutates an
archive, Convos checkpoints and validates a mode-0600
`<database>.pre-vN.bak`; retries retain the same recovery copy. Relay and
`state.db` remain rebuildable and are not migrated during this clean cutover.

## Full-Text Search

FTS index on `messages` table covering `content` and `thinking` columns.

```sql
PRAGMA create_fts_index('messages', 'id', 'content', 'thinking', overwrite=1)
```

**Query with FTS:**
```sql
SELECT m.*, fts_main_messages.match_bm25(m.id, 'search term') as score
FROM messages m
WHERE score IS NOT NULL
ORDER BY score DESC
```

## Hybrid Search

`convos query` combines FTS (BM25) with vector similarity over the
`embedding` column using DuckDB's built-in `array_cosine_similarity`. Top-50
from each source is fused with Reciprocal Rank Fusion (`SUM(1/(60+rank))`),
then the strongest message from each conversation is returned in fused order.
Source/day/role/cwd/conversation filters are applied before candidate selection;
injected skill and local-command wrapper messages are excluded from semantic
candidates. A cwd filter includes the exact recorded path and descendants.

Embeddings are produced by embeddinggemma-300M (768d) with the
`task: search result | document:` prefix at index time and `query:` at query
time. Truncation only — no chunking — at 1600 chars.

Use `convos embed` to backfill missing embeddings without fetching from web
APIs. Hooks and `convos sync` queue new or changed messages for just-in-time
embedding by `convos query`. Inference runs without a database lock; only each
result batch update is locked.

Installed applications can call
`ai_convos.cli.hybrid_hits(query, source, days, role, limit, local_only=False, cwd=None, conversation=None)`
for the same ordered, one-hit-per-conversation records without parsing CLI
output. Records retain full content plus exact message and conversation IDs.
`local_only=True` forbids an implicit retrieval-model download.

The `embedding` column is preserved across upserts when message `content`
is unchanged, so only new or edited messages are queued again.

## ID Generation

All IDs are generated deterministically:

```python
def gen_id(source: str, oid: str) -> str:
    return hashlib.sha256(f"{source}:{oid}".encode()).hexdigest()[:16]
```

This ensures:
- Same record always gets same ID
- Upserts update rather than duplicate
- IDs are stable across syncs

## Common Queries

**Conversations with message counts:**
```sql
SELECT c.*, COUNT(m.id) as msg_count
FROM conversations c
LEFT JOIN messages m ON c.id = m.conversation_id
GROUP BY c.id
ORDER BY c.created_at DESC
```

**Search with context:**
```sql
SELECT m.content, c.title, c.source,
       fts_main_messages.match_bm25(m.id, 'python') as score
FROM messages m
JOIN conversations c ON m.conversation_id = c.id
WHERE score IS NOT NULL
ORDER BY score DESC
LIMIT 20
```

**Most edited files:**
```sql
SELECT file_path, COUNT(*) as edits
FROM file_edits
GROUP BY file_path
ORDER BY edits DESC
LIMIT 10
```

**Tool usage by source:**
```sql
SELECT c.source, tc.tool_name, COUNT(*) as uses
FROM tool_calls tc
JOIN messages m ON tc.message_id = m.id
JOIN conversations c ON m.conversation_id = c.id
GROUP BY c.source, tc.tool_name
ORDER BY uses DESC
```
