---
summary: "Normative database connection, locking, and transaction invariants."
read_when:
  - Adding or changing any database access
  - Debugging lock contention or slow sync
  - Reviewing archive mutation safety
---

# Database connection invariants

These are product invariants, not implementation advice. `tests/test_database_connections.py`
mechanically inventories production connection sites and fails when a new site is not
classified. The exhaustive per-acquisition report is `database-connection-ledger.md`.

## Lock contract

Logical operation leases (Remote sync, hook drain, and embedding) are advisory `flock`s
on stable `0600` files and use one core acquisition and error path. A holder writes one compact JSON object:
`{"v":1,"purpose":"remote sync","pid":123,"process":"convos","host":"host","os_user":"alice","started_at":0.0,"heartbeat_at":0.0,"stage":"started"}`;
Remote may add `remote_user`, `user_id`, `device`, and `device_id`. Long-lived operation
leases update `heartbeat_at` and `stage` only when the operation advances; updates are serialized
when concurrent parser workers commit progress. Acquisition uses
randomized exponential retry. A new heartbeat resets the inactivity deadline, so a progressing
operation has no fixed total wait limit; a stalled or unknown holder produces a concise error naming
the requested purpose, holder identity, start time, and last progress. The kernel lock remains
authoritative: metadata and old heartbeats never grant ownership, and locks are never stolen.
Manual Remote sync and repull hold a priority lease: background syncs yield at the
next request or phase boundary, and the manual command waits until the background process yields
or stops reporting progress; a stalled holder is reported after five seconds of inactivity. Tests
inventory every production `flock` call. Contention-only waiter notices also use `flock`, solely so
a crashed waiter is distinguishable from a live one and its stale notice can be removed safely.

## DuckDB archive

1. Every live archive connection goes through `get_db`/`open_db`, which resolves the
   canonical path and immediately attempts the native DuckDB connection. The uncontended path
   performs no process-lock, marker, metadata, or queue operation. Every production opening has
   a non-empty literal purpose.
2. A connection scope contains database work and materialization only. Network requests,
   model inference, Git commands, signing/encryption, attachment hashing/copying, secret
   inspection, and output-file writing happen before acquisition or after close.
3. Ordinary canonical writes are bounded transactions and revalidate the archive generation
   or exact rows on which unlocked preparation depended. Remote publication may instead persist
   an immutable signed revision already captured at watermark `G`; it records only `G`, so later
   archive changes remain pending for the next idempotent sync.
4. Read snapshots are materialized and closed before expensive Python processing. A read
   lock is not harmless: DuckDB cannot admit a writer from another process while it is open.
5. DuckDB's native cross-process lock is authoritative. After a real native conflict, Convos
   correlates DuckDB's holder PID with any active operation lease, publishes a temporary waiter
   notice, and retries with randomized exponential delays capped at one second. A progressing
   known holder has no total timeout; an unknown or non-progressing holder fails after the
   configured inactivity interval. No path steals or deletes DuckDB's lock.
6. Multi-page operations check waiter notices only after closing a page. A real waiter gets a
   brief acquisition window before the next page; ordinary one-shot connections never check.
   This is deliberately not a userspace FIFO queue: bounded pages, jitter, and cooperative yields
   make starvation extremely unlikely without taxing the normal path.
7. Any archive connection held longer than five seconds emits its mode, duration, and purpose
   to stderr.
8. Long exclusive scopes are allowed only for explicit maintenance whose consistency
   requires exclusion: schema migration and its pre-migration backup, manual backup, FTS
   rebuild, and an embedding-profile reset. They must have a `maintenance.*` or `schema.*`
   purpose, remain local, and provide command-level progress or a completion message.

The only direct `duckdb.connect` outside the gateway opens a detached backup candidate in
read-only mode to validate it. It is not the live archive and takes no archive lock.

## Reviewed archive connection families

| Family | Mode and lifetime |
|---|---|
| Local sync and hooks | Parse and hash unlocked; commit conversations, dependency-ordered messages, tools, attachments, artifacts, and edits in restart-safe 500-row transactions; commit edit evidence last; yield between chunks only when another client has actually contended |
| Provenance | Read plan, Git/filesystem inspection unlocked, bounded commit |
| Embedding | Short candidate read, inference unlocked, bounded batch write, cooperative yield after each batch |
| Search, read, SQL, export, resume, redact, changegraph, explore, memory | Read-only snapshot; presentation, export writing, and secret inspection after close |
| Remote publication and audit | Full/incremental discovery, repair inventory, retained-replica reconstruction, edit-evidence inventory/projection, and received-row audit use bounded or adaptive pages and release the connection between pages; signing, encryption, relay inventory, and upload happen after close |
| Remote receive | Verify/decrypt before opening DuckDB; bounded projection transaction; semantic proofs, ancestor links, and affected edit evidence are applied set-wise without archive-wide reconciliation |
| Attachment relocation | Read plan, hash/copy/fsync unlocked, exact-row revalidation and bounded metadata write |
| Backup and migration | Explicit maintenance exception; consistent checkpoint/backup precedes mutation |

## SQLite databases

SQLite stores browser cookies (read-only), Remote client working state, Remote relay state,
Memory state, and Redact audit state. These are not protected by the DuckDB lock. Their exact
production constructors are separately frozen by the connection inventory test. Remote and
Memory state use their own transactions/leases; a SQLite transaction must not be left open
across a relay request or model call. Backup/cutover connections are explicit maintenance
exceptions and operate on staged or destination databases.

## Review procedure

For every new connection, reviewers determine: database identity, read/write mode, purpose,
maximum rows or pages, transaction boundary, work performed while open, consistency token
used after unlocked preparation, interruption behavior, and whether a maintenance exception
is truly required. Update the implementation, this document if a family changes, and the
inventory test in the same commit.
