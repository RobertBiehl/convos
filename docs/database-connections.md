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
classified.

## DuckDB archive

1. Every live archive connection goes through `get_db`/`open_db`, which resolves the
   canonical path and takes the matching process lock. Every production opening has a
   non-empty literal purpose.
2. A connection scope contains database work and materialization only. Network requests,
   model inference, Git commands, signing/encryption, attachment hashing/copying, secret
   inspection, and output-file writing happen before acquisition or after close.
3. Ordinary writes are bounded transactions. Work prepared without a lock is committed
   only after revalidating the archive generation or the exact rows on which it depended.
4. Read snapshots are materialized and closed before expensive Python processing. A read
   lock is not harmless: DuckDB cannot admit a writer from another process while it is open.
5. A writer records its PID and purpose in the lock file. A waiter timeout names both its
   requested purpose and the recorded writer. Any archive lock held longer than five seconds
   emits its mode, duration, and purpose to stderr.
6. Long exclusive scopes are allowed only for explicit maintenance whose consistency
   requires exclusion: schema migration and its pre-migration backup, manual backup, FTS
   rebuild, and an embedding-profile reset. They must have a `maintenance.*` or `schema.*`
   purpose, remain local, and provide command-level progress or a completion message.

The only direct `duckdb.connect` outside the gateway opens a detached backup candidate in
read-only mode to validate it. It is not the live archive and takes no archive lock.

## Reviewed archive connection families

| Family | Mode and lifetime |
|---|---|
| Local sync and hooks | Parse and hash unlocked; commit conversations, dependency-ordered messages, tools, attachments, artifacts, and edits in restart-safe 500-row transactions; commit edit evidence last |
| Provenance | Read plan, Git/filesystem inspection unlocked, bounded commit |
| Embedding | Short candidate read, inference unlocked, bounded batch write |
| Search, read, SQL, export, resume, redact, changegraph, explore, memory | Read-only snapshot; presentation, export writing, and secret inspection after close |
| Remote publication | Bounded change scans normally; an initial/rebaseline scan is a visible full read snapshot; signing, encryption, relay inventory, and upload happen after close |
| Remote receive | Verify/decrypt before opening DuckDB; bounded projection transaction |
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
