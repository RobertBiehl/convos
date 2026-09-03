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

## Process lock contract

Every Convos lock is an advisory `flock` on a stable `0600` file and uses the same
core acquisition and error path. An exclusive holder writes one compact JSON object:
`{"v":1,"purpose":"remote sync","pid":123,"process":"convos","host":"host","os_user":"alice","started_at":0.0,"heartbeat_at":0.0,"stage":"started"}`;
Remote may add `remote_user`, `user_id`, `device`, and `device_id`. Long-lived operation
leases update `heartbeat_at` and `stage` at phase boundaries. Contention is a concise CLI error naming the requested
purpose and available holder identity, start time, and last activity. The kernel lock is
authoritative: file existence or an old heartbeat never proves ownership, locks are released
on process exit, and a successful exclusive acquisition removes stale diagnostics. Each shared
archive reader has a unique `0600` sidecar carrying the same identity, timing, purpose, heartbeat,
and stage fields; contention reports every recorded reader rather than pretending one owns the
shared lock. Manual Remote sync and repull hold a priority lease: background syncs yield at the
next request or phase boundary, and the manual command waits at most five seconds for the sync
lease before reporting the blocking process. Tests inventory every production `flock` call through
this contract.

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
5. Archive locks follow the process lock contract above. Any archive lock held longer than
   five seconds emits its mode, duration, and purpose to stderr.
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
| Remote publication | Bounded change scans normally; initial/rebaseline scans use generation-checked 5,000-record pages and release the reader between pages; signing, encryption, relay inventory, and upload happen after close |
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
