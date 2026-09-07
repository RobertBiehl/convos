# b7 validation

This release hardens capture, signed-row reconstruction, dependency recovery,
relay transactions, and HTTP handling. The regression cases cover failure and
restart boundaries, author ownership, exact body export, and repeated replay.
Released v1 logical rows keep their encoding and identity recipes. Core schema
v12 is additive and uses the existing verified database-and-attachments backup
before migration.

## Measured work

The following are local measurements, not production capacity guarantees.
Synthetic measurements use disposable archives or loopback relays. The private
archive experiment uses verified copies; the relay replay permits read
operations only.

| Measurement | Before | b7 work |
|---|---:|---:|
| Duplicate validation, 8,000 rows | 980 ms | 1.67 ms |
| One-message team selection with 100,000 conversation scopes | 7.57 s | 0.006 s |
| Settled receipt membership with 1,000,000 entries | 0.61 s | 0.00002 s |
| New edit capture, 30 alternating samples | 219.84 ms | 186.05 ms |
| Signed hook capture, 40 alternating samples | 89.36 ms | 76.23 ms |
| First attestation, 501 rows with 4 KiB bodies, eight alternating samples | 252.64 ms | 206.45 ms |
| Two-key preservation lookup in 548,389 proof headers | 31-44 ms | 6.35 ms |
| Insert 500 proof headers, without/with the source index | 4.28 ms | 4.34 ms |

The signed-hook comparison includes preservation of the preceding signed body.
Unchanged ingestion has a regression check proving that it performs no proof
head lookup. An updated native row keeps its current signed body recoverable
until a durable replacement attestation permits that predecessor body to retire;
independent revision branches remain preserved.

The preservation comparison isolates the new guard before and after query
optimization, rather than comparing a missing safety check with a complete one.
Its query plan changes from scanning the proof table to reading four indexed
proof rows. The index write comparison uses alternating rolled-back inserts
into independent copies of the same archive. Individual timings include
machine and cache effects; the query plans establish the bounded lookup work.

[Relay measurements](relay-b7-hardening.md) include 20 concurrent clients,
20,000 ledger entries, a blocked writer, and large encrypted event manifests.
They cover complete HTTP requests and verify exact event counts and SQLite
integrity.

## Full archive replay

A read-only relay replay into a verified 8.3 GB archive copy received 548,387
retained replicas. The resulting audit checked 510,991 current signed heads:
510,988 matched their typed projection and three differing conversation bodies
were retained exactly alongside the local versions. No current signed head was
unavailable after replay; three were unavailable in the original local copy.
Replay and the full audit together took 2,983 seconds.

An exact comparison of all 43 original tables found no changes to conversations,
messages, tools, edits, provenance facts, or FTS contents. All 69,971,712 embedding
values matched bit for bit. The 115 attachment references changed only their
local file paths; all 100 distinct body files matched their recorded size and
SHA-256 hash. Other changes were the expected schema, change-generation, and
replica-recovery metadata. The source snapshot hash remained unchanged.

This test recovered surviving relay evidence without replacing native content.
It does not establish what originally caused the three signed-body differences.

A disposable copy of the 4.65 GB production relay database also initialized
under b7 successfully. Every row in all 20 existing SQLite tables compared
exactly with its verified backup, including 548,387 row replicas, 41,887 semantic
replicas, and 100 blobs. SQLite's integrity check passed, and the b7 client
verified the copied relay's signed workspace state with the existing identity.
The production service continued running during this check.

## Storage experiment

Rewriting an isolated 8,282,189,824-byte archive produced a 3,077,320,704-byte
copy. Validation compared all 43 tables in both directions, schema catalogs,
FTS results, and all 69,971,712 float32 embedding elements bit for bit. The source
hash remained unchanged. This shows substantial physical storage churn in this
archive; it does not establish a universal compression ratio.

Automatic live compaction is deliberately deferred. DuckDB's native file lock
does not cover the gap between closing the old database and replacing its path;
a concurrent writer could commit to the old file during that gap. The compacted
copy is an experiment, not a promoted production database.

## Installation and compatibility

All eight product versions and internal dependency minimums move together.
Installing Remote b7 into an environment with core b6 upgrades the core and
Redact to b7. A fresh macOS wheel installation also exercises public dependency
resolution rather than only the development lockfile; its focused sync and
preservation suite passed 120 tests with DuckDB 1.5.5 and cryptography 50.0.1.
The development environment uses DuckDB 1.4.3. A fresh test with the former
minimum, DuckDB 1.2.0, failed schema initialization because `json_each` is
unavailable. The declared minimum is now the fully tested 1.4.3 release.

macOS includes the llama.cpp semantic runtime and may need a compiler when no
compatible dependency wheel exists. A binary-only PyPI resolution was not
available for that dependency; normal installation and native-library import
passed using a cached built dependency. Linux's default core and relay remain
compiler-free; Linux semantic retrieval is opt-in.

## Known limits

Existing b6 native markers can be ambiguous. This upgrade preserves them and
does not claim to prove or repair historical ownership without retained bodies
and independent source evidence. Recovery cannot reconstruct an exact signed
body that no surviving source, replica, or backup contains.

Origin transfer remains unpaged. Its client response cap is intentionally not
reduced by this release, because a valid existing origin response can be large.
Other byte-bounded protocol responses and HTTP error bodies have explicit
client limits. Tests and these experiments do not certify capacity at hundreds
of gigabytes or replace a staged team deployment.
