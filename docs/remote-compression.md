# Default replica compression

New uploads use **Zstd level 1**, automatically negotiated with the relay. There
is no per-workspace setting and no level to tune. An older relay without the
capability receives the existing uncompressed envelopes. This applies to both
archive rows and semantic replicas; event history and attachment bodies retain
their encodings. Existing data can be compacted with `convos remote compact`,
with an optional workspace argument, resumable progress, and conditional
replacement.

Each replica is an independent frame, compressed before encryption without an
external dictionary. Compression uses the native extension and its single-thread
default, avoiding extra worker pools while agents sync concurrently. A frame is
used only if it saves more than the added header overhead. All original signed
bytes, identities, authorization evidence, and decompression bounds are retained.
See the [Python Zstd compressor API](https://python-zstandard.readthedocs.io/en/latest/compressor.html)
for the codec's level and threading parameters.

## Apple Silicon measurements

Measured on an Apple M4 running native arm64 Python 3.12.12, python-zstandard
0.25.0, and Zstd 1.5.7. All input came from a read-only, previously verified relay
snapshot. Plaintext remained in memory and was checked for exact recovery.

The corpus contains 12,586 replicas across 13 record kinds, including 2,516
messages, 269 conversations, 5,104 tool calls, 1,495 file edits, and semantic
provenance/memory records. Selection combines:

- Every 53rd row-channel cursor and every 19th semantic-channel cursor: 12,554
  replicas and 41.85 MB of plaintext.
- The 16 largest replicas in each channel: 32 additional replicas and 107.64 MB.
  The largest individual replica was 9.10 MB.

The second group deliberately stresses large inputs and is not representative
of the archive's frequency distribution. Keeping it separate avoids reporting a
large-record stress ratio as the ordinary archive ratio. The combined corpus is
149.49 MB; its median record is 1,188 bytes and its 99th percentile is 40,723 bytes.

Seven warm measurements follow one warmup. Case order rotates and reverses
between trials. Each encode creates the same per-record compressor context as
production; decode includes frame validation and exact-size decompression.

| Zstd level | Systematic sample payload, MB | Full stress payload, MB | Full stress encode, ms | Full stress decode, ms |
| --- | ---: | ---: | ---: | ---: |
| -3 | 27.170 | 134.697 | 67.89 | 26.53 |
| -1 | 25.964 | 133.461 | 75.75 | 28.87 |
| **1** | **20.043** | **100.482** | **163.28** | **100.25** |
| 2 | 19.942 | 100.330 | 179.48 | 103.56 |
| 3 | 19.713 | 100.006 | 204.40 | 103.81 |
| 5 | 19.426 | 99.579 | 453.65 | 104.59 |
| 7 | 19.329 | 99.407 | 552.13 | 105.17 |
| 9 | 19.310 | 99.243 | 793.49 | 106.68 |

Level 1 reduces the systematic sample's payload by 52.11%. Level 3 saves another
1.64% relative to level 1 in that sample, or 0.47% in the stress corpus, for 25%
more codec encode time. Level 5 takes 2.8 times as much encode time as level 1.
The faster negative modes require substantially more storage and transfer. These
results favor level 1 for a default used by many concurrent agents, including
workspaces where one upload serves many downloads. They do not imply level 1
will beat every other level for every dataset or machine.

## Complete replica codec path

The same corpus also went through the actual `seal_replica` and `open_replica`
functions, including canonical JSON, row/proof hashes, AES-GCM, and base64.
Seven warm measurements used the same alternating order, with byte-exact payload
recovery verified for every frame. Input JSON parsing was outside the sealing
timer; all receive-side parsing and row-hash checking was inside the opening
timer. Summed wire bytes include each canonical envelope and its authenticated
headers, excluding the small HTTP/page wrapper.

| Encoding | Wire MB | Seal, ms | Open and validate, ms |
| --- | ---: | ---: | ---: |
| Uncompressed | 202.748 | 838.83 | 770.39 |
| Zstd -1 | 181.920 | 913.14 | 783.83 |
| **Zstd 1** | **137.948** | **956.57** | **814.91** |
| Zstd 3 | 137.313 | 997.81 | 816.72 |

Level 1 saves 64.80 MB on the wire in this stress corpus, adding about 118 ms of
sender CPU-path wall time and 45 ms on receipt. Level 3 saves only another
0.63 MB and adds about 41 ms to sealing. Network transfer, SQLite access, and
DuckDB projection are excluded; the receive CPU overhead must not be described
as a CPU speedup. These numbers are measured samples, not a forecast of total
database savings or end-to-end sync latency.

## Compaction without body downloads

Compaction inventories only uncompressed replicas and receives headers, wire
hashes, sizes, and cursors. It first reconstructs each signed payload locally;
re-encryption with the retained header and nonce must match the original wire
hash before that copy can be used. A verified fallback fetch is needed only when
the exact local payload or lineage is unavailable. Replacements still compare
the expected old digest atomically. No local archive mutation or new signature
is needed.

Archive proofs are indexed in bounded pages into a temporary SQLite lookup.
Archive bodies are read in batches; current semantic payloads use a temporary
lookup during the operation. These lookups are discarded when compaction ends.
Already-compressed inventories need no local archive scan or lookup construction.

Measured on the same M4 with real retained envelopes and JSON serialization:

| No-op case | Response body bytes | Compaction requests | Local median |
| --- | ---: | ---: | ---: |
| Previous rescan of 2,411 already compressed replicas | 6,163,343 | 6 | 47.13 ms |
| Metadata-only rescan of the same replicas | 29 | 1 | 1.44 ms |

These are compaction-engine timings, excluding CLI startup, network latency,
HTTP headers, and the initial workspace refresh. The complete warm CLI with an
already-caught-up cursor measured 3.18 ms against the 590,274-replica snapshot,
with one state request and one inventory request, using an in-process transport.
An unchanged cursor is not rewritten. A first scan of a larger compressed
archive still scans its eligible headers on the relay; this small sample's time
must not be extrapolated as a constant-time guarantee.

Tests verify byte-exact local reconstruction for current rows, proof lineage,
and semantic payloads; fallback for changed or unavailable historical bodies;
no ciphertext reads for already-compressed inventories; uploader boundaries;
conditional fetch/replacement; and recovery after lost acknowledgments.

## Rollout

Upgrade all receiving clients before upgrading the relay to advertise compression
support. The relay cannot decrypt or transcode for an old reader. This readiness
requirement is operational; the current relay does not enforce reader versions.
No extra configuration is needed after the upgrades. Retained legacy replicas
remain readable, and normal sync does not implicitly rewrite historical storage.
