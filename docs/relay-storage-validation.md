# Relay storage validation

The implementation is on `feature/relay-storage`, based on release commit
`c98a350`. These measurements used private copies; neither the live relay nor
the live DuckDB archive was migrated.

## Full relay snapshot

The retained pre-b7-release backup was downloaded and verified against its
recorded SHA-256 before conversion. Its source file remained byte-identical.

| Measurement | Result |
| --- | ---: |
| Original SQLite file | 4,654,825,472 bytes |
| Binary SQLite file after compaction | 3,611,987,968 bytes |
| Reduction | 22.40% |
| Row replicas | 548,387 |
| Semantic replicas | 41,887 |
| Attachment bodies | 100 |
| Tables compared | 20 |
| Migration including initial checksum | 110.75 seconds |
| Migration plus independent comparison and final checksum | 169.95 seconds |

All original columns except the intentionally split envelope and recalculated
usage cache matched. Every original encrypted payload was independently decoded
from base64 and compared byte-for-byte with its new BLOB. Reconstructed headers,
wire sizes, wire digests, cursors, authority records, and other tables matched;
SQLite integrity checks passed. This is an older 4.65 GB snapshot, not a claim
about the exact post-migration size of the newer live relay.

## Read performance and compression sample

The same 2,411 real retained rows were measured in three SQLite representations.
Seven warm measurements per representation followed one warmup, with alternating
order. Every compressed plaintext was checked for byte-exact recovery. This is a
tool/edit-heavy sample; it does not forecast the entire relay's compression ratio.

| Representation | File bytes | Relay read and JSON serialization, median | Read, decrypt, parse and row-hash check, median |
| --- | ---: | ---: | ---: |
| Original JSON/base64 storage | 16,617,472 | 36.80 ms | 95.96 ms |
| Binary ciphertext, existing HTTP format | 13,520,896 | 38.28 ms | 97.27 ms |
| Binary ciphertext and Zstd level 1 | 6,754,304 | 19.49 ms | 93.81 ms |

Binary storage alone adds a small CPU cost to reconstruct base64 for the existing
HTTP protocol. The combined compressed path was faster in this sample and used
59.35% less SQLite space. These measurements exclude network transfer and DuckDB
projection; they are not a guarantee for every payload or deployment. Zstd is
loaded only when compression is used, avoiding an import on legacy-only paths.

## Verification and activation

The complete non-integration suite passed: 846 tests, 9 live integrations
deselected. Compression tests also passed with the declared minimum
`zstandard==0.23.0` and the resolved `0.25.0`. Coverage includes old envelopes,
authenticated metadata, decompression limits, malformed frames, conditional
replacement, uploader boundaries, acknowledgment loss, exact second-device
projection, and preservation on failed storage migrations.

The live relay had only 5.7 GiB available during the audit. The full migration ran
locally to avoid using that remaining space. Final activation needs sufficient
staging space and a stopped-relay snapshot/cutover; a snapshot taken while writes
continue must not later replace the active database. Compression remains opt-in
and requires all receiving clients to have been upgraded. No workspace minimum
reader version is enforced by the current relay.
