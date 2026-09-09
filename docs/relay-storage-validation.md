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

The default is now automatic Zstd level 1 on supporting relays. A subsequent
[Apple M4 comparison](remote-compression.md) measured eight Zstd levels across
12,586 records, including a systematic archive sample and the largest replicas.
It also records the modest CPU overhead in the larger stress sample rather than
extrapolating this earlier sample's faster decode result to every workload.

## Verification and activation

The b8 release requires the complete non-integration suite, package builds, and
isolated wheel checks; final run counts are recorded in the release PR.
Compression tests also passed with the declared minimum
`zstandard==0.23.0` and the resolved `0.25.0`. Coverage includes old envelopes,
authenticated metadata, decompression limits, malformed frames, conditional
replacement, uploader boundaries, acknowledgment loss, exact second-device
projection, automatic row/semantic compression, old-relay fallback, resumable
compaction without configuration, metadata-only no-ops, exact local reconstruction
with verified fallback fetches, archive lock release before network requests, and
preservation on failed storage migrations.

The live relay had only 5.7 GiB available during the audit. The full migration ran
locally to avoid using that remaining space. Final activation needs sufficient
staging space and a stopped-relay snapshot/cutover; a snapshot taken while writes
continue must not later replace the active database. Compression is automatic
once the relay advertises support, so upgrade all receiving clients before the
relay. No workspace minimum reader version is enforced by the current relay.


## GPT-6 Pro release review

The review examined candidate `c5281d48159857022b003cf129f42562a92867ad`
and identified two reproducible migration defects. Both are corrected in b8:

- Publication now uses an atomic, non-overwriting hard link for migration. A
  destination created during conversion survives intact; the source is unchanged
  and staging is cleaned up. The separate backup command retains its overwrite
  behavior.
- Quota rebuilding computes byte lengths before union/grouping, so the grouping
  sorter receives identifiers and integers instead of full ciphertext BLOBs.
  A regression test lowers SQLite's record-size limit only during accounting;
  the former query fails and the corrected query returns the exact byte total.

The review found no concrete authorization bypass, unsafe published nonce reuse,
or historical-payload substitution in compact. Oracle ran 29 selected tests in
an isolated source harness; it could not run the complete DuckDB/Zstd product
suite because its environment lacked dependencies and network access. Local and
Linux CI validation supply that separate evidence. The two reviewed fixes are
covered by local regression tests, not a second Oracle submission.

Receiver readiness includes restarting long-running processes with the upgraded
code. Installing the new package on disk alone is insufficient. Do not activate
compression while an older receiver is expected to reconnect.
