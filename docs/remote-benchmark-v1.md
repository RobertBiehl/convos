---
summary: "Measured Remote v1 recovery, performance, and storage release evidence."
read_when:
  - Changing row replication, reconciliation, projection, or recovery
  - Changing proof, relay, state, or migration storage
  - Evaluating Remote v1 release gates
---

# Remote v1 benchmark

Measured 2026-08-14 on a 10-core Apple M4 MacBook Air with 24 GB RAM,
macOS 26.4.1, Python 3.12.12, and DuckDB 1.4.3. The command was:

```sh
uv run python scripts/benchmark_remote.py ~/.convos/data/convos.db
```

The benchmark used an APFS copy-on-write clone, two isolated clients, and
in-process local relay databases. It made no network request and never
published archive content. The source contained 14,026 conversations, 109,943
messages, 152,810 tool calls, 832 attachments, 16,765 file edits, and 20,170
provenance facts: 314,546 current semantic objects in total.

## Results

| Gate | Measured result |
|---|---:|
| Current-schema initialization | 0.035 s; no migration required |
| Initial proof creation including retry scan | 113.752 s |
| Interrupted first attempt | 88.170 s |
| Lost-response reconciliation and remaining upload | 269.157 s |
| One-row incremental sync | 0.950 s; exactly 1 replica |
| Fresh-holder verification and projection | 321.891 s for 314,547 revisions; 977.185/s |
| Empty-relay repair by the foreign holder | 271.560 s for 314,546 current objects; 1,158.293/s |

The fresh holder starts without a core or state database. Its measurement
includes all pull, verification, causal resolution, and projection work. Both
recovery paths therefore pass the fixed 500 verified/projected
objects-per-second gate without subtracting setup or validation work.

Current benchmark output also reports an isolated full replica-page walk over
the production-shaped encrypted relay rows. This separates relay cursor paging
and envelope decoding from client decryption, verification, and projection.

| Storage | Bytes | Relative to 4,102,565,888-byte source |
|---|---:|---:|
| Automatic migration backup | 0 | Current schema; no migration |
| Core proof/index growth | 140,771,328 | 3.431% |
| Settled `state.db` | 39,378,944 | 0.960% |
| Settled relay database | 2,442,850,304 | 59.544% |
| Relay encrypted envelope/blob payload | 2,111,465,712 | 51.467% |
| Fresh rebuilt relay database | 2,441,584,640 | 59.514% |

The relay copy is intentionally substantial: disaster recovery requires an
encrypted semantic copy independent of any one core. The compact proof/index
overhead stays below the 10% target, and settled device state remains small and
content-free. When a core migration is required, its one-time backup temporarily
requires another full archive-sized allocation and is preserved for rollback
rather than silently deleted; this already-current source required no backup.

`scripts/benchmark_remote.py` fails when proof overhead exceeds 10%, fresh
verification falls below 500 objects per second, incremental sync emits other
than one replica, or empty-relay repair does not restore every current proof.
