# Relay b7 hardening evidence

The b7 relay keeps the existing SQLite schema and signed v1 encodings. It makes
authorization and mutation one transaction, gives reads one consistent
snapshot, and opens request connections without schema initialization.
The regression suite reproduces an upload paused after its epoch check while a
rotation commits: b6 stores an epoch-1 event after an epoch-2 signed boundary
with tail zero. The fixed relay serializes those requests and includes the
committed event in the next rotation boundary.

HTTP workers and connection lifetimes are bounded. Framing tests cover negative,
duplicate, missing, malformed, and oversized lengths, transfer encoding,
truncated bodies, invalid JSON operation shapes, overload recovery, idle
connections, and slow trickling. Internal error details stay in server logs.
Large-event manifests avoid materializing ciphertext bodies in Python, and
ledger heads use the existing author-sequence index.

## Reproduce the synthetic comparison

From this checkout with the development environment installed:

```bash
git show ab03124:apps/remote_server/src/ai_convos_remote_server/__init__.py > /tmp/convos-relay-b6.py
uv run python scripts/benchmark_relay.py --baseline /tmp/convos-relay-b6.py --requests 1000 --concurrency 20
```

The benchmark creates and removes its own temporary databases, uses loopback
HTTP, and never reads a personal archive or contacts a live relay. It seeds
20,000 synthetic opaque ledger rows over 20 authors for SQL scaling, then uses
authenticated operations and signed encrypted events for the HTTP write and
large-page checks. Each version receives exactly 3,001 HTTP requests and ends
with exactly 21,008 events; SQLite `quick_check` must pass. Counts have explicit
upper bounds. The output includes the server source hash and SQLite version.

## Local measurements

A Python 3.12 / SQLite 3.50.4 run on September 7, 2026, with the other local test
suite finished first, produced these medians. They measure complete loopback
HTTP requests, including connection setup and response consumption, where
marked HTTP.

| Measurement | b6 | b7 changes |
|---|---:|---:|
| State, HTTP, 1,000 requests | 0.866 ms | 0.647 ms |
| Event upload, HTTP, 1,000 requests | 0.943 ms | 0.825 ms |
| State, HTTP, 20 concurrent clients, 1,000 requests | 29.048 ms | 28.092 ms |
| Concurrent state throughput | 670.01 requests/s | 702.29 requests/s |
| State, HTTP, while another connection holds a writer for 300 ms | 391.949 ms | 0.633 ms |
| Exact ledger heads, 20,000 events / 20 authors, five calls | 20.368 ms | 3.673 ms |
| Manifest page for eight large encrypted events, five calls | 4.368 ms | 1.166 ms |
| Python peak allocation during the ledger call | 10,719,605 bytes | 9,092 bytes |
| Python peak allocation during the large-event page | 11,198,711 bytes | 6,767 bytes |

The large-page ciphertext totals 11,189,688 encoded bytes. Both versions retain
all seeded and uploaded events. Python allocation figures exclude SQLite's C
heap and the OS page cache. One-time startup and database file size are also
reported by the script, but filesystem timing and page allocation vary across
runs; these measurements establish neither production capacity nor behavior at
hundreds of gigabytes. Cryptographic recovery, authorization, and live HTTP
delivery are checked separately by the existing acceptance tests.

Validation for these changes passed 171 tests:

```bash
uv run pytest tests/test_remote_server.py tests/test_remote_server_http.py tests/test_remote_control.py tests/test_remote_client.py tests/test_remote_operations.py tests/test_remote_acceptance.py tests/test_remote_examples.py tests/test_remote_testbed.py tests/test_budget.py -q
```
