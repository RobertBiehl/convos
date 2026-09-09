# Minimal, public b7 reproducer

One fresh DuckDB archive: **one conversation, one message, one edit, three signed
proofs**. No tools, attachments, embeddings, real repositories, or extra sessions.
**21 rows across all populated tables**, including schema state and bookkeeping.
The full b7 schema accounts for most of the uncompressed file size.

Everything is synthetic, built from constants using the incident's failure
relationships. Text is `Synthetic answer.` and `beta`; path is `example.txt`;
dates are in January 2000. IDs, hashes, signatures, and the public certificate
are newly derived from explicit test values. No production DB pages, source
exports, original text, account/device IDs, paths, timestamps, or keys were used.
The generator contains deterministic PUBLIC TEST signing seeds, not credentials
for any real account. Never enroll that test identity or upload it to a relay.

## Reproduce before and after

Extract the ZIP into a separate directory. It includes `convos.db`, a complete
`rows.json` export, a count/hash manifest, the standalone generator/checker,
recorded results, and `recovery.patch` with the proposed upstream fixes.

From a Convos b7 checkout, install its test dependencies once:

```sh
uv sync --extra dev
```

Set the extracted bundle's location; the checker always uses the checkout
specified by `--checkout`, not whichever Convos happens to be installed:

```sh
convos_fixture="/path/to/extracted-fixture"

# Unpatched v0.11.6b7 or v0.11.6b7.post1: all three defects must reproduce.
uv run python "$convos_fixture/b7_minimal_fixture.py" check "$convos_fixture" --checkout . --expect broken

# In a clean, disposable checkout: apply the supplied proposed fix once.
git apply --check "$convos_fixture/recovery.patch"
git apply "$convos_fixture/recovery.patch"

# Patched checkout: all three cases must now pass.
uv run python "$convos_fixture/b7_minimal_fixture.py" check "$convos_fixture" --checkout . --expect fixed
```

Checks take about a second locally. Exit 0 means **all three cases match the
requested expectation**: with `--expect broken`, it means the bugs reproduced,
not that the archive is healthy. Default expectation is `fixed`; a buggy checkout
then exits 1. `--report /new/result.json` saves the result without overwriting.

Each case gets its own temporary copy. The delivered fixture's hashes are checked
before and after; no hooks, source import, relay, enrollment, or real archive is
used. Do not replace `~/.convos` with this fixture or run normal full provider
sync on it: the runner exercises the relevant provenance/signing/audit boundaries
directly, without reading your actual Claude/Codex sessions.

## What the three cases prove

| Case | Minimal relationship | Unpatched b7 | Patched |
| --- | --- | --- | --- |
| Scope migration | Edit `e` has a hash-valid historical file mapping; v3 invents a different placeholder scope | Full provenance capture raises `provenance edit scope conflict` | Capture succeeds twice; repeat adds no edit observation |
| Edit attestation | Two heads for edit `e`; the local base identifies one hash-verified, same-scope predecessor | `row revision conflict: edit.observed:e` | Adds one successor, zero on repeat; competing body stays |
| Retained history | One signed file-version body covers an origin whose physical projection is absent; a successor is signed | Previous body deleted; audit unavailable changes 0 to 1, original proof unchanged | Previous body retained; audit stays at zero unavailable |

The scope case replays the v3 initializer on its temporary copy, then calls the
same full provenance capture used by `sync --full`. The delivered archive itself
is schema 12 with the faulty placeholder, matching an already-migrated archive.
Existing v12 production archives still need the explicit backup-first scope
repair; applying a source patch alone does not retroactively rerun migration v3.

The retained-history case uses one file-version fact to isolate the retirement
bug from the separate edit-fork refusal. It intentionally has no current physical
version row: the valid retained body must be enough for a complete audit.
This is a behavioral reduction, not a literal subset of any production archive.
The hook/audit race requires scheduling, not extra data; its runtime regression
tests remain in `tests/test_maintenance_recovery.py` in the supplied patch.

## Rebuild instead of shipping a binary

All 21 rows can be inspected in `rows.json`. To recreate the same logical dataset
with your local DuckDB version, choose a new output directory:

```sh
uv run python "$convos_fixture/b7_minimal_fixture.py" build /new/synthetic-fixture --checkout .
```

Logical row exports are deterministic. DuckDB binary hashes can differ across
builds/engine versions; each build writes its own checksums. Existing output
directories are refused. No original data is needed to rebuild the fixture.
