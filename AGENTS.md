AGENTS
======

This repo is a single-file CLI that imports and fetches conversation data into DuckDB for search.

Quick Reference
---------------

- Entry point: `src/ai_convos/cli.py`
- Database: `data/convos.db`
- Tests: `tests/`
- Docs: `docs/` (integration details, architecture, DB schema)

Coding Style
------------

Dense, functional Python. Inspired by tinygrad. Each line should pack as much meaning as possible.

Compatibility
-------------

There is no backward compatibility unless explicitly requested or covered by tests.
Released signed logical-row encodings are the exception: retained relay replicas
must remain ingestible for their documented lifetime. Physical-ID recipe changes
require both a backed-up local migration and receive-path coverage proving old
logical replicas project to the current physical identity; workspace IDs are
authorization evidence and must never be inferred as wire-level row identity.

Line Budget
-----------

Stay below the explicit 1300-line core budget (tinygrad-style constraint, enforced by `tests/test_budget.py` via a token-aware count). The increase from 1200 keeps durable schema migrations in the cohesive core instead of hiding archive mutation behind an optional product. Any new feature must fit in the remaining budget, so design for minimal line growth and high density. Prefer no new dependencies when possible.

Package boundaries must represent user-installable products, not internal
modules or a way to evade line budgets. A cohesive product may declare one
explicit larger budget in `test_budget.py`; keep its implementation compact
inside that boundary.

Canonical Write Boundary
------------------------

Core owns schema initialization, ingestion, hooks, provenance capture, migrations, validation, and every DuckDB mutation. Optional products submit typed records through core writer functions and may keep only settings, disposable caches, and working state. Changegraph is strictly read-only. A future plugin write API must be core-managed, transactional, and replayable; no optional writer may create historical gaps when the product is absent.

Conversation ingestion and Git provenance enrichment are separate transactions. A Git inspection failure may delay provenance and cost a retry, but must never roll back or erase imported conversations.

**Do:**
- Pack meaning into each line - comprehensions over loops with append
- Construct results at the end, not via mutation in loops
- Know your data - access keys directly, crash on unexpected structure
- Use walrus operators (`:=`) when the assignment result is immediately used
- Prefer stdlib over deps
- ASCII only in source and comments

**Don't:**
- Use `.get()` defensively - only when a key is genuinely optional
- Mutate accumulators in loops when a comprehension works
- Add try/except unless recovery is meaningful
- Create classes for data - use dicts and `ParseResult`
- Over-abstract or add indirection

**Example style:**
```python
def parse_session(path: Path) -> ParseResult:
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return ParseResult(
        convs=[dict(id=gen_id("src", str(path)), created_at=ts_from_iso(events[0]["timestamp"]), ...)],
        msgs=[dict(id=gen_id("src", f"{path}:{i}"), role=e["type"], **extract_content(e["message"]))
              for i, e in enumerate(events) if "message" in e])
```

Note: `extract_content` returns a dict with text, thinking, tool_calls, attachments - whatever the message contains. Filtering by role or type happens at query time, not parse time. Parse everything, decide later.

Adding a New Integration
------------------------

1. Create fetcher (`fetch_X`) or parser (`parse_X`) returning `ParseResult`
2. Add to `fetchers` or `parsers` dict in cli.py
3. Add tests in `tests/test_parsers.py`
4. Add doc in `docs/X-integration.md`
5. Update schema if new fields needed (rare - prefer metadata JSON)

Testing
-------

```bash
uv sync --all-extras          # install dev deps
uv run pytest tests/ -v       # run all tests
uv run pytest -m "not integration"  # skip live API tests
```

**Test categories:**
- Schema validation: detect API changes before they break fetchers
- Parser tests: verify local file format parsing
- Deduplication: ensure continued conversations don't create duplicates
- Error handling: graceful failures on auth/network issues

**Adding tests:**
- New parser → add tests in `test_parsers.py`
- New API → add schema validation in `test_integrations.py`
- Use `tmp_path` fixture for file-based tests
- Mark live API tests with `@pytest.mark.integration`

**Running integration tests against live APIs:**
```bash
uv run pytest -m integration  # requires valid browser cookies
```

Manual Validation
-----------------

```bash
uv run convos init             # create db
uv run convos sync             # sync local CLI sessions + fetch web (needs cookies)
uv run convos sql "SELECT source, COUNT(*) FROM conversations GROUP BY source"  # verify counts
uv run convos search "test"    # verify FTS works
uv run convos query "test"     # verify hybrid pipeline
```
