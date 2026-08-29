---
summary: "System architecture: data flow, components, and design decisions."
read_when:
  - Understanding how the system works
  - Adding new features
  - Debugging data flow issues
---

# Architecture

Normative product boundaries for archive ownership, replication, recovery, and
provenance live in [Product invariants](invariants.md). Implementation plans and
older design notes yield to those rules unless they are deliberately revised.
Canonical public and internal names live in the [Naming contract](naming.md).

## Overview

Single-file CLI that normalizes conversations from multiple AI providers into a unified DuckDB database with full-text search.

Optional installable products add change provenance, semantic trails,
deterministic project handoff and session replay, secret-safe sharing, memory
synchronization, the remote client, and a self-hosted E2EE relay without
changing the core conversation contract. See [semantic
exploration](explore.md), [project handoff and replay](resume.md), [secret
protection](redact.md), [memory synchronization](memory.md), and
[`remote.md`](remote.md).

Resume also exposes deterministic conversation replay: it selects a bounded
message window, then joins tool calls and file edits only by those exact message
IDs. This gives humans and agents one ordered, explicitly truncated view of
captured activity without a browser or generation model.
The memory product separates device-local checkout paths from repository scope:
normalized origin evidence identifies clones and worktrees, root commits provide
a fallback lineage anchor, and differing fork origins remain isolated.
When both optional clients are installed, an entry-point adapter maps current
canonical memory revisions onto user-root-signed semantic objects carried by
the encrypted semantic-replica channel. Incoming objects remain
normal revisioned sources in the memory ledger, so deterministic one-sided
advances converge while concurrent semantic changes use the existing
plan/resolve/apply path. Team workspaces never receive memory objects.
Exact archive evidence belongs to the device-local memory ledger and one
canonical revision hash. It stores only archive identity and content hashes,
renders direct local read pivots, and is omitted from remote canonical events.
After a user-owned forget, the adapter retains a signed bodyless descendant
whose complete ancestry defeats every known active revision. Any holder may
repair that proof on a replacement relay without the author's private key;
recipients purge a canonical only when it remains remote-only and unchanged.
Old ciphertext already retained by a peer, relay backup, or operator is not
cryptographically erased by this semantic tombstone.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Sources   │     │   Processors    │     │    Storage      │
├─────────────────┤     ├─────────────────┤     ├─────────────────┤
│ ChatGPT API     │────▶│ fetch_chatgpt   │────▶│                 │
│ Claude API      │────▶│ fetch_claude    │     │   ParseResult   │
│ Claude Code     │────▶│ parse_claude_*  │────▶│   (normalized)  │
│ Codex           │────▶│ parse_codex     │     │                 │
│ Export files    │────▶│ parse_*         │     └────────┬────────┘
└─────────────────┘     └─────────────────┘              │
                                                         ▼
                                              ┌─────────────────┐
                                              │    DuckDB       │
                                              ├─────────────────┤
                                              │ conversations   │
                                              │ messages (FTS)  │
                                              │ tool_calls      │
                                              │ attachments     │
                                              │ file_edits      │
                                              └─────────────────┘
```

## Key Design Decisions

### Single-file core

All core ingest, schema, and retrieval logic lives in `src/ai_convos/cli.py`.
Installable applications live under `apps/` and attach through entry points.
The `convos.commands` group registers commands, `convos.doctor` contributes
diagnostics, and `convos.init` performs explicitly bounded setup after core
initialization. Init callbacks must be local, idempotent, and non-destructive;
model downloads, remote configuration, credentials, and network enrollment
always require their product's explicit command.
Core initialization also runs the ordinary sync pipeline in `--local-only`
mode. It honors configured Codex and Claude roots, imports local agent sessions
and configured export paths incrementally, and does not plan either web source
or load the embedding model.
The same bundled-skill resolver powers installation and `doctor`. Health is
content-exact rather than version-label based: each regular non-symlink managed
copy must match the wheel's bundled `SKILL.md`, so upgrades cannot silently
leave one agent on stale retrieval policy.
Capture hooks use the same exact-state rule. Their managed command pins the
`convos` executable beside the running interpreter plus the resolved archive
root. Health requires one exact handler in each provider event, preventing an
old tool path, duplicate, or misplaced lifecycle handler from masquerading as
working capture.
The total `src/ai_convos/` core remains under the 1200-line budget enforced by
`tests/test_budget.py`; every application has its own honest product budget.

### ParseResult Normalization

Every data source produces a `ParseResult` containing:
- `convs` - conversation metadata
- `msgs` - message content
- `tools` - tool call records
- `attachs` - file/image attachments
- `artifacts` - Claude artifacts (code blocks, etc.)
- `edits` - file edit operations

This normalization happens at parse time, not query time.

### Deterministic IDs

IDs are SHA256 hashes of `source:original_id`. This ensures:
- Same conversation always gets same ID
- Re-syncing updates rather than duplicates
- IDs are stable across machines

### Cookie-Based Auth

Web fetchers extract cookies from Safari or Chrome to authenticate with APIs. No passwords stored, no OAuth flows. Cookies expire naturally.

### FTS via DuckDB

Full-text search uses DuckDB's FTS extension with BM25 scoring. The index covers `content` and `thinking` columns. Ingest marks it dirty and the next search rebuilds it.

### Hybrid Semantic Search

`convos query` adds vector retrieval on top of BM25. Embeddings live in
`messages.embedding` (`FLOAT[]`, NULL until embedded). Vector similarity is
computed brute-force via DuckDB's `list_cosine_similarity` — at the current
scale (tens of thousands of messages) this is fast enough that a vector index
(VSS/HNSW) would only add complexity.

Pipeline: filtered BM25 top-50 ∪ cosine top-50 → Reciprocal Rank Fusion → one
strongest message per conversation.
The embedding model loads lazily, so users pay model startup/download cost only
when using `convos query` or `convos embed`.

`hybrid_hits()` is the public in-process form of that pipeline for installed
applications. It returns untruncated exact turn records and accepts
`local_only=True` when an application must fail rather than download a missing
retrieval model. This also applies while embedding pending hook messages.
Installed retrieval products can use that mode; generation and presentation
remain outside core.

Literal `search` is conversation-first as well: BM25 ranks messages, then only
the strongest matching message from each conversation consumes a result slot.

macOS includes EmbeddingGemma through `llama-cpp-python`; its pinned model is
fetched on first semantic use. Linux installs only the compiler-free core and
relay products by default. `embedding_state` records the complete vector-space
profile; changing it clears incompatible vectors transactionally before they
can participate in retrieval. `CONVOS_SEMANTIC=0` disables semantic work while
leaving literal retrieval available, and the `semantic` extra plus
`CONVOS_SEMANTIC=llama` explicitly enables llama.cpp on other platforms.

## Data Flow

1. **Fetch/Parse**: Read from API or file without holding the DuckDB lock
2. **Upsert**: Acquire the writer briefly per completed `ParseResult`
3. **Index**: Rebuild FTS under a short writer connection
4. **Embed**: Compute vectors unlocked, then update each batch under a short writer connection

### Just-in-time local ingestion

Claude Code and Codex lifecycle hooks write coalesced records under
`data/hook_inbox/`; records contain only provider, transcript path, size, and
mtime. The hook process never opens DuckDB. A detached drain atomically claims
each record, parses stable transcript snapshots without a DB lock, upserts each
snapshot under a short writer connection, marks changed text for one coalesced
FTS rebuild, and records the processed snapshot. New events cannot be deleted
by an older in-flight claim, and orphaned claims are retried after a process crash.

`search`, `query`, `read`, and `sql` drain pending records before opening their read
connection. `search` and `query` rebuild FTS once when dirty, while `query`
also atomically claims and embeds only dirty message ids;
concurrent queries share an embedding lock without holding DuckDB open. Ingest
is additive: missing records are never deleted, and rewritten records preserve
the prior payload under a deterministic history id with provenance metadata.
`sync` drains the same inbox before its normal local/web reconciliation.
5. **Query**: CLI commands read from DB, apply filters, format output

## File Layout

User data (DuckDB + state) lives under `~/.convos` by default (override with `CONVOS_PROJECT_ROOT`).

```
convos/
├── src/ai_convos/
│   ├── __init__.py      # exports app
│   ├── cli.py           # all logic
│   └── __main__.py      # module entry point
├── apps/                # optional user-installable products
├── data/
│   └── convos.db        # DuckDB database
├── tests/
│   ├── test_integrations.py
│   └── test_parsers.py
└── docs/                # this documentation
```

## Extension Points

**Adding a new source:**
1. Write `fetch_X` or `parse_X` returning `ParseResult`
2. Register in `fetchers` or `parsers` dict
3. Add tests

**Adding new metadata:**
1. Prefer storing in `metadata` JSON column
2. Only add schema columns for frequently queried fields

**Adding new table:**
1. Add to `init_schema()`
2. Add to `ParseResult` class
3. Add to `upsert()` function
