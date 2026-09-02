# Convos

[![Tests](https://github.com/RobertBiehl/convos/actions/workflows/tests.yml/badge.svg)](https://github.com/RobertBiehl/convos/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/convos?style=flat)](https://pypi.org/project/convos/)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat)](LICENSE)

**Queryable memory for coding agents, backed by exact conversation evidence.**

Convos captures Claude Code and Codex work automatically, recalls relevant
decisions across ChatGPT, Claude, Claude Code, and Codex, then gives agents the
exact turns they need to continue.

## Give this to your agent

```text
Install Convos from PyPI with `uv tool install convos` (use pipx only if uv is unavailable), run `convos init`, then run `convos doctor` and repair any reported skill or hook issue. Run `convos embed` so semantic retrieval is ready, use `convos query "why did we make this decision?"` to find a relevant past conversation, and verify the strongest result with `convos read CONVERSATION_ID --around MESSAGE_ID`. Do not configure web sync, credentials, or a remote service unless I ask.
```

The daily agent workflow is **Capture -> Recall -> Continue**: lifecycle hooks
capture completed local turns, hybrid retrieval finds the right prior work, and
bounded reads provide exact evidence instead of generated recollections.

## Why this exists

- Resume work across coding agents without reconstructing old sessions
- Retrieve prior decisions, commands, evidence, and edits without dumping whole transcripts
- Keep ChatGPT, Claude, Claude Code, and Codex history locally searchable
- Keep the same encrypted memory available across computers without path allowlists
- Share project-associated prompts and changes automatically with encrypted team workspaces
- Use a CLI skill and lifecycle hooks; the self-hosted relay is optional

## Features

- Fast full-text search with direct source, day, role, project, conversation, and thinking filters
- Hybrid semantic search (BM25 + embeddings + Reciprocal Rank Fusion) via `convos query`
- Fetch from ChatGPT and Claude using browser cookies
- Import exports from ChatGPT, Claude, Claude Code, and Codex
- Capture completed Claude Code + Codex turns just in time with lifecycle hooks
- Deterministic project resume packets and exact session replay
- Optional code-change provenance: blame, timeline, time travel, and graph browsing
- Optional end-to-end encrypted personal multi-device and team synchronization
- Local secret scanning with mandatory pre-encryption team redaction
- Export to JSON or CSV

## Install

Install from PyPI with uv, initialize local capture, and prepare semantic recall:

```bash
uv tool install convos
convos init
convos embed
convos doctor
```

Semantic retrieval is included on macOS and Linux. Linux uses the compact
Model2Vec `potion-base-8M` model without a compiler toolchain; macOS keeps the
existing EmbeddingGemma model through llama.cpp. Set `CONVOS_SEMANTIC=0` to
disable model loading, embedding, and semantic queries while retaining literal
`convos search`. The `semantic` extra remains available when llama.cpp is
explicitly wanted on another platform.

Upgrade later with:

```bash
uv tool upgrade convos
convos install-skills
```

macOS may compile `llama-cpp-python` locally when no compatible wheel is
available. The default Linux install, literal search, capture, and remote sync
do not require a native compiler.

`convos init` creates the archive, installs the bundled Codex + Claude Code
skill and capture hooks, imports existing local Codex and Claude Code sessions,
and performs safe local setup for installed products. That first local scan is
incremental on later runs and never probes ChatGPT or Claude web. With the
Memory product installed, the same command turns on automatic memory delivery
for the current project. It never downloads the retrieval model, configures a
remote service, or deletes data. Codex asks you to review new or changed hooks once
through `/hooks`. Refresh only the skill with:

```bash
convos install-skills
```

## Encrypted Remote

Install the Remote client beside Convos to carry the same encrypted archive
between personal devices or into team workspaces:

```bash
uv tool install convos --with convos-remote
```

The relay is a separate, independently deployable package and never receives
conversation plaintext, repository names, file paths, embeddings, attachments,
or workspace keys:

```bash
uv tool install convos-remote-server
```

See [self-hosting, recovery, team policy, and installation](docs/remote.md) and
the runnable synthetic [personal and team scenarios](examples/remote/README.md).

## Additional workspace products

Redact is published on PyPI because it is part of Remote's team boundary. The
other applications below remain source-only while their public product surface
is still being shaped.

Optionally add code-change provenance without expanding the core CLI package:

```bash
uv tool install convos --with "convos-changegraph @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/changegraph"
```

This adds `convos blame`, `timeline`, `at`, `graph`, and `browse`.

Navigate semantically from any exact conversation or turn without sending
archive text to a generation service:

```bash
uv tool install convos --with "convos-explore @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/explore"
convos related CONVERSATION_OR_MESSAGE_ID
convos trail CONVERSATION_OR_MESSAGE_ID
```

`related` returns one strongest matching turn per neighboring conversation,
collapses exact duplicate turns, excludes known injected agent scaffolding, and
prints exact `read --around` pivots. `trail` follows those evidence turns
through a bounded, cycle-free multi-hop semantic graph with text, JSON, JSONL,
and DOT output. See [semantic conversation exploration](docs/explore.md).

Audit an archive for high-confidence credentials without printing their values:

```bash
uv tool install "convos[redact]"
convos redact scan
```

The encrypted remote client installs this scanner as a required dependency and
scrubs every team record before signing or encryption. Personal synchronization
remains lossless. Team attachment bodies are omitted because arbitrary binary
content cannot be proven safe by the dependency-free scanner. Use
`convos redact status` to inspect value-free automatic-redaction records. See
[local secret protection](docs/redact.md).

Resume a project from live repository state and exact archived evidence without
asking a model to invent a summary:

```bash
uv tool install convos --with "convos-resume @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/resume"
cd /path/to/project
convos resume
```

The packet includes current Git branch, HEAD and bounded dirty status, recent
cwd-scoped sessions, exact last-turn IDs, touched files, tool statuses, and
secret-scrubbed turn excerpts under a global evidence budget. It labels archived
text as untrusted and prints exact `read --around` commands for verification.
`-f json` exposes the same deterministic structure to agents. See [project
resume packets](docs/resume.md).

```bash
convos replay CONVERSATION_ID
convos replay CONVERSATION_ID --around MESSAGE_ID -n 40 --activity 120
```

Replay returns a bounded exact message window with its ordered tool calls and
file edits. It is evidence of captured activity, not an inferred summary.

Track and reconcile Codex and Claude Code memories through a canonical local
overlay without rewriting either provider's generated state:

```bash
uv tool install convos --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
convos init
```

`init` is safe to rerun after adding the Memory extra. Existing installations
can use `convos memory enable` as the narrower repair/upgrade command. New
projects initialize automatically on first context delivery; use `convos memory
enable --all` only to warm every already-discovered scope up front. Memory adds
one-command project synchronization with deterministic safe
bootstrapping and exact matching, agent-assisted semantic resolution,
agent-ready automatic Claude and Codex session injection, direct revisioned
user-owned `remember`/`forget`, automatic safe
reconciliation during delivery, history, and reversible drift-safe Claude
projection. A remembered revision can cite exact local archive turns with
repeatable `--from MESSAGE_ID`; audits verify their hashes and print direct
`read --around` pivots without duplicating or remotely syncing conversation
text. `convos memory sync --all` safely settles
mechanical updates across every project without exposing memory content. The normal
`convos doctor` also checks its ledger and delivery setup. If a memory decision
is needed, `convos memory review` shows plain before/new/current text without
engine IDs; then just tell Codex or Claude `sync my memories`. The skill handles
the plan and transaction. Codex requires one-time review of new or
changed hook entries through `/hooks`. See [the memory synchronization
workflow](docs/memory.md).

Run bare `convos memory` for plain current-project health: available memories,
automatic delivery, and the exact next action when attention is needed. Engine
counts, hook trust, and source provenance stay in `convos doctor` and JSON.
Help shows only normal human commands; the synchronization protocol remains
available to installed agents without cluttering the first-run API.
`convos memory backup` creates a private, consistent snapshot of the complete
ledger. `convos memory restore SNAPSHOT` previews recovery; `--yes` restores it
only after automatically preserving the current ledger as a rescue snapshot.
The snapshot's private Git evidence lets matching clones and worktrees reuse the
same memory scope at different checkout paths while distinct fork origins stay
isolated.

`convos memory audit` verifies every current and historical evidence hash in
the surrounding project without printing memory or transcript content. Use
`--message MESSAGE_ID` for reverse provenance or `--all --json` for a
content-free cross-project health check.
Ordinary revision, history, and preview-first deletion accept a memory's
displayed first-line title or unique literal text, so stable `mem_...` IDs are
available for automation without being required for routine use.

For an unreleased Git snapshot, install both products from the same revision:

```bash
uv tool install --reinstall "git+https://github.com/RobertBiehl/convos.git" \
  --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
```

The encrypted remote uses one client package and one independently installable
server package, so the local archive stays server-free by default.
When the memory and remote clients are both present, personal background sync
automatically carries canonical memory revisions in the same end-to-end
encrypted event stream; concurrent semantic changes remain reviewable instead
of becoming last-write-wins. User-owned forget operations also remove safe
remote-only copies and the author's prior relay ciphertext while preserving
local or provider divergence. Neither product depends on the other.
See [`examples/insights`](examples/insights/README.md) for local decision,
comparison, archive-statistics, and prompt-to-change query recipes.

## Quickstart

```bash
convos init
convos sync                  # optional ChatGPT, Claude web, and export backfill
convos doctor
convos search "prompt" -s claude -n 10
convos query "conceptual search"
```

If Safari cookies are protected by macOS privacy, `sync` will fall back to Chrome.

## Common commands

Search:

```bash
convos search "vector database" -s chatgpt -d 30   # BM25 only
convos search "decision" --cwd /path/to/repo       # exact project scope
convos query "why did we choose this?" --conversation f2b9c5a9
convos search "reasoning" --thinking
convos read f2b9c5a9 -n 20 -f jsonl              # bounded recent context from one result
convos embed --limit 1000                         # explicitly backfill a bounded batch
convos query "how do I store vectors in duckdb"    # hybrid: BM25 + embeddings + RRF
convos fts                                        # explicitly refresh BM25 after imports
convos backup                                     # database plus retained attachment bodies
```

Both discovery commands return the strongest matching message from each
conversation, so `-n` controls the number of distinct conversation candidates.
Both accept `--cwd`/`-w` to include one recorded directory and its descendants,
plus `--conversation` for an exact conversation-ID prefix. These direct options
replace the deferred custom query language.

Semantic search is included by default. Run `convos embed` after install to
backfill embeddings with a progress bar. `search` and `query` never ingest,
reindex, or embed as a side effect. When BM25 is stale they use a complete
literal scan and warn on stderr; run `convos fts` for BM25 ranking and
`convos embed` for semantic coverage. macOS uses the 768-dimensional
`embeddinggemma-300m-qat-q8_0` model through llama.cpp; Linux has no default
semantic runtime. The archive stores
the exact active profile and atomically queues a full rebuild before a different
model, revision, dimension, prefix, or normalization contract can be used.

Read a known conversation using an ID prefix from search/query:

```bash
convos read f2b9c5a9 -n 20 -c 2000 -f jsonl
convos read f2b9c5a9 --around 01ab -n 20 -f jsonl
```

Replay one conversation with its captured tool and edit evidence:

```bash
uv tool install convos --with "convos-resume @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/resume"
convos replay CONVERSATION_ID
convos replay CONVERSATION_ID --around MESSAGE_ID -f json
```

Replay is part of the Resume product and keeps the same deterministic,
explicitly bounded evidence contract. See [project handoff and
replay](docs/resume.md).

List and analyze with read-only DuckDB SQL (schema in `docs/database.md`):

```bash
convos sql "SELECT id, title, created_at FROM conversations ORDER BY created_at DESC LIMIT 20" -f json
```

Sync:

```bash
convos sync                         # local plus available web/export sources
convos sync --local-only            # local agents and configured exports; no web
convos sync -w -i 600
```

Local Claude Code and Codex sessions can be ingested after each completed turn:

```bash
convos install-hooks             # repair or refresh hooks installed by init
convos install-hooks --status
convos install-hooks --remove    # remove only convos hook handlers
```

Start a new agent session after installing hooks. In Codex, review the user
hook through `/hooks`; after the first completed turn, `convos doctor` should
show a recent `ingest: ... last=...` timestamp.

Hooks enqueue only the local transcript path and file metadata, then return
immediately. A coalescing background drain parses and upserts the transcript;
retrieval commands only read committed archive state. `convos fts` explicitly
rebuilds BM25, and `convos embed [--limit N]` explicitly computes missing
vectors without holding DuckDB during model inference. `sync` remains the
reconciliation path for missed local events, web
providers, pre-hook sessions, and imports rather than a routine local update.

Check the complete local pipeline with `convos doctor`. It reports the running
version, archive/schema/FTS health, embedding backlog, queued ingestion, hook
installation, exact freshness of both installed agent skill copies, and
web-cookie availability without modifying the archive. Missing or stale skill
content reports `convos install-skills` as the repair. Hook health similarly
requires the current executable and archive root exactly once in every required
agent event; stale, duplicated, or misplaced handlers report `convos
install-hooks`.

Auto-import export paths with:

```bash
CONVOS_IMPORT_PATHS="~/Downloads/chatgpt-export.zip,~/.claude/projects" convos sync
```

Export:

```bash
convos export out.json -f json
convos export out.csv -f csv -s claude
```

## Example output

```bash
convos sync
```
```text
Syncing Claude Code (2 convs, 118 msgs, 12 tools, 0 attachs, 4 edits)
Syncing Codex (8 convs, 214 msgs, 19 tools, 0 attachs, 0 edits)
Syncing ChatGPT (142 convs, 1734 msgs, 97 tools, 12 attachs, 0 edits)
Syncing Claude (96 convs, 842 msgs, 0 tools, 5 attachs, 0 edits)
Updated Codex (0 new, 1 updated convs; 0 convs, 9 msgs, 0 tools, 0 attachs, 0 edits processed)
Updated 0 new, 1 updated convs; 9 msgs, 0 tools, 0 attachs, 0 edits
Total: 248 convs, 2908 msgs, 128 tools, 17 attachs, 4 edits
```

```bash
convos search "vector database" -s chatgpt -d 30
```
```text
f2b9c5a9  ChatGPT  "Indexing embeddings with DuckDB"  2026-01-14T09:22:11Z
8a1d0c3e  ChatGPT  "Choosing ANN libraries"           2026-01-10T18:03:42Z
```

```bash
convos read f2b9c5a9 -f jsonl
```
```text
{"id":"01ab...","role":"user","content":"How do I store vectors in DuckDB?","thinking":null,"created_at":"2026-01-14 09:22:11"}
{"id":"02cd...","role":"assistant","content":"Use a table with a FLOAT[] column and an HNSW index...","thinking":null,"created_at":"2026-01-14 09:22:42"}
```

## Data model

Data lives in `<root>/data/convos.db` (DuckDB). Default root is `~/.convos` (override with `CONVOS_PROJECT_ROOT`).

- `conversations`
- `messages`
- `tool_calls`
- `attachments`
- `artifacts`
- `file_edits`

## Privacy and security

This is local-first. Your data never leaves your machine unless you export it
or explicitly configure the optional encrypted remote. The remote receives
ciphertext and synchronization metadata, never workspace keys or plaintext.

On macOS, Safari cookie access requires Full Disk Access for your terminal.
If you prefer not to grant it, use Chrome cookies with `-b chrome`.

## FAQ

Q: Why is fetch failing on Safari?
A: macOS blocks access to Safari cookies without Full Disk Access. Use `-b chrome` or grant access.

Q: Where is the database stored?
A: `~/.convos/data/convos.db` by default (override with `CONVOS_PROJECT_ROOT`).

Q: Can I reset the DB?
A: Delete `~/.convos/data/convos.db` (or `<root>/data/convos.db`) and re-run `convos init`.

## Contributing

PRs welcome. Keep changes small and focused. See `AGENTS.md` for architecture and coding style.

## Agent usage

Agents should use the CLI only. See `skills/convos/SKILL.md`.
For setup and usage with Codex/Claude, see `docs/skills-setup.md`.
