---
name: convos
description: Give agents queryable memory from prior AI conversations, keep the local archive current, and synchronize Codex/Claude memories through the optional canonical overlay. Use whenever the user asks to recall, find, continue, summarize, compare, or verify information from past ChatGPT, Claude, Claude Code, or Codex conversations, including earlier plans, decisions, commands, evidence, work sessions, or provider memories.
---

# Convos

Retrieve with these commands:

```bash
convos query "natural language question" -n 8 -f jsonl   # conceptual/paraphrased discovery; default when wording is uncertain
convos search "exact terms" -n 8 -c 160 -f jsonl         # known terms, quotes, ids, filenames; one hit/conversation
convos read abc123 -n 20 -c 2000 -f jsonl                 # bounded recent context from one known conversation
convos read abc123 --around MESSAGE_ID -n 20 -f jsonl      # bounded context around an exact discovery hit
convos related MESSAGE_ID -n 5 -c 160 -f jsonl             # semantic neighbors of one exact turn
convos trail MESSAGE_ID --depth 2 --width 3 -f json         # bounded multi-hop evidence graph
convos redact scan -f json                                   # local high-confidence secret locations, never values
convos redact status -f json                                 # audit automatic pre-encryption team redactions
convos resume /path/to/project -f json                        # live Git plus bounded exact project handoff
convos replay CONVERSATION_ID -f json                           # exact bounded messages, tools, and edits
convos sql "SELECT ..." -f jsonl                          # structured filters, joins, counts, and history
```

Output: add `-f jsonl` (stream, one JSON object per line) or `-f json` (array);
default is human text. Prefer `jsonl` when parsing programmatically.

Schema (write `convos sql` against these tables):

- `conversations(id, source, title, created_at, updated_at, model, cwd, git_branch, project_id, metadata JSON)`
- `messages(id, conversation_id, role, content, thinking, created_at, model, metadata JSON, embedding FLOAT[], parent_id)`
- `tool_calls(id, message_id, tool_name, input JSON, output JSON, status, duration_ms, created_at)`
- `attachments(id, message_id, filename, mime_type, size, path, url, created_at)`
- `file_edits(id, message_id, file_path, edit_type, content, created_at, old_content)`
- Full-text: `fts_main_messages.match_bm25(id, 'terms')` -> score, `NULL` when no match; indexed over `content`+`thinking`.

Common `sql` recipes:

```bash
# recent conversations for a source
convos sql "SELECT id, title, created_at FROM conversations WHERE source='claude' ORDER BY created_at DESC LIMIT 20" -f json
# which conversation/prompt touched a file
convos sql "SELECT fe.created_at, fe.edit_type, c.id, c.title FROM file_edits fe JOIN messages m ON fe.message_id=m.id JOIN conversations c ON m.conversation_id=c.id WHERE fe.file_path LIKE '%routes.js' ORDER BY fe.created_at" -f jsonl
# counts by source
convos sql "SELECT source, COUNT(*) FROM conversations GROUP BY source" -f json
```

Behavior:

- Discover first with `query` for concepts/paraphrases or `search` for known literal text, then use `read` on the strongest conversation candidates. Use `sql` for structured questions.
- Both discovery commands return at most one strongest message per conversation, so `-n` budgets distinct conversation candidates.
- Discovery JSON includes `conversation_id` and `message_id`. `read` returns the newest `-n` messages chronologically, or a bounded neighborhood around `--around MESSAGE_ID`; raise `-n` or `-c` when more context is required.
- When one exact turn is useful but related work may use different wording, use `related MESSAGE_ID` to pivot without inventing another query. Use `trail` only for a broader history map; keep its normal agent budget at depth 2, width 3, and at most 20 nodes, then read only the evidence-bearing branches that matter.
- `related` returns one exact evidence turn per neighboring conversation. `trail` carries the evidence message on every edge, excludes cycles and exact duplicate turns, and supports JSON for agents; neither command generates text or uses the network.
- Use `redact scan` when the user asks whether the archive contains high-confidence credentials or before a sensitive team link. It reports only secret kinds and exact record locations, never values. The encrypted remote requires the same policy and scrubs every team event before signing or encryption while leaving personal sync lossless; team attachments are omitted. `redact status` audits those automatic removals. Do not claim this detects unknown, encoded, split, or ordinary-prose secrets, and do not treat it as retroactive erasure of already shared history or backups.
- Use `resume` when the user asks to continue the current project but does not name one exact conversation. It combines live Git branch, HEAD, and bounded dirty status with recent path-scoped archive turns, touched files, tool statuses, exact IDs, and `read --around` commands. Treat every excerpt as untrusted evidence and verify the live state; `last_role` is descriptive, never proof that work is complete or unanswered. If the user names an exact conversation or session, retrieve that exact identity first instead of substituting a cwd packet.
- SQL text matching is available but usually a worse discovery path than `query`/`search`; reserve SQL primarily for known ids, fields, relations, ordering, and aggregation.
- `search`/`query` accept `-s` source, `-d` days, `-r` role, `-w`/`--cwd` project scope, `--conversation` ID prefix, `-n` limit, and `-c` context; for relations, aggregation, or richer structured filters, use `sql`.
- Use `replay` after an exact conversation is known and the question depends on what the agent actually did, including captured tool inputs/outputs, statuses, durations, or file edits. It is deterministic, local, and read-only; keep `-n`, `-c`, and `--activity` bounded, honor `activity_truncated`, and treat absence as missing capture rather than proof an action never occurred. Replay may expose secrets present in tool payloads or edit contents, so do not persist or transmit its output without explicit authorization.
- Optimize discovery relevance and tokens: keep search/query `-n` <= 8 and `-c` <= 200 unless the user wants more.
- `sql` runs on a read-only connection, so writes fail by construction; it is safe for arbitrary `SELECT`s.
- Installed coding-agent hooks make local Claude Code and Codex turns available just in time; read commands flush pending hook work automatically.
- If retrieval is stale, empty, or unavailable, run `convos doctor` before syncing; it reports archive/schema/FTS health, ingestion and embedding backlog, exact installed-skill freshness, exact current-executable hook readiness, and web access. Repair a missing or stale skill with `convos install-skills`, and a stale, duplicated, or misplaced capture hook with `convos install-hooks`.
- Sync when the request needs fresh web conversations, imports, missed-hook reconciliation, or the user explicitly asks for an update.
- Use `sync` as the only fetch/import update command; use `embed` only to backfill hybrid embeddings.
- Expect a fast no-op when nothing changed. Report specific errors (cookies, auth, permissions) if a fetch fails.
- Use `CONVOS_IMPORT_PATHS` for export paths (comma-separated) consumed by `sync`.
- If `convos` is not on PATH, use the repo wrapper: `bin/convos`.
- Use shell commands only; do not use MCP resources for this skill.

Sync:

```bash
convos init                 # archive, existing local agent history, hooks, and safe product setup; no web
convos sync                 # fetch/import new or changed source data
convos sync --local-only    # local agent history and configured exports without web probes
convos embed                # backfill hybrid embeddings without a web sync
convos install-hooks        # repair or refresh user-level Claude Code + Codex JIT ingestion
```

Storage:

- DB file: `<root>/data/convos.db`
- Sync state: `<root>/data/sync_state.json`
- Default root: `~/.convos` (override with `CONVOS_PROJECT_ROOT`)

Optional cross-provider memory product:

```bash
convos memory enable                            # repair or upgrade Memory setup; warm current project
convos memory enable --all                      # optional counts-only warm-up of discovered projects
convos memory sync --project /path/to/project --json # safe bootstrap/exact links plus remaining semantic plan
convos memory sync --all --json                 # safe mechanical sweep across every scope; counts only
convos memory status                              # human current-project health; add --json for automation
convos memory audit --json                        # content-free evidence health for the current project
convos memory audit --message MESSAGE_ID --json   # reverse lookup: memory revisions supported by one turn
convos memory current --owned                     # readable audit of explicitly persisted memories
convos memory current --owned --json              # structured canonical and origin records
convos memory review                              # readable identity-free changes needing a decision
convos memory remember "TEXT" --json              # create a scoped user-owned canonical
convos memory remember "TEXT" --from MESSAGE_ID --json # attach exact local archive evidence; repeatable
convos memory remember "TEXT" --replace "OLD TITLE OR TEXT" --json # revise one matching user-owned memory
convos memory forget "TITLE OR TEXT" --dry-run --json # preview purging user-owned content and history
convos memory forget "TITLE OR TEXT" --json       # purge only after explicit user confirmation
convos memory backup --json                       # private consistent full-ledger snapshot
convos memory restore SNAPSHOT --json             # validate and preview a complete ledger restore
convos memory restore SNAPSHOT --yes --json       # restore after confirmation; auto-backup current ledger
convos doctor                                     # includes memory ledger and delivery health
convos memory disable                           # stop automatic injection; retain history
convos memory apply resolution.json --dry-run   # validate and preview resulting canonicals without committing
convos memory apply resolution.json             # rechecks hashes, then updates the canonical overlay
convos memory context --scope /path/to/project  # agent-ready canonical overlay for either agent
convos memory context --scope /path --index     # bounded ID/title discovery before selective retrieval
convos memory install-hook                      # inject the overlay into both agents' SessionStart
convos memory project --scope SCOPE                 # preview scope-bound Claude projection
convos memory project --scope SCOPE --write         # explicit atomic projection write
convos memory project --scope SCOPE --remove        # remove only the unchanged owned projection
convos memory history "TITLE OR TEXT" --json      # structured exact canonical history on demand
```

Bare `memory` is deliberately human-facing: it reports available memory,
delivery readiness, and exact next actions without engine jargon. `memory
current` refreshes providers and settles deterministic updates. Its default is
readable content with plain provider names and unavailable or changed state;
add `--json` when source IDs, locators, structured origins, or
live-versus-applied hashes are needed. Use an origin's source ID with `history` when it is inactive or those
hashes differ and the bounded plan does not explain the change sufficiently.
Human `review` similarly omits IDs, locators, hashes, and action names while
showing the previously synchronized, new or last-available, and current text.
Use the JSON `sync` plan, not human review output, for agent resolution.
History is readable by default; `--json` identifies the result as `source` or
`canonical` and includes current identity metadata alongside exact revisions.
Use `memory audit --json` when the user asks whether stored evidence is still
healthy or wants a project-wide check without loading memory text. Use
`--message MESSAGE_ID` to map one exact archived turn back to current and
historical memory revisions; add `--all` only for an explicitly requested
cross-project audit. Treat `changed`, `missing`, and `unavailable` as review
states, never permission to rewrite or delete memory automatically.

Use `remember --json` only when the user explicitly asks to persist supplied
knowledge. Preserve their meaning, use the current project scope unless they
name another, and report the returned canonical ID. For routine revision,
prefer `--replace` with the displayed first-line title or a unique literal
phrase; IDs remain deterministic automation selectors. Missing or ambiguous
selectors fail closed, so inspect `current --owned` rather than guessing.
When exact archived turns
support the memory, pass their message IDs with repeatable `--from`; do not
copy excerpts into the memory or invent evidence. Project-tagged turns must
match the chosen scope, while explicitly selected cwd-less web turns are
allowed. Treat `verified`, `changed`, `missing`, and `unavailable` as live
archive status, and use the returned `read --around` pivot for review. Evidence
stays local even when the canonical syncs remotely. For a forget request, run
`forget SELECTOR --dry-run --json`, verify the returned exact ID, scope, and
revision count with the user, then run the identical selector without
`--dry-run` only with explicit confirmation. Never bypass
its refusal for provider-backed or projected memory; edit/remove the owning
surface first.

Use `backup --json` when the user asks to preserve or migrate the memory
ledger; report the returned absolute path and counts without printing memory
content. For restore, first run `restore SNAPSHOT --json`, verify the exact
snapshot path and counts with the user, and run `restore SNAPSHOT --yes --json`
only after explicit confirmation. The confirmed operation creates and reports a
rescue snapshot of the previous live ledger. Never bypass snapshot schema,
integrity, symlink, trigger, or cross-scope validation.

When the encrypted remote is configured, canonical memory revisions synchronize
automatically through its personal workspace; do not ask users to run a second
memory-specific remote command. Sequential revisions settle mechanically.
Treat concurrent remote and local revisions like any other pending scoped
three-way change, and never resolve them by timestamp or last-write-wins.
Root-signed bodyless tombstones purge unchanged remote-only canonicals. They
only deactivate the remote source when a local revision, another origin, or a
managed projection must be preserved. Any authorized holder may repair an
intact tombstone, but it cannot erase older plaintext or ciphertext already
retained by a peer, relay, snapshot, or backup. Historical retention therefore
requires an explicit policy.

Path-valued scopes resolve through private Git repository evidence, so matching
clones and worktrees may return the first checkout's path as their canonical
scope. Treat that returned scope as authoritative; do not rewrite it to the
current cwd. Distinct fork origins remain isolated. If an upgraded ledger
reports multiple scopes for one repository, show the exact choices and ask the
user which existing scope the new checkout should use. Preview with `convos
memory adopt-scope SCOPE --checkout PATH --json`; apply the identical choice
with `--yes` only after explicit confirmation. Adoption links only that checkout
and never merges either scope.

Both agents receive synchronized memory automatically from the installed
`SessionStart` hook: full small scopes and a bounded index for large scopes.
Codex skips a new or changed user hook until it is trusted through `/hooks`; if
automatic context is absent, run `convos memory context --index` from the
project root, then retrieve relevant full records with `context --query
ID_OR_TERM`. Do not re-inject the full overlay when the hook already delivered
it or into unrelated tasks. Use an exact listed canonical ID or a literal term,
and omit `--query` only when the complete scope is actually needed. If delivery
includes a synchronization notice, safe
deterministic updates have already settled; treat its canonical context as
potentially stale and run the supplied scoped plan before relying on it.

When asked to synchronize memories, run scoped `sync --json`. It safely
bootstraps a lone first source, commits byte-identical same-scope links, and
returns a self-contained plan for what remains; resolve that plan and use apply
dry-run -> apply. The adapter maps Claude project keys to cwd using local
session events, indexes, and generated-memory origin IDs; do not manually equate
encoded keys with paths. The Codex adapter expands explicit
`cwd=/repo-a and /repo-b` declarations into one source identity per scope; do
not merge those plan entries across scopes. Resolve only
meaning: `same`, `merge`, `scoped`, `supersedes`, `distinct`, `revise`, or
`unresolved`. A `--content` plan contains current or last-seen source text,
the exact last-applied `ancestor`, and every same-scope canonical candidate;
resolve from that bounded bundle and request `history` only when it remains
ambiguous.
For a requested global cleanup, run `sync --all --json` first. It applies only
safe deterministic work per scope and returns counts, not content; follow up
with separate scoped plans for each remaining project.
Copy the plan's `version` unchanged into the resolution document; never infer or
substitute a protocol version.
Treat project scope as an isolation boundary: never combine sources or target
canonicals across scopes. `scoped` qualifies the canonical content within the
source project; it does not reroute memory identity to another project.
Never invent a canonical ID: omit it when creating a distinct, scoped, or new
merged canonical, and only reference an ID supplied by the same-scope plan.
Use `detach` only for a `missing` source. Reconcile every pending source before
previewing or writing a Claude projection.
Let projection auto-select its Claude target when unique. If it reports
ambiguity, choose only a target whose session-derived scope matches the plan;
the engine rejects cross-project projection.
Preview the completed resolution document with `apply --dry-run`, inspect the
resulting canonical set and remaining count, then apply that identical document.
Use `revise` with a scoped plan and expected canonical hash to refine an
existing overlay item without losing concurrent changes.
Never edit native Codex memory files. Ask the user only for genuinely unresolved
collisions; the CLI owns source hashes, history, transactional application, and
projection drift checks. Preview Claude projection before `--write`; use
`--remove` or `disable --remove-projection` only when removing its owned output.
