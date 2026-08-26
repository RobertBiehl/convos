# Cross-provider memory synchronization

The optional `convos-memory` product tracks exact revisions of Codex,
Claude Code, and user-owned memories in a separate SQLite ledger, then builds a
canonical local overlay through an agent-assisted synchronization workflow.

Provider files are read-only inputs. Codex generated memory is never rewritten,
and Claude files are not modified behind the user's back. Both agents consume
the canonical overlay automatically through scoped `SessionStart` hooks;
`convos memory current` is the explicit canonical inspection surface, while
`convos memory audit` checks archive evidence health without printing memory or
message content.

`current` refreshes read-only providers, commits the same safe deterministic
updates as context delivery, then renders each canonical and its provenance for
humans. Every origin shows its source ID, provider, provider-relative locator,
live activity, and whether a newer revision is pending. Add `--json` for the
full structured records, including live and last-applied hashes. An inactive
source or unequal hashes expose pending input directly, and the source ID can be
passed to `history`.
`history` identifies its result as `source` or `canonical`, repeats the
identity's current scope and hash plus provider metadata where applicable, and
returns its exact timestamped revisions. User-owned revisions may also carry
local archive evidence: exact message pivots with live `verified`, `changed`,
`missing`, or `unavailable` status.

## Backup and recovery

```bash
convos memory backup
convos memory backup /secure/path/memory.db
convos memory restore /secure/path/memory.db
convos memory restore /secure/path/memory.db --yes
```

`backup` creates a transactionally consistent SQLite snapshot of the complete
ledger: all scopes, observed provider and user sources, canonical state, both
revision histories, local evidence links, projection ownership, and remote
reassembly state.
The default destination is a unique timestamped file in the live ledger's
`backups/` directory. Snapshots
are mode `0600`, published atomically, and never overwrite an existing file or
write through a symlink target.

Restore is preview-first. The default command validates the snapshot's
integrity, exact required schema, absence of injected triggers, and cross-scope
link invariant, then prints only counts. `--yes` first creates a new rescue
snapshot of the current ledger and then restores through SQLite's transactional
backup API. The source must be a regular non-symlink file and cannot be the live
ledger itself.

Snapshots from supported older ledger schemas are migrated in a temporary
in-memory copy during validation and restore. The snapshot file remains
unchanged, while the restored live ledger is always written in the current
schema. Malformed base tables, injected triggers, failed integrity checks, and
newer unknown schemas still fail closed.

A ledger snapshot does not contain native Codex or Claude memory files, agent
hook settings, or installed skills. It does preserve repository-scope evidence:
after restore, a clone or worktree at another path automatically reuses the
existing scope when its normalized non-local `origin` matches. SSH and HTTPS
forms normalize to the same repository. Without a usable origin, shared Git
root commits provide the fallback identity. Non-Git and unavailable paths remain
literal. Run `enable` after machine recovery to reinstall delivery.

## Encrypted multi-device delivery

When both `convos-memory` and `convos-remote` are installed, the remote
client discovers the memory adapter automatically. Its normal background sync
publishes canonical revisions only to the user's personal workspace and
projects incoming revisions into the private memory ledger. There is no second
memory account, workspace link, sync command, or server feature to configure.
The packages remain independently usable: memory has no remote dependency, and
remote continues normally when memory is absent.

Canonical content, IDs, repository identity, revisions, and tombstones travel
as user-root-signed semantic objects inside ordinary end-to-end encrypted row
replicas. The proof binds the personal workspace, object identity, content
hash, state, full causal ancestry, author user and device, and authorization
epoch. The relay sees workspace, uploader, epoch, ciphertext size, and timing,
but not memory content, repository names, or checkout paths. Absolute paths
never enter the object.
Matching Git origins bind a received repository scope to a local clone; a
remote-only scope remains portable until such a clone is observed.
Archive evidence stays device-local: remote records contain no message or
conversation IDs, titles, timestamps, roles, or evidence hashes.

Offline edits queue locally. Repeated delivery and older revisions are
idempotent. A single remote source advances an unchanged canonical
automatically. Each current proof carries the hashes of its complete ancestry,
so intermediate bodies may be discarded without making a later descendant
ambiguous. Concurrent local and remote changes are deliberately not
last-write-wins: the local canonical stays visible and the remote revision
appears in the normal three-way `review`/agent-resolution workflow. A remote
deletion automatically purges an unchanged remote-only canonical and its local
memory revisions. A locally revised, projected,
or mixed-origin canonical is retained instead; its remote origin becomes
missing so local or provider-owned knowledge cannot be erased by another
device.

Each canonical is one semantic object and one replica, subject to the relay's
48 MiB row-replica ceiling. This keeps signing, verification, conflict handling,
and recovery identical for small and large memories without a second part
assembly protocol. Plaintext exists only in local memory ledgers.

What users still need to do is the one-time setup for both products: enable the
installed products with `convos init`, approve the Codex hook once, configure
the encrypted remote on the first device, and retain its recovery key. Existing
archives can run `convos memory enable` instead of repeating the complete local
initializer. After recovery on another device, ordinary worker sync carries
memory automatically. Semantic conflicts are the only routine interruption.
Keep `memory backup` snapshots for complete private-ledger recovery; remote
events synchronize canonical state, not every device-local provider
observation, hook setting, or native provider file.

## Install

```bash
uv tool install convos --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
convos init
```

The core extra is installation metadata, not an implementation boundary:
`convos-memory` remains independently versioned and its metadata enforces
the compatible core range. For an unreleased source snapshot, install both
products from the same Git revision:

```bash
uv tool install --reinstall "git+https://github.com/RobertBiehl/convos.git" \
  --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
```

Memory 0.10 supports `convos>=0.10,<0.11`. The package metadata enforces
that contract, so an independently installed memory wheel cannot silently pair
with an unsupported core CLI.
`init` installs both agent skill copies, core capture hooks, and both agents'
Memory `SessionStart` hooks, imports existing local Codex and Claude Code
history, then warms the current project. It is local, incremental, and safe to
rerun after adding the extra. It never probes web sources, downloads an optional
model, configures a remote service, or deletes data. Codex requires one review
of entries shown as new or changed through `/hooks`; untrusted hooks are skipped
until approved. New projects bootstrap on their first context delivery. Use
`memory enable` as a narrower repair/upgrade command, adding `--all` only to
warm every already-discovered scope while exposing per-scope counts globally.

## User workflow

```bash
convos memory
convos memory sync
convos memory status
convos memory current --owned
convos memory review
convos memory remember "Prefer concise factual answers."
convos memory forget "Prefer concise factual answers." --dry-run
convos memory backup
convos doctor
```

Bare `convos memory` is the plain-language current-project health surface. It
reports whether automatic delivery is ready, how many memories are available,
and a concrete next action when needed without exposing ledger, source, hook,
skill, or trust terminology. Human `status` similarly summarizes memories,
remembered items, unavailable origins, and changes needing review. `status
--json` and the normal top-level `convos doctor` retain exact engine and
delivery diagnostics. Default help lists only the human workflow; agent
protocol and troubleshooting commands remain callable but hidden from that
first-run surface.

### User-owned memory

`remember` writes directly to the local ledger without modifying either
provider. It creates a normal project-scoped canonical backed by a `user`
source, so `current`, `context`, provenance, synchronization, and exact history
all treat it like other memory:

```bash
convos memory remember "Prefer concise factual answers."
convos memory remember "Prefer cited evidence." --from MESSAGE_ID
convos memory remember "Prefer concise answers with evidence." --replace "Prefer concise factual answers."
convos memory remember "Revised claim." --replace "Prefer concise answers" --from MESSAGE_ID
printf '%s' "Multiline memory" | convos memory remember -
```

The first command reports a successful project-scoped creation without making
the user handle engine IDs; `--json` returns the canonical and source IDs for
automation. Repeating identical content is idempotent. `--replace` accepts an exact or abbreviated ID,
the displayed first-line title, or unique literal body text; matching is
case-insensitive for text and fails closed when absent or ambiguous. It revises
only a canonical whose sole origin is `user`, so it cannot silently rewrite a
Codex- or Claude-backed canonical. Human `current` output uses that selectable
title as its heading, retains the stable ID beneath it, and names providers
without exposing source IDs or locators; `current --json` carries exact
provenance. Stable IDs remain accepted as `--replace` selectors for automation.
Use `current --owned` to audit only explicitly persisted canonicals. Scoped and
global `status` include `user_owned` counts, and `context --index` marks their
origin as `[user]` while using the first meaningful content line as the label.

Repeat `--from MESSAGE_ID` to attach the exact archived turns that justify this
revision. Prefixes must resolve to one current, content-bearing message. A
message with project cwd metadata must match the memory scope; cwd-less web
turns are allowed because the user selected them explicitly. The ledger stores
the message and conversation IDs, source, title, role, timestamp, and a content
hash, but not a second copy of the conversation text. `current`, `context`, and
canonical `history` show ready-to-run `convos read CONVERSATION --around
MESSAGE` pivots. They verify the stored hash against the live local archive and
mark changed or missing evidence instead of silently treating it as current.
Evidence is bound to the immutable canonical revision hash, so revising a
memory does not rewrite the prior revision's support.

### Evidence health and reverse provenance

```bash
convos memory audit
convos memory audit --message MESSAGE_ID
convos memory audit --all --json
```

The default audit checks current and historical evidence links for the current
project. `--all` widens only to every memory scope, and `--message` resolves one
unique archived-message prefix and returns the memory revisions it supports.
Structured records contain canonical and revision hashes, exact IDs, status,
and read pivots, but no canonical or conversation text.

Human output summarizes `verified`, `changed`, `missing`, and `unavailable`
counts. A clean audit omits the verified rows; a reverse-message lookup prints
its matching rows even when verified. `changed` means the exact current archive
record has a different content hash. `missing` means the archive is reachable
but the exact current message is absent. `unavailable` means archive health
could not be checked and must not be interpreted as deletion. The audit is
read-only: it never rewrites evidence or memory, and unhealthy rows require
review through their exact `read` and canonical `history` pivots.

`forget` is the matching destructive operation. It accepts the same unique
ID, displayed title, or literal-text selector in the current project, supports
a transactionally rolled-back `--dry-run`, and purges the user source plus both
source and canonical revision histories.
The readable preview states how many stored source-plus-canonical revision
records would be purged and tells the user to omit `--dry-run` only after
confirmation; add `--json` for automation.
It refuses provider-backed canonicals and any canonical still present in a
Claude projection. Remove or refresh that owned projection first. Native
provider memories are never changed or deleted.

With encrypted personal sync configured, the next successful worker cycle
publishes a root-signed bodyless tombstone containing the full known revision
ancestry. Other devices purge unchanged remote-only copies. Any authorized
holder can re-encrypt and repair the intact tombstone on a replacement relay
without the author's private key; only the author can sign a successor.
Local divergence, another provider origin, or a managed projection prevents
automatic canonical deletion and remains visible for review. Tombstones govern
current semantic state; they cannot erase plaintext or ciphertext already kept
by another peer, an operator, a filesystem snapshot, or a backup, so historical
retention remains an explicit policy.

`history MEMORY` renders source or canonical identity plus every chronological
revision as readable Markdown. Canonicals accept the same scoped human
selector; low-level source histories still use a source ID.
`history MEMORY --json` preserves the complete structured identity and revision
contract for agents.

Human commands such as `sync`, `status`, and `current` default to the current
Git project and accept an explicit `--project`. Hidden agent protocol commands
such as `plan`, `reconcile`, `context`, and `project` retain the precise
`--scope` spelling for automation and remote identities. `sync` scans providers,
bootstraps an isolated first memory, groups byte-identical same-scope sources,
links exact canonical matches, and reports either `clean` or how many genuinely
semantic choices remain. In the latter case, tell either coding agent **"sync
my memories"**. The installed skill handles the internal plan, dry-run, and
apply sequence:

```bash
convos memory sync --project /path/to/project --json
convos memory apply /tmp/memory-resolution.json --dry-run
convos memory apply /tmp/memory-resolution.json
convos memory context --scope /path/to/project --index
convos memory context --scope /path/to/project
```

`review` is optional human visibility, not another resolution step. It names
the project and friendly provider, then shows only the previously synchronized
text, the new or last-available provider version, and the current memory.
Source IDs, locators, canonical IDs, hashes, and protocol action names are
omitted. A clean project returns one short confirmation. Agents continue to use
the bounded JSON plan for actual resolution.

`status --all` retains aggregate totals and adds an ordered per-scope health
dashboard. Its JSON form includes the same breakdown under `scopes`, so global
backlogs identify the exact projects requiring review instead of exposing only
one unexplained pending count.

`sync --all` performs the same deterministic pass independently across every
discovered scope. Its global JSON contains only per-scope status and counts,
never memory content. Semantic changes remain pending in their original
projects; use scoped agent resolution for those entries.

Every path-valued public `--project`, protocol `--scope`, Codex `applies_to`
path, and Claude session cwd normalizes to the nearest existing Git root. Commands launched from a repository
subdirectory therefore address the same memory scope. A private repository
index then maps equivalent clones and worktrees to the first observed scope
using normalized non-local `origin` evidence, or root commits when no origin is
available. Absolute checkout paths remain local aliases; they are not the
repository identity. Distinct fork origins remain distinct even when their
lineage is shared. Nonexistent paths remain literal, preserving historical and
remote scope identities.

Schema-v1 ledgers migrate in place by indexing every existing, available Git
scope without rewriting source or canonical identity. If that history already
contains multiple scopes for one repository, known checkouts retain their
separation and an unseen clone fails closed instead of choosing or merging.
Preview an explicit selection with:

```bash
convos memory adopt-scope /existing/scope --checkout /new/clone
convos memory adopt-scope /existing/scope --checkout /new/clone --yes
```

This changes only the checkout-to-scope alias. It does not merge, rewrite, or
delete either scope.
An explicit Codex declaration such as `cwd=/repo-a and /repo-b` expands into
one stable source identity per path. Descriptive text is not heuristically
split; only a conjunction followed by another absolute path is multi-scope.

The normal top-level `convos doctor` includes memory ledger, hook-contract,
Codex trust-state, and bundled-versus-installed agent-skill parity checks when
this product is installed. A configured but untrusted or modified Codex hook is
attention, not ready, and points directly to `/hooks`; an unavailable trust
probe reports `unknown` or `unavailable` instead of guessing. It refreshes
read-only provider state, reports the scoped ledger snapshot without modifying
provider memories, and gives a direct repair command for stale or missing
delivery, or the same conversational handoff for pending reconciliation.

Advanced agent protocol, inspection, and explicit Claude projection remain
available even though these commands are hidden from default help:

```bash
convos memory status --all
convos memory status --all --json
convos memory scan
convos memory reconcile --scope /path/to/project --dry-run
convos memory plan --scope /path/to/project --content
convos memory project --scope /path/to/project
convos memory project --scope /path/to/project --write
convos memory project --scope /path/to/project --remove
```

`enable` installs the shared agent skill and idempotent Claude Code and Codex
`SessionStart` hooks once, then initializes the current project or every
discovered project with `--all`. New projects initialize automatically on first
delivery. Codex users review the exact new or changed commands once through `/hooks`.
`disable` removes only those automatic hooks; it deliberately retains revision history,
canonical memory, and the shared conversation-retrieval skill. Add
`--remove-projection` to also remove this project's unchanged owned projection.
Both hook targets are validated before that projection is removed, so malformed
settings cannot turn a failed disable into a partial destructive operation.
Disabling a never-enabled installation is a no-op and does not create settings.
Managed skill and hook files are replaced atomically, preserve existing
permissions, default new files to mode `0600`, and reject symlink or non-file
targets. Both agent-skill destinations are preflighted before either is written,
so a bad second destination cannot leave a partial installation. A shared
Codex/Claude skills directory is accepted only when both declared destinations
resolve to the same regular managed file.
Hook settings are preflighted together and written atomically per file, preserve
existing permissions, default new files to mode `0600`, and reject malformed
structures without rewriting either target. Unrelated settings remain intact
and the resulting JSON stays human-readable.

Claude project keys are not treated as path identities: the adapter recovers
the authoritative cwd from live session events, `sessions-index.json`
`projectPath`, or a generated memory's `originSessionId` joined to Claude's own
history. It falls back to the encoded key only when none of those sources proves
the project. Existing paths normalize to their nearest Git root, so launches
from repository subdirectories converge without conflating separate repositories
or worktrees. Conflicting normalized cwd metadata fails closed. Projection
auto-selects a target when exactly one Claude project has that metadata-derived
scope, even before Claude creates its first native memory; use `--target` to
resolve ambiguity. Explicit targets must have metadata-derived scope matching
the request. `plan --scope` therefore selects the same project across providers.
The scope is part of the plan hash and resolution document; unrelated project
changes do not stale it, and exact-content suggestions never cross scopes.

Projection is reversible and owns only `memory/convos-synced.md`. `--write`
writes atomically with mode `0600`; `--remove` is idempotent when the file is
absent. Both refuse untracked, manually edited, or symlinked paths, so native
Claude memories and user changes are never silently overwritten or removed.
Preview returns concise JSON metadata by default; add `--content` to include the
complete rendered Markdown without writing it.

`plan` records a deterministic hash over every source, link, and canonical
revision plus the protocol version. With `--content`, it is a self-contained
reconciliation bundle:
pending entries include the current or last-seen provider text plus the exact
previously applied `ancestor`, while `canonicals` contains every current
candidate in the requested scope. This gives the agent the three-way evidence
for matching, merging, or superseding without loading unrelated history.
`apply` scans again and rejects the resolution if anything changed while the
agent was reasoning.

Run `apply --dry-run` first. It executes the real transaction and returns the
exact resulting scoped canonicals plus predicted remaining work, then rolls back
canonical and link mutations. Applying the identical document commits only
after another source scan and plan-hash check.

`scan`, `sync`, `status`, `plan`, `current`, `doctor`, and context delivery all refresh
read-only provider state first, so pending and missing counts describe the live
snapshot rather than a previous command's ledger state. Context delivery also
settles the same safe lone-source, byte-identical, and exact-match updates as
`sync`; only genuine semantic choices produce a synchronization notice.
Scoped commands inspect and tombstone only provider identities in that project,
so malformed metadata in an unrelated project cannot block healthy work.
Explicit global `scan` and `status --all` still inspect every project and surface
those provider errors.
`status` uses the current Git project unless `--project` or `--all` is
supplied. Unrecognized provider formats, incompatible ledgers, SQLite failures,
and filesystem errors fail closed with a concise error across every public CLI
and hook path; they do not emit a traceback or mark previously observed sources
missing. Runtime-hook event, cwd, and transcript fields are shape-validated
before provider scanning or ledger access.

`reconcile` bootstraps a lone first source, groups byte-identical unlinked
sources, and links byte-identical sources to an existing canonical in the same
scope. When stronger provider metadata reclassifies a source, an inactive
identity is detached automatically only if an active replacement has the same
provider, locator, and exact content hash; its source and canonical history
remain intact. Its `--dry-run` uses the real apply transaction and rolls back.
Changed, missing, or meaningfully ambiguous memories remain pending for the
agent. Run this deterministic pass before preparing a semantic resolution. If
another session wins the same safe update between plan and apply, reconciliation
replans once and converges; explicit agent resolution documents remain strictly
stale-rejected.

A resolution document is explicit and auditable:

```json
{
  "version": 1,
  "plan": "plan-id-from-memory-plan",
  "scope": "/path/to/project",
  "resolutions": [
    {"source": "source-id", "action": "distinct", "scope": "/project"},
    {"source": "source-id", "action": "same", "canonical": "mem_existing"},
    {
      "sources": ["claude-source", "codex-source"],
      "action": "merge",
      "scope": "/project",
      "content": "Resolved canonical memory"
    },
    {
      "action": "revise",
      "canonical": "mem_existing",
      "hash": "expected-current-hash",
      "content": "Refined canonical memory"
    },
    {"source": "source-id", "action": "unresolved"}
  ]
}
```

Actions are `same`, `merge`, `scoped`, `supersedes`, `distinct`, `revise`,
`unresolved`, and `detach`. `revise` uses a scoped plan plus the expected
canonical hash, so deliberate refinement cannot lose a concurrent update. The
CLI does not guess semantic equivalence. The agent resolves meaning; SQLite
transactions, source hashes, revision history, and canonical lineage remain
deterministic.

Every resolution must copy `version` from its plan. Missing or unsupported
versions fail before provider scanning or ledger mutation, so a stale agent or
external integration cannot accidentally apply a different protocol. Apply
and dry-run results repeat that version. Malformed top-level fields, action
fields, identifiers, scopes, hashes, or content fail at the same pre-scan
boundary with one concise error.

Project scope is an isolation boundary. One resolution cannot combine sources
from different scopes, target a canonical from another scope, or change the
source scope. `scoped` means the resolved content states the conditions under
which each memory is true; it does not move a memory into another project.
Ledger startup also rejects any legacy or externally introduced cross-scope
link before context can be delivered.

Canonical IDs are allocated deterministically by the engine; resolution
documents may reference existing same-scope IDs but cannot invent new ones.
`detach` only acknowledges a source that has disappeared, never an active
source. Claude projection refreshes providers and refuses to preview or write
while that scope has pending reconciliation.

`context` is the Codex/shared-overlay projection: it renders only canonical
memories in the requested scope as agent-ready Markdown. Delivery first refreshes
the read-only provider revisions and commits only deterministic reconciliation.
If scoped semantic changes remain, the output retains the canonicals but marks
them potentially stale and gives the agent-facing `sync --json` command. It
never resolves meaning automatically.
`install-hook` adds idempotent Claude Code and Codex `SessionStart` hooks that
inject this context on startup, resume, clear, and compaction; they never edit
either provider's native memory. Codex requires one-time review of entries shown
as new or changed through `/hooks`, because untrusted hooks remain skipped until trusted.
Hook delivery is full below 16,000 rendered characters. Larger scopes receive a
bounded canonical ID/title index with exact
commands for exact canonical-ID or literal body `--query` retrieval, or the
complete explicit `context`; `current --query` accepts the same selector and
an exact scoped ID takes precedence even if another body mentions it. Explicit
CLI retrieval is never truncated, and a Markdown query miss returns a clear
no-match response instead of blank output. This avoids spending tens of
thousands of characters at every session start without hiding that memory
exists. `context --index` explicitly renders the same bounded index for Codex
or human discovery even when the scope is small; follow it with exact-ID
queries rather than loading every body. On an empty ledger, the first session
normalizes its cwd to the nearest Git root before discovering provider memory,
so launching from a repository subdirectory still bootstraps the right scope.
Use `install-hook --status` to inspect both hooks and Codex's current trust
state, or `--remove` to remove both.
Codex receives the overlay through its documented `SessionStart`
`hookSpecificOutput.additionalContext` surface
([Codex hooks documentation](https://developers.openai.com/codex/hooks)).
The installed skill keeps explicit `context --index` and exact-ID retrieval as
a fallback when the hook is disabled, untrusted, or intentionally omitted.

Claude only loads auto-memory `MEMORY.md` at startup; sibling topic files are
on-demand ([Claude memory documentation](https://code.claude.com/docs/en/memory)).
The runtime path uses the documented
[`SessionStart` additional context](https://code.claude.com/docs/en/hooks#sessionstart)
surface. `project` therefore previews an explicit namespaced artifact rather
than silently claiming startup delivery. `--write` owns only
`memory/convos-synced.md`, writes atomically with mode `0600`, and refuses to
overwrite a foreign or manually edited file. Canonical IDs and revision hashes
are embedded as HTML comments, and the source adapter excludes this projection
to prevent echo loops.

## Storage and adapters

The default ledger is `~/.convos/memory/state.db`. Override it with
`CONVOS_MEMORY_DB`. A new ledger is created with mode `0600` before SQLite
opens it; database-file symlinks and non-file targets are rejected. Existing
unversioned ledgers and schema versions 1 through 4 are adopted as schema
version 5, including
repository-scope indexing for Git checkouts that still exist. Versions newer
than 5 fail before schema SQL runs, preventing accidental downgrade or
reinterpretation.

- Codex adapter: imports task groups from generated
  `$CODEX_HOME/memories/MEMORY.md` (default `~/.codex/memories/MEMORY.md`).
- Claude Code adapter: imports topic files under
  `$CLAUDE_CONFIG_DIR/projects/*/memory/` (default `~/.claude/projects/`);
  `MEMORY.md` indexes are not duplicated.
- Disappeared inputs become inactive. Their prior revisions and canonical
  memories remain available until explicitly detached.
- Remote adapter: exchanges canonical revisions and tombstones through an
  installed encrypted personal remote without importing remote as a hard
  dependency.

For isolated testing or alternate installations, set
`CONVOS_CODEX_MEMORY_ROOT` and `CONVOS_CLAUDE_PROJECTS_ROOT`; these override
the roots derived from `CODEX_HOME` and `CLAUDE_CONFIG_DIR`.
