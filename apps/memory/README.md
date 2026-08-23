# AI Convos Memory

`convos-memory` tracks exact revisions of local Codex, Claude Code, and
user-owned memories and synchronizes them into one canonical local overlay.
Provider files are read-only inputs. An active coding agent resolves semantic
collisions; the CLI owns hashes, history, transactions, and stale-plan rejection.

Install it alongside `convos`, then initialize once:

```bash
uv tool install convos --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
convos init
```

The core `memory` extra is only installation metadata: `convos-memory`
remains an independently versioned user-installable product. For an unreleased
source snapshot, pair both packages from the same Git revision:

```bash
uv tool install --reinstall "git+https://github.com/RobertBiehl/convos.git" \
  --with "convos-memory @ git+https://github.com/RobertBiehl/convos.git#subdirectory=apps/memory"
```

`init` creates the archive, installs the user-level skill and core capture
hooks, imports existing local agent sessions without touching web sources,
invokes Memory's safe local initializer, and warms the current project. It is
idempotent and can be rerun after adding the extra. Existing archives may use
`convos memory enable` as the narrower repair/upgrade command. Review Codex
entries shown as new or changed once through `/hooks`; Claude Code needs no
matching approval step. New projects bootstrap automatically on their first
context delivery, so there is no per-project enable ritual. Use `convos memory
enable --all` to warm every already-discovered project during setup.

The 0.9 release requires `convos>=0.9,<0.10`; installers reject older or
newer incompatible core releases instead of creating a broken mixed-version
CLI.

If `convos-remote` is installed and configured too, its ordinary personal
workspace sync discovers this product automatically and carries canonical
memory revisions between devices as root-signed semantic objects inside the
existing encrypted row replicas. No memory-specific remote setup or sync
command is added. Absolute
checkout paths stay local, sequential revisions settle automatically, and
concurrent meaning changes remain pending for the normal three-way agent
resolution instead of using last-write-wins. Remote deletion deactivates the
remote origin when local or provider state must be preserved, but automatically
purges unchanged remote-only canonicals. Its bodyless proof carries complete
revision ancestry, so any authorized holder can repair it without the author's
key while only the author can create a successor. Older ciphertext already
retained by peers or backups is not erased. The relay still observes envelope
metadata such as uploader, timing, epoch, and ciphertext size. Each canonical
is one replica subject to the relay's 48 MiB row-replica ceiling.

Routine use is deliberately small:

```bash
convos memory
convos memory sync
convos memory status
convos memory audit
convos memory current --owned
convos memory review
convos memory remember "Prefer concise answers."
convos memory remember "Prefer cited evidence." --from MESSAGE_ID
convos memory remember "Prefer concise factual answers." --replace "Prefer concise answers."
convos memory forget "Prefer concise factual answers." --dry-run
convos memory backup
convos doctor
```

Bare `convos memory` reports whether automatic memory is ready for the current
project and gives the exact next action when it is not. Its normal output talks
only about memories, projects, delivery, and decisions; `convos doctor` and
`--json` retain ledger, source, hash, hook, and trust diagnostics. Default help
shows only the human-facing workflow. Agent protocol and troubleshooting
commands such as `plan`, `apply`, `context`, `project`, and `install-hook`
remain callable and documented without making users learn them.

`remember` creates a project-scoped `user` source with exact source and
canonical revision history. Use `--replace TITLE_OR_TEXT` to revise the one
matching user-owned memory; IDs remain accepted for automation, and missing or
ambiguous literal selectors fail closed.
Its default confirmation reports the project and result without exposing
engine IDs; add `--json` for exact identity and automation.
Repeat `--from MESSAGE_ID` to bind exact archived turns to that one canonical
revision. The ledger keeps local IDs and content hashes, not duplicate message
text. `current`, `context`, and canonical `history` report live verification
state and exact `convos read --around` pivots. Project-tagged turns must match
the memory scope; an explicitly selected cwd-less web turn is allowed. Evidence
is included in private ledger backups but never in personal remote records.
`current` is human-readable by default, showing selectable titles, content, and
plain provider names plus unavailable or changed state. Exact source IDs,
locators, hashes, and activity remain in `--json`. `current --owned` lists only
memories explicitly persisted this way, while human `status` summarizes
memories, remembered items, unavailable origins, and changes needing review.
Its JSON retains complete counts, and the bounded index labels plain text with
its first meaningful line.
`audit` checks current and historical archive evidence without printing memory
or message content. It reports verified, changed, missing, and unavailable
counts for the current scope; `--all` checks every scope, while `--message ID`
maps one exact archived turn back to the revisions it supports.
Human `current` output presents each memory under its selectable first-line
title and retains the stable ID below it. `history` and `forget` accept that
title, unique literal body text, or an ID. `forget` previews with `--dry-run`,
then purges only a `remember`-created
canonical that has no Codex or Claude origin and is absent from projections.
The readable preview states the exact revision count and confirmation command;
`--json` exposes the same mutation record. It never deletes or edits provider
memory. Personal remote sync propagates the tombstone and safely purges
unchanged remote-only copies; historical backup files remain subject to the
operator's retention policy. `history` similarly renders identity and
chronological revision content by default, with structured output behind
`--json`.

`disable --remove-projection` validates both agent hook configurations before
removing the unchanged owned Claude projection, so malformed settings fail
without partially performing the destructive request.

`backup` uses SQLite's online backup operation to write a transactionally
consistent mode-`0600` snapshot containing every scope, source observation,
canonical, exact revision, local evidence link, projection ownership, and
remote reassembly record. The default destination is a unique file under the ledger's `backups/`
directory; an
explicit destination must not already exist or be a symlink.
`restore SNAPSHOT` validates integrity, schema, trigger absence, and scope-link
invariants, then reports counts without changing anything. Add `--yes` only
after reviewing that preview. A confirmed restore first creates a fresh rescue
snapshot of the current ledger and then uses SQLite's backup transaction to
replace it. Native provider files, hook settings, and installed skills are not
part of a ledger snapshot. Supported older snapshots are upgraded in an
in-memory copy during validation; their source files are never rewritten.

Exact or isolated changes settle mechanically. When meaning genuinely needs a
decision, tell Codex or Claude **"sync my memories"**; the installed skill
handles the versioned plan, dry-run, and transactional apply. `convos doctor`
also verifies the ledger, both hook contracts, Codex trust state, and both
installed skill copies. `convos memory install-hook --status` exposes the same
Codex state directly instead of equating a configured hook with a running one.
`review` gives humans readable change evidence without requiring them to edit a
resolution document: friendly provider names, previously synchronized text,
the new or last-available version, and the current memory. Engine IDs, locators,
hashes, and protocol action names stay out of this human view.
`status --all` groups global health by project in both human and JSON output.
`sync --all` settles every mechanically safe project update in one pass while
leaving semantic decisions isolated and reporting only per-project counts.
Context delivery settles safe first-source and exact-match updates automatically,
so agents interrupt only for semantic choices. `current` applies that same safe
pass before exposing ordered source IDs, locators, and freshly observed versus
applied hashes for direct provenance and self-describing history lookup. Every
scoped command defaults to the surrounding Git project; semantic plans never
silently widen to unrelated projects, and broken metadata elsewhere cannot
block the healthy current project. Claude scope recovery uses live events,
session indexes, and generated-memory origin IDs before retaining an opaque
project key. Both agents receive full small scopes and a bounded ID/title index
for large ones at session start, resume, clear, and compaction. Exact IDs from
that index select one canonical through `context --query`, take precedence over
body matches, and produce an explicit no-match response when absent. Either
agent or a human can request the same cheap discovery surface explicitly with
`context --index`; unfiltered scoped context remains complete.

Repository scope follows repository evidence rather than one device path.
Equivalent clones and worktrees with normalized matching non-local origins
reuse the first observed scope automatically; shared root commits are the
fallback when no origin exists. Distinct fork origins stay separate. A restored
snapshot carries this private mapping to another machine. Ambiguous legacy
clone splits fail closed and can be bound explicitly, without merging content,
through preview-first `memory adopt-scope`.

See the [complete synchronization, safety, projection, and storage
guide](https://github.com/RobertBiehl/convos/blob/master/docs/memory.md).
