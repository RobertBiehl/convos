---
name: orchestrate
description: Report this session's work to Convos Orchestrate, the user's agentic IDE. Use when the user types /orchestrate or $orchestrate, or when a session was started from orchestrate. Gives tools to publish work items with statuses, live activity, results (images, PDFs, markdown, links), chat threads, and questions the user answers on the board.
---

# Orchestrate

The user watches and steers your work in the orchestrate IDE instead of your terminal.
You get tools; how you use them is your call. Keep the board truthful: it is what the
user sees of you.

## Join first

Loading this skill means: join now, before any other command or work. Every other command
needs it, and the user sees nothing of you until you do. (A session started from orchestrate is
already joined, and its instructions say so: skip this.)

```sh
convos orchestrate join --title "SHORT GOAL OF THIS SESSION"
```

Joining is idempotent and binds to your provider session automatically (Claude Code, Codex
and Muse expose their session id to your shell). If join says no provider session was
detected, pass `-s SESSION` on every later command. Then put your work on the board.

## Tools

| Command | Purpose |
|---|---|
| `add TITLE [-d MARKDOWN] [--status S] [--why REASON] [--eta WHEN] [-p PARENT] [--again]` | Add an item (a unit of work). Prints its id, e.g. `i3`. A title already under the same parent returns that row and adds nothing; `--again` adds another. |
| `set ITEM [--status S] [--why REASON] [--eta WHEN] [--title T] [-d MARKDOWN]` | Update an item. `-d -` reads stdin. Only real changes are written. |
| `now "TEXT"` | One-line live activity for the session ("Running the test suite"). `now ""` clears. |
| `attach FILE_OR_URL --on ITEM [--caption C] [--replaces aN]` | Append a result: image, video, PDF, markdown, any file, or a link. `--replaces` puts a newer version in place of an older result on the same row. |
| `attach --text MARKDOWN --on ITEM --name NAME.md` | Append an inline markdown result. |
| `attach --open PATH --on ITEM [--caption C]` | A result the user starts with one click: an app (or any file or folder) opens on their Mac. |
| `attach --run "COMMAND" --on ITEM [--caption C]` | A result the user starts with one click: the command runs in a new Terminal window, in your current folder. |
| `say "TEXT" [--on ITEM] [--reply COMMENT]` | Write in the session chat, or in an item's thread. |
| `ask "QUESTION" [-o A -o B] [--multi] [--on ITEM] [--detail MD] [--wait SEC]` | Ask the user. The item shows *needs you* until answered. |
| `need "WHAT THE USER DOES" [--on ITEM] [--detail MD] [--wait SEC]` | Ask the user to do something only they can. The item shows *needs you* until they mark it Done or Can't. |
| `withdraw QUESTION...` | Take back open questions or needs that no longer apply. |
| `inbox [--wait SEC]` | Receive user comments and answers not yet delivered to you. |
| `show` | Your board as the user sees it. Rows are marked `· unseen by the user`, `· archivable`, or `(archived)`. |
| `archive ITEM...` | Move finished top-level rows, with everything under them, off the Current board. |
| `close` | Close the session; the board stays available. |

`add` and `set` are safe to repeat, so a script that builds your board can run again without
duplicating it. Statuses: `planned`, `working`, `blocked`, `done`, `failed`, `skipped`. An open question
on an item makes it show as *needs you*. The session status is derived from its items.

A row that waits on the user to do something (run a test on their machines, sign in, plug in a
device) is not blocked; say what they do: `need "Run the LAN test on the Air and the M2 Pro" --on i4
--detail "HOW-TO-LAN.md in the bundle on your Desktop"`. The row shows *needs you* with that action,
and the user's Done or Can't (with an optional note) reaches you like an answer. A decision is a
question (`ask`). Blocked is for blockers outside the user's hands, and a blocked row must say why in
one line the user reads on the row itself: `set i4 --status blocked --why "Waits for the CI runner"`.
Failed and skipped rows take a `--why` too. A new status drops the old reason.

Give a row an ETA when you start work that takes a while: `--eta 45m`, `2h`, `18:30`, or an ISO
time (`--eta none` clears it). The user sees it on the row, and it turns overdue once it passes;
move it when your estimate changes rather than letting it lapse. A lapsed ETA also sends you an
orchestrate notice on that row: finish the row or give it a new ETA.

Commands end with a **Board check** when something needs tidying: a lapsed ETA, a row left
*working* with no news, a question the user may have settled in a comment, messages that have not
reached you, or finished rows you may archive. Act on it before you go on; it disappears once
the board is in order.

Keep Current about the work at hand: once the work has moved on, archive the rows `show` marks
`· archivable`. Those rows are finished, with no open questions, and the user has looked at
everything in them since it last changed. Anything else is refused with the reason, so never
archive to tidy up work the user has not seen. Anything new on an archived row brings it back,
and only the user restores rows.

All commands accept `--json` for structured output.

## The board is the report

Every `add`, `set`, `attach`, and `now` already reaches the user: each changed row shows as
unread on their board, and the session as unread in their list. So never post status recaps or
"what I did" messages with `say`; put a row's news in the row itself (its status, `set ITEM -d`,
a result). `say` is for conversation: answering the user where they spoke, or something no row
can hold. Keep it to a sentence or two; details go in the row, not the reply.

## The user talks back

The user comments on the session or on a specific item, and answers your questions.
These arrive as messages starting with `[orchestrate]` (headless sessions), as hook
context (interactive sessions), or via `inbox`. Treat them as the user's words about the
named target. Respond however fits: answer in that item's thread with
`say --on ITEM --reply COMMENT`, update the item, add new items, change course, or ask
back. Reply where the user spoke so the conversation stays attached to its item.

When the user asks you something, answer in that thread first, before longer work: they are
waiting for it. An instruction needs no reply; just do it. The board shows the user ✓✓ on
their comment once it is in your context, so there's no need to acknowledge it.

Questions are decisions, so put what they rest on in front of the user: attach the evidence
(screenshots, comparison sheets, notes) before you ask. The question card shows the results
on its item and every result the question names by id, so name the ones that decide it.
When a comment settles an open question, or a newer question replaces one, withdraw it
(`withdraw q3`): a comment arrives with the questions still open on its row. The user can
also dismiss a question; you then receive `q3 dismissed` and should not wait for it.

A comment can carry images (screenshots, mockups). Each arrives as an `image: PATH` line
under the comment; open the file to see it before you act on the comment.

## Subagents

A subagent works on one row. Before delegating, create or pick the item, then give the
subagent this exact prefix for its commands:

```sh
convos orchestrate --scope ITEM --as NAME ...
```

With `--scope`, writes are confined to that item's subtree: new items default under it,
and comments, results, and questions land on it. Subagents share your session, so they
need no join. `inbox` with `--scope` returns only messages for that subtree.

A subagent that runs as its own agent (Codex, another Claude Code session, a collab teammate)
is invisible to the user unless it joins. Put this in its brief, with the orchestrate skill:
`convos orchestrate join --title "ITS GOAL" --parent SESSION:ITEM`. It then shows as a child
session under that row, with its own board.

## Worktrees

Every project keeps its git worktrees in one place: `.claude/worktrees/NAME` in the repo's main checkout (the
first path `git worktree list` prints), where Claude Code puts its own. Make yours there, run from the main
checkout: `git worktree add .claude/worktrees/NAME -b BRANCH`. Never put one beside the repo (in `~/Workspace`)
or in a folder of your own choosing, and leave alone the checkouts other sessions made elsewhere.

## Results

Prefer attaching the real artifact (a screenshot, a screen recording, the generated PDF,
the report file) over describing it. Attach an HTML page as the page itself (`attach proposal.html`):
the IDE renders it in Quick Look, scripts included, so do not also attach screenshots of it. Videos play in the IDE as MP4 (H.264) or WebM. Links to PRs, commits, and dashboards are results too.

When the user should try something themselves (a build to play-test, a tool to run), attach it so it starts
with one click instead of a path they copy into a terminal: `attach --open build/Game.app --on i4 --caption "Play build 9"`
for an app, or `attach --run "make demo" --on i4` for a command. Then name the result in your `need`
(`need "Play-test build 9 (a12)" --on i4`), so its button sits on the request.

A row shows its results on one line, newest first, so keep each row to a handful. A newer version of a
result (a revised design, a re-run measurement) replaces the old one: `attach design-v2.md --on i4 --replaces a7`.
The old one leaves the row, and its id leads to the new one. Work that keeps producing results belongs in
child rows (`add TITLE -p i4`), each with its own.

## References

Point at anything by its id in any text you write (`say`, `add -d`, `set -d`, `ask`,
`--detail`, captions, markdown results): rows `i3`, results `a2`, questions `q1`,
comments `c5`, other sessions `o5619965e7ab`, and rows in other sessions
`o5619965e7ab:i3`. The IDE turns them into links; `[the LAN test](i3)` links your own words
instead. Point at the one or two that matter, not every id you touched. `show` lists every id, including each
row's results. The user references the same way, so read ids in their messages as
pointers.
