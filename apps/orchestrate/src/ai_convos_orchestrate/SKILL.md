---
name: orchestrate
description: Report this session's work to Convos Orchestrate, the user's agentic IDE. Use when the user types /orchestrate or $orchestrate, or when a session was started from orchestrate. Gives tools to publish work items with statuses, live activity, results (images, PDFs, markdown, links), chat threads, and questions the user answers on the board.
---

# Orchestrate

The user watches and steers your work in the orchestrate IDE instead of your terminal.
You get tools; how you use them is your call. Keep the board truthful: it is what the
user sees of you.

## Join

```sh
convos orchestrate join --title "SHORT GOAL OF THIS SESSION"
```

Joining is idempotent and binds to your provider session automatically (Claude Code and
Codex expose their session id to your shell). If join says no provider session was
detected, pass `-s SESSION` on every later command.

## Tools

| Command | Purpose |
|---|---|
| `add TITLE [-d MARKDOWN] [--status S] [--eta WHEN] [-p PARENT] [--again]` | Add an item (a unit of work). Prints its id, e.g. `i3`. A title already under the same parent returns that row and adds nothing; `--again` adds another. |
| `set ITEM [--status S] [--eta WHEN] [--title T] [-d MARKDOWN]` | Update an item. `-d -` reads stdin. Only real changes are written. |
| `now "TEXT"` | One-line live activity for the session ("Running the test suite"). `now ""` clears. |
| `attach FILE_OR_URL --on ITEM [--caption C]` | Append a result: image, video, PDF, markdown, any file, or a link. |
| `attach --text MARKDOWN --on ITEM --name NAME.md` | Append an inline markdown result. |
| `say "TEXT" [--on ITEM] [--reply COMMENT]` | Write in the session chat, or in an item's thread. |
| `ask "QUESTION" [-o A -o B] [--multi] [--on ITEM] [--detail MD] [--wait SEC]` | Ask the user. The item shows *needs you* until answered. |
| `withdraw QUESTION...` | Take back open questions that no longer need an answer. |
| `inbox [--wait SEC]` | Receive user comments and answers not yet delivered to you. |
| `show` | Your board as the user sees it. Rows are marked `· unseen by the user`, `· archivable`, or `(archived)`. |
| `archive ITEM...` | Move finished top-level rows, with everything under them, off the Current board. |
| `close` | Close the session; the board stays available. |

`add` and `set` are safe to repeat, so a script that builds your board can run again without
duplicating it. Statuses: `planned`, `working`, `blocked`, `done`, `failed`, `skipped`. An open question
on an item makes it show as *needs you*. The session status is derived from its items.

Give a row an ETA when you start work that takes a while: `--eta 45m`, `2h`, `18:30`, or an ISO
time (`--eta none` clears it). The user sees it on the row, and it turns overdue once it passes;
move it when your estimate changes rather than letting it lapse.

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

## Results

Prefer attaching the real artifact (a screenshot, a screen recording, the generated PDF,
the report file) over describing it. Attach an HTML page as the page itself (`attach proposal.html`):
the IDE renders it in Quick Look, scripts included, so do not also attach screenshots of it. Videos play in the IDE as MP4 (H.264) or WebM. Links to PRs, commits, and dashboards are results too.

## References

Point at anything by its id in any text you write (`say`, `add -d`, `set -d`, `ask`,
`--detail`, captions, markdown results): rows `i3`, results `a2`, questions `q1`,
comments `c5`, other sessions `o5619965e7ab`, and rows in other sessions
`o5619965e7ab:i3`. The IDE turns them into links. `show` lists every id, including each
row's results. The user references the same way, so read ids in their messages as
pointers.
