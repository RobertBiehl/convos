---
summary: "Orchestrate: a keyboard-first agentic IDE where every agent session reports items, results, and questions, and people steer them."
read_when:
  - Starting, steering, or reviewing agent sessions from the orchestrate IDE
  - Giving an agent the orchestrate tools
  - Changing how sessions receive the user's comments
---

# Orchestrate

Orchestrate is a durable home for agent sessions. Each session publishes a board:
**items** (units of work) with statuses, a live activity line, **results** attached to
items (images, videos, PDFs, markdown, files, links), **threads** on the session and on every
item, and **questions** the user answers. People read the board instead of terminals,
comment on any row, and the agent decides how to act on it.

```sh
uv tool install convos --with convos-orchestrate --prerelease allow   # beta: only pre-releases exist
convos orchestrate install     # the /orchestrate and $orchestrate skills
convos orchestrate open        # open the IDE (starts its local server)
```

Any Claude Code session joins with `/orchestrate`, any Codex session with
`$orchestrate`. Sessions started from the IDE run headless and are owned by it.

## Statuses

Items are `planned`, `working`, `blocked`, `done`, `failed`, or `skipped`. An item with
an open question shows **needs you**. A session's status is derived from its items with
this precedence: needs you, working, blocked, failed, planned, done (idle when empty,
closed when closed).

## Agent tools

Agents talk to orchestrate only through the CLI. The skill (`convos orchestrate
install`) documents the tools; how an agent uses them is left to the agent.

| Command | Effect |
|---|---|
| `join --title T` | Register the session (idempotent per provider session). |
| `add`, `set` | Create and update items. Both are safe to repeat: re-adding a title under the same parent returns that row (`--again` adds another), and a `set` that changes nothing writes nothing, so it cannot bring an archived row back. |
| `now TEXT` | Live activity line. |
| `attach FILE_OR_URL --on ITEM`, `attach --text MD --on ITEM` | Append a result. |
| `say TEXT [--on ITEM] [--reply C]` | Write in a thread. |
| `ask Q [-o A -o B] [--multi] [--on ITEM] [--wait S]` | Ask the user. |
| `inbox [--wait S]` | Pull undelivered comments, answers, and notices. |
| `spawn PROMPT --on ITEM` | Start a child session working for this item. |
| `archive ITEM...` | Archive finished top-level rows the user has read (see below). |
| `show`, `ls`, `close`, `reopen` | Inspect and close. `show` marks rows `· unseen by the user`, `· archivable`, `(archived)`. |

**References.** Any text (comments, descriptions, questions, markdown results) can point at
rows `i3`, results `a2`, questions `q1`, comments `c5`, sessions `o5619965e7ab`, and things in
other sessions (`o5619965e7ab:i3`). Ids that exist render as links that select the row, open
the result, focus the question, or highlight the comment. In the IDE composer, `#` opens a
picker that inserts them. `show` lists every id, including each row's results.

**Images in comments.** A comment can carry images: paste, drop, or pick them in the composer. They are stored as archive attachments of
the comment's event, shown in the thread, and open in Quick Look. The agent receives each as an `image: PATH` line with a local
path it can open.

**Subagents** share their parent's session. The parent hands them a prefix,
`convos orchestrate --scope ITEM --as NAME`, and every write is then confined to that
item's subtree. Writes outside the subtree are refused.

**Sessions are hierarchical.** A session records its parent `SESSION[:ITEM]`. The IDE
nests children under their parent and shows them on the parent's row. When a child
reaches *needs you*, *done*, *failed*, or *closed*, the parent receives a notice in that
item's thread, so one session can orchestrate everything below it.

## Delivery to sessions

The user's comments and answers, and child notices, reach the session through the best
available channel. Whatever cannot be pushed stays pending for `inbox`.

| Session | Channel |
|---|---|
| Started in the IDE (headless Claude Code) | `stream-json` on the process's stdin; resumed with `--resume` after a restart |
| Joined Claude Code | the session's inbox socket (`CLAUDE_CODE_MESSAGING_SOCKET`), recorded by the agent's own CLI calls |
| Joined Codex | `codex queue --thread ID --message TEXT` |

Headless permission prompts arrive on the board as questions (*Allow*, *Allow always*,
*Deny*) through a small MCP tool passed as `--permission-prompt-tool`.

## Storage

Each orchestrate session is one archive conversation (`source='orchestrate'`). Every
change is one versioned event message written through the core writer, so the board is
searchable with `convos search`, survives reboots, and stays available after a session
closes. Attachments are content-addressed archive attachments. Projections under
`~/.convos/orchestrate/sessions/*/view.json` are disposable caches rebuilt from the
archive when missing or dirty. Provider socket endpoints are local working state, never
archive data.

## The IDE

Keyboard first; every shortcut is shown on screen as a key chip. The board lists items
oldest to newest under **Current**. Rows stay there until you archive them: `e` or the archive
button on a row moves it, with everything under it, to the scrollback above (a row with open work
asks for a second `e`), and **Archive N finished** (`Shift+E`) clears every finished row at once.
Anything new on an archived row (a status change, comment, result, question) brings it back.
Agents archive too, but only a finished tree (done or skipped, no open questions) whose rows
you have all read since they last changed; such rows show *archived by* the agent, and only you
bring rows back.

**Read state.** A row that changed since you last looked (its status, title, description, a
result, a question, or an agent's comment) has a bright title and an accent dot; read rows are
dimmed. A row counts as read once it stays selected for a second while the page is visible, or
as soon as you preview it in Quick Look, open one of its results, comment on it, or answer its
question. Rows that existed before read tracking began count as read. Read state is local
working state (`sessions/<id>/seen.json`), not archive data.

**Following.** The board stays pinned to the newest rows as they arrive. Scroll up and it stays
put; a **Latest** button (with the count of new rows) brings you back, as do `⌘↓` and `Esc`.
Selection never moves the board. With no row selected, the right pane is the session: its
info, results, and chat, plus **Works with**: the collab rooms the session belongs to and
its teammates, each linking to their board when they are on orchestrate. Selecting a row switches it to that item and its thread.

| Keys | Action |
|---|---|
| `1`-`9`, `[` `]` | Switch session |
| `j` `k`, `↑` `↓` | Select a row (from the session: the most relevant one) |
| `⌘↑` / `⌘↓` | First current row (press again for the archive) / last row, following new rows |
| `Esc` | Leave the row: back to the session, following new rows |
| `←` `→` | Select a result in the row (the first by default; each row remembers its pick) |
| `Space` | Quick Look: preview the result or row; `←` `→` results, `↑` `↓` rows |
| `+` `−` `0` (in Quick Look) | Zoom an image (also pinch, ⌘-scroll), fit it again; a click toggles fit and 100%; drag, scroll, or `Shift+arrows` pan; the minimap shows and moves the visible part. Zoomed views always draw from the full-resolution original |
| `p` `m` `⇧←` `⇧→` (in Quick Look) | Video: play/pause, mute, seek 5 s (videos start playing when they open) |
| `↵` / `o` | Open the selected result |
| `c` | Comment on the selected row, or on the session when none is selected |
| `⌘↵` / `Ctrl+↵` (while writing) | Send the comment; the composer lets go of focus so the board keys work again. `↵`, `⌥↵`, `⇧↵` start a new line |
| `#` (while writing) | Reference a row, result, question, or session |
| `⌘V` (while writing) | Attach the copied image to the comment (also drop images on the pane, or the paperclip); `⌫` in an empty composer removes the last one |
| `Shift+C` | Comment on the whole session |
| `a`, then `1`-`9` / `↵` | Answer a question |
| `s` | Set the row's status |
| `n`, `Shift+N` | New session, new child session |
| `r` `x` `Shift+X` | Resume, stop, close / reopen |
| `e`, `Shift+E` | Archive the row with everything under it, or bring it back (open work asks for a second `e`); archive every finished row |
| `a` | Go to the question that needs you (also the needs-you badges) |
| `⌘K`, `?` | Command palette, all shortcuts |

The server binds `127.0.0.1` only. API calls need the per-server token (set as a
`SameSite=Strict` cookie by the launch URL), a loopback `Host`, and, for writes, an
`X-Orchestrate` header. Attachments are served sandboxed.
