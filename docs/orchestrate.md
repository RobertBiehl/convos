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
items (images, videos, PDFs, HTML pages, markdown, files, links), **threads** on the session and on every
item, and **questions** the user answers. People read the board instead of terminals,
comment on any row, and the agent decides how to act on it.

```sh
uv tool install convos --with convos-orchestrate --prerelease allow   # beta: only pre-releases exist
convos orchestrate install     # the /orchestrate and $orchestrate skills
convos orchestrate open        # open the IDE (starts its local server)
```

Any Claude Code or Muse session joins with `/orchestrate`, any Codex session with
`$orchestrate`; the skill tells the agent to join before anything else. Join binds the board to the
agent's own session id (`CLAUDE_CODE_SESSION_ID`, `CODEX_THREAD_ID`, or `MUSE_SESSION_ID`, which
wins when a Muse runs inside a Claude Code shell). A joined Muse session has no message channel
yet: your words wait for its `inbox`, which every command's board check points it to. Sessions started from the IDE (`n`) run headless and are
owned by it. The dialog offers every agent installed on this Mac: Claude Code, Codex and Muse. Each
comes with its current models, the newest of each class, read from the tool itself: the model ids
the installed Claude Code binary knows, Codex's `models_cache.json`, and Muse's model catalog.
Efforts follow the chosen model, and permissions the agent (Claude Code's permission modes, Codex's
sandbox, Muse sandboxed or `--yolo`). The session pane changes a started session's permission in
place: a live Claude Code agent switches at once (it is launched with
`--allow-dangerously-skip-permissions`, so even into `bypassPermissions`), Codex and Muse with their
next turn. An agent launched before that needs a restart on its conversation for `bypassPermissions`;
the pane asks you to confirm it, and the agent then restarts at once if it waits on a permission
prompt (the prompt goes away and the agent is told to go on), otherwise once its turn ends.

The pane also switches a started session's model and effort. Codex and Muse take them with their next
turn; a live Claude Code agent restarts on its conversation for them (`--resume`), at once when idle,
otherwise once its current turn ends. The model a session runs stays selectable after a newer one of
its class replaces it in the list. **Restart** (`⇧R`) starts a live Claude Code agent again on its
conversation the same way, with the Claude Code installed now: the pane shows the version the agent
runs, and once an update installs a newer one, the button reads *Restart into VERSION*. Codex and Muse
start a fresh process every turn, so an update reaches them by itself.

A joined Claude Code or Codex session can **Move into orchestrate**. The pane first shows what that
does: the terminal agent it ends (Claude Code names each running session in its `sessions` folder; the
agent's own orchestrate calls also record its pid), and the model, effort and permission the
conversation continues with (Claude Code: the transcript's model, the session's effort, the terminal's
permission flags; Codex: the thread's last turn context). Once confirmed, the terminal agent gets
SIGTERM and the same conversation resumes headless here, in the folder it was started in; one stopped
mid-turn is told to continue. From then on it is a session started here. A session another program runs
headless, a Codex thread collab manages, or one Codex still holds open is refused with what to do.

## Statuses

Items are `planned`, `working`, `blocked`, `done`, `failed`, or `skipped`. An item with
an open question, or an open need (an action only the user can take), shows **needs you**. A session's status is derived from its items with
this precedence: needs you, working, blocked, failed, planned, done (idle when empty,
closed when closed).

A row's status can carry a one-line reason (`--why`). An agent's blocked row must have one, saying
what it waits for; failed and skipped rows may. A row that waits on the user to do something is not
blocked: the agent puts the action to the user with `need`, which shows the row as needs you. The board row shows the reason in place
of the description's first line, the item view shows it under the status, its history keeps it with
the status change, and a blocked session shows its newest reason in the session list. A new status
drops the old reason. Rows the user blocks need none.

## Agent tools

Agents talk to orchestrate only through the CLI. The skill (`convos orchestrate
install`) documents the tools; how an agent uses them is left to the agent.

| Command | Effect |
|---|---|
| `join --title T` | Register the session (idempotent per provider session). |
| `add`, `set` | Create and update items; `--why REASON` says why a row is blocked, failed, or skipped; `--eta 45m`, `2h`, `18:30`, or an ISO time sets when the agent expects the row done (`none` clears). Both are safe to repeat: re-adding a title under the same parent returns that row (`--again` adds another), and a `set` that changes nothing writes nothing, so it cannot bring an archived row back. |
| `now TEXT` | Live activity line. |
| `attach FILE_OR_URL --on ITEM`, `attach --text MD --on ITEM` | Append a result. `--replaces aN` puts a newer version in place of a result on the same row; the old id then leads to the new one. Past 8 results on a row, `attach` suggests child rows. |
| `attach --open PATH --on ITEM`, `attach --run "COMMAND" --on ITEM` | A result the user starts with one click: an app (or any file or folder) opens with macOS `open`; a command runs in a new Terminal window, in the folder it was attached from, under the user's login shell (a `.command` file, so orchestrate needs no permission to control Terminal). Nothing is copied, and the board starts only results recorded on the session. Named in a `need`, its button sits on the request. |
| `say TEXT [--on ITEM] [--reply C]` | Write in a thread: answers to the user, not status recaps (changed rows already show as unread). A `say` that is not a reply ends with that reminder. |
| `ask Q [-o A -o B] [--multi] [--on ITEM] [--detail MD] [--wait S]` | Ask the user. The card shows the results the question names and the results on its item. |
| `need ACTION [--on ITEM] [--detail MD] [--wait S]` | Ask the user to do something only they can (run a test on their hardware, sign in). The card reads **Your action** and offers Done and Can't with an optional note; the reply reaches the agent like an answer (`Action: ... -> Done`). |
| `withdraw Q...` | Take back open questions or needs that no longer apply. |
| `inbox [--wait S]` | Pull undelivered comments, answers, and notices. |
| `spawn PROMPT --on ITEM [--provider claude-code\|codex\|muse] [--model M] [--permission P]` | Start a child session working for this item. On the same agent, it runs with this session's permission unless `--permission` says otherwise. |
| `archive ITEM...` | Archive finished top-level rows the user has read (see below). |
| `show`, `ls`, `close`, `reopen` | Inspect and close. `show` marks rows `· unseen by the user`, `· archivable`, `(archived)`. |

**Board check.** Every agent command ends with what needs tidying, when anything does: a lapsed ETA,
a row the agent blocked without a reason, a row left *working* with no news for half an hour (and no ETA), an open question on a row the user
commented on after it was asked, user messages that have not reached the agent, and rows finished an
hour ago that the user has read (which the agent may archive). `--json` carries them as `reminders`.

**References.** Any text (comments, descriptions, questions, markdown results) can point at
rows `i3`, results `a2`, questions `q1`, comments `c5`, sessions `o5619965e7ab`, and things in
other sessions (`o5619965e7ab:i3`). Ids that exist render as small pills (the id, or another
session's short name) with what they point at as the tooltip; `[the LAN test](i3)` links the
writer's own words instead. Either selects the row, opens the result, focuses the question, or
highlights the comment. In the IDE composer, `#` opens a
picker that inserts them. `show` lists every id, including each row's results.

**Images in comments.** A comment can carry images: paste, drop, or pick them in the composer. They are stored as archive attachments of
the comment's event, shown in the thread, and open in Quick Look. The agent receives each as an `image: PATH` line with a local
path it can open.

**Subagents** share their parent's session. The parent hands them a prefix,
`convos orchestrate --scope ITEM --as NAME`, and every write is then confined to that
item's subtree. Writes outside the subtree are refused.

**Worktrees live in one place per project.** The skill tells agents to make every git worktree at
`.claude/worktrees/NAME` in the repo's main checkout, where Claude Code puts its own, and never beside
the repo in `~/Workspace`. Git ignores that folder once `**/.claude/worktrees/` is in the global
excludes file (`~/.config/git/ignore`); Claude Code also adds it to a repo's `.git/info/exclude`
when it first makes one there.

**Sessions are hierarchical.** A session records its parent `SESSION[:ITEM]`. The IDE
nests children under their parent and shows them on the parent's row. Agents make children
with `spawn --on ITEM`; a session the user starts stands alone unless its dialog's *Part of*
field names the current session or its selected row. When a child
reaches *needs you*, *done*, *failed*, or *closed*, the parent receives a notice in that
item's thread, so one session can orchestrate everything below it.

## Delivery to sessions

The user's comments and answers, and child notices, reach the session through the best
available channel. Whatever cannot be pushed stays pending for `inbox`.

| Session | Channel |
|---|---|
| Started in the IDE, Claude Code | `stream-json` on the process's stdin; resumed with `--resume` after a restart |
| Started in the IDE, Codex or Muse | one process per turn (`codex exec`, then `codex exec resume THREAD`; `muse exec --session-id`), the prompt from a file. Words that arrive during a turn wait and start the next one, so the session shows *ready* between turns. A Codex agent's sandbox may write to the Convos root, where its board lives |
| Joined Claude Code | the session's inbox socket (`CLAUDE_CODE_MESSAGING_SOCKET`), recorded by the agent's own CLI calls |
| Joined Codex | `codex queue --thread ID --message TEXT`; the thread takes it at its next turn, and the comment shows *queued in Codex* until then. Orchestrate also gets the thread that turn: a thread collab runs as a managed worker gets a collab wake (one turn with collab's own model and sandbox), and an interactive thread (started by the Codex TUI) is resumed by Codex's local daemon (`codex queue --resume`, after `codex app-server daemon start`). A thread another program drives with `codex exec` is not resumed, because the daemon keeps a resumed thread's write lock and would lock that program out; its messages wait for its next turn |

**Received.** Your comment shows *✓ sent* once its channel has it, and *✓✓ received* once the agent
has it in its context: the session's own provider transcript shows it (a message Claude Code queues
mid-turn counts only when the agent takes it, a queued Codex message when a turn takes it). Agents
spend nothing on this, so they reply to questions and just act on instructions. The transcript is
read incrementally, and words count only under their own session's header.

**Not taking messages.** Claude Code logs each message it reads from its stdin entering its queue
within a second, even in the middle of a long tool call. When a live agent started here has logged
neither that nor the words two minutes after orchestrate wrote them, the session shows *not taking
your messages* (in the list, on the model chip, and on the comments it never took), with **Restart
and resend**: the agent starts again on its conversation at once and gets those words again. This
caught Claude Code 2.1.327 agents that, after a skill reload, read their input and dropped it.

Each agent's stdin has its own writer thread, every session has its own lock, and the server's
delivery loop runs each session's delivery, and the pickup of agents, in the background, one run
at a time each. An agent that stops reading, or a long wait for the archive, therefore holds up only
its own session. The server log records each delivery (`gave c12 via stdin`), why words wait, agents
not taking words, and every thread's stack when a background run takes over two minutes;
`kill -USR1 PID` prints the stacks on demand.

A headless agent outlives the server that started it: its stdin is a named pipe in the session's
folder, which the agent itself also holds open, and its output goes to a file there. A restarted
server picks it up again, in or between turns; a Codex or Muse turn is followed to its end the same way. A session never runs two agents: while any agent
of a session is alive (found by the session id in its command line), orchestrate starts no other,
and words for an agent it cannot reach wait until that agent ends; `x` stops it.

**Usage limits.** When the provider's usage limit stops a session's turn, the IDE shows it on the
session (in the list, the header, and the session panel, with the provider's own notice) until the
session replies again. Orchestrate reads this from the provider's transcript of the session (Claude
Code's `projects/*/SESSION.jsonl`, Codex's `rollout-*-THREAD.jsonl`) and stores nothing. Your
comments and answers to a stopped session wait and go out a minute after the limit resets, so they
reach an agent that can act on them; a notice without a reset time (out of credits) holds nothing.
A Codex session's panel also offers **Use a reset** while you have Codex usage-limit resets (Codex
0.170 or later): after you confirm, orchestrate redeems the one that expires first through Codex's
app server, and what every Codex session held for the limit goes out at once, since a reset covers
the account, and each Codex session started here whose turn the limit stopped continues. When Codex
says usage is allowed again (a reset used elsewhere), held words go out too and the panel offers
**Continue**; the time is kept in `resets.json` beside the sessions. Claude Code and Muse offer no
reset orchestrate can redeem or read, so when you reset or extend that usage yourself, **Usage is
back · continue** on a held session tells orchestrate: held words go out to every session of that
provider, and each one started here whose turn the limit stopped continues. An agent that hits the
limit again shows it again.

**Questions.** Several open questions read as one stack under **Needs you**: the one in focus
(else the oldest) is open, the rest are one line each, and `a` steps through them. A card shows
the pictures and files the question rests on (the results it names by id, then the rest of its
item's results); `Space` opens them in Quick Look. When you comment on the question's item after it
was asked, the card says so: the comment may have settled it. The agent receives that comment with
the questions still open there and withdraws the ones it settled; you can also dismiss one (`⌫`
or `×`), and the agent is told it was dismissed unanswered.

Headless permission prompts arrive on the board as questions (*Allow*, *Allow always*,
*Deny*) through a small MCP tool passed as `--permission-prompt-tool`. A prompt can wait six hours
for your answer; the tool is configured with that timeout, since Claude Code otherwise gives up on it
after 30 minutes.

Claude Code still asks for some calls in a session set to *bypassPermissions*. Orchestrate allows
those itself when they stay inside one git worktree of the session's repo, as `git worktree list`
names them, the main checkout included. An `Edit` or `Write` qualifies when its file lies there. A
`Bash` command qualifies when it starts by `cd`-ing into the worktree and names no other place on
disk, no `~/` or `$HOME`, and no `..`. Either way, it must name no `.git`, `hooks` or `.claude` path
inside the worktree. Everything else still comes to you. Each call allowed this way is logged in `allowed.jsonl` in the session's folder.
The rule applies to agents started after the change.

## Storage

Each orchestrate session is one archive conversation (`source='orchestrate'`). Every
change is one versioned event message written through the core writer, so the board is
searchable with `convos search`, survives reboots, and stays available after a session
closes. Attachments are content-addressed archive attachments. Projections under
`~/.convos/orchestrate/sessions/*/view.json` are disposable caches rebuilt from the
archive when missing or dirty. Provider socket endpoints are local working state, never
archive data.
`~/.convos/orchestrate/heard.json` records how far each provider transcript has been read for
the user's words (✓✓ received). After a restart the server resumes from there, as long as the file
is still the same one, instead of reading transcripts of a gigabyte or more from the start.

## The IDE

Keyboard first; every shortcut is shown on screen as a key chip. The board lists items
oldest to newest under **Current**. Rows stay there until you archive them: `e` or the archive
button on a row moves it, with everything under it, to the scrollback above (a row with open work
asks for a second `e`), and **Archive N finished** (`Shift+E`) clears every finished row at once.
Anything new on an archived row (a status change, comment, result, question) brings it back.
Agents archive too, but only a finished tree (done or skipped, no open questions) whose rows
you have all read since they last changed; such rows show *archived by* the agent, and only you
bring rows back.

**ETAs.** A row with an ETA shows it in its tail and in the item pane while it is planned, working, or
blocked; once it passes, the row shows how far past it is, in red. The agent is also sent one notice
on that row, like your comments: a joined session gets it through its channel, and a headless one
is started for it. Each ETA is announced once; ETAs that lapsed over an hour before the server
noticed are left to the Board check.

**Convos on this computer.** The chip at the bottom right, next to *live*, says whether the archive
holds what the agents wrote: *Archive current*, *N files behind*, or *Syncing...*, with the relay's last
pass and the disk once it is over 90% full. `i` opens the sheet behind it. Each local session file
counts as current when a hook captured that exact size and time or a sync read it after its last
change; the rest are listed newest first as being written now, changed after their last capture, or
never captured (Claude Code subagent transcripts wait for a sync, since hooks capture a session's
main file). The sheet also lists files that fail to import, the last sync and hook batch, the
newest conversation each web sync captured, the relay watcher, and disk use with the migration
backups. It reads core's state files and stats the session files, never the archive or the
network, so it refreshes every few seconds while open. Its one action is core's own `convos sync`
in the background: `s` for local sessions, `Shift+S` for everything (web too, which reads browser
cookies); its output goes to `~/.convos/orchestrate/sync.log`.

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
The pane scrolls as one: the session opens at its newest chat, a row at its header. Chat is
conversation, not record (what lasts belongs in rows and results), so a thread shows its newest
10 messages; **Show earlier** pages back, and following a link to an older comment reveals it.

A row shows its results on one line, newest first: as many as fit, the selected one always among
them, and `+N` for the rest. The right pane and Quick Look list them in the same order.

| Keys | Action |
|---|---|
| `1`-`9`, `[` `]` | Switch session |
| `j` `k`, `↑` `↓` | Select a row (from the session: the most relevant one) |
| `⌘↑` / `⌘↓` | First current row (press again for the archive) / last row, following new rows |
| `Esc` | Leave the row: back to the session, following new rows |
| `←` `→` | Select a result in the row (the newest by default; each row remembers its pick) |
| `Space` | Quick Look: preview the result or row; `←` `→` results, `↑` `↓` rows |
| `+` `−` `0` (in Quick Look) | Zoom an image (also pinch, ⌘-scroll), fit it again; a click toggles fit and 100%; drag, scroll, or `Shift+arrows` pan; the minimap shows and moves the visible part. Zoomed views always draw from the full-resolution original |
| `p` `m` `⇧←` `⇧→` (in Quick Look) | Video: play/pause, mute, seek 5 s (videos start playing when they open) |
| `c` (in Quick Look) | Copy a text result whole: markdown, or text under a type browsers only download (JSON, a patch, a script), which the board shows in place, JSON indented. `⌘C` copies a selection, or everything when nothing is selected; the result's tile has a Copy button too |
| `↵` / `o` | Open the selected result |
| `c` | Comment on the selected row, or on the session when none is selected |
| `⌘↵` / `Ctrl+↵` (while writing) | Send the comment; the composer lets go of focus so the board keys work again. `↵`, `⌥↵`, `⇧↵` start a new line |
| `#` (while writing) | Reference a row, result, question, or session |
| `⌘V` (while writing) | Attach the copied image to the comment (also drop images on the pane, or the paperclip); `⌫` in an empty composer removes the last one |
| `Shift+C` | Comment on the whole session |
| `a`, then `1`-`9` / `↵` | Answer a question; `a` again goes to the next one |
| `Space`, `⌫` (on a question) | Look at the results it rests on; dismiss it unanswered |
| `s` | Set the row's status |
| `n` | New session (*Part of* in the dialog nests it) |
| `r` `x` `Shift+X` | Resume, stop, close / reopen |
| `e`, `Shift+E` | Archive the row with everything under it, or bring it back (open work asks for a second `e`); archive every finished row |
| `a` | Go to the question that needs you (also the needs-you badges) |
| `i` | Convos on this computer; in it `s` / `Shift+S` sync local sessions / everything, `r` check again |
| `⌘K`, `?` | Command palette, all shortcuts |

The server binds `127.0.0.1` only. API calls need the per-server token (set as a
`SameSite=Strict` cookie by the launch URL), a loopback `Host`, and, for writes, an
`X-Orchestrate` header. Attachments are served sandboxed. An HTML result renders in Quick Look
with its scripts (tooltips, sortable tables) in an opaque origin, so it cannot read the token,
call the API, or load other results.
