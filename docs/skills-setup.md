Skills Setup (Codex + Claude Code)
==================================

This repo ships one skill: `convos`.

Install
-------

Run:

```bash
bin/install-skills
```

This installs to:
- Codex: `~/.codex/skills/convos/SKILL.md`
- Claude Code: `~/.claude/skills/convos/SKILL.md`

Optional: install only one
```bash
bin/install-skills codex
bin/install-skills claude
```

Use
---

Tell your agent to use the skill, e.g. "Use convos to sync then search."

Just-in-time ingestion
----------------------

`convos init` installs user-level lifecycle hooks for both coding agents during
normal setup and imports their existing local sessions without contacting web
sources. Inspect, refresh, or remove the hooks independently with:

```bash
convos install-hooks
convos install-hooks --status
```

Install and removal preflight and parse both agent config files before either is
rewritten, so an unsafe or malformed second config cannot leave partial hooks.

Claude Code uses `Stop` plus `SessionEnd`; Codex uses `Stop`. Remove only these
convos handlers with `convos install-hooks --remove`. Codex may require
reviewing the new command hook through `/hooks` after installation. The approval
screen shows the literal `convos capture codex` command; Codex has no separate
handler display-name field, while `Saving conversation to Convos` is shown as
its runtime status. Start a new agent session, complete one turn, then use
`convos doctor` to verify that `ingest: ... last=...` is recent and both skill
copies are current. It compares their complete contents with the bundled skill;
run `convos install-skills` when either copy is missing or stale.

Hook status is exact rather than suffix-based. The installed command must point
to the current `convos` executable, carry the current custom archive root when
configured, and appear exactly once under every required event. Old executable
paths, duplicate handlers, and a Claude handler misplaced between `Stop` and
`SessionEnd` report `convos install-hooks` as the repair.

Capture hooks are installed on turn completion (`Stop`) and Claude `SessionEnd`,
not tool calls. Intermediate events from custom or older hook configurations
can launch a capture at most once per 60 seconds per transcript; completion
always bypasses that limit. Suppressed events remain in the provider transcript
and are imported on the next eligible event or local sync. There is no periodic
timer. Unchanged successful captures skip worker launch and archive access.
`convos read` and `convos sql` also drain pending captures before reading, so
those commands can acquire write locks even when invoked from an agent tool.

Cross-provider memory delivery
------------------------------

When the optional Memory product is present, `convos init` also installs its
second pair of `SessionStart` hooks. Existing archives can initialize or repair
only Memory with:

```bash
convos memory enable
convos memory install-hook --status
```

These inject full small project scopes or a bounded ID/title index into both
agents. Codex skips untrusted user hooks until entries shown as new or changed
are reviewed once through `/hooks`. The installer preflights both agent configuration files before either
atomic write, and `convos memory disable` removes only these memory handlers.
