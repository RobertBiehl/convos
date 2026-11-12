"""What the provider's own transcript of a session says: whether its usage limit stopped it, and which of the user's
words have reached it.

Claude Code keeps projects/*/SESSION.jsonl under its config dir; Codex keeps sessions/YYYY/MM/DD/rollout-*-THREAD.jsonl.
For limits the latest turn decides: a session stays interrupted until it replies again. Nothing is stored; a transcript
is read again only when it changes.
"""
import datetime as dt, json, os, re, threading, time, zoneinfo
from pathlib import Path

MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
# "resets 12pm (Europe/Berlin)", "resets 12:50am (Europe/Rome)", "resets Oct 15 at 1pm (Europe/Rome)"
CLAUDE = re.compile(r"resets (?:([A-Z][a-z]{2}) (\d{1,2}),? (?:at )?)?(\d{1,2})(?::(\d{2}))?\s*([ap]m)(?: \(([^)]+)\))?")
# "try again at 2:59 AM.", "try again at Sep 7th, 2026 7:41 AM." (this machine's local time)
CODEX = re.compile(r"try again at (?:([A-Z][a-z]{2}) (\d{1,2})(?:st|nd|rd|th)?,? (\d{4}),? )?(\d{1,2}):(\d{2})\s*([AP]M)")
GRACE = 60  # the provider shows its reset to the minute; held words go out a minute after it
FOUND, STATE, SEEN, LOCK = {}, {}, {}, threading.Lock()
# how the user's words read wherever they reach an agent (a pushed message, inbox output, hook context): a line of the
# message starting "- c5 user comment". A literal start keeps the scan at gigabytes per second.
WORDS = re.compile(rb'- ([cq]\d+) (?:user comment|answered on|dismissed by the user|orchestrate notice)')
QUEUED = b'{"type":"queue-operation"'  # Claude Code logs a message it queues mid-turn before the agent sees it

def wall(tz, y, m, d, h, mi): return dt.datetime(y, m, d, h, mi, tzinfo=tz) if tz else dt.datetime(y, m, d, h, mi).astimezone()

def first_after(at, tz, month, day, year, hour, minute):
    """The first time matching the notice's wall clock after the limit was hit; None when it names no real time."""
    local = at.astimezone(tz) if tz else at.astimezone()
    try:
        if month:
            when = wall(tz, int(year or local.year), MONTHS.index(month)+1, int(day), hour, minute)
            return when if year or when > at else wall(tz, local.year+1, MONTHS.index(month)+1, int(day), hour, minute)
        when = wall(tz, local.year, local.month, local.day, hour, minute)
        return when if when > at else wall(tz, *(local+dt.timedelta(days=1)).timetuple()[:3], hour, minute)
    except ValueError: return None

def claude_reset(text, at):
    if not (m := CLAUDE.search(text)): return None
    try: tz = zoneinfo.ZoneInfo(m[6]) if m[6] else None
    except (zoneinfo.ZoneInfoNotFoundError, ValueError): return None
    return first_after(at, tz, m[1], m[2], None, int(m[3]) % 12 + (12 if m[5] == "pm" else 0), int(m[4] or 0))

def codex_reset(text, at):
    if not (m := CODEX.search(text)): return None
    return first_after(at, None, m[1], m[2], m[3], int(m[4]) % 12 + (12 if m[6] == "PM" else 0), int(m[5]))

def tail(path, size=1 << 18):
    with path.open("rb") as f:
        start = max(0, f.seek(0, os.SEEK_END)-size)
        f.seek(start)
        lines = f.read().splitlines()
    return lines[1:] if start else lines

def entry(line):
    try: return json.loads(line)
    except ValueError: return None

def stamp(value): return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))

def claude(path):
    """Claude Code writes its limit notice in place of the reply; the session's last reply decides."""
    for line in reversed(tail(path)):
        if b'"assistant"' in line and (e := entry(line)) and e.get("type") == "assistant" and not e.get("isSidechain"):
            if e.get("error") != "rate_limit": return None
            text = " ".join(c["text"] for c in e["message"]["content"] if c.get("type") == "text").strip()
            return dict(text=text, at=e["timestamp"], resets=claude_reset(text, stamp(e["timestamp"])))
    return None

def codex(path):
    """Codex ends the turn with a usage-limit error; any later turn decides instead."""
    for line in reversed(tail(path)):
        if b'"event_msg"' in line and (e := entry(line)) and e.get("type") == "event_msg" and (p := e["payload"])["type"] in ("task_started", "task_complete", "turn_aborted"):
            if p["type"] != "task_complete" or (p.get("error") or {}).get("codex_error_info") != "usage_limit_exceeded": return None
            return dict(text=p["error"]["message"], at=e["timestamp"], resets=codex_reset(p["error"]["message"], stamp(e["timestamp"])))
    return None

READ = {"claude-code": claude, "codex": codex}

def transcript(meta):
    """The provider's own file for this session: found once, and looked for again a minute after a miss."""
    key = (meta["provider"], ps := meta.get("provider_session"))
    if meta["provider"] not in READ or not ps or not re.fullmatch(r"[0-9a-f-]{8,64}", ps): return None
    if (hit := FOUND.get(key)) and (hit[0] and hit[0].is_file() or not hit[0] and time.monotonic() < hit[1]): return hit[0]
    if meta["provider"] == "claude-code": found = sorted((Path(os.environ.get("CLAUDE_CONFIG_DIR", "~/.claude")).expanduser()/"projects").glob(f"*/{ps}.jsonl"))
    else: found = sorted((Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()/"sessions").glob(f"*/*/*/rollout-*-{ps}.jsonl"))
    FOUND[key] = (found[-1] if found else None, time.monotonic()+60)
    return FOUND[key][0]

def limit(meta):
    """This session's usage-limit interruption ({text, at, resets} with resets None when the notice names no time), or None."""
    if not (path := transcript(meta)): return None
    try: mark = ((st := path.stat()).st_mtime_ns, st.st_size)
    except OSError: return None
    if (hit := STATE.get(path)) and hit[0] == mark: return hit[1]
    try: value = READ[meta["provider"]](path)
    except (OSError, KeyError, TypeError, AttributeError): value = None
    if value: value = dict(value, resets=value["resets"] and value["resets"].astimezone(dt.timezone.utc).isoformat())
    STATE[path] = (mark, value)
    return value

def holding(meta):
    """Words for an interrupted session wait until its limit resets, so they reach an agent that can act on them."""
    return bool((v := limit(meta)) and v["resets"] and dt.datetime.fromisoformat(v["resets"]).timestamp()+GRACE > time.time())

def heard(data, i, sid):
    """The word at i starts a line of a message (in JSON, after an escaped newline or the opening quote) for this session:
    the nearest "(session o...)" header on its transcript line names it, so an agent quoting another session's words does
    not count (inbox output from before it had that header does). Claude Code's queue bookkeeping does not count either."""
    start = data.rfind(b"\n", 0, i)+1
    head = data.rfind(b"(session o", start, i)
    return (data[i-1:i] == b'"' or data[i-2:i] == b"\\n") and not data.startswith(QUEUED, start) and (head < 0 or data.startswith(sid, head+9))

def arrived(meta):
    """Ids of the user's words this session's agent has in its context: its own transcript shows them. Codex logs a queued
    message only once a turn takes it; Claude Code logs a message it queues mid-turn twice, and only the second time is the
    agent's. Read incrementally, so a long transcript is read whole once; None when there is no transcript to read."""
    if not (path := transcript(meta)): return None
    sid = meta["id"].encode()
    with LOCK:
        offset, ids = SEEN.get(path, (0, frozenset()))
        try:
            if path.stat().st_size <= offset: return ids
            with path.open("rb") as f:
                f.seek(offset)
                rest, found = b"", set()
                while chunk := f.read(1 << 24):
                    data = rest+chunk
                    end = data.rfind(b"\n")+1
                    found |= {m[1].decode() for m in WORDS.finditer(data, 0, end) if heard(data, m.start(), sid)}
                    offset, rest = offset+end, data[end:]
        except OSError: return ids
        SEEN[path] = (offset, ids := ids | found)
        return ids
