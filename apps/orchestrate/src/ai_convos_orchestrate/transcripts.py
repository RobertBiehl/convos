"""What the provider's own transcript of a session says: whether its usage limit stopped it, and which of the user's
words have reached it.

Claude Code keeps projects/*/SESSION.jsonl under its config dir; Codex keeps sessions/YYYY/MM/DD/rollout-*-THREAD.jsonl.
For limits the latest turn decides: a session stays interrupted until it replies again. A transcript is read again only
when it changes, and how far its words were read is kept in heard.json, so a restarted server does not read gigabytes again.
"""
import datetime as dt, functools, json, os, re, threading, time, zoneinfo
from pathlib import Path
from . import store

MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()
# "resets 12pm (Europe/Berlin)", "resets 12:50am (Europe/Rome)", "resets Oct 15 at 1pm (Europe/Rome)"
CLAUDE = re.compile(r"resets (?:([A-Z][a-z]{2}) (\d{1,2}),? (?:at )?)?(\d{1,2})(?::(\d{2}))?\s*([ap]m)(?: \(([^)]+)\))?")
# "try again at 2:59 AM.", "try again at Sep 7th, 2026 7:41 AM." (this machine's local time)
CODEX = re.compile(r"try again at (?:([A-Z][a-z]{2}) (\d{1,2})(?:st|nd|rd|th)?,? (\d{4}),? )?(\d{1,2}):(\d{2})\s*([AP]M)")
GRACE = 60  # the provider shows its reset to the minute; held words go out a minute after it
FOUND, STATE, SEEN, LOCKS, SAVED, SAVE = {}, {}, {}, {}, {}, threading.Lock()
SAVE_EVERY = 1 << 26  # bytes read between saves of heard.json: the most a restart reads again of one transcript
# how the user's words read wherever they reach an agent (a pushed message, inbox output, hook context): a line of the
# message starting "- c5 user comment". A literal start keeps the scan at gigabytes per second.
WORDS = re.compile(rb'- ([cq]\d+) (?:user comment|answered on|dismissed by the user|orchestrate notice)')
QUEUED = b'{"type":"queue-operation"'  # Claude Code logs a message it queues mid-turn before the agent sees it
# Claude Code logs each message it reads from its stdin entering its queue at once, with no content, even mid tool call; the
# words themselves show only once the model takes them
ENQUEUED = re.compile(rb'^\{"type":"queue-operation","operation":"enqueue","timestamp":"([^"]+)","sessionId":"[0-9a-f-]+"\}$', re.M)

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

def antigravity(path):
    """Antigravity logs an ERROR step when a quota or rate limit stops a turn; any later model reply clears it."""
    for line in reversed(tail(path)):
        if not (e := entry(line)): continue
        if e.get("type") == "PLANNER_RESPONSE" and e.get("status") == "DONE": return None
        if e.get("status") == "ERROR" and (text := (e.get("error") or e.get("content") or "").strip()) and re.search(r"rate.?limit|quota|resource.?exhausted|too many requests", text, re.I):
            return dict(text=text, at=e["created_at"], resets=None)
    return None

def latest_model(meta):
    """Latest exact model recorded in the session's Antigravity transcript, if available."""
    if meta.get("provider") != "antigravity" or not (path := transcript(meta)): return None
    return next((e["model"] for line in reversed(tail(path)) if b'"model"' in line and (e := entry(line)) and e.get("type") == "PLANNER_RESPONSE" and e.get("model")), None)

READ = {"claude-code": claude, "codex": codex, "antigravity": antigravity}

def transcript(meta):
    """The provider's own file for this session: found once, and looked for again a minute after a miss."""
    key = (meta["provider"], ps := meta.get("provider_session"))
    if meta["provider"] not in READ or not ps or not re.fullmatch(r"[0-9a-f-]{8,64}", ps): return None
    if (hit := FOUND.get(key)) and (hit[0] and hit[0].is_file() or not hit[0] and time.monotonic() < hit[1]): return hit[0]
    if meta["provider"] == "claude-code": found = sorted((Path(os.environ.get("CLAUDE_CONFIG_DIR", "~/.claude")).expanduser()/"projects").glob(f"*/{ps}.jsonl"))
    elif meta["provider"] == "antigravity":
        logs = Path(os.environ.get("ANTIGRAVITY_APP_DATA_DIR", "~/.gemini/antigravity-cli")).expanduser()/"brain"/ps/".system_generated"/"logs"
        found = [p for name in ("transcript.jsonl", "transcript_full.jsonl") if (p := logs/name).is_file()]
    else: found = sorted((Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()/"sessions").glob(f"*/*/*/rollout-*-{ps}.jsonl"))
    FOUND[key] = (found[-1] if found else None, time.monotonic()+60)
    return FOUND[key][0]

def released(provider):
    """When the user last lifted this provider's usage limit early (a reset redeemed from orchestrate, or the provider saying
    usage is allowed again); a limit hit before then no longer holds words. Kept in resets.json beside the sessions."""
    try: return dt.datetime.fromisoformat(json.loads((store.root()/"resets.json").read_text())[provider])
    except (OSError, ValueError, KeyError): return None

def release(provider):
    path = store.root()/"resets.json"
    try: known = json.loads(path.read_text())
    except (OSError, ValueError): known = {}
    (tmp := path.with_suffix(".tmp")).write_text(json.dumps(dict(known, **{provider: dt.datetime.now(dt.timezone.utc).isoformat()})))
    tmp.replace(path)

def limit(meta):
    """This session's usage-limit interruption ({text, at, resets} with resets None when the notice names no time), or None.
    A limit lifted early resets when it was lifted (minus the grace, so held words go out at once) and names that time."""
    if not (path := transcript(meta)): return None
    try: mark = ((st := path.stat()).st_mtime_ns, st.st_size)
    except OSError: return None
    if (hit := STATE.get(path)) and hit[0] == mark: value = hit[1]
    else:
        try: value = READ[meta["provider"]](path)
        except (OSError, KeyError, TypeError, AttributeError): value = None
        if value: value = dict(value, resets=value["resets"] and value["resets"].astimezone(dt.timezone.utc).isoformat())
        STATE[path] = (mark, value)
    if value and (lifted := released(meta["provider"])) and lifted > stamp(value["at"]): return dict(value, resets=(lifted-dt.timedelta(seconds=GRACE)).isoformat(), released=lifted.isoformat())
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

@functools.cache
def remembered():
    """How far earlier servers read each transcript: {path: (offset, ids, last enqueue, inode)}."""
    try: return {Path(k): (offset, frozenset(ids), last, ino) for k, (offset, ids, last, ino) in json.loads((store.root()/"heard.json").read_text()).items()}
    except (OSError, ValueError): return {}

def save():
    with SAVE:
        known = {**remembered(), **SEEN}  # one copy: other threads keep reading transcripts meanwhile
        (tmp := store.root()/"heard.json.tmp").write_text(json.dumps({str(k): [offset, sorted(ids), last, ino] for k, (offset, ids, last, ino) in known.items()}))
        tmp.replace(store.root()/"heard.json")
        SAVED.update((k, v[0]) for k, v in known.items())

def arrived(meta):
    """Ids of the user's words this session's agent has in its context: its own transcript shows them. Codex logs a queued
    message only once a turn takes it; Claude Code logs a message it queues mid-turn twice, and only the second time is the
    agent's. Read incrementally from where the last read stopped, even in an earlier server (a transcript only grows; another
    file under its name starts over); one transcript's first read does not hold up the others. None without a transcript."""
    if not (path := transcript(meta)): return None
    sid = meta["id"].encode()
    with LOCKS.setdefault(path, threading.Lock()):
        try: st = path.stat()
        except OSError: return SEEN.get(path, (0, frozenset()))[1]
        before = SEEN.get(path) or remembered().get(path)
        offset, ids, last, ino = before if before and before[3] == st.st_ino else (0, frozenset(), None, st.st_ino)
        if st.st_size <= offset:
            SEEN[path] = (offset, ids, last, ino)
            return ids
        try:
            with path.open("rb") as f:
                f.seek(offset)
                rest, found = b"", set()
                while chunk := f.read(1 << 24):
                    data = rest+chunk
                    end = data.rfind(b"\n")+1
                    found |= {m[1].decode() for m in WORDS.finditer(data, 0, end) if heard(data, m.start(), sid)}
                    last = max((m[1].decode() for m in ENQUEUED.finditer(data, 0, end)), default=last)
                    offset, rest = offset+end, data[end:]
        except OSError: return ids
        SEEN[path] = (offset, ids | found, last, ino)
    if found - ids or last != (before or (0, 0, None))[2] or offset-SAVED.get(path, 0) >= SAVE_EVERY: save()
    return ids | found

def enqueued(meta):
    """When a Claude Code agent last took a message from its stdin into its queue, as its transcript logs it; None before any."""
    return None if arrived(meta) is None else SEEN.get(transcript(meta), (0, 0, None))[2]
