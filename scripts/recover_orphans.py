#!/usr/bin/env python3
"""One-off recovery for orphaned attribution (see PR #16) after Claude Code's retention
deleted transcripts. Two exact, reversible recoveries (rows tagged in metadata):

1. id-inversion: orphaned tool_calls/file_edits reference message ids that were never
   inserted, but gen_id is deterministic -- enumerate sha256("claude-code:{cid}:{idx}")
   over known conversation ids and re-create the missing messages as role='unknown' stubs.
2. history.jsonl: conversations whose transcript vanished before any sync still have user
   prompts in ~/.claude/history.jsonl (display/timestamp/project/sessionId); session ids
   map deterministically to conversation ids via the projects-dir path munge.

Undo: DELETE FROM messages WHERE metadata LIKE '%"recovered"%' (and conversations alike).
"""
import hashlib, json, re, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import duckdb

DB = Path.home() / ".convos" / "data" / "convos.db"
HIST = Path.home() / ".claude" / "history.jsonl"
gen_id = lambda source, oid: hashlib.sha256(f"{source}:{oid}".encode()).hexdigest()[:16]
munge = lambda project: re.sub(r"[^A-Za-z0-9-]", "-", project)
sid_path = lambda project, sid: f"{Path.home()}/.claude/projects/{munge(project)}/{sid}.jsonl"

def verify_munge(hist):
    """Check the path-munge rule against surviving on-disk sessions before trusting it."""
    disk = {p.stem: str(p.parent.name) for p in (Path.home() / ".claude" / "projects").rglob("*.jsonl")}
    pairs = [(h["project"], h["sessionId"]) for h in hist if h.get("sessionId") in disk]
    bad = [(p, s) for p, s in pairs if munge(p) != disk[s]]
    assert pairs and not bad, f"munge rule mismatch: {bad[:3] or 'no overlapping sessions to verify'}"
    print(f"munge rule verified on {len(pairs)} on-disk sessions")

def recover_stubs(conn):
    convs = [r[0] for r in conn.execute("SELECT id FROM conversations WHERE source = 'claude-code'").fetchall()]
    orph = dict(conn.execute("""SELECT message_id, MIN(created_at) FROM (
        SELECT fe.message_id, fe.created_at FROM file_edits fe LEFT JOIN messages m ON fe.message_id = m.id WHERE m.id IS NULL
        UNION ALL SELECT tc.message_id, tc.created_at FROM tool_calls tc LEFT JOIN messages m ON tc.message_id = m.id WHERE m.id IS NULL)
        GROUP BY message_id""").fetchall())
    found = {h: (cid, i) for cid in convs for i in range(5000) if (h := gen_id("claude-code", f"{cid}:{i}")) in orph}
    meta = json.dumps({"recovered": "id-inversion"})
    conn.executemany("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?,?,?,NULL,NULL)",
                     [(mid, cid, "unknown", "", None, orph[mid], None, meta) for mid, (cid, i) in found.items()])
    print(f"id-inversion: re-created {len(found)} of {len(orph)} missing messages")

def recover_history(conn):
    hist = [json.loads(l) for l in HIST.read_text().splitlines() if l.strip()]
    hist = [h for h in hist if h.get("sessionId") and h.get("project") and h.get("display")]
    verify_munge(hist)
    known = {r[0] for r in conn.execute("SELECT id FROM conversations").fetchall()}
    by_conv = defaultdict(list)
    for h in hist: by_conv[gen_id("claude-code", sid_path(h["project"], h["sessionId"]))].append(h)
    missing = {cid: hs for cid, hs in by_conv.items() if cid not in known}
    meta = json.dumps({"recovered": "history.jsonl"})
    conn.executemany("INSERT OR IGNORE INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?)",
                     [(cid, "claude-code", f"{hs[0]['project']} ({hs[0]['sessionId'][:8]})",
                       datetime.fromtimestamp(min(h["timestamp"] for h in hs) / 1000),
                       datetime.fromtimestamp(max(h["timestamp"] for h in hs) / 1000),
                       "claude", hs[0]["project"], None, None, meta) for cid, hs in missing.items()])
    conn.executemany("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?,?,?,NULL,NULL)",
                     [(gen_id("claude-code", f"hist:{cid}:{h['timestamp']}"), cid, "user", h["display"], None,
                       datetime.fromtimestamp(h["timestamp"] / 1000), None, meta) for cid, hs in missing.items() for h in hs])
    print(f"history.jsonl: recovered {len(missing)} conversations / {sum(len(h) for h in missing.values())} prompts "
          f"({len(by_conv) - len(missing)} conversations already known)")

if __name__ == "__main__":
    conn = duckdb.connect(str(DB))
    recover_stubs(conn)
    recover_history(conn)
    left = conn.execute("SELECT COUNT(*) FROM file_edits fe LEFT JOIN messages m ON fe.message_id = m.id WHERE m.id IS NULL").fetchone()[0]
    print(f"orphaned file_edits remaining: {left}")
    conn.close()
