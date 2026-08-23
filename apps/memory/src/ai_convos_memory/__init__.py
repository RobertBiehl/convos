"""Read-only provider adapters plus a deterministic plan/resolve/apply memory ledger."""
import contextlib, hashlib, json, os, queue, re, shlex, shutil, site, sqlite3, subprocess, sys, sysconfig, threading, time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Optional

import typer

memory = typer.Typer(help="Keep project memory consistent across Codex, Claude, and your own notes.", invoke_without_command=True, no_args_is_help=False)
HOOK_CONTEXT_CHARS = 16000
SCHEMA = """PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY,provider TEXT NOT NULL,scope TEXT NOT NULL,locator TEXT NOT NULL,path TEXT NOT NULL,hash TEXT NOT NULL,content TEXT NOT NULL,active INTEGER NOT NULL,observed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revisions(source TEXT NOT NULL REFERENCES sources(id),hash TEXT NOT NULL,content TEXT NOT NULL,observed_at TEXT NOT NULL,PRIMARY KEY(source,hash));
CREATE TABLE IF NOT EXISTS canonicals(id TEXT PRIMARY KEY,scope TEXT NOT NULL,content TEXT NOT NULL,hash TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS canonical_revisions(canonical TEXT NOT NULL REFERENCES canonicals(id),hash TEXT NOT NULL,content TEXT NOT NULL,observed_at TEXT NOT NULL,PRIMARY KEY(canonical,hash));
CREATE TABLE IF NOT EXISTS links(source TEXT PRIMARY KEY REFERENCES sources(id),canonical TEXT NOT NULL REFERENCES canonicals(id),applied_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS projections(provider TEXT NOT NULL,target TEXT NOT NULL,hash TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(provider,target));
CREATE TABLE IF NOT EXISTS repository_scopes(repository TEXT NOT NULL,lineage TEXT,scope TEXT NOT NULL,checkout TEXT NOT NULL UNIQUE,observed_at TEXT NOT NULL,PRIMARY KEY(repository,checkout));
CREATE TABLE IF NOT EXISTS remote_semantics(workspace TEXT NOT NULL,author TEXT NOT NULL,entity TEXT NOT NULL,revision TEXT NOT NULL,state TEXT NOT NULL,body TEXT NOT NULL,proof TEXT NOT NULL,owned INTEGER NOT NULL,PRIMARY KEY(workspace,author,entity,revision));
CREATE TABLE IF NOT EXISTS remote_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(canonical TEXT NOT NULL REFERENCES canonicals(id),hash TEXT NOT NULL,message TEXT NOT NULL,conversation TEXT NOT NULL,source TEXT NOT NULL,title TEXT,role TEXT NOT NULL,created_at TEXT,content_hash TEXT NOT NULL,PRIMARY KEY(canonical,hash,message));
PRAGMA user_version=6;"""

def _hash(value): return hashlib.sha256(value.encode()).hexdigest()
def atomic_json(path, data): path.parent.mkdir(parents=True, exist_ok=True); mode = path.stat().st_mode&0o777 if path.exists() else 0o600; tmp = path.with_name(f".{path.name}.{os.getpid()}"); tmp.touch(mode=mode, exist_ok=False); os.chmod(tmp, mode); tmp.write_text(json.dumps(data, indent=2)+"\n"); os.replace(tmp, path)
def _db_path(root=None):
    if root is not None: return Path(root).expanduser()/"memory"/"state.db"
    base=Path(os.environ.get("CONVOS_PROJECT_ROOT",Path.home()/".convos")).expanduser(); return Path(os.environ.get("CONVOS_MEMORY_DB",base/"memory"/"state.db")).expanduser()
def _remote(value):
    value = value.strip().removesuffix(".git").rstrip("/")
    if not value or value.startswith(("/",".","file:")): return None
    if "://" not in value and (m := re.fullmatch(r"(?:[^@/:]+@)?([^/:]+):(.+)", value)): value = f"{m.group(1)}/{m.group(2)}"
    else: value = re.sub(r"^[a-z][a-z0-9+.-]*://(?:[^@/]+@)?","",value,flags=re.I)
    host, slash, path = value.partition("/"); return host.lower() + (slash+path if slash else "")
def _git(root, *args):
    result = subprocess.run(("git","-C",str(root),*args),capture_output=True,text=True)
    if result.returncode: raise ValueError(result.stderr.strip() or "Git repository evidence unavailable")
    return result.stdout.strip()
@lru_cache(maxsize=256)
def _repository(root):
    root = Path(root).resolve()
    try:
        if Path(_git(root,"rev-parse","--show-toplevel")).resolve() != root: return None
        try: origin = _remote(_git(root,"remote","get-url","origin"))
        except ValueError: origin = None
        try: roots = sorted(_git(root,"rev-list","--max-parents=0","HEAD").splitlines())
        except ValueError: roots = []
        lineage = "lineage_"+_hash("\0".join(roots))[:20] if roots else None; return dict(repository="repo_"+_hash("remote\0"+origin)[:20] if origin else lineage, lineage=lineage, checkout=str(root)) if origin or lineage else None
    except (OSError, ValueError): return None
def _raw_scope(scope=None):
    path = Path(scope or Path.cwd()).expanduser(); missing = bool(scope) and not path.exists(); path = path if missing else path.resolve(); return scope if missing else str(next((p for p in (path, *path.parents) if (p/".git").exists()), path))
def _scope(scope=None):
    raw = _raw_scope(scope); repo, path = _repository(raw), _db_path()
    if not repo or path.is_symlink() or not path.is_file(): return raw
    db = sqlite3.connect(path)
    if db.execute("PRAGMA user_version").fetchone()[0] in (0,1,2,3,4): db.close(); upgraded = connect(); upgraded.close(); return _scope(raw)
    try:
        if (row := db.execute("SELECT repository,scope FROM repository_scopes WHERE checkout=?", (repo["checkout"],)).fetchone()) and row[0] == repo["repository"]: return row[1]
        rows = db.execute("SELECT DISTINCT scope FROM repository_scopes WHERE repository=? ORDER BY scope", (repo["repository"],)).fetchall()
        if len(rows) > 1: raise ValueError(f"Repository maps to multiple memory scopes: {', '.join(r[0] for r in rows)}; preview one with convos memory adopt-scope SCOPE")
        if rows: db.execute("INSERT OR REPLACE INTO repository_scopes VALUES (?,?,?,?,?)", (repo["repository"],repo["lineage"],rows[0][0],repo["checkout"],datetime.now(timezone.utc).isoformat())); db.commit()
        return rows[0][0] if rows else raw
    except sqlite3.Error: return raw
    finally: db.close()
def _bind_scope(db, scope):
    if not (repo := _repository(scope)): return scope
    row = db.execute("SELECT repository,lineage,scope FROM repository_scopes WHERE checkout=?", (repo["checkout"],)).fetchone()
    if row and row["repository"] == repo["repository"]: return row["scope"]
    scopes = [r[0] for r in db.execute("SELECT DISTINCT scope FROM repository_scopes WHERE repository=? ORDER BY scope", (repo["repository"],))]
    if len(scopes) > 1: raise ValueError(f"Repository maps to multiple memory scopes: {', '.join(scopes)}; preview one with convos memory adopt-scope SCOPE")
    chosen = scopes[0] if scopes else f"{scope}@{repo['repository'][5:13]}" if row else scope
    db.execute("INSERT OR REPLACE INTO repository_scopes VALUES (?,?,?,?,?)", (repo["repository"],repo["lineage"],chosen,repo["checkout"],datetime.now(timezone.utc).isoformat())); return chosen
def connect(root=None):
    path = _db_path(root); path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.exists() and not path.is_file(): raise ValueError("Memory database path must be a regular non-symlink file")
    path.touch(mode=0o600, exist_ok=True); os.chmod(path, 0o600); db = sqlite3.connect(path); db.row_factory = sqlite3.Row
    if (version := db.execute("PRAGMA user_version").fetchone()[0]) not in (0, 1, 2, 3, 4, 5, 6): db.close(); raise ValueError("Unsupported memory ledger schema version; expected 0 through 6")
    db.executescript(SCHEMA)
    if version<6: db.executescript("DROP TABLE IF EXISTS remote_heads;DROP TABLE IF EXISTS remote_parts;")
    db.execute("INSERT OR IGNORE INTO remote_meta VALUES ('ledger_id',?)",(os.urandom(16).hex(),)); db.commit()
    if version < 2:
        [db.execute("INSERT OR IGNORE INTO repository_scopes VALUES (?,?,?,?,?)", (repo["repository"],repo["lineage"],scope,repo["checkout"],datetime.now(timezone.utc).isoformat())) for scope, in db.execute("SELECT scope FROM sources UNION SELECT scope FROM canonicals") if (repo := _repository(scope))]
        db.commit()
    if db.execute("SELECT 1 FROM links l JOIN sources s ON s.id=l.source JOIN canonicals c ON c.id=l.canonical WHERE s.scope<>c.scope LIMIT 1").fetchone(): db.close(); raise ValueError("Cross-scope memory link in ledger")
    return db
@contextlib.contextmanager
def _transaction(db,commit=True):
    db.execute("BEGIN IMMEDIATE")
    try: yield
    except BaseException: db.rollback(); raise
    else: db.commit() if commit else db.rollback()
def _codex_scopes(content): return [_scope(s.strip()) for s in re.split(r"\s+and\s+(?=/)", m.group(1).strip())] if (m := re.search(r"(?m)^applies_to: cwd=([^;\n]+)", content)) else ["global"]
def _codex():
    root = Path(os.environ.get("CONVOS_CODEX_MEMORY_ROOT", Path(os.environ.get("CODEX_HOME", Path.home()/".codex"))/"memories")).expanduser(); path = root/"MEMORY.md"
    if not root.exists(): return [], set()
    if not path.exists(): return [], {"codex"}
    text = path.read_text(); parts = [p.strip() for p in re.split(r"(?m)(?=^# Task Group: )", text) if p.lstrip().startswith("# Task Group:")]
    if text.strip() and not parts: raise ValueError("Unrecognized Codex MEMORY.md format; refusing to mark prior sources missing")
    return [dict(provider="codex", scope=scope, locator=f"MEMORY.md#{content.splitlines()[0][2:].strip()}", path=str(path), content=content)
            for content in parts for scope in _codex_scopes(content)], {"codex"}
def _session_cwd(path):
    with path.open() as lines:
        for line in lines:
            if '"cwd":' not in line: continue
            try:
                if cwd := json.loads(line).get("cwd"): return cwd
            except json.JSONDecodeError: continue
def _claude_history():
    path = _claude_root().parent/"history.jsonl"; rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []; history = {}
    [history.setdefault(e["sessionId"], set()).add(e["project"]) for e in rows if e.get("sessionId") and e.get("project")]
    return history
def _claude_scope(project, scope=None, history=None):
    index = project/"sessions-index.json"; indexed = json.loads(index.read_text())["entries"] if index.exists() else []
    cwds = {_scope(cwd) for path in project.glob("*.jsonl") if (cwd := _session_cwd(path))} | {_scope(e["projectPath"]) for e in indexed}
    origins = {m.group(1) for path in (project/"memory").glob("*.md") if (m := re.search(r"(?m)^originSessionId:\s*(\S+)", path.read_text()))}; history = _claude_history() if history is None else history; cwds |= {_scope(cwd) for origin in origins for cwd in history.get(origin, ())}
    if len(cwds) > 1:
        if scope is None or scope in cwds: raise ValueError(f"Conflicting Claude cwd metadata in {project}")
        return None
    return next(iter(cwds), project.name)
def _claude_root(): return Path(os.environ.get("CONVOS_CLAUDE_PROJECTS_ROOT", Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude"))/"projects")).expanduser()
def _claude(scope=None):
    root = _claude_root()
    if not root.exists(): return [], set()
    files = sorted(p for p in root.glob("*/memory/*.md") if p.name not in ("MEMORY.md", "convos-synced.md")); history = _claude_history(); scopes = {p:_claude_scope(p, scope, history) for p in {f.parent.parent for f in files}}
    return [dict(provider="claude-code", scope=scopes[p.parent.parent], locator=str(p.relative_to(root)), path=str(p), content=p.read_text()) for p in files if scopes[p.parent.parent] is not None and (scope is None or scopes[p.parent.parent] == scope)], {"claude-code"}
def discover(scope=None):
    ci, ca = _codex(); hi, ha = _claude(scope); return [i for i in ci + hi if scope is None or i["scope"] == scope], ca | ha
def _sid(item): return _hash("\0".join(item[k] for k in ("provider", "scope", "locator")))[:20]

def scan_store(scope=None):
    _repository.cache_clear(); items, available = discover(scope); db, now, revisions = connect(), datetime.now(timezone.utc).isoformat(), 0
    with db:
        scope = _bind_scope(db,scope) if scope is not None else None; items = list({(item["provider"],bound := _bind_scope(db,item["scope"]),item["locator"]):{**item,"scope":bound} for item in items}.values())
        [db.execute("UPDATE sources SET active=0 WHERE provider=?" + (" AND scope=?" if scope is not None else ""), (provider, scope) if scope is not None else (provider,)) for provider in available]
        for item in items:
            sid, digest = _sid(item), _hash(item["content"])
            db.execute("""INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                provider=excluded.provider,scope=excluded.scope,locator=excluded.locator,path=excluded.path,hash=excluded.hash,content=excluded.content,active=1,observed_at=excluded.observed_at""",
                       (sid, item["provider"], item["scope"], item["locator"], item["path"], digest, item["content"], 1, now))
            revisions += db.execute("INSERT OR IGNORE INTO revisions VALUES (?,?,?,?)", (sid, digest, item["content"], now)).rowcount
    counts = dict(db.execute("SELECT COUNT(*),SUM(active),SUM(NOT active) FROM sources WHERE ? IS NULL OR scope=?", (scope, scope)).fetchone()); db.close()
    return dict(scanned=len(items), revisions=revisions, sources=counts["COUNT(*)"], active=counts["SUM(active)"] or 0, missing=counts["SUM(NOT active)"] or 0)
def _state(db, scope=None):
    args = (scope, scope)
    return dict(sources=[tuple(r) for r in db.execute("SELECT id,hash,active FROM sources WHERE ? IS NULL OR scope=? ORDER BY id", args)],
                links=[tuple(r) for r in db.execute("SELECT l.source,l.canonical,l.applied_hash FROM links l JOIN sources s ON s.id=l.source WHERE ? IS NULL OR s.scope=? ORDER BY l.source", args)],
                canonicals=[tuple(r) for r in db.execute("SELECT id,hash FROM canonicals WHERE ? IS NULL OR scope=? ORDER BY id", args)])
def plan_data(include_content=False, db=None, scope=None):
    own, db = db is None, db or connect()
    rows = [dict(r) for r in db.execute("""SELECT s.*,l.canonical,l.applied_hash,
        (SELECT id FROM canonicals c WHERE c.hash=s.hash AND c.scope=s.scope ORDER BY id LIMIT 1) exact,
        (SELECT id FROM sources n WHERE n.active AND n.provider=s.provider AND n.locator=s.locator AND n.hash=s.hash AND n.id<>s.id ORDER BY n.id LIMIT 1) replacement,
        (SELECT content FROM revisions v WHERE v.source=s.id AND v.hash=l.applied_hash) ancestor
        FROM sources s LEFT JOIN links l ON l.source=s.id
        WHERE ((s.active AND (l.source IS NULL OR l.applied_hash<>s.hash)) OR (NOT s.active AND l.source IS NOT NULL))
        AND (? IS NULL OR s.scope=?) ORDER BY s.provider,s.scope,s.locator""", (scope, scope))]
    collisions = {c for c, n in db.execute("SELECT canonical,COUNT(*) FROM links JOIN sources ON source=id WHERE active AND applied_hash<>hash AND (? IS NULL OR scope=?) GROUP BY canonical HAVING COUNT(*)>1", (scope, scope))}
    pending = [dict(source=r["id"], provider=r["provider"], scope=r["scope"], locator=r["locator"], path=r["path"], hash=r["hash"], replacement=r["replacement"],
                    kind="missing" if not r["active"] else "collision" if r["canonical"] in collisions else "exact" if r["canonical"] is None and r["exact"] else "unlinked" if r["canonical"] is None else "changed",
                    canonical=r["canonical"], suggested=r["exact"], **({"content":r["content"], "ancestor":r["ancestor"]} if include_content else {})) for r in rows]
    plan = _hash(json.dumps(dict(version=1, state=_state(db, scope), scope=scope), sort_keys=True, separators=(",", ":")))[:20]
    canonicals = [dict(r) for r in db.execute("SELECT id,scope,hash,content,updated_at FROM canonicals WHERE ? IS NULL OR scope=? ORDER BY id", (scope, scope))] if include_content else None
    if own: db.close()
    return dict(version=1, plan=plan, scope=scope, pending=pending, **({"canonicals":canonicals} if include_content else {}))
def _canonical(db, cid, scope, content, now):
    digest = _hash(content)
    db.execute("""INSERT INTO canonicals VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
        scope=excluded.scope,content=excluded.content,hash=excluded.hash,updated_at=excluded.updated_at""", (cid, scope, content, digest, now))
    db.execute("INSERT OR IGNORE INTO canonical_revisions VALUES (?,?,?,?)", (cid, digest, content, now))
def _archive_evidence(refs, scope):
    db = None
    try:
        from ai_convos import cli
        cli.drain_hooks()
        if not (db := cli.get_db(True)): raise ValueError("Conversation archive is unavailable; run convos sync first")
        out = []
        for ref in dict.fromkeys(refs):
            rows = db.execute("""SELECT m.id,m.conversation_id,c.source,c.title,m.role,m.created_at,m.content,c.cwd FROM messages m JOIN conversations c ON c.id=m.conversation_id
                WHERE starts_with(m.id,?) AND json_extract_string(m.metadata,'$.history_of') IS NULL AND COALESCE(m.content,'')<>'' LIMIT 2""", [ref]).fetchall()
            if len(rows) != 1: raise ValueError(f"Archived message is {'missing' if not rows else 'ambiguous'}: {ref}")
            mid,cid,source,title,role,created,body,cwd = rows[0]
            if cwd and _scope(cwd) != scope: raise ValueError(f"Archived message is outside memory scope: {mid}")
            out.append(dict(message=mid,conversation=cid,source=source,title=title,role=role,created_at=str(created) if created else None,content_hash=_hash(body)))
        return out
    except ValueError: raise
    except Exception as e: raise ValueError(f"Conversation archive lookup failed: {e}") from e
    finally:
        if db: db.close()
def remember_data(content, scope=None, canonical=None, evidence=()):
    content, scope = content.strip(), _scope(scope)
    if not content: raise ValueError("memory content cannot be empty")
    proofs, db, now = _archive_evidence(evidence,scope) if evidence else [], connect(), datetime.now(timezone.utc).isoformat()
    with contextlib.closing(db),_transaction(db):
        scope = _bind_scope(db,scope); replacing = canonical is not None; cid = _select_canonical(db,scope,canonical)["id"] if replacing else "mem_" + _hash(scope + "\0" + content)[:16]; row = db.execute("SELECT * FROM canonicals WHERE id=?", (cid,)).fetchone(); origins = db.execute("SELECT s.* FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=? ORDER BY s.id", (cid,)).fetchall()
        if replacing and (len(origins) != 1 or origins[0]["provider"] not in ("user","remote")): raise ValueError("only user-owned memories without Codex or Claude origins can be revised directly")
        if row and (row["scope"] != scope or not replacing and row["content"] != content): raise ValueError("canonical id collision")
        locator, digest = f"user/{cid}", _hash(content); sid = origins[0]["id"] if replacing else _hash("\0".join(("user",scope,locator)))[:20]; prior = db.execute("SELECT content FROM sources WHERE id=?", (sid,)).fetchone(); status = "unchanged" if prior and prior["content"] == content else "revised" if replacing else "linked" if row else "created"
        db.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET hash=excluded.hash,content=excluded.content,active=1,observed_at=excluded.observed_at", (sid,"user",scope,locator,str(_db_path()),digest,content,1,now)); db.execute("INSERT OR IGNORE INTO revisions VALUES (?,?,?,?)", (sid,digest,content,now))
        if not row or row["content"] != content: _canonical(db,cid,scope,content,now)
        db.execute("INSERT INTO links VALUES (?,?,?) ON CONFLICT(source) DO UPDATE SET canonical=excluded.canonical,applied_hash=excluded.applied_hash", (sid,cid,digest)); [db.execute("INSERT OR IGNORE INTO evidence VALUES (?,?,?,?,?,?,?,?,?)",(cid,digest,*(p[k] for k in ("message","conversation","source","title","role","created_at","content_hash")))) for p in proofs]
    return dict(id=cid,scope=scope,hash=digest,source=sid,status=status,evidence=len(proofs))
def forget_data(canonical, scope=None, dry_run=False):
    scope, db = _scope(scope), connect()
    with contextlib.closing(db),_transaction(db,not dry_run):
        row = _select_canonical(db,scope,canonical); canonical = row["id"]; origins = db.execute("SELECT s.id,s.provider,s.locator,s.hash,s.active,l.applied_hash FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=?", (canonical,)).fetchall()
        if not origins or any(r["provider"] not in ("user","remote") or r["provider"]=="user" and (not r["active"] or r["hash"]!=r["applied_hash"]) for r in origins): raise ValueError("only settled user-owned memories without Codex or Claude origins can be forgotten")
        if any((p := Path(r[0])).exists() and (p.is_symlink() or f"canonical:{canonical} " in p.read_text()) for r in db.execute("SELECT target FROM projections")): raise ValueError("canonical is present in a Claude projection; remove or refresh the projection first")
        revisions = db.execute("SELECT COUNT(*) FROM canonical_revisions WHERE canonical=?", (canonical,)).fetchone()[0] + sum(db.execute("SELECT COUNT(*) FROM revisions WHERE source=?", (r["id"],)).fetchone()[0] for r in origins); evidence = db.execute("SELECT COUNT(*) FROM evidence WHERE canonical=?",(canonical,)).fetchone()[0]
        [db.execute("UPDATE remote_semantics SET owned=1 WHERE entity=?",(r["locator"].split("/",2)[-1],)) for r in origins if r["provider"]=="remote"]; [db.execute("DELETE FROM links WHERE source=?", (r["id"],)) for r in origins]; [db.execute("DELETE FROM revisions WHERE source=?", (r["id"],)) for r in origins]; [db.execute("DELETE FROM sources WHERE id=?", (r["id"],)) for r in origins]; db.execute("DELETE FROM evidence WHERE canonical=?", (canonical,)); db.execute("DELETE FROM canonical_revisions WHERE canonical=?", (canonical,)); db.execute("DELETE FROM canonicals WHERE id=?", (canonical,))
    return dict(id=canonical,scope=scope,revisions=revisions,evidence=evidence,status="would_forget" if dry_run else "forgotten")
def apply_data(document, dry_run=False):
    if not isinstance(document, dict) or document.get("version") != 1 or isinstance(document.get("version"), bool): raise ValueError("Unsupported memory resolution version; expected 1")
    resolutions = document.get("resolutions")
    if not isinstance(document.get("plan"), str) or not isinstance(resolutions, list) or document.get("scope") is not None and not isinstance(document["scope"], str) or any(not isinstance(d, dict) or not isinstance(d.get("action"), str) or any(k in d and not isinstance(d[k], str) for k in ("canonical","scope","content","hash","source")) or ("sources" in d and (not isinstance(d["sources"], list) or not d["sources"] or not all(isinstance(s, str) for s in d["sources"]))) or (d.get("action") == "revise" and not all(isinstance(d.get(k), str) for k in ("canonical","hash","content"))) or (d.get("action") != "revise" and "source" not in d and "sources" not in d) for d in resolutions): raise ValueError("Malformed memory resolution document")
    scan_store(document.get("scope")); db, now, applied = connect(), datetime.now(timezone.utc).isoformat(), 0
    with contextlib.closing(db),_transaction(db,not dry_run):
        plan = plan_data(True, db, document.get("scope"))
        if document["plan"] != plan["plan"]: raise ValueError(f"stale plan {document['plan']}; current plan is {plan['plan']}")
        pending, used = {r["source"]:r for r in plan["pending"]}, set()
        for decision in resolutions:
            action = decision["action"]
            if action == "revise":
                row = db.execute("SELECT scope,hash FROM canonicals WHERE id=?", (decision["canonical"],)).fetchone()
                if not row or row["scope"] != document.get("scope") or row["hash"] != decision["hash"]: raise ValueError("canonical revision is stale, missing, or outside the plan scope")
                _canonical(db, decision["canonical"], row["scope"], decision["content"], now); applied += 1; continue
            ids = decision.get("sources") or [decision["source"]]
            if len(set(ids)) != len(ids) or used & set(ids) or any(s not in pending for s in ids): raise ValueError("resolution sources must be unique pending source ids")
            used |= set(ids); rows = [pending[s] for s in ids]
            if action == "unresolved": continue
            if action == "detach":
                if any(r["kind"] != "missing" for r in rows): raise ValueError("detach requires missing sources")
                [db.execute("DELETE FROM links WHERE source=?", (sid,)) for sid in ids]; applied += len(ids); continue
            if any(r["kind"] == "missing" for r in rows): raise ValueError("missing sources can only be unresolved or detached")
            source_scope = rows[0]["scope"]
            if any(r["scope"] != source_scope for r in rows): raise ValueError("resolution sources must share one scope")
            scope, content, cid = decision.get("scope", source_scope), decision.get("content", rows[0]["content"]), decision.get("canonical")
            if scope != source_scope: raise ValueError("resolution scope must match source scope")
            existing = db.execute("SELECT scope FROM canonicals WHERE id=?", (cid,)).fetchone() if cid else None
            if existing and existing["scope"] != scope: raise ValueError("canonical is outside resolution scope")
            if action in ("distinct", "scoped"):
                if len(ids) != 1: raise ValueError(f"{action} requires one source")
                if cid: raise ValueError("canonical ids are allocated by the engine")
                cid = "mem_" + _hash(scope + "\0" + content)[:16]
                if db.execute("SELECT 1 FROM canonicals WHERE id=?", (cid,)).fetchone(): raise ValueError(f"canonical {cid} already exists")
                _canonical(db, cid, scope, content, now)
            elif action == "same":
                if not existing: raise ValueError("same requires an existing canonical")
            elif action in ("merge", "supersedes"):
                if action == "supersedes" and not existing: raise ValueError("supersedes requires an existing canonical")
                if action == "merge" and cid and not existing: raise ValueError("merge canonical is missing")
                cid = cid or "mem_" + _hash(scope + "\0" + content)[:16]; _canonical(db, cid, scope, content, now)
            else: raise ValueError(f"unknown action {action}")
            [db.execute("INSERT INTO links VALUES (?,?,?) ON CONFLICT(source) DO UPDATE SET canonical=excluded.canonical,applied_hash=excluded.applied_hash", (sid, cid, pending[sid]["hash"])) for sid in ids]
            applied += len(ids)
        remaining = len(plan_data(db=db, scope=document.get("scope"))["pending"])
        preview = [dict(r) for r in db.execute("SELECT id,scope,hash,content FROM canonicals WHERE ? IS NULL OR scope=? ORDER BY id", (document.get("scope"), document.get("scope")))] if dry_run else None
    return dict(version=1, applied=applied, remaining=remaining, **({"dry_run":True,"canonicals":preview} if dry_run else {}))
def reconcile_data(scope, dry_run=False, refresh=True, retry=True):
    refresh and scan_store(scope); plan = plan_data(True, scope=scope); pending = plan["pending"]; groups = {(r["scope"],r["hash"]):[p for p in pending if (p["scope"],p["hash"]) == (r["scope"],r["hash"]) and p["kind"] == "unlinked"] for r in pending}
    resolutions = [dict(source=r["source"],action="detach") for r in pending if r["kind"] == "missing" and r["replacement"]] + [dict(source=r["source"],action="same",canonical=r["suggested"]) for r in pending if r["kind"] == "exact"] + [dict(sources=[r["source"] for r in rows],action="merge",scope=key[0]) for key,rows in groups.items() if len(rows)>1] + ([dict(source=pending[0]["source"],action="distinct")] if len(pending) == 1 and pending[0]["kind"] == "unlinked" and not plan["canonicals"] else [])
    if not resolutions: return dict(version=plan["version"],applied=0,remaining=len(pending),**({"dry_run":True,"canonicals":[{k:r[k] for k in ("id","scope","hash","content")} for r in plan["canonicals"]]} if dry_run else {}))
    try: return apply_data(dict(version=plan["version"],plan=plan["plan"],scope=scope,resolutions=resolutions), dry_run)
    except ValueError as e:
        if retry and str(e).startswith("stale plan "): return reconcile_data(scope, dry_run, False, False)
        raise
def sync_data(scope=None):
    scope = _scope(scope); automatic = reconcile_data(scope)["applied"]; plan = plan_data(True, scope=scope)
    return dict(**plan, status="clean" if not plan["pending"] else "needs_resolution", automatic=automatic, remaining=len(plan["pending"]))
def sync_all_data():
    scan_store(); db = connect(); scopes = [r[0] for r in db.execute("SELECT scope FROM sources UNION SELECT scope FROM canonicals ORDER BY scope")]; db.close(); rows = [sync_data(scope) for scope in scopes]
    concise = [{k:r[k] for k in ("scope","status","automatic","remaining")} for r in rows]; remaining = sum(r["remaining"] for r in rows)
    return dict(scope=None,status="clean" if not remaining else "needs_resolution",automatic=sum(r["automatic"] for r in rows),remaining=remaining,scopes=concise)
def review_data(scope=None):
    result = sync_data(scope); pending = result["pending"]; canonicals = {r["id"]:r for r in result["canonicals"]}; header = f"# Memory changes\n\nProject: `{result['scope']}`"
    return header + "\n\nNo memory changes need review." if not pending else "\n\n".join([header + f"\n\n{len(pending)} {'change needs' if len(pending) == 1 else 'changes need'} a decision. Ask Codex or Claude: \"sync my memories\"."] + [f"## {'Unavailable memory' if p['kind']=='missing' else 'Change'} from {_provider(p['provider'])}: {_context_label(p['content'])}" + (f"\n\n### Previously synchronized\n\n{p['ancestor']}" if p.get("ancestor") is not None else "") + f"\n\n### {'Last available version' if p['kind'] == 'missing' else 'New version'} from {_provider(p['provider'])}\n\n{p['content']}" + (f"\n\n### Current memory\n\n{canonicals[p['canonical']]['content']}" if p.get("canonical") in canonicals else "") for p in pending])
def status_data(scope):
    scan_store(scope); db = connect(); args = (scope, scope); sources = [r[0] for r in db.execute("SELECT active FROM sources WHERE ? IS NULL OR scope=?", args)]
    owned = lambda s: db.execute("SELECT COUNT(DISTINCT c.id) FROM canonicals c JOIN links l ON l.canonical=c.id JOIN sources x ON x.id=l.source WHERE x.provider='user' AND (? IS NULL OR c.scope=?)", (s,s)).fetchone()[0]
    revisions = db.execute("SELECT COUNT(*) FROM revisions r JOIN sources s ON s.id=r.source WHERE ? IS NULL OR s.scope=?", args).fetchone()[0]; canonicals = db.execute("SELECT COUNT(*) FROM canonicals WHERE ? IS NULL OR scope=?", args).fetchone()[0]; result = dict(scope=scope, sources=len(sources), active=sum(sources), missing=len(sources)-sum(sources), revisions=revisions, canonicals=canonicals, user_owned=owned(scope), pending=len(plan_data(db=db, scope=scope)["pending"]), database=str(_db_path()))
    if scope is None: result["scopes"] = [dict(scope=s,sources=len(a := [r[0] for r in db.execute("SELECT active FROM sources WHERE scope=?", (s,))]),active=sum(a),missing=len(a)-sum(a),revisions=db.execute("SELECT COUNT(*) FROM revisions r JOIN sources x ON x.id=r.source WHERE x.scope=?", (s,)).fetchone()[0],canonicals=db.execute("SELECT COUNT(*) FROM canonicals WHERE scope=?", (s,)).fetchone()[0],user_owned=owned(s),pending=len(plan_data(db=db,scope=s)["pending"])) for s, in db.execute("SELECT scope FROM sources UNION SELECT scope FROM canonicals ORDER BY scope")]
    db.close(); return result
def _snapshot_summary(db): return dict(sources=db.execute("SELECT COUNT(*) FROM sources").fetchone()[0],canonicals=db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0],revisions=sum(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("revisions","canonical_revisions")),evidence=db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],scopes=db.execute("SELECT COUNT(*) FROM (SELECT scope FROM sources UNION SELECT scope FROM canonicals)").fetchone()[0])
def _snapshot(path):
    path = Path(path).expanduser()
    if path.is_symlink() or not path.is_file(): raise ValueError(f"Memory snapshot must be a regular non-symlink file: {path}")
    db, expected = sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True), sqlite3.connect(":memory:"); db.execute("BEGIN"); expected.executescript(SCHEMA); tables = {r[0]:r[1] for r in expected.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}; expected.close()
    try:
        version=db.execute("PRAGMA user_version").fetchone()[0]; base=("sources","revisions","canonicals","canonical_revisions","links","projections")
        if version not in (1,2,3,4,5,6) or db.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone() or any(db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()!=(tables[name],) for name in base): raise ValueError("Invalid or incompatible memory snapshot")
        if version<6: migrated=sqlite3.connect(":memory:"); db.backup(migrated); db.close(); db=migrated; db.executescript(SCHEMA+"DROP TABLE IF EXISTS remote_heads;DROP TABLE IF EXISTS remote_parts;"); db.execute("INSERT OR IGNORE INTO remote_meta VALUES ('ledger_id',?)",(os.urandom(16).hex(),)); db.commit()
        if any(db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() != (sql,) for name,sql in tables.items()): raise ValueError("Invalid or incompatible memory snapshot")
        if db.execute("SELECT 1 FROM links l JOIN sources s ON s.id=l.source JOIN canonicals c ON c.id=l.canonical WHERE s.scope<>c.scope LIMIT 1").fetchone(): raise ValueError("Cross-scope memory link in snapshot")
        return db
    except BaseException: db.close(); raise
def backup_data(target=None):
    source = connect(); path = Path(target).expanduser() if target else _db_path().parent/"backups"/f"state-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.db"
    if path.is_symlink() or path.exists(): source.close(); raise ValueError(f"Memory backup target already exists or is a symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True); tmp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}"); tmp.touch(mode=0o600,exist_ok=False); os.chmod(tmp,0o600); out = sqlite3.connect(tmp)
    try: source.backup(out); summary = _snapshot_summary(out); out.close(); source.close(); os.link(tmp,path); tmp.unlink()
    except BaseException: out.close(); source.close(); tmp.unlink(missing_ok=True); raise
    return dict(status="backed_up",path=str(path.resolve()),bytes=path.stat().st_size,**summary)
def restore_data(source, yes=False):
    source, target = Path(source).expanduser(), _db_path().resolve()
    if source.resolve() == target: raise ValueError("Memory snapshot and live ledger must be different files")
    snapshot = _snapshot(source); summary = _snapshot_summary(snapshot)
    if not yes: snapshot.close(); return dict(status="would_restore",path=str(source.resolve()),**summary)
    rescue = backup_data(); live = connect()
    try: snapshot.backup(live); live.close(); snapshot.close()
    except BaseException: live.close(); snapshot.close(); raise
    return dict(status="restored",path=str(source.resolve()),rescue=rescue["path"],**summary)
def adopt_scope_data(scope, checkout=None, yes=False):
    raw = _raw_scope(checkout)
    if not (repo := _repository(raw)): raise ValueError(f"Checkout has no usable Git repository evidence: {raw}")
    db = connect(); canonicals = db.execute("SELECT COUNT(*) FROM canonicals WHERE scope=?", (scope,)).fetchone()[0]; sources = db.execute("SELECT COUNT(*) FROM sources WHERE scope=?", (scope,)).fetchone()[0]
    if not canonicals and not sources: db.close(); raise ValueError(f"Memory scope does not exist: {scope}")
    if yes: db.execute("INSERT OR REPLACE INTO repository_scopes VALUES (?,?,?,?,?)", (repo["repository"],repo["lineage"],scope,repo["checkout"],datetime.now(timezone.utc).isoformat())); db.commit()
    db.close(); return dict(status="adopted" if yes else "would_adopt",scope=scope,checkout=repo["checkout"],repository=repo["repository"],canonicals=canonicals,sources=sources)
def remote_records(root,user,workspace,kind):
    if kind!="personal": return []
    db=connect(root); stored=[dict(row=json.loads(r["body"]),proof=json.loads(r["proof"]),previous=None,owned=bool(r["owned"])) for r in db.execute("SELECT * FROM remote_semantics ORDER BY workspace,author,entity,revision")]; rows=[dict(r) for r in db.execute("""SELECT c.*,EXISTS(SELECT 1 FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=c.id AND s.provider<>'remote') local_origin,EXISTS(SELECT 1 FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=c.id AND s.provider='remote' AND s.hash=c.hash) remote_exact,EXISTS(SELECT 1 FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=c.id AND (NOT s.active OR s.hash<>l.applied_hash)) pending FROM canonicals c ORDER BY c.id""")]; repositories={r["scope"]:dict(x) for r in rows if (x:=db.execute("SELECT repository,lineage FROM repository_scopes WHERE scope=? ORDER BY checkout LIKE 'remote:%',checkout LIMIT 1",(r["scope"],)).fetchone())}; records,seen=stored.copy(),set()
    for value in rows:
        repo=repositories.get(value["scope"]); scope=repo["repository"] if repo else "global" if value["scope"]=="global" else "scope_"+_hash(value["scope"])[:20]; entity=f"memory:{scope}:{value['id']}"; matching=[r for r in stored if r["proof"]["author_user_id"]==user and r["row"]["id"]==entity]; imported=any(r["row"]["state"]=="active" and r["row"]["data"]["hash"]==value["hash"] and not r["owned"] for r in stored); local=value["local_origin"] or not value["remote_exact"] or not imported
        if not local: continue
        seen.add(entity); row={"v":1,"kind":"memory.canonical","id":entity,"state":"active","data":{"canonical":value["id"],"scope":scope,"repository":repo["repository"] if repo else None,"lineage":repo["lineage"] if repo else None,"hash":value["hash"],"content":value["content"],"updated_at":value["updated_at"]}}
        if len(matching)>1 and value["pending"]: continue
        if not matching or matching[0]["row"]!=row: records.append(dict(row=row,proof=None,previous=[r["proof"] for r in matching] if len(matching)>1 else matching[0]["proof"] if matching else None,owned=True))
    gone={}
    for r in stored: gone.setdefault((r["proof"]["workspace"],r["proof"]["author_user_id"],r["row"]["id"]),[]).append(r)
    records += [dict(row={"v":1,"kind":"memory.canonical","id":entity,"state":"deleted","data":None},proof=None,previous=[r["proof"] for r in values] if len(values)>1 else values[0]["proof"],owned=True) for (workspace,author,entity),values in gone.items() if any(r["owned"] for r in values) and any(r["row"]["state"]=="active" for r in values) and entity not in seen]
    db.close(); return records
def _remote_apply(db,root,workspace,author,entity,p,content,observed_at,conflict=False):
    locator=f"{workspace}/{author}/{entity}"; found=db.execute("SELECT id,scope FROM sources WHERE provider='remote' AND locator=?",(locator,)).fetchone()
    if content is None:
        if not found: return
        sid,target=found["id"],found["scope"]
    else:
        scopes=[r[0] for r in db.execute("SELECT DISTINCT scope FROM repository_scopes WHERE repository=? ORDER BY scope",(p["repository"],))] if p["repository"] else []
        if len(scopes)>1: raise ValueError("Remote repository maps to multiple memory scopes")
        target=scopes[0] if scopes else "global" if p["scope"]=="global" else "remote:"+p["scope"]
        if p["repository"] and not scopes: db.execute("INSERT OR IGNORE INTO repository_scopes VALUES (?,?,?,?,?)",(p["repository"],p["lineage"],target,"remote:"+p["repository"],observed_at))
        sid=found["id"] if found else _hash("\0".join(("remote",target,locator)))[:20]; target=found["scope"] if found else target
    if content is not None:
        digest=p["hash"]; link=db.execute("SELECT l.canonical,l.applied_hash,c.hash FROM links l JOIN canonicals c ON c.id=l.canonical WHERE l.source=?",(sid,)).fetchone(); db.execute("""INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET hash=excluded.hash,content=excluded.content,active=1,observed_at=excluded.observed_at""",(sid,"remote",target,locator,str(_db_path(root)),digest,content,1,observed_at)); db.execute("INSERT OR IGNORE INTO revisions VALUES (?,?,?,?)",(sid,digest,content,observed_at))
        if conflict: return
        blocked=link and db.execute("SELECT 1 FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=? AND s.id<>? AND (NOT s.active OR s.hash<>l.applied_hash) LIMIT 1",(link["canonical"],sid)).fetchone()
        if link and link["hash"]==link["applied_hash"] and not blocked: _canonical(db,link["canonical"],target,content,observed_at); db.execute("UPDATE links SET applied_hash=? WHERE source=?",(digest,sid))
        elif not link and (exact:=db.execute("SELECT id FROM canonicals WHERE scope=? AND hash=? ORDER BY id LIMIT 1",(target,digest)).fetchone()): db.execute("INSERT INTO links VALUES (?,?,?)",(sid,exact["id"],digest))
        elif not link and not db.execute("SELECT 1 FROM canonicals WHERE scope=? LIMIT 1",(target,)).fetchone(): cid="mem_"+_hash(target+"\0"+content)[:16]; _canonical(db,cid,target,content,observed_at); db.execute("INSERT INTO links VALUES (?,?,?)",(sid,cid,digest))
    elif found:
        if conflict: db.execute("DELETE FROM revisions WHERE source=?",(sid,)); db.execute("UPDATE sources SET content='',active=0,observed_at=? WHERE id=?",(observed_at,sid)); return
        link=db.execute("SELECT l.canonical,l.applied_hash,c.hash FROM links l JOIN canonicals c ON c.id=l.canonical WHERE l.source=?",(sid,)).fetchone(); protected=link and (db.execute("SELECT 1 FROM links WHERE canonical=? AND source<>? LIMIT 1",(link["canonical"],sid)).fetchone() or any((path:=Path(r[0])).exists() and (path.is_symlink() or f"canonical:{link['canonical']} " in path.read_text()) for r in db.execute("SELECT target FROM projections")))
        if link and link["hash"]==link["applied_hash"] and not protected: db.execute("DELETE FROM links WHERE source=?",(sid,)); db.execute("DELETE FROM revisions WHERE source=?",(sid,)); db.execute("DELETE FROM sources WHERE id=?",(sid,)); db.execute("DELETE FROM canonical_revisions WHERE canonical=?",(link["canonical"],)); db.execute("DELETE FROM canonicals WHERE id=?",(link["canonical"],))
        elif link: db.execute("DELETE FROM revisions WHERE source=?",(sid,)); db.execute("UPDATE sources SET content='',active=0,observed_at=? WHERE id=?",(observed_at,sid))
        else: db.execute("DELETE FROM revisions WHERE source=?",(sid,)); db.execute("DELETE FROM sources WHERE id=?",(sid,))
def remote_accept(root,row,proof,project=True):
    data=row["data"]; parts=row["id"].split(":",2); fields={"canonical","scope","repository","lineage","hash","content","updated_at"}
    if row["kind"]!="memory.canonical" or len(parts)!=3 or parts[0]!="memory" or not all(parts[1:]) or row["state"]=="active" and (set(data)!=fields or row["id"]!=f"memory:{data['scope']}:{data['canonical']}" or not all(isinstance(data[k],str) and data[k] for k in ("canonical","scope","hash","updated_at")) or not isinstance(data["content"],str) or _hash(data["content"])!=data["hash"] or data["repository"] is not None and not isinstance(data["repository"],str) or data["lineage"] is not None and not isinstance(data["lineage"],str) or data["repository"] is None and data["lineage"] is not None) or row["state"]=="deleted" and data is not None: raise ValueError("Malformed remote memory object")
    db=connect(root); author,entity=proof["author_user_id"],row["id"]
    with contextlib.closing(db),_transaction(db):
        old=db.execute("SELECT owned FROM remote_semantics WHERE workspace=? AND author=? AND entity=? AND revision=?",(proof["workspace"],author,entity,proof["revision"])).fetchone(); owned=bool(old and old["owned"]); db.execute("INSERT OR REPLACE INTO remote_semantics VALUES (?,?,?,?,?,?,?,?)",(proof["workspace"],author,entity,proof["revision"],row["state"],json.dumps(row,sort_keys=True,separators=(",",":")),json.dumps(proof,sort_keys=True,separators=(",",":")),int(owned or not project))); values=[dict(row=json.loads(r["body"]),proof=json.loads(r["proof"])) for r in db.execute("SELECT body,proof FROM remote_semantics WHERE workspace=? AND author=? AND entity=?",(proof["workspace"],author,entity))]; ancestors={a for value in values for a in value["proof"]["ancestors"]}; leaves=[value for value in values if value["proof"]["revision"] not in ancestors]; db.execute("DELETE FROM remote_semantics WHERE workspace=? AND author=? AND entity=? AND revision NOT IN (%s)"%",".join("?"*len(leaves)),(proof["workspace"],author,entity,*(v["proof"]["revision"] for v in leaves)))
        if project and not owned:
            current=(db.execute("SELECT c.hash FROM sources s JOIN links l ON l.source=s.id JOIN canonicals c ON c.id=l.canonical WHERE s.provider='remote' AND s.locator=?",(f"{proof['workspace']}/{author}/{entity}",)).fetchone() or [None])[0]; deleted=next((v for v in leaves if v["row"]["state"]=="deleted"),None); chosen=leaves if len(leaves)==1 else [deleted or next((v for v in leaves if v["row"]["data"]["hash"]!=current),leaves[0])]
            for value in chosen:
                body,p=value["row"],value["proof"]; payload=body["data"]; _remote_apply(db,root,proof["workspace"],p["author_user_id"],entity,payload,payload["content"],payload["updated_at"],len(leaves)>1) if body["state"]=="active" else _remote_apply(db,root,proof["workspace"],p["author_user_id"],entity,{},None,datetime.now(timezone.utc).isoformat(),len(leaves)>1)
    return len(leaves)==1
def remote_token(root):
    db=connect(root); value=db.execute("SELECT value FROM remote_meta WHERE key='ledger_id'").fetchone()[0]; db.close(); return value
def remote_bridge(): return dict(v=2,objects={"memory.canonical"},records=remote_records,accept=remote_accept,token=remote_token)
def _skill_source():
    rel = Path("skills")/"convos"/"SKILL.md"; root = Path(os.environ.get("CONVOS_PROJECT_ROOT", Path.home()/".convos")).expanduser()
    roots = (root, Path(__file__).resolve().parents[4], Path(sysconfig.get_paths().get("data", ""))/"share"/"convos", Path(site.getuserbase())/"share"/"convos")
    return next((p for r in roots if (p := r/rel).exists()), None)
def _hook_valid(command, path):
    parts = shlex.split(command); return len(parts) == 4 and parts[0] == f"CONVOS_MEMORY_DB={path}" and Path(parts[1]).is_file() and os.access(parts[1], os.X_OK) and parts[2:] == ["memory", "runtime-hook"]
def _hook_paths(): return [("claude-code",Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude")).expanduser()/"settings.json"),("codex",Path(os.environ.get("CODEX_HOME", Path.home()/".codex")).expanduser()/"hooks.json")]
def _hook_data(source, path):
    if path.is_symlink() or path.exists() and not path.is_file(): raise ValueError(f"Managed JSON path must be a regular non-symlink file: {path}")
    data = json.loads(path.read_text()) if path.exists() else {}; hooks = data.setdefault("hooks", {}) if isinstance(data, dict) else None
    if not isinstance(hooks, dict) or not isinstance((groups := hooks.get("SessionStart", [])), list) or any(not isinstance(g, dict) or not isinstance(g.get("hooks", []), list) or any(not isinstance(h, dict) or not isinstance(h.get("command", ""), str) for h in g.get("hooks", [])) for g in groups): raise ValueError(f"{source} SessionStart hooks must be a list of hook objects")
    return data, hooks
def _rpc_reader(pipe, output):
    for line in pipe: output.put(json.loads(line))
def _rpc_wait(output, id_, timeout=3):
    deadline = time.monotonic() + timeout
    while (left := deadline-time.monotonic()) > 0:
        try: row = output.get(timeout=left)
        except queue.Empty: break
        if row.get("id") == id_: return row["result"]
    raise TimeoutError("Codex hooks/list timed out")
def _codex_hook_trust(command, cwd):
    if not command or not (binary := shutil.which("codex")): return "missing" if not command else "unavailable"
    process = None
    try:
        process = subprocess.Popen([binary,"app-server","--stdio"], cwd=cwd, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL); output = queue.Queue(); threading.Thread(target=_rpc_reader,args=(process.stdout,output),daemon=True).start()
        process.stdin.write(json.dumps({"id":1,"method":"initialize","params":{"clientInfo":{"name":"convos-memory","title":"convos-memory","version":"0.8.0"}}})+"\n"); process.stdin.flush(); _rpc_wait(output, 1)
        process.stdin.write(json.dumps({"method":"initialized","params":{}})+"\n"+json.dumps({"id":2,"method":"hooks/list","params":{"cwds":[cwd]}})+"\n"); process.stdin.flush(); result = _rpc_wait(output, 2)
        return next((h["trustStatus"] for row in result["data"] for h in row["hooks"] if h["command"] == command), "missing")
    except (OSError, ValueError, KeyError, TypeError, TimeoutError): return "unknown"
    finally:
        if process:
            process.terminate()
            try: process.wait(1)
            except subprocess.TimeoutExpired: process.kill()
def _health_data():
    scope, path = _scope(), _db_path().resolve(); claude = Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home()/".claude")).expanduser(); codex = Path(os.environ.get("CODEX_HOME", Path.home()/".codex")).expanduser()
    if not (skill := _skill_source()): raise ValueError("bundled agent skill is missing")
    commands = [(source,h["command"]) for source,config in _hook_paths() for g in _hook_data(source, config)[1].get("SessionStart", []) for h in g.get("hooks", []) if _hook_valid(h.get("command", ""), path)]; hooks = len(commands); codex_command = next((c for s,c in commands if s == "codex"), None); trust = _codex_hook_trust(codex_command, scope); expected = _hash(skill.read_text()); skills = sum(p.exists() and _hash(p.read_text()) == expected for root in (codex, claude) if (p := root/"skills"/"convos"/"SKILL.md"))
    if not path.exists(): return dict(scope=scope,ledger=False,sources=0,active=0,canonicals=0,pending=0,hooks=hooks,trust=trust,skills=skills,ready=False,repairs=["convos memory enable"])
    scan_store(scope); db = sqlite3.connect(path.as_uri()+"?mode=ro", uri=True); sources, active = db.execute("SELECT COUNT(*),COALESCE(SUM(active),0) FROM sources WHERE scope=?", (scope,)).fetchone(); canonicals = db.execute("SELECT COUNT(*) FROM canonicals WHERE scope=?", (scope,)).fetchone()[0]; pending = db.execute("""SELECT COUNT(*) FROM sources s LEFT JOIN links l ON l.source=s.id WHERE ((s.active AND (l.source IS NULL OR l.applied_hash<>s.hash)) OR (NOT s.active AND l.source IS NOT NULL)) AND s.scope=?""", (scope,)).fetchone()[0]; db.close()
    trusted = trust in ("trusted","managed"); trust_repair = "install Codex to verify memory delivery" if trust == "unavailable" else "review Codex hooks with /hooks" if trust in ("untrusted","modified") else "verify Codex hook loading with /hooks"; ready = hooks == 2 and skills == 2 and trusted and not pending; repairs = ([] if hooks == 2 and skills == 2 else ["convos memory enable"]) + ([] if trusted or not codex_command else [trust_repair]) + ([] if not pending else ['ask Codex or Claude: "sync my memories"'])
    return dict(scope=scope,ledger=True,sources=sources,active=active,canonicals=canonicals,pending=pending,hooks=hooks,trust=trust,skills=skills,ready=ready,repairs=repairs)
def doctor_status():
    try:
        r=_health_data(); detail="ledger=missing" if not r["ledger"] else f"sources={r['sources']}, active={r['active']}, canonicals={r['canonicals']}, pending={r['pending']}"; return "\n".join([f"memory: {'ready' if r['ready'] else 'attention'}, scope={r['scope']}, {detail}, hooks={r['hooks']}/2, codex_trust={r['trust']}, skills={r['skills']}/2",*[f"repair: {x}" for x in r["repairs"]]])
    except Exception as e: return f"memory: unavailable ({e})"
def _render_home(r):
    if not r["ledger"]: return f"Memory is not enabled for `{r['scope']}`.\nRun `convos memory enable`."
    count=f"{r['canonicals']} {'memory' if r['canonicals']==1 else 'memories'} available"; first=f"Memory {'is ready' if r['ready'] else 'needs attention'} for `{r['scope']}`: {count}."; next_=lambda x:f"Run `{x}`." if x.startswith("convos ") else x[0].upper()+x[1:]+"."; return first + ("\nAutomatic delivery is active in Codex and Claude." if r["ready"] else "\n" + "\n".join(f"Next: {next_(x)}" for x in r["repairs"]))

def _emit(value): typer.echo(json.dumps(value, sort_keys=True))
def _safe(fn):
    try: return fn()
    except (ValueError, OSError, sqlite3.Error) as e: typer.echo(str(e), err=True); raise typer.Exit(1)
def _evidence(db, canonical, digest):
    rows = [dict(r) for r in db.execute("SELECT message,conversation,source,title,role,created_at,content_hash FROM evidence WHERE canonical=? AND hash=? ORDER BY created_at,message",(canonical,digest))]
    if not rows: return rows
    archive = None
    try:
        from ai_convos import cli
        if not (archive:=cli.get_db(True)): raise ValueError
        live={mid:_hash(body) for mid,body in archive.execute("SELECT id,COALESCE(content,'') FROM messages WHERE id IN (SELECT UNNEST(?)) AND json_extract_string(metadata,'$.history_of') IS NULL",[ [r["message"] for r in rows] ]).fetchall()}
        return [{**r,"status":"verified" if live.get(r["message"]) == r["content_hash"] else "changed" if r["message"] in live else "missing","read":f"convos read {r['conversation']} --around {r['message']}"} for r in rows]
    except Exception: return [{**r,"status":"unavailable","read":f"convos read {r['conversation']} --around {r['message']}"} for r in rows]
    finally:
        if archive: archive.close()
def audit_data(scope=None, message=None):
    db=connect(); matches=db.execute("SELECT DISTINCT e.message FROM evidence e JOIN canonicals c ON c.id=e.canonical WHERE (? IS NULL OR c.scope=?) AND substr(e.message,1,length(?))=? LIMIT 2",(scope,scope,message,message)).fetchall() if message else []
    if message and len(matches)!=1: db.close(); raise ValueError("memory evidence message is missing or ambiguous")
    message=matches[0][0] if message else None; groups=db.execute("SELECT DISTINCT e.canonical,e.hash,c.scope,e.hash=c.hash current FROM evidence e JOIN canonicals c ON c.id=e.canonical WHERE (? IS NULL OR c.scope=?) AND (? IS NULL OR e.message=?) ORDER BY c.scope,e.canonical,e.hash",(scope,scope,message,message)).fetchall()
    rows=[{**e,"canonical":g["canonical"],"hash":g["hash"],"scope":g["scope"],"current":bool(g["current"])} for g in groups for e in _evidence(db,g["canonical"],g["hash"]) if message is None or e["message"]==message]; db.close(); counts={s:sum(r["status"]==s for r in rows) for s in ("verified","changed","missing","unavailable")}
    return dict(scope=scope,message=message,evidence=len(rows),**counts,records=rows)
def _render_audit(result):
    rows=result["records"] if result["message"] else [r for r in result["records"] if r["status"]!="verified"]; summary=f"# Memory evidence audit\n\nScope: `{result['scope'] or 'all scopes'}`  \nEvidence: {result['evidence']}  \nVerified: {result['verified']}  \nChanged: {result['changed']}  \nMissing: {result['missing']}  \nUnavailable: {result['unavailable']}"; return summary + ("\n\nNo evidence links found." if not result["evidence"] else "\n\nAll evidence links verified." if not rows else "\n\n" + "\n".join(f"- [{r['status']}] {'current' if r['current'] else 'historical'} `{r['canonical']}` @ `{r['scope']}`: {r['source']} {r['title'] or 'Untitled'}; `{r['read']}`" for r in rows))
def _current(scope, query=None):
    db = connect(); rows = [{**dict(r),"origins":[dict(o) for o in db.execute("SELECT s.id source,s.provider,s.locator,s.hash source_hash,l.applied_hash,s.active FROM links l JOIN sources s ON s.id=l.source WHERE l.canonical=? ORDER BY s.provider,s.locator,s.id", (r["id"],))],"evidence":_evidence(db,r["id"],r["hash"])} for r in db.execute("SELECT * FROM canonicals c WHERE c.scope=? AND (? IS NULL OR c.id=? OR (NOT EXISTS(SELECT 1 FROM canonicals x WHERE x.scope=c.scope AND x.id=?) AND INSTR(LOWER(c.content),LOWER(?))>0)) ORDER BY c.id", (scope, query, query, query, query))]; db.close(); return rows
def _render_evidence(rows): return "" if not rows else "\n\nEvidence:\n" + "\n".join(f"- [{r['status']}] {r['source']} {r['title'] or 'Untitled'}: `{r['read']}`" for r in rows)
def _render_context(rows): return "\n\n".join(["# Synchronized memory context"] + [f"## {r['id']}\n\nScope: `{r['scope']}`  \nOrigins: {', '.join(sorted({o['provider'] for o in r['origins']})) or 'canonical'}{_render_evidence(r['evidence'])}\n\n{r['content']}" for r in rows])
def _provider(provider): return {"user":"saved by you","codex":"Codex","claude-code":"Claude","remote":"another device"}.get(provider,provider)
def _render_current(rows): return "\n\n".join(["# Current memories"] + [f"## {_context_label(r['content'])}\n\nID: `{r['id']}`  \nProject: `{r['scope']}`\n\nAvailable from:\n" + "\n".join(f"- {_provider(o['provider'])}" + (" (unavailable)" if not o["active"] else " (changed since last sync)" if o["source_hash"] != o["applied_hash"] else "") for o in r["origins"]) + _render_evidence(r["evidence"]) + f"\n\n{r['content']}" for r in rows])
def _render_history(result):
    details = f"Provider: {result['provider']}  \nLocator: `{result['locator']}`  \nActive: {'yes' if result['active'] else 'no'}  \n" if result["kind"] == "source" else ""
    return "\n\n".join([f"# {'Imported memory' if result['kind']=='source' else 'Memory'} history\n\nID: `{result['id']}`  \nProject: `{result['scope']}`  \n{details}Current hash: `{result['hash']}`  \nRevisions: {len(result['revisions'])}"] + [f"## {r['observed_at']}\n\nHash: `{r['hash']}`{_render_evidence(r.get('evidence',[]))}\n\n{r['content']}" for r in result["revisions"]])
def _context_label(content):
    lines = content.splitlines(); return (next((line.split(":",1)[1].strip().strip("\"'") for line in lines[:12] if line.startswith("name:")), None) or next((line.lstrip("# ").strip() for line in lines if line.startswith("#")), None) or next((line.strip() for line in lines if line.strip()), "memory"))[:96]
def _select_canonical(db, scope, selector):
    rows=[dict(r) for r in db.execute("SELECT * FROM canonicals WHERE scope=? ORDER BY id",(scope,))]; key=selector.strip().casefold(); tiers=([r for r in rows if r["id"]==selector],[r for r in rows if key and r["id"].startswith(selector)],[r for r in rows if r["content"].strip().casefold()==key],[r for r in rows if _context_label(r["content"]).casefold()==key],[r for r in rows if key and key in r["content"].casefold()]); matches=next((m for m in tiers if m),[])
    if len(matches)!=1: raise ValueError(f"memory selector is {'missing' if not matches else 'ambiguous'} in {scope}; run convos memory current")
    return matches[0]
def _render_index(rows, scope, size):
    shown = rows[:64]; command = f"convos memory context --scope {shlex.quote(scope)}"
    return f"# Synchronized memory index\n\nFull scoped context is {size} characters across {len(rows)} {'memory' if len(rows) == 1 else 'memories'}. This bounded index avoids loading every body. Run `{command} --query ID_OR_TERM` for one exact ID or matching full records, or `{command}` for all records.\n\n" + "\n".join(f"- `{r['id']}` [{', '.join(sorted({o['provider'] for o in r['origins']})) or 'canonical'}] {_context_label(r['content'])} ({len(r['content'])} characters)" for r in shown) + (f"\n- {len(rows)-len(shown)} additional memories omitted from the index." if len(rows)>len(shown) else "")
def context_data(scope, query=None, pending=0, limit=None, index=False):
    body = _render_context(rows) if (rows := _current(scope, query)) else f"# No synchronized memory match\n\nNo canonical in scope `{scope}` matched `{query}`." if query is not None else ""
    if index or limit and len(body) > limit: body = _render_index(rows, scope, len(body))
    notice = f"# Memory synchronization notice\n\n{pending} source revision{'s' if pending != 1 else ''} await{'s' if pending == 1 else ''} reconciliation for this scope. Treat canonical context as potentially stale and run `convos memory sync --project {shlex.quote(scope)} --json` from an agent."
    return body + (("\n\n" if body else "") + notice if pending else "")
def live_context(scope=None, query=None, payload=None, limit=None, index=False):
    scope = scope or runtime_scope(payload); scan_store(scope); return context_data(scope, query, reconcile_data(scope, refresh=False)["remaining"], limit, index)
def runtime_scope(payload):
    cwd, key, db = payload["cwd"], Path(payload.get("transcript_path") or "").parent.name, connect()
    row = db.execute("""SELECT m.scope FROM (SELECT c.scope,s.provider,s.locator FROM canonicals c LEFT JOIN links l ON l.canonical=c.id LEFT JOIN sources s ON s.id=l.source
        UNION SELECT scope,provider,locator FROM sources) m WHERE m.scope=? OR ? LIKE m.scope||'/%' OR (m.provider='claude-code' AND m.locator LIKE ?)
        GROUP BY m.scope ORDER BY m.scope=? DESC,LENGTH(m.scope) DESC LIMIT 1""", (cwd, cwd, key+"/%", cwd)).fetchone(); db.close(); return row[0] if row else _scope(cwd)
def runtime_context(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("hook_event_name"), str): raise ValueError("Malformed agent hook payload")
    if payload["hook_event_name"] != "SessionStart": return {}
    if not isinstance(payload.get("cwd"), str) or not payload["cwd"] or payload.get("transcript_path") is not None and not isinstance(payload["transcript_path"], str): raise ValueError("Malformed agent SessionStart payload")
    return {} if not (content := live_context(payload=payload, limit=HOOK_CONTEXT_CHARS)) else dict(hookSpecificOutput=dict(hookEventName="SessionStart", additionalContext=content))
def context_hook_config(remove=False, status=False):
    plans = []
    for source,path in _hook_paths():
        if remove and not path.exists(): plans.append((source,path,None,0)); continue
        data, hooks = _hook_data(source, path)
        if not status:
            for group in hooks.get("SessionStart", []): group["hooks"] = [h for h in group.get("hooks", []) if not h.get("command", "").endswith(" memory runtime-hook")]
            hooks["SessionStart"] = [g for g in hooks.get("SessionStart", []) if g.get("hooks")]
            if not hooks["SessionStart"]: hooks.pop("SessionStart")
            if not remove:
                binary = Path(sys.executable).with_name("convos"); command = f"CONVOS_MEMORY_DB={shlex.quote(str(_db_path().resolve()))} {shlex.quote(str(binary))} memory runtime-hook"
                hooks.setdefault("SessionStart", []).append(dict(hooks=[dict(type="command", command=command, timeout=5, statusMessage="Loading synchronized memory")]))
        plans.append((source,path,data,sum(h.get("command", "").endswith(" memory runtime-hook") for g in hooks.get("SessionStart", []) for h in g.get("hooks", []))))
    if not status: [atomic_json(path, data) for _,path,data,_ in plans if data is not None]
    return [(source,path,count) for source,path,_,count in plans]
def enable_data(scope=None, all_=False, install=True):
    context_hook_config(status=True); sync = sync_all_data() if all_ else sync_data(scope); skills = 2
    if install:
        binary = Path(sys.executable).with_name("convos"); result = subprocess.run([binary, "install-skills"], text=True, capture_output=True); skills = sum(line.startswith("Installed ") for line in result.stdout.splitlines())
        if result.returncode or skills != 2: raise ValueError(result.stderr.strip() or f"Agent skill installation incomplete: expected 2, installed {skills}")
    configs = context_hook_config(); codex = next(r[1] for r in configs if r[0] == "codex"); command = next(h["command"] for g in _hook_data("codex",codex)[1]["SessionStart"] for h in g["hooks"] if h["command"].endswith(" memory runtime-hook")); return dict(**sync, skills=skills, hooks=sum(r[2] for r in configs), codex_trust=_codex_hook_trust(command, str(Path.cwd())), settings=[str(r[1]) for r in configs])
def _enable_message(result, checked): return f"Automatic memory is on for Codex and Claude; checked {checked}." + ("" if not result["remaining"] else f" {result['remaining']} {'change needs' if result['remaining'] == 1 else 'changes need'} a decision; ask Codex or Claude: \"sync my memories\".") + (" Codex delivery is not yet verified; review new or changed hooks with /hooks." if result["codex_trust"] not in ("trusted","managed") else "")
def initialize():
    try: result = enable_data(install=False); return _enable_message(result, f"project `{result['scope']}`")
    except Exception as e: return f"Memory setup needs attention: {e}. Run `convos memory enable` after fixing it."
def _projection_path(target, scope):
    root = _claude_root().resolve()
    if target is None:
        targets = {p.name for p in root.iterdir() if p.is_dir() and _claude_scope(p, scope) == scope}
        if len(targets) != 1: raise ValueError("Claude projection target is missing or ambiguous for scope")
        target = next(iter(targets))
    project = (root/target).resolve()
    if project.parent != root or not project.is_dir(): raise ValueError("Claude projection target must be an existing direct child of the projects root")
    if _claude_scope(project, scope) != scope: raise ValueError("Claude projection target scope does not match requested scope")
    memory_dir, path = project/"memory", project/"memory"/"convos-synced.md"
    if memory_dir.is_symlink() or path.is_symlink(): raise ValueError("Claude projection path cannot contain symlinks")
    return path
def projection_data(target, scope):
    scan_store(scope); path = _projection_path(target, scope)
    if pending := plan_data(scope=scope)["pending"]: raise ValueError(f"{len(pending)} pending source revision{'s' if len(pending) != 1 else ''}; reconcile scope before projection")
    rows = _current(scope)
    if not rows: raise ValueError(f"No canonical memories for scope {scope}")
    content = "\n\n".join(["<!-- convos-memory projection:v1 -->\n# Synchronized memories"] + [f"<!-- canonical:{r['id']} revision:{r['hash']} -->\n## {r['id']}\n\nScope: `{r['scope']}`\n\n{r['content']}" for r in rows]) + "\n"
    digest, existing = _hash(content), _hash(path.read_text()) if path.exists() else None; db = connect(); known = db.execute("SELECT hash FROM projections WHERE provider='claude-code' AND target=?", (str(path),)).fetchone(); db.close()
    status = "create" if existing is None else "unchanged" if existing == digest else "update" if known and existing == known[0] else "drift"
    return dict(provider="claude-code", target=str(path), scope=scope, hash=digest, status=status, content=content)
def write_projection(target, scope):
    result = projection_data(target, scope)
    if result["status"] == "drift": raise ValueError(f"Projection drift at {result['target']}; refusing to overwrite")
    path = Path(result["target"]); path.parent.mkdir(parents=True, exist_ok=True)
    if result["status"] != "unchanged":
        tmp = path.with_name(f".{path.name}.{os.getpid()}"); tmp.touch(mode=0o600, exist_ok=False); os.chmod(tmp, 0o600); tmp.write_text(result["content"]); os.replace(tmp, path)
    db = connect()
    with db: db.execute("INSERT INTO projections VALUES ('claude-code',?,?,?) ON CONFLICT(provider,target) DO UPDATE SET hash=excluded.hash,updated_at=excluded.updated_at", (str(path), result["hash"], datetime.now(timezone.utc).isoformat()))
    db.close(); return {k:v for k,v in result.items() if k != "content"}
def remove_projection(target, scope):
    path = _projection_path(target, scope); db = connect(); known = db.execute("SELECT hash FROM projections WHERE provider='claude-code' AND target=?", (str(path),)).fetchone(); existing = _hash(path.read_text()) if path.exists() else None
    if existing is not None and not known: db.close(); raise ValueError(f"Projection at {path} is not owned; refusing to remove")
    if existing is not None and existing != known[0]: db.close(); raise ValueError(f"Projection drift at {path}; refusing to remove")
    if path.exists(): path.unlink()
    with db: db.execute("DELETE FROM projections WHERE provider='claude-code' AND target=?", (str(path),))
    db.close(); return dict(provider="claude-code", target=str(path), scope=scope, status="removed" if existing else "absent")
@memory.command(hidden=True)
def scan(): """Record exact provider revisions and mark disappeared sources."""; _emit(_safe(scan_store))
@memory.command()
def status(scope: Optional[str] = typer.Option(None,"--project"), all_: bool = typer.Option(False, "--all"), json_output: bool = typer.Option(False, "--json")):
    """Show current-project status by default, or global status with --all."""
    result = _safe(lambda: status_data(None if all_ else _scope(scope)))
    if json_output: _emit(result); return
    summary=lambda r:f"{r['canonicals']} {'memory' if r['canonicals']==1 else 'memories'}, {r['user_owned']} remembered, {r['pending']} {'change needs' if r['pending']==1 else 'changes need'} review"+(f", {r['missing']} {'origin is' if r['missing']==1 else 'origins are'} unavailable" if r["missing"] else ""); label = result["scope"] or "all projects"; state = "ready" if not result["pending"] else "needs attention"
    typer.echo(f"Memory {state} for {label}: {summary(result)}.")
    for row in result.get("scopes", []): typer.echo(f"- {'ready' if not row['pending'] else 'needs attention'} {row['scope']}: {summary(row)}.")
@memory.command(help="Check whether remembered conversation evidence is still available.")
def audit(scope: Optional[str] = typer.Option(None,"--project"), all_: bool = typer.Option(False,"--all"), message: Optional[str] = typer.Option(None,"--message",help="Find the memory revisions supported by one exact archived message."), json_output: bool = typer.Option(False,"--json")):
    if all_ and scope is not None: typer.echo("--all cannot be combined with --project",err=True); raise typer.Exit(1)
    result=_safe(lambda:audit_data(None if all_ else _scope(scope),message)); _emit(result) if json_output else typer.echo(_render_audit(result))
@memory.command()
def sync(scope: Optional[str] = typer.Option(None,"--project"), all_: bool = typer.Option(False, "--all"), json_output: bool = typer.Option(False, "--json")):
    """Bring safe memory changes up to date and leave conflicts for an agent."""
    if all_ and scope is not None: typer.echo("--all cannot be combined with --project", err=True); raise typer.Exit(1)
    result = _safe(sync_all_data if all_ else lambda: sync_data(scope))
    if json_output: _emit(result)
    else:
        automatic, remaining = result["automatic"], result["remaining"]; updated = "nothing changed" if not automatic else f"updated {automatic} {'memory' if automatic == 1 else 'memories'}"
        if not all_: typer.echo(f"Memory {'is up to date' if not remaining else 'checked'} for {result['scope']}: {updated}." + ("" if not remaining else f" {remaining} {'change needs' if remaining == 1 else 'changes need'} a decision; ask Codex or Claude: \"sync my memories\"."))
        else:
            attention = sum(bool(r["remaining"]) for r in result["scopes"]); typer.echo(f"Memory {'is up to date' if not remaining else 'checked'} across {len(result['scopes'])} projects: {updated}." + ("" if not remaining else f" {remaining} {'change needs' if remaining == 1 else 'changes need'} a decision in {attention} {'project' if attention == 1 else 'projects'}."))
            [typer.echo(f"- {r['scope']}: {r['remaining']} {'change needs' if r['remaining']==1 else 'changes need'} review.") for r in result["scopes"] if r["remaining"]]
@memory.command(help="Read memory changes that still need a decision.")
def review(scope: Optional[str] = typer.Option(None,"--project")): typer.echo(_safe(lambda: review_data(scope)))
@memory.command(help="Create or revise a memory you own.")
def remember(content: str, scope: Optional[str] = typer.Option(None,"--project"), canonical: Optional[str] = typer.Option(None, "--replace", help="Revise the uniquely matching user-owned memory by ID, title, or text."), from_: Optional[list[str]] = typer.Option(None, "--from", help="Attach exact archived message evidence; repeatable."), json_output: bool = typer.Option(False, "--json", help="Emit the structured mutation result.")):
    result = _safe(lambda: remember_data(sys.stdin.read() if content == "-" else content, scope, canonical, from_ or ())); _emit(result) if json_output else typer.echo(f"{'Memory unchanged' if result['status'] == 'unchanged' else result['status'].title()+' memory'} for `{result['scope']}`" + (f" with {result['evidence']} evidence link." if result["evidence"] == 1 else f" with {result['evidence']} evidence links." if result["evidence"] else "."))
@memory.command(help="Delete a memory created with remember, including its history.")
def forget(memory: str = typer.Argument(...,metavar="MEMORY",help="Unique memory ID, title, or text."), scope: Optional[str] = typer.Option(None,"--project"), dry_run: bool = typer.Option(False, "--dry-run"), json_output: bool = typer.Option(False, "--json", help="Emit the structured purge result.")):
    result = _safe(lambda: forget_data(memory, scope, dry_run)); _emit(result) if json_output else typer.echo(f"{'Would forget' if dry_run else 'Forgot'} `{result['id']}` from `{result['scope']}`; {'would purge' if dry_run else 'purged'} {result['revisions']} stored revision record{'s' if result['revisions'] != 1 else ''} and {result['evidence']} evidence link{'s' if result['evidence'] != 1 else ''}." + (" Run again without --dry-run to confirm." if dry_run else ""))
@memory.command(help="Save a private backup of all memory and history.")
def backup(target: Optional[str] = typer.Argument(None), json_output: bool = typer.Option(False, "--json", help="Emit structured backup details.")):
    result = _safe(lambda: backup_data(target)); _emit(result) if json_output else typer.echo(f"Memory backed up to `{result['path']}`: {result['canonicals']} {'memory' if result['canonicals']==1 else 'memories'} and {result['revisions']} revisions across {result['scopes']} {'project' if result['scopes']==1 else 'projects'} ({result['bytes']} bytes).")
@memory.command(help="Preview or restore a complete memory backup.")
def restore(source: str, yes: bool = typer.Option(False, "--yes", help="Restore after preserving the current memory."), json_output: bool = typer.Option(False, "--json", help="Emit structured backup details.")):
    result = _safe(lambda: restore_data(source, yes)); _emit(result) if json_output else typer.echo((f"Would restore `{result['path']}`: {result['canonicals']} {'memory' if result['canonicals']==1 else 'memories'} and {result['revisions']} revisions across {result['scopes']} {'project' if result['scopes']==1 else 'projects'}. Run again with --yes to confirm." if not yes else f"Memory restored from `{result['path']}`; the previous memory was preserved at `{result['rescue']}`."))
@memory.command("adopt-scope",hidden=True,help="Preview or bind one checkout to an existing repository-memory scope.")
def adopt_scope(scope: str, checkout: Optional[str] = typer.Option(None), yes: bool = typer.Option(False,"--yes"), json_output: bool = typer.Option(False,"--json")):
    result = _safe(lambda: adopt_scope_data(scope,checkout,yes)); _emit(result) if json_output else typer.echo((f"Would bind `{result['checkout']}` to `{result['scope']}` ({result['canonicals']} canonicals, {result['sources']} sources). Run again with --yes to confirm." if not yes else f"Checkout `{result['checkout']}` now uses memory scope `{result['scope']}`."))
@memory.command()
def enable(scope: Optional[str] = typer.Option(None,"--project"), all_: bool = typer.Option(False, "--all", help="Check every discovered project instead of only the selected project.")):
    """Turn on automatic memory for Codex and Claude."""
    if all_ and scope is not None: typer.echo("--all cannot be combined with --project", err=True); raise typer.Exit(1)
    result = _safe(lambda: enable_data(scope, all_)); checked = f"{len(result['scopes'])} discovered {'project' if len(result['scopes']) == 1 else 'projects'}" if all_ else f"project `{result['scope']}`"
    typer.echo(_enable_message(result, checked))
    if all_: [typer.echo(f"- {r['scope']}: {r['remaining']} {'change needs' if r['remaining']==1 else 'changes need'} review.") for r in result["scopes"] if r["remaining"]]
@memory.command()
def disable(projection: bool = typer.Option(False, "--remove-projection",hidden=True), target: Optional[str] = typer.Option(None, "--target",hidden=True), scope: Optional[str] = typer.Option(None,hidden=True)):
    """Turn off automatic memory while keeping saved history."""
    removed, configs = _safe(lambda: (context_hook_config(status=True), remove_projection(target, _scope(scope)) if projection else None, context_hook_config(True))[1:])
    typer.echo("Automatic memory is off for Codex and Claude. Saved memory and history were kept." + (f" The generated Claude copy is {removed['status']}." if removed else ""))
@memory.command(hidden=True)
def plan(content: bool = typer.Option(False, "--content", help="Include current source content for agent resolution."), scope: Optional[str] = typer.Option(None)): """Create a deterministic synchronization plan."""; _emit(_safe(lambda: (scan_store(s := _scope(scope)), plan_data(content, scope=s))[1]))
@memory.command(hidden=True)
def apply(resolutions: str, dry_run: bool = typer.Option(False, "--dry-run")):
    """Apply an agent resolution JSON document after rechecking every source hash."""
    _emit(_safe(lambda: apply_data(json.loads(sys.stdin.read() if resolutions == "-" else Path(resolutions).read_text()), dry_run)))
@memory.command(hidden=True)
def reconcile(scope: Optional[str] = typer.Option(None), dry_run: bool = typer.Option(False, "--dry-run")):
    """Apply safe same-scope updates without semantic decisions."""
    _emit(_safe(lambda: reconcile_data(_scope(scope), dry_run)))
@memory.command(help="List current synchronized memories for one project.")
def current(scope: Optional[str] = typer.Option(None,"--project"), query: Optional[str] = typer.Option(None), owned: bool = typer.Option(False, "--owned", help="Return only memories explicitly saved with remember."), json_output: bool = typer.Option(False, "--json", help="Emit structured memory and origin records.")):
    scope = _safe(lambda: _scope(scope)); rows = _safe(lambda: (sync_data(scope), [r for r in _current(scope, query) if not owned or any(o["provider"] == "user" for o in r["origins"])])[1])
    _emit(rows) if json_output else typer.echo(_render_current(rows) if rows else f"No synchronized memories for `{scope}`.")
@memory.command(help="Render agent-ready synchronized context.", hidden=True)
def context(scope: Optional[str] = typer.Option(None), query: Optional[str] = typer.Option(None), index: bool = typer.Option(False, "--index", help="List bounded canonical IDs and titles instead of bodies.")):
    typer.echo(_safe(lambda: live_context(_scope(scope), query, index=index)))
@memory.command("runtime-hook", hidden=True)
def runtime_hook():
    _emit(_safe(lambda: runtime_context(json.load(sys.stdin))))
@memory.command("install-hook", help="Install, inspect, or remove agent context injection.", hidden=True)
def install_hook(remove: bool = typer.Option(False, "--remove"), status: bool = typer.Option(False, "--status")):
    try: configs = context_hook_config(remove, status)
    except (ValueError, OSError, AttributeError, TypeError) as e: typer.echo(f"Invalid agent hook settings: {e}", err=True); raise typer.Exit(1)
    for source,path,count in configs:
        trust = _codex_hook_trust(next((h["command"] for g in _hook_data(source,path)[1].get("SessionStart", []) for h in g.get("hooks", []) if h["command"].endswith(" memory runtime-hook")), None), _scope()) if status and source == "codex" else None
        typer.echo(f"{source}: {count} memory hook{'s' if count != 1 else ''}{' installed' if not status and not remove else ''}" + (f", trust={trust}" if trust else "") + f" ({path})")
    if not status and not remove: typer.echo("Codex requires one-time review of new or changed hooks through /hooks.")
@memory.command(help="Preview, write, or safely remove a namespaced Claude Code projection.", hidden=True)
def project(target: Optional[str] = typer.Option(None, "--target"), scope: Optional[str] = typer.Option(None), write: bool = typer.Option(False, "--write"), remove: bool = typer.Option(False, "--remove"), content: bool = typer.Option(False, "--content", help="Include rendered Markdown in a preview.")):
    if write and remove: typer.echo("--write and --remove are mutually exclusive", err=True); raise typer.Exit(1)
    scope = _safe(lambda: _scope(scope)); result = _safe(lambda: remove_projection(target, scope) if remove else write_projection(target, scope) if write else projection_data(target, scope)); _emit(result if content else {k:v for k,v in result.items() if k != "content"})
@memory.command(help="Show a memory's exact revision history.")
def history(memory: str = typer.Argument(...,metavar="MEMORY",help="Source ID or unique memory ID, title, or text."), scope: Optional[str] = typer.Option(None,"--project"), json_output: bool = typer.Option(False, "--json", help="Emit structured identity and revision records.")):
    db = _safe(connect); sources, canonicals = _safe(lambda: (db.execute("SELECT id,provider,scope,locator,hash,active,observed_at FROM sources WHERE substr(id,1,length(?))=? LIMIT 2", (memory,memory)).fetchall(), db.execute("SELECT id,scope,hash,updated_at FROM canonicals WHERE substr(id,1,length(?))=? LIMIT 2", (memory,memory)).fetchall()))
    if not sources and not canonicals: canonicals = [_safe(lambda:_select_canonical(db,_scope(scope),memory))]
    if len(sources) + len(canonicals) != 1: db.close(); typer.echo("memory id is missing or ambiguous", err=True); raise typer.Exit(1)
    record, kind, table, column = (dict(sources[0]), "source", "revisions", "source") if sources else (dict(canonicals[0]), "canonical", "canonical_revisions", "canonical")
    rows = _safe(lambda: [{**dict(r),**({"evidence":_evidence(db,record["id"],r["hash"])} if kind == "canonical" else {})} for r in db.execute(f"SELECT hash,content,observed_at FROM {table} WHERE {column}=? ORDER BY observed_at", (record["id"],))]); db.close(); result = dict(kind=kind, **record, revisions=rows); _emit(result) if json_output else typer.echo(_render_history(result))
@memory.callback()
def home(ctx: typer.Context):
    """Show current-project memory and delivery health when no command is supplied."""
    if ctx.invoked_subcommand is None:
        try: typer.echo(_render_home(_health_data()))
        except Exception as e: typer.echo(f"Memory health is unavailable: {e}",err=True); raise typer.Exit(1)
def register(app: typer.Typer): app.add_typer(memory, name="memory")
