"""Local secret scanning and mandatory safe team projections."""
import contextlib, hashlib, json, os, re, sqlite3
from datetime import datetime, timezone
from pathlib import Path

import typer

PATTERNS=[
    ("private_key",re.compile(r"-----BEGIN ((?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY)-----.*?-----END \1-----",re.S),0),
    ("anthropic_key",re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,200}\b"),0),
    ("openai_key",re.compile(r"\bsk-(?!ant-)(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,200}\b"),0),
    ("github_token",re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,255}|github_pat_[A-Za-z0-9_]{20,255})\b"),0),
    ("gitlab_token",re.compile(r"\bglpat-[A-Za-z0-9_-]{20,200}\b"),0),
    ("aws_access_key",re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),0),
    ("google_api_key",re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),0),
    ("slack_token",re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,200}\b"),0),
    ("stripe_key",re.compile(r"\b(?:sk|rk)_live_[A-Za-z0-9]{16,200}\b"),0),
    ("pypi_token",re.compile(r"\bpypi-[A-Za-z0-9_-]{40,300}\b"),0),
    ("npm_token",re.compile(r"\bnpm_[A-Za-z0-9]{36,200}\b"),0),
    ("jwt",re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),0),
    ("authorization",re.compile(r"(?i)\b(?:authorization\s*:\s*(?:bearer|basic)|bearer)\s+([A-Za-z0-9._~+/-]{12,}={0,2})"),1),
    ("credential_url",re.compile(r"\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^@\s/]+@",re.I),0),
    ("assigned_secret",re.compile(r"""(?i)\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|session[_-]?token|client[_-]?secret|aws[_-]?secret[_-]?access[_-]?key|secret[_-]?access[_-]?key|password|passwd)\s*[:=]\s*["']?([A-Za-z0-9/+_=-]{8,})"""),1),
]
FIELDS={"conversations":("title","metadata"),"messages":("content","thinking","metadata"),"tool_calls":("input","output"),"attachments":("filename","url"),"artifacts":("title","content"),"file_edits":("file_path","content","old_content")}
PREFILTER=r"-----BEGIN|sk-(?:ant-|proj-|svcacct-)?|gh[pousr]_|github_pat_|glpat-|(?:AKIA|ASIA)[A-Z0-9]|AIza|xox[baprs]-|(?:sk|rk)_live_|pypi-|npm_|eyJ|authorization\s*:|bearer\s+|://[^/@\s]+:[^/@\s]+@|(?:api[_-]?key|access[_-]?token|auth[_-]?token|session[_-]?token|client[_-]?secret|aws[_-]?secret[_-]?access[_-]?key|secret[_-]?access[_-]?key|password|passwd)\s*[:=]"
SCHEMA="""CREATE TABLE IF NOT EXISTS findings(id TEXT PRIMARY KEY,workspace TEXT NOT NULL,entity TEXT NOT NULL,record_kind TEXT NOT NULL,secret_kind TEXT NOT NULL,path TEXT NOT NULL,line INT NOT NULL,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL);"""
redact=typer.Typer(help="Find secrets locally and audit automatic team redactions")

def spans(text):
    found=[]
    for kind,pattern,group in PATTERNS:
        for match in pattern.finditer(text):
            start,end=match.span(group)
            if not any(start<b and a<end for a,b,*_ in found): found.append((start,end,kind))
    return sorted(found)
def scrub(text,path="$"):
    matches=spans(text)
    found=[dict(kind=kind,path=path,line=text.count("\n",0,start)+1,start=start) for start,_,kind in matches]
    safe=text
    for start,end,kind in reversed(matches): safe=safe[:start]+f"[REDACTED:{kind}]"+safe[end:]
    return safe,found
def inspect(value,path="$"):
    if isinstance(value,str): return scrub(value,path)
    if isinstance(value,dict):
        rows=[(k,*inspect(v,f"{path}.{k}")) for k,v in value.items()]
        return {k:safe for k,safe,_ in rows},[f for _,_,findings in rows for f in findings]
    if isinstance(value,list):
        rows=[inspect(v,f"{path}[{i}]") for i,v in enumerate(value)]
        return [safe for safe,_ in rows],[f for _,findings in rows for f in findings]
    return value,[]
def _root(root=None): return Path(root or os.environ.get("CONVOS_PROJECT_ROOT",Path.home()/".convos")).expanduser()
def _private_json(path,data):
    from ai_convos.cli import atomic_json
    if path.parent.is_symlink(): raise ValueError("Redaction state directory must not be a symlink")
    path.parent.mkdir(parents=True,exist_ok=True)
    os.chmod(path.parent,0o700)
    if path.is_symlink() or path.exists() and not path.is_file(): raise ValueError("Redaction state must be a regular non-symlink file")
    atomic_json(path,data)
def _audit(root,workspace,record,findings):
    if not findings: return
    path=_root(root)/"redact/audit.db"
    if path.parent.is_symlink(): raise ValueError("Redaction audit directory must not be a symlink")
    path.parent.mkdir(parents=True,exist_ok=True)
    os.chmod(path.parent,0o700)
    if path.is_symlink() or path.exists() and not path.is_file(): raise ValueError("Redaction audit database must be a regular non-symlink file")
    path.touch(mode=0o600,exist_ok=True)
    os.chmod(path,0o600)
    with contextlib.closing(sqlite3.connect(path)) as db,db:
        db.executescript(SCHEMA)
        now=datetime.now(timezone.utc).isoformat()
        for finding in findings:
            ident="sec_"+hashlib.sha256(f"{workspace}\0{record['entity']}\0{finding['path']}\0{finding['line']}\0{finding['start']}\0{finding['kind']}".encode()).hexdigest()[:20]
            db.execute("INSERT INTO findings VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen",(ident,workspace,record["entity"],record["kind"],finding["kind"],finding["path"],finding["line"],now,now))
def protect(record,root=None,workspace="team"):
    if record["kind"] in ("attachment.record","attachment.chunk"):
        if record["kind"]=="attachment.chunk":
            _audit(root,workspace,record,[dict(kind="attachment_redacted",path="$.payload",line=1,start=0)])
            return None
        if record["payload"].get("state")=="deleted": return record
        _audit(root,workspace,record,[dict(kind="attachment_redacted",path="$.payload",line=1,start=0)])
        p,row=record["payload"],dict(zip(record["payload"]["columns"],record["payload"]["row"]))
        row.update(filename="[REDACTED:attachment]",mime_type=None,size=None,path=None,url=None,**({"body_hash":None} if "body_hash" in row else {}))
        return dict(record,payload={**p,"row":[row[c] for c in p["columns"]]})
    payload,findings=inspect(record["payload"],"$.payload")
    _audit(root,workspace,record,findings)
    return dict(record,payload=payload)
def protect_all(records,root=None,workspace="team"):
    pairs=[(r,protect(r,root,workspace)) for r in records]
    edits={r["payload"]["row"][0]:(r,s) for r,s in pairs if s and r["kind"]=="file_edit.record" and r!=s and r["payload"].get("state")!="deleted"}
    facts={r["payload"]["id"]:r["payload"] for r,s in pairs if r["kind"]=="edit.observed" and r["payload"]["id"] in edits}
    files,repos={p["file"] for p in facts.values()},{p["repository"] for p in facts.values()}
    out=[]
    for original,safe in pairs:
        if not safe or safe["kind"]=="file.version" and safe["payload"]["file"] in files or safe["kind"]=="git.checkpoint" and safe["payload"]["repository"] in repos or safe["kind"]=="checkpoint.link" and safe["payload"]["edit"] in edits: continue
        if safe["kind"]=="edit.observed" and safe["payload"]["id"] in edits:
            record=edits[safe["payload"]["id"]][1]["payload"]
            row=dict(zip(record["columns"],record["row"]))
            safe=dict(safe,payload={**safe["payload"],"old_content_hash":hashlib.sha256(row["old_content"].encode()).hexdigest() if row["old_content"] is not None else None,"new_content_hash":hashlib.sha256((row["content"] or "").encode()).hexdigest()})
        if safe["kind"]=="repository.observed" and safe["payload"]["id"] in repos: safe=dict(safe,payload={**safe["payload"],"lineage":None,"roots":[],"head":None})
        out.append(safe)
    return out
def scan_data(cache=False):
    from ai_convos.cli import DB_PATH,get_db
    before,saved=(str(DB_PATH.resolve()),DB_PATH.stat().st_mtime_ns,DB_PATH.stat().st_size),_root()/"redact/scan.json"
    if cache and (saved.parent.is_symlink() or saved.is_symlink()): raise ValueError("Redaction scan cache must not use a symlink")
    if cache and saved.is_file() and not saved.is_symlink():
        os.chmod(saved,0o600)
        try:
            old=json.loads(saved.read_text())
            if old["key"]==list(before): return dict(old["data"],cached=True)
        except (OSError,ValueError,KeyError,TypeError): pass
    db,findings=get_db(read_only=True),[]
    try:
        for table,fields in FIELDS.items():
            for field in fields:
                cursor=db.execute(f"SELECT id,CAST({field} AS VARCHAR) FROM {table} WHERE {field} IS NOT NULL AND regexp_matches(CAST({field} AS VARCHAR), ?, 'i')",[PREFILTER])
                for rows in iter(lambda:cursor.fetchmany(64),[]):
                    for row_id,value in rows:
                        _,seen=inspect(value,f"$.{field}")
                        findings.extend(dict(f,table=table,row_id=row_id,field=field) for f in seen)
    finally: db.close()
    data=dict(status="clean" if not findings else "secrets_found",total=len(findings),by_kind={kind:sum(f["kind"]==kind for f in findings) for kind in sorted({f["kind"] for f in findings})},findings=findings,cached=False)
    if cache and (str(DB_PATH.resolve()),DB_PATH.stat().st_mtime_ns,DB_PATH.stat().st_size)==before: _private_json(saved,{"key":before,"data":data})
    return data
def audit_data(root=None):
    path=_root(root)/"redact/audit.db"
    if path.parent.is_symlink(): raise ValueError("Redaction audit database must be a regular non-symlink file")
    if not path.exists(): return dict(status="clean",total=0,by_kind={},findings=[])
    if path.is_symlink() or not path.is_file(): raise ValueError("Redaction audit database must be a regular non-symlink file")
    with contextlib.closing(sqlite3.connect(path)) as db: rows=db.execute("SELECT id,workspace,entity,record_kind,secret_kind,path,line,first_seen,last_seen FROM findings ORDER BY last_seen DESC").fetchall()
    keys=("id","workspace","entity","record_kind","kind","path","line","first_seen","last_seen")
    return dict(status="clean" if not rows else "redacted",total=len(rows),by_kind={kind:sum(r[4]==kind for r in rows) for kind in sorted({r[4] for r in rows})},findings=[dict(zip(keys,row)) for row in rows])
def emit(data,fmt):
    if fmt=="json": return typer.echo(json.dumps(data))
    typer.echo(f"{data['status']}: {data['total']} finding{'s' if data['total']!=1 else ''}")
    [typer.echo(f"- {r['kind']}: {r.get('table',r.get('record_kind'))}:{r.get('row_id',r.get('entity'))} {r['path']}:{r['line']}") for r in data["findings"]]
@redact.command("scan")
def scan_cmd(fmt:str=typer.Option("text","-f","--format"),fresh:bool=typer.Option(False,"--fresh",help="Ignore an exact unchanged-database scan cache.")):
    if fmt not in ("text","json"): raise typer.BadParameter("must be text or json","--format")
    emit(scan_data(not fresh),fmt)
@redact.command("status")
def status_cmd(fmt:str=typer.Option("text","-f","--format")):
    if fmt not in ("text","json"): raise typer.BadParameter("must be text or json","--format")
    emit(audit_data(),fmt)
def doctor_status():
    return f"redact: {(data:=audit_data())['total']} automatic team redaction{'s' if data['total']!=1 else ''} recorded"
def register(app): app.add_typer(redact,name="redact")
