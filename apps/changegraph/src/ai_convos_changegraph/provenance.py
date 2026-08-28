"""Read-only typed provenance views over the core DuckDB archive."""
import subprocess
from pathlib import Path

from ai_convos.cli import repository

def _run(root,*args): return subprocess.run(("git","-C",str(root),*args),capture_output=True,check=True).stdout

_PROMPT="""(SELECT u.content FROM messages u WHERE u.conversation_id=m.conversation_id AND u.role='user' AND u.content!='' AND u.created_at<=fe.created_at ORDER BY u.created_at DESC LIMIT 1)"""
_HISTORY=f"""SELECT fe.created_at observed_at,f.repository,f.path,fe.edit_type,x.evidence,fe.message_id changeset_id,COALESCE(m.conversation_id,'unknown') conversation,fe.message_id turn,{_PROMPT} prompt,x.old_content_hash,x.new_content_hash FROM provenance.file_edit_files x JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id AND v.status='confirmed' JOIN provenance.files f ON f.id=x.file_id JOIN file_edits fe ON fe.id=x.file_edit_id LEFT JOIN messages m ON m.id=fe.message_id"""
_QUERIES={
    "file_history":(_HISTORY,"path"),
    "changeset_files":(f"SELECT fe.message_id id,m.conversation_id,fe.message_id turn,{_PROMPT} prompt,f.repository,f.path,fe.edit_type,x.evidence FROM provenance.file_edit_files x JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id AND v.status='confirmed' JOIN provenance.files f ON f.id=x.file_id JOIN file_edits fe ON fe.id=x.file_edit_id JOIN messages m ON m.id=fe.message_id","id"),
    "conversation_changes":(f"SELECT conversation,changeset_id,ANY_VALUE(prompt) prompt,COUNT(DISTINCT repository) repositories,COUNT(DISTINCT COALESCE(repository,'')||':'||path) files,MIN(observed_at) first_edit,MAX(observed_at) last_edit FROM ({_HISTORY}) h GROUP BY conversation,changeset_id","conversation"),
    "commit_conversations":("SELECT p.head,m.conversation_id conversation,fe.message_id changeset_id,p.repository,x.evidence FROM provenance.checkpoint_edits x JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id AND v.status='confirmed' JOIN provenance.git_checkpoints p ON p.id=x.checkpoint_id JOIN file_edits fe ON fe.id=x.file_edit_id JOIN messages m ON m.id=fe.message_id","head"),
    "repository_activity":("SELECT f.repository,m.conversation_id conversation,fe.message_id changeset_id,COUNT(*) edits,MAX(fe.created_at) last_edit FROM provenance.file_edit_files x JOIN provenance.file_edit_evidence v ON v.file_edit_id=x.file_edit_id AND v.status='confirmed' JOIN provenance.files f ON f.id=x.file_id JOIN file_edits fe ON fe.id=x.file_edit_id JOIN messages m ON m.id=fe.message_id GROUP BY f.repository,m.conversation_id,fe.message_id","repository"),
    "checkpoint_states":("SELECT repository,head,state_hash,paths,capture_source,observed_at FROM provenance.git_checkpoints","repository"),
    "repository_lineages":("SELECT lineage,COUNT(*) repositories,STRING_AGG(id,',' ORDER BY id) repository_ids FROM provenance.repositories GROUP BY lineage","lineage"),
}
def _rows(cur): return [dict(zip((d[0] for d in cur.description),row)) for row in cur.fetchall()]
def query(db,name,arg=None):
    if name=="checkpoint_diff":
        before,after=arg.split("..",1); rows=[_rows(db.execute("SELECT repository,head,state_hash FROM provenance.git_checkpoints WHERE id=?",(x,))) for x in (before,after)]; a,b=(r[0] if r else None for r in rows)
        if not a or not b or a["repository"]!=b["repository"]: raise ValueError("checkpoint ids must exist in one repository")
        checkout=_rows(db.execute("SELECT root FROM provenance.repository_checkouts WHERE repository=? LIMIT 1",(a["repository"],))); available=bool(checkout and a["head"] and b["head"]); changed=_run(checkout[0]["root"],"diff","--name-status",a["head"],b["head"]).decode().splitlines() if available else None
        return [dict(repository=a["repository"],before=before,after=after,head_before=a["head"],head_after=b["head"],available=available,changed=changed,state_changed=a["state_hash"]!=b["state_hash"])]
    if name=="current_activity":
        repo=repository(Path(arg or ".").resolve()); return query(db,"repository_activity",repo["id"] if repo else "")
    if name not in _QUERIES: raise ValueError(f"unknown graph view {name}")
    sql,column=_QUERIES[name]; return _rows(db.execute(f"SELECT * FROM ({sql}) q"+(f" WHERE {column}=?" if arg is not None else ""), (arg,) if arg is not None else ()))
