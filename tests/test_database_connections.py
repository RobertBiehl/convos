import ast, json, os, re, subprocess, sys
from collections import Counter
from pathlib import Path
import pytest

from ai_convos import cli

ROOT=Path(__file__).resolve().parents[1]
SOURCES=[ROOT/"src/ai_convos/cli.py",*ROOT.glob("apps/*/src/**/*.py")]
SQLITE=Counter({
    ("src/ai_convos/cli.py","read_chrome_cookies"):1,("src/ai_convos/cli.py","chrome_cookie_domains"):1,
    ("apps/memory/src/ai_convos_memory/__init__.py","_scope"):1,("apps/memory/src/ai_convos_memory/__init__.py","connect"):1,("apps/memory/src/ai_convos_memory/__init__.py","backup_data"):1,("apps/memory/src/ai_convos_memory/__init__.py","_health_data"):1,("apps/memory/src/ai_convos_memory/__init__.py","_snapshot"):3,
    ("apps/redact/src/ai_convos_redact/__init__.py","_audit"):1,("apps/redact/src/ai_convos_redact/__init__.py","audit_data"):1,
    ("apps/remote_server/src/ai_convos_remote_server/__init__.py","connect"):1,("apps/remote_server/src/ai_convos_remote_server/__init__.py","main"):2,
    ("apps/remote/src/ai_convos_remote/__init__.py","rescue_bindings"):1,("apps/remote/src/ai_convos_remote/migrations.py","migrate_state"):1,
    ("apps/remote/src/ai_convos_remote/projection.py","_connect"):1,("apps/remote/src/ai_convos_remote/projection.py","read_state"):1,("apps/remote/src/ai_convos_remote/projection.py","inspect_state"):1,("apps/remote/src/ai_convos_remote/projection.py","cutover_state"):2,
})

def calls():
    for path in SOURCES:
        tree=ast.parse(path.read_text()); parents={child:node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            if not isinstance(node,ast.Call): continue
            owner=node
            while owner in parents and not isinstance((owner:=parents[owner]),(ast.FunctionDef,ast.AsyncFunctionDef)): pass
            yield path.relative_to(ROOT).as_posix(),getattr(owner,"name","<module>"),node

def test_archive_connections_are_gatewayed_and_purpose_labeled():
    direct=[]; missing=[]
    for path,owner,node in calls():
        name=node.func.id if isinstance(node.func,ast.Name) else node.func.attr if isinstance(node.func,ast.Attribute) else None
        module=node.func.value.id if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) else None
        if (module,name)==("duckdb","connect"): direct.append((path,owner))
        if name in {"get_db","open_db","_core","commit_result"} and owner not in {"get_db","open_db","_core","commit_result"}:
            purpose=next((k.value for k in node.keywords if k.arg=="purpose"),None)
            if not isinstance(purpose,ast.Constant) or not isinstance(purpose.value,str) or not purpose.value: missing.append((path,owner,node.lineno))
    assert Counter(direct)==Counter({("src/ai_convos/cli.py","_open_db"):1,("src/ai_convos/cli.py","_check_archive"):1})
    assert not missing,f"Archive connection lacks a literal purpose: {missing}"

def test_connection_ledger_has_every_archive_and_sqlite_acquisition_once():
    text=(ROOT/"docs/database-connection-ledger.md").read_text()
    archive=Counter((path,owner,next(k.value.value for k in node.keywords if k.arg=="purpose")) for path,owner,node in calls() if (node.func.id if isinstance(node.func,ast.Name) else node.func.attr if isinstance(node.func,ast.Attribute) else None) in {"get_db","open_db","_core","commit_result"} and owner not in {"get_db","open_db","_core","commit_result"})
    sqlite=Counter((path,owner) for path,owner,node in calls() if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and (node.func.value.id,node.func.attr)==("sqlite3","connect"))
    assert Counter(re.findall(r"\| A\d+ \| `([^`]+)::([^`]+)` \| `([^`]+)` \|",text))==archive
    assert Counter(re.findall(r"\| S\d+ \| `([^`]+)::([^`]+)` \|",text))==sqlite

def test_database_drivers_cannot_hide_behind_import_aliases():
    hidden=[]
    for path in SOURCES:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node,ast.Import): hidden += [(path.relative_to(ROOT),node.lineno,a.name,a.asname) for a in node.names if a.name in {"duckdb","sqlite3"} and a.asname]
            if isinstance(node,ast.ImportFrom) and node.module in {"duckdb","sqlite3"}: hidden.append((path.relative_to(ROOT),node.lineno,node.module,"from-import"))
    assert not hidden,f"Database driver import bypasses the inventory: {hidden}"

def test_sqlite_connection_inventory_is_complete():
    found=Counter((path,owner) for path,owner,node in calls() if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and (node.func.value.id,node.func.attr)==("sqlite3","connect"))
    assert found==SQLITE

def test_lock_diagnostics_name_waiter_and_holder(tmp_path,capsys):
    path=tmp_path/"archive.db"; holder=cli.open_db(path,purpose="remote attestation")
    assert "remote attestation" in path.with_name(".archive.db.lock").read_text()
    try:
        try: cli.open_db(path,wait=.01,purpose="local sync")
        except cli.LockBusy as error: message=str(error)
        assert "local sync" in message and "remote attestation" in message and "PID " in message
        holder.audit=(holder.audit[0],holder.audit[1],holder.audit[2]-6,*holder.audit[3:])
    finally: holder.close()
    error=capsys.readouterr().err
    assert "write lock held" in error and "remote attestation" in error and json.loads(path.with_name(".archive.db.lock").read_text())["stage"]=="finished"

def test_shared_reader_diagnostics_are_identified_and_crash_records_are_pruned(tmp_path):
    path=tmp_path/"archive.db"; writer=cli.open_db(path,purpose="fixture"); writer.close(); reader=cli.open_db(path,True,purpose="remote.scan"); reader.execute("SELECT 1")
    sidecars=list(path.with_name(".archive.db.lock.readers").glob("*.json")); assert len(sidecars)==1 and (owner:=json.loads(sidecars[0].read_text()))["purpose"]=="remote.scan" and owner["stage"]=="query"
    with pytest.raises(cli.LockBusy,match=r"local embed.*PID.*remote\.scan") as error: cli.open_db(path,wait=0,purpose="local embed")
    assert "shared archive reader" not in str(error.value)
    reader.close(); assert not list(path.with_name(".archive.db.lock.readers").glob("*.json"))
    sidecars[0].write_text(json.dumps({"purpose":"stale"})); writer=cli.open_db(path,purpose="recovery"); writer.close(); assert not sidecars[0].exists()

def test_all_process_locks_use_the_shared_contract_and_survive_crashes(tmp_path):
    found=Counter((path,owner) for path,owner,node in calls() if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and (node.func.value.id,node.func.attr)==("fcntl","flock"))
    assert found==Counter({("src/ai_convos/cli.py","_flock"):1,("src/ai_convos/cli.py","get_db"):1,("src/ai_convos/cli.py","operation_lock"):1})
    path=tmp_path/"nested/work.lock"; child=subprocess.run([sys.executable,"-c","import os,sys; from pathlib import Path; from ai_convos.cli import operation_lock\nwith operation_lock(Path(sys.argv[1]),'test.crash'): os._exit(17)",str(path)])
    assert child.returncode==17
    with cli.operation_lock(path,"test.recovery") as pulse: pulse("verified")
    owner=json.loads(path.read_text()); assert path.stat().st_mode&0o777==0o600 and owner["v"]==1 and owner["purpose"]=="test.recovery" and owner["pid"]==os.getpid() and owner["stage"]=="finished" and owner["started_at"]<=owner["heartbeat_at"]

def test_external_duckdb_contention_uses_the_concise_lock_error(tmp_path,monkeypatch):
    monkeypatch.setattr(cli,"_open_db",lambda *args:(_ for _ in ()).throw(cli.duckdb.IOException("Conflicting lock is held by external PID 12")))
    with pytest.raises(cli.LockBusy,match="outside the Convos lock contract.*external PID 12"): cli.open_db(tmp_path/"archive.db",wait=0,purpose="test.external")

def test_remote_signing_and_attachment_io_run_without_archive_lock(tmp_path,monkeypatch):
    from ai_convos_remote import projection
    from ai_convos_remote.protocol import certificate,identity,public,public_id
    path=tmp_path/"data/convos.db"; db=cli.open_db(path,purpose="fixture")
    cli.init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}'); INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','user','{}')"); db.close()
    root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); signer={"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True}; control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:signer}}; cfg={"user":user,"device":device,"workspaces":{"w":{"kind":"personal","epoch":1}},"controls":{"w":control},"server_state":{"workspaces":[{"id":"w","controls":[control]}]}}
    real_proof=projection.row_proof
    def proof(*args,**kwargs):
        cli.open_db(path,wait=0,purpose="concurrent signing probe").close()
        return real_proof(*args,**kwargs)
    monkeypatch.setattr(projection,"row_proof",proof)
    record=dict(kind="conversation.record",entity="conversations:c",payload=dict(table="conversations",columns=cli.ARCHIVE_COLUMNS["conversations"],row=["c","codex",None,None,None,None,None,None,None,"{}"]))
    assert projection.attest_rows(path,cfg,"w",[record])==1
    db=cli.open_db(path,purpose="fixture"); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('d','codex','{}')"); generation=cli.archive_state(db)[1]; cli._archive_touch(db,[("conversations","d")]); generation=cli.archive_state(db)[1]; db.close(); changed=False
    def changing_proof(*args,**kwargs):
        nonlocal changed
        if not changed:
            writer=cli.open_db(path,wait=0,purpose="concurrent archive change"); writer.execute("UPDATE conversations SET title='changed' WHERE id='d'"); cli._archive_touch(writer,[("conversations","d")]); writer.close(); changed=True
        return real_proof(*args,**kwargs)
    monkeypatch.setattr(projection,"row_proof",changing_proof); other={**record,"entity":"conversations:d","payload":{**record["payload"],"row":["d","codex",None,None,None,None,None,None,None,"{}"]}}
    with pytest.raises(RuntimeError,match="Archive changed"): projection.attest_rows(path,cfg,"w",[other],generation=generation)
    legacy=tmp_path/"legacy"; legacy.mkdir(); body=legacy/"body"; body.write_bytes(b"content"); db=cli.open_db(path,purpose="fixture"); db.execute("INSERT INTO attachments VALUES ('a','m','body',NULL,7,?,NULL,NULL)",[str(body)]); db.close(); real_hash=projection.file_hash
    def hashed(value):
        cli.open_db(path,wait=0,purpose="concurrent attachment probe").close()
        return real_hash(value)
    monkeypatch.setattr(projection,"file_hash",hashed)
    assert projection.relocate_attachments(path,legacy)==1

def test_remote_full_scan_releases_the_archive_between_bounded_pages(tmp_path):
    from ai_convos_remote import projection
    path=tmp_path/"data/convos.db"; db=cli.open_db(path,purpose="fixture"); cli.init_schema(db); db.executemany("INSERT INTO conversations(id,source,metadata) VALUES (?,'codex','{}')",[(str(i),) for i in range(3)]); generation=cli.archive_state(db)[1]; db.close(); state=projection.connect(tmp_path/"state.db"); stages=[]
    def progress(stage):
        with cli.open_db(path,wait=0,purpose="concurrent page probe"): pass
        stages.append(stage)
    records=projection.scan_archive(path,state,generation=generation,progress=progress,page=1); state.close()
    assert {r["payload"]["row"][0] for r in records}=={"0","1","2"} and stages==["scanning archive 1","scanning archive 2","scanning archive 3"]

def test_remote_repair_inventory_releases_the_archive_between_pages(tmp_path,monkeypatch):
    import base64, ai_convos_remote
    path=tmp_path/"data/convos.db"; db=cli.open_db(path,purpose="fixture"); cli.init_schema(db); db.executemany("INSERT INTO remote.row_proofs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[(f"p{i}","w","w","conversations",f"c{i}",1,"h",f"r{i}",None,"active","u","d",1,"s") for i in range(2)]); db.close(); stages=[]; monkeypatch.setattr(ai_convos_remote,"bridge_records",lambda *_:[])
    def progress(stage):
        with cli.open_db(path,wait=0,purpose="concurrent inventory probe"): pass
        stages.append(stage)
    assert len(ai_convos_remote.local_replica_ids(tmp_path,{"keys":{"w:1":base64.b64encode(b"x"*32).decode()}},"w","team",{1},1,progress))==2 and stages==["p0","p1"]

def test_redaction_and_export_processing_run_without_archive_lock(monkeypatch):
    from ai_convos_redact import scan_data
    import ai_convos_redact
    db=cli.open_db(purpose="fixture"); cli.init_schema(db); db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','test','{}'); INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('m','c','user','sk-proj-abcdefghijklmnopqrstuvwxyz','{}')"); db.close()
    real=ai_convos_redact.inspect
    def inspect(*args,**kwargs):
        cli.open_db(wait=0,purpose="concurrent redaction probe").close()
        return real(*args,**kwargs)
    monkeypatch.setattr(ai_convos_redact,"inspect",inspect)
    assert scan_data()["total"]==1
    class Output:
        def write_text(self,text):
            cli.open_db(wait=0,purpose="concurrent export probe").close()
            self.text=text
    output=Output(); cli.export(output,"json",None); assert '"id": "c"' in output.text

def test_memory_scope_resolution_runs_without_archive_lock(tmp_path,monkeypatch):
    import ai_convos_memory
    path=tmp_path/"archive.db"; db=cli.open_db(path,purpose="fixture"); cli.init_schema(db); db.execute("INSERT INTO conversations(id,source,cwd,metadata) VALUES ('c','codex','/repo','{}'); INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('m','c','user','evidence','{}')"); db.close(); monkeypatch.setattr(cli,"DB_PATH",path); monkeypatch.setattr(cli,"drain_hooks",lambda:0)
    def scope(_):
        with cli.open_db(path,wait=0,purpose="concurrent memory probe"): pass
        return "/repo"
    monkeypatch.setattr(ai_convos_memory,"_scope",scope)
    assert ai_convos_memory._archive_evidence(["m"],"/repo")[0]["message"]=="m"

def test_ingest_chunks_are_ordered_restart_safe_and_preserve_evidence(tmp_path,monkeypatch):
    path=tmp_path/"archive.db"; monkeypatch.setattr(cli,"DB_PATH",path); db=cli.open_db(path,purpose="fixture"); cli.init_schema(db); db.close()
    conv=dict(id="c",source="codex",title="long",created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata="{}")
    msgs=list(reversed([dict(id=f"m{i}",conversation_id="c",role="assistant",content="done",thinking=None,created_at=None,model=None,metadata="{}",parent_id=f"m{i-1}" if i else None) for i in range(501)]))
    tools=[dict(id=f"t{i}",message_id="m500",tool_name="apply_patch",input="{}",output="{}",status="complete",duration_ms=None,created_at=None) for i in range(501)]
    edits=[dict(id=f"e{i}",message_id="m500",file_path=f"{i}.py",edit_type="write",content="x",created_at=None,old_content=None) for i in range(501)]
    evidence=[dict(file_edit_id=f"e{i}",status="confirmed",reason="tool_success",tool_call_id=f"t{i}") for i in range(501)]
    result=cli.ParseResult(convs=[conv],msgs=msgs,tools=tools,edits=edits,edit_evidence=evidence); real,calls=cli.upsert,[]
    def interrupted(conn,part):
        calls.append((len(part.convs),len(part.msgs),len(part.tools),len(part.edits),len(part.edit_evidence)))
        if len(calls)==5: raise RuntimeError("interrupt")
        return real(conn,part)
    monkeypatch.setattr(cli,"upsert",interrupted)
    with pytest.raises(RuntimeError,match="interrupt"): cli.commit_result(result,purpose="test.ingest")
    db=cli.open_db(path,read_only=True,purpose="fixture.read"); assert db.execute("SELECT (SELECT count(*) FROM conversations),(SELECT count(*) FROM messages),(SELECT count(*) FROM tool_calls),(SELECT count(*) FROM file_edits)").fetchone()==(1,501,500,0); db.close()
    calls.clear()
    def recorded(conn,part):
        calls.append((len(part.convs),len(part.msgs),len(part.tools),len(part.edits),len(part.edit_evidence)))
        return real(conn,part)
    monkeypatch.setattr(cli,"upsert",recorded); cli.commit_result(result,purpose="test.ingest")
    db=cli.open_db(path,read_only=True,purpose="fixture.read"); assert db.execute("SELECT (SELECT count(*) FROM tool_calls),(SELECT count(*) FROM file_edits),(SELECT count(*) FROM provenance.file_edit_evidence WHERE status='confirmed')").fetchone()==(501,501,501); db.close()
    assert calls==[(1,0,0,0,0),(0,500,0,0,0),(0,1,0,0,0),(0,0,500,0,0),(0,0,1,0,0),(0,0,0,500,0),(0,0,0,1,0),(0,0,0,0,500),(0,0,0,0,1)] and result.provenance_edits=={f"e{i}" for i in range(501)}
