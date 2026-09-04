import copy, json, os, shutil, sqlite3, subprocess, sys, threading, time, urllib.error
from pathlib import Path

import duckdb
import pytest
import ai_convos.cli as core_module
import ai_convos_memory as memory_module
import ai_convos_remote as remote_client
import ai_convos_remote.projection as projection_module
from ai_convos.cli import ARCHIVE_COLUMNS, archive_state, capture_provenance, index_attachment_body, init_schema, project_archive_row, project_logical_row
from ai_convos_remote import (_upload_batches, add_member, approve_device, approve_history, archive_info, bind_origin, configure_sharing, connect, control_body, create, doctor_status, fetch_lazy, grant_all, key, load, pull, pull_origins, publish, refresh, rehome_client, remove_device,
                              repull_once, request_device, request_history, rescue_bindings, setup_client, sync_once, upload, workspace)
from ai_convos_remote.control import sign as control_sign, vote as device_vote
from ai_convos_remote.projection import inspect_state, scan, sharing
from ai_convos_remote.protocol import certificate, event, identity, logical_row, open_blob, open_origin, open_replica, seal_blob, seal_event, seal_key, seal_origin, seal_replica, sign_control, unb64
from ai_convos_remote_server import action, connect as server_connect


def transport(db):
    def call(cfg,body,auth=True): return action(db,body,cfg.get("token") if auth else None)
    return call
@pytest.fixture(autouse=True)
def no_approval_delay(monkeypatch): monkeypatch.setattr("ai_convos_remote_server.APPROVAL_DELAY",0)
def conversation(title="shared",id="c"):
    cols=["id","source","title","created_at","updated_at","model","cwd","git_branch","project_id","metadata"]
    return {"kind":"conversation.record","entity":f"conversations:{id}","payload":{"table":"conversations","columns":cols,"row":[id,"codex",title,"2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]}}
def policy(value="shared",id="p"): return {"kind":"workspace.policy","entity":f"policy:test:{id}","payload":{"kind":"test","value":value}}
def ledger_event(value="shared",id="p"): return {"kind":"workspace.membership","entity":f"membership:test:{id}","payload":{"marker":value}}
def write_archive(path,title):
    path.parent.mkdir(parents=True,exist_ok=True); db=duckdb.connect(str(path)); init_schema(db); db.execute("BEGIN"); project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],["c","codex",title,"2026-01-01","2026-01-01",None,None,None,None,"{}"]); db.execute("COMMIT"); info=archive_state(db); db.close(); return info
def replicate_conversation(root,ws,title="shared",id="c"):
    cfg=load(root); refresh(cfg,root); path=root/"data/convos.db"; path.parent.mkdir(parents=True,exist_ok=True); db=duckdb.connect(str(path)); init_schema(db); project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],[id,"codex",title,"2026-01-01T00:00:00","2026-01-01T00:00:00",None,None,None,None,"{}"]); db.close(); record=conversation(title,id); state=connect(root/"remote/state.db"); bindings={r[0]:r[1] for r in state.execute("SELECT origin,epoch FROM origin_bindings WHERE workspace=?",(ws,)).fetchall()}; projection_module.attest_rows(path,cfg,ws,[record],set(bindings)); envs=projection_module.row_replicas(path,cfg,ws,[record],{cfg["workspaces"][ws]["epoch"]:key(cfg,ws,cfg["workspaces"][ws]["epoch"])},origins=set(bindings),origin_epochs=bindings); remote_client.reconcile_replicas(cfg,state,root,ws,envs); state.close(); return envs
def inject(cfg,state,server,ws,kind,payload_v=1):
    seq=int((state.execute("SELECT value FROM meta WHERE key=?",(f"seq:{ws}",)).fetchone() or ["0"])[0])+1; prev=(state.execute("SELECT value FROM meta WHERE key=?",(f"prev:{ws}",)).fetchone() or [None])[0]; value=event(cfg["device"],seq,kind,f"{kind}:1",{"new_field":[1,2,3]},[prev] if prev else (),payload_v=payload_v); action(server,{"op":"upload_many","envelopes":[seal_event(value,ws,cfg["workspaces"][ws]["epoch"],key(cfg,ws,cfg["workspaces"][ws]["epoch"]))]},cfg["token"]); return value

def test_upload_batches_bound_count_and_wire_size():
    row=lambda size:{"size":size}
    assert [len(x) for x in _upload_batches([row(1)]*501,1000)]==[500,1] and [len(x) for x in _upload_batches([row(6)]*2,10)]==[1,1]

def test_replica_outbox_is_disk_and_request_bounded(tmp_path,monkeypatch):
    root=tmp_path/"client"; monkeypatch.setattr(remote_client,"REPLICA_BATCH_BYTES",700); envs=[{"workspace":"w","replica":f"{i:064x}","epoch":1,"payload":"x"*120} for i in range(19)]; prepared=remote_client.prepare_replicas(root,envs); remote_client.publish_replicas(prepared); files=list((root/"remote/outbox").glob("replica-batch-*")); calls=[]
    assert len(files)>1 and max(p.stat().st_size for p in files)<=700
    def request(cfg,body,auth=True):
        calls.append(len(json.dumps(body).encode())); return {"present":{rid:i+1 for i,rid in enumerate(body["replicas"])}} if body["op"]=="replica_reconcile" else {"replicas":[]}
    monkeypatch.setattr(remote_client,"request",request); state=connect(root/"remote/state.db"); remote_client.upload_replicas({},state,root); assert max(calls)<=700 and state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==len(envs) and not list((root/"remote/outbox").glob("replica-*")); state.close()

def test_replica_preparation_interrupt_removes_temporary_batches(tmp_path,monkeypatch):
    root=tmp_path/"client"; monkeypatch.setattr(remote_client,"REPLICA_BATCH_BYTES",700); envs=[{"workspace":"w","replica":f"{i:064x}","epoch":1,"payload":"x"*120} for i in range(19)]; real=remote_client._prepare_replica; calls=[]
    def interrupted(*args):
        calls.append(1)
        if len(calls)==2: raise KeyboardInterrupt
        return real(*args)
    monkeypatch.setattr(remote_client,"_prepare_replica",interrupted)
    with pytest.raises(KeyboardInterrupt): remote_client.prepare_replicas(root,envs)
    assert len(calls)==2 and not list((root/"remote/outbox").glob(".replica-*"))

def test_replica_inventory_uses_server_limit_and_legacy_default(tmp_path,monkeypatch):
    state=connect(tmp_path/"state.db"); candidates=[(f"{i:064x}",1) for i in range(5001)]; calls=[]
    monkeypatch.setattr(remote_client,"request",lambda cfg,body,auth=True:calls.append(len(body["replicas"])) or {"present":{rid:i+1 for i,rid in enumerate(body["replicas"])}})
    assert len(remote_client.replica_inventory({"server_state":{"capabilities":{"replica_reconcile_limit":2500}}},state,"w",candidates))==5001 and calls==[2500,2500,1]
    calls.clear(); assert len(remote_client.replica_inventory({},state,"w",candidates[:1001]))==1001 and calls==[500,500,1]; state.close()

def test_first_publication_does_not_reconcile_each_preparation_page(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); calls=[]
    monkeypatch.setattr("ai_convos_remote.request",lambda cfg,body,auth=True:calls.append(body["op"]) or direct(cfg,body,auth)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); write_archive(root/"data/convos.db","first publication"); orphan=root/"remote/outbox/.replica-batch-orphan.json.1.1"; orphan.write_text("ignored staging data"); calls.clear(); sync_once(root)
    assert "replica_reconcile" not in calls and not orphan.exists() and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1

def test_legacy_replica_batch_stream_splits_and_resumes(tmp_path,monkeypatch):
    root=tmp_path/"client"; monkeypatch.setattr(remote_client,"REPLICA_BATCH_BYTES",700); envs=[{"workspace":"w","replica":f"{i:064x}","epoch":1,"payload":"x"*120} for i in range(19)]; path=remote_client.replica_file(root,envs,False); path.parent.mkdir(parents=True); path.write_text(json.dumps({"semantic":False,"envelopes":envs},separators=(",",":"))); real=Path.read_text
    monkeypatch.setattr(Path,"read_text",lambda self,*a,**k:(_ for _ in ()).throw(AssertionError("batch loaded whole")) if self.name.startswith("replica-batch-") else real(self,*a,**k)); uploads=[0]
    def failing(cfg,body,auth=True):
        if body["op"]=="replica_reconcile": return {"present":{}}
        uploads[0]+=1
        if uploads[0]==2: raise ConnectionError("interrupted")
        return {"replicas":[{"cursor":i+1} for i in range(len(body["envelopes"]))]}
    monkeypatch.setattr(remote_client,"request",failing); state=connect(root/"remote/state.db")
    with pytest.raises(ConnectionError,match="interrupted"): remote_client.upload_replicas({},state,root)
    saved=state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]; assert 0<saved<len(envs) and 0<len(list((root/"remote/outbox").glob("replica-*")))<len(envs)
    monkeypatch.setattr(remote_client,"request",lambda cfg,body,auth=True:{"present":{rid:i+100 for i,rid in enumerate(body["replicas"])}} if body["op"]=="replica_reconcile" else {"replicas":[]}); remote_client.upload_replicas({},state,root); assert state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==len(envs) and not list((root/"remote/outbox").glob("replica-*")); state.close()

def test_remote_timeout_names_operation(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen",lambda *args,**kwargs:(_ for _ in ()).throw(urllib.error.URLError(TimeoutError("write timed out"))))
    with pytest.raises(ConnectionError,match="replica_upload_many.*120s.*write timed out"): remote_client.request({"url":"http://localhost","token":"t"},{"op":"replica_upload_many","envelopes":[]})


def test_sync_lease_is_nonblocking_and_separate_from_mutation_lease(tmp_path):
    with remote_client.mutation_lock(tmp_path):
        with remote_client.sync_run(tmp_path): pass
        with pytest.raises(RuntimeError,match="another operation"):
            with remote_client.mutation_lock(tmp_path): pass
    remote_client.save({"name":"alice","user":"user-id","device":{"name":"laptop","id":"device-id"}},tmp_path)
    with remote_client.sync_run(tmp_path) as pulse:
        pulse("uploading replicas")
        owner=json.loads((tmp_path/"remote/sync.lock").read_text())
        assert owner["remote_user"]=="alice" and owner["user_id"]=="user-id" and owner["device"]=="laptop" and owner["pid"]==os.getpid() and owner["started_at"]<=owner["heartbeat_at"] and owner["stage"]=="uploading replicas"
        with pytest.raises(core_module.LockBusy,match=r"remote user alice.*device laptop.*OS user.*PID .*started .*last progress .*uploading replicas"):
            with remote_client.sync_run(tmp_path): pass
    with pytest.raises(KeyboardInterrupt):
        with remote_client.sync_run(tmp_path): raise KeyboardInterrupt

def test_sync_lease_is_recoverable_after_unclean_process_exit(tmp_path):
    result=subprocess.run([sys.executable,"-c","import os,sys; from pathlib import Path; from ai_convos_remote import sync_run;\nwith sync_run(Path(sys.argv[1])): os._exit(17)",str(tmp_path)])
    assert result.returncode==17 and (tmp_path/"remote/sync.lock").exists()
    with remote_client.sync_run(tmp_path): pass

def test_manual_sync_preempts_a_cooperative_background_sync(tmp_path,monkeypatch):
    monkeypatch.setattr(remote_client,"MANUAL_WAIT",1); other=tmp_path/"other"; started=threading.Event(); errors=[]
    with core_module.operation_lock(other/"remote/manual.lock","other manual"):
        with remote_client.sync_run(tmp_path): remote_client._progress("independent root")
    def background():
        try:
            with remote_client.sync_run(tmp_path):
                started.set()
                while True: time.sleep(.01); remote_client._progress("background request boundary")
        except Exception as error: errors.append(error)
    worker=threading.Thread(target=background); worker.start(); assert started.wait(1)
    with remote_client.sync_run(tmp_path,True,"repull"): pass
    worker.join(1); assert not worker.is_alive() and len(errors)==1 and isinstance(errors[0],InterruptedError) and "yielded to a manual command" in str(errors[0])

def test_manual_repull_fails_fast_against_a_noncooperative_background_sync(tmp_path,monkeypatch):
    monkeypatch.setattr(remote_client,"MANUAL_WAIT",.05)
    with remote_client.sync_run(tmp_path):
        started=time.monotonic()
        with pytest.raises(core_module.LockBusy,match=r"remote repull.*for 0.05s.*purpose remote sync"):
            with remote_client.sync_run(tmp_path,True,"repull"): pass
        assert time.monotonic()-started<.5

def test_sync_config_updates_cannot_overwrite_newer_control_or_unrelated_fields(tmp_path,monkeypatch):
    root=tmp_path/"client"; current={"user":"u","marker":"keep","sharing":{"w":{"auto_contribute":False,"match":[]}},"controls":{"w":{"revision":2}},"workspaces":{"w":{"epoch":2}},"keys":{},"server_state":{}}; remote_client.save(current,root); stale=copy.deepcopy(current); stale.pop("marker"); stale["controls"]["w"]={"revision":1}; stale["server_state"]={"workspaces":[]}
    with pytest.raises(RuntimeError,match="changed during sync"): remote_client.sync_save(stale,root)
    assert load(root)==current
    state=connect(root/"remote/state.db"); monkeypatch.setattr(remote_client,"archive_info",lambda *_:("archive",7,1)); remote_client.remember_archive(stale,state,root); saved=load(root); state.close(); assert saved["marker"]=="keep" and saved["sharing"]==current["sharing"] and saved["archive"]=={"id":"archive","generation":7}

def test_remote_sync_publishes_captured_snapshot_then_concurrent_change(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); ws=workspace(cfg,"Personal"); path=root/"data/convos.db"; generation=write_archive(path,"snapshot")[1]; prepare=remote_client.prepare_replicas; changed=[]
    def concurrent(root,envelopes,semantic=False):
        if envelopes and not semantic and not changed: changed.append(write_archive(path,"later")[1])
        return prepare(root,envelopes,semantic)
    monkeypatch.setattr(remote_client,"prepare_replicas",concurrent); sync_once(root,manual=True); state=connect(root/"remote/state.db"); assert int(state.execute("SELECT value FROM meta WHERE key=?",(f"core_generation:{ws}",)).fetchone()[0])==generation<changed[0]; state.close()
    assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1
    sync_once(root,manual=True); rows=[open_replica(json.loads(r[0]),key(load(root),ws,1)) for r in server.execute("SELECT envelope FROM row_replicas ORDER BY cursor").fetchall()]; assert [r["row"]["data"]["title"] for r in rows]==["snapshot","later"] and rows[1]["proof"]["previous_revision"]==rows[0]["proof"]["revision"]
    sync_once(root,manual=True); assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==2

def test_sync_revalidates_sharing_policy_before_publication(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); write_archive(root/"data/convos.db","private until validated"); real=remote_client.sharing_routes; calls=[]
    def changed(*args,**kwargs):
        routes=real(*args,**kwargs); calls.append(routes); return routes if len(calls)==1 else (routes[0],routes[1],[] if routes[2] else ["cwd"])
    monkeypatch.setattr(remote_client,"sharing_routes",changed)
    with pytest.raises(RuntimeError,match="sharing configuration changed"): sync_once(root,True)
    assert len(calls)==2 and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==0 and not list((root/"remote/outbox").glob("replica-*.json"))

def test_sync_serializes_state_cutover_with_control_mutations(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); state=connect(root/"remote/state.db"); state.execute("UPDATE meta SET value='1' WHERE key='state_schema'"); state.commit(); state.close(); real=remote_client.cutover_state; seen=[]
    def guarded(path):
        with pytest.raises(RuntimeError,match="remote mutation.*another operation"):
            with remote_client.mutation_lock(root): pass
        seen.append(True); return real(path)
    monkeypatch.setattr(remote_client,"cutover_state",guarded); sync_once(root); assert seen==[True] and inspect_state(root/"remote/state.db")["status"]=="current"
    with remote_client.sync_run(tmp_path): pass


def test_watch_persists_exponential_backoff_and_clears_failure_after_recovery(tmp_path,monkeypatch):
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(tmp_path))
    outcomes=[ValueError("first"),ValueError("second"),None,KeyboardInterrupt()]
    snapshots=[]
    def attempt():
        if (outcome:=outcomes.pop(0)) is not None: raise outcome
    def sleep(delay): snapshots.append((delay,json.loads((tmp_path/"remote/watch.json").read_text())))
    monkeypatch.setattr(remote_client,"sync_once",attempt); monkeypatch.setattr(remote_client.time,"sleep",sleep)
    with pytest.raises(KeyboardInterrupt): remote_client.watch(2)
    assert [delay for delay,status in snapshots]==[2,4,2] and [status["failures"] for delay,status in snapshots]==[1,2,0]
    assert snapshots[0][1]["next_retry"]>=snapshots[0][1]["failed_at"]+2 and snapshots[1][1]["next_retry"]>=snapshots[1][1]["failed_at"]+4 and not (tmp_path/"remote/last_error").exists()


def test_doctor_reports_watch_failure_and_next_retry(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db")
    monkeypatch.setattr("ai_convos_remote.request",transport(server))
    monkeypatch.setattr(remote_client,"health",lambda cfg:{"ok":True})
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(tmp_path/"client"))
    setup_client("http://server","alice",root=tmp_path/"client")
    remote_client.save_watch_status({"failures":3,"next_retry":123.0})
    status=doctor_status()
    assert "watch_failures=3" in status and "next_retry=123.0" in status


def test_settled_state_is_payload_size_independent_and_contains_no_bodies(tmp_path,monkeypatch):
    def settled(root,size):
        server=server_connect(tmp_path/f"{root.name}.server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); cfg,_=setup_client("http://server","alice",root=root); ws=workspace(cfg,"Personal"); state=connect(root/"remote/state.db"); marker=f"settled-{size}-marker-"+"x"*size; eid=publish(cfg,state,ws,ledger_event(marker),root); pending=next((root/"remote/outbox").iterdir()); assert marker.encode() not in pending.read_bytes() and state.execute("SELECT COUNT(*) FROM outbox WHERE event=?",(eid,)).fetchone()[0]==1; upload(cfg,state,root); state.execute("PRAGMA wal_checkpoint(TRUNCATE)"); pages=state.execute("PRAGMA page_count").fetchone()[0]; columns={r[1] for table, in state.execute("SELECT name FROM sqlite_master WHERE type='table'") for r in state.execute(f"PRAGMA table_info({table})")}; counts=tuple(state.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("receipts","publication_heads","event_sequences")); state.close(); files=[root/"remote/state.db",root/"remote/state.db-wal"]
        assert not list((root/"remote/outbox").iterdir()) and all(marker.encode() not in path.read_bytes() for path in files if path.exists()) and not {"event_json","envelope","content","data"}&columns
        return (root/"remote/state.db").stat().st_size,pages,counts
    assert settled(tmp_path/"one",1024)==settled(tmp_path/"two",1024*1024)


def test_remote_scan_is_read_only_and_does_not_self_trigger(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); repo=root/"repo"; repo.mkdir(parents=True); subprocess.run(("git","-C",str(repo),"init","-q"),check=True); subprocess.run(("git","-C",str(repo),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo),"config","user.name","A"),check=True); (repo/"a.py").write_text("one\n"); subprocess.run(("git","-C",str(repo),"add","."),check=True); subprocess.run(("git","-C",str(repo),"commit","-qm","initial"),check=True)
    path=root/"data/convos.db"; path.parent.mkdir(); db=duckdb.connect(str(path)); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','provenance','2026-01-01','2026-01-01',NULL,?,NULL,NULL,'{}')",[str(repo)]); db.execute("INSERT INTO messages VALUES ('m','c','assistant','done',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','one\n','2026-01-01',NULL)",[str(repo/"a.py")]); db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL)"); db.close(); capture_provenance(path); sync_once(root,True)
    state=connect(root/"remote/state.db"); ws=workspace(load(root),"Personal"); generation=int(state.execute("SELECT value FROM meta WHERE key=?",(f"core_generation:{ws}",)).fetchone()[0]); state.close(); db=duckdb.connect(str(path),read_only=True); observed=db.execute("SELECT observed_at FROM provenance.repositories").fetchone()[0]; assert generation==archive_state(db)[1]; db.close(); count=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    sync_once(root); db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT observed_at FROM provenance.repositories").fetchone()[0]==observed; db.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==count

def test_manual_noop_sync_does_not_force_repair_or_scan_archive_bridges(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); write_archive(root/"data/convos.db","settled"); sync_once(root,True)
    monkeypatch.setattr(remote_client,"scan_archive",lambda *args,**kwargs:(_ for _ in ()).throw(AssertionError("no-op archive scan"))); monkeypatch.setattr(remote_client,"edit_evidence_records",lambda *args,**kwargs:(_ for _ in ()).throw(AssertionError("no-op evidence scan")))
    sync_once(root,manual=True); state=connect(root/"remote/state.db"); assert not state.execute("SELECT 1 FROM meta WHERE key LIKE 'replica_repair:%'").fetchone(); state.close()

def test_manual_sync_cli_repairs_only_when_requested(monkeypatch):
    calls=[]; monkeypatch.setattr(remote_client,"sync_once",lambda *args,**kwargs:calls.append(kwargs))
    remote_client.sync_cmd(); remote_client.sync_cmd(True)
    assert calls==[{"repair":False,"manual":True},{"repair":True,"manual":True}]


def test_link_uses_stable_grant_token_and_immutable_git_evidence(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); create(cfg,"Team","team",root); repo=root/"repo"; repo.mkdir(); subprocess.run(("git","-C",str(repo),"init","-q"),check=True); subprocess.run(("git","-C",str(repo),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo),"config","user.name","A"),check=True); (repo/"a").write_text("x"); subprocess.run(("git","-C",str(repo),"add","."),check=True); subprocess.run(("git","-C",str(repo),"commit","-qm","initial"),check=True); subprocess.run(("git","-C",str(repo),"remote","add","origin","git@github.com:acme/project.git"),check=True); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(root)); remote_client.link_cmd(repo,"Team"); state=connect(root/"remote/state.db"); first=state.execute("SELECT value,evidence FROM policies WHERE kind='repository'").fetchone(); events=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; subprocess.run(("git","-C",str(repo),"remote","set-url","origin","https://github.com/other/fork.git"),check=True); remote_client.link_cmd(repo,"Team"); second=state.execute("SELECT value,evidence FROM policies WHERE kind='repository'").fetchone()
    assert first==second and first[0]!=core_module.repository(repo)["id"] and json.loads(first[1])["remotes"]==["https://github.com/acme/project"] and state.execute("SELECT COUNT(*) FROM policies WHERE kind='repository'").fetchone()[0]==1 and server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==events


def test_incremental_sync_reads_only_core_marked_rows(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); path=root/"data/convos.db"; write_archive(path,"one"); sync_once(root,True); before=server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]; seen=[]; real=projection_module.scan; monkeypatch.setattr(projection_module,"scan",lambda *args,**kwargs:seen.append(args[5]) or real(*args,**kwargs)); write_archive(path,"two"); sync_once(root)
    assert seen==[{("conversations","c")}] and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==before+1


def test_incremental_core_delete_emits_signed_tombstone(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); path=root/"data/convos.db"; write_archive(path,"gone"); sync_once(root,True); before=server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]; db=duckdb.connect(str(path)); db.execute("BEGIN"); project_logical_row(db,logical_row("conversations",identity="c",state="deleted"),{},"",True); db.execute("COMMIT"); db.close(); sync_once(root)
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT p.state FROM remote.row_proofs p WHERE p.source_row_id='c' AND NOT EXISTS (SELECT 1 FROM remote.row_proofs c WHERE c.previous_revision=p.revision)").fetchone()==("deleted",) and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==before+1; db.close()


def test_sync_settles_encrypted_row_replicas_without_state_content(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); monkeypatch.setattr("ai_convos_remote.open_replica",lambda *args:(_ for _ in ()).throw(AssertionError("own acknowledged replica reopened"))); root=tmp_path/"client"; setup_client("http://server","alice",root=root); marker="replica-plaintext-must-stay-local"; write_archive(root/"data/convos.db",marker); sync_once(root,True)
    assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1 and marker.encode() not in (tmp_path/"server.db").read_bytes(); state=connect(root/"remote/state.db"); tables={r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}; assert "replica_outbox" not in tables and state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==1 and not {"envelope","content","data"}&{r[1] for table in tables for r in state.execute(f"PRAGMA table_info({table})")}; state.close(); assert not list((root/"remote/outbox").glob("replica-*"))


def test_lost_direct_replica_response_reconciles_without_outbox(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); write_archive(root/"data/convos.db","lost response"); lost=[False]
    def request(cfg,body,auth=True):
        result=direct(cfg,body,auth)
        if body["op"]=="replica_upload_many" and not lost[0]: lost[0]=True; raise ConnectionError("response lost")
        return result
    monkeypatch.setattr("ai_convos_remote.request",request)
    with pytest.raises(ConnectionError,match="response lost"): sync_once(root,True)
    state=connect(root/"remote/state.db"); assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1 and not state.execute("SELECT 1 FROM replica_receipts").fetchone() and "replica_outbox" not in {r[0] for r in state.execute("SELECT name FROM sqlite_master WHERE type='table'")}; state.close(); monkeypatch.setattr("ai_convos_remote.request",direct); sync_once(root,True); state=connect(root/"remote/state.db"); assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==1; state.close()


def test_replica_alone_recovers_row_and_original_proof(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); write_archive(a/"data/convos.db","replica only"); before=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; sync_once(a,True); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1; sync_once(b,True)
    db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchone()==("c","replica only") and not db.execute("SELECT * FROM remote.row_origins").fetchall() and db.execute("SELECT author_user_id FROM remote.row_proofs").fetchone()[0]==alice["user"]; db.close()


def test_foreign_holder_repairs_valid_rows_and_blocks_drifted_rows(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); alice=load(a); path=a/"data/convos.db"; write_archive(path,"survives through bob"); db=duckdb.connect(str(path)); db.execute("INSERT INTO messages VALUES ('m','c','user','keep the relationship',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.close(); sa=connect(a/"remote/state.db"); core=duckdb.connect(str(path),read_only=True); records=scan(core,sa); core.close(); projection_module.attest_rows(path,alice,team,records); envs=projection_module.row_replicas(path,alice,team,records,{2:key(alice,team,2)}); action(server,{"op":"replica_upload_many","envelopes":envs},alice["token"]); sb=connect(b/"remote/state.db"); pull(load(b),sb,b); db=duckdb.connect(str(b/"data/convos.db")); db.execute("UPDATE messages SET content='drifted'"); db.close(); server.execute("DELETE FROM row_replicas"); server.commit(); bob=load(b); blocked=[]; repaired=projection_module.row_replicas(b/"data/convos.db",bob,team,[],{2:key(bob,team,2)},inventory=lambda ids:set(),blocked=blocked)
    assert blocked==[("messages","m")] and len(repaired)==1 and {e["uploader"] for e in repaired}=={bob["device"]["id"]} and all(r["created"] for r in action(server,{"op":"replica_upload_many","envelopes":repaired},bob["token"])["replicas"]) and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1

def test_repull_replaces_received_rows_and_preserves_local_rows(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); alice=load(a); source=a/"data/convos.db"; write_archive(source,"remote original"); db=duckdb.connect(str(source)); db.execute("INSERT INTO messages VALUES ('m','c','user','signed original',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.close(); state=connect(a/"remote/state.db"); core=duckdb.connect(str(source),read_only=True); records=scan(core,state); core.close(); projection_module.attest_rows(source,alice,team,records); envs=projection_module.row_replicas(source,alice,team,records,{2:key(alice,team,2)}); action(server,{"op":"replica_upload_many","envelopes":envs},alice["token"]); state.close(); state=connect(b/"remote/state.db"); pull(load(b),state,b); state.close()
    path=b/"data/convos.db"; db=duckdb.connect(str(path)); received=db.execute("SELECT physical_row_id FROM remote.row_origins WHERE table_name='messages' AND source_row_id='m'").fetchone()[0]; project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],["local","codex","local kept",None,None,None,None,None,None,"{}"]); project_archive_row(db,"messages",ARCHIVE_COLUMNS["messages"],["local-m","local","user","local kept",None,None,None,"{}",None]); db.execute("UPDATE messages SET content='local drift' WHERE id=?",(received,)); db.execute("INSERT INTO tool_calls(id,message_id,tool_name,input,output,status) VALUES ('derived',?,'write','{}','{}','complete')",(received,)); orphan=core_module.remote_id(alice["user"],"conversations","orphan"); project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],[orphan,"codex","relay orphan",None,None,None,None,None,None,"{}"],{"workspace_id":team,"author_user_id":alice["user"],"author_device_id":alice["device"]["id"],"source_row_id":"orphan","source_event_id":"missing","content_key":"conversations:orphan","observed_at":None}); db.close(); state=connect(b/"remote/state.db"); state.execute("INSERT OR REPLACE INTO meta VALUES (?,'1')",(f"replica_repair:{team}",)); state.execute("UPDATE sync_states SET lifecycle='blocked',error='old mismatch' WHERE workspace=?",(team,)); state.commit(); state.close()
    removed,audit=repull_once(b); assert removed["conversations"]==2 and removed["messages"]==1 and removed["tool_calls"]==1 and sum(audit["totals"].get(k,0) for k in ("projection_mismatch","projection_missing","proof_missing"))==0 and not list((b/"data").glob("convos.db.pre-remote-repull-*.bak*"))
    db=duckdb.connect(str(path),read_only=True); snapshot={table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in ("conversations","messages","tool_calls")}; assert db.execute("SELECT title FROM conversations WHERE id='local'").fetchone()[0]=="local kept" and db.execute("SELECT content FROM messages WHERE id='local-m'").fetchone()[0]=="local kept" and db.execute("SELECT content FROM messages WHERE id<>'local-m'").fetchone()[0]=="signed original" and not db.execute("SELECT 1 FROM conversations WHERE id=?",(orphan,)).fetchone() and not db.execute("SELECT 1 FROM tool_calls WHERE id='derived'").fetchone(); db.close(); state=connect(b/"remote/state.db"); assert not state.execute("SELECT 1 FROM meta WHERE key=?",(f"replica_repair:{team}",)).fetchone() and not state.execute("SELECT 1 FROM sync_states WHERE lifecycle<>'ready'").fetchone(); state.close()
    repull_once(b); db=duckdb.connect(str(path),read_only=True); assert snapshot=={table:db.execute(f"SELECT * FROM {table} ORDER BY id").fetchall() for table in snapshot}; db.close()

def test_failed_repull_retains_backup_and_is_retryable(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); write_archive(a/"data/convos.db","remote original"); sync_once(a,True); sync_once(b,True); db=duckdb.connect(str(b/"data/convos.db")); project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],["local","codex","local kept",None,None,None,None,None,None,"{}"]); db.close()
    monkeypatch.setattr(remote_client,"request",lambda cfg,body,auth=True:(_ for _ in ()).throw(ConnectionError("relay interrupted")) if body["op"]=="replica_pull" else direct(cfg,body,auth))
    with pytest.raises(RuntimeError,match="Backup retained at .*relay interrupted"): repull_once(b)
    backups=list((b/"data").glob("convos.db.pre-remote-repull-*.bak")); assert len(backups)==1 and backups[0].with_name(backups[0].name+".attachments").is_dir(); db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT title FROM conversations WHERE id='local'").fetchone()[0]=="local kept"; db.close()
    monkeypatch.setattr(remote_client,"request",direct); repull_once(b); db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert {r[0] for r in db.execute("SELECT title FROM conversations").fetchall()}=={"local kept","remote original"}; db.close(); assert backups[0].is_file()


def test_disjoint_peers_refound_union_on_fresh_relay(tmp_path,monkeypatch):
    old=server_connect(tmp_path/"old.db"); monkeypatch.setattr("ai_convos_remote.request",transport(old)); roots={name:tmp_path/name for name in ("alice","bob","carol")}; clients={name:setup_client("http://old",name,root=root)[0] for name,root in roots.items()}; origin=create(clients["alice"],"Old Team","team",roots["alice"]); add_member(clients["alice"],origin,"bob",root=roots["alice"]); add_member(clients["alice"],origin,"carol",root=roots["alice"]); [replicate_conversation(roots[name],origin,f"from {name}",name) for name in roots]
    fresh=server_connect(tmp_path/"fresh.db"); monkeypatch.setattr("ai_convos_remote.request",transport(fresh)); clients={name:rehome_client(load(root),"http://fresh",root)[0] for name,root in roots.items()}; mallory,_=setup_client("http://fresh","mallory",root=tmp_path/"mallory"); replacement=create(clients["alice"],"Replacement","team",roots["alice"]); add_member(clients["alice"],replacement,"bob",root=roots["alice"]); add_member(clients["alice"],replacement,"carol",root=roots["alice"]); uploaded=[]
    for name in ("carol","alice","bob"):
        root=roots[name]; cfg=load(root); refresh(cfg,root); state=connect(root/"remote/state.db"); bind_origin(cfg,state,replacement,origin,root); epoch=state.execute("SELECT epoch FROM origin_bindings WHERE workspace=? AND origin=?",(replacement,origin)).fetchone()[0]; envs=projection_module.row_replicas(root/"data/convos.db",cfg,replacement,[conversation(f"from {name}",name)],{cfg["workspaces"][replacement]["epoch"]:key(cfg,replacement,cfg["workspaces"][replacement]["epoch"])},origins={origin},origin_epochs={origin:epoch}); state.close(); assert len(envs)==1; uploaded+=envs; action(fresh,{"op":"replica_upload_many","envelopes":envs},cfg["token"])
    with pytest.raises(PermissionError,match="access denied"): action(fresh,{"op":"replica_upload_many","envelopes":[uploaded[0]|{"uploader":mallory["device"]["id"]}]},mallory["token"])
    state=connect(roots["alice"]/"remote/state.db"); pull(load(roots["alice"]),state,roots["alice"]); state.close(); db=duckdb.connect(str(roots["alice"]/"data/convos.db"),read_only=True); assert {r[0] for r in db.execute("SELECT title FROM conversations").fetchall()}=={f"from {name}" for name in roots} and {r[0] for r in db.execute("SELECT DISTINCT author_user_id FROM remote.row_proofs WHERE workspace_id=?",(origin,)).fetchall()}=={clients[name]["user"] for name in roots}; db.close(); assert fresh.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==3


def test_unchanged_archive_repairs_cleared_relay_despite_local_receipts(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); write_archive(root/"data/convos.db","recover me"); sync_once(root,True); state=connect(root/"remote/state.db"); receipts=state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]; state.close(); assert receipts==server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1; server.execute("DELETE FROM row_replicas"); server.execute("DELETE FROM replica_usage"); server.commit(); sync_once(root)
    assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==1 and connect(root/"remote/state.db").execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==1


def test_explicit_sync_repairs_interior_row_and_blob_loss(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); data=root/"data"; data.mkdir(); db=duckdb.connect(str(data/"convos.db")); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','repair','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); db.execute("INSERT INTO messages VALUES ('m','c','user','two files',NULL,'2026-01-01',NULL,'{}',NULL,NULL)")
    for i,payload in enumerate((b"first",b"second")):
        source=tmp_path/f"{i}.bin"; source.write_bytes(payload); db.execute("INSERT INTO attachments VALUES (?,?,?,'application/octet-stream',?,?,NULL,'2026-01-01')",(str(i),"m",source.name,len(payload),str(source))); index_attachment_body(db,str(i),source,len(payload))
    db.close(); sync_once(root,True); expected=(server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0],server.execute("SELECT COUNT(*) FROM blob_replicas").fetchone()[0]); assert expected==(4,2); server.execute("DELETE FROM row_replicas WHERE cursor=(SELECT MIN(cursor) FROM row_replicas)"); server.execute("DELETE FROM blob_replicas WHERE cursor=(SELECT MIN(cursor) FROM blob_replicas)"); server.commit(); sync_once(root); assert (server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0],server.execute("SELECT COUNT(*) FROM blob_replicas").fetchone()[0])==(3,1); sync_once(root,True); assert (server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0],server.execute("SELECT COUNT(*) FROM blob_replicas").fetchone()[0])==expected


def test_holder_refounds_lost_team_on_fresh_relay_without_excluded_member(tmp_path,monkeypatch):
    old=server_connect(tmp_path/"old.db"); monkeypatch.setattr("ai_convos_remote.request",transport(old)); a,b=tmp_path/"alice",tmp_path/"bob"; alice,_=setup_client("http://old","alice",root=a); setup_client("http://old","bob",root=b); origin=create(alice,"Old Team","team",a); add_member(alice,origin,"bob",root=a); alice=load(a); path=a/"data/convos.db"; write_archive(path,"portable original"); repo=a/"repo"; repo.mkdir(); subprocess.run(("git","-C",str(repo),"init","-q"),check=True); subprocess.run(("git","-C",str(repo),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo),"config","user.name","A"),check=True); (repo/"a.py").write_text("portable\n"); subprocess.run(("git","-C",str(repo),"add","."),check=True); subprocess.run(("git","-C",str(repo),"commit","-qm","initial"),check=True); db=duckdb.connect(str(path)); db.execute("UPDATE conversations SET cwd=?",[str(repo)]); db.execute("INSERT INTO messages VALUES ('m','c','user','portable child',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','portable\n','2026-01-01',NULL)",[str(repo/"a.py")]); db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL)"); db.close(); capture_provenance(path); sa=connect(a/"remote/state.db"); core=duckdb.connect(str(path),read_only=True); records=scan(core,sa); core.close(); projection_module.attest_rows(path,alice,origin,records); envs=projection_module.row_replicas(path,alice,origin,records,{2:key(alice,origin,2)}); action(old,{"op":"replica_upload_many","envelopes":envs},alice["token"]); pull(load(b),connect(b/"remote/state.db"),b)
    fresh=server_connect(tmp_path/"fresh.db"); monkeypatch.setattr("ai_convos_remote.request",transport(fresh)); bob,_=rehome_client(load(b),"http://fresh",b); carol,_=setup_client("http://fresh","carol",root=tmp_path/"carol"); mallory,_=setup_client("http://fresh","mallory",root=tmp_path/"mallory"); replacement=create(bob,"Replacement","team",b); add_member(bob,replacement,"carol",root=b); bob=load(b); state=connect(b/"remote/state.db"); assert bind_origin(bob,state,replacement,origin,b)==2; state.close(); sync_once(b,True); sync_once(tmp_path/"carol",True)
    db=duckdb.connect(str(tmp_path/"carol/data/convos.db"),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="portable original" and db.execute("SELECT content FROM messages").fetchone()[0]=="portable child" and db.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]==1 and db.execute("SELECT COUNT(*) FROM provenance.git_checkpoints").fetchone()[0]==1 and set(db.execute("SELECT workspace_id,authorization_workspace_id,author_user_id FROM remote.row_proofs").fetchall())=={(origin,origin,alice["user"])}; db.close(); assert fresh.execute("SELECT COUNT(*) FROM origin_bundles").fetchone()[0]==1 and fresh.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==len(records)
    with pytest.raises(PermissionError,match="access denied"): action(fresh,{"op":"origin_pull","workspace":replacement},mallory["token"])


def test_second_generation_refound_carries_authorization_dependency(tmp_path,monkeypatch):
    first=server_connect(tmp_path/"first.db"); monkeypatch.setattr("ai_convos_remote.request",transport(first)); a,b,c=tmp_path/"alice",tmp_path/"bob",tmp_path/"carol"; alice,_=setup_client("http://first","alice",root=a); setup_client("http://first","bob",root=b); origin=create(alice,"Origin","team",a); add_member(alice,origin,"bob",root=a); replicate_conversation(a,origin,"origin")
    second=server_connect(tmp_path/"second.db"); monkeypatch.setattr("ai_convos_remote.request",transport(second)); alice,_=rehome_client(load(a),"http://second",a); setup_client("http://second","bob",root=b); successor=create(alice,"Successor","team",a); add_member(alice,successor,"bob",root=a); state=connect(a/"remote/state.db"); bind_origin(load(a),state,successor,origin,a); state.close(); replicate_conversation(a,successor,"continued"); pull(load(b),connect(b/"remote/state.db"),b)
    third=server_connect(tmp_path/"third.db"); monkeypatch.setattr("ai_convos_remote.request",transport(third)); bob,_=rehome_client(load(b),"http://third",b); setup_client("http://third","carol",root=c); replacement=create(bob,"Replacement","team",b); add_member(bob,replacement,"carol",root=b); state=connect(b/"remote/state.db"); bind_origin(load(b),state,replacement,origin,b); state.close(); sync_once(b,True); sync_once(c,True); db=duckdb.connect(str(c/"data/convos.db"),read_only=True)
    assert db.execute("SELECT title FROM conversations").fetchone()[0]=="continued" and {r[0] for r in db.execute("SELECT DISTINCT workspace_id FROM remote.workspace_controls").fetchall()}>={origin,successor} and third.execute("SELECT COUNT(*) FROM origin_bundles").fetchone()[0]==2


def test_personal_recovery_multidevice_delivery_and_replay(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"
    alice,recovery=setup_client("http://server","alice","laptop",root=a); ws=workspace(alice,"Personal"); replicate_conversation(a,ws)
    desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); state_b=connect(b/"remote/state.db"); pull(desktop,state_b,b); count=server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]; pull(desktop,state_b,b)
    db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT title FROM conversations").fetchall()==[("shared",)] and db.execute("SELECT author_user_id FROM remote.row_proofs").fetchone()[0]==alice["user"]; db.close(); assert server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==count
    assert len(load(b)["keys"])==2 and server.execute("SELECT epoch FROM workspaces WHERE id=?",(ws,)).fetchone()[0]==2
    assert os.stat(a/"remote").st_mode&0o777==0o700 and os.stat(a/"remote/config.json").st_mode&0o777==0o600 and os.stat(a/"remote/state.db").st_mode&0o777==0o600


def test_epoch_boundary_flushes_pending_events_before_signing(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); state=connect(a/"remote/state.db"); event_id=publish(alice,state,team,policy("pending"),a); state.close(); add_member(alice,team,"bob",root=a); control=load(a)["controls"][team]
    cursor,seq=server.execute("SELECT cursor,seq FROM events WHERE event=?",(event_id,)).fetchone(); assert control["boundary"]["heads"][alice["device"]["id"]]=={"seq":seq,"event":event_id} and control["boundary"]["tail"]==cursor


def test_deleted_state_adopts_an_intact_import_only_archive(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); ws=workspace(alice,"Personal"); replicate_conversation(a,ws); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); state=connect(b/"remote/state.db"); pull(desktop,state,b); state.close(); before=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; state_path=b/"remote/state.db"; state_path.unlink(); sync_once(b,True)
    db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==1; db.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before


def test_interrupted_archive_mode_is_bound_to_exact_core_identity(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); refresh(cfg,root); first=write_archive(root/"data/convos.db","first"); state=connect(root/"remote/state.db"); remote_client.prepare_archive(cfg,state,root); basis=lambda:json.loads(state.execute("SELECT value FROM meta WHERE key LIKE 'archive_basis:%'").fetchone()[0]); assert basis()["id"]==first[0]; replacement=tmp_path/"replacement.db"; second=write_archive(replacement,"second"); shutil.copyfile(replacement,root/"data/convos.db"); remote_client.prepare_archive(cfg,state,root)
    assert first[0]!=second[0] and basis()["id"]==second[0]


def test_personal_sync_automatically_bridges_encrypted_memory_between_devices(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b)
    monkeypatch.delenv("CONVOS_MEMORY_DB",raising=False); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); created=memory_module.remember_data("relay cannot read this","global"); sync_once(a,True); wire="".join(r[0] for r in server.execute("SELECT envelope FROM events").fetchall())
    wire+="".join(r[0] for r in server.execute("SELECT envelope FROM row_replicas").fetchall()); assert "relay cannot read this" not in wire and str(a) not in wire
    sync_once(b,True); db=sqlite3.connect(b/"memory/state.db"); assert db.execute("SELECT content FROM canonicals").fetchall()==[("relay cannot read this",)]; db.close()
    large="second device-safe revision\n"+"x"*70000; memory_module.remember_data(large,"global",created["id"]); sync_once(a,True); sync_once(b,True); count=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; sync_once(a,True); sync_once(b,True)
    db=sqlite3.connect(b/"memory/state.db"); assert db.execute("SELECT content FROM canonicals").fetchall()==[(large,)] and db.execute("SELECT state,COUNT(*) FROM remote_semantics GROUP BY state").fetchall()==[("active",1)]; db.close(); state=connect(b/"remote/state.db"); assert state.execute("SELECT COUNT(*) FROM lazy_events").fetchone()[0]==0; state.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==count and "second device-safe revision" not in "".join(r[0] for r in server.execute("SELECT envelope FROM row_replicas").fetchall())
    memory_module.forget_data(created["id"],"global"); sync_once(a,True); sync_once(b,True); db=sqlite3.connect(b/"memory/state.db"); assert db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0]==db.execute("SELECT COUNT(*) FROM sources").fetchone()[0]==0 and db.execute("SELECT state,body FROM remote_semantics").fetchone()[0]=="deleted"; db.close(); assert b"second device-safe revision" not in (b/"remote/state.db").read_bytes()
    recreated=memory_module.remember_data(large,"global"); assert recreated["id"]!=created["id"]; sync_once(a,True); sync_once(b,True); db=sqlite3.connect(b/"memory/state.db"); assert db.execute("SELECT content FROM canonicals").fetchall()==[(large,)]; db.close()


def test_lost_semantic_replica_response_retries_without_resurrecting_forgotten_memory(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a=tmp_path/"a"; setup_client("http://server","alice",root=a); monkeypatch.delenv("CONVOS_MEMORY_DB",raising=False); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); created=memory_module.remember_data("replica retry secret","global"); sync_once(a,True); memory_module.forget_data(created["id"],"global"); before=server.execute("SELECT COUNT(*) FROM semantic_replicas").fetchone()[0]; lost=[False]
    def request_lost(cfg,body,auth=True):
        result=direct(cfg,body,auth)
        if body["op"]=="replica_upload_many" and not lost[0]: lost[0]=True; raise ConnectionError("replica response lost")
        return result
    monkeypatch.setattr("ai_convos_remote.request",request_lost)
    with pytest.raises(ConnectionError,match="response lost"): sync_once(a,True)
    assert server.execute("SELECT COUNT(*) FROM semantic_replicas").fetchone()[0]==before+1
    monkeypatch.setattr("ai_convos_remote.request",direct); sync_once(a,True); assert server.execute("SELECT COUNT(*) FROM semantic_replicas").fetchone()[0]==before+1; db=sqlite3.connect(a/"memory/state.db"); assert db.execute("SELECT state FROM remote_semantics").fetchall()==[("deleted",)]; db.close()


def test_same_device_rebuilds_lost_memory_ledger_and_can_revise_and_forget(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a=tmp_path/"a"; setup_client("http://server","alice","laptop",root=a); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); created=memory_module.remember_data("recover my memory","global"); sync_once(a,True); (a/"memory/state.db").unlink(); sync_once(a,True); db=sqlite3.connect(a/"memory/state.db"); assert db.execute("SELECT id,content FROM canonicals").fetchone()==(created["id"],"recover my memory"); db.close()
    assert memory_module.remember_data("revised after recovery","global",created["id"])["status"]=="revised"; sync_once(a,True); memory_module.forget_data(created["id"],"global"); sync_once(a,True); db=sqlite3.connect(a/"memory/state.db"); assert not db.execute("SELECT 1 FROM canonicals").fetchone() and db.execute("SELECT state FROM remote_semantics").fetchall()==[("deleted",)]; db.close()


def test_relay_cannot_fabricate_semantic_replica_or_mutate_memory(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); monkeypatch.delenv("CONVOS_MEMORY_DB",raising=False); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.remember_data("relay forgery target","global"); sync_once(a,True); ws=workspace(load(b),"Personal")
    def forged(cfg,body,auth=True):
        result=copy.deepcopy(direct(cfg,body,auth))
        if body["op"]=="replica_pull" and result["replicas"]: ciphertext=result["replicas"][0]["envelope"]["ciphertext"]; result["replicas"][0]["envelope"]["ciphertext"]=("B" if ciphertext[0]=="A" else "A")+ciphertext[1:]
        return result
    monkeypatch.setattr("ai_convos_remote.request",forged)
    state=connect(b/"remote/state.db")
    with pytest.raises(ValueError,match="invalid row replica"): pull(load(b),state,b)
    state.close()
    state=connect(b/"remote/state.db"); assert state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked"; state.close(); db=sqlite3.connect(b/"memory/state.db"); assert not db.execute("SELECT 1 FROM canonicals").fetchone() and not db.execute("SELECT 1 FROM remote_semantics").fetchone(); db.close()
    monkeypatch.setattr("ai_convos_remote.request",direct); state=connect(b/"remote/state.db"); pull(load(b),state,b); state.close(); assert sqlite3.connect(b/"memory/state.db").execute("SELECT content FROM canonicals").fetchone()==("relay forgery target",)

def test_pull_can_block_bad_workspace_and_continue_healthy_workspaces(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); team=create(cfg,"Team","team",root); personal=workspace(cfg,"Personal"); attempted=[]
    monkeypatch.setattr(remote_client,"pull_origins",lambda *args:set()); monkeypatch.setattr(remote_client,"pull_blobs",lambda *args:0)
    def replicas(cfg,state,root,ws,*args):
        attempted.append(ws["id"])
        if ws["id"]==personal: raise ValueError("row replica proof mismatch")
        return 0
    monkeypatch.setattr(remote_client,"pull_row_replicas",replicas); state=connect(root/"remote/state.db"); summary=pull(cfg,state,root,keep_going=True)
    assert set(attempted)=={personal,team} and str(summary[personal]["error"])=="row replica proof mismatch" and "error" not in summary[team]
    assert dict(state.execute("SELECT workspace,lifecycle FROM sync_states").fetchall())=={personal:"blocked",team:"ready"} and state.execute("SELECT error FROM sync_states WHERE workspace=?",(personal,)).fetchone()[0]=="row replica proof mismatch"; state.close()

def test_pull_never_treats_query_cancellation_as_a_workspace_failure(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); create(cfg,"Team","team",root); attempted=[]
    monkeypatch.setattr(remote_client,"pull_origins",lambda *args:set()); monkeypatch.setattr(remote_client,"pull_blobs",lambda *args:0)
    def cancelled(cfg,state,root,ws,*args): attempted.append(ws["id"]); raise duckdb.InterruptException("Interrupted")
    monkeypatch.setattr(remote_client,"pull_row_replicas",cancelled); state=connect(root/"remote/state.db")
    with pytest.raises(duckdb.InterruptException): pull(cfg,state,root,keep_going=True)
    assert len(attempted)==1; state.close()

def test_duckdb_query_cancellation_stops_manual_and_watched_sync(monkeypatch,capsys):
    monkeypatch.setattr(remote_client,"sync_once",lambda *args,**kwargs:(_ for _ in ()).throw(duckdb.InterruptException("Interrupted")))
    with pytest.raises(remote_client.typer.Exit): remote_client.sync_cmd()
    assert capsys.readouterr().err=="Error: Interrupted\n"
    with pytest.raises(KeyboardInterrupt): remote_client.watch()

def test_sync_reports_partial_failure_after_finishing_healthy_workspace(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); team=create(cfg,"Team","team",root); personal=workspace(cfg,"Personal"); monkeypatch.setattr(remote_client,"pull_origins",lambda *args:set()); monkeypatch.setattr(remote_client,"pull_blobs",lambda *args:0)
    def replicas(cfg,state,root,ws,*args):
        if ws["id"]==personal: raise ValueError("row replica proof mismatch")
        return 0
    monkeypatch.setattr(remote_client,"pull_row_replicas",replicas)
    with pytest.raises(RuntimeError,match="completed partially.*Personal: row replica proof mismatch"): sync_once(root)
    state=connect(root/"remote/state.db"); assert dict(state.execute("SELECT workspace,lifecycle FROM sync_states").fetchall())=={personal:"blocked",team:"ready"} and not state.execute("SELECT value FROM meta WHERE key='last_sync'").fetchone(); state.close()


def test_later_uploader_copy_heals_poisoned_replica_across_pages(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); monkeypatch.delenv("CONVOS_MEMORY_DB",raising=False); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.remember_data("healed delivery copy","global"); sync_once(a,True); cfg=load(b); ws=workspace(cfg,"Personal"); poison_cursor,raw=server.execute("SELECT cursor,envelope FROM semantic_replicas ORDER BY cursor").fetchone(); original=json.loads(raw); body=open_replica(original,key(cfg,ws,original["epoch"])); repaired=seal_replica(body["row"],body["proof"],ws,original["epoch"],key(cfg,ws,original["epoch"]),cfg["device"]["id"]); original["ciphertext"]=("A" if original["ciphertext"][0]!="A" else "B")+original["ciphertext"][1:]; server.execute("UPDATE semantic_replicas SET envelope=?",(json.dumps(original),)); server.commit()
    def paged(cfg,request_,auth=True): return direct(cfg,request_|({"limit":1} if request_["op"]=="replica_pull" else {}),auth)
    monkeypatch.setattr("ai_convos_remote.request",paged); state=connect(b/"remote/state.db")
    with pytest.raises(ValueError,match="no valid delivery copy"): pull(cfg,state,b)
    assert state.execute("SELECT value FROM meta WHERE key=?",(f"replica_cursor:{ws}",)).fetchone()[0]==str(poison_cursor-1) and state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked" and not state.execute("SELECT 1 FROM replica_receipts").fetchone(); action(server,{"op":"replica_upload_many","envelopes":[repaired]},cfg["token"]); pull(cfg,state,b); tail=server.execute("SELECT MAX(cursor) FROM row_replicas").fetchone()[0]
    assert state.execute("SELECT value FROM meta WHERE key=?",(f"replica_cursor:{ws}",)).fetchone()[0]==str(tail) and state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==1 and state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="ready"; pull(cfg,state,b); state.close(); db=sqlite3.connect(b/"memory/state.db"); assert db.execute("SELECT content FROM canonicals").fetchall()==[("healed delivery copy",)] and db.execute("SELECT COUNT(*) FROM remote_semantics").fetchone()[0]==1; db.close()


def test_later_uploader_copy_heals_poisoned_blob_across_pages(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); path=a/"data/convos.db"; write_archive(path,"blob"); db=duckdb.connect(str(path)); db.execute("INSERT INTO messages VALUES ('m','c','user','file',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); source=tmp_path/"body.bin"; source.write_bytes(b"healed blob body"); db.execute("INSERT INTO attachments VALUES ('a','m','body.bin','application/octet-stream',?,?,NULL,'2026-01-01')",(source.stat().st_size,str(source))); index_attachment_body(db,"a",source,source.stat().st_size); db.close(); sync_once(a,True); cfg=load(b); ws=workspace(cfg,"Personal"); item=action(server,{"op":"blob_pull","workspace":ws},cfg["token"])["blobs"][0]; poison_cursor,original=item["cursor"],item["envelope"]; data,body_hash=open_blob(original,key(cfg,ws,original["epoch"])); repaired=seal_blob(data,ws,original["epoch"],key(cfg,ws,original["epoch"]),cfg["device"]["id"]); raw=bytearray(server.execute("SELECT ciphertext FROM blob_replicas").fetchone()[0]); raw[0]^=1; server.execute("UPDATE blob_replicas SET ciphertext=?",(bytes(raw),)); server.commit()
    def paged(cfg,request_,auth=True): return direct(cfg,request_|({"limit":1} if request_["op"]=="blob_pull" else {}),auth)
    monkeypatch.setattr("ai_convos_remote.request",paged); state=connect(b/"remote/state.db")
    with pytest.raises(ValueError,match="no valid delivery copy"): pull(cfg,state,b)
    assert state.execute("SELECT value FROM meta WHERE key=?",(f"blob_cursor:{ws}",)).fetchone()[0]==str(poison_cursor-1); action(server,{"op":"blob_upload","envelope":repaired},cfg["token"]); pull(cfg,state,b); state.close(); db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT content_hash FROM attachment_bodies").fetchone()[0]==body_hash; db.close()


def test_later_uploader_copy_heals_poisoned_origin_bundle(tmp_path,monkeypatch):
    old=server_connect(tmp_path/"old.db"); monkeypatch.setattr("ai_convos_remote.request",transport(old)); a,b,c=tmp_path/"alice",tmp_path/"bob",tmp_path/"carol"; alice,_=setup_client("http://old","alice",root=a); setup_client("http://old","bob",root=b); origin=create(alice,"Origin","team",a); add_member(alice,origin,"bob",root=a); replicate_conversation(a,origin); pull(load(b),connect(b/"remote/state.db"),b)
    fresh=server_connect(tmp_path/"fresh.db"); direct=transport(fresh); monkeypatch.setattr("ai_convos_remote.request",direct); bob,_=rehome_client(load(b),"http://fresh",b); setup_client("http://fresh","carol",root=c); replacement=create(bob,"Replacement","team",b); add_member(bob,replacement,"carol",root=b); bob=load(b); state=connect(b/"remote/state.db"); bind_origin(bob,state,replacement,origin,b); state.close(); carol=load(c); server_state=refresh(carol,c); raw=fresh.execute("SELECT envelope FROM origin_bundles").fetchone()[0]; original=json.loads(raw); body=open_origin(original,key(carol,replacement,original["epoch"])); repaired=seal_origin(body["controls"],replacement,original["epoch"],key(carol,replacement,original["epoch"]),carol["device"]["id"],body["rows"]); original["ciphertext"]=("A" if original["ciphertext"][0]!="A" else "B")+original["ciphertext"][1:]; fresh.execute("UPDATE origin_bundles SET envelope=?",(json.dumps(original),)); fresh.commit(); action(fresh,{"op":"origin_upload","envelope":repaired},carol["token"]); state=connect(c/"remote/state.db"); ws=next(w for w in server_state["workspaces"] if w["id"]==replacement)
    real_core=remote_client._core
    @__import__("contextlib").contextmanager
    def broken(root,**kwargs):
        with real_core(root,**kwargs) as db:
            class Proxy:
                def __getattr__(self,name): return getattr(db,name)
                def execute(self,sql,*args):
                    if sql=="COMMIT": raise RuntimeError("commit failed")
                    return db.execute(sql,*args)
            yield Proxy()
    monkeypatch.setattr(remote_client,"_core",broken)
    with pytest.raises(RuntimeError,match="commit failed"): pull_origins(carol,state,c,ws)
    assert not state.execute("SELECT 1 FROM origin_bindings WHERE workspace=?",(replacement,)).fetchone()
    monkeypatch.setattr(remote_client,"_core",real_core)
    class BrokenState:
        def __getattr__(self,name): return getattr(state,name)
        def commit(self): raise RuntimeError("state commit failed")
    with pytest.raises(RuntimeError,match="state commit failed"): pull_origins(carol,BrokenState(),c,ws)
    assert duckdb.connect(str(c/"data/convos.db"),read_only=True).execute("SELECT COUNT(*) FROM remote.workspace_controls").fetchone()[0]>0 and not state.execute("SELECT 1 FROM origin_bindings WHERE workspace=?",(replacement,)).fetchone()
    assert pull_origins(carol,state,c,ws)=={origin} and state.execute("SELECT origin FROM origin_bindings WHERE workspace=?",(replacement,)).fetchone()[0]==origin; state.close()


def test_fresh_remote_and_memory_state_recovers_signed_tombstone_without_content(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,c=tmp_path/"a",tmp_path/"c"; _,recovery=setup_client("http://server","alice","laptop",root=a); monkeypatch.delenv("CONVOS_MEMORY_DB",raising=False); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); created=memory_module.remember_data("recover deleted history","global"); sync_once(a,True); memory_module.forget_data(created["id"],"global"); sync_once(a,True); setup_client("http://server","alice","fresh",recovery,root=c); sync_once(c,True); state=connect(c/"remote/state.db"); ws=workspace(load(c),"Personal"); assert state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="ready" and not state.execute("SELECT 1 FROM sequence_gaps").fetchone(); state.close(); db=sqlite3.connect(c/"memory/state.db"); assert not db.execute("SELECT 1 FROM canonicals").fetchone() and db.execute("SELECT state FROM remote_semantics").fetchall()==[("deleted",)]; db.close()


def test_device_certificates_reject_relay_key_substitution_without_auto_certifying(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,recovery=setup_client("http://server","alice",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); server.execute("DELETE FROM device_certificates WHERE device=?",(alice["device"]["id"],)); server.commit(); desktop,_=setup_client("http://server","alice","desktop",recovery,root=c); state=refresh(desktop,c); assert server.execute("SELECT COUNT(*) FROM device_certificates WHERE device IN (?,?)",(alice["device"]["id"],desktop["device"]["id"])).fetchone()[0]==1 and next(w for w in state["workspaces"] if w["kind"]=="personal")["device_authorized"]; attacker=identity("attacker")
    def tamper(op,device):
        def call(cfg,body,auth=True):
            result=copy.deepcopy(direct(cfg,body,auth))
            if body["op"]==op:
                for workspace_ in result.get("workspaces",[]):
                    for found in workspace_["devices"]:
                        if found["id"]==device: found["box_public"]=attacker["box_public"]
                for found in result.get("devices",[]): found["box_public"]=attacker["box_public"]
            return result
        return call
        monkeypatch.setattr("ai_convos_remote.request",tamper("directory",""))
        with pytest.raises(ValueError,match="certificate"): add_member(alice,team,"bob",root=a)
    monkeypatch.setattr("ai_convos_remote.request",direct); request_device(desktop,team,c,0); monkeypatch.setattr("ai_convos_remote.request",tamper("state",alice["device"]["id"]))
    assert approve_device(alice,team,desktop["device"]["id"],root=a)["approved"]
    monkeypatch.setattr("ai_convos_remote.request",direct); add_member(alice,team,bob["user"],root=a); server.execute("DELETE FROM device_certificates WHERE device=?",(bob["device"]["id"],)); server.commit(); add_member(alice,team,bob["user"],True,root=a); assert server.execute("SELECT active FROM members WHERE workspace=? AND user_id=?",(team,bob["user"])).fetchone()[0]==0
    mallory,_=setup_client("http://server","mallory",root=tmp_path/"m"); action(server,{"op":"certify","certificate":certificate(bob["root"],bob["user"],bob["device"])},bob["token"]); server.execute("UPDATE users SET name=? WHERE id=?",(bob["user"],mallory["user"])); server.commit(); add_member(alice,team,bob["user"],root=a); assert server.execute("SELECT active FROM members WHERE workspace=? AND user_id=?",(team,bob["user"])).fetchone()[0]==1 and not server.execute("SELECT 1 FROM members WHERE workspace=? AND user_id=?",(team,mallory["user"])).fetchone()
    def substitute(cfg,body,auth=True):
        result=copy.deepcopy(direct(cfg,{**body,"user":mallory["user"]},auth)) if body["op"]=="directory" else direct(cfg,body,auth)
        if body["op"]=="directory": result["users"][0]["name"]=bob["user"]
        return result
    monkeypatch.setattr("ai_convos_remote.request",substitute)
    with pytest.raises(ValueError,match="directory user"): add_member(alice,team,bob["user"],root=a)


@pytest.mark.parametrize("field,value",(("id","wrong"),("kind","personal"),("epoch",1)))
def test_refresh_rejects_relay_workspace_metadata(tmp_path,monkeypatch,field,value):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a)
    def tamper(cfg,body,auth=True):
        result=copy.deepcopy(direct(cfg,body,auth))
        if body["op"]=="state":
            found=next(w for w in result["workspaces"] if w["id"]==team); found[field]=value
        return result
    monkeypatch.setattr("ai_convos_remote.request",tamper)
    with pytest.raises(ValueError,match="metadata"): refresh(bob,b)
    assert team not in load(b)["workspaces"]


def test_refresh_rejects_relay_key_beyond_signed_history(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); old=alice["keys"][f"{team}:1"]; add_member(alice,team,"bob",root=a)
    def tamper(cfg,body,auth=True):
        result=copy.deepcopy(direct(cfg,body,auth))
        if body["op"]=="state":
            next(w for w in result["workspaces"] if w["id"]==team)["keys"].append({"epoch":1,"envelope":json.dumps(seal_key(unb64(old),bob["device"]["box_public"],f"workspace:{team}:epoch:1"))})
        return result
    monkeypatch.setattr("ai_convos_remote.request",tamper)
    with pytest.raises(ValueError,match="entitlement"): refresh(bob,b)


def test_relay_workspace_omission_stops_stale_upload(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a=tmp_path/"a"; alice,_=setup_client("http://server","alice",root=a); team=create(alice,"Team","team",a); state=connect(a/"remote/state.db"); baseline=server.execute("SELECT COUNT(*) FROM events WHERE workspace=?",(team,)).fetchone()[0]; pending=publish(alice,state,team,policy("must stay local"),a)
    def omit(cfg,body,auth=True):
        result=copy.deepcopy(direct(cfg,body,auth))
        if body["op"]=="state": result["workspaces"]=[w for w in result["workspaces"] if w["id"]!=team]
        return result
    monkeypatch.setattr("ai_convos_remote.request",omit); upload(alice,state,a)
    assert team in load(a)["workspaces"] and server.execute("SELECT COUNT(*) FROM events WHERE workspace=?",(team,)).fetchone()[0]==baseline and state.execute("SELECT COUNT(*) FROM outbox WHERE event=?",(pending,)).fetchone()[0]==1


def test_team_future_only_complete_history_and_removal(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"
    alice,_=setup_client("http://server","alice","laptop",root=a); bob,_=setup_client("http://server","bob","desktop",root=b); team=create(alice,"Team","team",a); sa,sb=connect(a/"remote/state.db"),connect(b/"remote/state.db")
    replicate_conversation(a,team,"before bob"); add_member(load(a),team,"bob",root=a); bob=load(b); pull(bob,sb,b); assert not (b/"data/convos.db").exists()
    replicate_conversation(a,team,"after bob","new"); bob=load(b); pull(bob,sb,b); assert duckdb.connect(str(b/"data/convos.db"),read_only=True).execute("SELECT title FROM conversations").fetchall()==[("after bob",)]
    alice=load(a); previous=alice["controls"][team]; members={**previous["members"],bob["user"]:{**previous["members"][bob["user"]],"history_from":1}}; control=control_body(alice,previous,key(alice,team,previous["epoch"]),"history",members=members); incomplete={"op":"grant_all","workspace":team,"user":bob["user"],"control":control,"envelopes":{}}
    with pytest.raises(ValueError,match="every workspace epoch"): action(server,sign_control(alice["device"],incomplete),alice["token"])
    future={**incomplete,"envelopes":{"999":{bob["device"]["id"]:{}}}}
    with pytest.raises(ValueError,match="outside"): action(server,sign_control(alice["device"],future),alice["token"])
    assert grant_all(alice,team,"bob",a)>=2; bob=load(b); pull(bob,sb,b); assert any(name.endswith(":1") for name in load(b)["keys"])
    add_member(alice,team,"bob",True,root=a); bob=load(b); pull(bob,sb,b); assert team not in {w["id"] for w in load(b)["server_state"]["workspaces"]} and f"{team}:3" not in load(b)["keys"]


def test_future_only_recovered_device_reasserts_explicit_sharing_preference(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,_=setup_client("http://server","alice",root=a); bob,recovery=setup_client("http://server","bob","laptop",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); bob=load(b); refresh(bob,b); state=connect(b/"remote/state.db"); configure_sharing(bob,state,team,False,["edit"],b); state.close(); desktop,_=setup_client("http://server","bob","desktop",recovery,root=c); request_device(desktop,team,c,0); approve_device(load(b),team,desktop["device"]["id"],root=b); sync_once(c); state=connect(c/"remote/state.db"); assert sharing(state,team,desktop["user"])["auto_contribute"] is False and sharing(state,team,desktop["user"])["match"]==["edit"] and load(c)["sharing"][team]=={"auto_contribute":False,"match":["edit"]}


def test_device_approval_does_not_republish_another_users_signed_preference(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db")
    monkeypatch.setattr("ai_convos_remote.request",transport(server))
    monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None)
    a,b,c=tmp_path/"alice",tmp_path/"bob",tmp_path/"recovered"
    alice,recovery=setup_client("http://server","alice","laptop",root=a)
    bob,_=setup_client("http://server","bob",root=b)
    recovered,_=setup_client("http://server","alice","recovered",recovery,root=c)
    team=create(alice,"Team","team",a)
    add_member(alice,team,"bob",root=a)
    bob=load(b)
    refresh(bob,b)
    state=connect(b/"remote/state.db")
    configure_sharing(bob,state,team,False,["edit"],b)
    state.close()
    sync_once(a)
    request_device(recovered,team,c,0)
    result=approve_device(load(a),team,recovered["device"]["id"],root=a)
    assert result["approved"] and result["history"]>0
    sync_once(c)
    remove_device(load(a),team,recovered["device"]["id"],a)
    sync_once(c)
    current=load(c)
    assert recovered["device"]["id"] in current["controls"][team]["removed"] and f"{team}:{current['workspaces'][team]['epoch']}" not in current["keys"]


def test_epoch_preference_retention_resumes_after_rotation_crash(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); state=connect(a/"remote/state.db"); configure_sharing(alice,state,team,False,["edit"],a); state.close(); real=remote_client.retain_sharing; monkeypatch.setattr(remote_client,"retain_sharing",lambda *args,**kwargs:0); add_member(load(a),team,"bob",root=a); monkeypatch.setattr(remote_client,"retain_sharing",real); before=server.execute("SELECT COUNT(*) FROM events WHERE workspace=? AND epoch=2",(team,)).fetchone()[0]; sync_once(a); after=server.execute("SELECT COUNT(*) FROM events WHERE workspace=? AND epoch=2",(team,)).fetchone()[0]; sync_once(a); assert after==before+1 and server.execute("SELECT COUNT(*) FROM events WHERE workspace=? AND epoch=2",(team,)).fetchone()[0]==after


def test_current_repository_policy_is_retained_for_future_only_member(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); repo=a/"repo"; repo.mkdir(); subprocess.run(("git","-C",str(repo),"init","-q"),check=True); subprocess.run(("git","-C",str(repo),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo),"config","user.name","A"),check=True); (repo/"a").write_text("x"); subprocess.run(("git","-C",str(repo),"add","."),check=True); subprocess.run(("git","-C",str(repo),"commit","-qm","initial"),check=True); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); remote_client.link_cmd(repo,"Team"); add_member(load(a),team,"bob",root=a); bob=load(b); state=connect(b/"remote/state.db"); pull(bob,state,b); assert tuple(state.execute("SELECT owner,kind FROM policies WHERE workspace=?",(team,)).fetchone())==(alice["user"],"repository")


def test_released_repository_policy_is_reauthored_before_future_only_delivery(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,_=setup_client("http://server","alice",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); repo=a/"repo"; repo.mkdir(); subprocess.run(("git","-C",str(repo),"init","-q"),check=True); subprocess.run(("git","-C",str(repo),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo),"config","user.name","A"),check=True); (repo/"a").write_text("x"); subprocess.run(("git","-C",str(repo),"add","."),check=True); subprocess.run(("git","-C",str(repo),"commit","-qm","initial"),check=True); observed=core_module.capture_repository(repo,a/"data/convos.db"); state=connect(a/"remote/state.db"); publish(alice,state,team,{"kind":"workspace.policy","entity":f"policy:repository:{observed['id']}","payload":{"kind":"repository","value":observed["id"]}},a); upload(alice,state,a); state.close(); sync_once(a); state=connect(a/"remote/state.db"); assert state.execute("SELECT evidence FROM policies WHERE workspace=? AND owner=? AND value=?",(team,alice["user"],observed["id"])).fetchone()[0] and state.execute("SELECT 1 FROM policy_proofs WHERE workspace=? AND owner=? AND value=?",(team,alice["user"],observed["id"])).fetchone(); state.close(); add_member(load(a),team,"bob",root=a); state=connect(b/"remote/state.db"); pull(load(b),state,b); assert state.execute("SELECT evidence FROM policies WHERE workspace=? AND owner=? AND value=?",(team,alice["user"],observed["id"])).fetchone()[0]


def test_path_promotion_runs_without_archive_mutation_and_converges_once(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); team=create(cfg,"Team","team",root); folder=root/"project"; folder.mkdir(); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(root)); remote_client.link_cmd(folder,"Team"); sync_once(root); before=archive_info(root)[1]; subprocess.run(("git","-C",str(folder),"init","-q"),check=True); subprocess.run(("git","-C",str(folder),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(folder),"config","user.name","A"),check=True); (folder/"a").write_text("x"); subprocess.run(("git","-C",str(folder),"add","."),check=True); subprocess.run(("git","-C",str(folder),"commit","-qm","initial"),check=True); assert archive_info(root)[1]==before; sync_once(root); state=connect(root/"remote/state.db"); assert state.execute("SELECT COUNT(*) FROM policies WHERE workspace=? AND kind='repository'",(team,)).fetchone()[0]==1; events=server.execute("SELECT COUNT(*) FROM events WHERE workspace=?",(team,)).fetchone()[0]; sync_once(root); assert server.execute("SELECT COUNT(*) FROM events WHERE workspace=?",(team,)).fetchone()[0]==events
def test_path_bindings_are_rescued_to_config_before_state_cutover(tmp_path):
    path=tmp_path/"state.db"; db=sqlite3.connect(path); db.execute("CREATE TABLE policies(workspace TEXT,kind TEXT,value TEXT,local_root TEXT)"); db.execute("INSERT INTO policies VALUES ('w','path','token','/local/worktree'),('w','repository','repo','/ignored')"); db.commit(); db.close(); cfg={"bindings":{}}
    assert rescue_bindings(cfg,path,tmp_path)==1 and cfg["bindings"]=={"w:path:token":"/local/worktree"} and json.loads((tmp_path/"remote/config.json").read_text())["bindings"]==cfg["bindings"]


def test_released_untyped_config_binding_is_rescued_before_state_cutover(tmp_path):
    path=tmp_path/"state.db"; db=sqlite3.connect(path); db.execute("CREATE TABLE policies(workspace TEXT,owner TEXT,kind TEXT,value TEXT)"); db.execute("INSERT INTO policies VALUES ('w','u','path','token')"); db.commit(); db.close(); cfg={"bindings":{"w:token":"/local/worktree"}}; assert rescue_bindings(cfg,path,tmp_path)==1 and cfg["bindings"]=={"w:path:token":"/local/worktree"}


def test_unknown_required_event_blocks_ready_without_storing_content(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); ws=workspace(desktop,"Personal"); source=connect(b/"remote/state.db"); value=inject(desktop,source,server,ws,"future.opaque")
    target=connect(a/"remote/state.db")
    with pytest.raises(ValueError,match="required event is unsupported"): pull(load(a),target,a)
    assert tuple(target.execute("SELECT kind,payload_v,required FROM deferred_events WHERE event=?",(value["id"],)).fetchone())==("future.opaque",1,1) and target.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked" and b"new_field" not in (a/"remote/state.db").read_bytes() and action(server,{"op":"fetch","workspace":ws,"event":value["id"]},desktop["token"])["envelope"]["event"]==value["id"]


def test_uninstalled_semantic_bridge_skips_object_then_projects_after_install(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.remember_data("install bridge later","global"); sync_once(a,True); real=projection_module.bridges; monkeypatch.setattr(projection_module,"bridges",lambda:[]); target=connect(b/"remote/state.db"); ws=workspace(load(b),"Personal"); result=pull(load(b),target,b); assert result[ws]["replicas"]==1 and not target.execute("SELECT 1 FROM replica_receipts").fetchone() and not (b/"memory/state.db").exists()
    monkeypatch.setattr(projection_module,"bridges",real); result=pull(load(b),target,b); target.close(); assert result[ws]["replicas"]==1 and sqlite3.connect(b/"memory/state.db").execute("SELECT content FROM canonicals").fetchone()==("install bridge later",)


def test_large_record_is_fetched_during_convergent_pull(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); state_a,state_b=connect(a/"remote/state.db"),connect(b/"remote/state.db")
    replicate_conversation(a,ws,"x"*70000); result=pull(desktop,state_b,b); assert state_b.execute("SELECT COUNT(*) FROM lazy_events").fetchone()[0]==0 and result[ws]["cursor"]==result[ws]["tail"] and duckdb.connect(str(b/"data/convos.db"),read_only=True).execute("SELECT length(title) FROM conversations").fetchone()[0]==70000


def test_lazy_fetch_rejects_swapped_envelope(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); sa,sb=connect(a/"remote/state.db"),connect(b/"remote/state.db"); [publish(alice,sa,ws,ledger_event(str(i)*70000,str(i)),a) for i in range(2)]; upload(alice,sa,a); ids=[r[0] for r in server.execute("SELECT event FROM events WHERE LENGTH(envelope)>65536 ORDER BY event").fetchall()]
    def swapped(cfg,body,auth=True): return direct(cfg,{**body,"event":ids[1]} if body["op"]=="fetch" and body["event"]==ids[0] else body,auth)
    monkeypatch.setattr("ai_convos_remote.request",swapped)
    with pytest.raises(ValueError,match="mismatch"): pull(desktop,sb,b)
    assert sb.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked"


def test_attachment_bytes_are_redacted_lazy_and_reassembled(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); state_a,state_b=connect(a/"remote/state.db"),connect(b/"remote/state.db")
    payload=bytes(range(256))*800; source=tmp_path/"private"/"evidence.bin"; source.parent.mkdir(); source.write_bytes(payload); (a/"data").mkdir(); core=duckdb.connect(str(a/"data/convos.db")); init_schema(core); core.execute("INSERT INTO conversations VALUES ('c','codex','attachment','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')"); core.execute("INSERT INTO messages VALUES ('m','c','user','see file',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); core.execute("INSERT INTO attachments VALUES ('a','m','evidence.bin','application/octet-stream',?,?,NULL,'2026-01-01')",(len(payload),str(source))); body_hash=index_attachment_body(core,"a",source,len(payload)); records=scan(core,state_a); core.close()
    assert str(source) not in json.dumps(records) and body_hash in json.dumps(records); state_a.close(); state_b.close(); sync_once(a,True); sync_once(b,True); path=duckdb.connect(str(b/"data/convos.db"),read_only=True).execute("SELECT path FROM attachments").fetchone()[0]; assert open(path,"rb").read()==payload and os.stat(path).st_mode&0o777==0o600 and server.execute("SELECT LENGTH(ciphertext) FROM blob_replicas").fetchone()[0]==len(payload)+16 and payload not in (tmp_path/"server.db").read_bytes()
    fresh=server_connect(tmp_path/"fresh.db"); monkeypatch.setattr("ai_convos_remote.request",transport(fresh)); survivor,_=rehome_client(load(b),"http://fresh",b); replacement=workspace(survivor,"Personal"); state=connect(b/"remote/state.db"); assert bind_origin(survivor,state,replacement,ws,b); state.close(); sync_once(b,True); assert tuple(fresh.execute("SELECT COUNT(*),LENGTH(ciphertext) FROM blob_replicas").fetchone())==(1,len(payload)+16)


def test_deleted_state_rebaselines_before_publishing_existing_archive(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); replicate_conversation(a,ws,"remote","remote"); sync_once(b,True); core=duckdb.connect(str(b/"data/convos.db"),read_only=True); restored=core.execute("SELECT id,title FROM conversations").fetchall(); origins=core.execute("SELECT * FROM remote.row_origins").fetchall(); core.close(); assert restored==[("remote","remote")] and not origins
    before=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; replicas_before=server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]; state_path=b/"remote/state.db"; [Path(str(state_path)+suffix).unlink(missing_ok=True) for suffix in ("-wal","-shm")]; state_path.unlink(); legacy=sqlite3.connect(state_path); legacy.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)"); legacy.execute("CREATE TABLE legacy_payload(value TEXT)"); legacy.execute("INSERT INTO meta VALUES ('state_schema','2')"); legacy.execute("INSERT INTO legacy_payload VALUES ('preserve me')"); legacy.commit(); legacy.close(); db=duckdb.connect(str(b/"data/convos.db")); db.execute("INSERT INTO conversations VALUES ('local','codex','new local','2026-01-02','2026-01-02',NULL,NULL,NULL,NULL,'{}')"); db.close()
    def offline(cfg,body,auth=True):
        if body["op"] in ("pull","replica_pull"): raise ConnectionError("relay unavailable")
        return direct(cfg,body,auth)
    monkeypatch.setattr("ai_convos_remote.request",offline)
    with pytest.raises(ConnectionError,match="unavailable"): sync_once(b,True)
    state=connect(state_path); report=json.loads(state.execute("SELECT value FROM meta WHERE key='state_cutover'").fetchone()[0]); state.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before and inspect_state(state_path)["status"]=="current" and Path(report["backup"]).is_dir(); backup=sqlite3.connect(Path(report["backup"])/"state.db"); assert backup.execute("SELECT value FROM legacy_payload").fetchone()[0]=="preserve me"; backup.close()
    applied=[]; real=remote_client.apply_row_replicas; monkeypatch.setattr(remote_client,"apply_row_replicas",lambda path,bodies,*args,**kwargs:applied.append(len(bodies)) or real(path,bodies,*args,**kwargs)); monkeypatch.setattr("ai_convos_remote.request",direct); sync_once(b,True); assert not any(applied) and server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==replicas_before+1
    state=connect(b/"remote/state.db"); assert state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="ready" and not state.execute("SELECT 1 FROM sqlite_master WHERE name='imported_rows'").fetchone(); state.close(); core=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert core.execute("SELECT id,title FROM conversations ORDER BY id").fetchall()==[("local","new local"),("remote","remote")] and not core.execute("SELECT * FROM remote.row_origins").fetchall(); core.close()


def test_missing_archive_recovers_owned_rows_without_republishing(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; cfg,_=setup_client("http://server","alice",root=root); path=root/"data/convos.db"; write_archive(path,"latest"); sync_once(root,True); before=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; db=duckdb.connect(str(path),read_only=True); old=archive_state(db)[0]; db.close(); path.unlink(); remember=remote_client.remember_archive; monkeypatch.setattr(remote_client,"remember_archive",lambda *args:(_ for _ in ()).throw(ConnectionError("after recovery")))
    with pytest.raises(ConnectionError,match="after recovery"): sync_once(root,True)
    monkeypatch.setattr(remote_client,"remember_archive",remember); sync_once(root,True)
    db=duckdb.connect(str(path),read_only=True); assert db.execute("SELECT id,title FROM conversations").fetchall()==[("c","latest")] and not db.execute("SELECT * FROM remote.row_origins").fetchall() and archive_state(db)[0]!=old; db.close(); state=connect(root/"remote/state.db"); ws=workspace(load(root),"Personal"); assert state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="ready"; state.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before


@pytest.mark.parametrize("lost_anchor",("state","config"))
def test_rolled_back_archive_recovers_additively_and_blocks_reversion(tmp_path,monkeypatch,lost_anchor):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); root=tmp_path/"client"; setup_client("http://server","alice",root=root); path=root/"data/convos.db"; first=write_archive(path,"old"); sync_once(root,True); backup=tmp_path/"old.db"; shutil.copyfile(path,backup); second=write_archive(path,"current"); assert first[0]==second[0] and first[1]<second[1]; sync_once(root,True); before=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; shutil.copyfile(backup,path)
    if lost_anchor=="state": (root/"remote/state.db").unlink()
    else: cfg=load(root); cfg.pop("archive"); remote_client.save(cfg,root)
    with pytest.raises(ValueError,match="recovered additively"): sync_once(root,True)
    db=duckdb.connect(str(path),read_only=True); recovered=(archive_state(db)[1],db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]); assert {r[0] for r in db.execute("SELECT title FROM conversations").fetchall()}=={"old","current"} and db.execute("SELECT author_user_id FROM remote.row_origins").fetchone()[0]==load(root)["user"]; db.close()
    with pytest.raises(ValueError,match="recovered additively"): sync_once(root,True)
    db=duckdb.connect(str(path),read_only=True); assert (archive_state(db)[1],db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0])==recovered; db.close(); state=connect(root/"remote/state.db"); ws=workspace(load(root),"Personal"); assert state.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked" and state.execute("SELECT value FROM meta WHERE key=?",(f"archive_mode:{ws}",)).fetchone()[0]=="import"; state.close(); assert server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==before


def test_ready_rejects_history_missing_before_signed_checkpoint(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); ws=workspace(alice,"Personal"); state=connect(a/"remote/state.db"); publish(alice,state,ws,ledger_event("must survive"),a); upload(alice,state,a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); server.execute("DELETE FROM events WHERE workspace=? AND author=?",(ws,alice["device"]["id"])); server.commit(); target=connect(b/"remote/state.db")
    with pytest.raises(ValueError,match="signed history checkpoint"): pull(desktop,target,b)
    assert target.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked"


def test_doctor_reports_legacy_state_without_modifying_it(tmp_path,monkeypatch):
    root=tmp_path/"client"; remote=root/"remote"; remote.mkdir(parents=True); (remote/"config.json").write_text(json.dumps({"url":"http://server","user":"user","device":{"id":"device"},"workspaces":{},"keys":{}})); path=remote/"state.db"; db=sqlite3.connect(path); db.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)"); db.execute("INSERT INTO meta VALUES ('state_schema','2')"); db.commit(); db.close(); before=(path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in remote.iterdir()}); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(root)); monkeypatch.setattr("ai_convos_remote.health",lambda cfg:{"ok":True})
    assert "state=incompatible" in doctor_status() and "backup+rebaseline" in doctor_status()
    assert (path.read_bytes(),path.stat().st_mtime_ns,{p.name for p in remote.iterdir()})==before


def test_pull_converges_past_relay_batch_limit(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); state=connect(a/"remote/state.db")
    [publish(alice,state,ws,policy(f"row {i}",f"row-{i}"),a,True) for i in range(510)]; state.commit(); upload(alice,state,a); calls=[]
    def counted(cfg,body,auth=True): calls.append(body["op"]); return direct(cfg,body,auth)
    monkeypatch.setattr("ai_convos_remote.request",counted); target=connect(b/"remote/state.db"); result=pull(desktop,target,b); assert target.execute("SELECT COUNT(*) FROM policies WHERE workspace=?",(ws,)).fetchone()[0]==510 and calls.count("pull")>=2 and result[ws]["cursor"]==result[ws]["tail"]


def test_crash_after_duckdb_projection_replays_before_cursor_commit(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); replicate_conversation(a,ws,"projected once","once"); target=connect(b/"remote/state.db"); real=remote_client.apply_row_replicas
    def crash(*args,**kwargs): result=real(*args,**kwargs); raise ConnectionError("after DuckDB commit")
    monkeypatch.setattr(remote_client,"apply_row_replicas",crash)
    with pytest.raises(ConnectionError,match="DuckDB"): pull(desktop,target,b)
    assert target.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==0 and target.execute("SELECT value FROM meta WHERE key=?",(f"replica_cursor:{ws}",)).fetchone()[0]=="0" and target.execute("SELECT lifecycle FROM sync_states WHERE workspace=?",(ws,)).fetchone()[0]=="blocked"
    db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==db.execute("SELECT COUNT(*) FROM remote.row_origins").fetchone()[0]==1; db.close()
    monkeypatch.setattr(remote_client,"apply_row_replicas",real); result=pull(desktop,target,b); assert result[ws]["cursor"]==result[ws]["tail"] and duckdb.connect(str(b/"data/convos.db"),read_only=True).execute("SELECT COUNT(*) FROM conversations").fetchone()[0]==1


def test_team_user_multiple_devices_and_admin_device_removal(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,_=setup_client("http://server","alice",root=a); bob,recovery=setup_client("http://server","bob","laptop",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); bob=load(b); pull(bob,connect(b/"remote/state.db"),b); bob2,_=setup_client("http://server","bob","desktop",load(b)["recovery"],root=c)
    assert f"{team}:2" not in bob2["keys"]; pull(bob2,connect(c/"remote/state.db"),c); assert not (c/"data/convos.db").exists(); request_device(bob2,team,c,0); bob=load(b); result=approve_device(bob,team,bob2["device"]["id"],root=b); assert result["approved"] and result["history"]>=1; bob2=load(c); pull(bob2,connect(c/"remote/state.db"),c)
    replicate_conversation(a,team,"team device"); bob2=load(c); pull(bob2,connect(c/"remote/state.db"),c); assert duckdb.connect(str(c/"data/convos.db"),read_only=True).execute("SELECT title FROM conversations").fetchone()[0]=="team device"
    alice=load(a); remove_device(alice,team,bob2["device"]["id"],a); bob2=load(c); pull(bob2,connect(c/"remote/state.db"),c); state=refresh(bob2,c); assert next(w for w in state["workspaces"] if w["kind"]=="personal")["device_authorized"] and not next(w for w in state["workspaces"] if w["id"]==team)["device_authorized"] and server.execute("SELECT active FROM devices WHERE id=?",(bob2["device"]["id"],)).fetchone()[0]==1


def test_pending_or_removed_admin_device_cannot_authorize_itself(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b=tmp_path/"a",tmp_path/"b"; laptop,recovery=setup_client("http://server","alice","laptop",root=a); team=create(laptop,"Team","team",a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b)
    assert server.execute("SELECT epoch FROM workspaces WHERE id=?",(team,)).fetchone()[0]==1 and not server.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND device=?",(team,desktop["device"]["id"])).fetchone(); req={"op":"rotate","workspace":team,"epoch":2,"members":{laptop["user"]:"admin"},"envelopes":{d["id"]:{} for d in (laptop["device"],desktop["device"])}}
    grant={"op":"grant_all","workspace":team,"user":laptop["user"],"envelopes":{}}
    with pytest.raises(PermissionError,match="signature"): action(server,sign_control(desktop["device"],req),laptop["token"])
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],grant),desktop["token"])
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],req),desktop["token"])
    request_device(desktop,team,b,0)
    with pytest.raises(PermissionError,match="vote"): approve_device(desktop,team,desktop["device"]["id"],root=b)
    assert approve_device(load(a),team,desktop["device"]["id"],False,root=a)=={"approved":False,"rejected":True}
    with pytest.raises(ValueError,match="not found"): approve_device(load(a),team,desktop["device"]["id"],root=a)
    request_device(desktop,team,b,0)
    laptop=load(a); approve_device(laptop,team,desktop["device"]["id"],root=a); laptop=load(a); remove_device(laptop,team,desktop["device"]["id"],a); req|={"epoch":4,"activate_devices":[desktop["device"]["id"]]}
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],grant),desktop["token"])
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],req),desktop["token"])
    with pytest.raises(ValueError,match="not pending"): request_device(load(b),team,b,0)
    grant_all(laptop,team,"alice",a); assert not server.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=3 AND device=?",(team,desktop["device"]["id"])).fetchone()
    assert server.execute("SELECT epoch FROM workspaces WHERE id=?",(team,)).fetchone()[0]==3 and server.execute("SELECT 1 FROM workspace_device_exclusions WHERE workspace=? AND device=?",(team,desktop["device"]["id"])).fetchone() and not server.execute("SELECT 1 FROM key_envelopes WHERE workspace=? AND epoch=4",(team,)).fetchone()
    personal=workspace(laptop,"Personal"); remove_device(laptop,personal,desktop["device"]["id"],a); req|={"workspace":personal,"epoch":4}
    grant|={"workspace":personal}
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],grant),desktop["token"])
    with pytest.raises(PermissionError,match="authorized"): action(server,sign_control(desktop["device"],req),desktop["token"])
    assert server.execute("SELECT epoch FROM workspaces WHERE id=?",(personal,)).fetchone()[0]==3 and server.execute("SELECT 1 FROM workspace_device_exclusions WHERE workspace=? AND device=?",(personal,desktop["device"]["id"])).fetchone()


def test_orphan_device_requires_user_majority_and_inherits_role_not_history(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b,c,d=tmp_path/"a",tmp_path/"b",tmp_path/"c",tmp_path/"d"; alice,recovery=setup_client("http://server","alice","laptop",root=a); bob,_=setup_client("http://server","bob",root=b); carol,_=setup_client("http://server","carol",root=c); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); alice=load(a); add_member(alice,team,"carol",root=a); recovered,_=setup_client("http://server","alice","recovered",recovery,root=d); alice=load(a); remove_device(alice,team,alice["device"]["id"],a); proposal=request_device(recovered,team,d,0); bad=control_sign(bob["device"],{**{k:v for k,v in device_vote(bob["device"],bob["user"],proposal).items() if k not in ("author","signature")},"voter":recovered["user"]})
    with pytest.raises(PermissionError,match="vote"): action(server,{"op":"vote","vote":bad},bob["token"])
    forged=control_sign(recovered["device"],{**{k:v for k,v in proposal.items() if k not in ("author","signature")},"certificate_hash":"0"*64})
    with pytest.raises(PermissionError,match="proposal"): action(server,{"op":"propose","proposal":forged},recovered["token"])
    assert server.execute("SELECT COUNT(*) FROM device_votes").fetchone()[0]==0 and server.execute("SELECT COUNT(*) FROM device_proposals").fetchone()[0]==1
    first=approve_device(load(b),team,recovered["device"]["id"],root=b); assert first=={"approved":False,"votes":1,"needed":2}
    final=approve_device(load(c),team,recovered["device"]["id"],root=c); assert final["approved"] and final["history"]==0
    state=refresh(load(d),d); control=next(w for w in state["workspaces"] if w["id"]==team)["controls"][-1]; assert control["members"][recovered["user"]]["role"]=="admin" and not control["devices"][recovered["device"]["id"]]["history"]


def test_history_can_be_approved_later_and_sync_rewinds(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,recovery=setup_client("http://server","alice","laptop",root=a); bob,_=setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); replicate_conversation(a,team,"old"); add_member(load(a),team,"bob",root=a); grant_all(load(a),team,"bob",a); recovered,_=setup_client("http://server","alice","recovered",recovery,root=c); alice=load(a); remove_device(alice,team,alice["device"]["id"],a); request_device(recovered,team,c,0); assert approve_device(load(b),team,recovered["device"]["id"],root=b)["history"]==0
    replicate_conversation(b,team,"new","new"); recovered=load(c); sc=connect(c/"remote/state.db"); pull(recovered,sc,c); assert duckdb.connect(str(c/"data/convos.db"),read_only=True).execute("SELECT title FROM conversations").fetchall()==[("new",)]
    request_history(recovered,team,c,0); result=approve_history(load(b),team,recovered["device"]["id"],root=b); assert result["approved"] and result["history"]>=4
    recovered=load(c); pull(recovered,sc,c); assert {r[0] for r in duckdb.connect(str(c/"data/convos.db"),read_only=True).execute("SELECT title FROM conversations").fetchall()}=={"old","new"}


def test_same_user_approval_rewraps_complete_history(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,_=setup_client("http://server","alice",root=a); bob,recovery=setup_client("http://server","bob","laptop",root=b); team=create(alice,"Team","team",a); sb=connect(b/"remote/state.db"); replicate_conversation(a,team,"complete"); add_member(load(a),team,"bob",root=a); grant_all(load(a),team,"bob",a); pull(load(b),sb,b); desktop,_=setup_client("http://server","bob","desktop",recovery,root=c); request_device(desktop,team,c,0); assert approve_device(load(b),team,desktop["device"]["id"],root=b)["approved"]
    pull(load(c),connect(c/"remote/state.db"),c); assert duckdb.connect(str(c/"data/convos.db"),read_only=True).execute("SELECT title FROM conversations").fetchone()[0]=="complete"


def test_relay_clock_enforces_proposal_delay(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); monkeypatch.setattr("ai_convos_remote.request",transport(server)); monkeypatch.setattr("ai_convos_remote_server.APPROVAL_DELAY",3600); a,b,c=tmp_path/"a",tmp_path/"b",tmp_path/"c"; alice,recovery=setup_client("http://server","alice",root=a); setup_client("http://server","bob",root=b); team=create(alice,"Team","team",a); add_member(alice,team,"bob",root=a); recovered,_=setup_client("http://server","alice","recovered",recovery,root=c); remove_device(alice,team,alice["device"]["id"],a); request_device(recovered,team,c,0)
    with pytest.raises(ValueError,match="active"): approve_device(load(b),team,recovered["device"]["id"],root=b)


@pytest.mark.parametrize("kind,direction,when",[(kind,direction,when) for kind in ("row","blob") for direction in ("upload","pull") for when in ("before","after")])
def test_replication_boundary_crash_matrix_converges_after_restart(tmp_path,monkeypatch,kind,direction,when):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice",root=a); data=a/"data"; data.mkdir(); db=duckdb.connect(str(data/"convos.db")); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','crash matrix','2026-01-01','2026-01-01',NULL,NULL,NULL,NULL,'{}')")
    payload=b"restart-safe attachment"
    if kind=="blob":
        source=tmp_path/"attachment.bin"; source.write_bytes(payload); db.execute("INSERT INTO messages VALUES ('m','c','user','attached',NULL,'2026-01-01',NULL,'{}',NULL,NULL)"); db.execute("INSERT INTO attachments VALUES ('a','m','attachment.bin','application/octet-stream',?,?,NULL,'2026-01-01')",(len(payload),str(source))); index_attachment_body(db,"a",source,len(payload))
    db.close(); fired=[False]
    if direction=="upload":
        op="replica_upload_many" if kind=="row" else "blob_upload"
        def cut(cfg,body,auth=True):
            if body["op"]==op and not fired[0]: fired[0]=True; when=="before" and (_ for _ in ()).throw(ConnectionError("boundary crash")); direct(cfg,body,auth); raise ConnectionError("boundary crash")
            return direct(cfg,body,auth)
        monkeypatch.setattr("ai_convos_remote.request",cut)
        with pytest.raises(ConnectionError,match="boundary crash"): sync_once(a,True)
        monkeypatch.setattr("ai_convos_remote.request",direct); sync_once(a,True)
    else:
        sync_once(a,True); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); name="apply_row_replicas" if kind=="row" else "project_attachment_body"; real=getattr(remote_client,name)
        def cut(*args,**kwargs):
            if not fired[0]: fired[0]=True; when=="before" and (_ for _ in ()).throw(ConnectionError("boundary crash")); real(*args,**kwargs); raise ConnectionError("boundary crash")
            return real(*args,**kwargs)
        monkeypatch.setattr(remote_client,name,cut)
        with pytest.raises(ConnectionError,match="boundary crash"): sync_once(b,True)
        monkeypatch.setattr(remote_client,name,real); sync_once(b,True); db=duckdb.connect(str(b/"data/convos.db"),read_only=True); assert db.execute("SELECT title FROM conversations").fetchone()[0]=="crash matrix"; path=db.execute("SELECT path FROM attachments").fetchone()[0] if kind=="blob" else None; db.close(); assert path is None or Path(path).read_bytes()==payload
    assert fired[0] and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==(1 if kind=="row" else 3) and server.execute("SELECT COUNT(*) FROM blob_replicas").fetchone()[0]==(kind=="blob")


def test_keyboard_interrupt_releases_sync_lease_and_retry_converges(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db")
    direct=transport(server)
    monkeypatch.setattr("ai_convos_remote.request",direct)
    monkeypatch.setattr("ai_convos_remote.drain_hooks",lambda:None)
    root=tmp_path/"client"
    setup_client("http://server","alice",root=root)
    write_archive(root/"data/convos.db","interrupt safe")
    fired=False
    def interrupt(cfg,body,auth=True):
        nonlocal fired
        result=direct(cfg,body,auth)
        if body["op"]=="replica_upload_many" and not fired:
            fired=True
            raise KeyboardInterrupt
        return result
    monkeypatch.setattr("ai_convos_remote.request",interrupt)
    with pytest.raises(KeyboardInterrupt): sync_once(root,True)
    monkeypatch.setattr("ai_convos_remote.request",direct)
    sync_once(root,True)
    state=connect(root/"remote/state.db")
    assert fired and server.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]==1
    state.close()


def test_lost_upload_response_and_interrupted_pull_recover_idempotently(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice",root=a); desktop,_=setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); ws=workspace(alice,"Personal"); state_a,state_b=connect(a/"remote/state.db"),connect(b/"remote/state.db"); baseline=server.execute("SELECT COUNT(*) FROM events").fetchone()[0]; publish(alice,state_a,ws,policy("crash safe"),a)
    def lost(cfg,body,auth=True):
        result=direct(cfg,body,auth)
        if body["op"]=="upload_many": raise ConnectionError("response lost")
        return result
    monkeypatch.setattr("ai_convos_remote.request",lost)
    with pytest.raises(ConnectionError): upload(alice,state_a,a)
    assert state_a.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]==1 and server.execute("SELECT COUNT(*) FROM events").fetchone()[0]==baseline+1
    monkeypatch.setattr("ai_convos_remote.request",direct); upload(alice,state_a,a); assert state_a.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]==0 and state_a.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]>0
    def cut(cfg,body,auth=True):
        result=direct(cfg,body,auth)
        if body["op"]=="pull": raise ConnectionError("pull interrupted")
        return result
    monkeypatch.setattr("ai_convos_remote.request",cut)
    with pytest.raises(ConnectionError): pull(desktop,state_b,b)
    assert not state_b.execute("SELECT * FROM cursors").fetchall()
    monkeypatch.setattr("ai_convos_remote.request",direct); pull(desktop,state_b,b); assert state_b.execute("SELECT value FROM policies WHERE workspace=? AND kind='test'",(ws,)).fetchone()[0]=="crash safe"


def test_lost_upload_response_survives_epoch_rotation_without_resealing(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db"); direct=transport(server); monkeypatch.setattr("ai_convos_remote.request",direct); a,b=tmp_path/"a",tmp_path/"b"; alice,recovery=setup_client("http://server","alice","laptop",root=a); ws=workspace(alice,"Personal"); state=connect(a/"remote/state.db"); eid=publish(alice,state,ws,policy("rotate after lost response"),a); original=json.loads(next((a/"remote/outbox").iterdir()).read_text()); lost=[False]
    def drop(cfg,body,auth=True):
        result=direct(cfg,body,auth)
        if body["op"]=="upload_many" and not lost[0]: lost[0]=True; raise ConnectionError("response lost")
        return result
    monkeypatch.setattr("ai_convos_remote.request",drop)
    with pytest.raises(ConnectionError,match="response lost"): upload(alice,state,a)
    monkeypatch.setattr("ai_convos_remote.request",direct); setup_client("http://server","alice","desktop",recovery,root=b); alice=load(a); upload(alice,state,a); stored=json.loads(server.execute("SELECT envelope FROM events WHERE event=?",(eid,)).fetchone()[0])
    assert stored==original and state.execute("SELECT epoch FROM receipts WHERE event=?",(eid,)).fetchone()[0]==1 and not state.execute("SELECT 1 FROM outbox WHERE event=?",(eid,)).fetchone()
