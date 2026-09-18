#!/usr/bin/env python3
"""Run persistent customer lifecycle checks locally or in the Titan test container."""
import argparse
import hashlib
import json
import os
import re
import resource
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from ai_convos.cli import archive_state, atomic_json, capture_provenance, init_schema, operation_lock

USERS={"fresh":("convos-fresh-a","convos-fresh-b"),"canary":("convos-canary-a","convos-canary-b")}
STATE=Path("/var/lib/convos-testbed")
TEST_USERS={user for users in USERS.values() for user in users}


def inside(path,base): return (path:=Path(path).resolve())==(base:=Path(base).resolve()) or base in path.parents
def test_root(user,root):
    if user not in TEST_USERS or not inside(root,Path("/home")/user/"convos-testbed"): raise ValueError(f"refusing non-test Convos root: {root}")
    return Path(root)
def isolated_root(root):
    if len(users:=[user for user in TEST_USERS if inside(root,Path("/home")/user/"convos-testbed")])!=1: raise ValueError(f"refusing non-test Convos root: {root}")
    return test_root(users[0],root)


@dataclass(frozen=True,slots=True)
class Client:
    user: str
    root: Path
    venv: Path

    def __post_init__(self): test_root(self.user,self.root)

    @property
    def home(self): return Path("/home")/self.user
    @property
    def convos(self): return self.venv/"bin/convos"
    @property
    def python(self): return self.venv/"bin/python"


def run(command,*,input=None,check=True,env=None,timeout=None):
    command=tuple(map(str,command))
    try: return subprocess.run(command,input=input,text=True,capture_output=True,check=check,env=env,timeout=timeout)
    except subprocess.CalledProcessError as error:
        shown=['<redacted>' if i and command[i-1] in ('--recovery','--token','--password') else value for i,value in enumerate(command)]
        output=re.sub(r'(Recovery key \(store offline\): )\S+',r'\1<redacted>',(error.stdout or '')+(error.stderr or ''))
        raise RuntimeError(f"command failed ({error.returncode}): {' '.join(shown)}\n{output}") from None
def as_user(client,*command,input=None,check=True):
    env=("env",f"HOME={client.home}",f"PATH={client.venv/'bin'}:/usr/bin:/bin",f"CONVOS_PROJECT_ROOT={client.root}")
    return run(("runuser","-u",client.user,"--",*env,*command),input=input,check=check)
def cli(client,*args,input=None,check=True): return as_user(client,client.convos,*args,input=input,check=check)
def sha256(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def package_version(client): return as_user(client,client.python,"-c","import importlib.metadata\nprint(importlib.metadata.version('convos'))").stdout.strip()
def wait_health(url,process,timeout=15,diagnostics=None,version=2):
    started,opener,last=time.monotonic(),urllib.request.build_opener(urllib.request.ProxyHandler({})),None
    while time.monotonic()-started<timeout:
        if process.poll() is not None: raise RuntimeError(f"relay exited ({process.returncode}): {process.stderr.read() if process.stderr else 'see relay.log'}")
        try: return json.loads(opener.open(url+f"/v{version}/health",timeout=1).read())
        except Exception as error:
            last=error
            time.sleep(.1)
    if diagnostics and sys.platform=='darwin': subprocess.run(('/usr/bin/sample',str(process.pid),'1','-file',str(diagnostics)),capture_output=True,timeout=10)
    raise TimeoutError(f"relay did not become healthy: {last}")


class Relay:
    def __init__(self,lane,venv,port):
        self.lane,self.venv,self.port=lane,Path(venv),port
        self.base=STATE/lane
        self.db=self.base/"server.db"
        self.url=f"http://127.0.0.1:{port}"
        self.process=None
    def start(self):
        command=("runuser","-u","convos-relay","--",self.venv/"bin/convos-server","serve","--db",self.db,"--host","127.0.0.1","--port",str(self.port))
        self.process=subprocess.Popen(tuple(map(str,command)),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True,start_new_session=True)
        wait_health(self.url,self.process)
    def stop(self):
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid,signal.SIGTERM)
            self.process.wait(timeout=5)
        self.process=None
    def restart(self):
        self.stop()
        self.start()
    def restore_roundtrip(self):
        self.stop()
        backup=self.base/"server.db.testbed-backup"
        shutil.copy2(self.db,backup)
        original=sha256(self.db)
        self.db.unlink()
        shutil.copy2(backup,self.db)
        relay_user=__import__("pwd").getpwnam("convos-relay")
        os.chown(self.db,relay_user.pw_uid,relay_user.pw_gid)
        if sha256(self.db)!=original: raise AssertionError("relay backup restore changed bytes")
        self.start()
        return {"path":str(backup),"sha256":original,"bytes":backup.stat().st_size}
    def __enter__(self):
        self.start()
        return self
    def __exit__(self,*exc): self.stop()


def reset_lane(lane,clients,relay):
    if lane!="fresh": return
    relay.stop()
    for path in relay.base.glob("server.db*"): path.unlink()
    for base,client in {client.home/"convos-testbed"/lane:client for client in clients}.items():
        shutil.rmtree(base,ignore_errors=True)
        base.mkdir(parents=True)
        user=__import__("pwd").getpwnam(client.user)
        os.chown(client.home/"convos-testbed",user.pw_uid,user.pw_gid)
        os.chown(base,user.pw_uid,user.pw_gid)


def setup(client,url,name,device):
    output=cli(client,"remote","setup",url,name,"--device",device).stdout
    user=re.search(r"User ID: ([0-9a-f]{32})",output)
    recovery=re.search(r"Recovery key \(store offline\): (\S+)",output)
    if not user or not recovery: raise AssertionError(f"unexpected setup output: {output}")
    return user.group(1),recovery.group(1)
def recover(client,url,name,device,recovery):
    cli(client,"remote","recover",url,name,"--device",device,"--recovery",recovery)
def git(client,root,*args): return as_user(client,"git","-C",root,*args).stdout.strip()
def seed(client,cid,title,prompt,cwd=None,edit=None):
    args=(client.python,__file__,"seed",str(client.root),cid,title,prompt)
    if cwd: args+=("--cwd",str(cwd))
    if edit: args+=("--edit",str(edit[0]),"--content",edit[1],"--old-content",edit[2])
    as_user(client,*args)
def seed_archive(root,cid,title,prompt,cwd=None,edit=None,content=None,old_content=None):
    path=root/"data/convos.db"
    path.parent.mkdir(parents=True,exist_ok=True)
    db=duckdb.connect(str(path))
    init_schema(db)
    db.execute("INSERT INTO conversations (id,source,title,created_at,updated_at,cwd,metadata) VALUES (?,?,?,'2026-01-01','2026-01-01',?,'{}')",(cid,"testbed",title,str(cwd) if cwd else None))
    db.execute("INSERT INTO messages (id,conversation_id,role,content,created_at,metadata) VALUES (?,?, 'user',?,'2026-01-01 00:00:00','{}'),(?,?, 'assistant','done','2026-01-01 00:00:01','{}')",(f"u-{cid}",cid,prompt,f"a-{cid}",cid))
    if edit:
        db.execute("INSERT INTO file_edits (id,message_id,file_path,edit_type,content,created_at,old_content) VALUES (?,?,?,'write',?,'2026-01-01 00:00:01',?)",(f"e-{cid}",f"a-{cid}",str(edit),content,old_content))
        if db.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='provenance' AND table_name='file_edit_evidence'").fetchone():
            db.execute("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','synthetic_testbed',NULL)",(f"e-{cid}",))
    db.close()
    capture_provenance(path)
def archive_evidence(client):
    path=client.root/"data/convos.db"
    db=duckdb.connect(str(path),read_only=True)
    counts={table:db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("conversations","messages","tool_calls","attachments","artifacts","file_edits")}
    semantic=db.execute("SELECT id,source,title FROM conversations ORDER BY id").fetchall(),db.execute("SELECT id,conversation_id,role,content FROM messages ORDER BY id").fetchall(),db.execute("SELECT id,message_id,file_path,edit_type,content FROM file_edits ORDER BY id").fetchall(),db.execute("SELECT file_edit_id,status,reason,tool_call_id FROM provenance.file_edit_evidence ORDER BY file_edit_id").fetchall()
    state=archive_state(db)
    conflicts=db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]
    db.close()
    return {"counts":counts,"semantic_sha256":hashlib.sha256(json.dumps(semantic,sort_keys=True).encode()).hexdigest(),"archive":state[:2],"conflicts":conflicts}
def assert_team_projection(client,prompt,exact=True):
    db=duckdb.connect(str(client.root/"data/convos.db"),read_only=True)
    found=db.execute("SELECT COUNT(*) FROM messages WHERE content=?",(prompt,)).fetchone()[0]
    paths=db.execute("SELECT file_path FROM file_edits ORDER BY id").fetchall()
    graph=db.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]
    confirmed=db.execute("SELECT COUNT(*) FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' JOIN messages a ON a.id=fe.message_id WHERE a.conversation_id IN (SELECT conversation_id FROM messages WHERE content=?)",(prompt,)).fetchone()[0]
    db.close()
    if not found or ("app.py",) not in paths or not graph or exact and not confirmed: raise AssertionError("team projection is incomplete")
def assert_message(client,prompt):
    db=duckdb.connect(str(client.root/"data/convos.db"),read_only=True)
    found=db.execute("SELECT COUNT(*) FROM messages WHERE content=?",(prompt,)).fetchone()[0]
    db.close()
    if not found: raise AssertionError(f"message was not projected: {prompt}")
def assert_opaque(relay,*sentinels):
    raw=b"".join(path.read_bytes() for path in relay.base.glob("server.db*") if path.is_file())
    leaked=[str(value) for value in sentinels if str(value).encode() in raw]
    if leaked: raise AssertionError(f"relay contains plaintext: {leaked}")
def assert_relay_isolation(path,users,team):
    with sqlite3.connect(path) as db:
        found={r[0] for r in db.execute("SELECT id FROM users")}
        workspaces={r[0]:(r[1],r[2]) for r in db.execute("SELECT id,kind,created_by FROM workspaces")}
        members={ws:{u for w,u in db.execute("SELECT workspace,user_id FROM members WHERE active=1") if w==ws} for ws in workspaces}
    personal={ws for ws,(kind,_) in workspaces.items() if kind=="personal"}
    if found!=set(users) or any(creator not in users for _,creator in workspaces.values()): raise AssertionError("test relay contains a non-test user")
    if set(workspaces)!=personal|{team} or len(personal)!=len(users) or {workspaces[ws][1] for ws in personal}!=set(users) or workspaces.get(team,(None,))[0]!="team": raise AssertionError("test relay contains an unexpected workspace")
    if any(members[ws]!={creator} for ws,(kind,creator) in workspaces.items() if kind=="personal"): raise AssertionError("personal test workspace membership is not isolated")
    if members.get(team)!=set(users): raise AssertionError("team test workspace membership is not isolated")


def fresh_lane(venv,commit,released_venv=None):
    lane="fresh"
    a=Client(USERS[lane][0],Path(f"/home/{USERS[lane][0]}/convos-testbed/{lane}/laptop"),Path(venv))
    a2=Client(a.user,Path(f"/home/{a.user}/convos-testbed/{lane}/desktop"),Path(venv))
    b=Client(USERS[lane][1],Path(f"/home/{USERS[lane][1]}/convos-testbed/{lane}/desktop"),Path(venv))
    relay=Relay(lane,venv,8787)
    reset_lane(lane,(a,a2,b),relay)
    started=time.monotonic()
    with relay:
        alice,recovery=setup(a,relay.url,"fresh-alice","laptop")
        personal_prompt="fresh personal sentinel 7b5ff1"
        seed(a,"fresh-personal","Fresh personal",personal_prompt)
        cli(a,"remote","sync")
        recover(a2,relay.url,"fresh-alice","desktop",recovery)
        cli(a2,"remote","sync")
        bob,_=setup(b,relay.url,"fresh-bob","desktop")
        workspace=cli(a,"remote","workspace","Backend").stdout.strip().splitlines()[-1]
        cli(a,"remote","invite","Backend",bob)
        a_repo=a.home/"convos-testbed"/lane/"checkouts/backend"
        b_repo=b.home/"convos-testbed"/lane/"different/backend"
        as_user(a,"mkdir","-p",a_repo)
        git(a,a_repo,"init","-q")
        git(a,a_repo,"config","user.email","fresh@example.invalid")
        git(a,a_repo,"config","user.name","Fresh A")
        (a_repo/"app.py").write_text("before\n")
        os.chown(a_repo/"app.py",__import__("pwd").getpwnam(a.user).pw_uid,__import__("pwd").getpwnam(a.user).pw_gid)
        git(a,a_repo,"add",".")
        git(a,a_repo,"commit","-qm","initial")
        shared=Path("/srv/convos-testbed")/lane
        shutil.rmtree(shared,ignore_errors=True)
        shared.mkdir(parents=True)
        alice_user=__import__("pwd").getpwnam(a.user)
        os.chown(shared,alice_user.pw_uid,alice_user.pw_gid)
        as_user(a,"git","clone","-q","--bare",a_repo,shared/"backend.git")
        bob_user=__import__("pwd").getpwnam(b.user)
        for path in (shared/"backend.git",*(shared/"backend.git").rglob("*")):
            os.chown(path,bob_user.pw_uid,bob_user.pw_gid)
        as_user(b,"mkdir","-p",b_repo.parent)
        as_user(b,"git","clone","-q",shared/"backend.git",b_repo)
        cli(a,"remote","link",a_repo,"Backend")
        cli(b,"remote","sync")
        cli(b,"remote","link",b_repo,workspace)
        cli(a2,"remote","sync")
        device=json.loads((a2.root/"remote/config.json").read_text())["device"]["id"]
        cli(a2,"remote","request-device",workspace)
        cli(a,"remote","approve-device","Backend",device)
        cli(a2,"remote","sync")
        team_prompt="fresh team sentinel 892cf4"
        (a_repo/"app.py").write_text("after\n")
        os.chown(a_repo/"app.py",__import__("pwd").getpwnam(a.user).pw_uid,__import__("pwd").getpwnam(a.user).pw_gid)
        seed(a,"fresh-team","Fresh team",team_prompt,a_repo,(a_repo/"app.py","after\n","before\n"))
        cli(a,"remote","sync")
        relay.restart()
        cli(b,"remote","sync")
        cli(a2,"remote","sync")
        assert_team_projection(b,team_prompt)
        assert_team_projection(a2,team_prompt)
        if released_venv:
            old=Client(b.user,b.home/"convos-testbed"/lane/"released-probe",Path(released_venv))
            shutil.rmtree(old.root,ignore_errors=True)
            (old.root/"remote").mkdir(parents=True)
            shutil.copy2(b.root/"remote/config.json",old.root/"remote/config.json")
            for path in (old.root,*old.root.rglob("*")): os.chown(path,bob_user.pw_uid,bob_user.pw_gid)
            cli(old,"remote","sync")
            assert_message(old,team_prompt)
        concurrent_a="fresh concurrent A sentinel 96da8f"
        concurrent_b="fresh concurrent B sentinel b20c74"
        a_prior=(a_repo/"app.py").read_text()
        b_prior=(b_repo/"app.py").read_text()
        (a_repo/"app.py").write_text("concurrent a\n")
        (b_repo/"app.py").write_text("concurrent b\n")
        alice_owner=__import__("pwd").getpwnam(a.user)
        bob_owner=__import__("pwd").getpwnam(b.user)
        os.chown(a_repo/"app.py",alice_owner.pw_uid,alice_owner.pw_gid)
        os.chown(b_repo/"app.py",bob_owner.pw_uid,bob_owner.pw_gid)
        seed(a,"fresh-concurrent-a","Fresh concurrent A",concurrent_a,a_repo,(a_repo/"app.py","concurrent a\n",a_prior))
        seed(b,"fresh-concurrent-b","Fresh concurrent B",concurrent_b,b_repo,(b_repo/"app.py","concurrent b\n",b_prior))
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda client:cli(client,"remote","sync"),(a,b)))
        cli(a,"remote","sync")
        cli(b,"remote","sync")
        assert_message(a,concurrent_b)
        assert_message(b,concurrent_a)
        cli(a,"remote","remove-device","Backend",device)
        cli(a2,"remote","sync")
        removed=json.loads((a2.root/"remote/config.json").read_text())["controls"][workspace]["removed"]
        if device not in removed: raise AssertionError("removed device retained team authorization")
        backup=relay.restore_roundtrip()
        cli(a,"remote","sync")
        cli(b,"remote","sync")
        before={"a":archive_evidence(a),"a2":archive_evidence(a2),"b":archive_evidence(b)}
        with sqlite3.connect(relay.db) as db: relay_rows=db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]
        cli(a,"remote","sync")
        cli(b,"remote","sync")
        after={"a":archive_evidence(a),"a2":archive_evidence(a2),"b":archive_evidence(b)}
        with sqlite3.connect(relay.db) as db: final_relay_rows=db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]
        if before!=after or final_relay_rows!=relay_rows: raise AssertionError("second sync was not idempotent")
        assert_relay_isolation(relay.db,{alice,bob},workspace)
        assert_opaque(relay,personal_prompt,team_prompt,a_repo,b_repo)
        doctors={name:cli(client,"doctor").stdout.strip() for name,client in (("a",a),("a2",a2),("b",b))}
        evidence={"lane":lane,"commit":commit,"version":package_version(a),"passed":True,"seconds":round(time.monotonic()-started,3),"users":[a.user,b.user],"devices":3,"workspace":workspace,"user_ids":[alice,bob],"archives":after,"relay_rows":relay_rows,"relay_plaintext":False,"backup":backup,"concurrent_updates":2,"device_recovery_and_removal":True,"mixed_released_client":bool(released_venv),"doctors":doctors,"peak_child_kib":resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,"input_outcomes":{"synthetic":4,"imported":4,"skipped":0,"failed":0},"harness_retries":0,"failures":[],"conflicts":sum(archive["conflicts"] for archive in after.values())}
    path=STATE/"evidence"/f"fresh-{int(time.time())}-{commit[:12]}.json"
    path.write_text(json.dumps(evidence,sort_keys=True,indent=2))
    os.chmod(path,0o600)
    print(json.dumps({"evidence":str(path),**evidence},sort_keys=True))


def canary_lane(released_venv,current_venv,released_commit,current_commit):
    lane="canary"
    released=Path(released_venv)
    current=Path(current_venv)
    a_old=Client(USERS[lane][0],Path(f"/home/{USERS[lane][0]}/convos-testbed/{lane}/laptop"),released)
    b_old=Client(USERS[lane][1],Path(f"/home/{USERS[lane][1]}/convos-testbed/{lane}/desktop"),released)
    a=Client(a_old.user,a_old.root,current)
    b=Client(b_old.user,b_old.root,current)
    manifest_path=STATE/lane/"manifest.json"
    bootstrap=not manifest_path.exists()
    if bootstrap:
        with Relay(lane,released,8788) as relay:
            alice,_=setup(a_old,relay.url,"canary-alice","laptop")
            bob,_=setup(b_old,relay.url,"canary-bob","desktop")
            workspace=cli(a_old,"remote","workspace","Backend").stdout.strip().splitlines()[-1]
            cli(a_old,"remote","invite","Backend",bob)
            repo=a_old.home/"convos-testbed"/lane/"checkout/backend"
            as_user(a_old,"mkdir","-p",repo)
            git(a_old,repo,"init","-q")
            git(a_old,repo,"config","user.email","canary@example.invalid")
            git(a_old,repo,"config","user.name","Canary A")
            file=repo/"app.py"
            file.write_text("released\n")
            owner=__import__("pwd").getpwnam(a_old.user)
            os.chown(file,owner.pw_uid,owner.pw_gid)
            git(a_old,repo,"add",".")
            git(a_old,repo,"commit","-qm","released baseline")
            cli(a_old,"remote","link",repo,"Backend")
            seed(a_old,"canary-released","Canary released","canary released sentinel 2f00ab",repo,(file,"released\n",None))
            cli(a_old,"remote","sync")
            cli(b_old,"remote","sync")
            manifest={"released_commit":released_commit,"workspace":workspace,"user_ids":[alice,bob],"history":[]}
            manifest_path.write_text(json.dumps(manifest,sort_keys=True,indent=2))
            os.chmod(manifest_path,0o600)
    manifest=json.loads(manifest_path.read_text())
    run_number=len(manifest["history"])+1
    cid=f"canary-{run_number}-{current_commit[:8]}"
    prompt=f"canary persistent sentinel {run_number} {current_commit[:12]}"
    repo=a.home/"convos-testbed"/lane/"checkout/backend"
    file=repo/"app.py"
    prior=file.read_text()
    content=f"current run {run_number}\n"
    file.write_text(content)
    owner=__import__("pwd").getpwnam(a.user)
    os.chown(file,owner.pw_uid,owner.pw_gid)
    started=time.monotonic()
    with Relay(lane,current,8788) as relay:
        before={"a":archive_evidence(a),"b":archive_evidence(b)}
        seed(a,cid,f"Canary run {run_number}",prompt,repo,(file,content,prior))
        cli(a,"remote","sync")
        mixed_client=b_old if bootstrap else b
        cli(mixed_client,"remote","sync")
        assert_team_projection(mixed_client,prompt,not bootstrap)
        cli(b,"remote","sync")
        assert_team_projection(b,prompt)
        after={"a":archive_evidence(a),"b":archive_evidence(b)}
        if any(after[name]["counts"]["conversations"]<before[name]["counts"]["conversations"] for name in before): raise AssertionError("canary archive lost conversations")
        assert_opaque(relay,prompt,repo)
        with sqlite3.connect(relay.db) as db: relay_rows=db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]
        backups={name:sorted(str(path) for path in client.root.joinpath("data").glob("convos.db.pre-v*.bak")) for name,client in (("a",a),("b",b))}
        assert_relay_isolation(relay.db,set(manifest["user_ids"]),manifest["workspace"])
        entry={"run":run_number,"released_commit":released_commit,"current_commit":current_commit,"released_version":package_version(a_old),"current_version":package_version(a),"mixed_released_client":bootstrap,"seconds":round(time.monotonic()-started,3),"archives":after,"migration_backups":backups,"relay_rows":relay_rows,"relay_plaintext":False,"peak_child_kib":resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,"doctors":{"a":cli(a,"doctor").stdout.strip(),"b":cli(b,"doctor").stdout.strip()},"harness_retries":0,"failures":[],"conflicts":sum(archive["conflicts"] for archive in after.values())}
    manifest["history"].append(entry)
    tmp=manifest_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest,sort_keys=True,indent=2))
    os.chmod(tmp,0o600)
    tmp.replace(manifest_path)
    evidence_path=STATE/"evidence"/f"canary-{run_number}-{current_commit[:12]}.json"
    evidence_path.write_text(json.dumps({"lane":lane,"passed":True,**entry},sort_keys=True,indent=2))
    os.chmod(evidence_path,0o600)
    print(json.dumps({"evidence":str(evidence_path),"lane":lane,"passed":True,**entry},sort_keys=True))


def desktop_client(root,venv):
    root=Path(root).resolve()
    return dict(root=root,venv=Path(venv).resolve(),env={**os.environ,'CONVOS_PROJECT_ROOT':str(root/'archive'),'CODEX_HOME':str(root/'codex'),'CLAUDE_CONFIG_DIR':str(root/'claude'),'TZ':{'laptop':'America/Los_Angeles','desktop':'UTC','other-user':'Asia/Kathmandu'}[root.name],'CONVOS_SEMANTIC':'off','NO_PROXY':'127.0.0.1,localhost,::1','no_proxy':'127.0.0.1,localhost,::1'})


def desktop_cli(client,*args,input=None,check=True,timeout=None):
    return run((client['venv']/'bin/convos',*args),input=input,check=check,env=client['env'],timeout=timeout)


def desktop_transcript(client,session,worktree,turns=2):
    path=client['root']/'codex/sessions'/f'rollout-2026-01-01T00-00-00-{session}.jsonl'
    path.parent.mkdir(parents=True,exist_ok=True)
    rows=[dict(type='session_meta',timestamp='2026-01-01T00:00:00Z',payload=dict(id=session,cwd=str(worktree),cli_version='qualification'))]
    rows += [dict(type='response_item',timestamp=(datetime(2026,1,1)+timedelta(seconds=i+1)).isoformat()+'Z',payload=dict(type='message',role='user' if i%2==0 else 'assistant',content=[dict(type='input_text' if i%2==0 else 'output_text',text=f'canary {session} turn {i}')])) for i in range(turns)]
    rows[1]['payload']['content'].append(dict(type='input_image',image_url='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aWQAAAABJRU5ErkJggg=='))
    path.write_text('\n'.join(json.dumps(row) for row in rows)+'\n')
    return path


def desktop_claude_transcript(client,session,worktree):
    path=client['root']/'claude/projects/checkout'/f'{session}.jsonl'
    path.parent.mkdir(parents=True,exist_ok=True)
    rows=[dict(type='system',sessionId=session,cwd=str(worktree)),
          dict(type='user',uuid='prompt',timestamp='2026-01-01T00:00:01Z',message=dict(content='inspect created.txt')),
          dict(type='assistant',uuid='answer',parentUuid='prompt',timestamp='2026-01-01T00:00:02Z',message=dict(content=[dict(type='thinking',thinking='inspect the captured file'),dict(type='tool_use',id='read-1',name='Read',input=dict(file_path='created.txt'))])),
          dict(type='user',uuid='result',parentUuid='answer',timestamp='2026-01-01T00:00:03Z',message=dict(content=[dict(type='tool_result',tool_use_id='read-1',content='captured file contents')])),
          dict(type='assistant',uuid='write',parentUuid='result',timestamp='2026-01-01T00:00:04Z',message=dict(content=[dict(type='tool_use',id='write-1',name='Write',input=dict(file_path='created.txt',content='captured file contents'))])),
          dict(type='user',uuid='written',parentUuid='write',timestamp='2026-01-01T00:00:05Z',message=dict(content=[dict(type='tool_result',tool_use_id='write-1',content='file written')]))]
    path.write_text('\n'.join(json.dumps(row) for row in rows)+'\n')
    return path


def desktop_inventory(client):
    query="SELECT c.source,json_extract_string(c.metadata,'$.session_id') provider_session,coalesce(co.source_row_id,c.id) conversation_id,coalesce(mo.source_row_id,m.id) message_id,coalesce(po.source_row_id,m.parent_id) parent_id,m.role,m.content,m.thinking,TRY_CAST(json_extract_string(m.metadata,'$.provider_index') AS BIGINT) provider_index,CAST(m.created_at AS VARCHAR) created FROM conversations c JOIN messages m ON m.conversation_id=c.id LEFT JOIN remote.row_origins co ON co.table_name='conversations' AND co.physical_row_id=c.id LEFT JOIN remote.row_origins mo ON mo.table_name='messages' AND mo.physical_row_id=m.id LEFT JOIN remote.row_origins po ON po.table_name='messages' AND po.physical_row_id=m.parent_id ORDER BY 1,2,3,10,9,4"
    return json.loads(desktop_cli(client,'sql',query,'--format','json').stdout)


def desktop_lane(root,venv,commit,baseline_venv=None,relay_venv=None):
    root=Path(root).resolve()
    with operation_lock(root.parent/f'.{root.name}.lock','customer lifecycle testbed',0): return _desktop_lane(root,venv,commit,baseline_venv,relay_venv)


def _desktop_lane(root,venv,commit,baseline_venv=None,relay_venv=None):
    import socket,uuid
    root=Path(root).resolve()
    marker=root/'testbed.json'
    if root.exists() and any(root.iterdir()) and not marker.is_file(): raise ValueError(f'refusing non-testbed directory: {root}')
    root.mkdir(parents=True,exist_ok=True)
    os.chmod(root,0o700)
    if marker.exists():
        manifest=json.loads(marker.read_text())
        if manifest.get('kind')!='convos-desktop-testbed-v1': raise ValueError('unknown testbed marker')
    else:
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port=sock.getsockname()[1]
        manifest=dict(kind='convos-desktop-testbed-v1',port=port,session=str(uuid.uuid4()),runs=[])
        atomic_json(marker,manifest)
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        sock.bind(('127.0.0.1',manifest['port']))
    clients=[desktop_client(root/name,venv) for name in ('laptop','desktop','other-user')]
    url=f"http://127.0.0.1:{manifest['port']}"
    evidence=root/f"run-{len(manifest['runs'])+1}-{int(time.time())}"
    evidence.mkdir()
    log=(evidence/'relay.log').open('w')
    relay=Path(relay_venv or venv)/'bin/convos-server'
    if baseline_venv and not manifest['runs']:
        old=[desktop_client(root/name,baseline_venv) for name in ('laptop','desktop','other-user')]
        baseline=subprocess.Popen((Path(baseline_venv)/'bin/convos-server','serve','--db',root/'relay.db','--port',str(manifest['port'])),stdout=log,stderr=subprocess.STDOUT)
        try:
            wait_health(url,baseline,version=1)
            desktop_cli(old[0],'remote','setup',url,'canary-alice','--device','laptop')
            recovery=json.loads((old[0]['root']/'archive/remote/config.json').read_text())['recovery']
            desktop_cli(old[1],'remote','recover',url,'canary-alice','--device','desktop',input=recovery+'\n')
            desktop_cli(old[2],'remote','setup',url,'canary-bob','--device','independent-author')
            for client in (old[0],old[2]):
                path=desktop_transcript(client,manifest['session'],client['root']/'deleted-checkout',2)
                desktop_cli(client,'capture','codex',input=json.dumps(dict(transcript_path=str(path),hook_event_name='Stop')))
                desktop_cli(client,'drain-hooks','--block')
                desktop_cli(client,'remote','sync')
            desktop_cli(old[1],'remote','sync')
            manifest['upgrade_identities']=[dict(user=cfg['user'],device=cfg['device']['id']) for client in old for cfg in [json.loads((client['root']/'archive/remote/config.json').read_text())]]
        finally:
            baseline.terminate()
            baseline.wait(timeout=10)
    server=subprocess.Popen((relay,'serve','--db',root/'relay.db','--port',str(manifest['port'])),stdout=log,stderr=subprocess.STDOUT)
    started=time.monotonic()
    try:
        wait_health(url,server,diagnostics=evidence/'relay-stack.log')
        a,b,c=clients
        if not (a['root']/'archive/remote/config.json').exists():
            output=desktop_cli(a,'remote','setup',url,'canary-alice','--device','laptop').stdout
            recovery=re.search(r'Recovery key \(store offline\): (\S+)',output)
            if not recovery: raise AssertionError('setup did not return a recovery key')
            desktop_cli(b,'remote','recover',url,'canary-alice','--device','desktop','--recovery',recovery[1])
            desktop_cli(c,'remote','setup',url,'canary-bob','--device','independent-author')
        session,turns=manifest['session'],manifest.get('turns',2)
        native_edits=[]
        for client in clients:
            if client is b:
                desktop_cli(a,'remote','sync')
                desktop_cli(b,'remote','sync')
            repo=client['root']/'checkout'
            if not repo.exists():
                repo.mkdir(parents=True)
                run(('git','-C',repo,'init','-q'))
                run(('git','-C',repo,'-c','user.name=Convos Testbed','-c','user.email=convos-testbed@example.invalid','commit','--allow-empty','-qm','initial'))
            worktree=repo/'.koder/worktrees/task'
            if not worktree.exists():
                worktree.parent.mkdir(parents=True,exist_ok=True)
                run(('git','-C',repo,'worktree','add','--detach',worktree,'HEAD'))
            path=desktop_transcript(client,session,worktree,2 if client is c else turns)
            desktop_cli(client,'capture','codex',input=json.dumps(dict(transcript_path=str(path),hook_event_name='Stop')))
            desktop_cli(client,'drain-hooks','--block')
            path=desktop_claude_transcript(client,session+'-claude',worktree)
            desktop_cli(client,'capture','claude-code',input=json.dumps(dict(transcript_path=str(path),hook_event_name='Stop')))
            desktop_cli(client,'drain-hooks','--block')
            native_edits.append({row['id'] for row in json.loads(desktop_cli(client,'sql',"SELECT e.id FROM file_edits e JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id WHERE e.content='captured file contents' AND v.status='confirmed'",'--format','json').stdout)})
            if not native_edits[-1]: raise AssertionError('provider-confirmed source edit was not captured')
        for iteration in range(3):
            for index,client in enumerate(clients):
                output=desktop_cli(client,'remote','sync')
                (evidence/f'sync-{iteration}-{index}.log').write_text(output.stdout+output.stderr)
        if baseline_venv:
            rejected=desktop_cli(desktop_client(root/'desktop',baseline_venv),'remote','sync',check=False)
            if rejected.returncode==0: raise AssertionError('legacy client was allowed across the reset boundary')
            identities=[dict(user=cfg['user'],device=cfg['device']['id']) for client in clients for cfg in [json.loads((client['root']/'archive/remote/config.json').read_text())]]
            if identities!=manifest['upgrade_identities']: raise AssertionError('automatic reenrollment changed a user or device identity')
        inventories=[desktop_inventory(client) for client in clients]
        for index,values in enumerate(inventories):
            (evidence/f'archive-{index}.json').write_text(json.dumps(values,indent=2)+'\n')
            selected=[v for v in values if v['provider_session']==session]
            expected=2 if index==2 else turns
            if sorted(v['content'] for v in selected)!=sorted(f'canary {session} turn {i}' for i in range(expected)): raise AssertionError(f'client {index}: expected {expected} exact Codex turns, got {len(selected)}')
            selected=[v for v in values if v['provider_session']==session+'-claude']
            if len(selected)!=5: raise AssertionError(f'client {index}: expected five linked Claude turns, got {len(selected)}')
            if [v['provider_index'] for v in selected]!=[1,2,3,4,5] or selected[1]['thinking']!='inspect the captured file': raise AssertionError(f'client {index}: Claude turn identity or thinking changed')
        if inventories[0]!=inventories[1]: raise AssertionError('same-user clients disagree on current session content')
        if len(inventories[2])!=7: raise AssertionError('independent user received private content')
        server.terminate()
        server.wait(timeout=10)
        manifest['turns']=turns+2
        path=desktop_transcript(a,session,a['root']/'checkout/.koder/worktrees/task',manifest['turns'])
        desktop_cli(a,'capture','codex',input=json.dumps(dict(transcript_path=str(path),hook_event_name='Stop')))
        desktop_cli(a,'drain-hooks','--block')
        offline=desktop_cli(a,'remote','sync',check=False)
        (evidence/'offline.log').write_text(offline.stdout+offline.stderr)
        if offline.returncode==0: raise AssertionError('offline relay reported a successful sync')
        server=subprocess.Popen((relay,'serve','--db',root/'relay.db','--port',str(manifest['port'])),stdout=log,stderr=subprocess.STDOUT)
        wait_health(url,server,diagnostics=evidence/'relay-restart-stack.log')
        for _ in range(3):
            for client in clients: desktop_cli(client,'remote','sync')
        current=[desktop_inventory(client) for client in clients]
        if current[0]!=current[1] or current[2]!=inventories[2]: raise AssertionError('offline continuation did not converge privately')
        for index in (0,1):
            if sorted(v['content'] for v in current[index] if v['provider_session']==session)!=sorted(f'canary {session} turn {i}' for i in range(manifest['turns'])): raise AssertionError('offline continuation lost or duplicated a turn')
        inventories=current
        for client in clients:
            repo=client['root']/'checkout'
            run(('git','-C',repo,'worktree','remove',repo/'.koder/worktrees/task'))
            desktop_cli(client,'sync','--full','--local-only')
            desktop_cli(client,'remote','sync')
        for _ in range(2):
            for client in clients: desktop_cli(client,'remote','sync')
        for index,client in enumerate(clients):
            audit=json.loads(desktop_cli(client,'remote','audit','--format','json').stdout)
            (evidence/f'audit-{index}.json').write_text(json.dumps(audit,indent=2)+'\n')
            if audit['totals'].get('unavailable',0) or any(v['rows'] for v in audit['relationships'].values()): raise AssertionError(f'client {index}: unresolved archive evidence')
            actual=desktop_inventory(client)
            (evidence/f'reimport-{index}.json').write_text(json.dumps(dict(expected=inventories[index],actual=actual),indent=2)+'\n')
            if actual!=inventories[index]: raise AssertionError(f'client {index}: deleted worktree reimport changed content')
            attachments=json.loads(desktop_cli(client,'sql','SELECT a.path,b.content_hash FROM attachments a JOIN attachment_bodies b ON b.attachment_id=a.id','--format','json').stdout)
            if not attachments or any(not row['path'] or sha256(Path(row['path']))!=row['content_hash'] for row in attachments): raise AssertionError(f'client {index}: attachment body missing or changed')
            edits=json.loads(desktop_cli(client,'sql',"SELECT e.id,e.content,v.status FROM file_edits e LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id",'--format','json').stdout)
            (evidence/f'edits-{index}.json').write_text(json.dumps(edits,indent=2)+'\n')
            expected_edits=native_edits[0]|native_edits[1] if index<2 else native_edits[2]
            if any(row['content']!='captured file contents' for row in edits) or not expected_edits<={row['id'] for row in edits if row['status']=='confirmed'}: raise AssertionError(f'client {index}: successful source edit lost its provider evidence')
        untrusted={**a,'env':{**a['env'],'GIT_TEST_ASSUME_DIFFERENT_OWNER':'1','GIT_CONFIG_COUNT':'1','GIT_CONFIG_KEY_0':'safe.directory','GIT_CONFIG_VALUE_0':''}}
        repo=a['root']/'checkout'
        denied=run(('git','-C',repo,'status','--porcelain'),check=False,env=untrusted['env'])
        (evidence/'git-ownership.log').write_text(denied.stderr)
        if denied.returncode==0 or 'dubious ownership' not in denied.stderr: raise AssertionError('Git ownership fault was not exercised')
        ownership_session=session+f'-ownership-{len(manifest["runs"])+1}'
        path=desktop_transcript(a,ownership_session,repo)
        desktop_cli(untrusted,'capture','codex',input=json.dumps(dict(transcript_path=str(path),hook_event_name='Stop')))
        desktop_cli(untrusted,'drain-hooks','--block')
        if len([v for v in desktop_inventory(untrusted) if v['provider_session']==ownership_session])!=2: raise AssertionError('Git ownership failure lost captured turns')
        pending=json.loads(desktop_cli(untrusted,'sql','SELECT count(*) pending FROM provenance.pending','--format','json').stdout)[0]['pending']
        if not pending: raise AssertionError('failed Git enrichment was not retained for retry')
        desktop_cli(a,'sync','--local-only')
        for _ in range(3):
            for client in clients: desktop_cli(client,'remote','sync')
        after=[desktop_inventory(client) for client in clients]
        if after[0]!=after[1] or after[2]!=inventories[2] or len([v for v in after[1] if v['provider_session']==ownership_session])!=2: raise AssertionError('ownership-failure capture did not converge privately')
        outcome=dict(commit=commit,seconds=time.monotonic()-started,success=True,evidence=str(evidence))
    except BaseException as error:
        outcome=dict(commit=commit,seconds=time.monotonic()-started,success=False,error=f'{type(error).__name__}: {error}',evidence=str(evidence))
        for index,client in enumerate(clients):
            try:
                snapshot={table:json.loads(desktop_cli(client,'sql',f'SELECT * FROM {table}','--format','json').stdout) for table in ('conversations','messages','tool_calls','file_edits','parser_message_lineage','parser_tool_lineage','parser_retired_rows','remote.row_proofs','remote.local_row_bases','remote.row_origins','remote.row_conflicts','provenance.file_edit_evidence')}
                (evidence/f'failure-archive-{index}.json').write_text(json.dumps(snapshot,indent=2)+'\n')
            except Exception as diagnostic: (evidence/f'failure-archive-{index}.log').write_text(f'{type(diagnostic).__name__}: {diagnostic}\n')
        raise
    finally:
        outcome['relay']=str(relay)
        manifest['runs'].append(outcome)
        atomic_json(marker,manifest)
        server.terminate()
        server.wait(timeout=10)
        log.close()
    print(json.dumps(outcome,indent=2))


def customer_projection(client):
    code="""import json
from pathlib import Path
from ai_convos import cli as core
from ai_convos_remote import load
root=core.PROJECT_ROOT
cfg=load(root)
with core.open_db(root/'data/convos.db',True,purpose='testbed.private.projection') as db:
 claims=[(kind,pid,source,author,'active') for kind in core.ARCHIVE_COLUMNS for pid,source,author in db.execute(f"SELECT t.id,coalesce(o.source_row_id,t.id),coalesce(o.author_user_id,?) FROM {kind} t LEFT JOIN remote.row_origins o ON o.table_name=? AND o.physical_row_id=t.id",[cfg['user'],kind]).fetchall()]
 paths=core.captured_edit_paths(db,[c[1] for c in claims if c[0]=='file_edits'])
 rows={(c[0],c[2]):{**row,'data':{**row['data'],'file_path':paths[c[1]]}} if c[0]=='file_edits' and c[1] in paths else row for c,row in core.typed_logical_rows(db,claims).items()}
 print(json.dumps({kind:dict(rows=len(selected),sha256=core.provenance_digest(selected)) for kind in core.ARCHIVE_COLUMNS for selected in [[rows[key] for key in sorted(rows) if key[0]==kind]]}))
"""
    return json.loads(run((client['venv']/'bin/python','-c',code),env=client['env']).stdout)


def customer_source_counts(client,corpus=None):
    code="""import collections,json,sys
from pathlib import Path
from ai_convos import cli as core
out=collections.defaultdict(lambda:dict.fromkeys(core.ARCHIVE_COLUMNS,0))
key=lambda source,session:json.dumps([source,session],separators=(',',':'))
if len(sys.argv)>1:
 for source,parser,path in [('codex',core.parse_codex,Path(sys.argv[1])/'codex'),('claude-code',core.parse_claude_code,Path(sys.argv[1])/'claude/projects')]:
  parsed=parser(path)
  conversations={c['id']:key(source,json.loads(c['metadata']).get('session_id')) for c in parsed.convs}
  messages={m['id']:conversations[m['conversation_id']] for m in parsed.msgs}
  for kind,rows in [('conversations',parsed.convs),('messages',parsed.msgs),('tool_calls',parsed.tools),('attachments',parsed.attachs),('artifacts',parsed.artifacts),('file_edits',parsed.edits)]:
   owners={r['id']:conversations[r['id']] if kind=='conversations' else conversations[r['conversation_id']] if kind in ('messages','artifacts') else messages[r['message_id']] for r in rows}
   for owner,count in collections.Counter(owners.values()).items(): out[owner][kind]+=count
else:
 with core.open_db(core.DB_PATH,True,purpose='testbed.private.source-counts') as db:
  for kind in core.ARCHIVE_COLUMNS:
   joined='owners c' if kind=='conversations' else 'artifacts t JOIN owners c ON c.id=t.conversation_id' if kind=='artifacts' else ('messages m' if kind=='messages' else ('current_file_edits' if kind=='file_edits' else kind)+' t JOIN messages m ON m.id=t.message_id')+' JOIN owners c ON c.id=m.conversation_id'
   current='' if kind in ('conversations','artifacts') else " AND json_extract_string(m.metadata,'$.history_of') IS NULL"
   if kind=='tool_calls': current+=' AND NOT EXISTS(SELECT 1 FROM parser_tool_history h WHERE h.old_id=t.id)'
   for source,session,count in db.execute("WITH owners AS MATERIALIZED (SELECT id,source,json_extract_string(metadata,'$.session_id') AS provider_session FROM conversations) SELECT c.source,c.provider_session,count(*) FROM "+joined+" WHERE c.source IN ('codex','claude-code')"+current+' GROUP BY 1,2').fetchall(): out[key(source,session)][kind]=count
print(json.dumps(out,sort_keys=True))
"""
    return json.loads(run((client['venv']/'bin/python','-c',code,*([str(corpus)] if corpus else [])),env=client['env'],timeout=600).stdout)


def customer_resume(client,session,evidence,measure):
    paths=[desktop_transcript(client,f'{session}-resume-{i}',client['root']/'absent-worktree') for i in range(25)]
    ledger=client['root']/'archive/data/sync_state.json'
    with (evidence/'interrupted-import.log').open('w') as log:
        process=subprocess.Popen((client['venv']/'bin/convos','sync','--local-only'),env=client['env'],stdout=log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+30
        try:
            while process.poll() is None and time.monotonic()<deadline:
                files=json.loads(ledger.read_text()).get('local',{}).get('codex',{}).get('files',{}) if ledger.exists() else {}
                completed=[str(p) for p in paths if str(p) in files]
                if len(completed)==20:
                    process.send_signal(signal.SIGINT)
                    break
                time.sleep(.01)
            else: raise AssertionError('installed import did not expose its first saved batch for interruption')
            if process.wait(timeout=10)==0: raise AssertionError('interruption did not stop the installed importer')
        finally:
            if process.poll() is None: process.kill()
            process.wait(timeout=10)
    output=measure('resume-import',client,'sync','--local-only',budget=30)
    if 'importing 5 transcript(s)' not in output.stderr: raise AssertionError('resume repeated completed transcript imports')
    (evidence/'resume.json').write_text(json.dumps(dict(checkpointed=20,resumed=5,interrupted_returncode=process.returncode))+'\n')


def customer_activity(clients,session,turns,measure):
    turns=max([turns,*[sum(json.loads(line)['type']=='response_item' for line in p.read_text().splitlines()) for c in clients[:2] for p in [c['root']/'codex/sessions'/f'rollout-2026-01-01T00-00-00-{session}-{c["root"].name}.jsonl'] if p.exists()]])
    for client in clients[:2]:
        owned=session+'-'+client['root'].name
        repo=client['root']/'activity-checkout'
        if not repo.exists():
            repo.mkdir()
            run(('git','-C',repo,'init','-q'))
            run(('git','-C',repo,'-c','user.name=Convos Testbed','-c','user.email=test@example.invalid','commit','--allow-empty','-qm','initial'))
        desktop_transcript(client,owned,repo,turns)
        desktop_claude_transcript(client,owned+'-claude',repo)
        measure('activity-seed-'+client['root'].name,client,'sync','--local-only',budget=60)
    def append(client):
        owned=session+'-'+client['root'].name
        path=client['root']/'codex/sessions'/f'rollout-2026-01-01T00-00-00-{owned}.jsonl'
        for i in range(turns,turns+4):
            row=dict(type='response_item',timestamp=(datetime(2026,1,1)+timedelta(seconds=i+1)).isoformat()+'Z',payload=dict(type='message',role='user' if i%2==0 else 'assistant',content=[dict(type='input_text' if i%2==0 else 'output_text',text=f'canary {owned} turn {i}')]))
            with path.open('a') as stream: stream.write(json.dumps(row)+'\n')
            for event in ('PostToolUse','Stop','SessionEnd'):
                measure(f'capture-{client["root"].name}-{i}-{event}',client,'capture','codex',input=json.dumps(dict(transcript_path=str(path),hook_event_name=event)),budget=5)
        claude=client['root']/'claude/projects/checkout'/f'{owned}-claude.jsonl'
        measure('capture-claude-'+client['root'].name,client,'capture','claude-code',input=json.dumps(dict(transcript_path=str(claude),hook_event_name='Stop')),budget=5)
    with ThreadPoolExecutor(max_workers=10) as pool:
        jobs=[pool.submit(append,client) for client in clients[:2]]
        jobs += [pool.submit(measure,f'concurrent-{client["root"].name}-{args[0]}',client,*args,budget=budget,check=args[0]!='sync') for client in clients[:2] for args,budget in ((('sync','--local-only'),30),(('remote','sync'),120),(('search','canary'),15),(('doctor',),15))]
        for job in jobs: job.result()
    for client in clients[:2]:
        measure('settle-hooks-'+client['root'].name,client,'drain-hooks','--block',budget=30)
        measure('settle-local-'+client['root'].name,client,'sync','--local-only',budget=30)
    for iteration in range(3):
        for client in clients: measure(f'activity-remote-{iteration}-{client["root"].name}',client,'remote','sync',budget=120)
    rows=[desktop_inventory(client) for client in clients]
    if rows[0]!=rows[1] or rows[2]: raise AssertionError('concurrent customer capture did not converge privately')
    for values in rows[:2]:
        for client in clients[:2]:
            owned=session+'-'+client['root'].name
            if sorted(v['content'] for v in values if v['provider_session']==owned)!=sorted(f'canary {owned} turn {i}' for i in range(turns+4)): raise AssertionError('concurrent capture lost or duplicated a turn')
    return rows,turns+4


def customer_large_append(client,manifest,evidence,measure):
    from ai_convos.cli import _backup_copy, open_db
    entry=max((v for v in manifest['corpus'] if v['path'].startswith('codex/sessions/')),key=lambda v:v['bytes'])
    source=client['root']/entry['path']
    archive=client['root']/'archive'
    clone=desktop_client(evidence/'large-append/laptop',client['venv'])
    clone['env']={**client['env'],'CONVOS_PROJECT_ROOT':str(clone['root']/'archive')}
    target=clone['root']/'archive'
    (target/'data').mkdir(parents=True,mode=0o700)
    (target/'remote').mkdir(mode=0o700)
    backup=evidence/'large-append/transcript.original'
    with operation_lock(archive/'data/.sync.lock','testbed.large-append.snapshot',0),operation_lock(archive/'data/hook_inbox/.drain.lock','testbed.large-append.capture',30):
        with open_db(archive/'data/convos.db',True,purpose='testbed.large-append.snapshot'):
            if (archive/'data/convos.db.wal').exists(): raise ValueError('large-append snapshot requires a checkpointed archive')
            _backup_copy(archive/'data/convos.db',target/'data/convos.db')
        for relative in ('data/sync_state.json','remote/config.json'):
            shutil.copy2(archive/relative,target/relative)
        if (archive/'data/attachments').exists(): shutil.copytree(archive/'data/attachments',target/'data/attachments',copy_function=_backup_copy)
        before,children=desktop_inventory(clone),customer_projection(clone)
        if sha256(source)!=entry['sha256']: raise ValueError('large-append input differs from its frozen source')
        _backup_copy(source,backup)
        shutil.copystat(source,backup)
        sentinels=[f'private large-transcript qualification {evidence.name} turn {i}' for i in range(2)]
        try:
            with source.open('a') as stream:
                for i,text in enumerate(sentinels):
                    stream.write(json.dumps(dict(type='response_item',timestamp=f'2026-09-18T00:00:0{i}Z',payload=dict(type='message',role='user' if i==0 else 'assistant',content=[dict(type='input_text' if i==0 else 'output_text',text=text)])))+'\n')
            measure('large-append-capture',clone,'capture','codex',input=json.dumps(dict(transcript_path=str(source),hook_event_name='Stop')),budget=5)
            with ThreadPoolExecutor(max_workers=4) as pool:
                jobs=[pool.submit(measure,'large-append-'+label,clone,*args,budget=budget) for label,args,budget in (('drain',('drain-hooks','--block'),120),('sync',('sync','--local-only'),120),('search',('search','qualification'),15),('doctor',('doctor',),15))]
                for job in jobs: job.result()
            after=desktop_inventory(clone)
            if [row for row in after if row['content'] not in sentinels]!=before or sorted(row['content'] for row in after if row['content'] in sentinels)!=sorted(sentinels): raise AssertionError('large transcript append lost, duplicated, or changed existing turns')
            changed=customer_projection(clone)
            if any(changed[kind]!=value for kind,value in children.items() if kind not in ('conversations','messages')): raise AssertionError('large transcript append changed unrelated child rows')
            (evidence/'large-append.json').write_text(json.dumps(dict(bytes=entry['bytes'],added_messages=2,original_messages=len(before),success=True),indent=2)+'\n')
        finally:
            os.replace(backup,source)
            if sha256(source)!=entry['sha256']: raise AssertionError('large-append test did not restore its private transcript copy')


def customer_lane(root,venv,commit,codex,claude,sessions=12,full=False):
    import socket
    root,venv=Path(root).resolve(),Path(venv).resolve()
    if os.environ.get('CI'): raise ValueError('real conversation qualification is local-only; CI uses the synthetic desktop lane')
    marker=root/'customer.json'
    if root.exists() and any(root.iterdir()) and not marker.is_file(): raise ValueError('refusing a non-customer-testbed directory')
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    os.chmod(root,0o700)
    os.umask(0o077)
    with operation_lock(root/'.qualification.lock','private customer qualification',0):
        if marker.exists():
            manifest=json.loads(marker.read_text())
            if manifest['kind']!='convos-private-customer-v1': raise ValueError('unknown customer testbed marker')
        else:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0))
                port=sock.getsockname()[1]
            manifest=dict(kind='convos-private-customer-v1',port=port,corpus=[],runs=[])
            sources={'codex/sessions':Path(codex).resolve()/'sessions','claude/projects':Path(claude).resolve()/'projects'}
            for target,source in sources.items():
                files=sorted(p for p in source.rglob('*.jsonl') if p.is_file() and not p.is_symlink() and inside(p,source) and p.stat().st_mtime<time.time()-60)
                if target.startswith('codex'):
                    selected=[files[i] for i in sorted({round(j*(len(files)-1)/max(1,min(sessions,len(files))-1)) for j in range(min(sessions,len(files)))})]
                    files=sorted(set(selected+sorted(files,key=lambda p:p.stat().st_size)[-2:]))
                if not files: raise ValueError(f'no stable transcript inputs in {source}')
                for path in files:
                    with path.open('rb') as stream:
                        before=os.fstat(stream.fileno())
                        body=stream.read(before.st_size)
                        after=os.fstat(stream.fileno())
                    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns): raise ValueError('a selected transcript changed during its snapshot; retry')
                    if not body.endswith(b'\n'): body=body[:body.rfind(b'\n')+1]
                    if not body: raise ValueError('empty transcript snapshot')
                    relative=Path(target)/path.relative_to(source)
                    destination=root/'corpus'/relative
                    destination.parent.mkdir(parents=True,exist_ok=True)
                    destination.write_bytes(body)
                    manifest['corpus'].append(dict(source=str(path),path=str(relative),bytes=len(body),sha256=hashlib.sha256(body).hexdigest()))
            atomic_json(marker,manifest)
        clients=[desktop_client(root/name,venv) for name in ('laptop','desktop','other-user')]
        for client in clients:
            home=client['root']/'home'
            home.mkdir(parents=True,exist_ok=True)
            client['env']={key:value for key,value in client['env'].items() if key in ('CONVOS_PROJECT_ROOT','CODEX_HOME','CLAUDE_CONFIG_DIR','CONVOS_SEMANTIC','TZ','NO_PROXY','no_proxy')}
            client['env'].update(HOME=str(home),PATH=f'{venv}/bin:/usr/bin:/bin:/usr/sbin:/sbin',XDG_CONFIG_HOME=str(home/'config'),XDG_CACHE_HOME=str(home/'cache'),XDG_DATA_HOME=str(home/'data'),LANG='en_US.UTF-8')
        for entry in manifest['corpus']:
            source=root/'corpus'/entry['path']
            if Path(entry['path']).is_absolute() or not inside(source,root/'corpus'): raise ValueError('invalid corpus snapshot path')
            if source.is_symlink() or sha256(source)!=entry['sha256']: raise ValueError('private corpus snapshot changed')
            for client in clients[:2]:
                destination=client['root']/entry['path']
                destination.parent.mkdir(parents=True,exist_ok=True)
                if not destination.exists(): shutil.copyfile(source,destination)
                if sha256(destination)!=entry['sha256']: raise ValueError('client transcript copy changed')
        url=f"http://127.0.0.1:{manifest['port']}"
        evidence=root/f"check-{len(manifest['runs'])+1}-{int(time.time())}"
        evidence.mkdir(mode=0o700)
        started=time.monotonic()
        timings=[]
        def measure(label,client,*args,budget,input=None,check=True):
            tick=time.monotonic()
            try:
                output=desktop_cli(client,*args,input=input,check=check,timeout=budget)
                (evidence/f'{label}.log').write_text(output.stdout+output.stderr)
                return output
            except subprocess.TimeoutExpired as error:
                (evidence/f'{label}.log').write_bytes((error.stdout or b'')+(error.stderr or b''))
                raise
            finally: timings.append(dict(workload=label,seconds=time.monotonic()-tick,budget=budget))
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            sock.bind(('127.0.0.1',manifest['port']))
        with (evidence/'relay.log').open('w') as log:
            server=subprocess.Popen((venv/'bin/convos-server','serve','--db',root/'relay.db','--host','127.0.0.1','--port',str(manifest['port'])),stdout=log,stderr=subprocess.STDOUT,env=clients[2]['env'])
            try:
                wait_health(url,server)
                a,b,c=clients
                if not (a['root']/'archive/remote/config.json').exists(): desktop_cli(a,'remote','setup',url,'customer-alice','--device','laptop')
                if not (b['root']/'archive/remote/config.json').exists():
                    recovery=json.loads((a['root']/'archive/remote/config.json').read_text())['recovery']
                    desktop_cli(b,'remote','recover',url,'customer-alice','--device','desktop',input=recovery+'\n')
                if not (c['root']/'archive/remote/config.json').exists(): desktop_cli(c,'remote','setup',url,'customer-bob','--device','independent-user')
                for index,client in enumerate(clients):
                    if index==1:
                        measure('source-owner-seed',clients[0],'remote','sync',budget=600)
                        measure('peer-owner-receive',client,'remote','sync',budget=600)
                    measure(f'import-{index}',client,'sync','--local-only',*(['--full'] if full else []),budget=600)
                before=[desktop_inventory(client) for client in clients[:2]]
                activity=manifest.setdefault('activity_session','qualification-live-'+hashlib.sha256(str(root).encode()).hexdigest()[:16])
                content=lambda rows:sorted((json.dumps({k:v for k,v in row.items() if k not in ('conversation_id','message_id','parent_id')},sort_keys=True) for row in rows if not (row['provider_session'] or '').startswith(activity)))
                content_hash=lambda rows:hashlib.sha256(json.dumps(content(rows)).encode()).hexdigest()
                if 'source_projection' not in manifest:
                    if content(before[0])!=content(before[1]): raise AssertionError('same source snapshots parsed differently across device timezones')
                    manifest['source_projection']=content_hash(before[0])
                    atomic_json(marker,manifest)
                if 'source_counts' not in manifest:
                    manifest['source_counts']=customer_source_counts(a,root/'corpus')
                    atomic_json(marker,manifest)
                for iteration in range(3):
                    for index,client in enumerate(clients):
                        measure(f'sync-{iteration}-{index}',client,'remote','sync',budget=600)
                    if iteration<2:
                        for index,client in enumerate(clients[:2]): measure(f'source-bindings-{iteration}-{index}',client,'sync','--local-only',budget=600)
                source_counts=[{key:value for key,value in customer_source_counts(client).items() if not (json.loads(key)[1] or '').startswith(activity)} for client in clients]
                (evidence/'source-counts.json').write_text(json.dumps(dict(expected=manifest['source_counts'],clients=source_counts),indent=2)+'\n')
                if source_counts[:2]!=[manifest['source_counts']]*2 or source_counts[2]: raise AssertionError('synced current rows do not match frozen source multiplicity')
                inventories=[desktop_inventory(client) for client in clients]
                if inventories[0]!=inventories[1]: raise AssertionError('real conversation IDs, parents, or content diverged between same-user devices')
                if content_hash(inventories[0])!=manifest['source_projection']: raise AssertionError('real conversation sync lost or duplicated source turns')
                if inventories[2]: raise AssertionError('independent user received private conversation content')
                projections=[customer_projection(client) for client in clients]
                if projections[0]!=projections[1] or any(v['rows'] for v in projections[2].values()): raise AssertionError('real conversation child rows diverged or crossed users')
                for index,client in enumerate(clients): measure(f'noop-local-{index}',client,'sync','--local-only',budget=15)
                for index,client in enumerate(clients): measure(f'noop-remote-{index}',client,'remote','sync',budget=15)
                if [customer_projection(client) for client in clients]!=projections: raise AssertionError('no-op import changed retained logical rows')
                if not manifest.get('resume_verified'):
                    customer_resume(clients[0],activity+f'-attempt-{len(manifest["runs"])}',evidence,measure)
                    manifest['resume_verified']=True
                inventories,manifest['activity_turns']=customer_activity(clients,activity,manifest.get('activity_turns',2),measure)
                if content_hash(inventories[0])!=manifest['source_projection']: raise AssertionError('concurrent activity changed frozen source turns')
                projections=[customer_projection(client) for client in clients]
                if projections[0]!=projections[1]: raise AssertionError('concurrent activity diverged in child rows')
                for index,client in enumerate(clients):
                    audit=json.loads(desktop_cli(client,'remote','audit','--format','json').stdout)
                    (evidence/f'audit-{index}.json').write_text(json.dumps(audit,indent=2)+'\n')
                    if audit['totals'].get('unavailable',0) or any(v['rows'] for v in audit['relationships'].values()): raise AssertionError(f'client {index}: unresolved real archive evidence')
                    measure(f'doctor-{index}',client,'doctor',budget=15)
                customer_large_append(a,manifest,evidence,measure)
                outcome=dict(commit=commit,success=True,full_reimport=full,seconds=time.monotonic()-started,projections=projections,messages=[len(rows) for rows in inventories],projection_sha256=[hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest() for rows in inventories],evidence=str(evidence))
            except BaseException as error:
                outcome=dict(commit=commit,success=False,seconds=time.monotonic()-started,error=f'{type(error).__name__}: {error}',evidence=str(evidence))
                raise
            finally:
                server.terminate()
                server.wait(timeout=10)
                manifest['runs'].append(outcome)
                (evidence/'timings.json').write_text(json.dumps(timings,indent=2)+'\n')
                atomic_json(marker,manifest)
        print(json.dumps(outcome,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest="command",required=True)
    lane=sub.add_parser("fresh")
    lane.add_argument("--venv",type=Path,required=True)
    lane.add_argument("--commit",required=True)
    lane.add_argument("--released-venv",type=Path)
    canary=sub.add_parser("canary")
    canary.add_argument("--released-venv",type=Path,required=True)
    canary.add_argument("--current-venv",type=Path,required=True)
    canary.add_argument("--released-commit",required=True)
    canary.add_argument("--current-commit",required=True)
    desktop=sub.add_parser("desktop")
    desktop.add_argument("--root",type=Path,required=True)
    desktop.add_argument("--venv",type=Path,required=True)
    desktop.add_argument("--commit",required=True)
    desktop.add_argument("--baseline-venv",type=Path)
    desktop.add_argument("--relay-venv",type=Path)
    customer=sub.add_parser("customer")
    for name in ('root','venv','codex','claude'): customer.add_argument('--'+name,type=Path,required=True)
    customer.add_argument('--commit',required=True)
    customer.add_argument('--sessions',type=int,default=12)
    customer.add_argument('--full',action='store_true')
    seed_parser=sub.add_parser("seed")
    seed_parser.add_argument("root",type=Path)
    seed_parser.add_argument("cid")
    seed_parser.add_argument("title")
    seed_parser.add_argument("prompt")
    seed_parser.add_argument("--cwd",type=Path)
    seed_parser.add_argument("--edit",type=Path)
    seed_parser.add_argument("--content")
    seed_parser.add_argument("--old-content")
    args=parser.parse_args()
    if args.command=="fresh": fresh_lane(args.venv,args.commit,args.released_venv)
    elif args.command=="canary": canary_lane(args.released_venv,args.current_venv,args.released_commit,args.current_commit)
    elif args.command=="desktop": desktop_lane(args.root,args.venv,args.commit,args.baseline_venv,args.relay_venv)
    elif args.command=="customer": customer_lane(args.root,args.venv,args.commit,args.codex,args.claude,args.sessions,args.full)
    else: seed_archive(isolated_root(args.root),args.cid,args.title,args.prompt,args.cwd,args.edit,args.content,args.old_content)
if __name__=="__main__": main()
