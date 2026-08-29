#!/usr/bin/env python3
"""Run isolated multi-user Convos qualification lanes inside the Titan container."""
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
from pathlib import Path

import duckdb
from ai_convos.cli import archive_state, capture_provenance, init_schema

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


def run(command,*,input=None,check=True,env=None):
    command=tuple(map(str,command))
    try: return subprocess.run(command,input=input,text=True,capture_output=True,check=check,env=env)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"command failed ({error.returncode}): {' '.join(command)}\n{error.stdout}{error.stderr}") from error
def as_user(client,*command,input=None,check=True):
    env=("env",f"HOME={client.home}",f"PATH={client.venv/'bin'}:/usr/bin:/bin",f"CONVOS_PROJECT_ROOT={client.root}")
    return run(("runuser","-u",client.user,"--",*env,*command),input=input,check=check)
def cli(client,*args,input=None,check=True): return as_user(client,client.convos,*args,input=input,check=check)
def sha256(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def package_version(client): return as_user(client,client.python,"-c","import importlib.metadata\nprint(importlib.metadata.version('convos'))").stdout.strip()
def wait_health(url,process,timeout=15):
    started=time.monotonic()
    while time.monotonic()-started<timeout:
        if process.poll() is not None: raise RuntimeError(f"relay exited: {process.stderr.read()}")
        try: return json.loads(urllib.request.urlopen(url+"/v1/health",timeout=.2).read())
        except Exception: time.sleep(.1)
    raise TimeoutError("relay did not become healthy")


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
    else: seed_archive(isolated_root(args.root),args.cid,args.title,args.prompt,args.cwd,args.edit,args.content,args.old_content)
if __name__=="__main__": main()
