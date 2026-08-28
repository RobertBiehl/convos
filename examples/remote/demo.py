#!/usr/bin/env python3
"""Run isolated personal and team remote examples with synthetic data only."""
import argparse, json, os, shutil, socket, subprocess, sys, tempfile, time, urllib.request
from contextlib import contextmanager
from pathlib import Path

import duckdb
from ai_convos.cli import capture_provenance, init_schema
from ai_convos_remote import add_member, create, setup_client


def free_port():
    with socket.socket() as sock: sock.bind(("127.0.0.1",0)); return sock.getsockname()[1]
def run(root,*args):
    env={**os.environ,"CONVOS_PROJECT_ROOT":str(root)}
    return subprocess.run((sys.executable,"-m","ai_convos",*args),env=env,text=True,capture_output=True,check=True).stdout.strip()
def git(root,*args): return subprocess.run(("git","-C",str(root),*args),check=True,text=True,capture_output=True).stdout.strip()
def seed(root,cid,title,prompt,cwd=None,edit=None):
    path=Path(root)/"data/convos.db"; path.parent.mkdir(parents=True,exist_ok=True); db=duckdb.connect(str(path)); init_schema(db)
    db.execute("INSERT INTO conversations (id,source,title,created_at,updated_at,cwd,metadata) VALUES (?,?,?,'2026-01-01','2026-01-01',?,'{}')",(cid,"demo",title,str(cwd) if cwd else None)); db.execute("INSERT INTO messages (id,conversation_id,role,content,created_at,metadata) VALUES (?,?, 'user',?,'2026-01-01 00:00:00','{}'),(?,?, 'assistant','done','2026-01-01 00:00:01','{}')",(f"u-{cid}",cid,prompt,f"a-{cid}",cid))
    if edit:
        db.execute("INSERT INTO file_edits (id,message_id,file_path,edit_type,content,created_at,old_content) VALUES (?,?,?,'write',?,'2026-01-01 00:00:01',?)",(f"e-{cid}",f"a-{cid}",str(edit[0]),edit[1],edit[2]))
        db.execute("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','synthetic_fixture',NULL)",(f"e-{cid}",))
    db.close(); capture_provenance(path)
def wait_message(root,content,workers,timeout=10,graph=False):
    start=time.monotonic(); path=Path(root)/"data/convos.db"
    while time.monotonic()-start<timeout:
        for base,worker in workers:
            if worker.poll() is not None: raise RuntimeError(f"worker exited for {base}")
            if (error:=Path(base)/"remote/last_error").exists() and "Could not set lock on file" not in (failure:=error.read_text()): raise RuntimeError(failure)
        if path.exists():
            try:
                db=duckdb.connect(str(path),read_only=True); found=db.execute("SELECT COUNT(*) FROM messages WHERE content=?",(content,)).fetchone()[0] and (not graph or db.execute("SELECT COUNT(*) FROM provenance.file_edit_files").fetchone()[0]); db.close()
                if found: return time.monotonic()-start
            except duckdb.Error: pass
        time.sleep(.1)
    raise TimeoutError(f"message was not delivered within {timeout}s")
def assert_opaque(path,*values):
    raw=b"".join(p.read_bytes() for p in path.parent.glob(path.name+"*") if p.is_file())
    assert all(str(value).encode() not in raw for value in values)


@contextmanager
def sandbox(name,keep=False):
    path=Path(tempfile.mkdtemp(prefix=f"convos-{name}-"))
    try: yield path
    finally:
        if keep: print(json.dumps({"kept":str(path)}))
        else: shutil.rmtree(path,ignore_errors=True)
@contextmanager
def relay(base):
    port=free_port(); url=f"http://127.0.0.1:{port}"; db=base/"relay.db"; process=subprocess.Popen((sys.executable,"-c","from ai_convos_remote_server import main; main()","serve","--db",str(db),"--port",str(port)),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try: urllib.request.urlopen(url+"/v1/health",timeout=.1).read(); break
            except Exception: time.sleep(.05)
        else: raise RuntimeError("relay did not start")
        yield url,db
    finally: process.terminate(); process.wait(timeout=3)
@contextmanager
def watchers(*roots):
    workers=[(root,subprocess.Popen((sys.executable,"-m","ai_convos","remote","watch","--interval","1"),env={**os.environ,"CONVOS_PROJECT_ROOT":str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)) for root in roots]
    try: yield workers
    finally:
        for _,worker in workers: worker.terminate()
        for _,worker in workers: worker.wait(timeout=3)


def personal(keep=False):
    with sandbox("personal",keep) as base, relay(base) as (url,server):
        laptop,desktop=base/"laptop",base/"desktop"; _,recovery=setup_client(url,"demo-alice","laptop",root=laptop); setup_client(url,"demo-alice","desktop",recovery,root=desktop)
        with watchers(laptop,desktop) as running:
            prompt="Preserve causal parents in the parser."; seed(laptop,"personal-demo","Personal demo",prompt); elapsed=wait_message(desktop,prompt,running)
        assert_opaque(server,prompt,laptop,desktop); db=duckdb.connect(str(desktop/"data/convos.db"),read_only=True); title=db.execute("SELECT title FROM conversations WHERE source='demo'").fetchone()[0]; db.close(); result={"scenario":"personal","automatic":True,"seconds":round(elapsed,2),"title":title,"relay_plaintext":False}; print(json.dumps(result)); return result
def team(keep=False):
    with sandbox("team",keep) as base, relay(base) as (url,server):
        alice_root,bob_root=base/"alice-device",base/"bob-device"; alice,_=setup_client(url,"demo-alice","laptop",root=alice_root); setup_client(url,"demo-bob","desktop",root=bob_root); workspace=create(alice,"Backend","team",alice_root); add_member(alice,workspace,"demo-bob",root=alice_root)
        alice_repo=base/"alice-checkouts/backend"; alice_repo.mkdir(parents=True); git(alice_repo,"init","-q"); git(alice_repo,"config","user.email","demo@example.com"); git(alice_repo,"config","user.name","Demo"); (alice_repo/"app.py").write_text("before\n"); git(alice_repo,"add","."); git(alice_repo,"commit","-qm","initial"); bob_repo=base/"unrelated/location/backend"; bob_repo.parent.mkdir(parents=True); subprocess.run(("git","clone","-q",str(alice_repo),str(bob_repo)),check=True); run(alice_root,"remote","link",str(alice_repo),"Backend")
        prompt="Update the backend response."; (alice_repo/"app.py").write_text("after\n"); seed(alice_root,"team-demo","Team demo",prompt,alice_repo,(alice_repo/"app.py","after\n","before\n"))
        with watchers(alice_root,bob_root) as running: elapsed=wait_message(bob_root,prompt,running,graph=True)
        db=duckdb.connect(str(bob_root/"data/convos.db"),read_only=True); shared_path=db.execute("SELECT file_path FROM file_edits").fetchone()[0]; repositories,files,activity=db.execute("SELECT COUNT(DISTINCT f.repository),COUNT(DISTINCT f.path),COUNT(*) FROM provenance.file_edit_files x JOIN provenance.files f ON f.id=x.file_id").fetchone(); db.close()
        assert shared_path=="app.py" and activity; assert_opaque(server,prompt,alice_repo,bob_repo); result={"scenario":"team","automatic":True,"seconds":round(elapsed,2),"different_checkout_paths":True,"shared_path":shared_path,"repositories":repositories,"files":files,"repository_activity":activity,"relay_plaintext":False}; print(json.dumps(result)); return result


def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("scenario",choices=("personal","team","all"),nargs="?",default="all"); parser.add_argument("--keep",action="store_true",help="keep synthetic files for inspection"); args=parser.parse_args()
    if args.scenario in ("personal","all"): personal(args.keep)
    if args.scenario in ("team","all"): team(args.keep)
if __name__=="__main__": main()
