import json, os, socket, sqlite3, subprocess, time, urllib.request
from pathlib import Path

import duckdb
from ai_convos.cli import capture_provenance, init_schema
from ai_convos.cli import repository
from ai_convos_remote import add_member, connect, create, load, publish, setup_client, upload


def port():
    with socket.socket() as s: s.bind(("127.0.0.1",0)); return s.getsockname()[1]
def wait(url,timeout=5):
    end=time.time()+timeout
    while time.time()<end:
        try: return urllib.request.urlopen(url,timeout=.2).read()
        except Exception: time.sleep(.05)
    raise AssertionError(f"server did not start: {url}")


def test_real_http_background_two_device_delivery_under_ten_seconds(tmp_path):
    p=port(); url=f"http://127.0.0.1:{p}"; server=subprocess.Popen(("convos-server","serve","--db",str(tmp_path/"server.db"),"--port",str(p)),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True); workers=[]
    try:
        wait(url+"/v1/health"); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client(url,"alice","laptop",root=a); setup_client(url,"alice","desktop",recovery,root=b)
        (a/"data").mkdir(parents=True); db=duckdb.connect(str(a/"data/convos.db")); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','from laptop','2026-01-01','2026-01-01','m',NULL,NULL,NULL,'{}')"); db.execute("INSERT INTO messages VALUES ('m','c','user','automatic delivery',NULL,'2026-01-01','m','{}',NULL,NULL)"); db.close()
        for root in (a,b): workers.append(subprocess.Popen(("convos","remote","watch","--interval","1"),env={**dict(__import__('os').environ),"CONVOS_PROJECT_ROOT":str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
        end=time.time()+10; found=False
        while time.time()<end:
            try:
                db=duckdb.connect(str(b/"data/convos.db"),read_only=True); found=db.execute("SELECT COUNT(*) FROM messages WHERE content='automatic delivery'").fetchone()[0]==1; db.close()
                if found: break
            except Exception: pass
            time.sleep(.2)
        assert found
    finally:
        [w.terminate() for w in workers]; [w.wait(timeout=3) for w in workers]; server.terminate(); server.wait(timeout=3)


def test_real_http_background_delivers_large_encrypted_memory_without_lazy_fetch(tmp_path):
    p=port(); url=f"http://127.0.0.1:{p}"; server=subprocess.Popen(("convos-server","serve","--db",str(tmp_path/"server.db"),"--port",str(p)),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True); workers=[]
    try:
        wait(url+"/v1/health"); a,b=tmp_path/"a",tmp_path/"b"; _,recovery=setup_client(url,"alice","laptop",root=a); setup_client(url,"alice","desktop",recovery,root=b); content="private large memory marker\n"+"x"*70000; env={**os.environ,"CONVOS_PROJECT_ROOT":str(a)}; created=json.loads(subprocess.run(("convos","memory","remember","-","--project","global","--json"),input=content,text=True,env=env,check=True,capture_output=True).stdout)
        for root in (a,b): workers.append(subprocess.Popen(("convos","remote","watch","--interval","1"),env={**os.environ,"CONVOS_PROJECT_ROOT":str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
        end=time.time()+10; found=False
        while time.time()<end:
            try: db=sqlite3.connect(b/"memory/state.db"); found=db.execute("SELECT content FROM canonicals").fetchone()==(content,) and db.execute("SELECT state,COUNT(*) FROM remote_semantics GROUP BY state").fetchall()==[("active",1)]; db.close()
            except Exception: pass
            if found: break
            time.sleep(.2)
        state=connect(b/"remote/state.db"); lazy=state.execute("SELECT COUNT(*) FROM lazy_events").fetchone()[0]; state.close(); raw=(tmp_path/"server.db").read_bytes().decode(errors="ignore")
        assert found and lazy==0 and "private large memory marker" not in raw and str(a) not in raw and str(b) not in raw
        subprocess.run(("convos","memory","forget",created["id"],"--project","global","--json"),env=env,check=True,capture_output=True); end=time.time()+10; forgotten=False
        while time.time()<end:
            try: db=sqlite3.connect(b/"memory/state.db"); forgotten=db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0]==0 and db.execute("SELECT state FROM remote_semantics").fetchall()==[("deleted",)]; db.close()
            except Exception: forgotten=False
            if forgotten: break
            time.sleep(.2)
        state=connect(b/"remote/state.db"); settled=state.execute("SELECT COUNT(*) FROM replica_receipts").fetchone()[0]; state.close(); assert forgotten and settled and b"private large memory marker" not in (b/"remote/state.db").read_bytes()
        subprocess.run(("convos","memory","remember","-","--project","global","--json"),input=content,text=True,env=env,check=True,capture_output=True); end=time.time()+10; restored=False
        while time.time()<end:
            try: db=sqlite3.connect(b/"memory/state.db"); restored=db.execute("SELECT content FROM canonicals").fetchone()==(content,); db.close()
            except Exception: pass
            if restored: break
            time.sleep(.2)
        assert restored
    finally:
        [w.terminate() for w in workers]; [w.wait(timeout=3) for w in workers]; server.terminate(); server.wait(timeout=3)


def test_real_http_team_policy_across_different_checkout_paths(tmp_path):
    p=port(); url=f"http://127.0.0.1:{p}"; server=subprocess.Popen(("convos-server","serve","--db",str(tmp_path/"server.db"),"--port",str(p)),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); workers=[]
    try:
        wait(url+"/v1/health"); a,b=tmp_path/"alice",tmp_path/"bob"; repo_a=tmp_path/"alice-work/backend"; repo_a.mkdir(parents=True); subprocess.run(("git","-C",str(repo_a),"init","-q"),check=True); subprocess.run(("git","-C",str(repo_a),"config","user.email","a@b.c"),check=True); subprocess.run(("git","-C",str(repo_a),"config","user.name","A"),check=True); (repo_a/"app.py").write_text("new\n"); subprocess.run(("git","-C",str(repo_a),"add","."),check=True); subprocess.run(("git","-C",str(repo_a),"commit","-qm","init"),check=True); repo_b=tmp_path/"elsewhere/deep/backend"; repo_b.parent.mkdir(parents=True); subprocess.run(("git","clone","-q",str(repo_a),str(repo_b)),check=True)
        alice,_=setup_client(url,"alice",root=a); setup_client(url,"bob",root=b); team=create(alice,"Backend","team",a); add_member(alice,team,"bob",root=a); alice=load(a); state=connect(a/"remote/state.db"); rid=repository(repo_a)["id"]; state.execute("INSERT INTO policies VALUES (?,?,?,?,?)",(team,alice["user"],"repository",rid,None)); state.execute("DELETE FROM meta WHERE key LIKE 'core_generation:%'"); state.commit(); publish(alice,state,team,{"kind":"workspace.policy","entity":f"policy:repository:{rid}","payload":{"kind":"repository","value":rid}},a); upload(alice,state,a)
        (a/"data").mkdir(parents=True); db=duckdb.connect(str(a/"data/convos.db")); init_schema(db); db.execute("INSERT INTO conversations VALUES ('c','codex','team work','2026-01-01','2026-01-01','m',?,NULL,NULL,'{}')",[str(repo_a)]); db.execute("INSERT INTO messages VALUES ('u','c','user','change backend',NULL,'2026-01-01 00:00:00','m','{}',NULL,NULL),('m','c','assistant','done',NULL,'2026-01-01 00:00:01','m','{}',NULL,NULL)"); db.execute("INSERT INTO file_edits VALUES ('e','m',?,'write','new\n','2026-01-01 00:00:01',NULL)",[str(repo_a/'app.py')]); db.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL)"); db.close(); capture_provenance(a/"data/convos.db")
        for root in (a,b): workers.append(subprocess.Popen(("convos","remote","watch","--interval","1"),env={**dict(__import__('os').environ),"CONVOS_PROJECT_ROOT":str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
        end=time.time()+10; found=False; activity=None
        while time.time()<end:
            try: check=duckdb.connect(str(b/"data/convos.db"),read_only=True); found=check.execute("SELECT COUNT(*) FROM messages WHERE content='change backend'").fetchone()[0]==1; activity=check.execute("SELECT COUNT(*) FROM provenance.files f JOIN provenance.file_edit_files x ON x.file_id=f.id WHERE f.repository=?",[rid]).fetchone()[0] if found else None; check.close()
            except Exception: pass
            if found: break
            time.sleep(.2)
        assert found and activity==1
        raw=(tmp_path/"server.db").read_bytes().decode(errors="ignore"); assert str(repo_a) not in raw and str(repo_b) not in raw and "change backend" not in raw
    finally:
        [w.terminate() for w in workers]; [w.wait(timeout=3) for w in workers]; server.terminate(); server.wait(timeout=3)
