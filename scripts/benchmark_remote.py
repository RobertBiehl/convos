#!/usr/bin/env python3
"""Measure Remote v1 locally against an archive without publishing its content."""
import argparse, json, shutil, subprocess, sys, tempfile, time
from datetime import timedelta
from pathlib import Path

import duckdb
import ai_convos_remote as remote
from ai_convos.cli import ARCHIVE_COLUMNS, init_schema, project_archive_row
from ai_convos_remote.projection import connect as state_connect
from ai_convos_remote_server import action, connect as relay_connect


def elapsed(start): return round(time.perf_counter()-start,3)
def phase(name): print(f"[{time.strftime('%H:%M:%S')}] {name}",file=sys.stderr,flush=True)
def clone(source,target):
    target.parent.mkdir(parents=True,exist_ok=True)
    subprocess.run(("cp","-c",str(source),str(target)),check=True)
def files_size(*paths): return sum(p.stat().st_size for path in paths for p in ([path] if path.is_file() else path.glob(path.name+"*") if path.parent.exists() else []) if p.exists() and p.is_file())
def sqlite_settle(db,path):
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.commit()
    return files_size(path)
def transport(db): return lambda cfg,body,auth=True:action(db,body,cfg.get("token") if auth else None)
def duck_settle(path):
    db=duckdb.connect(str(path))
    db.execute("CHECKPOINT")
    counts={t:db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("remote.row_proofs","remote.row_signers","remote.provenance_origins","provenance.local_facts")}
    db.close()
    return path.stat().st_size,counts
def relay_page_walk(db,cfg,ws):
    after=count=0
    started=time.perf_counter()
    while True:
        page=action(db,{"op":"replica_pull","workspace":ws,"after":after},cfg["token"])
        count+=len(page["replicas"])
        if not page["replicas"]: return count,time.perf_counter()-started
        after=page["replicas"][-1]["cursor"]
        if after>=page["tail"]: return count,time.perf_counter()-started


def run(source,keep=False):
    root=Path(tempfile.mkdtemp(prefix="convos-remote-benchmark-"))
    a,b=root/"author",root/"holder"
    core,result,ok=a/"data/convos.db",{"protocol":1,"source_bytes":source.stat().st_size},False
    try:
        phase("copy-on-write archive clone")
        clone(source,core)
        started=time.perf_counter()
        db=duckdb.connect(str(core))
        init_schema(db)
        archive_counts={table:db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("conversations","messages","tool_calls","attachments","artifacts","file_edits")}
        db.close()
        backup,(migrated,_)=core.with_name(core.name+".pre-v1.bak"),duck_settle(core)
        result|={"archive_counts":archive_counts,"migration_seconds":elapsed(started),"migration_backup_bytes":backup.stat().st_size if backup.exists() else 0,"migration_growth_bytes":migrated-result["source_bytes"]}
        relay=relay_connect(relay_path:=root/"relay.db")
        direct=transport(relay)
        remote.request,remote.drain_hooks=direct,lambda:None
        phase("create two-device personal workspace")
        alice,recovery=remote.setup_client("http://local","benchmark","author",root=a)
        remote.setup_client("http://local","benchmark","holder",recovery,root=b)
        origin=remote.workspace(alice,"Personal")
        timings=[]
        attest,apply=remote.attest_rows,remote.apply_row_replicas
        def timed_attest(*args,**kwargs):
            start=time.perf_counter()
            made=attest(*args,**kwargs)
            timings.append({"phase":"sign","seconds":elapsed(start),"records":len(args[3]),"created":made})
            return made
        def timed_apply(*args,**kwargs):
            start=time.perf_counter()
            out=apply(*args,**kwargs)
            timings.append({"phase":"verify_project","seconds":elapsed(start),"records":len(args[1]),"applied":sum(out)})
            return out
        remote.attest_rows,remote.apply_row_replicas=timed_attest,timed_apply
        failed=[False]
        def interrupted(cfg,body,auth=True):
            answer=direct(cfg,body,auth)
            if body["op"]=="replica_upload_many" and not failed[0]:
                failed[0]=True
                raise ConnectionError("simulated lost upload response")
            return answer
        phase("initial signing and simulated interruption")
        remote.request=interrupted
        started=time.perf_counter()
        try: remote.sync_once(a,True)
        except ConnectionError as e:
            if str(e)!="simulated lost upload response": raise
        interrupted_attempt_seconds,remote.request=elapsed(started),direct
        phase("idempotent interruption recovery")
        started=time.perf_counter()
        remote.sync_once(a,True)
        interruption_recovery_seconds=elapsed(started)
        initial_size,initial_counts,initial_replicas=(*duck_settle(core),relay.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0])
        phase("isolated relay replica page walk")
        relay_page_replicas,page_seconds=relay_page_walk(relay,alice,origin)
        result|={"interrupted_attempt_seconds":interrupted_attempt_seconds,"interruption_recovery_seconds":interruption_recovery_seconds,"initial_signing_seconds":sum(t["seconds"] for t in timings if t["phase"]=="sign"),"core_replication_growth_bytes":initial_size-result["source_bytes"],"core_counts":initial_counts,"relay_page_replicas":relay_page_replicas,"relay_page_seconds":round(page_seconds,3),"relay_page_rows_per_second":round(relay_page_replicas/page_seconds,3)}
        phase("one-row incremental sync")
        db=duckdb.connect(str(core))
        row=list(db.execute("SELECT * FROM conversations ORDER BY id LIMIT 1").fetchone())
        row[4]=(row[4] or row[3])+timedelta(microseconds=1)
        db.execute("BEGIN")
        project_archive_row(db,"conversations",ARCHIVE_COLUMNS["conversations"],row)
        db.execute("COMMIT")
        db.close()
        started=time.perf_counter()
        remote.sync_once(a)
        result|={"incremental_seconds":elapsed(started),"incremental_replicas":relay.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]-initial_replicas}
        phase("fresh-holder verification and projection")
        verification_replicas=relay.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]
        started=time.perf_counter()
        remote.sync_once(b,True)
        verification_seconds=elapsed(started)
        holder_size,_=duck_settle(b/"data/convos.db")
        result|={"verification_seconds":verification_seconds,"verification_replicas":verification_replicas,"verification_rows_per_second":round(verification_replicas/verification_seconds,3),"holder_core_bytes":holder_size}
        phase("empty-relay re-home and full holder repair")
        fresh=relay_connect(fresh_path:=root/"fresh-relay.db")
        remote.request,started=transport(fresh),time.perf_counter()
        bob,_=remote.rehome_client(remote.load(b),"http://fresh",b)
        replacement=remote.workspace(bob,"Personal")
        state=state_connect(b/"remote/state.db")
        remote.bind_origin(bob,state,replacement,origin,b)
        state.close()
        remote.sync_once(b,True)
        result|={"empty_relay_rebuild_seconds":elapsed(started),"empty_relay_replicas":fresh.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]}
        state_bytes=sqlite_settle(state:=state_connect(a/"remote/state.db"),a/"remote/state.db")
        state.close()
        relay_wire_bytes=sum(relay.execute(f"SELECT COALESCE(SUM(length({column})),0) FROM {table}").fetchone()[0] for table,column in (("events","envelope"),("row_replicas","envelope"),("blob_replicas","ciphertext"),("origin_bundles","envelope")))
        relay_bytes=sqlite_settle(relay,relay_path)
        relay.close()
        fresh_relay_bytes=sqlite_settle(fresh,fresh_path)
        fresh.close()
        result|={"state_bytes":state_bytes,"relay_wire_bytes":relay_wire_bytes,"relay_bytes":relay_bytes,"fresh_relay_bytes":fresh_relay_bytes,"timings":{phase:{"calls":len(rows),"seconds":round(sum(r["seconds"] for r in rows),3),"records":sum(r["records"] for r in rows)} for phase in {t["phase"] for t in timings} for rows in [[t for t in timings if t["phase"]==phase]]},"proof_overhead_ratio":round(result["core_replication_growth_bytes"]/result["source_bytes"],6)}
        if result["proof_overhead_ratio"]>.1 or result["verification_rows_per_second"]<500 or result["relay_page_replicas"]!=initial_replicas or result["incremental_replicas"]!=1 or result["empty_relay_replicas"]!=initial_counts["remote.row_proofs"]: raise AssertionError("remote release gate failed")
        ok=True
        return result
    finally:
        if keep or not ok: print(f"benchmark files: {root}",file=sys.stderr,flush=True)
        else: shutil.rmtree(root)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive",nargs="?",type=Path,default=Path.home()/".convos/data/convos.db")
    parser.add_argument("--keep",action="store_true")
    args=parser.parse_args()
    source=args.archive.expanduser().resolve()
    if not source.is_file(): parser.error(f"archive not found: {source}")
    print(json.dumps(run(source,args.keep),sort_keys=True))
if __name__=="__main__": main()
