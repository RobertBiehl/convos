"""A same-archive donor must repair exact signed variants without replacing current rows."""
import hashlib, json, shutil

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import apply_row_replicas, audit_rows, row_replicas
from ai_convos_remote.protocol import certificate, digest, identity, open_replica, public, public_id, row_proof


def variants():
    keys=[(identity(name+" root"),identity(name+" device")) for name in ("a","b")]
    people=[(public_id(root["sign_public"]),device) for root,device in keys]
    control={"workspace":"w","revision":1,"epoch":1,"devices":{device["id"]:{"user":user,"root_public":root["sign_public"],"device":public(device),"certificate":certificate(root,user,device),"history":True} for (root,device),(user,_) in zip(keys,people)}}
    return people,control


@pytest.mark.parametrize("same",[False,True])
@pytest.mark.parametrize("native",["neither","donor","current"])
def test_donor_repairs_typed_repository_variant_and_replay_is_idempotent(tmp_path,same,native):
    people,control=variants()
    rows=[[dict(v=1,kind="repository.observed",id=f"repo{j}",state="active",data=dict(lineage="lineage",roots=[],remotes=[f"https://example.test/{0 if same else i}/repo{j}.git"])) for j in range(3)] for i in range(2)]
    bodies=[[dict(row=row,proof=row_proof(device,user,"w",1,row)) for row in group] for group,(user,device) in zip(rows,people)]
    path,donor=tmp_path/"current.db",tmp_path/"donor.bak"
    with duckdb.connect(str(path)) as db: core.init_schema(db)
    shutil.copyfile(path,donor)
    apply_row_replicas(donor,bodies[0],"w",[control],local_user=people[0][0] if native=="donor" else "receiver")
    apply_row_replicas(path,bodies[1],"w",[control],local_user=people[1][0] if native=="current" else "receiver")
    before=hashlib.sha256(donor.read_bytes()).hexdigest()
    expected={digest(body["proof"]):body for group in bodies for body in group}
    cfg={"user":people[0 if native=="donor" else 1][0] if native!="neither" else "receiver","device":people[0 if native=="donor" else 1][1]}
    for _ in range(2):
        core.merge_archive_backup(path,donor,page=1)
        with duckdb.connect(str(path),read_only=True) as db:
            markers=set(db.execute("SELECT kind,entity FROM provenance.local_facts").fetchall())
            assert markers=={("repository.observed",row["id"]) for row in rows[1] if native=="current" or native=="donor" and same}
            records=core.provenance_records(db,markers)
        recovered={digest(body["proof"]):body for env in row_replicas(path,cfg,"w",records,{1:bytes(32)}) for body in [open_replica(env,bytes(32))]}
        assert {pid:{"row":body["row"],"proof":body["proof"]} for pid,body in recovered.items()}==expected
        assert audit_rows(path,local_user=cfg["user"])["totals"]["unavailable"]==0
        with duckdb.connect(str(path),read_only=True) as db:
            assert [json.loads(remotes) for remotes, in db.execute("SELECT remotes FROM provenance.repositories ORDER BY id").fetchall()]==[row["data"]["remotes"] for row in rows[1]]
            assert db.execute("SELECT COUNT(*) FROM remote.row_proofs").fetchone()[0]==6
            assert db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==3*int(not same)
        assert hashlib.sha256(donor.read_bytes()).hexdigest()==before


def test_donor_restores_missing_native_repository_and_marker(tmp_path):
    people,control=variants()
    user,device=people[0]
    row=dict(v=1,kind="repository.observed",id="repo",state="active",data=dict(lineage="lineage",roots=[],remotes=[]))
    path,donor=tmp_path/"current.db",tmp_path/"donor.bak"
    with duckdb.connect(str(path)) as db: core.init_schema(db)
    shutil.copyfile(path,donor)
    body=dict(row=row,proof=row_proof(device,user,"w",1,row))
    apply_row_replicas(donor,[body],"w",[control],local_user=user)
    for _ in range(2):
        core.merge_archive_backup(path,donor,page=1)
        with duckdb.connect(str(path),read_only=True) as db:
            assert db.execute("SELECT kind,entity FROM provenance.local_facts").fetchall()==[("repository.observed","repo")]
            assert db.execute("SELECT COUNT(*) FROM remote.row_conflicts").fetchone()[0]==0
            records=core.provenance_records(db,{("repository.observed","repo")})
        recovered=[open_replica(env,bytes(32)) for env in row_replicas(path,{"user":user,"device":device},"w",records,{1:bytes(32)})]
        assert [{"row":value["row"],"proof":value["proof"]} for value in recovered]==[body]
