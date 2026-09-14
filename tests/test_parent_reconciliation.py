"""Replay and backed-up repair preserve signed rows across mixed physical identities."""
import copy, json

import duckdb, pytest
from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.protocol import row_proof
from tests.test_remote_projection import signed_edit_graph


def mixed_archive(path, kind="messages", broken=False, base=True):
    _, device, user, control, rows, proofs, bodies, _ = signed_edit_graph()
    parent = "conversations" if kind in ("messages", "artifacts") else "messages"
    if kind == "attachments":
        rows[kind] = dict(v=1,kind=kind,id="a",state="active",data=dict(message_id="m",filename="a.txt",mime_type="text/plain",size=None,body_hash=None,created_at=None))
    if kind == "artifacts":
        rows[kind] = dict(v=1,kind=kind,id="a",state="active",data=dict(conversation_id="c",artifact_type="text",title="A",content="body",language=None,created_at=None,version=1))
    if kind not in proofs: proofs[kind] = row_proof(device,user,"w",1,rows[kind])
    signer = control["devices"][device["id"]]
    with core._core(path,purpose="test.schema") as db: core.init_schema(db)
    with core._core(path,purpose="test.mixed") as db, core._transaction(db):
        for table in ("conversations", "messages") if parent == "messages" else ("conversations",):
            core.project_attested_rows(db,[(rows[table],proofs[table])],signer["root_public"],signer["certificate"])
            core.project_logical_row(db,rows[table],proofs[table],core.provenance_digest(proofs[table]),native=True)
        core.project_row_proof(db,proofs[kind],signer["root_public"],signer["certificate"])
        key = rows[parent]["id"]
        core.project_logical_row(db,rows[kind],proofs[kind],core.provenance_digest(proofs[kind]),parent_map=None if broken else {(parent,key):key})
        if broken: db.execute("INSERT INTO remote.row_references VALUES (?,?,?,?)",[parent,core.remote_id(user,parent,key),user,key])
        if not base: db.execute("DELETE FROM remote.local_row_bases")
    return user,control,dict(row=rows[kind],proof=proofs[kind]),device


@pytest.mark.parametrize("kind", ["messages","tool_calls","file_edits","attachments","artifacts"])
@pytest.mark.parametrize("successor", [False,True])
def test_mixed_parent_replay_and_successor_preserve_graph(tmp_path,kind,successor):
    path=tmp_path/"data/convos.db"
    user,control,body,device=mixed_archive(path,kind)
    original=copy.deepcopy(body)
    if successor:
        body=copy.deepcopy(body)
        field={"messages":"content","tool_calls":"output","file_edits":"content","attachments":"filename","artifacts":"content"}[kind]
        body["row"]["data"][field]="new value"
        body["proof"]=row_proof(device,user,"w",1,body["row"],original["proof"]["revision"])
    for _ in range(2):
        projection.apply_row_replicas(path,[body],"w",[control],local_user=user)
        audit=projection.audit_rows(path,local_user=user)
        assert not audit["relationships"] and audit["totals"]["unavailable"]==0
        assert audit["totals"]["projection_mismatch"]==0
    with core._core(path,True,purpose="test.identity") as db:
        assert db.execute(f"SELECT id FROM {kind} WHERE id=?",[core.remote_id(user,kind,body["row"]["id"])]).fetchone()
        assert db.execute("SELECT content_hash FROM remote.row_proofs WHERE id=?",[core.provenance_digest(original["proof"])]).fetchone()==(core.provenance_digest(original["row"]),)


@pytest.mark.parametrize("kind", ["messages","tool_calls","file_edits","attachments","artifacts"])
def test_migration_repairs_only_physical_links_and_is_idempotent(tmp_path,kind):
    path=tmp_path/"data/convos.db"
    user,control,body,_=mixed_archive(path,kind,broken=True)
    tables=["remote.row_proofs","remote.row_origins","remote.row_references","remote.row_conflicts","provenance.file_edit_evidence"]
    with core._core(path,purpose="test.old_schema") as db:
        db.execute("UPDATE core_schema SET version=12")
        before={t:db.execute(f"SELECT * FROM {t}").fetchall() for t in tables}
        claim=(kind,core.remote_id(user,kind,body["row"]["id"]),body["row"]["id"],user,"active")
        logical=core.typed_logical_rows(db,[claim])
        core.init_schema(db)
        assert db.execute("SELECT version FROM core_schema").fetchone()==(core.CORE_VERSION,)
        assert not core.archive_relationships(db)
        assert core.typed_logical_rows(db,[claim])==logical
        assert {t:db.execute(f"SELECT * FROM {t}").fetchall() for t in tables}==before
        generation=db.execute("SELECT generation FROM archive_state").fetchone()
        core.init_schema(db)
        with core._transaction(db): assert core.repair_parent_links(db)==0
        assert db.execute("SELECT generation FROM archive_state").fetchone()==generation
    backup=path.with_name(path.name+".pre-v13.bak")
    assert backup.is_file()
    with duckdb.connect(str(backup),read_only=True) as db: assert core.archive_relationships(db)
    projection.apply_row_replicas(path,[body],"w",[control],local_user=user)
    assert not projection.audit_rows(path,local_user=user)["relationships"]


@pytest.mark.parametrize("refusal", ["unknown_author","different_account","foreign_parent","bad_reference","changed_body"])
def test_repair_does_not_guess_or_change_unverified_rows(tmp_path,refusal):
    path=tmp_path/"data/convos.db"
    user,control,body,_=mixed_archive(path,broken=True,base=False)
    with core._core(path,purpose="test.refusal") as db, core._transaction(db):
        if refusal=="foreign_parent": db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id,author_user_id,source_row_id) VALUES ('conversations','c','another','c')")
        if refusal=="bad_reference": db.execute("UPDATE remote.row_references SET source_row_id='different'")
        if refusal=="changed_body": db.execute("UPDATE messages SET content='unpublished local work'")
        before=db.execute("SELECT * FROM messages").fetchall()
        assert core.repair_parent_links(db,None if refusal=="unknown_author" else "other" if refusal=="different_account" else user)==0
        assert db.execute("SELECT * FROM messages").fetchall()==before


def test_repair_failure_rolls_back_links_and_version_then_resumes(tmp_path,monkeypatch):
    path=tmp_path/"data/convos.db"
    mixed_archive(path,broken=True)
    original=core._archive_touch
    def fail(*args): raise RuntimeError("interrupted repair")
    with core._core(path,purpose="test.interruption") as db:
        db.execute("UPDATE core_schema SET version=12")
        monkeypatch.setattr(core,"_archive_touch",fail)
        with pytest.raises(RuntimeError,match="interrupted repair"): core.init_schema(db)
        assert db.execute("SELECT version FROM core_schema").fetchone()==(12,)
        assert core.archive_relationships(db)
        monkeypatch.setattr(core,"_archive_touch",original)
        core.init_schema(db)
        assert not core.archive_relationships(db)


def test_parent_arriving_after_received_child_repairs_link(tmp_path):
    path=tmp_path/"data/convos.db"
    user,control,body,_=mixed_archive(path,broken=True,base=False)
    with core._core(path,purpose="test.delayed_parent") as db:
        parent=db.execute("SELECT * FROM conversations").fetchone()
        db.execute("DELETE FROM conversations")
    projection.apply_row_replicas(path,[body],"w",[control],local_user=user)
    assert projection.audit_rows(path,local_user=user)["relationships"]
    with core._core(path,purpose="test.local_parent") as db, core._transaction(db):
        db.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?)",parent)
        assert core.repair_parent_links(db,user,{("conversations","c")})==1
    assert not projection.audit_rows(path,local_user=user)["relationships"]


@pytest.mark.parametrize("arrival", ["parent_only","child_first","parent_first","local_attestation"])
def test_parent_delivery_or_attestation_settles_preexisting_received_child(tmp_path,arrival):
    path=tmp_path/'data/convos.db'
    user,control,child,device=mixed_archive(path,broken=True,base=False)
    with core._core(path,purpose='test.parent_body') as db:
        expected=db.execute("SELECT content_hash FROM remote.row_proofs WHERE row_kind='conversations'").fetchone()[0]
        claim=('conversations','c','c',user,'active')
        row=core.matching_logical_row(core.typed_logical_rows(db,[claim])[claim],expected)
        if arrival in ('child_first','parent_first'): db.execute('DELETE FROM conversations')
    parent=dict(row=row,proof=row_proof(device,user,'w',1,row))
    if arrival=='local_attestation':
        signer=control['devices'][device['id']]
        with core._core(path,purpose='test.attestation') as db,core._transaction(db): core.project_attested_rows(db,[(row,parent['proof'])],signer['root_public'],signer['certificate'])
    else:
        incoming=[parent] if arrival=='parent_only' else [parent,child] if arrival=='parent_first' else [child,parent]
        projection.apply_row_replicas(path,incoming,'w',[control],local_user=user)
    assert not projection.audit_rows(path,local_user=user)['relationships']
