"""Legacy placeholders must not override already captured file identities."""
import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import attest_rows, audit_rows, row_replicas
from ai_convos_remote.protocol import digest, logical_fact, open_replica, row_proof
from tests.test_native_provenance import people, scanned
from tests.test_provenance import core as archive, repo


def legacy_archive(tmp_path):
    path, checkout = tmp_path / "archive.db", repo(tmp_path / "repo")
    with archive(path, checkout, [(tmp_path / "external.py", "write", "one\n", None)]):
        pass
    core.capture_provenance(path)
    with duckdb.connect(str(path)) as db:
        original = db.execute("SELECT * FROM provenance.file_edit_scopes").fetchone()
        db.execute("UPDATE provenance.file_edit_scopes SET path=?,repository=NULL,root=NULL,checkout=NULL,route=NULL,observed_at=NULL", [f"external/{core.provenance_digest('e0')[:24]}/unknown"])
        db.execute("INSERT INTO provenance.pending SELECT 'file_edits','e0',generation FROM archive_state")
    return path, original


def test_legacy_scope_recovery_unblocks_capture_without_changing_signed_history(tmp_path):
    path, original = legacy_archive(tmp_path)
    users, _, _, cfg = people()
    attest_rows(path, cfg, "w", scanned(path, tmp_path / "state.db"))
    with pytest.raises(ValueError, match="provenance edit scope conflict"):
        core.capture_provenance(path)
    with duckdb.connect(str(path)) as db:
        tables = ("file_edits", "provenance.file_edit_files", "provenance.file_edit_evidence", "remote.row_proofs")
        before = {t: db.execute(f"SELECT * FROM {t}").fetchall() for t in tables}
        planned = core.repair_legacy_edit_scopes(db)
        assert len(planned) == 1 and planned[0][1][1:3] == original[1:3]
        with core._transaction(db):
            assert core.repair_legacy_edit_scopes(db, apply=True) == planned
        assert {t: db.execute(f"SELECT * FROM {t}").fetchall() for t in tables} == before
        assert core.repair_legacy_edit_scopes(db, apply=True) == []
    assert any(r["kind"] == "edit.observed" for r in core.capture_provenance(path))
    assert not any(r["kind"] == "edit.observed" for r in core.capture_provenance(path))
    assert audit_rows(path, local_user=users[0])["totals"]["unavailable"] == 0


@pytest.mark.parametrize("reason", ["real_scope", "captured_route", "foreign", "ambiguous", "bad_file_hash"])
def test_legacy_scope_recovery_refuses_unproven_rebindings(tmp_path, reason):
    path, _ = legacy_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        if reason == "real_scope":
            db.execute("UPDATE provenance.file_edit_scopes SET path='external/real/path'")
        elif reason == "captured_route":
            db.execute("UPDATE provenance.file_edit_scopes SET route='/original/path'")
        elif reason == "foreign":
            db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id) VALUES ('file_edits','e0')")
        elif reason == "ambiguous":
            db.execute("UPDATE provenance.file_edit_files SET evidence='legacy_scope_conflict'")
        else:
            db.execute("UPDATE provenance.files SET path='not-the-hashed-path'")
        before = db.execute("SELECT * FROM provenance.file_edit_scopes").fetchall()
        assert core.repair_legacy_edit_scopes(db, apply=True) == []
        assert db.execute("SELECT * FROM provenance.file_edit_scopes").fetchall() == before


@pytest.mark.parametrize("status", ["invalid", "unknown", "unverified"])
def test_legacy_scope_recovery_never_promotes_failed_or_uncertain_edits(tmp_path, status):
    path, _ = legacy_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE provenance.file_edit_evidence SET status=?,reason='historical'", [status])
        before = db.execute("SELECT * FROM provenance.file_edit_evidence").fetchall()
        assert len(core.repair_legacy_edit_scopes(db, apply=True)) == 1
        assert db.execute("SELECT * FROM provenance.file_edit_evidence").fetchall() == before


def test_v3_migration_reuses_unique_historical_file_instead_of_inventing_a_scope(tmp_path):
    path, original = legacy_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("DELETE FROM provenance.file_edit_scopes")
        db.execute("UPDATE core_schema SET version=2")
        core.init_schema(db)
        assert db.execute("SELECT path,repository FROM provenance.file_edit_scopes WHERE file_edit_id='e0'").fetchone() == original[1:3]
    assert any(r["kind"] == "edit.observed" for r in core.capture_provenance(path))


@pytest.mark.parametrize("base", ["current", "missing", "stale", "other_author", "scope_conflict", "different_turn", "different_repository", "unretained", "bad_retained_hash"])
def test_attestation_extends_only_recorded_live_branch_without_resolving_forks(tmp_path, base):
    import json

    path, _ = legacy_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        core.repair_legacy_edit_scopes(db, apply=True)
    core.capture_provenance(path)
    users, devices, control, cfg = people()
    records = scanned(path, tmp_path / "state.db")
    attest_rows(path, cfg, "w", records)
    observation = next(r for r in records if r["kind"] == "edit.observed")
    scope = {"scope_conflict": {"file": "different-file"}, "different_turn": {"turn": "different-turn"}, "different_repository": {"repository": "different-repository"}}
    alternate = logical_fact(observation | {"payload": observation["payload"] | {"evidence": "legacy_scope_conflict"} | scope.get(base, {})})
    proof = row_proof(devices[0], users[0], "w", 1, alternate)
    signer = control["devices"][devices[0]["id"]]
    with duckdb.connect(str(path)) as db:
        predecessor = db.execute("SELECT revision FROM remote.local_row_bases WHERE kind='edit.observed' AND entity='e0'").fetchone()[0]
        core.project_row_proof(db, proof, signer["root_public"], signer["certificate"])
        core._insert_pages(db, "remote.row_conflicts", [(digest(proof), json.dumps(alternate))])
        with core.preserve_fact_heads(db, [("file_edits", "e0")], observed=True):
            db.execute("UPDATE file_edits SET content='two'")
            core._archive_touch(db, [("file_edits", "e0")])
            db.execute("INSERT INTO provenance.pending SELECT 'file_edits','e0',generation FROM archive_state")
        if base == "missing":
            db.execute("DELETE FROM remote.local_row_bases WHERE kind='edit.observed'")
        elif base == "stale":
            db.execute("UPDATE remote.local_row_bases SET revision='not-a-current-head' WHERE kind='edit.observed'")
        elif base == "other_author":
            db.execute("UPDATE remote.local_row_bases SET author=? WHERE kind='edit.observed'", [users[1]])
        elif base in scope:
            db.execute("UPDATE remote.local_row_bases SET revision=? WHERE kind='edit.observed'", [proof["revision"]])
    core.capture_provenance(path)
    if base in ("unretained", "bad_retained_hash"):
        with duckdb.connect(str(path)) as db:
            where = "proof_id IN (SELECT id FROM remote.row_proofs WHERE row_kind='edit.observed' AND revision=?)"
            if base == "unretained":
                db.execute(f"DELETE FROM remote.row_conflicts WHERE {where}", [predecessor])
            else:
                db.execute(f"UPDATE remote.row_conflicts SET body=? WHERE {where}", [json.dumps(alternate), predecessor])
    current = [r for r in scanned(path, tmp_path / "state.db") if r["kind"] == "edit.observed"]
    if base != "current":
        with pytest.raises(ValueError, match="row revision conflict"):
            attest_rows(path, cfg, "w", current)
        return
    assert attest_rows(path, cfg, "w", current) == 1
    assert attest_rows(path, cfg, "w", current) == 0
    with duckdb.connect(str(path), read_only=True) as db:
        heads = db.execute("SELECT p.content_hash,p.previous_revision FROM remote.row_proofs p WHERE row_kind='edit.observed' AND NOT EXISTS (SELECT 1 FROM remote.row_proofs n WHERE n.row_kind=p.row_kind AND n.source_row_id=p.source_row_id AND n.author_user_id=p.author_user_id AND n.previous_revision=p.revision)").fetchall()
        assert set(heads) == {(digest(alternate), None), (digest(logical_fact(current[0])), predecessor)}
    assert audit_rows(path, local_user=users[0])["totals"]["unavailable"] == 0
    exported = [open_replica(value, bytes(32)) for value in row_replicas(path, cfg, "w", current, {1: bytes(32)})]
    assert any(body["row"] == alternate and body["proof"] == proof for body in exported)
    assert any(body["row"] == logical_fact(current[0]) for body in exported)
