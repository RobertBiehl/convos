"""Recovery must preserve signed history and expose broken physical relationships."""
import json
import shutil

import pytest
from typer.testing import CliRunner

from ai_convos import cli as core
import ai_convos_remote as client
from ai_convos_remote import projection, protocol
from tests.test_native_provenance import people, scanned
from tests.test_provenance import core as archive, repo
from tests.test_remote_projection import signed_edit_graph


def signed_native_edit(tmp_path):
    path, checkout = tmp_path / "archive.db", repo(tmp_path / "repo")
    with archive(path, checkout, [(tmp_path / "outside.txt", "write", "one\n", None)]):
        pass
    core.capture_provenance(path)
    users, devices, control, cfg = people()
    projection.attest_rows(path, cfg, "w", scanned(path, tmp_path / "state.db"))
    with core.open_db(path, purpose="fixture.inspect") as db:
        proof = db.execute("SELECT * FROM remote.row_proofs WHERE row_kind='edit.observed'").fetchone()
        assert not db.execute("SELECT 1 FROM remote.provenance_origins WHERE kind='edit.observed'").fetchone()
    return path, cfg, proof


def test_invalidation_preserves_exact_signed_body_and_failed_status(tmp_path):
    path, cfg, proof = signed_native_edit(tmp_path)
    with core.open_db(path, purpose="fixture.invalidate") as db, core._transaction(db):
        with core.preserve_fact_heads(db, [("file_edits", proof[4])]):
            db.execute("UPDATE provenance.file_edit_evidence SET status='invalid',reason='provider_failure'")
        assert not core.provenance_records(db, {("edit.observed", proof[4])})
        assert protocol.digest(json.loads(db.execute("SELECT body FROM remote.row_conflicts WHERE proof_id=?", [proof[0]]).fetchone()[0])) == proof[6]
    assert projection.audit_rows(path, local_user=cfg['user'])['totals']['unavailable'] == 0
    envelopes = projection.row_replicas(path, cfg, 'w', [], {1: bytes(range(32))})
    assert any(protocol.open_replica(env, bytes(range(32)))['proof']['content_hash'] == proof[6] for env in envelopes)
    with core.open_db(path, read_only=True, purpose="fixture.verify") as db:
        assert db.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone() == ('invalid', 'provider_failure')
        assert db.execute("SELECT * FROM remote.row_proofs WHERE id=?", [proof[0]]).fetchone() == proof


def test_donor_recovers_legacy_filtered_body_without_confirming_failed_edit(tmp_path):
    path, cfg, proof = signed_native_edit(tmp_path)
    with core.open_db(path, purpose="fixture.legacy-invalid") as db:
        db.execute("UPDATE provenance.file_edit_evidence SET status='invalid',reason='provider_failure'")
        db.execute("DELETE FROM remote.row_conflicts WHERE proof_id=?", [proof[0]])
    assert projection.audit_rows(path, local_user=cfg['user'])['totals']['unavailable'] == 1
    donor = tmp_path / 'donor.db'
    shutil.copyfile(path, donor)
    with core.open_db(path, read_only=True, purpose='fixture.before-repair') as db:
        generation = db.execute('SELECT generation FROM archive_state').fetchone()[0]
    core.merge_archive_backup(path, donor)
    with core.open_db(path, read_only=True, purpose='fixture.after-repair') as db:
        assert db.execute('SELECT generation FROM archive_state').fetchone()[0] > generation
    assert projection.audit_rows(path, local_user=cfg['user'])['totals']['unavailable'] == 0
    core.merge_archive_backup(path, donor)
    with core.open_db(path, read_only=True, purpose="fixture.verify") as db:
        assert db.execute("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?", [proof[0]]).fetchone()[0] == 1
        assert not core.provenance_records(db, {('edit.observed', proof[4])})
        assert db.execute("SELECT status,reason FROM provenance.file_edit_evidence").fetchone() == ('invalid', 'provider_failure')


@pytest.mark.parametrize('apply', [False, True])
def test_narrow_repair_reconstructs_failed_edit_without_retained_donor_json(tmp_path, apply):
    from tests.test_recovery_script import repair
    path, cfg, proof = signed_native_edit(tmp_path)
    root = tmp_path / 'source'
    (root / 'data').mkdir(parents=True)
    path = path.rename(root / 'data/convos.db')
    with core.open_db(path, purpose='fixture.failed-edit') as db:
        db.execute("UPDATE provenance.file_edit_evidence SET status='invalid',reason='provider_failure'")
        db.execute('DELETE FROM remote.row_conflicts WHERE proof_id=?', [proof[0]])
    unavailable = []
    projection.audit_rows(path, local_user=cfg['user'], on_unavailable=unavailable.append)
    with core.open_db(path, read_only=True, purpose='fixture.diagnosis') as db:
        diagnosis = dict(format='convos-archive-diagnosis-v1', archive_id=repair.identity(db)[0], unavailable=unavailable, proof_rows=db.execute('SELECT * FROM remote.row_proofs').fetchall())
    donor, _ = repair.snapshot(path, tmp_path / 'donor')
    checksum = core._file_sha256(path)
    result = repair.run(root, tmp_path / 'repair', apply=apply, donor=donor, diagnosis=diagnosis)
    assert result['restored_bodies'] == 1
    assert apply or core._file_sha256(path) == checksum
    assert projection.audit_rows(result['target'], local_user=cfg['user'])['totals']['unavailable'] == 0
    with core.open_db(result['target'], read_only=True, purpose='fixture.repaired') as db:
        assert db.execute('SELECT * FROM remote.row_proofs WHERE id=?', [proof[0]]).fetchone() == proof
        assert db.execute('SELECT status,reason FROM provenance.file_edit_evidence').fetchone() == ('invalid', 'provider_failure')
        assert not core.provenance_records(db, {('edit.observed', proof[4])})
        assert repair.history_restore_plan(db, donor, diagnosis) == []


def test_audit_reports_unsigned_orphans_and_history_without_changing_archive(tmp_path, monkeypatch):
    path = tmp_path / 'archive.db'
    with core.open_db(path, purpose="fixture.orphans") as db:
        core.init_schema(db)
        db.execute("INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('current','absent','assistant','{}'),('old','absent','assistant','{\"history_of\":\"current\"}')")
        generation = db.execute("SELECT generation FROM archive_state").fetchone()[0]
    result = projection.audit_rows(path)
    assert result['totals'] == {} and result['archive_generation'] == generation
    assert result['relationships'] == {'messages.conversation_id': {'rows': 2, 'parent_ids': 1, 'marked_history_rows': 1}}
    monkeypatch.setattr(client, 'load', lambda: {'user': 'test'})
    monkeypatch.setattr(client, 'audit_rows', lambda *args, **kwargs: result)
    text = CliRunner().invoke(client.remote, ['audit'])
    assert text.exit_code == 1 and 'Missing parent messages.conversation_id: rows=2' in text.output
    machine = CliRunner().invoke(client.remote, ['audit', '--format', 'json'])
    assert json.loads(machine.output) == result
    monkeypatch.setattr(client, 'repull_once', lambda **kwargs: ({}, result))
    repull = CliRunner().invoke(client.remote, ['repull'])
    assert repull.exit_code == 0 and 'relationship repair is not' in repull.output
    with core.open_db(path, read_only=True, purpose="fixture.verify") as db:
        assert db.execute("SELECT generation FROM archive_state").fetchone()[0] == generation
        assert db.execute("SELECT count(*) FROM messages").fetchone()[0] == 2


@pytest.mark.parametrize('compression', ['none', 'zstd'])
def test_signed_child_before_parent_replay_repairs_relationship_with_original_proofs(tmp_path, compression):
    _, device, user, control, rows, proofs, _, _ = signed_edit_graph()
    key, path = bytes(range(32)), tmp_path / 'receiver.db'
    # Force compression to be worthwhile and exercise both transport encodings.
    rows['messages']['data']['content'] = 'preserved content\n' * 1000
    proofs['messages'] = protocol.row_proof(device, user, 'w', 1, rows['messages'])
    def receive(kind):
        env = protocol.seal_replica(rows[kind], proofs[kind], 'w', 1, key, device['id'], compression=compression)
        if kind == 'messages':
            assert env['v'] == (2 if compression == 'zstd' else 1)
        body = protocol.open_replica(env, key)
        return projection.apply_row_replicas(path, [body], 'w', [control], local_user='receiver')
    receive('messages')
    before = projection.audit_rows(path, local_user='receiver')
    assert before['totals']['unavailable'] == 0
    assert before['relationships']['messages.conversation_id']['rows'] == 1
    receive('conversations')
    receive('messages')
    after = projection.audit_rows(path, local_user='receiver')
    assert after['totals']['unavailable'] == 0 and not after['relationships']
    with core.open_db(path, read_only=True, purpose="fixture.verify") as db:
        assert db.execute("SELECT count(*) FROM messages m JOIN conversations c ON c.id=m.conversation_id").fetchone()[0] == 1
        assert {p[0] for p in db.execute("SELECT id FROM remote.row_proofs").fetchall()} == {protocol.digest(proofs[k]) for k in ('messages', 'conversations')}


def test_unrelated_writer_still_invalidates_audit_instead_of_reporting_mixed_state(tmp_path):
    _, _, user, control, _, _, bodies, _ = signed_edit_graph()
    path = tmp_path / 'archive.db'
    projection.apply_row_replicas(path, bodies, 'w', [control], local_user=user)
    def mutate(stage):
        with core.open_db(path, purpose='fixture.concurrent-write') as db:
            core._archive_touch(db)
    with pytest.raises(RuntimeError, match='Archive changed'):
        projection.audit_rows(path, page=1, progress=mutate, local_user=user)
