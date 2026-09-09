"""A signed successor must not discard a body still referenced by a received origin."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import audit_rows, row_replicas
from ai_convos_remote.protocol import digest, row_proof, open_replica, repack_replica
from tests.test_remote_projection import signed_edit_graph
from tests.test_recovery_script import repair


@pytest.mark.parametrize('compression', ['none', 'zstd'])
@pytest.mark.parametrize('kind', ['messages', 'edit.observed', 'file.version', 'git.checkpoint'])
def test_attestation_preserves_origin_referenced_predecessor_until_reference_moves(tmp_path, kind, compression):
    _, device, user, control, rows, _, _, _ = signed_edit_graph()
    facts = {
        'edit.observed': dict(turn='m', file='f', repository=None, old_content_hash=None, new_content_hash=digest(b'old'), evidence='captured_exact'),
        'file.version': dict(file='f', content_hash=digest(b'old'), observed_at='2026-01-01T00:00:00'),
        'git.checkpoint': dict(repository='r', head='h', state_hash='s', paths=[], capture_source='sync', observed_at='2026-01-01T00:00:00'),
    }
    row = rows['messages'] if kind == 'messages' else dict(v=1, kind=kind, id='fact', state='active', data=facts[kind])
    delta = {'content': 'new'} if kind == 'messages' else {'new_content_hash': digest(b'new')} if kind == 'edit.observed' else {'observed_at': '2026-02-01T00:00:00'}
    successor = row | {'data': row['data'] | delta}
    before = row_proof(device, user, 'w', 1, row)
    after = row_proof(device, user, 'w', 1, successor, before['revision'])
    path, signer = tmp_path / 'archive.db', control['devices'][device['id']]
    table = 'remote.row_origins' if kind == 'messages' else 'remote.provenance_origins'
    with core.open_db(path, purpose='fixture') as db:
        core.init_schema(db)
        core.project_attested_rows(db, [(row, before)], signer['root_public'], signer['certificate'])
        if kind == 'messages':
            db.execute('INSERT INTO remote.row_origins(table_name,physical_row_id,workspace_id,author_user_id,source_row_id,proof_id) VALUES (?,?,?,?,?,?)', [kind, row['id'], 'w', user, row['id'], digest(before)])
        else:
            db.execute('INSERT INTO remote.provenance_origins VALUES (?,?,?,?,?,?)', [kind, row['id'], 'w', user, row['id'], digest(before)])
    assert audit_rows(path, local_user=user)['totals']['unavailable'] == 0
    with core.open_db(path, purpose='fixture.attest') as db:
        original = db.execute('SELECT * FROM remote.row_proofs WHERE id=?', [digest(before)]).fetchone()
        core.project_attested_rows(db, [(successor, after)], signer['root_public'], signer['certificate'])
    assert audit_rows(path, local_user=user)['totals']['unavailable'] == 0
    key = bytes(range(32))
    cfg = dict(user=user, device=device, workspaces={'w': dict(kind='personal', epoch=1)}, controls={'w': control},
               server_state={'capabilities': {'replica_compression': ['zstd'] if compression == 'zstd' else []}})
    envs = row_replicas(path, cfg, 'w', [], {1: key}, retained={digest(before)})
    original_env = next(env for env in envs if open_replica(env, key)['proof'] == before)
    assert original_env['v'] == (2 if compression == 'zstd' else 1)
    assert open_replica(original_env, key)['row'] == row
    assert open_replica(repack_replica(original_env, key, 'none' if compression == 'zstd' else 'zstd'), key, True) == open_replica(original_env, key, True)
    with core.open_db(path, purpose='fixture.advance-origin') as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?', [digest(before)]).fetchone()[0]) == row
        assert db.execute('SELECT * FROM remote.row_proofs WHERE id=?', [digest(before)]).fetchone() == original
        db.execute(f'UPDATE {table} SET proof_id=? WHERE proof_id=?', [digest(after), digest(before)])
        core.retire_row_bodies(db, [(kind, row['id'], user, before['revision'])])
        assert not db.execute('SELECT 1 FROM remote.row_conflicts WHERE proof_id=?', [digest(before)]).fetchone()
    assert audit_rows(path, local_user=user)['totals']['unavailable'] == 0


def history_fixture(tmp_path):
    _, device, user, control, rows, proofs, _, _ = signed_edit_graph()
    path, signer = tmp_path / 'source/data/convos.db', control['devices'][device['id']]
    with core.open_db(path, purpose='fixture') as db:
        core.init_schema(db)
        core.project_attested_rows(db, [(rows['messages'], proofs['messages'])], signer['root_public'], signer['certificate'])
    backup, _ = repair.snapshot(path, tmp_path / 'before', False)
    with core.open_db(path, purpose='fixture.lost-body') as db:
        db.execute('DELETE FROM remote.row_conflicts')
    return path, backup, user, proofs['messages'], rows['messages']


def test_history_recovery_uses_diagnosed_claims_and_exact_backup_without_changing_source(tmp_path, monkeypatch):
    path, backup, user, proof, row = history_fixture(tmp_path)
    output, diagnosis = tmp_path / 'restored', tmp_path / 'diagnosis.json'
    script = Path(__file__).resolve().parents[1] / 'scripts/archive_recovery/diagnose.py'
    result = subprocess.run([sys.executable, str(script), '--database', str(path), '--user-id', user, '--output', str(diagnosis)], capture_output=True, text=True)
    assert result.returncode == 1, result.stderr
    assert json.loads(diagnosis.read_text())['audit']['totals']['unavailable'] == 1
    checksum = core._file_sha256(path)
    monkeypatch.setattr(sys, 'argv', ['repair', '--root', str(path.parent.parent), '--output', str(output), '--database-only', '--donor', str(backup), '--diagnosis', str(diagnosis)])
    repair.main()
    report = json.loads((output / 'report.json').read_text())
    assert report['success'] and report['restored_bodies'] == 1 and report['repaired_scopes'] == 0
    assert {'main.messages', 'remote.row_proofs'} <= set(report['protected_tables'])
    assert core._file_sha256(path) == checksum
    assert (output / 'plan.json').stat().st_mode & 0o777 == 0o600
    assert audit_rows(report['target'], local_user=user)['totals']['unavailable'] == 0
    with core.open_db(report['target'], read_only=True, purpose='fixture.verify') as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?', [digest(proof)]).fetchone()[0]) == row
        assert repair.history_restore_plan(db, backup, json.loads(diagnosis.read_text())) == []


@pytest.mark.parametrize('damage', ['body', 'proof', 'existing'])
def test_history_recovery_refuses_mismatched_or_corrupt_history(tmp_path, damage):
    path, backup, user, proof, row = history_fixture(tmp_path)
    with core.open_db(path, read_only=True, purpose='fixture.diagnosis') as db:
        claims = dict(format='convos-archive-diagnosis-v1', archive_id=repair.identity(db)[0], proof_rows=db.execute('SELECT * FROM remote.row_proofs').fetchall(), unavailable=[dict(proof=digest(proof), kind='messages', source='m', author=user, expected=proof['content_hash'], state='active')])
    target = path if damage == 'existing' else backup
    with core.open_db(target, purpose='fixture.damage') as db:
        if damage == 'proof':
            db.execute("UPDATE remote.row_proofs SET signature='tampered'")
        elif damage == 'body':
            db.execute('UPDATE remote.row_conflicts SET body=?', [json.dumps(row | {'data': row['data'] | {'content': 'tampered'}})])
        else:
            core._insert_pages(db, 'remote.row_conflicts', [(digest(proof), json.dumps(row | {'data': row['data'] | {'content': 'tampered'}}))])
    checksum = core._file_sha256(path)
    with core.open_db(path, read_only=True, purpose='fixture.refuse') as db, pytest.raises(ValueError, match='mismatch|corrupt'):
        repair.history_restore_plan(db, backup, claims)
    assert core._file_sha256(path) == checksum


@pytest.mark.parametrize('damage', ['archive', 'diagnostic-proof', 'donor-archive'])
def test_restore_binds_diagnosis_and_donor_to_the_target_archive(tmp_path, damage):
    path, backup, user, proof, row = history_fixture(tmp_path)
    with core.open_db(path, read_only=True, purpose='fixture.claims') as db:
        claims = dict(format='convos-archive-diagnosis-v1', archive_id=repair.identity(db)[0], proof_rows=[list(r) for r in db.execute('SELECT * FROM remote.row_proofs').fetchall()], unavailable=[dict(proof=digest(proof), kind='messages', source='m', author=user, expected=proof['content_hash'], state='active')])
    if damage == 'archive': claims['archive_id'] = 'another-archive'
    elif damage == 'diagnostic-proof': claims['proof_rows'][0][-1] = 'changed-signature'
    else:
        with core.open_db(backup, purpose='fixture.other-archive') as db:
            db.execute("UPDATE archive_state SET archive_id='00000000-0000-0000-0000-000000000099'")
    checksum = core._file_sha256(path)
    with core.open_db(path, read_only=True, purpose='fixture.refuse') as db, pytest.raises(ValueError, match='archive|mismatch'):
        repair.history_restore_plan(db, backup, claims)
    assert core._file_sha256(path) == checksum


def test_diagnosis_uses_canonical_counts_and_remains_private_and_read_only(tmp_path):
    path, backup, user, proof, row = history_fixture(tmp_path)
    output = tmp_path / 'diagnosis.json'
    script = Path(__file__).resolve().parents[1] / 'scripts/archive_recovery/diagnose.py'
    checksum = core._file_sha256(path)
    result = subprocess.run([sys.executable, str(script), '--database', str(path), '--user-id', user, '--output', str(output)], capture_output=True, text=True)
    assert result.returncode == 1, result.stderr
    report = json.loads(output.read_text())
    assert report['audit'] == audit_rows(path, local_user=user)
    assert len(report['unavailable']) == report['audit']['totals']['unavailable'] == 1
    assert report['unavailable'][0]['proof'] == digest(proof)
    assert output.stat().st_mode & 0o777 == 0o600 and core._file_sha256(path) == checksum
    saved = output.read_bytes()
    again = subprocess.run([sys.executable, str(script), '--database', str(path), '--user-id', user, '--output', str(output)], capture_output=True, text=True)
    assert again.returncode != 0 and output.read_bytes() == saved


@pytest.mark.parametrize('kind', ['conversations', 'messages'])
def test_restoring_foreign_body_does_not_encode_a_native_deletion(tmp_path, kind):
    from ai_convos_remote.projection import _records, apply_row_replicas
    _, device, user, control, rows, proofs, bodies, _ = signed_edit_graph()
    path = tmp_path / 'archive.db'
    apply_row_replicas(path, bodies, 'w', [control], local_user='receiver')
    with core.open_db(path, purpose='fixture.restore') as db:
        proof = db.execute('SELECT * FROM remote.row_proofs WHERE row_kind=?', [kind]).fetchone()
        pid = proof[0]
        db.execute('DELETE FROM remote.row_conflicts WHERE proof_id=?', [pid])
        generation = db.execute('SELECT generation FROM archive_state').fetchone()[0]
        with core._transaction(db): core.restore_signed_bodies(db, [dict(proof_id=pid, proof_row=proof, body=rows[kind])])
        _, changes = core.archive_changes(db, generation)
        assert not _records(db, None, changes=changes)
        assert changes == [('retained.body', pid)]


def attachment_history(tmp_path):
    _, device, user, control, _, _, _, _ = signed_edit_graph()
    path, data = tmp_path / 'source/data/convos.db', b'irreplaceable attachment bytes'
    blob = core.attachment_body(data, path.parent)
    row = dict(v=1, kind='attachments', id='a', state='active', data=dict(message_id='m', filename='evidence.txt', mime_type='text/plain', size=len(data), body_hash=blob.name, url=None, created_at=None))
    proof = row_proof(device, user, 'w', 1, row)
    signer = control['devices'][device['id']]
    with core.open_db(path, purpose='fixture.attachment') as db:
        core.init_schema(db)
        core.project_attested_rows(db, [(row, proof)], signer['root_public'], signer['certificate'])
        stored = db.execute('SELECT * FROM remote.row_proofs').fetchone()
    donor, _ = repair.snapshot(path, tmp_path / 'donor')
    with core.open_db(path, purpose='fixture.missing-attachment') as db:
        db.execute('DELETE FROM remote.row_conflicts')
        diagnosis = dict(format='convos-archive-diagnosis-v1', archive_id=repair.identity(db)[0], proof_rows=[stored], unavailable=[dict(proof=digest(proof), kind='attachments', physical='a', source='a', author=user, expected=proof['content_hash'], state='active')])
    blob.unlink()
    return path, donor, row, stored, diagnosis


@pytest.mark.parametrize('apply', [False, True])
def test_attachment_recovery_stages_exact_bytes_and_remains_completely_backuppable(tmp_path, apply):
    path, donor, row, proof, diagnosis = attachment_history(tmp_path)
    checksum = core._file_sha256(path)
    result = repair.run(path.parent.parent, tmp_path / 'repair', apply=apply, donor=donor, diagnosis=diagnosis)
    assert result['restored_bodies'] == 1 and (apply or core._file_sha256(path) == checksum)
    target = Path(result['target'])
    assert core.attachment_index(target.parent / 'attachments' / row['data']['body_hash'], row['data']['size']) == (row['data']['body_hash'], row['data']['size'])
    backup, _ = repair.snapshot(target, tmp_path / 'complete-after')
    assert (backup.with_name(backup.name + '.attachments') / row['data']['body_hash']).is_file()
    again = repair.run(target.parent.parent, tmp_path / 'repeat', donor=donor, diagnosis=diagnosis)
    assert again['restored_bodies'] == 0
    repair.snapshot(Path(again['target']), tmp_path / 'complete-repeated-simulation')


def test_backup_merge_stages_attachment_before_restoring_its_reference(tmp_path):
    path, donor, row, proof, diagnosis = attachment_history(tmp_path)
    core.merge_archive_backup(path, donor)
    with core.open_db(path, read_only=True, purpose='fixture.merged-attachment') as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?', [proof[0]]).fetchone()[0]) == row
    repair.snapshot(path, tmp_path / 'complete-merged')


@pytest.mark.parametrize('failure', ['file', 'store', 'parent'])
def test_core_attachment_restore_requires_durable_content_before_reference(tmp_path, monkeypatch, failure):
    path, donor, row, proof, diagnosis = attachment_history(tmp_path)
    data = (donor.with_name(donor.name + '.attachments') / row['data']['body_hash']).read_bytes()
    blob = core.attachment_body(data, path.parent)
    failed = {'file': blob, 'store': blob.parent, 'parent': blob.parent.parent}[failure]
    flush = core._fsync
    def fail(target):
        if Path(target) == failed: raise OSError('durability failure')
        return flush(target)
    monkeypatch.setattr(core, '_fsync', fail)
    with core.open_db(path, purpose='fixture.durability') as db, core._transaction(db):
        generation = repair.identity(db)[1]
        with pytest.raises(OSError, match='durability failure'):
            core.restore_signed_bodies(db, [dict(proof_id=proof[0], proof_row=proof, body=row)])
        assert not db.execute('SELECT 1 FROM remote.row_conflicts').fetchone()
        assert repair.identity(db)[1] == generation


@pytest.mark.parametrize('damage', ['missing', 'hash', 'size', 'symlink'])
def test_attachment_recovery_fails_closed_without_exact_bytes(tmp_path, damage):
    path, donor, row, proof, diagnosis = attachment_history(tmp_path)
    blob = donor.with_name(donor.name + '.attachments') / row['data']['body_hash']
    if damage == 'hash': blob.write_bytes(b'x' * row['data']['size'])
    elif damage == 'size': blob.write_bytes(b'short')
    elif damage == 'symlink':
        saved = blob.rename(tmp_path / 'saved-blob')
        blob.symlink_to(saved)
    else: blob.unlink()
    checksum = core._file_sha256(path)
    with pytest.raises(ValueError, match='Exact attachment bytes'):
        repair.run(path.parent.parent, tmp_path / 'repair', apply=True, donor=donor, diagnosis=diagnosis)
    assert core._file_sha256(path) == checksum
    with core.open_db(path, purpose='fixture.direct-writer') as db, core._transaction(db):
        with pytest.raises(ValueError, match='attachment body is unavailable'):
            core.restore_signed_bodies(db, [dict(proof_id=proof[0], proof_row=proof, body=row)])
        assert not db.execute('SELECT 1 FROM remote.row_conflicts').fetchone()
