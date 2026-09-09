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
    script = Path(__file__).resolve().parents[1] / 'scripts/archive_recovery/diagnose_b7_rows.py'
    result = subprocess.run([sys.executable, str(script), '--database', str(path), '--user-id', user, '--output', str(diagnosis)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(diagnosis.read_text())['counts']['unavailable'] == 1
    checksum = core._file_sha256(path)
    monkeypatch.setattr(sys, 'argv', ['repair', '--root', str(path.parent.parent), '--output', str(output), '--database-only', '--restore-history', str(backup), '--restore-claims', str(diagnosis)])
    repair.main()
    report = json.loads((output / 'report.json').read_text())
    assert report['success'] and report['restored_bodies'] == 1 and report['repaired_scopes'] == 0
    assert set(report['altered_tables']) == {'"remote"."row_conflicts"', '"main"."archive_state"', '"main"."archive_changes"', '"main"."retrieval_state"'}
    assert core._file_sha256(path) == checksum
    assert (output / 'history-preimages.json').stat().st_mode & 0o777 == 0o600
    assert audit_rows(report['target'], local_user=user)['totals']['unavailable'] == 0
    with core.open_db(report['target'], read_only=True, purpose='fixture.verify') as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?', [digest(proof)]).fetchone()[0]) == row
        assert repair.history_restore_plan(db, backup, json.loads(diagnosis.read_text())) == []


@pytest.mark.parametrize('damage', ['body', 'proof', 'existing'])
def test_history_recovery_refuses_mismatched_or_corrupt_history(tmp_path, damage):
    path, backup, user, proof, row = history_fixture(tmp_path)
    claims = {'unavailable': [dict(proof=digest(proof), kind='messages', source='m', author=user, expected=proof['content_hash'], state='active')]}
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
