"""Metadata-only attachment history requires no invented file; claimed bytes this device lacks are recorded as absent."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ai_convos import cli as core
from ai_convos_remote.protocol import logical_row, row_proof
from tests.test_remote_projection import signed_edit_graph
from tests.test_recovery_script import repair


def attachment_archive(tmp_path, body_hash=None, size=None, filename='[REDACTED:attachment]'):
    _, device, user, control, _, _, _, _ = signed_edit_graph()
    path = tmp_path / 'source/data/convos.db'
    row = logical_row('attachments', [*core.ARCHIVE_COLUMNS['attachments'], 'body_hash'], ['a', 'm', filename, None, size, None, None, None, body_hash])
    proof, signer = row_proof(device, user, 'w', 1, row), control['devices'][device['id']]
    with core.open_db(path, purpose='fixture.attachment-history') as db:
        core.init_schema(db)
        core.project_attested_rows(db, [(row, proof)], signer['root_public'], signer['certificate'])
        stored = db.execute('SELECT * FROM remote.row_proofs').fetchone()
    return path, row, stored


@pytest.mark.parametrize('filename', ['[REDACTED:attachment]', 'unfetched.png'])
@pytest.mark.parametrize('size', [None, 0, 12])
def test_backup_preserves_metadata_only_history_without_requiring_a_blob(tmp_path, filename, size):
    path, row, proof = attachment_archive(tmp_path, size=size, filename=filename)
    with core.open_db(path, purpose='fixture.backup') as db:
        before = db.execute('SELECT * FROM remote.row_conflicts').fetchall()
        backup = core._migration_backup(db, 'metadata-only')
        assert db.execute('SELECT * FROM remote.row_conflicts').fetchall() == before
        assert db.execute('SELECT * FROM remote.row_proofs').fetchone() == proof
    manifest = json.loads((backup.with_name(backup.name + '.attachments') / 'manifest.json').read_text())
    assert manifest['references'] == manifest['attachments'] == {}
    assert core._file_sha256(backup) == core._file_sha256(path) == manifest['database_sha256']


@pytest.mark.parametrize('apply', [False, True])
def test_repair_restores_metadata_only_history_and_can_back_up_the_result(tmp_path, apply):
    path, row, proof = attachment_archive(tmp_path)
    donor, _ = repair.snapshot(path, tmp_path / 'donor')
    with core.open_db(path, purpose='fixture.missing-history') as db:
        db.execute('DELETE FROM remote.row_conflicts')
        diagnosis = dict(format='convos-archive-diagnosis-v1', archive_id=repair.identity(db)[0], proof_rows=[proof], unavailable=[dict(proof=proof[0], kind='attachments', physical='a', source='a', author=proof[10], expected=proof[6], state='active')])
    result = repair.run(path.parent.parent, tmp_path / 'repair', apply=apply, donor=donor, diagnosis=diagnosis)
    assert result['restored_bodies'] == 1
    with core.open_db(result['target'], read_only=True, purpose='fixture.verify') as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts').fetchone()[0]) == row
        assert db.execute('SELECT * FROM remote.row_proofs').fetchone() == proof
    repair.snapshot(Path(result['target']), tmp_path / 'after')


def damaged_archive(tmp_path, damage):
    data = b'original bytes'
    body_hash = core.provenance_digest(data)
    path, row, proof = attachment_archive(tmp_path, body_hash, len(data))
    blob = path.parent / 'attachments' / body_hash
    blob.parent.mkdir()
    if damage == 'wrong-hash': blob.write_bytes(b'x' * len(data))
    elif damage == 'wrong-size': blob.write_bytes(b'short')
    elif damage == 'symlink':
        target = tmp_path / 'bytes'
        target.write_bytes(data)
        blob.symlink_to(target)
    return path, proof, blob, body_hash, len(data)


@pytest.mark.parametrize('damage', ['missing', 'wrong-hash', 'wrong-size'])
def test_backup_records_claimed_bytes_this_device_does_not_hold(tmp_path, damage):
    """A row whose file never arrived (or arrived damaged) is recoverable from its author; the backup must not block migrations on it."""
    path, proof, blob, body_hash, size = damaged_archive(tmp_path, damage)
    before = blob.read_bytes() if blob.exists() else None
    with core.open_db(path, purpose='fixture.absent') as db:
        backup = core._migration_backup(db, 'absent-blob')
        assert core._migration_backup(db, 'absent-blob') == backup
    manifest = json.loads((backup.with_name(backup.name + '.attachments') / 'manifest.json').read_text())
    assert manifest['absent'] == {'conflict:' + proof[0]: [body_hash, size]} and manifest['references'] == manifest['attachments'] == {}
    assert (blob.read_bytes() if blob.exists() else None) == before


def test_backup_refuses_a_symlinked_body(tmp_path):
    path, proof, blob, body_hash, size = damaged_archive(tmp_path, 'symlink')
    with core.open_db(path, purpose='fixture.refuse') as db:
        before = db.execute('SELECT * FROM remote.row_conflicts').fetchall()
        with pytest.raises(ValueError, match='attachment body is a symlink') as failure:
            core._migration_backup(db, 'symlink-blob')
        assert proof[0] in str(failure.value) and body_hash in str(failure.value)
        assert db.execute('SELECT * FROM remote.row_conflicts').fetchall() == before
    assert not list(path.parent.glob('*.bak'))


@pytest.mark.parametrize('missing_first_path', [False, True])
def test_backup_resolves_shared_claimed_bytes_through_any_indexed_path(tmp_path, missing_first_path):
    data = b'external attachment'
    body_hash = core.provenance_digest(data)
    path, row, proof = attachment_archive(tmp_path, body_hash, len(data), 'external.bin')
    external = tmp_path / 'external.bin'
    external.write_bytes(data)
    with core.open_db(path, purpose='fixture.external-paths') as db:
        for identity, location in ([('missing', tmp_path / 'absent.bin')] if missing_first_path else []) + [('available', external)]:
            db.execute('INSERT INTO attachments(id,message_id,path,size) VALUES (?,?,?,?)', [identity, 'm', str(location), len(data)])
            db.execute('INSERT INTO attachment_bodies VALUES (?,?,?)', [identity, body_hash, len(data)])
        backup = core._migration_backup(db, 'external-path')
    bundle = backup.with_name(backup.name + '.attachments')
    manifest = json.loads((bundle / 'manifest.json').read_text())
    assert (bundle / body_hash).read_bytes() == data
    assert manifest['references']['conflict:' + proof[0]] == [body_hash, len(data)]
    assert len(manifest['attachments']) == 1


def test_backup_keeps_the_archive_locked_against_a_second_writer(tmp_path):
    """Closing any descriptor of the archive drops this process's fcntl lock; a hook then writes beside the migration."""
    path, _, _ = attachment_archive(tmp_path)
    with core.open_db(path, purpose='fixture.backup') as db:
        core._migration_backup(db, 'lock')
        other = subprocess.run([sys.executable, '-c', 'import duckdb, sys\nduckdb.connect(sys.argv[1]).execute("CHECKPOINT")', str(path)], capture_output=True, text=True)
    assert other.returncode and 'lock' in other.stderr, other.stderr
