"""Simulate or apply evidence-backed archive repair. No imports, signing, or network access."""
import argparse, contextlib, json, os, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(REPO / 'src'), str(REPO / 'apps/remote/src')]
from ai_convos import cli as core


def write_json(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(json.dumps(value, indent=2, default=str) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    core._fsync(path.parent)


def snapshot(source, directory, attachments=True):
    directory.mkdir(mode=0o700)
    backup = directory / 'convos.db'
    with core._core(source, read_only=True, purpose='repair.snapshot') as db:
        if Path(str(source) + '.wal').exists(): raise ValueError('Archive has a WAL; run convos backup as its owner before repair')
        checksum = core._file_sha256(source)
        if attachments: core._backup_attachments(db, source, backup, checksum)
        core.atomic_publish(backup, lambda temp: (core._backup_copy(source, temp), core.required(core._file_sha256(temp) == checksum, ValueError('Backup checksum mismatch')), core._check_archive(temp)))
        core._fsync(directory.parent)
    return backup, checksum


def identity(db): return db.execute('SELECT archive_id::VARCHAR,generation FROM archive_state WHERE singleton').fetchone()


def history_restore_plan(db, donor, diagnosis):
    core.required(diagnosis['format'] == 'convos-archive-diagnosis-v1' and diagnosis['archive_id'] == identity(db)[0], ValueError('Diagnosis belongs to a different archive'))
    claims = {r['proof']: r for r in diagnosis['unavailable']}
    core.required(None not in claims, ValueError('A missing proof cannot be repaired from a body'))
    ids = sorted(claims)
    current = {r[0]: r for r in db.execute('SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))', [ids]).fetchall()}
    recorded = {r[0]: tuple(r) for r in diagnosis['proof_rows']}
    existing = dict(db.execute('SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))', [ids]).fetchall())
    with core._core(donor, read_only=True, purpose='repair.donor') as source:
        core.required(identity(source)[0] == identity(db)[0], ValueError('Donor is not a backup of this archive'))
        proofs = {r[0]: r for r in source.execute('SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))', [ids]).fetchall()}
        bodies = dict(source.execute('SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))', [ids]).fetchall())
        missing = [c for pid, c in claims.items() if pid not in bodies and pid not in existing]
        found = core.typed_logical_rows(source, [tuple(c[k] for k in ('kind', 'physical', 'source', 'author', 'state')) for c in missing], historical=True)
        paths = core.captured_edit_paths(source, [c['physical'] for c in missing if c['kind'] == 'file_edits'])
        bodies.update({c['proof']: json.dumps(row) for c in missing if (row := core.matching_logical_row(found[tuple(c[k] for k in ('kind', 'physical', 'source', 'author', 'state'))], c['expected'], [paths[c['physical']]] if c['physical'] in paths else ())) is not None})
    for pid, claim in claims.items():
        core.required(pid in current and current[pid] == proofs.get(pid) == recorded.get(pid), ValueError(f'Donor/current/diagnostic proof mismatch: {pid}'))
        proof = current[pid]
        core.required((proof[3], proof[4], proof[10], proof[6], proof[9]) == tuple(claim[k] for k in ('kind', 'source', 'author', 'expected', 'state')), ValueError(f'Diagnostic claim mismatch: {pid}'))
        core.required(pid not in existing or core.provenance_digest(json.loads(existing[pid])) == proof[6], ValueError(f'Existing retained body is corrupt: {pid}'))
        if pid in existing: continue
        core.required(pid in bodies, ValueError(f'Donor has no retained body: {pid}'))
        body = json.loads(bodies[pid])
        core.required((body['v'], body['kind'], body['id'], body['state'], core.provenance_digest(body)) == (proof[5], proof[3], proof[4], proof[9], proof[6]), ValueError(f'Donor body mismatch: {pid}'))
    return [dict(proof_id=pid, proof_row=current[pid], body=json.loads(bodies[pid])) for pid in ids if pid not in existing]


def plan(db, donor=None, diagnosis=None):
    scopes = core.repair_legacy_edit_scopes(db)
    eligible = {before[0] for before, after in scopes}
    mismatched = {edit for edit, path, repository, file in db.execute('SELECT s.file_edit_id,s.path,s.repository,x.file_id FROM provenance.file_edit_scopes s JOIN provenance.file_edit_files x USING(file_edit_id)').fetchall() if core.provenance_digest(dict(repository=repository, path=path)) != file}
    blocked = sorted(e['id'] for e in core._provenance_edits(db) if e['id'] in mismatched - eligible)
    return dict(archive=identity(db), scopes=scopes, bodies=history_restore_plan(db, donor, diagnosis) if diagnosis is not None else [], blocked_scopes=blocked)


def verify_unchanged(db, backup, allowed):
    # Compare complete row multisets in SQL; aggregate fingerprints can hide changes.
    db.execute("ATTACH '" + str(backup).replace("'", "''") + "' AS recovery_before (READ_ONLY)")
    tables = lambda catalog: set(db.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog=? AND table_type='BASE TABLE'", [catalog]).fetchall())
    current = tables(db.execute('SELECT current_database()').fetchone()[0])
    core.required(current == tables('recovery_before'), ValueError('Repair changed the table inventory'))
    for schema, table in sorted(current - allowed):
        name = '.'.join('"' + s.replace('"', '""') + '"' for s in (schema, table))
        changed = db.execute(f'SELECT EXISTS ((SELECT * FROM {name} EXCEPT ALL SELECT * FROM recovery_before.{name}) UNION ALL (SELECT * FROM recovery_before.{name} EXCEPT ALL SELECT * FROM {name}))').fetchone()[0]
        core.required(not changed, ValueError(f'Repair changed protected table: {schema}.{table}'))
    return ['.'.join(t) for t in sorted(current - allowed)]


def run(root, output, apply=False, database_only=False, donor=None, diagnosis=None):
    source = root / 'data/convos.db'
    core.required(source.is_file(), ValueError(f'Archive does not exist: {source}'))
    core.required(not apply or source.stat().st_uid == os.getuid(), ValueError('Live repair requires the archive owner'))
    core.required(not (apply and database_only), ValueError('Live repair requires a verified attachment backup'))
    output.mkdir(mode=0o700)
    core._fsync(output.parent)
    report = dict(format='convos-archive-repair-v1', mode='apply' if apply else 'simulation', source=str(source), success=False, committed=False)
    try:
        with contextlib.ExitStack() as leases:
            if apply:
                from ai_convos_remote import sync_run
                leases.enter_context(sync_run(root, True, 'repair.archive'))
                for path in ('data/.sync.lock', 'data/hook_inbox/.drain.lock'): leases.enter_context(core.operation_lock(root / path, 'repair.archive', wait=30))
            backup, checksum = snapshot(source, output / 'before', not database_only)
            report.update(backup=str(backup), backup_sha256=checksum, attachments_backed_up=not database_only)
            with core._core(backup, read_only=True, purpose='repair.plan') as db: expected = plan(db, donor, diagnosis)
            write_json(output / 'plan.json', expected)
            core.required(not expected['blocked_scopes'], ValueError('Unexplained active scope conflicts; inspect plan.json'))
            target = source if apply else output / 'simulation/data/convos.db'
            if not apply:
                target.parent.mkdir(parents=True, mode=0o700)
                core._backup_copy(backup, target)
                target.chmod(0o600)
            report['target'] = str(target)
            bundles = [backup.with_name(backup.name + '.attachments'), *( [donor.with_name(donor.name + '.attachments'), donor.parent / 'attachments'] if donor else [])]
            if not apply and not database_only:
                for blob, size in json.loads((bundles[0] / 'manifest.json').read_text())['attachments'].items():
                    core.required(core.attachment_index(bundles[0] / blob, size) == (blob, size), ValueError('Snapshot attachment changed'))
                    core.required(core.attachment_body((bundles[0] / blob).read_bytes(), target.parent), ValueError('Snapshot attachment exceeds storage limit'))
            for body in [r['body'] for r in expected['bodies'] if r['body']['kind'] == 'attachments' and r['body']['state'] == 'active']:
                blob, size = body['data']['body_hash'], body['data']['size']
                candidates = [target.parent / 'attachments' / (blob or ''), *(directory / (blob or '') for directory in bundles)]
                found = next((p for p in candidates if core.attachment_index(p, size) == (blob, size)), None)
                core.required(found, ValueError('Exact attachment bytes are unavailable'))
                core.required((stored := core.attachment_body(found.read_bytes(), target.parent)) and stored.name == blob, ValueError('Attachment changed during recovery'))
            with core._core(target, purpose='repair.apply') as db, core._transaction(db):
                core.required(plan(db, donor, diagnosis) == expected, ValueError('Archive or donor changed since planning'))
                repaired = core.repair_legacy_edit_scopes(db, apply=True)
                restored = core.restore_signed_bodies(db, expected['bodies'])
                allowed = {('provenance', 'file_edit_scopes'), ('main', 'archive_state'), ('main', 'archive_changes')}
                if restored: allowed |= {('remote', 'row_conflicts'), ('main', 'retrieval_state')}
                protected = verify_unchanged(db, backup, allowed)
                remaining = plan(db, donor, diagnosis)
                core.required(not remaining['scopes'] and not remaining['bodies'], ValueError('Repair is not idempotent'))
            report.update(success=True, committed=True, repaired_scopes=len(repaired), restored_bodies=restored, protected_tables=protected)
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally: write_json(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=core.PROJECT_ROOT)
    parser.add_argument('--output', type=Path, required=True, help='New private directory for backup, plan, and result')
    parser.add_argument('--apply', action='store_true', help='Apply to the owned archive; default repairs an isolated copy')
    parser.add_argument('--database-only', action='store_true', help='Simulation only, when source attachments are inaccessible')
    parser.add_argument('--donor', type=Path, help='Earlier backup containing exact missing retained bodies')
    parser.add_argument('--diagnosis', type=Path, help='Private diagnose.py output selecting the missing bodies')
    args = parser.parse_args()
    if bool(args.donor) != bool(args.diagnosis): parser.error('--donor and --diagnosis must be supplied together')
    result = run(args.root.expanduser().resolve(), args.output.expanduser().resolve(), args.apply, args.database_only, args.donor.expanduser().resolve() if args.donor else None, json.loads(args.diagnosis.read_text()) if args.diagnosis else None)
    print(json.dumps({k: result[k] for k in ('mode', 'target', 'repaired_scopes', 'restored_bodies', 'success')}, sort_keys=True))


if __name__ == '__main__': main()
