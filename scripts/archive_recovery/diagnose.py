"""Audit an archive and export exact unavailable-body claims. Read-only and offline."""
import argparse, json, os, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(REPO / 'src'), str(REPO / 'apps/remote/src')]
from ai_convos import cli as core
from ai_convos_remote.projection import audit_rows


def diagnose(path, user):
    unavailable = []
    audit = audit_rows(path, local_user=user, on_unavailable=unavailable.append)
    with core._core(path, read_only=True, purpose='repair.diagnosis') as db:
        archive, generation = db.execute('SELECT archive_id::VARCHAR,generation FROM archive_state WHERE singleton').fetchone()
        core.required(generation == audit['archive_generation'], RuntimeError('Archive changed during diagnosis; retry'))
        proofs = db.execute('SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?)) ORDER BY id', [[r['proof'] for r in unavailable]]).fetchall()
    return dict(format='convos-archive-diagnosis-v1', archive_id=archive, audit=audit, unavailable=unavailable, proof_rows=proofs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--user-id', required=True, help='Archive owner account ID, used to resolve local signed row identities')
    parser.add_argument('--output', type=Path, required=True, help='New private JSON report; never overwritten')
    args = parser.parse_args()
    if args.output.exists(): parser.error('Output already exists')
    result = diagnose(args.database.expanduser().resolve(), args.user_id)
    with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
        stream.write(json.dumps(result, indent=2, default=str) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(result['audit'], sort_keys=True))
    if result['unavailable'] or result['audit']['relationships']: raise SystemExit(1)


if __name__ == '__main__': main()
