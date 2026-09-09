"""Read-only signed-body availability inventory; export exact failing claims privately."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import time

from repair_b7_archive import write_json
from ai_convos import cli as core
from ai_convos_remote.projection import PROVENANCE, captured_edit_paths, digest, foreign_id, matching_logical_row, typed_logical_rows


def diagnose(path: Path, user: str, kinds: list[str]) -> dict:
    with core._core(path, read_only=True, purpose="repair.body-diagnostics") as db:
        origins = db.execute("SELECT o.table_name,o.physical_row_id,o.source_row_id,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.row_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id UNION ALL SELECT o.kind,o.physical_entity,o.source_entity,o.author_user_id,o.proof_id,p.content_hash,p.state FROM remote.provenance_origins o LEFT JOIN remote.row_proofs p ON p.id=o.proof_id").fetchall()
        seen = {r[4] for r in origins}
        heads = db.execute("SELECT p.row_kind,p.source_row_id,p.author_user_id,p.id,p.content_hash,p.state FROM remote.row_proofs p WHERE NOT EXISTS(SELECT 1 FROM remote.row_proofs c WHERE (c.row_kind,c.source_row_id,c.author_user_id,c.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision))").fetchall()
        origins += [(kind, source if author == user or kind in PROVENANCE - {'edit.observed', 'checkpoint.link'} else foreign_id(author, 'file_edits' if kind == 'edit.observed' else kind, source), source, author, pid, expected, state) for kind, source, author, pid, expected, state in heads if pid not in seen]
        selected = [r for r in origins if not kinds or r[0] in kinds]
        unavailable, counts = [], Counter()
        for at in range(0, len(selected), 5000):
            batch = selected[at:at + 5000]
            found = typed_logical_rows(db, [(kind, physical, source, author, state) for kind, physical, source, author, pid, expected, state in batch])
            retained = {pid: json.loads(body) for pid, body in db.execute("SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))", [[r[4] for r in batch]]).fetchall()}
            paths = captured_edit_paths(db, [r[1] for r in batch if r[0] == 'file_edits'])
            for kind, physical, source, author, pid, expected, state in batch:
                row = found[kind, physical, source, author, state]
                projection = row is not None and expected is not None and matching_logical_row(row, expected, [paths[physical]] if kind == 'file_edits' and physical in paths else ()) is not None
                kept = pid in retained and digest(retained[pid]) == expected
                counts['checked'] += 1
                counts['projection_match' if projection else 'retained' if kept else 'unavailable'] += 1
                if not projection and not kept:
                    unavailable.append(dict(kind=kind, physical=physical, source=source, author=author, proof=pid, expected=expected, state=state, projection='missing' if row is None else 'mismatch', retained_present=pid in retained))
            if at % 50000 == 0 or at + 5000 >= len(selected):
                print(f"Checked {min(at + 5000, len(selected))}/{len(selected)}; unavailable={len(unavailable)}", flush=True)
        ids = [r['proof'] for r in unavailable]
        proofs = db.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?)) ORDER BY id", [ids]).fetchall()
    return dict(selected_kinds=kinds, counts=dict(counts), unavailable_by_kind=dict(Counter(r['kind'] for r in unavailable)), unavailable=unavailable, proof_rows=proofs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--user-id', required=True)
    parser.add_argument('--kind', action='append', default=[])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists():
        raise SystemExit(f'Refusing to overwrite {args.output}')
    started = time.monotonic()
    result = diagnose(args.database, args.user_id, args.kind)
    result['seconds'] = round(time.monotonic() - started, 2)
    write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('counts', 'unavailable_by_kind', 'seconds')}, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
