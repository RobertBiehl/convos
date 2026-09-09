"""Back up, inventory, and narrowly repair v3 placeholders in a b7 archive.

Default: repair an isolated copy, never the source. --apply requires ownership.
Private exports contain conversations and signed history; do not send them.
Run with this checkout's core on PYTHONPATH and the installed Convos Python.
"""
from __future__ import annotations

import argparse
import contextlib
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "src"), str(REPO / "apps/remote/src")]

from ai_convos import cli as core


def write_json(path: Path, value: object) -> None:
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o600)


def snapshot(source: Path, directory: Path, attachments: bool = True) -> tuple[Path, str]:
    """A shared DuckDB reader excludes writers for the entire verified copy."""
    directory.mkdir(mode=0o700)
    target = directory / "convos.db"
    with core._core(source, read_only=True, purpose="repair.snapshot") as db:
        if source.with_name(source.name + ".wal").exists():
            raise ValueError("Source has a WAL; run `convos backup` as its owner first, then retry")
        core._backup_copy(source, target)
        target.chmod(0o600)
        expected = core._file_sha256(source)
        if core._file_sha256(target) != expected:
            raise ValueError("Snapshot hash mismatch; no repair attempted")
        core._check_archive(target)
        if attachments:
            core._backup_attachments(db, source, target, expected)
    return target, expected


def fingerprints(db) -> dict:
    """Order-independent, full-row checks, including JSON and embeddings."""
    tables = [f'"{schema}"."{name}"' for schema, name in db.execute(
        "SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' "
        "AND table_schema IN ('main','provenance','remote') ORDER BY 1,2"
    ).fetchall()]
    return {table: db.execute(f"SELECT count(*),bit_xor(hash(t)),sum(hash(t)::HUGEINT) FROM {table} t").fetchone() for table in tables}


def inventory(db) -> dict:
    rows = db.execute(
        "SELECT s.file_edit_id,s.path,s.repository,s.root,s.checkout,s.observed_at,s.route,"
        "f.id,f.path,f.repository,x.evidence,v.status,"
        "EXISTS(SELECT 1 FROM remote.row_origins o WHERE o.table_name='file_edits' AND o.physical_row_id=s.file_edit_id),"
        "EXISTS(SELECT 1 FROM provenance.pending p WHERE p.kind='file_edits' AND p.entity=s.file_edit_id) "
        "FROM provenance.file_edit_scopes s JOIN provenance.file_edit_files x ON x.file_edit_id=s.file_edit_id "
        "JOIN provenance.files f ON f.id=x.file_id LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=s.file_edit_id "
        "ORDER BY s.file_edit_id"
    ).fetchall()
    mismatches = [r for r in rows if core.provenance_digest({"repository": r[2], "path": r[1]}) != r[7]]
    planned = core.repair_legacy_edit_scopes(db)
    eligible = {before[0] for before, _ in planned}
    active = core._provenance_edits(db)
    blocked = [e for e in active if e["id"] in {r[0] for r in mismatches}]
    errors = []
    for edit in blocked[:3]:
        record = next(r for r in core._observe_provenance([edit])[0] if r["kind"] == "edit.observed")
        errors.append(core.provenance_issue(db, record))
    return {
        "archive": db.execute("SELECT archive_id::VARCHAR,generation FROM archive_state").fetchone(),
        "schema": db.execute("SELECT version FROM core_schema").fetchone()[0],
        "scopes_with_file_mapping": len(rows), "mismatches": len(mismatches),
        "eligible": len(planned), "eligible_statuses": dict(Counter(r[11] for r in mismatches if r[0] in eligible)),
        "pending_capture_edits": len(active), "blocked_capture_edits": len(blocked),
        "blocked_ineligible": [e["id"] for e in blocked if e["id"] not in eligible],
        "reproduced_errors": errors, "mismatch_rows": mismatches, "plan": planned,
    }


def extract(db, directory: Path, analysis: dict) -> None:
    """Export exact preimages, linked content, signatures, and retained bodies."""
    ids = [r[0] for r in analysis["mismatch_rows"]]
    selected = "SELECT UNNEST(?)"
    edits = f"SELECT message_id FROM file_edits WHERE id IN ({selected})"
    files = f"SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id IN ({selected})"
    proof_ids = f"SELECT id FROM remote.row_proofs WHERE source_row_id IN ({selected})"
    queries = {
        "file_edits": (f"id IN ({selected})", ids),
        "messages": (f"id IN ({edits})", ids),
        "conversations": (f"id IN (SELECT conversation_id FROM messages WHERE id IN ({edits}))", ids),
        "tool_calls": (f"message_id IN ({edits})", ids),
        "provenance.file_edit_scopes": (f"file_edit_id IN ({selected})", ids),
        "provenance.file_edit_files": (f"file_edit_id IN ({selected})", ids),
        "provenance.file_edit_evidence": (f"file_edit_id IN ({selected})", ids),
        "provenance.files": (f"id IN ({files})", ids),
        "provenance.file_versions": (f"file_id IN ({files})", ids),
        "provenance.checkpoint_edits": (f"file_edit_id IN ({selected})", ids),
        "remote.row_origins": (f"table_name='file_edits' AND physical_row_id IN ({selected})", ids),
        "remote.provenance_origins": (f"kind='edit.observed' AND physical_entity IN ({selected})", ids),
        "remote.row_proofs": (f"source_row_id IN ({selected})", ids),
        "remote.row_conflicts": (f"proof_id IN ({proof_ids})", ids),
    }
    # The full snapshot also covers foreign logical IDs and all dependency tables.
    with (directory / "affected-rows.jsonl").open("x") as output:
        for table, (where, values) in queries.items():
            cursor = db.execute(f"SELECT * FROM {table} WHERE {where}", [values])
            columns = [d[0] for d in cursor.description]
            while rows := cursor.fetchmany(500):
                for row in rows:
                    output.write(json.dumps({"table": table, "columns": columns, "row": row}, default=str) + "\n")
        output.flush()
        os.fsync(output.fileno())
    (directory / "affected-rows.jsonl").chmod(0o600)
    write_json(directory / "scope-preimages.json", analysis)


def capture(path: Path) -> list[dict]:
    outcomes = []
    for iteration in range(2):
        started = time.monotonic()
        print(f"Full provenance capture {iteration + 1}/2: {path}", flush=True)
        records = core.capture_provenance(path)
        outcomes.append({"seconds": round(time.monotonic() - started, 2), "records": dict(Counter(r["kind"] for r in records))})
    return outcomes


def history_restore_plan(db, backup: Path, diagnosis: dict) -> list[dict]:
    claims = {r["proof"]: r for r in diagnosis["unavailable"]}
    ids = sorted(claims)
    current = {r[0]: r for r in db.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))", [ids]).fetchall()}
    existing = dict(db.execute("SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))", [ids]).fetchall())
    with core._core(backup, read_only=True, purpose="repair.history-source") as source:
        original = {r[0]: r for r in source.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?))", [ids]).fetchall()}
        bodies = dict(source.execute("SELECT proof_id,body FROM remote.row_conflicts WHERE proof_id IN (SELECT UNNEST(?))", [ids]).fetchall())
    plan = []
    for pid in ids:
        claim = claims[pid]
        if pid not in current or pid not in original or current[pid] != original[pid]:
            raise ValueError(f"Original/current proof mismatch: {pid}")
        proof = current[pid]
        if (proof[3], proof[4], proof[10], proof[6], proof[9]) != tuple(claim[k] for k in ("kind", "source", "author", "expected", "state")):
            raise ValueError(f"Diagnostic/proof mismatch: {pid}")
        if pid in existing:
            if core.provenance_digest(json.loads(existing[pid])) != proof[6]:
                raise ValueError(f"Existing retained body is corrupt; refusing overwrite: {pid}")
            continue
        if pid not in bodies:
            raise ValueError(f"Exact retained body unavailable in backup: {pid}")
        body = json.loads(bodies[pid])
        if (body["v"], body["kind"], body["id"], body["state"], core.provenance_digest(body)) != (proof[5], proof[3], proof[4], proof[9], proof[6]):
            raise ValueError(f"Backup body/proof mismatch: {pid}")
        plan.append(dict(proof_id=pid, proof_row=proof, body=body))
    return plan


def full_sync(root: Path, output: Path, results: list[dict]) -> None:
    from ai_convos_remote import sync_run
    environment = {**os.environ, "CONVOS_PROJECT_ROOT": str(root), "PYTHONPATH": os.pathsep.join([str(REPO / "src"), str(REPO / "apps/remote/src"), os.environ.get("PYTHONPATH", "")])}
    for iteration in range(2):
        started = time.monotonic()
        logfile = output / f"full-sync-{iteration + 1}.log"
        print(f"Full local-only sync {iteration + 1}/2; log: {logfile}", flush=True)
        with sync_run(root, True, "repair.full-sync"), logfile.open("x") as log:
            with subprocess.Popen([sys.executable, "-m", "ai_convos", "sync", "--full", "--local-only"], env=environment, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="", flush=True)
                status = process.wait()
        results.append({"exit_code": status, "seconds": round(time.monotonic() - started, 2), "log": str(logfile)})
        if status:
            raise ValueError(f"Full sync failed with exit code {status}; repaired scopes and backups preserved; see {logfile}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.home() / ".convos")
    parser.add_argument("--output", type=Path, required=True, help="New private directory; must not exist")
    parser.add_argument("--apply", action="store_true", help="Repair the owned live database after backup; default is simulation")
    parser.add_argument("--capture", action="store_true", help="Run full provenance capture twice on the repaired target")
    parser.add_argument("--sync-full", action="store_true", help="Run real local-only import and provenance twice; requires source ownership")
    parser.add_argument("--database-only", action="store_true", help="Simulation only: attachments remain in the inaccessible source account")
    parser.add_argument("--restore-history", type=Path, help="Recover exact retained bodies from this earlier verified archive backup")
    parser.add_argument("--restore-claims", type=Path, help="Private unavailable-row report from diagnose_b7_rows.py; requires --restore-history")
    args = parser.parse_args()
    os.umask(0o077)
    root, output = args.root.expanduser().resolve(), args.output.expanduser().resolve()
    source = root / "data/convos.db"
    if args.apply and source.stat().st_uid != os.getuid():
        raise SystemExit("--apply must run as the database owner; simulation is available read-only")
    if args.sync_full and source.stat().st_uid != os.getuid():
        raise SystemExit("--sync-full must run as the source owner; --capture simulates the provenance phase read-only")
    if args.apply and args.database_only:
        raise SystemExit("--apply requires a verified attachment backup; --database-only is simulation-only")
    if bool(args.restore_history) != bool(args.restore_claims):
        raise SystemExit("History recovery requires both --restore-history and --restore-claims")
    diagnosis = json.loads(args.restore_claims.read_text()) if args.restore_claims else None
    output.mkdir(mode=0o700)
    report = {"started": datetime.now(timezone.utc).isoformat(), "source": str(source), "mode": "apply" if args.apply else "simulation"}
    try:
        with contextlib.ExitStack() as locks:
            if args.apply:
                from ai_convos_remote import sync_run
                locks.enter_context(sync_run(root, True, "repair.b7"))
                locks.enter_context(core.operation_lock(root / "data/.sync.lock", "repair.b7", wait=30))
                locks.enter_context(core.operation_lock(root / "data/hook_inbox/.drain.lock", "repair.b7", wait=30))
                with core._core(source, purpose="repair.checkpoint") as db:
                    db.execute("CHECKPOINT")
            print(f"Creating verified snapshot: {source}", flush=True)
            saved, checksum = snapshot(source, output / "before", not args.database_only)
            report.update(backup=str(saved), backup_sha256=checksum, attachments_backed_up=not args.database_only)
            with core._core(saved, read_only=True, purpose="repair.inventory") as db:
                analysis, before = inventory(db), fingerprints(db)
                extract(db, output, analysis)
                history = history_restore_plan(db, args.restore_history, diagnosis) if diagnosis is not None else []
            if args.restore_history:
                write_json(output / "history-preimages.json", history)
                report["history_source_sha256"] = core._file_sha256(args.restore_history)
            report["before"] = {k: v for k, v in analysis.items() if k not in ("mismatch_rows", "plan")}
            print(f"Eligible scopes: {analysis['eligible']}; blocked captures: {analysis['blocked_capture_edits']}; ineligible blockers: {len(analysis['blocked_ineligible'])}", flush=True)
            if analysis["blocked_ineligible"]:
                raise ValueError("Unexplained active scope conflicts; review private preimages before applying")
            if args.apply:
                target = source
            else:
                target = output / "simulation/data/convos.db"
                target.parent.mkdir(parents=True, mode=0o700)
                core._backup_copy(saved, target)
                target.chmod(0o600)
            report["target"] = str(target)
            with core._core(target, purpose="repair.legacy-scopes") as db, core._transaction(db):
                if core.repair_legacy_edit_scopes(db) != analysis["plan"]:
                    raise ValueError("Repair preconditions changed since backup; no repair committed")
                if diagnosis is not None and history_restore_plan(db, args.restore_history, diagnosis) != history:
                    raise ValueError("History preconditions changed since backup; no repair committed")
                changed = core.repair_legacy_edit_scopes(db, apply=True)
                if history:
                    core.restore_signed_bodies(db, history)
                after = fingerprints(db)
                allowed = {'"provenance"."file_edit_scopes"', '"main"."archive_state"', '"main"."archive_changes"'}
                if history:
                    allowed.update({'"remote"."row_conflicts"', '"main"."retrieval_state"'})
                altered = [t for t in before if before[t] != after[t]]
                if set(altered) - allowed:
                    raise ValueError(f"Unexpected table changes; transaction rolled back: {altered}")
                if core.repair_legacy_edit_scopes(db):
                    raise ValueError("Repair was not idempotent; transaction rolled back")
                if diagnosis is not None and history_restore_plan(db, args.restore_history, diagnosis):
                    raise ValueError("History recovery was not idempotent; transaction rolled back")
            report.update(repaired_scopes=len(changed), altered_tables=altered, unchanged_tables=[t for t in before if t not in altered])
            report["restored_bodies"] = len(history)
            write_json(output / "repair-verification.json", {"before": before, "after": after, "altered": altered})
            with core._core(target, read_only=True, purpose="repair.verify") as db:
                remaining = inventory(db)
            report["after"] = {k: v for k, v in remaining.items() if k not in ("mismatch_rows", "plan")}
            print(f"Repaired {len(changed)} scopes; restored {len(history)} retained bodies; archive content, evidence statuses, and proofs unchanged.", flush=True)
            if args.capture:
                report["capture"] = capture(target)
        if args.sync_full:
            report["full_sync"] = []
            full_sync(target.parent.parent, output, report["full_sync"])
        report["success"] = True
    except BaseException as error:
        report.update(success=False, error=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["finished"] = datetime.now(timezone.utc).isoformat()
        write_json(output / "report.json", report)
        print(f"Report: {output / 'report.json'}", flush=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["convos"]:
        from ai_convos import app
        sys.argv.pop(1)
        app()
    else:
        main()
