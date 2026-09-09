"""Simulate causal branch continuation on an existing private repair copy."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import time

from repair_b7_archive import fingerprints, write_json
from ai_convos import cli as core
from ai_convos_remote import load
from ai_convos_remote.projection import attest_rows, clean
from ai_convos_remote.protocol import digest, logical_fact


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="A successful simulation report, never a live apply report")
    args = parser.parse_args()
    repair = json.loads(args.report.read_text())
    path, source = Path(repair["target"]), Path(repair["source"])
    if repair["mode"] != "simulation" or not repair["success"] or path.resolve() == source.resolve() or path.resolve() != (args.report.parent / "simulation/data/convos.db").resolve():
        raise SystemExit("Attestation verification accepts only an isolated simulation")
    output = args.report.parent / "attestation.json"
    if output.exists():
        raise SystemExit(f"Refusing to overwrite {output}")
    cfg = load(source.parent.parent)
    workspaces = [w["id"] for w in cfg["server_state"]["workspaces"] if w["device_authorized"] and cfg["workspaces"][w["id"]]["kind"] == "personal"]
    if len(workspaces) != 1:
        raise SystemExit("Simulation requires exactly one authorized personal workspace")
    started = time.monotonic()
    with core._core(path, read_only=True, purpose="repair.branch-inventory") as db:
        rows = db.execute("WITH heads AS (SELECT p.row_kind,p.source_row_id,p.revision,p.content_hash FROM remote.row_proofs p JOIN provenance.local_facts l ON (l.kind,l.entity)=(p.row_kind,p.source_row_id) WHERE p.author_user_id=? AND NOT EXISTS(SELECT 1 FROM remote.row_proofs n WHERE (n.row_kind,n.source_row_id,n.author_user_id,n.previous_revision)=(p.row_kind,p.source_row_id,p.author_user_id,p.revision))) SELECT * FROM heads QUALIFY count(DISTINCT revision) OVER (PARTITION BY row_kind,source_row_id)>1", [cfg["user"]]).fetchall()
        heads = {}
        for kind, entity, revision, content in rows:
            heads.setdefault((kind, entity), {})[revision] = content
        bases = {(kind, entity): revision for kind, entity, revision in db.execute("SELECT kind,entity,revision FROM remote.local_row_bases WHERE author=?", [cfg["user"]]).fetchall()}
        records = [clean(r) for r in core.provenance_records(db, set(heads))]
        candidates = [r for r in records if digest(logical_fact(r)) not in heads[r["kind"], r["entity"]].values()]
        ids = [r["entity"] for r in candidates]
        proofs = db.execute("SELECT * FROM remote.row_proofs WHERE author_user_id=? AND source_row_id IN (SELECT UNNEST(?)) ORDER BY id", [cfg["user"], ids]).fetchall()
        bodies = db.execute("SELECT c.proof_id,c.body FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.author_user_id=? AND p.source_row_id IN (SELECT UNNEST(?)) ORDER BY c.proof_id", [cfg["user"], ids]).fetchall()
        before = fingerprints(db)
    stored = {pid: json.loads(raw) for pid, raw in bodies}
    eligible = {(kind, entity): stored[pid] for pid, _, _, kind, entity, _, expected, revision, *_ in proofs if kind == "edit.observed" and bases.get((kind, entity)) == revision and revision in heads[kind, entity] and pid in stored and digest(stored[pid]) == expected}
    ambiguous = [(r["kind"], r["entity"]) for r in candidates if not (body := eligible.get((r["kind"], r["entity"]))) or (body["kind"], body["id"]) != (r["kind"], r["entity"]) or any(body["data"][k] != r["payload"][k] for k in ("turn", "file", "repository"))]
    write_json(args.report.parent / "branch-preimages.json", {"current_records": candidates, "proof_rows": proofs, "retained_bodies": bodies, "branches": [{"kind": kind, "entity": entity, "heads": values, "local_base": bases.get((kind, entity))} for (kind, entity), values in heads.items()]})
    result = {"mode": "simulation", "attestation": {"forked_fact_groups": len(heads), "current_facts": len(records), "candidates": len(candidates), "ambiguous": len(ambiguous), "candidate_kinds": dict(Counter(r["kind"] for r in candidates))}}
    try:
        if ambiguous:
            raise ValueError(f"{len(ambiguous)} facts lack a verified, scope-matching current predecessor; no signatures created")
        print(f"Attesting {len(candidates)} observations on their recorded branches in the isolated copy", flush=True)
        result["attestation"]["successor_proofs"] = attest_rows(path, cfg, workspaces[0], candidates)
        result["attestation"]["repeat_proofs"] = attest_rows(path, cfg, workspaces[0], candidates)
        if result["attestation"]["repeat_proofs"]:
            raise ValueError("Repeated attestation was not idempotent")
        with core._core(path, read_only=True, purpose="repair.branch-verify") as db:
            after = fingerprints(db)
            original = db.execute("SELECT * FROM remote.row_proofs WHERE id IN (SELECT UNNEST(?)) ORDER BY id", [[r[0] for r in proofs]]).fetchall()
        result["altered_tables"] = [table for table in before if before[table] != after[table]]
        content = [table for table in before if table.startswith('"provenance".') or table in [f'"main"."{t}"' for t in core.ARCHIVE_COLUMNS]]
        if any(before[table] != after[table] for table in content) or original != proofs:
            raise ValueError("Simulation changed archive content, evidence, or an original proof")
        result["attestation"].update(original_proofs_unchanged=len(proofs), content_tables_unchanged=len(content))
        result["success"] = True
    except BaseException as error:
        result.update(success=False, error=f"{type(error).__name__}: {error}")
        raise
    finally:
        result["seconds"] = round(time.monotonic() - started, 2)
        write_json(output, result)
        print(f"Report: {output}", flush=True)
    print(json.dumps(result["attestation"], sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
