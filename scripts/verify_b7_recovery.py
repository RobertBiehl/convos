"""Verify a repair report's target, including every required signed row body."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "src"), str(REPO / "apps/remote/src")]

from ai_convos import cli as core
from ai_convos_remote.projection import audit_rows
from repair_b7_archive import fingerprints, write_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--user-id", help="Known account ID for an offline simulation without access to the source credentials")
    parser.add_argument("--repull", action="store_true", help="Resume relay recovery on the owned live target; default is offline audit only")
    args = parser.parse_args()
    repair = json.loads(args.report.read_text())
    target, backup = Path(repair["target"]), Path(repair["backup"])
    if args.repull and (repair["mode"] != "apply" or target.resolve() != Path(repair["source"]).resolve() or target.stat().st_uid != os.getuid()):
        raise SystemExit("--repull requires the owner-run live repair report, never a simulation")
    user = args.user_id or json.loads((Path(repair["source"]).parent.parent / "remote/config.json").read_text())["user"]
    output = args.report.parent / "audit.json"
    if output.exists():
        raise SystemExit(f"Refusing to overwrite {output}")
    checks = {}
    for label, path in (("before", backup), ("after", target)):
        with core._core(path, read_only=True, purpose="repair.verify.contents") as db:
            checks[label] = fingerprints(db)
    required = [f'"main"."{t}"' for t in core.ARCHIVE_COLUMNS] + ['"provenance"."file_edit_evidence"', '"remote"."row_proofs"']
    changed = [table for table in required if checks["before"][table] != checks["after"][table]]
    started, last = time.monotonic(), [0.0]

    def progress(stage: str) -> None:
        if time.monotonic() - last[0] >= 15:
            print(f"{int(time.monotonic() - started)}s | {stage}", flush=True)
            last[0] = time.monotonic()

    result = {"target": str(target), "mode": repair["mode"], "repull": args.repull, "archive_changes_since_backup": changed}
    try:
        if args.repull:
            from ai_convos_remote import repull_once
            _, result["audit"] = repull_once(target.parent.parent)
        else:
            result["audit"] = audit_rows(target, progress=progress, local_user=user)
        result["seconds"] = round(time.monotonic() - started, 2)
        result["preservation_complete"] = not result["audit"]["totals"].get("unavailable", 0)
        result["relationships_complete"] = not result["audit"]["relationships"]
        result["success"] = result["preservation_complete"]
    except BaseException as error:
        result.update(success=False, error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(output, result)
        print(f"Audit report: {output}", flush=True)
    print(json.dumps({k: result[k] for k in ("preservation_complete", "relationships_complete")}, sort_keys=True), flush=True)
    print(json.dumps(result["audit"]["totals"], sort_keys=True), flush=True)
    if not result["success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
