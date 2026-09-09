"""Build a maintainer bundle from code and explicitly allowlisted report fields."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import zipfile

REPO = Path(__file__).resolve().parents[1]
FILES = [
    "CHANGELOG.md", "src/ai_convos/cli.py", "apps/remote/src/ai_convos_remote/projection.py", "apps/remote/src/ai_convos_remote/__init__.py",
    "docs/database-connection-ledger.md", "docs/b7-archive-recovery.md", "docs/b7-minimal-fixture.md",
    "scripts/repair_b7_archive.py", "scripts/verify_b7_recovery.py", "scripts/verify_b7_attestation.py", "scripts/package_b7_recovery.py", "scripts/diagnose_b7_rows.py",
    "tests/test_legacy_scope_recovery.py", "tests/test_maintenance_recovery.py", "tests/test_recovery_script.py", "tests/test_retained_origin_recovery.py",
    "scripts/b7_minimal_fixture.py", "tests/test_minimal_b7_fixture.py",
]
INVENTORY_FIELDS = {
    "schema", "scopes_with_file_mapping", "mismatches", "eligible", "eligible_statuses",
    "pending_capture_edits", "blocked_capture_edits", "blocked_ineligible", "reproduced_errors",
}


def sanitize(report: dict) -> dict:
    if "unavailable_by_kind" in report:
        return {k: report[k] for k in ("selected_kinds", "unavailable_by_kind", "seconds")} | {"counts": {k: report["counts"][k] for k in ("checked", "projection_match", "retained", "unavailable") if k in report["counts"]}}
    if "attestation" in report:
        return {k: report[k] for k in ("mode", "seconds", "success", "altered_tables")} | {"attestation": {k: report["attestation"][k] for k in ("forked_fact_groups", "current_facts", "candidates", "ambiguous", "candidate_kinds", "successor_proofs", "repeat_proofs", "original_proofs_unchanged", "content_tables_unchanged") if k in report["attestation"]}}
    if "audit" in report:
        return {k: report[k] for k in ("mode", "repull", "seconds", "success", "preservation_complete", "relationships_complete", "archive_changes_since_backup") if k in report} | {
            "audit": {k: report["audit"][k] for k in ("totals", "tables", "relationships", "archive_generation") if k in report["audit"]}
        }
    result = {k: report[k] for k in ("started", "finished", "mode", "backup_sha256", "attachments_backed_up", "repaired_scopes", "restored_bodies", "history_source_sha256", "altered_tables", "unchanged_tables", "capture", "success") if k in report}
    result.update({stage: {k: value for k, value in report[stage].items() if k in INVENTORY_FIELDS} for stage in ("before", "after") if stage in report})
    if "full_sync" in report:
        result["full_sync"] = [{k: run[k] for k in ("exit_code", "seconds")} for run in report["full_sync"]]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base", default="v0.11.6b7.post1")
    parser.add_argument("--report", action="append", default=[], metavar="LABEL=JSON")
    parser.add_argument("--note", type=Path, required=True, help="Manually reviewed maintainer report; no raw incident logs")
    parser.add_argument("--diagnostics", type=Path)
    args = parser.parse_args()
    for path in FILES:
        subprocess.run(["git", "ls-files", "--error-unmatch", path], cwd=REPO, check=True, stdout=subprocess.DEVNULL)
    patch = subprocess.check_output(["git", "diff", "--binary", args.base, "--", *FILES], cwd=REPO)
    if not patch:
        raise SystemExit("No code changes to package")
    base = subprocess.check_output(["git", "rev-parse", args.base + "^{commit}"], cwd=REPO, text=True).strip()
    entries = {
        "recovery.patch": patch,
        "MAINTAINER_REPORT.md": args.note.read_bytes(),
        "BASE.txt": f"Base: {base} ({args.base}; upstream b7 plus existing fork integration)\nNo new version, dependency pin, or relay changes are included.\n".encode(),
        "README.md": b"# Convos b7 recovery evidence\n\nThis bundle contains a proposed upstream patch, synthetic regression tests, private-repair tools, and sanitized outcome reports. No database, attachment, credentials, conversation text, or signed row bodies are included.\n\nApply in a Convos b7 checkout:\n\n```sh\ngit apply --check /path/to/recovery.patch\ngit apply /path/to/recovery.patch\nuv sync --extra dev\nuv run pytest tests/test_legacy_scope_recovery.py tests/test_maintenance_recovery.py tests/test_recovery_script.py tests/test_retained_origin_recovery.py -q\nuv run pytest -m 'not integration' -q\n```\n\nStart with MAINTAINER_REPORT.md. See docs/b7-archive-recovery.md for simulation and backup-first application. Do not run an apply command on an unbacked-up archive. The scripts require the patched checkout, not just the extracted scripts folder.\n",
    }
    entries.update({path: (REPO / path).read_bytes() for path in FILES if path.startswith(("scripts/", "tests/", "docs/b7-"))})
    for value in args.report:
        label, path = value.split("=", 1)
        if not re.fullmatch(r"[a-z0-9-]+", label):
            raise ValueError("Report labels must contain only lowercase letters, digits, and hyphens")
        entries[f"reports/{label}.json"] = (json.dumps(sanitize(json.loads(Path(path).read_text())), indent=2) + "\n").encode()
    if args.diagnostics:
        entries["scripts/convos-b7-diagnostics.py"] = args.diagnostics.read_bytes()
    entries["SHA256SUMS"] = "".join(f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in sorted(entries.items())).encode()
    with zipfile.ZipFile(args.output, "x", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, data in sorted(entries.items()):
            bundle.writestr(name, data)
    with zipfile.ZipFile(args.output) as bundle:
        if bundle.testzip() is not None or set(bundle.namelist()) != set(entries):
            raise ValueError("ZIP verification failed")
        for name, data in entries.items():
            if bundle.read(name) != data:
                raise ValueError(f"ZIP content mismatch: {name}")
    print(f"ZIP: {args.output}\nFiles: {len(entries)}\nBytes: {args.output.stat().st_size}\nSHA256: {hashlib.sha256(args.output.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
