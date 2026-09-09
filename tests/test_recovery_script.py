"""The operator's dry run must be isolated, private, and fail closed."""
import importlib.util
import json
from pathlib import Path
import sys
import subprocess

import duckdb
import pytest

from ai_convos import cli as core
from tests.test_legacy_scope_recovery import legacy_archive

SPEC = importlib.util.spec_from_file_location("repair_b7_archive", Path(__file__).resolve().parents[1] / "scripts/repair_b7_archive.py")
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def test_repair_script_default_is_private_simulation_with_exact_preimages(tmp_path, monkeypatch):
    path, original = legacy_archive(tmp_path)
    root, output = tmp_path / "source", tmp_path / "report"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    source = root / "data/convos.db"
    checksum = core._file_sha256(source)
    monkeypatch.setattr(sys, "argv", ["repair", "--root", str(root), "--output", str(output), "--database-only", "--capture"])
    repair.main()
    report = json.loads((output / "report.json").read_text())
    assert report["mode"] == "simulation" and report["success"] and report["repaired_scopes"] == 1
    assert core._file_sha256(source) == checksum == report["backup_sha256"]
    assert report["after"]["blocked_capture_edits"] == 0 and report["after"]["eligible"] == 0
    assert output.stat().st_mode & 0o777 == 0o700
    assert (output / "scope-preimages.json").stat().st_mode & 0o777 == 0o600
    assert {json.loads(line)["table"] for line in (output / "affected-rows.jsonl").read_text().splitlines()} >= {"file_edits", "messages", "provenance.file_edit_scopes", "provenance.file_edit_files"}
    with duckdb.connect(report["target"], read_only=True) as db:
        assert db.execute("SELECT path,repository FROM provenance.file_edit_scopes").fetchone() == original[1:3]


def test_repair_script_unexplained_conflict_refuses_before_any_source_mutation(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    root, output = tmp_path / "source", tmp_path / "report"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    source = root / "data/convos.db"
    with duckdb.connect(str(source)) as db:
        db.execute("UPDATE provenance.file_edit_scopes SET path='external/real-captured-path'")
    checksum = core._file_sha256(source)
    monkeypatch.setattr(sys, "argv", ["repair", "--root", str(root), "--output", str(output), "--database-only"])
    with pytest.raises(ValueError, match="Unexplained active scope conflicts"):
        repair.main()
    assert not json.loads((output / "report.json").read_text())["success"]
    assert core._file_sha256(source) == checksum and (output / "scope-preimages.json").is_file()
    assert not (output / "simulation").exists()


def test_repair_script_never_allows_live_database_only_backup(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    (tmp_path / "data").mkdir()
    path.rename(tmp_path / "data/convos.db")
    monkeypatch.setattr(sys, "argv", ["repair", "--root", str(tmp_path), "--output", str(tmp_path / "out"), "--apply", "--database-only"])
    with pytest.raises(SystemExit, match="requires a verified attachment backup"):
        repair.main()
    assert not (tmp_path / "out").exists()


def test_maintainer_report_excludes_private_data_and_keeps_verifiable_totals():
    spec = importlib.util.spec_from_file_location("package_b7_recovery", Path(__file__).resolve().parents[1] / "scripts/package_b7_recovery.py")
    package = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(package)
    secret = "PRIVATE-CONVERSATION-AND-CREDENTIAL-CANARY"
    raw = {"success": True, "mode": "simulation", "target": secret, "source": secret, "config": secret,
           "before": {"mismatches": 9, "plan": secret, "mismatch_rows": secret},
           "after": {"eligible": 0}, "full_sync": [{"exit_code": 0, "seconds": 1, "log": secret}]}
    clean = package.sanitize(raw)
    assert secret not in json.dumps(clean) and clean["before"] == {"mismatches": 9}
    assert clean["after"] == {"eligible": 0} and clean["full_sync"] == [{"exit_code": 0, "seconds": 1}]
    audit = package.sanitize({"success": True, "target": secret, "audit": {"totals": {"unavailable": 0}, "tables": {}, "examples": secret}})
    assert secret not in json.dumps(audit) and audit["audit"]["totals"]["unavailable"] == 0
    branch = package.sanitize({"mode": "simulation", "seconds": 1, "success": True, "altered_tables": [], "attestation": {"candidates": 176, "current_records": secret}})
    assert secret not in json.dumps(branch) and branch["attestation"] == {"candidates": 176}
    diagnosed = package.sanitize({"selected_kinds": [], "unavailable_by_kind": {"edit.observed": 20}, "seconds": 1, "counts": {"checked": 100, "unavailable": 20, "raw": secret}, "unavailable": secret, "proof_rows": secret})
    assert secret not in json.dumps(diagnosed) and diagnosed["counts"] == {"checked": 100, "unavailable": 20}


def test_branch_verifier_refuses_live_archive_before_loading_credentials(tmp_path):
    source = tmp_path / "convos.db"
    source.write_bytes(b"untouched archive")
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"mode": "apply", "success": True, "source": str(source), "target": str(source)}))
    script = Path(__file__).resolve().parents[1] / "scripts/verify_b7_attestation.py"
    result = subprocess.run([sys.executable, str(script), str(report)], capture_output=True, text=True)
    assert result.returncode != 0 and "only an isolated simulation" in result.stderr
    assert source.read_bytes() == b"untouched archive" and not (tmp_path / "branch-preimages.json").exists()
