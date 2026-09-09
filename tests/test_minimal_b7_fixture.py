"""The portable public fixture must reproduce real behavior without private input."""
import json
from pathlib import Path
import subprocess
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/b7_minimal_fixture.py"


def run_fixture(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args), "--checkout", str(ROOT)], capture_output=True, text=True)


def test_public_fixture_is_tiny_repeatable_and_preserves_every_original_row(tmp_path, monkeypatch):
    fixture, report = tmp_path / "fixture", tmp_path / "result.json"
    unrelated = tmp_path / "unrelated-archive"
    monkeypatch.setenv("CONVOS_PROJECT_ROOT", str(unrelated))
    built = run_fixture("build", fixture)
    assert built.returncode == 0, built.stderr
    manifest = json.loads((fixture / "manifest.json").read_text())
    assert manifest["archive_counts"] == dict(conversations=1, messages=1, tool_calls=0, attachments=0, artifacts=0, file_edits=1)
    assert manifest["table_counts"]["remote.row_proofs"] == 3
    rows = json.loads((fixture / "rows.json").read_text())
    raw = (fixture / "rows.json").read_text()
    assert not any(value in raw for value in (str(Path.home()), str(ROOT), "sign_private", "box_private"))
    assert rows["main.conversations"]["rows"][0][0] == "c"
    assert "Synthetic answer." in raw and "example.txt" in raw
    checked = run_fixture("check", fixture, "--report", report)
    assert checked.returncode == 0, checked.stdout + checked.stderr
    result = json.loads(report.read_text())
    assert result["success"] and result["fixture_unchanged"]
    assert result["cases"]["scope_migration"]["capture_passes"] == 2
    assert result["cases"]["edit_attestation"]["successor_proofs"] == 1
    assert result["cases"]["retained_history"]["unavailable_after"] == 0
    again = run_fixture("check", fixture)
    assert again.returncode == 0, again.stderr
    wrong_expectation = run_fixture("check", fixture, "--expect", "broken")
    assert wrong_expectation.returncode == 1
    rebuilt = tmp_path / "rebuilt"
    assert run_fixture("build", rebuilt).returncode == 0
    assert (rebuilt / "rows.json").read_bytes() == (fixture / "rows.json").read_bytes()
    assert not unrelated.exists()
    with duckdb.connect(str(fixture / "convos.db"), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM messages m LEFT JOIN conversations c ON c.id=m.conversation_id WHERE c.id IS NULL").fetchone()[0] == 0
        assert db.execute("SELECT version FROM core_schema").fetchone()[0] == 12


def test_fixture_builder_refuses_existing_directory_and_checker_rejects_changed_input(tmp_path):
    fixture = tmp_path / "fixture"
    assert run_fixture("build", fixture).returncode == 0
    assert run_fixture("build", fixture).returncode != 0
    (fixture / "rows.json").write_text("{}")
    assert run_fixture("check", fixture).returncode != 0
