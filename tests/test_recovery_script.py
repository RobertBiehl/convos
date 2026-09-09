"""The operator's dry run must be isolated, private, and fail closed."""
import importlib.util
import json
from pathlib import Path
import sys

import duckdb
import pytest

from ai_convos import cli as core
from tests.test_legacy_scope_recovery import legacy_archive

SPEC = importlib.util.spec_from_file_location("archive_repair", Path(__file__).resolve().parents[1] / "scripts/archive_recovery/repair.py")
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def test_repair_script_default_is_private_simulation_with_exact_preimages(tmp_path, monkeypatch):
    path, original = legacy_archive(tmp_path)
    root, output = tmp_path / "source", tmp_path / "report"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    source = root / "data/convos.db"
    checksum = core._file_sha256(source)
    monkeypatch.setattr(sys, "argv", ["repair", "--root", str(root), "--output", str(output), "--database-only"])
    repair.main()
    report = json.loads((output / "report.json").read_text())
    assert report["mode"] == "simulation" and report["success"] and report["repaired_scopes"] == 1
    assert core._file_sha256(source) == checksum == report["backup_sha256"]
    assert {"main.messages", "remote.row_proofs", "provenance.file_edit_evidence"} <= set(report["protected_tables"])
    assert output.stat().st_mode & 0o777 == 0o700
    assert (output / "plan.json").stat().st_mode & 0o777 == 0o600
    assert len(json.loads((output / "plan.json").read_text())["scopes"]) == 1
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
    assert core._file_sha256(source) == checksum and (output / "plan.json").is_file()
    assert not (output / "simulation").exists()


def test_repair_script_never_allows_live_database_only_backup(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    (tmp_path / "data").mkdir()
    path.rename(tmp_path / "data/convos.db")
    monkeypatch.setattr(sys, "argv", ["repair", "--root", str(tmp_path), "--output", str(tmp_path / "out"), "--apply", "--database-only"])
    with pytest.raises(ValueError, match="requires a verified attachment backup"):
        repair.main()
    assert not (tmp_path / "out").exists()


def test_repair_rolls_back_unexpected_content_change_and_keeps_backup(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    root, output = tmp_path / "source", tmp_path / "repair"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    checksum = core._file_sha256(root / "data/convos.db")
    restore = core.restore_signed_bodies

    def faulty_writer(db, records):
        db.execute("UPDATE messages SET content='unrelated damage'")
        return restore(db, records)

    monkeypatch.setattr(core, "restore_signed_bodies", faulty_writer)
    with pytest.raises(ValueError, match="protected table: main.messages"):
        repair.run(root, output, database_only=True)
    report = json.loads((output / "report.json").read_text())
    assert not report["success"] and not report["committed"]
    assert core._file_sha256(root / "data/convos.db") == checksum == core._file_sha256(Path(report["backup"]))
    with core.open_db(report["target"], read_only=True, purpose="fixture.rollback") as db:
        assert len(core.repair_legacy_edit_scopes(db)) == 1
        assert not db.execute("SELECT 1 FROM messages WHERE content='unrelated damage'").fetchone()


def test_live_repair_uses_backup_and_is_idempotent_without_importing(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    root = tmp_path / "source"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    monkeypatch.setattr(core, "capture_provenance", lambda *a, **kw: pytest.fail("repair must not capture"))
    first = repair.run(root, tmp_path / "first", apply=True)
    assert first["committed"] and first["attachments_backed_up"] and first["repaired_scopes"] == 1
    second = repair.run(root, tmp_path / "second", apply=True)
    assert second["success"] and second["repaired_scopes"] == second["restored_bodies"] == 0
    with core.open_db(first["backup"], read_only=True, purpose="fixture.original") as db:
        assert len(core.repair_legacy_edit_scopes(db)) == 1


def test_repair_rejects_archive_changed_after_snapshot(tmp_path, monkeypatch):
    path, _ = legacy_archive(tmp_path)
    root, output = tmp_path / "source", tmp_path / "repair"
    (root / "data").mkdir(parents=True)
    path.rename(root / "data/convos.db")
    original = repair.snapshot

    def concurrent_writer(source, directory, attachments=True):
        saved = original(source, directory, attachments)
        with core.open_db(source, purpose="fixture.concurrent") as db:
            db.execute("UPDATE messages SET content='new capture'")
            core._archive_touch(db, [('messages', 'm')])
        return saved

    monkeypatch.setattr(repair, "snapshot", concurrent_writer)
    with pytest.raises(ValueError, match="changed since planning"):
        repair.run(root, output, apply=True)
    with core.open_db(root / "data/convos.db", read_only=True, purpose="fixture.preserved") as db:
        assert db.execute("SELECT content FROM messages").fetchone()[0] == 'new capture'
        assert len(core.repair_legacy_edit_scopes(db)) == 1
