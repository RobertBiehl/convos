"""Maintenance queues capture without discarding input or monopolizing DuckDB."""
import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.projection import apply_row_replicas, audit_rows
from tests.test_hooks import hooks, transcript
from tests.test_remote_projection import _provider_alias_archive, signed_edit_graph


@pytest.mark.parametrize("interrupt", [False, True])
def test_audit_queues_capture_and_releases_maintenance_leases(hooks, interrupt):
    sessions, data = hooks
    data.mkdir(exist_ok=True)
    path = data / "convos.db"
    _, _, _, control, _, _, bodies, _ = signed_edit_graph()
    apply_row_replicas(path, bodies, "w", [control], local_user="receiver")
    session = sessions / "new.jsonl"
    transcript(session, user="arrived during audit")
    called = []

    def progress(stage):
        if called:
            return
        called.append(stage)
        core.enqueue_hook("codex", {"transcript_path": str(session)})
        assert core.drain_hooks() == 0
        assert list(core.HOOK_DIR.glob("*.json"))
        with pytest.raises(core.LockBusy):
            core._sync_leader(lambda _: None)
        with core.open_db(path, wait=0, purpose="independent.writer") as db:
            assert db.execute("SELECT count(*) FROM conversations").fetchone()[0] == 1
        if interrupt:
            raise InterruptedError("test cancellation")

    if interrupt:
        with pytest.raises(InterruptedError):
            audit_rows(path, page=1, progress=progress)
    else:
        assert audit_rows(path, page=1, progress=progress)["totals"]["unavailable"] == 0
    assert called and core.drain_hooks(block=True) == 1
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM messages WHERE content='arrived during audit'").fetchone()[0] == 1
    core._sync_leader(lambda _: None)


def test_alias_metadata_conflict_rejects_before_expanding_dependent_bodies(tmp_path, monkeypatch):
    _, path, _, _, _, cfg, _ = _provider_alias_archive(tmp_path, session_b="different")
    monkeypatch.setattr(projection, "_alias_pages", lambda *_: pytest.fail("expanded bodies for contradictory session evidence"))
    result = projection.reconcile_provider_aliases(path, cfg, "personal")
    assert result["changed"] == result["settled"] == 0
    assert list(result["blocked"].values()) == ["provider alias exact evidence conflicts"]


def test_full_sync_holds_capture_lease_and_leaves_new_input_queued(hooks, monkeypatch):
    sessions, data = hooks
    monkeypatch.setattr(core, "STATE_PATH", data / "sync_state.json")
    session = sessions / "during-full.jsonl"
    called = []
    observe = core.capture_provenance

    def capture(*args, **kwargs):
        transcript(session, user="arrived during full sync")
        core.enqueue_hook("codex", {"transcript_path": str(session)})
        assert core.drain_hooks() == 0
        called.append(True)
        return []

    monkeypatch.setattr(core, "capture_provenance", capture)
    core.sync(False, 300, False, False, True, False, True)
    assert called and list(core.HOOK_DIR.glob("*.json"))
    monkeypatch.setattr(core, "capture_provenance", observe)
    assert core.drain_hooks(block=True) == 1


@pytest.mark.parametrize("interrupt", [False, True])
def test_alias_reconciliation_queues_capture_between_pages(hooks, tmp_path, monkeypatch, interrupt):
    sessions, _ = hooks
    _, path, _, _, _, cfg, _ = _provider_alias_archive(tmp_path)
    for name, value in (("DATA_DIR", path.parent), ("DB_PATH", path), ("HOOK_DIR", path.parent / "hook_inbox")):
        monkeypatch.setattr(core, name, value)
    session = sessions / "during-alias.jsonl"
    transcript(session, user="arrived during alias reconciliation")
    called = []
    real_yield = projection.archive_yield

    def yielding(value):
        if not called:
            called.append(True)
            core.enqueue_hook("codex", {"transcript_path": str(session)})
            assert core.drain_hooks() == 0
            with pytest.raises(core.LockBusy):
                core._sync_leader(lambda _: None)
            with core.open_db(path, wait=0, purpose="alias.independent.writer"):
                pass
            if interrupt:
                raise InterruptedError("test cancellation")
        return real_yield(value)

    monkeypatch.setattr(projection, "archive_yield", yielding)
    stages = []
    if interrupt:
        with pytest.raises(InterruptedError):
            projection.reconcile_provider_aliases(path, cfg, "personal", stages.append)
    else:
        assert projection.reconcile_provider_aliases(path, cfg, "personal", stages.append) == {"changed": 1, "settled": 0, "blocked": {}}
        assert stages[-1] == "provider aliases 1/1"
    assert stages[0] == "provider aliases 0/1"
    assert called and core.drain_hooks(block=True) == 1
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM messages WHERE content='arrived during alias reconciliation'").fetchone()[0] == 1
    core._sync_leader(lambda _: None)
