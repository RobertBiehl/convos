"""Maintenance queues capture without discarding input or monopolizing DuckDB."""
import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.projection import apply_row_replicas, audit_rows
from tests.test_hooks import hooks, transcript
from tests.test_remote_projection import signed_edit_graph


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
