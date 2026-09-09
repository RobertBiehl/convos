"""Doctor reports exact physical gaps without importing, repairing, or needing Remote."""
import json

from typer.testing import CliRunner

from ai_convos import cli as core
from tests.test_hooks import hooks


def test_doctor_reports_all_orphan_kinds_and_preserves_archive_and_queue(hooks, monkeypatch):
    _, data = hooks
    monkeypatch.setattr(core, 'entry_points', lambda **kwargs: [])
    monkeypatch.setattr(core, 'safari_cookie_domains', lambda: [])
    monkeypatch.setattr(core, 'chrome_cookie_domains', lambda: [])
    monkeypatch.setattr(core, 'drain_hooks', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('doctor must not drain capture')))
    path = data / 'convos.db'
    with core.open_db(path, purpose='fixture.doctor') as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source) VALUES ('c','codex')")
        db.execute("INSERT INTO messages(id,conversation_id,role,metadata,parent_id) VALUES ('unmarked','absent','assistant','{}',NULL),('history','absent','assistant','{\"history_of\":\"unmarked\"}',NULL),('reply','c','assistant','{}','missing-message')")
        db.execute("INSERT INTO tool_calls(id,message_id,tool_name) VALUES ('t','missing-message','read')")
        db.execute("INSERT INTO file_edits(id,message_id,file_path,edit_type) VALUES ('e','missing-message','example.txt','write')")
        db.execute("INSERT INTO attachments(id,message_id,filename) VALUES ('a','missing-message','example.txt')")
        db.execute("INSERT INTO artifacts(id,conversation_id,artifact_type) VALUES ('x','absent','text')")
    core.HOOK_DIR.mkdir(parents=True, exist_ok=True)
    queue = core.HOOK_DIR / 'queued.json'
    payload = json.dumps({'source': 'codex', 'synthetic': True})
    queue.write_text(payload)
    before = core._file_sha256(path)
    result = CliRunner().invoke(core.app, ['doctor'])
    assert result.exit_code == 0, result.output
    assert 'missing physical parents:' in result.output
    assert 'messages.conversation_id: rows=2, parent_ids=1, marked_history_rows=1' in result.output
    assert 'messages.parent_id: rows=1, parent_ids=1, marked_history_rows=0' in result.output
    for key in ('tool_calls.message_id', 'file_edits.message_id', 'attachments.message_id', 'artifacts.conversation_id'):
        assert f'{key}: rows=1, parent_ids=1' in result.output
    assert 'ingest: pending=1' in result.output
    assert core._file_sha256(path) == before and queue.read_text() == payload


def test_doctor_prints_zero_counts_for_healthy_relationships(hooks, monkeypatch):
    _, data = hooks
    monkeypatch.setattr(core, 'entry_points', lambda **kwargs: [])
    monkeypatch.setattr(core, 'safari_cookie_domains', lambda: [])
    monkeypatch.setattr(core, 'chrome_cookie_domains', lambda: [])
    with core.open_db(data / 'convos.db', purpose='fixture.empty') as db:
        core.init_schema(db)
    result = CliRunner().invoke(core.app, ['doctor'])
    assert result.exit_code == 0
    assert result.output.count('rows=0, parent_ids=0') == 6


def test_doctor_does_not_report_zero_orphans_for_incomplete_schema(hooks, monkeypatch):
    _, data = hooks
    monkeypatch.setattr(core, 'entry_points', lambda **kwargs: [])
    monkeypatch.setattr(core, 'safari_cookie_domains', lambda: [])
    monkeypatch.setattr(core, 'chrome_cookie_domains', lambda: [])
    with core.open_db(data / 'convos.db', purpose='fixture.incomplete') as db:
        db.execute('CREATE TABLE messages(id VARCHAR,content VARCHAR)')
    result = CliRunner().invoke(core.app, ['doctor'])
    assert result.exit_code == 0
    assert 'missing physical parents: unavailable (schema incomplete)' in result.output
    assert 'rows=0, parent_ids=0' not in result.output
