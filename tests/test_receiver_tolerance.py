"""Receiver tolerance: same-author edit claims move or are ignored, never downgrade, and park instead of blocking a batch."""
import json

import duckdb
import pytest
import typer

from ai_convos import cli as core
import ai_convos_remote.projection as projection
from ai_convos_remote.projection import apply_row_replicas, audit_rows, retry_own_replicas
from ai_convos_remote.protocol import row_proof
from tests.test_source_device_cutover import signed_graph


def torn(bodies, device, user):
    """B lists the tool call's edit as unknown/unclassified_input (torn producer snapshot); A' returns to the first content."""
    conv, msg, tool = bodies
    edit = tool['row']['data']['edits'][0]
    b_row = {**msg['row'], 'data': {**msg['row']['data'], 'edits': [{**edit, 'status': 'unknown', 'reason': 'unclassified_input'}]}}
    b = dict(row=b_row, proof=row_proof(device, user, 'w', 1, b_row, msg['proof']['revision']))
    return b, dict(row=msg['row'], proof=row_proof(device, user, 'w', 1, msg['row'], b['proof']['revision']))


def edits(path):
    with duckdb.connect(str(path), read_only=True) as db:
        return db.execute("SELECT e.id,v.status,v.tool_call_id,d.parent_kind,d.parent_id FROM file_edits e LEFT JOIN provenance.file_edit_evidence v ON v.file_edit_id=e.id LEFT JOIN remote.derived_edits d ON d.physical_id=e.id ORDER BY e.id").fetchall()


def parked(path):
    with duckdb.connect(str(path), read_only=True) as db:
        return db.execute("SELECT row_kind,source_row_id,reason FROM remote.parked_rows ORDER BY 1,2").fetchall()


def retained(path, source_row_id):
    with duckdb.connect(str(path), read_only=True) as db:
        return db.execute("SELECT count(*) FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.source_row_id=?", [source_row_id]).fetchone()[0]


def test_torn_message_revision_in_its_own_batch_keeps_edit_under_tool_call(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    b, a2 = torn(bodies, device, user)
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies)
    assert apply([b]) == [True]  # b25: ValueError('Derived edit ownership conflict') and the cursor never moves
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')] and not parked(path)
    assert audit_rows(path, local_user=user)['tables']['messages']['projection_mismatch'] == 1 and retained(path, 'm') == 1  # B is retained, not projected with its downgraded claim
    assert apply([b]) == [False] and edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')]
    assert apply([a2]) == [True]
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')] and not parked(path)
    assert audit_rows(path, local_user=user)['totals']['projection_mismatch'] == 0 and retained(path, 'm') == 0


def test_torn_revision_and_tool_call_in_one_batch_keep_edit_under_tool_call(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    conv, msg, tool = bodies
    b, a2 = torn(bodies, device, user)
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    assert apply([conv, b, tool]) == [True, True, True]  # 3c162af: t parked 'Derived edit claimed twice' and e projected unknown under m
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')] and not parked(path)
    assert apply([a2]) == [True]
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')] and not parked(path)  # 3c162af: [] (stale-deleted by A')
    assert audit_rows(path, local_user=user)['totals']['projection_mismatch'] == 0


def test_hand_moved_edit_is_not_downgraded_and_survives_closure(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    b, a2 = torn(bodies, device, user)
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies)
    with duckdb.connect(str(path)) as db: db.execute("UPDATE remote.derived_edits SET parent_kind='messages',parent_id='m'")  # the 2026-09-22 hand move
    assert apply([b]) == [True]
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')]  # b25: ('e','unknown',None,'messages','m')
    assert apply([a2]) == [True]
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')] and not parked(path)  # b25: [] (deleted as stale)
    assert audit_rows(path, local_user=user)['totals']['projection_mismatch'] == 0


@pytest.mark.parametrize('order', [(0, 1), (1, 0)])
def test_same_author_move_between_tool_calls_in_one_batch_keeps_captured_file(tmp_path, order):
    user, device, peer, control, bodies = signed_graph()
    conv, msg, tool = bodies
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies)
    with duckdb.connect(str(path)) as db: db.execute("INSERT INTO provenance.files VALUES ('f',NULL,'a.py','external'); INSERT INTO provenance.file_edit_files VALUES ('e','f',NULL,NULL,'captured')")
    moved = {**tool['row'], 'data': {**tool['row']['data'], 'edits': []}}
    other = core.logical_row('tool_calls', core.ARCHIVE_COLUMNS['tool_calls'], ['t2', 'm', 'write', '{}', '{}', 'complete', None, None])
    other['data']['edits'] = tool['row']['data']['edits']
    batch = [dict(row=moved, proof=row_proof(device, user, 'w', 1, moved, tool['proof']['revision'])), dict(row=other, proof=row_proof(device, user, 'w', 1, other))]
    assert apply([batch[i] for i in order]) == [True, True]  # b25: ValueError('Derived edit ownership conflict')
    assert edits(path) == [('e', 'confirmed', 't2', 'tool_calls', 't2')] and not parked(path)
    with duckdb.connect(str(path), read_only=True) as db: assert db.execute("SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id='e'").fetchall() == [('f',)]
    assert audit_rows(path, local_user=user)['totals']['projection_mismatch'] == 0


def test_duplicate_classified_claims_bind_to_one_parent_and_converge(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    conv, msg, tool = bodies
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    other = core.logical_row('tool_calls', core.ARCHIVE_COLUMNS['tool_calls'], ['t2', 'm', 'write', '{}', '{}', 'complete', None, None])
    other['data']['edits'] = tool['row']['data']['edits']
    second = dict(row=other, proof=row_proof(device, user, 'w', 1, other))
    winner = max((tool, second), key=lambda body: core.provenance_digest(body['proof']))['row']['id']
    assert apply([*bodies, second]) == [True, True, True, True]  # b25: ValueError('Derived edit ownership conflict'); 3c162af: both tool calls parked and re-parked on every retry
    loser = ({'t', 't2'} - {winner}).pop()
    assert not parked(path) and edits(path) == [('e', 'confirmed', winner, 'tool_calls', winner)]
    with duckdb.connect(str(path), read_only=True) as db: assert db.execute("SELECT id FROM tool_calls ORDER BY id").fetchall() == [('t',), ('t2',)]
    assert retained(path, winner) == 0 and retained(path, loser) == 1  # the losing parent still lists the edit: retained as a variant
    assert audit_rows(path, local_user=user)['tables']['tool_calls'] | dict(projection_mismatch=1, retained_variants=1, unavailable=0) == audit_rows(path, local_user=user)['tables']['tool_calls']
    retry_own_replicas(path, user, peer['id'])  # the author's own retained variant re-applies alone: a same-author move (R1), then nothing is left to retry
    assert edits(path) == [('e', 'confirmed', loser, 'tool_calls', loser)] and retained(path, loser) == 0 and not parked(path)
    retry_own_replicas(path, user, peer['id'])
    assert apply([*bodies, second]) == [False, False, False, False]  # re-receiving the batch keeps the current holder
    assert edits(path) == [('e', 'confirmed', loser, 'tool_calls', loser)] and not parked(path)


def test_received_parent_naming_a_native_edit_parks_with_an_ownership_reason(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies[:2])
    with duckdb.connect(str(path)) as db: db.execute("INSERT INTO file_edits VALUES ('e','m','a.py','write','mine',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','provider_success',NULL)")
    assert apply(bodies[2:]) == [False]  # b25: ValueError('Derived edit ownership conflict'); 3c162af: [True] for a row that was not applied
    assert parked(path) == [('tool_calls', 't', 'Derived edit ownership conflict')]
    assert edits(path) == [('e', 'confirmed', None, None, None)]
    with duckdb.connect(str(path), read_only=True) as db:
        assert not db.execute("SELECT id FROM tool_calls").fetchall() and db.execute("SELECT content FROM file_edits").fetchall() == [('mine',)]
        assert db.execute("SELECT count(*) FROM remote.row_proofs WHERE source_row_id='t'").fetchone() == (0,)  # 3c162af: proof stored, body retained in row_conflicts, which a rolled-back b25 retries and fails on
    assert retained(path, 't') == 0
    retry_own_replicas(path, user, peer['id'])
    assert parked(path) == [('tool_calls', 't', 'Derived edit ownership conflict')] and edits(path) == [('e', 'confirmed', None, None, None)]


def test_ownership_park_survives_the_owners_next_revision(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device='third')
    apply(bodies)
    row = {**bodies[-1]['row'], 'data': {**bodies[-1]['row']['data'], 'edits': []}}
    assert apply([dict(row=row, proof=row_proof(peer, user, 'w', 1, row, bodies[-1]['proof']['revision']))]) == [False]
    assert parked(path) == [('tool_calls', 't', 'Source device ownership conflict; replica cannot revise this conversation')]
    row2 = {**bodies[-1]['row'], 'data': {**bodies[-1]['row']['data'], 'status': 'done'}}
    assert apply([dict(row=row2, proof=row_proof(device, user, 'w', 1, row2, bodies[-1]['proof']['revision']))]) == [True]
    assert parked(path) == [('tool_calls', 't', 'Source device ownership conflict; replica cannot revise this conversation')]  # 3c162af: cleared, sync back to exit 0
    with duckdb.connect(str(path), read_only=True) as db: assert not db.execute("SELECT 1 FROM remote.row_proofs WHERE author_device_id=?", [peer['id']]).fetchall()
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')]


def test_claim_conflicts_park_every_row_of_a_key_in_one_retry(tmp_path, monkeypatch):
    user, device, peer, control, bodies = signed_graph()
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=None)
    apply(bodies)
    rows = [core.logical_row('messages', core.ARCHIVE_COLUMNS['messages'], [f'p{i}', 'c', 'assistant', f'peer {i}', None, None, None, '{}', None]) for i in range(6)]
    calls, original = [], projection.claim_row_owners
    monkeypatch.setattr(projection, 'claim_row_owners', lambda db, records: (calls.append(len(records)), original(db, records))[1])
    assert apply([dict(row=row, proof=row_proof(peer, user, 'w', 1, row)) for row in rows]) == [False] * 6
    assert calls == [6, 0] and len(parked(path)) == 6  # 3c162af: one row per pass, 7 passes


def test_source_device_ownership_conflict_parks_without_storing_the_proof(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies)
    row = {**bodies[-1]['row'], 'data': {**bodies[-1]['row']['data'], 'edits': []}}
    assert apply([dict(row=row, proof=row_proof(peer, user, 'w', 1, row, bodies[-1]['proof']['revision']))]) == [False]  # b25: ValueError('Source device ownership conflict')
    assert parked(path) == [('tool_calls', 't', 'Source device ownership conflict; replica cannot revise this conversation')]
    with duckdb.connect(str(path), read_only=True) as db:
        assert not db.execute("SELECT 1 FROM remote.row_proofs WHERE author_device_id=?", [peer['id']]).fetchall()
        assert db.execute("SELECT device FROM remote.row_owners WHERE kind='tool_calls'").fetchall() == [(device['id'],)]
    assert edits(path) == [('e', 'confirmed', 't', 'tool_calls', 't')]


def test_sync_exits_non_zero_only_while_an_ownership_park_exists(tmp_path, monkeypatch):
    import ai_convos_remote as remote_client
    monkeypatch.setenv("CONVOS_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setattr(remote_client, "sync_once", lambda **kwargs: None)
    (tmp_path / "data").mkdir()
    with duckdb.connect(str(tmp_path / "data/convos.db")) as db:
        core.init_schema(db)
        db.execute("INSERT INTO remote.parked_rows VALUES ('p1','tool_calls','t','u','same-author inconsistency')")  # no current reason without 'ownership'; pins the contract
    remote_client.sync_cmd()
    remote_client.doctor_cmd()
    assert "parked_rows=1 (same-author inconsistency: 1)" in remote_client.recovery_notice()
    with duckdb.connect(str(tmp_path / "data/convos.db")) as db: db.execute("INSERT INTO remote.parked_rows VALUES ('p2','conversations','c','u','Source device ownership conflict; replica cannot revise this conversation')")
    with pytest.raises(typer.Exit) as failure: remote_client.sync_cmd()
    assert failure.value.exit_code == 1 and "parked_rows=2" in remote_client.recovery_notice()
    with pytest.raises(typer.Exit) as failure: remote_client.doctor_cmd()  # 3c162af: exit 0
    assert failure.value.exit_code == 1
    monkeypatch.setattr(remote_client.time, "sleep", lambda delay: (_ for _ in ()).throw(KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt): remote_client.watch(interval=0)
    base = remote_client.paths()[0]
    assert "ownership conflict" in (base / "last_error").read_text()  # 3c162af: watch reported success
    assert json.loads((base / "watch.json").read_text())["failures"] == 1
