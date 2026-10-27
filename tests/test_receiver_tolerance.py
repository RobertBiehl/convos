"""Receiver tolerance: same-author edit claims move or are ignored, never downgrade, and park instead of blocking a batch."""
import duckdb
import pytest
import typer

from ai_convos import cli as core
from ai_convos_remote.projection import apply_row_replicas, audit_rows
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


def test_duplicate_classified_claims_park_both_parents_and_the_rest_of_the_batch_applies(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    conv, msg, tool = bodies
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    other = core.logical_row('tool_calls', core.ARCHIVE_COLUMNS['tool_calls'], ['t2', 'm', 'write', '{}', '{}', 'complete', None, None])
    other['data']['edits'] = tool['row']['data']['edits']
    second = dict(row=other, proof=row_proof(device, user, 'w', 1, other))
    assert apply([*bodies, second]) == [True, True, True, True]  # b25: ValueError('Derived edit ownership conflict')
    assert parked(path) == [('tool_calls', 't', 'Derived edit claimed twice'), ('tool_calls', 't2', 'Derived edit claimed twice')]
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT id FROM messages").fetchall() == [('m',)] and not db.execute("SELECT id FROM tool_calls").fetchall() and not edits(path)
        assert db.execute("SELECT count(*) FROM remote.row_proofs WHERE row_kind='tool_calls'").fetchone() == (2,)
    assert retained(path, 't') == 1 and retained(path, 't2') == 1
    assert apply([*bodies, second]) == [False, False, False, False] and len(parked(path)) == 2
    released = {**tool['row'], 'data': {**tool['row']['data'], 'edits': []}}
    assert apply([dict(row=released, proof=row_proof(device, user, 'w', 1, released, tool['proof']['revision']))]) == [True]
    assert parked(path) == [('tool_calls', 't2', 'Derived edit claimed twice')]
    assert apply([second]) == [False] and not parked(path) and edits(path) == [('e', 'confirmed', 't2', 'tool_calls', 't2')]
    assert audit_rows(path, local_user=user)['totals']['projection_mismatch'] == 0


def test_received_parent_naming_a_native_edit_parks_with_an_ownership_reason(tmp_path):
    user, device, peer, control, bodies = signed_graph()
    path = tmp_path / 'peer.db'
    apply = lambda values: apply_row_replicas(path, values, 'w', [control], local_user=user, local_device=peer['id'])
    apply(bodies[:2])
    with duckdb.connect(str(path)) as db: db.execute("INSERT INTO file_edits VALUES ('e','m','a.py','write','mine',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','provider_success',NULL)")
    assert apply(bodies[2:]) == [True]  # b25: ValueError('Derived edit ownership conflict')
    assert parked(path) == [('tool_calls', 't', 'Derived edit ownership conflict')]
    assert edits(path) == [('e', 'confirmed', None, None, None)]
    with duckdb.connect(str(path), read_only=True) as db: assert not db.execute("SELECT id FROM tool_calls").fetchall() and db.execute("SELECT content FROM file_edits").fetchall() == [('mine',)]


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
        db.execute("INSERT INTO remote.parked_rows VALUES ('p1','tool_calls','t','u','Derived edit claimed twice')")
    remote_client.sync_cmd()
    assert "parked_rows=1 (Derived edit claimed twice: 1)" in remote_client.recovery_notice()
    with duckdb.connect(str(tmp_path / "data/convos.db")) as db: db.execute("INSERT INTO remote.parked_rows VALUES ('p2','conversations','c','u','Source device ownership conflict; replica cannot revise this conversation')")
    with pytest.raises(typer.Exit) as failure: remote_client.sync_cmd()
    assert failure.value.exit_code == 1 and "parked_rows=2" in remote_client.recovery_notice()
