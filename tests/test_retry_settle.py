"""The own-row retry must let a sync settle: retained fork bodies never retire, so re-applying them each pass kept the watcher running full passes."""
import duckdb
import ai_convos_remote as remote_client
from ai_convos_remote import connect, load, setup_client, sync_once, workspace
from ai_convos_remote.projection import apply_row_replicas, retry_own_replicas
from ai_convos_remote.protocol import row_proof
from ai_convos_remote_server import connect as server_connect
from tests.test_remote_client import inject, no_approval_delay, transport, write_archive
from tests.test_source_device_cutover import signed_graph


def test_retry_does_not_rewrite_an_applied_sibling_fork(tmp_path):
    user,device,peer,control,bodies=signed_graph()
    msg=bodies[1]
    changed={**msg['row'],'data':{**msg['row']['data'],'content':'changed'}}
    side=dict(row=changed,proof=row_proof(device,user,'w',1,changed,msg['proof']['revision']))
    fork=dict(row=changed,proof=row_proof(device,user,'w',1,changed))
    path=tmp_path/'peer.db'
    for batch in (bodies,[side],[dict(row=msg['row'],proof=row_proof(device,user,'w',1,msg['row'],side['proof']['revision']))],[fork],[dict(row=msg['row'],proof=row_proof(device,user,'w',1,msg['row'],fork['proof']['revision']))]): apply_row_replicas(path,batch,'w',[control],local_user=user,local_device=peer['id'])
    state=lambda:duckdb.connect(str(path),read_only=True).execute('SELECT generation,(SELECT count(*) FROM remote.row_conflicts),(SELECT content FROM messages) FROM archive_state').fetchone()
    before=state()
    assert before[1:]==(2,'done')  # both fork tips stay retained
    for _ in range(2): retry_own_replicas(path,user,peer['id'])
    assert state()==before  # b32: generation +1 per pass, so the next sync was never settled


def test_retry_runs_only_after_the_archive_changed(tmp_path,monkeypatch):
    server=server_connect(tmp_path/'server.db'); monkeypatch.setattr(remote_client,'request',transport(server)); monkeypatch.setattr(remote_client,'drain_hooks',lambda:None)
    calls,retry=[],remote_client.retry_own_replicas
    monkeypatch.setattr(remote_client,'retry_own_replicas',lambda *a:(calls.append(1),retry(*a))[1])
    root=tmp_path/'client'; setup_client('http://server','alice',root=root); write_archive(root/'data/convos.db','one'); sync_once(root,True)
    calls.clear(); state=connect(root/'remote/state.db'); inject(load(root),state,server,workspace(load(root),'Personal'),'workspace.membership'); state.close(); sync_once(root)
    assert calls==[]  # b32: every full pass retried every retained row
    write_archive(root/'data/convos.db','two'); sync_once(root)
    assert calls==[1]
