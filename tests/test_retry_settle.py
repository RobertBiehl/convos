"""The own-row retry must let a sync settle: retained fork bodies never retire, so re-applying them each pass advanced the archive generation and kept the watcher running full passes."""
import duckdb
from ai_convos_remote.projection import apply_row_replicas, retry_own_replicas
from ai_convos_remote.protocol import row_proof
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



import json, pytest
from ai_convos_remote.protocol import digest

@pytest.mark.parametrize('base',['A','B'])
def test_retry_advances_a_stale_native_base_to_the_retained_head(tmp_path,base):
    # A(done) -> B(changed) -> C(done); the archive already holds C. Base A: C has the base's content but is its successor. Base B: the archive matches C, but the base must still advance before C's body may retire.
    user,device,peer,control,bodies=signed_graph()
    msg=bodies[1]
    changed={**msg['row'],'data':{**msg['row']['data'],'content':'changed'}}
    b=dict(row=changed,proof=row_proof(device,user,'w',1,changed,msg['proof']['revision']))
    c=dict(row=msg['row'],proof=row_proof(device,user,'w',1,msg['row'],b['proof']['revision']))
    path=tmp_path/'own.db'
    for batch in (bodies,[b],[c]): apply_row_replicas(path,batch,'w',[control],local_user=user,local_device=device['id'])
    stale={'A':msg,'B':b}[base]['proof']['revision']
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE remote.local_row_bases SET revision=? WHERE kind='messages' AND entity='m'",[stale])
        db.execute("INSERT OR IGNORE INTO remote.row_conflicts VALUES (?,?)",[digest(c['proof']),json.dumps(c['row'],sort_keys=True,separators=(',',':'))])
    retry_own_replicas(path,user,device['id'])
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT revision FROM remote.local_row_bases WHERE kind='messages' AND entity='m'").fetchone()[0]==c['proof']['revision']
        assert db.execute("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?",[digest(c['proof'])]).fetchone()[0]==1
