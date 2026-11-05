"""The own-row retry must let a sync settle: retained fork bodies never retire, so re-applying them each pass advanced the archive generation and kept the watcher running full passes."""
import json, duckdb, pytest
from ai_convos import cli as core
from ai_convos_remote import projection
from ai_convos_remote.projection import apply_row_replicas, connect, retry_own_replicas
from ai_convos_remote.protocol import digest, row_proof
from tests.test_source_device_cutover import signed_graph


def forked(tmp_path):
    # Seen from the peer: the author's two fork tips with the same content, both bodies retained.
    user,device,peer,control,bodies=signed_graph()
    msg=bodies[1]
    changed={**msg['row'],'data':{**msg['row']['data'],'content':'changed'}}
    side=dict(row=changed,proof=row_proof(device,user,'w',1,changed,msg['proof']['revision']))
    fork=dict(row=changed,proof=row_proof(device,user,'w',1,changed))
    path=tmp_path/'peer.db'
    for batch in (bodies,[side],[dict(row=msg['row'],proof=row_proof(device,user,'w',1,msg['row'],side['proof']['revision']))],[fork],[dict(row=msg['row'],proof=row_proof(device,user,'w',1,msg['row'],fork['proof']['revision']))]): apply_row_replicas(path,batch,'w',[control],local_user=user,local_device=peer['id'])
    return path,user,device,peer,control,changed


def test_retry_does_not_rewrite_an_applied_sibling_fork(tmp_path):
    path,user,device,peer,control,changed=forked(tmp_path)
    state=lambda:duckdb.connect(str(path),read_only=True).execute('SELECT generation,(SELECT count(*) FROM remote.row_conflicts),(SELECT content FROM messages) FROM archive_state').fetchone()
    before=state()
    assert before[1:]==(2,'done')  # both fork tips stay retained
    for _ in range(2): retry_own_replicas(path,connect(tmp_path/'state.db'),user,peer['id'])
    assert state()==before  # b32: generation +1 per pass, so the next sync was never settled

@pytest.mark.parametrize('change,again',[(None,0),('unrelated',0),('entity',1),('base',1),('proof',2)])
def test_retry_waits_for_a_changed_input(tmp_path,monkeypatch,change,again):
    path,user,device,peer,control,changed=forked(tmp_path)
    state,real,retried=connect(tmp_path/'state.db'),apply_row_replicas,[]
    monkeypatch.setattr(projection,'apply_row_replicas',lambda *a,**k:(retried.extend(k['retry']),real(*a,**k))[-1])
    retry=lambda:(retried.clear(),retry_own_replicas(path,state,user,peer['id']),len(retried))[-1]
    assert retry()==1
    with duckdb.connect(str(path)) as db:
        if change in ('unrelated','entity'): core._archive_touch(db,[('messages','other' if change=='unrelated' else 'm')])  # hook ingests touch other rows constantly
        if change=='base': db.execute("INSERT INTO remote.local_row_bases VALUES ('messages','m',?,'local')",[user])
    if change=='proof': real(path,[dict(proof=row_proof(device,user,'w',1,third:={**changed,'data':{**changed['data'],'content':'third'}}),row=third)],'w',[control],local_user=user,local_device=peer['id'])
    assert retry()==again  # b34: both settled tips re-applied on every pass, ~17 s on a real archive
    assert retry()==0

@pytest.mark.parametrize('change,query',[(None,0),('unrelated',0),('entity',1),('base',1),('proof',1),('state',1),('conflict',1)])
def test_settled_retry_skips_its_query_until_an_input_changes(tmp_path,monkeypatch,change,query):
    path,user,device,peer,control,changed=forked(tmp_path)
    state,real_open,ran=connect(tmp_path/'state.db'),projection.open_db,[]
    class Spy:
        def __init__(self,db): self.db=db
        def execute(self,sql,*a): return (ran.append(sql) if sql.startswith('WITH r AS') else None,self.db.execute(sql,*a))[1]
        def __getattr__(self,name): return getattr(self.db,name)
    monkeypatch.setattr(projection,'open_db',lambda *a,**k:Spy(real_open(*a,**k)))
    retry=lambda:(ran.clear(),retry_own_replicas(path,state,user,peer['id']),len(ran))[-1]
    assert [retry(),retry(),retry()]==[1,1,0]  # applies, finds nothing left, then skips the ~1.4 s query of a real archive
    with duckdb.connect(str(path)) as db:
        if change in ('unrelated','entity'): core._archive_touch(db,[('messages','other' if change=='unrelated' else 'm')])  # hook ingests touch unrelated rows every turn
        if change=='base': db.execute("INSERT INTO remote.local_row_bases VALUES ('messages','m',?,'local')",[user])
        if change=='state': db.execute("UPDATE remote.row_proofs SET state='retired' WHERE id=(SELECT min(proof_id) FROM remote.row_conflicts)")
        if change=='conflict': db.execute("DELETE FROM remote.row_conflicts WHERE proof_id=(SELECT min(proof_id) FROM remote.row_conflicts)")
    if change=='proof': projection.apply_row_replicas(path,[dict(proof=row_proof(device,user,'w',1,third:={**changed,'data':{**changed['data'],'content':'third'}}),row=third)],'w',[control],local_user=user,local_device=peer['id'])
    assert retry()==query

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
    retry_own_replicas(path,connect(tmp_path/'state.db'),user,device['id'])
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT revision FROM remote.local_row_bases WHERE kind='messages' AND entity='m'").fetchone()[0]==c['proof']['revision']
        assert db.execute("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?",[digest(c['proof'])]).fetchone()[0]==1


def test_quiet_retry_reports_a_repaired_attachment_body_once(tmp_path):
    # The attachment row is unchanged; only its body association differs, which scans read as the attachment's body_hash.
    user,device,peer,control,bodies=signed_graph()
    attachment=core.logical_row('attachments',[*core.ARCHIVE_COLUMNS['attachments'],'body_hash'],['a','m','a.txt',None,3,None,None,None,'h'])
    signed=[*bodies,dict(row=attachment,proof=row_proof(device,user,'w',1,attachment))]
    path=tmp_path/'peer.db'
    apply_row_replicas(path,signed,'w',[control],local_user=user,local_device=peer['id'])
    with duckdb.connect(str(path)) as db: (db.execute("INSERT OR IGNORE INTO remote.row_conflicts VALUES (?,?)",[digest(signed[-1]['proof']),json.dumps(attachment,sort_keys=True,separators=(',',':'))]),db.execute("UPDATE attachment_bodies SET content_hash='stale'"))
    retry=lambda:apply_row_replicas(path,[],None,[],local_user=user,local_device=peer['id'],ready=False,retry=[(digest(signed[-1]['proof']),'')])
    state=lambda:duckdb.connect(str(path),read_only=True).execute('SELECT generation,(SELECT content_hash FROM attachment_bodies) FROM archive_state').fetchone()
    start=state()[0]
    retry()
    assert state()==(start+1,'h')  # c2c52c4: repaired silently, generation unchanged
    retry()
    assert state()==(start+1,'h')
