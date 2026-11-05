"""Only fresh source observations may establish local provenance ownership."""
from datetime import datetime

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import apply_row_replicas, attest_rows, audit_rows, clean, connect, row_replicas, scan
from ai_convos_remote.protocol import certificate, digest, identity, open_replica, public, public_id, row_proof
from tests.test_provenance import core as archive, git, repo


def people():
    roots,devices=[identity(name+'-root') for name in ('local','foreign')],[identity(name+'-device') for name in ('local','foreign')]
    users=[public_id(root['sign_public']) for root in roots]
    control={'workspace':'w','revision':1,'epoch':1,'devices':{device['id']:{'user':user,'root_public':root['sign_public'],'device':public(device),'certificate':certificate(root,user,device),'history':True} for root,device,user in zip(roots,devices,users)}}
    cfg={'user':users[0],'device':devices[0],'workspaces':{'w':{'kind':'personal','epoch':1}},'controls':{'w':control},'server_state':{'workspaces':[{'id':'w','controls':[control]}]}}
    return users,devices,control,cfg


def fixture(tmp_path):
    checkout=repo(tmp_path/'repo'); git(checkout,'remote','add','origin','https://example.test/local/repo.git')
    path=tmp_path/'archive.db'
    with archive(path,checkout,[(checkout/'x.py','write','one\n',None)]): pass
    native=core.repository(checkout)
    fid=digest({'repository':native['id'],'path':'x.py'}); content=digest(b'one\n'); vid=digest({'file':fid,'content':content}); checkpoint=core._checkpoint(native,'sync')
    rows=[dict(v=1,kind='repository.observed',id=native['id'],state='active',data=dict(lineage=native['lineage'],roots=[],remotes=['https://example.test/foreign/repo.git'])),
          dict(v=1,kind='file.observed',id=fid,state='active',data=dict(repository=native['id'],path='x.py',kind='external')),
          dict(v=1,kind='file.version',id=vid,state='active',data=dict(file=fid,content_hash=content,observed_at='2000-01-01T00:00:00.000000')),
          dict(v=1,kind='git.checkpoint',id=checkpoint['id'],state='active',data={k:v for k,v in checkpoint.items() if k!='id'}|dict(paths=['foreign.txt'],capture_source='foreign',observed_at='2000-01-01T00:00:00.000000'))]
    return path,checkout,rows


def scanned(path,state):
    with duckdb.connect(str(path),read_only=True) as db,connect(state) as graph: return scan(db,graph)


@pytest.mark.parametrize('native_first',[False,True])
def test_shared_facts_publish_only_native_observations_in_both_arrival_orders(tmp_path,native_first):
    path,checkout,foreign=fixture(tmp_path); users,devices,control,cfg=people(); state=tmp_path/'state.db'
    bodies=[dict(row=row,proof=row_proof(devices[1],users[1],'w',1,row)) for row in foreign]
    if native_first: core.capture_provenance(path)
    before={(r['kind'],r['entity']):core.logical_fact(r) for r in scanned(path,state) if r['kind'] in core.PROVENANCE_KINDS}
    apply_row_replicas(path,bodies,'w',[control],local_user=users[0])
    observed=core.capture_provenance(path) if not native_first else []
    records=scanned(path,state); local={(r['kind'],r['entity']):core.logical_fact(r) for r in records if r['kind'] in core.PROVENANCE_KINDS}
    expected={(r['kind'],r['entity']):clean(r) for r in observed}
    for row in foreign:
        key=row['kind'],row['id']; value=local[key]
        assert value!=row
        if native_first: assert value==before[key]
        elif row['kind'] in ('file.version','git.checkpoint'):
            assert datetime.fromisoformat(value['data']['observed_at'])==datetime.fromisoformat(expected[key]['observed_at'].replace('Z','+00:00')).replace(tzinfo=None)
    assert local[('repository.observed',foreign[0]['id'])]['data']['remotes']==['https://example.test/local/repo']
    assert local[('file.observed',foreign[1]['id'])]['data']['kind']=='repository'
    assert local[('git.checkpoint',foreign[3]['id'])]['data']['capture_source']=='sync'
    assert local[('git.checkpoint',foreign[3]['id'])]['data']['paths']==[]
    assert attest_rows(path,cfg,'w',records)>0
    exported={digest(body['proof']):body for env in row_replicas(path,cfg,'w',records,{1:bytes(32)}) for body in [open_replica(env,bytes(32))]}
    assert all(exported[digest(body['proof'])]['row']==body['row'] for body in bodies)
    assert {body['row']['kind']:body['row'] for body in exported.values() if body['proof']['author_user_id']==users[0] and body['row']['kind'] in core.PROVENANCE_KINDS}=={kind:row for (kind,entity),row in local.items()}
    assert audit_rows(path,local_user=users[0])['totals']['unavailable']==0


@pytest.mark.parametrize('native_first',[False,True])
def test_repository_capture_owns_fresh_git_body_and_preserves_received_proof(tmp_path,native_first):
    path,checkout,foreign=fixture(tmp_path); users,devices,control,cfg=people(); row=foreign[0]; proof=row_proof(devices[1],users[1],'w',1,row)
    if native_first: core.capture_repository(checkout,path)
    apply_row_replicas(path,[dict(row=row,proof=proof)],'w',[control],local_user=users[0])
    if not native_first: core.capture_repository(checkout,path)
    records=scanned(path,tmp_path/'state.db'); native=next(core.logical_fact(r) for r in records if r['kind']=='repository.observed')
    assert native['data']['remotes']==['https://example.test/local/repo']
    attest_rows(path,cfg,'w',records)
    exported=[open_replica(env,bytes(32)) for env in row_replicas(path,cfg,'w',records,{1:bytes(32)})]
    assert any(body['proof']==proof and body['row']==row for body in exported)
    assert any(body['proof']['author_user_id']==users[0] and body['row']==native for body in exported)


def test_self_author_receive_requires_source_capture_before_ownership(tmp_path):
    path,checkout,rows=fixture(tmp_path); users,devices,control,cfg=people(); checkpoint=rows[-1]['id']
    rows.append(dict(v=1,kind='checkpoint.link',id=digest(dict(checkpoint=checkpoint,edit='e0')),state='active',data=dict(checkpoint=checkpoint,edit='e0',evidence='foreign')))
    bodies=[dict(row=row,proof=row_proof(devices[0],users[0],'w',1,row)) for row in rows]
    apply_row_replicas(path,bodies,'w',[control],local_user=users[0])
    with duckdb.connect(str(path),read_only=True) as db: assert not db.execute('SELECT * FROM provenance.local_facts').fetchall()
    assert not [r for r in scanned(path,tmp_path/'state.db') if r['kind'] in core.PROVENANCE_KINDS]
    core.capture_provenance(path)
    records=scanned(path,tmp_path/'state.db'); link=next(r for r in records if r['kind']=='checkpoint.link')
    assert link['payload']['evidence']=='full_content_match'
    exported={digest(body['proof']):body['row'] for env in row_replicas(path,cfg,'w',[],{1:bytes(32)}) for body in [open_replica(env,bytes(32))]}
    assert exported=={digest(body['proof']):body['row'] for body in bodies}
    assert audit_rows(path,local_user=users[0])['totals']['unavailable']==0


@pytest.mark.parametrize('earlier_first',[True,False])
def test_one_accounts_devices_converge_on_the_earliest_observation(tmp_path,earlier_first):
    # Both devices of one account observed the same facts; keeping each its own observation re-signed them on every sync, in turn, forever.
    root,devices=identity('root'),[identity(f'device-{i}') for i in (0,1)]
    user=public_id(root['sign_public'])
    control={'workspace':'w','revision':1,'epoch':1,'devices':{d['id']:{'user':user,'root_public':root['sign_public'],'device':public(d),'certificate':certificate(root,user,d),'history':True} for d in devices}}
    path,checkout,foreign=fixture(tmp_path); paths=[path,tmp_path/'other.db']; paths[1].write_bytes(path.read_bytes())
    [core.capture_provenance(p) for p in paths]
    kinds=('file.version','git.checkpoint'); facts=[(r['kind'],r['id']) for r in foreign if r['kind'] in kinds]
    def device(i): return dict(path=paths[i],state=tmp_path/f'state{i}.db',cfg={'user':user,'device':devices[i],'workspaces':{'w':{'kind':'personal','epoch':1}},'controls':{'w':control},'server_state':{'workspaces':[{'id':'w','controls':[control]}]}})
    def observed(d):
        with duckdb.connect(str(d['path']),read_only=True) as db: return [db.execute(f"SELECT observed_at FROM provenance.{core._FACT_TABLES[k][0]} WHERE id=?",[e]).fetchone()[0] for k,e in facts]
    def sync(sender,receiver):
        records=scanned(sender['path'],sender['state']); attest_rows(sender['path'],sender['cfg'],'w',records)
        bodies=[body for env in row_replicas(sender['path'],sender['cfg'],'w',records,{1:bytes(32)}) for body in [open_replica(env,bytes(32))] if body['row']['kind'] in kinds]
        apply_row_replicas(receiver['path'],bodies,'w',[control],local_user=user,local_device=receiver['cfg']['device']['id'])
    def signed(d):
        with duckdb.connect(str(d['path']),read_only=True) as db: return db.execute(f"SELECT count(*) FROM remote.row_proofs WHERE row_kind IN ('file.version','git.checkpoint')").fetchone()[0]
    a,b=device(0),device(1); earliest=min(observed(a)[0],observed(b)[0])
    first,second=(a,b) if earlier_first==(observed(a)[0]<observed(b)[0]) else (b,a)
    sync(first,second); sync(second,first); sync(first,second)
    proofs=signed(a),signed(b)
    sync(second,first); sync(first,second)
    assert (signed(a),signed(b))==proofs
    assert observed(a)==observed(b) and observed(a)[0]==earliest
    core.capture_provenance(paths[0],source='sync'); core.capture_provenance(paths[1],source='sync')
    assert observed(a)==observed(b) and observed(a)[0]==earliest
