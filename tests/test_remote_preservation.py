"""Exact signed bodies must survive projection order and shared observations."""
import json

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote.projection import apply_row_replicas, audit_rows, row_replicas
from ai_convos_remote.protocol import certificate, digest, identity, open_replica, public, public_id, row_proof
from tests.test_remote_projection import signed_edit_graph


def authors():
    pairs=[(identity(name+'-root'),identity(name+'-device')) for name in ('a','b')]
    people=[(public_id(root['sign_public']),device) for root,device in pairs]
    control={'workspace':'w','revision':1,'epoch':1,'devices':{device['id']:{'user':user,'root_public':root['sign_public'],'device':public(device),'certificate':certificate(root,user,device),'history':True} for (root,device),(user,_) in zip(pairs,people)}}
    return people,control


def recovered(path,device,user='receiver'):
    return {digest(body['proof']):body['row'] for env in row_replicas(path,{'user':user,'device':device},'w',[],{1:bytes(32)}) for body in [open_replica(env,bytes(32))]}


@pytest.mark.parametrize('batch',[False,True])
def test_independent_repository_observations_remain_exact(tmp_path,batch):
    people,control=authors()
    rows=[dict(v=1,kind='repository.observed',id='repo',state='active',data=dict(lineage='lineage',roots=[],remotes=[f'https://example.test/{i}/repo.git'])) for i in range(2)]
    bodies=[dict(row=row,proof=row_proof(device,user,'w',1,row)) for row,(user,device) in zip(rows,people)]
    path=tmp_path/'archive.db'
    for page in [bodies] if batch else [[body] for body in bodies]: assert apply_row_replicas(path,page,'w',[control],local_user='receiver')==[True]*len(page)
    assert recovered(path,people[0][1])=={digest(body['proof']):body['row'] for body in bodies}
    assert audit_rows(path,local_user='receiver')['totals']['unavailable']==0
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==1


@pytest.mark.parametrize('native',[False,True])
def test_local_repository_capture_preserves_existing_signed_head(tmp_path,native):
    people,control=authors()
    user,device=people[0]
    row=dict(v=1,kind='repository.observed',id='repo',state='active',data=dict(lineage='lineage',roots=[],remotes=['https://example.test/old/repo.git']))
    proof=row_proof(device,user,'w',1,row)
    path=tmp_path/'archive.db'
    apply_row_replicas(path,[dict(row=row,proof=proof)],'w',[control],local_user=user if native else 'receiver')
    with core.open_db(path,purpose='test.capture') as db,core._transaction(db):
        core.project_provenance(db,dict(kind='repository.observed',entity='repo',observed_at=None,payload=dict(id='repo',**(row['data']|{'remotes':['https://example.test/new/repo.git']}))))
    assert recovered(path,device,user if native else 'receiver')[digest(proof)]==row
    assert audit_rows(path,local_user=user if native else 'receiver')['totals']['unavailable']==0


@pytest.mark.parametrize('kind',['messages','tool_calls','file_edits','attachments','artifacts','checkpoint.link'])
def test_child_before_parent_remains_relayable(tmp_path,kind):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    rows.update(attachments=dict(v=1,kind='attachments',id='a',state='active',data=dict(message_id='m',filename='a.txt',mime_type='text/plain',size=0,body_hash=None,created_at=None)),artifacts=dict(v=1,kind='artifacts',id='a',state='active',data=dict(conversation_id='c',artifact_type='code',title='a',content='x',language='python',created_at=None,version=1)))
    rows['checkpoint.link']=dict(v=1,kind='checkpoint.link',id=digest(dict(checkpoint='checkpoint',edit='e')),state='active',data=dict(checkpoint='checkpoint',edit='e',evidence='captured_exact'))
    row=rows[kind]
    proof=row_proof(device,user,'w',1,row)
    path=tmp_path/'archive.db'
    assert apply_row_replicas(path,[dict(row=row,proof=proof)],'w',[control],local_user='receiver')==[True]
    assert audit_rows(path,local_user='receiver')['totals']['unavailable']==0
    assert recovered(path,device)[digest(proof)]==row
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM remote.row_references').fetchone()[0]==1
    apply_row_replicas(path,bodies,'w',[control],local_user='receiver')
    assert recovered(path,device)[digest(proof)]==row
    assert audit_rows(path,local_user='receiver')['totals']['unavailable']==0
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute('SELECT count(*) FROM remote.row_references').fetchone()[0]==0


def test_unrecoverable_checkpoint_is_reported_without_aborting_audit(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    row=dict(v=1,kind='checkpoint.link',id=digest(dict(checkpoint='checkpoint',edit='e')),state='active',data=dict(checkpoint='checkpoint',edit='e',evidence='captured_exact'))
    path=tmp_path/'legacy.db'
    apply_row_replicas(path,[dict(row=row,proof=row_proof(device,user,'w',1,row))],'w',[control],local_user='receiver')
    with core.open_db(path,purpose='test.legacy') as db:
        db.execute('DELETE FROM remote.row_conflicts')
        db.execute('DELETE FROM remote.row_references')
    assert audit_rows(path,local_user='receiver')['totals']['unavailable']==1
    blocked=[]
    assert row_replicas(path,{'user':'receiver','device':device},'w',[],{1:bytes(32)},blocked=blocked)==[]
    assert blocked==[('checkpoint.link',row['id'])]


@pytest.mark.parametrize('mutation',['delete','reparent'])
def test_archive_edit_mutation_preserves_joined_signed_fact(tmp_path,mutation):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    fid=digest(dict(repository=None,path='a.py'))
    facts=[dict(v=1,kind='file.observed',id=fid,state='active',data=dict(repository=None,path='a.py',kind='external')),dict(v=1,kind='edit.observed',id='e',state='active',data=dict(turn='m',file=fid,repository=None,old_content_hash=None,new_content_hash='h',evidence='captured_exact'))]
    fact_bodies=[dict(row=row,proof=row_proof(device,user,'w',1,row)) for row in facts]
    path=tmp_path/'archive.db'
    apply_row_replicas(path,[*bodies,*fact_bodies],'w',[control],local_user='receiver')
    changed=dict(v=1,kind='file_edits',id='e',state='deleted',data=None) if mutation=='delete' else rows['file_edits']|{'data':rows['file_edits']['data']|{'message_id':'later'}}
    proof=row_proof(device,user,'w',1,changed,proofs['file_edits']['revision'])
    apply_row_replicas(path,[dict(row=changed,proof=proof)],'w',[control],local_user='receiver')
    assert recovered(path,device)[digest(fact_bodies[1]['proof'])]==facts[1]
    assert audit_rows(path,local_user='receiver')['totals']['unavailable']==0


def test_reference_schema_upgrade_preserves_signed_rows_and_backups(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    path=tmp_path/'archive.db'
    apply_row_replicas(path,bodies,'w',[control],local_user='receiver')
    with core.open_db(path,purpose='test.v11') as db:
        db.execute('DROP TABLE remote.row_references')
        db.execute('DROP TABLE provenance.pending')
        db.execute('UPDATE core_schema SET version=11')
    with core.open_db(path,purpose='test.upgrade') as db: core.init_schema(db)
    assert recovered(path,device)=={digest(body['proof']):body['row'] for body in bodies}
    with duckdb.connect(str(path.with_name('archive.db.pre-v12.bak')),read_only=True) as db:
        assert db.execute('SELECT version FROM core_schema').fetchone()[0]==11
        assert db.execute('SELECT count(*) FROM remote.row_proofs').fetchone()[0]==len(bodies)
