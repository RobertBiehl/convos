import json, subprocess, sys

import duckdb
import pytest
import ai_convos.cli as core
import ai_convos_remote as client
import ai_convos_remote.projection as projection
from ai_convos_remote.protocol import certificate, digest, identity, public, public_id, row_proof
from tests.test_remote_projection import signed_edit_graph


def graph():
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph()
    other_root,other_device=identity('other root'),identity('other device')
    other=public_id(other_root['sign_public'])
    control['devices'][other_device['id']]={'user':other,'root_public':other_root['sign_public'],'device':public(other_device),'certificate':certificate(other_root,other,other_device),'history':True}
    file=dict(v=1,kind='file.observed',id=digest(dict(repository=None,path='a.py')),state='active',data=dict(repository=None,path='a.py',kind='external'))
    fact=lambda name:dict(v=1,kind='edit.observed',id=name,state='active',data=dict(turn='m',file=file['id'],repository=None,old_content_hash=None,new_content_hash='h',evidence='captured_exact'))
    signed=lambda row,foreign=False:dict(row=row,proof=row_proof(other_device if foreign else device,other if foreign else user,'w',1,row))
    return user,control,rows,bodies,file,fact,signed


def remaining(path):
    with duckdb.connect(str(path),read_only=True) as db:
        return db.execute("SELECT count(*) FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.row_kind='edit.observed'").fetchone()[0]


def test_shared_file_from_another_author_wakes_pending_edit(tmp_path):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply([signed(fact('e'))]); apply(bodies)
    assert remaining(path)==1
    apply([signed(file,True)])
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT file_id FROM provenance.file_edit_files WHERE file_edit_id=?',[projection.foreign_id(user,'file_edits','e')]).fetchone()==(file['id'],)
        assert db.execute('SELECT count(*) FROM remote.edit_dependencies').fetchone()[0]==0
    assert remaining(path)==0 and projection.audit_rows(path,local_user='receiver')['totals']['unavailable']==0


def test_archive_edit_dependency_is_author_qualified(tmp_path,monkeypatch):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply([signed(fact('e')),signed(file,True)])
    projection.retry_edit_replicas(path,'receiver')
    real,seen=projection.project_logical_rows,[]
    monkeypatch.setattr(projection,'project_logical_rows',lambda db,items,**kw:seen.extend(row['kind'] for row,*_ in items) or real(db,items,**kw))
    apply([signed(row,True) for row in rows.values()])
    assert 'edit.observed' not in seen and remaining(path)==1
    apply(bodies)
    assert remaining(path)==0


def test_pending_wakeup_rolls_back_with_the_parent_transaction(tmp_path,monkeypatch):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply([*bodies,signed(fact('e'))])
    projection.retry_edit_replicas(path,'receiver')
    real=projection.project_edit_dependencies
    def crash(*args,**kwargs):
        real(*args,**kwargs)
        raise RuntimeError('crash before commit')
    monkeypatch.setattr(projection,'project_edit_dependencies',crash)
    with pytest.raises(RuntimeError,match='crash before commit'): apply([signed(file,True)])
    with duckdb.connect(str(path),read_only=True) as db:
        assert not db.execute('SELECT 1 FROM provenance.files WHERE id=?',[file['id']]).fetchone()
        assert db.execute('SELECT count(*) FROM remote.edit_dependencies').fetchone()[0]==2
        assert not db.execute("SELECT 1 FROM remote.edit_ready WHERE dependency_key<>''").fetchone()
    monkeypatch.setattr(projection,'project_edit_dependencies',real)
    apply([signed(file,True)])
    assert remaining(path)==0


def test_ready_overflow_resumes_after_restart_without_arrivals(tmp_path):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply(bodies[:-1]+[signed(dict(rows['file_edits'],id=f'e{i}')) for i in range(1001)])
    apply([signed(fact(f'e{i}')) for i in range(1001)])
    assert remaining(path)==1001
    apply([signed(file,True)])
    assert remaining(path)==501
    code="""import json,sys
from pathlib import Path
import ai_convos_remote.projection as p
sizes=[]
real=p.project_logical_rows
def project(db,items,**kw):
 sizes.append(len(items))
 return real(db,items,**kw)
p.project_logical_rows=project
p.retry_edit_replicas(Path(sys.argv[1]),'receiver')
print(json.dumps(sizes))
"""
    child=subprocess.run([sys.executable,'-c',code,str(path)],capture_output=True,text=True,timeout=60)
    assert child.returncode==0,child.stderr
    sizes=json.loads(child.stdout)
    assert sum(sizes)==501 and max(sizes)<=500 and remaining(path)==0
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM remote.edit_dependencies').fetchone()[0]==0
        assert db.execute('SELECT * FROM remote.edit_ready').fetchall()==[('','done')]


def test_retry_projection_and_cursor_roll_back_together(tmp_path,monkeypatch):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply([*bodies,signed(fact('e'))]); projection.retry_edit_replicas(path,'receiver')
    real_work=projection._edit_retry_work
    monkeypatch.setattr(projection,'_edit_retry_work',lambda db:(_ for _ in ()).throw(RuntimeError('interrupted after parent commit')))
    with pytest.raises(RuntimeError,match='after parent commit'): apply([signed(file,True)])
    monkeypatch.setattr(projection,'_edit_retry_work',real_work)
    with duckdb.connect(str(path),read_only=True) as db: before=db.execute('SELECT * FROM remote.edit_ready ORDER BY dependency_key').fetchall()
    real_update=projection.project_edit_dependencies
    def crash(*args,**kwargs):
        real_update(*args,**kwargs)
        raise RuntimeError('interrupted before retry commit')
    monkeypatch.setattr(projection,'project_edit_dependencies',crash)
    with pytest.raises(RuntimeError,match='before retry commit'): projection.retry_edit_replicas(path,'receiver')
    with duckdb.connect(str(path),read_only=True) as db: assert db.execute('SELECT * FROM remote.edit_ready ORDER BY dependency_key').fetchall()==before
    assert remaining(path)==1
    monkeypatch.setattr(projection,'project_edit_dependencies',real_update)
    projection.retry_edit_replicas(path,'receiver')
    assert remaining(path)==0


def test_legacy_retained_edits_are_seeded_without_another_arrival(tmp_path):
    user,control,rows,bodies,file,fact,signed=graph(); path=tmp_path/'db'
    apply=lambda values:projection.apply_row_replicas(path,values,'w',[control],local_user='receiver')
    apply([*bodies,signed(fact('e'))])
    with duckdb.connect(str(path)) as db:
        db.execute('DROP TABLE remote.edit_dependencies; DROP TABLE remote.edit_ready')
        core.project_provenance(db,dict(kind=file['kind'],entity=file['id'],payload=dict(id=file['id'],**file['data']),observed_at=None))
        core.init_schema(db)
    projection.retry_edit_replicas(path,'receiver')
    assert remaining(path)==0


def test_late_ancestor_body_is_kept_when_the_current_head_cannot_reconstruct(tmp_path):
    root,device,user,control,rows,proofs,bodies,evidence=signed_edit_graph(); path=tmp_path/'db'
    original=bodies[0]
    successor=dict(rows['conversations'],data=dict(rows['conversations']['data'],title='current'))
    head=row_proof(device,user,'w',1,successor,original['proof']['revision'])
    projection.apply_row_replicas(path,[original,dict(row=successor,proof=head)],'w',[control],local_user='receiver')
    with duckdb.connect(str(path)) as db: db.execute("UPDATE conversations SET title='drift'")
    projection.apply_row_replicas(path,[original],'w',[control],local_user='receiver')
    with duckdb.connect(str(path),read_only=True) as db:
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?',[digest(original['proof'])]).fetchone()[0])==original['row']


def test_core_body_retirement_is_exact_and_transactional():
    revision=('conversations',"quoted'source",'author','head')
    rows=[(f'p{i}',*value) for i,value in enumerate([revision,revision,('messages',*revision[1:]),(revision[0],'other',*revision[2:]),(*revision[:2],'other',revision[3]),(*revision[:3],'fork')])]
    with duckdb.connect() as db:
        core.init_schema(db)
        core._insert_pages(db,'remote.row_proofs',rows,('id','row_kind','source_row_id','author_user_id','revision'))
        core._insert_pages(db,'remote.row_conflicts',[(r[0],json.dumps({'id':r[0]})) for r in rows])
        with pytest.raises(RuntimeError,match='rollback'):
            with core._transaction(db):
                core.retire_row_bodies(db,[revision,revision])
                assert db.execute('SELECT proof_id FROM remote.row_conflicts ORDER BY proof_id').fetchall()==[(f'p{i}',) for i in range(2,6)]
                raise RuntimeError('rollback')
        assert db.execute('SELECT count(*) FROM remote.row_conflicts').fetchone()[0]==6
        with core._transaction(db): core.retire_row_bodies(db,[revision])
        core.retire_row_bodies(db,[])
        with pytest.raises(ValueError): core.retire_row_bodies(db,[revision[:3]])
        assert db.execute('SELECT proof_id FROM remote.row_conflicts ORDER BY proof_id').fetchall()==[(f'p{i}',) for i in range(2,6)]


def test_settled_sync_resumes_ready_metadata_without_generation_change(tmp_path,monkeypatch):
    from tests.test_remote_client import server_connect, transport, write_archive
    server=server_connect(tmp_path/'server.db'); direct,calls=transport(server),[]
    monkeypatch.setattr(client,'request',lambda cfg,body,auth=True:calls.append(body['op']) or direct(cfg,body,auth))
    monkeypatch.setattr(client,'drain_hooks',lambda:None)
    root=tmp_path/'client'; client.setup_client('http://server','alice',root=root)
    path=root/'data/convos.db'; write_archive(path,'settled')
    user,control,rows,bodies,file,fact,signed=graph()
    projection.apply_row_replicas(path,[*bodies,signed(file,True)],'w',[control],local_user='receiver')
    client.sync_once(root,True); client.sync_once(root)
    marker=client._archive_marker(root)
    body=signed(fact('e')); proof=body['proof']; signer=control['devices'][proof['author_device_id']]
    with duckdb.connect(str(path)) as db,core._transaction(db):
        core.project_row_proofs(db,[proof],signer['root_public'],signer['certificate'])
        core._insert_pages(db,'remote.row_conflicts',[(digest(proof),json.dumps(body['row']))])
        key=digest(['file.observed',None,file['id']])
        core.project_edit_dependencies(db,[(key,digest(proof))],[key])
    assert client._archive_marker(root)[:3]==marker[:3] and client._archive_marker(root)[3]
    calls.clear(); client.sync_once(root)
    assert remaining(path)==0 and not client._archive_marker(root)[3]
    calls.clear(); client.sync_once(root)
    assert calls==['state']
