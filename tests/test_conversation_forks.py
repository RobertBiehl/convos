"""Concurrent parser bookkeeping must converge without deciding content conflicts."""
import copy
import json
import shutil

import duckdb
import pytest

from ai_convos import cli as core
from ai_convos_remote import projection as remote
from ai_convos_remote.protocol import certificate, digest, identity, open_replica, public, row_proof
from tests.test_native_provenance import people, scanned


def current(path,user):
    with duckdb.connect(str(path),read_only=True) as db:
        return core.typed_logical_rows(db,[('conversations','c','c',user,'active')])[('conversations','c','c',user,'active')]


def lineage(label):
    return dict(old_id=core.gen_id('old',label),old_hash=digest(['old',label]),current_id=core.gen_id('new',label),current_hash=digest(['new',label]))


def fork(tmp_path,conflict=None):
    users,devices,control,cfg=people()
    second=identity('second-device'); cfg2=copy.deepcopy(cfg); cfg2['device']=second
    root=identity('shared-root')
    from ai_convos_remote.protocol import public_id
    user=public_id(root['sign_public']); cfg['user']=cfg2['user']=user
    control['devices']={d['id']:dict(user=user,root_public=root['sign_public'],device=public(d),certificate=certificate(root,user,d),history=True) for d in (devices[0],second)}
    for c in (cfg,cfg2): c['controls']={'w':control}; c['server_state']={'workspaces':[{'id':'w','controls':[control]}]}
    paths=[tmp_path/'laptop.db',tmp_path/'desktop.db']
    with core.open_db(paths[0],purpose='test.fork.init') as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,title,created_at,updated_at,metadata) VALUES ('c','codex','same conversation','2026-01-01','2026-01-01',?)",[json.dumps(dict(session_id='qualification-live',timestamp_basis='utc'))])
        db.execute("INSERT INTO messages(id,conversation_id,role,content) VALUES ('m','c','user','preserved')")
    remote.attest_rows(paths[0],cfg,'w',scanned(paths[0],tmp_path/'scan.db'))
    with duckdb.connect(str(paths[0]),read_only=True) as db: base=remote._heads(db,user,{'conversations':{'c'}})[('conversations','c')]
    shutil.copy2(paths[0],paths[1]); original=current(paths[0],user)
    rows=[{**original,'data':{**original['data'],'updated_at':f'2026-01-01T00:00:{turn:02d}.000000','metadata':{**original['data']['metadata'],'convos_message_lineage':dict(v=1,records=[lineage(str(turn))])}}} for turn in (6,10)]
    if conflict=='title': rows[1]['data']['title']='different content'
    if conflict=='legacy': rows[1]['data']['metadata']['timestamp_basis']='legacy_local'
    if conflict=='session': rows[1]['data']['metadata']['session_id']='another-session'
    if conflict=='lineage': rows[1]['data']['metadata']['convos_message_lineage']['records'][0]['old_hash']='invalid'
    bodies=[dict(row=row,proof=row_proof(c['device'],user,'w',1,row,base['revision'])) for row,c in zip(rows,(cfg,cfg2))]
    for path,c,body in zip(paths,(cfg,cfg2),bodies):
        with core.open_db(path,purpose='test.fork.local') as db,core._transaction(db):
            core.project_attested_rows(db,[(body['row'],body['proof'])],root['sign_public'],control['devices'][c['device']['id']]['certificate'])
            core.project_logical_rows(db,[(body['row'],body['proof'],digest(body['proof']),True)])
        remote.apply_row_replicas(path,list(reversed(bodies)) if path==paths[0] else bodies,'w',[control],local_user=user,local_device=c['device']['id'])
    return paths,(cfg,cfg2),bodies


def exported(path,cfg):
    records=scanned(path,path.with_suffix('.scan.db'))
    remote.attest_rows(path,cfg,'w',records)
    return [open_replica(env,bytes(32)) for env in remote.row_replicas(path,cfg,'w',records,{1:bytes(32)})]


def test_concurrent_conversation_bookkeeping_converges_and_can_advance(tmp_path):
    paths,configs,bodies=fork(tmp_path); user=configs[0]['user']
    assert current(paths[0],user)!=current(paths[1],user)
    for path,cfg in zip(paths,configs): assert not remote.reconcile_provider_aliases(path,cfg,'w')['blocked']
    merged=current(paths[0],user)
    assert current(paths[1],user)==merged
    assert merged['data']['updated_at']=='2026-01-01T00:00:10.000000'
    assert set(map(digest,merged['data']['metadata']['convos_message_lineage']['records']))==set(map(digest,[lineage('6'),lineage('10')]))
    for path,cfg in zip(paths,configs):
        retained=exported(path,cfg)
        assert all(any(x['row']==body['row'] and x['proof']==body['proof'] for x in retained) for body in bodies)
        assert remote.audit_rows(path,local_user=user)['totals']['unavailable']==0
    with core.open_db(paths[0],purpose='test.fork.next') as db,core._transaction(db),core.preserve_fact_heads(db,[('conversations','c')]):
        db.execute("UPDATE conversations SET title='later source update',updated_at='2026-01-01T00:00:11' WHERE id='c'")
        core._archive_touch(db,[('conversations','c')])
    remote.attest_rows(paths[0],configs[0],'w',scanned(paths[0],tmp_path/'next.db'))
    replicas=exported(paths[0],configs[0])
    for values in (list(reversed(replicas)),replicas,replicas): remote.apply_row_replicas(paths[1],values,'w',[configs[0]['controls']['w']],local_user=user,local_device=configs[1]['device']['id'])
    assert current(paths[1],user)==current(paths[0],user)
    assert current(paths[1],user)['data']['title']=='later source update'
    assert all(remote.audit_rows(path,local_user=user)['totals']['unavailable']==0 for path in paths)
    for path,cfg in zip(paths,configs):
        retained=exported(path,cfg)
        assert all(any(x['proof']==body['proof'] and x['row']==body['row'] for x in retained) for body in bodies)


@pytest.mark.parametrize('conflict',['title','legacy','session','lineage'])
def test_conversation_merge_keeps_incompatible_forks(tmp_path,conflict):
    paths,configs,bodies=fork(tmp_path,conflict)
    for path,cfg,body in zip(paths,configs,bodies):
        result=remote.reconcile_provider_aliases(path,cfg,'w')
        assert result['blocked']
        assert current(path,cfg['user'])==body['row']
        assert remote.audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_conversation_merge_rolls_back_and_retries_without_losing_heads(tmp_path,monkeypatch):
    paths,configs,bodies=fork(tmp_path); path,cfg=paths[0],configs[0]
    def snapshot():
        with duckdb.connect(str(path),read_only=True) as db:
            return {table:db.execute(f'SELECT * FROM {table} ORDER BY ALL').fetchall() for table in ('conversations','messages','remote.row_proofs','remote.row_conflicts','remote.local_row_bases','archive_state')}
    before=snapshot(); project=remote.project_logical_rows
    def interrupted(*args,**kwargs): raise RuntimeError('interrupted merge projection')
    monkeypatch.setattr(remote,'project_logical_rows',interrupted)
    with pytest.raises(RuntimeError,match='interrupted merge projection'): remote.reconcile_provider_aliases(path,cfg,'w')
    assert snapshot()==before
    monkeypatch.setattr(remote,'project_logical_rows',project)
    assert not remote.reconcile_provider_aliases(path,cfg,'w')['blocked']
    retained=exported(path,cfg)
    assert all(any(x['proof']==body['proof'] and x['row']==body['row'] for x in retained) for body in bodies)
    before=snapshot()
    assert not remote.reconcile_provider_aliases(path,cfg,'w')['blocked']
    assert snapshot()==before


def test_conversation_fork_does_not_overwrite_unpublished_content(tmp_path):
    paths,configs,bodies=fork(tmp_path); path,cfg=paths[0],configs[0]
    with core.open_db(path,purpose='test.fork.dirty') as db,core._transaction(db),core.preserve_fact_heads(db,[('conversations','c')]):
        db.execute("UPDATE conversations SET title='unpublished local title' WHERE id='c'")
        core._archive_touch(db,[('conversations','c')])
    assert remote.reconcile_provider_aliases(path,cfg,'w')['blocked']
    assert current(path,cfg['user'])['data']['title']=='unpublished local title'
    assert remote.audit_rows(path,local_user=cfg['user'])['totals']['unavailable']==0


def test_alias_successor_extends_all_equivalent_conversation_tips(tmp_path):
    paths,configs,bodies=fork(tmp_path); path,cfg=paths[0],configs[0]; user=cfg['user']
    assert not remote.reconcile_provider_aliases(path,cfg,'w')['blocked']
    before=current(path,user); row={**before,'data':{**before['data'],'updated_at':'2026-01-01T00:00:12.000000'}}
    with core.open_db(path,purpose='test.fork.alias') as db,core._transaction(db),core.preserve_fact_heads(db,[('conversations','c')]):
        heads=remote._heads(db,user,{'conversations':{'c'}},True)[('conversations','c')]
        assert len(heads)==2
        proof=row_proof(cfg['device'],user,'w',1,row,heads[0]['revision'])
        pid=remote._alias_proofs(db,cfg,'w',[(row,proof)])[0]
        core.project_logical_rows(db,[(row,proof,pid,True)])
        after=remote._heads(db,user,{'conversations':{'c'}},True)[('conversations','c')]
        assert {p['previous_revision'] for p in after}=={p['revision'] for p in heads}
        assert {p['content_hash'] for p in after}=={digest(row)}
    retained=exported(path,cfg)
    assert all(any(x['proof']==body['proof'] and x['row']==body['row'] for x in retained) for body in bodies)


def test_interrupted_attestation_cannot_split_one_conversation_across_pages(tmp_path,monkeypatch):
    paths,configs,bodies=fork(tmp_path); path,cfg=paths[0],configs[0]; user=cfg['user']
    remote.reconcile_provider_aliases(path,cfg,'w'); exported(path,cfg)
    with core.open_db(path,purpose='test.fork.page') as db,core._transaction(db),core.preserve_fact_heads(db,[('conversations','c')]):
        before={p['revision'] for p in remote._heads(db,user,{'conversations':{'c'}},True)[('conversations','c')]}
        db.execute("UPDATE conversations SET title='source update' WHERE id='c'")
        db.executemany("INSERT INTO messages(id,conversation_id,role,content) VALUES (?,'c','user','new source row')",[(f'm-{i}',) for i in range(499)])
        core._archive_touch(db,[('conversations','c'),*[('messages',f'm-{i}') for i in range(499)]])
    records=sorted(scanned(path,tmp_path/'attest.db'),key=lambda r:r['kind']=='conversation.record')
    store=remote._store_proofs; pages=[]
    def interrupted(path,rows,*args):
        pages.append(rows)
        if len(pages)==2: raise RuntimeError('interrupted second page')
        return store(path,rows,*args)
    monkeypatch.setattr(remote,'_store_proofs',interrupted)
    with pytest.raises(RuntimeError,match='interrupted second page'): remote.attest_rows(path,cfg,'w',records)
    with duckdb.connect(str(path),read_only=True) as db:
        assert {p['revision'] for p in remote._heads(db,user,{'conversations':{'c'}},True)[('conversations','c')]}==before
    assert [len(page) for page in pages]==[499,2]
    monkeypatch.setattr(remote,'_store_proofs',store)
    assert remote.attest_rows(path,cfg,'w',records)==2
    assert remote.audit_rows(path,local_user=user)['totals']['unavailable']==0
