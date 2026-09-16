"""Alias repair must retain signed history and converge across parser generations."""
import json

import duckdb
import pytest
from ai_convos import cli as core
from ai_convos_remote import provider_alias_accept, provider_alias_id, provider_alias_records
from ai_convos_remote import projection
from ai_convos_remote.protocol import semantic_proof
from tests.test_remote_projection import _provider_alias_archive

SESSION='019a2f3d-9455-7820-b4f6-0beeb2bf1f6f'
LEGACY='rollout-2025-10-29T10-12-36-'+SESSION


def archive(tmp_path,legacy=True):
    root,path,identity,device,user,cfg,entry=_provider_alias_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute('DELETE FROM remote.provider_session_aliases')
        db.execute('DELETE FROM remote.semantic_ancestors')
        db.execute('DELETE FROM provider_sessions')
        db.execute("UPDATE conversations SET metadata=? WHERE id='a'",[json.dumps({'session_id':LEGACY if legacy else SESSION,'cli_version':'old'})])
        db.execute("UPDATE conversations SET metadata=? WHERE id='b'",[json.dumps({'session_id':SESSION,'session_kind':'main','session_kind_evidence':'exact'})])
        db.execute("INSERT INTO provider_sessions VALUES ('codex',?,'b')",[SESSION])
    attest(tmp_path,path,cfg)
    return root,path,identity,device,user,cfg,entry


def attest(tmp_path,path,cfg):
    state=projection.connect(tmp_path/'attest-state.db')
    with duckdb.connect(str(path)) as db: records=projection.scan(db,state)
    state.close()
    projection.attest_rows(path,cfg,'personal',records)


def accept(root,identity,device,user,session,members):
    row={'v':1,'kind':'provider.session','id':provider_alias_id('codex',session),'state':'active','data':{'source':'codex','session_id':session,'members':sorted(members),'canonical':min(members)}}
    assert provider_alias_accept(root,row,semantic_proof(identity,user,device['id'],'personal',1,row))


@pytest.mark.parametrize('session',[LEGACY,SESSION])
def test_legacy_equivalence_preserves_signed_metadata_and_binds_native_session(tmp_path,session):
    root,path,identity,device,user,cfg,_=archive(tmp_path)
    accept(root,identity,device,user,session,['a','b'])
    with duckdb.connect(str(path),read_only=True) as db: before=db.execute("SELECT metadata FROM conversations WHERE id='a'").fetchone()
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':1,'settled':0,'blocked':{}}
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT id,metadata FROM conversations').fetchall()==[('a',before[0])]
        assert db.execute('SELECT conversation_id FROM messages').fetchall()==[('a',)]
        assert core.session_bindings(db)[('codex',SESSION)]=='a'
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':0,'settled':1,'blocked':{}}
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_alias_discovery_unifies_legacy_shapes_before_signing(tmp_path):
    root,path,identity,device,user,cfg,_=archive(tmp_path)
    records=provider_alias_records(root,user,'personal','personal')
    assert len(records)==1
    assert records[0]['row']['data']=={'source':'codex','session_id':SESSION,'members':['a','b'],'canonical':'a'}


def test_overlapping_legacy_and_native_aliases_use_one_canonical(tmp_path):
    root,path,identity,device,user,cfg,_=archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex',?)",[json.dumps({'session_id':SESSION})])
        db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('message-c','c','user','unique c','{}')")
    attest(tmp_path,path,cfg)
    accept(root,identity,device,user,LEGACY,['a','b'])
    accept(root,identity,device,user,SESSION,['b','c'])
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':1,'settled':0,'blocked':{}}
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':0,'settled':1,'blocked':{}}
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT id FROM conversations').fetchall()==[('a',)]
        assert db.execute('SELECT content,conversation_id FROM messages ORDER BY content').fetchall()==[('B','a'),('unique c','a')]
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_stale_origin_cannot_change_live_native_row_ownership(tmp_path):
    root,path,identity,device,user,cfg,_=_provider_alias_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id,source_row_id,author_user_id,author_device_id,workspace_id,proof_id) SELECT 'conversations','gone-b','b',?,?,'personal',id FROM remote.row_proofs WHERE row_kind='conversations' AND source_row_id='b'",[user,device['id']])
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':1,'settled':0,'blocked':{}}
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT id FROM conversations').fetchall()==[('a',)]
        assert db.execute("SELECT physical_row_id FROM remote.row_origins WHERE physical_row_id='gone-b'").fetchone()==('gone-b',)
        assert db.execute('SELECT conversation_id FROM messages').fetchall()==[('a',)]
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_two_live_origins_remain_ambiguous(tmp_path):
    root,path,identity,device,user,cfg,_=_provider_alias_archive(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("INSERT INTO conversations SELECT 'extra-b',* EXCLUDE(id) FROM conversations WHERE id='b'")
        db.execute("INSERT INTO remote.row_origins(table_name,physical_row_id,source_row_id,author_user_id) VALUES ('conversations','extra-b','b',?)",[user])
    result=projection.reconcile_provider_aliases(path,cfg,'personal')
    assert result['changed']==0 and list(result['blocked'].values())==['provider alias member projection is ambiguous: b']


@pytest.mark.parametrize('source,value,expected',[
    ('codex',LEGACY,SESSION),('codex',SESSION,SESSION),('claude-code',LEGACY,LEGACY),
    ('codex',LEGACY+'-suffix',LEGACY+'-suffix'),('codex','prefix-'+LEGACY,'prefix-'+LEGACY),
    ('codex','rollout-'+SESSION,'rollout-'+SESSION),('codex',None,None)])
def test_session_normalization_only_recognizes_exact_codex_legacy_shape(source,value,expected):
    assert core.provider_session_key(source,value)==expected


def settled(tmp_path):
    root,path,identity,device,user,cfg,_=_provider_alias_archive(tmp_path)
    state=projection.connect(tmp_path/'cache.db')
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['changed']==1
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['settled']==1
    return path,user,cfg,state


def test_settled_cache_skips_body_walk_after_unrelated_capture(tmp_path,monkeypatch):
    path,user,cfg,state=settled(tmp_path)
    monkeypatch.setattr(projection,'_alias_pages',lambda *a,**k:pytest.fail('unchanged alias walked its bodies'))
    with duckdb.connect(str(path)) as db:
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('unrelated','codex','{}')")
        core._archive_touch(db,[('conversations','unrelated')])
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)=={'changed':0,'settled':1,'blocked':{}}
    state.close()


def test_cache_rechecks_changed_child_and_preserves_integrity_failure(tmp_path):
    path,user,cfg,state=settled(tmp_path)
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE messages SET content='changed source content' WHERE id='message-b'")
        core._archive_touch(db,[('messages','message-b')])
    result=projection.reconcile_provider_aliases(path,cfg,'personal',state=state)
    assert result['settled']==0 and 'body/proof mismatch' in next(iter(result['blocked'].values()))
    state.close()


def test_cache_rechecks_new_proofs_without_archive_generation_change(tmp_path):
    path,user,cfg,state=settled(tmp_path)
    with duckdb.connect(str(path)) as db:
        head=projection._heads(db,user,{'messages':{'message-b'}})[('messages','message-b')]
        row=core.typed_logical_rows(db,[('messages','message-b','message-b',user,'active')])[('messages','message-b','message-b',user,'active')]
        row['data']['content']='new remote head, body not delivered yet'
        proof=projection.row_proof(cfg['device'],user,'personal',1,row,head['revision'])
        signer=cfg['controls']['personal']['devices'][cfg['device']['id']]
        before=db.execute('SELECT generation FROM archive_state').fetchone()
        core.project_row_proof(db,proof,signer['root_public'],signer['certificate'])
        assert db.execute('SELECT generation FROM archive_state').fetchone()==before
    result=projection.reconcile_provider_aliases(path,cfg,'personal',state=state)
    assert result['settled']==0 and 'body/proof mismatch' in next(iter(result['blocked'].values()))
    state.close()


def test_cache_rechecks_binding_and_archive_identity(tmp_path):
    path,user,cfg,state=settled(tmp_path)
    with duckdb.connect(str(path)) as db: db.execute("UPDATE provider_sessions SET conversation_id='b'")
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['changed']==1
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['settled']==1
    with duckdb.connect(str(path)) as db: db.execute('UPDATE archive_state SET archive_id=uuid()')
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['settled']==1
    state.close()


@pytest.mark.parametrize('dirty',[False,True])
@pytest.mark.parametrize('recorded_base',[False,True])
def test_own_signed_successor_advances_only_unchanged_local_body(tmp_path,dirty,recorded_base):
    from tests.test_remote_projection import signed_edit_graph
    _,device,user,control,rows,proofs,bodies,_=signed_edit_graph()
    path=tmp_path/'device.db'
    projection.apply_row_replicas(path,bodies,'w',[control],local_user=user)
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE conversations SET cwd='/this/device/worktree',git_branch='local-branch' WHERE id='c'")
        if dirty: db.execute("UPDATE messages SET content='unsynced local edit' WHERE id='m'")
        if not recorded_base: db.execute('DELETE FROM remote.local_row_bases')
    changed={**rows['messages'],'data':{**rows['messages']['data'],'content':'remote successor'}}
    proof=projection.row_proof(device,user,'w',1,changed,proofs['messages']['revision'])
    conv={**rows['conversations'],'data':{**rows['conversations']['data'],'title':'updated title'}}
    cproof=projection.row_proof(device,user,'w',1,conv,proofs['conversations']['revision'])
    for _ in range(2):
        projection.apply_row_replicas(path,[dict(row=changed,proof=proof),dict(row=conv,proof=cproof)],'w',[control],local_user=user,local_device='another-device')
        projection.apply_row_replicas(path,bodies,'w',[control],local_user=user)
        projection.retry_own_replicas(path,user)
        with duckdb.connect(str(path),read_only=True) as db:
            assert db.execute("SELECT content FROM messages WHERE id='m'").fetchone()==('unsynced local edit' if dirty else 'remote successor',)
            assert db.execute("SELECT title,cwd,git_branch FROM conversations WHERE id='c'").fetchone()==('updated title','/this/device/worktree','local-branch')
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


@pytest.mark.parametrize('legacy',[False,True])
def test_equivalent_provider_messages_retire_after_alias_and_replay(tmp_path,legacy):
    root,path,identity,device,user,cfg,_=archive(tmp_path,False)
    with duckdb.connect(str(path)) as db:
        db.execute('DELETE FROM messages')
        db.execute("DELETE FROM remote.row_proofs WHERE row_kind='messages'")
        for cid in ('a','b'):
            for index in (1,2):
                mid=core.gen_id('codex',f'{cid}:{index}')
                db.execute("INSERT INTO messages(id,conversation_id,role,content,created_at,metadata) VALUES (?,?,'user','repeated text','2026-01-01',?)",[mid,cid,json.dumps({} if legacy and cid=='a' else dict(provider_index=index))])
    attest(tmp_path,path,cfg)
    accept(root,identity,device,user,SESSION,['a','b'])
    assert projection.reconcile_provider_aliases(path,cfg,'personal')['changed']==1
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(4 if legacy else 2,)
        assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='messages'").fetchone()==(0 if legacy else 2,)
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':0,'settled':1,'blocked':{}}
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_cyclic_retirement_cannot_remove_every_current_message(tmp_path):
    path=tmp_path/'cycle.db'
    with duckdb.connect(str(path)) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}')")
        ids=[core.gen_id('codex',str(i)) for i in range(2)]
        db.executemany("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES (?,'c','user','same','{}')",[(mid,) for mid in ids])
        rows=core.typed_logical_rows(db,[('messages',mid,mid,'','active') for mid in ids])
        lineage=[dict(old_id=ids[i],old_hash=core.provenance_digest(rows[('messages',ids[i],ids[i],'','active')]),current_id=ids[1-i],current_hash=core.provenance_digest(rows[('messages',ids[1-i],ids[1-i],'','active')])) for i in range(2)]
        db.execute("UPDATE conversations SET metadata=?",[json.dumps(dict(convos_message_lineage=dict(v=1,records=lineage)))])
        core.rebuild_parser_lineage(db)
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(2,)
        assert core.archive_relationships(db)=={}


def test_semantic_proof_arrival_invalidates_settled_sync_without_row_mutation(tmp_path):
    from ai_convos_remote import _archive_marker
    root,path,identity,device,user,cfg,_=archive(tmp_path)
    before=_archive_marker(root)
    accept(root,identity,device,user,SESSION,['a','b'])
    after=_archive_marker(root)
    assert before[:4]==after[:4] and before[4]<after[4]


def test_missing_received_evidence_is_not_published_as_an_author_verdict():
    from ai_convos_remote import _edit_record
    for reason in ('signed_replica_missing_evidence','signed_evidence_conflict'):
        assert _edit_record(('e','revision','physical'),{'physical':('unverified',reason,None)},{},{},{},'author') is None


@pytest.mark.parametrize('conflict',[False,True])
def test_alias_reconciles_independent_native_changes_but_preserves_content_conflicts(tmp_path,conflict):
    root,path,identity,device,user,cfg,_=archive(tmp_path,False)
    with duckdb.connect(str(path)) as db:
        head=projection._heads(db,user,{'conversations':{'a'}})[('conversations','a')]
        original=core.typed_logical_rows(db,[('conversations','a','a',user,'active')])[('conversations','a','a',user,'active')]
        with core.preserve_fact_heads(db,[('conversations','a')]):
            db.execute("UPDATE conversations SET title='local title' WHERE id='a'")
            core._archive_touch(db,[('conversations','a')])
        core.retire_row_bodies(db,[('conversations','a',user,head['revision'])])
        assert json.loads(db.execute('SELECT body FROM remote.row_conflicts WHERE proof_id=?',[projection.digest(head)]).fetchone()[0])==original
    remote={**original,'data':{**original['data'],**({'title':'competing title'} if conflict else {'metadata':{**original['data']['metadata'],'parser':'new'}})}}
    proof=projection.row_proof(device,user,'personal',1,remote,head['revision'])
    projection.apply_row_replicas(path,[dict(row=remote,proof=proof)],'personal',[cfg['controls']['personal']],local_user=user,local_device=device['id'])
    accept(root,identity,device,user,SESSION,['a','b'])
    result=projection.reconcile_provider_aliases(path,cfg,'personal')
    assert bool(result['blocked'])==conflict
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute("SELECT title FROM conversations WHERE id='a'").fetchone()==('local title',)
        if not conflict: assert json.loads(db.execute("SELECT metadata FROM conversations WHERE id='a'").fetchone()[0])['parser']=='new'
        else: assert db.execute('SELECT count(*) FROM conversations').fetchone()==(2,)
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0
