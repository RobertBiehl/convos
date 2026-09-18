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


@pytest.mark.parametrize('interrupt',[False,True])
def test_settled_alias_checkpoint_survives_other_repairs_and_interruption(tmp_path,monkeypatch,interrupt):
    root,path,identity,device,user,cfg,_=_provider_alias_archive(tmp_path)
    assert projection.reconcile_provider_aliases(path,cfg,'personal')['changed']==1
    session=next('other-'+str(i) for i in range(100) if provider_alias_id('codex','other-'+str(i))>provider_alias_id('codex','same'))
    with core.open_db(path,purpose='test.additional-session') as db:
        db.executemany("INSERT INTO conversations(id,source,metadata) VALUES (?,'codex',?)",[(key,json.dumps(dict(session_id=session))) for key in ('c','d')])
        db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('message-d','d','user','D','{}')")
        db.execute("INSERT INTO provider_sessions VALUES ('codex',?,'d')",[session])
    attest(tmp_path,path,cfg)
    accept(root,identity,device,user,session,['c','d'])
    state=projection.connect(tmp_path/'cache.db')
    def progress(stage):
        if interrupt and stage=='provider aliases 1/2': raise KeyboardInterrupt()
    if interrupt:
        with pytest.raises(KeyboardInterrupt): projection.reconcile_provider_aliases(path,cfg,'personal',progress=progress,state=state)
    else:
        assert projection.reconcile_provider_aliases(path,cfg,'personal',progress=progress,state=state)==dict(changed=1,settled=1,blocked={})
    state.close()
    state=projection.connect(tmp_path/'cache.db')
    original=projection._alias_pages
    def pages(db_path,author,members,**kwargs):
        assert set(members)!={'a','b'},'a settled alias was rescanned after another group changed or the process stopped'
        return original(db_path,author,members,**kwargs)
    monkeypatch.setattr(projection,'_alias_pages',pages)
    assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)==dict(changed=int(interrupt),settled=1 if interrupt else 2,blocked={})
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0
    state.close()


@pytest.mark.parametrize('copies',[1,2,3])
@pytest.mark.parametrize('cached',[False,True])
def test_alias_repair_merges_native_lineage_with_equal_bases_signed_by_multiple_devices(tmp_path,monkeypatch,copies,cached):
    from ai_convos_remote.protocol import certificate,identity,public,row_proof
    root,path,signer,device,user,cfg,_=_provider_alias_archive(tmp_path)
    with core.open_db(path,purpose='test.same-base') as db:
        base=core.typed_logical_rows(db,[('messages','message-b','message-b',user,'active')])[('messages','message-b','message-b',user,'active')]
        original=projection._heads(db,user,{'messages':{'message-b'}})[('messages','message-b')]
        for index in range(copies):
            other=identity('other-'+str(index))
            entry=dict(user=user,root_public=signer['sign_public'],device=public(other),certificate=certificate(signer,user,other),history=True)
            cfg['controls']['personal']['devices'][other['id']]=entry
            proof=row_proof(other,user,'personal',1,base,original['previous_revision'])
            assert proof['revision']==original['revision']
            core.project_row_proof(db,proof,signer['sign_public'],entry['certificate'])
        lineage=lambda tag:dict(v=1,records=[dict(old_id=core.gen_id('old',tag),old_hash=core.provenance_digest(['old',tag]),current_id=core.gen_id('new',tag),current_hash=core.provenance_digest(['new',tag]))])
        local,remote=lineage('local'),lineage('remote')
        with core.preserve_fact_heads(db,[('messages','message-b')],observed=True):
            db.execute('UPDATE messages SET metadata=?',[json.dumps(dict(convos_edit_lineage=local))])
        core._archive_touch(db,[('messages','message-b')])
    cfg['controls']['personal']['revision']=2
    incoming={**base,'data':{**base['data'],'metadata':dict(convos_edit_lineage=remote)}}
    proof=row_proof(other,user,'personal',1,incoming,original['revision'])
    projection.apply_row_replicas(path,[dict(row=incoming,proof=proof)],'personal',[cfg['controls']['personal']],local_user=user,local_device=device['id'])
    state=projection.connect(tmp_path/'equal-bases-state.db')
    if cached:
        with monkeypatch.context() as old:
            old.setattr(projection,'ALIAS_VERSION',14)
            old.setattr(projection,'_alias_merge_native',lambda *args:None)
            assert projection.reconcile_provider_aliases(path,cfg,'personal',state=state)['blocked']
    result=projection.reconcile_provider_aliases(path,cfg,'personal',state=state)
    state.close()
    assert not result['blocked']
    with core.open_db(path,True,purpose='test.merged-native-lineage') as db:
        metadata=json.loads(db.execute("SELECT metadata FROM messages WHERE id='message-b'").fetchone()[0])
        assert metadata['convos_edit_lineage']==projection._lineage_union([local,remote])
        assert db.execute("SELECT content,conversation_id FROM messages WHERE id='message-b'").fetchone()==('B','a')
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


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


@pytest.mark.parametrize('replacement',['missing','orphan','present'])
def test_historical_retirement_waits_for_a_present_replacement(tmp_path,replacement):
    with duckdb.connect(str(tmp_path/'history.db')) as db:
        core.init_schema(db)
        db.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}')")
        db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('old','c','user','retained message','{}')")
        body=core.typed_logical_rows(db,[('messages','old','old','','active')])[('messages','old','old','','active')]
        core._insert_pages(db,'parser_retired_rows',[('messages','old','old','',core.provenance_digest(body),body,dict(conversation_id='c',parent_id=None),'new')])
        if replacement!='missing': db.execute("INSERT INTO messages SELECT 'new',* EXCLUDE(id) FROM messages WHERE id='old'")
        if replacement=='orphan': db.execute("UPDATE messages SET parent_id='missing' WHERE id='new'")
        core.retire_parser_rows(db)
        assert set(db.execute('SELECT id,content FROM messages').fetchall())=={(mid,'retained message') for mid in (['old'] if replacement=='missing' else ['old','new'] if replacement=='orphan' else ['new'])}
        assert db.execute('SELECT count(*) FROM parser_retired_rows').fetchone()==(1,)


@pytest.mark.parametrize('historical_reverse',[False,True])
def test_alias_canonical_direction_supersedes_reverse_parser_history(tmp_path,historical_reverse):
    root,path,identity,device,user,cfg,_=archive(tmp_path,False)
    ids=sorted(core.gen_id('codex',str(i)) for i in range(2))
    with duckdb.connect(str(path)) as db:
        db.execute('DELETE FROM messages')
        db.execute("DELETE FROM remote.row_proofs WHERE row_kind='messages'")
        db.executemany("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES (?,'a','user','same',?)",[(mid,json.dumps(dict(provider_index=1))) for mid in ids])
        claims=[('messages',mid,mid,user,'active') for mid in ids]
        bodies=core.typed_logical_rows(db,claims)
        records=[dict(old_id=ids[i],old_hash=core.provenance_digest(bodies[claims[i]]),current_id=ids[1-i],current_hash=core.provenance_digest(bodies[claims[1-i]])) for i in range(2)]
        metadata=json.loads(db.execute("SELECT metadata FROM conversations WHERE id='a'").fetchone()[0])
        db.execute("UPDATE conversations SET metadata=? WHERE id='a'",[json.dumps({**metadata,'convos_message_lineage':dict(v=1,records=records)})])
        if historical_reverse:
            core._insert_pages(db,'parser_retired_rows',[('messages',ids[0],ids[0],user,records[0]['old_hash'],bodies[claims[0]],dict(conversation_id='a',parent_id=None),ids[1])])
    attest(tmp_path,path,cfg)
    with duckdb.connect(str(path),read_only=True) as db:
        carrier=core.typed_logical_rows(db,[('conversations','a','a',user,'active')])[('conversations','a','a',user,'active')]
        proof=projection._heads(db,user,{'conversations':{'a'}})[('conversations','a')]
    accept(root,identity,device,user,SESSION,['a','b'])
    assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':1,'settled':0,'blocked':{}}
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT 1 FROM remote.row_conflicts c JOIN remote.row_proofs p ON p.id=c.proof_id WHERE p.revision=?',[proof['revision']]).fetchone()==(1,)
    for _ in range(2):
        projection.apply_row_replicas(path,[dict(row=carrier,proof=proof)],'personal',cfg['server_state']['workspaces'][0]['controls'],local_user=user)
        assert projection.reconcile_provider_aliases(path,cfg,'personal')=={'changed':0,'settled':1,'blocked':{}}
        with duckdb.connect(str(path),read_only=True) as db:
            assert db.execute('SELECT id FROM messages').fetchall()==[(ids[0],)]
            assert db.execute('SELECT 1 FROM remote.row_proofs WHERE revision=?',[proof['revision']]).fetchone()==(1,)
            assert core.archive_relationships(db)=={}
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_semantic_proof_arrival_invalidates_settled_sync_without_row_mutation(tmp_path):
    from ai_convos_remote import _archive_marker
    root,path,identity,device,user,cfg,_=archive(tmp_path)
    before=_archive_marker(root)
    accept(root,identity,device,user,SESSION,['a','b'])
    after=_archive_marker(root)
    assert before[:4]==after[:4] and before[4]<after[4]


@pytest.mark.parametrize('evidence',['valid','wrong_hash','cycle','fork'])
@pytest.mark.parametrize('settled',[False,True])
def test_alias_normalizes_only_exact_unambiguous_source_lineage(tmp_path,evidence,settled):
    root,path,identity,device,user,cfg,_=archive(tmp_path,False)
    ids=sorted(core.gen_id('codex',str(i)) for i in range(3))
    if settled:
        accept(root,identity,device,user,SESSION,['a','b'])
        assert projection.reconcile_provider_aliases(path,cfg,'personal')['changed']==1
    with duckdb.connect(str(path)) as db:
        db.execute('DELETE FROM messages')
        db.execute("DELETE FROM remote.row_proofs WHERE row_kind='messages'")
        selected=ids if evidence=='fork' else ids[:2]
        db.executemany("INSERT INTO messages(id,conversation_id,role,content,created_at,metadata) VALUES (?,'a','user','same event',?,?)",[(mid,f'2026-01-01T{hour}:00:00',json.dumps(dict(provider_index=1))) for mid,hour in zip(selected,('16','00','01'))])
        claims=[('messages',mid,mid,user,'active') for mid in selected]
        hashes={claim[1]:core.provenance_digest(body) for claim,body in core.typed_logical_rows(db,claims).items()}
        links=[(ids[0],ids[1])]+([(ids[1],ids[0])] if evidence=='cycle' else [(ids[0],ids[2])] if evidence=='fork' else [])
        records=[dict(old_id=old,old_hash='0'*64 if evidence=='wrong_hash' else hashes[old],current_id=new,current_hash=hashes[new]) for old,new in links]
        metadata=json.loads(db.execute("SELECT metadata FROM conversations WHERE id='a'").fetchone()[0])
        db.execute("UPDATE conversations SET metadata=? WHERE id='a'",[json.dumps({**metadata,'convos_message_lineage':dict(v=1,records=records)})])
    attest(tmp_path,path,cfg)
    accept(root,identity,device,user,SESSION,['a','b'])
    assert projection.reconcile_provider_aliases(path,cfg,'personal')['changed']==(1 if not settled or evidence=='valid' else 0)
    with duckdb.connect(str(path),read_only=True) as db:
        found=db.execute("SELECT id,created_at::VARCHAR FROM messages ORDER BY id").fetchall()
        assert found==[(ids[0],'2026-01-01 00:00:00')] if evidence=='valid' else len(found)==len(selected)
        assert not core.archive_relationships(db)
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


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


@pytest.mark.parametrize('parent_evidence',['valid','wrong_hash','fork','cycle'])
@pytest.mark.parametrize('metadata_changed',[False,True])
def test_timestamp_repair_requires_exact_retired_parent_lineage(tmp_path,monkeypatch,parent_evidence,metadata_changed):
    def message(key,parent,stamp,index):
        return core.logical_row('messages',list(data:=dict(id=key,conversation_id='a',role='user',content='event',thinking=None,created_at=stamp,model=None,metadata=dict(provider_index=index),parent_id=parent)),list(data.values()))
    old_parent,new_parent,old,new,alternate=[core.gen_id('codex',label) for label in ('old-parent','new-parent','old','new','alternate')]
    parent=message(new_parent,None,'2026-01-01T00:00:01',0)
    retired=message(old_parent,None,'2026-01-01T00:00:01',0)
    former=message(old,old_parent,'2025-12-31T16:00:02',1)
    current=message(new,new_parent,'2026-01-01T00:00:02',1)
    rows=[parent,{**former,'data':{**former['data'],'parent_id':new_parent}},current]
    records=[dict(old_id=old_parent,old_hash=projection.digest(retired),current_id=new_parent,current_hash=projection.digest(parent)),dict(old_id=old,old_hash=projection.digest(former),current_id=new,current_hash=projection.digest(current))]
    if parent_evidence=='wrong_hash': records[0]['old_hash']='0'*64
    if parent_evidence=='fork': records.append({**records[0],'current_id':alternate})
    if parent_evidence=='cycle': records.append(dict(old_id=new_parent,old_hash=projection.digest(parent),current_id=old_parent,current_hash=projection.digest(retired)))
    carrier=core.logical_row('conversations',list(data:={**dict.fromkeys(core.ROW_FIELDS_V1['conversations']), 'id':'a','source':'codex','metadata':dict(convos_message_lineage=dict(v=1,records=records))}),list(data.values()))
    if metadata_changed: rows[-1]={**current,'data':{**current['data'],'metadata':{**current['data']['metadata'],'convos_edit_lineage':dict(v=1,records=[dict(old_id=old,old_hash='a'*64,current_id=new,current_hash='b'*64)])}}}
    revisions,lineage=projection._alias_message_plan([(row,{},True,{},None) for row in [carrier,*rows]],'codex',['a'],[retired,current])
    assert bool(lineage)==(parent_evidence=='valid')
    if parent_evidence=='valid':
        assert {(row['data']['created_at'],row['data']['parent_id']) for row,*_ in revisions if row['kind']=='messages'}=={('2026-01-01T00:00:02',new_parent)}
    else: assert revisions==[]
    from ai_convos_remote.protocol import certificate,identity,public_id,row_proof
    signer,device=identity('source'),identity('capture')
    user=public_id(signer['sign_public'])
    with core.open_db(tmp_path/'source-forks.db',purpose='test.source-forks') as db:
        core.init_schema(db)
        for body in [carrier,*rows]: core._insert_pages(db,body['kind'],[[body['id'] if k=='id' else json.dumps(body['data'][k]) if k=='metadata' else body['data'].get(k) for k in core.ARCHIVE_COLUMNS[body['kind']]]],core.ARCHIVE_COLUMNS[body['kind']])
        db.execute('INSERT INTO parser_retired_rows VALUES (?,?,?,?,?,?,?,?)',['messages',old_parent,old_parent,'',projection.digest(retired),json.dumps(retired),'{}',new_parent])
        proof=row_proof(device,user,'w',1,current)
        pid=core.project_row_proof(db,proof,signer['sign_public'],certificate(signer,user,device))
        db.execute('INSERT INTO remote.row_conflicts VALUES (?,?)',[pid,json.dumps(current)])
        versions=[rows[1],{**rows[1],'data':rows[-1]['data']}]
        history,loaded,context=projection._message_history,[],{}
        def load(*args):
            loaded.append(args[2])
            return history(*args)
        monkeypatch.setattr(projection,'_message_history',load)
        for _ in range(2):
            if parent_evidence=='valid':
                merged=projection._source_message_join(db,versions,user,context)
                assert merged['data']['created_at']=='2026-01-01T00:00:02.000000'
                assert merged['data']['parent_id']==new_parent
            else:
                with pytest.raises(ValueError,match='content conflict'): projection._source_message_join(db,versions,user,context)
        assert len(loaded)==1
def test_alias_reconciliation_wakes_hooks_after_releasing_capture_lease(tmp_path,monkeypatch):
    from ai_convos import cli as core
    from ai_convos_remote import projection
    data=tmp_path/'data'
    queued=data/'hook_inbox/event.json'
    seen=[]
    def reconcile(*args):
        core.atomic_json(queued,dict(source='codex',path='/unused-test-transcript'))
        return {'changed':0}
    def wake(*args,**kwargs):
        with core.operation_lock(data/'hook_inbox/.drain.lock','test.released',0): seen.append(kwargs)
    monkeypatch.setattr(projection,'_reconcile_provider_aliases',reconcile)
    monkeypatch.setattr(projection,'wake_hooks',wake)
    assert projection.reconcile_provider_aliases(data/'convos.db',{},'test')=={'changed':0}
    assert seen==[{'root':tmp_path}]


@pytest.mark.parametrize('interrupt',[False,True,'retirement'])
def test_message_reconciliation_commits_bounded_pages_and_resumes_with_history(tmp_path,monkeypatch,interrupt):
    root,path,identity,device,user,cfg,_=archive(tmp_path,False)
    with duckdb.connect(str(path)) as db:
        db.execute('DELETE FROM messages')
        db.execute("DELETE FROM remote.row_proofs WHERE row_kind='messages'")
        for cid in ('a','b'):
            for i in range(1,7):
                mid,tid,eid=(core.gen_id('codex',f'{kind}:{cid}:{i}') for kind in ('message','tool','edit'))
                parent=core.gen_id('codex',f'message:{cid}:{i-1}') if i>1 else None
                db.execute("INSERT INTO messages(id,conversation_id,role,content,metadata,parent_id) VALUES (?,?,'assistant',?,?,?)",[mid,cid,f'turn {i}',json.dumps(dict(provider_index=i)),parent])
                db.execute("INSERT INTO tool_calls VALUES (?,?,'shell','{}','\"retained payload\"','complete',NULL,NULL)",[tid,mid])
                db.execute("INSERT INTO file_edits VALUES (?,?,'shared.py','write','retained edit',NULL,NULL)",[eid,mid])
                db.execute("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','test',?)",[eid,tid])
    core.capture_provenance(path)
    attest(tmp_path,path,cfg)
    accept(root,identity,device,user,SESSION,['a','b'])
    calls=[]
    write=projection.project_logical_rows
    monkeypatch.setattr(projection,'ALIAS_WRITE_PAGE',2,raising=False)
    def recording(db,items,*args,**kwargs):
        if db.audit[0]=='remote.alias.message-lineage':
            calls.append(len(items))
            if interrupt is True and len(calls)==2 or interrupt=='retirement' and any(row['kind']=='conversations' for row,*_ in items): raise RuntimeError('interrupted after committed repair page')
        return write(db,items,*args,**kwargs)
    monkeypatch.setattr(projection,'project_logical_rows',recording)
    result=projection.reconcile_provider_aliases(path,cfg,'personal')
    if interrupt:
        assert result['blocked'] and len(calls)>=2
        with duckdb.connect(str(path),read_only=True) as db:
            assert core.archive_relationships(db)=={}
            assert db.execute('SELECT count(*) FROM messages').fetchone()==(12,)
        assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0
        monkeypatch.setattr(projection,'project_logical_rows',write)
        result=projection.reconcile_provider_aliases(path,cfg,'personal')
    assert not result['blocked']
    assert max(calls)<=4
    with duckdb.connect(str(path),read_only=True) as db:
        assert core.archive_relationships(db)=={}
        assert db.execute('SELECT count(*) FROM messages').fetchone()==(6,)
        assert db.execute("SELECT count(*) FROM tool_calls WHERE output='\"retained payload\"'").fetchone()==(12,)
        assert db.execute("SELECT count(*) FROM file_edits WHERE content='retained edit'").fetchone()==(12,)
        assert db.execute("SELECT count(*) FROM parser_retired_rows WHERE kind='messages'").fetchone()==(6,)
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0


def test_alias_reconciles_committed_native_source_update_before_attestation(tmp_path):
    root,path,identity,device,user,cfg,_=_provider_alias_archive(tmp_path)
    with core.open_db(path,purpose='test.alias.source') as db,core._transaction(db),core.preserve_fact_heads(db,[('messages','message-b')]):
        db.execute("UPDATE messages SET content='new committed source content' WHERE id='message-b'")
        core._archive_touch(db,[('messages','message-b')])
    result=projection.reconcile_provider_aliases(path,cfg,'personal')
    assert not result['blocked']
    with duckdb.connect(str(path),read_only=True) as db:
        assert db.execute('SELECT conversation_id,content FROM messages').fetchall()==[('a','new committed source content')]
    assert projection.audit_rows(path,local_user=user)['totals']['unavailable']==0
