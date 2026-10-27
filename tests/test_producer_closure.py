"""Producer closure: turn-major ingest parts, the evidence rules of a chunked write, the open-ingest marker the signer honours, and convergence of torn heads."""
import copy, functools, json, os, subprocess

import pytest
import ai_convos.cli as cli
import ai_convos_remote as remote_client
import ai_convos_remote.projection as P
from ai_convos_remote import setup_client, sync_once
from ai_convos_remote.protocol import certificate, identity, public, public_id
from ai_convos_remote_server import action, connect as server_connect

KINDS=("convs","msgs","tools","attachs","artifacts","edits","edit_evidence")
def session(path,n,text="turn"):
    events,prev=[],None
    for i in range(n):
        tu,au,uu=f"toolu_{i:04d}",f"a{i:04d}",f"u{i:04d}"
        events.append({"type":"assistant","uuid":au,"parentUuid":prev,"sessionId":"sess1","timestamp":f"2026-10-23T10:{i//60:02d}:{i%60:02d}.000Z","cwd":"/tmp/x","message":{"model":"m","content":[{"type":"text","text":f"{text} {i}"},{"type":"tool_use","id":tu,"name":"Edit","input":{"file_path":f"/tmp/x/f{i}.py","old_string":"a","new_string":"b"}}]}})
        events.append({"type":"user","uuid":uu,"parentUuid":au,"sessionId":"sess1","timestamp":f"2026-10-23T10:{i//60:02d}:{i%60:02d}.500Z","cwd":"/tmp/x","message":{"content":[{"type":"tool_result","tool_use_id":tu,"content":"ok"}]}})
        prev=uu
    path.parent.mkdir(parents=True,exist_ok=True); path.write_text("".join(json.dumps(e)+"\n" for e in events)); return path
def parsed(path): return cli.hook_result("claude-code",path)
def query(sql,params=(),path=None):
    with cli._core(path,read_only=True,purpose="test.read") as db: return db.execute(sql,params).fetchall()
def generation(path=None): return query("SELECT generation FROM archive_state WHERE singleton",path=path)[0][0]
def markers(path=None): return query("SELECT count(*) FROM open_ingests",path=path)[0][0] if query("SELECT 1 FROM information_schema.tables WHERE table_name='open_ingests'",path=path) else -1
HISTORY="SELECT id FROM messages WHERE json_extract(metadata,'$.history_of') IS NOT NULL"
def proofs(path=None):
    # History rows carry the wall-clock time of their supersession, so their proofs are counted, not compared.
    return sorted(query(f"SELECT row_kind,source_row_id,content_hash FROM remote.row_proofs WHERE state='active' AND source_row_id NOT IN ({HISTORY})",path=path)),query(f"SELECT count(*) FROM remote.row_proofs WHERE state='active' AND source_row_id IN ({HISTORY})",path=path)[0][0]
def tables(path=None):
    messages=[(*r[:7],json.dumps({k:v for k,v in json.loads(r[7]).items() if k!="superseded_at"},sort_keys=True),*r[8:]) for r in query("SELECT * FROM messages ORDER BY id",path=path)]
    return {t:query(f"SELECT * FROM {t} ORDER BY id",path=path) for t in ("conversations","tool_calls","file_edits")}|{"messages":messages,"evidence":query("SELECT * FROM provenance.file_edit_evidence ORDER BY 1",path=path)}
def mismatches(path,user): return {k:v["projection_mismatch"] for k,v in P.audit_rows(path,local_user=user)["tables"].items()}
def fresh_archive(monkeypatch,path):
    monkeypatch.setattr(cli,"DB_PATH",path)
    with cli._core(ready=True,purpose="test.schema"): pass
def signer(state_path,ws="w"):
    # A one-device signer without a relay: what sync_once does for the personal workspace, called by hand between ingest parts.
    root,device=identity("root"),identity("agent"); user=public_id(root["sign_public"])
    entry=dict(user=user,root_public=root["sign_public"],device=public(device),certificate=certificate(root,user,device),history=True)
    control=dict(workspace=ws,revision=1,epoch=1,devices={device["id"]:entry})
    cfg=dict(user=user,device=device,workspaces={ws:dict(epoch=1,kind="personal",name="p")},controls={ws:dict(epoch=1,revision=1,devices={device["id"]:entry})},server_state=dict(workspaces=[dict(id=ws,controls=[control],device_authorized=True,kind="personal")]),keys={})
    state=P.connect(state_path)
    def sign(since=None):
        covered=getattr(P,"closed_generation",generation)(cli.DB_PATH)
        return P.attest_rows(cli.DB_PATH,cfg,ws,P.scan_archive(cli.DB_PATH,state,"personal",(),(),ws,None,("cwd","edit"),user,covered,None,2500,since),()),covered
    return user,sign
def between_parts(monkeypatch,hook):
    real=cli.archive_yield
    monkeypatch.setattr(cli,"archive_yield",lambda path=None:(hook(),real(path))[1])

def test_turn_major_parts_keep_a_turn_and_its_evidence_together():
    msg=lambda i:dict(id=f"m{i}",conversation_id="c",role="assistant",content="x",thinking=None,created_at=None,model=None,metadata="{}",parent_id=f"m{i-1}" if i else None)
    tool=lambda tid,mid:dict(id=tid,message_id=mid,tool_name="apply_patch",input="{}",output="{}",status="complete",duration_ms=None,created_at=None)
    edit=lambda eid,mid:dict(id=eid,message_id=mid,file_path=f"{eid}.py",edit_type="write",content="x",created_at=None,old_content=None)
    evidence=lambda eid,tid:dict(file_edit_id=eid,status="confirmed",reason="tool_success",tool_call_id=tid)
    small,big=[(f"t{i}",f"m{i}",f"e{i}") for i in range(40)],[(f"b{j}","m39",f"x{j}") for j in range(30)]
    r=cli.ParseResult(convs=[dict(id="c",source="codex",title="t",created_at=None,updated_at=None,model=None,cwd=None,git_branch=None,project_id=None,metadata="{}")],msgs=[msg(i) for i in reversed(range(40))],tools=[tool(t,m) for t,m,e in small+big],attachs=[dict(id=f"a{i}",message_id=f"m{i}",filename="f",mime_type="text/plain",size=1,path=None,url=None,created_at=None) for i in range(40)],artifacts=[dict(id="art",conversation_id="c",artifact_type="code",title="t",content="x",language=None,created_at=None,version=1)],edits=[edit(e,m) for t,m,e in small+big],edit_evidence=[evidence(e,t) for t,m,e in small+big])
    parts=cli.ingest_parts(copy.deepcopy(r),size=20)
    at={(k,v.get("id",v.get("file_edit_id"))):i for i,p in enumerate(parts) for k in KINDS for v in getattr(p,k)}
    assert sum(len(getattr(p,k)) for p in parts for k in KINDS)==len(at)==1+40+70+40+1+70+70 and len(parts)>=13
    assert all(sum(len(getattr(p,k)) for k in KINDS)<=20 for p in parts) and at[("convs","c")]==at[("artifacts","art")]==0
    assert all(at[("tools",t)]==at[("edits",e)]==at[("edit_evidence",e)]>=at[("msgs",m)] for t,m,e in small+big) and all(at[("attachs",f"a{i}")]>=at[("msgs",f"m{i}")] for i in range(40))
    assert all(at[("tools",t)]==at[("attachs",f"a{i}")]==at[("msgs",m)] for i,(t,m,e) in enumerate(small[:39])) and len({at[("tools",t)] for t,m,e in big})>1
    assert all(at[("msgs",f"m{i}")]<=at[("msgs",f"m{i+1}")] for i in range(39)) and all(p.scopes is not None and p.edit_scopes is not None for p in parts)

def test_signer_between_parts_mints_nothing_until_the_last_part_commits(tmp_path,monkeypatch):
    src=session(tmp_path/"projects/-tmp-x/sess1.jsonl",40)
    fresh_archive(monkeypatch,tmp_path/"archive.db"); monkeypatch.setattr(cli,"ingest_parts",functools.partial(cli.ingest_parts,size=100))
    cli.commit_result(parsed(src),"test.first")
    user,sign=signer(tmp_path/"state.db")
    minted,start=sign(); assert minted==121
    seen=[]
    between_parts(monkeypatch,lambda:seen.append((sign(seen[-1][1] if seen else start)[0],getattr(P,"closed_generation",generation)(cli.DB_PATH),markers(),generation())))
    for text,expected in (("turn",0),("redo",80)):  # a changed assistant message is a revision plus a history row
        del seen[:]; cli.commit_result(parsed(session(src,40,text)),"test.again")
        assert [minted for minted,covered,open_,real in seen[:-1]]==[0]*(len(seen)-1), seen  # nothing is signed while the multi-part write is open
        assert len(seen)==3 and seen[-1][0]==expected and all(covered==start and open_==1 for minted,covered,open_,real in seen[:-1]) and seen[-1][1:]==(generation(),0,generation()), seen
    assert {n for n, in query("SELECT count(*) FROM remote.row_proofs WHERE row_kind='messages' GROUP BY source_row_id")}=={1,2} and query("SELECT count(*) FROM remote.row_proofs WHERE row_kind='messages' AND previous_revision IS NOT NULL")==[(40,)]
    assert query("SELECT count(*) FROM remote.row_conflicts")==[(0,)] and set(mismatches(cli.DB_PATH,user).values())=={0}

def test_evidence_default_and_relink_keep_tool_ownership_and_resign_both_parents(tmp_path,monkeypatch):
    r=parsed(session(tmp_path/"projects/-tmp-x/sess1.jsonl",3)); fresh_archive(monkeypatch,tmp_path/"archive.db"); cli.commit_result(r,"test.first")
    user,sign=signer(tmp_path/"state.db"); sign()
    e,m,(t1,t2)=r.edits[0]["id"],r.edits[0]["message_id"],[t["id"] for t in r.tools[:2]]
    before=query("SELECT * FROM provenance.file_edit_evidence ORDER BY 1")
    with cli._core(purpose="test.edits-only") as db,cli._transaction(db): cli.upsert(db,cli.ParseResult(edits=[*r.edits,dict(r.edits[1],id="fresh")],scopes=[],edit_scopes=[]))
    assert query("SELECT * FROM provenance.file_edit_evidence ORDER BY 1")==sorted([*before,("fresh","unknown","unclassified_input",None)]) and sign()[0]==1  # the new edit joins its message
    g=generation()
    with cli._core(purpose="test.relink") as db,cli._transaction(db): cli.upsert(db,cli.ParseResult(edit_evidence=[dict(file_edit_id=e,status="confirmed",reason="provider_success",tool_call_id=t2)],scopes=[],edit_scopes=[]))
    assert set(query("SELECT kind,entity FROM archive_changes WHERE generation>?",[g]))>={("tool_calls",t1),("tool_calls",t2),("file_edits",e)} and ("messages",m) not in set(query("SELECT kind,entity FROM archive_changes WHERE generation>?",[g]))
    assert sign(g)[0]==2 and dict(query("SELECT source_row_id,count(*) FROM remote.row_proofs WHERE source_row_id IN (?,?,?) GROUP BY 1",[t1,t2,m]))=={t1:2,t2:2,m:1} and set(mismatches(cli.DB_PATH,user).values())=={0}

def test_part_size_does_not_change_the_archive_or_the_proofs(tmp_path,monkeypatch):
    src=session(tmp_path/"projects/-tmp-x/sess1.jsonl",60); first=parsed(src); second=parsed(session(src,60,"redo")); parts=cli.ingest_parts; outcomes={}
    for size in (100,500,5000):
        fresh_archive(monkeypatch,tmp_path/f"{size}.db"); monkeypatch.setattr(cli,"ingest_parts",functools.partial(parts,size=size))
        cli.commit_result(copy.deepcopy(first),"test.first"); user,sign=signer(tmp_path/f"{size}-state.db"); sign(); cli.commit_result(copy.deepcopy(second),"test.again"); sign()
        outcomes[size]=(tables(),proofs(),markers(),mismatches(cli.DB_PATH,user))
    assert len({json.dumps(v,sort_keys=True,default=str) for v in outcomes.values()})==1 and outcomes[100][2]==0 and (len(outcomes[100][1][0]),outcomes[100][1][1])==(1+60*2+60+60,60)

def test_crash_at_any_part_boundary_restarts_to_the_uninterrupted_result(tmp_path,monkeypatch):
    src=session(tmp_path/"projects/-tmp-x/sess1.jsonl",50); first=parsed(src); second=parsed(session(src,50,"redo")); real,parts=cli.archive_yield,len(cli.ingest_parts(copy.deepcopy(second),size=100))
    monkeypatch.setattr(cli,"ingest_parts",functools.partial(cli.ingest_parts,size=100))
    fresh_archive(monkeypatch,tmp_path/"reference.db"); cli.commit_result(copy.deepcopy(first),"test.first"); user,sign=signer(tmp_path/"reference-state.db"); sign(); cli.commit_result(copy.deepcopy(second),"test.again"); sign()
    reference=(tables(),proofs()); dead=subprocess.Popen(["true"]); dead.wait()
    assert parts==3
    for k in range(1,parts+1):
        fresh_archive(monkeypatch,tmp_path/f"crash{k}.db"); cli.commit_result(copy.deepcopy(first),"test.first"); user,sign=signer(tmp_path/f"crash{k}-state.db"); sign(); start,calls=generation(),[0]
        def crash(path=None,k=k): return (calls.__setitem__(0,calls[0]+1),calls[0]==k and (_ for _ in ()).throw(RuntimeError("crash")),real(path))[-1]
        monkeypatch.setattr(cli,"archive_yield",crash)
        with pytest.raises(RuntimeError,match="crash"): cli.commit_result(copy.deepcopy(second),"test.again")
        assert calls==[k] and markers()==(k<parts) and generation()>start
        if k<parts:
            assert query("SELECT pid,generation FROM open_ingests")==[(os.getpid(),start)] and P.closed_generation(cli.DB_PATH)==start
            assert sign()[0]==0
            with cli._core(purpose="test.stale") as db: db.execute("UPDATE open_ingests SET heartbeat_at=heartbeat_at-?",[cli.OPEN_INGEST_STALE+1])
            assert P.closed_generation(cli.DB_PATH)==generation()
            with cli._core(purpose="test.dead") as db: db.execute("UPDATE open_ingests SET heartbeat_at=heartbeat_at+?,pid=?",[cli.OPEN_INGEST_STALE+1,dead.pid])
            assert P.closed_generation(cli.DB_PATH)==generation()
            with cli._core(purpose="test.alive") as db: db.execute("UPDATE open_ingests SET pid=?",[os.getpid()])
        monkeypatch.setattr(cli,"archive_yield",real); cli.commit_result(copy.deepcopy(second),"test.restart"); sign()
        assert markers()==0 and (tables(),proofs())==reference

def test_legacy_torn_head_converges_on_producer_and_receiver(tmp_path,monkeypatch):
    server=server_connect(tmp_path/"server.db")
    monkeypatch.setattr(remote_client,"request",lambda cfg,body,auth=True:action(server,body,cfg.get("token") if auth else None)); monkeypatch.setattr(remote_client,"drain_hooks",lambda:None)
    roots=[tmp_path/name for name in ("laptop","desktop")]
    a,recovery=setup_client("http://server","alice",device="laptop",root=roots[0]); setup_client("http://server","alice",device="desktop",recovery=recovery,root=roots[1])
    try:
        path,user=roots[0]/"data/convos.db",a["user"]; fresh_archive(monkeypatch,path)
        r=parsed(session(tmp_path/"projects/-tmp-x/sess1.jsonl",3)); cli.commit_result(r,"test.first"); sync_once(roots[0],manual=True)
        e,m,t=r.edits[0]["id"],r.edits[0]["message_id"],r.edit_evidence[0]["tool_call_id"]
        assert query("SELECT count(*) FROM remote.row_proofs WHERE source_row_id=?",[m])==[(1,)]
        # The b25 torn window, replayed: the edits part downgraded the evidence and touched the edit (so its new parent, the message); the signer minted B.
        with cli._core(purpose="test.b25.edits") as db,cli._transaction(db): db.execute("UPDATE provenance.file_edit_evidence SET status='unknown',reason='unclassified_input',tool_call_id=NULL WHERE file_edit_id=?",[e]); cli._archive_touch(db,[("file_edits",e)])
        sync_once(roots[0],manual=True)
        claim=("messages",m,m,user,"active")
        with cli._core(read_only=True,purpose="test.read") as db: body=cli.typed_logical_rows(db,[claim])[claim]
        (bid,brev),=query("SELECT id,revision FROM remote.row_proofs WHERE source_row_id=? AND previous_revision IS NOT NULL",[m])
        # Then the evidence part restored the tool-call link without touching the message; b25 retained B's body under B and recorded B as the local base.
        with cli._core(purpose="test.b25.evidence") as db,cli._transaction(db): db.execute("UPDATE provenance.file_edit_evidence SET status='confirmed',reason='provider_success',tool_call_id=? WHERE file_edit_id=?",[t,e]); db.execute("INSERT OR IGNORE INTO remote.row_conflicts VALUES (?,?)",[bid,json.dumps(body,sort_keys=True,separators=(",",":"))]); db.execute("INSERT OR REPLACE INTO remote.local_row_bases VALUES ('messages',?,?,?)",[m,user,brev])
        assert mismatches(path,user)["messages"]==1
        state=P.connect(roots[0]/"remote/state.db"); state.execute("DELETE FROM meta WHERE key='retained_rows_checked'"); state.commit(); state.close()
        for _ in range(2):
            sync_once(roots[0],manual=True)
            chain=query("SELECT previous_revision,content_hash FROM remote.row_proofs WHERE source_row_id=? ORDER BY previous_revision IS NOT NULL,previous_revision=?",[m,brev])
            assert len(chain)==3 and chain[2][0]==brev and chain[2][1]==chain[0][1]!=chain[1][1] and set(mismatches(path,user).values())=={0} and query("SELECT count(*) FROM remote.row_conflicts WHERE proof_id=?",[bid])==[(0,)]
        sync_once(roots[1],manual=True); peer=roots[1]/"data/convos.db"
        assert query("SELECT status,tool_call_id FROM provenance.file_edit_evidence WHERE file_edit_id=?",[e],peer)==[("confirmed",t)] and set(mismatches(peer,user).values())=={0}
        assert query("SELECT content FROM messages WHERE id=?",[m],peer)==query("SELECT content FROM messages WHERE id=?",[m],path) and query("SELECT count(*) FROM remote.row_origins",path=peer)==[(1+6+3,)]
    finally: server.close()
