"""Compression changes representation, never signed content, identity, or authorization."""
import copy, json, os, sqlite3
from contextlib import closing

import pytest
import zstandard
import duckdb
import ai_convos_remote as client
import ai_convos_remote.protocol as protocol
import ai_convos_remote_server as server
from tests.test_remote_server import account, create_ws, rotate_ws


def message(author,workspace,key,identity="m",epoch=1,compression="none"):
    columns=["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"]
    row=protocol.logical_row("messages",columns,[identity,"c","assistant","repeated tool output\n"*10000,None,None,None,'{"original":"unchanged"}',None])
    proof=protocol.row_proof(author["device"],author["user"],workspace,epoch,row)
    return protocol.seal_replica(row,proof,workspace,epoch,key,author["device"]["id"],compression=compression)


def configured(db,kind="personal"):
    author=account(db,"alice")
    ws,key="personal",bytes(range(32))
    control=create_ws(db,author,ws,key,kind)
    cfg={**author,"controls":{ws:control},"keys":{f"{ws}:1":protocol.b64(key)},"workspaces":{ws:{"kind":"personal","epoch":1}}}
    cfg["server_state"]=server.action(db,{"op":"state"},author["token"])
    return cfg,ws,key


def test_compressed_and_legacy_replicas_have_identical_signed_bytes(tmp_path):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        legacy=message(cfg,ws,key)
        compressed=protocol.repack_replica(legacy,key)
        assert legacy["v"]==1 and compressed["v"]==2 and compressed["compression"]=="zstd"
        assert compressed["replica"]==legacy["replica"] and compressed["nonce"]!=legacy["nonce"]
        assert len(protocol.canon(compressed))<len(protocol.canon(legacy))//10
        assert protocol.open_replica(legacy,key,True)==protocol.open_replica(compressed,key,True)
        body=protocol.open_replica(compressed,key)
        default=protocol.seal_replica(body["row"],body["proof"],ws,1,key,cfg["device"]["id"])
        assert default["v"]==2 and protocol.open_replica(default,key,True)==protocol.open_replica(legacy,key,True)
        assert protocol.verify_row_proof(body["proof"],body["row"],protocol.certificate(cfg["root"],cfg["user"],cfg["device"]),cfg["root"]["sign_public"])
        assert protocol.open_replica(protocol.repack_replica(compressed,key,"none"),key)==body


@pytest.mark.parametrize("change",[{"compression":"future"},{"plaintext_size":True},{"plaintext_size":0},{"plaintext_size":protocol.REPLICA_PLAIN_LIMIT+1},{"v":3},{"unexpected":1}])
def test_unsupported_compression_and_invalid_sizes_fail_closed(tmp_path,change):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        env=protocol.repack_replica(message(cfg,ws,key),key)
        with pytest.raises(ValueError,match="replica"): protocol.open_replica({**env,**change},key)


def test_size_metadata_is_authenticated_before_decompression(tmp_path,monkeypatch):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        env=protocol.repack_replica(message(cfg,ws,key),key)
        monkeypatch.setattr(zstandard,"get_frame_parameters",lambda raw:pytest.fail("must authenticate before reading compressed frame"))
        with pytest.raises(ValueError,match="replica"): protocol.open_replica({**env,"plaintext_size":env["plaintext_size"]-1},key)


@pytest.mark.parametrize("damage",["wrong_size","unknown_size","truncated","trailing","concatenated","oversized_window"])
def test_authenticated_but_invalid_compressed_frames_are_rejected(tmp_path,monkeypatch,damage):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        old=message(cfg,ws,key)
        raw=protocol.open_replica(old,key,True)
        packed=zstandard.ZstdCompressor(write_content_size=damage!="unknown_size").compress(raw)
        if damage=="truncated": packed=packed[:-1]
        if damage=="trailing": packed+=b"extra"
        if damage=="concatenated": packed+=zstandard.ZstdCompressor().compress(b"another frame")
        header={**{k:v for k,v in old.items() if k!="ciphertext"},"v":2,"compression":"zstd","plaintext_size":len(raw)+(damage=="wrong_size")}
        env=protocol._seal(header,packed,key)
        if damage=="oversized_window":
            from types import SimpleNamespace
            monkeypatch.setattr(zstandard,"get_frame_parameters",lambda data:SimpleNamespace(content_size=len(raw),window_size=protocol.REPLICA_PLAIN_LIMIT+1,dict_id=0))
            monkeypatch.setattr(zstandard,"ZstdDecompressor",lambda:pytest.fail("oversized window must be rejected before allocation"))
        with pytest.raises(ValueError,match="replica"): protocol.open_replica(env,key)


def test_small_or_incompressible_bytes_keep_the_legacy_representation():
    key=bytes(range(32))
    header={"v":1,"nonce":protocol.b64(os.urandom(12))}
    raw=os.urandom(128)
    env=protocol._seal_replica(header,raw,key,"zstd")
    assert env["v"]==1 and "compression" not in env
    assert protocol._open(env,header,key,True)[1]==raw


def test_replica_replacement_is_conditional_smaller_and_preserves_cursors(tmp_path):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        env=message(cfg,ws,key)
        original=server.action(db,{"op":"replica_upload_many","envelopes":[env]},cfg["token"])["replicas"][0]
        before=server.action(db,{"op":"state"},cfg["token"])["workspaces"][0]["sync"]
        compressed=protocol.repack_replica(env,key)
        item={"semantic":False,"expected_wire_hash":protocol.digest(env),"envelope":compressed}
        replace=lambda value:server.action(db,{"op":"replica_replace_many","replacements":[value]},cfg["token"])["replicas"][0]
        changed=replace(item)
        assert changed["replaced"] and changed["cursor"]==original["cursor"] and not changed["created"]
        assert replace(item)["conflict"]
        larger=replace({**item,"expected_wire_hash":protocol.digest(compressed),"envelope":env})
        assert not larger["replaced"] and not larger["conflict"]
        after=server.action(db,{"op":"state"},cfg["token"])["workspaces"][0]["sync"]
        assert after==before
        assert server.action(db,{"op":"replica_pull","workspace":ws},cfg["token"])["replicas"]==[{"cursor":original["cursor"],"envelope":compressed}]


def test_client_migrates_both_channels_and_resumes_after_lost_ack(tmp_path,monkeypatch):
    with closing(server.connect(tmp_path/"relay.db")) as db,closing(client.connect(tmp_path/"state.db")) as state:
        cfg,ws,key=configured(db)
        original=[message(cfg,ws,key,str(i)) for i in range(2)]
        for semantic,env in enumerate(original): server.action(db,{"op":"replica_upload_many","envelopes":[env],"semantic":bool(semantic)},cfg["token"])
        failed=[]
        def request(cfg,body):
            result=server.action(db,body,cfg["token"])
            if body["op"]=="replica_replace_many" and not failed:
                failed.append(True)
                raise ConnectionError("lost acknowledgement")
            return result
        monkeypatch.setattr(client,"request",request)
        with pytest.raises(ConnectionError): client.repack_replicas(cfg,state,ws)
        assert state.execute("SELECT * FROM meta WHERE key LIKE 'replica_compression_cursor:%'").fetchall()==[]
        resumed=client.repack_replicas(cfg,state,ws)
        assert resumed["scanned"]==0 and resumed["replaced"]==0
        assert client.repack_replicas(cfg,state,ws)["scanned"]==0
        retained=server.action(db,{"op":"replica_pull","workspace":ws,"semantic":True},cfg["token"])["replicas"]
        assert len(retained)==2
        for old,item in zip(original,retained): assert protocol.open_replica(old,key,True)==protocol.open_replica(item["envelope"],key,True)
        assert db.execute("SELECT COUNT(*) FROM row_replicas").fetchone()[0]==db.execute("SELECT COUNT(*) FROM semantic_replicas").fetchone()[0]==1


def test_repack_inventory_and_replacement_cannot_touch_another_uploader(tmp_path):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db,"team")
        other=account(db,"bob")
        rotate_ws(db,cfg,cfg["controls"][ws],key,((cfg,"admin"),(other,"member")))
        env=message(cfg,ws,key,epoch=2)
        server.action(db,{"op":"replica_upload_many","envelopes":[env]},cfg["token"])
        assert not server.action(db,{"op":"replica_repack_pull","workspace":ws},other["token"])["replicas"]
        replacement={"semantic":False,"expected_wire_hash":protocol.digest(env),"envelope":protocol.repack_replica(env,key)}
        with pytest.raises(PermissionError): server.action(db,{"op":"replica_replace_many","replacements":[replacement]},other["token"])
        assert server.stored_envelope(db.execute("SELECT * FROM row_replicas").fetchone())==env


@pytest.mark.parametrize("capabilities",[{}, {"replica_repack":1}])
def test_compaction_requires_relay_capability_before_touching_state(capabilities):
    cfg={"server_state":{"capabilities":capabilities}}
    original=copy.deepcopy(cfg)
    with pytest.raises(ValueError,match="upgrade the relay"): client.repack_replicas(cfg,None,"ws")
    assert cfg==original


def test_pages_bound_expanded_bytes_even_for_small_encrypted_payloads(tmp_path):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db)
        compressed=protocol.repack_replica(message(cfg,ws,key),key)
        # An opaque relay cannot check whether the uploader's claimed size is true.
        envs=[{**compressed,"replica":f"{i:064x}","plaintext_size":32*1024**2} for i in range(3)]
        server.action(db,{"op":"replica_upload_many","envelopes":envs},cfg["token"])
        page=server.action(db,{"op":"replica_pull","workspace":ws},cfg["token"])["replicas"]
        assert len(page)==1 and client.replica_page(page)==page
        with pytest.raises(ValueError,match="decompression limits"): client.replica_page([{"envelope":env} for env in envs])


@pytest.mark.parametrize("supports_compression",[True,False])
def test_normal_upload_automatically_negotiates_compression_and_projects_exactly(tmp_path,monkeypatch,supports_compression):
    from tests.test_remote_client import replicate_conversation, transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        direct=transport(db)
        def request(cfg,body,auth=True):
            if body["op"]=="replica_upload_many" and not supports_compression: assert all(env["v"]==1 for env in body["envelopes"])
            response=direct(cfg,body,auth)
            if body["op"]=="state" and not supports_compression: response["capabilities"].pop("replica_compression")
            return response
        monkeypatch.setattr(client,"request",request)
        monkeypatch.setattr(client,"drain_hooks",lambda:None)
        author,reader=tmp_path/"author",tmp_path/"reader"
        cfg,recovery=client.setup_client("http://server","alice","laptop",root=author)
        ws=client.workspace(cfg,"Personal")
        title="An exact retained conversation title. "*1000
        envs=replicate_conversation(author,ws,title)
        assert envs and all(env["v"]==(2 if supports_compression else 1) for env in envs)
        assert "replica_compression" not in client.load(author)
        client.setup_client("http://server","alice","desktop",recovery,root=reader)
        client.sync_once(reader,manual=True)
        with closing(duckdb.connect(str(reader/"data/convos.db"),read_only=True)) as archive:
            assert archive.execute("SELECT title FROM conversations").fetchall()==[(title,)]


def test_compact_command_migrates_existing_replicas_without_configuration(tmp_path,monkeypatch):
    from typer.testing import CliRunner
    from tests.test_remote_client import transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        monkeypatch.setattr(client,"request",transport(db))
        root=tmp_path/"client"
        monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(root))
        cfg,_=client.setup_client("http://server","alice",root=root)
        ws=client.workspace(cfg,"Personal")
        key=client.key(cfg,ws,1)
        original=message(cfg,ws,key)
        server.action(db,{"op":"replica_upload_many","envelopes":[original]},cfg["token"])
        runner=CliRunner()
        first=runner.invoke(client.remote,["compact","Personal"])
        assert first.exit_code==0,first.output
        assert json.loads(first.stdout)["replaced"]==1
        second=runner.invoke(client.remote,["compact","Personal"])
        assert second.exit_code==0 and json.loads(second.stdout)["scanned"]==0
        restart=runner.invoke(client.remote,["compact","Personal","--restart"])
        assert restart.exit_code==0 and json.loads(restart.stdout)["scanned"]==0 and json.loads(restart.stdout)["replaced"]==0
        stored=server.stored_envelope(db.execute("SELECT * FROM row_replicas").fetchone())
        assert stored["v"]==2 and protocol.open_replica(stored,key,True)==protocol.open_replica(original,key,True)
        assert "replica_compression" not in client.load(root)


def test_client_does_not_rewrite_or_advance_past_an_unauthentic_replica(tmp_path,monkeypatch):
    with closing(server.connect(tmp_path/"relay.db")) as db,closing(client.connect(tmp_path/"state.db")) as state:
        cfg,ws,key=configured(db)
        env=message(cfg,ws,key)
        raw=protocol.unb64(env["ciphertext"])
        corrupted={**env,"ciphertext":protocol.b64(bytes([raw[0]^1])+raw[1:])}
        server.action(db,{"op":"replica_upload_many","envelopes":[corrupted]},cfg["token"])
        monkeypatch.setattr(client,"request",lambda cfg,body:server.action(db,body,cfg["token"]))
        with pytest.raises(ValueError,match="invalid row replica"): client.repack_replicas(cfg,state,ws)
        assert not state.execute("SELECT * FROM meta WHERE key LIKE 'replica_compression_cursor:%'").fetchall()
        assert server.stored_envelope(db.execute("SELECT * FROM row_replicas").fetchone())==corrupted


def test_compaction_skips_compressed_bodies_on_relay_and_advances_without_local_scan(tmp_path,monkeypatch):
    with closing(server.connect(tmp_path/"relay.db")) as db,closing(client.connect(tmp_path/"state.db")) as state:
        cfg,ws,key=configured(db)
        env=message(cfg,ws,key,compression="zstd")
        server.action(db,{"op":"replica_upload_many","envelopes":[env]},cfg["token"])
        calls=[]
        def request(cfg,body):
            assert body["op"]=="replica_repack_pull"
            response=server.action(db,body,cfg["token"])
            calls.append(response)
            return response
        monkeypatch.setattr(client,"request",request)
        monkeypatch.setattr(client,"repack_index",lambda *args:pytest.fail("no local archive scan for compressed replicas"))
        db.set_authorizer(lambda op,table,column,*args:sqlite3.SQLITE_DENY if op==sqlite3.SQLITE_READ and table in ("row_replicas","semantic_replicas") and column=="ciphertext" else sqlite3.SQLITE_OK)
        result=client.repack_replicas(cfg,state,ws)
        assert result["cursor"]>0 and result["scanned"]==result["downloaded"]==0
        assert calls==[{"cursor":result["cursor"],"replicas":[]}]
        assert client.repack_replicas(cfg,state,ws,restart=True)==result


@pytest.mark.parametrize("change",["none","changed","history"])
def test_compaction_reconstructs_exact_local_rows_and_fetches_only_unavailable_bodies(tmp_path,monkeypatch,change):
    from ai_convos.cli import open_db
    from tests.test_remote_client import replicate_conversation, transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        author=tmp_path/"author"
        direct=transport(db)
        modern=False
        calls=[]
        def request(cfg,body,auth=True):
            if modern:
                with open_db(author/"data/convos.db",wait=0,purpose="test.compaction.concurrent-writer"): pass
            calls.append(body["op"])
            response=direct(cfg,body,auth)
            if body["op"]=="state" and not modern: response["capabilities"].pop("replica_compression")
            return response
        monkeypatch.setattr(client,"request",request)
        cfg,_=client.setup_client("http://server","alice",root=author)
        ws=client.workspace(cfg,"Personal")
        original=replicate_conversation(author,ws,"original signed title "*1000)
        if change=="history": original+=replicate_conversation(author,ws,"new signed title "*1000)
        if change=="changed":
            with closing(duckdb.connect(str(author/"data/convos.db"))) as local: local.execute("UPDATE conversations SET title='changed after upload'")
        modern=True
        cfg=client.load(author)
        client.refresh(cfg,author)
        before=protocol.digest((author/"data/convos.db").read_bytes())
        calls.clear()
        with closing(client.connect(author/"remote/state.db")) as state: result=client.repack_replicas(cfg,state,ws,author)
        assert result["replaced"]==len(original)
        assert result["local"]==(0 if change=="changed" else 1)
        assert result["downloaded"]==(0 if change=="none" else 1)==calls.count("replica_repack_get")
        assert protocol.digest((author/"data/convos.db").read_bytes())==before
        stored=[server.stored_envelope(r) for r in db.execute("SELECT * FROM row_replicas ORDER BY cursor")]
        key=client.key(cfg,ws,1)
        assert [protocol.open_replica(e,key,True) for e in stored]==[protocol.open_replica(e,key,True) for e in original]


def test_compaction_reconstructs_local_semantic_payload_without_downloading(tmp_path,monkeypatch):
    import ai_convos_memory as memory
    from tests.test_remote_client import transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        author=tmp_path/"author"
        monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(author))
        monkeypatch.setattr(client,"drain_hooks",lambda:None)
        direct=transport(db)
        modern=False
        def request(cfg,body,auth=True):
            assert body["op"]!="replica_repack_get","semantic payload should be local"
            response=direct(cfg,body,auth)
            if body["op"]=="state" and not modern: response["capabilities"].pop("replica_compression")
            return response
        monkeypatch.setattr(client,"request",request)
        cfg,_=client.setup_client("http://server","alice",root=author)
        ws=client.workspace(cfg,"Personal")
        memory.remember_data("exact semantic body "*1000,"global")
        client.sync_once(author,True)
        original=[server.stored_envelope(r) for r in db.execute("SELECT * FROM semantic_replicas ORDER BY cursor")]
        assert original and all(e["v"]==1 for e in original)
        modern=True
        cfg=client.load(author)
        client.refresh(cfg,author)
        with closing(client.connect(author/"remote/state.db")) as state:
            result=client.repack_replicas(cfg,state,ws,author)
            assert not state.execute("SELECT name FROM sqlite_temp_master WHERE name='compact_local'").fetchall()
        assert result["local"]==result["replaced"]==len(original) and result["downloaded"]==0
        stored=[server.stored_envelope(r) for r in db.execute("SELECT * FROM semantic_replicas ORDER BY cursor")]
        key=client.key(cfg,ws,1)
        assert [protocol.open_replica(e,key,True) for e in stored]==[protocol.open_replica(e,key,True) for e in original]


def test_compact_without_workspace_processes_all_accessible_workspaces(tmp_path,monkeypatch):
    from typer.testing import CliRunner
    from tests.test_remote_client import transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        monkeypatch.setattr(client,"request",transport(db))
        root=tmp_path/"client"
        monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(root))
        cfg,_=client.setup_client("http://server","alice",root=root)
        team=client.create(cfg,"Team",root=root)
        for ws in cfg["workspaces"]:
            env=message(cfg,ws,client.key(cfg,ws,1))
            server.action(db,{"op":"replica_upload_many","envelopes":[env]},cfg["token"])
        result=CliRunner().invoke(client.remote,["compact"])
        assert result.exit_code==0,result.output
        workspaces=json.loads(result.stdout)["workspaces"]
        assert set(workspaces)==set(cfg["workspaces"]) and team in workspaces
        assert all(r["replaced"]==1 for r in workspaces.values())


def test_fallback_fetch_checks_uploader_epoch_and_expected_digest(tmp_path):
    with closing(server.connect(tmp_path/"relay.db")) as db:
        cfg,ws,key=configured(db,"team")
        other=account(db,"bob")
        rotate_ws(db,cfg,cfg["controls"][ws],key,((cfg,"admin"),(other,"member")))
        env=message(cfg,ws,key,epoch=2)
        server.action(db,{"op":"replica_upload_many","envelopes":[env]},cfg["token"])
        item=server.action(db,{"op":"replica_repack_pull","workspace":ws},cfg["token"])["replicas"][0]
        assert "envelope" not in item and "ciphertext" not in item["header"]
        req={"op":"replica_repack_get","workspace":ws,"cursor":item["cursor"],"semantic":False,"wire_hash":item["wire_hash"]}
        assert server.action(db,req,cfg["token"])["envelope"]==env
        with pytest.raises(ValueError,match="unavailable or changed"): server.action(db,req,other["token"])
        with pytest.raises(ValueError,match="unavailable or changed"): server.action(db,{**req,"wire_hash":"0"*64},cfg["token"])
