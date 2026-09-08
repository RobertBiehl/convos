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


def message(author,workspace,key,identity="m",epoch=1):
    columns=["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"]
    row=protocol.logical_row("messages",columns,[identity,"c","assistant","repeated tool output\n"*10000,None,None,None,'{"original":"unchanged"}',None])
    proof=protocol.row_proof(author["device"],author["user"],workspace,epoch,row)
    return protocol.seal_replica(row,proof,workspace,epoch,key,author["device"]["id"])


def configured(db,kind="personal"):
    author=account(db,"alice")
    ws,key="personal",bytes(range(32))
    control=create_ws(db,author,ws,key,kind)
    cfg={**author,"controls":{ws:control},"keys":{f"{ws}:1":protocol.b64(key)},"workspaces":{ws:{"kind":"personal","epoch":1}},"replica_compression":{ws:"zstd"}}
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
    with closing(server.connect(tmp_path/"relay.db")) as db,closing(sqlite3.connect(":memory:")) as state:
        state.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)")
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
        assert state.execute("SELECT * FROM meta").fetchall()==[]
        resumed=client.repack_replicas(cfg,state,ws)
        assert resumed["scanned"]==2 and resumed["replaced"]==0
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


def test_compression_selection_requires_relay_capability_and_does_not_mutate_on_failure(tmp_path):
    cfg={"server_state":{"capabilities":{}},"replica_compression":{}}
    original=copy.deepcopy(cfg)
    with pytest.raises(ValueError,match="upgrade the relay"): client.configure_compression(cfg,"ws","zstd",tmp_path)
    assert cfg==original and not (tmp_path/"remote/config.json").exists()


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


def test_compressed_normal_upload_projects_exactly_on_a_second_device(tmp_path,monkeypatch):
    from tests.test_remote_client import replicate_conversation, transport
    with closing(server.connect(tmp_path/"relay.db")) as db:
        monkeypatch.setattr(client,"request",transport(db))
        monkeypatch.setattr(client,"drain_hooks",lambda:None)
        author,reader=tmp_path/"author",tmp_path/"reader"
        cfg,recovery=client.setup_client("http://server","alice","laptop",root=author)
        ws=client.workspace(cfg,"Personal")
        client.configure_compression(cfg,ws,"zstd",author)
        title="An exact retained conversation title. "*1000
        envs=replicate_conversation(author,ws,title)
        assert envs and all(env["v"]==2 for env in envs)
        client.setup_client("http://server","alice","desktop",recovery,root=reader)
        client.sync_once(reader,manual=True)
        with closing(duckdb.connect(str(reader/"data/convos.db"),read_only=True)) as archive:
            assert archive.execute("SELECT title FROM conversations").fetchall()==[(title,)]


def test_client_does_not_rewrite_or_advance_past_an_unauthentic_replica(tmp_path,monkeypatch):
    with closing(server.connect(tmp_path/"relay.db")) as db,closing(sqlite3.connect(":memory:")) as state:
        state.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)")
        cfg,ws,key=configured(db)
        env=message(cfg,ws,key)
        raw=protocol.unb64(env["ciphertext"])
        corrupted={**env,"ciphertext":protocol.b64(bytes([raw[0]^1])+raw[1:])}
        server.action(db,{"op":"replica_upload_many","envelopes":[corrupted]},cfg["token"])
        monkeypatch.setattr(client,"request",lambda cfg,body:server.action(db,body,cfg["token"]))
        with pytest.raises(ValueError,match="invalid row replica"): client.repack_replicas(cfg,state,ws)
        assert not state.execute("SELECT * FROM meta").fetchall()
        assert server.stored_envelope(db.execute("SELECT * FROM row_replicas").fetchone())==corrupted
