import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest


def load_testbed():
    path=Path(__file__).parents[1]/"scripts/remote_testbed.py"
    spec=importlib.util.spec_from_file_location("remote_testbed",path)
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    return module


def test_testbed_clients_refuse_personal_or_cross_user_roots():
    module=load_testbed()
    module.Client("convos-fresh-a",Path("/home/convos-fresh-a/convos-testbed/fresh/laptop"),Path("/tmp/venv"))
    assert module.isolated_root(Path("/home/convos-fresh-a/convos-testbed/fresh/laptop"))==Path("/home/convos-fresh-a/convos-testbed/fresh/laptop")
    for user,root in (("robert",Path("/Users/robert/.convos")),("convos-fresh-a",Path("/home/convos-fresh-a/.convos")),("convos-fresh-a",Path("/home/convos-fresh-b/convos-testbed/fresh"))):
        with pytest.raises(ValueError,match="non-test Convos root"): module.Client(user,root,Path("/tmp/venv"))
    with pytest.raises(ValueError,match="non-test Convos root"): module.isolated_root(Path("/Users/robert/.convos"))


def relay(path,extra=False,bad_team=False):
    with sqlite3.connect(path) as db:
        db.executescript("CREATE TABLE users(id TEXT);CREATE TABLE workspaces(id TEXT,kind TEXT,created_by TEXT);CREATE TABLE members(workspace TEXT,user_id TEXT,active INT);")
        db.executemany("INSERT INTO users VALUES (?)",[("alice",),("bob",)]+([("personal",)] if extra else []))
        db.executemany("INSERT INTO workspaces VALUES (?,?,?)",[("pa","personal","alice"),("pb","personal","bob"),("team","team","alice")])
        db.executemany("INSERT INTO members VALUES (?,?,1)",[("pa","alice"),("pb","bob"),("team","alice")]+([] if bad_team else [("team","bob")]))


def test_relay_isolation_requires_only_test_users_and_exact_memberships(tmp_path):
    module=load_testbed()
    good=tmp_path/"good.db"
    relay(good)
    module.assert_relay_isolation(good,{"alice","bob"},"team")
    for name,options,match in (("extra",{"extra":True},"non-test user"),("membership",{"bad_team":True},"team test workspace")):
        path=tmp_path/f"{name}.db"
        relay(path,**options)
        with pytest.raises(AssertionError,match=match): module.assert_relay_isolation(path,{"alice","bob"},"team")


def test_private_customer_lane_refuses_ci_and_nonempty_foreign_storage(tmp_path,monkeypatch):
    module=load_testbed()
    marker=tmp_path/'personal-data'
    marker.write_text('preserve')
    monkeypatch.setenv('CI','true')
    with pytest.raises(ValueError,match='local-only'): module.customer_lane(tmp_path,tmp_path,'test',tmp_path,tmp_path)
    monkeypatch.delenv('CI')
    with pytest.raises(ValueError,match='non-customer-testbed'): module.customer_lane(tmp_path,tmp_path,'test',tmp_path,tmp_path)
    assert marker.read_text()=='preserve'


@pytest.mark.parametrize('failure',['large-append-capture','large-append-drain'])
def test_large_append_restores_private_source_when_capture_or_drain_fails(tmp_path,monkeypatch,failure):
    from contextlib import nullcontext
    import hashlib
    from ai_convos import cli as core
    module=load_testbed()
    client=module.desktop_client(tmp_path/'laptop',tmp_path/'venv')
    source=client['root']/'codex/sessions/session.jsonl'
    source.parent.mkdir(parents=True)
    original=b'{"type":"session_meta","payload":{"id":"private-session"}}\n'
    source.write_bytes(original)
    before=source.stat()
    for relative in ('data/convos.db','data/sync_state.json','remote/config.json'):
        path=client['root']/'archive'/relative
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('{}')
    evidence=tmp_path/'evidence'
    evidence.mkdir()
    entry=dict(path='codex/sessions/session.jsonl',bytes=len(original),sha256=hashlib.sha256(original).hexdigest())
    monkeypatch.setattr(module,'operation_lock',lambda *args,**kwargs:nullcontext())
    monkeypatch.setattr(core,'open_db',lambda *args,**kwargs:nullcontext())
    monkeypatch.setattr(module,'desktop_inventory',lambda client:[])
    monkeypatch.setattr(module,'customer_projection',lambda client:{})
    def measure(label,cloned,*args,**kwargs):
        assert cloned['env']['CONVOS_PROJECT_ROOT']!=client['env']['CONVOS_PROJECT_ROOT']
        assert source.read_bytes().startswith(original) and source.stat().st_size>len(original)
        if label==failure: raise RuntimeError('injected qualification failure')
    with pytest.raises(RuntimeError,match='injected qualification failure'):
        module.customer_large_append(client,dict(corpus=[entry]),evidence,measure)
    assert source.read_bytes()==original and source.stat().st_mtime_ns==before.st_mtime_ns
    assert (client['root']/'archive/data/convos.db').read_text()=='{}'
