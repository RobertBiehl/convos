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
