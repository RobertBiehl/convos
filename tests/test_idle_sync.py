"""An idle sync pass (nothing new on the relay, hooks still writing) must cost stats and a state request, not git runs, relay pulls and archive reopens."""
import collections, json, subprocess
import duckdb, pytest
import ai_convos.cli as core
import ai_convos_remote as remote
from ai_convos.cli import ARCHIVE_COLUMNS, project_archive_row
from ai_convos_remote import connect, sharing_routes
from ai_convos_remote_server import connect as server_connect
from tests.test_remote_client import transport, write_archive
from tests.test_remote_projection import git, source


@pytest.fixture
def counted(monkeypatch):
    counts,real_run,real_open=collections.Counter(),subprocess.run,core._open_db
    monkeypatch.setattr(subprocess,"run",lambda args,*a,**k:(counts.update(["git"] if args[0]=="git" else []),real_run(args,*a,**k))[-1])
    monkeypatch.setattr(core,"_open_db",lambda *a,**k:(counts.update(["connections"]),real_open(*a,**k))[-1])
    return counts


def test_unchanged_checkout_resolves_without_git_across_runs(tmp_path,counted):
    root,core_db=source(tmp_path); repo=core.repository(root,core_db); evidence={k:repo[k] for k in ("lineage","remotes")}
    state=connect(tmp_path/"state.db"); state.execute("INSERT INTO policies VALUES ('w','member','repository','grant',?)",(json.dumps(evidence),)); state.commit()
    bindings={"w:repository:grant":{"path":str(root),"repository":repo["id"],"checkout":repo["checkout"]}}
    routes=lambda state:(counted.clear(),sharing_routes(state,"w","member",bindings,core_db)[0],counted["git"])[1:]
    assert routes(state)==([repo["id"]],6)
    state.close(); state=connect(tmp_path/"state.db")  # the next sync run
    assert routes(state)==([repo["id"]],0)  # b34: six git subprocesses per bound checkout on every pass
    git(root,"commit","--allow-empty","-qm","next"); assert routes(state)==([repo["id"]],6)
    git(root,"remote","add","origin","git@github.com:acme/renamed.git"); assert core.repository(root,core_db)["remotes"]==remote.cached_repository(state,str(root),core_db)["remotes"]==["https://github.com/acme/renamed"]


def test_idle_pass_skips_relay_pulls_git_and_archive_reopens(tmp_path,monkeypatch,counted):
    server,ops,stages=server_connect(tmp_path/"server.db"),collections.Counter(),[]
    monkeypatch.setattr(remote,"request",lambda cfg,body,auth=True:(ops.update([body["op"]]),transport(server)(cfg,body,auth))[-1])
    monkeypatch.setattr(remote,"drain_hooks",lambda:None)
    a=tmp_path/"a"; monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); alice,_=remote.setup_client("http://server","alice",root=a); remote.create(alice,"Team","team",a)
    repo=a/"repo"; repo.mkdir(); [git(repo,*args) for args in (("init","-q"),("config","user.email","a@b.c"),("config","user.name","A"))]; (repo/"a").write_text("x"); git(repo,"add","."); git(repo,"commit","-qm","init")
    write_archive(a/"data/convos.db","first"); remote.link_cmd(repo,"Team")
    for _ in range(2): remote.sync_once(a)
    lock=a/"remote/sync.lock"
    for name in ("converge_own_rows","settle_sharing","reconcile_replicas"): monkeypatch.setattr(remote,name,(lambda real:lambda *x,**k:(stages.append((real.__name__,json.loads(lock.read_text())["stage"])),real(*x,**k))[-1])(getattr(remote,name)))
    with duckdb.connect(str(a/"data/convos.db")) as db: project_archive_row(db,"messages",ARCHIVE_COLUMNS["messages"],["m1","c","user","hook text",None,"2026-01-01",None,"{}",None])
    ops.clear(); remote.sync_once(a)
    assert {"origin_pull","blob_pull","pull"}.isdisjoint(ops)  # only the stream this pass uploaded to is pulled back
    assert {name:stage for name,stage in stages}=={"converge_own_rows":"checking retained rows","settle_sharing":"applying sharing policies","reconcile_replicas":"reconciling replicas"}  # b34: still "retrying retained own updates" or older
    counted.clear(); ops.clear(); remote.sync_once(a)
    assert dict(ops)=={"state":2} and counted["git"]==0 and counted["connections"]<=7  # b34: 18 relay calls, 12 git runs, 12 archive connections
