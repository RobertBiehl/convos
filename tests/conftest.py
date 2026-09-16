import gc, hashlib, os, sys, tempfile
from pathlib import Path

import pytest


if "ai_convos.cli" in sys.modules: raise RuntimeError("Convos loaded before the test sandbox")
_SUITE=tempfile.TemporaryDirectory(prefix="convos-tests-")
_SUITE_ROOT=Path(_SUITE.name)
def _paths(root):
    return {"CONVOS_PROJECT_ROOT":root/"archive", "CODEX_HOME":root/"codex", "CLAUDE_CONFIG_DIR":root/"claude", "CONVOS_SERVER_DB":root/"relay/server.db"}
[os.environ.__setitem__(name,str(path)) for name,path in _paths(_SUITE_ROOT).items()]
os.environ["CONVOS_TEST_ROOT"]=str(_SUITE_ROOT)
[os.environ.pop(name,None) for name in ("CONVOS_IMPORT_PATHS","CONVOS_CODEX_MEMORY_ROOT","CONVOS_CLAUDE_PROJECTS_ROOT","CONVOS_MEMORY_DB")]


@pytest.fixture(autouse=True)
def isolated_product_state(tmp_path,monkeypatch):
    root=tmp_path/"runtime"
    [monkeypatch.setenv(name,str(path)) for name,path in _paths(root).items()]
    monkeypatch.setenv("CONVOS_TEST_ROOT",str(root))
    [monkeypatch.delenv(name,raising=False) for name in ("CONVOS_IMPORT_PATHS","CONVOS_CODEX_MEMORY_ROOT","CONVOS_CLAUDE_PROJECTS_ROOT","CONVOS_MEMORY_DB")]
    from ai_convos import cli
    data=root/"archive/data"
    for name,path in (("PROJECT_ROOT",root/"archive"),("DATA_DIR",data),("DB_PATH",data/"convos.db"),("STATE_PATH",data/"sync_state.json"),("HOOK_DIR",data/"hook_inbox"),("HOOK_STATE",data/"hook_state.json"),("HOOK_PROGRESS",data/"hook_progress.json"),("HOOK_EMBED_DIRTY",data/"hook_embeddings_dirty"),("HOOK_FTS_DIRTY",data/"hook_fts_dirty")): monkeypatch.setattr(cli,name,path)
    assert all(path.is_relative_to(root) for path in (cli.PROJECT_ROOT,cli.DATA_DIR,cli.DB_PATH,cli.STATE_PATH,cli.HOOK_DIR,cli.HOOK_STATE,cli.HOOK_PROGRESS,cli.HOOK_EMBED_DIRTY,cli.HOOK_FTS_DIRTY))


def pytest_runtest_teardown(): gc.collect()


def pytest_collection_modifyitems(config,items):
    if not (value:=os.environ.get('CONVOS_TEST_SHARD')): return
    index,count=map(int,value.split('/'))
    if not 0<=index<count: raise pytest.UsageError('CONVOS_TEST_SHARD requires index/count with 0 <= index < count')
    chosen=[item for item in items if int.from_bytes(hashlib.sha256(item.nodeid.encode()).digest(),'big')%count==index]
    selected=set(chosen)
    config.hook.pytest_deselected(items=[item for item in items if item not in selected])
    items[:]=chosen
