"""Tests for the hybrid BM25 + vector RRF search pipeline."""
import json
import duckdb, pytest
from pathlib import Path
from typer.testing import CliRunner
from ai_convos import cli


@pytest.fixture(autouse=True)
def explicit_test_semantic_runtime(monkeypatch): monkeypatch.setenv("CONVOS_SEMANTIC","llama")


def _emb(idx: int, dim: int | None = None) -> list[float]:
    dim=dim or cli.embedding_profile()["dimensions"]
    v = [0.0] * dim
    v[idx % dim] = 1.0
    return v


@pytest.fixture
def hybrid_db(tmp_path, monkeypatch):
    """Five messages: m1/m3 share an embedding (idx=1) and the term "apple";
    m2/m5 share an embedding (idx=2) and the term "date"; m4 is unrelated."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db))
    cli.init_schema(conn)
    conn.execute(
        "INSERT INTO conversations VALUES (?, 'test', 'Conv', NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ["c1"],
    )
    rows = [
        ("m1", "apple banana cherry", _emb(1)),
        ("m2", "banana date elderberry", _emb(2)),
        ("m3", "apple fruit basket", _emb(1)),
        ("m4", "totally unrelated content", _emb(50)),
        ("m5", "date raisin elderberry", _emb(2)),
    ]
    for mid, content, emb in rows:
        conn.execute(
            "INSERT INTO messages VALUES (?, 'c1', 'user', ?, NULL, NULL, NULL, NULL, ?, NULL)",
            [mid, content, emb],
        )
    conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",[json.dumps(cli.embedding_profile())])
    cli.rebuild_fts_index(conn)
    conn.close()
    return db


def test_rrf_sql_formula():
    """RRF CTE: each source contributes 1/(60+rank); sums across sources."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE fts (id VARCHAR, r INT)")
    conn.execute("CREATE TABLE vec (id VARCHAR, r INT)")
    conn.execute("INSERT INTO fts VALUES ('a', 1), ('b', 2), ('c', 3)")
    conn.execute("INSERT INTO vec VALUES ('a', 1), ('b', 2), ('d', 3)")
    rows = dict(conn.execute("""
        SELECT id, SUM(1.0/(60+r)) AS rrf FROM (SELECT id, r FROM fts UNION ALL SELECT id, r FROM vec)
        GROUP BY id ORDER BY rrf DESC
    """).fetchall())
    assert rows["a"] == pytest.approx(1/61 + 1/61, rel=1e-9)
    assert rows["b"] == pytest.approx(1/62 + 1/62, rel=1e-9)
    assert rows["c"] == pytest.approx(1/63, rel=1e-9)
    assert rows["d"] == pytest.approx(1/63, rel=1e-9)
    # 'a' (top in both) ranks highest
    top = max(rows, key=rows.get)
    assert top == "a"


def test_query_pipeline_end_to_end(hybrid_db, monkeypatch):
    """Querying for 'apple' returns the strongest relevant message, not unrelated content."""
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False, local_only=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "apple", "-n", "5"])
    assert r.exit_code == 0, r.output
    out = r.output
    assert "apple" in out
    assert "totally unrelated" not in out


def test_query_returns_one_hit_per_conversation(hybrid_db, monkeypatch):
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False, local_only=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "apple", "-n", "5", "-c", "5", "-f", "json"])
    assert r.exit_code == 0
    hits = __import__("json").loads(r.output)
    assert len(hits) == 1 and set(hits[0])=={"score","message_id","role","content","created_at","title","source","conversation_id","cwd"} and isinstance(hits[0]["score"],float) and hits[0]["message_id"] in ("m1", "m3") and hits[0]["content"] == "apple..."


def test_search_returns_one_hit_per_conversation(hybrid_db):
    r = CliRunner().invoke(cli.app, ["search", "apple", "-n", "5", "-f", "json"])
    assert r.exit_code == 0
    hits = __import__("json").loads(r.output)
    assert len(hits) == 1 and hits[0]["conversation_id"] == "c1" and hits[0]["message_id"] in ("m1", "m3")


def test_search_structured_output_honors_context(hybrid_db):
    conn = duckdb.connect(str(hybrid_db)); conn.execute("UPDATE messages SET thinking='reasoning detail' WHERE id='m1'"); cli.rebuild_fts_index(conn); conn.close()
    r = CliRunner().invoke(cli.app, ["search", "cherry", "-c", "5", "-t", "-f", "json"]); hit = __import__("json").loads(r.output)[0]
    assert hit["content"] == "apple..." and hit["thinking"] == "reaso..."
    assert __import__("json").loads(CliRunner().invoke(cli.app, ["search", "cherry", "-c", "5", "-f", "json"]).output)[0]["thinking"] is None
def test_read_known_conversation_is_bounded_and_chronological(tmp_path, monkeypatch):
    db = tmp_path/"test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations (id,source,title) VALUES ('abcdef1234567890','codex','Work')")
    conn.executemany("INSERT INTO messages (id,conversation_id,role,content,thinking,created_at,metadata) VALUES (?,'abcdef1234567890','user',?,?,?,?)", [("m1","first message",None,"2026-01-01","{}"),("m2","second message","secret thought","2026-01-02","{}"),("m3","third message",None,"2026-01-03","{}"),("m4","fourth message",None,"2026-01-04","{}"),("m5","fifth message",None,"2026-01-05","{}"),("old","stale",None,"2025-01-01",'{"history_of":"m1"}')]); conn.close()
    r = CliRunner().invoke(cli.app, ["read", "abcdef12", "-n", "2", "-c", "6", "-t", "-f", "jsonl"]); rows = [__import__("json").loads(x) for x in r.output.splitlines()]
    assert r.exit_code == 0 and [x["id"] for x in rows] == ["m4", "m5"] and [x["content"] for x in rows] == ["fourth...", "fifth ..."]
    around = __import__("json").loads(CliRunner().invoke(cli.app, ["read", "abcdef12", "-a", "m2", "-n", "3", "-f", "json"]).output)
    assert [x["id"] for x in around] == ["m1", "m2", "m3"] and around[1]["thinking"] is None
    shown = __import__("json").loads(CliRunner().invoke(cli.app, ["read", "abcdef12", "-a", "m2", "-n", "1", "-c", "6", "-t", "-f", "json"]).output)
    assert shown[0]["thinking"] == "secret..."


def test_read_rejects_unknown_or_ambiguous_prefix(tmp_path, monkeypatch):
    db = tmp_path/"test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations (id,source) VALUES ('abc1','x'),('abc2','x')"); conn.close()
    assert CliRunner().invoke(cli.app, ["read", "missing"]).exit_code == 1
    r = CliRunner().invoke(cli.app, ["read", "abc"]); assert r.exit_code == 1 and "Ambiguous prefix" in r.output
    assert CliRunner().invoke(cli.app, ["read", "abc1", "-a", "missing"]).exit_code == 1


def test_query_filters_candidates_and_skips_injected_boilerplate(tmp_path, monkeypatch):
    db = tmp_path / "test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('noise','noise','N',NULL,NULL,NULL,NULL,NULL,NULL,NULL), ('target','target','T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
    rows = [(f"n{i}", "noise", "user", "needle noise", _emb(1)) for i in range(55)] + [("skill", "target", "user", "Base directory for this skill: needle", _emb(1)),("agents","target","user","# AGENTS.md instructions for /repo needle",_emb(1)),("goal","target","user",'<codex_internal_context source="goal">needle',_emb(1)),("plugins","target","user","<recommended_plugins>needle",_emb(1)), ("wanted", "target", "user", "needle wanted", _emb(1))]
    conn.executemany("INSERT INTO messages (id,conversation_id,role,content,embedding) VALUES (?,?,?,?,?)", rows); conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",[json.dumps(cli.embedding_profile())]); cli.rebuild_fts_index(conn); conn.close()
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False, local_only=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "needle", "-s", "target", "-n", "5", "-f", "json"]); hits = __import__("json").loads(r.output)
    assert [h["content"] for h in hits] == ["needle wanted"]


def test_search_and_query_have_direct_project_and_conversation_filters(hybrid_db,monkeypatch):
    conn=duckdb.connect(str(hybrid_db)); conn.execute("UPDATE conversations SET cwd='/repo/sub' WHERE id='c1'"); conn.execute("INSERT INTO conversations (id,source,title,cwd) VALUES ('c2','test','Other','/other')"); conn.execute("INSERT INTO messages (id,conversation_id,role,content,embedding) VALUES ('m6','c2','user','apple outside',?)",[_emb(1)]); cli.rebuild_fts_index(conn); conn.close()
    monkeypatch.setattr(cli,"embed_text",lambda s,doc=False,local_only=False:_emb(1)); runner=CliRunner()
    literal=__import__("json").loads(runner.invoke(cli.app,["search","apple","--cwd","/repo","-f","json"]).output); scoped=__import__("json").loads(runner.invoke(cli.app,["search","apple","--conversation","c2","-f","json"]).output)
    hybrid=__import__("json").loads(runner.invoke(cli.app,["query","apple","--cwd","/repo","-f","json"]).output); exact=__import__("json").loads(runner.invoke(cli.app,["query","apple","--conversation","c2","-f","json"]).output)
    assert {r["conversation_id"] for r in literal+hybrid}=={"c1"} and {r["conversation_id"] for r in scoped+exact}=={"c2"}


def test_public_hybrid_hits_can_forbid_model_download(hybrid_db,monkeypatch):
    seen=[]
    monkeypatch.setattr(cli,"embed_text",lambda s,doc=False,local_only=False:seen.append(local_only) or _emb(1))
    rows=cli.hybrid_hits("apple",limit=2,local_only=True)
    assert seen==[True] and len(rows)==1 and rows[0]["content"].startswith("apple")


def test_embedding_model_path_is_revision_pinned_and_can_be_local_only(monkeypatch):
    calls=[]
    monkeypatch.setattr("huggingface_hub.hf_hub_download",lambda *a,**k:calls.append((a,k)) or "/model.gguf")
    monkeypatch.setattr(cli,"_file_sha256",lambda _:cli._MCFG["artifact_sha256"])
    assert cli.embedding_model_path(True)=="/model.gguf"
    assert calls==[((cli._MCFG["repo_id"],cli._MCFG["filename"]),{"revision":cli._MCFG["revision"],"local_files_only":True})]

def test_missing_semantic_extra_has_actionable_error(monkeypatch):
    real=__import__; monkeypatch.setattr("builtins.__import__",lambda name,*args,**kwargs:(_ for _ in ()).throw(ImportError(name)) if name=="huggingface_hub" else real(name,*args,**kwargs))
    with pytest.raises(ValueError,match=r"convos\[semantic\].*convos embed"): cli.embedding_model_path(True)


def test_retired_profile_invalidates_incompatible_vectors(tmp_path,monkeypatch):
    path=tmp_path/"archive.db"; monkeypatch.setattr(cli,"DB_PATH",path); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); monkeypatch.setenv("CONVOS_SEMANTIC","llama")
    retired={"backend":"model2vec","model":"potion-base-8M","dimensions":256}
    db=duckdb.connect(str(path)); cli.init_schema(db); db.execute("INSERT INTO conversations(id,source) VALUES ('c','x')"); db.execute("INSERT INTO messages(id,conversation_id,role,embedding) VALUES ('m','c','user',?)",[_emb(1,256)]); db.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",[json.dumps(retired)]); db.close()
    profile,changed=cli._activate_embedding_profile(); db=duckdb.connect(str(path),read_only=True); stored=json.loads(db.execute("SELECT CAST(profile AS VARCHAR) FROM embedding_state").fetchone()[0]); assert changed and profile==stored and profile["dimensions"]==768 and db.execute("SELECT embedding IS NULL FROM messages").fetchone()[0] and db.execute("SELECT column_type FROM (DESCRIBE messages) WHERE column_name='embedding'").fetchone()[0]=="FLOAT[]"; db.close()

def test_semantic_model_artifact_hash_is_enforced(monkeypatch):
    monkeypatch.setattr("huggingface_hub.hf_hub_download",lambda *a,**k:"/model.gguf"); monkeypatch.setattr(cli,"_file_sha256",lambda _:"bad")
    with pytest.raises(ValueError,match="artifact hash mismatch"): cli.embedding_model_path(True)


def test_semantic_runtime_can_be_disabled(monkeypatch):
    monkeypatch.setenv("CONVOS_SEMANTIC","0"); assert not cli.semantic_enabled()
    with pytest.raises(ValueError,match="CONVOS_SEMANTIC=0"): cli.embedding_profile()
    monkeypatch.delenv("CONVOS_SEMANTIC"); monkeypatch.setattr(cli.sys,"platform","linux"); assert not cli.semantic_enabled()
    with pytest.raises(ValueError,match="macOS includes it"): cli.embedding_profile()
    monkeypatch.setenv("CONVOS_SEMANTIC","llama"); assert cli.semantic_enabled() and cli.semantic_backend()=="llama"


def test_query_without_embeddings_returns_complete_lexical_results(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db))
    cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1', 'test', 'Conv', NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1', 'c1', 'user', 'hello', NULL, NULL, NULL, NULL, NULL, NULL)")
    cli.rebuild_fts_index(conn)
    conn.close()
    monkeypatch.setattr(cli,"embed_text",lambda *_:(_ for _ in ()).throw(AssertionError("query loaded model without vectors")))
    r = CliRunner().invoke(cli.app, ["query", "hello"])
    assert r.exit_code == 0 and "hello" in r.stdout and "Semantic coverage is partial (0/1)" in r.stderr


def test_query_refuses_hidden_schema_migration(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db))
    conn.execute("CREATE TABLE conversations (id VARCHAR PRIMARY KEY, source VARCHAR NOT NULL, title VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP, model VARCHAR, cwd VARCHAR, git_branch VARCHAR, project_id VARCHAR, metadata JSON)")
    conn.execute("CREATE TABLE messages (id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, role VARCHAR NOT NULL, content VARCHAR, thinking VARCHAR, created_at TIMESTAMP, model VARCHAR, metadata JSON)")
    conn.execute("INSERT INTO conversations VALUES ('c1', 'test', 'Conv', NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1', 'c1', 'user', 'hello', NULL, NULL, NULL, NULL)")
    conn.close()
    r = CliRunner().invoke(cli.app, ["query", "hello"])
    assert r.exit_code == 1 and "Run `convos sync`" in r.output
    conn = duckdb.connect(str(db))
    assert not conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone()
    conn.close()


def test_embedding_progress_keeps_stdout_clean(hybrid_db, monkeypatch, capsys):
    conn = duckdb.connect(str(hybrid_db)); conn.execute("UPDATE messages SET embedding=NULL WHERE id='m1'"); conn.execute("INSERT INTO messages (id,conversation_id,role,content) VALUES ('noise','c1','user','<codex_internal_context source=\"goal\">skip')"); conn.close()
    seen=[]; monkeypatch.setattr(cli, "embed_texts", lambda ss, doc=False, local_only=False: seen.append(local_only) or [_emb(1) for _ in ss]); cli.embed_pending(ids=["m1","noise"],local_only=True)
    out = capsys.readouterr(); assert out.out == "" and "Embedding 1 messages" in out.err and "1/1" in out.err
    conn=duckdb.connect(str(hybrid_db)); assert seen==[True] and conn.execute("SELECT embedding IS NOT NULL FROM messages WHERE id='m1'").fetchone()[0] and not conn.execute("SELECT embedding IS NOT NULL FROM messages WHERE id='noise'").fetchone()[0]; conn.close()


def test_read_commands_handle_locked_db(monkeypatch):
    """Read commands should print a friendly lock message instead of a traceback."""
    monkeypatch.setattr(cli, "get_db", lambda read_only=False,**_: (_ for _ in ()).throw(ValueError("Database is locked by another convos process.")))
    r = CliRunner().invoke(cli.app, ["search", "x"])
    assert r.exit_code == 1
    assert "locked" in (r.output + (r.stderr if r.stderr_bytes is not None else ""))


@pytest.mark.parametrize("read_only", [True, False])
def test_db_waits_for_writer(tmp_path, monkeypatch, read_only):
    """Readers and writers retry transient DuckDB locks before giving up."""
    db, calls = tmp_path / "test.db", {"n": 0}; db.touch()
    monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path); monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    def connect(path, read_only=False):
        calls["n"] += 1
        if calls["n"] < 3: raise duckdb.IOException("Conflicting lock is held")
        return type("Connection",(),{"close":lambda _:None})()
    monkeypatch.setattr(cli.duckdb, "connect", connect)
    conn=cli.get_db(read_only=read_only); conn.close()
    assert calls["n"] == 3


def test_db_retry_notices_are_per_archive_and_fast_path_has_none(tmp_path,monkeypatch):
    events=[]; [p.touch() for p in (tmp_path/"a.db",tmp_path/"b.db")]
    monkeypatch.setattr(cli.fcntl,"flock",lambda lock,op:events.append(Path(lock.name)))
    calls={}
    def connect(path,read_only=False):
        calls[path]=calls.get(path,0)+1
        if path.endswith("a.db") and calls[path]==1: raise duckdb.IOException("Conflicting lock is held")
        return type("Connection",(),{"close":lambda _:None})()
    monkeypatch.setattr(cli.duckdb,"connect",connect); monkeypatch.setattr(cli.time,"sleep",lambda _:None)
    cli.get_db(path=tmp_path/"a.db").close(); cli.get_db(path=tmp_path/"b.db").close()
    assert len(events)==1 and events[0].parent==tmp_path/".a.db.waiters" and not (tmp_path/".b.db.waiters").exists()


def test_db_symlink_alias_uses_canonical_database_path(tmp_path,monkeypatch):
    db,alias=tmp_path/"archive.db",tmp_path/"alias.db"; db.touch(); alias.symlink_to(db); seen=[]
    monkeypatch.setattr(cli,"_open_db",lambda path,read_only=False:(seen.append((path,read_only)),type("Connection",(),{"close":lambda _:None})())[-1])
    cli.get_db(path=alias).close(); assert seen==[(db,False)] and not (tmp_path/".archive.db.waiters").exists()


def test_db_rejects_hardlink_aliases(tmp_path):
    db,alias=tmp_path/"archive.db",tmp_path/"alias.db"; db.touch(); alias.hardlink_to(db)
    with pytest.raises(ValueError,match="hardlink aliases are unsafe"): cli.get_db(path=alias)


def test_search_and_query_never_run_ingest_or_maintenance(hybrid_db,monkeypatch):
    monkeypatch.setattr(cli,"drain_hooks",lambda *_a,**_k:(_ for _ in ()).throw(AssertionError("retrieval drained hooks")))
    monkeypatch.setattr(cli,"embed_text",lambda *_a,**_k:_emb(1))
    runner=CliRunner(); assert runner.invoke(cli.app,["search","apple"]).exit_code==0 and runner.invoke(cli.app,["query","apple"]).exit_code==0


def test_stale_fts_scans_complete_rows_excludes_history_and_keeps_json_clean(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path)
    conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test')"); conn.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('old','c','user','old needle','{}')"); cli._archive_touch(conn,[("messages","old")]); cli.rebuild_fts_index(conn)
    conn.execute("INSERT INTO messages(id,conversation_id,role,content,metadata) VALUES ('new','c','user','fresh needle','{}'),('history','c','user','hidden unique','{\"history_of\":\"old\"}')"); cli._archive_touch(conn,[("messages","new"),("messages","history")]); before=conn.execute("SELECT * FROM retrieval_state").fetchone(); conn.close()
    runner=CliRunner(); result=runner.invoke(cli.app,["search","fresh","-f","json"]); rows=json.loads(result.stdout)
    assert result.exit_code==0 and set(rows[0])=={"message_id","role","content","thinking","created_at","score","title","source","conversation_id","cwd"} and rows[0]["message_id"]=="new" and isinstance(rows[0]["score"],float) and "complete lexical scan" in result.stderr
    assert json.loads(runner.invoke(cli.app,["search","unique","-f","json"]).stdout)==[]
    conn=duckdb.connect(str(db)); assert conn.execute("SELECT * FROM retrieval_state").fetchone()==before; conn.close()


def test_stale_search_can_match_thinking_only_rows(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test'); INSERT INTO messages(id,conversation_id,role,thinking) VALUES ('m','c','assistant','private needle')"); cli._archive_touch(conn,[("messages","m")]); conn.close()
    result=CliRunner().invoke(cli.app,["search","needle","--thinking","-f","json"]); assert json.loads(result.stdout)[0]["thinking"]=="private needle"


def test_require_complete_fails_without_mutating_retrieval_state(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path)
    conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test')"); conn.execute("INSERT INTO messages(id,conversation_id,role,content) VALUES ('m','c','user','needle')"); cli._archive_touch(conn,[("messages","m")]); state=conn.execute("SELECT * FROM retrieval_state").fetchone(); conn.close()
    result=CliRunner().invoke(cli.app,["query","needle","--require-complete"]); assert result.exit_code==1 and "Retrieval is incomplete" in result.stderr
    conn=duckdb.connect(str(db)); assert conn.execute("SELECT * FROM retrieval_state").fetchone()==state; conn.close()


def test_query_model_inference_does_not_hold_archive_lock(hybrid_db,monkeypatch):
    def embed(*_args,**_kwargs):
        with cli.get_db(path=hybrid_db,wait=0) as conn: conn.execute("UPDATE conversations SET title='unlocked' WHERE id='c1'")
        return _emb(1)
    monkeypatch.setattr(cli,"embed_text",embed); assert cli.hybrid_hits("apple")


def test_bounded_embedding_rejects_content_changed_during_inference(tmp_path,monkeypatch,capsys):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path)
    conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test')"); conn.execute("INSERT INTO messages(id,conversation_id,role,content) VALUES ('m','c','user','before')"); conn.close()
    def embed(ss,doc=False,local_only=False):
        with cli.get_db(path=db) as writer: writer.execute("UPDATE messages SET content='after' WHERE id='m'")
        return [_emb(1) for _ in ss]
    monkeypatch.setattr(cli,"embed_texts",embed); assert cli.embed_pending(limit=1)==1 and "1 messages remain unembedded" in capsys.readouterr().err
    conn=duckdb.connect(str(db)); assert conn.execute("SELECT content,embedding IS NULL FROM messages").fetchone()==("after",True); conn.close()

def test_candidate_limits_count_distinct_conversations(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); conn=duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations(id,source) VALUES ('a','test'),('b','test')"); conn.executemany("INSERT INTO messages(id,conversation_id,role,content,embedding) VALUES (?,'a','user','needle',?)",[(f"a{i}",_emb(1)) for i in range(60)]); conn.execute("INSERT INTO messages(id,conversation_id,role,content,embedding) VALUES ('b','b','user','needle',?)",[_emb(1)]); conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,?)",[json.dumps(cli.embedding_profile())]); conn.close(); monkeypatch.setattr(cli,"embed_text",lambda *_a,**_k:_emb(1)); runner=CliRunner()
    assert {r["conversation_id"] for r in json.loads(runner.invoke(cli.app,["search","needle","-n","2","-f","json"]).stdout)}=={"a","b"} and {r["conversation_id"] for r in json.loads(runner.invoke(cli.app,["query","needle","-n","2","-f","json"]).stdout)}=={"a","b"}

def test_empty_machine_query_and_unavailable_archive_contracts(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); conn=duckdb.connect(str(db)); cli.init_schema(conn); cli.rebuild_fts_index(conn); conn.close(); runner=CliRunner()
    assert runner.invoke(cli.app,["query","missing","-f","json"]).stdout.strip()=="[]" and runner.invoke(cli.app,["query","missing","-f","jsonl"]).stdout==""
    db.unlink(); result=runner.invoke(cli.app,["search","missing","-f","json"]); assert result.exit_code==1 and result.stdout=="" and "Database not found" in result.stderr

def test_strict_query_revalidates_coverage_after_model_inference(hybrid_db,monkeypatch):
    def embed(*_a,**_k):
        with cli.get_db(path=hybrid_db) as conn: conn.execute("INSERT INTO messages(id,conversation_id,role,content) VALUES ('late','c1','user','apple late')"); cli._archive_touch(conn,[("messages","late")]); cli.rebuild_fts_index(conn)
        return _emb(1)
    monkeypatch.setattr(cli,"embed_text",embed); result=CliRunner().invoke(cli.app,["query","apple","--require-complete"]); assert result.exit_code==1 and "5/6" in result.stderr

def test_limited_embedding_refuses_reset_and_skips_history(tmp_path,monkeypatch,capsys):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source) VALUES ('c','test'); INSERT INTO messages(id,conversation_id,role,content,metadata,embedding) VALUES ('current','c','user','now','{}',?),('history','c','user','old','{\"history_of\":\"current\"}',NULL)",[_emb(1)]); conn.execute("INSERT OR REPLACE INTO embedding_state VALUES (TRUE,'{\"backend\":\"old\"}')"); conn.close()
    before=duckdb.connect(str(db),read_only=True).execute("SELECT count(*) FROM messages WHERE embedding IS NOT NULL").fetchone()[0]; result=CliRunner().invoke(cli.app,["embed","--limit","1"]); assert result.exit_code==1 and "requires full maintenance" in result.stderr and duckdb.connect(str(db),read_only=True).execute("SELECT count(*) FROM messages WHERE embedding IS NOT NULL").fetchone()[0]==before
    conn=duckdb.connect(str(db)); conn.execute("DELETE FROM embedding_state; UPDATE messages SET embedding=NULL"); conn.close(); monkeypatch.setattr(cli,"embed_texts",lambda ss,**_k:[_emb(1) for _ in ss]); assert cli.embed_pending(limit=1)==1 and duckdb.connect(str(db),read_only=True).execute("SELECT id FROM messages WHERE embedding IS NOT NULL").fetchone()[0]=="current"


def test_sql_select_and_blocks_writes(tmp_path, monkeypatch):
    """convos sql runs read-only SELECTs (json + text) and fails writes cleanly."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','test','T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)"); conn.close()
    r = CliRunner().invoke(cli.app, ["sql", "SELECT id, source FROM conversations", "-f", "json"])
    assert r.exit_code == 0, r.output
    assert '"c1"' in r.output and '"test"' in r.output
    w = CliRunner().invoke(cli.app, ["sql", "UPDATE conversations SET title='x'"])
    assert w.exit_code == 0
    assert "Query failed" in (w.output + (w.stderr if w.stderr_bytes is not None else ""))


def test_json_output_formats(tmp_path, monkeypatch):
    """-f json emits an array; -f jsonl emits one object per line; across read commands."""
    import json as _json
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','test','T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1','c1','user','hello',NULL,NULL,NULL,NULL,NULL,NULL)")
    conn.execute("INSERT INTO messages VALUES ('m2','c1','assistant','hi there',NULL,NULL,NULL,NULL,NULL,NULL)")
    cli.rebuild_fts_index(conn); conn.close()
    data = _json.loads(CliRunner().invoke(cli.app, ["sql", "SELECT id, source FROM conversations", "-f", "json"]).output)
    assert data == [{"id": "c1", "source": "test"}]
    out = CliRunner().invoke(cli.app, ["sql", "SELECT role FROM messages ORDER BY role", "-f", "jsonl"]).output
    objs = [_json.loads(l) for l in out.strip().splitlines() if l.strip()]
    assert [o["role"] for o in objs] == ["assistant", "user"]
    sd = _json.loads(CliRunner().invoke(cli.app, ["search", "hello", "-f", "json"]).stdout)
    assert isinstance(sd, list)


def test_search_scans_without_rebuilding_missing_fts_index(tmp_path, monkeypatch):
    db = tmp_path / "test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','test','T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1','c1','user','recoverable',NULL,NULL,NULL,NULL,NULL,NULL)"); conn.close()
    r = CliRunner().invoke(cli.app, ["search", "recoverable", "-f", "json"])
    assert r.exit_code == 0 and __import__("json").loads(r.stdout)[0]["conversation_id"] == "c1" and "complete lexical scan" in r.stderr
    conn = duckdb.connect(str(db)); assert not conn.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone(); conn.close()


def test_export_parameterizes_source_and_path(tmp_path, monkeypatch):
    db = tmp_path / "test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1',?, 'T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)", ["quoted'source"])
    conn.execute("INSERT INTO messages VALUES ('m1','c1','user','hello',NULL,NULL,NULL,NULL,NULL,NULL)"); conn.close()
    out = tmp_path / "quoted'out.csv"; r = CliRunner().invoke(cli.app, ["export", str(out), "-f", "csv", "-s", "quoted'source"])
    assert r.exit_code == 0 and "hello" in out.read_text()

def test_json_export_retains_and_labels_unconfirmed_edits(tmp_path,monkeypatch):
    db=tmp_path/"test.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(cli,"DATA_DIR",tmp_path); conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.execute("INSERT INTO conversations(id,source,metadata) VALUES ('c','codex','{}'); INSERT INTO messages(id,conversation_id,role,metadata) VALUES ('m','c','assistant','{}'); INSERT INTO file_edits VALUES ('ok','m','a.py','write','a',NULL,NULL),('bad','m','b.py','write','b',NULL,NULL); INSERT INTO provenance.file_edit_evidence VALUES ('ok','confirmed','provider_success','t1'),('bad','invalid','provider_failure','t2')"); conn.close(); out=tmp_path/"export.json"; result=CliRunner().invoke(cli.app,["export",str(out)])
    assert result.exit_code==0 and [(e["file"],e["evidence_status"],e["tool_call_id"]) for e in __import__("json").loads(out.read_text())[0]["file_edits"]]==[("b.py","invalid","t2"),("a.py","confirmed","t1")]
