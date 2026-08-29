"""Tests for the hybrid BM25 + vector RRF search pipeline."""
import json
import duckdb, pytest
from typer.testing import CliRunner
from ai_convos import cli


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
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "apple", "-n", "5"])
    assert r.exit_code == 0, r.output
    out = r.output
    assert "apple banana cherry" in out
    assert "totally unrelated" not in out


def test_query_returns_one_hit_per_conversation(hybrid_db, monkeypatch):
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "apple", "-n", "5", "-c", "5", "-f", "json"])
    assert r.exit_code == 0
    hits = __import__("json").loads(r.output)
    assert len(hits) == 1 and hits[0]["score"] > 0 and hits[0]["message_id"] in ("m1", "m3") and hits[0]["content"] == "apple..." and "rerank" not in hits[0]


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
    conn.executemany("INSERT INTO messages (id,conversation_id,role,content,embedding) VALUES (?,?,?,?,?)", rows); cli.rebuild_fts_index(conn); conn.close()
    monkeypatch.setattr(cli, "embed_text", lambda s, doc=False: _emb(1))
    r = CliRunner().invoke(cli.app, ["query", "needle", "-s", "target", "-n", "5", "-f", "json"]); hits = __import__("json").loads(r.output)
    assert [h["content"] for h in hits] == ["needle wanted"]


def test_search_and_query_have_direct_project_and_conversation_filters(hybrid_db,monkeypatch):
    conn=duckdb.connect(str(hybrid_db)); conn.execute("UPDATE conversations SET cwd='/repo/sub' WHERE id='c1'"); conn.execute("INSERT INTO conversations (id,source,title,cwd) VALUES ('c2','test','Other','/other')"); conn.execute("INSERT INTO messages (id,conversation_id,role,content,embedding) VALUES ('m6','c2','user','apple outside',?)",[_emb(1)]); cli.rebuild_fts_index(conn); conn.close()
    monkeypatch.setattr(cli,"embed_text",lambda s,doc=False:_emb(1)); runner=CliRunner()
    literal=__import__("json").loads(runner.invoke(cli.app,["search","apple","--cwd","/repo","-f","json"]).output); scoped=__import__("json").loads(runner.invoke(cli.app,["search","apple","--conversation","c2","-f","json"]).output)
    hybrid=__import__("json").loads(runner.invoke(cli.app,["query","apple","--cwd","/repo","-f","json"]).output); exact=__import__("json").loads(runner.invoke(cli.app,["query","apple","--conversation","c2","-f","json"]).output)
    assert {r["conversation_id"] for r in literal+hybrid}=={"c1"} and {r["conversation_id"] for r in scoped+exact}=={"c2"}


def test_public_hybrid_hits_can_forbid_model_download(hybrid_db,monkeypatch):
    seen=[]
    monkeypatch.setattr(cli,"drain_hooks",lambda *a,**k:seen.append(("drain",a,k)))
    monkeypatch.setattr(cli,"embed_text",lambda s,doc=False,local_only=False:seen.append(local_only) or _emb(1))
    rows=cli.hybrid_hits("apple",limit=2,local_only=True)
    assert seen==[("drain",(),{"embed":True,"local_only":True}),True] and len(rows)==1 and rows[0]["content"].startswith("apple")


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


def test_query_no_embeddings_returns_friendly_error(tmp_path, monkeypatch):
    """When no rows have embeddings, query_cmd prints a guidance message and exits cleanly."""
    db = tmp_path / "test.db"
    monkeypatch.setattr(cli, "DB_PATH", db)
    monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db))
    cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1', 'test', 'Conv', NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1', 'c1', 'user', 'hello', NULL, NULL, NULL, NULL, NULL, NULL)")
    cli.rebuild_fts_index(conn)
    conn.close()
    r = CliRunner().invoke(cli.app, ["query", "hello"])
    assert r.exit_code == 0
    assert "No embeddings yet" in (r.output + (r.stderr if r.stderr_bytes is not None else ""))


def test_query_migrates_old_db_before_embedding_check(tmp_path, monkeypatch):
    """Old databases have no embedding column; query should migrate then print guidance."""
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
    assert r.exit_code == 0
    assert "No embeddings yet" in (r.output + (r.stderr if r.stderr_bytes is not None else ""))
    conn = duckdb.connect(str(db))
    assert conn.execute("SELECT 1 FROM information_schema.columns WHERE table_name='messages' AND column_name='embedding'").fetchone()
    conn.close()


def test_embedding_progress_keeps_stdout_clean(hybrid_db, monkeypatch, capsys):
    conn = duckdb.connect(str(hybrid_db)); conn.execute("UPDATE messages SET embedding=NULL WHERE id='m1'"); conn.execute("INSERT INTO messages (id,conversation_id,role,content) VALUES ('noise','c1','user','<codex_internal_context source=\"goal\">skip')"); conn.close()
    seen=[]; monkeypatch.setattr(cli, "embed_texts", lambda ss, doc=False, local_only=False: seen.append(local_only) or [_emb(1) for _ in ss]); cli.embed_pending(ids=["m1","noise"],local_only=True)
    out = capsys.readouterr(); assert out.out == "" and "Embedding 1 messages" in out.err and "1/1" in out.err
    conn=duckdb.connect(str(hybrid_db)); assert seen==[True] and conn.execute("SELECT embedding IS NOT NULL FROM messages WHERE id='m1'").fetchone()[0] and not conn.execute("SELECT embedding IS NOT NULL FROM messages WHERE id='noise'").fetchone()[0]; conn.close()


def test_read_commands_handle_locked_db(monkeypatch):
    """Read commands should print a friendly lock message instead of a traceback."""
    monkeypatch.setattr(cli, "get_db", lambda read_only=False: (_ for _ in ()).throw(ValueError("Database is locked by another convos process.")))
    r = CliRunner().invoke(cli.app, ["search", "x"])
    assert r.exit_code == 0
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
    sd = _json.loads(CliRunner().invoke(cli.app, ["search", "hello", "-f", "json"]).output)
    assert isinstance(sd, list)


def test_search_rebuilds_missing_fts_index(tmp_path, monkeypatch):
    db = tmp_path / "test.db"; monkeypatch.setattr(cli, "DB_PATH", db); monkeypatch.setattr(cli, "DATA_DIR", tmp_path)
    conn = duckdb.connect(str(db)); cli.init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','test','T',NULL,NULL,NULL,NULL,NULL,NULL,NULL)")
    conn.execute("INSERT INTO messages VALUES ('m1','c1','user','recoverable',NULL,NULL,NULL,NULL,NULL,NULL)"); conn.close()
    r = CliRunner().invoke(cli.app, ["search", "recoverable", "-f", "json"])
    assert r.exit_code == 0 and __import__("json").loads(r.output)[0]["conversation_id"] == "c1"
    conn = duckdb.connect(str(db)); assert conn.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='fts_main_messages'").fetchone()[0] == 1; conn.close()


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
