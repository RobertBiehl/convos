import json, os, sqlite3, subprocess, sys, tomllib
from pathlib import Path

import duckdb, pytest, typer
from typer.testing import CliRunner
import ai_convos_memory as memory_module
from ai_convos_memory import _current, adopt_scope_data, apply_data, audit_data, backup_data, context_data, context_hook_config, doctor_status, initialize, memory, plan_data, projection_data, reconcile_data, remote_accept, remote_records, remove_projection, restore_data, runtime_context, scan_store, sync_all_data, sync_data, write_projection


def roots(tmp_path, monkeypatch):
    codex, claude = tmp_path/"codex", tmp_path/"claude"; codex.mkdir(); claude.mkdir()
    monkeypatch.setenv("CONVOS_CODEX_MEMORY_ROOT", str(codex)); monkeypatch.setenv("CONVOS_CLAUDE_PROJECTS_ROOT", str(claude)); monkeypatch.setenv("CONVOS_MEMORY_DB", str(tmp_path/"memory.db")); monkeypatch.setenv("CODEX_HOME", str(tmp_path/"codex-home")); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path/"claude-home"))
    return codex, claude
def test_memory_product_import_does_not_reenter_core_plugins():
    result = subprocess.run([sys.executable,"-c","import sys,ai_convos_memory; assert 'ai_convos.cli' not in sys.modules"], text=True, capture_output=True)
    assert result.returncode == 0 and result.stderr == ""
def test_memory_distribution_metadata_and_public_help():
    root = Path(__file__).resolve().parents[1]; project = tomllib.loads((root/"apps"/"memory"/"pyproject.toml").read_text())["project"]; core = tomllib.loads((root/"pyproject.toml").read_text())["project"]
    assert project["readme"] == "README.md" and set(project["urls"]) == {"Documentation","Repository"} and "convos>=0.9,<0.10" in project["dependencies"] and project["entry-points"]["convos.init"] == {"memory":"ai_convos_memory:initialize"} and project["entry-points"]["convos.remote"] == {"memory":"ai_convos_memory:remote_bridge"} and memory_module.remote_bridge()["v"] == 2 and memory_module.remote_bridge()["objects"] == {"memory.canonical"} and set(memory_module.remote_bridge()) == {"v","objects","records","accept","token"} and "memory" not in core["optional-dependencies"]
    commands=typer.main.get_command(memory).commands; public={name for name,command in commands.items() if not command.hidden}; hidden={name for name,command in commands.items() if command.hidden}
    assert {"status","audit","sync","review","remember","forget","backup","restore","enable","disable","current","history"} <= public
    assert {"scan","plan","apply","reconcile","context","install-hook","project","runtime-hook","adopt-scope"} <= hidden
    for command, description in (("current","List current synchronized memories"),("audit","Check whether remembered conversation evidence"),("context","Render agent-ready"),("review","Read memory changes that still need a decision"),("remember","Create or revise a memory you own"),("forget","Delete a memory created with remember"),("backup","Save a private backup of all memory"),("restore","Preview or restore a complete memory backup"),("install-hook","Install, inspect, or remove"),("history","Show a memory's exact revision history")):
        result = CliRunner().invoke(memory, [command,"--help"]); assert result.exit_code == 0 and description in result.output
    remember,forget=commands["remember"],commands["forget"]; options=lambda command:{opt for param in command.params for opt in param.opts}; descriptions=" ".join((param.help or "") for command in (remember,forget) for param in command.params)
    assert {"--from","--replace","--project"} <= options(remember) and "--project" in options(forget) and "--scope" not in options(remember)|options(forget) and "repeatable" in descriptions and "ID, title, or text" in descriptions
def codex_memory(root, content="alpha", scope="/repo"):
    (root/"MEMORY.md").write_text(f"# Task Group: Core choice\n\nscope: test\napplies_to: cwd={scope}; reuse_rule=safe\n\n## Reusable knowledge\n\n- {content}\n")
def claude_memory(root, content="beta"):
    project, path = root/"-repo", root/"-repo"/"memory"; path.mkdir(parents=True); (path/"choice.md").write_text(content); (path/"MEMORY.md").write_text("index only")
    (project/"session.jsonl").write_text(json.dumps({"type":"system","cwd":"/repo"})+"\n")
def git(root, *args): return subprocess.run(("git","-C",str(root),*args),check=True,text=True,capture_output=True).stdout.strip()
def git_repo(root, remote=None):
    root.mkdir(); git(root,"init","-q"); git(root,"config","user.email","memory@example.test"); git(root,"config","user.name","Memory Test"); (root/"tracked").write_text("one"); git(root,"add","tracked"); git(root,"commit","-qm","initial")
    if remote: git(root,"remote","add","origin",remote)
    return root
def archive(tmp_path, monkeypatch, rows):
    from ai_convos import cli
    data, path = tmp_path/"archive", tmp_path/"archive"/"convos.db"; data.mkdir(); db=duckdb.connect(str(path))
    db.execute("CREATE TABLE conversations(id VARCHAR PRIMARY KEY,source VARCHAR,title VARCHAR,cwd VARCHAR)"); db.execute("CREATE TABLE messages(id VARCHAR PRIMARY KEY,conversation_id VARCHAR,role VARCHAR,content VARCHAR,created_at TIMESTAMP,metadata JSON)")
    [db.execute("INSERT OR IGNORE INTO conversations VALUES (?,?,?,?)",(cid,source,title,cwd)) for cid,source,title,cwd,_,_,_,_ in rows]; [db.execute("INSERT INTO messages VALUES (?,?,?,?,?,?)",(mid,cid,role,content,created,"{}")) for cid,_,_,_,mid,role,content,created in rows]; db.close()
    monkeypatch.setattr(cli,"DATA_DIR",data); monkeypatch.setattr(cli,"DB_PATH",path); monkeypatch.setattr(cli,"drain_hooks",lambda *a,**k: None); return path


def test_scan_tracks_exact_revisions_and_missing_sources(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude)
    assert scan_store() == {"scanned":2, "revisions":2, "sources":2, "active":2, "missing":0}
    codex_memory(codex, "alpha two"); (claude/"-repo"/"memory"/"choice.md").unlink()
    assert scan_store() == {"scanned":1, "revisions":1, "sources":2, "active":1, "missing":1}
    db = sqlite3.connect(tmp_path/"memory.db")
    assert db.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == 3
    contents = [r[0] for r in db.execute("SELECT content FROM revisions").fetchall()]
    assert "beta" in contents and any(c.endswith("- alpha") for c in contents) and any(c.endswith("- alpha two") for c in contents) and os.stat(tmp_path/"memory.db").st_mode&0o777 == 0o600


def test_codex_multi_cwd_declarations_expand_without_splitting_prose(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); a, b = tmp_path/"a", tmp_path/"b"; (a/".git").mkdir(parents=True); (b/".git").mkdir(parents=True); prose = "/repo with historical-reference reads in /other"
    (codex/"MEMORY.md").write_text(f"# Task Group: Shared\n\nscope: test\napplies_to: cwd={a} and {b}; reuse_rule=safe\n\n## Reusable knowledge\n\n- shared truth\n\n# Task Group: Descriptive\n\nscope: test\napplies_to: cwd={prose}; reuse_rule=safe\n\n## Reusable knowledge\n\n- descriptive truth\n")
    assert scan_store() == {"scanned":3,"revisions":3,"sources":3,"active":3,"missing":0}; db = sqlite3.connect(tmp_path/"memory.db"); rows = db.execute("SELECT id,scope,locator,content FROM sources ORDER BY scope").fetchall(); db.close()
    assert {r[1] for r in rows} == {str(a),str(b),prose} and len({r[0] for r in rows}) == 3 and len({r[2] for r in rows if r[1] in (str(a),str(b))}) == 1 and all(r[3].endswith("shared truth") for r in rows if r[1] in (str(a),str(b)))
    shared = next(r for r in rows if r[1] == str(a)); db = sqlite3.connect(tmp_path/"memory.db"); db.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?)", ("legacy","codex",f"{a} and {b}",shared[2],str(codex/"MEMORY.md"),"legacy",shared[3],1,"old")); db.execute("INSERT INTO revisions VALUES (?,?,?,?)", ("legacy","legacy",shared[3],"old")); db.commit(); db.close()
    assert scan_store() == {"scanned":3,"revisions":0,"sources":4,"active":3,"missing":1}; db = sqlite3.connect(tmp_path/"memory.db"); assert db.execute("SELECT active FROM sources WHERE id='legacy'").fetchone() == (0,) and db.execute("SELECT COUNT(*) FROM revisions WHERE source='legacy'").fetchone() == (1,); db.close()
    assert sync_data(str(a))["status"] == "clean" and len(plan_data(scope=str(b))["pending"]) == 1


def test_git_clones_and_worktrees_share_scope_while_forks_remain_distinct(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); a = git_repo(tmp_path/"a","git@github.com:Example/Memory.git"); b, worktree, fork = tmp_path/"b", tmp_path/"worktree", tmp_path/"fork"; git(tmp_path,"clone","-q",str(a),str(b)); git(b,"remote","set-url","origin","https://github.com/Example/Memory.git"); git(a,"worktree","add","-qb","memory-worktree",str(worktree)); git(tmp_path,"clone","-q",str(a),str(fork)); git(fork,"remote","set-url","origin","https://github.com/Example/Fork.git")
    (codex/"MEMORY.md").write_text(f"# Task Group: Portable\n\nscope: test\napplies_to: cwd={a} and {b} and {worktree}; reuse_rule=safe\n\n## Reusable knowledge\n\n- portable truth\n")
    shared = sync_all_data(); assert shared["status"] == "clean" and shared["automatic"] == 1 and shared["remaining"] == 0 and [r["scope"] for r in shared["scopes"]] == [str(a)]
    runner = CliRunner(); from_clone = json.loads(runner.invoke(memory, ["remember","clone-owned","--project",str(b),"--json"]).output); from_worktree = json.loads(runner.invoke(memory, ["current","--project",str(worktree),"--json"]).output)
    assert from_clone["scope"] == str(a) and {r["content"] for r in from_worktree} == {"clone-owned",(codex/"MEMORY.md").read_text().strip()}
    separate = json.loads(runner.invoke(memory, ["remember","fork-owned","--project",str(fork),"--json"]).output); assert separate["scope"] == str(fork) and [r["content"] for r in json.loads(runner.invoke(memory, ["current","--project",str(fork),"--json"]).output)] == ["fork-owned"]
    db = sqlite3.connect(tmp_path/"memory.db"); mappings = db.execute("SELECT repository,lineage,scope,checkout FROM repository_scopes ORDER BY checkout").fetchall(); db.close()
    shared_mappings = [r for r in mappings if r[2] == str(a)]; assert len(mappings) == 4 and len(shared_mappings) == 3 and len({r[0] for r in shared_mappings}) == 1 and len({r[1] for r in mappings}) == 1 and {r[2] for r in mappings} == {str(a),str(fork)}


def test_schema_v1_migration_indexes_existing_repo_scope_for_new_clone(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); a = git_repo(tmp_path/"a","https://github.com/Example/Migrate.git"); runner = CliRunner(); created = json.loads(runner.invoke(memory, ["remember","migrated","--project",str(a),"--json"]).output); db = sqlite3.connect(tmp_path/"memory.db"); db.execute("DROP TABLE repository_scopes"); db.execute("PRAGMA user_version=1"); db.commit(); db.close()
    b = tmp_path/"elsewhere"/"b"; b.parent.mkdir(); git(tmp_path,"clone","-q",str(a),str(b)); git(b,"remote","set-url","origin","git@github.com:Example/Migrate.git")
    result = json.loads(runner.invoke(memory, ["current","--project",str(b),"--json"]).output); assert [r["id"] for r in result] == [created["id"]] and result[0]["scope"] == str(a)
    db = sqlite3.connect(tmp_path/"memory.db"); assert db.execute("PRAGMA user_version").fetchone()[0] == 6 and db.execute("SELECT COUNT(DISTINCT scope),COUNT(DISTINCT checkout) FROM repository_scopes").fetchone() == (1,2); db.close()


def test_schema_v1_migration_preserves_legacy_clone_split_and_fails_closed_for_third_clone(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); a = git_repo(tmp_path/"a","https://github.com/Example/Ambiguous.git"); b, c = tmp_path/"b", tmp_path/"c"; git(tmp_path,"clone","-q",str(a),str(b)); git(tmp_path,"clone","-q",str(a),str(c)); [git(path,"remote","set-url","origin","git@github.com:Example/Ambiguous.git") for path in (b,c)]
    created = memory_module.remember_data("first",str(a)); db = sqlite3.connect(tmp_path/"memory.db"); cid, sid, digest, now = "mem_legacy","legacy-user",__import__("hashlib").sha256(b"second").hexdigest(),"2026-01-01"
    db.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?)",(sid,"user",str(b),"user/mem_legacy",str(tmp_path/"memory.db"),digest,"second",1,now)); db.execute("INSERT INTO revisions VALUES (?,?,?,?)",(sid,digest,"second",now)); db.execute("INSERT INTO canonicals VALUES (?,?,?,?,?)",(cid,str(b),"second",digest,now)); db.execute("INSERT INTO canonical_revisions VALUES (?,?,?,?)",(cid,digest,"second",now)); db.execute("INSERT INTO links VALUES (?,?,?)",(sid,cid,digest)); db.execute("DROP TABLE repository_scopes"); db.execute("PRAGMA user_version=1"); db.commit(); db.close()
    assert [r["id"] for r in json.loads(CliRunner().invoke(memory, ["current","--project",str(a),"--json"]).output)] == [created["id"]] and [r["id"] for r in json.loads(CliRunner().invoke(memory, ["current","--project",str(b),"--json"]).output)] == [cid]
    blocked = CliRunner().invoke(memory, ["current","--project",str(c),"--json"]); assert blocked.exit_code == 1 and "Repository maps to multiple memory scopes" in blocked.output and "Traceback" not in blocked.output
    preview = adopt_scope_data(str(b),str(c)); assert preview["status"] == "would_adopt" and preview["scope"] == str(b) and preview["canonicals"] == preview["sources"] == 1 and CliRunner().invoke(memory, ["current","--project",str(c),"--json"]).exit_code == 1
    adopted = json.loads(CliRunner().invoke(memory, ["adopt-scope",str(b),"--checkout",str(c),"--yes","--json"]).output); assert adopted["status"] == "adopted" and [r["id"] for r in json.loads(CliRunner().invoke(memory, ["current","--project",str(c),"--json"]).output)] == [cid]


def test_ledger_is_private_before_open_and_rejects_symlink_target(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); path, outside = tmp_path/"memory.db", tmp_path/"outside.db"; outside.write_text(""); os.chmod(outside, 0o644); path.symlink_to(outside)
    with pytest.raises(ValueError, match="non-symlink"): scan_store()
    assert outside.read_text() == "" and os.stat(outside).st_mode&0o777 == 0o644
    path.unlink(); opened = []; real = sqlite3.connect
    monkeypatch.setattr("ai_convos_memory.sqlite3.connect", lambda target,*a,**k: (opened.append(os.stat(target).st_mode&0o777) or real(target,*a,**k)))
    scan_store(); assert opened[0] == 0o600 and os.stat(path).st_mode&0o777 == 0o600
    path.write_bytes(b"not a database"); result = CliRunner().invoke(memory, ["current","--project","/repo"])
    assert result.exit_code == 1 and "not a database" in result.output and "Traceback" not in result.output


def test_ledger_adopts_version_zero_and_rejects_newer_schema(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); path = tmp_path/"memory.db"; db = sqlite3.connect(path); db.execute("CREATE TABLE legacy(value TEXT)"); db.commit(); db.close()
    scan_store(); db = sqlite3.connect(path); assert db.execute("PRAGMA user_version").fetchone()[0] == 6 and db.execute("SELECT name FROM sqlite_master WHERE name='legacy'").fetchone()
    db.execute("DROP TABLE evidence"); db.execute("PRAGMA user_version=4"); db.commit(); db.close(); scan_store(); db=sqlite3.connect(path); assert db.execute("PRAGMA user_version").fetchone()[0]==6 and db.execute("SELECT name FROM sqlite_master WHERE name='evidence'").fetchone()==("evidence",)
    db.execute("DROP TABLE projections"); db.execute("PRAGMA user_version=7"); db.commit(); db.close()
    with pytest.raises(ValueError, match="ledger schema version"): scan_store()
    db = sqlite3.connect(path); assert db.execute("PRAGMA user_version").fetchone()[0] == 7 and not db.execute("SELECT name FROM sqlite_master WHERE name='projections'").fetchone(); db.close()
    for args in (["current","--project","/repo"],["history","source"]):
        result = CliRunner().invoke(memory, args); assert result.exit_code == 1 and "Unsupported memory ledger schema version" in result.output and "Traceback" not in result.output


def test_plan_resolve_apply_and_stale_hash_guard(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); first = plan_data(True)
    assert first["version"] == 1 and len(first["pending"]) == 2 and {p["kind"] for p in first["pending"]} == {"unlinked"} and first["canonicals"] == [] and all(p["ancestor"] is None for p in first["pending"])
    a, b = first["pending"]; result = apply_data({"version":1,"plan":first["plan"], "resolutions":[{"source":a["source"],"action":"distinct"},{"source":b["source"],"action":"same","canonical":"mem_"+__import__("hashlib").sha256((a["scope"]+"\0"+a["content"]).encode()).hexdigest()[:16]}]})
    assert result == {"version":1,"applied":2,"remaining":0} and len(plan_data()["pending"]) == 0
    codex_memory(codex, "changed while resolving")
    with pytest.raises(ValueError, match="stale plan"): apply_data({"version":1,"plan":first["plan"], "resolutions":[]}, True)


def test_apply_rejects_missing_or_unknown_protocol_before_scan(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); monkeypatch.setattr("ai_convos_memory.scan_store", lambda: (_ for _ in ()).throw(AssertionError("provider scan occurred")))
    for document in ([], {}, {"version":True}, {"version":2}):
        with pytest.raises(ValueError, match="resolution version"): apply_data(document)
    malformed = ({"version":1}, {"version":1,"plan":"p","resolutions":[{}]}, {"version":1,"plan":"p","resolutions":[{"action":"distinct","sources":[]}]}, {"version":1,"plan":"p","scope":1,"resolutions":[]})
    for document in malformed:
        with pytest.raises(ValueError, match="Malformed memory resolution"): apply_data(document)
    result = CliRunner().invoke(memory, ["apply","-"], input=json.dumps(malformed[1]))
    assert result.exit_code == 1 and "Malformed memory resolution" in result.output and "Traceback" not in result.output and not (tmp_path/"memory.db").exists()
    missing = CliRunner().invoke(memory, ["apply",str(tmp_path/"missing.json")])
    assert missing.exit_code == 1 and "No such file" in missing.output and "Traceback" not in missing.output


def test_reconcile_applies_only_exact_same_scope_matches(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); scan_store(); first = plan_data(True, scope="/repo")
    apply_data({"version":1,"plan":first["plan"],"scope":"/repo","resolutions":[{"source":first["pending"][0]["source"],"action":"distinct"}]})
    project, text = claude/"-repo", first["pending"][0]["content"]; (project/"memory").mkdir(parents=True); (project/"memory"/"same.md").write_text(text); (project/"memory"/"different.md").write_text("different"); (project/"session.jsonl").write_text(json.dumps({"cwd":"/repo"})+"\n")
    preview = reconcile_data("/repo", True)
    assert preview["dry_run"] and preview["applied"] == 1 and preview["remaining"] == 1 and {p["kind"] for p in plan_data(scope="/repo")["pending"]} == {"exact","unlinked"}
    result = json.loads(CliRunner().invoke(memory, ["sync","--project","/repo","--json"]).output)
    assert result["status"] == "needs_resolution" and result["automatic"] == result["remaining"] == 1 and result["pending"][0]["kind"] == "unlinked"
    output = CliRunner().invoke(memory, ["sync","--project","/repo"]).output; assert "1 change needs a decision" in output and 'ask Codex or Claude: "sync my memories"' in output and all(word not in output for word in ("deterministic","resolution","canonical"))
    review=CliRunner().invoke(memory,["review","--project","/repo"]).output; pending=result["pending"][0]; assert "# Memory changes" in review and "1 change needs a decision" in review and "## Change from Claude: different" in review and "### New version from Claude" in review and pending["content"] in review and pending["source"] not in review and pending["locator"] not in review


def test_sync_bootstraps_only_unambiguous_new_memory(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); first = sync_data("/repo")
    assert first["status"] == "clean" and first["automatic"] == 1 and first["remaining"] == 0 and len(first["canonicals"]) == 1
    assert "Memory is up to date for /repo: nothing changed." in CliRunner().invoke(memory, ["sync","--project","/repo"]).output
    (tmp_path/"memory.db").unlink(); claude_memory(claude, (codex/"MEMORY.md").read_text().strip()); exact = sync_data("/repo")
    assert exact["status"] == "clean" and exact["automatic"] == 2 and exact["remaining"] == 0 and len(exact["canonicals"]) == 1
    (claude/"-repo"/"memory"/"choice.md").write_text("different"); changed = sync_data("/repo")
    assert changed["status"] == "needs_resolution" and changed["automatic"] == 0 and changed["remaining"] == 1


def test_user_owned_remember_revise_and_forget_are_scoped_and_auditable(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); repo = tmp_path/"repo"; repo.mkdir(); (repo/".git").mkdir(); monkeypatch.chdir(repo); runner = CliRunner()
    created = json.loads(runner.invoke(memory, ["remember","Prefer concise answers.","--json"]).output); cid, sid = created["id"], created["source"]
    assert created["scope"] == str(repo) and created["status"] == "created" and json.loads(runner.invoke(memory, ["remember","Prefer concise answers.","--json"]).output)["status"] == "unchanged"
    assert f"Memory unchanged for `{repo}`." in runner.invoke(memory, ["remember","Prefer concise answers."]).output
    current = json.loads(runner.invoke(memory, ["current","--json"]).output); owned = json.loads(runner.invoke(memory, ["current","--owned","--json"]).output); indexed = runner.invoke(memory, ["context","--index"]).output; status = json.loads(runner.invoke(memory, ["status","--json"]).output)
    assert [r["id"] for r in current] == [cid] == [r["id"] for r in owned] and current[0]["origins"][0]["provider"] == "user" and status["user_owned"] == 1 and f"`{cid}` [user] Prefer concise answers." in indexed and "across 1 memory." in indexed
    revised = json.loads(runner.invoke(memory, ["remember","Prefer concise factual answers.","--replace","prefer concise answers.","--json"]).output)
    readable=runner.invoke(memory,["current"]).output
    assert revised["id"] == cid and revised["status"] == "revised" and json.loads(runner.invoke(memory, ["history","concise factual","--json"]).output)["kind"] == "canonical" and len(json.loads(runner.invoke(memory, ["history",sid,"--json"]).output)["revisions"]) == 2 and f"## Prefer concise factual answers.\n\nID: `{cid}`" in readable
    history = runner.invoke(memory, ["history",sid]).output; assert "# Imported memory history" in history and f"ID: `{sid}`" in history and "Provider: user" in history and "Revisions: 2" in history and "Prefer concise factual answers." in history
    project = claude/"project"; (project/"memory").mkdir(parents=True); (project/"session.jsonl").write_text(json.dumps({"cwd":str(repo)})+"\n"); assert runner.invoke(memory, ["project","--write"]).exit_code == 0
    projected = runner.invoke(memory, ["forget","concise factual","--dry-run"]); assert projected.exit_code == 1 and "present in a Claude projection" in projected.output
    assert runner.invoke(memory, ["project","--remove"]).exit_code == 0; preview = json.loads(runner.invoke(memory, ["forget","concise factual","--dry-run","--json"]).output); assert preview["status"] == "would_forget" and preview["id"]==cid and len(json.loads(runner.invoke(memory, ["current","--json"]).output)) == 1
    human_preview = runner.invoke(memory, ["forget",cid,"--dry-run"]).output; assert f"Would forget `{cid}`" in human_preview and "would purge 4 stored revision records" in human_preview and "without --dry-run to confirm" in human_preview
    forgotten = json.loads(runner.invoke(memory, ["forget",cid,"--json"]).output); db = sqlite3.connect(tmp_path/"memory.db")
    assert forgotten["status"] == "forgotten" and forgotten["revisions"] == 4 and json.loads(runner.invoke(memory, ["current","--json"]).output) == [] and json.loads(runner.invoke(memory, ["status","--json"]).output)["user_owned"] == 0 and db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0] == db.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 0; db.close()
    empty = runner.invoke(memory, ["remember","  "]); assert empty.exit_code == 1 and "cannot be empty" in empty.output and "Traceback" not in empty.output


def test_human_memory_selectors_are_literal_scoped_and_fail_closed(tmp_path,monkeypatch):
    roots(tmp_path,monkeypatch); runner=CliRunner(); alpha=json.loads(runner.invoke(memory,["remember","Alpha rule\nUse one.","--project","/repo","--json"]).output); beta=json.loads(runner.invoke(memory,["remember","Beta rule\nUse two.","--project","/repo","--json"]).output)
    revised=json.loads(runner.invoke(memory,["remember","Alpha rule revised\nUse three.","--project","/repo","--replace","Alpha rule","--json"]).output); assert revised["id"]==alpha["id"] and revised["status"]=="revised"
    other=json.loads(runner.invoke(memory,["remember","Alpha rule revised\nOther scope.","--project","/other","--json"]).output)
    assert json.loads(runner.invoke(memory,["history","alpha rule revised","--project","/repo","--json"]).output)["id"]==alpha["id"] and json.loads(runner.invoke(memory,["history","alpha rule revised","--project","/other","--json"]).output)["id"]==other["id"]
    for args in (["remember","wrong","--project","/repo","--replace","rule"],["forget","rule","--project","/repo","--dry-run"],["history","rule","--project","/repo"]):
        failed=runner.invoke(memory,args); assert failed.exit_code==1 and "memory selector is ambiguous" in failed.output and "Traceback" not in failed.output
    assert {r["id"] for r in json.loads(runner.invoke(memory,["current","--project","/repo","--json"]).output)}=={alpha["id"],beta["id"]}
    preview=json.loads(runner.invoke(memory,["forget","Beta rule","--project","/repo","--dry-run","--json"]).output); assert preview["id"]==beta["id"] and preview["status"]=="would_forget"
    assert json.loads(runner.invoke(memory,["forget","Beta rule","--project","/repo","--json"]).output)["status"]=="forgotten" and [r["id"] for r in json.loads(runner.invoke(memory,["current","--project","/repo","--json"]).output)]==[alpha["id"]]


def test_user_owned_commands_refuse_provider_backed_canonical(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); codex_memory(codex); canonical = sync_data("/repo")["canonicals"][0]["id"]; runner = CliRunner()
    assert json.loads(runner.invoke(memory, ["current","--project","/repo","--owned","--json"]).output) == []
    revise = runner.invoke(memory, ["remember","replacement","--project","/repo","--replace",canonical]); forget = runner.invoke(memory, ["forget",canonical,"--project","/repo"])
    assert revise.exit_code == forget.exit_code == 1 and "without Codex or Claude origins" in revise.output and "without Codex or Claude origins" in forget.output
    linked = json.loads(runner.invoke(memory, ["remember",(codex/"MEMORY.md").read_text().strip(),"--project","/repo","--json"]).output); assert linked["id"] == canonical and linked["status"] == "linked"
    assert {o["provider"] for o in json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)[0]["origins"]} == {"codex","user"} and [r["id"] for r in json.loads(runner.invoke(memory, ["current","--project","/repo","--owned","--json"]).output)] == [canonical] and runner.invoke(memory, ["forget",canonical,"--project","/repo"]).exit_code == 1


def test_remember_attaches_local_archive_evidence_to_exact_revisions(tmp_path, monkeypatch):
    roots(tmp_path,monkeypatch); m1,m2="message000000000001","message000000000002"; path=archive(tmp_path,monkeypatch,[("conversation0000001","codex","Decision","/repo",m1,"user","private first turn","2026-01-01"),("conversation0000001","codex","Decision","/repo",m2,"assistant","private second turn","2026-01-02")]); runner=CliRunner()
    created=json.loads(runner.invoke(memory,["remember","Prefer the proven path.","--project","/repo","--from",m1,"--from",m2,"--json"]).output); cid=created["id"]
    assert created["evidence"]==2 and created["status"]=="created"; current=json.loads(runner.invoke(memory,["current","--project","/repo","--json"]).output)[0]
    assert [e["message"] for e in current["evidence"]]==[m1,m2] and {e["status"] for e in current["evidence"]}=={"verified"} and all(e["read"]==f"convos read conversation0000001 --around {e['message']}" for e in current["evidence"])
    human=runner.invoke(memory,["current","--project","/repo"]).output; context=runner.invoke(memory,["context","--scope","/repo"]).output
    assert "[verified] codex Decision" in human and all(f"convos read conversation0000001 --around {mid}" in human+context for mid in (m1,m2)) and "private first turn" not in human+context
    revised=json.loads(runner.invoke(memory,["remember","Prefer the revised proven path.","--project","/repo","--replace",cid,"--from",m2,"--json"]).output); history=json.loads(runner.invoke(memory,["history",cid,"--json"]).output)
    assert revised["evidence"]==1 and [len(r["evidence"]) for r in history["revisions"]]==[2,1]
    db=duckdb.connect(str(path)); db.execute("UPDATE messages SET content='changed archive turn' WHERE id=?",[m1]); db.execute("DELETE FROM messages WHERE id=?",[m2]); db.close(); history=json.loads(runner.invoke(memory,["history",cid,"--json"]).output)
    assert [[e["status"] for e in r["evidence"]] for r in history["revisions"]]==[["changed","missing"],["missing"]]
    ledger=sqlite3.connect(tmp_path/"memory.db"); assert [r[1] for r in ledger.execute("PRAGMA table_info(evidence)")] == ["canonical","hash","message","conversation","source","title","role","created_at","content_hash"] and ledger.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]==3; ledger.close()
    target=Path(backup_data(tmp_path/"with-evidence.db")["path"]); remote=tmp_path/"remote-root"; (remote/"memory").mkdir(parents=True); (remote/"memory"/"state.db").write_bytes(target.read_bytes()); payload=json.dumps(remote_records(remote,"u","personal","personal"))
    assert all(secret not in payload for secret in (m1,m2,"conversation0000001","content_hash","evidence")); memory_module.forget_data(cid,"/repo"); assert sqlite3.connect(tmp_path/"memory.db").execute("SELECT COUNT(*) FROM evidence").fetchone()[0]==0
    assert restore_data(target,True)["status"]=="restored" and sqlite3.connect(tmp_path/"memory.db").execute("SELECT COUNT(*) FROM evidence").fetchone()[0]==3


def test_remember_evidence_is_exact_scope_checked_and_allows_cwdless_web_turns(tmp_path, monkeypatch):
    roots(tmp_path,monkeypatch); rows=[("conversation0000001","codex","One","/repo","message000000000001","user","one","2026-01-01"),("conversation0000002","codex","Two","/repo","message000000000002","assistant","two","2026-01-02"),("conversation0000003","codex","Other","/other","message000000000003","user","other","2026-01-03"),("conversation0000004","chatgpt","Web",None,"message000000000004","assistant","web","2026-01-04")]; archive(tmp_path,monkeypatch,rows); runner=CliRunner()
    for ref,error in (("message","ambiguous"),("absent","missing"),("message000000000003","outside memory scope")):
        result=runner.invoke(memory,["remember","blocked","--project","/repo","--from",ref]); assert result.exit_code==1 and error in result.output and "Traceback" not in result.output
    result=json.loads(runner.invoke(memory,["remember","portable web fact","--project","/repo","--from","message000000000004","--json"]).output); assert result["evidence"]==1
    assert json.loads(runner.invoke(memory,["current","--project","/repo","--json"]).output)[0]["evidence"][0]["source"]=="chatgpt"


def test_evidence_audit_finds_reverse_provenance_and_reports_live_health_without_content(tmp_path,monkeypatch):
    roots(tmp_path,monkeypatch); m1,m2,m3="message000000000001","message000000000002","message000000000003"; path=archive(tmp_path,monkeypatch,[("conversation0000001","codex","Decision","/repo",m1,"user","private first turn","2026-01-01"),("conversation0000001","codex","Decision","/repo",m2,"assistant","private second turn","2026-01-02"),("conversation0000002","chatgpt","Web",None,m3,"assistant","private web turn","2026-01-03")]); runner=CliRunner()
    first=json.loads(runner.invoke(memory,["remember","private old memory","--project","/repo","--from",m1,"--from",m2,"--json"]).output); runner.invoke(memory,["remember","private current memory","--project","/repo","--replace",first["id"],"--from",m2]); runner.invoke(memory,["remember","private other memory","--project","/other","--from",m3])
    clean=audit_data("/repo"); reverse=audit_data("/repo",m2); all_=audit_data()
    assert clean["evidence"]==clean["verified"]==3 and clean["changed"]==clean["missing"]==clean["unavailable"]==0 and sum(r["current"] for r in clean["records"])==1
    assert reverse["message"]==m2 and len(reverse["records"])==2 and {r["canonical"] for r in reverse["records"]}=={first["id"]} and {r["current"] for r in reverse["records"]}=={False,True}
    assert all_["evidence"]==4 and {r["scope"] for r in all_["records"]}=={"/repo","/other"} and "All evidence links verified." in runner.invoke(memory,["audit","--project","/repo"]).output
    db=duckdb.connect(str(path)); db.execute("UPDATE messages SET content='changed archive turn' WHERE id=?",[m1]); db.execute("DELETE FROM messages WHERE id=?",[m2]); db.close(); result=json.loads(runner.invoke(memory,["audit","--project","/repo","--json"]).output); human=runner.invoke(memory,["audit","--project","/repo"]).output
    assert result["changed"]==1 and result["missing"]==2 and result["verified"]==0 and {r["status"] for r in result["records"]}=={"changed","missing"} and "[changed] historical" in human and "[missing] current" in human
    assert all(secret not in json.dumps(result)+human for secret in ("private first turn","private second turn","private old memory","private current memory")) and f"convos read conversation0000001 --around {m1}" in human
    for args in (["audit","--project","/repo","--message","message"],["audit","--project","/repo","--message",m3],["audit","--all","--project","/repo"]):
        failed=runner.invoke(memory,args); assert failed.exit_code==1 and "Traceback" not in failed.output
    monkeypatch.setattr("ai_convos.cli.DB_PATH",tmp_path/"missing.db"); unavailable=audit_data("/repo"); assert unavailable["unavailable"]==3 and unavailable["verified"]==unavailable["changed"]==unavailable["missing"]==0


def test_private_snapshot_preview_restore_and_rescue(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); runner = CliRunner(); first = json.loads(runner.invoke(memory, ["remember","first","--project","/repo","--json"]).output); target = tmp_path/"backup.db"
    result = backup_data(target); assert result["status"] == "backed_up" and result["canonicals"] == 1 and result["revisions"] == 2 and result["scopes"] == 1 and os.stat(target).st_mode&0o777 == 0o600 and sqlite3.connect(target).execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    runner.invoke(memory, ["remember","second","--project","/repo"]); preview = restore_data(target); assert preview["status"] == "would_restore" and len(json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)) == 2
    restored = restore_data(target, True); rows = json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)
    assert restored["status"] == "restored" and Path(restored["rescue"]).is_file() and os.stat(restored["rescue"]).st_mode&0o777 == 0o600 and [r["id"] for r in rows] == [first["id"]]
    assert json.loads(runner.invoke(memory, ["restore",restored["rescue"],"--json"]).output)["status"] == "would_restore" and "Run again with --yes" in runner.invoke(memory, ["restore",restored["rescue"]]).output
    assert "Memory backed up to" in runner.invoke(memory, ["backup",str(tmp_path/"cli.db")]).output and runner.invoke(memory, ["backup",str(tmp_path/"cli.db")]).exit_code == 1


def test_restore_migrates_valid_older_snapshot_without_modifying_source(tmp_path, monkeypatch):
    roots(tmp_path,monkeypatch); created=memory_module.remember_data("before schema upgrade","/repo"); current=Path(backup_data(tmp_path/"current.db")["path"]); legacy=tmp_path/"schema3.db"; legacy.write_bytes(current.read_bytes()); db=sqlite3.connect(legacy); db.execute("DROP TABLE remote_semantics"); db.execute("DROP TABLE remote_meta"); db.execute("CREATE TABLE remote_parts(x TEXT)"); db.execute("PRAGMA user_version=3"); db.commit(); db.close(); memory_module.remember_data("new live state","/repo")
    preview=restore_data(legacy); assert preview["status"]=="would_restore" and preview["canonicals"]==1 and sqlite3.connect(legacy).execute("PRAGMA user_version").fetchone()[0]==3 and sqlite3.connect(legacy).execute("SELECT name FROM sqlite_master WHERE name='remote_parts'").fetchone()
    restored=restore_data(legacy,True); db=sqlite3.connect(tmp_path/"memory.db"); assert restored["status"]=="restored" and db.execute("PRAGMA user_version").fetchone()[0]==6 and db.execute("SELECT id,content FROM canonicals").fetchall()==[(created["id"],"before schema upgrade")] and not db.execute("SELECT name FROM sqlite_master WHERE name='remote_parts'").fetchone() and db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0]==0 and db.execute("SELECT value FROM remote_meta WHERE key='ledger_id'").fetchone(); db.close()


def test_restore_rejects_unsafe_or_incompatible_snapshot_without_touching_live_ledger(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); runner = CliRunner(); created = json.loads(runner.invoke(memory, ["remember","safe","--project","/repo","--json"]).output); bad, outside, link = tmp_path/"bad.db", tmp_path/"outside.db", tmp_path/"link.db"
    db = sqlite3.connect(bad); db.execute("PRAGMA user_version=2"); db.close(); outside.write_bytes(bad.read_bytes()); link.symlink_to(outside); altered = backup_data(tmp_path/"altered.db")["path"]; db = sqlite3.connect(altered); db.execute("CREATE TRIGGER injected AFTER INSERT ON canonicals BEGIN DELETE FROM canonicals; END"); db.commit(); db.close()
    for path, message in ((bad,"Invalid or incompatible"),(altered,"Invalid or incompatible"),(link,"regular non-symlink"),(tmp_path/"missing.db","regular non-symlink"),(tmp_path/"memory.db","must be different")):
        result = runner.invoke(memory, ["restore",str(path),"--yes"]); assert result.exit_code == 1 and message in result.output and "Traceback" not in result.output
    assert [r["id"] for r in json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)] == [created["id"]] and not (tmp_path/"backups").exists()


def test_remote_memory_replays_updates_conflicts_and_tombstones_without_paths(tmp_path, monkeypatch):
    from ai_convos_remote.protocol import identity, public_id, semantic_proof
    roots(tmp_path, monkeypatch); a, b = tmp_path/"device-a", tmp_path/"device-b"; a.mkdir(); b.mkdir(); repo_a = git_repo(a/"repo","git@github.com:Example/Portable.git"); git(tmp_path,"clone","-q",str(repo_a),str(b/"repo")); git(b/"repo","remote","set-url","origin","https://github.com/Example/Portable.git")
    monkeypatch.delenv("CONVOS_MEMORY_DB"); root,device=identity("root"),identity("a"); user=public_id(root["sign_public"]); sign=lambda item:semantic_proof(root,user,device["id"],"personal",1,item["row"],item["previous"]); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); created=memory_module.remember_data("portable one",str(repo_a)); record=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); first_row=record["row"]; one=sign(record); remote_accept(a,first_row,one,False)
    assert remote_records(a,user,"team","team") == [] and str(repo_a) not in json.dumps(record) and record["row"]["data"]["repository"].startswith("repo_") and remote_accept(b,record["row"],one)
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(b)); target = memory_module._scope(str(b/"repo")); db = memory_module.connect(b); assert db.execute("SELECT content FROM canonicals WHERE scope=?",(target,)).fetchone()[0] == "portable one"; db.close()
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.remember_data("portable two",str(repo_a),created["id"]); record=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); two=semantic_proof(root,user,identity("b")["id"],"personal",1,record["row"],record["previous"]); remote_accept(a,record["row"],two,False); assert remote_accept(b,record["row"],two) and remote_accept(b,first_row,one)
    db = memory_module.connect(b); cid = db.execute("SELECT id FROM canonicals WHERE scope=?",(target,)).fetchone()[0]; assert db.execute("SELECT content FROM canonicals WHERE id=?",(cid,)).fetchone()[0] == "portable two" and db.execute("SELECT COUNT(*) FROM sources WHERE provider='remote'").fetchone()[0]==1; memory_module._canonical(db,cid,target,"local divergence","2026-01-02T12:00:00Z"); db.commit(); db.close()
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.remember_data("portable three",str(repo_a),created["id"]); record=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); three=sign(record); remote_accept(a,record["row"],three,False); assert remote_accept(b,record["row"],three)
    db = memory_module.connect(b); assert db.execute("SELECT content FROM canonicals WHERE id=?",(cid,)).fetchone()[0] == "local divergence" and plan_data(True,db,target)["pending"][0]["kind"] == "changed"; db.close()
    memory_module.forget_data(created["id"],str(repo_a)); deleted=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); assert deleted["row"]["state"]=="deleted" and deleted["row"]["data"] is None; tombstone=sign(deleted); remote_accept(a,deleted["row"],tombstone,False); assert remote_accept(b,deleted["row"],tombstone)
    db = memory_module.connect(b); assert tuple(db.execute("SELECT active,content FROM sources WHERE provider='remote'").fetchone()) == (0,"") and db.execute("SELECT COUNT(*) FROM revisions r JOIN sources s ON s.id=r.source WHERE s.provider='remote'").fetchone()[0] == 0 and db.execute("SELECT content FROM canonicals WHERE id=?",(cid,)).fetchone()[0] == "local divergence" and plan_data(db=db,scope=target)["pending"][0]["kind"] == "missing"; db.close()


def test_large_remote_memory_is_one_semantic_replica_without_historical_bodies(tmp_path, monkeypatch):
    from ai_convos_remote.protocol import identity, public_id, semantic_proof
    roots(tmp_path,monkeypatch); a,b=tmp_path/"a",tmp_path/"b"; a.mkdir(); monkeypatch.delenv("CONVOS_MEMORY_DB"); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); first="large marker\n"+"x"*70000; created=memory_module.remember_data(first,"global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); proof=semantic_proof(root,user,device["id"],"personal",1,item["row"]); assert "large marker" not in json.dumps(proof) and remote_accept(a,item["row"],proof,False) and remote_accept(b,item["row"],proof)
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); second="new marker\n"+"y"*70000; memory_module.remember_data(second,"global",created["id"]); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); newer=semantic_proof(root,user,device["id"],"personal",1,item["row"],item["previous"]); remote_accept(a,item["row"],newer,False); assert remote_accept(b,item["row"],newer); db=memory_module.connect(b); assert db.execute("SELECT content FROM canonicals").fetchone()[0]==second and db.execute("SELECT COUNT(*) FROM remote_semantics").fetchone()[0]==1 and first not in db.execute("SELECT proof FROM remote_semantics").fetchone()[0]; db.close()


def test_concurrent_semantic_proofs_wait_for_resolution_then_merge_ancestry(tmp_path,monkeypatch):
    from ai_convos_remote.protocol import identity,public_id,semantic_proof
    roots(tmp_path,monkeypatch); a,b=tmp_path/"a",tmp_path/"b"; a.mkdir(); monkeypatch.delenv("CONVOS_MEMORY_DB"); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); root,one,two=identity("root"),identity("one"),identity("two"); user=public_id(root["sign_public"]); memory_module.remember_data("base","global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); first=semantic_proof(root,user,one["id"],"personal",1,item["row"]); remote_accept(a,item["row"],first,False); remote_accept(b,item["row"],first); row=lambda content:{**item["row"],"data":{**item["row"]["data"],"hash":memory_module._hash(content),"content":content}}; left,right=row("left"),row("right"); pl,pr=semantic_proof(root,user,one["id"],"personal",1,left,first),semantic_proof(root,user,two["id"],"personal",1,right,first); assert remote_accept(b,left,pl) and not remote_accept(b,right,pr)
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(b)); plan=plan_data(True,scope="global"); cid=plan["canonicals"][0]["id"]; assert len(plan["pending"])==1 and apply_data(dict(version=1,plan=plan["plan"],scope="global",resolutions=[dict(action="merge",source=plan["pending"][0]["source"],canonical=cid,content="resolved")]))["remaining"]==0; item=next(r for r in remote_records(b,user,"personal","personal") if r["proof"] is None); merged=semantic_proof(root,user,two["id"],"personal",1,item["row"],item["previous"]); remote_accept(b,item["row"],merged,False); db=memory_module.connect(b); assert {pl["revision"],pr["revision"],first["revision"]}<=set(merged["ancestors"]) and db.execute("SELECT COUNT(*) FROM remote_semantics").fetchone()[0]==1 and db.execute("SELECT content FROM canonicals").fetchone()[0]=="resolved"; db.close()


def test_concurrent_remote_tombstone_never_erases_active_branch(tmp_path,monkeypatch):
    from ai_convos_remote.protocol import identity,public_id,semantic_proof
    roots(tmp_path,monkeypatch); b=tmp_path/"b"; root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); row=lambda content:{"v":1,"kind":"memory.canonical","id":"memory:global:m","state":"active","data":{"canonical":"m","scope":"global","repository":None,"lineage":None,"hash":memory_module._hash(content),"content":content,"updated_at":"2026-01-01"}}; base,left=row("base"),row("left"); first=semantic_proof(root,user,device["id"],"personal",1,base); branch=semantic_proof(root,user,device["id"],"personal",1,left,first); deleted={**base,"state":"deleted","data":None}; tombstone=semantic_proof(root,user,device["id"],"personal",1,deleted,first); assert remote_accept(b,base,first) and remote_accept(b,left,branch) and not remote_accept(b,deleted,tombstone); db=memory_module.connect(b); assert db.execute("SELECT content FROM canonicals").fetchone()[0]=="left" and plan_data(db=db,scope="global")["pending"][0]["kind"]=="missing"; db.close(); monkeypatch.delenv("CONVOS_MEMORY_DB"); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(b)); memory_module.forget_data("m","global"); item=next(r for r in remote_records(b,user,"personal","personal") if r["proof"] is None); resolved=semantic_proof(root,user,device["id"],"personal",1,item["row"],item["previous"]); assert {branch["revision"],tombstone["revision"]}<=set(resolved["ancestors"])


def test_remote_tombstone_purges_unchanged_remote_only_memory_and_decrypted_state(tmp_path, monkeypatch):
    from ai_convos_remote.protocol import identity, public_id, semantic_proof
    roots(tmp_path,monkeypatch); a,b=tmp_path/"a",tmp_path/"b"; a.mkdir(); monkeypatch.delenv("CONVOS_MEMORY_DB"); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); created=memory_module.remember_data("forget me remotely","global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); active=semantic_proof(root,user,device["id"],"personal",1,item["row"]); remote_accept(a,item["row"],active,False); remote_accept(b,item["row"],active)
    memory_module.forget_data(created["id"],"global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); tombstone=semantic_proof(root,user,device["id"],"personal",1,item["row"],item["previous"]); remote_accept(a,item["row"],tombstone,False); assert remote_accept(b,item["row"],tombstone)
    db=memory_module.connect(b); assert db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0]==db.execute("SELECT COUNT(*) FROM sources").fetchone()[0]==db.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]==0 and db.execute("SELECT COUNT(*) FROM remote_semantics WHERE state='deleted'").fetchone()[0]==1 and "forget me remotely" not in db.execute("SELECT body||proof FROM remote_semantics").fetchone()[0]; db.close()


def test_remote_tombstone_redacts_origin_but_preserves_mixed_local_memory(tmp_path, monkeypatch):
    from ai_convos_remote.protocol import identity, public_id, semantic_proof
    roots(tmp_path,monkeypatch); a,b=tmp_path/"a",tmp_path/"b"; a.mkdir(); monkeypatch.delenv("CONVOS_MEMORY_DB"); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); root,device=identity("root"),identity("device"); user=public_id(root["sign_public"]); created=memory_module.remember_data("shared local truth","global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); active=semantic_proof(root,user,device["id"],"personal",1,item["row"]); remote_accept(a,item["row"],active,False); remote_accept(b,item["row"],active)
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(b)); linked=memory_module.remember_data("shared local truth","global"); assert linked["status"]=="linked"; monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(a)); memory_module.forget_data(created["id"],"global"); item=next(r for r in remote_records(a,user,"personal","personal") if r["proof"] is None); tombstone=semantic_proof(root,user,device["id"],"personal",1,item["row"],item["previous"]); remote_accept(a,item["row"],tombstone,False); remote_accept(b,item["row"],tombstone)
    db=memory_module.connect(b); sources=[tuple(r) for r in db.execute("SELECT provider,active,content FROM sources ORDER BY provider")]; assert db.execute("SELECT content FROM canonicals").fetchone()[0]=="shared local truth" and sources==[("remote",0,""),("user",1,"shared local truth")] and plan_data(db=db,scope="global")["pending"][0]["kind"]=="missing"; db.close()


def test_sync_all_settles_each_scope_independently_and_reports_only_counts(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); a, b = tmp_path/"a", tmp_path/"b"; (a/".git").mkdir(parents=True); (b/".git").mkdir(parents=True)
    write = lambda one: (codex/"MEMORY.md").write_text(f"# Task Group: A\n\nscope: test\napplies_to: cwd={a}; reuse_rule=safe\n\n## Reusable knowledge\n\n- {one}\n\n# Task Group: B\n\nscope: test\napplies_to: cwd={b}; reuse_rule=safe\n\n## Reusable knowledge\n\n- shared\n")
    write("shared"); result = sync_all_data()
    assert result["scope"] is None and result["status"] == "clean" and result["automatic"] == 2 and result["remaining"] == 0 and {r["scope"] for r in result["scopes"]} == {str(a),str(b)} and all(set(r) == {"scope","status","automatic","remaining"} for r in result["scopes"])
    db = sqlite3.connect(tmp_path/"memory.db"); canonicals = db.execute("SELECT id,scope FROM canonicals ORDER BY scope").fetchall(); db.close(); assert len({r[0] for r in canonicals}) == 2 and {r[1] for r in canonicals} == {str(a),str(b)}
    write("changed"); runner = CliRunner(); data = json.loads(runner.invoke(memory, ["sync","--all","--json"]).output); attention = [r for r in data["scopes"] if r["remaining"]]
    assert data["status"] == "needs_resolution" and data["automatic"] == 0 and data["remaining"] == 1 and attention == [{"automatic":0,"remaining":1,"scope":str(a),"status":"needs_resolution"}] and "content" not in json.dumps(data)
    human = runner.invoke(memory, ["sync","--all"]); conflict = runner.invoke(memory, ["sync","--all","--project",str(a)])
    assert "across 2 projects: nothing changed. 1 change needs a decision in 1 project." in human.output and f"- {a}: 1 change needs review." in human.output and conflict.exit_code == 1 and "--all cannot be combined with --project" in conflict.output


def test_deterministic_reconcile_converges_when_another_session_wins(tmp_path, monkeypatch):
    import ai_convos_memory as module
    codex, _ = roots(tmp_path, monkeypatch); codex_memory(codex); real, raced = module.apply_data, []
    def competing_apply(document, dry_run=False): raced.append(True); real(document); return real(document, dry_run)
    monkeypatch.setattr(module, "apply_data", competing_apply); result = reconcile_data("/repo")
    assert raced == [True] and result["applied"] == result["remaining"] == 0 and plan_data(scope="/repo")["pending"] == []
    db = sqlite3.connect(tmp_path/"memory.db"); assert db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0] == db.execute("SELECT COUNT(*) FROM links").fetchone()[0] == 1


def test_sync_and_status_default_to_current_git_root(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); repo, sub = tmp_path/"repo", tmp_path/"repo"/"sub"; sub.mkdir(parents=True); (repo/".git").mkdir(); other_repo, other = tmp_path/"other", claude/"other"; (other_repo/".git").mkdir(parents=True); (other/"memory").mkdir(parents=True); (other/"memory"/"choice.md").write_text("other truth"); (other/"session.jsonl").write_text(json.dumps({"cwd":str(other_repo)})+"\n"); monkeypatch.chdir(sub); codex_memory(codex, scope=str(sub))
    result = sync_data(); assert result["scope"] == str(repo) and result["status"] == "clean" and result["automatic"] == 1 and result["remaining"] == 0
    runner = CliRunner(); human = runner.invoke(memory, ["status"]).output; assert f"Memory ready for {repo}" in human and "1 memory, 0 remembered, 0 changes need review" in human and all(word not in human for word in ("source","revision","canonical","database"))
    status = json.loads(runner.invoke(memory, ["status","--json"]).output); assert status["scope"] == str(repo) and status["sources"] == status["active"] == status["canonicals"] == 1 and status["pending"] == 0
    other_scoped = json.loads(runner.invoke(memory, ["plan","--scope",str(other_repo)]).output); scoped = json.loads(runner.invoke(memory, ["plan"]).output); assert len(other_scoped["pending"]) == 1 and scoped["scope"] == str(repo) and scoped["pending"] == [] and sqlite3.connect(tmp_path/"memory.db").execute("SELECT active FROM sources WHERE scope=?", (str(other_repo),)).fetchone()[0] == 1 and json.loads(runner.invoke(memory, ["current","--json"]).output)[0]["scope"] == str(repo) and "Synchronized memory context" in runner.invoke(memory, ["context"]).output
    assert json.loads(runner.invoke(memory, ["reconcile"]).output)["remaining"] == 0
    all_human = runner.invoke(memory, ["status","--all"]).output; all_status = json.loads(runner.invoke(memory, ["status","--all","--json"]).output); scopes = {r["scope"]:r for r in all_status["scopes"]}
    assert "all projects" in all_human and f"- ready {repo}" in all_human and f"- needs attention {other_repo}" in all_human and "1 change needs review" in all_human and all_status["scope"] is None and scopes[str(repo)]["pending"] == 0 and scopes[str(repo)]["canonicals"] == 1 and scopes[str(other_repo)]["pending"] == 1 and scopes[str(other_repo)]["canonicals"] == 0


def test_doctor_reports_delivery_and_scoped_ledger_health(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); repo = tmp_path/"repo"; repo.mkdir(); (repo/".git").mkdir(); monkeypatch.chdir(repo); monkeypatch.setenv("CODEX_HOME", str(tmp_path/"codex-home")); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path/"claude-home"))
    assert "memory: attention" in doctor_status() and "ledger=missing" in doctor_status() and "repair: convos memory enable" in doctor_status()
    home=CliRunner().invoke(memory,[]); assert home.exit_code==0 and f"Memory is not enabled for `{repo}`" in home.output and "Run `convos memory enable`" in home.output and all(word not in home.output for word in ("ledger","canonical","hooks=","skills="))
    codex_memory(codex, scope=str(repo)); scan_store(); plan = plan_data(True, scope=str(repo)); apply_data({"version":1,"plan":plan["plan"],"scope":str(repo),"resolutions":[{"source":plan["pending"][0]["source"],"action":"distinct"}]})
    source = (Path(__file__).resolve().parents[1]/"skills"/"convos"/"SKILL.md").read_text()
    for root in (tmp_path/"codex-home", tmp_path/"claude-home"): skill = root/"skills"/"convos"/"SKILL.md"; skill.parent.mkdir(parents=True); skill.write_text(source)
    context_hook_config(); monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"trusted")
    result = doctor_status(); assert f"memory: ready, scope={repo}" in result and "sources=1, active=1, canonicals=1, pending=0, hooks=2/2, codex_trust=trusted, skills=2/2" in result and "repair:" not in result
    home = CliRunner().invoke(memory, []); assert home.exit_code == 0 and f"Memory is ready for `{repo}`: 1 memory available." in home.output and "Automatic delivery is active in Codex and Claude." in home.output and all(word not in home.output for word in ("source","canonical","ledger","hooks=","skills=","trust"))
    monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"untrusted"); result = doctor_status(); home=CliRunner().invoke(memory,[]); assert "memory: attention" in result and "codex_trust=untrusted" in result and "repair: review Codex hooks with /hooks" in result and "Memory needs attention" in home.output and "Next: Review Codex hooks with /hooks." in home.output
    monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"trusted")
    (tmp_path/"codex-home"/"skills"/"convos"/"SKILL.md").write_text("stale")
    result = doctor_status(); home=CliRunner().invoke(memory,[]); assert "memory: attention" in result and "skills=1/2" in result and "repair: convos memory enable" in result and "Next: Run `convos memory enable`." in home.output
    (tmp_path/"codex-home"/"skills"/"convos"/"SKILL.md").write_text(source)
    config = tmp_path/"claude-home"/"settings.json"; data = json.loads(config.read_text()); data["hooks"]["SessionStart"][0]["hooks"][0]["command"] = f"CONVOS_MEMORY_DB={tmp_path/'memory.db'} /missing/convos memory runtime-hook"; config.write_text(json.dumps(data))
    result = doctor_status(); home=CliRunner().invoke(memory,[]); assert "memory: attention" in result and "hooks=1/2" in result and "repair: convos memory enable" in result and "Next: Run `convos memory enable`." in home.output
    context_hook_config()
    codex_memory(codex, "changed", str(repo)); result = doctor_status(); home=CliRunner().invoke(memory,[])
    assert "memory: attention" in result and "pending=1" in result and 'ask Codex or Claude: "sync my memories"' in result and 'Next: Ask Codex or Claude: "sync my memories".' in home.output


def test_codex_hook_trust_uses_bounded_hook_list(monkeypatch):
    class Input:
        def __init__(self): self.writes = []
        def write(self, value): self.writes.append(value)
        def flush(self): pass
    class Process:
        def __init__(self):
            self.stdin = Input(); self.stdout = iter([json.dumps({"id":1,"result":{}})+"\n",json.dumps({"id":2,"result":{"data":[{"hooks":[{"command":"memory-command","trustStatus":"untrusted"}]}]}})+"\n"])
        def terminate(self): pass
        def wait(self, timeout): return 0
        def kill(self): pass
    monkeypatch.setattr(memory_module.shutil, "which", lambda _:None); assert memory_module._codex_hook_trust(None, "/repo") == "missing" and memory_module._codex_hook_trust("memory-command", "/repo") == "unavailable"
    process = Process(); monkeypatch.setattr(memory_module.shutil, "which", lambda _:"codex"); monkeypatch.setattr(memory_module.subprocess, "Popen", lambda *a,**k:process)
    assert memory_module._codex_hook_trust("memory-command", "/repo") == "untrusted"
    assert [row["method"] for row in map(json.loads, "".join(process.stdin.writes).splitlines())] == ["initialize","initialized","hooks/list"]


def test_top_level_doctor_loads_memory_health_entry_point(tmp_path, monkeypatch):
    from ai_convos import cli
    roots(tmp_path, monkeypatch); repo = tmp_path/"repo"; repo.mkdir(); (repo/".git").mkdir(); monkeypatch.chdir(repo)
    for name, value in (("DATA_DIR", tmp_path/"archive"), ("DB_PATH", tmp_path/"archive"/"convos.db"), ("HOOK_DIR", tmp_path/"archive"/"hooks"), ("HOOK_STATE", tmp_path/"archive"/"state.json"), ("HOOK_EMBED_DIRTY", tmp_path/"archive"/"dirty")): monkeypatch.setattr(cli, name, value)
    monkeypatch.setattr(cli, "install_hooks", lambda **_: None); monkeypatch.setattr(cli, "safari_cookie_domains", lambda: []); monkeypatch.setattr(cli, "chrome_cookie_domains", lambda: [])
    result = CliRunner().invoke(cli.app, ["doctor"])
    assert result.exit_code == 0 and f"memory: attention, scope={repo}, ledger=missing" in result.output


def test_enable_and_disable_are_idempotent_and_fail_before_hook(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); repo = tmp_path/"repo"; repo.mkdir(); (repo/".git").mkdir(); monkeypatch.chdir(repo); codex_memory(codex, scope=str(repo))
    config, codex_config = tmp_path/"config", tmp_path/"codex-home"; monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config)); installed = subprocess.CompletedProcess([], 0, "Installed codex\nInstalled claude\n", ""); monkeypatch.setattr("ai_convos_memory.subprocess.run", lambda *a, **k: installed); monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"untrusted"); runner = CliRunner()
    assert runner.invoke(memory, ["disable"]).exit_code == 0 and not (config/"settings.json").exists() and not (codex_config/"hooks.json").exists()
    first, second = runner.invoke(memory, ["enable"]), runner.invoke(memory, ["enable"]); assert first.exit_code == second.exit_code == 0 and "Automatic memory is on for Codex and Claude" in first.output and f"checked project `{repo}`" in first.output and all(word not in first.output for word in ("2 agent hooks","skill","canonical","resolution"))
    paths = (config/"settings.json",codex_config/"hooks.json"); data = json.loads(paths[0].read_text()); assert all(sum(h["command"].endswith(" memory runtime-hook") for g in json.loads(path.read_text())["hooks"]["SessionStart"] for h in g["hooks"]) == 1 for path in paths) and all(os.stat(path).st_mode&0o777 == 0o600 for path in paths) and all(path.read_text().startswith("{\n  ") for path in paths) and "not yet verified" in first.output and "/hooks" in first.output
    monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"trusted"); trusted = runner.invoke(memory, ["enable"]); assert trusted.exit_code == 0 and "/hooks" not in trusted.output
    warmed = runner.invoke(memory, ["enable","--all"]); conflict = runner.invoke(memory, ["enable","--all","--project",str(repo)]); assert warmed.exit_code == 0 and "checked 1 discovered project" in warmed.output and conflict.exit_code == 1 and "--all cannot be combined with --project" in conflict.output
    project = claude/"project"; project.mkdir(); (project/"session.jsonl").write_text(json.dumps({"cwd":str(repo)})+"\n"); assert runner.invoke(memory, ["project","--write"]).exit_code == 0; projection = project/"memory"/"convos-synced.md"; codex_good = paths[1].read_text(); paths[1].write_text('{"hooks":[]}'); failed_remove = runner.invoke(memory, ["disable","--remove-projection"]); assert failed_remove.exit_code == 1 and projection.exists() and "SessionStart hooks" in failed_remove.output; paths[1].write_text(codex_good)
    assert "Saved memory and history were kept" in runner.invoke(memory, ["disable"]).output and all(json.loads(path.read_text()).get("hooks") == {} for path in paths)
    (config/"settings.json").write_text('{"hooks":[]}'); os.chmod(config/"settings.json", 0o640)
    for args in (["disable"],["install-hook"],["install-hook","--status"]):
        malformed = runner.invoke(memory, args); assert malformed.exit_code == 1 and "SessionStart hooks" in malformed.output and "Traceback" not in malformed.output and (config/"settings.json").read_text() == '{"hooks":[]}' and os.stat(config/"settings.json").st_mode&0o777 == 0o640
    (config/"settings.json").write_text("{}"); (codex_config/"hooks.json").write_text('{"hooks":[]}'); before = (config/"settings.json").read_text(); malformed = runner.invoke(memory, ["install-hook"]); assert malformed.exit_code == 1 and (config/"settings.json").read_text() == before
    (codex_config/"hooks.json").write_text("{}")
    settings, outside = config/"settings.json", tmp_path/"outside-settings"; settings.unlink(); outside.write_text("sentinel"); settings.symlink_to(outside)
    blocked = runner.invoke(memory, ["install-hook"]); assert blocked.exit_code == 1 and "non-symlink" in blocked.output and "Traceback" not in blocked.output and outside.read_text() == "sentinel" and settings.is_symlink()
    settings.unlink(); settings.write_text("{}"); (codex_config/"hooks.json").write_text("{}")
    monkeypatch.setattr("ai_convos_memory.subprocess.run", lambda *a, **k: subprocess.CompletedProcess([], 1, "", "skill failed"))
    failed = runner.invoke(memory, ["enable"]); assert failed.exit_code == 1 and "skill failed" in failed.output and all(path.read_text() == "{}" for path in paths)
    monkeypatch.setattr("ai_convos_memory.subprocess.run", lambda *a, **k: subprocess.CompletedProcess([], 0, "Installed codex\n", ""))
    incomplete = runner.invoke(memory, ["enable"]); assert incomplete.exit_code == 1 and "expected 2, installed 1" in incomplete.output and all(path.read_text() == "{}" for path in paths)


def test_core_initializer_enables_memory_without_reinstalling_skill(tmp_path, monkeypatch):
    codex, _ = roots(tmp_path, monkeypatch); repo = tmp_path/"repo"; repo.mkdir(); (repo/".git").mkdir(); monkeypatch.chdir(repo); codex_memory(codex, scope=str(repo)); monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"untrusted"); run = subprocess.run; monkeypatch.setattr(memory_module.subprocess, "run", lambda args,*a,**k: (_ for _ in ()).throw(AssertionError("initializer reinstalled skills")) if "install-skills" in args else run(args,*a,**k))
    first, second = initialize(), initialize(); configs = context_hook_config(status=True)
    assert "Automatic memory is on for Codex and Claude" in first and f"project `{repo}`" in first and "/hooks" in first and second == first and sum(r[2] for r in configs) == 2 and sync_data(str(repo))["status"] == "clean"
    (codex/"MEMORY.md").write_text("unrecognized"); assert "Memory setup needs attention" in initialize() and "convos memory enable" in initialize()


def test_claude_scope_comes_from_sessions_and_plan_can_select_project(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude)
    other = claude/"ambiguous-encoded-key"; (other/"memory").mkdir(parents=True); (other/"memory"/"choice.md").write_text("beta")
    (other/"broken.jsonl").write_text('{"cwd":'); (other/"valid.jsonl").write_text(json.dumps({"cwd":"/other-project"})+"\n")
    scan_store(); plan = plan_data(True, scope="/repo")
    assert plan["scope"] == "/repo" and len(plan["pending"]) == 2 and {p["provider"] for p in plan["pending"]} == {"codex", "claude-code"}
    assert len(plan_data(scope="/other-project")["pending"]) == 1 and plan["plan"] != plan_data(scope="/other-project")["plan"]
    (other/"memory"/"choice.md").write_text("changed outside planned scope")
    result = apply_data({"version":1,"plan":plan["plan"],"scope":"/repo","resolutions":[{"source":p["source"],"action":"distinct"} for p in plan["pending"]]})
    assert result == {"version":1,"applied":2,"remaining":0} and len(plan_data()["pending"]) == 1 and plan_data(scope="/other-project")["pending"][0]["suggested"] is None


def test_claude_scope_uses_session_index_and_origin_history_without_decoding_keys(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); a, b = tmp_path/"a", tmp_path/"b"; (a/".git").mkdir(parents=True); (b/".git").mkdir(parents=True)
    indexed = claude/"opaque-indexed"; (indexed/"memory").mkdir(parents=True); (indexed/"memory"/"indexed.md").write_text("indexed"); (indexed/"sessions-index.json").write_text(json.dumps({"entries":[{"projectPath":str(a)}]}))
    historical = claude/"opaque-history"; (historical/"memory").mkdir(parents=True); (historical/"memory"/"history.md").write_text("---\noriginSessionId: old-session\n---\nhistorical"); (tmp_path/"history.jsonl").write_text(json.dumps({"sessionId":"old-session","project":str(b)})+"\n")
    assert scan_store() == {"scanned":2,"revisions":2,"sources":2,"active":2,"missing":0}; db = sqlite3.connect(tmp_path/"memory.db"); rows = db.execute("SELECT scope,locator FROM sources ORDER BY scope").fetchall(); db.close()
    assert {r[0] for r in rows} == {str(a),str(b)} and {r[1].split("/",1)[0] for r in rows} == {"opaque-indexed","opaque-history"}
    (tmp_path/"history.jsonl").write_text(json.dumps({"sessionId":"old-session","project":str(a)})+"\n"+json.dumps({"sessionId":"old-session","project":str(b)})+"\n")
    with pytest.raises(ValueError, match="Conflicting Claude cwd"): scan_store()


def test_exact_scope_reclassification_detaches_only_the_inactive_identity(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); project = claude/"opaque"; (project/"memory").mkdir(parents=True); (project/"memory"/"choice.md").write_text("---\noriginSessionId: old-session\n---\ntruth")
    assert sync_data("opaque")["status"] == "clean"; repo = tmp_path/"repo"; (repo/".git").mkdir(parents=True); (tmp_path/"history.jsonl").write_text(json.dumps({"sessionId":"old-session","project":str(repo)})+"\n")
    result = sync_all_data(); assert result["status"] == "clean" and result["automatic"] == 2 and result["remaining"] == 0
    db = sqlite3.connect(tmp_path/"memory.db"); sources = db.execute("SELECT scope,active FROM sources ORDER BY active").fetchall(); links = db.execute("SELECT s.scope FROM links l JOIN sources s ON s.id=l.source").fetchall(); canonicals = db.execute("SELECT scope FROM canonicals ORDER BY scope").fetchall(); db.close()
    assert sources == [("opaque",0),(str(repo),1)] and links == [(str(repo),)] and set(canonicals) == {("opaque",),(str(repo),)}


def test_custom_agent_configs_are_default_provider_and_projection_roots(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); monkeypatch.delenv("CONVOS_CODEX_MEMORY_ROOT"); monkeypatch.delenv("CONVOS_CLAUDE_PROJECTS_ROOT"); codex, config = tmp_path/"custom-codex", tmp_path/"custom-claude"; monkeypatch.setenv("CODEX_HOME", str(codex)); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config)); (codex/"memories").mkdir(parents=True); codex_memory(codex/"memories", "codex truth")
    project = config/"projects"/"-repo"; (project/"memory").mkdir(parents=True); (project/"memory"/"choice.md").write_text("claude truth"); (project/"session.jsonl").write_text(json.dumps({"cwd":"/repo"})+"\n")
    assert scan_store()["scanned"] == 2; plan = plan_data(True, scope="/repo"); assert {p["provider"] for p in plan["pending"]} == {"codex","claude-code"}
    apply_data({"version":1,"plan":plan["plan"],"scope":"/repo","resolutions":[{"source":p["source"],"action":"distinct"} for p in plan["pending"]]})
    assert projection_data(None, "/repo")["target"] == str(project/"memory"/"convos-synced.md")


def test_apply_cannot_cross_project_scope_boundaries(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude)
    other = claude/"other"; (other/"memory").mkdir(parents=True); (other/"memory"/"choice.md").write_text("other truth"); (other/"session.jsonl").write_text(json.dumps({"cwd":"/other"})+"\n")
    scan_store()
    for scope in ("/repo","/other"):
        plan = plan_data(True, scope=scope); apply_data({"version":1,"plan":plan["plan"],"scope":scope,"resolutions":[{"source":p["source"],"action":"distinct"} for p in plan["pending"]]})
    foreign = sqlite3.connect(tmp_path/"memory.db").execute("SELECT id FROM canonicals WHERE scope='/other'").fetchone()[0]
    codex_memory(codex, "repo changed"); scan_store(); repo = plan_data(True, scope="/repo"); source = repo["pending"][0]["source"]
    for action in ("same","merge","supersedes"):
        with pytest.raises(ValueError, match="outside resolution scope"): apply_data({"version":1,"plan":repo["plan"],"scope":"/repo","resolutions":[{"source":source,"action":action,"canonical":foreign,"content":"hijack"}]})
    with pytest.raises(ValueError, match="resolution scope must match"): apply_data({"version":1,"plan":repo["plan"],"scope":"/repo","resolutions":[{"source":source,"action":"distinct","scope":"/other"}]})
    (other/"memory"/"choice.md").write_text("other changed"); scan_store(); global_plan = plan_data(True)
    with pytest.raises(ValueError, match="must share one scope"): apply_data({"version":1,"plan":global_plan["plan"],"resolutions":[{"sources":[p["source"] for p in global_plan["pending"]],"action":"merge","content":"cross-project"}]})
    assert len(plan_data()["pending"]) == 2 and sqlite3.connect(tmp_path/"memory.db").execute("SELECT COUNT(*) FROM links l JOIN sources s ON s.id=l.source JOIN canonicals c ON c.id=l.canonical WHERE s.scope<>c.scope").fetchone()[0] == 0
    db = sqlite3.connect(tmp_path/"memory.db"); db.execute("UPDATE canonicals SET scope='/corrupt' WHERE id=?", (foreign,)); db.commit(); db.close()
    with pytest.raises(ValueError, match="Cross-scope memory link"): scan_store()


def test_conflicting_claude_session_scopes_fail_closed(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); claude_memory(claude)
    (claude/"-repo"/"other.jsonl").write_text(json.dumps({"cwd":"/different"})+"\n")
    with pytest.raises(ValueError, match="Conflicting Claude cwd"): scan_store()


def test_scoped_refresh_ignores_only_unrelated_broken_projects(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); broken = claude/"broken"; (broken/"memory").mkdir(parents=True); (broken/"memory"/"choice.md").write_text("unrelated"); (broken/"a.jsonl").write_text(json.dumps({"cwd":"/other-a"})+"\n"); (broken/"b.jsonl").write_text(json.dumps({"cwd":"/other-b"})+"\n")
    result = sync_data("/repo"); assert result["status"] == "clean" and result["remaining"] == 0 and json.loads(CliRunner().invoke(memory, ["status","--project","/repo","--json"]).output)["active"] == 1
    (codex/"MEMORY.md").unlink(); missing = json.loads(CliRunner().invoke(memory, ["status","--project","/repo","--json"]).output); assert missing["active"] == 0 and missing["missing"] == missing["pending"] == 1
    for args in (["scan"],["status","--all"],["sync","--project","/other-a"]):
        failed = CliRunner().invoke(memory, args); assert failed.exit_code == 1 and "Conflicting Claude cwd" in failed.output and "Traceback" not in failed.output


def test_claude_session_subdirectories_normalize_to_git_root(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); claude_memory(claude); repo, sub = tmp_path/"repo", tmp_path/"repo"/"sub"; sub.mkdir(parents=True); (repo/".git").mkdir()
    project = claude/"-repo"; (project/"session.jsonl").write_text(json.dumps({"cwd":str(repo)})+"\n"); (project/"sub.jsonl").write_text(json.dumps({"cwd":str(sub)})+"\n")
    scan_store(); plan = plan_data(scope=str(repo)); assert len(plan["pending"]) == 1 and plan["pending"][0]["scope"] == str(repo)
    other = tmp_path/"other"; other.mkdir(); (other/".git").mkdir(); (project/"other.jsonl").write_text(json.dumps({"cwd":str(other)})+"\n")
    with pytest.raises(ValueError, match="Conflicting Claude cwd"): scan_store()


def test_agent_merge_history_and_cli_json(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); plan = plan_data(True); ids = [p["source"] for p in plan["pending"]]
    document = {"version":1,"plan":plan["plan"],"resolutions":[{"sources":ids,"action":"merge","scope":"/repo","content":"merged truth"}]}
    runner = CliRunner(); preview = json.loads(runner.invoke(memory, ["apply","-","--dry-run"], input=json.dumps(document)).output)
    assert preview["version"] == 1 and preview["dry_run"] and preview["applied"] == 2 and preview["remaining"] == 0 and preview["canonicals"][0]["content"] == "merged truth" and plan_data()["plan"] == plan["plan"]
    assert apply_data(document) == {"version":1,"applied":2,"remaining":0}
    current = runner.invoke(memory, ["current", "--project", "/repo", "--json"]); row = json.loads(current.output)[0]
    assert current.exit_code == 0 and row["content"] == "merged truth" and [o["provider"] for o in row["origins"]] == ["claude-code","codex"]
    assert all(set(o) == {"source","provider","locator","source_hash","applied_hash","active"} and o["source_hash"] == o["applied_hash"] and o["active"] == 1 for o in row["origins"])
    human = runner.invoke(memory, ["current","--project","/repo"]).output; assert "# Current memories" in human and f"## merged truth\n\nID: `{row['id']}`" in human and "Available from:\n- Claude\n- Codex" in human and all(o["source"] not in human and o["locator"] not in human for o in row["origins"]) and human.endswith("merged truth\n")
    assert "Origins: claude-code, codex" in context_data("/repo")
    assert "Memory ready for /repo" in runner.invoke(memory, ["status","--project","/repo"]).output
    status = json.loads(runner.invoke(memory, ["status","--project","/repo","--json"]).output); assert status["canonicals"] == 1 and status["pending"] == 0 and status["revisions"] == 2
    codex_memory(codex, "current sees changes"); row = json.loads(runner.invoke(memory, ["current", "--project", "/repo", "--json"]).output)[0]; changed = next(o for o in row["origins"] if o["provider"] == "codex")
    changed_human=runner.invoke(memory, ["current","--project","/repo"]).output; assert "- Codex (changed since last sync)" in changed_human and changed["source"] not in changed_human and changed["locator"] not in changed_human
    status = json.loads(runner.invoke(memory, ["status","--project","/repo","--json"]).output); assert status["pending"] == 1 and status["revisions"] == 3
    source_history = json.loads(runner.invoke(memory, ["history",changed["source"],"--json"]).output); canonical_history = json.loads(runner.invoke(memory, ["history",row["id"],"--json"]).output)
    assert changed["source_hash"] != changed["applied_hash"] and source_history["kind"] == "source" and source_history["id"] == changed["source"] and source_history["provider"] == "codex" and source_history["scope"] == "/repo" and source_history["hash"] == changed["source_hash"] and source_history["active"] == 1 and len(source_history["revisions"]) == 2
    assert canonical_history["kind"] == "canonical" and canonical_history["id"] == row["id"] and canonical_history["scope"] == "/repo" and canonical_history["hash"] == row["hash"] and len(canonical_history["revisions"]) == 1
    (claude/"-repo"/"memory"/"choice.md").unlink(); row = json.loads(runner.invoke(memory, ["current", "--project", "/repo", "--json"]).output)[0]
    missing=next(o for o in row["origins"] if o["provider"] == "claude-code"); assert missing["active"] == 0
    missing_review=runner.invoke(memory,["review","--project","/repo"]).output
    assert "Claude (unavailable)" in runner.invoke(memory, ["current","--project","/repo"]).output and "merged truth" in context_data("/repo") and row["id"].startswith("mem_") and "## Unavailable memory from Claude" in missing_review and "### Last available version from Claude" in missing_review and missing["source"] not in missing_review and missing["locator"] not in missing_review


def test_canonical_revision_uses_scoped_plan_and_expected_hash(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); first = plan_data(True); ids = [p["source"] for p in first["pending"]]
    apply_data({"version":1,"plan":first["plan"],"resolutions":[{"sources":ids,"action":"merge","scope":"/repo","content":"raw"}]})
    row = json.loads(CliRunner().invoke(memory, ["current", "--project", "/repo", "--json"]).output)[0]; revision = plan_data(scope="/repo")
    assert apply_data({"version":1,"plan":revision["plan"],"scope":"/repo","resolutions":[{"action":"revise","canonical":row["id"],"hash":row["hash"],"content":"refined"}]}) == {"version":1,"applied":1,"remaining":0}
    db = sqlite3.connect(tmp_path/"memory.db"); assert db.execute("SELECT COUNT(*) FROM canonical_revisions").fetchone()[0] == 2
    stale = plan_data(scope="/repo")
    with pytest.raises(ValueError, match="canonical revision is stale"): apply_data({"version":1,"plan":stale["plan"],"scope":"/repo","resolutions":[{"action":"revise","canonical":row["id"],"hash":row["hash"],"content":"lost update"}]})
    assert context_data("/repo").endswith("refined")


def test_collision_plan_is_a_three_way_resolution_bundle(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); first = plan_data(True); ids = [p["source"] for p in first["pending"]]
    apply_data({"version":1,"plan":first["plan"],"resolutions":[{"sources":ids,"action":"merge","scope":"/repo","content":"shared truth"}]})
    codex_memory(codex, "alpha two"); (claude/"-repo"/"memory"/"choice.md").write_text("beta two"); scan_store(); collision = plan_data(True, scope="/repo")
    assert {p["kind"] for p in collision["pending"]} == {"collision"} and {p["canonical"] for p in collision["pending"]} == {collision["canonicals"][0]["id"]} and collision["canonicals"][0]["content"] == "shared truth"
    assert {p["content"].split("- ")[-1] for p in collision["pending"]} == {"alpha two","beta two"} and {p["ancestor"].split("- ")[-1] for p in collision["pending"]} == {"alpha","beta"}
    review = CliRunner().invoke(memory, ["review","--project","/repo"]).output; assert "# Memory changes" in review and "2 changes need a decision" in review and review.count("## Change from ") == 2 and "Previously synchronized" in review and "New version from Codex" in review and "New version from Claude" in review and "Current memory" in review and all(text in review for text in ("alpha two","beta two","shared truth")) and all(p["source"] not in review and p["locator"] not in review and p["canonical"] not in review for p in collision["pending"])
    assert apply_data({"version":1,"plan":collision["plan"],"scope":"/repo","resolutions":[{"sources":[p["source"] for p in collision["pending"]],"action":"merge","canonical":collision["canonicals"][0]["id"],"content":"resolved truth"}]}) == {"version":1,"applied":2,"remaining":0}
    assert context_data("/repo").endswith("resolved truth") and "No memory changes need review." in CliRunner().invoke(memory, ["review","--project","/repo"]).output


def test_context_delivery_settles_safe_revisions_without_user_sync(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); payload = {"hook_event_name":"SessionStart","cwd":"/repo","transcript_path":str(claude/"-repo"/"session.jsonl")}
    first = runtime_context(payload)["hookSpecificOutput"]["additionalContext"]
    assert first.endswith("- alpha") and "synchronization notice" not in first.lower() and plan_data(scope="/repo")["pending"] == []
    claude_memory(claude, (codex/"MEMORY.md").read_text().strip()); second = runtime_context(payload)["hookSpecificOutput"]["additionalContext"]
    assert second.endswith("- alpha") and "synchronization notice" not in second.lower() and plan_data(scope="/repo")["pending"] == []
    db = sqlite3.connect(tmp_path/"memory.db"); assert db.execute("SELECT COUNT(*) FROM links").fetchone()[0] == 2


def test_runtime_hook_indexes_large_scope_but_explicit_context_stays_complete(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); blocks = [f"# Task Group: {name}\n\nscope: test\napplies_to: cwd=/repo; reuse_rule=safe\n\n## Reusable knowledge\n\n- marker-{name} " + "x"*9000 for name in ("First","Second")]; (codex/"MEMORY.md").write_text("\n\n".join(blocks))
    scan_store(); plan = plan_data(scope="/repo"); apply_data({"version":1,"plan":plan["plan"],"scope":"/repo","resolutions":[{"source":p["source"],"action":"distinct"} for p in plan["pending"]]}); full = context_data("/repo"); ids = [r["id"] for r in _current("/repo")]
    payload = {"hook_event_name":"SessionStart","cwd":"/repo","transcript_path":str(claude/"-repo"/"session.jsonl")}; indexed = runtime_context(payload)["hookSpecificOutput"]["additionalContext"]
    explicit_index = CliRunner().invoke(memory, ["context","--scope","/repo","--index"]); assert explicit_index.exit_code == 0
    assert len(full) > 18000 and "marker-First" in full and "marker-Second" in full and len(indexed) < 3000 and "Synchronized memory index" in indexed and all(i in indexed for i in ids) and "x"*100 not in indexed
    assert "Synchronized memory index" in explicit_index.output and all(i in explicit_index.output for i in ids) and "x"*100 not in explicit_index.output
    assert "context --scope /repo --query ID_OR_TERM" in indexed and "context --scope /repo` for all records" in indexed and "marker-First" in context_data("/repo","First") and "marker-Second" not in context_data("/repo","First")
    db = sqlite3.connect(tmp_path/"memory.db"); db.execute("UPDATE canonicals SET content=content||? WHERE id=?", ("\nmentions "+ids[0],ids[1])); db.commit(); db.close(); exact = context_data("/repo",ids[0]); current = json.loads(CliRunner().invoke(memory, ["current","--project","/repo","--query",ids[0],"--json"]).output)
    assert exact.count("## mem_") == 1 and ids[0] in exact and ids[1] not in exact and [r["id"] for r in current] == [ids[0]] and _current("/other",ids[0]) == []
    miss = context_data("/repo","absent-memory"); assert miss.startswith("# No synchronized memory match") and "`/repo`" in miss and "`absent-memory`" in miss
    (codex/"MEMORY.md").write_text((codex/"MEMORY.md").read_text().replace("marker-First","marker-First-changed")); changed = runtime_context(payload)["hookSpecificOutput"]["additionalContext"]
    assert "Synchronized memory index" in changed and "1 source revision awaits reconciliation" in changed


def test_current_settles_safe_revisions_without_prior_sync(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); runner = CliRunner()
    first = json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)
    assert len(first) == 1 and first[0]["content"].endswith("- alpha") and len(first[0]["origins"]) == 1 and plan_data(scope="/repo")["pending"] == []
    claude_memory(claude, (codex/"MEMORY.md").read_text().strip()); exact = json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)
    assert len(exact) == 1 and [o["provider"] for o in exact[0]["origins"]] == ["claude-code","codex"] and plan_data(scope="/repo")["pending"] == []


def test_session_hook_injects_context_and_installs_idempotently(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); runner = CliRunner()
    notice = runner.invoke(memory, ["context","--scope","/repo"]).output
    assert "2 source revisions await reconciliation" in notice and "convos memory sync --project /repo --json" in notice
    plan = plan_data(True); apply_data({"version":1,"plan":plan["plan"],"resolutions":[{"sources":[p["source"] for p in plan["pending"]],"action":"merge","scope":"/repo","content":"runtime truth"}]})
    payload = {"hook_event_name":"SessionStart","cwd":"/repo/subdir","transcript_path":str(claude/"-repo"/"new.jsonl")}; result = runtime_context(payload)
    assert result["hookSpecificOutput"]["additionalContext"].endswith("runtime truth") and runtime_context({**payload,"cwd":"/worktree"}) == result and runtime_context({**payload,"transcript_path":None}) == result and runtime_context({**payload,"hook_event_name":"Stop"}) == {}
    codex_memory(codex, "changed provider truth"); changed = runtime_context(payload)["hookSpecificOutput"]["additionalContext"]
    assert "runtime truth" in changed and "1 source revision awaits reconciliation" in changed and plan_data(scope="/repo")["pending"][0]["kind"] == "changed"
    changed_plan = plan_data(True, scope="/repo"); canonical = json.loads(runner.invoke(memory, ["current","--project","/repo","--json"]).output)[0]["id"]
    assert changed_plan["canonicals"][0]["content"] == "runtime truth" and changed_plan["pending"][0]["ancestor"].endswith("- alpha") and changed_plan["pending"][0]["content"].endswith("- changed provider truth")
    apply_data({"version":1,"plan":changed_plan["plan"],"scope":"/repo","resolutions":[{"source":changed_plan["pending"][0]["source"],"action":"supersedes","canonical":canonical,"content":"current runtime truth"}]}); (claude/"-repo"/"memory"/"choice.md").unlink()
    missing_result = runtime_context(payload); missing = missing_result["hookSpecificOutput"]["additionalContext"]; missing_plan = plan_data(True, scope="/repo")
    assert "1 source revision awaits reconciliation" in missing and missing_plan["pending"][0]["kind"] == "missing" and missing_plan["pending"][0]["content"] == missing_plan["pending"][0]["ancestor"] == "beta"
    detached = apply_data({"version":1,"plan":missing_plan["plan"],"scope":"/repo","resolutions":[{"source":missing_plan["pending"][0]["source"],"action":"detach"}]}, True)
    assert detached["remaining"] == 0 and detached["canonicals"][0]["content"] == "current runtime truth" and plan_data(scope="/repo")["pending"][0]["kind"] == "missing"
    config, codex_config = tmp_path/"config", tmp_path/"codex-home"; config.mkdir(); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config)); (config/"settings.json").write_text(json.dumps({"keep":1,"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"keep"}]}]}})); os.chmod(config/"settings.json", 0o600)
    assert runner.invoke(memory, ["install-hook"]).exit_code == runner.invoke(memory, ["install-hook"]).exit_code == 0
    data = json.loads((config/"settings.json").read_text()); installed = [h for g in data["hooks"]["SessionStart"] for h in g["hooks"] if h["command"].endswith(" memory runtime-hook")]
    assert data["keep"] == 1 and len(installed) == 1 and installed[0]["statusMessage"] == "Loading synchronized memory" and json.loads(runner.invoke(memory, ["runtime-hook"], input=json.dumps(payload)).output) == missing_result and os.stat(config/"settings.json").st_mode&0o777 == 0o600 and json.loads((codex_config/"hooks.json").read_text())["hooks"]["SessionStart"][0]["hooks"][0]["statusMessage"] == "Loading synchronized memory"
    monkeypatch.setattr(memory_module, "_codex_hook_trust", lambda *_:"untrusted"); status = runner.invoke(memory, ["install-hook","--status"]).output; assert "claude-code: 1 memory hook" in status and "codex: 1 memory hook, trust=untrusted" in status and runner.invoke(memory, ["install-hook","--remove"]).exit_code == 0
    assert json.loads((config/"settings.json").read_text())["hooks"]["SessionStart"] == [{"hooks":[{"type":"command","command":"keep"}]}] and json.loads((codex_config/"hooks.json").read_text()).get("hooks") == {}


def test_first_runtime_hook_normalizes_empty_ledger_subdirectory(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); repo, sub = tmp_path/"repo", tmp_path/"repo"/"sub"; sub.mkdir(parents=True); (repo/".git").mkdir(); codex_memory(codex, "first-run truth", str(repo)); payload = {"hook_event_name":"SessionStart","cwd":str(sub),"transcript_path":str(claude/"project"/"new.jsonl")}
    assert not (tmp_path/"memory.db").exists(); result = runtime_context(payload); content = result["hookSpecificOutput"]["additionalContext"]
    assert content.endswith("- first-run truth") and f"Scope: `{repo}`" in content and plan_data(scope=str(repo))["pending"] == [] and sqlite3.connect(tmp_path/"memory.db").execute("SELECT scope FROM canonicals").fetchone()[0] == str(repo)


def test_runtime_hook_rejects_malformed_payload_before_ledger_access(tmp_path, monkeypatch):
    roots(tmp_path, monkeypatch); runner = CliRunner()
    documents = ([], {}, {"hook_event_name":1}, {"hook_event_name":"SessionStart"}, {"hook_event_name":"SessionStart","cwd":1}, {"hook_event_name":"SessionStart","cwd":"/repo","transcript_path":[]})
    for document in documents:
        result = runner.invoke(memory, ["runtime-hook"], input=json.dumps(document)); assert result.exit_code == 1 and "Malformed agent" in result.output and "Traceback" not in result.output
    invalid = runner.invoke(memory, ["runtime-hook"], input="{"); assert invalid.exit_code == 1 and "Traceback" not in invalid.output
    assert not (tmp_path/"memory.db").exists()


def test_apply_rolls_back_every_resolution_on_error(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); plan = plan_data()
    a, b = [p["source"] for p in plan["pending"]]
    with pytest.raises(ValueError, match="unknown action"): apply_data({"version":1,"plan":plan["plan"],"resolutions":[{"source":a,"action":"distinct"},{"source":b,"action":"guess"}]}, True)
    for resolution, error in [({"source":a,"action":"distinct","canonical":"mem_agent"},"allocated by the engine"),({"source":a,"action":"merge","canonical":"mem_agent"},"canonical is missing"),({"source":a,"action":"detach"},"detach requires missing")]:
        with pytest.raises(ValueError, match=error): apply_data({"version":1,"plan":plan["plan"],"resolutions":[resolution]}, True)
    db = sqlite3.connect(tmp_path/"memory.db")
    assert db.execute("SELECT COUNT(*) FROM canonicals").fetchone()[0] == 0 and db.execute("SELECT COUNT(*) FROM links").fetchone()[0] == 0


def test_unknown_codex_format_fails_without_tombstoning(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); (codex/"MEMORY.md").write_text("# New provider format")
    with pytest.raises(ValueError, match="refusing"): scan_store()
    runner = CliRunner(); payload = json.dumps({"hook_event_name":"SessionStart","cwd":"/repo","transcript_path":str(claude/"-repo"/"session.jsonl")})
    for args, input_ in [(["scan"],None),(["plan","--scope","/repo"],None),(["status","--json"],None),(["context","--scope","/repo"],None),(["runtime-hook"],payload)]:
        result = runner.invoke(memory, args, input=input_); assert result.exit_code == 1 and "Unrecognized Codex MEMORY.md format" in result.output and "Traceback" not in result.output
    assert doctor_status().startswith("memory: unavailable (Unrecognized Codex MEMORY.md format")
    db = sqlite3.connect(tmp_path/"memory.db")
    assert db.execute("SELECT COUNT(*) FROM sources WHERE active").fetchone()[0] == 2


def test_claude_projection_is_scoped_idempotent_and_drift_safe(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); codex_memory(codex); claude_memory(claude); scan_store(); plan = plan_data(True); ids = [p["source"] for p in plan["pending"]]
    apply_data({"version":1,"plan":plan["plan"],"resolutions":[{"sources":ids,"action":"merge","scope":"/repo","content":"shared truth"}]})
    preview = projection_data(None, "/repo")
    assert preview["status"] == "create" and preview["target"].endswith("-repo/memory/convos-synced.md") and "shared truth" in preview["content"] and "<!-- canonical:mem_" in preview["content"]
    cli = CliRunner().invoke(memory, ["project","--scope","/repo"])
    assert cli.exit_code == 0 and json.loads(cli.output)["status"] == "create"
    written = write_projection("-repo", "/repo"); path = claude/"-repo"/"memory"/"convos-synced.md"
    assert written["status"] == "create" and path.exists() and os.stat(path).st_mode&0o777 == 0o600 and projection_data("-repo", "/repo")["status"] == "unchanged"
    removed = json.loads(CliRunner().invoke(memory, ["project","--target","-repo","--scope","/repo","--remove"]).output)
    assert removed["status"] == "removed" and not path.exists() and remove_projection("-repo", "/repo")["status"] == "absent" and write_projection("-repo", "/repo")["status"] == "create"
    assert scan_store()["scanned"] == 2
    codex_memory(codex, "new source truth"); stale_projection = path.read_text()
    with pytest.raises(ValueError, match="1 pending source revision"): projection_data("-repo", "/repo")
    assert path.read_text() == stale_projection
    changed = plan_data(True); canonical = json.loads(CliRunner().invoke(memory, ["current", "--project", "/repo", "--json"]).output)[0]["id"]
    apply_data({"version":1,"plan":changed["plan"],"resolutions":[{"source":changed["pending"][0]["source"],"action":"supersedes","canonical":canonical,"content":"updated truth"}]})
    assert projection_data("-repo", "/repo")["status"] == "update" and write_projection("-repo", "/repo")["status"] == "update" and "updated truth" in path.read_text()
    path.write_text(path.read_text()+"manual edit")
    assert projection_data("-repo", "/repo")["status"] == "drift"
    with pytest.raises(ValueError, match="refusing"): write_projection("-repo", "/repo")
    with pytest.raises(ValueError, match="refusing"): remove_projection("-repo", "/repo")
    other = claude/"-other"; other.mkdir(); (other/"session.jsonl").write_text(json.dumps({"cwd":"/other"})+"\n")
    with pytest.raises(ValueError, match="scope does not match"): projection_data("-other", "/repo")
    with pytest.raises(ValueError, match="direct child"): projection_data("../outside", "/repo")
    duplicate = claude/"duplicate"; (duplicate/"memory").mkdir(parents=True); (duplicate/"memory"/"choice.md").write_text("duplicate"); (duplicate/"session.jsonl").write_text(json.dumps({"cwd":"/repo"})+"\n")
    with pytest.raises(ValueError, match="missing or ambiguous"): projection_data(None, "/repo")
    path.write_text(path.read_text().removesuffix("manual edit")); monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path/"config"))
    disabled = CliRunner().invoke(memory, ["disable","--remove-projection","--target","-repo","--scope","/repo"])
    assert disabled.exit_code == 0 and "generated Claude copy is removed" in disabled.output and not path.exists()


def test_projection_bootstraps_claude_project_without_native_memory(tmp_path, monkeypatch):
    codex, claude = roots(tmp_path, monkeypatch); repo, sub = tmp_path/"repo", tmp_path/"repo"/"sub"; sub.mkdir(parents=True); (repo/".git").mkdir(); monkeypatch.chdir(sub); codex_memory(codex, scope=str(repo)); project = claude/"-repo"; project.mkdir(); (project/"session.jsonl").write_text(json.dumps({"cwd":str(repo)})+"\n")
    scan_store(); plan = plan_data(True, scope=str(repo)); apply_data({"version":1,"plan":plan["plan"],"scope":str(repo),"resolutions":[{"source":plan["pending"][0]["source"],"action":"distinct"}]})
    preview = projection_data(None, str(repo))
    assert preview["status"] == "create" and preview["target"].endswith("-repo/memory/convos-synced.md") and "alpha" in preview["content"]
    concise = json.loads(CliRunner().invoke(memory, ["project"]).output); detailed = json.loads(CliRunner().invoke(memory, ["project","--content"]).output)
    assert concise["status"] == detailed["status"] == "create" and concise["scope"] == str(repo) and "content" not in concise and "alpha" in detailed["content"]
    assert write_projection(None, str(repo))["status"] == "create" and (project/"memory"/"convos-synced.md").exists()


def test_projection_rejects_symlinked_owned_paths(tmp_path, monkeypatch):
    _, claude = roots(tmp_path, monkeypatch); project, outside = claude/"evil", tmp_path/"outside"; project.mkdir(); outside.mkdir(); (project/"session.jsonl").write_text(json.dumps({"cwd":"/evil"})+"\n"); (project/"memory").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinks"): projection_data("evil", "/evil")
