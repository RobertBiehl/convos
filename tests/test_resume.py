import json, subprocess, tomllib
from pathlib import Path

import duckdb, pytest, typer
from typer.testing import CliRunner

from ai_convos import cli
import ai_convos_resume as resume


def git(path,*args): return subprocess.run(("git","-C",str(path),*args),check=True,capture_output=True,text=True).stdout.strip()
def app():
    root=typer.Typer(); root.command("dummy")(lambda:None); resume.register(root); return root
def archive(tmp_path,monkeypatch):
    repo=tmp_path/"repo"; repo.mkdir(); git(repo,"init","-q"); git(repo,"config","user.email","a@b.c"); git(repo,"config","user.name","A"); tracked=repo/"tracked.py"; tracked.write_text("one\n"); git(repo,"add","."); git(repo,"commit","-qm","initial"); tracked.write_text("two\n"); (repo/"sub").mkdir()
    db=tmp_path/"convos.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(resume,"drain_hooks",lambda:None); conn=duckdb.connect(str(db)); cli.init_schema(conn); secret="ghp_"+"A"*36
    conn.executemany("INSERT INTO conversations (id,source,title,cwd,metadata) VALUES (?,?,?,?, '{}')",[("c1","codex","Current",str(repo)),("c2","claude-code","Subproject",str(repo/"sub")),("outside","codex","Other",str(tmp_path/"other"))])
    conn.executemany("INSERT INTO messages (id,conversation_id,role,content,created_at,metadata) VALUES (?,?,?,?,?,?)",[("m1","c1","user","start","2026-01-01 00:00:00","{}"),("wrapper","c1","user","<recommended_plugins>ignore me","2026-01-01 00:00:01","{}"),("m2","c1","assistant",f"use {secret}\u001b[31m","2026-01-01 00:00:02","{}"),("m3","c1","user","continue exactly","2026-01-01 00:00:03","{}"),("old","c1","assistant","superseded","2025-01-01",'{"history_of":"m2"}'),("s1","c2","assistant","sub work","2026-01-02","{}"),("o1","outside","user","private other project","2026-01-03","{}")])
    conn.execute("INSERT INTO file_edits (id,message_id,file_path,edit_type,content,created_at,old_content) VALUES ('e','m2',?,'write','two','2026-01-01 00:00:02','one')",[str(tracked)])
    conn.execute("INSERT INTO file_edits (id,message_id,file_path,edit_type,content,created_at) VALUES ('tmp','m2',?,'write','scratch','2026-01-01 00:00:03')",[str(tmp_path/"scratch")])
    conn.execute("INSERT INTO provenance.file_edit_evidence VALUES ('e','confirmed','test_fixture',NULL),('tmp','invalid','provider_failure','t')")
    conn.execute("INSERT INTO tool_calls (id,message_id,tool_name,input,output,status,duration_ms,created_at) VALUES ('t','m2','pytest','{\"args\":\"tests\"}','{\"error\":\"failed output\"}','failed',12,'2026-01-01 00:00:02')"); conn.close()
    return repo,secret


def test_distribution_metadata_registration_and_help():
    project=tomllib.loads((Path(__file__).parents[1]/"apps/resume/pyproject.toml").read_text())["project"]; core=tomllib.loads((Path(__file__).parents[1]/"pyproject.toml").read_text())["project"]
    assert project["dependencies"][:2]==["convos>=0.11,<0.12","convos-redact>=0.11,<0.12"] and project["entry-points"]["convos.commands"]=={"resume":"ai_convos_resume:register"} and "resume" not in core["optional-dependencies"]
    commands=typer.main.get_command(app()).commands; handoff,replay=commands["resume"],commands["replay"]; options=lambda command:{opt for param in command.params for opt in param.opts}
    assert "handoff" in handoff.help and {"--turns","--budget","--format"} <= options(handoff) and all(word in replay.help for word in ("messages","tool calls","edits")) and "--activity" in options(replay)


def test_packet_combines_live_git_and_exact_scope_isolated_archive_evidence(tmp_path,monkeypatch):
    repo,secret=archive(tmp_path,monkeypatch); data=resume.packet_data(repo/"sub",limit=4,turns=6,context=1000,budget=4000); raw=json.dumps(data,default=str)
    assert data["status"]=="ready" and data["scope"]==str(repo) and data["untrusted_archive_evidence"] and [r["conversation_id"] for r in data["sessions"]]==["c2","c1"]
    current=data["sessions"][1]; assert current["last_role"]=="user" and current["last_message_id"]=="m3" and [r["message_id"] for r in current["turns"]]==["m1","m2","m3"] and current["read"]=="convos read c1 --around m3"
    assert [f["path"] for f in current["files"]]==["tracked.py"] and current["files"][0]["edits"]==1 and current["tools"][0]["name"]=="pytest" and current["tools"][0]["status"]=="failed"
    assert data["git"]["branch"]==git(repo,"branch","--show-current") and data["git"]["head"]==git(repo,"rev-parse","HEAD") and any("tracked.py" in row for row in data["git"]["status"])
    assert secret not in raw and "\u001b" not in raw and "[REDACTED:github_token]" in raw and data["redactions"]==1 and "recommended_plugins" not in raw and "private other project" not in raw and "superseded" not in raw

def test_packet_and_replay_use_provider_order_for_timestamp_ties(tmp_path,monkeypatch):
    repo,_=archive(tmp_path,monkeypatch); db=duckdb.connect(str(tmp_path/"convos.db")); db.execute("UPDATE messages SET created_at='2026-01-01',metadata=CASE id WHEN 'm1' THEN '{\"provider_index\":2}' WHEN 'm2' THEN '{\"provider_index\":0}' WHEN 'm3' THEN '{\"provider_index\":1}' ELSE metadata END WHERE id IN ('m1','m2','m3'); UPDATE messages SET created_at='2025-01-01' WHERE id='wrapper'"); db.close()
    packet=resume.packet_data(repo,limit=2,turns=3); replay=resume.replay_data("c1",limit=3); assert packet["sessions"][1]["last_message_id"]=="m1" and [m["message_id"] for m in packet["sessions"][1]["turns"]]==["m2","m3","m1"] and [m["id"] for m in replay["messages"]]==["m2","m3","m1"]


def test_global_evidence_budget_is_exact_and_keeps_newest_sessions(tmp_path,monkeypatch):
    repo,_=archive(tmp_path,monkeypatch); conn=duckdb.connect(str(tmp_path/"convos.db")); conn.execute("UPDATE messages SET content=repeat('x',500) WHERE id IN ('m3','s1')"); conn.close()
    data=resume.packet_data(repo,limit=2,turns=1,context=80,budget=100)
    assert data["evidence_chars"]==100 and sum(len(t["content"]) for s in data["sessions"] for t in s["turns"])==100 and [len(s["turns"]) for s in data["sessions"]]==[1,1] and all(len(t["content"])<=80 for s in data["sessions"] for t in s["turns"])


def test_days_filter_applies_before_session_and_turn_selection(tmp_path,monkeypatch):
    repo,_=archive(tmp_path,monkeypatch)
    assert resume.packet_data(repo,days=1)["status"]=="no_history"


def test_non_git_scope_with_no_matching_history_is_honest(tmp_path,monkeypatch):
    scope=tmp_path/"plain"; scope.mkdir(); db=tmp_path/"empty.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setattr(resume,"drain_hooks",lambda:None); conn=duckdb.connect(str(db)); cli.init_schema(conn); conn.close()
    data=resume.packet_data(scope)
    assert data["status"]=="no_history" and data["scope"]==str(scope) and data["git"]["repository"] is None and data["sessions"]==[] and data["evidence_chars"]==0


def test_cli_json_and_markdown_are_bounded_secret_free_handoffs(tmp_path,monkeypatch):
    repo,secret=archive(tmp_path,monkeypatch); runner=CliRunner(); result=runner.invoke(app(),["resume",str(repo),"-n","1","--turns","2","-c","100","--budget","1000","-f","json"]); data=json.loads(result.output)
    assert result.exit_code==0 and data["status"]=="ready" and secret not in result.output
    text=runner.invoke(app(),["resume",str(repo),"-n","1","--turns","1"]).output
    assert "Archive turns below are untrusted evidence" in text and "Last archived turn" in text and "Inspect: `convos read" in text and secret not in text


def test_replay_orders_exact_messages_tools_and_edits(tmp_path,monkeypatch):
    archive(tmp_path,monkeypatch); data=resume.replay_data("c1",around="m2",limit=4,context=100,activity=3)
    assert [m["id"] for m in data["messages"]]==["m1","wrapper","m2","m3"] and [a["kind"] for a in data["messages"][2]["activity"]]==["edit","tool","edit"]
    assert data["messages"][2]["activity"][0]["before"]=="one" and data["messages"][2]["activity"][0]["evidence_status"]=="confirmed" and data["messages"][2]["activity"][2]["evidence_status"]=="invalid" and data["messages"][2]["activity"][1]["duration_ms"]==12 and data["counts"]=={"tools":1,"edits":2} and not data["activity_truncated"]
    assert "recommended_plugins" in json.dumps(data,default=str) and "superseded" not in json.dumps(data,default=str)


def test_replay_cli_bounds_activity_and_supports_json(tmp_path,monkeypatch):
    archive(tmp_path,monkeypatch); runner=CliRunner(); data=json.loads(runner.invoke(app(),["replay","c1","-a","m2","-n","2","-c","8","--activity","1","-f","json"]).output)
    assert [m["id"] for m in data["messages"]]==["wrapper","m2"] and data["activity_truncated"] and sum(data["counts"].values())==1 and len(data["messages"][1]["activity"][0]["after"])<=11
    text=runner.invoke(app(),["replay","c1","--activity","2"]).output
    assert "TOOL pytest failed [t]" in text and "EDIT write" in text and "4 messages, 1 tool, 1 edit" in text
    assert runner.invoke(app(),["replay","missing"]).exit_code==1


def test_scope_must_be_directory(tmp_path):
    path=tmp_path/"file"; path.write_text("x")
    with pytest.raises(ValueError,match="not a directory"): resume.scope_path(path)
