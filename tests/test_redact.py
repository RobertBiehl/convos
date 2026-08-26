import hashlib, json, tomllib
from pathlib import Path

import duckdb, pytest, typer
from typer.testing import CliRunner

from ai_convos import cli
import ai_convos_redact as redact


def app():
    root=typer.Typer(); redact.register(root); return root


def test_distribution_metadata_registration_and_remote_dependency():
    project=tomllib.loads((Path(__file__).parents[1]/"apps/redact/pyproject.toml").read_text())["project"]; remote=tomllib.loads((Path(__file__).parents[1]/"apps/remote/pyproject.toml").read_text())["project"]; core=tomllib.loads((Path(__file__).parents[1]/"pyproject.toml").read_text())["project"]
    assert project["dependencies"][0]=="convos>=0.10,<0.11" and project["entry-points"]["convos.commands"]=={"redact":"ai_convos_redact:register"} and project["entry-points"]["convos.doctor"]=={"redact":"ai_convos_redact:doctor_status"}
    assert "convos-redact>=0.10,<0.11" in remote["dependencies"] and core["optional-dependencies"]["redact"]==["convos-redact>=0.10,<0.11"]
    help_=CliRunner().invoke(app(),["redact","--help"]).output
    assert "scan" in help_ and "status" in help_


@pytest.mark.parametrize(("kind","secret"),[
    ("private_key","-----BEGIN PRIVATE KEY-----\nvery-secret-material\n-----END PRIVATE KEY-----"),
    ("anthropic_key","sk-ant-"+"a"*32),
    ("openai_key","sk-proj-"+"A"*32),
    ("github_token","ghp_"+"a"*36),
    ("gitlab_token","glpat-"+"A"*24),
    ("aws_access_key","AKIA"+"A"*16),
    ("google_api_key","AIza"+"A"*35),
    ("slack_token","xoxb-"+"1"*12+"-"+"A"*24),
    ("stripe_key","sk_live_"+"A"*24),
    ("pypi_token","pypi-"+"A"*50),
    ("npm_token","npm_"+"A"*36),
    ("jwt","eyJ"+"A"*12+"."+"B"*12+"."+"C"*12),
    ("authorization","Authorization: Bearer "+"A"*32),
    ("credential_url","https://alice:correcthorsebattery@example.com"),
    ("assigned_secret","password=correcthorsebattery"),
    ("assigned_secret","AWS_SECRET_ACCESS_KEY="+"A"*40),
])
def test_high_confidence_secret_families_are_removed(kind,secret):
    safe,findings=redact.inspect({"nested":[f"before {secret} after"]})
    assert secret not in json.dumps(safe) and f"[REDACTED:{kind}]" in json.dumps(safe) and [f["kind"] for f in findings]==[kind]


def test_placeholders_hashes_and_short_examples_are_not_redacted():
    text="api_key=${TOKEN} sk-test abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    assert redact.scrub(text)==(text,[])


def test_archive_scan_reports_locations_without_secret_values(tmp_path,monkeypatch):
    db=tmp_path/"convos.db"; monkeypatch.setattr(cli,"DB_PATH",db); conn=duckdb.connect(str(db)); cli.init_schema(conn); secret="sk-proj-"+"Z"*32
    conn.execute("INSERT INTO conversations (id,source,title,metadata) VALUES ('c','codex','scan','{}')")
    conn.execute("INSERT INTO messages (id,conversation_id,role,content,metadata) VALUES ('m','c','user',?,'{}')",[f"use {secret}"]); conn.close()
    data=redact.scan_data(); raw=json.dumps(data)
    assert data["status"]=="secrets_found" and data["total"]==1 and data["findings"][0]["table"]=="messages" and data["findings"][0]["row_id"]=="m" and secret not in raw


def test_unchanged_database_cache_is_exact_and_value_free(tmp_path,monkeypatch):
    db=tmp_path/"convos.db"; monkeypatch.setattr(cli,"DB_PATH",db); monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(tmp_path)); conn=duckdb.connect(str(db)); cli.init_schema(conn); secret="AKIA"+"A"*16; conn.execute("INSERT INTO conversations (id,source,title,metadata) VALUES ('c','codex','scan','{}')"); conn.execute("INSERT INTO messages (id,conversation_id,role,content,metadata) VALUES ('m','c','user',?,'{}')",[secret]); conn.close()
    first=redact.scan_data(True); monkeypatch.setattr(redact,"inspect",lambda *_:pytest.fail("unchanged cache missed")); second=redact.scan_data(True)
    assert not first["cached"] and second["cached"] and first["findings"]==second["findings"] and secret.encode() not in (tmp_path/"redact/scan.json").read_bytes()


def message(content,mid="m"):
    return {"kind":"message.record","entity":f"messages:{mid}","payload":{"table":"messages","columns":["id","conversation_id","role","content","thinking","created_at","model","metadata","parent_id"],"row":[mid,"c","user",content,None,"2026-01-01",None,"{}",None]}}


def test_every_team_record_is_scrubbed_and_personal_source_is_unchanged(tmp_path):
    secret="ghp_"+"A"*36; source=message(secret); team=redact.protect_all([source],tmp_path,"team")[0]
    assert secret not in json.dumps(team) and team["payload"]["row"][3]=="[REDACTED:github_token]" and source["payload"]["row"][3]==secret
    audit=redact.audit_data(tmp_path); assert audit["status"]=="redacted" and audit["total"]==1 and secret not in json.dumps(audit) and not secret.encode() in (tmp_path/"redact/audit.db").read_bytes()


def test_team_attachment_is_explicit_placeholder_without_body(tmp_path):
    columns=cli.ARCHIVE_COLUMNS["attachments"]+["body_hash"]; record={"kind":"attachment.record","entity":"attachments:a","payload":{"table":"attachments","columns":columns,"row":["a","m","secret.bin","application/octet-stream",6,None,"https://secret",None,"a"*64]}}; value=redact.protect(record,tmp_path); assert redact.protect({"kind":"attachment.chunk","entity":"attachment:a:x:0","payload":{"body":"secret"}},tmp_path) is None
    row=dict(zip(columns,value["payload"]["row"])); assert row["id"]=="a" and row["filename"]=="[REDACTED:attachment]" and all(row[k] is None for k in ("mime_type","size","path","url","body_hash"))
    assert redact.audit_data(tmp_path)["by_kind"]=={"attachment_redacted":2}


def test_team_tombstone_and_secret_derived_provenance_are_safe(tmp_path):
    tomb={"kind":"attachment.record","entity":"attachments:a","payload":{"table":"attachments","state":"deleted","id":"a"}}; assert redact.protect(tomb,tmp_path)==tomb; secret="ghp_"+"A"*36; columns=cli.ARCHIVE_COLUMNS["file_edits"]; edit={"kind":"file_edit.record","entity":"file_edits:e","payload":{"table":"file_edits","columns":columns,"row":["e","m","a.py","write",secret,"2026-01-01",secret]}}; observed={"kind":"edit.observed","entity":"e","payload":{"id":"e","file":"f","repository":"r","old_content_hash":hashlib.sha256(secret.encode()).hexdigest(),"new_content_hash":hashlib.sha256(secret.encode()).hexdigest()}}; derived=[{"kind":"file.version","entity":"v","payload":{"id":"v","file":"f"}},{"kind":"git.checkpoint","entity":"g","payload":{"id":"g","repository":"r"}},{"kind":"checkpoint.link","entity":"l","payload":{"id":"l","edit":"e"}},{"kind":"repository.observed","entity":"r","payload":{"id":"r","lineage":"secret-hash","roots":["secret-root"],"head":"secret-head"}}]; safe=redact.protect_all([edit,observed,*derived],tmp_path); raw=json.dumps(safe)
    assert secret not in raw and not {"file.version","git.checkpoint","checkpoint.link"}&{r["kind"] for r in safe}; fact=next(r for r in safe if r["kind"]=="edit.observed"); redacted="[REDACTED:github_token]"; assert fact["payload"]["old_content_hash"]==fact["payload"]["new_content_hash"]==hashlib.sha256(redacted.encode()).hexdigest(); repo=next(r for r in safe if r["kind"]=="repository.observed")["payload"]; assert (repo["lineage"],repo["roots"],repo["head"])==(None,[],None)


def test_cli_json_never_prints_detected_value(tmp_path,monkeypatch):
    monkeypatch.setenv("CONVOS_PROJECT_ROOT",str(tmp_path)); secret="sk-ant-"+"A"*30; redact._audit(tmp_path,"w",{"entity":"messages:m","kind":"message.record"},[{"kind":"anthropic_key","path":"$.payload","line":1,"start":0}])
    result=CliRunner().invoke(app(),["redact","status","-f","json"])
    assert result.exit_code==0 and json.loads(result.output)["total"]==1 and secret not in result.output and redact.doctor_status()=="redact: 1 automatic team redaction recorded"


def test_audit_refuses_symlink(tmp_path):
    target=tmp_path/"target"; target.mkdir(); (tmp_path/"redact").symlink_to(target,target_is_directory=True)
    with pytest.raises(ValueError,match="symlink"): redact._audit(tmp_path,"w",{"entity":"e","kind":"message.record"},[{"kind":"jwt","path":"$.payload","line":1,"start":0}])
    with pytest.raises(ValueError,match="symlink"): redact.audit_data(tmp_path)
