"""Structural hook-ingest throughput checks: counters (full ingests, retry flags, rows parsed), never wall time."""
import contextlib, duckdb, json, subprocess, sys
from datetime import datetime, timedelta, timezone
import pytest
from typer.testing import CliRunner
from ai_convos import cli
POPEN = subprocess.Popen
class Killed(BaseException): pass  # process death: no except handler in drain_hooks runs

def turns(sid, start, stop):
    stamp = lambda i: (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [{"type": (role := "user" if i % 2 == 0 else "assistant"), "uuid": f"{sid}-{i}", "parentUuid": f"{sid}-{i-1}" if i else None, "sessionId": sid, "timestamp": stamp(i), "message": {"role": role, "content": f"turn {i}" if role == "user" else [{"type": "text", "text": f"turn {i}"}]}} for i in range(start, stop)]

@pytest.fixture
def hooks(tmp_path, monkeypatch):  # transcripts, a meter of what each drain did, and grow/capture/queue helpers
    monkeypatch.setattr(cli.subprocess, "Popen", lambda *a, **k: None if a[0][1:4] == ["-m", "ai_convos", "drain-hooks"] else POPEN(*a, **k))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(root := tmp_path / "claude"))
    monkeypatch.setattr(cli, "HOOK_DRAIN_SECONDS", 3600)  # a loaded CI runner must not hit the budget where a case does not set it
    meter, lines, parse, commit, save = dict(parsed=[], committed=[], retry=[], path=None), {}, cli.hook_result, cli.commit_result, cli.atomic_json
    monkeypatch.setattr(cli, "hook_result", lambda s, p, *a: (r := parse(s, p, *a), meter.__setitem__("path", p.stem), meter["parsed"].append((p.stem, len(r.msgs))))[0])
    monkeypatch.setattr(cli, "commit_result", lambda r, **k: (meter["committed"].append((meter["path"], len(r.msgs), lines[meter["path"]])), commit(r, **k))[1])
    monkeypatch.setattr(cli, "atomic_json", lambda p, v: (isinstance(v, dict) and v.get("retry") and meter["retry"].append(p.stem), save(p, v))[1])
    def grow(sid, n):
        (path := root / "projects" / "-repo" / f"{sid}.jsonl").parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f: f.writelines(json.dumps(e) + "\n" for e in turns(sid, lines.get(sid, 0), lines.setdefault(sid, 0) + n))
        lines[sid] += n
        assert CliRunner().invoke(cli.app, ["capture", "claude-code"], input=json.dumps({"transcript_path": str(path), "hook_event_name": "Stop"})).exit_code == 0
    full = lambda: [sid for sid, n, total in meter["committed"] if n == total]  # every row of the transcript committed again
    queue = lambda: sorted((p.suffix, json.loads(p.read_text()).get("retry", False)) for p in [*cli.HOOK_DIR.glob("*.json"), *cli.HOOK_DIR.glob("*.work")])
    return grow, meter, full, queue

def warm(grow, meter, sids, size=200):  # the first ingest of every transcript is full by design
    [grow(sid, size) for sid in sids]; assert cli.drain_hooks() == len(sids); [meter[k].clear() for k in ("parsed", "committed", "retry")]

def test_budget_breaks_over_successive_drains_never_cascade_into_full_ingests(hooks, monkeypatch):  # 8 events, every drain stops after its first one
    grow, meter, full, queue = hooks; sids = [f"s{i}" for i in range(8)]; warm(grow, meter, sids)
    [grow(sid, 4) for sid in sids]; monkeypatch.setattr(cli, "HOOK_DRAIN_SECONDS", 0)
    assert [cli.drain_hooks() for _ in sids] == [1] * 8 and set(json.loads(cli.HOOK_TAIL.read_text())) == {cli.gen_id("hook", f"claude-code:{p}") for p in cli.hook_root("claude-code").resolve().rglob("*.jsonl")}
    assert (full(), meter["retry"], queue()) == ([], [], []) and sorted(sid for sid, _, _ in meter["committed"]) == sids

def steady(grow):  # 20 Stop events over 3 growing transcripts; every 4th drain waits so two events share it
    for i in range(20):
        grow(f"g{i % 3}", 6 + 2 * (i % 4))
        if i % 4 != 2: cli.drain_hooks()
    cli.drain_hooks()

def test_steady_state_has_no_full_ingest_after_the_first(hooks, monkeypatch):
    grow, meter, full, queue = hooks; warm(grow, meter, ["g0", "g1", "g2"], 300); steady(grow)
    assert (full(), meter["retry"], queue()) == ([], [], []) and all(n <= 24 for _, n, _ in meter["committed"])

def test_steady_state_parses_only_new_lines(hooks, monkeypatch):
    grow, meter, full, queue = hooks; warm(grow, meter, ["g0", "g1", "g2"], 300); steady(grow)
    assert all(n <= 24 for _, n in meter["parsed"]), meter["parsed"]

def crash(hooks, monkeypatch, victim):  # three transcripts grow, the drain dies inside victim's commit, then two more drains
    grow, meter, full, queue = hooks; sids = ["c0", "c1", "c2"]; warm(grow, meter, sids); upsert = cli.upsert
    [grow(sid, 4) for sid in sids]; monkeypatch.setattr(cli, "upsert", lambda conn, part, *a, **k: (_ for _ in ()).throw(Killed()) if meter["path"] == victim else upsert(conn, part, *a, **k))
    with pytest.raises(Killed): cli.drain_hooks()
    monkeypatch.setattr(cli, "upsert", upsert); cli.drain_hooks(); [grow(sid, 4) for sid in sids]; cli.drain_hooks()
    return full(), queue(), sorted(sid for sid, _, _ in meter["committed"])

def test_crash_mid_commit_fully_reingests_only_that_transcript_once(hooks, monkeypatch):
    assert crash(hooks, monkeypatch, "c2") == (["c2"], [], ["c0", "c0", "c1", "c1", "c2", "c2", "c2"])  # c2: killed, retried in full, tail

@pytest.mark.xfail(strict=True, reason="b34 work item: a dead drain cannot tell its unstarted claims from the interrupted one, so every claim it held is retried in full")
def test_crash_mid_commit_spares_the_claims_the_drain_never_started(hooks, monkeypatch):
    assert crash(hooks, monkeypatch, "c1") == (["c1"], [], ["c0", "c0", "c1", "c1", "c1", "c2", "c2"])  # c2 was claimed but never started

def test_sync_holding_the_archive_delays_drains_without_retry(hooks, monkeypatch):
    grow, meter, full, queue = hooks; sids = [f"y{i}" for i in range(4)]; warm(grow, meter, sids)
    [grow(sid, 4) for sid in sids]; monkeypatch.setattr(cli, "HOOK_DRAIN_SECONDS", 1)  # the wait for the archive outlasts the budget
    sync = POPEN([sys.executable, "-c", "import duckdb,sys,time; c=duckdb.connect(sys.argv[1]); c.execute('BEGIN'); c.execute('SELECT count(*) FROM messages'); print(flush=True); time.sleep(3)", str(cli.DB_PATH)], stdout=subprocess.PIPE, text=True)
    assert sync.stdout.readline() == "\n"
    drained = [cli.drain_hooks() for _ in range(len(sids)) if queue()]
    assert sync.wait() == 0 and sum(drained) == len(sids) and (full(), meter["retry"], queue()) == ([], [], [])

def test_drains_keep_appended_tables_in_small_row_groups(hooks):  # a checkpoint rewrites the whole last row group of each table it appended to
    grow, meter, full, queue = hooks; cli.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.closing(duckdb.connect(str(cli.DB_PATH))) as db:  # a schema-19 archive: one 10,000-row group per table, written without the size
        cli.init_schema(db); db.execute("UPDATE core_schema SET version=19; INSERT INTO conversations(id,source) VALUES ('c','claude-code'); INSERT INTO messages(id,conversation_id,role,content) SELECT 'm'||range,'c','user','old' FROM range(10000); INSERT INTO tool_calls(id,message_id,output) SELECT 't'||range,'m'||range,'\"old\"' FROM range(10000)")
    warm(grow, meter, ["r0", "r1"], 300); [(grow(f"r{i % 2}", 6), cli.drain_hooks()) for i in range(6)]
    with contextlib.closing(cli.open_db(purpose="test.row_groups")) as db:
        groups = {t: [n for n, in db.execute(f"SELECT sum(count) FROM pragma_storage_info('{t}') WHERE column_path='[0]' GROUP BY row_group_id").fetchall()] for t in ("messages", "tool_calls")}
        kept = db.execute("SELECT (SELECT count(*) FROM messages WHERE content='old'), (SELECT count(*) FROM tool_calls WHERE output='\"old\"'), (SELECT count(*) FROM messages)").fetchone()
        with pytest.raises(duckdb.ConstraintException): db.execute("INSERT INTO tool_calls(id,message_id) VALUES ('t0','m0')")
    assert max(groups["messages"] + groups["tool_calls"]) <= cli.ROW_GROUP_ROWS, groups
    assert kept == (10000, 10000, 10636) and cli.DB_PATH.with_name("convos.db.pre-v20.bak").is_file()
