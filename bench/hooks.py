#!/usr/bin/env python3
"""Hook-ingest throughput bench on an APFS clone (`cp -c`) of this user's archive. Never writes ~/.convos, never contacts the relay.

Grows copies of 4 real Claude Code transcripts (~2, 16, 40, 350 MB; sources are only read) line by line at their recorded rates
(gaps capped at 30 s), fires a Stop hook through `convos capture claude-code` after every `end_turn` line and lets the spawned
`drain-hooks` run as installed (10 s budget). Each copy starts with the tail record a drain would have stored for its prefix,
so every event should be a tail ingest. The drains are instrumented through a generated sitecustomize. --sync adds a
`remote sync`-like writer that holds the archive in 15 s batches. Writes OUT_DIR/report.md with p50/p95 against the targets.
Usage: python bench/hooks.py OUT_DIR [--sync] [--minutes N]   (run under `nice`; the archive clone grows as it is written)
"""
import argparse, contextlib, json, os, shutil, subprocess, sys, threading, time
from datetime import datetime
from pathlib import Path

GAP, MIN_FREE, SIZES = 30, 15 * 2**30, (2e6, 16e6, 40e6, 350e6)
TARGETS = [  # (metric, value key, limit, unit): spec targets; bytes parsed has none there, a turn appends 20-200 KB
    ("capture -> committed latency, p95", "latency", 15, "s"), ("tail drain (<= 100 new messages) per event, p95", "tail_event", 2, "s"),
    ("archive write-lock hold (drain), p95", "hold", 2, "s"), ("checkpoint on close (drain write connections), p95", "close", 1, "s"),
    ("bytes parsed per event, p95", "parsed_mb", 1, "MB")]

def probe():  # runs in every spawned `drain-hooks` (sitecustomize): logs per-event timings, full ingests, retry flags, lock holds
    if "drain-hooks" not in sys.orig_argv: return
    from ai_convos import cli
    log, ev, now = lambda **r: Path(os.environ["CONVOS_BENCH_LOG"]).open("a").write(json.dumps(dict(pid=os.getpid(), t=time.time(), **r)) + "\n"), {}, time.time
    flush = lambda: ev and (log(kind="event", **ev), ev.clear())
    def timed(fn, after):
        def run(*a, **k):
            start = now()
            return (out := fn(*a, **k), after(start, out, *a))[0]
        return run
    def lock(ol):
        @contextlib.contextmanager
        def wrapped(path, purpose, *a, **k):
            with ol(path, purpose, *a, **k) as pulse:
                yield (lambda stage: (stage.startswith("processing ") and (flush(), ev.update(key=stage.split()[1].rsplit(".", 1)[0], start=now())), pulse(stage))[-1]) if pulse and purpose == "hooks.drain" else pulse
        return wrapped
    def cut(r, tail):
        before = len(r.msgs)
        return (out := tail_cut(r, tail), ev.update(rows=len(r.msgs), full=len(r.msgs) == before > 0, tail=tail is not None, end=now()))[0]
    close, tail_cut, drain = cli._LockedDB.close, cli.tail_cut, cli.drain_hooks
    def closing(self):
        purpose, ro, opened, start = *self.audit, now()
        return (out := close(self), self.db is None and log(kind="close", purpose=purpose, ro=ro, hold=time.monotonic() - opened, close=now() - start))[0] if self.db else close(self)
    cli.operation_lock, cli.tail_cut, cli._LockedDB.close = lock(cli.operation_lock), cut, closing
    cli.session_bindings = timed(cli.session_bindings, lambda s, out, *a: ev.update(bindings=now() - s))
    cli.hook_result = timed(cli.hook_result, lambda s, r, src, path, *a: ev.update(size=path.stat().st_size, parsed=len(r.msgs), parse=now() - s, end=now()))
    cli.commit_result = timed(cli.commit_result, lambda s, out, *a: (ev.update(commit=now() - s, committed=True, end=now()), log(kind="commit", key=ev["key"], size=ev["size"])))
    cli.retry_hook = timed(cli.retry_hook, lambda s, out, work, force=False: log(kind="retry" if force else "requeue", key=work.stem))
    cli.drain_hooks = timed(drain, lambda s, n, *a, **k: (flush(), log(kind="drain", start=s, processed=n)))

def pct(xs, q): return sorted(xs)[min(len(xs) - 1, int(q * len(xs)))] if xs else None
def fmt(x, unit): return "-" if x is None else f"{x:.2f} {unit}" if unit != "MB" else f"{x:.3f} MB"
def free(path): return shutil.disk_usage(path).free

def clone_archive(src, data):  # a read-only connection keeps every writer out while the files are cloned; a busy archive frees it between commits
    import duckdb
    end = time.monotonic() + 1800
    while time.monotonic() < end:
        try: conn = duckdb.connect(str(src), read_only=True)
        except duckdb.IOException as e:
            if "Conflicting lock" not in str(e): raise
            time.sleep(.01)
            continue
        with conn: return [subprocess.run(["cp", "-c", f, data / f.name], check=True) for f in (src, src.with_name(src.name + ".wal")) if f.exists()]
    sys.exit("live archive stayed locked for 30 min")

def transcripts(root):  # the main-session transcript nearest each target size
    files = sorted(((p.stat().st_size, p) for p in root.glob("*/*.jsonl")), key=lambda x: x[0])
    return [min(((abs(s / size - 1), p) for s, p in files), key=lambda x: x[0])[1] for size in SIZES]

def turn_end(line):  # Claude Code fires Stop when the main agent ends its turn
    try: e = json.loads(line)
    except ValueError: return False
    return e.get("type") == "assistant" and not e.get("isSidechain") and (e.get("message") or {}).get("stop_reason") == "end_turn"

def stamp(line):
    try: return datetime.fromisoformat(json.loads(line)["timestamp"].replace("Z", "+00:00")).timestamp()
    except (ValueError, KeyError, TypeError): return None

def schedule(src, seconds):  # the tail of the transcript covering `seconds` of recorded time: [(offset s, bytes, stop)], prefix bytes
    lines, t, i, last = src.read_bytes().splitlines(keepends=True), 0.0, None, None
    for i in range(len(lines) - 1, 0, -1):
        if (s := stamp(lines[i])) is not None: (t := t + min(GAP, max(0.0, last - s)) if last else t, last := s)
        if t >= seconds or len(lines) - i > 20000: break
    start, at, prev, plan = i, 0.0, None, []
    for j, line in enumerate(lines[start:], start):
        s = stamp(line) or prev
        at, prev = at + (min(GAP, max(0.0, s - prev)) if s and prev else 0), s
        plan.append((at, line, j + 1 == len(lines) or turn_end(line)))
    return plan, sum(map(len, lines[:start]))

def sync_writer(cli, stop, holds):  # remote-sync-like: every 45 s two 15 s write batches (scan + inserts), yielding to waiters between
    while not stop.wait(5):
        for _ in range(2):
            start = time.monotonic()
            with cli._core(purpose="remote sync", wait=120) as c, cli._transaction(c):
                c.execute("CREATE TABLE IF NOT EXISTS bench_sync(t DOUBLE, body VARCHAR)")
                while time.monotonic() - start < 15: c.execute("SELECT count(*), sum(length(content)) FROM messages").fetchall() and c.execute("INSERT INTO bench_sync SELECT epoch(now()), repeat('x', 200) FROM range(20000)")
            holds.append(time.monotonic() - start)
            cli.archive_yield(cli.DB_PATH)
        stop.wait(40)

def seed(cli, copies):  # the tail record a drain stores after committing each prefix; the clone already holds those rows (the live archive ingested the originals)
    with cli._core(read_only=True, purpose="bench.seed") as conn: bindings = cli.session_bindings(conn)
    cli.atomic_json(cli.HOOK_TAIL, {c["key"]: cli.tail_cut(cli.hook_result("claude-code", c["path"], bindings), None) for c in copies})

def capture(cli, path):
    return subprocess.Popen([sys.executable, "-m", "ai_convos", "capture", "claude-code"], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True).communicate(json.dumps(dict(transcript_path=str(path), hook_event_name="Stop", session_id=path.stem)))

def records(log, since=0): return [r for line in log.read_text().splitlines() if (r := json.loads(line))["t"] >= since] if log.exists() else []

def wait(cli, log, until, limit, out, note):
    end = time.time() + limit
    while time.time() < end and free(out) > MIN_FREE:
        if until(records(log)): return True
        time.sleep(1)
    return print(f"{note}: gave up after {limit:.0f} s (free {free(out) / 2**30:.1f} GiB)", file=sys.stderr) or False

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out"), ap.add_argument("--sync", action="store_true"), ap.add_argument("--minutes", type=float, default=10)
    a = ap.parse_args()
    out, live, home_claude = Path(a.out).expanduser().resolve(), Path.home() / ".convos/data/convos.db", Path.home() / ".claude/projects"
    if out.exists(): sys.exit(f"{out} exists; pass a new folder")
    (out / "data").mkdir(parents=True), (out / "probe").mkdir()
    (out / "probe/sitecustomize.py").write_text(f"import importlib.util as u\ns = u.spec_from_file_location('convos_hook_bench', {str(Path(__file__).resolve())!r})\nm = u.module_from_spec(s)\ns.loader.exec_module(m)\nm.probe()\n")
    os.environ.update(CONVOS_PROJECT_ROOT=str(out), CLAUDE_CONFIG_DIR=str(out / "claude"), CODEX_HOME=str(out / "codex"), CONVOS_BENCH_LOG=str(log := out / "probe.jsonl"), PYTHONPATH=str(out / "probe"))
    from ai_convos import cli
    assert cli.DB_PATH.is_relative_to(out) and not str(cli.DB_PATH).startswith(str(Path.home() / ".convos")), cli.DB_PATH
    say, disk = lambda *m: print(time.strftime("%H:%M:%S"), *m, file=sys.stderr, flush=True), [free(out)]
    say(f"free {disk[0] / 2**30:.1f} GiB; cloning archive"), clone_archive(live, out / "data")
    copies = []
    for src in transcripts(home_claude):
        plan, prefix = schedule(src, a.minutes * 60)
        (dst := out / "claude/projects" / src.parent.name / src.name).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["cp", "-c", src, dst], check=True), os.truncate(dst, prefix)
        copies.append(dict(path=dst, key=cli.gen_id("hook", f"claude-code:{dst.resolve()}"), plan=plan, size=src.stat().st_size, prefix=prefix))
        say(f"transcript {src.stat().st_size / 1e6:.0f} MB: prefix {prefix / 1e6:.1f} MB, {len(plan)} lines, {sum(p[2] for p in plan)} Stop events planned")
    seed(cli, copies)
    say("tail records seeded")
    committed = lambda rs, c, size, since=0: next((r["t"] for r in rs if r["kind"] == "commit" and r["key"] == c["key"] and r["size"] >= size and r["t"] >= since), None)
    stop, holds = threading.Event(), []
    syncer = threading.Thread(target=sync_writer, args=(cli, stop, holds), daemon=True) if a.sync else None
    plan = sorted(((t, i, line, s) for i, c in enumerate(copies) for t, line, s in c["plan"]), key=lambda x: (x[0], x[1]))
    t0, fired, aborted = time.time(), [], None
    syncer and syncer.start()
    for t, i, line, s in plan:
        if t > a.minutes * 60: break
        if (wait_s := t0 + t - time.time()) > 0: time.sleep(wait_s)
        if (f := free(out)) < MIN_FREE: aborted = f"free space {f / 2**30:.1f} GiB < 15 GiB"
        if aborted: break
        with copies[i]["path"].open("ab") as fh: fh.write(line)
        if s: threading.Thread(target=capture, args=(cli, copies[i]["path"]), daemon=True).start() or fired.append((time.time(), i, copies[i]["path"].stat().st_size))
    t1 = time.time()
    say(f"growth done: {len(fired)} Stop events in {t1 - t0:.0f} s{'; ABORTED: ' + aborted if aborted else ''}")
    stop.set()
    syncer and syncer.join()
    def idle(rs):  # every event committed, nothing queued, and no drain holds the drain lock
        if [*cli.HOOK_DIR.glob("*.json"), *cli.HOOK_DIR.glob("*.work")] or not all(committed(rs, copies[i], size) for _, i, size in fired): return False
        with cli.operation_lock(cli.HOOK_DIR / ".drain.lock", "bench.idle", 0, mandatory=False) as free_lock: return free_lock is not None
    drained = wait(cli, log, idle, 900, out, "drain-out")
    disk.append(free(out))
    for pid in {r["pid"] for r in records(log)}:  # a spawned drain outlives the bench: stop it before the clone is trashed
        with contextlib.suppress(ProcessLookupError): os.kill(pid, 15)
    report(out, cli, a, copies, fired, records(log, t0), t0, t1, holds, drained, aborted, disk, committed)
    say(f"report: {out / 'report.md'}")

def report(out, cli, a, copies, fired, rs, t0, t1, holds, drained, aborted, disk, committed):
    from importlib.metadata import version
    events = [r for r in rs if r["kind"] == "event" and r["start"] >= t0]
    done, end = [r for r in events if r.get("committed")], time.time()
    latency = [(committed(rs, copies[i], size, at) or end) - at for at, i, size in fired]
    pending = sum(not committed(rs, copies[i], size, at) for at, i, size in fired)
    closes = [r for r in rs if r["kind"] == "close" and not r["ro"]]
    last, appended = {}, []
    for r in sorted(done, key=lambda r: r["start"]): (appended.append(r["size"] - last[r["key"]]) if r["key"] in last else None, last.__setitem__(r["key"], r["size"]))
    v = dict(latency=latency, tail_event=[r["end"] - r["start"] for r in done if r["rows"] <= 100], all_event=[r["end"] - r["start"] for r in done],
             hold=[r["hold"] for r in closes], close=[r["close"] for r in closes], parsed_mb=[r["size"] / 1e6 for r in done], appended_mb=[x / 1e6 for x in appended],
             rows=[r["rows"] for r in done], bindings=[r["bindings"] for r in events if "bindings" in r], sync=holds)
    full, retry, requeue = sum(bool(r.get("full")) for r in done), sum(r["kind"] == "retry" for r in rs), sum(r["kind"] == "requeue" for r in rs)
    git = subprocess.run(["git", "-C", Path(cli.__file__).parent, "describe", "--always", "--dirty"], capture_output=True, text=True).stdout.strip()
    row = lambda name, xs, unit, *extra: "| " + " | ".join([name, fmt(pct(xs, .5), unit), fmt(pct(xs, .95), unit), str(len(xs)), *extra]) + " |"
    lines = [f"# Hook ingest bench: convos {version('convos')} ({git})", "",
             f"{datetime.now():%Y-%m-%d %H:%M}; growth {t1 - t0:.0f} s of {a.minutes:g} min; concurrent sync writer: {'on' if a.sync else 'off'}; load average at end {os.getloadavg()[0]:.0f}; run under nice {os.nice(0)}",
             f"Transcripts: {', '.join(f'{c['size'] / 1e6:.0f} MB' for c in copies)}; Stop events fired {len(fired)}, not committed at the end {pending}{'; drain-out timed out' if not drained else ''}{'; ABORTED: ' + aborted if aborted else ''}", "",
             "| Metric | p50 | p95 | n | Target | Result |", "|---|---|---|---|---|---|",
             *[row(name, v[key], unit, f"< {limit} {unit}", "PASS" if v[key] and pct(v[key], .95) < limit else "FAIL" if v[key] else "n/a") for name, key, limit, unit in TARGETS],
             f"| full ingests | {full} | | {len(done)} | 0 | {'PASS' if full == 0 else 'FAIL'} |",
             f"| retry flags | {retry} | | | 0 | {'PASS' if retry == 0 else 'FAIL'} |", "",
             "| Context | p50 | p95 | n | Note |", "|---|---|---|---|---|",
             row("drain time per committed event, all", v["all_event"], "s", ""), row("rows committed per event", v["rows"], "rows", ""), row("appended per event", v["appended_mb"], "MB", "parsed should be about this"),
             row("session_bindings per drain", v["bindings"], "s", ""), row("sync writer batch hold", v["sync"], "s", "emulated, 15 s per batch"),
             f"| events requeued (transcript grew during the parse) | {requeue} | | | |", f"| free disk start / end | {disk[0] / 2**30:.1f} GiB | {disk[-1] / 2**30:.1f} GiB | | |", ""]
    (out / "report.md").write_text("\n".join(lines))

if __name__ == "__main__": main()
