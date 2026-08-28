"""Tests for the changegraph app (spec 02): exact replay or labeled unknown."""

import duckdb
import typer

from ai_convos.cli import init_schema
from ai_convos_changegraph import cut, edits_for, register, replay

def confirm(conn,*ids): conn.executemany("INSERT INTO provenance.file_edit_evidence VALUES (?,'confirmed','test_fixture',NULL)",[(i,) for i in ids])


def _e(type, content, old=None, ts="2024-01-01 00:00:00", conv="c1", source="claude-code", prompt="p"):
    return dict(type=type, content=content, old=old, ts=ts, conv=conv, source=source, prompt=prompt)


def test_replay_write_then_edit():
    text, prov = replay([_e("write", "a\nb\nc"), _e("edit", "B", old="b", conv="c2")])
    assert text == ["a", "B", "c"]
    assert [p["conv"] for p in prov] == ["c1", "c2", "c1"]


def test_replay_multiline_edit():
    text, prov = replay([_e("write", "a\nb\nc\nd"), _e("edit", "x\ny\nz", old="b\nc", conv="c2")])
    assert text == ["a", "x", "y", "z", "d"]
    assert [p["conv"] for p in prov] == ["c1", "c2", "c2", "c2", "c1"]


def test_replay_shrinking_edit():
    text, prov = replay([_e("write", "a\nb\nc"), _e("edit", "", old="b\n", conv="c2")])
    assert text == ["a", "c"]
    assert len(prov) == 2


def test_replay_unknown_on_shell():
    assert replay([_e("write", "a"), _e("shell", "sed -i s/a/b/ f")]) == (None, None)


def test_replay_unknown_on_unmatched_old():
    assert replay([_e("write", "a"), _e("edit", "y", old="zzz")]) == (None, None)


def test_replay_unknown_on_missing_old():
    assert replay([_e("write", "a"), _e("multiedit", "y")]) == (None, None)


def test_replay_write_resets_unknown():
    text, prov = replay([_e("write", "a"), _e("shell", "x"), _e("write", "b\nc", conv="c2")])
    assert text == ["b", "c"]
    assert all(p["conv"] == "c2" for p in prov)


def test_cut_by_conv_and_timestamp():
    edits = [_e("write", "a", conv="aaa111", ts="2024-01-01 00:00:00"),
             _e("edit", "b", old="a", conv="bbb222", ts="2024-02-01 00:00:00")]
    assert cut(edits, None) == edits
    assert cut(edits, "aaa") == edits[:1]
    assert cut(edits, "bbb") == edits
    assert cut(edits, "2024-01") == edits[:1]
    assert cut(edits, "2024-03") == edits


def test_cut_treats_date_as_date_before_conversation_substring():
    edits = [_e("write", "a", conv="abc2024-01", ts="2023-12-01 00:00:00"),
             _e("edit", "b", old="a", conv="c2", ts="2024-02-01 00:00:00")]
    assert cut(edits, "2024-01") == edits[:1]


def test_edits_for_attributes_prompt(tmp_path):
    conn = duckdb.connect(str(tmp_path / "t.db"))
    init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','claude-code','t','2024-01-01','2024-01-01','m',NULL,NULL,NULL,'{}')")
    conn.execute("INSERT INTO messages VALUES ('u1','c1','user','please edit',NULL,'2024-01-01 00:00:00',NULL,'{}',NULL,NULL),"
                 "('a1','c1','assistant','done',NULL,'2024-01-01 00:00:01',NULL,'{}',NULL,'u1')")
    conn.execute("INSERT INTO file_edits VALUES ('e1','a1','/f.py','write','x','2024-01-01 00:00:01',NULL)")
    confirm(conn,"e1")
    edits = edits_for(conn, "/f.py")
    conn.close()
    assert len(edits) == 1
    assert edits[0]["prompt"] == "please edit"
    assert edits[0]["conv"] == "c1"
    assert edits[0]["type"] == "write"

def test_exact_views_exclude_unconfirmed_edits(tmp_path):
    conn=duckdb.connect(str(tmp_path/"t.db")); init_schema(conn); conn.execute("INSERT INTO file_edits VALUES ('ok','gone','/f.py','write','ok','2024-01-01',NULL),('bad','gone','/f.py','write','bad','2024-01-02',NULL),('maybe','gone','/f.py','write','maybe','2024-01-03',NULL),('old','gone','/f.py','write','old','2024-01-04',NULL); INSERT INTO provenance.file_edit_evidence VALUES ('ok','confirmed','provider_success',NULL),('bad','invalid','provider_failure',NULL),('maybe','unknown','nonterminal_result',NULL),('old','unverified','source_unavailable',NULL)")
    assert [e["content"] for e in edits_for(conn,"/f.py")]==["ok"]


def test_register_adds_commands():
    app = typer.Typer()
    register(app)
    assert {c.callback.__name__ for c in app.registered_commands} == {"blame", "timeline", "at", "graph", "browse"}


def test_orphaned_edits_labeled_unknown(tmp_path):
    conn = duckdb.connect(str(tmp_path / "t.db"))
    init_schema(conn)
    conn.execute("INSERT INTO file_edits VALUES ('e1','gone','/f.py','write','x','2024-01-01',NULL)")
    confirm(conn,"e1")
    edits = edits_for(conn, "/f.py")
    conn.close()
    assert edits[0]["conv"] == "unknown"
    assert edits[0]["source"] == "?"
    assert edits[0]["prompt"] is None


def _tui_db(tmp_path):
    conn = duckdb.connect(str(tmp_path / "t.db"))
    init_schema(conn)
    conn.execute("INSERT INTO conversations VALUES ('c1','claude-code','fix greeting','2024-01-01','2024-01-01','m',NULL,NULL,NULL,'{}')")
    conn.execute("INSERT INTO messages VALUES ('u1','c1','user','please fix',NULL,'2024-01-01 00:00:00',NULL,'{}',NULL,NULL),"
                 "('a1','c1','assistant','',NULL,'2024-01-01 00:00:01',NULL,'{}',NULL,'u1')")
    conn.execute("INSERT INTO file_edits VALUES ('e1','a1','/f.py','edit','new','2024-01-01 00:00:01','old')")
    confirm(conn,"e1")
    return conn


def test_tui_graph_panes_and_walk(tmp_path):
    import curses
    from ai_convos_changegraph.tui import _gkey, _graph, _nbrs, _nodes
    conn = _tui_db(tmp_path)
    conn.execute("INSERT INTO file_edits VALUES ('e2','a1','/g.py','write','x','2024-01-02',NULL),"
                 "('e3','gone','/f.py','shell','rm x','2024-01-03',NULL)")  # second file + an orphaned edit
    confirm(conn,"e2","e3")
    v = _graph(conn)
    files, convs = _nodes(v["E"], 0), _nodes(v["E"], 1)
    assert [f[0] for f in files] == ["/f.py", "/g.py"]  # recency order
    assert {c[0] for c in convs} == {"c1", "unknown"}  # orphans surface as a node
    nbrs = _nbrs(v["E"], 0, "/f.py")
    assert {n[0] for n in nbrs} == {"c1", "unknown"}
    v["prim"], v["nbrs"], v["sels"] = files, nbrs, [0, 0]
    tl = _gkey(10, v, None, 24, 80)  # enter on a file -> timeline view
    assert tl["title"] == "/f.py" and len(tl["rows"]) == 2
    detail = tl["enter"](tl["rows"][0])
    assert "- old" in detail["rows"] and "+ new" in detail["rows"]
    v["focus"] = 1  # enter on a conversation neighbor -> re-roots the graph on it
    assert _gkey(10, v, None, 24, 80) is None and v["mode"] == 1 and _nodes(v["E"], 1)[v["sels"][0]][0] == nbrs[0][0]
    conn.close()


def test_tui_unknown_conv_has_no_pivot(tmp_path):
    from ai_convos_changegraph.tui import _timeline
    conn = duckdb.connect(str(tmp_path / "t.db"))
    init_schema(conn)
    conn.execute("INSERT INTO file_edits VALUES ('e1','gone','/f.py','shell','rm x','2024-01-01',NULL)")
    confirm(conn,"e1")
    tl = _timeline(conn, "/f.py")
    assert "unknown" in tl["fmt"](tl["rows"][0])
    assert tl["conv"](tl["rows"][0]) is None
    conn.close()
