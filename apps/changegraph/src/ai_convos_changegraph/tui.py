"""Curses browser for the change graph. Root view draws the bipartite graph itself: primary
nodes left (files or conversations, tab flips), the selected node's neighbors right, edges
drawn in the gutter labeled with edit counts. Enter on a file drills into timeline -> diff;
enter on a conversation re-roots the graph on it. Deeper views are plain lists."""
import curses, os

from . import _conn, edits_for

_EDGES = """SELECT fe.file_path, COALESCE(m.conversation_id, 'unknown'), COALESCE(c.source, '?'),
    COALESCE(c.title, '(transcripts deleted)'), COUNT(*), MAX(fe.created_at)
FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' LEFT JOIN messages m ON fe.message_id = m.id LEFT JOIN conversations c ON c.id = m.conversation_id
GROUP BY 1, 2, 3, 4"""
_FILES = """SELECT fe.file_path, COUNT(*), MAX(fe.created_at), COALESCE(string_agg(DISTINCT c.source, ','), '?')
FROM file_edits fe JOIN provenance.file_edit_evidence v ON v.file_edit_id=fe.id AND v.status='confirmed' LEFT JOIN messages m ON fe.message_id = m.id LEFT JOIN conversations c ON c.id = m.conversation_id
WHERE m.conversation_id = ? GROUP BY fe.file_path ORDER BY MAX(fe.created_at) DESC"""

def _files(conn, conv):
    rows = conn.execute(_FILES, [conv]).fetchall()
    title = f"conv {conv[:8]} ({(conn.execute('SELECT title FROM conversations WHERE id = ?', [conv]).fetchone() or ['?'])[0]})"
    return dict(title=title[:60], rows=rows, fmt=lambda r: f"{str(r[2])[:16]}  {r[1]:>4} edits  {r[3]:<24} {r[0]}",
                enter=lambda r: _timeline(conn, r[0]))

def _timeline(conn, path):
    return dict(title=path, rows=edits_for(conn, path),
                fmt=lambda e: f"{str(e['ts'])[:16]}  {e['source']:<11} {e['type']:<9} {'exact' if e['type'] == 'write' or e['old'] else 'unk':<5} {e['conv'][:8]}  {(e['prompt'] or chr(10)).splitlines()[0][:70]}",
                enter=lambda e: _detail(conn, path, e), conv=lambda e: _files(conn, e["conv"]) if e["conv"] != "unknown" else None)

def _detail(conn, path, e):
    body = (e["content"] or "").splitlines() if e["type"] == "shell" else \
           ([f"- {l}" for l in e["old"].splitlines()] if e["old"] else ["(no before-state captured)"]) + [f"+ {l}" for l in (e["content"] or "").splitlines()]
    rows = [f"file:  {path}", f"conv:  {e['conv']}  ({e['source']})", f"when:  {e['ts']}   type: {e['type']}", "",
            "prompt:", *[f"  {l}" for l in (e["prompt"] or "(unknown)").splitlines()], "",
            "command:" if e["type"] == "shell" else "change:", *body]
    return dict(title=f"{e['type']} @ {str(e['ts'])[:16]}", rows=rows, fmt=str, enter=None,
                conv=lambda _: _files(conn, e["conv"]) if e["conv"] != "unknown" else None)

_HOME = os.path.expanduser("~")

def _nodes(E, mode, flt=""):
    """Aggregate edges into primary nodes: [key, label, total_edits, last_ts, sources]."""
    agg = {}
    for f, c, s, t, n, l in E:
        k, lbl = (f, f.replace(_HOME, "~")) if mode == 0 else (c, f"{c[:8]} {t or ''}")
        a=agg.setdefault(k,[lbl,0,"",set()])
        a[:]=a[0],a[1]+n,max(a[2],str(l)),a[3]|{s}
    return sorted(([k, v[0], v[1], v[2], ",".join(sorted(v[3]))] for k, v in agg.items() if flt.lower() in v[0].lower()),
                  key=lambda r: r[3], reverse=True)

def _nbrs(E, mode, key):
    """Neighbors of a node: the other side of its incident edges, with per-edge weights."""
    return sorted(([c if mode == 0 else f, f"{c[:8]} {t or ''}" if mode == 0 else f.replace(_HOME, "~"), n, str(l), s]
                   for f, c, s, t, n, l in E if (f if mode == 0 else c) == key), key=lambda r: r[3], reverse=True)

def _fit(s, n):
    """Truncate keeping head (date/count) and tail (filename), eliding the middle."""
    return s if len(s) <= n else s[:16] + ".." + s[-(n - 18):] if n > 24 else s[:n]

def _graph(conn):
    return dict(title="graph", conn=conn, E=[tuple(r) for r in conn.execute(_EDGES).fetchall()],
                mode=0, focus=0, sels=[0, 0], flt="", draw=_gdraw, key=_gkey)

def _gdraw(scr, v, stack, h, w):
    lw=w//2-4
    gx,rx,vis=lw+1,lw+9,max(h-3,1)
    prim = v["prim"] = _nodes(v["E"], v["mode"], v["flt"])
    ps = v["sels"][0] = min(v["sels"][0], max(len(prim) - 1, 0))
    nbrs = v["nbrs"] = _nbrs(v["E"], v["mode"], prim[ps][0]) if prim else []
    ns = v["sels"][1] = min(v["sels"][1], max(len(nbrs) - 1, 0))
    kinds = ("files", "conversations") if v["mode"] == 0 else ("conversations", "files")
    scr.addnstr(0, 0, f"change graph  {len(prim)} {kinds[0]} <-> {len(nbrs)} {kinds[1]} of [{prim[ps][1] if prim else '-'}]"
                + (f"  /{v['flt']}" if v["flt"] else ""), w - 1, curses.A_BOLD)
    ptop,ntop=max(0,ps-vis+1),max(0,ns-vis+1) if v["focus"] else 0
    yp=1+ps-ptop
    for y, r in enumerate(prim[ptop:ptop + vis]):
        scr.addnstr(y + 1, 0, _fit(f"{r[3][:10]} {r[2]:>4} {r[1]}", lw - 3), lw - 3,
                    curses.A_REVERSE if ptop + y == ps and v["focus"] == 0 else curses.A_BOLD if ptop + y == ps else 0)
    nys = list(range(1, 1 + len(nbrs[ntop:ntop + vis])))
    for y in range(min(nys + [yp]), max(nys + [yp]) + 1): scr.addch(y, gx, ord("|"))  # spine
    if prim:
        scr.addnstr(yp,lw-2,"--",2)
        scr.addch(yp,gx,ord("+"))
    for y, r in zip(nys, nbrs[ntop:ntop + vis]):
        scr.addch(y,gx,ord("+"))
        scr.addnstr(y,gx+1,f"-{r[2]:>3}->",6)
        scr.addnstr(y, rx, _fit(f"{r[4]:<12} {r[1]}", w - rx - 1), w - rx - 1, curses.A_REVERSE if ntop + y - 1 == ns and v["focus"] else 0)
    scr.addnstr(h - 1, 0, "arrows:move/focus  tab:flip side  enter:timeline/re-root  /:filter  q:quit"[:w - 1], w - 1, curses.A_DIM)

def _gkey(k, v, scr, h, w):
    f, sel, rows = v["focus"], v["sels"], v["prim"] if v["focus"] == 0 else v["nbrs"]
    if k == curses.KEY_UP: sel[f] = max(sel[f] - 1, 0)
    elif k == curses.KEY_DOWN: sel[f] = min(sel[f] + 1, max(len(rows) - 1, 0))
    elif k == 9: v.update(mode=v["mode"] ^ 1, focus=0, sels=[0, 0], flt="")
    elif k == curses.KEY_RIGHT and v["nbrs"]: v["focus"] = 1
    elif k == curses.KEY_LEFT: v["focus"] = 0
    elif k in (10, 13, curses.KEY_ENTER) and rows:
        key = rows[sel[f]][0]
        if (v["mode"] == 0) == (f == 0): return _timeline(v["conn"], key)  # that side holds files
        v.update(mode=1, focus=0, flt="", sels=[next((i for i, r in enumerate(_nodes(v["E"], 1)) if r[0] == key), 0), 0])
    elif k == ord("/"): v.update(flt=_ask(scr, h, w), sels=[0, 0])

def _ask(scr, h, w):
    curses.echo()
    try:
        scr.addnstr(h-1,0,"filter: ".ljust(w-1),w-1)
        return scr.getstr(h-1,8,60).decode()
    finally: curses.noecho()

def _ldraw(scr, v, stack, h, w):
    rows,sel=(rows:=v.get("flt",v["rows"])),min(v.setdefault("sel",0),max(len(rows)-1,0))
    v["sel"]=sel
    top = max(0, sel - (h - 4))
    scr.addnstr(0, 0, " > ".join(x["title"] for x in stack)[-(w - 1):], w - 1, curses.A_BOLD)
    for y, r in enumerate(rows[top:top + h - 3]):
        s=v["fmt"](r)
        attr=curses.color_pair(1) if s.startswith("- ") else curses.color_pair(2) if s.startswith("+ ") else 0
        scr.addnstr(y + 2, 0, s, w - 1, curses.A_REVERSE if top + y == sel else attr)
    scr.addnstr(h - 1, 0, "arrows:move  enter:open  esc:back  c:conversation  /:filter  q:quit"[:w - 1], w - 1, curses.A_DIM)

def _lkey(k, v, scr, h, w):
    rows, sel = v.get("flt", v["rows"]), v["sel"]
    if k == curses.KEY_UP: v["sel"] = max(sel - 1, 0)
    elif k == curses.KEY_DOWN: v["sel"] = min(sel + 1, max(len(rows) - 1, 0))
    elif k == curses.KEY_PPAGE: v["sel"] = max(sel - (h - 4), 0)
    elif k == curses.KEY_NPAGE: v["sel"] = min(sel + h - 4, max(len(rows) - 1, 0))
    elif k in (10, 13, curses.KEY_ENTER, curses.KEY_RIGHT) and rows and v["enter"]: return v["enter"](rows[sel])
    elif k == curses.KEY_LEFT: return "pop"
    elif k == ord("c") and rows and v.get("conv"): return v["conv"](rows[sel])
    elif k == ord("/") and v["rows"]:
        q=_ask(scr,h,w)
        v["flt"],v["sel"]=[r for r in v["rows"] if q.lower() in v["fmt"](r).lower()] if q else v["rows"],0

def _ui(scr, conn):
    curses.curs_set(0)
    curses.use_default_colors()
    [curses.init_pair(i, c, -1) for i, c in ((1, curses.COLOR_RED), (2, curses.COLOR_GREEN))]
    stack = [_graph(conn)]
    while True:
        h,w=scr.getmaxyx()
        scr.erase()
        v=stack[-1]
        (v.get("draw") or _ldraw)(scr, v, stack, h, w)
        k = scr.getch()
        if k == ord("q"): return
        if k in (27,curses.KEY_BACKSPACE,127) and len(stack)>1:
            stack.pop()
            continue
        if (nv := (v.get("key") or _lkey)(k, v, scr, h, w)) == "pop":
            if len(stack) > 1: stack.pop()
        elif nv: stack.append(nv)

def browse():
    """Browse the change graph: a live two-pane graph (files <-> conversations, edges drawn
    and weighted) that drills into per-file timelines and prompt + diff edit details."""
    os.environ.setdefault("ESCDELAY","25")
    conn=_conn()
    try: curses.wrapper(lambda scr: _ui(scr, conn))
    finally: conn.close()
