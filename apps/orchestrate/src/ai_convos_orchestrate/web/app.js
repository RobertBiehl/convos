// Convos Orchestrate: keyboard-first agentic IDE. Preact + htm, no build step.
import { html, render, useState, useEffect, useRef, useLayoutEffect } from "/vendor/preact-htm.js";

// ---------------------------------------------------------------- state
const store = (k, v) => { try { if (v === undefined) return JSON.parse(localStorage.getItem("orch." + k)); localStorage.setItem("orch." + k, JSON.stringify(v)); } catch { return null; } };
const S = {
  sessions: [], meta: null, cur: null, detail: {}, sel: {}, collab: null,
  overlay: null, closed: false, toast: null, pulses: {}, conn: false, askFocus: null, askPick: {}, drafts: {}, attach: {}, drop: false, optimistic: {}, res: {},
  seen: store("seen") || {}, prev: {},
};
let paint = () => {}, followLatest = () => {};
const set = (patch) => { Object.assign(S, typeof patch === "function" ? patch(S) : patch); paint(); };
const keyStack = [];
const useKeys = (fn) => useEffect(() => { keyStack.push(fn); return () => keyStack.splice(keyStack.indexOf(fn), 1); }, [fn]);

const api = {
  async get(p) { const r = await fetch(p); if (r.status === 401) return locked(); const j = await r.json(); if (!r.ok) throw new Error(j.error || r.statusText); return j; },
  async post(p, b) { const r = await fetch(p, { method: "POST", headers: { "Content-Type": "application/json", "X-Orchestrate": "1" }, body: JSON.stringify(b) }); const j = await r.json(); if (!r.ok) throw new Error(j.error || r.statusText); return j; },
};
function locked() {
  document.getElementById("app").innerHTML = `<div class="empty" style="margin-top:22vh;font-size:14px;line-height:1.9"><b style="font-size:16px;color:var(--text)">This tab is not signed in to Orchestrate</b><br>Open it with <code>convos orchestrate open</code> in a terminal;<br>it prints and opens a link that signs this browser in.</div>`;
  throw new Error("unauthorized");
}
const toast = (text, err = false) => { const t = { text, err, n: Math.random() }; set({ toast: t }); setTimeout(() => S.toast === t && set({ toast: null }), err ? 6000 : 3200); };
const fail = (e) => toast(e.message || String(e), true);

// ---------------------------------------------------------------- helpers
const STATUS = {
  input: "needs you", working: "working", planned: "planned", blocked: "blocked", failed: "failed", done: "done", skipped: "skipped", idle: "idle", closed: "closed",
};
const ITEM_STATUSES = ["planned", "working", "blocked", "done", "failed", "skipped"];
const BAR = ["done", "working", "input", "blocked", "failed", "planned", "skipped"];
const num = (id) => parseInt(id.slice(1), 10);
const base = (p) => (p || "").split("/").filter(Boolean).pop() || p;
const home = (p) => S.meta && p?.startsWith(S.meta.home) ? "~" + p.slice(S.meta.home.length) : p;
const ago = (ts) => {
  if (!ts) return ""; const s = (Date.now() - new Date(ts)) / 1000;
  return s < 45 ? "now" : s < 3600 ? `${Math.round(s / 60)}m` : s < 86400 ? `${Math.round(s / 3600)}h` : `${Math.round(s / 86400)}d`;
};
const initials = (a) => a === "user" ? "you" : a === "agent" ? "A" : a === "orchestrate" ? "O" : a.slice(0, 2).toUpperCase();
const K = (...keys) => keys.map((k) => html`<kbd>${k}</kbd>`);
const Hot = ({ k, children }) => html`<span class="hot">${[].concat(k).map((x) => html`<kbd>${x}</kbd>`)}${children}</span>`;

const esc = (s) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const inline = (s) => s.split(/(`[^`\n]+`)/g).map((part, i) => i % 2 ? `<code>${esc(part.slice(1, -1))}</code>` : esc(part)
  .replace(/\*\*([^*\n]+)\*\*/g, "<strong>$1</strong>").replace(/(^|[^*\w])\*([^*\n]+)\*(?!\w)/g, "$1<em>$2</em>")
  .replace(/\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>')
  .replace(/(^|[\s(])(https?:\/\/[^\s<)]+)/g, '$1<a href="$2" target="_blank" rel="noopener noreferrer">$2</a>')).join("");
function md(src) {
  const out = [], lines = (src || "").replace(/\r/g, "").split("\n");
  let para = [], list = null;
  const flush = () => { if (para.length) out.push(`<p>${inline(para.join("\n")).replace(/\n/g, "<br>")}</p>`); para = []; if (list) out.push(`</${list}>`); list = null; };
  for (let i = 0; i < lines.length; i++) {
    const l = lines[i];
    if (l.startsWith("```")) { flush(); const body = []; while (++i < lines.length && !lines[i].startsWith("```")) body.push(lines[i]); out.push(`<pre><code>${esc(body.join("\n"))}</code></pre>`); continue; }
    let m;
    if ((m = l.match(/^(#{1,4})\s+(.*)/))) { flush(); out.push(`<h${m[1].length}>${inline(m[2])}</h${m[1].length}>`); continue; }
    if ((m = l.match(/^\s*([-*]|\d+\.)\s+(.*)/))) { const kind = /\d/.test(m[1]) ? "ol" : "ul"; if (para.length) { const p = para; para = []; out.push(`<p>${inline(p.join("\n"))}</p>`); } if (list !== kind) { if (list) out.push(`</${list}>`); out.push(`<${kind}>`); list = kind; } out.push(`<li>${inline(m[2])}</li>`); continue; }
    if ((m = l.match(/^>\s?(.*)/))) { flush(); out.push(`<blockquote>${inline(m[1])}</blockquote>`); continue; }
    if (/^\s*\|.*\|\s*$/.test(l) && /^\s*\|[\s:|-]+\|\s*$/.test(lines[i + 1] || "")) {
      flush(); const cells = (row) => row.trim().replace(/^\||\|$/g, "").split("|").map((c) => inline(c.trim()));
      const head = cells(l), body = []; i++;
      while (i + 1 < lines.length && /^\s*\|.*\|\s*$/.test(lines[i + 1])) body.push(cells(lines[++i]));
      out.push(`<table><thead><tr>${head.map((c) => `<th>${c}</th>`).join("")}</tr></thead><tbody>${body.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("")}</tbody></table>`); continue;
    }
    if (!l.trim()) { flush(); continue; }
    if (list) { out.push(`</${list}>`); list = null; }
    para.push(l);
  }
  flush();
  return refs(out.join(""));
}

// references: i3 row, a2 result, q1 question, c5 comment, o… session, o…:i3 something in another session. Only ids that exist become links.
const REF = /(^|[^\w/#:.=?&-])(?:(o[0-9a-f]{11})(?::([icqa][1-9]\d*))?|([icqa][1-9]\d*))(?![\w-])/g;
const clip = (t, n = 34) => (t = (t || "").replace(/\s+/g, " ").trim()).length > n ? t.slice(0, n - 1) + "…" : t;
function refTarget(sid, id) {
  const s = S.sessions.find((x) => x.id === sid), d = S.detail[sid];
  if (!s) return null;
  if (!id) return s.title;
  if (!d) return sid === S.cur ? null : `${clip(s.title, 20)} › ${id}`;
  const x = { i: d.items, a: d.attachments, q: d.asks, c: d.comments }[id[0]][id];
  if (!x) return null;
  const label = id[0] === "i" ? x.title : id[0] === "a" ? x.caption || x.name || x.url : id[0] === "q" ? x.text : `${x.actor === "user" ? "you" : x.actor}: ${x.text}`;
  return (sid === S.cur ? "" : `${clip(s.title, 16)} › `) + clip(label);
}
function refs(text) {
  let skip = 0;
  return text.split(/(<[^>]+>)/g).map((p) => {
    if (p.startsWith("<")) { if (/^<(a|code)[\s>]/.test(p)) skip++; else if (/^<\/(a|code)>/.test(p)) skip--; return p; }
    return skip ? p : p.replace(REF, (m, pre, sid, sub, local) => {
      const at = sid || S.cur, id = sid ? sub : local, label = at && refTarget(at, id);
      return label ? `${pre}<a class="ref" href="#${at}" data-ref="${at}:${id || ""}"><b>${id || "session"}</b>${esc(label)}</a>` : m;
    });
  }).join("");
}
async function follow(ref) {
  const [sid, id] = ref.split(":");
  if (S.overlay) set({ overlay: null });
  go(sid);
  if (!S.detail[sid]) await loadDetail(sid);
  const d = S.detail[sid], kind = id?.[0];
  if (kind === "q" && !d.asks[id].answer && !d.asks[id].cancelled) return needYou(sid, id);
  if (kind === "a") {
    const a = d.attachments[id];
    S.sel[sid] = a.target || null;
    S.res[`${sid}:${a.target || ""}`] = resultsFor(d, a.target).findIndex((x) => x.id === id);
    return set({ overlay: { kind: "look" } });
  }
  const x = kind === "i" ? d.items[id] : kind === "c" ? d.comments[id] : kind === "q" ? d.asks[id] : null;
  S.sel[sid] = kind === "i" ? id : x?.target || null;
  paint();
  if (kind === "c") setTimeout(() => { const el = document.querySelector(`.detail [data-c="${id}"]`); if (el) { el.scrollIntoView({ block: "center" }); el.classList.remove("hl"); void el.offsetWidth; el.classList.add("hl"); } }, 120);
}
addEventListener("click", (e) => { const a = e.target.closest?.("a.ref"); if (a) { e.preventDefault(); e.stopPropagation(); follow(a.dataset.ref); } }, true);
const Md =({ text, cls = "" }) => html`<div class="md ${cls}" dangerouslySetInnerHTML=${{ __html: md(text) }} />`;

// ---------------------------------------------------------------- icons
const ICON = {
  planned: html`<circle cx="8" cy="8" r="5.6" fill="none" stroke="currentColor" stroke-width="1.6" stroke-dasharray="2.4 2.1" />`,
  working: html`<circle cx="8" cy="8" r="5.6" fill="none" stroke="currentColor" stroke-opacity=".22" stroke-width="1.9" />`,
  arc: html`<path d="M8 2.4a5.6 5.6 0 0 1 5.6 5.6" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />`,
  ring: html`<circle cx="8" cy="8" r="6.6" fill="none" stroke="currentColor" stroke-width="1.3" />`,
  input: html`<circle cx="8" cy="8" r="5" fill="currentColor" /><path d="M6.6 6.5a1.45 1.45 0 1 1 2 1.33c-.4.18-.6.45-.6.9" fill="none" stroke="var(--bg)" stroke-width="1.35" stroke-linecap="round" /><circle cx="8" cy="10.75" r=".75" fill="var(--bg)" />`,
  blocked: html`<circle cx="8" cy="8" r="5.6" fill="none" stroke="currentColor" stroke-width="1.8" /><path d="M5.3 8h5.4" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" />`,
  done: html`<path d="M4.4 8.4l2.4 2.4 4.8-5.2" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" />`,
  failed: html`<circle cx="8" cy="8" r="6" fill="currentColor" /><path d="M5.9 5.9l4.2 4.2M10.1 5.9l-4.2 4.2" stroke="var(--bg)" stroke-width="1.6" stroke-linecap="round" />`,
  skipped: html`<circle cx="8" cy="8" r="5.6" fill="none" stroke="currentColor" stroke-width="1.5" /><path d="M5 11l6-6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" />`,
  idle: html`<circle cx="8" cy="8" r="4.6" fill="none" stroke="currentColor" stroke-width="1.5" />`,
  closed: html`<rect x="3.2" y="3.2" width="9.6" height="9.6" rx="2.6" fill="none" stroke="currentColor" stroke-width="1.5" />`,
};
// status icon. The looping parts (working spinner, needs-you pulse) are their own HTML layers animating only transform and opacity,
// so the compositor runs them; animating SVG shapes or box-shadow repaints on the main thread every frame and makes typing lag
const LOOP = { working: ["spin", "arc"], input: ["pulse", "ring"] };
const SI = ({ s, sm }) => {
  const cls = `si ${sm ? "sm" : ""} ${s} s-${s}`, loop = LOOP[s];
  if (!loop) return html`<svg class=${cls} viewBox="0 0 16 16" aria-label=${STATUS[s]}>${ICON[s] || ICON.idle}</svg>`;
  return html`<span class=${cls} role="img" aria-label=${STATUS[s]}><svg viewBox="0 0 16 16">${ICON[s]}</svg><span class=${loop[0]}><svg viewBox="0 0 16 16">${ICON[loop[1]]}</svg></span></span>`;
};
const Pill = ({ s, lg }) => html`<span class="pill ${lg ? "lg" : ""} s-${s}"><${SI} s=${s} sm=${!lg} />${STATUS[s]}</span>`;
const G = {
  chat: html`<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><path d="M3 3.5h10v7H7l-3 2.5v-2.5H3z" stroke-linejoin="round"/></svg>`,
  clip: html`<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><path d="M10.5 5.5l-4.6 4.6a1.4 1.4 0 0 0 2 2l5-5a2.8 2.8 0 0 0-4-4l-5 5a4.2 4.2 0 0 0 6 6l4-4" stroke-linecap="round"/></svg>`,
  tree: html`<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4"><circle cx="4" cy="4" r="1.6"/><circle cx="12" cy="8" r="1.6"/><circle cx="12" cy="13" r="1.6"/><path d="M4 5.6V11a2 2 0 0 0 2 2h4.4M4 8h6.4"/></svg>`,
  file: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M6 3h8l4 4v14H6z" stroke-linejoin="round"/><path d="M14 3v4h4"/></svg>`,
  play: html`<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="11" fill="rgba(8,10,14,.62)"/><path d="M10 8l6 4-6 4z" fill="#fff"/></svg>`,
  archive: html`<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"><rect x="2" y="2.8" width="12" height="3.2" rx="1"/><path d="M3.2 6v6.4a1 1 0 0 0 1 1h7.6a1 1 0 0 0 1-1V6M6.4 8.8h3.2"/></svg>`,
  unarchive: html`<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round" stroke-linecap="round"><rect x="2" y="2.8" width="12" height="3.2" rx="1"/><path d="M3.2 6v6.4a1 1 0 0 0 1 1h7.6a1 1 0 0 0 1-1V6M8 12V8.2M6.3 9.8L8 8.1l1.7 1.7"/></svg>`,
  pdf: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M6 3h8l4 4v14H6z" stroke-linejoin="round"/><path d="M9 13h6M9 16.5h4"/></svg>`,
  link: html`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1" stroke-linecap="round"/></svg>`,
  logo: html`<svg viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="var(--raise)" stroke="var(--line2)"/><circle cx="10" cy="16" r="3.4" fill="var(--accent)"/><circle cx="22" cy="9" r="3" fill="var(--done)"/><circle cx="22" cy="23" r="3" fill="var(--input)"/><path d="M13 15l6-4.5M13 17l6 4.5" stroke="var(--faint)" stroke-width="1.6"/></svg>`,
};

// ---------------------------------------------------------------- derived views
function ordered() {
  const vis = S.sessions.filter((s) => S.closed || !s.closed || s.id === S.cur), ids = new Set(vis.map((s) => s.id)), kids = {};
  for (const s of vis) { const p = s.parent?.split(":")[0]; (kids[p && ids.has(p) ? p : ""] ||= []).push(s); }
  const out = [], walk = (k, depth) => (kids[k] || []).sort((a, b) => a.created.localeCompare(b.created)).forEach((s) => { out.push({ s, depth }); walk(s.id, depth + 1); });
  walk("", 0);
  return out;
}
function flatItems(d) {
  const kids = {};
  for (const i of Object.values(d.items)) (kids[i.parent || ""] ||= []).push(i);
  const out = [], walk = (k, depth) => (kids[k] || []).sort((a, b) => num(a.id) - num(b.id)).forEach((i) => { out.push({ i, depth }); walk(i.id, depth + 1); });
  walk("", 0);
  return out;
}
// rows you archived live in the scrollback; everything else, finished or not, stays current until you archive it
const trees = (d) => flatItems(d).reduce((t, r) => { if (r.depth && t.length) t[t.length - 1].push(r); else t.push([r]); return t; }, []);
const finishedTree = (d, t) => t.every((r) => ["done", "skipped"].includes(d.effective[r.i.id]));
function boardRows(d) {
  const all = trees(d);
  return { archived: all.filter((t) => t[0].i.archived).flat(), current: all.filter((t) => !t[0].i.archived).flat() };
}
const rootOf = (d, id) => { let i = d.items[id]; while (i.parent) i = d.items[i.parent]; return i.id; };
// archiving is your call: it moves a top-level row and everything under it to the scrollback, or brings it back. The row moves at
// once; if the server refuses, it moves back and the toast says why. A row with open work under it takes a second e.
const finishedRows = (d) => trees(d).filter((t) => !t[0].i.archived && finishedTree(d, t)).map((t) => t[0].i.id);
async function setArchived(d, ids, on, msg) {
  const was = ids.map((i) => d.items[i].archived);
  ids.forEach((i) => { d.items[i].archived = on ? new Date().toISOString() : null; });
  toast(msg);
  if (await act({ kind: "archive", items: ids, archived: on })) return;
  ids.forEach((i, n) => { d.items[i].archived = was[n]; });
  paint();
}
let confirming = null;
async function archive(all, id = S.sel[S.cur]) {
  const d = det(); if (!d) return;
  if (all) {
    const ids = finishedRows(d);
    if (!ids.length) return toast("No finished rows to archive");
    return setArchived(d, ids, true, `Archived ${ids.length} finished ${ids.length === 1 ? "row" : "rows"} · e on one brings it back`);
  }
  if (!id) return toast("Select a row first (j k or a click), or ⇧E archives every finished row");
  const root = rootOf(d, id), back = !!d.items[root].archived, tree = trees(d).find((t) => t[0].i.id === root);
  const open = tree.filter((r) => !["done", "skipped"].includes(d.effective[r.i.id])).map((r) => `${r.i.id} ${STATUS[d.effective[r.i.id]]}`);
  if (!back && open.length && !(confirming?.root === root && Date.now() < confirming.until)) {
    confirming = { root, until: Date.now() + 6000 };
    return toast(`${root} still has open work (${open.slice(0, 4).join(", ")}${open.length > 4 ? `, ${open.length - 4} more` : ""}). Archive again to hide it anyway`, true);
  }
  confirming = null;
  const sel = S.sel[S.cur];
  if (!back && sel && rootOf(d, sel) === root) { const { current } = boardRows(d), i = current.findIndex((r) => r.i.id === root), rest = current.filter((r) => rootOf(d, r.i.id) !== root); S.sel[S.cur] = (rest.find((r, n) => n >= i) || rest[rest.length - 1])?.i.id || null; }
  setArchived(d, [root], !back, back ? `${root} is current again` : `Archived ${root}${tree.length > 1 ? ` with ${tree.length - 1} rows under it` : ""} · e brings it back`);
}
const navRows = (d) => { const b = boardRows(d); return [...b.archived, ...b.current]; };
const resKey = (target) => `${S.cur}:${target || ""}`;
const resIdx = (target) => S.res[resKey(target)] ?? 0;
const cur = () => S.sessions.find((s) => s.id === S.cur);
const det = () => S.detail[S.cur];
const selected = () => { const d = det(); return d && S.sel[S.cur] && d.items[S.sel[S.cur]] ? d.items[S.sel[S.cur]] : null; };
const childrenOf = (sid, item) => S.sessions.filter((s) => item ? s.parent === `${sid}:${item}` : s.parent?.split(":")[0] === sid);
const openAsks = (d) => Object.values(d.asks).filter((a) => !a.answer && !a.cancelled);
const commentsFor = (d, target) => [...Object.values(d.comments), ...(S.optimistic[S.cur] || [])].filter((c) => (c.target || null) === (target || null)).sort((a, b) => a.at.localeCompare(b.at));
const resultsFor = (d, target) => Object.values(d.attachments).filter((a) => (a.target || null) === (target || null));
const blobUrl = (a) => a.src || `/api/blob/${a.hash}?mime=${encodeURIComponent(a.mime || "")}&name=${encodeURIComponent(a.name)}`;
const pulse = (id, kind) => { S.pulses[id] = { kind, n: (S.pulses[id]?.n || 0) + 1, at: Date.now() }; };

// ---------------------------------------------------------------- data flow
async function loadSessions() { S.sessions = await api.get("/api/sessions"); paint(); }
const pendingDetail = {};
function scheduleDetail(sid) { clearTimeout(pendingDetail[sid]); pendingDetail[sid] = setTimeout(() => loadDetail(sid).catch(fail), 60); }
async function loadDetail(sid) {
  const d = await api.get(`/api/session/${sid}`), old = S.detail[sid];
  d.unread ||= [];  // a server from before read tracking sends none
  if (old) {
    for (const [id, it] of Object.entries(d.items)) { if (!old.items[id]) pulse(id, "new"); else if (old.effective[id] !== d.effective[id]) pulse(id, "status"); }
    for (const c of Object.values(d.comments)) if (!old.comments[c.id] && c.actor !== "user") pulse(c.target || "session", "comment");
    for (const a of Object.values(d.attachments)) if (!old.attachments[a.id]) pulse(a.target || "session", "result");
  }
  S.detail[sid] = d;
  if (S.sel[sid] && !d.items[S.sel[sid]]) S.sel[sid] = null;
  S.optimistic[sid] = (S.optimistic[sid] || []).filter((o) => !Object.values(d.comments).some((c) => c.actor === "user" && c.text === o.text && c.target === o.target && (c.files || []).length === o.files.length));
  if (sid === S.cur) { S.seen[sid] = d.seq; store("seen", S.seen); dwell(); }
  paint();
}
function upsert(s) {
  const i = S.sessions.findIndex((x) => x.id === s.id), before = S.prev[s.id];
  if (i < 0) S.sessions.push(s); else S.sessions[i] = s;
  if (before && before !== s.status && s.status === "input" && s.id !== S.cur) toast(`${s.title} needs you`);
  if (before && before !== "done" && s.status === "done") pulse(s.id, "status");
  S.prev[s.id] = s.status;
}
function connect() {
  const es = new EventSource("/api/stream");
  es.onopen = () => set({ conn: true });
  es.onerror = () => set({ conn: false });
  es.addEventListener("version", (e) => { const v = JSON.parse(e.data); if (!S.version) S.version = v; else if (v !== S.version) reloadInPlace(); });
  es.addEventListener("session", (e) => { const s = JSON.parse(e.data); upsert(s); if (s.id === S.cur) scheduleDetail(s.id); paint(); });}

// ---------------------------------------------------------------- updates reload in place, never while you are mid-thought
function reloadInPlace() {
  const typing = document.activeElement?.matches("input, textarea") && document.activeElement.value.trim();
  if (S.overlay || typing || S.askFocus) return setTimeout(reloadInPlace, 1500);
  const b = document.querySelector(".center .scroll"), d = document.querySelector(".dscroll");
  try {
    sessionStorage.setItem("orch.restore", JSON.stringify({
      at: Date.now(), cur: S.cur, sel: S.sel, res: S.res, closed: S.closed, drafts: S.drafts,
      scroll: b ? { top: b.scrollTop, pinned: b.scrollHeight - b.scrollTop - b.clientHeight < 40 } : null, dscroll: d?.scrollTop || 0,
      composer: !!document.activeElement?.closest(".composer"),
    }));
  } catch {}
  location.reload();
}
function restored() {
  try { const r = JSON.parse(sessionStorage.getItem("orch.restore")); sessionStorage.removeItem("orch.restore"); return r && Date.now() - r.at < 120000 ? r : null; } catch { return null; }
}

// ---------------------------------------------------------------- navigation and actions
function go(sid) {
  if (!sid) return;
  const apply = () => {
    S.cur = sid; S.askFocus = null;
    if (!S.detail[sid]) loadDetail(sid).catch(fail); else { S.seen[sid] = S.detail[sid].seq; store("seen", S.seen); }    history.replaceState(null, "", `#${sid}`);
    paint();
  };
  if (document.startViewTransition && S.cur && sid !== S.cur) document.startViewTransition(() => new Promise((r) => { apply(); setTimeout(r, 30); }));
  else apply();
}
// selection: a row (the right pane shows the item) or none (the right pane shows the session); each row remembers its result
function select(id) { S.sel[S.cur] = id; S.askFocus = null; paint(); dwell(); }
// read state: a row counts as read once you stay on it for a second (or preview or open its results); rows with changes you
// have not seen are bright with a dot, read rows are quiet. Agents may archive only finished rows you have read.
let dwelling = null;
function dwell() {
  clearTimeout(dwelling);
  const sid = S.cur, id = S.sel[sid];
  if (id && det()?.unread.includes(id)) dwelling = setTimeout(() => S.cur === sid && S.sel[sid] === id && document.visibilityState === "visible" && markSeen([id]), 1000);
}
function markSeen(ids) {
  const sid = S.cur, d = S.detail[sid], fresh = d ? ids.filter((i) => d.unread.includes(i)) : [];
  if (!fresh.length) return;
  d.unread = d.unread.filter((i) => !fresh.includes(i));
  paint();
  api.post("/api/seen", { session: sid, items: fresh }).catch(fail);
}
function move(delta) {
  const d = det(); if (!d) return;
  const rows = navRows(d); if (!rows.length) return;
  const i = rows.findIndex((r) => r.i.id === S.sel[S.cur]);
  select(rows[i < 0 && Math.abs(delta) < 1e9 ? rows.findIndex((r) => r.i.id === relevant(d)) : Math.max(0, Math.min(rows.length - 1, (i < 0 ? rows.length - 1 : i) + delta))].i.id);
}
function top() {
  const d = det(); if (!d) return;
  const { archived, current } = boardRows(d), rows = [...archived, ...current], i = rows.findIndex((r) => r.i.id === S.sel[S.cur]);
  if (rows.length) select(rows[current.length && (i < 0 || i > archived.length) ? archived.length : 0].i.id);
}
function rowResults() { const d = det(), it = selected(); return d ? resultsFor(d, it?.id) : []; }
function moveResult(delta) {
  const list = rowResults(), it = selected(); if (!list.length) return;
  S.res[resKey(it?.id)] = Math.max(0, Math.min(list.length - 1, resIdx(it?.id) + delta)); paint();
}
function look() { if (rowResults().length || selected()) set({ overlay: { kind: "look" } }); }
function openSelected() {
  const list = rowResults(), a = list[Math.min(list.length - 1, resIdx(selected()?.id))];
  if (!a) return false;
  if (a.kind === "link") window.open(a.url, "_blank", "noopener,noreferrer"); else set({ overlay: { kind: "look" } });
  return true;
}
function focusComposer() {
  S.askFocus = null; paint();
  requestAnimationFrame(() => { const t = document.querySelector(".composer textarea"); t?.focus(); });
}
function sessionChat() { select(null); focusComposer(); }
async function act(body, okText) {
  try { const r = await api.post("/api/act", { session: S.cur, ...body }); if (okText) toast(okText); scheduleDetail(S.cur); return r; } catch (e) { fail(e); }
}
function fly(fromEl, toEl, text) {
  if (!fromEl || !toEl || matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const a = fromEl.getBoundingClientRect(), b = toEl.getBoundingClientRect();
  const g = document.createElement("div");
  g.className = "ghost"; g.textContent = text.split("\n")[0].slice(0, 90);
  document.body.append(g);
  const x0 = a.left + 10, y0 = a.top + 4, x1 = b.left + Math.min(40, b.width / 3), y1 = b.top + b.height / 2 - 14;
  const mx = (x0 + x1) / 2, my = Math.min(y0, y1) - 70;
  g.animate([
    { transform: `translate(${x0}px, ${y0}px) scale(1)`, opacity: 1 },
    { transform: `translate(${mx}px, ${my}px) scale(.82)`, opacity: .95, offset: .45 },
    { transform: `translate(${x1}px, ${y1}px) scale(.42)`, opacity: 0 },
  ], { duration: 640, easing: "cubic-bezier(.45,.05,.25,1)" }).onfinish = () => {
    g.remove(); toEl.classList.remove("receive"); void toEl.offsetWidth; toEl.classList.add("receive");
  };
}
async function sendComment(text, target, fromEl, files = []) {
  const sid = S.cur, toEl = target ? document.querySelector(`.center [data-item="${target}"]`) || document.querySelector(".dhead") : document.querySelector(`.side [data-session="${sid}"]`) || document.querySelector(".shead h1");
  fly(fromEl, toEl, text || `${files.length} image${files.length > 1 ? "s" : ""}`);
  const mine = { id: "…", actor: "user", text, files: files.map(({ name, mime, src }) => ({ name, mime, src })), target: target || null, at: new Date().toISOString(), delivered: null, sending: true };
  (S.optimistic[sid] ||= []).push(mine);
  paint();
  try { await api.post("/api/act", { session: sid, kind: "comment", text, target: target || null, files: files.map(({ name, mime, data }) => ({ name, mime, data })) }); scheduleDetail(sid); }
  catch (e) {
    // nothing is lost: the text and images go back into the composer
    const key = `${sid}:${target || ""}`;
    S.optimistic[sid] = S.optimistic[sid].filter((o) => o !== mine);
    S.drafts[key] ||= text; S.attach[key] = [...files, ...(S.attach[key] || [])];
    fail(e);
  }
}

// images for a comment: pasted (⌘V), dropped on the detail pane, or picked; held per composer until sent
const IMAGE = /^image\/(png|jpeg|gif|webp|avif)$/;
const readImage = (f) => new Promise((ok, no) => { const r = new FileReader(); r.onload = () => ok({ name: f.name || "image.png", mime: f.type, size: f.size, src: URL.createObjectURL(f), data: r.result.split(",")[1] }); r.onerror = no; r.readAsDataURL(f); });
async function attachImages(key, list) {
  const all = [...list], imgs = all.filter((f) => IMAGE.test(f.type)), limit = S.meta?.image_limit || Infinity;
  if (!imgs.length) return all.length && toast("Only images (PNG, JPEG, GIF, WebP, AVIF) can be attached to comments", true);
  if (imgs.some((f) => f.size > limit)) return toast(`Images can be at most ${Math.round(limit / 2 ** 20)} MB`, true);
  if (imgs.length < all.length) toast("Only the images were attached", true);
  S.attach[key] = [...(S.attach[key] || []), ...await Promise.all(imgs.map(readImage))];
  paint();
  focusComposer();
}
function unattach(key, n) { URL.revokeObjectURL(S.attach[key][n].src); S.attach[key] = S.attach[key].filter((_, m) => m !== n); paint(); }
const dragsFiles = (e) => [...(e.dataTransfer?.types || [])].includes("Files");
const dropOver = (e) => { if (dragsFiles(e)) { e.preventDefault(); if (!S.drop) set({ drop: true }); } };
const dropLeave = (e) => !e.currentTarget.contains(e.relatedTarget) && S.drop && set({ drop: false });
const dropOn = (e, key) => { if (dragsFiles(e)) { e.preventDefault(); set({ drop: false }); attachImages(key, e.dataTransfer.files); } };
async function answer(ask, choices, text = "") {
  const el = document.querySelector(`[data-ask="${ask.id}"]`);
  el?.classList.add("answered");
  S.askFocus = null;
  await new Promise((r) => setTimeout(r, el ? 260 : 0));
  await act({ kind: "answer", ask: ask.id, choices, text }, ask.kind === "permission" ? `${choices[0]}` : "Answer sent");
  S.askPick[ask.id] = [];
}
function openResult(target, i) { S.res[resKey(target)] = i; set({ overlay: { kind: "look" } }); if (target) markSeen([target]); }

// ---------------------------------------------------------------- command registry (palette, help, keys)
function commands() {
  const s = cur(), it = selected(), d = det();
  return [
    { group: "Navigate", keys: ["1", "…", "9"], label: "Switch to session by number" },
    { group: "Navigate", keys: ["[", "]"], label: "Previous / next session", run: () => cycle(1) },
    { group: "Navigate", keys: ["j", "k"], label: "Next / previous item", run: () => move(1) },
    { group: "Navigate", keys: ["⌘", "↑"], label: "First current row; again for the archive (also g, Home)", run: top },
    { group: "Navigate", keys: ["⌘", "↓"], label: "Last row (also G, End)", run: () => move(1e9) },
    { group: "Navigate", keys: ["⌥", "↑↓"], label: "Page up / down (also PgUp, PgDn)", run: () => move(10) },
    { group: "Navigate", keys: ["⌘", "[ ]"], label: "Previous / next session (also [ ])", run: () => cycle(-1) },
    { group: "Items", keys: ["⌘", "← →"], label: "First / last result in the row", run: () => moveResult(1e9) },
    { group: "Navigate", keys: ["Esc"], label: "Leave the row: session info and chat", run: () => select(null), when: it },
    { group: "Navigate", keys: ["p"], label: "Go to parent session", run: () => s?.parent && go(s.parent.split(":")[0]), when: s?.parent },
    { group: "Talk", keys: ["c"], label: it ? `Comment on ${it.id} · ${it.title}` : "Comment on the session", run: focusComposer },
    { group: "Talk", keys: ["⇧", "C"], label: "Comment on the whole session", run: sessionChat },
    { group: "Talk", keys: ["a"], label: "Answer the next question", run: () => focusAsk(), when: d && openAsks(d).length },
    { group: "Items", keys: ["s"], label: "Set item status", run: () => it && set({ overlay: { kind: "status", item: it.id } }), when: it },
    { group: "Items", keys: ["←", "→"], label: "Select a result (each row remembers its own)", run: () => moveResult(1), when: it },
    { group: "Items", keys: ["Space"], label: "Quick Look: preview the result or row", run: look, when: d },
    { group: "Items", keys: ["e"], label: it && d?.items[rootOf(d, it.id)].archived ? "Bring the row back to current" : "Archive the finished row", run: () => archive(false), when: it },
    { group: "Items", keys: ["⇧", "E"], label: "Archive every finished row", run: () => archive(true), when: d },
    { group: "Items", keys: ["↵"], label: "Open the selected result (or o)", run: () => openSelected(), when: d },
    { group: "Sessions", keys: ["n"], label: "New session", run: () => set({ overlay: { kind: "new" } }) },
    { group: "Sessions", keys: ["⇧", "N"], label: it ? `New child session for ${it.id}` : "New child session", run: () => s && set({ overlay: { kind: "new", parent: it ? `${s.id}:${it.id}` : s.id } }), when: s },
    { group: "Sessions", keys: ["r"], label: "Resume session", run: () => act({ kind: "resume" }, "Resuming…"), when: s && s.origin === "gui" && !s.live && !s.closed },
    { group: "Sessions", keys: ["x"], label: "Stop session", run: () => act({ kind: "stop" }, "Stopping…"), when: s && s.origin === "gui" && s.live },
    { group: "Sessions", keys: ["⇧", "X"], label: s?.closed ? "Reopen session" : "Close session (keeps its board)", run: () => act({ kind: s.closed ? "reopen" : "close" }, s.closed ? "Reopened" : "Closed"), when: s },
    { group: "Sessions", keys: ["."], label: S.closed ? "Hide closed sessions" : "Show closed sessions", run: () => set({ closed: !S.closed }) },
    { group: "View", keys: ["⇧", "L"], label: "Toggle light / dark", run: toggleTheme },
    { group: "View", keys: ["⌘", "K"], label: "Command palette", run: () => set({ overlay: { kind: "palette" } }) },
    { group: "View", keys: ["?"], label: "Keyboard shortcuts", run: () => set({ overlay: { kind: "help" } }) },
  ];
}
function cycle(delta) { const o = ordered(), i = o.findIndex((r) => r.s.id === S.cur); if (o.length) go(o[(i + delta + o.length) % o.length].s.id); }
// every "needs you" signal lands here: the session, the row the question is on, and the question itself, ready for 1-9
async function needYou(sid = S.cur, ask = null) {
  go(sid);
  if (!S.detail[sid]) await loadDetail(sid);
  const a = ask ? S.detail[sid].asks[ask] : openAsks(S.detail[sid]).sort((x, y) => x.at.localeCompare(y.at))[0];
  if (!a) return;
  S.sel[sid] = a.target || null;
  set({ askFocus: a.id });
  requestAnimationFrame(() => document.querySelector(`.center [data-ask="${a.id}"]`)?.scrollIntoView({ block: "center", behavior: "smooth" }));
}
const focusAsk = () => needYou();
function toggleTheme() { const r = document.documentElement, next = r.dataset.theme === "light" ? "dark" : r.dataset.theme === "dark" ? "light" : matchMedia("(prefers-color-scheme: light)").matches ? "dark" : "light"; r.dataset.theme = next; store("theme", next); }

function onKey(e) {
  for (let i = keyStack.length - 1; i >= 0; i--) if (keyStack[i](e)) return;
  const mod = e.metaKey || e.ctrlKey, k = e.key;
  if (mod && k.toLowerCase() === "k") { e.preventDefault(); return set({ overlay: S.overlay?.kind === "palette" ? null : { kind: "palette" } }); }
  if (S.overlay || e.target.matches("input, textarea, select, [contenteditable]")) return;
  const native = {
    "Meta+ArrowDown": () => { move(1e9); followLatest(); }, "Meta+ArrowUp": top, "End": () => { move(1e9); followLatest(); }, "Home": top,
    "Meta+ArrowRight": () => moveResult(1e9), "Meta+ArrowLeft": () => moveResult(-1e9),
    "Alt+ArrowDown": () => move(10), "Alt+ArrowUp": () => move(-10), "PageDown": () => move(10), "PageUp": () => move(-10),
    "Meta+[": () => cycle(-1), "Meta+]": () => cycle(1), "Meta+Enter": () => openSelected(),
  }[`${e.metaKey ? "Meta+" : ""}${e.altKey ? "Alt+" : ""}${k}`];
  if (native) { e.preventDefault(); return native(); }
  if (mod || e.altKey) return;
  const d = det(), it = selected(), s = cur();
  if (S.askFocus && d) {
    const a = d.asks[S.askFocus];
    if (a && /^[1-9]$/.test(k) && a.options[+k - 1]) {
      e.preventDefault(); const opt = a.options[+k - 1], pick = S.askPick[a.id] || [];
      if (!a.multi) return answer(a, [opt]);
      S.askPick[a.id] = pick.includes(opt) ? pick.filter((x) => x !== opt) : [...pick, opt]; return paint();
    }
    if (k === "Enter" && a) { e.preventDefault(); return answer(a, S.askPick[a.id] || [], document.querySelector(`[data-ask="${a.id}"] input`)?.value || ""); }
    if (k === "Tab" && a) { e.preventDefault(); return document.querySelector(`[data-ask="${a.id}"] input`)?.focus(); }
    if (k === "Escape") return set({ askFocus: null });
  }
  if (/^[1-9]$/.test(k)) { const row = ordered()[+k - 1]; if (row) { e.preventDefault(); go(row.s.id); } return; }
  const map = {
    j: () => move(1), ArrowDown: () => move(1), k: () => move(-1), ArrowUp: () => move(-1), g: top, G: () => move(1e9),
    "[": () => cycle(-1), "]": () => cycle(1), Enter: () => openSelected(), l: () => moveResult(1), ArrowRight: () => moveResult(1), h: () => moveResult(-1), ArrowLeft: () => moveResult(-1),
    " ": look, Escape: () => select(null), e: () => archive(false), E: () => archive(true),
    c: focusComposer, C: sessionChat, a: focusAsk,
    s: () => it && set({ overlay: { kind: "status", item: it.id } }), o: () => openSelected(),
    n: () => set({ overlay: { kind: "new" } }), N: () => s && set({ overlay: { kind: "new", parent: it ? `${s.id}:${it.id}` : s.id } }),
    r: () => s?.origin === "gui" && !s.live && !s.closed && act({ kind: "resume" }, "Resuming…"), x: () => s?.origin === "gui" && s.live && act({ kind: "stop" }, "Stopping…"),
    X: () => s && act({ kind: s.closed ? "reopen" : "close" }, s.closed ? "Reopened" : "Closed · its board stays here"),
    p: () => s?.parent && go(s.parent.split(":")[0]), ".": () => set({ closed: !S.closed }),
    L: toggleTheme, "?": () => set({ overlay: { kind: "help" } }), "/": () => set({ overlay: { kind: "palette" } }),
  };
  if (map[k]) { e.preventDefault(); map[k](); }
}

// ---------------------------------------------------------------- components
function Progress({ counts, big }) {
  const total = Object.values(counts || {}).reduce((a, b) => a + b, 0);
  if (!total) return html`<div class="progress ${big ? "big" : ""}" />`;
  return html`<div class="progress ${big ? "big" : ""}" title=${BAR.filter((k) => counts[k]).map((k) => `${counts[k]} ${STATUS[k]}`).join(" · ")}>${BAR.map((k) => counts[k] ? html`<span class="b-${k}" style=${{ flexGrow: counts[k] }} />` : null)}</div>`;
}

function Sidebar() {
  const rows = ordered(), closedCount = S.sessions.filter((s) => s.closed).length;
  return html`<aside class="side">
    <h6><span class="grow">Sessions</span><${Hot} k="n">new<//></h6>
    <div class="sessions">
      ${rows.map(({ s, depth }, i) => {
        const unread = s.id !== S.cur && s.seq > (S.seen[s.id] || 0);
        return html`<div key=${s.id} data-session=${s.id} class="srow ${s.id === S.cur ? "cur" : ""} ${s.closed ? "closed" : ""} ${depth ? "child" : ""}" style=${depth > 1 ? { marginLeft: `${depth * 14}px` } : null} onClick=${() => go(s.id)}>
          <${SI} s=${s.status} />
          <div style="min-width:0">
            <div class="title">${s.title}</div>
            <div class="sub">${s.status === "working" && s.activity ? html`<span class="act">${s.activity.text}</span>` : html`<span>${base(s.cwd)}</span><span>·</span><span>${ago(s.updated)}</span>`}</div>
            ${s.items ? html`<div class="mini"><${Progress} counts=${s.counts} /></div>` : null}
          </div>
          <div class="right">${i < 9 ? html`<kbd>${i + 1}</kbd>` : null}${s.asks ? html`<button class="badge" title="Go to the question" onClick=${(e) => { e.stopPropagation(); needYou(s.id); }}>${s.asks}</button>` : unread ? html`<span class="unread" />` : null}${s.live ? html`<span class="dot live" title="live" />` : null}</div>
        </div>`;
      })}
      ${!rows.length ? html`<div class="empty">No sessions yet.<br />Press ${K("n")} to start one, or type <code>/orchestrate</code> in any Claude Code or Codex session.</div>` : null}
    </div>
    ${closedCount ? html`<h6 style="margin-bottom:12px"><span class="grow">${closedCount} closed</span><${Hot} k=".">${S.closed ? "hide" : "show"}<//></h6>` : null}
  </aside>`;
}

function Crumbs({ s }) {
  const chain = [];
  let p = s.parent;
  while (p) { const [psid, item] = p.split(":"), ps = S.sessions.find((x) => x.id === psid); if (!ps) break; chain.unshift({ ps, item }); p = ps.parent; }
  if (!chain.length) return null;
  return html`<div class="crumbs">${chain.map(({ ps, item }) => html`<button onClick=${() => go(ps.id)}>${ps.title}</button>${item ? html`<span>›</span><span>${item}</span>` : null}<span>›</span>`)}<${Hot} k="p">parent<//></div>`;
}

// the center keeps one slim bar so the board never moves when the selection changes; the full session info lives in the right pane
function SessionBar({ s }) {
  return html`<header class="shead slim">
    <${Crumbs} s=${s} />
    <h1><span class="t">${s.title}</span>${s.status === "input" ? html`<button class="pillbtn" title="Go to the question" onClick=${() => needYou(s.id)}><${Pill} s="input" /><${Hot} k="a">answer<//></button>` : html`<${Pill} s=${s.status} />`}</h1>
  </header>`;
}
// who this session works with: its collab rooms and teammates, each a jump to their board when they are on orchestrate
function WorksWith({ s }) {
  const rooms = (S.collab?.rooms || []).filter((r) => r.members?.some((m) => m.orchestrate === s.id));
  if (!rooms.length) return null;
  return html`<div class="works">${rooms.map((r) => html`<div key=${r.id}>
    <div class="rh">${G.tree}<span>Works with</span><b>${r.name}</b>${r.paused ? html`<span class="pill s-blocked">paused</span>` : null}</div>
    ${r.goal ? html`<div class="goal">${r.goal}</div>` : null}
    <div class="children" style="margin-top:6px">${r.members.filter((m) => m.orchestrate !== s.id).map((m) => {
      const o = S.sessions.find((x) => x.id === m.orchestrate), tip = [m.name, m.focus, m.report && `${m.report.state}: ${m.report.text}`].filter(Boolean).join("\n");
      return html`<button class="chip ${o ? "" : "off"}" title=${tip} onClick=${() => o && go(o.id)}>${o ? html`<${SI} s=${o.status} sm />` : html`<span class="dot ${m.active ? "on" : ""}" />`}<span class="ell">${o ? o.title : m.name}</span>${o ? null : html`<span style="color:var(--faint)">not on orchestrate</span>`}</button>`;
    })}</div>
  </div>`)}</div>`;
}
function SessionInfo({ s, d }) {
  const kids = childrenOf(s.id), counts = s.counts || {};
  const runtime = s.origin === "gui" ? (s.live ? (s.busy ? "running a turn" : "live, idle") : (d.runtime?.state === "failed" ? `failed (exit ${d.runtime.code})` : "stopped")) : (s.live ? "joined · reachable" : "joined · offline (messages wait in its inbox)");
  return html`<div class="sinfo">
    <div class="actions">
      ${s.origin === "gui" && s.live ? html`<button class="abtn" onClick=${() => act({ kind: "stop" }, "Stopping…")}>${K("x")} Stop</button>` : null}
      ${s.origin === "gui" && !s.live && !s.closed ? html`<button class="abtn" onClick=${() => act({ kind: "resume" }, "Resuming…")}>${K("r")} Resume</button>` : null}
      <button class="abtn" onClick=${() => set({ overlay: { kind: "new", parent: s.id } })}>${K("⇧", "N")} Child</button>
      <button class="abtn" onClick=${() => act({ kind: s.closed ? "reopen" : "close" }, s.closed ? "Reopened" : "Closed")}>${K("⇧", "X")} ${s.closed ? "Reopen" : "Close"}</button>
    </div>
    <div class="metaline">
      <span class="m"><span class="dot ${s.live ? "live" : ""}" />${runtime}</span>
      <span class="m">${s.provider}${s.model ? html` · <code>${s.model}</code>` : null}${s.effort ? ` · ${s.effort}` : ""}</span>
      <span class="m"><code>${home(s.cwd)}</code></span>
      <span class="m"><code>${s.id}</code></span>
      ${d.runtime?.cost ? html`<span class="m">$${d.runtime.cost.toFixed(2)}</span>` : null}
    </div>
    <div class="activity">${s.status === "working" || s.busy ? html`<${SI} s="working" sm />` : null}${s.activity && !["done", "closed"].includes(s.status) ? html`<span class="txt" key=${s.activity.at}>${s.activity.text}</span><span style="color:var(--faint)">· ${ago(s.activity.at)}</span>` : html`<span style="color:var(--faint)">${s.items ? `${counts.done || 0} of ${s.items} done` : "No items yet"}</span>`}</div>
    ${s.items ? html`<div style="margin-top:10px"><${Progress} counts=${counts} big /></div>` : null}
    ${kids.length ? html`<div class="children">${kids.map((k) => html`<button class="chip" onClick=${() => go(k.id)}><${SI} s=${k.status} sm /><span class="ell">${k.title}</span></button>`)}</div>` : null}
    <${WorksWith} s=${s} />
  </div>`;
}

function Question({ a, d }) {
  const focus = S.askFocus === a.id, pick = S.askPick[a.id] || [], target = a.target && d.items[a.target];
  const note = useRef();
  return html`<div class="qcard ${focus ? "focus" : ""}" data-ask=${a.id} onClick=${() => !focus && set({ askFocus: a.id })}>
    <div class="qh"><${SI} s="input" sm />${a.kind === "permission" ? "Permission" : "Question"} · ${a.actor !== "agent" ? html`<span class="who">${a.actor}</span>` : null}${target ? html`on <b style="color:var(--dim)">${target.id} ${target.title}</b>` : "for the session"}<span style="margin-left:auto">${ago(a.at)}</span>${!focus ? html`<span>${K("a")} answer</span>` : null}</div>
    <div class="qt">${a.text}</div>
    ${a.detail ? html`<${Md} text=${a.detail} cls="qd" />` : null}
    ${a.options.length ? html`<div class="opts">${a.options.map((o, i) => html`<button class="opt ${pick.includes(o) ? "on" : ""}" onClick=${(e) => { e.stopPropagation(); if (!a.multi) return answer(a, [o], note.current?.value || ""); S.askPick[a.id] = pick.includes(o) ? pick.filter((x) => x !== o) : [...pick, o]; paint(); }}>${focus && i < 9 ? html`<kbd>${i + 1}</kbd>` : null}${o}</button>`)}</div>` : null}
    ${focus || !a.options.length ? html`<div class="note"><input ref=${note} placeholder=${a.options.length ? "Optional note" : "Your answer"} onKeyDown=${(e) => { if (e.key === "Enter") { e.preventDefault(); answer(a, pick, e.target.value); } if (e.key === "Escape") { e.target.blur(); set({ askFocus: null }); } }} /><button class="abtn primary" onClick=${(e) => { e.stopPropagation(); answer(a, pick, note.current?.value || ""); }}>Send ${K("↵")}</button></div>` : null}
    ${focus ? html`<div style="margin-top:8px;color:var(--faint);font-size:11.5px;display:flex;gap:14px"><${Hot} k=${a.options.length ? ["1", "…", String(Math.min(9, a.options.length))] : "Tab"}>${a.multi ? "toggle" : "choose"}<//><${Hot} k="Tab">note<//><${Hot} k="↵">send<//><${Hot} k="Esc">later<//></div>` : null}
  </div>`;
}

const linkLabel = (url) => {
  try {
    const u = new URL(url), m = u.pathname.match(/\/(pull|issues|commit)\/([0-9a-f]+)/);
    if (u.hostname === "github.com" && m) return m[1] === "commit" ? `commit ${m[2].slice(0, 7)}` : `${m[1] === "pull" ? "PR" : "issue"} #${m[2]}`;
    return u.hostname.replace(/^www\./, "");
  } catch { return url; }
};
// the small preview of a result everywhere: images as themselves, videos as their first frame with a play badge, others as a glyph
const thumbOf = (a, alt = "") => a.kind === "image" ? html`<img src=${blobUrl(a)} alt=${alt} loading="lazy" />`
  : a.kind === "video" ? html`<span class="vthumb"><video src=${blobUrl(a) + "#t=0.1"} preload="metadata" muted playsinline />${G.play}</span>`
  : a.kind === "link" ? G.link : a.kind === "pdf" ? G.pdf : G.file;
function ResultChip({ a, on, onOpen }) {
  const label = a.kind === "link" ? linkLabel(a.url) : a.caption || a.name;
  return html`<button class="rchip k-${a.kind} ${on ? "on" : ""}" title=${a.kind === "link" ? a.url : a.name} onClick=${(e) => { e.stopPropagation(); onOpen(); }}>
    ${thumbOf(a)}<span>${label}</span></button>`;
}

function ItemRow({ i, depth, d, s }) {
  const eff = d.effective[i.id], p = S.pulses[i.id], fresh = p && Date.now() - p.at < 1600, sel = S.sel[S.cur] === i.id;
  const comments = Object.values(d.comments).filter((c) => c.target === i.id).length, results = resultsFor(d, i.id), kids = childrenOf(s.id, i.id), ri = sel ? resIdx(i.id) : -1;
  const pick = () => select(i.id);
  const open = (n) => { S.sel[S.cur] = i.id; S.res[resKey(i.id)] = n; openSelected(); };
  const unread = d.unread.includes(i.id);
  return html`<div class="irow st-${eff} ${sel ? "sel" : ""} ${unread ? "unseen" : ""} ${fresh && p.kind === "new" ? "enter" : ""}" data-item=${i.id} style=${depth ? { marginLeft: `${depth * 22}px` } : null} onClick=${pick}>
    ${fresh ? html`<span class="flash ${p.kind === "new" ? "new" : ""}" key=${p.n} />` : null}
    <div class="ind"><${SI} s=${eff} /></div>
    <div class="body">
      <div class="tt"><span class="name">${i.title}</span>${!["agent", "user"].includes(i.actor) ? html`<span class="who"><span class="avatar sub" style="width:12px;height:12px;font-size:7px">${initials(i.actor)}</span>${i.actor}</span>` : null}</div>
      ${i.description ? html`<div class="desc">${i.description.split("\n").find((l) => l.trim()).replace(/\*\*|__|`|^#+\s*|^[-*]\s+/g, "")}</div>` : null}
      ${results.length || kids.length ? html`<div class="rrow">
        ${results.map((a, n) => html`<${ResultChip} key=${a.id} a=${a} on=${n === ri} onOpen=${() => open(n)} />`)}
        ${kids.map((k) => html`<button class="rchip k-session" title=${k.title} onClick=${(e) => { e.stopPropagation(); go(k.id); }}><${SI} s=${k.status} sm /><span>${k.title}</span></button>`)}
      </div>` : null}
    </div>
    <div class="tail">
      ${eff === "input" ? html`<span class="pill s-input">needs you</span>` : null}
      ${comments ? html`<span class="n">${G.chat}${comments}</span>` : null}
      ${unread ? html`<span class="udot" title="Changed since you last looked" />` : null}${i.archived && i.archived_by && i.archived_by !== "user" ? html`<span title="Archived by ${i.archived_by} after you had read it">archived by ${i.archived_by}</span>` : null}
      <span class="id">${i.id}</span><span>${ago(i.updated)}</span>
      ${depth ? null : html`<button class="arch" title=${i.archived ? "Bring back to current (e)" : "Archive this row and everything under it (e)"} onClick=${(e) => { e.stopPropagation(); archive(false, i.id); }}>${i.archived ? G.unarchive : G.archive}</button>`}
    </div>
  </div>`;
}

function relevant(d) {
  const rows = navRows(d), eff = d.effective;
  return [...rows].reverse().find((r) => eff[r.i.id] === "input")?.i.id || [...rows].reverse().find((r) => eff[r.i.id] === "working")?.i.id || rows[rows.length - 1]?.i.id;
}
// the board follows its newest rows while you are at the bottom. Scrolling up, or selecting an older row, stops following; a
// "Latest" button then shows how many rows arrived below and jumps back (as do ⌘↓ and Esc). Content that grows late, like
// thumbnails loading, keeps a following board at the bottom.
function Board({ s, d }) {
  const { archived, current } = boardRows(d), asks = openAsks(d), box = useRef(), stack = useRef(), pinned = useRef(true), sel = S.sel[S.cur];
  const [away, setAway] = useState(null), total = archived.length + current.length;  // away: the row count when following stopped
  const follow = () => { const b = box.current; pinned.current = true; setAway(null); if (b) b.scrollTop = b.scrollHeight; };
  useLayoutEffect(() => { const ro = new ResizeObserver(() => { const b = box.current; if (b && pinned.current) b.scrollTop = b.scrollHeight; }); ro.observe(stack.current); return () => ro.disconnect(); }, []);
  useLayoutEffect(() => { if (!sel) follow(); }, [sel]);
  followLatest = follow;
  let n;
  const rowsOf = (list) => html`<div class="items">${list.map(({ i, depth }) => html`<${ItemRow} key=${i.id} i=${i} depth=${depth} d=${d} s=${s} />`)}</div>`;
  useLayoutEffect(() => {
    const b = box.current, r = S.restore?.cur === s.id && S.restore.scroll;
    if (!b) return;
    pinned.current = !r || r.pinned;
    b.scrollTop = pinned.current ? b.scrollHeight : r.top;
    setAway(pinned.current ? null : total);
    if (r) S.restore.scroll = null;
  }, [s.id]);
  useLayoutEffect(() => { const b = box.current; if (b && pinned.current) b.scrollTop = b.scrollHeight; });
  useLayoutEffect(() => {
    const b = box.current, el = b?.querySelector(`[data-item="${sel}"]`);
    if (!el) return;
    const r = el.getBoundingClientRect(), v = b.getBoundingClientRect();
    if (r.top < v.top + 8 || r.bottom > v.bottom - 8) el.scrollIntoView({ block: "nearest" });
  }, [sel]);
  const onScroll = (e) => {
    const b = e.currentTarget, at = b.scrollHeight - b.scrollTop - b.clientHeight < 40;
    if (at !== pinned.current) { pinned.current = at; setAway(at ? null : total); }
  };
  const fresh = away === null ? 0 : Math.max(0, total - away);
  return html`<div class="boardwrap"><div class="scroll anchored" ref=${box} onScroll=${onScroll}><div class="stack" ref=${stack}>
    ${archived.length ? html`<section class="section archive"><h5>Archive · ${archived.length}<span class="hint">archived rows · ${K("e")} brings one back · new activity does too</span></h5>${rowsOf(archived)}</section>` : null}
    <section class="section current">
      <h5>Current · ${current.length}${(n = finishedRows(d).length) ? html`<button class="cleanup" onClick=${() => archive(true)}>${G.archive}Archive ${n} finished ${K("⇧", "E")}</button>` : null}<span style="margin-left:auto;display:flex;gap:12px;text-transform:none;letter-spacing:0;font-weight:500"><${Hot} k=${["j", "k"]}>row<//><${Hot} k=${["←", "→"]}>result<//><${Hot} k="Space">preview<//><${Hot} k="c">comment<//><${Hot} k="s">status<//><${Hot} k="e">archive<//>${sel ? html`<${Hot} k="Esc">session<//>` : null}</span></h5>
      ${rowsOf(current)}
      ${!current.length && archived.length ? html`<div class="empty" style="padding:14px 8px">Nothing current. Archived rows are above.</div>` : null}
      ${!current.length && !archived.length ? html`<div class="empty">The agent has not published items yet.<br />Items appear here as it works: <code>convos orchestrate add "…"</code></div>` : null}
    </section>
    ${asks.length ? html`<section class="section"><h5><${SI} s="input" sm />Needs you · ${asks.length}</h5>${asks.map((a) => html`<${Question} key=${a.id} a=${a} d=${d} />`)}</section>` : null}
  </div></div>
  ${away !== null ? html`<button class="latest ${fresh ? "fresh" : ""}" onClick=${() => { move(1e9); follow(); }}>${fresh ? `${fresh} new ${fresh === 1 ? "row" : "rows"} below` : "Latest"}<span class="arrow">↓</span>${K("⌘", "↓")}</button>` : null}
  </div>`;
}

function Result({ a, target, i, on }) {
  const [text, setText] = useState(null);
  useEffect(() => { if (a.kind === "markdown" || a.kind === "text") fetch(blobUrl(a)).then((r) => r.text()).then(setText).catch(() => setText("(unavailable)")); }, [a.hash]);
  if (a.kind === "link") return html`<a class="res ${on ? "focus" : ""}" href=${a.url} target="_blank" rel="noopener noreferrer"><div class="thumb">${G.link}</div><div class="cap" title=${a.url}>${a.caption || a.name}</div></a>`;
  if (a.kind === "markdown" || a.kind === "text") return html`<div class="res wide ${on ? "focus" : ""}"><div class="cap" style="display:flex;gap:6px;cursor:pointer" onClick=${() => openResult(target, i)}>${a.name}${a.caption ? html`<span style="color:var(--faint)">· ${a.caption}</span>` : null}</div><div class="mdbox">${text === null ? "…" : a.kind === "markdown" ? html`<${Md} text=${text} />` : html`<pre style="margin:0;white-space:pre-wrap;font:11.5px var(--mono)">${text}</pre>`}</div></div>`;
  return html`<button class="res ${on ? "focus" : ""}" onClick=${() => openResult(target, i)}><div class="thumb">${thumbOf(a, a.name)}</div><div class="cap" title=${a.name}>${a.caption || a.name}</div></button>`;
}
function Results({ d, target }) {
  const list = resultsFor(d, target), on = Math.min(list.length - 1, resIdx(target));
  if (!list.length) return null;
  return html`<section class="dsec"><h5>Results · ${list.length}<span style="margin-left:auto;display:flex;gap:12px;text-transform:none;letter-spacing:0;font-weight:500"><${Hot} k=${["←", "→"]}>select<//><${Hot} k="Space">preview<//></span></h5><div class="results">${list.map((a, i) => html`<${Result} key=${a.id} a=${a} target=${target} i=${i} on=${i === on} />`)}</div></section>`;
}

function deliveryNote(c, s) {
  if (c.sending) return html`<span class="state q">sending…</span>`;
  if (c.delivered) return html`<span class="state d">✓ delivered</span>`;
  return html`<span class="state q">${s.origin === "gui" ? "queued" : s.live ? "delivering…" : "waits for the agent"}</span>`;
}
function Thread({ d, s, target }) {
  const list = commentsFor(d, target), byId = Object.fromEntries(Object.values(d.comments).map((c) => [c.id, c]));
  if (!list.length) return html`<div class="empty" style="padding:16px 8px">No messages yet. ${K(target ? "c" : "⇧", ...(target ? [] : ["C"]))} to write one.</div>`;
  return html`<div class="thread">${list.map((c) => html`<div class="msg ${c.actor === "user" ? "user" : ""}" key=${c.id + c.at} data-c=${c.id}>
    <div class="mh">${c.actor !== "user" ? html`<span class="avatar ${c.actor === "agent" ? "agent" : c.actor === "orchestrate" ? "sys" : "sub"}">${initials(c.actor)}</span>` : null}<span>${c.actor === "user" ? "you" : c.actor}</span><span>· ${ago(c.at)}</span>${c.actor === "user" ? deliveryNote(c, s) : null}</div>
    <div class="bub">${c.reply_to && byId[c.reply_to] ? html`<div class="quote">${byId[c.reply_to].text}</div>` : null}${c.text ? html`<${Md} text=${c.text} />` : null}
      ${c.files?.length ? html`<div class="cimgs">${c.files.map((f, n) => html`<button key=${n} title=${f.name} onClick=${() => set({ overlay: { kind: "look", files: c.files, i: n, from: c } })}><img src=${blobUrl(f)} alt=${f.name} loading="lazy" /></button>`)}</div>` : null}</div>
  </div>`)}</div>`;
}

// what # can point at from the composer: rows (newest first), results, open questions, other sessions
function refCandidates(q) {
  const d = det(); if (!d) return [];
  const all = [...navRows(d).reverse().map(({ i }) => ({ id: i.id, label: i.title, icon: d.effective[i.id] })), ...Object.values(d.attachments).reverse().map((a) => ({ id: a.id, label: a.caption || a.name || a.url, kind: a.kind })),
    ...openAsks(d).map((a) => ({ id: a.id, label: a.text, icon: "input" })), ...S.sessions.filter((x) => x.id !== S.cur && !x.closed).map((x) => ({ id: x.id, label: x.title, icon: x.status }))];
  const t = q.toLowerCase();
  return all.filter((c) => !t || c.id.startsWith(t) || c.label.toLowerCase().includes(t)).slice(0, 8);
}
function Composer({ s, target, label }) {
  const key = `${s.id}:${target || ""}`, ref = useRef(), [focus, setFocus] = useState(false), [pick, setPick] = useState(null);
  const pending = S.attach[key] || [], picker = useRef();
  const submit = () => { const text = (S.drafts[key] || "").trim(); if (!text && !pending.length) return; S.drafts[key] = ""; S.attach[key] = []; ref.current.value = ""; grow(); sendComment(text, target, ref.current.parentElement, pending); ref.current.blur(); };
  const paste = (e) => { const fs = [...(e.clipboardData?.files || [])]; if (fs.some((f) => f.type.startsWith("image/"))) { e.preventDefault(); attachImages(key, fs); } };
  const grow = () => { const t = ref.current; if (t) { t.style.height = "auto"; t.style.height = Math.min(160, t.scrollHeight) + "px"; } };
  useEffect(() => { if (ref.current) { ref.current.value = S.drafts[key] || ""; grow(); } setPick(null); }, [key]);
  const cands = pick ? refCandidates(pick.q) : [];
  const watch = (t) => { const m = t.value.slice(0, t.selectionStart).match(/(^|\s)#([\w-]*)$/); setPick(m ? { q: m[2], at: t.selectionStart - m[2].length - 1, i: 0 } : null); };
  const insert = (c) => {
    const t = ref.current, v = t.value, end = pick.at + 1 + pick.q.length, text = `${c.id} `;
    t.value = v.slice(0, pick.at) + text + v.slice(end);
    t.selectionStart = t.selectionEnd = pick.at + text.length;
    S.drafts[key] = t.value; setPick(null); grow(); t.focus();
  };
  const keys = (e) => {
    if (pick && cands.length) {
      const move = { ArrowDown: 1, ArrowUp: -1 }[e.key];
      if (move) { e.preventDefault(); return setPick({ ...pick, i: (pick.i + move + cands.length) % cands.length }); }
      if (e.key === "Enter" || e.key === "Tab") { e.preventDefault(); return insert(cands[pick.i]); }
      if (e.key === "Escape") { e.preventDefault(); return setPick(null); }
    }
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); submit(); }  // a bare Enter is a new line
    if (e.key === "Enter" && e.altKey) { e.preventDefault(); document.execCommand("insertText", false, "\n"); }  // the browser does nothing for Option Enter
    if (e.key === "Escape") { e.preventDefault(); e.target.blur(); }
    if (e.key === "Backspace" && !e.target.value && pending.length) { e.preventDefault(); unattach(key, pending.length - 1); }
  };
  return html`<div class="composer ${S.drop ? "over" : ""}">
    ${pick && cands.length ? html`<div class="refpick">${cands.map((c, n) => html`<div class=${n === pick.i ? "on" : ""} onMouseDown=${(e) => { e.preventDefault(); insert(c); }}>${c.icon ? html`<${SI} s=${c.icon} sm />` : html`<span class="g">${G[c.kind === "pdf" ? "pdf" : c.kind === "link" ? "link" : "file"]}</span>`}<code>${c.id}</code><span class="ell">${c.label}</span></div>`)}<div class="hint">${K("↑", "↓")} pick · ${K("↵")} insert · ${K("Esc")}</div></div>` : null}
    <div class="target">to <span class="to" key=${key}>${label}</span><span style="margin-left:auto;display:flex;gap:10px"><${Hot} k="#">reference<//><${Hot} k="c">row<//><${Hot} k=${["⇧", "C"]}>session<//></span></div>
    ${pending.length ? html`<div class="pend">${pending.map((f, n) => html`<span class="pimg" key=${f.src}><img src=${f.src} alt=${f.name} title=${f.name} /><button title="Remove (⌫ in an empty composer)" onMouseDown=${(e) => { e.preventDefault(); unattach(key, n); }}>×</button></span>`)}</div>` : null}
    <div class="box ${focus ? "focus" : ""}">
      <button class="clip" title="Attach images: or paste them (⌘V), or drop them here" onMouseDown=${(e) => e.preventDefault()} onClick=${() => picker.current.click()}>${G.clip}</button>
      <input ref=${picker} type="file" hidden multiple accept="image/png,image/jpeg,image/gif,image/webp,image/avif" onChange=${(e) => { attachImages(key, e.target.files); e.target.value = ""; }} />
      <textarea ref=${ref} rows="1" placeholder=${target ? "Comment on this item: ask, redirect, add context… # to reference, ⌘V an image" : "Message the session… # to reference, ⌘V an image"} onFocus=${() => setFocus(true)} onBlur=${() => { setFocus(false); setPick(null); }}
        onInput=${(e) => { S.drafts[key] = e.target.value; grow(); watch(e.target); }}
        onKeyDown=${keys} onPaste=${paste} />
      <span class="send">${K("⌘", "↵")} send · ${K("↵")} line · ${K("Esc")}</span>
    </div>
  </div>`;
}

function Detail({ s, d }) {
  const it = selected(), pinned = useRef(true), view = `${s.id}:${it?.id || ""}`;
  useLayoutEffect(() => { pinned.current = !it; }, [view]);
  useLayoutEffect(() => { const b = document.querySelector(".dscroll"); if (b && pinned.current && !(S.restore?.cur === s.id && S.restore.dscroll != null)) b.scrollTop = b.scrollHeight; });
  const onScroll = (e) => { const b = e.currentTarget; pinned.current = !it && b.scrollHeight - b.scrollTop - b.clientHeight < 60; };
  useLayoutEffect(() => { const r = S.restore; if (r?.cur === s.id && r.dscroll != null) { const el = document.querySelector(".dscroll"); if (el) { el.scrollTop = r.dscroll; r.dscroll = null; } } });
  if (!it) {
    const asks = openAsks(d).filter((a) => !a.target);
    return html`<aside class="detail" onDragOver=${dropOver} onDragLeave=${dropLeave} onDrop=${(e) => dropOn(e, `${s.id}:`)}>
      ${S.drop ? html`<div class="dropzone">Drop images to attach them to a message in the session chat</div>` : null}
      <div class="dhead"><div class="lbl">Session · ${s.origin === "gui" ? "started here" : "joined"} · ${ago(s.created)}<span style="margin-left:auto"><${Hot} k=${["j", "k"]}>pick a row<//></span></div><h2>${s.title}</h2>
        <${SessionInfo} s=${s} d=${d} /></div>
      <div class="dscroll" onScroll=${onScroll}>
        ${d.prompt ? html`<section class="dsec"><h5>Starting prompt</h5><div class="bub" style="padding:9px 11px;border-radius:10px;border:1px solid var(--line);background:var(--panel2)"><${Md} text=${d.prompt} /></div></section>` : null}
        ${asks.length ? html`<section class="dsec"><h5>Questions</h5>${asks.map((a) => html`<${Question} key=${a.id} a=${a} d=${d} />`)}</section>` : null}
        <${Results} d=${d} target=${null} />
        <section class="dsec"><h5>Session chat</h5><${Thread} d=${d} s=${s} target=${null} /></section>
      </div>
      <${Composer} s=${s} target=${null} label=${`session · ${s.title}`} />
    </aside>`;
  }
  const eff = d.effective[it.id], asks = openAsks(d).filter((a) => a.target === it.id), kids = childrenOf(s.id, it.id), p = S.pulses[it.id];
  return html`<aside class="detail" onDragOver=${dropOver} onDragLeave=${dropLeave} onDrop=${(e) => dropOn(e, `${s.id}:${it.id}`)}>
    ${S.drop ? html`<div class="dropzone">Drop images to attach them to a comment on ${it.id}</div>` : null}
    <div class="dhead ${p && Date.now() - p.at < 1600 ? "receive" : ""}" key=${it.id}>
      <div class="lbl"><code>${it.id}</code>${!["agent", "user"].includes(it.actor) ? html`<span class="who">${it.actor}</span>` : null}<span>created ${ago(it.created)}</span>${it.parent ? html`<span>· under ${it.parent}</span>` : null}<span style="margin-left:auto"><${Hot} k="Esc">session<//></span></div>
      <h2>${it.title}</h2>
      <div class="row"><button onClick=${() => set({ overlay: { kind: "status", item: it.id } })}><${Pill} s=${eff} /></button><${Hot} k="s">change<//>
        <button class="ghostbtn" style="margin-left:auto" onClick=${() => archive(false, it.id)}>${d.items[rootOf(d, it.id)].archived ? G.unarchive : G.archive}${d.items[rootOf(d, it.id)].archived ? "Bring back" : "Archive"}${it.parent ? ` ${rootOf(d, it.id)}` : ""}</button><${Hot} k="e" /></div>
    </div>
    <div class="dscroll">
      ${it.description ? html`<section class="dsec"><${Md} text=${it.description} /></section>` : null}
      ${asks.length ? html`<section class="dsec"><h5>Needs you</h5>${asks.map((a) => html`<${Question} key=${a.id} a=${a} d=${d} />`)}</section>` : null}
      <${Results} d=${d} target=${it.id} />
      ${kids.length ? html`<section class="dsec"><h5>Child sessions</h5><div class="children" style="margin-top:0">${kids.map((k) => html`<button class="chip" onClick=${() => go(k.id)}><${SI} s=${k.status} sm /><span class="ell">${k.title}</span></button>`)}</div></section>` : null}
      <section class="dsec"><h5>History</h5><div class="timeline">${it.history.map((h) => html`<div class="ev"><${SI} s=${h.status} sm />${STATUS[h.status]}<span style="color:var(--faint)">by ${h.actor === "user" ? "you" : h.actor}</span><time>${ago(h.at)}</time></div>`)}</div></section>
      <section class="dsec"><h5>Thread</h5><${Thread} d=${d} s=${s} target=${it.id} /></section>
    </div>
    <${Composer} s=${s} target=${it.id} label=${`${it.id} · ${it.title}`} />
  </aside>`;
}

function Center() {
  const s = cur(), d = det();
  if (!s) return html`<main class="center"><div class="empty" style="margin:auto">${S.sessions.length ? html`Press ${K("1")} to open a session.` : html`Press ${K("n")} to start a session, or type <code>/orchestrate</code> in any agent session to bring it here.`}</div></main>`;
  if (!d) return html`<main class="center"><div class="empty" style="margin:auto">Loading…</div></main>`;
  return html`<main class="center"><${SessionBar} s=${s} /><${Board} s=${s} d=${d} /></main>`;
}

// ---------------------------------------------------------------- overlays
function Modal({ title, onClose, children, footer, wide }) {
  return html`<div class="scrim" onMouseDown=${(e) => e.target === e.currentTarget && onClose()}><div class="modal" style=${wide ? "width:min(760px,94vw)" : ""}><header><span class="grow">${title}</span><${Hot} k="Esc">close<//></header>${children}${footer ? html`<footer>${footer}</footer>` : null}</div></div>`;
}

function Seg({ options, value, onChange, label }) {
  return html`<div class="seg" tabIndex="0" aria-label=${label} onKeyDown=${(e) => { const i = options.indexOf(value); if (e.key === "ArrowRight" || e.key === "l") { e.preventDefault(); onChange(options[(i + 1) % options.length]); } if (e.key === "ArrowLeft" || e.key === "h") { e.preventDefault(); onChange(options[(i - 1 + options.length) % options.length]); } }}>
    ${options.map((o) => html`<button type="button" tabIndex="-1" class=${o === value ? "on" : ""} onClick=${() => onChange(o)}>${o}</button>`)}
  </div>`;
}

function NewSession({ parent }) {
  const prov = S.meta.providers["claude-code"], ps = parent && S.sessions.find((s) => s.id === parent.split(":")[0]);
  const [f, setF] = useState({ cwd: home(ps?.cwd || cur()?.cwd || S.meta.recent[0] || S.meta.home), title: "", model: prov.models[0], effort: "high", permission: "default", skills: ["orchestrate"], prompt: "" });
  const [sugg, setSugg] = useState([]), [si, setSi] = useState(-1), [skills, setSkills] = useState([]), [ki, setKi] = useState(0), [busy, setBusy] = useState(false);
  const up = (patch) => setF((x) => ({ ...x, ...patch }));
  const abs = (p) => p.startsWith("~") ? S.meta.home + p.slice(1) : p;
  useEffect(() => { const t = setTimeout(() => api.get(`/api/skills?cwd=${encodeURIComponent(abs(f.cwd))}`).then(setSkills).catch(() => {}), 150); return () => clearTimeout(t); }, [f.cwd]);
  const complete = (v) => api.get(`/api/dirs?prefix=${encodeURIComponent(abs(v))}`).then((r) => { setSugg(r.map(home)); setSi(-1); }).catch(() => {});
  const submit = async () => {
    if (!f.prompt.trim()) return toast("Write a starting prompt", true);
    setBusy(true);
    try { const r = await api.post("/api/spawn", { ...f, cwd: abs(f.cwd), parent }); set({ overlay: null }); await loadSessions(); go(r.session); toast("Session started"); }
    catch (e) { fail(e); setBusy(false); }
  };
  useKeys((e) => {
    if (e.key === "Escape") { e.preventDefault(); set({ overlay: null }); return true; }
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); submit(); return true; }
    return false;
  });
  const toggle = (name) => name !== "orchestrate" && up({ skills: f.skills.includes(name) ? f.skills.filter((x) => x !== name) : [...f.skills, name] });
  return html`<${Modal} title=${parent ? html`New child session <span style="color:var(--faint);font-weight:400">of ${ps?.title || parent}${parent.includes(":") ? ` › ${parent.split(":")[1]}` : ""}</span>` : "New session"} onClose=${() => set({ overlay: null })}
    footer=${html`<span class="grow">Runs headless; you steer it from this board. ${K("Tab")} next field</span><button class="abtn" onClick=${() => set({ overlay: null })}>Cancel</button><button class="abtn primary" disabled=${busy} onClick=${submit}>${busy ? "Starting…" : "Start"} ${K("⌘", "↵")}</button>`}>
    <div class="mbody">
      <div class="field"><label>Path<span class="hint">${K("Tab")} complete · ${K("↑", "↓")} pick</span></label>
        <input type="text" class="mono" autoFocus value=${f.cwd} onInput=${(e) => { up({ cwd: e.target.value }); complete(e.target.value); }}
          onKeyDown=${(e) => {
            if (e.key === "ArrowDown" && sugg.length) { e.preventDefault(); setSi((si + 1) % sugg.length); }
            else if (e.key === "ArrowUp" && sugg.length) { e.preventDefault(); setSi((si - 1 + sugg.length) % sugg.length); }
            else if (e.key === "Tab" && !e.shiftKey && sugg.length) { e.preventDefault(); const v = sugg[Math.max(0, si)] + "/"; up({ cwd: v }); complete(v); }
            else if (e.key === "Enter" && si >= 0) { e.preventDefault(); up({ cwd: sugg[si] }); setSugg([]); }
          }} onBlur=${() => setTimeout(() => setSugg([]), 120)} />
        ${sugg.length ? html`<div class="sugg">${sugg.map((p, i) => html`<div class=${i === si ? "on" : ""} onMouseDown=${() => { up({ cwd: p }); setSugg([]); }}>${p}</div>`)}</div>` : null}
      </div>
      <div class="field"><label>Model<span class="hint">${K("←", "→")}</span></label><${Seg} label="Model" options=${prov.models} value=${f.model} onChange=${(model) => up({ model })} /></div>
      <div style="display:flex;gap:18px;flex-wrap:wrap">
        <div class="field"><label>Effort<span class="hint">${K("←", "→")}</span></label><${Seg} label="Effort" options=${prov.efforts} value=${f.effort} onChange=${(effort) => up({ effort })} /></div>
        <div class="field"><label>Permissions<span class="hint">${K("←", "→")}</span></label><${Seg} label="Permissions" options=${prov.permissions} value=${f.permission} onChange=${(permission) => up({ permission })} /></div>
      </div>
      <div class="field"><label>Skills<span class="hint">${K("↑", "↓")} move · ${K("Space")} toggle · orchestrate is always on</span></label>
        <div class="skills" tabIndex="0" onKeyDown=${(e) => {
          if (e.key === "ArrowDown" || e.key === "j") { e.preventDefault(); setKi(Math.min(skills.length - 1, ki + 1)); }
          if (e.key === "ArrowUp" || e.key === "k") { e.preventDefault(); setKi(Math.max(0, ki - 1)); }
          if (e.key === " " && skills[ki]) { e.preventDefault(); toggle(skills[ki].name); }
        }}>
          ${skills.map((sk, i) => html`<div class="skill ${i === ki ? "on" : ""} ${f.skills.includes(sk.name) || sk.name === "orchestrate" ? "checked" : ""} ${sk.name === "orchestrate" ? "locked" : ""}" onClick=${() => { setKi(i); toggle(sk.name); }}>
            <span class="box" /><div style="min-width:0"><div class="nm">${sk.name}${sk.scope === "project" ? html`<span class="tag">project</span>` : null}</div><div class="ds">${sk.description}</div></div></div>`)}
        </div>
      </div>
      <div class="field"><label>Title<span class="hint">optional · defaults to the prompt's first line</span></label><input type="text" value=${f.title} onInput=${(e) => up({ title: e.target.value })} /></div>
      <div class="field"><label>Starting prompt<span class="hint">${K("⌘", "↵")} start</span></label><textarea value=${f.prompt} placeholder="What should this session do?" onInput=${(e) => up({ prompt: e.target.value })} /></div>
    </div>
  <//>`;
}

function Palette() {
  const [q, setQ] = useState(""), [i, setI] = useState(0);
  const all = [...ordered().map(({ s }, n) => ({ group: "Sessions", label: s.title, keys: n < 9 ? [String(n + 1)] : [], icon: s.status, run: () => go(s.id) })), ...commands().filter((c) => c.run && c.when !== false && c.when !== null && c.when !== 0 && c.when !== "")];
  const list = all.filter((c) => !q || c.label.toLowerCase().includes(q.toLowerCase()) || c.group.toLowerCase().includes(q.toLowerCase()));
  const run = (c) => { set({ overlay: null }); setTimeout(() => c.run(), 0); };
  useKeys((e) => {
    if (e.key === "Escape") { e.preventDefault(); set({ overlay: null }); return true; }
    if (e.key === "ArrowDown" || (e.ctrlKey && e.key === "n")) { e.preventDefault(); setI(Math.min(list.length - 1, i + 1)); return true; }
    if (e.key === "ArrowUp" || (e.ctrlKey && e.key === "p")) { e.preventDefault(); setI(Math.max(0, i - 1)); return true; }
    if (e.key === "Enter" && list[i]) { e.preventDefault(); run(list[i]); return true; }
    return false;
  });
  let last = null;
  return html`<div class="scrim" onMouseDown=${(e) => e.target === e.currentTarget && set({ overlay: null })}><div class="modal palette">
    <input autoFocus placeholder="Type a command or session…" value=${q} onInput=${(e) => { setQ(e.target.value); setI(0); }} />
    <div class="list">${list.map((c, n) => { const head = c.group !== last ? html`<div class="group">${c.group}</div>` : null; last = c.group; return html`${head}<div class="pi ${n === i ? "on" : ""}" onMouseMove=${() => setI(n)} onClick=${() => run(c)}>${c.icon ? html`<${SI} s=${c.icon} sm />` : null}<span class="grow">${c.label}</span>${c.keys?.map((k) => html`<kbd>${k}</kbd>`)}</div>`; })}${!list.length ? html`<div class="empty">Nothing matches.</div>` : null}</div>
  </div></div>`;
}

function Help() {
  useKeys((e) => { if (e.key === "Escape" || e.key === "?") { e.preventDefault(); set({ overlay: null }); return true; } return false; });
  const extra = [{ group: "Talk", keys: ["1", "…", "9"], label: "Choose an option (while answering)" }, { group: "Talk", keys: ["⌘", "↵"], label: "Send comment" }, { group: "Talk", keys: ["↵"], label: "New line in a comment (also ⌥↵, ⇧↵)" }, { group: "Talk", keys: ["↵"], label: "Send answer" }, { group: "New session", keys: ["Tab"], label: "Next field · complete path" }, { group: "New session", keys: ["←", "→"], label: "Change model, effort, permissions" }, { group: "New session", keys: ["Space"], label: "Toggle skill" }, { group: "New session", keys: ["⌘", "↵"], label: "Start" }];
  const groups = [...commands(), ...extra].reduce((g, c) => ((g[c.group] ||= []).push(c), g), {});
  return html`<${Modal} title="Keyboard" wide onClose=${() => set({ overlay: null })}><div class="mbody"><div class="helpgrid">
    ${Object.entries(groups).map(([g, cs]) => html`<h5>${g}</h5>${cs.map((c) => html`<div class="hk"><span>${c.label}</span><span>${c.keys.map((k) => html`<kbd>${k}</kbd>`)}</span></div>`)}`)}
  </div></div><//>`;
}

function StatusPicker({ item }) {
  const d = det(), it = d?.items[item], [i, setI] = useState(Math.max(0, ITEM_STATUSES.indexOf(it?.status)));
  const pick = (st) => { set({ overlay: null }); if (st !== it.status) act({ kind: "set", item, status: st }); };
  useKeys((e) => {
    if (e.key === "Escape") { e.preventDefault(); set({ overlay: null }); return true; }
    if (/^[1-6]$/.test(e.key)) { e.preventDefault(); pick(ITEM_STATUSES[+e.key - 1]); return true; }
    if (e.key === "j" || e.key === "ArrowDown") { e.preventDefault(); setI((i + 1) % 6); return true; }
    if (e.key === "k" || e.key === "ArrowUp") { e.preventDefault(); setI((i + 5) % 6); return true; }
    if (e.key === "Enter") { e.preventDefault(); pick(ITEM_STATUSES[i]); return true; }
    return true;
  });
  if (!it) return null;
  const row = document.querySelector(`.center [data-item="${item}"]`)?.getBoundingClientRect();
  return html`<div class="scrim" style="background:transparent;backdrop-filter:none" onMouseDown=${(e) => e.target === e.currentTarget && set({ overlay: null })}>
    <div class="picker" style=${{ left: `${(row?.left || 300) + 40}px`, top: `${Math.min(innerHeight - 260, (row?.bottom || 200) + 4)}px` }}>
      ${ITEM_STATUSES.map((st, n) => html`<button class=${n === i ? "on" : ""} onClick=${() => pick(st)}><${SI} s=${st} /><span class="grow">${STATUS[st]}</span><kbd>${n + 1}</kbd></button>`)}
    </div></div>`;
}

// image viewer: fits the image to the stage; zoom (+ − 0, pinch, ⌘-scroll) around the pointer, click toggles fit and 100%, pan by
// drag or scroll, and a minimap that shows and moves the visible part whenever the image is larger than the view. Every frame is
// drawn into a canvas from the full-resolution original at device pixels, so zooming never shows a lower-resolution copy.
function ImageView({ a, api }) {
  const box = useRef(), cv = useRef(), drag = useRef(null), dragged = useRef(false), [pic, setPic] = useState(null), [size, setSize] = useState(null), [v, setV] = useState({ z: 1, x: 0, y: 0 }), [grab, setGrab] = useState(false);
  useLayoutEffect(() => { const el = box.current, ro = new ResizeObserver(() => setSize({ w: el.clientWidth, h: el.clientHeight })); ro.observe(el); return () => ro.disconnect(); }, []);
  useEffect(() => { let live = true; const im = new Image(); im.src = blobUrl(a); im.decode().then(() => live && setPic(im.naturalWidth ? im : false), () => live && setPic(false)); return () => { live = false; }; }, [a.hash]);
  const nat = pic && { w: pic.naturalWidth, h: pic.naturalHeight }, ready = nat && size, fit = ready ? Math.min(size.w / nat.w, size.h / nat.h, 1) : 1, s = fit * v.z, w = ready ? nat.w * s : 0, h = ready ? nat.h * s : 0;
  const cx = (x, w) => w <= size.w ? (size.w - w) / 2 : Math.min(0, Math.max(size.w - w, x)), cy = (y, h) => h <= size.h ? (size.h - h) / 2 : Math.min(0, Math.max(size.h - h, y));
  const x = ready ? cx(v.x, w) : 0, y = ready ? cy(v.y, h) : 0, zoomed = ready && (w > size.w + 1 || h > size.h + 1), top = Math.max(1, 6 / fit);
  // the visible part of the image, in view pixels relative to the image's top-left corner (the canvas covers exactly this)
  const vis = ready ? { l: Math.max(0, -x), t: Math.max(0, -y), r: Math.min(w, size.w - x), b: Math.min(h, size.h - y) } : null;
  const zoomAt = (factor, px = size.w / 2, py = size.h / 2) => ready && setV((o) => {
    const z = Math.min(top, Math.max(1, o.z * factor)), s0 = fit * o.z, s1 = fit * z, x0 = cx(o.x, nat.w * s0), y0 = cy(o.y, nat.h * s0);
    return { z, x: px - (px - x0) * s1 / s0, y: py - (py - y0) * s1 / s0 };
  });
  const fitView = () => setV({ z: 1, x: 0, y: 0 });
  const pan = (dx, dy) => setV((o) => ({ ...o, x: x + dx, y: y + dy }));
  useEffect(() => { api.current = { zoom: (f) => zoomAt(f), fit: fitView, pan }; });
  useEffect(() => () => { api.current = null; }, []);
  useLayoutEffect(() => {
    const c = cv.current;
    if (!c || !vis) return;
    const r = devicePixelRatio || 1, W = Math.max(1, Math.round((vis.r - vis.l) * r)), H = Math.max(1, Math.round((vis.b - vis.t) * r));
    if (c.width !== W || c.height !== H) { c.width = W; c.height = H; }
    const g = c.getContext("2d");
    g.imageSmoothingEnabled = s < 2;
    g.imageSmoothingQuality = "high";
    g.clearRect(0, 0, W, H);
    g.drawImage(pic, vis.l / s, vis.t / s, (vis.r - vis.l) / s, (vis.b - vis.t) / s, 0, 0, W, H);
  });
  const at = (e) => { const r = box.current.getBoundingClientRect(); return [e.clientX - r.left, e.clientY - r.top]; };
  const wheel = (e) => { if (!ready) return; e.preventDefault(); if (e.ctrlKey || e.metaKey) zoomAt(Math.exp(-e.deltaY * 0.01), ...at(e)); else if (zoomed) pan(-e.deltaX, -e.deltaY); };
  const down = (e) => { if (e.button) return; dragged.current = false; drag.current = zoomed ? { px: e.clientX, py: e.clientY, x, y } : null; if (zoomed) { e.currentTarget.setPointerCapture(e.pointerId); setGrab(true); } };
  const dragTo = (e) => { const g = drag.current; if (!g) return; dragged.current ||= Math.abs(e.clientX - g.px) + Math.abs(e.clientY - g.py) > 3; setV((o) => ({ ...o, x: g.x + e.clientX - g.px, y: g.y + e.clientY - g.py })); };
  const up = () => { drag.current = null; setGrab(false); };
  // a click zooms in to 100% (or 200% for images smaller than the view) and anything zoomed back to fit; a drag never counts as a click
  const inward = ready && (s < 0.995 || fit === 1 && v.z === 1), click = (e) => { if (!ready || e.detail > 1 || dragged.current) return; if (inward) zoomAt(s < 0.995 ? 1 / s : 2, ...at(e)); else fitView(); };
  const m = ready ? Math.min(176 / nat.w, 124 / nat.h) : 0, centre = (e) => { const r = e.currentTarget.getBoundingClientRect(); setV((o) => ({ ...o, x: size.w / 2 - (e.clientX - r.left) / m * s, y: size.h / 2 - (e.clientY - r.top) / m * s })); };
  return html`<div class="viewer ${zoomed ? "zoomed" : inward ? "" : "out"} ${grab ? "grabbing" : ""}" ref=${box} onWheel=${wheel} onPointerDown=${down} onPointerMove=${dragTo} onPointerUp=${up} onPointerCancel=${up} onClick=${click}>
    ${vis ? html`<canvas ref=${cv} class=${zoomed ? "" : "framed"} role="img" aria-label=${a.name} style=${{ left: `${x + vis.l}px`, top: `${y + vis.t}px`, width: `${vis.r - vis.l}px`, height: `${vis.b - vis.t}px` }} />`
      : html`<div class="vload">${pic === false ? "This image could not be decoded" : "Loading full resolution…"}</div>`}
    ${ready ? html`<div class="zoomlbl">${Math.round(s * 100)}%${v.z > 1 ? "" : " · fit"}<span>${K("+", "−")} zoom · ${K("0")} fit · click ${inward ? (s < 0.995 ? "100%" : "200%") : "fit"}${zoomed ? " · drag to pan" : ""}</span></div>` : null}
    ${zoomed ? html`<div class="mini" style=${{ width: `${nat.w * m}px`, height: `${nat.h * m}px` }} onClick=${(e) => e.stopPropagation()} onPointerDown=${(e) => { e.stopPropagation(); centre(e); e.currentTarget.setPointerCapture(e.pointerId); }} onPointerMove=${(e) => e.buttons && centre(e)}>
      <img src=${blobUrl(a)} alt="" draggable="false" /><span class="vp" style=${{ left: `${vis.l / s * m}px`, top: `${vis.t / s * m}px`, width: `${(vis.r - vis.l) / s * m}px`, height: `${(vis.b - vis.t) / s * m}px` }} /></div>` : null}
  </div>`;
}

// video: starts playing when it opens (muted only if the browser refuses sound), native controls; p play/pause, m mute, ⇧← ⇧→ seek 5 s
function VideoView({ a, api }) {
  const ref = useRef(), [bad, setBad] = useState(false);
  useEffect(() => { const v = ref.current; v.play().catch(() => { v.muted = true; return v.play(); }).catch(() => {}); }, [a.hash]);  // refused autoplay just leaves it paused
  useEffect(() => { const v = ref.current; api.current = { toggle: () => v.paused ? v.play() : v.pause(), mute: () => { v.muted = !v.muted; }, seek: (dt) => { v.currentTime = Math.max(0, Math.min(v.duration || 0, v.currentTime + dt)); } }; });
  useEffect(() => () => { api.current = null; }, []);
  return html`<div class="viewer video">${bad ? html`<div class="vload">This video format does not play in the browser. ${K("↵")} opens the file.</div>` : null}
    <video ref=${ref} src=${blobUrl(a)} controls playsinline tabindex="-1" onError=${() => setBad(true)} style=${bad ? "display:none" : ""} />
    <div class="zoomlbl">${K("p")} play/pause · ${K("m")} mute · ${K("⇧", "←")} ${K("⇧", "→")} 5 s</div></div>`;
}

// Quick Look: previews whatever is selected and follows the selection; ← → results, ↑ ↓ rows, Space or Esc closes
function Look() {
  const o = S.overlay, own = o.files, d = det(), s = cur(), it = selected(), list = own || (d ? resultsFor(d, it?.id) : []), i = Math.min(list.length - 1, own ? o.i : resIdx(it?.id)), a = list[i];
  const step = (n) => own ? set({ overlay: { ...o, i: Math.max(0, Math.min(list.length - 1, o.i + n)) } }) : moveResult(n), row = (n) => own || move(n), pick = (n) => own ? set({ overlay: { ...o, i: n } }) : (S.res[resKey(it?.id)] = n, paint());
  const [text, setText] = useState(null), zoom = useRef(null);
  useEffect(() => { if (it && !own) markSeen([it.id]); }, [it?.id]);
  useEffect(() => { setText(null); if (a && (a.kind === "markdown" || a.kind === "text")) fetch(blobUrl(a)).then((r) => r.text()).then(setText).catch(() => setText("(unavailable)")); }, [a?.id]);
  useKeys((e) => {
    const k = e.key, z = zoom.current;
    if (z?.seek && e.shiftKey && (k === "ArrowLeft" || k === "ArrowRight")) { e.preventDefault(); z.seek(k === "ArrowLeft" ? -5 : 5); return true; }
    if (z?.pan && e.shiftKey && k.startsWith("Arrow")) { e.preventDefault(); z.pan(...{ ArrowLeft: [120, 0], ArrowRight: [-120, 0], ArrowUp: [0, 120], ArrowDown: [0, -120] }[k]); return true; }
    const run = { "+": () => z?.zoom?.(1.25), "=": () => z?.zoom?.(1.25), "-": () => z?.zoom?.(0.8), "0": () => z?.fit?.(), p: () => z?.toggle?.(), m: () => z?.mute?.(), " ": () => set({ overlay: null }), Escape: () => set({ overlay: null }), ArrowRight: () => step(1), l: () => step(1), ArrowLeft: () => step(-1), h: () => step(-1),
      ArrowDown: () => row(1), j: () => row(1), ArrowUp: () => row(-1), k: () => row(-1), Enter: () => a && (a.kind === "link" ? window.open(a.url, "_blank", "noopener,noreferrer") : window.open(blobUrl(a), "_blank", "noopener")) }[k];
    if (run) { e.preventDefault(); run(); }
    return true;
  });
  useLayoutEffect(() => { document.querySelector(".lightbox .card.on")?.scrollIntoView({ block: "nearest", inline: "nearest" }); });
  if (!d || (!a && !it)) return null;
  const close = (e) => e.target === e.currentTarget && set({ overlay: null }), title = (r) => r.caption || (r.kind === "link" ? linkLabel(r.url) : r.name);
  const thumb = (r) => thumbOf(own ? { ...r, kind: "image" } : r);
  return html`<div class="lightbox" onMouseDown=${close}>
    <header><div class="grow ttl"><div class="t1">${a ? title(a) : it.title}</div><div class="t2">${own ? `${o.from.id === "…" ? "your comment" : `${o.from.id} · ${o.from.actor === "user" ? "your" : o.from.actor + "'s"} comment`} on ${o.from.target ? `${o.from.target} · ${d?.items[o.from.target]?.title}` : "the session"}` : it ? `${it.id} · ${it.title}` : `Session · ${s.title}`}${a && a.caption && a.kind !== "link" ? ` · ${a.name}` : ""}</div></div>
      ${list.length ? html`<span>${i + 1} / ${list.length}</span><${Hot} k=${["←", "→"]}>${own ? "image" : "result"}<//>` : null}${own ? null : html`<${Hot} k=${["↑", "↓"]}>row<//>`}${a ? html`<${Hot} k="↵">open<//>` : null}<${Hot} k="Space">close<//></header>
    <div class="stage" onMouseDown=${close} key=${a?.id || a?.hash || a?.src || it?.id}>
      ${!a ? html`<div class="doc"><div style="display:flex;gap:10px;align-items:center;margin-bottom:12px"><${Pill} s=${d.effective[it.id]} /><span style="color:var(--faint)">${it.id} · updated ${ago(it.updated)}</span></div><h2 style="margin:0 0 10px">${it.title}</h2>${it.description ? html`<${Md} text=${it.description} />` : html`<p style="color:var(--faint)">No description and no results on this row.</p>`}</div>`
        : a.kind === "image" || own ? html`<${ImageView} key=${a.id || a.hash || a.src} a=${a} api=${zoom} />`
        : a.kind === "video" ? html`<${VideoView} key=${a.id} a=${a} api=${zoom} />`
        : a.kind === "pdf" ? html`<iframe src=${blobUrl(a)} title=${a.name} />`
        : a.kind === "link" ? html`<div class="doc" style="height:auto"><div style="color:var(--faint);margin-bottom:6px">${linkLabel(a.url)}</div><h2 style="margin:0 0 10px">${a.caption || a.name}</h2><a href=${a.url} target="_blank" rel="noopener noreferrer" style="word-break:break-all">${a.url}</a><p style="color:var(--faint);margin-top:14px">${K("↵")} opens it in your browser.</p></div>`
        : a.kind === "markdown" ? html`<div class="doc">${text === null ? "…" : html`<${Md} text=${text} />`}</div>`
        : a.kind === "text" ? html`<div class="doc"><pre style="white-space:pre-wrap;font:12px var(--mono)">${text}</pre></div>`
        : html`<div class="doc" style="height:auto"><h2 style="margin:0 0 10px">${a.name}</h2><a href=${blobUrl(a)} download=${a.name}>Download</a></div>`}
    </div>
    ${list.length ? html`<div class="strip" onMouseDown=${close}>${list.map((r, n) => html`<button class="card ${n === i ? "on" : ""}" key=${r.id || n} onClick=${() => pick(n)}><div class="th">${thumb(r)}</div><div class="tl">${title(r)}</div></button>`)}</div>` : null}
  </div>`;
}

function Overlay() {
  const o = S.overlay;
  if (!o) return null;
  if (o.kind === "new") return html`<${NewSession} parent=${o.parent} />`;
  if (o.kind === "palette") return html`<${Palette} />`;
  if (o.kind === "help") return html`<${Help} />`;
  if (o.kind === "status") return html`<${StatusPicker} item=${o.item} />`;
  if (o.kind === "look") return html`<${Look} />`;
  return null;
}

function Footer() {
  const it = selected(), d = det();
  return html`<footer class="foot">
    <${Hot} k=${["1", "…", "9"]}>sessions<//><${Hot} k=${["j", "k"]}>items<//><${Hot} k="c">${it ? `comment ${it.id}` : "comment session"}<//><${Hot} k=${["⇧", "C"]}>session chat<//>${it ? html`<${Hot} k="Esc">session<//>` : null}<${Hot} k="Space">preview<//>
    ${d && openAsks(d).length ? html`<${Hot} k="a">answer<//>` : null}<${Hot} k="n">new<//><${Hot} k=${["⌘", "K"]}>commands<//><${Hot} k="?">all keys<//>
    <span class="conn"><span class="dot ${S.conn ? "on" : ""}" />${S.conn ? "live" : "reconnecting…"}</span>
  </footer>`;
}

function App() {
  const [, force] = useState(0);
  paint = () => force((x) => x + 1);
  const needing = S.sessions.filter((s) => s.status === "input" && !s.closed);
  useEffect(() => { document.title = needing.length ? `(${needing.length}) Orchestrate` : "Orchestrate"; });
  return html`<div class="shell">
    <div class="top">
      <div class="brand">${G.logo}Orchestrate</div>
      ${needing.length ? html`<button class="attention" onClick=${() => needYou(needing.find((s) => s.id === S.cur)?.id || needing[0].id)}><${SI} s="input" sm />${needing.length} ${needing.length === 1 ? "session needs" : "sessions need"} you</button>` : null}
      <span class="spacer" />
      <button class="btn" onClick=${() => set({ overlay: { kind: "palette" } })}>Commands ${K("⌘", "K")}</button>
      <button class="btn" onClick=${() => set({ overlay: { kind: "new" } })}>New session ${K("n")}</button>
      <button class="btn" onClick=${() => set({ overlay: { kind: "help" } })}>${K("?")}</button>
    </div>
    <div class="main"><${Sidebar} />${(() => { const s = cur(), d = det(); return html`<${Center} />${s && d ? html`<${Detail} s=${s} d=${d} />` : html`<aside class="detail" />`}`; })()}</div>
    <${Footer} />
    <${Overlay} />
    ${S.toast ? html`<div class="toast ${S.toast.err ? "err" : ""}" key=${S.toast.n}>${S.toast.err ? html`<${SI} s="failed" sm />` : html`<${SI} s="done" sm />`}${S.toast.text}</div>` : null}
  </div>`;
}

// ---------------------------------------------------------------- boot
(async function boot() {
  const theme = store("theme"); if (theme) document.documentElement.dataset.theme = theme;
  render(html`<${App} />`, document.getElementById("app"));
  addEventListener("keydown", onKey);
  try {
    [S.meta] = await Promise.all([api.get("/api/meta"), loadSessions()]);
    S.sessions.forEach((s) => { S.prev[s.id] = s.status; });
    const r = restored();
    if (r) Object.assign(S, { sel: r.sel || {}, res: r.res || {}, closed: r.closed, drafts: r.drafts || {}, restore: r });
    const want = r?.cur || location.hash.slice(1), o = ordered();
    go(S.sessions.find((s) => s.id === want)?.id || S.sessions.find((s) => s.status === "input" && !s.closed)?.id || [...o].sort((a, b) => b.s.updated.localeCompare(a.s.updated))[0]?.s.id);
    connect();
    const rooms = () => api.get("/api/collab").then((c) => set({ collab: c })).catch(() => {});
    rooms();
    setInterval(rooms, 20000);
    if (r?.composer) setTimeout(focusComposer, 300);
    if (r) setTimeout(() => toast("Orchestrate updated"), 400);
  } catch (e) { fail(e); }
  setInterval(paint, 30000);
})();
