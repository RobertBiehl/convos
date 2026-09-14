"""Frozen pre-attribution-fix parser functions from 1b27ccfb6547236362638ba08a0ee1c1efa8e317."""
import hashlib, json, re, typer
from pathlib import Path
from datetime import datetime


def gen_id(source: str, oid: str) -> str: return hashlib.sha256(f"{source}:{oid}".encode()).hexdigest()[:16]

def ts_from_iso(t): return datetime.fromisoformat(t.replace("Z", "+00:00")) if t else None

def extract_content(content) -> dict:
    if isinstance(content, str): return {"text": content, "thinking": None, "tools": [], "attachments": []}
    if not isinstance(content, list): return {"text": "", "thinking": None, "tools": [], "attachments": []}
    blocks = [b for b in content if isinstance(b, dict)]
    return {
        "text": "\n".join(b.get("text", "") or b.get("thinking", "") if b.get("type") in ("text", None) else "" for b in blocks).strip() or
                "\n".join(str(b) for b in content if isinstance(b, str)).strip(),
        "thinking": "\n".join(b["thinking"] for b in blocks if b.get("type") == "thinking" and b.get("thinking")).strip() or None,
        "tools": [{"name": b["name"], "input": b.get("input", {}), "id": b.get("id")} for b in blocks if b.get("type") == "tool_use"] +
                 [{"id": b.get("tool_use_id"), "output": b.get("content", "")} for b in blocks if b.get("type") == "tool_result"],
        "attachments": [{"filename": b.get("name", b.get("file_name")), "mime_type": b.get("content_type", b.get("file_type")),
                        "size": b.get("size", b.get("file_size")), "url": b.get("asset_pointer", b.get("url"))}
                       for b in blocks if b.get("type") in ("image_asset_pointer", "file") or b.get("content_type") in ("image_asset_pointer", "file")]
    }

def log_parse_error(context: str, err: Exception):
    if not os.environ.get("CONVOS_PARSE_LOG"):
        return
    typer.echo(f"  parse error ({context}): {type(err).__name__}: {err}", err=True)

def load_jsonl(path: Path) -> list[dict]:
    out = []
    for i, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip(): continue
        try:
            out.append(json.loads(line))
        except Exception as e:
            log_parse_error(f"jsonl {path} line {i}", e)
    return out

def parse_claude_code_session(jsonl: Path) -> dict:
    events = load_jsonl(jsonl)
    if not events: return None
    cid, src = gen_id("claude-code", str(jsonl)), "claude-code"
    timestamps = [ts_from_iso(e["timestamp"]) for e in events if "timestamp" in e]
    system = next((e for e in events if e.get("type") == "system"), {})
    msg_events = [(i, e) for i, e in enumerate(events) if "message" in e]

    def make_msg(idx, i, e):
        c = extract_content(e["message"].get("content", e["message"].get("text", "")))
        return dict(id=gen_id(src, f"{cid}:{idx}"), conversation_id=cid, role=e["type"],
                   content=c["text"], thinking=c["thinking"], created_at=ts_from_iso(e.get("timestamp")),
                   model="claude" if e["type"] == "assistant" else None, metadata="{}")

    def make_tools(idx, i, e):
        c, ts = extract_content(e["message"].get("content", [])), ts_from_iso(e.get("timestamp"))
        mid = gen_id(src, f"{cid}:{idx}")
        return [dict(id=gen_id(src, f"tool:{cid}:{idx}:{j}"), message_id=mid, tool_name=t.get("name", t.get("id")),
                    input=json.dumps(t.get("input", {})), output=json.dumps(t.get("output", "")) if "output" in t else "{}",
                    status="complete" if "output" in t else "pending", duration_ms=None, created_at=ts) for j, t in enumerate(c["tools"])]

    def make_edits(idx, i, e):
        c, ts = extract_content(e["message"].get("content", [])), ts_from_iso(e.get("timestamp"))
        mid = gen_id(src, f"{cid}:{idx}")
        return [dict(id=gen_id(src, f"edit:{cid}:{idx}:{j}"), message_id=mid, file_path=t["input"]["file_path"],
                    edit_type=t["name"].lower(), content=t["input"].get("content") or t["input"].get("new_string", ""), created_at=ts,
                    old_content=t["input"].get("old_string"))
               for j, t in enumerate(c["tools"]) if t.get("name") in ("Write", "Edit", "MultiEdit") and t.get("input", {}).get("file_path")]

    msgs = [make_msg(idx, i, e) for idx, (i, e) in enumerate(msg_events) if extract_content(e["message"].get("content", ""))["text"]]
    if not msgs: return None
    return {
        "conv": dict(id=cid, source=src, title=f"{jsonl.parent.name.replace('-Users-', '~/').replace('-', '/')} ({jsonl.stem[:8]})",
                    created_at=timestamps[0] if timestamps else None, updated_at=timestamps[-1] if timestamps else None,
                    model="claude", cwd=system.get("cwd"), git_branch=system.get("gitBranch"), project_id=None,
                    metadata=json.dumps({"session_id": jsonl.stem})),
        "msgs": msgs,
        "tools": [t for idx, (i, e) in enumerate(msg_events) for t in make_tools(idx, i, e)],
        "edits": [ed for idx, (i, e) in enumerate(msg_events) for ed in make_edits(idx, i, e)]}

def parse_codex_session(jsonl: Path) -> dict | None:
    events = load_jsonl(jsonl)
    if not events: return None
    cid, src = gen_id("codex", str(jsonl)), "codex"
    timestamps = [ts_from_iso(e["timestamp"]) for e in events if "timestamp" in e]
    meta = next((e["payload"] for e in events if e.get("type") == "session_meta"), {})
    items = [(i, e["payload"]) for i, e in enumerate(events) if e.get("type") == "response_item" and "payload" in e]

    def extract_msg_text(p):
        return "\n".join(b["text"] for b in p.get("content", []) if isinstance(b, dict) and b.get("type") in ("input_text", "output_text", "text") and b.get("text"))
    def norm_args(p):
        return json.loads(a) if isinstance((a := p.get("arguments", {})), str) else a

    msgs = [dict(id=gen_id(src, f"{cid}:{i}"), conversation_id=cid, role=p["role"], content=text.strip(),
                thinking=None, created_at=timestamps[i] if i < len(timestamps) else None, model=None, metadata="{}")
           for i, p in items if p.get("type") == "message" and p.get("role") not in ("developer", "system") and (text := extract_msg_text(p))]
    if not msgs: return None

    tools = [dict(id=gen_id(src, f"tool:{cid}:{i}"), message_id=gen_id(src, f"{cid}:{i}"), tool_name=p["name"],
                 input=json.dumps(args), output="{}", status="pending", duration_ms=None,
                 created_at=timestamps[i] if i < len(timestamps) else None)
            for i, p in items if p.get("type") == "function_call" and (args := norm_args(p))] + \
           [dict(id=gen_id(src, f"toolout:{cid}:{i}"), message_id=gen_id(src, f"{cid}:{i}"), tool_name=p.get("call_id"),
                 input="{}", output=json.dumps(p.get("output", "")), status="complete", duration_ms=None,
                 created_at=timestamps[i] if i < len(timestamps) else None)
            for i, p in items if p.get("type") == "function_call_output"]

    edits = [dict(id=gen_id(src, f"edit:{cid}:{i}:{j}"), message_id=gen_id(src, f"{cid}:{i}"),
                 file_path=m.group(1), edit_type="shell", content=cmd,
                 created_at=timestamps[i] if i < len(timestamps) else None, old_content=None)
            for i, p in items if p.get("type") == "function_call" and p.get("name") == "shell"
            and (args := norm_args(p)) and (c := args.get("command")) and (cmd := " ".join(c) if isinstance(c, list) else c)
            for j, pat in enumerate([r'(?:cat|echo).*[>].*?([^\s>]+)', r'(?:sed|awk).*?([^\s]+)$'])
            if (m := re.search(pat, cmd))]

    return {
        "conv": dict(id=cid, source=src, title=meta.get("cwd") or jsonl.stem,
                    created_at=timestamps[0] if timestamps else None, updated_at=timestamps[-1] if timestamps else None,
                    model=meta.get("model_provider", "openai"), cwd=meta.get("cwd"), git_branch=None, project_id=None,
                    metadata=json.dumps({"cli_version": meta.get("cli_version"), "session_id": jsonl.stem})),
        "msgs": msgs, "tools": tools, "edits": edits}
