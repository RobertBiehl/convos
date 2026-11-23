"""Minimal stdio MCP server for headless sessions: permission prompts become questions on the board.

Claude Code calls `permission` (via --permission-prompt-tool) whenever a tool needs approval. The call
blocks until the user answers in the IDE; "Allow always" is remembered for that tool in this session.
"""
import json, os, re, subprocess, sys
from pathlib import Path
from . import store, transcripts

TOOL = dict(name="permission", description="Ask the user in Convos Orchestrate to allow or deny a tool call.",
            inputSchema=dict(type="object", properties=dict(tool_name=dict(type="string"), input=dict(type="object"), tool_use_id=dict(type="string")), required=["tool_name", "input"]))
OPTIONS = ["Allow", "Allow always", "Deny"]

def preview(name, data):
    if name == "Bash": return f"```sh\n{data.get('command', '')}\n```\n{data.get('description', '')}"
    if "file_path" in data: return f"`{data['file_path']}`\n\n```\n{json.dumps({k: v for k, v in data.items() if k != 'file_path'}, indent=1)[:3000]}\n```"
    return f"```json\n{json.dumps(data, indent=1)[:4000]}\n```"

WRITERS, FILES = ("Bash", "Write", "Edit", "MultiEdit", "NotebookEdit"), ("file_path", "notebook_path")
GUARDED = re.compile(r"""(?:^|[/\s'"=])(?:\.git|hooks)(?:$|[/\s'"])|\.claude""")

def worktrees(cwd):
    """Every checkout of the repo cwd lies in, its main one included, as git lists them; none outside a repo."""
    out = subprocess.run(["git", "-C", cwd, "worktree", "list", "--porcelain"], capture_output=True, text=True).stdout
    return [line.removeprefix("worktree ") for line in out.splitlines() if line.startswith("worktree ")]

def shell_cwd(meta, call):
    """The folder a Claude Code Bash call runs in: the shell keeps its last cd, and the transcript records that folder on the
    entry that makes the call, which is written before the permission prompt comes."""
    if not call or not (path := transcripts.transcript(meta)): return None
    entries = (e for line in reversed(transcripts.tail(path, 1 << 23)) if call.encode() in line and (e := transcripts.entry(line)))
    return next((e.get("cwd") for e in entries if e.get("type") == "assistant"), None)

def in_worktree(meta, name, data, call=None):
    """In a bypassPermissions session Claude Code still asks for some calls; the user had orchestrate allow those (q11, q13 on
    o5619965e7ab:i60) that stay inside one git worktree of the session's repo, wherever it lies, and name no .git, hooks or .claude
    path there. A command must start by cd'ing into the worktree, or run where the shell already is inside one (c271); any other
    place on disk, the home folder, a bare or backward cd, or a way up out of it, and the call still comes to the user."""
    if meta.get("permission") != "bypassPermissions" or name not in WRITERS: return False
    text = data.get("command") if name == "Bash" else next((data[k] for k in FILES if k in data), None)
    if not isinstance(text, str) or not (roots := worktrees(meta["cwd"])): return False
    root = "(?:" + "|".join(re.escape(r) for r in sorted(roots, key=len, reverse=True)) + r""")(?=[/\s'"]|$)"""
    if name != "Bash" and not re.match(rf"{root}/", text): return False
    if name == "Bash" and not re.match(rf"""\s*cd\s+['"]?{root}""", text) and not ((here := shell_cwd(meta, call)) and re.match(root, here)): return False
    rest = re.sub(root, "WT", text).replace("/dev/null", "")
    tops = "|".join(re.escape(x) for x in os.listdir("/"))
    return not (GUARDED.search(rest) or re.search(rf"(?<![\w.$-])/(?:{tops})(?:/|\b)", rest) or re.search(r"""(?:^|[\s'"=:(])~/|\bcd(?:\s+-)?\s*(?:$|[;&|)])|\bcd\s+~(?:\s|$)|\$\{?(?:HOME|OLDPWD)\b|\bpopd\b|(?:^|[/\s'"])\.\.(?:[/\s'"`;&|)]|$)""", rest, re.M))

def decide(sid, args):
    name, data = args["tool_name"], args.get("input", {})
    meta = store.load(sid)["session"]
    if name in meta.get("allow", []): return dict(behavior="allow", updatedInput=data)
    if in_worktree(meta, name, data, args.get("tool_use_id")):
        with (store.sdir(sid)/"allowed.jsonl").open("a") as log: log.write(json.dumps(dict(at=store.now(), tool=name, input=data)) + "\n")
        return dict(behavior="allow", updatedInput=data)
    s, e = store.emit(sid, "ask", "agent", f"Allow {name}?", target=None, options=OPTIONS, multi=False, ask_kind="permission", detail=preview(name, data))
    s, answer = store.wait_for(sid, lambda s: s["asks"][e["ask"]]["answer"] or s["asks"][e["ask"]]["cancelled"], 6*3600)
    if not answer:
        store.emit(sid, "cancel", "orchestrate", ask=e["ask"])
        return dict(behavior="deny", message="No answer from the user in orchestrate within 6 hours.")
    store.emit(sid, "deliver", "orchestrate", ids=[e["ask"]])
    choice = (answer.get("choices") or ["Deny"])[0] if isinstance(answer, dict) else "Deny"
    if choice == "Allow always": store.emit(sid, "meta", "user", changes=dict(allow=sorted({*s["session"].get("allow", []), name})))
    if choice.startswith("Allow"): return dict(behavior="allow", updatedInput=data)
    return dict(behavior="deny", message=f"The user denied {name} in orchestrate." + (f" Note: {answer['text']}" if answer.get("text") else ""))

def main():
    sid = os.environ["CONVOS_ORCHESTRATE_SESSION"]
    def reply(rid, result=None, error=None):
        sys.stdout.write(json.dumps(dict(jsonrpc="2.0", id=rid, **(dict(error=error) if error else dict(result=result)))) + "\n")
        sys.stdout.flush()
    for line in sys.stdin:
        if not line.strip(): continue
        req = json.loads(line)
        method, rid = req.get("method"), req.get("id")
        if rid is None: continue
        if method == "initialize": reply(rid, dict(protocolVersion=req["params"]["protocolVersion"], capabilities=dict(tools={}), serverInfo=dict(name="orchestrate", version="1")))
        elif method == "tools/list": reply(rid, dict(tools=[TOOL]))
        elif method == "tools/call":
            try: reply(rid, dict(content=[dict(type="text", text=json.dumps(decide(sid, req["params"]["arguments"])))]))
            except (ValueError, KeyError, OSError) as error: reply(rid, dict(content=[dict(type="text", text=json.dumps(dict(behavior="deny", message=f"orchestrate error: {error}")))]))
        elif method == "ping": reply(rid, {})
        else: reply(rid, error=dict(code=-32601, message=f"Unknown method {method}"))

if __name__ == "__main__":
    from ai_convos import cli
    main()
