"""Minimal stdio MCP server for headless sessions: permission prompts become questions on the board.

Claude Code calls `permission` (via --permission-prompt-tool) whenever a tool needs approval. The call
blocks until the user answers in the IDE; "Allow always" is remembered for that tool in this session.
"""
import json, os, sys
from . import store

TOOL = dict(name="permission", description="Ask the user in Convos Orchestrate to allow or deny a tool call.",
            inputSchema=dict(type="object", properties=dict(tool_name=dict(type="string"), input=dict(type="object"), tool_use_id=dict(type="string")), required=["tool_name", "input"]))
OPTIONS = ["Allow", "Allow always", "Deny"]

def preview(name, data):
    if name == "Bash": return f"```sh\n{data.get('command', '')}\n```\n{data.get('description', '')}"
    if "file_path" in data: return f"`{data['file_path']}`\n\n```\n{json.dumps({k: v for k, v in data.items() if k != 'file_path'}, indent=1)[:3000]}\n```"
    return f"```json\n{json.dumps(data, indent=1)[:4000]}\n```"

def decide(sid, args):
    name, data = args["tool_name"], args.get("input", {})
    if name in store.load(sid)["session"].get("allow", []): return dict(behavior="allow", updatedInput=data)
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
