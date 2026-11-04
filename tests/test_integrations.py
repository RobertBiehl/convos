"""Integration tests for API fetchers - validates API schemas haven't changed."""

import pytest, json, os
from pathlib import Path
from unittest.mock import patch, MagicMock

# API response schemas for testing (moved here when the dead Playwright browser.py was removed)
EXPECTED_SCHEMAS = {
    "chatgpt_conversations": {"type": "object", "required": ["items", "total"],
        "properties": {"items": {"type": "array"}, "total": {"type": "integer"}}},
    "chatgpt_conversation": {"type": "object", "required": ["mapping"],
        "properties": {"mapping": {"type": "object"}}},
    "claude_organizations": {"type": "array", "items": {"type": "object", "required": ["uuid"]}},
    "claude_conversations": {"type": "array", "items": {"type": "object", "required": ["uuid"]}},
    "claude_conversation": {"type": "object", "required": ["uuid", "chat_messages"],
        "properties": {"chat_messages": {"type": "array"}}},
}

def validate_schema(data, schema_name):
    schema = EXPECTED_SCHEMAS.get(schema_name)
    if not schema:
        return False, f"Unknown schema: {schema_name}"
    if schema["type"] == "array":
        if not isinstance(data, list):
            return False, f"Expected array, got {type(data).__name__}"
        if data and "items" in schema and "required" in schema["items"]:
            for i, item in enumerate(data[:3]):
                for field in schema["items"]["required"]:
                    if field not in item:
                        return False, f"Item {i} missing required field: {field}"
        return True, "OK"
    if schema["type"] == "object":
        if not isinstance(data, dict):
            return False, f"Expected object, got {type(data).__name__}"
        for field in schema.get("required", []):
            if field not in data:
                return False, f"Missing required field: {field}"
        return True, "OK"
    return False, f"Unknown schema type: {schema['type']}"

def assert_live_import(result, source, tmp_path):
    from ai_convos import cli
    assert result.convs and all(c["source"] == source for c in result.convs)
    with cli._core(tmp_path/"convos.db", ready=True) as db:
        cli.upsert(db, result)
        assert db.execute("SELECT COUNT(*) FROM conversations WHERE source=?", [source]).fetchone()[0] == len(result.convs)
        assert db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == len(result.msgs)

# Skip all integration tests if SKIP_INTEGRATION is set
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION", "0") == "1",
    reason="Integration tests skipped via SKIP_INTEGRATION=1"
)

@pytest.fixture
def mock_cookies():
    return {"sessionKey": "test", "cf_clearance": "test"}

# ---- ChatGPT API Tests ----

class TestChatGPTAPI:
    """Tests for ChatGPT backend API structure."""

    @pytest.mark.integration
    def test_conversations_list_schema(self, mock_cookies):
        """Verify /backend-api/conversations returns expected schema."""

        # Mock response matching expected schema
        mock_response = {
            "items": [
                {"id": "abc123", "title": "Test", "create_time": 1704067200, "update_time": 1704067200}
            ],
            "total": 1,
            "limit": 100,
            "offset": 0
        }
        valid, msg = validate_schema(mock_response, "chatgpt_conversations")
        assert valid, f"Schema validation failed: {msg}"

    @pytest.mark.integration
    def test_conversations_list_missing_items(self):
        """Detect if API removes 'items' field."""

        broken_response = {"total": 1}  # missing items
        valid, msg = validate_schema(broken_response, "chatgpt_conversations")
        assert not valid
        assert "items" in msg

    @pytest.mark.integration
    def test_conversation_detail_schema(self):
        """Verify /backend-api/conversation/{id} returns expected schema."""

        mock_response = {
            "mapping": {
                "node1": {"message": {"author": {"role": "user"}, "content": {"parts": ["Hello"]}}}
            }
        }
        valid, msg = validate_schema(mock_response, "chatgpt_conversation")
        assert valid, f"Schema validation failed: {msg}"

    @pytest.mark.integration
    @pytest.mark.skipif(os.environ.get("CHATGPT_TEST") != "1", reason="Requires real cookies and CHATGPT_TEST=1")
    def test_live_chatgpt_api(self, tmp_path):
        """Fetch one ChatGPT conversation and ingest it into an isolated archive."""
        from ai_convos.cli import fetch_chatgpt
        assert_live_import(fetch_chatgpt("safari", limit=1), "chatgpt", tmp_path)

    def test_fetch_chatgpt_surfaces_total_failure(self, monkeypatch):
        """A fully failed fetch raises instead of returning empty (no silent 'success')."""
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda b: ["Default"])
        monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: (_ for _ in ()).throw(ValueError("HTTP Error 401: Unauthorized")))
        with pytest.raises(Exception):
            cli.fetch_chatgpt(browser="chrome")

    def test_fetch_chatgpt_dates_from_detail(self, monkeypatch):
        """Conv dates come from the detail endpoint (epoch) even when the list item omits them."""
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda b: [None])
        monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: ({}, "https://chatgpt.com"))
        monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {})
        def fake(url, *a, **k):
            if "/conversations?offset=0" in url: return {"items": [{"id": "c1", "title": "T", "create_time": None, "update_time": None}], "total": 1}
            if "/conversations?offset=" in url: return {"items": [], "total": 1}
            return {"create_time": 1709294400, "update_time": 1709294500, "current_node":"n1",
                    "mapping": {"n1": {"message": {"author": {"role": "assistant"}, "content": {"parts": ["hi"]}, "create_time": 1709294400,"status":"finished_successfully"}}}}
        monkeypatch.setattr(cli, "fetch_json", fake)
        r = cli.fetch_chatgpt("safari")
        assert len(r.convs) == 1
        assert r.convs[0]["created_at"] == cli.ts_from_epoch(1709294400)
        assert r.convs[0]["updated_at"] == cli.ts_from_epoch(1709294500)
        assert json.loads(r.convs[0]["metadata"])["remote_complete"] is True

    def test_fetch_chatgpt_dates_fall_back_to_messages(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: [None]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: ({}, "https://chatgpt.com")); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {})
        def fake(url, *a, **k):
            if "offset=0" in url: return {"items":[{"id":"c1","create_time":None,"update_time":None}], "total":1}
            if "/conversations?" in url: return {"items":[], "total":1}
            return {"mapping":{"a":{"parent":None,"message":{"author":{"role":"user"},"content":{"parts":["a"]},"create_time":100}},"b":{"parent":"a","message":{"author":{"role":"assistant"},"content":{"parts":["b"]},"create_time":"1970-01-01T00:03:20Z"}}}}
        monkeypatch.setattr(cli, "fetch_json", fake); conv = cli.fetch_chatgpt("safari").convs[0]
        assert conv["created_at"] == cli.ts_from_epoch(100) and conv["updated_at"] == cli.ts_from_iso("1970-01-01T00:03:20Z")

    def test_fetch_chatgpt_rejects_partial_detail_failure(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: [None]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: ({}, "https://chatgpt.com")); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {}); monkeypatch.setattr(cli.time,"sleep",lambda _:None); saved = []; items = [{"id":f"ok{i}","update_time":300-i} for i in range(20)]+[{"id":"bad","update_time":200}]
        def fake(url, *a, **k):
            if "offset=0" in url: return {"items":items, "total":21}
            if "/conversations?" in url: return {"items":[], "total":21}
            if url.endswith("/bad"): raise TimeoutError("detail timeout")
            return {"mapping":{}}
        monkeypatch.setattr(cli, "fetch_json", fake)
        with pytest.raises(ValueError, match="detail timeout"): cli.fetch_chatgpt("safari", sink=lambda r: saved.append([c["id"] for c in r.convs]))
        assert saved == [[cli.gen_id("chatgpt", f"ok{i}") for i in range(20)]]

    def test_fetch_chatgpt_paces_before_observed_limit(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); sleeps, clock, details = [], [0], []; items = [{"id":str(i),"update_time":i} for i in range(21)]
        def fake(url,*a,**k):
            k["before_request"]()
            if "/conversations?" in url: return {"items":items,"total":len(items)}
            details.append(url.rsplit("/",1)[-1])
            return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); monkeypatch.setattr(cli.time,"monotonic",lambda:clock[0]); monkeypatch.setattr(cli.time,"sleep",lambda n:(sleeps.append(n),clock.__setitem__(0,clock[0]+n))); cli.fetch_chatgpt("safari")
        assert details==[str(i) for i in range(21)] and sleeps==pytest.approx([1.875,1.875])

    @pytest.mark.parametrize("shared,expected", [(True,2),(False,0)])
    def test_fetch_chatgpt_rate_budget_is_account_scoped(self, monkeypatch, shared, expected):
        from ai_convos import cli
        sleeps, clock = [], [0]; monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda _b,_h,p:({},f"https://chatgpt.com/{p}")); monkeypatch.setattr(cli,"chatgpt_headers",lambda _c,b,*a,**k:{"ChatGPT-Account-ID":"same" if shared else b})
        def fake(url,*a,**k):
            k["before_request"](); profile = url.split("chatgpt.com/",1)[1].split("/",1)[0]
            return {"items":[{"id":f"{profile}-{i}","update_time":i} for i in range(10)],"total":10} if "/conversations?" in url else {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); monkeypatch.setattr(cli.time,"monotonic",lambda:clock[0]); monkeypatch.setattr(cli.time,"sleep",lambda n:(sleeps.append(n),clock.__setitem__(0,clock[0]+n))); cli.fetch_chatgpt("safari",profiles=["A","B"])
        assert len(sleeps)==expected

    def test_fetch_chatgpt_skips_unrelated_chrome_profile(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: ["Good", "Unused"]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda _, __, p: ({}, "https://chatgpt.com") if p == "Good" else (_ for _ in ()).throw(ValueError("no cookies"))); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {})
        def fake(url, *a, **k):
            if "offset=0" in url: return {"items":[{"id":"ok"}], "total":1}
            if "/conversations?" in url: return {"items":[], "total":1}
            return {"mapping":{}}
        monkeypatch.setattr(cli, "fetch_json", fake)
        assert [c["id"] for c in cli.fetch_chatgpt("chrome").convs] == [cli.gen_id("chatgpt", "ok")]

    def test_fetch_chatgpt_details_only_new_or_updated_conversations(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: [None]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: ({}, "https://chatgpt.com")); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {}); details = []
        def fake(url, *a, **k):
            if "offset=0" in url: return {"items":[{"id":"changed","update_time":200},{"id":"same","update_time":100}], "total":6}
            if "offset=1" in url: return {"items":[{"id":"same","update_time":100},{"id":"older","update_time":100},{"id":"missing-time","update_time":None},{"id":"null-stored","update_time":100},{"id":"new","update_time":None}], "total":6}
            if "/conversations?" in url: return {"items":[], "total":6}
            details.append(url.rsplit("/", 1)[-1]); return {"mapping":{}}
        monkeypatch.setattr(cli, "fetch_json", fake); known = {cli.gen_id("chatgpt", x):(t if t else None) for x, t in (("same",100),("older",150),("changed",150),("missing-time",150),("null-stored",None))}
        assert len(cli.fetch_chatgpt("safari", known=known).convs) == 4 and set(details) == {"changed", "missing-time", "null-stored", "new"}
        details.clear(); assert len(cli.fetch_chatgpt("safari", known={}).convs) == 6 and set(details) == {"same", "older", "changed", "missing-time", "null-stored", "new"}

    def test_fetch_chatgpt_tolerates_timestamp_skew_only_for_legacy_rows(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); details = []
        def fake(url,*a,**k):
            if "/conversations?" in url: return {"items":[{"id":"legacy","update_time":103},{"id":"exact","update_time":103}],"total":2}
            details.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); known = {cli.gen_id("chatgpt",x):100 for x in ("legacy","exact")}; cli.fetch_chatgpt("safari",known=known,legacy={cli.gen_id("chatgpt","legacy")})
        assert details==["exact"]

    def test_fetch_chatgpt_completed_frontier_covers_legacy_rows(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); details = []
        items = [{"id":"legacy-changed","update_time":301},{"id":"legacy-at","update_time":300},{"id":"legacy-old","update_time":299},{"id":"exact-changed","update_time":250},{"id":"new","update_time":200}]
        def fake(url,*a,**k):
            if "/conversations?" in url: return {"items":items,"total":len(items)}
            details.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        known = {cli.gen_id("chatgpt",x):100 for x in ("legacy-changed","legacy-at","legacy-old","exact-changed")}; legacy = {cli.gen_id("chatgpt",x) for x in ("legacy-changed","legacy-at","legacy-old")}
        monkeypatch.setattr(cli,"fetch_json",fake); cli.fetch_chatgpt("safari",known=known,legacy=legacy,frontiers={"default":{"account":"acct","updated":300}})
        assert details==["legacy-changed","exact-changed","new"]

    def test_fetch_chatgpt_stops_below_completed_update_frontier(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "chatgpt_profiles", lambda _: [None]); monkeypatch.setattr(cli, "chatgpt_cookie_base", lambda *a, **k: ({}, "https://chatgpt.com")); monkeypatch.setattr(cli, "chatgpt_headers", lambda *a, **k: {"ChatGPT-Account-ID":"acct"}); lists, details = [], []
        def fake(url, *a, **k):
            if "/conversations?" in url:
                assert "order=updated" in url; offset = int(url.split("offset=")[1].split("&")[0]); lists.append(offset)
                if offset == 0: return {"items":[{"id":"new","update_time":400},{"id":"changed","update_time":350}], "total":4}
                if offset == 1: return {"items":[{"id":"changed","update_time":350},{"id":"tie","update_time":300},{"id":"old","update_time":299}], "total":4}
                raise AssertionError(f"fetched past frontier: {offset}")
            details.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        known = {cli.gen_id("chatgpt",x):t for x,t in (("changed",300),("old",299))}; monkeypatch.setattr(cli, "fetch_json", fake)
        r = cli.fetch_chatgpt("safari", known=known, frontiers={"default":{"account":"acct","updated":300}})
        assert lists == [0,1] and set(details) == {"new","changed","tie"} and len(r.convs) == 3

    def test_fetch_chatgpt_scans_full_frontier_page_for_outliers(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); calls, mode = [], {"inverted":False}
        def fake(url,*a,**k):
            if "/conversations?" in url:
                offset = int(url.split("offset=")[1].split("&")[0]); calls.append(("list",offset))
                if offset==0: return {"items":[{"id":"new","update_time":400},{"id":"old","update_time":299},{"id":"missing","update_time":None}]+([{"id":"misplaced","update_time":350}] if mode["inverted"] else []),"total":4 if mode["inverted"] else 3}
                return {"items":[],"total":offset}
            calls.append(("detail",url.rsplit("/",1)[-1])); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); frontier = {"default":{"account":"acct","updated":300}}; known = {cli.gen_id("chatgpt","old"):299}
        cli.fetch_chatgpt("safari",known=known,frontiers=frontier); assert calls==[("list",0),("detail","new"),("detail","missing")]
        calls.clear(); mode["inverted"] = True; cli.fetch_chatgpt("safari",known=known,frontiers=frontier)
        assert calls==[("list",0),("detail","new"),("detail","missing"),("detail","misplaced")]

    def test_fetch_chatgpt_rejects_incomplete_list_and_account_mismatch_resets_frontier(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"new"})
        monkeypatch.setattr(cli,"fetch_json",lambda url,*a,**k:{"items":[],"total":1})
        with pytest.raises(ValueError,match="incomplete list"): cli.fetch_chatgpt("safari")
        calls = []
        def complete(url,*a,**k):
            if "offset=0" in url: return {"items":[{"id":"old","update_time":100}],"total":1}
            if "/conversations?" in url: return {"items":[],"total":1}
            calls.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",complete); cli.fetch_chatgpt("safari",frontiers={"default":{"account":"previous","updated":500}})
        assert calls==["old"]

    @pytest.mark.parametrize("shifted,total", [([{"id":"b","update_time":300},{"id":"d","update_time":100}],3),([{"id":"c","update_time":200},{"id":"d","update_time":100}],4)])
    def test_fetch_chatgpt_rejects_pagination_shifts(self, monkeypatch, shifted, total):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); details = []
        def fake(url,*a,**k):
            if "offset=0" in url: return {"items":[{"id":"a","update_time":400},{"id":"b","update_time":300}],"total":4}
            if "/conversations?" in url: return {"items":shifted,"total":total}
            details.append(url); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake)
        with pytest.raises(ValueError,match="unstable list"): cli.fetch_chatgpt("safari")
        assert details==[]

    def test_fetch_chatgpt_null_frontier_scans_boundary_page_for_unknowns(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{"ChatGPT-Account-ID":"acct"}); calls = []
        def fake(url,*a,**k):
            if "/conversations?" in url: calls.append("list"); return {"items":[{"id":"new","update_time":None},{"id":"head","update_time":None},{"id":"older","update_time":None},{"id":"late","update_time":None}],"total":4}
            calls.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); known = {cli.gen_id("chatgpt",x):1 for x in ("head","older")}; cli.fetch_chatgpt("safari",known=known,frontiers={"default":{"account":"acct","updated":None,"id":"head"}})
        assert calls==["list","new","head","late"]

    def test_fetch_chatgpt_deduplicates_repeated_list_slots(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"chatgpt_profiles",lambda _:[None]); monkeypatch.setattr(cli,"chatgpt_cookie_base",lambda *a,**k:({},"https://chatgpt.com")); monkeypatch.setattr(cli,"chatgpt_headers",lambda *a,**k:{}); details = []
        def fake(url,*a,**k):
            if "offset=0" in url: return {"items":[{"id":"a","update_time":300},{"id":"b","update_time":200}],"total":4}
            if "/conversations?" in url: return {"items":[{"id":"b","update_time":200},{"id":"a","update_time":100},{"id":"c","update_time":50}],"total":4}
            details.append(url.rsplit("/",1)[-1]); return {"mapping":{}}
        monkeypatch.setattr(cli,"fetch_json",fake); cli.fetch_chatgpt("safari"); assert details==["a","b","c"]


# ---- Claude API Tests ----

class TestClaudeAPI:
    """Tests for Claude.ai API structure."""

    def test_fetch_claude_rejects_partial_detail_failure(self, monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli, "get_cookies", lambda *_: {"session":"x"})
        def fake(url, *a, **k):
            if url.endswith("/api/organizations"): return [{"uuid":"org"}]
            if url.endswith("/chat_conversations"): return [{"uuid":"ok"}, {"uuid":"bad"}]
            if url.endswith("/bad"): raise TimeoutError("detail timeout")
            return {"chat_messages":[]}
        monkeypatch.setattr(cli, "fetch_json", fake)
        with pytest.raises(TimeoutError, match="detail timeout"): cli.fetch_claude("safari")

    def test_fetch_claude_keeps_structured_only_message_parents(self,monkeypatch):
        from ai_convos import cli
        monkeypatch.setattr(cli,"get_cookies",lambda *_:{"session":"x"})
        def fake(url,*a,**k):
            if url.endswith("/api/organizations"): return [{"uuid":"org"}]
            if url.endswith("/chat_conversations"): return [{"uuid":"c","name":"structured"}]
            return {"chat_messages":[{"uuid":"a","sender":"human","attachments":[{"file_name":"x.pdf"}],"content":[]},{"uuid":"t","sender":"assistant","content":[{"type":"tool_use","name":"lookup","input":{"q":"x"}}]}]}
        monkeypatch.setattr(cli,"fetch_json",fake); result=cli.fetch_claude("safari"); mids={m["id"] for m in result.msgs}
        assert len(result.msgs)==2 and result.attachs[0]["message_id"] in mids and result.tools[0]["message_id"] in mids

    def test_fetch_claude_reports_incremental_plan(self,monkeypatch,capsys):
        from ai_convos import cli
        monkeypatch.setattr(cli,"get_cookies",lambda *_:{"session":"x"}); details=[]
        def fake(url,*a,**k):
            if url.endswith("/api/organizations"): return [{"uuid":"org"}]
            if url.endswith("/chat_conversations"): return [{"uuid":str(i),"updated_at":f"2026-01-0{i}T00:00:00Z"} for i in range(1,5)]
            details.append(url); return {"chat_messages":[]}
        monkeypatch.setattr(cli,"fetch_json",fake); result=cli.fetch_claude("safari",since=cli.ts_from_iso("2026-01-03T00:00:00Z")); out=capsys.readouterr().out
        assert len(result.convs)==len(details)==1 and "claude listed 4; fetching 1" in out and out.count("claude fetched")==1

    @pytest.mark.integration
    def test_organizations_schema(self):
        """Verify /api/organizations returns expected schema."""

        mock_response = [{"uuid": "org-123", "name": "Personal"}]
        valid, msg = validate_schema(mock_response, "claude_organizations")
        assert valid, f"Schema validation failed: {msg}"

    @pytest.mark.integration
    def test_organizations_missing_uuid(self):
        """Detect if API changes organization structure."""

        broken_response = [{"id": "org-123"}]  # uuid -> id would break us
        valid, msg = validate_schema(broken_response, "claude_organizations")
        assert not valid
        assert "uuid" in msg

    @pytest.mark.integration
    def test_conversations_list_schema(self):
        """Verify /api/organizations/{id}/chat_conversations returns expected schema."""

        mock_response = [
            {"uuid": "conv-123", "name": "Test Chat", "created_at": "2024-01-01T00:00:00Z"}
        ]
        valid, msg = validate_schema(mock_response, "claude_conversations")
        assert valid, f"Schema validation failed: {msg}"

    @pytest.mark.integration
    def test_conversation_detail_schema(self):
        """Verify conversation detail endpoint returns expected schema."""

        mock_response = {
            "uuid": "conv-123",
            "chat_messages": [
                {"uuid": "msg-1", "sender": "human", "text": "Hello"}
            ]
        }
        valid, msg = validate_schema(mock_response, "claude_conversation")
        assert valid, f"Schema validation failed: {msg}"

    @pytest.mark.integration
    @pytest.mark.skipif(os.environ.get("CLAUDE_TEST") != "1", reason="Requires real cookies and CLAUDE_TEST=1")
    def test_live_claude_api(self, tmp_path):
        """Fetch one Claude conversation and ingest it into an isolated archive."""
        from ai_convos.cli import fetch_claude
        assert_live_import(fetch_claude("safari", limit=1), "claude", tmp_path)


# ---- Perplexity Tests ----

class TestPerplexity:
    """Perplexity threads via the web API, with fake responses."""

    @staticmethod
    def entry(uuid, query, answer, us):
        return {"uuid": uuid, "query_str": query, "created_us": us, "display_model": "pplx_pro", "text": json.dumps([{"step_type": "INITIAL_QUERY", "content": {}}, {"step_type": "FINAL", "content": {"answer": answer}}])}

    def fake(self, monkeypatch, threads, details, session={"user": {"id": "u"}}):
        from ai_convos import cli
        calls = []
        def fetch(url, cookies, *a, body=None, **k):
            calls.append((url.removeprefix("https://www.perplexity.ai"), body))
            if url.endswith("/list_ask_threads"): return threads[body["offset"]:body["offset"] + 2]
            if url.endswith("/api/auth/session"): return session
            return details[url.split("/rest/thread/")[1]]
        monkeypatch.setattr(cli, "get_cookies", lambda *a, **k: {"session": "s"}); monkeypatch.setattr(cli, "fetch_json", fetch)
        return calls

    def test_threads_follow_ups_pages_and_answers(self, monkeypatch):
        """Every thread is read in full (follow-ups, cursor pages); list pages advance by offset; FINAL answers are unwrapped."""
        from ai_convos import cli
        thread = lambda u, t, more: {"uuid": u, "title": u.upper(), "last_query_datetime": t, "query_count": 1, "has_next_page": more}
        threads = [thread("t1", "2026-01-03T00:00:00", True), thread("t2", "2026-01-02T00:00:00", True), thread("t3", "2026-01-01T00:00:00", False)]
        details = {"t1?limit=100": {"entries": [self.entry("e1", "q1", json.dumps({"answer": "a1"}), 1_767_225_600_000_000)], "has_next_page": True, "next_cursor": "c/1"},
                   "t1?limit=100&cursor=c%2F1": {"entries": [self.entry("e2", "q2", "plain a2", 1_767_225_660_000_000)], "has_next_page": False},
                   "t2?limit=100": {"entries": [self.entry("e3", "q3", None, 1)]}, "t3?limit=100": {"entries": []}}
        calls = self.fake(monkeypatch, threads, details)
        r = cli.fetch_perplexity("firefox")
        assert [c["title"] for c in r.convs] == ["T1", "T2", "T3"] and [b["offset"] for u, b in calls if b] == [0, 2]
        assert [(m["role"], m["content"], json.loads(m["metadata"])["provider_index"]) for m in r.msgs] == [("user", "q1", 0), ("assistant", "a1", 1), ("user", "q2", 2), ("assistant", "plain a2", 3), ("user", "q3", 0)]
        conv = r.convs[0]; meta = json.loads(conv["metadata"])
        assert conv["id"] == cli.gen_id("perplexity", "t1") and meta == {"session_id": "t1", "session_kind": "main", "session_kind_evidence": "exact", "capture_mode": "api", "remote_complete": True}
        assert conv["created_at"] == cli.ts_from_iso("2026-01-01T00:00:00") and conv["updated_at"] == cli.ts_from_iso("2026-01-03T00:00:00")
        assert json.loads(r.convs[1]["metadata"])["remote_complete"] is False
        assert len({m["id"] for m in r.msgs}) == 5 and r.msgs[0]["id"] == cli.gen_id("perplexity", f"{conv['id']}:e1:q")
        calls.clear(); since = cli.fetch_perplexity("firefox", since=cli.ts_from_iso("2026-01-02T00:00:00"))
        assert [c["title"] for c in since.convs] == ["T1"] and not any("t2" in u or "t3" in u for u, _ in calls)

    def test_sources_attachments_and_display_model_are_provider_metadata(self, monkeypatch):
        """Sources and attachments are kept with their messages; display_model (pplx_pro) is a product label, so model stays NULL."""
        from ai_convos import cli
        entry = {**self.entry("e1", "summarize", json.dumps({"answer": "a1", "web_results": [{"name": "Src", "url": "https://src.example/1"}]}), 1), "attachments": ["https://ppl-ai-file-upload.example/report.pdf"]}
        self.fake(monkeypatch, [{"uuid": "t1", "title": "T", "last_query_datetime": "2026-01-01T00:00:00", "has_next_page": False}], {"t1?limit=100": {"entries": [entry, {**self.entry("e2", "", None, 2), "attachments": ["https://ppl-ai-file-upload.example/only.png"]}]}})
        r = cli.fetch_perplexity("firefox")
        meta = [json.loads(m["metadata"]) for m in r.msgs]
        assert {m["model"] for m in r.msgs} == {None} and r.convs[0]["model"] is None
        assert meta[0] == {"provider_index": 0, "provider_id": "e1", "attachments": ["https://ppl-ai-file-upload.example/report.pdf"]}
        assert meta[1] == {"provider_index": 1, "provider_id": "e1", "sources": [{"name": "Src", "url": "https://src.example/1"}], "display_model": "pplx_pro"}
        assert meta[2]["attachments"] == ["https://ppl-ai-file-upload.example/only.png"] and len(r.msgs) == 3

    @pytest.mark.parametrize("session,error", [({}, "not logged in"), ({"user": {"id": "u"}}, "ended early at offset 2")])
    def test_an_empty_page_after_has_next_page_fails_the_scan(self, monkeypatch, session, error):
        """A login that expires mid-scan lists [] for the next page; that is a failed scan, never its end."""
        from ai_convos import cli
        threads = [{"uuid": u, "title": u, "last_query_datetime": "2026-01-01T00:00:00", "has_next_page": True} for u in ("t1", "t2")]
        self.fake(monkeypatch, threads, {"t1?limit=100": {"entries": []}, "t2?limit=100": {"entries": []}}, session=session)
        with pytest.raises(ValueError, match=error): cli.fetch_perplexity("firefox")

    def test_thread_contract_checks(self, monkeypatch):
        """A next page without a cursor is a broken thread, not its end; an empty thread is no evidence of a finished answer."""
        from ai_convos import cli
        thread = {"uuid": "t1", "title": "T", "last_query_datetime": "2026-01-01T00:00:00", "has_next_page": False}
        self.fake(monkeypatch, [thread], {"t1?limit=100": {"entries": [self.entry("e1", "q", "a", 1)], "has_next_page": True}})
        with pytest.raises(ValueError, match="next page but no cursor"): cli.fetch_perplexity("firefox")
        self.fake(monkeypatch, [thread], {"t1?limit=100": {"entries": [self.entry("e1", "q", "a", 1)], "has_next_page": True, "next_cursor": "c2"}, "t1?limit=100&cursor=c2": {"entries": [], "has_next_page": False}})
        with pytest.raises(ValueError, match="ended early after an advertised next page"): cli.fetch_perplexity("firefox")
        self.fake(monkeypatch, [thread], {"t1?limit=100": {"entries": []}})
        assert json.loads(cli.fetch_perplexity("firefox").convs[0]["metadata"])["remote_complete"] is False

    def test_logged_out_raises_instead_of_empty_list(self, monkeypatch):
        """An expired login lists no threads; that must fail loudly, not sync as '0 new'."""
        from ai_convos import cli
        self.fake(monkeypatch, [], {}, session={})
        with pytest.raises(ValueError, match="not logged in"): cli.fetch_perplexity("firefox")
        self.fake(monkeypatch, [], {})
        assert cli.fetch_perplexity("firefox").convs == []

    def test_json_body_is_posted_on_both_transports(self, monkeypatch):
        from ai_convos.cli import fetch_json
        import curl_cffi.requests
        sent = []
        monkeypatch.setattr(curl_cffi.requests, "request", lambda method, url, **k: sent.append((method, k["data"], k["headers"].get("Content-Type"))) or MagicMock(status_code=200, json=lambda: []))
        fetch_json("https://www.perplexity.ai/x", {}, browser="firefox", body={"offset": 0}); fetch_json("https://www.perplexity.ai/x", {}, browser="firefox")
        assert sent[0] == ("POST", b'{"offset": 0}', "application/json") and sent[1][0] == "GET"
        with patch("urllib.request.urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value.read.return_value = b"[]"
            fetch_json("https://www.perplexity.ai/x", {}, body={"offset": 0})
        req = urlopen.call_args.args[0]
        assert req.get_method() == "POST" and req.data == b'{"offset": 0}' and req.get_header("Content-type") == "application/json"


# ---- Cookie Extraction Tests ----

class TestCookieExtraction:
    """Tests for browser cookie extraction."""

    def test_safari_cookies_not_found(self):
        """Safari cookie function handles missing file gracefully."""
        from ai_convos.cookies import read_safari_cookies
        with patch("pathlib.Path.exists", return_value=False):
            cookies = read_safari_cookies("example.com")
            assert cookies == {}

    def test_chrome_cookies_not_found(self, tmp_path, monkeypatch):
        """Chrome cookie function handles missing file gracefully."""
        from ai_convos import cookies
        monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        assert cookies.read_chrome_cookies("example.com") == {} and cookies.chrome_cookie_domains() == set()

    @pytest.mark.parametrize("version", [24, 23])
    def test_chrome_cookies_strip_v10_prefix(self, tmp_path, monkeypatch, version):
        """Cookie db version 24 prepends SHA256(host_key) to each plaintext; strip it there, keep older values intact."""
        from ai_convos import cookies
        self.chrome_store(tmp_path / "Library/Application Support/Google/Chrome/Default", [("sessionKey", b"v10", b"peanuts", b"sk-ant-sid02-REAL", ".claude.ai")], 1003, version)
        monkeypatch.setattr(cookies, "PLATFORM", "darwin"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setattr(cookies.subprocess, "run", lambda *a, **k: MagicMock(returncode=0, stdout="peanuts"))
        assert cookies.read_chrome_cookies("claude.ai") == {"sessionKey": "sk-ant-sid02-REAL"}

    @pytest.mark.parametrize("keyring", [b"from-keyring", None])
    def test_chrome_cookies_linux_keyring_and_builtin_key(self, tmp_path, monkeypatch, keyring):
        """Linux Chrome writes v11 with the keyring secret (1 PBKDF2 round) and v10 with the built-in 'peanuts'; without a keyring the v11 cookie fails the install."""
        from ai_convos import cookies
        self.chrome_store(tmp_path / ".config/chromium/Default", [("new", b"v11", b"from-keyring", b"keyring-token", ".claude.ai"), ("old", b"v10", b"peanuts", b"builtin-token", ".claude.ai")])
        calls = []
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setattr(cookies, "_keyring", lambda command: calls.append(command) or keyring)
        if keyring: assert cookies.read_chrome_cookies("claude.ai") == {"new": "keyring-token", "old": "builtin-token"}
        else: pytest.raises(ValueError, cookies.read_chrome_cookies, "claude.ai")
        assert calls == [["secret-tool", "lookup", "application", "chromium"]] and cookies.chrome_profiles() == ["chromium/Default"]

    @staticmethod
    def chrome_store(path, rows, rounds=1, version=24):
        """A synthetic Chrome cookie store; rows are (name, tag, secret, value, host[, hashed host]); version >= 24 prepends SHA256(host) like Chrome."""
        import sqlite3, hashlib
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        def enc(tag, secret, value, host):
            plain = (hashlib.sha256(host.encode()).digest() if version >= 24 else b"") + value
            plain += bytes([16 - len(plain) % 16]) * (16 - len(plain) % 16)
            e = Cipher(algorithms.AES(hashlib.pbkdf2_hmac("sha1", secret, b"saltysalt", rounds, 16)), modes.CBC(b" " * 16)).encryptor()
            return tag + e.update(plain) + e.finalize()
        path.mkdir(parents=True); con = sqlite3.connect(str(path / "Cookies"))
        con.execute("CREATE TABLE meta (key TEXT, value TEXT)"); con.execute("INSERT INTO meta VALUES ('version', ?)", [str(version)])
        con.execute("CREATE TABLE cookies (name TEXT, encrypted_value BLOB, host_key TEXT)")
        con.executemany("INSERT INTO cookies VALUES (?,?,?)", [(name, enc(tag, secret, value, hashed[0] if hashed else host), host) for name, tag, secret, value, host, *hashed in rows]); con.commit(); con.close()

    def linux(self, monkeypatch, tmp_path, keyring=None):
        from ai_convos import cookies
        calls = []
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies, "_keyring", lambda command: calls.append(command[-1]) or keyring)
        return cookies, calls

    @pytest.mark.parametrize("broken_first", [True, False])
    def test_one_broken_install_does_not_block_another(self, tmp_path, monkeypatch, broken_first):
        """Installs are read one at a time and the first whose cookies all check out wins; a broken one counts only when none works."""
        (good, bad) = (".config/chromium/Default", ".config/google-chrome/Default") if broken_first else (".config/google-chrome/Default", ".config/chromium/Default")
        self.chrome_store(tmp_path / good, [("sessionKey", b"v10", b"peanuts", b"token", ".claude.ai")])
        (tmp_path / bad).mkdir(parents=True); (tmp_path / bad / "Cookies").write_bytes(b"not a database" * 100)
        cookies, _ = self.linux(monkeypatch, tmp_path)
        assert cookies.read_chrome_cookies("claude.ai") == {"sessionKey": "token"}
        (tmp_path / good / "Cookies").write_bytes(b"not a database" * 100)
        with pytest.raises(ValueError, match="Default: file is not a database .* chromium/Default: file is not a database"): cookies.read_chrome_cookies("claude.ai")

    def test_decryption_is_verified_not_guessed(self, tmp_path, monkeypatch):
        """A wrong keyring secret, a hash of another host or broken padding fail that install instead of yielding garbage; an empty value is a value."""
        self.chrome_store(tmp_path / ".config/google-chrome/Default", [("sessionKey", b"v11", b"old-keyring-secret", b"valid-session-token", ".perplexity.ai")])
        self.chrome_store(tmp_path / ".config/chromium/Default", [("sessionKey", b"v10", b"peanuts", b"chromium-token", ".perplexity.ai"), ("empty", b"v10", b"peanuts", b"", ".perplexity.ai")])
        self.chrome_store(tmp_path / ".config/chromium/Profile 1", [("sessionKey", b"v10", b"peanuts", b"token", ".perplexity.ai", ".evil.example")])
        cookies, _ = self.linux(monkeypatch, tmp_path, b"new-keyring-secret")
        assert cookies.read_chrome_cookies("perplexity.ai") == {"sessionKey": "chromium-token", "empty": ""}
        with pytest.raises(PermissionError, match="does not decrypt"): cookies.chrome_cookie_domains("Default")
        with pytest.raises(ValueError, match="does not decrypt"): cookies.read_chrome_cookies("perplexity.ai", "chromium/Profile 1")

    def test_cookie_bytes_that_are_not_utf8_are_kept(self, tmp_path, monkeypatch):
        """Chromium lets cookie values hold any 0x80-0xff octets: such a value is kept byte for byte (latin-1), and does not fail the install."""
        self.chrome_store(tmp_path / ".config/chromium/Default", [("sessionKey", b"v10", b"peanuts", b"token", ".claude.ai"), ("pref", b"v10", b"peanuts", b"caf\xe9", ".claude.ai"), ("name", b"v10", b"peanuts", "café".encode(), ".claude.ai")])
        cookies, _ = self.linux(monkeypatch, tmp_path)
        assert (got := cookies.read_chrome_cookies("claude.ai")) == {"sessionKey": "token", "pref": "caf\xe9", "name": "café"} and got["pref"].encode("latin-1") == b"caf\xe9"
        assert cookies.chrome_cookie_domains() == {".claude.ai"}

    def test_an_install_or_profile_that_cannot_be_listed_does_not_hide_the_others(self, tmp_path, monkeypatch):
        """A profile folder that cannot be probed (Python < 3.14 raises there) or an install that cannot be listed is left out, not the others with it; when no other install works, the listing error is reported, not taken for 'no cookies'."""
        import os
        self.chrome_store(tmp_path / ".config/google-chrome/Default", [("sessionKey", b"v10", b"peanuts", b"token", ".claude.ai")])
        (tmp_path / ".config/google-chrome/Profile 1").mkdir(); (locked := tmp_path / ".config/chromium").mkdir(parents=True); os.chmod(locked, 0)
        try:
            cookies, _ = self.linux(monkeypatch, tmp_path); exists = cookies.Path.exists
            monkeypatch.setattr(cookies.Path, "exists", lambda path, **k: (_ for _ in ()).throw(PermissionError(13, "Permission denied", str(path))) if "Profile 1" in path.parts else exists(path, **k))
            assert cookies.read_chrome_cookies("claude.ai") == {"sessionKey": "token"} and cookies.chrome_profiles() == ["Default"] and cookies.chrome_cookie_domains() == {".claude.ai"}
            with pytest.raises(ValueError, match="Permission denied: .*chromium"): cookies.read_chrome_cookies("example.com")
            (tmp_path / ".config/google-chrome/Default/Cookies").unlink()
            with pytest.raises(PermissionError, match="Permission denied: .*chromium"): cookies.chrome_cookie_domains()
        finally: os.chmod(locked, 0o700)

    def test_paths_with_uri_characters(self, tmp_path, monkeypatch):
        """'#' and '?' in a path are escaped in the SQLite URI, not read as its fragment or query (which opened, and created, a stray database)."""
        home = tmp_path / "we#ird?home"
        self.chrome_store(home / ".config/google-chrome/Default", [("sessionKey", b"v10", b"peanuts", b"chrome", ".claude.ai")])
        con = self.firefox_store(home / ".mozilla/firefox/abc.work#1", [("session", "firefox", ".claude.ai", "", 1)])
        cookies, _ = self.linux(monkeypatch, home)
        assert cookies.get_cookies("claude.ai", "firefox") == {"session": "firefox"} and not (home / ".mozilla/firefox/abc.work").exists()
        assert cookies.get_cookies("claude.ai", "chrome") == {"sessionKey": "chrome"} and not (tmp_path / "we").exists()
        con.close()

    def test_keyring_is_asked_only_for_rows_that_need_it_once_per_read(self, tmp_path, monkeypatch):
        """Fixed-key cookies never prompt the keyring; a read asks it at most once per browser, across installs."""
        for root, secret in ((".config/chromium/Default", b"stale-secret"), (".var/app/org.chromium.Chromium/config/chromium/Default", b"secret")):
            self.chrome_store(tmp_path / root, [("sessionKey", b"v10", b"peanuts", b"token", ".claude.ai"), ("sid", b"v11", secret, b"keyring-token", ".example.com")])
        cookies, calls = self.linux(monkeypatch, tmp_path, b"secret")
        assert cookies.read_chrome_cookies("claude.ai") == {"sessionKey": "token"} and calls == []
        assert cookies.read_chrome_cookies("example.com") == {"sid": "keyring-token"} and calls == ["chromium"]

    def test_a_cookie_of_an_unknown_encryption_fails_its_install(self, tmp_path, monkeypatch):
        """Chromium's opt-in Secret Portal writes v12: such a cookie fails its install instead of being left out of an otherwise readable set."""
        self.chrome_store(tmp_path / ".config/google-chrome/Default", [("session", b"v12", b"portal", b"portal-token", ".claude.ai"), ("pref", b"v10", b"peanuts", b"1", ".claude.ai")])
        self.chrome_store(tmp_path / ".config/chromium/Default", [("session", b"v10", b"peanuts", b"token", ".claude.ai")])
        cookies, _ = self.linux(monkeypatch, tmp_path)
        assert cookies.read_chrome_cookies("claude.ai") == {"session": "token"}
        with pytest.raises(PermissionError, match="a cookie of .claude.ai uses unsupported encryption b'v12'"): cookies.chrome_cookie_domains()

    def test_doctor_sees_a_locked_cookie_next_to_a_readable_one_of_the_same_host(self, tmp_path, monkeypatch):
        self.chrome_store(tmp_path / ".config/chromium/Default", [("sessionKey", b"v11", b"secret", b"token", ".claude.ai"), ("preference", b"v10", b"peanuts", b"1", ".claude.ai")])
        cookies, _ = self.linux(monkeypatch, tmp_path)
        with pytest.raises(PermissionError, match="keyring unavailable"): cookies.chrome_cookie_domains()

    def test_chromium_is_reachable_next_to_chrome(self, tmp_path, monkeypatch):
        """Both installs have a Default profile: each is its own profile; the default lookup and the bare name fall through to the one holding the domain, an install-qualified name does not."""
        from ai_convos import cookies
        self.chrome_store(tmp_path / ".config/google-chrome/Default", [("other", b"v10", b"peanuts", b"chrome-token", ".example.com")])
        self.chrome_store(tmp_path / ".config/chromium/Default", [("sessionKey", b"v10", b"peanuts", b"chromium-token", ".claude.ai")])
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies, "_keyring", lambda command: None)
        assert cookies.chrome_profiles() == ["Default", "chromium/Default"] and cookies.get_cookies("claude.ai", "chrome") == {"sessionKey": "chromium-token"}
        assert cookies.get_cookies("claude.ai", "chrome", "Default") == {"sessionKey": "chromium-token"} and cookies.get_cookies("claude.ai", "chrome", "chromium/Default") == {"sessionKey": "chromium-token"}
        assert cookies.get_cookies("example.com", "chrome", "chromium/Default") == {} and cookies.get_cookies("example.com", "chrome", "Default") == {"other": "chrome-token"}
        assert cookies.chrome_cookie_domains() == {".example.com", ".claude.ai"}

    @pytest.mark.parametrize("root,app", [("xdg/google-chrome", "chrome"), ("snap/chromium/common/chromium", "chromium"), (".var/app/com.google.Chrome/config/google-chrome", "chrome"), (".var/app/org.chromium.Chromium/config/chromium", "chromium")])
    def test_chrome_linux_installs(self, tmp_path, monkeypatch, root, app):
        """XDG_CONFIG_HOME, snap and flatpak installs are found, and their keyring secret is looked up under the right application."""
        from ai_convos import cookies
        self.chrome_store(tmp_path / root / "Default", [("sessionKey", b"v11", b"secret", b"token", ".claude.ai")])
        calls = []
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        monkeypatch.setattr(cookies, "_keyring", lambda command: calls.append(command) or b"secret")
        assert cookies.read_chrome_cookies("claude.ai") == {"sessionKey": "token"} and calls == [["secret-tool", "lookup", "application", app]]

    @pytest.mark.parametrize("platform,root,tag,rounds", [("linux", ".config/chromium/Default", b"v11", 1), ("darwin", "Library/Application Support/Google/Chrome/Default", b"v10", 1003)])
    def test_unavailable_keyring_is_reported_not_empty(self, tmp_path, monkeypatch, capsys, platform, root, tag, rounds):
        """A keyring that gives no secret (missing secret-tool, locked, no D-Bus, Keychain denied) is an error in sync and in doctor, not 'no cookies' or 'yes'."""
        from ai_convos import cli, cookies
        self.chrome_store(tmp_path / root, [("sessionKey", tag, b"secret", b"token", ".claude.ai")], rounds)
        monkeypatch.setattr(cookies, "PLATFORM", platform); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setattr(cookies.subprocess, "run", lambda *a, **k: MagicMock(returncode=1, stdout=""))
        with pytest.raises(ValueError, match="keyring unavailable"): cookies.get_cookies("claude.ai", "chrome")
        cli.doctor(False)
        assert "chrome: no access to cookies (Chrome keyring unavailable" in (out := capsys.readouterr().out) and "claude.ai=yes" not in out
        monkeypatch.setattr(cookies.subprocess, "run", lambda *a, **k: MagicMock(returncode=0, stdout="secret"))
        assert cookies.get_cookies("claude.ai", "chrome") == {"sessionKey": "token"} and cookies.chrome_cookie_domains() == {".claude.ai"}

    @staticmethod
    def firefox_store(folder, rows, mtime=None):
        """A Firefox cookie store with its rows in the WAL, as while Firefox runs; rows are (name, value, host, originAttributes, lastAccessed)."""
        import sqlite3, os
        folder.mkdir(parents=True); con = sqlite3.connect(str(folder / "cookies.sqlite")); con.execute("PRAGMA journal_mode=WAL"); con.execute("PRAGMA wal_autocheckpoint=0")
        con.execute("CREATE TABLE moz_cookies (name TEXT, value TEXT, host TEXT, originAttributes TEXT NOT NULL DEFAULT '', lastAccessed INTEGER)"); con.executemany("INSERT INTO moz_cookies VALUES (?,?,?,?,?)", rows); con.commit()
        assert (folder / "cookies.sqlite-wal").stat().st_size
        if mtime: [os.utime(folder / f, (t, t)) for f, t in zip(("cookies.sqlite", "cookies.sqlite-wal"), mtime)]
        return con

    def test_cookie_hosts_match_on_boundaries_and_firefox_containers_stay_apart(self, tmp_path, monkeypatch, capsys):
        """claude.ai takes its own, its subdomains' and its parents' cookies, never notclaude.ai's; Firefox reads its default container only."""
        from ai_convos import cli, cookies
        con = self.firefox_store(tmp_path / ".mozilla/firefox/p.default", [("sessionKey", "right", ".claude.ai", "", 1), ("sessionKey", "evil", ".notclaude.ai", "", 2), ("sessionKey", "work", ".claude.ai", "^userContextId=2", 3), ("cf", "www", "www.claude.ai", "", 1), ("lone", "x", "notperplexity.ai", "", 1)])
        self.chrome_store(tmp_path / ".config/google-chrome/Default", [("sessionKey", b"v10", b"peanuts", b"right", ".claude.ai"), ("sessionKey", b"v10", b"peanuts", b"evil", "notclaude.ai")])
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies, "_keyring", lambda command: None)
        assert cookies.get_cookies("claude.ai", "firefox") == {"sessionKey": "right", "cf": "www"} and cookies.get_cookies("perplexity.ai", "firefox") == {}
        assert cookies.get_cookies("claude.ai", "chrome") == {"sessionKey": "right"}
        monkeypatch.setattr(cookies, "_safari_records", lambda: iter([(".notclaude.ai", "sessionKey", "evil"), ("claude.ai", "sessionKey", "right"), ("openai.com", "parent", "p")]))
        assert cookies.read_safari_cookies("claude.ai") == {"sessionKey": "right"} and cookies.read_safari_cookies("chat.openai.com") == {"parent": "p"}
        cli.doctor(False)
        assert "firefox: chatgpt.com=no, chat.openai.com=no, openai.com=no, claude.ai=yes, perplexity.ai=no" in capsys.readouterr().out
        con.close()

    def test_firefox_profile_is_the_default_else_the_one_written_last(self, tmp_path, monkeypatch):
        """profiles.ini's default wins over a newer profile; without it, the profile whose db or WAL was written last; a name picks one; macOS keeps profiles under Profiles/."""
        import os
        from ai_convos import cookies
        root = tmp_path / ".mozilla/firefox"
        cons = [self.firefox_store(root / "a.default", [("session", "chosen", ".perplexity.ai", "", 1)], (1, 1)), self.firefox_store(root / "b.default-release", [("session", "other", ".perplexity.ai", "", 1)], (2, 2))]
        (root / "profiles.ini").write_text("[Profile1]\nName=b\nIsRelative=1\nPath=b.default-release\nDefault=1\n\n[Profile0]\nName=a\nIsRelative=1\nPath=a.default\n\n[Install4F96D1932A9F858E]\nDefault=a.default\nLocked=1\n")
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "chosen"} and cookies.get_cookies("perplexity.ai", "firefox", "b.default-release") == {"session": "other"}
        (root / "profiles.ini").unlink(); [os.utime(root / folder / name, (t, t)) for folder, times in (("a.default", (5, 1)), ("b.default-release", (2, 9))) for name, t in zip(("cookies.sqlite", "cookies.sqlite-wal"), times)]
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "other"}
        mac = tmp_path / "Library/Application Support/Firefox"; cons.append(self.firefox_store(mac / "Profiles/m.default-release", [("session", "mac", ".perplexity.ai", "", 1)]))
        (mac / "profiles.ini").write_text("[Profile0]\nPath=Profiles/m.default-release\nIsRelative=1\nDefault=1\n"); monkeypatch.setattr(cookies, "PLATFORM", "darwin")
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "mac"}
        [con.close() for con in cons]

    def test_firefox_snapshot_is_consistent_across_a_checkpoint(self, tmp_path, monkeypatch):
        """Firefox holds its store exclusively and checkpoints often: a db copied before a checkpoint and a WAL copied after it don't fit, so the copy is taken again."""
        import shutil
        from ai_convos import cookies
        writer = self.firefox_store(tmp_path / ".mozilla/firefox/p.default", [("session", "old", ".perplexity.ai", "", 1)])
        writer.execute("PRAGMA locking_mode=EXCLUSIVE"); writer.execute("UPDATE moz_cookies SET lastAccessed=2"); writer.commit()
        real, raced = shutil.copy2, []
        def copy_racing_a_checkpoint(src, dst):
            real(src, dst)
            if src.name == "cookies.sqlite" and not raced: raced.append(writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()); writer.execute("UPDATE moz_cookies SET value='new'"); writer.commit()
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies.shutil, "copy2", copy_racing_a_checkpoint)
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "new"} and raced == [(0, 0, 0)]
        writer.close()
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "new"}

    def test_firefox_snapshot_retries_when_the_wal_vanishes_mid_copy(self, tmp_path, monkeypatch):
        """A checkpoint can delete the WAL between exists() and the copy; that attempt is retaken, not the sync failed."""
        import shutil
        from ai_convos import cookies
        writer = self.firefox_store(tmp_path / ".mozilla/firefox/p.default", [("session", "token", ".perplexity.ai", "", 1)])
        writer.execute("PRAGMA locking_mode=EXCLUSIVE"); writer.execute("UPDATE moz_cookies SET lastAccessed=2"); writer.commit()
        real, vanished = shutil.copy2, []
        def copy(src, dst):
            if src.name.endswith("-wal") and not vanished: vanished.append(src); raise FileNotFoundError(src)
            return real(src, dst)
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies.shutil, "copy2", copy)
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "token"} and len(vanished) == 1
        writer.close()

    def test_firefox_copy_never_mixes_two_wal_generations(self, tmp_path, monkeypatch):
        """A checkpoint, then a WAL reset and reuse while the WAL is being copied: the copy holds the old header and first commit plus new frames, which SQLite reads as a state that never existed (a from before, b from after the checkpoint). The copy is taken again."""
        import shutil, sqlite3
        from ai_convos import cookies
        folder = tmp_path / ".mozilla/firefox/p.default"; folder.mkdir(parents=True)
        writer = sqlite3.connect(str(folder / "cookies.sqlite")); writer.execute("PRAGMA journal_mode=WAL"); writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE moz_cookies (name TEXT, value TEXT, host TEXT, originAttributes TEXT NOT NULL DEFAULT '', lastAccessed INTEGER)")
        set_ = lambda **values: (writer.executemany("UPDATE moz_cookies SET value=? WHERE name=?", [(f"{v}" + "x" * 3000, k) for k, v in values.items()]), writer.commit())
        writer.executemany("INSERT INTO moz_cookies VALUES (?,?,'.perplexity.ai','',1)", [(k, "0" + "x" * 3000) for k in "abc"]); writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)"); writer.execute("PRAGMA locking_mode=EXCLUSIVE")
        set_(a=1); set_(a=2, b=2); writer.execute("PRAGMA wal_checkpoint(FULL)").fetchone()  # main db (2,2,0); the old WAL stays, starting with the commit a=1
        page = writer.execute("PRAGMA page_size").fetchone()[0]
        real, reused = shutil.copy2, []
        def copy(src, dst):
            if not src.name.endswith("-wal") or reused: return real(src, dst)
            head = src.read_bytes()[:32 + page + 24]; set_(b=3, c=3); reused.append(src.read_bytes()[:32] != head[:32])
            Path(dst).write_bytes(head + src.read_bytes()[len(head):])
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path)); monkeypatch.setattr(cookies.shutil, "copy2", copy)
        assert {k: v[0] for k, v in cookies.get_cookies("perplexity.ai", "firefox").items()} == {"a": "2", "b": "3", "c": "3"} and reused == [True]
        writer.close()

    def test_a_firefox_db_symlinked_alone_keeps_its_wal(self, tmp_path, monkeypatch):
        """Only cookies.sqlite is symlinked elsewhere: its WAL lies next to the target, so that WAL is copied with it and tells when the profile was written last."""
        import os
        root = tmp_path / ".mozilla/firefox"; other = self.firefox_store(root / "a.default", [("session", "other", ".perplexity.ai", "", 1)], (5, 5))
        writer = self.firefox_store(tmp_path / "elsewhere", [("session", "old", ".perplexity.ai", "", 1)])
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)"); writer.execute("PRAGMA locking_mode=EXCLUSIVE"); writer.execute("UPDATE moz_cookies SET value='fresh'"); writer.commit()
        (root / "b.default").mkdir(); (root / "b.default/cookies.sqlite").symlink_to(tmp_path / "elsewhere/cookies.sqlite")
        [os.utime(tmp_path / "elsewhere" / name, (t, t)) for name, t in (("cookies.sqlite", 1), ("cookies.sqlite-wal", 9))]
        cookies, _ = self.linux(monkeypatch, tmp_path)
        assert cookies.get_cookies("perplexity.ai", "firefox", "b.default") == {"session": "fresh"} and cookies.get_cookies("perplexity.ai", "firefox") == {"session": "fresh"}
        other.close(); writer.close()

    @pytest.mark.parametrize("xdg", [True, False])
    def test_firefox_xdg_profiles_are_found(self, tmp_path, monkeypatch, xdg):
        """Firefox 147+ keeps new profiles under $XDG_CONFIG_HOME/mozilla/firefox (default ~/.config)."""
        from ai_convos import cookies
        root = (tmp_path / "xdg" if xdg else tmp_path / ".config") / "mozilla/firefox"
        con = self.firefox_store(root / "x.default-release", [("session", "xdg", ".perplexity.ai", "", 1)]); (root / "profiles.ini").write_text("[Profile0]\nPath=x.default-release\nIsRelative=1\nDefault=1\n")
        monkeypatch.setattr(cookies, "PLATFORM", "linux"); monkeypatch.setattr(cookies.Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg")) if xdg else monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        assert cookies.get_cookies("perplexity.ai", "firefox") == {"session": "xdg"}
        con.close()


# ---- Deduplication Tests ----

class TestDeduplication:
    """Tests for conversation deduplication logic."""

    def test_gen_id_deterministic(self):
        """gen_id produces consistent IDs for same input."""
        from ai_convos.cli import gen_id
        id1 = gen_id("claude", "conv-123")
        id2 = gen_id("claude", "conv-123")
        assert id1 == id2

    def test_gen_id_different_sources(self):
        """gen_id produces different IDs for different sources."""
        from ai_convos.cli import gen_id
        id1 = gen_id("claude", "conv-123")
        id2 = gen_id("chatgpt", "conv-123")
        assert id1 != id2

    def test_upsert_updates_existing(self, tmp_path):
        """Upserting same conversation updates rather than duplicates."""
        import duckdb
        from ai_convos.cli import archive_state, init_schema, upsert, ParseResult, gen_id

        db = duckdb.connect(str(tmp_path / "test.db"))
        init_schema(db)

        # First insert
        r1 = ParseResult()
        cid = gen_id("claude", "test-conv")
        r1.convs.append(dict(id=cid, source="claude", title="Original", created_at=None,
                            updated_at=None, model="claude", cwd=None, git_branch=None,
                            project_id=None, metadata="{}"))
        r1.msgs.append(dict(id=gen_id("claude", f"{cid}:0"), conversation_id=cid, role="user",
                           content="Hello", thinking=None, created_at=None, model=None, metadata="{}", parent_id=None))
        first = upsert(db, r1); assert first[5:7] == (1, 0) and len(first[7]) == 1

        # Second insert with same ID but updated title
        r2 = ParseResult()
        r2.convs.append(dict(id=cid, source="claude", title="Updated", created_at=None,
                            updated_at=None, model="claude", cwd=None, git_branch=None,
                            project_id=None, metadata="{}"))
        r2.msgs.append(dict(id=gen_id("claude", f"{cid}:1"), conversation_id=cid, role="assistant",
                           content="Hi there", thinking=None, created_at=None, model=None, metadata="{}", parent_id=None))
        second = upsert(db, r2); assert second[5:7] == (0, 1) and len(second[7]) == 1; generation=archive_state(db)[1]
        third = upsert(db, r2); assert third[5:7] == (0, 0) and not third[7] and archive_state(db)[1]==generation

        # Verify: 1 conversation, 2 messages, title updated
        conv_count = db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
        msg_count = db.execute("SELECT COUNT(*) FROM messages WHERE conversation_id = ?", [cid]).fetchone()[0]
        title = db.execute("SELECT title FROM conversations WHERE id = ?", [cid]).fetchone()[0]

        assert conv_count == 1, "Should have exactly 1 conversation"
        assert msg_count == 2, "Should have 2 messages"
        assert title == "Updated", "Title should be updated"
        db.close()

    def test_continued_conversation_no_duplicate(self, tmp_path):
        """Conversation continued on web and re-synced doesn't create duplicates."""
        import duckdb
        from ai_convos.cli import init_schema, upsert, ParseResult, gen_id

        db = duckdb.connect(str(tmp_path / "test.db"))
        init_schema(db)

        # Simulate: conversation started locally
        r1 = ParseResult()
        cid = gen_id("claude-code", "/path/to/session.jsonl")
        r1.convs.append(dict(id=cid, source="claude-code", title="Session", created_at=None,
                            updated_at=None, model="claude", cwd="/test", git_branch="main",
                            project_id=None, metadata="{}"))
        for i in range(3):
            r1.msgs.append(dict(id=gen_id("claude-code", f"{cid}:{i}"), conversation_id=cid,
                               role="user" if i % 2 == 0 else "assistant", content=f"Message {i}",
                               thinking=None, created_at=None, model=None, metadata="{}", parent_id=None))
        upsert(db, r1)

        # Simulate: same conversation continued (3 more messages)
        r2 = ParseResult()
        r2.convs.append(dict(id=cid, source="claude-code", title="Session", created_at=None,
                            updated_at=None, model="claude", cwd="/test", git_branch="main",
                            project_id=None, metadata="{}"))
        for i in range(6):  # includes original 3 + 3 new
            r2.msgs.append(dict(id=gen_id("claude-code", f"{cid}:{i}"), conversation_id=cid,
                               role="user" if i % 2 == 0 else "assistant", content=f"Message {i}",
                               thinking=None, created_at=None, model=None, metadata="{}", parent_id=None))
        upsert(db, r2)

        # Verify: still 1 conversation, now 6 messages
        conv_count = db.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
        msg_count = db.execute("SELECT COUNT(*) FROM messages WHERE conversation_id = ?", [cid]).fetchone()[0]

        assert conv_count == 1, "Should still have exactly 1 conversation"
        assert msg_count == 6, "Should have 6 messages (no duplicates)"
        db.close()


# ---- HTTP Error Handling Tests ----

class TestHTTPErrors:
    """Tests for handling API errors gracefully."""

    def test_403_forbidden_handling(self):
        """Verify helpful error on 403 Forbidden."""
        from ai_convos.cli import fetch_json
        import urllib.error

        with patch("urllib.request.urlopen") as mock_open:
            mock_open.side_effect = urllib.error.HTTPError(
                "https://api.example.com", 403, "Forbidden", {}, None
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                fetch_json("https://api.example.com", {"session": "test"})
            assert exc_info.value.code == 403

    def test_401_unauthorized_handling(self):
        """Verify 401 indicates expired cookies."""
        from ai_convos.cli import fetch_json
        import urllib.error

        with patch("urllib.request.urlopen") as mock_open:
            mock_open.side_effect = urllib.error.HTTPError(
                "https://api.example.com", 401, "Unauthorized", {}, None
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                fetch_json("https://api.example.com", {"session": "test"})
            assert exc_info.value.code == 401

    def test_browser_requests_impersonate_that_browser(self, monkeypatch):
        """With a browser, requests go through curl-cffi as that browser (its User-Agent, not ours) and HTTP errors keep urllib's shape for the retry logic."""
        from ai_convos.cli import fetch_json
        import urllib.error, curl_cffi.requests
        sent, replies = [], [MagicMock(status_code=429, reason="Too Many Requests", headers={"Retry-After": "7"}), MagicMock(status_code=200, json=lambda: {"ok": 1})]
        monkeypatch.setattr(curl_cffi.requests, "request", lambda method, url, **k: sent.append(k) or replies.pop(0))
        with patch("time.sleep") as sleep, patch("urllib.request.urlopen", side_effect=AssertionError("urllib used")):
            assert fetch_json("https://claude.ai/api", {"sessionKey": "s"}, {"Origin": "https://claude.ai"}, browser="firefox") == {"ok": 1}
        assert sleep.call_args.args[0] == 30 and [k["impersonate"] for k in sent] == ["firefox", "firefox"]
        assert sent[0]["headers"] == {"Cookie": "sessionKey=s", "Accept": "application/json", "Origin": "https://claude.ai"}
        monkeypatch.setattr(curl_cffi.requests, "request", lambda method, url, **k: MagicMock(status_code=403, reason="Forbidden", headers={}))
        with pytest.raises(urllib.error.HTTPError) as error: fetch_json("https://claude.ai/api", {}, retries=0, browser="chrome")
        assert error.value.code == 403

    def test_browser_requests_keep_the_system_proxy(self, monkeypatch):
        """urllib reads macOS's system proxy settings and bypass list; curl-cffi only reads the environment, so the system's proxies are passed on unless the host is bypassed, and the environment's left to curl."""
        from ai_convos import cli
        import curl_cffi.requests
        sent = []
        monkeypatch.setattr(curl_cffi.requests, "request", lambda method, url, **k: sent.append(k.get("proxies")) or MagicMock(status_code=200, json=lambda: {}))
        monkeypatch.setattr(cli.urllib.request, "getproxies_environment", lambda: {}); monkeypatch.setattr(cli.urllib.request, "getproxies", lambda: {"https": "http://proxy.corp:3128"})
        monkeypatch.setattr(cli.urllib.request, "proxy_bypass", lambda host: host == "www.perplexity.ai")
        cli.fetch_json("https://claude.ai/api", {}, browser="chrome"); cli.fetch_json("https://www.perplexity.ai/x", {}, browser="chrome")
        monkeypatch.setattr(cli.urllib.request, "getproxies_environment", lambda: {"https": "http://env:8080"})
        cli.fetch_json("https://claude.ai/api", {}, browser="chrome")
        assert sent == [{"https": "http://proxy.corp:3128"}, None, None]

    @pytest.mark.parametrize("headers,delay", [({},300),({"Retry-After":"420"},420)])
    def test_429_uses_visible_long_backoff(self, headers, delay):
        from ai_convos.cli import fetch_json
        import urllib.error
        error = urllib.error.HTTPError("https://api.example.com", 429, "Too Many Requests", headers, None); before = MagicMock()
        with patch("urllib.request.urlopen", side_effect=error), patch("time.sleep") as sleep:
            with pytest.raises(urllib.error.HTTPError): fetch_json("https://api.example.com", {}, retries=2, before_request=before, rate_limit_backoff=300)
        assert [x.args[0] for x in sleep.call_args_list] == [delay,delay] and before.call_count == 3
