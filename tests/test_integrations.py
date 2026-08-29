"""Integration tests for API fetchers - validates API schemas haven't changed."""

import pytest, json, os, plistlib
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
        monkeypatch.setattr(cli, "fetch_json", fake); known = {cli.gen_id("chatgpt", x):(cli.ts_any(t).timestamp() if t else None) for x, t in (("same",100),("older",150),("changed",150),("missing-time",150),("null-stored",None))}
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
        known = {cli.gen_id("chatgpt",x):cli.ts_any(t).timestamp() for x,t in (("changed",300),("old",299))}; monkeypatch.setattr(cli, "fetch_json", fake)
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
        monkeypatch.setattr(cli,"fetch_json",fake); frontier = {"default":{"account":"acct","updated":300}}; known = {cli.gen_id("chatgpt","old"):cli.ts_any(299).timestamp()}
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


# ---- Cookie Extraction Tests ----

class TestCookieExtraction:
    """Tests for browser cookie extraction."""

    def test_browser_ua_uses_installed_version_and_explicit_override(self, tmp_path, monkeypatch):
        from ai_convos import cli
        app=tmp_path/"Safari.app"; (app/"Contents").mkdir(parents=True); (app/"Contents/Info.plist").write_bytes(plistlib.dumps({"CFBundleShortVersionString":"26.4"})); monkeypatch.setattr(cli,"_BROWSER_APPS",{"safari":("Version/",(app,))})
        assert "Version/26.4 Safari/" in cli.browser_ua("safari")
        monkeypatch.setenv("CONVOS_BROWSER_USER_AGENT","test-agent"); assert cli.browser_ua("safari")=="test-agent"

    def test_safari_cookies_not_found(self):
        """Safari cookie function handles missing file gracefully."""
        from ai_convos.cli import read_safari_cookies
        with patch("pathlib.Path.exists", return_value=False):
            cookies = read_safari_cookies("example.com")
            assert cookies == {}

    def test_chrome_cookies_not_found(self):
        """Chrome cookie function handles missing file gracefully."""
        from ai_convos.cli import read_chrome_cookies
        with patch("pathlib.Path.exists", return_value=False):
            cookies = read_chrome_cookies("example.com")
            assert cookies == {}

    def test_chrome_keychain_failure(self):
        """Chrome cookies handles keychain access failure."""
        from ai_convos.cli import read_chrome_cookies
        with patch("pathlib.Path.exists", return_value=True):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=1)
                cookies = read_chrome_cookies("example.com")
                assert cookies == {}

    def test_chrome_cookies_strip_v10_prefix(self, tmp_path, monkeypatch):
        """Recent Chrome prepends a 32-byte hash to each decrypted cookie; strip it (legacy unprefixed values stay intact)."""
        import sqlite3, hashlib
        from hashlib import pbkdf2_hmac
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from ai_convos import cli
        key = pbkdf2_hmac("sha1", b"peanuts", b"saltysalt", 1003, 16)
        def enc_v10(value, prefix=b""):
            plain = prefix + value
            pad = 16 - (len(plain) % 16); plain += bytes([pad]) * pad
            e = Cipher(algorithms.AES(key), modes.CBC(b" " * 16)).encryptor()
            return b"v10" + e.update(plain) + e.finalize()
        cdir = tmp_path / "Library/Application Support/Google/Chrome/Default"; cdir.mkdir(parents=True)
        con = sqlite3.connect(str(cdir / "Cookies"))
        con.execute("CREATE TABLE cookies (name TEXT, encrypted_value BLOB, host_key TEXT)")
        con.execute("INSERT INTO cookies VALUES (?,?,?)", ["sessionKey", enc_v10(b"sk-ant-sid02-REAL", hashlib.sha256(b"claude.ai").digest()), ".claude.ai"])
        con.execute("INSERT INTO cookies VALUES (?,?,?)", ["legacy", enc_v10(b"plain-token-123"), ".claude.ai"])
        con.commit(); con.close()
        monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))
        monkeypatch.setattr(cli.subprocess, "run", lambda *a, **k: type("R", (), {"returncode": 0, "stdout": "peanuts"})())
        assert cli.read_chrome_cookies("claude.ai") == {"sessionKey": "sk-ant-sid02-REAL", "legacy": "plain-token-123"}


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

    @pytest.mark.parametrize("headers,delay", [({},300),({"Retry-After":"420"},420)])
    def test_429_uses_visible_long_backoff(self, headers, delay):
        from ai_convos.cli import fetch_json
        import urllib.error
        error = urllib.error.HTTPError("https://api.example.com", 429, "Too Many Requests", headers, None); before = MagicMock()
        with patch("urllib.request.urlopen", side_effect=error), patch("time.sleep") as sleep:
            with pytest.raises(urllib.error.HTTPError): fetch_json("https://api.example.com", {}, retries=2, before_request=before, rate_limit_backoff=300)
        assert [x.args[0] for x in sleep.call_args_list] == [delay,delay] and before.call_count == 3
