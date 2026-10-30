---
summary: "Perplexity integration: web API endpoints, pagination, answer format, and cookie auth."
read_when:
  - Fetching Perplexity threads
  - Debugging Perplexity API issues
  - Understanding Perplexity data structure
---

# Perplexity Integration

Web API fetching only; Perplexity has no conversation export.

## Authentication

Browser cookies for `perplexity.ai` (Safari, Chrome or Firefox), sent through
`curl-cffi` impersonating that browser: plain Python requests get a Cloudflare
challenge. An expired login is **not** an API error: `list_ask_threads` just
returns `[]`. When the first page is empty, `/api/auth/session` tells the two
apart; an empty session (`{}`) raises
`not logged in to perplexity.ai in <browser>`, so sync reports a failure
instead of "0 new" forever.

## Endpoints

**List threads** (POST only; GET answers 400), newest activity first:
```
POST https://www.perplexity.ai/rest/thread/list_ask_threads
{"offset": 0, "limit": 50}
```
Each item has `uuid`, `title`, `last_query_datetime` (naive ISO, UTC) and
`has_next_page`; the next page starts at `offset + len(page)`.
`query_count` stays `1` after follow-up questions, so it cannot be used to
skip the detail request.

**Thread entries** (one entry per question and its answer), oldest first:
```
GET https://www.perplexity.ai/rest/thread/{uuid}?limit=100[&cursor=...]
```
`has_next_page` and `next_cursor` page through long threads; the cursor is
an opaque JSON string and is URL-encoded in full.

## Mapping

| Convos | Perplexity |
| --- | --- |
| `conversations.id` | `gen_id("perplexity", thread uuid)` |
| `metadata.session_id` | thread `uuid`; `session_kind` `main`, evidence `exact`, `capture_mode` `api` |
| `title` | thread `title` |
| `created_at` / `updated_at` | first entry's `created_us` / thread `last_query_datetime` |
| user message | entry `query_str` |
| assistant message | the `FINAL` step's `content.answer` in entry `text` (a JSON document whose `answer` holds the markdown) |
| `messages.model` | entry `display_model` on the answer |
| `metadata.provider_index` | `2 * entry index` for the question, `+1` for the answer |
| `metadata.provider_id` | entry `uuid` |

Sources, attachments and intermediate search steps are not stored.

## Incremental sync

`sync` probes the newest thread (`uuid:last_query_datetime`). An unchanged head
is a no-op; otherwise only threads whose `last_query_datetime` is newer than the
last sync are fetched. A follow-up question moves its thread back to the top,
so it is fetched again in full.
