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
apart; a session without a user (`{}`) raises
`not logged in to perplexity.ai in <browser>`, so sync reports a failure
instead of "0 new" forever. The session's `user.id` (else `user.email`) is the
account identity sync scopes its cutoff to.

## Endpoints

**List threads** (POST only; GET answers 400), newest activity first:
```
POST https://www.perplexity.ai/rest/thread/list_ask_threads
{"offset": 0, "limit": 50}
```
Each item has `uuid`, `title`, `last_query_datetime` (naive ISO, UTC) and
`has_next_page`; the next page starts at `offset + len(page)`. An empty page
after `has_next_page` (a login that expired mid-scan, or a broken listing)
fails the scan: the session is checked, and the error is `not logged in` or
`thread list ended early`; the sync state keeps its old head and cutoff.
`query_count` stays `1` after follow-up questions, so it cannot be used to
skip the detail request.

**Thread entries** (one entry per question and its answer), oldest first:
```
GET https://www.perplexity.ai/rest/thread/{uuid}?limit=100[&cursor=...]
```
`has_next_page` and `next_cursor` page through long threads; the cursor is
an opaque JSON string and is URL-encoded in full. `has_next_page` without a
cursor raises, and so does an empty page reached through a cursor. A thread that answers `404` (deleted since it was listed or
left pending) is skipped.

## Mapping

| Convos | Perplexity |
| --- | --- |
| `conversations.id` | `gen_id("perplexity", thread uuid)` |
| `metadata.session_id` | thread `uuid`; `session_kind` `main`, evidence `exact`, `capture_mode` `api` |
| `title` | thread `title` |
| `created_at` / `updated_at` | first entry's `created_us` / thread `last_query_datetime` |
| user message | entry `query_str` |
| assistant message | the `FINAL` step's `content.answer` in entry `text` (a JSON document whose `answer` holds the markdown) |
| `messages.model`, `conversations.model` | NULL: `display_model` is a product label, not an exact model |
| `metadata.provider_index` | `2 * entry index` for the question, `+1` for the answer |
| `metadata.provider_id` | entry `uuid` |
| user `metadata.attachments` | entry `attachments`, as sent |
| assistant `metadata.sources` | the answer document's `web_results`, as sent |
| assistant `metadata.display_model` | entry `display_model` (e.g. `pplx_pro`) |
| `metadata.remote_complete` | at least one entry, and every entry has a `FINAL` answer |

A question with attachments but no text is kept. Intermediate search steps are
not stored.

## Incremental sync

`sync` probes the newest thread (`uuid:last_query_datetime`) and the account. An
unchanged head of the same account with no pending threads is a no-op; otherwise only threads whose `last_query_datetime`
is newer than the last sync are fetched. A follow-up question moves its thread
back to the top, so it is fetched again in full.

- The cutoff belongs to the account that set it. When sync falls back to
  another browser logged in to another account (or none can be established),
  that account's threads are fetched in full. Claude does the same with its
  organization.
- A thread synced while its answer was still being written
  (`remote_complete: false`), head or not, is kept in the sync state's
  `pending` list (`uuid`, `title`, `last_query_datetime`) and fetched again on
  every sync, whatever the cutoff, until it is complete: its
  `last_query_datetime` does not move when the answer lands. Pending threads
  belong to the account too.
