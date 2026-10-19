---
summary: "Claude.ai integration: API endpoints, export formats, and cookie auth."
read_when:
  - Fetching Claude conversations
  - Debugging Claude API issues
  - Importing Claude exports
  - Understanding Claude data structure
---

# Claude.ai Integration

Supports both web API fetching and export file parsing.

## Web API Fetching

Uses browser cookies to authenticate with Claude.ai's API.

Sync selects the browser's active chat organization (`lastActiveOrg`), falling
back to the first chat-capable organization. API-only organizations can appear
first in the organization list and return `403 Permission denied` for chat
endpoints even with a valid session. Older organization responses without a
`capabilities` field are treated as chat-capable.

### Endpoints

**Get organizations:**
```
GET https://claude.ai/api/organizations
```

Response:
```json
[
  {
    "uuid": "org-uuid",
    "name": "Personal"
  }
]
```

**List conversations:**
```
GET https://claude.ai/api/organizations/{org_id}/chat_conversations
```

Response:
```json
[
  {
    "uuid": "conv-uuid",
    "name": "Chat Title",
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-01T00:00:00Z",
    "model": "claude-3-opus",
    "project_uuid": "proj-uuid"
  }
]
```

**Get conversation detail:**
```
GET https://claude.ai/api/organizations/{org_id}/chat_conversations/{conv_id}
```

Response:
```json
{
  "uuid": "conv-uuid",
  "chat_messages": [
    {
      "uuid": "msg-uuid",
      "sender": "human",
      "text": "Hello",
      "created_at": "2024-01-01T00:00:00Z",
      "attachments": []
    }
  ]
}
```

### Required Headers

```python
headers = {
    "Origin": "https://claude.ai",
    "Referer": "https://claude.ai/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "anthropic-client-sha": "unknown",
    "anthropic-client-version": "unknown"
}
```

### Cookie Requirements

Must have valid session cookies from claude.ai:
- `sessionKey`
- `lastActiveOrg`
- `cf_clearance`

## Export File Parsing

Claude exports are JSON files with conversation array.

### Export Format

```json
[
  {
    "uuid": "conv-uuid",
    "name": "Chat Title",
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-01T00:00:00Z",
    "model": "claude-3-opus",
    "chat_messages": [
      {
        "uuid": "msg-uuid",
        "sender": "human",
        "text": "Message content",
        "created_at": "2024-01-01T00:00:00Z",
        "attachments": [
          {
            "file_name": "doc.pdf",
            "file_type": "application/pdf",
            "file_size": 1024,
            "url": "https://..."
          }
        ]
      }
    ]
  }
]
```

### Content Formats

Messages can have content in two formats:

**Simple text:**
```json
{"text": "Message content"}
```

**Content blocks (newer format):**
```json
{
  "content": [
    {"type": "text", "text": "Part 1"},
    {"type": "tool_use", "name": "search", "input": {}},
    {"type": "tool_result", "tool_use_id": "...", "content": "result"}
  ]
}
```

### Tool Calls

Tool usage appears as content blocks:

```json
{
  "type": "tool_use",
  "id": "tool-call-id",
  "name": "search",
  "input": {"query": "search term"}
}
```

Tool results:

```json
{
  "type": "tool_result",
  "tool_use_id": "tool-call-id",
  "content": "Result text"
}
```

## Usage

```bash
# Sync from web API (auto-tries browsers)
uv run convos sync

# Import export file via env
CONVOS_IMPORT_PATHS="~/Downloads/claude-export.json" uv run convos sync
```

## Troubleshooting

**403 Forbidden:**
- Cookies expired - log into claude.ai in browser
- `account_session_invalid` - open claude.ai and sign in again in that browser
- `Permission denied` with a valid session - verify the selected organization has chat access
- Headers may need updating if API changes
- `sync` will try Safari and Chrome

**Could not get org ID:**
- Not logged in or session expired
- Account may be in different region

**Missing attachments:**
- Attachment URLs may expire
- Large files may not be included in export
