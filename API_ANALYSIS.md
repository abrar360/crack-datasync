# DataSync API Analysis

## Configuration

| Setting | Value |
|---------|-------|
| Base URL | `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com` |
| API Key | `ds_02af351e5f871d09a5410dcfef149f2b` |
| Auth Method | Both `X-API-Key` header AND `dashboard_api_key` cookie |

## Two API Endpoints

### 1. Non-Streaming Endpoint (`/api/v1/events`)
- Higher throughput (can request `limit=100000` per page)
- Auth: X-API-Key header + cookie
- Subject to rate limiting (429)

### 2. Streaming Endpoint (dynamic path)
- Obtained from `/internal/dashboard/stream-access`
- Requires a temporary token (expires in ~300 seconds)
- Token passed via `X-Stream-Token` header
- Returns 403 when token expires → need to refresh
- Also rate limited

## The Hybrid Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│  Start: Use non-streaming endpoint (high throughput)            │
│                           │                                     │
│                           ▼                                     │
│              ┌────────────────────────┐                         │
│              │ Rate limited (429)?    │───No──→ Process page    │
│              └────────────────────────┘         Continue loop   │
│                           │ Yes                                 │
│                           ▼                                     │
│       ┌───────────────────────────────────────────┐             │
│       │ Switch to streaming endpoint              │             │
│       │ - Get stream token if not already have    │             │
│       │ - Fetch pages during rate limit window    │             │
│       │ - Refresh token on 403                    │             │
│       └───────────────────────────────────────────┘             │
│                           │                                     │
│                           ▼                                     │
│       Rate limit period ends → Switch back to non-streaming     │
└─────────────────────────────────────────────────────────────────┘
```

## API Endpoints Detail

### Stream Access Endpoint
```
POST /internal/dashboard/stream-access
Headers: X-API-Key, Content-Type: application/json
Cookie: dashboard_api_key

Response:
{
  "streamAccess": {
    "endpoint": "/api/v1/events/d4ta/x7k9/feed",
    "token": "b481966774f724f2589dc65219d121d7cc6354fc9d5d31b4c3e80bbd1905e510",
    "expiresIn": 300,
    "tokenHeader": "X-Stream-Token"
  }
}
```

### Non-Streaming Events Endpoint
```
GET /api/v1/events?limit=100000&cursor=<cursor>
Headers: X-API-Key
Cookie: dashboard_api_key
```

### Streaming Events Endpoint
```
GET /api/v1/events/d4ta/x7k9/feed?cursor=<cursor>
Headers: X-API-Key, X-Stream-Token: <token>
Cookie: dashboard_api_key
```

## Rate Limit Response Format
```json
{
  "rateLimit": {
    "retryAfter": 10
  }
}
```
- Status code: 429
- `retryAfter`: seconds to wait before retrying

## Event Schema

```typescript
interface Event {
  id: string;           // UUID
  sessionId: string;    // UUID
  userId: string;       // UUID
  type: string;         // "click" | "page_view" | "form_submit" | "api_call" | "scroll" | "error" | "video_play"
  name: string;         // "event_xxxxx"
  properties: {
    page: string;
  };
  timestamp: number | string;  // ⚠️ EITHER epoch ms (1769541612369) OR ISO string ("2026-01-27T19:19:13.629Z")
  session: {
    id: string;
    deviceType: string; // "mobile" | "tablet" | "desktop"
    browser: string;    // "Chrome" | "Safari" | "Edge" | "Firefox"
  };
}
```

**CRITICAL**: Timestamps can be either:
- Unix epoch milliseconds (number): `1769541612369`
- ISO 8601 string: `"2026-01-27T19:19:13.629Z"`

Must normalize during ingestion!

## Pagination

```json
{
  "limit": 25,
  "hasMore": true,
  "nextCursor": "eyJpZCI6ImU2NDRjOWFkLWM3NTItNGVhOC1hMzU5LTMwOGNkZjY5YTZmYyIsInRzIjoxNzY5NTQxMjQ4MzgwLCJ2IjoyLCJleHAiOjE3Njk3MjY2Mzg3ODR9",
  "cursorExpiresIn": 119
}
```

- `hasMore`: boolean indicating more pages available
- `nextCursor`: base64-encoded cursor for next page
- `cursorExpiresIn`: seconds until cursor expires (~119 seconds)

## Response Metadata

```json
{
  "meta": {
    "total": 3000000,
    "returned": 25,
    "requestId": "uuid"
  }
}
```

- Total events to ingest: **3,000,000**

## Critical Implementation Details

1. **Dual authentication**: BOTH cookie (`dashboard_api_key`) and header (`X-API-Key`) required
2. **Timestamp normalization**: Must handle both epoch milliseconds AND ISO strings
3. **Token lifecycle**: Stream tokens expire in ~300s, refresh on 403
4. **Cursor expiration**: Cursors expire in ~119s (must be used quickly)
5. **Total events**: 3,000,000 events to ingest
6. **Resumability**: Must persist cursor to DB for crash recovery
7. **Event validation**: Events must have `id`, `timestamp`, `type`, `sessionId`

## Key Functions from Python Prototype

| Function | Purpose |
|----------|---------|
| `get_stream_access()` | POST to `/internal/dashboard/stream-access` → returns `{endpoint, token, tokenHeader}` |
| `fetch_nonstream_page(cursor, limit)` | GET `/api/v1/events?limit=X&cursor=Y` → returns `(json, rate_limit_delay)` |
| `fetch_stream_page(...)` | GET streaming endpoint with token → handles 403 (token expired), 429 (rate limit) |
| `is_valid_event(event)` | Validates event has: `id`, `timestamp`, `type`, `sessionId` |

## Error Handling

| Status | Meaning | Action |
|--------|---------|--------|
| 200 | Success | Process data |
| 403 | Stream token expired | Refresh token via `/internal/dashboard/stream-access` |
| 429 | Rate limited | Switch to streaming endpoint, wait `retryAfter` seconds |
| 5xx | Server error | Retry with backoff |
