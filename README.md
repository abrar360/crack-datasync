## How to run my solution:
```bash
sh run-ingestion.sh
```

This will:
1. Start PostgreSQL and wait for health check
2. Build and start the ingestion service
3. Monitor progress by querying event count every 5 seconds
4. Report completion when "ingestion complete" appears in logs

## Architecture overview:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Compose                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────┐       ┌─────────────────────────────────┐  │
│  │                     │       │                                 │  │
│  │  PostgreSQL 16      │◄──────│  Ingestion Service (TypeScript) │  │
│  │                     │       │                                 │  │
│  │  - ingested_events  │       │  - Hybrid API Strategy          │  │
│  │  - ingestion_state  │       │  - Rate Limit Handling          │  │
│  │                     │       │  - Resumable Ingestion          │  │
│  └─────────────────────┘       └───────────────┬─────────────────┘  │
│                                                │                    │
└────────────────────────────────────────────────┼────────────────────┘
                                                 │
                                                 ▼
                              ┌─────────────────────────────────────┐
                              │     DataSync Analytics API          │
                              │                                     │
                              │  Primary: /api/v1/events            │
                              │  Fallback: /api/v1/events/.../feed  │
                              └─────────────────────────────────────┘
```

### Components

**Ingestion Service** (`packages/ingestion/`)
- `src/index.ts` - Main orchestration loop implementing hybrid strategy
- `src/api/client.ts` - API client with rate limiting, timeouts, token management
- `src/db/client.ts` - PostgreSQL client with batch inserts
- `src/db/schema.ts` - Database schema initialization
- `src/types/index.ts` - TypeScript interfaces

### Hybrid Ingestion Strategy

The system uses a hybrid approach to maximize throughput:

1. **Primary**: Non-streaming endpoint (`/api/v1/events`)
   - Fetches up to 100,000 events per request
   - Higher throughput but stricter rate limits

2. **Fallback**: Streaming endpoint (dynamic path from `/internal/dashboard/stream-access`)
   - Used when primary endpoint is rate-limited
   - Requires temporary token (expires in ~300s)
   - Continues fetching during rate limit window

```
Non-stream request
       │
       ▼
  Rate limited? ──No──► Process events ──► Next page
       │
      Yes
       │
       ▼
  Switch to stream endpoint
       │
       ▼
  Fetch until rate limit period ends
       │
       ▼
  Switch back to non-stream
```

### Key Features

| Feature | Implementation |
|---------|---------------|
| **Rate Limit Handling** | Reads `retryAfter` from 429 response, switches to streaming endpoint during wait |
| **Token Management** | Caches stream tokens, refreshes on 403 or proactively before expiry |
| **Resumable Ingestion** | Stores cursor + event count in `ingestion_state` table |
| **Idempotent Inserts** | Uses `ON CONFLICT DO NOTHING` for duplicate safety |
| **Batch Inserts** | Inserts in batches of 1000 for performance |
| **Timeout Handling** | 30s timeout on all API requests via AbortController |
| **Timestamp Normalization** | Handles both epoch milliseconds and ISO 8601 strings |

### Database Schema

```sql
-- Stores all ingested events
CREATE TABLE ingested_events (
  id UUID PRIMARY KEY,
  session_id UUID NOT NULL,
  user_id UUID NOT NULL,
  type VARCHAR(50) NOT NULL,
  name VARCHAR(100) NOT NULL,
  properties JSONB NOT NULL DEFAULT '{}',
  timestamp TIMESTAMPTZ NOT NULL,
  device_type VARCHAR(20) NOT NULL,
  browser VARCHAR(50) NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tracks ingestion progress for resumability
CREATE TABLE ingestion_state (
  id INTEGER PRIMARY KEY DEFAULT 1,
  cursor TEXT,
  events_ingested BIGINT NOT NULL DEFAULT 0,
  last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed BOOLEAN NOT NULL DEFAULT FALSE
);
```

## Any discoveries about the API:
- Outlined in API_NOTES.txt
- Timestamp formats vary: some events use epoch milliseconds, others use ISO 8601 strings
- Stream tokens expire in ~300 seconds, cursors expire in ~119 seconds
- Non-streaming endpoint allows up to 100,000 events per request
- Both endpoints share rate limits but have separate quotas
- Total events: 3,000,000

## What you would improve with more time:

If I had more time I would've implemented a more efficient approach by:
- Opening multiple parallel streams which each handle separate date ranges by specifying start and end parameters
- Implementing connection pooling with keep-alive to reduce connection overhead
- Adding metrics/observability (Prometheus, Grafana) to monitor ingestion progress
- Implementing backpressure handling if database inserts become a bottleneck
- Adding data validation and dead-letter queue for malformed events
- Implementing incremental/delta ingestion for ongoing sync after initial load
