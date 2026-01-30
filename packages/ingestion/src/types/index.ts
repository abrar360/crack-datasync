export interface Event {
  id: string;
  sessionId: string;
  userId: string;
  type: string;
  name: string;
  properties?: Record<string, unknown>;
  timestamp: number | string;
  session?: {
    id: string;
    deviceType?: string;
    browser?: string;
  };
}

export interface NormalizedEvent {
  id: string;
  session_id: string;
  user_id: string;
  type: string;
  name: string;
  properties: Record<string, unknown>;
  timestamp: Date;
  device_type: string;
  browser: string;
}

export interface StreamAccess {
  endpoint: string;
  token: string;
  tokenHeader: string;
  expiresIn: number;
}

export interface Pagination {
  limit?: number;
  hasMore?: boolean;
  nextCursor?: string;
  cursorExpiresIn?: number;
}

export interface ApiResponse {
  data?: Event[];
  pagination?: Pagination;
  meta?: {
    total?: number;
    returned?: number;
    requestId?: string;
  };
}

export interface RateLimitInfo {
  retryAfter: number;
}

export interface IngestionState {
  id: number;
  cursor: string | null;
  events_ingested: number;
  last_updated: Date;
  completed: boolean;
}
