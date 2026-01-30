import { Event, StreamAccess, ApiResponse, NormalizedEvent } from '../types';

const RETRY_DELAYS = [1000, 2000, 4000, 8000, 16000];
const REQUEST_TIMEOUT_MS = 30000;

export class ApiClient {
  private baseUrl: string;
  private apiKey: string;
  private streamAccess: StreamAccess | null = null;
  private streamAccessExpiry: number = 0;

  constructor(baseUrl: string, apiKey: string) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.apiKey = apiKey;
  }

  private getCommonHeaders(): Record<string, string> {
    return {
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      'Connection': 'keep-alive',
      'Referer': `${this.baseUrl}/`,
      'Origin': this.baseUrl,
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
      'X-API-Key': this.apiKey,
      'Content-Type': 'application/json',
      'Cookie': `dashboard_api_key=${this.apiKey}`,
    };
  }

  private async sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private async fetchWithTimeout(url: string, options: RequestInit = {}, timeoutMs: number = REQUEST_TIMEOUT_MS): Promise<Response> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });
      return response;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  private async retryWithBackoff<T>(
    operation: () => Promise<T>,
    operationName: string
  ): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error as Error;

        // Don't retry on 4xx errors (except 429 which is handled separately)
        if (error instanceof Error && error.message.includes('HTTP 4')) {
          if (!error.message.includes('HTTP 429')) {
            throw error;
          }
        }

        if (attempt < RETRY_DELAYS.length) {
          const delay = RETRY_DELAYS[attempt];
          console.log(`[WARN] ${operationName} failed (attempt ${attempt + 1}), retrying in ${delay}ms: ${lastError.message}`);
          await this.sleep(delay);
        }
      }
    }

    throw lastError;
  }

  async getStreamAccess(): Promise<StreamAccess> {
    // Return cached stream access if still valid (with 30s buffer)
    if (this.streamAccess && Date.now() < this.streamAccessExpiry - 30000) {
      return this.streamAccess;
    }

    console.log('[API] Requesting stream access...');

    const response = await this.retryWithBackoff(async () => {
      const res = await this.fetchWithTimeout(
        `${this.baseUrl}/internal/dashboard/stream-access`,
        {
          method: 'POST',
          headers: this.getCommonHeaders(),
        }
      );

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      }

      return res.json();
    }, 'getStreamAccess');

    this.streamAccess = {
      endpoint: response.streamAccess.endpoint,
      token: response.streamAccess.token,
      tokenHeader: response.streamAccess.tokenHeader,
      expiresIn: response.streamAccess.expiresIn,
    };
    this.streamAccessExpiry = Date.now() + (this.streamAccess.expiresIn * 1000);

    console.log(`[API] Stream access obtained, endpoint: ${this.streamAccess.endpoint}`);
    return this.streamAccess;
  }

  invalidateStreamAccess(): void {
    this.streamAccess = null;
    this.streamAccessExpiry = 0;
  }

  async fetchNonStreamPage(cursor?: string | null, limit: number = 100000): Promise<{ data: ApiResponse | null; rateLimitDelay: number }> {
    const url = new URL(`${this.baseUrl}/api/v1/events`);
    url.searchParams.set('limit', limit.toString());
    if (cursor) {
      url.searchParams.set('cursor', cursor);
    }

    const response = await this.fetchWithTimeout(url.toString(), {
      headers: this.getCommonHeaders(),
    });

    if (response.status === 429) {
      let delay = 10;
      try {
        const errorBody = await response.json();
        delay = errorBody?.rateLimit?.retryAfter || 10;
      } catch {
        // Failed to parse error body, use default delay
      }
      console.log(`[API] Non-stream rate limited, retryAfter: ${delay}s`);
      return { data: null, rateLimitDelay: delay };
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const data = await response.json() as ApiResponse;
    return { data, rateLimitDelay: 0 };
  }

  async fetchStreamPage(cursor?: string | null): Promise<{ data: ApiResponse | null; rateLimitDelay: number; tokenExpired: boolean }> {
    const streamAccess = await this.getStreamAccess();

    let url = `${this.baseUrl}${streamAccess.endpoint}`;
    if (cursor) {
      url += `?cursor=${cursor}`;
    }

    const headers = {
      ...this.getCommonHeaders(),
      [streamAccess.tokenHeader]: streamAccess.token,
    };

    const response = await this.fetchWithTimeout(url, { headers });

    if (response.status === 403) {
      console.log('[API] Stream token expired');
      this.invalidateStreamAccess();
      return { data: null, rateLimitDelay: 0, tokenExpired: true };
    }

    if (response.status === 429) {
      let delay = 10;
      try {
        const errorBody = await response.json();
        delay = errorBody?.rateLimit?.retryAfter || 10;
      } catch {
        // Failed to parse error body, use default delay
      }
      console.log(`[API] Stream rate limited, retryAfter: ${delay}s`);
      return { data: null, rateLimitDelay: delay, tokenExpired: false };
    }

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const data = await response.json() as ApiResponse;
    return { data, rateLimitDelay: 0, tokenExpired: false };
  }

  static isValidEvent(event: Event): boolean {
    return !!(event && event.id && event.timestamp && event.type && event.sessionId);
  }

  static normalizeTimestamp(timestamp: number | string): Date {
    if (typeof timestamp === 'number') {
      return new Date(timestamp);
    }
    return new Date(timestamp);
  }

  static normalizeEvent(event: Event): NormalizedEvent {
    return {
      id: event.id,
      session_id: event.sessionId,
      user_id: event.userId,
      type: event.type,
      name: event.name,
      properties: event.properties || {},
      timestamp: ApiClient.normalizeTimestamp(event.timestamp),
      device_type: event.session?.deviceType || 'unknown',
      browser: event.session?.browser || 'unknown',
    };
  }
}
