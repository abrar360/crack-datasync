import requests
import urllib3
import time
from requests.exceptions import HTTPError, ConnectionError, Timeout

# Ignore InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
API_KEY = "ds_02af351e5f871d09a5410dcfef149f2b"

COMMON_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": f"{BASE_URL}/",
    "Origin": BASE_URL,
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

COOKIES = {"dashboard_api_key": API_KEY}


def is_valid_event(event):
    required = ["id", "timestamp", "type", "sessionId"]
    return all(k in event for k in required)


def get_stream_access():
    """Get streaming endpoint credentials."""
    url = f"{BASE_URL}/internal/dashboard/stream-access"
    print(f"[INFO] Requesting stream access from {url}")
    resp = requests.post(url, headers=COMMON_HEADERS, cookies=COOKIES, verify=False)
    resp.raise_for_status()
    j = resp.json()
    endpoint = j["streamAccess"]["endpoint"]
    token = j["streamAccess"]["token"]
    token_header = j["streamAccess"]["tokenHeader"]
    print(f"[INFO] Stream endpoint: {endpoint}")
    print(f"[INFO] Retrieved stream token (tokenHeader: {token_header})")
    return endpoint, token, token_header


def fetch_nonstream_page(cursor=None, limit=100000):
    """
    Fetch a page from the non-streaming endpoint.
    Returns (response_json, rate_limit_delay) where rate_limit_delay > 0 if rate-limited.
    """
    params = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    url = f"{BASE_URL}/api/v1/events"

    response = requests.get(url, headers=COMMON_HEADERS, cookies=COOKIES, params=params, verify=False)

    if response.status_code == 429:
        error_body = response.json()
        delay = int(error_body.get("rateLimit", {}).get("retryAfter", 10))
        return None, delay

    response.raise_for_status()
    return response.json(), 0


def fetch_stream_page(endpoint, token, token_header, cursor=None):
    """
    Fetch a page from the streaming endpoint.
    Returns (response_json, rate_limit_delay) where rate_limit_delay > 0 if rate-limited.
    """
    url = f"{BASE_URL}{endpoint}"
    if cursor:
        url += f"?cursor={cursor}"

    stream_headers = COMMON_HEADERS.copy()
    stream_headers[token_header] = token

    resp = requests.get(url, headers=stream_headers, cookies=COOKIES, verify=False, timeout=30)

    if resp.status_code == 429:
        error_body = resp.json()
        delay = int(error_body.get("rateLimit", {}).get("retryAfter", 10))
        return None, delay

    resp.raise_for_status()
    return resp.json(), 0


def fetch_all_events_hybrid():
    """
    Fetch all events using non-streaming endpoint primarily.
    When rate-limited, switch to streaming endpoint for the wait period.
    """
    total_parsed = 0
    cursor = None
    page = 1

    # Stream access credentials (fetched lazily when needed)
    stream_endpoint = None
    stream_token = None
    stream_token_header = None

    while True:
        # Try non-streaming endpoint first
        print(f"[NONSTREAM] Fetching page {page} (cursor: {cursor[-8:] if cursor else 'None'})...")

        try:
            resp_json, rate_limit_delay = fetch_nonstream_page(cursor, limit=100000)
        except Exception as e:
            print(f"[ERROR] Non-stream fetch failed: {e}")
            raise

        if rate_limit_delay > 0:
            # Rate limited! Switch to streaming endpoint
            print(f"[WARN] Rate limit exceeded. Waiting {rate_limit_delay} seconds before retrying...")
            print(f"[INFO] Switching to streaming endpoint for {rate_limit_delay} seconds...")

            # Get stream access if we haven't already
            if stream_endpoint is None:
                stream_endpoint, stream_token, stream_token_header = get_stream_access()

            # Use streaming endpoint during rate limit period
            rate_limit_end = time.time() + rate_limit_delay

            while time.time() < rate_limit_end:
                try:
                    print(f"[STREAM] Fetching page {page} (cursor: {cursor[-8:] if cursor else 'None'})...")
                    resp_json, stream_rate_limit_delay = fetch_stream_page(stream_endpoint, stream_token, stream_token_header, cursor)

                    if stream_rate_limit_delay > 0:
                        # Streaming endpoint is also rate-limited
                        print(f"[WARN] Stream endpoint rate limited. Waiting {stream_rate_limit_delay} seconds...")
                        time.sleep(stream_rate_limit_delay + 1)  # Add buffer
                        continue

                    data = resp_json.get("data", [])
                    num_parsed = sum(1 for event in data if is_valid_event(event))
                    total_parsed += num_parsed
                    print(f"[STREAM] Page {page}: Parsed {num_parsed} events (total: {total_parsed})")

                    pagination = resp_json.get("pagination", {})
                    if not pagination.get("hasMore"):
                        print(f"[INFO] No more pages. Total events parsed: {total_parsed}")
                        return total_parsed

                    cursor = pagination.get("nextCursor")
                    page += 1

                except Exception as e:
                    print(f"[WARN] Stream fetch error: {e}")
                    # Wait a bit before retrying within stream mode
                    time.sleep(2)

            # Rate limit period over, switch back to non-streaming
            # Add buffer to ensure rate limit window has fully passed on server side
            time.sleep(1)
            print(f"[INFO] Rate limit period ended. Switching back to non-streaming endpoint...")
            continue

        # Non-streaming request succeeded
        data = resp_json.get("data", [])
        num_parsed = sum(1 for event in data if is_valid_event(event))
        total_parsed += num_parsed
        print(f"[NONSTREAM] Page {page}: Parsed {num_parsed} events (total: {total_parsed})")

        pagination = resp_json.get("pagination", {})
        if not pagination.get("hasMore"):
            break

        cursor = pagination.get("nextCursor")
        page += 1

    print(f"[INFO] Total events parsed: {total_parsed}")
    return total_parsed


if __name__ == "__main__":
    print("[INFO] Starting hybrid event fetch (non-stream primary, stream on rate limit)...")
    fetch_all_events_hybrid()
