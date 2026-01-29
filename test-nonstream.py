import requests
import time

BASE_URL = "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"
DASHBOARD_API_KEY = "ds_02af351e5f871d09a5410dcfef149f2b"
#IF_NONE_MATCH = 'W/"1c28-L4lALLO7RbSKmq+I2wEDaQt02d0"'

headers = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": f"{BASE_URL}/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "X-API-Key": DASHBOARD_API_KEY,
    #"If-None-Match": IF_NONE_MATCH
}

cookies = {
    "dashboard_api_key": DASHBOARD_API_KEY
}

def is_valid_event(event):
    required = ["id", "timestamp", "type", "sessionId"]
    return all(k in event for k in required)

def fetch_events_page(cursor=None, limit=20):
    params = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    url = f"{BASE_URL}/api/v1/events"
    while True:
        try:
            response = requests.get(url, headers=headers, cookies=cookies, params=params, verify=False)
            if response.status_code == 429:
                # Carefully parse the response
                #try:
                error_body = response.json()
                delay = int(error_body.get("rateLimit", {}).get("retryAfter", 10))
                #except Exception:
                #delay = 10
                print(f"[WARN] Rate limit exceeded. Waiting {delay} seconds before retrying...")
                time.sleep(delay+1)
                continue  # retry after waiting
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"[ERROR] HTTP error: {e}")
            raise
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
            raise

def fetch_and_parse_all_events():
    total_parsed = 0
    cursor = None
    page = 1

    while True:
        resp_json = fetch_events_page(cursor, limit=100000)
        data = resp_json.get("data", [])
        num_parsed = sum(1 for event in data if is_valid_event(event))
        total_parsed += num_parsed
        print(f"Page {page}: Parsed {num_parsed} events (total so far: {total_parsed}) Cursor: {cursor}")

        pagination = resp_json.get("pagination", {})
        if not pagination.get("hasMore"):
            break
        cursor = pagination.get("nextCursor")
        page += 1

    print(f"Total events parsed: {total_parsed}")

if __name__ == "__main__":
    fetch_and_parse_all_events()