import requests
import urllib3
import time
from requests.exceptions import HTTPError, ConnectionError, Timeout

# Ignore InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = 'http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com'
API_KEY = 'ds_02af351e5f871d09a5410dcfef149f2b'
COOKIE = {'dashboard_api_key': API_KEY}

COMMON_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': BASE_URL + '/',
    'Origin': BASE_URL,
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/144.0.0.0 Safari/537.36'
    ),
    'X-API-Key': API_KEY,
    'Content-Type': 'application/json'
}


def get_stream_access():
    url = f"{BASE_URL}/internal/dashboard/stream-access"
    print(f"[INFO] Requesting stream access from {url}")
    resp = requests.post(url, headers=COMMON_HEADERS, cookies=COOKIE, verify=False)
    resp.raise_for_status()
    j = resp.json()
    endpoint = j['streamAccess']['endpoint']
    token = j['streamAccess']['token']
    token_header = j['streamAccess']['tokenHeader']
    print(f"[INFO] Stream endpoint: {endpoint}")
    print(f"[INFO] Retrieved stream token (tokenHeader: {token_header})")
    return endpoint, token, token_header

def fetch_events(endpoint, token, token_header):
    total_events = 0
    cursor = None
    has_more = True
    page_num = 1
    MAX_RETRIES = 10
    RETRY_DELAY = 10  # seconds

    while has_more:
        url = f"{BASE_URL}{endpoint}"
        if cursor:
            url += f"?cursor={cursor}"
            print(f"[INFO] Fetching page {page_num} with cursor: {cursor[-8:]}...")
        else:
            print(f"[INFO] Fetching first page (no cursor)")

        stream_headers = COMMON_HEADERS.copy()
        stream_headers[token_header] = token

        attempt = 0
        while True:
            try:
                resp = requests.get(url, headers=stream_headers, cookies=COOKIE, verify=False, timeout=30)
                resp.raise_for_status()
                break  # Success!
            except (HTTPError, ConnectionError, Timeout) as e:
                attempt += 1
                status_code = getattr(e.response, "status_code", None) if isinstance(e, HTTPError) else None
                print(f"[WARN] Error fetching page {page_num}: {e} (attempt {attempt}/{MAX_RETRIES})")
                if attempt >= MAX_RETRIES:
                    print(f"[ERROR] Failed after {MAX_RETRIES} attempts. Exiting.")
                    raise
                print(f"[INFO] Waiting {RETRY_DELAY}s before retry")
                time.sleep(RETRY_DELAY)
        
        j = resp.json()
        if not isinstance(j, dict):
            print(f"[ERROR] Unexpected response type {type(j)}, response: {j}")
            raise ValueError("Expected response to be a dict, got something else.")

        events = j.get('data', [])
        print(f"[INFO] Received {len(events)} events on page {page_num}")

        total_events += len(events)
        pag = j.get('pagination', {})
        has_more = pag.get('hasMore', False)
        cursor = pag.get('nextCursor', None)
        page_num += 1

    print(f"[INFO] No more pages. Finished fetching events.")

    return total_events

def main():
    print("[INFO] Starting event streaming fetch process...")
    endpoint, token, token_header = get_stream_access()
    count = fetch_events(endpoint, token, token_header)
    print(f"Total number of events parsed: {count}")



if __name__ == '__main__':
    main()