import time
import random
import requests
from config.settings import STAMPED_API_KEY, STAMPED_BASE_URL, STAMPED_SHOP_ID


def get(endpoint, params=None, max_retries=7):
    url = f"{STAMPED_BASE_URL}{endpoint}"
    headers = {"stamped-api-key": STAMPED_API_KEY}
    params = params or {}

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (429, 500, 502, 503, 504):
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Stamped API max retries exceeded: {resp.status_code} {resp.text[:200]}")
                delay = (2 ** attempt) + random.uniform(0, 1)
                print(f"  Stamped {resp.status_code} — retrying in {delay:.1f}s (attempt {attempt + 1})")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Stamped API error {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            delay = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Stamped request error — retrying in {delay:.1f}s: {e}")
            time.sleep(delay)


def paginate(endpoint, params=None, page_size=250):
    params = params or {}
    params["limit"] = page_size
    page = 0
    total_fetched = 0

    while True:
        params["page"] = page
        data = get(endpoint, params=dict(params))

        records = data if isinstance(data, list) else data.get("data", [])
        if not records:
            break

        yield records
        total_fetched += len(records)
        print(f"  Page {page}: {len(records)} records (total so far: {total_fetched})")

        page += 1
        time.sleep(0.1)
