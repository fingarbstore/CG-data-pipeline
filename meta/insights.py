from datetime import datetime, timezone, date, timedelta
import time
import requests
import os

from google.cloud import bigquery

from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import GCP_PROJECT_ID

AD_ACCOUNT_ID = os.environ["META_AD_ACCOUNT_ID"]
ACCESS_TOKEN  = os.environ["META_ACCESS_TOKEN"]

BQ_DATASET = "meta"
TABLE      = "meta.ad_insights"
BQ_TABLE   = "ad_insights"

BASE_URL   = "https://graph.facebook.com/v19.0"
CHUNK_DAYS = 7

FIELDS = [
    "date_start",
    "campaign_name",
    "campaign_id",
    "adset_name",
    "adset_id",
    "ad_name",
    "ad_id",
    "impressions",
    "clicks",
    "spend",
    "actions",
    "action_values",
    "reach",
    "frequency",
    "cpm",
    "cpc",
    "ctr",
    "cpp",
]


def date_chunks(since_str, until_str):
    cursor = datetime.strptime(since_str, "%Y-%m-%d").date()
    end    = datetime.strptime(until_str, "%Y-%m-%d").date()
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), end)
        yield cursor.isoformat(), chunk_end.isoformat()
        cursor = chunk_end + timedelta(days=1)


def fetch_chunk(since_date, until_date):
    url = f"{BASE_URL}/{AD_ACCOUNT_ID}/insights"

    params = {
        "access_token":   ACCESS_TOKEN,
        "level":          "ad",
        "time_increment": "1",
        "fields":         ",".join(FIELDS),
        "time_range":     f'{{"since":"{since_date}","until":"{until_date}"}}',
        "limit":          "500",
    }

    def get_with_retry(request_url, request_params=None):
        for attempt in range(6):
            resp = requests.get(request_url, params=request_params)
            if resp.status_code == 200:
                return resp
            body = resp.json().get("error", {})
            wait = 2 ** attempt * 30
            print(f"      Meta error {resp.status_code} code={body.get('code')} msg={body.get('message','')!r} (attempt {attempt+1}), retrying in {wait}s...")
            time.sleep(wait)
        return resp

    rows = []
    first = True
    while True:
        if first:
            resp = get_with_retry(url, params)
            first = False
        else:
            resp = get_with_retry(next_url)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        time.sleep(0.3)

    return rows


def transform(r, now):
    def safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def safe_int(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    def action_count(action_type):
        return sum(
            float(a["value"]) for a in r.get("actions", [])
            if a.get("action_type") == action_type
        )

    def action_value(action_type):
        return sum(
            float(a["value"]) for a in r.get("action_values", [])
            if a.get("action_type") == action_type
        )

    return {
        "row_id":               f"{r['date_start']}_{r['ad_id']}",
        "date":                 r["date_start"],
        "campaign_id":          r.get("campaign_id"),
        "campaign_name":        r.get("campaign_name"),
        "adset_id":             r.get("adset_id"),
        "adset_name":           r.get("adset_name"),
        "ad_id":                r.get("ad_id"),
        "ad_name":              r.get("ad_name"),
        "impressions":          safe_int(r.get("impressions")),
        "clicks":               safe_int(r.get("clicks")),
        "reach":                safe_int(r.get("reach")),
        "spend":                safe_float(r.get("spend")),
        "frequency":            safe_float(r.get("frequency")),
        "cpm":                  safe_float(r.get("cpm")),
        "cpc":                  safe_float(r.get("cpc")),
        "ctr":                  safe_float(r.get("ctr")),
        "cpp":                  safe_float(r.get("cpp")),
        "result_indicator":     None,
        "add_to_cart":          action_count("add_to_cart"),
        "add_to_cart_value":    action_value("add_to_cart"),
        "initiate_checkout":    action_count("initiate_checkout"),
        "checkout_value":       action_value("initiate_checkout"),
        "conversions":          action_count("purchase"),
        "conv_value":           action_value("purchase"),
        "ingested_at":          now,
    }


def run(bq_client, since_date=None, until_date=None, dry_run=False):
    if since_date is None:
        last_run = get_last_run(bq_client, TABLE)
        if last_run:
            last_date = datetime.strptime(last_run[:10], "%Y-%m-%d").date()
            since_date = (last_date + timedelta(days=1)).isoformat()
        else:
            since_date = "2022-01-01"

    end = until_date or (date.today() - timedelta(days=1)).isoformat()

    if since_date > end:
        print(f"  Meta insights: already up to date ({since_date})")
        record_run(bq_client, TABLE, 0, "success")
        return 0

    print(f"  Meta insights: fetching {since_date} → {end}")

    now = datetime.now(timezone.utc).isoformat()
    all_rows = []

    for chunk_start, chunk_end in date_chunks(since_date, end):
        print(f"    Chunk {chunk_start} → {chunk_end}")
        raw = fetch_chunk(chunk_start, chunk_end)
        all_rows.extend(transform(r, now) for r in raw)
        time.sleep(30)

    print(f"  Fetched {len(all_rows)} rows total")

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        print(f"  Sample: {all_rows[0]}")
        return len(all_rows)

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, "row_id")
    print(f"  Upserted {count} rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry  = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if len(args) > 0 else None
    until = args[1] if len(args) > 1 else None
    run(client, since_date=since, until_date=until, dry_run=dry)
