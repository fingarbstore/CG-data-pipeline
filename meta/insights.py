from datetime import datetime, timezone, date, timedelta
import time
import requests

from google.cloud import bigquery

from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import GCP_PROJECT_ID

import os

AD_ACCOUNT_ID  = os.environ["META_AD_ACCOUNT_ID"]
ACCESS_TOKEN   = os.environ["META_ACCESS_TOKEN"]

BQ_DATASET = "meta"
TABLE      = "meta.ad_insights"
BQ_TABLE   = "ad_insights"

BASE_URL = "https://graph.facebook.com/v19.0"

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
    "conversions",
    "conversion_values",
    "reach",
    "frequency",
    "cpc",
    "ctr",
    "cpp",
    "objective",
]


def fetch_insights(since_date, until_date):
    url = f"{BASE_URL}/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token":  ACCESS_TOKEN,
        "level":         "ad",
        "time_increment": 1,
        "fields":        ",".join(FIELDS),
        "time_range":    f'{{"since":"{since_date}","until":"{until_date}"}}',
        "limit":         500,
    }

    rows = []
    while True:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        for r in data.get("data", []):
            rows.append(r)

        paging = data.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        # Follow next page
        url = next_url
        params = {}
        time.sleep(0.5)

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

    # conversions is a list of action objects
    conversions = sum(
        float(a["value"]) for a in r.get("conversions", [])
        if a.get("action_type") == "offsite_conversion.fb_pixel_purchase"
    ) if r.get("conversions") else 0

    conv_value = sum(
        float(a["value"]) for a in r.get("conversion_values", [])
        if a.get("action_type") == "offsite_conversion.fb_pixel_purchase"
    ) if r.get("conversion_values") else 0

    row_id = f"{r['date_start']}_{r['ad_id']}"

    return {
        "row_id":        row_id,
        "date":          r["date_start"],
        "campaign_id":   r.get("campaign_id"),
        "campaign_name": r.get("campaign_name"),
        "adset_id":      r.get("adset_id"),
        "adset_name":    r.get("adset_name"),
        "ad_id":         r.get("ad_id"),
        "ad_name":       r.get("ad_name"),
        "objective":     r.get("objective"),
        "impressions":   safe_int(r.get("impressions")),
        "clicks":        safe_int(r.get("clicks")),
        "reach":         safe_int(r.get("reach")),
        "spend":         safe_float(r.get("spend")),
        "frequency":     safe_float(r.get("frequency")),
        "cpc":           safe_float(r.get("cpc")),
        "ctr":           safe_float(r.get("ctr")),
        "cpp":           safe_float(r.get("cpp")),
        "conversions":   conversions,
        "conv_value":    conv_value,
        "ingested_at":   now,
    }


def run(bq_client, since_date=None, until_date=None, dry_run=False):
    if since_date is None:
        last_run = get_last_run(bq_client, TABLE)
        if last_run:
            since_date = last_run[:10]
        else:
            since_date = "2022-01-01"

    end = until_date or (date.today() - timedelta(days=1)).isoformat()

    print(f"  Meta insights: fetching {since_date} → {end}")

    raw = fetch_insights(since_date, end)
    print(f"  Fetched {len(raw)} rows from API")

    if not raw:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    now = datetime.now(timezone.utc).isoformat()
    rows = [transform(r, now) for r in raw]

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        print(f"  Sample: {rows[0]}")
        return len(rows)

    count = upsert(bq_client, rows, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, "row_id")
    print(f"  Upserted {count} rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if len(args) > 0 else None
    until = args[1] if len(args) > 1 else None
    run(client, since_date=since, until_date=until, dry_run=dry)
