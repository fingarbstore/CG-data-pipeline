import json
from datetime import datetime, timezone, date, timedelta
from google.cloud import bigquery

from stamped.client import paginate, get
from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import STAMPED_SHOP_ID, GCP_PROJECT_ID, BQ_DATASET_STAMPED, BQ_DATASET_STAMPED

TABLE = "stamped.customers"


def parse_customer_ts(ts):
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def parse_dob(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date().isoformat()
    except (ValueError, TypeError):
        return None


def transform(record):
    activity = record.get("activity", {}) or {}
    return {
        "customer_id":               str(record.get("customerId", "") or ""),
        "platform_id":               str(record.get("platformId", "") or "") or None,
        "email":                     (record.get("email") or "").lower() or None,
        "first_name":                record.get("firstName"),
        "last_name":                 record.get("lastName"),
        "date_of_birth":             parse_dob(record.get("dateOfBirth")),
        "referral_code":             record.get("referralCode"),
        "tags":                      json.dumps(record.get("tags") or []),
        "deleted":                   bool(record.get("deleted", False)),
        "date_deleted":              parse_customer_ts(record.get("dateDeleted")),
        "date_platform_created":     parse_customer_ts(record.get("datePlatformCreated")),
        "date_platform_updated":     parse_customer_ts(record.get("datePlatformUpdated")),
        "date_stamped_created":      parse_customer_ts(record.get("dateCreated")),
        "date_stamped_updated":      parse_customer_ts(record.get("dateUpdated")),
        "total_points":              int(activity.get("totalPoints") or 0),
        "vip_tier":                  activity.get("vipTier"),
        "total_affiliate_orders":    int(activity.get("totalAffiliateOrders") or 0),
        "date_total_points_updated": parse_customer_ts(activity.get("dateTotalPointsUpdated")),
        "date_vip_tier_updated":     parse_customer_ts(activity.get("dateVipTierUpdated")),
        "ingested_at":               datetime.now(timezone.utc).isoformat(),
    }


def parse_unix_ms(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def transform_individual(record):
    """Transform response from the individual /customers/{id} endpoint."""
    loyalty = record.get("loyalty", {}) or {}
    return {
        "customer_id":               str(record.get("customerId", "") or ""),
        "platform_id":               str(record.get("shopifyId", "") or "") or None,
        "email":                     (record.get("email") or "").lower() or None,
        "first_name":                record.get("firstName"),
        "last_name":                 record.get("lastName"),
        "date_of_birth":             parse_dob(record.get("dateOfBirth")),
        "referral_code":             record.get("referralCode"),
        "tags":                      json.dumps(record.get("tags") or []),
        "deleted":                   bool(record.get("deleted", False)),
        "date_deleted":              parse_unix_ms(record.get("dateDeleted")) if record.get("dateDeleted") else None,
        "date_platform_created":     parse_unix_ms(record.get("datePlatformCreated")),
        "date_platform_updated":     parse_unix_ms(record.get("datePlatformUpdated")),
        "date_stamped_created":      parse_unix_ms(record.get("dateStampedCreated")),
        "date_stamped_updated":      parse_unix_ms(record.get("dateStampedUpdated")),
        "total_points":              int(loyalty.get("totalPoints") or 0),
        "vip_tier":                  loyalty.get("vipTier"),
        "total_affiliate_orders":    int(loyalty.get("totalAffiliateOrders") or 0),
        "date_total_points_updated": parse_unix_ms(loyalty.get("datePointsUpdated")),
        "date_vip_tier_updated":     parse_unix_ms(loyalty.get("dateVipTierUpdated")),
        "ingested_at":               datetime.now(timezone.utc).isoformat(),
    }


def fetch_by_ids(customer_ids):
    """Fetch individual customers by their Stamped customer_id."""
    rows = []
    for cid in customer_ids:
        try:
            result = get(f"/merchant/shops/{STAMPED_SHOP_ID}/customers/{cid}")
            if result:
                record = result if isinstance(result, dict) else (result[0] if result else None)
                if record:
                    rows.append(transform_individual(record))
        except Exception as e:
            print(f"  Warning: failed to fetch customer {cid}: {e}")
    return rows


def get_active_customer_ids(bq_client, since_date, until_date):
    """Get customer_ids that had activity events in the given date range."""
    query = f"""
        SELECT DISTINCT customer_id
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_STAMPED}.activities`
        WHERE DATE(date_created) BETWEEN @start_date AND @end_date
          AND customer_id IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", since_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", until_date),
    ])
    return [row.customer_id for row in bq_client.query(query, job_config=job_config).result()]


def run(bq_client, since_date=None, until_date=None, full=False, dry_run=False):
    today = date.today().isoformat()
    end = until_date or today

    if full:
        # Full backfill — paginate all customers
        print("  Customers: full backfill mode")
        all_rows = []
        for page_records in paginate(f"/merchant/shops/{STAMPED_SHOP_ID}/customers"):
            all_rows.extend(transform(r) for r in page_records)
        print(f"  Fetched {len(all_rows)} customers (full)")
    else:
        # Incremental: new customers + customers with recent activity
        if since_date is None:
            last_run = get_last_run(bq_client, TABLE)
            since_date = last_run[:10] if last_run else (date.today() - timedelta(days=1)).isoformat()

        print(f"  Customers: incremental {since_date} → {end}")
        all_rows = []

        # Fetch customers who had activity events since last run.
        # This catches both new customers (customer/created event) and
        # existing customers whose points/tier changed.
        # Note: Stamped ignores date filters on the customers endpoint,
        # so we derive changed customers from the activities table instead.
        print("  Fetching customers with recent activity events...")
        active_ids = get_active_customer_ids(bq_client, since_date, end)
        print(f"  {len(active_ids)} customers had activity events — fetching updated profiles...")
        if active_ids:
            updated_rows = fetch_by_ids(active_ids)
            all_rows.extend(updated_rows)
            print(f"  Fetched {len(updated_rows)} customer profiles")

    print(f"  Total to upsert: {len(all_rows)}")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        if all_rows:
            print(f"  Sample: {list(all_rows[0].keys())}")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_STAMPED, "customers", "customer_id")
    print(f"  Upserted {count} customer rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Usage:
    #   python3 -m stamped.customers                        # incremental from last run
    #   python3 -m stamped.customers --full                 # full backfill
    #   python3 -m stamped.customers 2026-06-01             # incremental from specific date
    #   python3 -m stamped.customers 2026-06-01 --dry-run   # preview without writing
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    full = "--full" in sys.argv
    dry = "--dry-run" in sys.argv
    since = args[0] if args else None

    run(client, since_date=since, full=full, dry_run=dry)
