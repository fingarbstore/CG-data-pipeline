import json
from datetime import datetime, timezone, date, timedelta

from stamped.client import paginate
from bigquery.upsert import append_deduped
from bigquery.client import get_last_run, record_run
from config.settings import STAMPED_SHOP_ID, GCP_PROJECT_ID, BQ_DATASET_STAMPED

TABLE = "stamped.activities"


def parse_unix_ms(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def transform(record):
    return {
        "activity_id":          str(record.get("id", "") or ""),
        "customer_id":          str(record.get("customerId", "") or "") or None,
        "customer_email":       (record.get("customerEmail") or "").lower() or None,
        "shop_id":              str(record.get("shopId", "") or "") or None,
        "event":                record.get("event"),
        "points_debit":         int(record.get("pointsDebit") or 0),
        "points_credit":        int(record.get("pointsCredit") or 0),
        "order_id":             str(record.get("orderId") or "") or None,
        "rule_correlation_id":  str(record.get("ruleCorrelationId") or "") or None,
        "relationship_id":      str(record.get("relationshipId") or "") or None,
        "source_action":        record.get("sourceAction"),
        "source_id":            str(record.get("sourceId") or "") or None,
        "reference":            json.dumps(record.get("reference")) if record.get("reference") is not None else None,
        "reference_hash":       record.get("referenceHash"),
        "date_created":         parse_unix_ms(record.get("dateCreated")),
        "date_analytics":       parse_unix_ms(record.get("dateAnalytics")),
        "ingested_at":          datetime.now(timezone.utc).isoformat(),
    }


def run(bq_client, since_date=None, until_date=None, dry_run=False):
    # Resolve date range
    if since_date is None:
        last_run = get_last_run(bq_client, TABLE)
        if last_run:
            # Advance by one day so we don't re-fetch already-loaded records
            last_date = datetime.strptime(last_run[:10], "%Y-%m-%d").date()
            since_date = (last_date + timedelta(days=1)).isoformat()

    start = since_date or "2000-01-01"
    end = until_date or date.today().isoformat()

    # Convert cutoff to unix ms for comparison against API's dateCreated field.
    # The activities endpoint ignores date filter params — it only supports
    # fetching all records newest-first, so we paginate and stop early.
    cutoff_ms = int(datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int((datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp() * 1000)

    print(f"  Activities: fetching {start} → {end}")

    endpoint = f"/loyalty/shops/{STAMPED_SHOP_ID}/activities"
    all_rows = []
    stopped_early = False

    for page_records in paginate(endpoint, params={}):
        for r in page_records:
            ts = int(r.get("dateCreated") or 0)
            if ts < cutoff_ms:
                stopped_early = True
                break
            if ts < end_ms:
                all_rows.append(transform(r))
        if stopped_early:
            break

    print(f"  Fetched {len(all_rows)} activities")

    if dry_run:
        print("  [dry_run] Skipping BQ insert")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = append_deduped(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_STAMPED,
                           "activities", "activity_id", "date_created", (start, end))
    print(f"  Inserted {count} new activity rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Usage: python3 -m stamped.activities [since_date] [until_date] [--dry-run]
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv

    since = args[0] if len(args) > 0 else None
    until = args[1] if len(args) > 1 else None

    run(client, since_date=since, until_date=until, dry_run=dry)
