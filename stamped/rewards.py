from datetime import datetime, timezone, date

from stamped.client import paginate
from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import STAMPED_SHOP_ID, GCP_PROJECT_ID, BQ_DATASET_STAMPED

TABLE = "stamped.rewards"


def parse_unix_ms(ts):
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


def transform(record):
    return {
        "reward_id":            str(record.get("rewardId", "") or ""),
        "activity_id":          str(record.get("activityId") or "") or None,
        "rule_correlation_id":  str(record.get("ruleCorrelationId") or "") or None,
        "customer_id":          str(record.get("customerId") or "") or None,
        "relationship_id":      str(record.get("relationshipId") or "") or None,
        "status":               record.get("status"),
        "title":                record.get("title"),
        "description":          record.get("description"),
        "code":                 record.get("code"),
        "category":             record.get("category"),
        "type":                 record.get("type"),
        "value":                float(v) if (v := record.get("value")) and v != "null" else None,
        "service":              record.get("service"),
        "service_id":           str(record.get("serviceId") or "") or None,
        "profile":              record.get("profile"),
        "date_created":         parse_unix_ms(record.get("dateCreated")),
        "date_updated":         parse_unix_ms(record.get("dateUpdated")),
        "date_expire":          parse_unix_ms(record.get("dateExpire")),
        "date_analytics":       parse_unix_ms(record.get("dateAnalytics")),
        "ingested_at":          datetime.now(timezone.utc).isoformat(),
    }


def run(bq_client, dry_run=False):
    # Always full sync — only 7 pages, and status changes on old rewards can't be date-filtered
    print("  Rewards: full sync (status changes require full pull)")

    endpoint = f"/loyalty/shops/{STAMPED_SHOP_ID}/rewards"
    all_rows = []
    for page_records in paginate(endpoint):
        all_rows.extend(transform(r) for r in page_records)

    print(f"  Fetched {len(all_rows)} rewards")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_STAMPED,
                   "rewards", "reward_id")
    print(f"  Upserted {count} rewards")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    run(client, dry_run=dry)
