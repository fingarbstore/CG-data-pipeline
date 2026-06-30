from datetime import datetime, timezone, date

from google.cloud import bigquery

from bigquery.client import record_run
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE = "shopify.customer_status_snapshot"


def run(bq_client, snapshot_date=None, dry_run=False):
    today = snapshot_date or date.today().isoformat()
    print(f"  Customer status snapshot: {today}")

    rows_result = list(bq_client.query(f"""
        SELECT account_status, COUNT(*) AS customer_count
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_SHOPIFY}.customers`
        GROUP BY account_status
    """).result())

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "snapshot_date": today,
            "account_status": r["account_status"],
            "customer_count": r["customer_count"],
            "ingested_at": now,
        }
        for r in rows_result
    ]

    for r in rows:
        print(f"    {r['account_status']}: {r['customer_count']}")

    if dry_run:
        print("  [dry_run] Skipping insert")
        return len(rows)

    errors = bq_client.insert_rows_json(
        f"{GCP_PROJECT_ID}.{BQ_DATASET_SHOPIFY}.customer_status_snapshot", rows
    )
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")

    print(f"  Inserted {len(rows)} rows")
    record_run(bq_client, TABLE, len(rows), "success")
    return len(rows)


if __name__ == "__main__":
    import sys
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    snap_date = args[0] if args else None
    run(client, snapshot_date=snap_date, dry_run=dry)
