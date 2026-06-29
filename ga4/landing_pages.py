from datetime import datetime, timezone, date, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.cloud import bigquery

from bigquery.client import get_last_run, record_run
from bigquery.upsert import upsert
from config.settings import GCP_PROJECT_ID, GA4_PROPERTY_ID

BQ_DATASET = "ga4"
TABLE = "ga4.landing_pages"
BQ_TABLE = "landing_pages"


def run(bq_client, since_date=None, dry_run=False):
    if since_date is None:
        last_run = get_last_run(bq_client, TABLE)
        since_date = last_run[:10] if last_run else (date.today() - timedelta(days=90)).isoformat()

    # Don't include today — GA4 data for the current day is partial
    end_date = (date.today() - timedelta(days=1)).isoformat()

    if since_date >= end_date:
        print(f"  GA4 landing pages: already up to date ({since_date})")
        record_run(bq_client, TABLE, 0, "success")
        return 0

    print(f"  GA4 landing pages: fetching {since_date} → {end_date}")

    ga4_client = BetaAnalyticsDataClient()
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=since_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="landingPage"),
            Dimension(name="sessionDefaultChannelGroup"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="newUsers"),
            Metric(name="totalRevenue"),
            Metric(name="transactions"),
        ],
        limit=100000,
    )

    resp = ga4_client.run_report(req)

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for row in resp.rows:
        d = row.dimension_values
        m = row.metric_values
        date_str = d[0].value  # YYYYMMDD format from GA4
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        rows.append({
            "row_id":        f"{date_formatted}_{d[1].value}_{d[2].value}_{d[3].value}_{d[4].value}",
            "date":          date_formatted,
            "landing_page":  d[1].value,
            "channel_group": d[2].value,
            "source":        d[3].value,
            "medium":        d[4].value,
            "sessions":      int(m[0].value),
            "new_users":     int(m[1].value),
            "revenue":       float(m[2].value),
            "transactions":  int(m[3].value),
            "ingested_at":   now,
        })

    print(f"  Fetched {len(rows)} rows")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        if rows:
            print(f"  Sample: {rows[0]}")
        return len(rows)

    if not rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = upsert(bq_client, rows, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, "row_id")
    print(f"  Upserted {count} rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery as bq
    client = bq.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if args else None
    run(client, since_date=since, dry_run=dry)
