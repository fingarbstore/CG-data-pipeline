from datetime import datetime, timezone
from google.cloud import bigquery
from config.settings import GCP_PROJECT_ID, BQ_DATASET_METADATA


def get_last_run(client, table_name):
    """Returns last successful run_at as ISO string, or None if first run."""
    query = f"""
        SELECT run_at FROM `{GCP_PROJECT_ID}.{BQ_DATASET_METADATA}.run_log`
        WHERE table_name = @table_name AND status = 'success'
        ORDER BY run_at DESC LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
    ])
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    # Strip microseconds — Shopify's query filter silently rejects them
    return rows[0].run_at.strftime("%Y-%m-%dT%H:%M:%SZ")


def record_run(client, table_name, rows_processed, status, error=None):
    """Write run result to run_log."""
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "table_name": table_name,
        "last_run_at": now,
        "rows_processed": rows_processed,
        "status": status,
        "error_message": error,
        "run_at": now,
    }
    target = f"{GCP_PROJECT_ID}.{BQ_DATASET_METADATA}.run_log"
    job = client.load_table_from_json([row], target)
    job.result()
