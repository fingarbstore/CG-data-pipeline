import time
from google.cloud import bigquery


def upsert(client, rows, project, dataset, table, primary_key, partition_field=None, date_range=None):
    if not rows:
        return 0

    staging_id = f"{table}_staging_{int(time.time())}"
    staging_ref = f"{project}.{dataset}.{staging_id}"
    target_ref = f"`{project}.{dataset}.{table}`"

    target_table = client.get_table(f"{project}.{dataset}.{table}")
    staging_table = bigquery.Table(staging_ref, schema=target_table.schema)

    try:
        client.create_table(staging_table)

        # Deduplicate rows by primary key — keep last occurrence
        seen = {}
        for row in rows:
            seen[row[primary_key]] = row
        deduped_rows = list(seen.values())

        job = client.load_table_from_json(deduped_rows, staging_ref)
        job.result()
        if job.errors:
            raise RuntimeError(f"Staging load errors: {job.errors}")

        # Build SET clause from schema (BigQuery doesn't support UPDATE SET *)
        columns = [f.name for f in target_table.schema]
        set_clause = ", ".join(f"T.{c} = S.{c}" for c in columns)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"S.{c}" for c in columns)

        # Partition filter on TARGET only — never filter staging, rows with null timestamps would be lost
        partition_filter = ""
        if partition_field and date_range:
            start, end = date_range
            partition_filter = f"AND DATE(T.{partition_field}) BETWEEN '{start}' AND '{end}'"

        merge_sql = f"""
            MERGE {target_ref} T
            USING (SELECT * FROM `{staging_ref}`) S
            ON T.{primary_key} = S.{primary_key} {partition_filter}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        client.query(merge_sql).result()
        return len(rows)

    finally:
        client.delete_table(staging_ref, not_found_ok=True)


def append_deduped(client, rows, project, dataset, table, id_field, partition_field, date_range):
    if not rows:
        return 0

    start, end = date_range
    query = f"""
        SELECT {id_field}
        FROM `{project}.{dataset}.{table}`
        WHERE DATE({partition_field}) BETWEEN @start_date AND @end_date
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
    ])
    existing = {row[id_field] for row in client.query(query, job_config=job_config).result()}

    new_rows = [r for r in rows if r.get(id_field) not in existing]
    if not new_rows:
        return 0

    target_ref = f"{project}.{dataset}.{table}"
    job = client.load_table_from_json(new_rows, target_ref)
    job.result()
    if job.errors:
        raise RuntimeError(f"Append load errors: {job.errors}")

    return len(new_rows)
