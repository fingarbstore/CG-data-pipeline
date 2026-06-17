"""
Shopify Bulk Operations backfill for orders + line items.
Use this for the initial historical load. Daily incremental uses shopify/orders.py.
"""
import json
import time
import requests
import tempfile
import os
from datetime import datetime, timezone

from shopify.client import run_query, strip_gid
from shopify.orders import transform_order, transform_line_items
from bigquery.upsert import upsert
from bigquery.client import record_run
from config.settings import SHOPIFY_ACCESS_TOKEN, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE_ORDERS = "shopify.orders"
TABLE_LINE_ITEMS = "shopify.order_line_items"

BULK_INNER_QUERY = """
{
  orders {
    edges {
      node {
        id name createdAt updatedAt cancelledAt closedAt
        displayFinancialStatus displayFulfillmentStatus
        tags email note
        totalPriceSet { shopMoney { amount currencyCode } }
        subtotalPriceSet { shopMoney { amount } }
        totalDiscountsSet { shopMoney { amount } }
        totalShippingPriceSet { shopMoney { amount } }
        totalTaxSet { shopMoney { amount } }
        totalRefundedSet { shopMoney { amount } }
        customer { id }
        shippingAddress { city province country zip }
        discountCodes
        lineItems {
          edges {
            node {
              id title quantity sku variantTitle
              originalUnitPriceSet { shopMoney { amount } }
              discountedUnitPriceSet { shopMoney { amount } }
              totalDiscountSet { shopMoney { amount } }
              product { id productType tags }
              variant { id price compareAtPrice }
            }
          }
        }
      }
    }
  }
}
"""


def build_bulk_mutation(inner_query):
    escaped = inner_query.replace('\\', '\\\\').replace('"', '\\"')
    return f'''
mutation {{
  bulkOperationRunQuery(query: "{escaped}") {{
    bulkOperation {{ id status }}
    userErrors {{ field message }}
  }}
}}
'''


POLL_QUERY = """
query {
  currentBulkOperation {
    id status errorCode objectCount url partialDataUrl
  }
}
"""


def start_bulk_operation():
    data = run_query(build_bulk_mutation(BULK_INNER_QUERY))
    errors = data.get("bulkOperationRunQuery", {}).get("userErrors", [])
    if errors:
        raise RuntimeError(f"Bulk operation errors: {errors}")
    op = data["bulkOperationRunQuery"]["bulkOperation"]
    print(f"  Bulk operation started: {op['id']} status={op['status']}")
    return op["id"]


def poll_until_complete(poll_interval=10):
    print("  Polling for completion...")
    while True:
        data = run_query(POLL_QUERY)
        op = data.get("currentBulkOperation") or {}
        status = op.get("status")
        count = op.get("objectCount", "?")
        print(f"  Status: {status} ({count} objects)")

        if status == "COMPLETED":
            return op.get("url"), op.get("objectCount")
        elif status in ("FAILED", "CANCELED"):
            partial_url = op.get("partialDataUrl")
            raise RuntimeError(f"Bulk operation {status}. Partial data: {partial_url}")
        elif status == "TIMEOUT":
            partial_url = op.get("partialDataUrl")
            print(f"  WARNING: Bulk operation timed out. Partial data available: {partial_url}")
            return partial_url, op.get("objectCount")

        time.sleep(poll_interval)


def download_jsonl(url):
    print(f"  Downloading JSONL from signed URL...")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(mode='wb', suffix='.jsonl', delete=False)
    for chunk in resp.iter_content(chunk_size=8192):
        tmp.write(chunk)
    tmp.close()
    print(f"  Downloaded to {tmp.name}")
    return tmp.name


def parse_jsonl(filepath):
    """
    Bulk Operations JSONL is flat — line items have __parentId pointing to order GID.
    Reconstruct order nodes with nested lineItems.
    """
    orders = {}
    line_items_by_parent = {}

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            parent_id = record.get("__parentId")

            if parent_id is None:
                # Top-level order node
                record["lineItems"] = {"edges": []}
                orders[record["id"]] = record
            else:
                # Child (line item) — group by parent order ID
                if parent_id not in line_items_by_parent:
                    line_items_by_parent[parent_id] = []
                line_items_by_parent[parent_id].append(record)

    # Attach line items to their parent orders
    for order_id, items in line_items_by_parent.items():
        if order_id in orders:
            orders[order_id]["lineItems"]["edges"] = [{"node": item} for item in items]

    print(f"  Parsed {len(orders)} orders from JSONL")
    return list(orders.values())


def load_to_bigquery(bq_client, order_nodes, batch_size=5000):
    total_orders = 0
    total_line_items = 0

    # Process in batches to avoid memory/staging issues with 59k orders
    for i in range(0, len(order_nodes), batch_size):
        batch = order_nodes[i:i + batch_size]
        order_rows = [transform_order(n) for n in batch]
        line_item_rows = []
        for n in batch:
            line_item_rows.extend(transform_line_items(n))

        print(f"  Batch {i // batch_size + 1}: upserting {len(order_rows)} orders, {len(line_item_rows)} line items...")

        upsert(bq_client, order_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY, "orders", "order_id")
        upsert(bq_client, line_item_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY, "order_line_items", "line_item_id")

        total_orders += len(order_rows)
        total_line_items += len(line_item_rows)
        print(f"  Running total: {total_orders} orders, {total_line_items} line items")

    return total_orders, total_line_items


def run(bq_client, dry_run=False):
    print("=== Shopify Orders Bulk Backfill ===")

    print("Step 1: Starting bulk operation...")
    start_bulk_operation()

    print("Step 2: Waiting for completion...")
    url, count = poll_until_complete(poll_interval=15)

    if not url:
        print("  No data URL returned — no orders to process")
        return 0

    print(f"Step 3: Downloading {count} objects...")
    filepath = download_jsonl(url)

    print("Step 4: Parsing JSONL...")
    order_nodes = parse_jsonl(filepath)

    if dry_run:
        print(f"  [dry_run] Would load {len(order_nodes)} orders. Skipping BQ.")
        os.unlink(filepath)
        return len(order_nodes)

    print("Step 5: Loading to BigQuery...")
    total_orders, total_line_items = load_to_bigquery(bq_client, order_nodes)

    os.unlink(filepath)

    record_run(bq_client, TABLE_ORDERS, total_orders, "success")
    record_run(bq_client, TABLE_LINE_ITEMS, total_line_items, "success")

    print(f"\n=== Bulk backfill complete: {total_orders} orders, {total_line_items} line items ===")
    return total_orders


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    run(client, dry_run=dry)
