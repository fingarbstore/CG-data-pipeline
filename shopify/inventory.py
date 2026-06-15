from datetime import datetime, timezone, date

from shopify.client import paginate, strip_gid
from bigquery.upsert import append_deduped
from bigquery.client import record_run
from utils.transforms import parse_product_tags
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE = "shopify.inventory_snapshots"

# Reuse products query — inventory snapshot is derived from product/variant data
QUERY = """
query GetProducts($cursor: String) {
  products(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id title productType vendor status tags
        variants(first: 100) {
          edges {
            node {
              id title sku price compareAtPrice
              inventoryQuantity
            }
          }
        }
      }
    }
  }
}
"""


def transform(product_node, snapshot_date):
    tags = product_node.get("tags") or []
    tag_cols = parse_product_tags(tags)
    rows = []

    for edge in product_node.get("variants", {}).get("edges", []):
        v = edge["node"]
        price = float(v.get("price") or 0)
        compare = float(v.get("compareAtPrice") or 0) if v.get("compareAtPrice") else None
        is_on_sale = bool(compare and compare > price)
        discount_pct = round((compare - price) / compare * 100, 2) if is_on_sale else None

        rows.append({
            "snapshot_date":       snapshot_date,
            "variant_id":          strip_gid(v.get("id")),
            "product_id":          strip_gid(product_node.get("id")),
            "sku":                 v.get("sku"),
            "product_title":       product_node.get("title"),
            "variant_title":       v.get("title"),
            "price":               price,
            "compare_at_price":    compare,
            "is_on_sale":          is_on_sale,
            "discount_pct":        discount_pct,
            "inventory_quantity":  int(v.get("inventoryQuantity") or 0),
            "status":              product_node.get("status"),
            "vendor":              product_node.get("vendor"),
            "product_type":        product_node.get("productType"),
            "tag_season":          tag_cols.get("tag_season"),
            "tag_price_status":    tag_cols.get("tag_price_status"),
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })
    return rows


def run(bq_client, snapshot_date=None, dry_run=False):
    today = snapshot_date or date.today().isoformat()
    print(f"  Shopify inventory snapshot: {today}")

    all_rows = []
    for nodes in paginate(QUERY, "products"):
        for node in nodes:
            all_rows.extend(transform(node, today))

    print(f"  Fetched {len(all_rows)} variant snapshots")

    if dry_run:
        print("  [dry_run] Skipping BQ insert")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    # Append-only — deduplicate against today's existing snapshot
    count = append_deduped(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY,
                           "inventory_snapshots", "variant_id", "snapshot_date",
                           (today, today))
    print(f"  Inserted {count} inventory snapshot rows")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    snapshot_date = args[0] if args else None
    run(client, snapshot_date=snapshot_date, dry_run=dry)
