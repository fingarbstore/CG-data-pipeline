import json
from datetime import datetime, timezone

from shopify.client import paginate, strip_gid
from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from utils.transforms import parse_product_tags
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE = "shopify.products"

QUERY = """
query GetProducts($cursor: String) {
  products(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id title productType vendor status tags
        createdAt updatedAt publishedAt
        collections(first: 10) {
          edges { node { id title handle } }
        }
        variants(first: 100) {
          edges {
            node {
              id title sku barcode price compareAtPrice
              inventoryQuantity inventoryPolicy taxable
              createdAt updatedAt
            }
          }
        }
      }
    }
  }
}
"""


def transform(product_node):
    tags = product_node.get("tags") or []
    tag_cols = parse_product_tags(tags)
    collections = [
        {"id": strip_gid(e["node"]["id"]), "title": e["node"]["title"], "handle": e["node"]["handle"]}
        for e in product_node.get("collections", {}).get("edges", [])
    ]

    rows = []
    for edge in product_node.get("variants", {}).get("edges", []):
        v = edge["node"]
        price = float(v.get("price") or 0)
        compare = float(v.get("compareAtPrice") or 0) if v.get("compareAtPrice") else None
        is_on_sale = bool(compare and compare > price)
        discount_pct = round((compare - price) / compare * 100, 2) if is_on_sale else None

        rows.append({
            "variant_id":          strip_gid(v.get("id")),
            "product_id":          strip_gid(product_node.get("id")),
            "product_title":       product_node.get("title"),
            "variant_title":       v.get("title"),
            "sku":                 v.get("sku"),
            "barcode":             v.get("barcode"),
            "price":               price,
            "compare_at_price":    compare,
            "is_on_sale":          is_on_sale,
            "discount_pct":        discount_pct,
            "inventory_quantity":  int(v.get("inventoryQuantity") or 0),
            "inventory_policy":    v.get("inventoryPolicy"),
            "taxable":             bool(v.get("taxable", False)),
            "product_type":        product_node.get("productType"),
            "vendor":              product_node.get("vendor"),
            "status":              product_node.get("status"),
            "product_tags":        json.dumps(tags),
            "tag_colour":          tag_cols.get("tag_colour"),
            "tag_department":      tag_cols.get("tag_department"),
            "tag_gender":          tag_cols.get("tag_gender"),
            "tag_season":          tag_cols.get("tag_season"),
            "tag_category":        tag_cols.get("tag_category"),
            "tag_price_status":    tag_cols.get("tag_price_status"),
            "retail_pro_id":       tag_cols.get("retail_pro_id"),
            "collections":         json.dumps(collections),
            "published_at":        product_node.get("publishedAt"),
            "created_at":          product_node.get("createdAt"),
            "updated_at":          product_node.get("updatedAt"),
            "variant_created_at":  v.get("createdAt"),
            "variant_updated_at":  v.get("updatedAt"),
            "ingested_at":         datetime.now(timezone.utc).isoformat(),
        })
    return rows


def run(bq_client, dry_run=False):
    # Always full sync — products are relatively few and need tag/collection refresh
    print("  Shopify products: full sync")
    all_rows = []
    for nodes in paginate(QUERY, "products"):
        for node in nodes:
            all_rows.extend(transform(node))

    print(f"  Fetched {len(all_rows)} product variants")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY, "products", "variant_id")
    print(f"  Upserted {count} product variants")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    run(client, dry_run=dry)
