from datetime import datetime, timezone

from shopify.client import paginate, strip_gid
from bigquery.upsert import upsert
from bigquery.client import record_run
from utils.transforms import parse_product_tags
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE = "shopify.collections"

# Fetch collections with enough products to determine department.
# Querying from the collections side (not product.collections) is the only
# way to get smart collection membership via the GraphQL API.
QUERY = """
query GetCollections($cursor: String) {
  collections(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id handle title
        productsCount { count }
        products(first: 100) {
          edges { node { tags } }
        }
      }
    }
  }
}
"""

WOMENSWEAR_DEPTS = {"Womenswear", "Home", "Beauty"}


def derive_department(product_edges):
    departments = set()
    for edge in product_edges:
        tags = edge["node"].get("tags") or []
        tag_cols = parse_product_tags(tags)
        dept = tag_cols.get("tag_department")
        if dept:
            departments.add(dept)

    if not departments:
        return None
    if departments == {"Menswear"}:
        return "Menswear"
    if departments <= WOMENSWEAR_DEPTS:
        return "Womenswear"
    if "Menswear" in departments and departments & WOMENSWEAR_DEPTS:
        return "Both"
    return None


def transform(node):
    product_edges = node.get("products", {}).get("edges", [])
    return {
        "collection_id": strip_gid(node.get("id")),
        "handle":        node.get("handle"),
        "title":         node.get("title"),
        "product_count": int((node.get("productsCount") or {}).get("count") or 0),
        "department":    derive_department(product_edges),
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
    }


def run(bq_client, dry_run=False):
    print("  Shopify collections: full sync")
    rows = []
    for nodes in paginate(QUERY, "collections"):
        for node in nodes:
            rows.append(transform(node))

    print(f"  Fetched {len(rows)} collections")

    if dry_run:
        sample = [r for r in rows if r["department"]][:5]
        for r in sample:
            print(f"    {r['handle']:40s} → {r['department']}")
        return len(rows)

    count = upsert(bq_client, rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY,
                   "collections", "collection_id")
    print(f"  Upserted {count} collections")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    run(client, dry_run=dry)
