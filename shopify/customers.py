import json
from datetime import datetime, timezone, date, timedelta
from google.cloud import bigquery

from shopify.client import paginate, strip_gid
from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY
from utils.transforms import extract_stamped_tier

TABLE = "shopify.customers"

QUERY = """
query GetCustomers($cursor: String, $query: String) {
  customers(first: 250, after: $cursor, query: $query) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id email firstName lastName phone
        createdAt updatedAt numberOfOrders
        amountSpent { amount currencyCode }
        tags note verifiedEmail taxExempt
        emailMarketingConsent { marketingState marketingOptInLevel }
        smsMarketingConsent { marketingState }
        defaultAddress { city province country zip }
      }
    }
  }
}
"""


def transform(node):
    addr = node.get("defaultAddress") or {}
    email_consent = node.get("emailMarketingConsent") or {}
    sms_consent = node.get("smsMarketingConsent") or {}
    amount_spent = node.get("amountSpent") or {}
    tags = node.get("tags") or []

    return {
        "customer_id":                  strip_gid(node.get("id")),
        "email":                        (node.get("email") or "").lower() or None,
        "first_name":                   node.get("firstName"),
        "last_name":                    node.get("lastName"),
        "phone":                        node.get("phone"),
        "created_at":                   node.get("createdAt"),
        "updated_at":                   node.get("updatedAt"),
        "number_of_orders":             int(node.get("numberOfOrders") or 0),
        "total_spent":                  float(amount_spent.get("amount") or 0),
        "currency":                     amount_spent.get("currencyCode"),
        "email_marketing_state":        email_consent.get("marketingState"),
        "email_marketing_opt_in_level": email_consent.get("marketingOptInLevel"),
        "sms_marketing_state":          sms_consent.get("marketingState"),
        "verified_email":               bool(node.get("verifiedEmail", False)),
        "tax_exempt":                   bool(node.get("taxExempt", False)),
        "default_city":                 addr.get("city"),
        "default_province":             addr.get("province"),
        "default_country":              addr.get("country"),
        "default_zip":                  addr.get("zip"),
        "tags":                         json.dumps(tags),
        "stamped_vip_tier":             extract_stamped_tier(tags),
        "note":                         node.get("note"),
        "ingested_at":                  datetime.now(timezone.utc).isoformat(),
    }


def run(bq_client, since=None, full=False, dry_run=False):
    if full:
        query_filter = None
        print("  Shopify customers: full sync")
    else:
        if since is None:
            last_run = get_last_run(bq_client, TABLE)
            since = last_run if last_run else None

        if since:
            query_filter = f"updated_at:>{since}"
            print(f"  Shopify customers: incremental updated_at > {since}")
        else:
            query_filter = None
            print("  Shopify customers: no prior run — full sync")

    all_rows = []
    for nodes in paginate(QUERY, "customers", variables={"query": query_filter}):
        all_rows.extend(transform(n) for n in nodes)

    print(f"  Fetched {len(all_rows)} customers")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        return len(all_rows)

    if not all_rows:
        record_run(bq_client, TABLE, 0, "success")
        return 0

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY, "customers", "customer_id")
    print(f"  Upserted {count} Shopify customers")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    client = bigquery.Client(project=GCP_PROJECT_ID)
    full = "--full" in sys.argv
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if args else None
    run(client, since=since, full=full, dry_run=dry)
