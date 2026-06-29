from datetime import datetime, timezone

from shopify.client import paginate, strip_gid
from bigquery.upsert import upsert
from bigquery.client import get_last_run, record_run
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE = "shopify.discounts"

QUERY = """
query GetDiscounts($cursor: String) {
  codeDiscountNodes(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        codeDiscount {
          __typename
          ... on DiscountCodeBasic {
            title status asyncUsageCount usageLimit appliesOncePerCustomer
            codes(first: 10) { edges { node { id code } } }
            customerGets {
              value {
                ... on DiscountPercentage { percentage }
                ... on DiscountAmount { amount { amount currencyCode } }
              }
            }
            minimumRequirement {
              ... on DiscountMinimumQuantity { greaterThanOrEqualToQuantity }
              ... on DiscountMinimumSubtotal { greaterThanOrEqualToSubtotal { amount currencyCode } }
            }
            startsAt endsAt createdAt updatedAt
          }
          ... on DiscountCodeFreeShipping {
            title status asyncUsageCount usageLimit appliesOncePerCustomer
            codes(first: 10) { edges { node { id code } } }
            startsAt endsAt createdAt updatedAt
          }
          ... on DiscountCodeBxgy {
            title status asyncUsageCount usageLimit appliesOncePerCustomer
            codes(first: 10) { edges { node { id code } } }
            startsAt endsAt createdAt updatedAt
          }
        }
      }
    }
  }
}
"""


def transform(node):
    discount_id = strip_gid(node.get("id"))
    d = node.get("codeDiscount") or {}
    typename = d.get("__typename", "")

    value_type = value_pct = value_amt = value_currency = None
    min_type = min_value = None

    if typename == "DiscountCodeBasic":
        gets = d.get("customerGets") or {}
        val = gets.get("value") or {}
        if "percentage" in val:
            value_type = "PERCENTAGE"
            value_pct = float(val["percentage"])
        elif "amount" in val:
            value_type = "FIXED_AMOUNT"
            value_amt = float((val.get("amount") or {}).get("amount") or 0)
            value_currency = (val.get("amount") or {}).get("currencyCode")

        req = d.get("minimumRequirement") or {}
        if "greaterThanOrEqualToQuantity" in req:
            min_type = "QUANTITY"
            min_value = float(req["greaterThanOrEqualToQuantity"])
        elif "greaterThanOrEqualToSubtotal" in req:
            min_type = "SUBTOTAL"
            min_value = float((req.get("greaterThanOrEqualToSubtotal") or {}).get("amount") or 0)
    elif typename == "DiscountCodeFreeShipping":
        value_type = "FREE_SHIPPING"
    elif typename == "DiscountCodeBxgy":
        value_type = "BXGY"

    rows = []
    for code_edge in d.get("codes", {}).get("edges", []):
        code_node = code_edge["node"]
        rows.append({
            "discount_id":               discount_id,
            "code_id":                   strip_gid(code_node.get("id")),
            "code":                      code_node.get("code"),
            "title":                     d.get("title"),
            "discount_type":             typename,
            "status":                    d.get("status"),
            "usage_count":               int(d.get("asyncUsageCount") or 0),
            "usage_limit":               int(d.get("usageLimit")) if d.get("usageLimit") is not None else None,
            "applies_once_per_customer": bool(d.get("appliesOncePerCustomer", False)),
            "value_type":                value_type,
            "value_percentage":          value_pct,
            "value_amount":              value_amt,
            "value_currency":            value_currency,
            "min_requirement_type":      min_type,
            "min_requirement_value":     min_value,
            "starts_at":                 d.get("startsAt"),
            "ends_at":                   d.get("endsAt"),
            "created_at":                d.get("createdAt"),
            "updated_at":                d.get("updatedAt"),
            "ingested_at":               datetime.now(timezone.utc).isoformat(),
        })
    return rows


def run(bq_client, dry_run=False):
    print("  Shopify discounts: full sync")

    all_rows = []
    for nodes in paginate(QUERY, "codeDiscountNodes"):
        for node in nodes:
            all_rows.extend(transform(node))

    print(f"  Fetched {len(all_rows)} discount codes")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        return len(all_rows)

    count = upsert(bq_client, all_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY,
                   "discounts", "code_id")
    print(f"  Upserted {count} discount codes")
    record_run(bq_client, TABLE, count, "success")
    return count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    dry = "--dry-run" in sys.argv
    run(client, dry_run=dry)
