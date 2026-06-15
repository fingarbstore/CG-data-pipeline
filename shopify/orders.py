import json
from datetime import datetime, timezone

from shopify.client import paginate, strip_gid
from bigquery.upsert import upsert, append_deduped
from bigquery.client import get_last_run, record_run
from config.settings import GCP_PROJECT_ID, BQ_DATASET_SHOPIFY

TABLE_ORDERS = "shopify.orders"
TABLE_LINE_ITEMS = "shopify.order_line_items"

QUERY = """
query GetOrders($cursor: String, $query: String) {
  orders(first: 250, after: $cursor, query: $query) {
    pageInfo { hasNextPage endCursor }
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
        lineItems(first: 50) {
          edges {
            node {
              id title quantity sku variantTitle
              originalUnitPriceSet { shopMoney { amount } }
              discountedUnitPriceSet { shopMoney { amount } }
              totalDiscountSet { shopMoney { amount } }
              product { id productType tags }
              variant { id }
            }
          }
        }
      }
    }
  }
}
"""


def money(obj):
    if not obj:
        return None
    return float(obj.get("shopMoney", {}).get("amount") or 0)


def transform_order(node):
    shipping = node.get("shippingAddress") or {}
    customer = node.get("customer") or {}
    return {
        "order_id":           strip_gid(node.get("id")),
        "order_name":         node.get("name"),
        "customer_id":        strip_gid(customer.get("id")),
        "customer_email":     (node.get("email") or "").lower() or None,
        "created_at":         node.get("createdAt"),
        "updated_at":         node.get("updatedAt"),
        "cancelled_at":       node.get("cancelledAt"),
        "closed_at":          node.get("closedAt"),
        "financial_status":   node.get("displayFinancialStatus"),
        "fulfillment_status": node.get("displayFulfillmentStatus"),
        "total_price":        money(node.get("totalPriceSet")),
        "subtotal_price":     money(node.get("subtotalPriceSet")),
        "total_discounts":    money(node.get("totalDiscountsSet")),
        "total_shipping":     money(node.get("totalShippingPriceSet")),
        "total_tax":          money(node.get("totalTaxSet")),
        "total_refunded":     money(node.get("totalRefundedSet")),
        "currency":           (node.get("totalPriceSet") or {}).get("shopMoney", {}).get("currencyCode"),
        "discount_codes":     json.dumps(node.get("discountCodes") or []),
        "shipping_city":      shipping.get("city"),
        "shipping_province":  shipping.get("province"),
        "shipping_country":   shipping.get("country"),
        "shipping_zip":       shipping.get("zip"),
        "tags":               json.dumps(node.get("tags") or []),
        "note":               node.get("note"),
        "ingested_at":        datetime.now(timezone.utc).isoformat(),
    }


def transform_line_items(order_node):
    order_id = strip_gid(order_node.get("id"))
    order_name = order_node.get("name")
    customer = order_node.get("customer") or {}
    customer_id = strip_gid(customer.get("id"))
    created_at = order_node.get("createdAt")
    rows = []

    for edge in order_node.get("lineItems", {}).get("edges", []):
        item = edge["node"]
        product = item.get("product") or {}
        variant = item.get("variant") or {}
        rows.append({
            "line_item_id":          strip_gid(item.get("id")),
            "order_id":              order_id,
            "order_name":            order_name,
            "customer_id":           customer_id,
            "created_at":            created_at,
            "product_id":            strip_gid(product.get("id")),
            "variant_id":            strip_gid(variant.get("id")),
            "sku":                   item.get("sku"),
            "title":                 item.get("title"),
            "variant_title":         item.get("variantTitle"),
            "quantity":              int(item.get("quantity") or 0),
            "original_unit_price":   money(item.get("originalUnitPriceSet")),
            "discounted_unit_price": money(item.get("discountedUnitPriceSet")),
            "total_discount":        money(item.get("totalDiscountSet")),
            "product_type":          product.get("productType"),
            "product_tags":          json.dumps(product.get("tags") or []),
            "ingested_at":           datetime.now(timezone.utc).isoformat(),
        })
    return rows


def run(bq_client, since=None, full=False, dry_run=False):
    from datetime import date

    if full:
        query_filter = "status:any"
        print("  Shopify orders: full sync")
    else:
        if since is None:
            last_run = get_last_run(bq_client, TABLE_ORDERS)
            since = last_run if last_run else None

        if since:
            query_filter = f"status:any updated_at:>{since}"
            print(f"  Shopify orders: incremental updated_at > {since}")
        else:
            query_filter = "status:any"
            print("  Shopify orders: no prior run — full sync")

    order_rows = []
    line_item_rows = []

    for nodes in paginate(QUERY, "orders", variables={"query": query_filter}):
        for node in nodes:
            order_rows.append(transform_order(node))
            line_item_rows.extend(transform_line_items(node))

    print(f"  Fetched {len(order_rows)} orders, {len(line_item_rows)} line items")

    if dry_run:
        print("  [dry_run] Skipping BQ upsert")
        return len(order_rows)

    if not order_rows:
        record_run(bq_client, TABLE_ORDERS, 0, "success")
        record_run(bq_client, TABLE_LINE_ITEMS, 0, "success")
        return 0

    print("  Upserting orders...")
    order_count = upsert(bq_client, order_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY,
                         "orders", "order_id")

    print("  Upserting line items...")
    line_item_count = upsert(bq_client, line_item_rows, GCP_PROJECT_ID, BQ_DATASET_SHOPIFY,
                             "order_line_items", "line_item_id")

    print(f"  Done: {order_count} orders, {line_item_count} line items")
    record_run(bq_client, TABLE_ORDERS, order_count, "success")
    record_run(bq_client, TABLE_LINE_ITEMS, line_item_count, "success")
    return order_count


if __name__ == "__main__":
    import sys
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT_ID)
    full = "--full" in sys.argv
    dry = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    since = args[0] if args else None
    run(client, since=since, full=full, dry_run=dry)
