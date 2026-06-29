"""
Daily pipeline — runs all incremental updates.
Scheduled via GitHub Actions at 02:00 UTC.
"""
import sys
import traceback
from datetime import datetime, timezone

from google.cloud import bigquery
from config.settings import GCP_PROJECT_ID


def run_step(name, fn, *args, **kwargs):
    print(f"\n{'='*50}")
    print(f"  {name}")
    print(f"{'='*50}")
    try:
        result = fn(*args, **kwargs)
        print(f"  ✓ {name} complete: {result} rows")
        return True, result
    except Exception as e:
        print(f"  ✗ {name} FAILED: {e}")
        traceback.print_exc()
        return False, 0


def main():
    start = datetime.now(timezone.utc)
    print(f"=== CG Data Pipeline — Daily Update ===")
    print(f"  Started: {start.isoformat()}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    results = {}
    failures = []

    # --- Shopify ---
    from shopify.customers import run as shopify_customers
    ok, n = run_step("Shopify customers (incremental)", shopify_customers, client)
    results["shopify_customers"] = n
    if not ok: failures.append("shopify_customers")

    from shopify.orders import run as shopify_orders
    ok, n = run_step("Shopify orders (incremental)", shopify_orders, client)
    results["shopify_orders"] = n
    if not ok: failures.append("shopify_orders")

    from shopify.products import run as shopify_products
    ok, n = run_step("Shopify products (full sync)", shopify_products, client)
    results["shopify_products"] = n
    if not ok: failures.append("shopify_products")

    from shopify.inventory import run as shopify_inventory
    ok, n = run_step("Shopify inventory snapshot", shopify_inventory, client)
    results["shopify_inventory"] = n
    if not ok: failures.append("shopify_inventory")

    from shopify.discounts import run as shopify_discounts
    ok, n = run_step("Shopify discounts (full sync)", shopify_discounts, client)
    results["shopify_discounts"] = n
    if not ok: failures.append("shopify_discounts")

    # --- Stamped ---
    from stamped.activities import run as stamped_activities
    ok, n = run_step("Stamped activities (incremental)", stamped_activities, client)
    results["stamped_activities"] = n
    if not ok: failures.append("stamped_activities")

    from stamped.customers import run as stamped_customers
    ok, n = run_step("Stamped customers (incremental)", stamped_customers, client)
    results["stamped_customers"] = n
    if not ok: failures.append("stamped_customers")

    from stamped.rewards import run as stamped_rewards
    ok, n = run_step("Stamped rewards (full sync)", stamped_rewards, client)
    results["stamped_rewards"] = n
    if not ok: failures.append("stamped_rewards")

    # --- Summary ---
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n{'='*50}")
    print(f"  Pipeline complete in {elapsed:.0f}s")
    print(f"{'='*50}")
    for k, v in results.items():
        status = "✗ FAILED" if k in failures else "✓"
        print(f"  {status}  {k}: {v} rows")

    if failures:
        print(f"\n  FAILED steps: {', '.join(failures)}")
        sys.exit(1)
    else:
        print(f"\n  All steps succeeded.")


if __name__ == "__main__":
    main()
