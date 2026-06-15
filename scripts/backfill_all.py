"""
One-time backfill script — runs full historical loads for all tables.
Run this once to seed BigQuery. Daily updates use scripts/daily_update.py.

Usage:
  python3 -m scripts.backfill_all
  python3 -m scripts.backfill_all --skip-orders   # skip bulk orders (already done)
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
    skip_orders = "--skip-orders" in sys.argv

    start = datetime.now(timezone.utc)
    print(f"=== CG Data Pipeline — Full Backfill ===")
    print(f"  Started: {start.isoformat()}")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    failures = []

    # Shopify customers — full
    from shopify.customers import run as shopify_customers
    ok, _ = run_step("Shopify customers (full)", shopify_customers, client, full=True)
    if not ok: failures.append("shopify_customers")

    # Shopify orders — bulk operations backfill
    if not skip_orders:
        from shopify.bulk_orders import run as bulk_orders
        ok, _ = run_step("Shopify orders — bulk backfill", bulk_orders, client)
        if not ok: failures.append("shopify_orders_bulk")
    else:
        print("\n  Skipping bulk orders (--skip-orders)")

    # Shopify products — full sync
    from shopify.products import run as shopify_products
    ok, _ = run_step("Shopify products (full)", shopify_products, client)
    if not ok: failures.append("shopify_products")

    # Shopify inventory — today's snapshot
    from shopify.inventory import run as shopify_inventory
    ok, _ = run_step("Shopify inventory snapshot", shopify_inventory, client)
    if not ok: failures.append("shopify_inventory")

    # Stamped activities — full
    from stamped.activities import run as stamped_activities
    ok, _ = run_step("Stamped activities (full)", stamped_activities, client, full=True)
    if not ok: failures.append("stamped_activities")

    # Stamped customers — full
    from stamped.customers import run as stamped_customers
    ok, _ = run_step("Stamped customers (full)", stamped_customers, client, full=True)
    if not ok: failures.append("stamped_customers")

    # Stamped rewards — full sync
    from stamped.rewards import run as stamped_rewards
    ok, _ = run_step("Stamped rewards (full)", stamped_rewards, client)
    if not ok: failures.append("stamped_rewards")

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\n{'='*50}")
    print(f"  Backfill complete in {elapsed:.0f}s")
    if failures:
        print(f"  FAILED: {', '.join(failures)}")
        sys.exit(1)
    else:
        print(f"  All steps succeeded.")


if __name__ == "__main__":
    main()
