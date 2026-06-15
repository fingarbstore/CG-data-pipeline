import time
import requests
from config.settings import SHOPIFY_GRAPHQL_URL, SHOPIFY_ACCESS_TOKEN

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json",
}


def run_query(query, variables=None, max_retries=5):
    payload = {"query": query, "variables": variables or {}}

    for attempt in range(max_retries):
        resp = requests.post(SHOPIFY_GRAPHQL_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")

        # Cost-aware rate limiting
        cost = data.get("extensions", {}).get("cost", {})
        available = cost.get("throttleStatus", {}).get("currentlyAvailable", 20000)
        if available < 2000:
            wait = (2000 - available) / cost.get("throttleStatus", {}).get("restoreRate", 1000)
            print(f"  Shopify rate limit — waiting {wait:.1f}s (available: {available})")
            time.sleep(wait)

        return data["data"]

    raise RuntimeError("Shopify GraphQL max retries exceeded")


def paginate(query, data_path, variables=None):
    """Paginate a GraphQL query. data_path is e.g. 'orders' or 'customers'."""
    cursor = None
    total = 0

    while True:
        vars_ = {**(variables or {}), "cursor": cursor}
        data = run_query(query, vars_)
        connection = data[data_path]
        edges = connection["edges"]

        if not edges:
            break

        nodes = [e["node"] for e in edges]
        yield nodes
        total += len(nodes)

        page_info = connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break

        cursor = page_info["endCursor"]
        print(f"  {data_path}: {total} fetched so far...")


def strip_gid(gid):
    if gid is None:
        return None
    return gid.split("/")[-1]
