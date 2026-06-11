import os
from dotenv import load_dotenv

load_dotenv()

# Shopify
SHOPIFY_STORE_URL = os.environ["SHOPIFY_STORE_URL"]
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
SHOPIFY_API_VERSION = "2025-01"
SHOPIFY_GRAPHQL_URL = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Stamped
STAMPED_API_KEY = os.environ["STAMPED_API_KEY"]
STAMPED_SHOP_ID = os.environ["STAMPED_SHOP_ID"]
STAMPED_BASE_URL = "https://stamped.io/api/v3"

# BigQuery
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_SHOPIFY = os.getenv("BQ_DATASET_SHOPIFY", "shopify")
BQ_DATASET_STAMPED = os.getenv("BQ_DATASET_STAMPED", "stamped")
BQ_DATASET_METADATA = "pipeline_metadata"
BQ_DATASET_ANALYTICS = "analytics"
