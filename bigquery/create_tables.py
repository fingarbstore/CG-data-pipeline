from google.cloud import bigquery
from google.api_core.exceptions import Conflict

def create_dataset(client, project, dataset_id):
    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = "EU"
    try:
        client.create_dataset(dataset)
        print(f"  Created dataset {dataset_id}")
    except Conflict:
        print(f"  Dataset {dataset_id} already exists")


def create_table(client, project, dataset_id, table_id, schema, partition_field=None, partition_type="DAY", cluster_fields=None):
    table_ref = f"{project}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    if partition_field:
        if partition_type == "DAY" and any(f.name == partition_field and f.field_type == "DATE" for f in schema):
            table.range_partitioning = None
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        else:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

    if cluster_fields:
        table.clustering_fields = cluster_fields

    try:
        client.create_table(table)
        print(f"  Created table {dataset_id}.{table_id}")
    except Conflict:
        print(f"  Table {dataset_id}.{table_id} already exists")


def create_all(project):
    client = bigquery.Client(project=project)

    print("Creating datasets...")
    for ds in ["shopify", "stamped", "pipeline_metadata", "analytics"]:
        create_dataset(client, project, ds)

    print("\nCreating shopify.orders...")
    create_table(client, project, "shopify", "orders", schema=[
        bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_name", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("customer_email", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("cancelled_at", "TIMESTAMP"),
        bigquery.SchemaField("closed_at", "TIMESTAMP"),
        bigquery.SchemaField("financial_status", "STRING"),
        bigquery.SchemaField("fulfillment_status", "STRING"),
        bigquery.SchemaField("total_price", "FLOAT"),
        bigquery.SchemaField("subtotal_price", "FLOAT"),
        bigquery.SchemaField("total_discounts", "FLOAT"),
        bigquery.SchemaField("total_shipping", "FLOAT"),
        bigquery.SchemaField("total_tax", "FLOAT"),
        bigquery.SchemaField("total_refunded", "FLOAT"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("discount_codes", "STRING"),
        bigquery.SchemaField("shipping_city", "STRING"),
        bigquery.SchemaField("shipping_province", "STRING"),
        bigquery.SchemaField("shipping_country", "STRING"),
        bigquery.SchemaField("shipping_zip", "STRING"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="created_at", cluster_fields=["customer_id", "financial_status"])

    print("Creating shopify.order_line_items...")
    create_table(client, project, "shopify", "order_line_items", schema=[
        bigquery.SchemaField("line_item_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("order_name", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("variant_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("variant_title", "STRING"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("original_unit_price", "FLOAT"),
        bigquery.SchemaField("discounted_unit_price", "FLOAT"),
        bigquery.SchemaField("total_discount", "FLOAT"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("product_tags", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="created_at", cluster_fields=["order_id", "sku"])

    print("Creating shopify.customers...")
    create_table(client, project, "shopify", "customers", schema=[
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("number_of_orders", "INTEGER"),
        bigquery.SchemaField("total_spent", "FLOAT"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("email_marketing_state", "STRING"),
        bigquery.SchemaField("email_marketing_opt_in_level", "STRING"),
        bigquery.SchemaField("sms_marketing_state", "STRING"),
        bigquery.SchemaField("verified_email", "BOOLEAN"),
        bigquery.SchemaField("tax_exempt", "BOOLEAN"),
        bigquery.SchemaField("default_city", "STRING"),
        bigquery.SchemaField("default_province", "STRING"),
        bigquery.SchemaField("default_country", "STRING"),
        bigquery.SchemaField("default_zip", "STRING"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("stamped_vip_tier", "STRING"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="created_at", cluster_fields=["email"])

    print("Creating shopify.products...")
    create_table(client, project, "shopify", "products", schema=[
        bigquery.SchemaField("variant_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("product_title", "STRING"),
        bigquery.SchemaField("variant_title", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("barcode", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("compare_at_price", "FLOAT"),
        bigquery.SchemaField("is_on_sale", "BOOLEAN"),
        bigquery.SchemaField("discount_pct", "FLOAT"),
        bigquery.SchemaField("inventory_quantity", "INTEGER"),
        bigquery.SchemaField("inventory_policy", "STRING"),
        bigquery.SchemaField("taxable", "BOOLEAN"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("vendor", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("product_tags", "STRING"),
        bigquery.SchemaField("tag_colour", "STRING"),
        bigquery.SchemaField("tag_department", "STRING"),
        bigquery.SchemaField("tag_gender", "STRING"),
        bigquery.SchemaField("tag_season", "STRING"),
        bigquery.SchemaField("tag_category", "STRING"),
        bigquery.SchemaField("tag_price_status", "STRING"),
        bigquery.SchemaField("retail_pro_id", "STRING"),
        bigquery.SchemaField("collections", "STRING"),
        bigquery.SchemaField("published_at", "TIMESTAMP"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("variant_created_at", "TIMESTAMP"),
        bigquery.SchemaField("variant_updated_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="created_at", cluster_fields=["product_type", "vendor", "status"])

    print("Creating shopify.inventory_snapshots...")
    create_table(client, project, "shopify", "inventory_snapshots", schema=[
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("variant_id", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("product_title", "STRING"),
        bigquery.SchemaField("variant_title", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("compare_at_price", "FLOAT"),
        bigquery.SchemaField("is_on_sale", "BOOLEAN"),
        bigquery.SchemaField("discount_pct", "FLOAT"),
        bigquery.SchemaField("inventory_quantity", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("vendor", "STRING"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("tag_season", "STRING"),
        bigquery.SchemaField("tag_price_status", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="snapshot_date", cluster_fields=["variant_id"])

    print("\nCreating stamped.customers...")
    create_table(client, project, "stamped", "customers", schema=[
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("platform_id", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("date_of_birth", "DATE"),
        bigquery.SchemaField("referral_code", "STRING"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("deleted", "BOOLEAN"),
        bigquery.SchemaField("date_deleted", "TIMESTAMP"),
        bigquery.SchemaField("date_platform_created", "TIMESTAMP"),
        bigquery.SchemaField("date_platform_updated", "TIMESTAMP"),
        bigquery.SchemaField("date_stamped_created", "TIMESTAMP"),
        bigquery.SchemaField("date_stamped_updated", "TIMESTAMP"),
        bigquery.SchemaField("total_points", "INTEGER"),
        bigquery.SchemaField("vip_tier", "STRING"),
        bigquery.SchemaField("total_affiliate_orders", "INTEGER"),
        bigquery.SchemaField("date_total_points_updated", "TIMESTAMP"),
        bigquery.SchemaField("date_vip_tier_updated", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="date_stamped_created", cluster_fields=["platform_id", "vip_tier"])

    print("Creating stamped.activities...")
    create_table(client, project, "stamped", "activities", schema=[
        bigquery.SchemaField("activity_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("customer_email", "STRING"),
        bigquery.SchemaField("shop_id", "STRING"),
        bigquery.SchemaField("event", "STRING"),
        bigquery.SchemaField("points_debit", "INTEGER"),
        bigquery.SchemaField("points_credit", "INTEGER"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("rule_correlation_id", "STRING"),
        bigquery.SchemaField("relationship_id", "STRING"),
        bigquery.SchemaField("source_action", "STRING"),
        bigquery.SchemaField("source_id", "STRING"),
        bigquery.SchemaField("reference", "STRING"),
        bigquery.SchemaField("reference_hash", "STRING"),
        bigquery.SchemaField("date_created", "TIMESTAMP"),
        bigquery.SchemaField("date_analytics", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="date_created", cluster_fields=["customer_id", "event"])

    print("Creating stamped.rewards...")
    create_table(client, project, "stamped", "rewards", schema=[
        bigquery.SchemaField("reward_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("activity_id", "STRING"),
        bigquery.SchemaField("rule_correlation_id", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("relationship_id", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("code", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("value", "FLOAT"),
        bigquery.SchemaField("service", "STRING"),
        bigquery.SchemaField("service_id", "STRING"),
        bigquery.SchemaField("profile", "STRING"),
        bigquery.SchemaField("date_created", "TIMESTAMP"),
        bigquery.SchemaField("date_updated", "TIMESTAMP"),
        bigquery.SchemaField("date_expire", "TIMESTAMP"),
        bigquery.SchemaField("date_analytics", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ], partition_field="date_created", cluster_fields=["customer_id", "status"])

    print("\nCreating pipeline_metadata.run_log...")
    create_table(client, project, "pipeline_metadata", "run_log", schema=[
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("last_run_at", "TIMESTAMP"),
        bigquery.SchemaField("rows_processed", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("run_at", "TIMESTAMP"),
    ])

    print("\nAll tables created successfully.")


if __name__ == "__main__":
    from config.settings import GCP_PROJECT_ID
    create_all(GCP_PROJECT_ID)
