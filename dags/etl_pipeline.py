import os
import yaml
from datetime import datetime
from airflow import DAG

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from operators.review_translation import ReviewTranslationOperator
from operators.review_aspect_scoring import ReviewAspectScoringOperator

# --------------------------------------------------------------------------------
# 1) LOAD YAML CONFIG
# --------------------------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "etl_pipeline_config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 0}

dag = DAG(
    dag_id=config["dag_id"],
    start_date=datetime.strptime(config["start_date"], "%Y-%m-%d"),
    schedule_interval=config["schedule_interval"],
    default_args=default_args,
    catchup=False,
)

tasks = {}

# --------------------------------------------------------------------------------
# 2) LOAD TO STAGING (CSV -> GCS -> BigQuery)
# --------------------------------------------------------------------------------
# The rest of the tasks in config are "load_*" tasks
for task_cfg in config["tasks"]:
    table_name = task_cfg["bq_table"]

    # 3a) Upload CSV to GCS
    upload_task_id = f"load_{table_name}_to_gcs"
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id=upload_task_id,
        src=task_cfg["csv_file_path"],
        dst=task_cfg["gcs_object_name"],
        bucket=task_cfg["gcs_bucket"],
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    # 3b) Load GCS file into BigQuery (staging)
    load_task_id = f"load_{table_name}_to_bq"
    load_to_bq = GCSToBigQueryOperator(
        task_id=load_task_id,
        bucket=task_cfg["gcs_bucket"],
        source_objects=[task_cfg["gcs_object_name"]],
        destination_project_dataset_table=f'{task_cfg["bq_project"]}.{task_cfg["bq_dataset"]}.{task_cfg["bq_table"]}',
        source_format="CSV",
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    tasks[upload_task_id] = upload_to_gcs
    tasks[load_task_id] = load_to_bq

    # Modify the dependencies to only link upload_to_gcs -> load_to_bq
    upload_to_gcs >> load_to_bq

# --------------------------------------------------------------------------------
# 4) CLEAN TASKS (DUMMY SELECT *), EXCEPT ORDER_REVIEWS HAS NEW COLUMNS
# --------------------------------------------------------------------------------

# You can list your tables that need a simple pass-through clean
pass_through_tables = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "orders",
    "products",
    "sellers",
    "product_category_name_translation",
]

# For each pass-through table, create a "clean" task
for tbl in pass_through_tables:
    task_id = f"clean_{tbl}"
    tasks[task_id] = BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `correlion.olist_clean.{tbl}` AS
                    SELECT * 
                    FROM `correlion.olist_staging.{tbl}`;
                """,
                "useLegacySql": False,
            }
        },
        dag=dag,
    )
    # Make sure the cleaning depends on the staging load
    # e.g. tasks["load_customers_to_bq"] >> tasks["clean_customers"]
    tasks[f"load_{tbl}_to_bq"] >> tasks[task_id]

# Now define a special "clean" for order_reviews that adds two columns and removes duplicates
clean_order_reviews = BigQueryInsertJobOperator(
    task_id="clean_order_reviews",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE TABLE `correlion.olist_clean.order_reviews` AS
                WITH deduplicated AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_creation_date) as review_id_rank,
                        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_creation_date) as order_id_rank
                    FROM `correlion.olist_staging.order_reviews`
                )
                SELECT
                    review_id,
                    order_id,
                    review_score,
                    review_comment_title,
                    review_comment_message,
                    review_creation_date,
                    review_answer_timestamp,
                    -- Explicitly cast to STRING and JSON:
                    CAST(NULL AS STRING) AS review_comment_message_en,
                    CAST(NULL AS JSON) AS review_aspect_scores
                FROM deduplicated
                WHERE review_id_rank = 1 AND order_id_rank = 1;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

tasks["clean_order_reviews"] = clean_order_reviews
tasks["load_order_reviews_to_bq"] >> clean_order_reviews

# --------------------------------------------------------------------------------
# 5) ENRICH REVIEWS WITH TRANSLATIONS AND ASPECT SCORES
# --------------------------------------------------------------------------------
# First translate reviews
translate_reviews = ReviewTranslationOperator(
    task_id="translate_reviews",
    project_id="correlion",
    dataset_id="olist_clean",
    table_id="order_reviews",
    dag=dag,
)

# Then score aspects
score_review_aspects = ReviewAspectScoringOperator(
    task_id="score_review_aspects",
    project_id="correlion",
    dataset_id="olist_clean",
    table_id="order_reviews",
    dag=dag,
)

tasks["translate_reviews"] = translate_reviews
tasks["score_review_aspects"] = score_review_aspects

# Set up the sequence: clean -> translate -> score
clean_order_reviews >> translate_reviews >> score_review_aspects
# clean_order_reviews >> score_review_aspects

# --------------------------------------------------------------------------------
# 6) AGGREGATION TASKS
# --------------------------------------------------------------------------------
agg_sales_daily = BigQueryInsertJobOperator(
    task_id="agg_sales_daily",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE TABLE `correlion.olist_aggregated.agg_sales_daily` AS
                WITH daily_agg AS (
                  SELECT
                    DATE(o.order_purchase_timestamp) AS order_date,
                    COUNT(DISTINCT o.order_id) AS total_orders,
                    SUM(oi.price) AS total_revenue,
                    AVG(oi.freight_value) AS avg_freight_value,
                    AVG(rv.review_score) AS avg_review_score,
                    -- Total items sold: count of order_item_id
                    COUNT(oi.order_item_id) AS total_items_sold,
                    -- Average items per order: total_items_sold / total_orders
                    COUNT(oi.order_item_id) / COUNT(DISTINCT o.order_id) AS avg_items_per_order
                  FROM `correlion.olist_clean.orders` o
                  -- Join to order_items
                  LEFT JOIN `correlion.olist_clean.order_items` oi
                    ON o.order_id = oi.order_id
                  -- Join to order_reviews (to get review_score)
                  LEFT JOIN `correlion.olist_clean.order_reviews` rv
                    ON o.order_id = rv.order_id
                  GROUP BY 1
                )
                SELECT *
                FROM daily_agg;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

agg_sales_by_city = BigQueryInsertJobOperator(
    task_id="agg_sales_by_city",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE TABLE `correlion.olist_aggregated.agg_sales_by_city` AS
                WITH city_agg AS (
                  SELECT
                    -- Month-level (truncate to month)
                    DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
                    -- Also store a Year-Month string (e.g. '202501')
                    FORMAT_DATE('%Y%m', DATE(o.order_purchase_timestamp)) AS year_month,
                    
                    c.customer_city AS city,
                    c.customer_state AS state,
                    
                    COUNT(DISTINCT o.order_id) AS total_orders,
                    SUM(oi.price) AS total_revenue,
                    
                    -- Unique customers in that city/month
                    COUNT(DISTINCT o.customer_id) AS unique_customers,
                    
                    -- Total items sold (count of item rows)
                    COUNT(oi.order_item_id) AS total_items_sold,
                    -- Average items per order
                    COUNT(oi.order_item_id) / COUNT(DISTINCT o.order_id) AS avg_items_per_order,
                    
                    -- Review score
                    AVG(rv.review_score) AS avg_review_score,
                    
                    -- Freight
                    SUM(oi.freight_value) AS total_freight_value,
                    AVG(oi.freight_value) AS avg_freight_value
                  FROM `correlion.olist_clean.orders` o
                  JOIN `correlion.olist_clean.customers` c
                    ON o.customer_id = c.customer_id
                  LEFT JOIN `correlion.olist_clean.order_items` oi
                    ON o.order_id = oi.order_id
                  LEFT JOIN `correlion.olist_clean.order_reviews` rv
                    ON o.order_id = rv.order_id
                  GROUP BY
                    1, 2, 3, 4
                )
                SELECT
                  order_month,
                  year_month,
                  city,
                  state,
                  total_orders,
                  total_revenue,
                  unique_customers,
                  total_items_sold,
                  avg_items_per_order,
                  avg_review_score,
                  total_freight_value,
                  avg_freight_value
                FROM city_agg
                ORDER BY
                  order_month,
                  city,
                  state;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

tasks["agg_sales_daily"] = agg_sales_daily
tasks["agg_sales_by_city"] = agg_sales_by_city

# Set up dependencies for daily aggregation
[
    tasks["clean_orders"],
    tasks["clean_order_items"],
    clean_order_reviews,
] >> agg_sales_daily

# Set up dependencies for city aggregation (needs customers table too)
[
    tasks["clean_orders"],
    tasks["clean_customers"],
    tasks["clean_order_items"],
    clean_order_reviews,
] >> agg_sales_by_city
