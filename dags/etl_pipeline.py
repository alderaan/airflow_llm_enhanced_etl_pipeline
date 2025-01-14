import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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

# Now define a special "clean" for order_reviews that adds two columns
clean_order_reviews = BigQueryInsertJobOperator(
    task_id="clean_order_reviews",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE TABLE `correlion.olist_clean.order_reviews` AS
                SELECT
                    review_id,
                    order_id,
                    review_score,
                    review_comment_title,
                    review_comment_message,
                    review_creation_date,
                    review_answer_timestamp,
                    -- Explicitly cast to STRING and INT64:
                    CAST(NULL AS STRING) AS review_comment_message_en,
                    CAST(NULL AS INT64) AS review_sentiment_score
                FROM `correlion.olist_staging.order_reviews`;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

tasks["clean_order_reviews"] = clean_order_reviews
tasks["load_order_reviews_to_bq"] >> clean_order_reviews

# --------------------------------------------------------------------------------
# 5) ENRICH REVIEWS (SIMULATED BATCH LLM PROCESS AFTER CLEAN)
# --------------------------------------------------------------------------------
# For demo: we simulate waiting 24h and then updating the columns.
# In practice, you might run an external script to get real translations/sentiment.

enrich_order_reviews = BigQueryInsertJobOperator(
    task_id="enrich_order_reviews",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                -- Dummy example: update with some mock translations/sentiment
                UPDATE `correlion.olist_clean.order_reviews`
                SET 
                  review_comment_message_en = CONCAT('[EN] ', review_comment_message),
                  review_sentiment_score = 5  -- pretend everything is 5/5
                WHERE 1=1;
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

tasks["enrich_order_reviews"] = enrich_order_reviews

# Make sure the enrichment happens AFTER the clean task
clean_order_reviews >> enrich_order_reviews
