# flake8: noqa: E501
import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from operators.review_translation import (
    ReviewTranslationToGCSOperator,
    ReviewTranslationToBQOperator,
)
from operators.review_aspect_scoring import (
    ReviewAspectScoringToGCSOperator,
    ReviewAspectScoringToBQOperator,
)

# --------------------------------------------------------------------------------
# 1) LOAD YAML CONFIG
# --------------------------------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "etl_pipeline_config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=config["dag_id"],
    default_args=default_args,
    description="ETL pipeline for Olist data",
    schedule_interval=config["schedule_interval"],
    start_date=datetime.strptime(config["start_date"], "%Y-%m-%d"),
    catchup=False,
    tags=["olist"],
)

tasks = {}

# Use DAG context for all task groups
with dag:
    # --------------------------------------------------------------------------------
    # 2) LOAD TO STAGING (CSV -> GCS -> BigQuery)
    # --------------------------------------------------------------------------------
    with TaskGroup(
        group_id="staging_load", tooltip="Load CSV files to staging tables"
    ) as staging_group:
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
                # Special handling for product category translation table
                schema_fields=(
                    [
                        (
                            {
                                "name": "product_category_name",
                                "type": "STRING",
                                "mode": "NULLABLE",
                            }
                            if table_name == "product_category_name_translation"
                            else None
                        ),
                        (
                            {
                                "name": "product_category_name_english",
                                "type": "STRING",
                                "mode": "NULLABLE",
                            }
                            if table_name == "product_category_name_translation"
                            else None
                        ),
                    ]
                    if table_name == "product_category_name_translation"
                    else None
                ),
                gcp_conn_id="google_cloud_default",
                dag=dag,
            )

            tasks[upload_task_id] = upload_to_gcs
            tasks[load_task_id] = load_to_bq

            # Modify the dependencies to only link upload_to_gcs -> load_to_bq
            upload_to_gcs >> load_to_bq

        tasks["staging_load"] = staging_group

    # --------------------------------------------------------------------------------
    # 4) CLEAN TASKS (DUMMY SELECT *), EXCEPT ORDER_REVIEWS HAS NEW COLUMNS
    # --------------------------------------------------------------------------------
    with TaskGroup(
        group_id="data_cleaning", tooltip="Clean and prepare tables"
    ) as cleaning_group:
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
            tasks[f"load_{tbl}_to_bq"] >> tasks[task_id]

        # Now define a special "clean" for order_reviews that adds two columns
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

        tasks["data_cleaning"] = cleaning_group

    # --------------------------------------------------------------------------------
    # 5) ENRICH REVIEWS WITH TRANSLATIONS AND ASPECT SCORES
    # --------------------------------------------------------------------------------
    with TaskGroup(
        group_id="review_enrichment",
        tooltip="Enrich reviews with translations and scores",
    ) as review_enrichment:
        # First translate reviews
        translate_to_gcs = ReviewTranslationToGCSOperator(
            task_id="translate_to_gcs",
            project_id=config["tasks"][4]["bq_project"],
            dataset_id=config["tasks"][4]["bq_dataset"].replace("_staging", "_clean"),
            table_id=config["tasks"][4]["bq_table"],
            dag=dag,
        )

        translate_to_bq = ReviewTranslationToBQOperator(
            task_id="translate_to_bq",
            project_id=config["tasks"][4]["bq_project"],
            dataset_id=config["tasks"][4]["bq_dataset"].replace("_staging", "_clean"),
            table_id=config["tasks"][4]["bq_table"],
            dag=dag,
        )

        # Then score aspects
        score_to_gcs = ReviewAspectScoringToGCSOperator(
            task_id="score_to_gcs",
            project_id=config["tasks"][4]["bq_project"],
            dataset_id=config["tasks"][4]["bq_dataset"].replace("_staging", "_clean"),
            table_id=config["tasks"][4]["bq_table"],
            dag=dag,
        )

        score_to_bq = ReviewAspectScoringToBQOperator(
            task_id="score_to_bq",
            project_id=config["tasks"][4]["bq_project"],
            dataset_id=config["tasks"][4]["bq_dataset"].replace("_staging", "_clean"),
            table_id=config["tasks"][4]["bq_table"],
            dag=dag,
        )

        tasks["translate_to_gcs"] = translate_to_gcs
        tasks["translate_to_bq"] = translate_to_bq
        tasks["score_to_gcs"] = score_to_gcs
        tasks["score_to_bq"] = score_to_bq

        # Set up the sequence
        translate_to_gcs >> translate_to_bq >> score_to_gcs >> score_to_bq

        tasks["review_enrichment"] = review_enrichment

    # --------------------------------------------------------------------------------
    # 6) AGGREGATION TASKS
    # --------------------------------------------------------------------------------
    with TaskGroup(
        group_id="aggregations", tooltip="Create aggregated tables"
    ) as agg_group:
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
                            COUNT(oi.order_item_id) AS total_items_sold,
                            COUNT(oi.order_item_id) / COUNT(DISTINCT o.order_id) AS avg_items_per_order
                          FROM `correlion.olist_clean.orders` o
                          LEFT JOIN `correlion.olist_clean.order_items` oi
                            ON o.order_id = oi.order_id
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
                            DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
                            FORMAT_DATE('%Y%m', DATE(o.order_purchase_timestamp)) AS year_month,
                            c.customer_city AS city,
                            c.customer_state AS state,
                            COUNT(DISTINCT o.order_id) AS total_orders,
                            SUM(oi.price) AS total_revenue,
                            COUNT(DISTINCT o.customer_id) AS unique_customers,
                            COUNT(oi.order_item_id) AS total_items_sold,
                            COUNT(oi.order_item_id) / COUNT(DISTINCT o.order_id) AS avg_items_per_order,
                            AVG(rv.review_score) AS avg_review_score,
                            SUM(oi.freight_value) AS total_freight_value,
                            AVG(oi.freight_value) AS avg_freight_value
                          FROM `correlion.olist_clean.orders` o
                          JOIN `correlion.olist_clean.customers` c
                            ON o.customer_id = c.customer_id
                          LEFT JOIN `correlion.olist_clean.order_items` oi
                            ON o.order_id = oi.order_id
                          LEFT JOIN `correlion.olist_clean.order_reviews` rv
                            ON o.order_id = rv.order_id
                          GROUP BY 1, 2, 3, 4
                        )
                        SELECT *
                        FROM city_agg
                        ORDER BY order_month, city, state;
                    """,
                    "useLegacySql": False,
                }
            },
            dag=dag,
        )

        agg_aspect_score_by_product = BigQueryInsertJobOperator(
            task_id="agg_aspect_score_by_product",
            gcp_conn_id="google_cloud_default",
            configuration={
                "query": {
                    "query": """
                        CREATE OR REPLACE TABLE `correlion.olist_aggregated.agg_aspect_score_by_product` AS
                        WITH review_scores AS (
                          SELECT
                            p.product_id,
                            t.product_category_name_english AS product_category_name_en,
                            EXTRACT(YEAR FROM r.review_answer_timestamp) AS review_year,
                            EXTRACT(MONTH FROM r.review_answer_timestamp) AS review_month,
                            DATE_TRUNC(DATE(r.review_answer_timestamp), MONTH) AS review_month_date,
                            
                            -- Review counts
                            COUNT(r.review_id) AS total_reviews,
                            COUNTIF(r.review_aspect_scores IS NOT NULL) AS num_reviews_with_aspect_scores,
                            
                            -- Classic star rating average
                            AVG(r.review_score) AS avg_review_score,
                            
                            -- Average aspect scores
                            AVG(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.delivery') AS FLOAT64)) AS avg_delivery_score,
                            AVG(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.product_quality') AS FLOAT64)) AS avg_product_quality_score,
                            AVG(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.customer_service') AS FLOAT64)) AS avg_customer_service_score,
                            AVG(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.refund_process') AS FLOAT64)) AS avg_refund_process_score,
                            AVG(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.packaging_condition') AS FLOAT64)) AS avg_packaging_condition_score,
                            
                            -- Count of negative aspect scores (< 0)
                            COUNTIF(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.delivery') AS FLOAT64) < 0) AS count_negative_delivery,
                            COUNTIF(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.product_quality') AS FLOAT64) < 0) AS count_negative_product_quality,
                            COUNTIF(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.customer_service') AS FLOAT64) < 0) AS count_negative_customer_service,
                            COUNTIF(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.refund_process') AS FLOAT64) < 0) AS count_negative_refund_process,
                            COUNTIF(CAST(JSON_EXTRACT_SCALAR(r.review_aspect_scores, '$.packaging_condition') AS FLOAT64) < 0) AS count_negative_packaging_condition

                          FROM `correlion.olist_clean.order_reviews` AS r
                          JOIN `correlion.olist_clean.order_items` AS i
                            ON r.order_id = i.order_id
                          JOIN `correlion.olist_clean.products` AS p
                            ON i.product_id = p.product_id
                          JOIN `correlion.olist_clean.product_category_name_translation` AS t
                            ON p.product_category_name = t.product_category_name

                          WHERE r.review_aspect_scores IS NOT NULL

                          GROUP BY
                            p.product_id,
                            t.product_category_name_english,
                            review_year,
                            review_month,
                            review_month_date
                        )
                        SELECT
                          product_id,
                          product_category_name_en,
                          review_year,
                          review_month,
                          review_month_date,
                          total_reviews,
                          num_reviews_with_aspect_scores,
                          avg_review_score,
                          avg_delivery_score,
                          avg_product_quality_score,
                          avg_customer_service_score,
                          avg_refund_process_score,
                          avg_packaging_condition_score,
                          count_negative_delivery,
                          count_negative_product_quality,
                          count_negative_customer_service,
                          count_negative_refund_process,
                          count_negative_packaging_condition
                        FROM review_scores
                        ORDER BY
                          review_month_date,
                          product_id;
                    """,
                    "useLegacySql": False,
                }
            },
            dag=dag,
        )

        tasks["agg_sales_daily"] = agg_sales_daily
        tasks["agg_sales_by_city"] = agg_sales_by_city
        tasks["agg_aspect_score_by_product"] = agg_aspect_score_by_product

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

        # Set up dependencies for aspect score aggregation
        [
            tasks["clean_order_items"],
            tasks["clean_products"],
            tasks["clean_product_category_name_translation"],
            score_to_bq,
        ] >> agg_aspect_score_by_product

        tasks["aggregations"] = agg_group

# --------------------------------------------------------------------------------
# Set up dependencies between task groups
# --------------------------------------------------------------------------------

(
    tasks["staging_load"]
    >> tasks["data_cleaning"]
    >> tasks["review_enrichment"]
    >> tasks["aggregations"]
)
