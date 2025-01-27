# flake8: noqa: E501
from datetime import datetime, timedelta
from airflow import DAG
from operators.review_translation import (
    ReviewTranslationToGCSOperator,
    ReviewTranslationToBQOperator,
)
from operators.review_aspect_scoring import (
    ReviewAspectScoringToGCSOperator,
    ReviewAspectScoringToBQOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "review_processing",
    default_args=default_args,
    description="Process Olist reviews with translations and aspect scoring",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "reviews"],
) as dag:

    # Translation tasks
    translate_to_gcs = ReviewTranslationToGCSOperator(
        task_id="translate_to_gcs",
        project_id="correlion",
        dataset_id="olist",
        table_id="reviews",
    )

    translate_to_bq = ReviewTranslationToBQOperator(
        task_id="translate_to_bq",
        project_id="correlion",
        dataset_id="olist",
        table_id="reviews",
    )

    # Aspect scoring tasks
    score_to_gcs = ReviewAspectScoringToGCSOperator(
        task_id="score_to_gcs",
        project_id="correlion",
        dataset_id="olist",
        table_id="reviews",
    )

    score_to_bq = ReviewAspectScoringToBQOperator(
        task_id="score_to_bq",
        project_id="correlion",
        dataset_id="olist",
        table_id="reviews",
    )

    # Chain the tasks
    translate_to_gcs >> translate_to_bq >> score_to_gcs >> score_to_bq
