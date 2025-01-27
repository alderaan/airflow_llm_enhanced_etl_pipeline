# flake8: noqa: E501
import json
import time
from typing import List, Dict
import pandas as pd
from openai import OpenAI
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import BaseOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


class ReviewTranslationToGCSOperator(BaseOperator):
    """Translates reviews and uploads results to GCS."""

    def __init__(
        self, task_id: str, project_id: str, dataset_id: str, table_id: str, **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.client = OpenAI()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.gcp_conn_id = "google_cloud_default"

    def _fetch_reviews(self) -> pd.DataFrame:
        """Fetch reviews that need translation."""
        self.log.info("Starting to fetch reviews...")

        bq = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

        query = f"""
            SELECT review_id, review_comment_message
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE review_comment_message IS NOT NULL
              AND review_comment_message_en IS NULL
              LIMIT 10000
        """

        self.log.info("project is: " + str(self.project_id))
        df = bq.get_pandas_df(
            sql=query,
            dialect="standard",
            configuration={
                "query": {
                    "useLegacySql": False,
                    "defaultDataset": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_id,
                    },
                }
            },
        )
        self.log.info(f"Fetched {len(df)} reviews")
        return df

    def _prepare_batch_requests(self, reviews: pd.DataFrame) -> List[Dict]:
        """Convert reviews to JSONL format for batch processing."""
        requests = []
        seen_ids = set()  # Track seen IDs

        for idx, row in reviews.iterrows():
            review_id = row["review_id"]

            # Skip if we've seen this ID before
            if review_id in seen_ids:
                self.log.warning(f"Skipping duplicate review_id: {review_id}")
                continue

            seen_ids.add(review_id)

            request = {
                "custom_id": review_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-3.5-turbo-0125",
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a translator. "
                                "Translate the following Portuguese text to English. "
                                "Respond with only the translation, no explanations."
                            ),
                        },
                        {
                            "role": "user",
                            "content": row["review_comment_message"],
                        },
                    ],
                    "max_tokens": 1000,
                },
            }
            requests.append(request)

        self.log.info(f"Prepared {len(requests)} unique requests")
        return requests

    def _create_batch_file(self, requests: List[Dict]) -> str:
        """Create JSONL file and upload to OpenAI."""
        local_file = "tmp/translations_input.jsonl"

        with open(local_file, "w") as f:
            for req in requests:
                f.write(json.dumps(req) + "\n")

        self.log.info(f"Saved batch input file locally to: {local_file}")

        with open(local_file, "rb") as f:
            file = self.client.files.create(file=f, purpose="batch")

        return file.id

    def _process_batch_results(self, output_file_id: str, context):
        """Process batch results and upload to GCS."""
        content = self.client.files.content(output_file_id)

        # Save output file locally
        jsonl_file = "tmp/translations_output.jsonl"
        with open(jsonl_file, "w") as f:
            f.write(content.text)
        self.log.info(f"Saved batch output file locally to: {jsonl_file}")

        # Process translations
        translations = {}
        for line in content.text.strip().split("\n"):
            result = json.loads(line)
            review_id = result["custom_id"]
            if result["response"]["status_code"] == 200:
                translation = result["response"]["body"]["choices"][0]["message"][
                    "content"
                ]
                # Clean the translation text - replace newlines with spaces
                translation = translation.replace("\n", " ").strip()
                translations[review_id] = translation

        # Create DataFrame and save as CSV
        df = pd.DataFrame(
            [
                (review_id, translation)
                for review_id, translation in translations.items()
            ],
            columns=["review_id", "review_comment_message_en"],
        )
        csv_file = "tmp/translations.csv"
        df.to_csv(csv_file, index=False, quoting=2, escapechar="\\")
        self.log.info(f"Saved translations to CSV: {csv_file}")

        # Upload CSV to GCS
        gcs_object_name = "translations/translations.csv"
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f"{self.task_id}_upload_to_gcs",
            src=csv_file,
            dst=gcs_object_name,
            bucket="correlion_olist",
            gcp_conn_id=self.gcp_conn_id,
        )
        upload_task.execute(context)
        self.log.info(f"Uploaded translations to GCS: {gcs_object_name}")

    def execute(self, context):
        # 1. Fetch reviews needing translation
        reviews_df = self._fetch_reviews()
        if reviews_df.empty:
            return None

        # 2. Prepare and submit batch
        requests = self._prepare_batch_requests(reviews_df)
        file_id = self._create_batch_file(requests)

        # 3. Create batch job
        batch = self.client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )

        # 4. Poll until complete
        while True:
            status = self.client.batches.retrieve(batch.id)
            self.log.info(f"Current batch status: {status}")
            self.log.info(f"Status: {status.status}")

            if status.status == "completed":
                self._process_batch_results(status.output_file_id, context)
                return None
            elif status.status in ["failed", "expired", "cancelled"]:
                self.log.error(f"Batch failed. Full status object: {status}")
                if hasattr(status, "errors"):
                    self.log.error(f"Error details: {status.errors.data}")
                raise Exception(f"Batch failed with status: {status.status}")

            time.sleep(30)


class ReviewTranslationToBQOperator(BaseOperator):
    """Loads translated reviews from GCS to BigQuery."""

    def __init__(
        self, task_id: str, project_id: str, dataset_id: str, table_id: str, **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id  # This is the main dataset
        self.table_id = table_id
        self.gcp_conn_id = "google_cloud_default"

    def execute(self, context):
        gcs_path = "translations/translations.csv"
        staging_dataset = "olist_staging"
        staging_table = "translations"

        # Define schema for BigQuery load
        schema_fields = [
            {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "review_comment_message_en", "type": "STRING", "mode": "NULLABLE"},
        ]

        # Load from GCS to BigQuery staging
        load_task = GCSToBigQueryOperator(
            task_id=f"{self.task_id}_load_to_bq",
            bucket="correlion_olist",
            source_objects=[gcs_path],
            destination_project_dataset_table=(
                f"{self.project_id}.{staging_dataset}.{staging_table}"
            ),
            source_format="CSV",
            schema_fields=schema_fields,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            allow_quoted_newlines=True,
            gcp_conn_id=self.gcp_conn_id,
        )
        load_task.execute(context)
        self.log.info("Loaded translations to BigQuery staging table")

        # Update main table with translations
        update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}` t
            SET review_comment_message_en = s.review_comment_message_en
            FROM `{self.project_id}.{staging_dataset}.{staging_table}` s
            WHERE t.review_id = s.review_id
        """

        update_job = BigQueryInsertJobOperator(
            task_id=f"{self.task_id}_update_main",
            project_id=self.project_id,
            configuration={
                "query": {
                    "query": update_query,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=self.gcp_conn_id,
        )
        update_job.execute(context)
        self.log.info("Updated main table with translations")
