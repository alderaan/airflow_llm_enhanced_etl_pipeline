# flake8: noqa: E501
from typing import Dict, List
import json
import time
import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from openai import OpenAI


class ReviewAspectScoringToGCSOperator(BaseOperator):
    """Scores review aspects and uploads results to GCS."""

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
        """Fetch reviews that need aspect scoring."""
        self.log.info("Starting to fetch reviews...")

        bq = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        client = bq.get_client(project_id=self.project_id)

        query = f"""
            SELECT review_id, review_comment_message_en
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE review_comment_message_en IS NOT NULL
              AND review_aspect_scores IS NULL
              LIMIT 10000
        """

        query_job = client.query(query)
        df = query_job.to_dataframe()
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
                                "You are an aspect-based sentiment evaluator for "
                                "e-commerce reviews. The text you receive will be "
                                "a single review from a customer.\n\n"
                                "Task: For each of the following aspects, output a "
                                "numeric score between -1.0 and +1.0 (inclusive):\n"
                                "1. delivery_speed (Only about shipping speed/time.)\n"
                                "2. product_quality (Only about the item's quality or defects.)\n"
                                "3. customer_service (Only about service reps or support if contact already happened.)\n"
                                "4. refund_process (Only about ease/difficulty of returning or refunding if that already happened.)\n"
                                "5. packaging_condition (Only about how the package arrived (damaged or intact).)\n\n"
                                "6. correct_items (Only about whether the customer received the right or wrong item.)\n\n"
                                "• -1.0 = extremely negative\n"
                                "• 0.0 = neutral or not mentioned\n"
                                "• +1.0 = extremely positive\n\n"
                                "If the review does not mention an aspect, default "
                                "its score to 0.0.\n\n"
                                "Output Format: Return only a valid JSON object with "
                                "these five keys exactly: Provide no extra commentary "
                                "or text."
                            ),
                        },
                        {
                            "role": "user",
                            "content": f'Customer Review: "{row["review_comment_message_en"]}"',
                        },
                    ],
                    "response_format": {"type": "json_object"},
                    "max_tokens": 100,
                },
            }
            requests.append(request)

        self.log.info(f"Prepared {len(requests)} unique requests")
        return requests

    def _create_batch_file(self, requests: List[Dict]) -> str:
        """Create JSONL file and upload to OpenAI."""
        local_file = "tmp/aspect_scores_input.jsonl"

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
        jsonl_file = "tmp/aspect_scores_output.jsonl"
        with open(jsonl_file, "w") as f:
            f.write(content.text)
        self.log.info(f"Saved batch output file locally to: {jsonl_file}")

        # Process aspect scores
        aspect_scores = {}
        for line in content.text.strip().split("\n"):
            result = json.loads(line)
            review_id = result["custom_id"]
            if result["response"]["status_code"] == 200:
                scores = json.loads(
                    result["response"]["body"]["choices"][0]["message"]["content"]
                )
                aspect_scores[review_id] = scores

        # Create DataFrame and save as CSV
        df = pd.DataFrame(
            [
                (review_id, json.dumps(scores))
                for review_id, scores in aspect_scores.items()
            ],
            columns=["review_id", "review_aspect_scores"],
        )
        csv_file = "tmp/aspect_scores.csv"
        df.to_csv(csv_file, index=False, quoting=2, escapechar="\\")
        self.log.info(f"Saved aspect scores to CSV: {csv_file}")

        # Upload CSV to GCS
        gcs_object_name = "aspect_scores/aspect_scores.csv"
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f"{self.task_id}_upload_to_gcs",
            src=csv_file,
            dst=gcs_object_name,
            bucket="correlion_olist",
            gcp_conn_id=self.gcp_conn_id,
        )
        upload_task.execute(context)
        self.log.info(f"Uploaded aspect scores to GCS: {gcs_object_name}")

    def execute(self, context):
        # 1. Fetch reviews needing aspect scoring
        reviews_df = self._fetch_reviews()
        if reviews_df.empty:
            return None

        # 2. Prepare and submit batch
        requests = self._prepare_batch_requests(reviews_df)
        file_id = self._create_batch_file(requests)
        self.log.info(f"Created batch file with ID: {file_id}")

        # 3. Create batch job
        batch = self.client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        self.log.info(f"Created batch job with ID: {batch.id}")

        # 4. Poll until complete
        poll_count = 0
        while True:
            poll_count += 1
            status = self.client.batches.retrieve(batch.id)

            # Log both full status and clean status string
            self.log.info(f"Poll #{poll_count} - Full status: {status}")
            self.log.info(f"Poll #{poll_count} - Status: {status.status}")

            # Log event counts if available
            if hasattr(status, "events"):
                self.log.info(
                    f"Events - "
                    f"Created: {status.events.created_count}, "
                    f"Running: {status.events.running_count}, "
                    f"Succeeded: {status.events.succeeded_count}, "
                    f"Failed: {status.events.failed_count}",
                )

            if status.status == "completed":
                self.log.info("Batch completed successfully")
                self._process_batch_results(status.output_file_id, context)
                return None
            elif status.status in ["failed", "expired", "cancelled"]:
                self.log.error(f"Batch failed. Full status object: {status}")
                if hasattr(status, "errors"):
                    self.log.error(f"Error details: {status.errors.data}")
                raise Exception(f"Batch failed with status: {status.status}")

            self.log.info("Waiting 30 seconds before next poll...")
            time.sleep(30)


class ReviewAspectScoringToBQOperator(BaseOperator):
    """Loads aspect scores from GCS to BigQuery."""

    def __init__(
        self, task_id: str, project_id: str, dataset_id: str, table_id: str, **kwargs
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id  # This is the main dataset
        self.table_id = table_id
        self.gcp_conn_id = "google_cloud_default"

    def execute(self, context):
        gcs_path = "aspect_scores/aspect_scores.csv"
        staging_dataset = "olist_staging"  # Fixed staging dataset name
        staging_table = "aspect_scores"

        # Define schema for BigQuery load
        schema_fields = [
            {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "review_aspect_scores", "type": "STRING", "mode": "NULLABLE"},
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
        self.log.info("Loaded aspect scores to BigQuery staging table")

        # Update main table with aspect scores
        update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}` t
            SET review_aspect_scores = PARSE_JSON(s.review_aspect_scores)
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
        self.log.info("Updated main table with aspect scores")
