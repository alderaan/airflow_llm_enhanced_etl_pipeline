import json
import time
from typing import List, Dict
import pandas as pd
from openai import OpenAI
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.log.logging_mixin import LoggingMixin


class ReviewAspectScoringOperator(BigQueryInsertJobOperator, LoggingMixin):
    def __init__(
        self, task_id: str, project_id: str, dataset_id: str, table_id: str, **kwargs
    ):
        # Initialize OpenAI client
        self.client = OpenAI()
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

        # We'll override this query later in execute()
        query = "SELECT 1"  # Placeholder
        super().__init__(
            task_id=task_id,
            project_id=project_id,
            gcp_conn_id="google_cloud_default",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            **kwargs,
        )

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
              LIMIT 1000
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
                    "max_tokens": 1000,
                },
            }
            requests.append(request)

        self.log.info(f"Prepared {len(requests)} unique requests")
        return requests

    def _create_batch_file(self, requests: List[Dict]) -> str:
        """Create JSONL file and upload to OpenAI."""
        # Save timestamp for unique filename
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        local_file = f"tmp/aspect_scores_input_{timestamp}.jsonl"

        # Write requests to temp JSONL file
        with open(local_file, "w") as f:
            for req in requests:
                f.write(json.dumps(req) + "\n")

        self.log.info(f"Saved batch input file locally to: {local_file}")

        # Upload file to OpenAI
        with open(local_file, "rb") as f:
            file = self.client.files.create(file=f, purpose="batch")

        return file.id

    def _process_batch_results(self, output_file_id: str) -> Dict[str, Dict]:
        """Process batch results and extract aspect scores."""
        content = self.client.files.content(output_file_id)

        # Save output file locally
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        local_file = f"tmp/aspect_scores_output_{timestamp}.jsonl"
        with open(local_file, "w") as f:
            f.write(content.text)
        self.log.info(f"Saved batch output file locally to: {local_file}")

        aspect_scores = {}
        for line in content.text.strip().split("\n"):
            result = json.loads(line)
            review_id = result["custom_id"]
            if result["response"]["status_code"] == 200:
                scores = json.loads(
                    result["response"]["body"]["choices"][0]["message"]["content"]
                )
                aspect_scores[review_id] = scores

        return aspect_scores

    def _update_aspect_scores(self, aspect_scores: Dict[str, Dict], context):
        """Update BigQuery table with aspect scores."""
        # Create parameters for each set of scores
        parameters = []
        update_statements = []

        for i, (review_id, scores) in enumerate(aspect_scores.items()):
            param_name = f"scores_{i}"
            parameters.append(
                {
                    "name": param_name,
                    "parameterType": {"type": "STRING"},
                    "parameterValue": {"value": json.dumps(scores)},
                }
            )
            update_statements.append(
                f"WHEN review_id = '{review_id}' THEN PARSE_JSON(@{param_name})"
            )

        cases = "\n                ".join(update_statements)
        review_ids = "', '".join(aspect_scores.keys())

        query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}`
            SET review_aspect_scores = (
                CASE
                {cases}
                ELSE review_aspect_scores
                END
            )
            WHERE review_id IN ('{review_ids}');
        """

        self.log.info(
            "Executing update query with %d aspect scores", len(aspect_scores)
        )

        update_job = BigQueryInsertJobOperator(
            task_id=f"{self.task_id}_update",
            project_id=self.project_id,
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "location": "US",
                    "queryParameters": parameters,
                }
            },
            gcp_conn_id=self.gcp_conn_id,
        )
        update_job.execute(context=context)

    def execute(self, context):
        # 1. Fetch reviews needing aspect scoring
        reviews_df = self._fetch_reviews()
        if reviews_df.empty:
            self.log.info("No reviews to score")
            return "No reviews to score"

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
                aspect_scores = self._process_batch_results(status.output_file_id)
                self.log.info(
                    f"Processed {len(aspect_scores)} aspect scores",
                )
                self._update_aspect_scores(aspect_scores, context)
                self.log.info("Updated BigQuery with aspect scores")
                break
            elif status.status in ["failed", "expired", "cancelled"]:
                self.log.error(
                    f"Batch failed. Full status object: {status}",
                )
                if hasattr(status, "errors"):
                    self.log.error(f"Error details: {status.errors.data}")
                raise Exception(f"Batch failed with status: {status.status}")

            self.log.info("Waiting 30 seconds before next poll...")
            time.sleep(30)
