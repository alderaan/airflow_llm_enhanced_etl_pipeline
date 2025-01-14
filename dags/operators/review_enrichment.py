import json
import time
from typing import List, Dict
import pandas as pd
from openai import OpenAI
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.log.logging_mixin import LoggingMixin


class ReviewEnrichmentOperator(BigQueryInsertJobOperator, LoggingMixin):
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
        """Fetch reviews that need translation."""
        self.log.info("Starting to fetch reviews...")

        bq = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

        query = f"""
            SELECT review_id, review_comment_message
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE review_comment_message IS NOT NULL
              AND review_comment_message_en IS NULL
            LIMIT 1000
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
        for idx, row in reviews.iterrows():
            request = {
                "custom_id": row["review_id"],
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
        return requests

    def _create_batch_file(self, requests: List[Dict]) -> str:
        """Create JSONL file and upload to OpenAI."""
        # Write requests to temp JSONL file
        with open("/tmp/reviews_batch.jsonl", "w") as f:
            for req in requests:
                f.write(json.dumps(req) + "\n")

        # Upload file to OpenAI
        with open("/tmp/reviews_batch.jsonl", "rb") as f:
            file = self.client.files.create(file=f, purpose="batch")
        return file.id

    def _process_batch_results(self, output_file_id: str) -> Dict[str, str]:
        """Process batch results and extract translations."""
        content = self.client.files.content(output_file_id)
        translations = {}

        for line in content.text.strip().split("\n"):
            result = json.loads(line)
            review_id = result["custom_id"]
            if result["response"]["status_code"] == 200:
                translation = result["response"]["body"]["choices"][0]["message"][
                    "content"
                ]
                translations[review_id] = translation

        return translations

    def _update_translations(self, translations: Dict[str, str]):
        """Update BigQuery table with translations."""
        # Convert translations to SQL CASE statement
        # Remember to escape single quotes in the translation text
        cases = "\n".join(
            [
                f"WHEN review_id = '{review_id}' THEN '{translation.replace(chr(39), chr(39) + chr(39))}'"
                for review_id, translation in translations.items()
            ]
        )

        query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.{self.table_id}`
            SET 
                review_comment_message_en = CASE
                    {cases}
                    ELSE review_comment_message_en
                END
            WHERE review_id IN ({','.join([f"'{rid}'" for rid in translations.keys()])});
        """

        bq = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id, project_id=self.project_id  # Pass it here too
        )
        bq.run_query(query)

    def execute(self, context):
        # 1. Fetch reviews needing translation
        reviews_df = self._fetch_reviews()
        if reviews_df.empty:
            return "No reviews to translate"

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
            if status.status == "completed":
                # Process results and update BigQuery
                translations = self._process_batch_results(status.output_file_id)
                self._update_translations(translations)
                break
            elif status.status in ["failed", "expired", "cancelled"]:
                raise Exception(f"Batch failed with status: {status.status}")
            time.sleep(30)
