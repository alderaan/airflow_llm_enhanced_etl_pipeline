from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class ReviewEnrichmentOperator(BigQueryInsertJobOperator):
    def __init__(
        self, task_id: str, project_id: str, dataset_id: str, table_id: str, **kwargs
    ):
        query = f"""
            UPDATE `{project_id}.{dataset_id}.{table_id}`
            SET 
                review_comment_message_en = CONCAT('[EN] ', review_comment_message),
                review_sentiment_score = 5  -- pretend everything is 5/5
            WHERE 1=1;
        """

        super().__init__(
            task_id=task_id,
            gcp_conn_id="google_cloud_default",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            **kwargs,
        )
