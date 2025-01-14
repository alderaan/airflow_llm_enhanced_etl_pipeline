import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# Path to the YAML config (relative to this Python file)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "etl_pipeline_config.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 0}

dag = DAG(
    dag_id=config["dag_id"],
    start_date=datetime.strptime(config["start_date"], "%Y-%m-%d"),
    schedule_interval=config["schedule_interval"],
    default_args=default_args,
    catchup=False,  # Disable backfills for simplicity
)

tasks = {}

# Create "extract" and "transform" tasks as before
for task_cfg in config["tasks"][:-1]:  # Skip the "load" task for custom logic
    task_id = task_cfg["id"]
    bash_command = task_cfg["bash_command"]
    tasks[task_id] = BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)

# Add the "load" task that uploads to BigQuery
load_task_cfg = config["tasks"][-1]

# Upload the local CSV to GCS
upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs",
    src=load_task_cfg["csv_file_path"],
    dst=load_task_cfg["gcs_object_name"],
    bucket=load_task_cfg["gcs_bucket"],
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Load the GCS CSV file into BigQuery as an actual table (not an external table)
load_to_bq = GCSToBigQueryOperator(
    task_id="load_to_bq",
    bucket=load_task_cfg["gcs_bucket"],
    source_objects=[load_task_cfg["gcs_object_name"]],
    destination_project_dataset_table=f'{load_task_cfg["bq_project"]}.{load_task_cfg["bq_dataset"]}.{load_task_cfg["bq_table"]}',
    source_format="CSV",
    autodetect=True,  # Automatically detect the schema
    skip_leading_rows=1,  # Skip the header row
    write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Task dependencies
tasks["extract"] >> tasks["transform"] >> upload_to_gcs >> load_to_bq
