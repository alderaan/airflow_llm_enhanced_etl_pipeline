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
for task_cfg in config["tasks"][:2]:  # Only extract and transform tasks
    task_id = task_cfg["id"]
    bash_command = task_cfg["bash_command"]
    tasks[task_id] = BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)

# Create tasks for each dataset
for task_cfg in config["tasks"][2:]:  # Skip extract/transform, load all datasets
    table_name = task_cfg["bq_table"]

    # Upload CSV to GCS
    upload_task_id = f"load_{table_name}_to_gcs"
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id=upload_task_id,
        src=task_cfg["csv_file_path"],
        dst=task_cfg["gcs_object_name"],
        bucket=task_cfg["gcs_bucket"],
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    # Load GCS file into BigQuery
    load_task_id = f"load_{table_name}_to_bq"
    load_to_bq = GCSToBigQueryOperator(
        task_id=load_task_id,
        bucket=task_cfg["gcs_bucket"],
        source_objects=[task_cfg["gcs_object_name"]],
        destination_project_dataset_table=f'{task_cfg["bq_project"]}.{task_cfg["bq_dataset"]}.{task_cfg["bq_table"]}',
        source_format="CSV",
        autodetect=True,  # Automatically detect the schema
        skip_leading_rows=1,  # Skip header row
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
        allow_quoted_newlines=True,  # Important for handling multi-line fields
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    # Define task dependencies: extract -> transform -> upload_to_gcs -> load_to_bq
    tasks["extract"] >> tasks["transform"] >> upload_to_gcs >> load_to_bq
