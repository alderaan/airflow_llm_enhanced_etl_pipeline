import os
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

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

# Dynamically create BashOperator tasks from the YAML config
for task_cfg in config["tasks"]:
    task_id = task_cfg["id"]
    bash_command = task_cfg["bash_command"]

    tasks[task_id] = BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)

# Simple linear dependency: extract -> transform -> load
tasks["extract"] >> tasks["transform"] >> tasks["load"]
