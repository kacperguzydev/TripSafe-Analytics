import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PYTHON = "python3"
SCRIPTS = "/opt/airflow/dags"

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tripsafe_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    detect_fraud = BashOperator(
        task_id="run_fraud_detector",
        bash_command=f"{PYTHON} {SCRIPTS}/fraud_detector.py",
    )

    roll_metrics = BashOperator(
        task_id="run_trip_metrics",
        bash_command=f"{PYTHON} {SCRIPTS}/trip_metrics.py",
    )

    detect_fraud >> roll_metrics
