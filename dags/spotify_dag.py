from datetime import datetime, timedelta
import os, sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from transform import transform_spotify_data
from modeling import create_models
from load import load_to_bigquery

default_args = {
    "owner": "kaan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_etl_pipeline",
    default_args=default_args,
    description="Spotify ETL Pipeline (transform -> model -> load)",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "etl"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_spotify_data,
    )

    modeling_task = PythonOperator(
        task_id="model",
        python_callable=create_models,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_bigquery,
    )

    transform_task >> modeling_task >> load_task
