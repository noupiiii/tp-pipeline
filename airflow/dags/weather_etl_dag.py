"""DAG Airflow orchestrant le pipeline complet : ingestion -> data lake -> transform -> load.

Planifié quotidiennement. Chaque tâche appelle les modules de `src/` pour
garder la logique métier testable indépendamment d'Airflow.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "tp-louis-aurelien",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest(**context) -> None:
    from src.ingestion.api_client import run as ingest_run

    local_path = ingest_run()
    context["ti"].xcom_push(key="local_raw_path", value=str(local_path))


def _upload_to_datalake(**context) -> None:
    from pathlib import Path

    from src.datalake.hdfs_writer import upload_raw_file

    local_path = Path(context["ti"].xcom_pull(key="local_raw_path", task_ids="ingest"))
    remote_path = upload_raw_file(local_path)
    context["ti"].xcom_push(key="hdfs_raw_path", value=remote_path)


def _transform(**context) -> None:
    from src.config import HDFS
    from src.transform.spark_transform import run as transform_run

    hdfs_uri = f"hdfs://{HDFS.namenode_host}:9000{HDFS.raw_path}"
    result = transform_run(hdfs_uri, "hdfs://namenode:9000/data-lake/processed/weather")
    context["ti"].xcom_push(key="transform_result", value=result)


def _load(**context) -> None:
    from src.load.postgres_loader import load_parquet_to_postgres

    load_parquet_to_postgres("hdfs://namenode:9000/data-lake/processed/weather")


with DAG(
    dag_id="weather_etl_pipeline",
    description="Ingestion API météo -> Data Lake HDFS -> Spark -> PostgreSQL",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "weather", "tp-louis-aurelien"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    upload_to_datalake = PythonOperator(task_id="upload_to_datalake", python_callable=_upload_to_datalake)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    load = PythonOperator(task_id="load", python_callable=_load)

    ingest >> upload_to_datalake >> transform >> load
