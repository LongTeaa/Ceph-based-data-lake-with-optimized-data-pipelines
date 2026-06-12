"""Airflow orchestration for the NYC Taxi Data Lake pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "${DATA_LAKE_PROJECT_ROOT:-/opt/airflow/project}"
PYTHON_BIN = "${DATA_LAKE_PYTHON_BIN:-python}"

DEFAULT_SOURCE_DIR = "data/source/nyc-taxi"
DEFAULT_FILE_NAME = "yellow_tripdata_2025-01.parquet"
DEFAULT_MANIFEST_PATH = (
    "data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json"
)
DEFAULT_OUTPUT_DIR = "results"
DEFAULT_TRANSFORM_MODE = "overwrite"


def project_command(command: str) -> str:
    return f'cd "{PROJECT_ROOT}" && {command}'


with DAG(
    dag_id="nyc_taxi_data_lake_pipeline",
    description="Prepare, ingest, and transform NYC Taxi data from bronze to silver and gold.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "source_dir": DEFAULT_SOURCE_DIR,
        "file_name": DEFAULT_FILE_NAME,
        "manifest_path": DEFAULT_MANIFEST_PATH,
        "output_dir": DEFAULT_OUTPUT_DIR,
        "transform_mode": DEFAULT_TRANSFORM_MODE,
    },
    tags=["ceph", "data-lake", "nyc-taxi"],
) as dag:
    check_config = BashOperator(
        task_id="check_config",
        bash_command=project_command(
            f"{PYTHON_BIN} infrastructure/scripts/config_check.py "
            "S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION "
            "BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET"
        ),
    )

    check_storage = BashOperator(
        task_id="check_storage",
        bash_command=project_command(
            f"{PYTHON_BIN} infrastructure/buckets/storage_smoke.py --health-only"
        ),
    )

    prepare_manifest = BashOperator(
        task_id="prepare_manifest",
        bash_command=project_command(
            f"{PYTHON_BIN} ingestion/nyc_taxi_manifest.py "
            '--source-dir "{{ params.source_dir }}" '
            '--file-name "{{ params.file_name }}" '
            '--manifest-path "{{ params.manifest_path }}"'
        ),
    )

    upload_bronze = BashOperator(
        task_id="upload_bronze",
        bash_command=project_command(
            f"{PYTHON_BIN} ingestion/bronze_upload.py "
            '--manifest-path "{{ params.manifest_path }}"'
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=project_command(
            f"{PYTHON_BIN} spark/jobs/nyc_taxi_bronze_to_silver.py "
            '--manifest-path "{{ params.manifest_path }}" '
            '--output-dir "{{ params.output_dir }}" '
            '--mode "{{ params.transform_mode }}"'
        ),
        execution_timeout=timedelta(hours=2),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=project_command(
            f"{PYTHON_BIN} spark/jobs/nyc_taxi_silver_to_gold.py "
            '--manifest-path "{{ params.manifest_path }}" '
            '--output-dir "{{ params.output_dir }}" '
            '--mode "{{ params.transform_mode }}"'
        ),
        execution_timeout=timedelta(hours=2),
    )

    check_config >> check_storage >> prepare_manifest >> upload_bronze
    upload_bronze >> bronze_to_silver >> silver_to_gold
