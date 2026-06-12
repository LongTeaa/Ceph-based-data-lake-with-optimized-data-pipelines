# Airflow Orchestration

Phase 4 starts with a manual-trigger DAG for the NYC Taxi Data Lake pipeline.

## DAG

```text
airflow/dags/nyc_taxi_pipeline.py
```

The DAG id is:

```text
nyc_taxi_data_lake_pipeline
```

Task order:

```text
check_config
  -> check_storage
  -> prepare_manifest
  -> upload_bronze
  -> bronze_to_silver
  -> silver_to_gold
```

The DAG only orchestrates existing scripts and Spark jobs. ETL logic remains in
the `ingestion/` and `spark/jobs/` modules.

## Runtime Assumptions

The Airflow environment must:

- mount this repository into the container or host path configured by
  `DATA_LAKE_PROJECT_ROOT`;
- run with dependencies from `requirements.txt`;
- have access to the same `.env` values used by the local scripts;
- reach the S3-compatible endpoint configured by `S3_ENDPOINT`.

For a local Airflow container, set:

```env
DATA_LAKE_PROJECT_ROOT=/opt/airflow/project
DATA_LAKE_PYTHON_BIN=python
```

If running Airflow directly from the repository virtual environment on Windows,
set `DATA_LAKE_PROJECT_ROOT` to the repository path and `DATA_LAKE_PYTHON_BIN`
to the virtualenv Python executable.

## Parameters

The DAG supports these manual trigger parameters:

```json
{
  "source_dir": "data/source/nyc-taxi",
  "file_name": "yellow_tripdata_2025-01.parquet",
  "manifest_path": "data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json",
  "output_dir": "results",
  "transform_mode": "overwrite"
}
```

Use a different `file_name` and `manifest_path` to process another NYC Taxi
month.

## Local Validation

Validate the DAG source without requiring an Airflow installation:

```bash
make dag-check
```

Run all local checks:

```bash
make test
```
