# Airflow Orchestration

Phase 4 starts with a manual-trigger DAG and local Docker Compose services for
the NYC Taxi Data Lake pipeline.

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

## Local Docker Runtime

Start MinIO, Postgres, Airflow webserver, and Airflow scheduler:

```bash
make airflow-up
```

Open the Airflow UI:

```text
http://localhost:8080
```

Default local credentials from `.env.example`:

```text
username: admin
password: admin
```

List loaded DAGs from the webserver container:

```bash
make airflow-dag-list
```

Follow scheduler and webserver logs:

```bash
make airflow-logs
```

Stop Airflow services:

```bash
make airflow-down
```

The Compose runtime uses:

- `postgres` for Airflow metadata;
- `airflow-init` to migrate the metadata database and create the admin user;
- `airflow-webserver` for the UI;
- `airflow-scheduler` to schedule and execute DAG tasks;
- `minio` as the local S3-compatible object storage.

The Airflow containers install lightweight runtime dependencies from:

```text
docker/airflow/requirements.txt
```

This keeps webserver and scheduler startup quick. Spark execution inside
Airflow will need either a Spark-capable Airflow image or a separate Spark
cluster submission path before the transform tasks are run from the UI.

The Airflow containers mount the repository at:

```text
/opt/airflow/project
```

They also override `S3_ENDPOINT` to:

```text
http://minio:9000
```

This is different from host-local `.env` values such as
`http://localhost:9000`, because containers must reach MinIO by service name on
the Compose network.

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

## Manual Trigger

After `make airflow-up`, open the Airflow UI, unpause
`nyc_taxi_data_lake_pipeline`, and trigger it manually. The default parameters
process:

```text
data/source/nyc-taxi/yellow_tripdata_2025-01.parquet
```

Before triggering the DAG, make sure buckets exist:

```bash
make init-buckets
```

When running inside Docker, the DAG uses the environment variables injected by
Compose, while the scripts still read `.env` as a fallback.
