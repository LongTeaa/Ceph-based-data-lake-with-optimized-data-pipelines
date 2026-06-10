# Ceph-based Data Lake with Optimized Data Pipelines

This repository is a student research project for building a Data Lake on
Ceph Object Storage and evaluating distributed data pipelines.

The target system uses Ceph RGW as an S3-compatible object storage layer,
Spark for ETL/query, Airflow for orchestration, and Prometheus/Grafana for
monitoring. The implementation roadmap is kept locally in `workflow.md`.

## Current Status

Phase 1 is complete and Phase 2 bronze ingestion is available:

- Repository skeleton exists.
- Runtime configuration template exists in `.env.example`.
- Local S3-compatible storage can be started with Docker Compose.
- Bucket initialization and upload/download/checksum smoke tests are available.
- NYC Taxi source manifest generation is available.
- Idempotent bronze upload for manifest-described files is available.
- Dataset documentation is available in `docs/datasets.md`.
- Spark, Airflow DAGs, monitoring, and benchmark runners are not implemented yet.

## Prerequisites

For the current local workflow:

- Git
- Python 3.11+
- GNU Make
- Docker Desktop or Docker Engine with Compose v2
- One local NYC Taxi Parquet file for bronze ingestion

For later phases:

- A Ceph RGW endpoint or a Linux/Kubernetes environment for Ceph deployment
- Enough disk space for NYC Taxi data and generated benchmark artifacts
- Optional: 3 Linux VM/node environment for meaningful Ceph benchmark and
  fault-tolerance experiments

## Quick Start

Create a local environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Validate required configuration:

```bash
make config-check
```

Run local checks:

```bash
make test
```

Start local S3-compatible storage, create buckets, and run a smoke test:

```bash
make up
make init-buckets
make storage-smoke
make health
```

Prepare and ingest the default NYC Taxi source file:

```bash
make prepare-nyc-taxi
make ingest
```

Stop local storage:

```bash
make down
```

If `make` is not installed, run the scripts directly:

```bash
python infrastructure/scripts/config_check.py S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET
python infrastructure/buckets/init_buckets.py
python infrastructure/buckets/storage_smoke.py
python ingestion/nyc_taxi_manifest.py --source-dir data/source/nyc-taxi --file-name yellow_tripdata_2025-01.parquet
python ingestion/bronze_upload.py --manifest-path data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
```

## Dataset Policy

Large datasets are local-only and ignored by Git.

Expected local paths:

- NYC Yellow Taxi source Parquet: `data/source/nyc-taxi/`
- Optional Wikimedia image dataset: `data/source/images/`
- Pipeline outputs: `data/bronze/`, `data/silver/`, `data/gold/`
- Benchmark output: `results/`

The main analytics dataset is NYC Yellow Taxi. Synthetic data is only for tests,
scale experiments, and object-storage benchmark payloads.

Prepare and ingest the NYC Taxi source file:

```bash
make prepare-nyc-taxi
make ingest
```

The default source file is `data/source/nyc-taxi/yellow_tripdata_2025-01.parquet`.
Override `SOURCE`, `FILE`, or `MANIFEST` from the command line when needed.

See [docs/datasets.md](docs/datasets.md) for the expected local path, manifest
format, and bronze layout.

## Repository Layout

```text
airflow/           Airflow DAGs and orchestration code
benchmark/         Storage and query benchmark runners
data/              Local source data, samples, and generated outputs
docker/            Compose files and service configuration
docs/              Technical documentation and runbooks
generator/         Synthetic tabular/binary data generators
ingestion/         Dataset preparation and upload utilities
infrastructure/    Ceph, bucket, and environment bootstrap scripts
spark/             PySpark jobs, SQL, and Spark configuration
tests/             Unit and integration tests
```

## Local Storage

Phase 1 uses MinIO as a lightweight local S3-compatible endpoint. This is only a
development backend for validating bucket layout and S3 scripts. The same
scripts are intended to run against Ceph RGW after changing `.env`.

See [docs/local-storage.md](docs/local-storage.md) for details.

## Next Phase

Phase 3 will transform bronze NYC Taxi data into silver/gold datasets with
Spark.
