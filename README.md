# Ceph-based Data Lake with Optimized Data Pipelines

This repository is a student research project for building a Data Lake on
Ceph Object Storage and evaluating distributed data pipelines.

The target system uses Ceph RGW as an S3-compatible object storage layer,
Spark for ETL/query, Airflow for orchestration, and Prometheus/Grafana for
monitoring. The implementation roadmap is kept locally in `workflow.md`.

## Current Status

Phase 1 is complete, Phase 2 bronze ingestion is available, Phase 3 Spark ETL
is available for NYC Taxi bronze, silver, and gold datasets, Phase 4 has a
manual Airflow DAG that submits transform jobs to local Spark standalone, and
Phase 5 has a query layer with Spark SQL smoke/benchmark queries and an
optional Trino service for SQL analytics over gold Parquet, and Phase 6 has
local Prometheus/Grafana monitoring for MinIO, Spark, and Airflow metrics:

- Repository skeleton exists.
- Runtime configuration template exists in `.env.example`.
- Local S3-compatible storage can be started with Docker Compose.
- Bucket initialization and upload/download/checksum smoke tests are available.
- NYC Taxi source manifest generation is available.
- Idempotent bronze upload for manifest-described files is available.
- NYC Taxi bronze-to-silver Spark transform is available.
- NYC Taxi silver-to-gold Spark aggregations are available.
- A manual-trigger Airflow DAG orchestrates config check, storage check,
  bronze ingest, silver transform, and gold transform through Spark standalone.
- Local Airflow services are available in Docker Compose.
- Local Spark standalone master/worker services are available in Docker Compose.
- Spark SQL smoke queries are available for NYC Taxi silver/gold outputs.
- Trino can register and query NYC Taxi gold Parquet tables from MinIO.
- Prometheus and Grafana can be started locally with provisioned scrape config,
  datasource, and dashboard files.
- Dataset documentation is available in `docs/datasets.md`.
- Storage benchmark runner is not implemented yet.

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
make dag-check
```

Start local S3-compatible storage, create buckets, and run a smoke test:

```bash
make up
make init-buckets
make storage-smoke
make health
```

Start local Airflow services when you want to orchestrate the pipeline through
the Airflow UI. This also starts Spark master/worker for transform tasks:

```bash
make airflow-up
make airflow-dag-list
```

Start local Spark standalone services when you want Spark master/worker instead
of `local[*]`:

```bash
make spark-up
make spark-submit-silver
make spark-submit-gold
```

Prepare and ingest the default NYC Taxi source file:

```bash
make prepare-nyc-taxi
make ingest
```

Transform bronze NYC Taxi data into silver and gold Parquet:

```bash
make transform
```

Run Spark SQL smoke queries against the silver/gold datasets:

```bash
make query-smoke
```

Run a small Spark SQL query benchmark:

```bash
make benchmark-query QUERY_BENCHMARK_WARMUP=0 QUERY_BENCHMARK_ITERATIONS=1
```

Compare partitioned and non-partitioned silver Parquet layouts:

```bash
make benchmark-query-layout QUERY_LAYOUT_BENCHMARK_WARMUP=0 QUERY_LAYOUT_BENCHMARK_ITERATIONS=1
```

Compare small-file and compacted silver Parquet layouts:

```bash
make benchmark-query-compaction QUERY_LAYOUT_BENCHMARK_WARMUP=0 QUERY_LAYOUT_BENCHMARK_ITERATIONS=1
```

Compare CSV and Parquet silver layouts:

```bash
make benchmark-query-format QUERY_LAYOUT_BENCHMARK_WARMUP=0 QUERY_LAYOUT_BENCHMARK_ITERATIONS=1
```

Start Trino and run SQL smoke queries against gold tables:

```bash
make trino-up
make trino-smoke
```

Run a small Trino query benchmark:

```bash
make benchmark-trino TRINO_BENCHMARK_WARMUP=0 TRINO_BENCHMARK_ITERATIONS=1
```

Run a lightweight S3-compatible storage benchmark:

```bash
make benchmark-storage
```

Use a one-operation smoke benchmark on small machines:

```bash
make benchmark-storage STORAGE_BENCHMARK_OBJECT_SIZES=4KiB STORAGE_BENCHMARK_CONCURRENCY=1 STORAGE_BENCHMARK_OPERATIONS=put STORAGE_BENCHMARK_WARMUP=0 STORAGE_BENCHMARK_ITERATIONS=1
```

Start local monitoring dashboards:

```bash
make monitoring-up
```

Prometheus is available at <http://localhost:9090>. Grafana is available at
<http://localhost:3000> with the default local credentials from `.env`.

Open an interactive Trino CLI:

```bash
make trino-cli
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
python spark/jobs/nyc_taxi_bronze_to_silver.py --manifest-path data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
python spark/jobs/nyc_taxi_silver_to_gold.py --manifest-path data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
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

## Airflow

Phase 4 includes a manual-trigger DAG for the NYC Taxi pipeline and local
Airflow services in Docker Compose. The transform tasks use `spark-submit` to
run on Spark standalone. See [docs/airflow.md](docs/airflow.md) for startup,
credentials, parameters, and runtime notes.

## Spark Standalone

Phase 4 includes a local Spark master/worker runtime in Docker Compose. See
[docs/spark.md](docs/spark.md) for startup commands, submit targets, container
network settings, and validation order.

## Query Layer

Phase 5 starts with Spark SQL smoke queries over NYC Taxi silver/gold Parquet
outputs, a lightweight Spark SQL benchmark runner, and Trino external tables
over the gold layer with a Trino benchmark runner. See
[docs/query.md](docs/query.md) for the query set, run commands, result metrics,
and Trino usage.

## Monitoring

Phase 6 includes local Prometheus/Grafana provisioning for MinIO, Spark, and
Airflow metrics. See [docs/monitoring.md](docs/monitoring.md) for startup
commands, endpoints, dashboard details, and current Trino/Ceph limitations.

## Storage Benchmark

Phase 7 includes a boto3-based S3 PUT/GET/mixed benchmark runner with warm-up,
concurrency, checksum validation, raw JSONL output, and CSV/JSON summaries. See
[docs/storage-benchmark.md](docs/storage-benchmark.md) for run commands,
scenario parameters, and output format.

## Next Phase

Continue Phase 8 by running the layout, compaction, and format benchmarks with
more iterations or against a larger NYC Taxi dataset.
