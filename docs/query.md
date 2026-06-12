# Query Layer

Phase 5 starts with Spark SQL smoke queries over the NYC Taxi silver and gold
Parquet datasets stored in S3-compatible object storage.

The first implementation intentionally uses Spark SQL before adding Trino. This
matches the project workflow: establish a stable query baseline first, then add
Trino later when the local environment has enough disk and memory headroom.

## Query Set

The standard SQL files live in:

```text
spark/sql/
```

Current queries:

```text
01_daily_revenue.sql
02_top_pickup_locations.sql
03_avg_tip_by_payment.sql
04_hourly_distance_fare.sql
05_selective_pickup_date.sql
06_full_scan_location_aggregation.sql
```

They cover:

- daily revenue and trip counts from gold daily metrics;
- top pickup locations in a date range from gold location metrics;
- average tip by payment type from gold payment metrics;
- hourly distance/fare analysis from silver trips;
- a selective single-day query for partition pruning;
- a full-scan location aggregation baseline.

## Run

Make sure MinIO, Spark master, and Spark worker are running and silver/gold
outputs already exist:

```bash
make airflow-up
```

Run the Spark SQL smoke queries:

```bash
make query-smoke
```

The target runs:

```text
spark/jobs/nyc_taxi_query_smoke.py
```

It reads the manifest configured by `MANIFEST`, registers temporary Spark SQL
views, executes every `.sql` file under `spark/sql/`, and writes metrics to:

```text
results/nyc_taxi_query_smoke/year=YYYY/month=MM/metrics.json
```

## Benchmark

Run the Spark SQL benchmark:

```bash
make benchmark-query
```

By default this runs one warm-up pass and three measured iterations for every
query. On a small local machine, reduce the work:

```bash
make benchmark-query QUERY_BENCHMARK_WARMUP=0 QUERY_BENCHMARK_ITERATIONS=1
```

Benchmark outputs are written under:

```text
benchmark/results/<run_id>/query/spark_sql/<timestamp>/
```

Each run directory contains:

```text
environment.json
scenario.json
raw-results.jsonl
summary.csv
summary.json
```

`raw-results.jsonl` keeps one record per query execution. `summary.csv`
contains per-query min, median, p95, and max durations for measured runs.

## Runtime Notes

`make query-smoke` runs through the Docker Compose `spark-submit` service and
uses the internal Compose endpoint:

```text
S3_ENDPOINT=http://minio:9000
SPARK_MASTER_URL=spark://spark-master:7077
```

Host-local scripts still use values from `.env`, such as:

```text
S3_ENDPOINT=http://localhost:19000
SPARK_MASTER_URL=local[*]
```

## Interpreting Results

Each query metric includes:

- query file;
- duration in seconds;
- returned row count;
- a small sample of result rows.

The selective date query is the one to use when explaining partition pruning:
it filters `silver_trips` by `pickup_date`, which matches the silver Parquet
partition column.

Use `query-smoke` to prove correctness quickly. Use `benchmark-query` when you
need repeated measurements for reports or later comparisons.

## Trino

Trino remains a later Phase 5 extension. Add it after the Spark SQL baseline is
stable and local disk pressure is under control.
