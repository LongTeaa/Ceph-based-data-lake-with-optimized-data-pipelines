# Query Layer

Phase 5 starts with Spark SQL smoke queries over the NYC Taxi silver and gold
Parquet datasets stored in S3-compatible object storage.

The first implementation used Spark SQL to establish a stable query baseline.
The phase now also includes a lightweight Trino service for SQL analytics over
gold Parquet data in MinIO.

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

## Trino

Trino is the interactive SQL query service for Phase 5. It is separate from
Spark:

```text
Spark SQL smoke/benchmark  -> correctness and baseline timings
Trino                      -> analytics SQL service over gold Parquet
```

Start MinIO and Trino:

```bash
make trino-up
```

The Trino UI is available at:

```text
http://localhost:8083
```

Register the NYC Taxi gold tables and run smoke queries:

```bash
make trino-smoke
```

The setup SQL is:

```text
docker/trino/sql/nyc_taxi_gold_setup.sql
```

It creates an external Hive catalog schema and registers these gold tables:

```text
lake.nyc_taxi.daily_trip_metrics
lake.nyc_taxi.location_metrics
lake.nyc_taxi.payment_metrics
```

The smoke SQL is:

```text
docker/trino/sql/nyc_taxi_gold_smoke.sql
```

Open an interactive SQL shell:

```bash
make trino-cli
```

Example query:

```sql
SELECT pickup_date, trip_count, total_revenue
FROM daily_trip_metrics
ORDER BY pickup_date
LIMIT 10;
```

Trino uses the Docker internal S3 endpoint:

```text
http://minio:9000
```

The host browser still opens MinIO at:

```text
http://localhost:19001
```

This initial Trino setup uses the Hive connector with a file metastore stored
in a Docker volume. It avoids adding a separate Hive Metastore container while
still allowing external tables over Parquet objects in MinIO. The first Trino
scope intentionally focuses on the gold layer because those tables are smaller,
stable, and ready for analytics.

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

## Next Query Extensions

Useful next extensions:

- add Trino benchmark output comparable to Spark SQL benchmark results;
- register selected silver tables in Trino for partition-pruning demos;
- add BI or notebook access after the SQL engine is stable.
