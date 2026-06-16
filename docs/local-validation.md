# Local Validation Results

This document records the current local validation baseline for the Data Lake
demo environment. These results prove functional correctness and provide a
small query-optimization baseline on local Docker/MinIO. They are not production
Ceph performance claims.

## Environment

- Backend: local MinIO S3-compatible storage
- Processing: Spark standalone through Docker Compose
- Orchestration: Airflow through Docker Compose
- Dataset: NYC Yellow Taxi `yellow_tripdata_2025-01.parquet`
- Manifest: `data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json`
- Benchmark run id: `local-baseline`

## Functional Checks

The unit and syntax checks passed:

```bash
make test
```

Result:

```text
Ran 49 tests
OK
```

The local end-to-end smoke workflow passed:

```bash
make e2e-smoke
```

The smoke workflow validated:

- S3 configuration and bucket initialization;
- S3 upload/download/checksum smoke test;
- NYC Taxi manifest generation;
- bronze upload;
- Spark bronze-to-silver transform;
- Spark silver-to-gold aggregation;
- Spark SQL smoke queries.

The main output metrics were:

| Step | Metric | Value |
|---|---:|---:|
| Bronze to silver | input rows | 3,475,226 |
| Bronze to silver | output rows | 3,328,747 |
| Bronze to silver | rejected rows | 146,479 |
| Bronze to silver | duration seconds | 79.914 |
| Silver to gold | input rows | 3,328,747 |
| Silver to gold | daily rows | 33 |
| Silver to gold | location rows | 255,307 |
| Silver to gold | payment rows | 159 |
| Silver to gold | duration seconds | 71.971 |
| Query smoke | query count | 6 |
| Query smoke | duration seconds | 55.925 |

Metrics files:

```text
results/nyc_taxi_bronze_to_silver/year=2025/month=01/metrics.json
results/nyc_taxi_silver_to_gold/year=2025/month=01/metrics.json
results/nyc_taxi_query_smoke/year=2025/month=01/metrics.json
```

The Airflow DAG also passed through the UI:

```text
nyc_taxi_data_lake_pipeline
```

The confirmed DAG tasks were:

```text
check_config
check_storage
prepare_manifest
upload_bronze
bronze_to_silver
silver_to_gold
```

Observed Airflow run duration was about `00:05:50`.

## Spark SQL Baseline

Run directory:

```text
benchmark/results/local-baseline/query/spark_sql/20260612T160823Z/
```

Scenario:

```text
iterations: 1
warmup: 0
query count: 6
```

Summary:

| Query | Rows | Median seconds |
|---|---:|---:|
| `01_daily_revenue` | 33 | 0.848 |
| `02_top_pickup_locations` | 10 | 2.609 |
| `03_avg_tip_by_payment` | 6 | 1.448 |
| `04_hourly_distance_fare` | 24 | 2.853 |
| `05_selective_pickup_date` | 1 | 0.466 |
| `06_full_scan_location_aggregation` | 20 | 2.478 |

## Phase 8 Query Layout Results

### Partitioned vs Non-Partitioned Parquet

Use this run as the valid partition comparison:

```text
benchmark/results/local-baseline/query/spark_layout/20260614T080602Z/
```

Scenario:

```text
comparison: parquet_partitioned_vs_non_partitioned
iterations: 1
warmup: 0
result_consistent: true
```

Summary:

| Layout | Query | Rows | Median seconds |
|---|---|---:|---:|
| non-partitioned | `04_hourly_distance_fare` | 24 | 1.344 |
| non-partitioned | `05_selective_pickup_date` | 1 | 0.687 |
| non-partitioned | `06_full_scan_location_aggregation` | 20 | 1.200 |
| partitioned | `04_hourly_distance_fare` | 24 | 4.972 |
| partitioned | `05_selective_pickup_date` | 1 | 0.914 |
| partitioned | `06_full_scan_location_aggregation` | 20 | 3.898 |

In this local run, the partitioned layout was slower than the non-partitioned
copy. Treat this as a local observation, not a general conclusion. The dataset,
file sizes, object listing cost, and single-iteration measurement can dominate
the expected benefit of partition pruning.

Do not use this earlier run as evidence:

```text
benchmark/results/local-baseline/query/spark_layout/20260614T080302Z/
```

It reported `result_consistent=false`.

### Small Files vs Compacted Files

Run directory:

```text
benchmark/results/local-baseline/query/spark_layout/20260614T141634Z/
```

Scenario:

```text
comparison: compaction
iterations: 1
warmup: 0
result_consistent: true
```

Summary:

| Layout | Query | Rows | Median seconds |
|---|---|---:|---:|
| small files | `04_hourly_distance_fare` | 24 | 3.464 |
| small files | `05_selective_pickup_date` | 1 | 1.247 |
| small files | `06_full_scan_location_aggregation` | 20 | 1.343 |
| compacted | `04_hourly_distance_fare` | 24 | 1.470 |
| compacted | `05_selective_pickup_date` | 1 | 0.470 |
| compacted | `06_full_scan_location_aggregation` | 20 | 0.853 |

This is the clearest Phase 8 local result: compaction reduced query latency for
all three measured queries.

### CSV vs Parquet

Run directory:

```text
benchmark/results/local-baseline/query/spark_layout/20260614T143134Z/
```

Scenario:

```text
comparison: format
iterations: 1
warmup: 0
result_consistent: true
```

Summary:

| Format | Query | Rows | Median seconds |
|---|---|---:|---:|
| CSV | `04_hourly_distance_fare` | 24 | 12.006 |
| CSV | `05_selective_pickup_date` | 1 | 8.676 |
| CSV | `06_full_scan_location_aggregation` | 20 | 7.328 |
| Parquet | `04_hourly_distance_fare` | 24 | 3.515 |
| Parquet | `05_selective_pickup_date` | 1 | 0.960 |
| Parquet | `06_full_scan_location_aggregation` | 20 | 1.323 |

This run supports using Parquet for silver and gold analytical datasets. Parquet
was substantially faster than CSV for every measured query.

## Interpretation

The local validation supports these conclusions:

- the pipeline works end to end on NYC Taxi data through MinIO and Spark
  standalone;
- Airflow can orchestrate the core ingest and transform workflow;
- Spark SQL can query the generated silver and gold datasets;
- compaction improves the local query benchmark;
- Parquet outperforms CSV for the local analytical benchmark.

The local validation does not prove:

- Ceph RGW performance;
- Ceph-vs-MinIO performance differences;
- production-scale throughput or latency;
- fault tolerance under OSD/RGW/node failure.

## Next Steps

Recommended next steps:

1. Run the storage benchmark on local MinIO as a baseline.
2. Repeat the same storage benchmark against a Ceph RGW endpoint.
3. Record Ceph topology, pool settings, host resources, and network placement.
4. Run at least one fault-tolerance scenario if lab infrastructure is available.
