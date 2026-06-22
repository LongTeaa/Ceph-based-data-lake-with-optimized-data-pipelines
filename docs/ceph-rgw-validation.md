# Ceph RGW Validation Results

This document records the first end-to-end validation run against the external
Ceph RGW cluster. It proves that the Data Lake pipeline can use Ceph Object
Gateway through the S3-compatible API instead of local MinIO.

These results are lab validation results, not production performance claims.

## Environment

- Storage backend: Ceph RGW
- RGW endpoint: `http://192.168.56.101:7480`
- Ceph deployment: 3 Ubuntu VirtualBox VMs managed by `cephadm`
- Ceph version: `18.2.8 reef`
- Hosts:
  - `hadoop-master` / `192.168.56.101`
  - `hadoop-worker1` / `192.168.56.102`
  - `hadoop-worker2` / `192.168.56.103`
- OSD layout: 3 OSDs, one 15 GiB virtual disk per VM
- Processing: Spark standalone through Docker Compose
- Dataset: NYC Yellow Taxi `yellow_tripdata_2025-01.parquet`
- Manifest: `data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json`
- Benchmark run id: `ceph-3vm-baseline`

## Ceph Cluster State

The cluster reached a healthy state before the pipeline run:

```text
health: HEALTH_OK
mon: 3 daemons, quorum hadoop-master,hadoop-worker1,hadoop-worker2
osd: 3 osds: 3 up, 3 in
rgw: 1 daemon active
pgs: active+clean
```

Ceph Dashboard showed the Data Lake buckets after the pipeline wrote data:

| Bucket | Used capacity | Objects |
|---|---:|---:|
| `datalake-bronze` | 56.4 MiB | 2 |
| `datalake-silver` | 81 MiB | 129 |
| `datalake-gold` | 3.5 MiB | 101 |
| `datalake-system` | 0 B | 0 |

## S3 Checks

The repo S3 smoke checks passed against Ceph RGW:

```bash
make health
make storage-smoke
```

The smoke test validated:

- bucket listing through RGW;
- object upload to `datalake-system`;
- metadata checksum verification;
- object download and payload checksum verification;
- object deletion.

## Pipeline Validation

The pipeline was run with only Spark master/worker containers enabled to keep
the Windows host memory usage low. Spark was configured with a lightweight
profile:

```text
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g
SPARK_WORKER_MEMORY=1g
SPARK_WORKER_CORES=1
SPARK_SQL_SHUFFLE_PARTITIONS=4
```

Validated flow:

```text
Ceph bronze -> Spark bronze_to_silver -> Ceph silver
Ceph silver -> Spark silver_to_gold -> Ceph gold
Ceph silver/gold -> Spark SQL query smoke
```

Commands:

```bash
make spark-submit-silver
make spark-submit-gold
make query-smoke
```

## Output Locations

Input and output datasets are stored in Ceph RGW buckets:

```text
s3://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet
s3://datalake-silver/nyc-taxi/year=2025/month=01
s3://datalake-gold/daily_trip_metrics/year=2025/month=01
s3://datalake-gold/location_metrics/year=2025/month=01
s3://datalake-gold/payment_metrics/year=2025/month=01
```

The local metrics files are:

```text
results/nyc_taxi_bronze_to_silver/year=2025/month=01/metrics.json
results/nyc_taxi_silver_to_gold/year=2025/month=01/metrics.json
results/nyc_taxi_query_smoke/year=2025/month=01/metrics.json
```

## Functional Results

| Step | Metric | Value |
|---|---:|---:|
| Bronze to silver | input rows | 3,475,226 |
| Bronze to silver | output rows | 3,328,747 |
| Bronze to silver | rejected rows | 146,479 |
| Bronze to silver | duration seconds | 391.652 |
| Silver to gold | input rows | 3,328,747 |
| Silver to gold | daily rows | 33 |
| Silver to gold | location rows | 255,307 |
| Silver to gold | payment rows | 159 |
| Silver to gold | duration seconds | 424.031 |
| Query smoke | query count | 6 |
| Query smoke | duration seconds | 258.929 |

The query smoke runner completed all six Spark SQL queries:

| Query | Rows | Duration seconds |
|---|---:|---:|
| `01_daily_revenue` | 33 | 6.937 |
| `02_top_pickup_locations` | 10 | 9.324 |
| `03_avg_tip_by_payment` | 6 | 8.339 |
| `04_hourly_distance_fare` | 24 | 32.627 |
| `05_selective_pickup_date` | 1 | 2.026 |
| `06_full_scan_location_aggregation` | 20 | 22.309 |

## Trino Query Validation

Trino was also validated against the Ceph-backed gold tables. Only the Trino
container was started for this check to keep the Windows host memory usage low.

Commands:

```bash
make trino-up
make trino-smoke
```

The Trino setup step registered external tables over the gold Parquet outputs:

```text
s3://datalake-gold/daily_trip_metrics/year=2025/month=01
s3://datalake-gold/location_metrics/year=2025/month=01
s3://datalake-gold/payment_metrics/year=2025/month=01
```

Smoke query results:

| Check | Result |
|---|---:|
| Daily metric rows | 33 |
| Total trips | 3,328,747 |
| Total revenue | 90,289,567.86 |
| Payment type rows returned | 6 |

This validates that Trino can use the Docker Compose S3 configuration to query
Ceph RGW data through the same S3-compatible endpoint used by Spark.

## Fault Tolerance Smoke Test

A controlled single-node outage was tested by gracefully shutting down
`hadoop-worker2` through VirtualBox while keeping `hadoop-master` and
`hadoop-worker1` online. The RGW daemon remained on `hadoop-master`.

Before the outage, the cluster was healthy:

```text
health: HEALTH_OK
mon: 3 daemons, quorum hadoop-master,hadoop-worker1,hadoop-worker2
osd: 3 osds: 3 up, 3 in
pgs: 194 active+clean
objects: 496 objects, 141 MiB
```

After `hadoop-worker2` was shut down, Ceph reported the expected degraded
state:

```text
health: HEALTH_WARN
mon: 3 daemons, quorum hadoop-master,hadoop-worker1, out of quorum: hadoop-worker2
osd: 3 osds: 2 up, 3 in
500/1500 objects degraded (33.333%)
111 active+undersized
83 active+undersized+degraded
```

The OSD tree showed `osd.2` down on `hadoop-worker2`, while `osd.0` and
`osd.1` remained up:

```text
hadoop-master   osd.0 up
hadoop-worker1  osd.1 up
hadoop-worker2  osd.2 down
```

The S3-compatible endpoint and query path remained usable during the outage.
Trino was able to query the Ceph-backed gold tables while `hadoop-worker2` was
offline:

```bash
make trino-smoke
```

Trino returned the same gold-table smoke results that were observed before the
outage:

| Check | Result |
|---|---:|
| Daily metric rows | 33 |
| Total trips | 3,328,747 |
| Total revenue | 90,289,567.86 |
| Payment type rows returned | 6 |

The first rows returned by the daily metrics query included:

```text
"2024-12-31","21","589.17"
"2025-01-01","82399","2374859.2"
"2025-01-02","82037","2422974.36"
```

Both repository storage checks also passed while `hadoop-worker2` was offline:

```bash
make health
make storage-smoke
```

The smoke test confirmed bucket reachability, object upload, checksum
verification, download, and deletion through RGW:

```text
reachable buckets: datalake-bronze, datalake-gold, datalake-silver, datalake-system
uploaded: s3://datalake-system/smoke-tests/20260622T080807Z-700a1147b8c0.bin
checksum ok: 700a1147b8c0d5aa3beacf6419aa9947fee59c4e84bad704bc498b8aefd8cf45
deleted: s3://datalake-system/smoke-tests/20260622T080807Z-700a1147b8c0.bin
storage-smoke ok
```

After `hadoop-worker2` was restarted, network connectivity returned and the
cluster recovered:

```text
ping hadoop-worker2: 3 transmitted, 3 received, 0% packet loss
health: HEALTH_OK
mon: 3 daemons, quorum hadoop-master,hadoop-worker1,hadoop-worker2
osd: 3 osds: 3 up, 3 in
pgs: 194 active+clean
```

This demonstrates that the 3-node lab cluster can tolerate a temporary
single-node outage for demo purposes. During the outage, redundancy was reduced
and Ceph correctly reported `HEALTH_WARN`, but quorum remained available and
RGW continued serving S3 requests. Trino could still query curated gold data
from Ceph RGW, proving that the Data Lake remained usable for read-side
analytics while one storage node was unavailable.

## Ceph Storage Smoke Benchmark

Two lightweight storage benchmark runs were executed against Ceph RGW. These
runs are intended to verify that the benchmark runner works against Ceph and to
capture an initial smoke-level baseline. They are too small for final
performance conclusions.

PUT run:

```bash
make benchmark-storage BENCHMARK_RUN_ID=ceph-3vm-baseline STORAGE_BENCHMARK_BACKEND=ceph-rgw STORAGE_BENCHMARK_OBJECT_SIZES=4KiB STORAGE_BENCHMARK_CONCURRENCY=1 STORAGE_BENCHMARK_OPERATIONS=put STORAGE_BENCHMARK_WARMUP=0 STORAGE_BENCHMARK_ITERATIONS=5
```

Result directory:

```text
benchmark/results/ceph-3vm-baseline/storage/s3/20260620T093717Z/
```

GET run:

```bash
make benchmark-storage BENCHMARK_RUN_ID=ceph-3vm-baseline STORAGE_BENCHMARK_BACKEND=ceph-rgw STORAGE_BENCHMARK_OBJECT_SIZES=4KiB STORAGE_BENCHMARK_CONCURRENCY=1 STORAGE_BENCHMARK_OPERATIONS=get STORAGE_BENCHMARK_WARMUP=0 STORAGE_BENCHMARK_ITERATIONS=5
```

Result directory:

```text
benchmark/results/ceph-3vm-baseline/storage/s3/20260620T093728Z/
```

Summary:

| Operation | Object size | Concurrency | Runs | Errors | Throughput MiB/s | Ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PUT | 4 KiB | 1 | 5 | 0 | 0.046 | 11.666 | 58.733 | 141.375 | 141.649 |
| GET | 4 KiB | 1 | 5 | 0 | 0.251 | 64.276 | 9.790 | 33.070 | 37.530 |

An expanded 1 MiB storage benchmark was also executed:

```bash
make benchmark-storage BENCHMARK_RUN_ID=ceph-3vm-baseline STORAGE_BENCHMARK_BACKEND=ceph-rgw STORAGE_BENCHMARK_OBJECT_SIZES=1MiB STORAGE_BENCHMARK_CONCURRENCY=1,4 STORAGE_BENCHMARK_OPERATIONS=put,get,mixed STORAGE_BENCHMARK_WARMUP=1 STORAGE_BENCHMARK_ITERATIONS=3
```

Result directory:

```text
benchmark/results/ceph-3vm-baseline/storage/s3/20260620T094044Z/
```

Scenario:

```text
backend: ceph-rgw
engine: boto3
bucket: datalake-system
endpoint: http://192.168.56.101:7480
object size: 1 MiB
concurrency: 1, 4
operations: PUT, GET, mixed
warmup: 1
measured iterations: 3
errors: 0 in all scenarios
```

Summary:

| Operation | Object size | Concurrency | Runs | Errors | Throughput MiB/s | Ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| GET | 1 MiB | 1 | 3 | 0 | 16.398 | 16.398 | 55.836 | 72.165 | 73.616 |
| GET | 1 MiB | 4 | 3 | 0 | 13.922 | 13.922 | 196.241 | 210.195 | 211.436 |
| mixed | 1 MiB | 1 | 3 | 0 | 8.403 | 8.403 | 116.116 | 128.502 | 129.603 |
| mixed | 1 MiB | 4 | 3 | 0 | 10.433 | 10.433 | 271.014 | 285.377 | 286.654 |
| PUT | 1 MiB | 1 | 3 | 0 | 6.221 | 6.221 | 128.413 | 219.659 | 227.769 |
| PUT | 1 MiB | 4 | 3 | 0 | 7.585 | 7.585 | 360.249 | 390.216 | 392.880 |

Interpretation:

- Both Ceph RGW storage benchmark smoke runs completed with zero errors.
- The benchmark runner can use the same S3 API path against Ceph that was used
  for the MinIO baseline.
- The 1 MiB run also completed with zero errors across GET, PUT, and mixed
  workloads at concurrency `1` and `4`.
- In this small lab run, GET throughput was higher than PUT throughput for
  1 MiB objects. Increasing concurrency from `1` to `4` improved PUT and mixed
  throughput, but GET throughput decreased and latency increased.
- The values are expected to be modest because the environment is a
  resource-constrained VirtualBox lab.
- Higher object sizes, repeated measured runs, and host resource metrics are
  still required before drawing stronger Ceph-vs-MinIO conclusions.

## Interpretation

This validation proves that:

- the repository can connect to Ceph RGW through the S3-compatible API;
- Data Lake buckets can be created and accessed on Ceph;
- the bronze NYC Taxi source file can be uploaded to Ceph;
- Spark running in Docker Compose can read from and write to Ceph through S3A;
- silver and gold Parquet outputs are stored on Ceph;
- Spark SQL can query the Ceph-backed silver/gold outputs;
- Trino can register and query the Ceph-backed gold outputs;
- Trino can still query Ceph-backed gold data during a controlled single-node
  outage.

These results do not yet prove:

- stable Ceph throughput under repeated benchmark runs;
- fair Ceph-vs-MinIO performance differences;
- production-grade fault tolerance beyond a controlled single-node smoke test;
- query performance under larger datasets or multi-client concurrency.

## Operational Notes

The cluster runs on a resource-constrained Windows laptop with three VirtualBox
VMs. After host sleep or reboot, temporary `HEALTH_WARN` messages such as slow
OSD heartbeats or recent daemon crashes may appear. In the observed run, the
cluster recovered to `HEALTH_OK` after the crash report was archived and all
PGs returned to `active+clean`.

Because host memory is limited, avoid starting Airflow, Trino, Spark,
Prometheus, and Grafana all at once. Start only the services required for each
validation step.

## Next Steps

1. Compare the expanded Ceph benchmark with the existing MinIO local baseline.
2. Capture a final phase summary with commands, outputs, and limitations.
3. Optionally repeat benchmarks with a more controlled host resource profile.
