# Ceph RGW Transition Summary

This document summarizes the project phase that moved the local Data Lake
pipeline from a MinIO-only storage backend to an external 3-VM Ceph RGW lab.
It is a phase close-out note, while detailed evidence is kept in:

- `docs/ceph-rgw-validation.md`
- `docs/storage-backend-comparison.md`

## Goal

The goal of this phase was to prove that the existing Data Lake pipeline can
run against real Ceph object storage through the S3-compatible RGW API without
rewriting the Spark, Airflow, Trino, and benchmark layers.

## Final Topology

Ceph runs outside Docker Desktop on three VirtualBox VMs:

| Host | IP | Role |
|---|---|---|
| `hadoop-master` | `192.168.56.101` | Ceph host, RGW endpoint |
| `hadoop-worker1` | `192.168.56.102` | Ceph host |
| `hadoop-worker2` | `192.168.56.103` | Ceph host |

Ceph services:

- Ceph version: `18.2.8 reef`
- Deployment tool: `cephadm`
- MON quorum: 3 MONs
- OSD layout: 3 OSDs, one 15 GiB virtual disk per VM
- RGW endpoint: `http://192.168.56.101:7480`
- Dashboard endpoint: exposed by the active Ceph manager

The rest of the Data Lake stack still runs through Docker Compose on the
Windows host:

- Spark standalone for transformations and Spark SQL checks;
- Trino for gold-table queries;
- Airflow for orchestration;
- Prometheus/Grafana for the local application stack when needed.

## Repository Changes

The Docker Compose runtime was changed so services no longer hard-code MinIO as
the only S3 endpoint. The stack now reads S3 configuration from `.env`.

Important behavior:

- `S3_ENDPOINT` can point to MinIO for local development.
- `S3_ENDPOINT` can point to Ceph RGW for the 3-VM lab.
- Spark, Airflow, Trino, bucket initialization, smoke tests, and benchmarks use
  the same S3-compatible configuration path.
- Proxy environment handling was added so local Windows proxy settings do not
  break access to the host-only Ceph network.

The `.env` file contains local credentials and must remain uncommitted.

## Validated Commands

Ceph S3 reachability:

```bash
make health
make storage-smoke
```

NYC Taxi pipeline on Ceph-backed buckets:

```bash
make prepare-nyc-taxi
make ingest
make spark-submit-silver
make spark-submit-gold
make query-smoke
```

Trino validation:

```bash
make trino-up
make trino-smoke
make trino-down
```

Storage benchmark:

```bash
make benchmark-storage BENCHMARK_RUN_ID=ceph-3vm-baseline STORAGE_BENCHMARK_BACKEND=ceph-rgw STORAGE_BENCHMARK_OBJECT_SIZES=1MiB STORAGE_BENCHMARK_CONCURRENCY=1,4 STORAGE_BENCHMARK_OPERATIONS=put,get,mixed STORAGE_BENCHMARK_WARMUP=1 STORAGE_BENCHMARK_ITERATIONS=3
```

## Validation Results

The end-to-end Ceph run completed successfully:

| Area | Result |
|---|---|
| Ceph health before pipeline | `HEALTH_OK` |
| S3 health check | Passed |
| S3 storage smoke | Passed |
| Bronze upload | Passed |
| Spark bronze to silver | Passed |
| Spark silver to gold | Passed |
| Spark SQL query smoke | 6/6 queries passed |
| Trino gold query smoke | Passed |
| Storage benchmark | Completed with zero errors |
| Single-node outage smoke | Passed and recovered to `HEALTH_OK` |

Key pipeline outputs:

| Step | Metric | Value |
|---|---:|---:|
| Bronze to silver | input rows | 3,475,226 |
| Bronze to silver | output rows | 3,328,747 |
| Bronze to silver | rejected rows | 146,479 |
| Silver to gold | daily rows | 33 |
| Silver to gold | location rows | 255,307 |
| Silver to gold | payment rows | 159 |
| Trino smoke | total trips | 3,328,747 |

Ceph-backed bucket locations:

```text
s3://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet
s3://datalake-silver/nyc-taxi/year=2025/month=01
s3://datalake-gold/daily_trip_metrics/year=2025/month=01
s3://datalake-gold/location_metrics/year=2025/month=01
s3://datalake-gold/payment_metrics/year=2025/month=01
```

## Fault-Tolerance Result

A controlled outage was tested by gracefully shutting down `hadoop-worker2`.
Ceph correctly reported `HEALTH_WARN`, one MON out of quorum, one OSD down, and
degraded placement groups. The cluster still retained quorum through
`hadoop-master` and `hadoop-worker1`.

During the outage:

- `make health` passed;
- `make storage-smoke` passed;
- RGW continued serving S3 requests through `hadoop-master`.

After `hadoop-worker2` was restarted, the cluster returned to:

```text
health: HEALTH_OK
mon: 3 daemons, quorum hadoop-master,hadoop-worker1,hadoop-worker2
osd: 3 osds: 3 up, 3 in
pgs: 194 active+clean
```

## Benchmark Interpretation

The Ceph RGW benchmark completed successfully, but the local MinIO baseline was
faster in this lab. That result is expected because the Ceph path includes
VirtualBox networking, RGW, replicated Ceph pools, and virtual OSD disks.

The comparison proves functional compatibility and gives an initial lab
baseline. It does not prove a general production performance ranking between
MinIO and Ceph.

## Operational Guidance

Because the Windows host is memory constrained, avoid starting the full stack
at once. Prefer narrow service groups:

| Task | Start |
|---|---|
| S3 checks | No Docker Compose services required |
| Spark pipeline | `spark-master`, `spark-worker`, `spark-submit` |
| Trino validation | `trino` only |
| Airflow workflow | `postgres`, `spark-master`, `spark-worker`, `airflow-init`, `airflow-webserver`, `airflow-scheduler` |
| Monitoring | Start only when collecting metrics |

After host sleep or reboot, check Ceph first:

```bash
sudo cephadm shell -- ceph -s
```

If the cluster reports old crash records after recovery, inspect and archive
them only after confirming the daemons are healthy:

```bash
sudo cephadm shell -- ceph crash ls-new
sudo cephadm shell -- ceph crash archive-all
```

## Remaining Limitations

This phase is complete for lab validation, but it still has these limitations:

- the Ceph cluster runs on small VirtualBox VMs;
- each OSD is a small virtual disk;
- benchmark iterations are limited;
- host CPU, memory, disk, and network metrics were not captured during every
  benchmark;
- the single-node outage test is a controlled smoke test, not a full disaster
  recovery campaign;
- Airflow has not yet been revalidated end-to-end against Ceph after the
  Compose endpoint changes.

## Recommended Next Phase

The next phase should focus on orchestration and observability:

1. Run the Airflow DAG end-to-end against Ceph RGW.
2. Record Airflow task logs and final S3 bucket outputs.
3. Decide whether to integrate Ceph metrics into the repo monitoring stack or
   document Ceph Dashboard/cephadm monitoring as the storage-side observability
   path.
4. Add a final architecture diagram or report section that separates storage
   services on the VMs from compute/orchestration services in Docker Compose.
