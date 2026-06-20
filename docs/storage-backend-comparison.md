# Storage Backend Comparison

This document compares the current S3 storage benchmark baselines for local
MinIO and the 3-VM Ceph RGW lab. The goal is to compare observed behavior in
the project lab, not to make general production claims about either system.

## Compared Runs

MinIO baseline:

```text
benchmark/results/local-baseline/storage/s3/20260616T084308Z/
```

Ceph RGW baseline:

```text
benchmark/results/ceph-3vm-baseline/storage/s3/20260620T094044Z/
```

Common scenario subset:

```text
object size: 1 MiB
concurrency: 1, 4
operations: PUT, GET, mixed
measured iterations: 3
errors: 0 for all compared scenarios
```

## Environment Differences

The results are not a fully fair apples-to-apples benchmark.

| Dimension | MinIO local baseline | Ceph RGW lab baseline |
|---|---|---|
| Backend | MinIO | Ceph RGW |
| Deployment | Docker Compose on local machine | 3 Ubuntu VirtualBox VMs |
| Client | Windows host Python/boto3 | Windows host Python/boto3 |
| Storage path | Docker volume/local disk path | RGW -> Ceph pools -> 3 virtual OSD disks |
| Network path | Local Docker/host path | VirtualBox host-only network |
| Redundancy | MinIO local dev backend | Ceph replicated/distributed storage |
| Intended use | Functional/dev baseline | Ceph integration and lab benchmark |

Because the Ceph path crosses VM networking and writes through a distributed
storage stack, higher latency and lower throughput are expected in this lab.

## Throughput

Throughput is shown in MiB/s.

| Operation | Concurrency | MinIO | Ceph RGW | Observation |
|---|---:|---:|---:|---|
| GET | 1 | 73.817 | 16.398 | MinIO higher |
| GET | 4 | 135.459 | 13.922 | MinIO higher |
| mixed | 1 | 25.001 | 8.403 | MinIO higher |
| mixed | 4 | 62.633 | 10.433 | MinIO higher |
| PUT | 1 | 20.719 | 6.221 | MinIO higher |
| PUT | 4 | 35.872 | 7.585 | MinIO higher |

In this lab, MinIO had higher 1 MiB throughput for all compared scenarios. The
Ceph run still completed every scenario with zero errors, which validates the
benchmark runner and S3 integration path against RGW.

## Latency

p95 latency is shown in milliseconds.

| Operation | Concurrency | MinIO p95 | Ceph RGW p95 | Observation |
|---|---:|---:|---:|---|
| GET | 1 | 13.942 | 72.165 | Ceph higher |
| GET | 4 | 19.915 | 210.195 | Ceph higher |
| mixed | 1 | 64.776 | 128.502 | Ceph higher |
| mixed | 4 | 46.454 | 285.377 | Ceph higher |
| PUT | 1 | 76.160 | 219.659 | Ceph higher |
| PUT | 4 | 82.092 | 390.216 | Ceph higher |

Ceph RGW had higher p95 latency in this lab. This is consistent with the extra
network and storage layers in the 3-VM setup.

## Scaling With Concurrency

In the MinIO run, increasing concurrency from `1` to `4` improved throughput for
GET, PUT, and mixed workloads.

In the Ceph RGW run:

- PUT throughput improved from `6.221` to `7.585 MiB/s`;
- mixed throughput improved from `8.403` to `10.433 MiB/s`;
- GET throughput decreased from `16.398` to `13.922 MiB/s`;
- p95 latency increased for every operation.

This suggests the current Ceph lab is resource constrained. The VM network,
virtual disks, OSD scheduling, or host memory pressure may dominate before
additional client concurrency can improve read throughput.

## Interpretation

The current evidence supports these conclusions:

- both backends are functionally compatible with the same S3 benchmark runner;
- Ceph RGW is correctly integrated with the Data Lake buckets and S3 API;
- the local MinIO baseline is faster in this small lab benchmark;
- the Ceph 3-VM lab has higher latency, especially at concurrency `4`;
- Ceph results should be treated as lab evidence, not production performance.

The comparison does not prove that MinIO is generally faster than Ceph. The
benchmarks use different deployment models and a very small number of measured
iterations.

## Next Benchmark Improvements

To make the comparison stronger:

1. Run both backends with the same object matrix: `4KiB`, `1MiB`, `64MiB`.
2. Increase measured iterations from `3` to at least `10`.
3. Keep warm-up operations enabled.
4. Capture host CPU, memory, disk, and network metrics during the run.
5. Record Ceph pool size, OSD placement, VM RAM/CPU allocation, and disk type.
6. Avoid running Spark, Airflow, Trino, and Grafana during storage benchmarks.
