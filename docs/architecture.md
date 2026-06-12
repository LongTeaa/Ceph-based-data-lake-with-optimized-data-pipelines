# Architecture

This project builds a research Data Lake using Ceph Object Storage as the
central storage layer.

## Target Flow

```text
Local source datasets
    -> ingestion utilities
    -> Ceph RGW via S3 API
    -> bronze bucket
    -> Spark ETL
    -> silver bucket
    -> Spark aggregation
    -> gold bucket
    -> Spark SQL / Trino query benchmark
```

## Storage Layers

- `bronze`: immutable source files such as NYC Taxi Parquet, derived CSV/JSONL
  experiment inputs, optional image files, and source manifests.
- `silver`: cleaned and typed Parquet datasets partitioned for analytics.
- `gold`: aggregate Parquet datasets used by demos and query benchmarks.
- `system`: checkpoints, run manifests, and benchmark result objects.

## Dataset Strategy

NYC Yellow Taxi is the primary analytics dataset. Synthetic tabular data is used
for test cases and scale experiments. Synthetic binary files are used only for
object-storage PUT/GET benchmarks. Optional Wikimedia images demonstrate storage
of unstructured data; Spark processes only their metadata.

## Environments

- `local`: functional development and small demos, usually Docker Compose.
- `benchmark`: storage and fault-tolerance evaluation, ideally 3 Linux nodes or
  VM instances with Ceph deployed using `cephadm`.

The local environment is not used for final Ceph-vs-MinIO performance claims.

## Phase 1 Local Storage

The local Phase 1 backend is MinIO, exposed as an S3-compatible endpoint at
`http://localhost:19000`. This is a development convenience for validating S3
scripts and bucket layout. The same bucket and smoke-test scripts are intended
to run unchanged against Ceph RGW once `S3_ENDPOINT`, `S3_ACCESS_KEY`, and
`S3_SECRET_KEY` point to a real RGW user.
