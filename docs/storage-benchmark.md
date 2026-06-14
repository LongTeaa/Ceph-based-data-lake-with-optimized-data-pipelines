# Storage Benchmark

Phase 7 adds a reusable S3-compatible storage benchmark runner. It uses the
same `.env` S3 configuration as the bucket initialization and smoke-test
scripts, so it can run against local MinIO or a Ceph RGW endpoint.

## Run

Start local storage and initialize buckets:

```bash
make up
make init-buckets
```

Run the default lightweight benchmark:

```bash
make benchmark-storage
```

The default local matrix is intentionally small:

```text
object sizes: 4KiB, 1MiB
concurrency: 1, 4
operations: put, get, mixed
warm-up operations per scenario: 2
measured operations per scenario: 10
```

For a quick smoke run:

```bash
make benchmark-storage STORAGE_BENCHMARK_OBJECT_SIZES=4KiB STORAGE_BENCHMARK_CONCURRENCY=1 STORAGE_BENCHMARK_OPERATIONS=put STORAGE_BENCHMARK_WARMUP=0 STORAGE_BENCHMARK_ITERATIONS=1
```

For the fuller workflow matrix:

```bash
make benchmark-storage STORAGE_BENCHMARK_OBJECT_SIZES=4KiB,1MiB,64MiB,256MiB STORAGE_BENCHMARK_CONCURRENCY=1,4,16,32 STORAGE_BENCHMARK_OPERATIONS=put,get,mixed STORAGE_BENCHMARK_WARMUP=5 STORAGE_BENCHMARK_ITERATIONS=50
```

Use the same command against Ceph RGW after changing `.env`:

```dotenv
S3_ENDPOINT=http://<rgw-host>:7480
S3_ACCESS_KEY=<rgw-access-key>
S3_SECRET_KEY=<rgw-secret-key>
STORAGE_BACKEND=ceph-rgw
```

Or set the backend label for one run:

```bash
make benchmark-storage STORAGE_BENCHMARK_BACKEND=ceph-rgw
```

## Output

Results are written under:

```text
benchmark/results/<run_id>/storage/s3/<timestamp>/
```

Each run directory contains:

```text
environment.json
scenario.json
raw-results.jsonl
summary.csv
summary.json
notes.json
```

`raw-results.jsonl` keeps one record per object operation. `summary.csv`
contains per-scenario:

- successful/failed operation counts;
- total measured wall time;
- throughput in MiB/s;
- operations per second;
- latency p50, p95, and p99.

The runner uploads deterministic payloads, validates GET checksums, and deletes
benchmark objects by default. Pass `--keep-objects` directly to
`benchmark/storage/s3_benchmark.py` only when you need to inspect generated
objects.

## Notes

The local MinIO result is useful for checking the benchmark method and demoing
metrics in Prometheus/Grafana. It should not be used as evidence for production
Ceph performance.

For fair Ceph-vs-MinIO comparison, keep the same client host, payload sizes,
concurrency, bucket policy, network path, and measured iterations. Record the
Ceph pool topology and MinIO topology alongside each run.
