# Spark Standalone

Phase 4 adds a local Spark standalone runtime for running Spark jobs through a
master/worker pair instead of `local[*]`.

## Services

Docker Compose defines:

```text
spark-master
spark-worker
spark-submit
```

The services use a local Spark image built from:

```text
docker/spark/Dockerfile
```

The image starts from `apache/spark:3.5.1` and adds Python so PySpark jobs can
run through `spark-submit`.

`spark-master` accepts applications on:

```text
spark://spark-master:7077
```

The Spark master UI is exposed on:

```text
http://localhost:8081
```

The Spark worker UI is exposed on:

```text
http://localhost:8082
```

## Start And Stop

Start Spark standalone with MinIO:

```bash
make spark-up
```

Stop Spark services:

```bash
make spark-down
```

Follow Spark logs:

```bash
make spark-logs
```

## Submit Jobs

Run the bronze-to-silver job through Spark standalone:

```bash
make spark-submit-silver
```

Run the silver-to-gold job through Spark standalone:

```bash
make spark-submit-gold
```

These targets use the `spark-submit` service, which mounts the repository at:

```text
/opt/spark/project
```

Inside Docker, Spark uses:

```text
S3_ENDPOINT=http://minio:9000
SPARK_MASTER_URL=spark://spark-master:7077
```

This differs from host-local runs, where `.env` may use:

```text
S3_ENDPOINT=http://localhost:9000
SPARK_MASTER_URL=local[*]
```

## Recommended Validation Order

Validate Spark standalone directly first:

```bash
make spark-up
make init-buckets
make prepare-nyc-taxi
make ingest
make spark-submit-silver
make spark-submit-gold
```

After both submit targets pass, start Airflow:

```bash
make airflow-up
```

The Airflow image includes a Spark client, so the DAG transform tasks call
`spark-submit --master spark://spark-master:7077` and run the applications on
the same `spark-master`/`spark-worker` services.

## Notes

This Docker Compose Spark cluster is a local integration environment. It is
good for proving orchestration and distributed Spark execution, but benchmark
results from a single laptop should be reported as local baseline results, not
as production Ceph/Spark performance.
