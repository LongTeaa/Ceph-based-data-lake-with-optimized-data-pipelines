# Datasets

Phase 2 prepares source datasets and uploads immutable source files into the
bronze bucket. Phase 3 transforms bronze NYC Taxi data into silver Parquet and
gold analytics datasets.

## NYC Yellow Taxi

The primary analytics dataset is NYC Yellow Taxi Trip Records.

Expected local source file:

```text
data/source/nyc-taxi/yellow_tripdata_2025-01.parquet
```

Prepare a local manifest:

```bash
make prepare-nyc-taxi
```

To prepare a different local file:

```bash
make prepare-nyc-taxi SOURCE=data/source/nyc-taxi FILE=yellow_tripdata_2025-02.parquet
```

This creates:

```text
data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
```

The manifest records:

- dataset and taxi type;
- year and month parsed from the file name;
- local source path;
- file size;
- SHA-256 checksum;
- bronze bucket/key/URI;
- source URL.

Upload source Parquet and manifest to bronze:

```bash
make ingest
```

To upload a specific manifest:

```bash
make ingest MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2025-02.manifest.json
```

Expected bronze layout:

```text
s3://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet
s3://datalake-bronze/nyc-taxi/year=2025/month=01/manifest.json
```

The upload is idempotent. If a remote object already exists with the same
checksum and size, the script prints `exists` and skips it. If a remote object
exists with different content, the script fails unless you intentionally pass
`--force` to `ingestion/bronze_upload.py`.

## Notes

Row count and schema inspection are deferred to the Spark phase. Phase 2 only
locks down file provenance, checksum, and bronze object layout.

## Silver NYC Taxi

Transform the manifest-described bronze file into cleaned silver Parquet and
gold analytics datasets:

```bash
make transform
```

To transform a specific manifest:

```bash
make transform MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2025-02.manifest.json
```

Expected silver layout:

```text
s3://datalake-silver/nyc-taxi/year=2025/month=01/pickup_date=YYYY-MM-DD/*.parquet
```

The first silver job:

- reads the source Parquet object recorded in the manifest;
- casts key columns to timestamp, integer, and double types;
- renames pickup/dropoff location columns to snake case;
- removes trips with invalid timestamps, negative distance, or negative amounts;
- drops duplicate trips based on stable trip attributes;
- adds `pickup_date` and writes partitioned Parquet;
- writes run metrics under `results/nyc_taxi_bronze_to_silver/`.

## Gold NYC Taxi

Aggregate silver NYC Taxi data into gold datasets:

```bash
make transform-gold
```

Expected gold layout:

```text
s3://datalake-gold/daily_trip_metrics/year=2025/month=01/pickup_date=YYYY-MM-DD/*.parquet
s3://datalake-gold/location_metrics/year=2025/month=01/pickup_date=YYYY-MM-DD/*.parquet
s3://datalake-gold/payment_metrics/year=2025/month=01/pickup_date=YYYY-MM-DD/*.parquet
```

The gold job creates:

- daily trip metrics with trip count, revenue, tips, distance, and passenger
  averages;
- pickup/dropoff location metrics with trip count, revenue, and average
  distance;
- payment metrics with trip count, revenue, fare, total tip, and average tip;
- run metrics under `results/nyc_taxi_silver_to_gold/`.
