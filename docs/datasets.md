# Datasets

Phase 2 prepares source datasets and uploads immutable source files into the
bronze bucket.

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
