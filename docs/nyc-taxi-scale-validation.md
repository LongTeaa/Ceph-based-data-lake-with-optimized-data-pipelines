# NYC Taxi Scale Validation

This document records the first scaled NYC Taxi validation run against Ceph
RGW. The goal is to show that the pipeline can move beyond a single monthly
file and process a multi-file real dataset while keeping the laptop resource
profile conservative.

These results are functional scale validation results, not production
throughput claims.

## Purpose

The mentor feedback asked for a larger data story around Ceph as Data Lake
storage. This validation adds that evidence by:

- downloading multiple real NYC TLC Yellow Taxi Parquet files;
- creating one manifest that describes all files in the batch;
- uploading all source files into the Ceph bronze bucket;
- running Spark bronze-to-silver and silver-to-gold over the multi-file batch;
- running Spark SQL query smoke checks over the scaled silver/gold outputs.

## Data Source

Source: NYC TLC Yellow Taxi Trip Records.

The scale downloader uses official monthly Parquet files from the NYC TLC
CloudFront data endpoint:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

The latest resource-safe end-to-end scale run used six months:

| Month | Rows | Local file size |
|---|---:|---:|
| `2023-01` | 3,066,766 | 47,673,370 bytes |
| `2023-02` | 2,913,955 | 47,748,012 bytes |
| `2023-03` | 3,403,766 | 56,127,762 bytes |
| `2023-04` | 3,288,250 | 54,222,699 bytes |
| `2023-05` | 3,513,649 | 58,654,627 bytes |
| `2023-06` | 3,307,234 | 54,999,465 bytes |
| **Total** | **19,493,620** | **319,425,935 bytes** |

Manifest:

```text
data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json
```

Because `data/source/*` is ignored, this manifest is local run evidence unless
it is intentionally force-added for a report artifact.

## Commands

Download the first six files and create the multi-file manifest:

```powershell
make download-nyc-taxi-scale NYC_TAXI_SCALE_LIMIT_FILES=6 NYC_TAXI_SCALE_MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json
```

Upload the manifest-described files to Ceph bronze:

```powershell
make ingest MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json
```

Start Spark with a low-memory profile:

```powershell
$env:SPARK_DRIVER_MEMORY='1g'
$env:SPARK_EXECUTOR_MEMORY='1g'
$env:SPARK_WORKER_MEMORY='1g'
$env:SPARK_WORKER_CORES='1'
$env:SPARK_SQL_SHUFFLE_PARTITIONS='4'
make spark-up
```

Run the scaled pipeline:

```powershell
make spark-submit-silver MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json OUTPUT_DIR=results-scale
make spark-submit-gold MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json OUTPUT_DIR=results-scale
make query-smoke MANIFEST=data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2023-06_6files.manifest.json OUTPUT_DIR=results-scale
```

Stop Spark after the run to avoid memory pressure:

```powershell
make spark-down
```

## Ceph Locations

Bronze source files:

```text
s3://datalake-bronze/nyc-taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet
s3://datalake-bronze/nyc-taxi/year=2023/month=02/yellow_tripdata_2023-02.parquet
s3://datalake-bronze/nyc-taxi/year=2023/month=03/yellow_tripdata_2023-03.parquet
s3://datalake-bronze/nyc-taxi/year=2023/month=04/yellow_tripdata_2023-04.parquet
s3://datalake-bronze/nyc-taxi/year=2023/month=05/yellow_tripdata_2023-05.parquet
s3://datalake-bronze/nyc-taxi/year=2023/month=06/yellow_tripdata_2023-06.parquet
```

Silver output:

```text
s3://datalake-silver/nyc-taxi/year=scale/month=2023-01_2023-06_6files
```

Gold outputs:

```text
s3://datalake-gold/daily_trip_metrics/year=scale/month=2023-01_2023-06_6files
s3://datalake-gold/location_metrics/year=scale/month=2023-01_2023-06_6files
s3://datalake-gold/payment_metrics/year=scale/month=2023-01_2023-06_6files
```

## Pipeline Results

Bronze to silver:

| Metric | Value |
|---|---:|
| Source files | 6 |
| Input rows | 19,493,620 |
| Output rows | 19,312,233 |
| Rejected rows | 181,387 |
| Duration seconds | 1,798.381 |

Silver to gold:

| Metric | Value |
|---|---:|
| Input rows | 19,312,233 |
| Daily metric rows | 197 |
| Location metric rows | 1,270,477 |
| Payment metric rows | 931 |
| Duration seconds | 2,151.675 |

Spark SQL query smoke:

| Query | Rows | Duration seconds |
|---|---:|---:|
| `01_daily_revenue` | 197 | 41.495 |
| `02_top_pickup_locations` | 10 | 49.880 |
| `03_avg_tip_by_payment` | 6 | 45.808 |
| `04_hourly_distance_fare` | 24 | 154.685 |
| `05_selective_pickup_date` | 1 | 0.971 |
| `06_full_scan_location_aggregation` | 20 | 133.775 |

Full local metrics were written under:

```text
results-scale/nyc_taxi_bronze_to_silver/year=scale/month=2023-01_2023-06_6files/metrics.json
results-scale/nyc_taxi_silver_to_gold/year=scale/month=2023-01_2023-06_6files/metrics.json
results-scale/nyc_taxi_query_smoke/year=scale/month=2023-01_2023-06_6files/metrics.json
```

`results-scale/` is ignored to keep large local run artifacts out of Git.

## Implementation Notes

The scaled real NYC files exposed a schema drift issue: some Parquet files
store columns such as `passenger_count` with a different physical type.

The bronze-to-silver Spark job now reads each manifest source URI separately,
applies the shared cleaning and casting logic per file, then combines the
normalized DataFrames with `unionByName`. This keeps the one-file path working
while allowing multi-file real NYC batches.

The manifest reader now supports both:

- the original single-file manifest;
- a multi-file scale manifest with a `files` array.

## Path Toward 100 Million Rows

The safe scale point for this laptop lab is the six-file run:

| Target | Approximate size | Recommendation |
|---|---:|---|
| 3 files | 9.38M rows | Completed initial smoke-scale run |
| 6 files | 19.49M rows | Completed end-to-end scale run |
| 16 files | 51.38M rows | Downloaded and uploaded to bronze, but Spark processing was not stable on the laptop |
| 20-30 files | roughly 60M-100M rows | Keep as a script-supported target for larger hardware |

For the final report, this is a stronger story than generating duplicate fake
rows because the input is real public data with real schema and quality issues.
The 16-file raw batch proves that Ceph RGW can hold a larger bronze dataset in
this lab, while the six-file run is the largest completed end-to-end pipeline
validation under the current memory constraints.
