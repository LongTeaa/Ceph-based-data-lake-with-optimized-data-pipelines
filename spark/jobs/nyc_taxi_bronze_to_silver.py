#!/usr/bin/env python3
"""Transform bronze NYC Taxi Parquet data into cleaned silver Parquet data."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_dotenv, load_settings
from spark.jobs.nyc_taxi_common import (
    batch_from_manifest,
    load_manifest,
    metrics_path,
    missing_required_columns,
)


JOB_NAME = "nyc_taxi_bronze_to_silver"
DEFAULT_SPARK_JARS_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4"
DEDUP_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "trip_distance",
    "total_amount",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform NYC Taxi bronze data to silver.")
    parser.add_argument(
        "--manifest-path",
        required=True,
        help="Local manifest created by ingestion/nyc_taxi_manifest.py.",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Local directory for metrics JSON.",
    )
    parser.add_argument(
        "--mode",
        choices=("overwrite", "append"),
        default="overwrite",
        help="Write mode for the silver partition.",
    )
    return parser.parse_args()


def require_pyspark():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import DoubleType, IntegerType, TimestampType
    except ImportError as exc:
        print(
            "Missing dependency: pyspark. Install dependencies with "
            "`pip install -r requirements.txt` inside your virtual environment.",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    return SparkSession, F, DoubleType, IntegerType, TimestampType


def get_config_value(name: str, dotenv_values: dict[str, str], default: str = "") -> str:
    return os.getenv(name) or dotenv_values.get(name) or default


def configure_s3a(spark, settings) -> None:
    dotenv_values = load_dotenv()
    local_dir = get_config_value("SPARK_LOCAL_DIR", dotenv_values, str(PROJECT_ROOT / ".spark-tmp"))
    local_path = Path(local_dir)
    if not local_path.is_absolute():
        local_path = PROJECT_ROOT / local_path
    local_path.mkdir(parents=True, exist_ok=True)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", settings.endpoint)
    hadoop_conf.set("fs.s3a.access.key", settings.access_key)
    hadoop_conf.set("fs.s3a.secret.key", settings.secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", str(settings.path_style_access).lower())
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(settings.use_ssl).lower())
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.connection.timeout", "10000")
    hadoop_conf.set("fs.s3a.attempts.maximum", "5")
    hadoop_conf.set("fs.s3a.buffer.dir", str(local_path))
    hadoop_conf.set("hadoop.tmp.dir", str(local_path))


def create_spark_session(app_name: str = JOB_NAME):
    SparkSession, _, _, _, _ = require_pyspark()
    dotenv_values = load_dotenv()
    master_url = get_config_value("SPARK_MASTER_URL", dotenv_values, "local[*]")
    shuffle_partitions = get_config_value("SPARK_SQL_SHUFFLE_PARTITIONS", dotenv_values, "8")
    driver_memory = get_config_value("SPARK_DRIVER_MEMORY", dotenv_values, "2g")
    executor_memory = get_config_value("SPARK_EXECUTOR_MEMORY", dotenv_values, "2g")
    jars_packages = get_config_value(
        "SPARK_JARS_PACKAGES",
        dotenv_values,
        DEFAULT_SPARK_JARS_PACKAGES,
    )
    ivy_dir = get_config_value(
        "SPARK_IVY_DIR",
        dotenv_values,
        str(PROJECT_ROOT / ".spark-ivy"),
    )
    ivy_path = Path(ivy_dir)
    if not ivy_path.is_absolute():
        ivy_dir = str(PROJECT_ROOT / ivy_path)
    local_dir = get_config_value(
        "SPARK_LOCAL_DIR",
        dotenv_values,
        str(PROJECT_ROOT / ".spark-tmp"),
    )
    local_path = Path(local_dir)
    if not local_path.is_absolute():
        local_dir = str(PROJECT_ROOT / local_path)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.local.dir", local_dir)
    )
    if jars_packages:
        builder = builder.config("spark.jars.packages", jars_packages)
    return builder.getOrCreate()


def clean_nyc_taxi_dataframe(df):
    _, F, DoubleType, IntegerType, TimestampType = require_pyspark()
    missing = missing_required_columns(df.columns)
    if missing:
        raise ValueError("Missing required NYC Taxi columns: " + ", ".join(missing))

    cleaned = (
        df.withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("pu_location_id", F.col("PULocationID").cast(IntegerType()))
        .withColumn("do_location_id", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID")
    )

    valid = cleaned.where(
        (F.col("pickup_datetime").isNotNull())
        & (F.col("dropoff_datetime").isNotNull())
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        & (F.col("pickup_date").isNotNull())
        & (F.col("trip_distance") >= 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("tip_amount") >= 0)
        & (F.col("total_amount") >= 0)
    )

    return valid.dropDuplicates(DEDUP_COLUMNS)


def union_clean_nyc_taxi_files(spark, source_uris: tuple[str, ...]):
    cleaned_frames = []
    input_rows = 0
    for source_uri in source_uris:
        source_df = spark.read.parquet(source_uri)
        input_rows += source_df.count()
        cleaned_frames.append(clean_nyc_taxi_dataframe(source_df))

    if not cleaned_frames:
        raise ValueError("At least one NYC Taxi source URI is required")

    silver_df = cleaned_frames[0]
    for cleaned_df in cleaned_frames[1:]:
        silver_df = silver_df.unionByName(cleaned_df, allowMissingColumns=True)

    return input_rows, silver_df.dropDuplicates(DEDUP_COLUMNS)


def write_metrics(path: Path, metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def run_transform(manifest_path: Path, output_dir: Path, mode: str) -> dict[str, Any]:
    start = time.perf_counter()
    settings = load_settings()
    manifest = load_manifest(manifest_path)
    batch = batch_from_manifest(manifest, settings.silver_bucket)

    spark = create_spark_session()
    try:
        configure_s3a(spark, settings)
        input_rows, silver_df = union_clean_nyc_taxi_files(spark, batch.source_uris)
        output_rows = silver_df.count()

        (
            silver_df.write.mode(mode)
            .partitionBy("pickup_date")
            .parquet(batch.silver_uri)
        )

        duration_seconds = round(time.perf_counter() - start, 3)
        metrics = {
            "job_name": JOB_NAME,
            "dataset": batch.dataset,
            "taxi_type": batch.taxi_type,
            "year": batch.year,
            "month": batch.month,
            "bronze_uri": batch.source_uri,
            "bronze_uris": list(batch.source_uris),
            "source_file_count": len(batch.source_uris),
            "silver_uri": batch.silver_uri,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "rejected_rows": input_rows - output_rows,
            "duration_seconds": duration_seconds,
            "write_mode": mode,
        }
        write_metrics(metrics_path(output_dir, JOB_NAME, batch.year, batch.month), metrics)
        return metrics
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        metrics = run_transform(Path(args.manifest_path), Path(args.output_dir), args.mode)
        print(f"bronze_uri: {metrics['bronze_uri']}")
        print(f"silver_uri: {metrics['silver_uri']}")
        print(f"input_rows: {metrics['input_rows']}")
        print(f"output_rows: {metrics['output_rows']}")
        print(f"rejected_rows: {metrics['rejected_rows']}")
        print("nyc_taxi_bronze_to_silver ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_bronze_to_silver failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
