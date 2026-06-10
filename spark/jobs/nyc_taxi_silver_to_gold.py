#!/usr/bin/env python3
"""Aggregate silver NYC Taxi data into gold analytics datasets."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_settings
from spark.jobs.nyc_taxi_bronze_to_silver import (
    configure_s3a,
    create_spark_session,
)
from spark.jobs.nyc_taxi_common import (
    batch_from_manifest,
    gold_paths,
    load_manifest,
    metrics_path,
)


JOB_NAME = "nyc_taxi_silver_to_gold"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate NYC Taxi silver data to gold.")
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
        help="Write mode for gold datasets.",
    )
    return parser.parse_args()


def require_pyspark_functions():
    try:
        from pyspark.sql import functions as F
    except ImportError as exc:
        print(
            "Missing dependency: pyspark. Install dependencies with "
            "`pip install -r requirements.txt` inside your virtual environment.",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    return F


def daily_trip_metrics(silver_df):
    F = require_pyspark_functions()
    return (
        silver_df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("fare_amount").alias("fare_revenue"),
            F.sum("tip_amount").alias("total_tip"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("passenger_count").alias("avg_passenger_count"),
        )
        .orderBy("pickup_date")
    )


def location_metrics(silver_df):
    F = require_pyspark_functions()
    return (
        silver_df.groupBy("pickup_date", "pu_location_id", "do_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_trip_distance"),
        )
        .orderBy("pickup_date", "pu_location_id", "do_location_id")
    )


def payment_metrics(silver_df):
    F = require_pyspark_functions()
    return (
        silver_df.groupBy("pickup_date", "payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("fare_amount").alias("fare_revenue"),
            F.sum("tip_amount").alias("total_tip"),
            F.avg("tip_amount").alias("avg_tip_amount"),
        )
        .orderBy("pickup_date", "payment_type")
    )


def write_metrics(path: Path, metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_gold_dataframe(df, uri: str, mode: str) -> int:
    row_count = df.count()
    df.write.mode(mode).partitionBy("pickup_date").parquet(uri)
    return row_count


def run_transform(manifest_path: Path, output_dir: Path, mode: str) -> dict[str, Any]:
    start = time.perf_counter()
    settings = load_settings()
    manifest = load_manifest(manifest_path)
    batch = batch_from_manifest(manifest, settings.silver_bucket)
    paths = gold_paths(settings.gold_bucket, batch.year, batch.month)

    spark = create_spark_session()
    try:
        configure_s3a(spark, settings)
        silver_df = spark.read.parquet(batch.silver_uri)
        input_rows = silver_df.count()

        daily_df = daily_trip_metrics(silver_df)
        location_df = location_metrics(silver_df)
        payment_df = payment_metrics(silver_df)

        daily_rows = write_gold_dataframe(daily_df, paths.daily_metrics_uri, mode)
        location_rows = write_gold_dataframe(location_df, paths.location_metrics_uri, mode)
        payment_rows = write_gold_dataframe(payment_df, paths.payment_metrics_uri, mode)

        duration_seconds = round(time.perf_counter() - start, 3)
        metrics = {
            "job_name": JOB_NAME,
            "dataset": batch.dataset,
            "taxi_type": batch.taxi_type,
            "year": batch.year,
            "month": batch.month,
            "silver_uri": batch.silver_uri,
            "daily_metrics_uri": paths.daily_metrics_uri,
            "location_metrics_uri": paths.location_metrics_uri,
            "payment_metrics_uri": paths.payment_metrics_uri,
            "input_rows": input_rows,
            "daily_rows": daily_rows,
            "location_rows": location_rows,
            "payment_rows": payment_rows,
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
        print(f"silver_uri: {metrics['silver_uri']}")
        print(f"daily_metrics_uri: {metrics['daily_metrics_uri']}")
        print(f"location_metrics_uri: {metrics['location_metrics_uri']}")
        print(f"payment_metrics_uri: {metrics['payment_metrics_uri']}")
        print(f"input_rows: {metrics['input_rows']}")
        print(f"daily_rows: {metrics['daily_rows']}")
        print(f"location_rows: {metrics['location_rows']}")
        print(f"payment_rows: {metrics['payment_rows']}")
        print("nyc_taxi_silver_to_gold ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_silver_to_gold failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
