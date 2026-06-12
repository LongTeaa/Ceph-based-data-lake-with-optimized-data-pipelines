#!/usr/bin/env python3
"""Run standard Spark SQL smoke queries against NYC Taxi silver and gold data."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from string import Template
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_settings
from spark.jobs.nyc_taxi_bronze_to_silver import configure_s3a, create_spark_session
from spark.jobs.nyc_taxi_common import batch_from_manifest, gold_paths, load_manifest, metrics_path


JOB_NAME = "nyc_taxi_query_smoke"
DEFAULT_SQL_DIR = PROJECT_ROOT / "spark" / "sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NYC Taxi Spark SQL smoke queries.")
    parser.add_argument(
        "--manifest-path",
        required=True,
        help="Local manifest created by ingestion/nyc_taxi_manifest.py.",
    )
    parser.add_argument(
        "--sql-dir",
        default=str(DEFAULT_SQL_DIR),
        help="Directory containing .sql query files.",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Local directory for metrics JSON.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Maximum result rows to store per query in metrics JSON.",
    )
    return parser.parse_args()


def write_metrics(path: Path, metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def load_sql_queries(sql_dir: Path) -> list[Path]:
    queries = sorted(sql_dir.glob("*.sql"))
    if not queries:
        raise ValueError(f"No SQL files found in {sql_dir}")
    return queries


def render_sql(path: Path, values: dict[str, str]) -> str:
    template = Template(path.read_text(encoding="utf-8"))
    return template.safe_substitute(values)


def collect_sample(df, limit: int) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.limit(limit).collect()]


def run_queries(
    manifest_path: Path,
    sql_dir: Path,
    output_dir: Path,
    sample_limit: int,
) -> dict[str, Any]:
    start = time.perf_counter()
    settings = load_settings()
    manifest = load_manifest(manifest_path)
    batch = batch_from_manifest(manifest, settings.silver_bucket)
    paths = gold_paths(settings.gold_bucket, batch.year, batch.month)

    spark = create_spark_session(JOB_NAME)
    try:
        configure_s3a(spark, settings)

        silver_df = spark.read.parquet(batch.silver_uri)
        daily_df = spark.read.parquet(paths.daily_metrics_uri)
        location_df = spark.read.parquet(paths.location_metrics_uri)
        payment_df = spark.read.parquet(paths.payment_metrics_uri)

        silver_df.createOrReplaceTempView("silver_trips")
        daily_df.createOrReplaceTempView("daily_trip_metrics")
        location_df.createOrReplaceTempView("location_metrics")
        payment_df.createOrReplaceTempView("payment_metrics")

        min_max = silver_df.selectExpr(
            "CAST(MIN(pickup_date) AS STRING) AS start_date",
            "CAST(MAX(pickup_date) AS STRING) AS end_date",
        ).collect()[0]
        daily_dates = daily_df.selectExpr("CAST(MIN(pickup_date) AS STRING) AS pickup_date").collect()[0]
        values = {
            "year": batch.year,
            "month": batch.month,
            "start_date": min_max["start_date"],
            "end_date": min_max["end_date"],
            "pickup_date": daily_dates["pickup_date"] or min_max["start_date"],
        }

        query_metrics: list[dict[str, Any]] = []
        for query_path in load_sql_queries(sql_dir):
            query_start = time.perf_counter()
            sql = render_sql(query_path, values)
            result_df = spark.sql(sql)
            row_count = result_df.count()
            duration_seconds = round(time.perf_counter() - query_start, 3)
            query_metrics.append(
                {
                    "query_name": query_path.stem,
                    "query_file": str(query_path.relative_to(PROJECT_ROOT)),
                    "duration_seconds": duration_seconds,
                    "rows_returned": row_count,
                    "sample_rows": collect_sample(result_df, sample_limit),
                }
            )

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
            "query_count": len(query_metrics),
            "queries": query_metrics,
            "duration_seconds": round(time.perf_counter() - start, 3),
        }
        write_metrics(metrics_path(output_dir, JOB_NAME, batch.year, batch.month), metrics)
        return metrics
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        metrics = run_queries(
            Path(args.manifest_path),
            Path(args.sql_dir),
            Path(args.output_dir),
            args.sample_limit,
        )
        print(f"silver_uri: {metrics['silver_uri']}")
        print(f"query_count: {metrics['query_count']}")
        for query in metrics["queries"]:
            print(
                f"{query['query_name']}: rows={query['rows_returned']} "
                f"duration_seconds={query['duration_seconds']}"
            )
        print("nyc_taxi_query_smoke ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_query_smoke failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
