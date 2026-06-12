#!/usr/bin/env python3
"""Benchmark standard Spark SQL queries against NYC Taxi silver and gold data."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_settings
from spark.jobs.nyc_taxi_bronze_to_silver import configure_s3a, create_spark_session
from spark.jobs.nyc_taxi_common import batch_from_manifest, gold_paths, load_manifest
from spark.jobs.nyc_taxi_query_smoke import DEFAULT_SQL_DIR, load_sql_queries, render_sql


JOB_NAME = "nyc_taxi_spark_sql_benchmark"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark NYC Taxi Spark SQL queries.")
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--sql-dir", default=str(DEFAULT_SQL_DIR))
    parser.add_argument("--output-dir", default="benchmark/results")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", "local-baseline"))
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    return parser.parse_args()


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    if percentile_value < 0 or percentile_value > 100:
        raise ValueError("percentile must be between 0 and 100")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (percentile_value / 100)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize_results(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measured = [record for record in records if record["phase"] == "measured" and record["status"] == "success"]
    query_names = sorted({record["query_name"] for record in measured})
    summary: list[dict[str, Any]] = []
    for query_name in query_names:
        rows = [record for record in measured if record["query_name"] == query_name]
        durations = [float(record["duration_seconds"]) for record in rows]
        returned_rows = {int(record["rows_returned"]) for record in rows}
        summary.append(
            {
                "query_name": query_name,
                "runs": len(rows),
                "rows_returned": returned_rows.pop() if len(returned_rows) == 1 else "inconsistent",
                "min_seconds": round(min(durations), 3),
                "median_seconds": round(statistics.median(durations), 3),
                "p95_seconds": round(percentile(durations, 95), 3),
                "max_seconds": round(max(durations), 3),
            }
        )
    return summary


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def write_summary_csv(path: Path, summary: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "query_name",
        "runs",
        "rows_returned",
        "min_seconds",
        "median_seconds",
        "p95_seconds",
        "max_seconds",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)


def benchmark_queries(
    manifest_path: Path,
    sql_dir: Path,
    output_dir: Path,
    run_id: str,
    iterations: int,
    warmup: int,
) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be >= 1")
    if warmup < 0:
        raise ValueError("warmup must be >= 0")

    settings = load_settings()
    manifest = load_manifest(manifest_path)
    batch = batch_from_manifest(manifest, settings.silver_bucket)
    paths = gold_paths(settings.gold_bucket, batch.year, batch.month)
    queries = load_sql_queries(sql_dir)
    run_started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / run_id / "query" / "spark_sql" / run_started_at

    spark = create_spark_session(JOB_NAME)
    records: list[dict[str, Any]] = []
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

        scenario = {
            "run_id": run_id,
            "engine": "spark_sql",
            "job_name": JOB_NAME,
            "dataset": batch.dataset,
            "taxi_type": batch.taxi_type,
            "year": batch.year,
            "month": batch.month,
            "iterations": iterations,
            "warmup": warmup,
            "queries": [str(path.relative_to(PROJECT_ROOT)) for path in queries],
            "silver_uri": batch.silver_uri,
            "daily_metrics_uri": paths.daily_metrics_uri,
            "location_metrics_uri": paths.location_metrics_uri,
            "payment_metrics_uri": paths.payment_metrics_uri,
        }
        environment = {
            "python": sys.version,
            "platform": platform.platform(),
            "spark_version": spark.version,
            "s3_endpoint": settings.endpoint,
            "spark_master": spark.sparkContext.master,
        }

        write_json(run_dir / "scenario.json", scenario)
        write_json(run_dir / "environment.json", environment)

        phases = [("warmup", warmup), ("measured", iterations)]
        for phase, phase_iterations in phases:
            for iteration in range(1, phase_iterations + 1):
                for query_path in queries:
                    sql = render_sql(query_path, values)
                    started = time.perf_counter()
                    record = {
                        "run_id": run_id,
                        "phase": phase,
                        "iteration": iteration,
                        "query_name": query_path.stem,
                        "query_file": str(query_path.relative_to(PROJECT_ROOT)),
                    }
                    try:
                        row_count = spark.sql(sql).count()
                        record.update(
                            {
                                "status": "success",
                                "rows_returned": row_count,
                                "duration_seconds": round(time.perf_counter() - started, 3),
                            }
                        )
                    except Exception as exc:
                        record.update(
                            {
                                "status": "failed",
                                "rows_returned": None,
                                "duration_seconds": round(time.perf_counter() - started, 3),
                                "error": str(exc),
                            }
                        )
                        records.append(record)
                        raise
                    records.append(record)

        summary = summarize_results(records)
        append_jsonl(run_dir / "raw-results.jsonl", records)
        write_summary_csv(run_dir / "summary.csv", summary)
        write_json(run_dir / "summary.json", {"summary": summary})

        return {
            "run_dir": str(run_dir),
            "scenario": scenario,
            "summary": summary,
        }
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        result = benchmark_queries(
            Path(args.manifest_path),
            Path(args.sql_dir),
            Path(args.output_dir),
            args.run_id,
            args.iterations,
            args.warmup,
        )
        print(f"run_dir: {result['run_dir']}")
        for row in result["summary"]:
            print(
                f"{row['query_name']}: runs={row['runs']} rows={row['rows_returned']} "
                f"median_seconds={row['median_seconds']} p95_seconds={row['p95_seconds']}"
            )
        print("nyc_taxi_spark_sql_benchmark ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_spark_sql_benchmark failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
