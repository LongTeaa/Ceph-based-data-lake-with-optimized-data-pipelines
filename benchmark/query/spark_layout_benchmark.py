#!/usr/bin/env python3
"""Compare Spark SQL query performance across NYC Taxi silver data layouts."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_settings
from spark.jobs.nyc_taxi_bronze_to_silver import configure_s3a, create_spark_session
from spark.jobs.nyc_taxi_common import batch_from_manifest, load_manifest, s3_uri
from spark.jobs.nyc_taxi_query_smoke import render_sql


JOB_NAME = "nyc_taxi_spark_layout_benchmark"
DEFAULT_SQL_FILES = (
    PROJECT_ROOT / "spark" / "sql" / "04_hourly_distance_fare.sql",
    PROJECT_ROOT / "spark" / "sql" / "05_selective_pickup_date.sql",
    PROJECT_ROOT / "spark" / "sql" / "06_full_scan_location_aggregation.sql",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark NYC Taxi silver data layouts.")
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--output-dir", default="benchmark/results")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", "local-baseline"))
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument(
        "--comparison",
        choices=("partition", "compaction", "format"),
        default="partition",
        help="Comparison to run: partitioned vs non-partitioned, small-files vs compacted, or CSV vs Parquet.",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=0,
        help="Coalesce generated layout before write. For compaction, values > 0 define compacted file count.",
    )
    parser.add_argument(
        "--sql-files",
        default=",".join(str(path) for path in DEFAULT_SQL_FILES),
        help="Comma-separated SQL files that only depend on the silver_trips view.",
    )
    parser.add_argument("--keep-layout", action="store_true", help="Keep generated benchmark layout objects.")
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


def parse_sql_files(value: str) -> list[Path]:
    paths = [Path(item.strip()) for item in value.split(",") if item.strip()]
    if not paths:
        raise ValueError("sql files must not be empty")
    for path in paths:
        if not path.exists():
            raise ValueError(f"SQL file does not exist: {path}")
    return paths


def normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, Decimal):
        return round(float(value), 6)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in sorted(row.items())}


def result_fingerprint(df) -> tuple[int, str]:
    rows = [
        json.dumps(normalize_row(row.asDict(recursive=True)), sort_keys=True, separators=(",", ":"))
        for row in df.collect()
    ]
    rows = sorted(rows)
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return len(rows), digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def write_summary_csv(path: Path, summary: list[dict[str, Any]]) -> None:
    fieldnames = [
        "layout",
        "query_name",
        "runs",
        "rows_returned",
        "result_consistent",
        "min_seconds",
        "median_seconds",
        "p95_seconds",
        "max_seconds",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)


def summarize_results(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measured = [record for record in records if record["phase"] == "measured" and record["status"] == "success"]
    keys = sorted({(record["layout"], record["query_name"]) for record in measured})
    baseline_hashes: dict[str, set[str]] = {}
    for record in measured:
        baseline_hashes.setdefault(record["query_name"], set()).add(record["result_sha256"])

    summary: list[dict[str, Any]] = []
    for layout, query_name in keys:
        rows = [record for record in measured if record["layout"] == layout and record["query_name"] == query_name]
        durations = [float(record["duration_seconds"]) for record in rows]
        returned_rows = {int(record["rows_returned"]) for record in rows}
        summary.append(
            {
                "layout": layout,
                "query_name": query_name,
                "runs": len(rows),
                "rows_returned": returned_rows.pop() if len(returned_rows) == 1 else "inconsistent",
                "result_consistent": len(baseline_hashes.get(query_name, set())) == 1,
                "min_seconds": round(min(durations), 3),
                "median_seconds": round(statistics.median(durations), 3),
                "p95_seconds": round(percentile(durations, 95), 3),
                "max_seconds": round(max(durations), 3),
            }
        )
    return summary


def cleanup_s3_prefix(spark, uri: str) -> None:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(hadoop_conf)
    if filesystem.exists(path):
        filesystem.delete(path, True)


def read_layout(spark, uri: str, data_format: str, schema):
    if data_format == "parquet":
        return spark.read.parquet(uri)
    if data_format == "csv":
        return spark.read.option("header", "true").schema(schema).csv(uri)
    raise ValueError(f"unsupported layout format: {data_format}")


def build_layout_specs(
    spark,
    source_df,
    system_bucket: str,
    run_id: str,
    run_started_at: str,
    year: str,
    month: str,
    partitioned_uri: str,
    comparison: str,
    coalesce: int,
) -> tuple[list[tuple[str, str, str]], list[str], dict[str, str]]:
    base_prefix = f"benchmark-layouts/{run_id}/{run_started_at}"
    if comparison == "partition":
        non_partitioned_uri = s3_uri(
            system_bucket,
            f"{base_prefix}/silver_non_partitioned/year={year}/month={month}",
        )
        cleanup_s3_prefix(spark, non_partitioned_uri)
        output_df = source_df if coalesce == 0 else source_df.coalesce(coalesce)
        output_df.write.mode("overwrite").parquet(non_partitioned_uri)
        return (
            [
                ("partitioned", partitioned_uri, "parquet"),
                ("non_partitioned", non_partitioned_uri, "parquet"),
            ],
            [non_partitioned_uri],
            {"partitioned_uri": partitioned_uri, "non_partitioned_uri": non_partitioned_uri},
        )

    if comparison == "format":
        parquet_uri = s3_uri(
            system_bucket,
            f"{base_prefix}/silver_format_parquet/year={year}/month={month}",
        )
        csv_uri = s3_uri(
            system_bucket,
            f"{base_prefix}/silver_format_csv/year={year}/month={month}",
        )
        cleanup_s3_prefix(spark, parquet_uri)
        cleanup_s3_prefix(spark, csv_uri)
        output_df = source_df if coalesce == 0 else source_df.coalesce(coalesce)
        output_df.write.mode("overwrite").parquet(parquet_uri)
        output_df.write.mode("overwrite").option("header", "true").csv(csv_uri)
        return (
            [
                ("parquet", parquet_uri, "parquet"),
                ("csv", csv_uri, "csv"),
            ],
            [parquet_uri, csv_uri],
            {"parquet_uri": parquet_uri, "csv_uri": csv_uri},
        )

    if coalesce <= 0:
        raise ValueError("coalesce must be > 0 for compaction comparison")

    small_files_uri = s3_uri(
        system_bucket,
        f"{base_prefix}/silver_small_files/year={year}/month={month}",
    )
    compacted_uri = s3_uri(
        system_bucket,
        f"{base_prefix}/silver_compacted/year={year}/month={month}",
    )
    cleanup_s3_prefix(spark, small_files_uri)
    cleanup_s3_prefix(spark, compacted_uri)
    source_df.repartition("pickup_date").write.mode("overwrite").parquet(small_files_uri)
    source_df.coalesce(coalesce).write.mode("overwrite").parquet(compacted_uri)
    return (
        [
            ("small_files", small_files_uri, "parquet"),
            ("compacted", compacted_uri, "parquet"),
        ],
        [small_files_uri, compacted_uri],
        {"small_files_uri": small_files_uri, "compacted_uri": compacted_uri},
    )


def benchmark_layouts(
    manifest_path: Path,
    sql_files: list[Path],
    output_dir: Path,
    run_id: str,
    iterations: int,
    warmup: int,
    comparison: str,
    coalesce: int,
    keep_layout: bool,
) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be >= 1")
    if warmup < 0:
        raise ValueError("warmup must be >= 0")
    if coalesce < 0:
        raise ValueError("coalesce must be >= 0")
    if comparison == "compaction" and coalesce <= 0:
        raise ValueError("coalesce must be > 0 for compaction comparison")

    settings = load_settings()
    manifest = load_manifest(manifest_path)
    batch = batch_from_manifest(manifest, settings.silver_bucket)
    run_started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / run_id / "query" / "spark_layout" / run_started_at

    spark = create_spark_session(JOB_NAME)
    records: list[dict[str, Any]] = []
    generated_uris: list[str] = []
    try:
        configure_s3a(spark, settings)
        partitioned_df = spark.read.parquet(batch.silver_uri)
        layouts, generated_uris, generated_uri_doc = build_layout_specs(
            spark=spark,
            source_df=partitioned_df,
            system_bucket=settings.system_bucket,
            run_id=run_id,
            run_started_at=run_started_at,
            year=batch.year,
            month=batch.month,
            partitioned_uri=batch.silver_uri,
            comparison=comparison,
            coalesce=coalesce,
        )

        values_row = partitioned_df.selectExpr(
            "CAST(MIN(pickup_date) AS STRING) AS start_date",
            "CAST(MAX(pickup_date) AS STRING) AS end_date",
        ).collect()[0]
        values = {
            "year": batch.year,
            "month": batch.month,
            "start_date": values_row["start_date"],
            "end_date": values_row["end_date"],
            "pickup_date": values_row["start_date"],
        }
        scenario = {
            "run_id": run_id,
            "engine": "spark_sql",
            "job_name": JOB_NAME,
            "comparison": comparison,
            "dataset": batch.dataset,
            "taxi_type": batch.taxi_type,
            "year": batch.year,
            "month": batch.month,
            "iterations": iterations,
            "warmup": warmup,
            "coalesce": coalesce,
            "queries": [str(path.relative_to(PROJECT_ROOT)) for path in sql_files],
            "keep_layout": keep_layout,
        }
        scenario.update(generated_uri_doc)
        environment = {
            "python": sys.version,
            "platform": platform.platform(),
            "spark_version": spark.version,
            "s3_endpoint": settings.endpoint,
            "spark_master": spark.sparkContext.master,
        }
        write_json(run_dir / "scenario.json", scenario)
        write_json(run_dir / "environment.json", environment)

        silver_schema = partitioned_df.schema
        for layout_name, layout_uri, layout_format in layouts:
            silver_df = read_layout(spark, layout_uri, layout_format, silver_schema)
            silver_df.createOrReplaceTempView("silver_trips")
            for phase, phase_iterations in [("warmup", warmup), ("measured", iterations)]:
                for iteration in range(1, phase_iterations + 1):
                    for query_path in sql_files:
                        started = time.perf_counter()
                        record = {
                            "run_id": run_id,
                            "phase": phase,
                            "layout": layout_name,
                            "layout_uri": layout_uri,
                            "layout_format": layout_format,
                            "iteration": iteration,
                            "query_name": query_path.stem,
                            "query_file": str(query_path.relative_to(PROJECT_ROOT)),
                        }
                        try:
                            result_df = spark.sql(render_sql(query_path, values))
                            row_count, fingerprint = result_fingerprint(result_df)
                            record.update(
                                {
                                    "status": "success",
                                    "rows_returned": row_count,
                                    "result_sha256": fingerprint,
                                    "duration_seconds": round(time.perf_counter() - started, 3),
                                }
                            )
                        except Exception as exc:
                            record.update(
                                {
                                    "status": "failed",
                                    "rows_returned": None,
                                    "result_sha256": "",
                                    "duration_seconds": round(time.perf_counter() - started, 3),
                                    "error": str(exc),
                                }
                            )
                            records.append(record)
                            raise
                        records.append(record)

        summary = summarize_results(records)
        write_jsonl(run_dir / "raw-results.jsonl", records)
        write_summary_csv(run_dir / "summary.csv", summary)
        write_json(run_dir / "summary.json", {"summary": summary})
        write_json(
            run_dir / "notes.json",
            {
                "cleanup_generated_layouts": not keep_layout,
                "generated_layout_count": len(generated_uris),
            },
        )
        return {"run_dir": str(run_dir), "summary": summary}
    finally:
        if not keep_layout:
            for uri in generated_uris:
                cleanup_s3_prefix(spark, uri)
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        result = benchmark_layouts(
            manifest_path=Path(args.manifest_path),
            sql_files=parse_sql_files(args.sql_files),
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
            iterations=args.iterations,
            warmup=args.warmup,
            comparison=args.comparison,
            coalesce=args.coalesce,
            keep_layout=args.keep_layout,
        )
        print(f"run_dir: {result['run_dir']}")
        for row in result["summary"]:
            print(
                f"{row['layout']} {row['query_name']}: runs={row['runs']} rows={row['rows_returned']} "
                f"consistent={row['result_consistent']} median_seconds={row['median_seconds']} "
                f"p95_seconds={row['p95_seconds']}"
            )
        print("nyc_taxi_spark_layout_benchmark ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_spark_layout_benchmark failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
