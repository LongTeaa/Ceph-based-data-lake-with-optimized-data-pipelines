#!/usr/bin/env python3
"""Benchmark Trino SQL queries against NYC Taxi gold data."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from benchmark.query.spark_sql_benchmark import percentile


DEFAULT_SQL_DIR = PROJECT_ROOT / "docker" / "trino" / "sql" / "benchmark"
DEFAULT_SETUP_SQL = PROJECT_ROOT / "docker" / "trino" / "sql" / "nyc_taxi_gold_setup.sql"
DEFAULT_COMPOSE_FILE = PROJECT_ROOT / "docker" / "compose.yml"
JOB_NAME = "nyc_taxi_trino_benchmark"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark NYC Taxi Trino SQL queries.")
    parser.add_argument("--compose-file", default=str(DEFAULT_COMPOSE_FILE))
    parser.add_argument("--service", default="trino")
    parser.add_argument("--server", default="localhost:8080")
    parser.add_argument("--catalog", default="lake")
    parser.add_argument("--schema", default="nyc_taxi")
    parser.add_argument("--setup-sql", default=str(DEFAULT_SETUP_SQL))
    parser.add_argument("--sql-dir", default=str(DEFAULT_SQL_DIR))
    parser.add_argument("--output-dir", default="benchmark/results")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", "local-baseline"))
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    return parser.parse_args()


def load_sql_queries(sql_dir: Path) -> list[Path]:
    queries = sorted(path for path in sql_dir.glob("*.sql") if path.is_file())
    if not queries:
        raise ValueError(f"No SQL files found in {sql_dir}")
    return queries


def read_sql(path: Path) -> str:
    sql = path.read_text(encoding="utf-8").strip()
    if sql.endswith(";"):
        sql = sql[:-1].strip()
    if not sql:
        raise ValueError(f"SQL file is empty: {path}")
    return sql


def container_sql_path(host_path: Path) -> str:
    sql_root = PROJECT_ROOT / "docker" / "trino" / "sql"
    relative = host_path.resolve().relative_to(sql_root.resolve())
    return f"/etc/trino/sql/{relative.as_posix()}"


def trino_command(
    compose_file: Path,
    service: str,
    server: str,
    catalog: str | None = None,
    schema: str | None = None,
    execute: str | None = None,
    file_path: str | None = None,
) -> list[str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        service,
        "trino",
        "--server",
        server,
    ]
    if catalog:
        command.extend(["--catalog", catalog])
    if schema:
        command.extend(["--schema", schema])
    if execute is not None:
        command.extend(["--output-format", "CSV", "--execute", execute])
    if file_path is not None:
        command.extend(["--file", file_path])
    return command


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=PROJECT_ROOT, text=True, capture_output=True, check=False)


def count_csv_rows(output: str) -> int:
    lines = [line for line in output.splitlines() if line.strip()]
    if not lines:
        return 0
    return sum(1 for _ in csv.reader(lines))


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


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
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
    compose_file: Path,
    service: str,
    server: str,
    catalog: str,
    schema: str,
    setup_sql: Path,
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

    queries = load_sql_queries(sql_dir)
    run_started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / run_id / "query" / "trino" / run_started_at

    setup = run_command(
        trino_command(
            compose_file=compose_file,
            service=service,
            server=server,
            file_path=container_sql_path(setup_sql),
        )
    )
    if setup.returncode != 0:
        raise RuntimeError(setup.stderr.strip() or setup.stdout.strip() or "Trino setup failed")

    scenario = {
        "run_id": run_id,
        "engine": "trino",
        "job_name": JOB_NAME,
        "catalog": catalog,
        "schema": schema,
        "iterations": iterations,
        "warmup": warmup,
        "queries": [str(path.relative_to(PROJECT_ROOT)) for path in queries],
        "setup_sql": str(setup_sql.relative_to(PROJECT_ROOT)),
    }
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "compose_file": str(compose_file.relative_to(PROJECT_ROOT)),
        "service": service,
        "server": server,
    }

    write_json(run_dir / "scenario.json", scenario)
    write_json(run_dir / "environment.json", environment)

    records: list[dict[str, Any]] = []
    phases = [("warmup", warmup), ("measured", iterations)]
    for phase, phase_iterations in phases:
        for iteration in range(1, phase_iterations + 1):
            for query_path in queries:
                sql = read_sql(query_path)
                started = time.perf_counter()
                record = {
                    "run_id": run_id,
                    "phase": phase,
                    "iteration": iteration,
                    "query_name": query_path.stem,
                    "query_file": str(query_path.relative_to(PROJECT_ROOT)),
                }
                result = run_command(
                    trino_command(
                        compose_file=compose_file,
                        service=service,
                        server=server,
                        catalog=catalog,
                        schema=schema,
                        execute=sql,
                    )
                )
                duration = round(time.perf_counter() - started, 3)
                if result.returncode == 0:
                    record.update(
                        {
                            "status": "success",
                            "rows_returned": count_csv_rows(result.stdout),
                            "duration_seconds": duration,
                        }
                    )
                else:
                    record.update(
                        {
                            "status": "failed",
                            "rows_returned": None,
                            "duration_seconds": duration,
                            "error": result.stderr.strip() or result.stdout.strip(),
                        }
                    )
                    records.append(record)
                    raise RuntimeError(record["error"])
                records.append(record)

    summary = summarize_results(records)
    write_jsonl(run_dir / "raw-results.jsonl", records)
    write_summary_csv(run_dir / "summary.csv", summary)
    write_json(run_dir / "summary.json", {"summary": summary})

    return {
        "run_dir": str(run_dir),
        "scenario": scenario,
        "summary": summary,
    }


def main() -> int:
    args = parse_args()
    try:
        result = benchmark_queries(
            compose_file=Path(args.compose_file),
            service=args.service,
            server=args.server,
            catalog=args.catalog,
            schema=args.schema,
            setup_sql=Path(args.setup_sql),
            sql_dir=Path(args.sql_dir),
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
            iterations=args.iterations,
            warmup=args.warmup,
        )
        print(f"run_dir: {result['run_dir']}")
        for row in result["summary"]:
            print(
                f"{row['query_name']}: runs={row['runs']} rows={row['rows_returned']} "
                f"median_seconds={row['median_seconds']} p95_seconds={row['p95_seconds']}"
            )
        print("nyc_taxi_trino_benchmark ok")
        return 0
    except Exception as exc:
        print(f"nyc_taxi_trino_benchmark failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
