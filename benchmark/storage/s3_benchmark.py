#!/usr/bin/env python3
"""Benchmark S3-compatible object storage PUT/GET workloads."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import create_s3_client, load_settings


JOB_NAME = "s3_storage_benchmark"
SIZE_UNITS = {
    "b": 1,
    "kib": 1024,
    "kb": 1000,
    "mib": 1024 * 1024,
    "mb": 1000 * 1000,
    "gib": 1024 * 1024 * 1024,
    "gb": 1000 * 1000 * 1000,
}


@dataclass(frozen=True)
class Scenario:
    operation: str
    object_size_bytes: int
    concurrency: int

    @property
    def name(self) -> str:
        return f"{self.operation}_size={self.object_size_bytes}_concurrency={self.concurrency}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark S3-compatible object storage.")
    parser.add_argument("--bucket", default="", help="Bucket to use; defaults to SYSTEM_BUCKET.")
    parser.add_argument("--key-prefix", default="benchmark/storage", help="Object key prefix.")
    parser.add_argument("--object-sizes", default="4KiB,1MiB", help="Comma-separated sizes, for example 4KiB,1MiB,64MiB.")
    parser.add_argument("--concurrency", default="1,4", help="Comma-separated concurrency values.")
    parser.add_argument("--operations", default="put,get,mixed", help="Comma-separated operations: put,get,mixed.")
    parser.add_argument("--iterations", type=int, default=10, help="Measured operations per scenario.")
    parser.add_argument("--warmup", type=int, default=2, help="Warm-up operations per scenario.")
    parser.add_argument("--mixed-read-ratio", type=float, default=0.30, help="Read ratio for mixed workload.")
    parser.add_argument("--output-dir", default="benchmark/results")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", "local-baseline"))
    parser.add_argument("--backend", default=os.getenv("STORAGE_BACKEND", "local-s3"))
    parser.add_argument("--keep-objects", action="store_true", help="Keep generated benchmark objects.")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def parse_size(value: str) -> int:
    text = value.strip().lower()
    if not text:
        raise ValueError("size value must not be empty")
    digits = ""
    suffix = ""
    for char in text:
        if char.isdigit():
            if suffix:
                raise ValueError(f"invalid size: {value}")
            digits += char
        else:
            suffix += char
    if not digits:
        raise ValueError(f"invalid size: {value}")
    unit = suffix or "b"
    if unit not in SIZE_UNITS:
        raise ValueError(f"unsupported size unit in {value}")
    size = int(digits) * SIZE_UNITS[unit]
    if size <= 0:
        raise ValueError("size must be positive")
    return size


def parse_csv_ints(value: str, name: str) -> list[int]:
    values = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise ValueError(f"{name} must not be empty")
    if any(item <= 0 for item in values):
        raise ValueError(f"{name} values must be positive")
    return values


def parse_operations(value: str) -> list[str]:
    operations = [item.strip().lower() for item in value.split(",") if item.strip()]
    valid = {"put", "get", "mixed"}
    invalid = [operation for operation in operations if operation not in valid]
    if invalid:
        raise ValueError("unsupported operations: " + ", ".join(invalid))
    if not operations:
        raise ValueError("operations must not be empty")
    return operations


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (percentile_value / 100)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def deterministic_payload(size_bytes: int, seed: int) -> bytes:
    rng = random.Random(seed)
    chunk = bytearray(1024 * 1024)
    output = bytearray()
    while len(output) < size_bytes:
        for index in range(len(chunk)):
            chunk[index] = rng.randrange(0, 256)
        output.extend(chunk[: min(len(chunk), size_bytes - len(output))])
    return bytes(output)


def sha256(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


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
        "operation",
        "object_size_bytes",
        "concurrency",
        "runs",
        "errors",
        "total_seconds",
        "throughput_mib_s",
        "ops_per_second",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)


def make_scenarios(object_sizes: list[int], concurrency_values: list[int], operations: list[str]) -> list[Scenario]:
    return [
        Scenario(operation=operation, object_size_bytes=size, concurrency=concurrency)
        for operation in operations
        for size in object_sizes
        for concurrency in concurrency_values
    ]


def put_object(client, bucket: str, key: str, payload: bytes, payload_sha: str) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        Metadata={"sha256": payload_sha},
        ContentType="application/octet-stream",
    )


def get_object(client, bucket: str, key: str, expected_sha: str) -> int:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    actual_sha = sha256(body)
    if actual_sha != expected_sha:
        raise RuntimeError(f"checksum mismatch for {key}")
    return len(body)


def delete_objects(client, bucket: str, keys: list[str]) -> None:
    for offset in range(0, len(keys), 1000):
        batch = keys[offset : offset + 1000]
        if not batch:
            continue
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True})


def prepare_read_objects(
    client,
    bucket: str,
    key_prefix: str,
    scenario: Scenario,
    payload: bytes,
    payload_sha: str,
) -> list[str]:
    count = max(scenario.concurrency, 1)
    keys = [f"{key_prefix}/{scenario.name}/read-seed-{index}.bin" for index in range(count)]
    for key in keys:
        put_object(client, bucket, key, payload, payload_sha)
    return keys


def run_one_operation(
    client,
    bucket: str,
    key: str,
    operation: str,
    payload: bytes,
    payload_sha: str,
    read_keys: list[str],
    read_index: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    logical_bytes = len(payload)
    try:
        if operation == "put":
            put_object(client, bucket, key, payload, payload_sha)
        elif operation == "get":
            logical_bytes = get_object(client, bucket, read_keys[read_index % len(read_keys)], payload_sha)
        else:
            raise ValueError(f"unsupported concrete operation: {operation}")
        return {
            "status": "success",
            "logical_bytes": logical_bytes,
            "latency_seconds": time.perf_counter() - started,
            "error": "",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "logical_bytes": 0,
            "latency_seconds": time.perf_counter() - started,
            "error": str(exc),
        }


def concrete_operation(scenario_operation: str, mixed_read_ratio: float, rng: random.Random) -> str:
    if scenario_operation in {"put", "get"}:
        return scenario_operation
    return "get" if rng.random() < mixed_read_ratio else "put"


def run_phase(
    client,
    bucket: str,
    key_prefix: str,
    scenario: Scenario,
    payload: bytes,
    payload_sha: str,
    read_keys: list[str],
    phase: str,
    operation_count: int,
    mixed_read_ratio: float,
    seed: int,
) -> list[dict[str, Any]]:
    if operation_count <= 0:
        return []
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []
    submitted = []
    phase_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=scenario.concurrency) as executor:
        for index in range(operation_count):
            operation = concrete_operation(scenario.operation, mixed_read_ratio, rng)
            key = f"{key_prefix}/{scenario.name}/{phase}-{index}-{time.time_ns()}.bin"
            submitted.append(
                (
                    index,
                    operation,
                    key,
                    executor.submit(
                        run_one_operation,
                        client,
                        bucket,
                        key,
                        operation,
                        payload,
                        payload_sha,
                        read_keys,
                        index,
                    ),
                )
            )
        future_to_context = {future: (index, operation, key) for index, operation, key, future in submitted}
        for future in as_completed(future_to_context):
            index, operation, key = future_to_context[future]
            result = future.result()
            records.append(
                {
                    "phase": phase,
                    "scenario": scenario.name,
                    "scenario_operation": scenario.operation,
                    "operation": operation,
                    "iteration": index + 1,
                    "object_size_bytes": scenario.object_size_bytes,
                    "concurrency": scenario.concurrency,
                    "key": key,
                    "phase_elapsed_seconds": round(time.perf_counter() - phase_started, 6),
                    "status": result["status"],
                    "logical_bytes": result["logical_bytes"],
                    "latency_seconds": round(result["latency_seconds"], 6),
                    "error": result["error"],
                }
            )
    return sorted(records, key=lambda record: record["iteration"])


def summarize_results(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    measured = [record for record in records if record["phase"] == "measured"]
    scenario_keys = sorted(
        {
            (record["scenario_operation"], int(record["object_size_bytes"]), int(record["concurrency"]))
            for record in measured
        }
    )
    summary: list[dict[str, Any]] = []
    for operation, size, concurrency in scenario_keys:
        rows = [
            record
            for record in measured
            if record["scenario_operation"] == operation
            and int(record["object_size_bytes"]) == size
            and int(record["concurrency"]) == concurrency
        ]
        successes = [record for record in rows if record["status"] == "success"]
        latencies = [float(record["latency_seconds"]) for record in successes]
        total_seconds = max((float(record["phase_elapsed_seconds"]) for record in rows), default=0.0)
        logical_bytes = sum(int(record["logical_bytes"]) for record in successes)
        summary.append(
            {
                "operation": operation,
                "object_size_bytes": size,
                "concurrency": concurrency,
                "runs": len(rows),
                "errors": len(rows) - len(successes),
                "total_seconds": round(total_seconds, 3),
                "throughput_mib_s": round((logical_bytes / (1024 * 1024)) / total_seconds, 3) if total_seconds else 0,
                "ops_per_second": round(len(successes) / total_seconds, 3) if total_seconds else 0,
                "latency_p50_ms": round(percentile(latencies, 50) * 1000, 3) if latencies else 0,
                "latency_p95_ms": round(percentile(latencies, 95) * 1000, 3) if latencies else 0,
                "latency_p99_ms": round(percentile(latencies, 99) * 1000, 3) if latencies else 0,
            }
        )
    return summary


def benchmark_storage(
    output_dir: Path,
    run_id: str,
    backend: str,
    bucket: str,
    key_prefix: str,
    object_sizes: list[int],
    concurrency_values: list[int],
    operations: list[str],
    iterations: int,
    warmup: int,
    mixed_read_ratio: float,
    keep_objects: bool,
    seed: int,
) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be >= 1")
    if warmup < 0:
        raise ValueError("warmup must be >= 0")
    if mixed_read_ratio < 0 or mixed_read_ratio > 1:
        raise ValueError("mixed read ratio must be between 0 and 1")

    settings = load_settings()
    client = create_s3_client(settings)
    benchmark_bucket = bucket or settings.system_bucket
    started_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_dir / run_id / "storage" / "s3" / started_at
    run_prefix = f"{key_prefix.rstrip('/')}/{run_id}/{started_at}"
    scenarios = make_scenarios(object_sizes, concurrency_values, operations)

    scenario_doc = {
        "run_id": run_id,
        "backend": backend,
        "engine": "boto3",
        "job_name": JOB_NAME,
        "bucket": benchmark_bucket,
        "key_prefix": run_prefix,
        "object_sizes": object_sizes,
        "concurrency": concurrency_values,
        "operations": operations,
        "iterations": iterations,
        "warmup": warmup,
        "mixed_read_ratio": mixed_read_ratio,
        "keep_objects": keep_objects,
        "seed": seed,
    }
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "s3_endpoint": settings.endpoint,
        "s3_region": settings.region,
        "path_style_access": settings.path_style_access,
        "use_ssl": settings.use_ssl,
    }
    write_json(run_dir / "scenario.json", scenario_doc)
    write_json(run_dir / "environment.json", environment)

    all_records: list[dict[str, Any]] = []
    cleanup_keys: list[str] = []
    try:
        for scenario_index, scenario in enumerate(scenarios):
            payload = deterministic_payload(scenario.object_size_bytes, seed + scenario_index)
            payload_sha = sha256(payload)
            scenario_prefix = f"{run_prefix}/{scenario.name}"
            read_keys: list[str] = []
            if scenario.operation in {"get", "mixed"}:
                read_keys = prepare_read_objects(client, benchmark_bucket, scenario_prefix, scenario, payload, payload_sha)
                cleanup_keys.extend(read_keys)

            for phase, count in [("warmup", warmup), ("measured", iterations)]:
                records = run_phase(
                    client,
                    benchmark_bucket,
                    scenario_prefix,
                    scenario,
                    payload,
                    payload_sha,
                    read_keys,
                    phase,
                    count,
                    mixed_read_ratio,
                    seed + scenario_index + (0 if phase == "warmup" else 10000),
                )
                for record in records:
                    record.update(
                        {
                            "run_id": run_id,
                            "backend": backend,
                            "bucket": benchmark_bucket,
                            "payload_sha256": payload_sha,
                        }
                    )
                all_records.extend(records)
                cleanup_keys.extend([record["key"] for record in records if record["operation"] == "put"])

        summary = summarize_results(all_records)
        write_jsonl(run_dir / "raw-results.jsonl", all_records)
        write_summary_csv(run_dir / "summary.csv", summary)
        write_json(run_dir / "summary.json", {"summary": summary})
        write_json(run_dir / "notes.json", {"cleanup_objects": not keep_objects, "cleanup_key_count": len(cleanup_keys)})
        return {"run_dir": str(run_dir), "summary": summary}
    finally:
        if not keep_objects and cleanup_keys:
            delete_objects(client, benchmark_bucket, cleanup_keys)


def main() -> int:
    args = parse_args()
    try:
        result = benchmark_storage(
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
            backend=args.backend,
            bucket=args.bucket,
            key_prefix=args.key_prefix,
            object_sizes=[parse_size(item) for item in args.object_sizes.split(",") if item.strip()],
            concurrency_values=parse_csv_ints(args.concurrency, "concurrency"),
            operations=parse_operations(args.operations),
            iterations=args.iterations,
            warmup=args.warmup,
            mixed_read_ratio=args.mixed_read_ratio,
            keep_objects=args.keep_objects,
            seed=args.seed,
        )
        print(f"run_dir: {result['run_dir']}")
        for row in result["summary"]:
            print(
                f"{row['operation']} size={row['object_size_bytes']} concurrency={row['concurrency']} "
                f"runs={row['runs']} errors={row['errors']} throughput_mib_s={row['throughput_mib_s']} "
                f"p95_ms={row['latency_p95_ms']}"
            )
        print("s3_storage_benchmark ok")
        return 0
    except Exception as exc:
        print(f"s3_storage_benchmark failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
