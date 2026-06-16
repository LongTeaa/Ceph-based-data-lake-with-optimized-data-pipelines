#!/usr/bin/env python3
"""Generate deterministic synthetic tabular records for tests and scale checks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


MANIFEST_VERSION = "synthetic_tabular_manifest_v1"
SCHEMA_VERSION = "synthetic_trip_v1"
BASE_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class GeneratedFile:
    path: Path
    role: str
    format: str
    content_type: str
    size_bytes: int
    checksum_sha256: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic synthetic tabular data.")
    parser.add_argument("--rows", type=int, default=1000, help="Number of records to generate.")
    parser.add_argument("--days", type=int, default=7, help="Date spread in days.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed.")
    parser.add_argument("--output-dir", default="data/source/synthetic/tabular", help="Output directory.")
    parser.add_argument("--batch-id", default="", help="Batch id; defaults to rows/days/seed.")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def validate_request(rows: int, days: int) -> None:
    if rows < 1:
        raise ValueError("rows must be >= 1")
    if days < 1:
        raise ValueError("days must be >= 1")


def make_record(index: int, days: int, rng: random.Random) -> dict[str, Any]:
    pickup = BASE_DATE + timedelta(
        days=index % days,
        minutes=(index * 17) % (24 * 60),
        seconds=rng.randrange(0, 60),
    )
    duration_minutes = rng.randrange(4, 61)
    dropoff = pickup + timedelta(minutes=duration_minutes)
    distance = round(rng.uniform(0.4, 35.0), 2)
    fare = round(3.0 + distance * rng.uniform(2.1, 4.5), 2)
    tip = round(fare * rng.uniform(0, 0.3), 2)
    payment_type = rng.choice([1, 1, 1, 2, 3])
    total = round(fare + tip + rng.uniform(0.5, 5.0), 2)

    record = {
        "record_id": f"synthetic-{index:08d}",
        "pickup_datetime": pickup.isoformat(),
        "dropoff_datetime": dropoff.isoformat(),
        "passenger_count": rng.randrange(1, 7),
        "trip_distance": distance,
        "pickup_location_id": rng.randrange(1, 266),
        "dropoff_location_id": rng.randrange(1, 266),
        "payment_type": payment_type,
        "fare_amount": fare,
        "tip_amount": tip,
        "total_amount": total,
        "quality_case": "valid",
    }

    if index > 0 and index % 25 == 0:
        record["quality_case"] = "negative_amount"
        record["fare_amount"] = -abs(fare)
        record["total_amount"] = round(record["fare_amount"] + tip, 2)
    elif index > 0 and index % 40 == 0:
        record["quality_case"] = "bad_timestamp"
        record["dropoff_datetime"] = (pickup - timedelta(minutes=5)).isoformat()
    elif index > 0 and index % 55 == 0:
        record["quality_case"] = "null_passenger_count"
        record["passenger_count"] = ""
    elif index > 0 and index % 70 == 0:
        record["quality_case"] = "duplicate_candidate"
        record["record_id"] = f"synthetic-{index - 1:08d}"

    return record


def generate_records(rows: int, days: int, seed: int) -> list[dict[str, Any]]:
    validate_request(rows, days)
    rng = random.Random(seed)
    return [make_record(index, days, rng) for index in range(rows)]


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def describe_file(path: Path, role: str, file_format: str, content_type: str) -> GeneratedFile:
    return GeneratedFile(
        path=path,
        role=role,
        format=file_format,
        content_type=content_type,
        size_bytes=path.stat().st_size,
        checksum_sha256=sha256_file(path),
    )


def create_manifest(
    batch_id: str,
    rows: int,
    days: int,
    seed: int,
    files: list[GeneratedFile],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    quality_counts: dict[str, int] = {}
    for record in records:
        quality_case = str(record["quality_case"])
        quality_counts[quality_case] = quality_counts.get(quality_case, 0) + 1

    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": utc_now(),
        "dataset": "synthetic_tabular",
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "rows": rows,
        "days": days,
        "seed": seed,
        "quality_counts": quality_counts,
        "files": [
            {
                "role": item.role,
                "format": item.format,
                "content_type": item.content_type,
                "local_path": item.path.as_posix(),
                "file_name": item.path.name,
                "size_bytes": item.size_bytes,
                "checksum_algorithm": "sha256",
                "checksum_sha256": item.checksum_sha256,
            }
            for item in files
        ],
    }


def generate_dataset(rows: int, days: int, seed: int, output_dir: Path, batch_id: str = "") -> dict[str, Any]:
    batch = batch_id or f"synthetic-tabular-rows={rows}-days={days}-seed={seed}"
    batch_dir = output_dir / batch
    records = generate_records(rows, days, seed)

    jsonl_path = batch_dir / "records.jsonl"
    csv_path = batch_dir / "records.csv"
    write_jsonl(jsonl_path, records)
    write_csv(csv_path, records)

    files = [
        describe_file(jsonl_path, "records", "jsonl", "application/x-ndjson"),
        describe_file(csv_path, "records", "csv", "text/csv"),
    ]
    manifest = create_manifest(batch, rows, days, seed, files, records)
    manifest_path = batch_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    args = parse_args()
    try:
        manifest = generate_dataset(
            rows=args.rows,
            days=args.days,
            seed=args.seed,
            output_dir=Path(args.output_dir),
            batch_id=args.batch_id,
        )
        print(f"batch_id: {manifest['batch_id']}")
        print(f"rows: {manifest['rows']}")
        for item in manifest["files"]:
            print(f"{item['format']}: {item['local_path']} sha256={item['checksum_sha256']}")
        print("generate_test_records ok")
        return 0
    except Exception as exc:
        print(f"generate_test_records failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
