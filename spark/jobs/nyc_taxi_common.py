"""Shared helpers for NYC Taxi Spark jobs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REQUIRED_NYC_TAXI_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
)


@dataclass(frozen=True)
class NYCTaxiBatch:
    dataset: str
    taxi_type: str
    year: str
    month: str
    bronze_bucket: str
    silver_bucket: str
    source_key: str
    source_uri: str
    source_uris: tuple[str, ...]
    silver_prefix: str
    silver_uri: str


@dataclass(frozen=True)
class NYCTaxiGoldPaths:
    daily_metrics_uri: str
    location_metrics_uri: str
    payment_metrics_uri: str


def s3_uri(bucket: str, key: str) -> str:
    normalized_key = key.strip("/")
    if not bucket or not normalized_key:
        raise ValueError("bucket and key are required")
    return f"s3a://{bucket}/{normalized_key}"


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def batch_from_manifest(manifest: dict[str, Any], silver_bucket: str) -> NYCTaxiBatch:
    if manifest.get("dataset") != "nyc_taxi":
        raise ValueError(f"Unsupported dataset: {manifest.get('dataset')}")
    files = manifest.get("files") or []
    if not files:
        raise ValueError("NYC Taxi manifest must describe at least one source file")

    source = files[0]
    year = str(manifest["year"])
    month = str(manifest["month"])
    silver_prefix = f"nyc-taxi/year={year}/month={month}"
    source_key = source["bronze_key"]
    source_uris = tuple(s3_uri(manifest["bronze_bucket"], item["bronze_key"]) for item in files)

    return NYCTaxiBatch(
        dataset=manifest["dataset"],
        taxi_type=manifest["taxi_type"],
        year=year,
        month=month,
        bronze_bucket=manifest["bronze_bucket"],
        silver_bucket=silver_bucket,
        source_key=source_key,
        source_uri=source_uris[0],
        source_uris=source_uris,
        silver_prefix=silver_prefix,
        silver_uri=s3_uri(silver_bucket, silver_prefix),
    )


def missing_required_columns(columns: Iterable[str]) -> list[str]:
    present = set(columns)
    return [column for column in REQUIRED_NYC_TAXI_COLUMNS if column not in present]


def metrics_path(output_dir: Path, job_name: str, year: str, month: str) -> Path:
    return output_dir / job_name / f"year={year}" / f"month={month}" / "metrics.json"


def gold_paths(gold_bucket: str, year: str, month: str) -> NYCTaxiGoldPaths:
    partition_suffix = f"year={year}/month={month}"
    return NYCTaxiGoldPaths(
        daily_metrics_uri=s3_uri(gold_bucket, f"daily_trip_metrics/{partition_suffix}"),
        location_metrics_uri=s3_uri(gold_bucket, f"location_metrics/{partition_suffix}"),
        payment_metrics_uri=s3_uri(gold_bucket, f"payment_metrics/{partition_suffix}"),
    )
