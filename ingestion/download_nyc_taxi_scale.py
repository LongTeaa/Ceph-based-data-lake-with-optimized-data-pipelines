#!/usr/bin/env python3
"""Download multiple NYC Taxi Parquet files and create a scale manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_settings


MANIFEST_VERSION = "nyc_taxi_manifest_v1"
DEFAULT_SOURCE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


@dataclass(frozen=True)
class DownloadedTaxiFile:
    year: str
    month: str
    taxi_type: str
    file_name: str
    url: str
    local_path: Path
    size_bytes: int
    checksum_sha256: str
    row_count: int

    @property
    def bronze_prefix(self) -> str:
        return f"nyc-taxi/year={self.year}/month={self.month}"

    @property
    def bronze_key(self) -> str:
        return f"{self.bronze_prefix}/{self.file_name}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a multi-month NYC Taxi dataset and create a manifest."
    )
    parser.add_argument(
        "--months",
        default="",
        help=(
            "Comma-separated YYYY-MM months. Defaults to 30 months from "
            "2023-01 through 2025-06."
        ),
    )
    parser.add_argument(
        "--taxi-type",
        choices=("yellow", "green", "fhv", "fhvhv"),
        default="yellow",
        help="NYC TLC taxi dataset type.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/source/nyc-taxi/scale",
        help="Directory for downloaded Parquet files.",
    )
    parser.add_argument(
        "--manifest-path",
        default="data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2025-06_30files.manifest.json",
        help="Output manifest path.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Optional limit for smoke testing the first N files.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download files that already exist locally.",
    )
    return parser.parse_args()


def default_months() -> list[str]:
    months: list[str] = []
    year = 2023
    month = 1
    while len(months) < 30:
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def parse_months(raw: str) -> list[str]:
    months = [item.strip() for item in raw.split(",") if item.strip()] if raw else default_months()
    invalid = [month for month in months if not MONTH_RE.match(month)]
    if invalid:
        raise ValueError("Invalid months: " + ", ".join(invalid))
    return months


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def parquet_row_count(path: Path) -> int:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required to read Parquet row counts. "
            "Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    return int(pq.ParquetFile(path).metadata.num_rows)


def file_name_for(taxi_type: str, month: str) -> str:
    return f"{taxi_type}_tripdata_{month}.parquet"


def url_for(taxi_type: str, month: str) -> str:
    return f"{TLC_BASE_URL}/{file_name_for(taxi_type, month)}"


def download_file(url: str, destination: Path, force: bool) -> None:
    if destination.exists() and destination.stat().st_size > 0 and not force:
        print(f"exists: {destination}")
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=str(destination.parent)) as tmp:
        tmp_path = Path(tmp.name)

    try:
        print(f"downloading: {url}")
        with urllib.request.urlopen(url) as response, tmp_path.open("wb") as output:
            while True:
                chunk = response.read(8 * 1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
        tmp_path.replace(destination)
        print(f"downloaded: {destination}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def inspect_downloaded_file(taxi_type: str, month: str, path: Path) -> DownloadedTaxiFile:
    year, month_part = month.split("-", maxsplit=1)
    return DownloadedTaxiFile(
        year=year,
        month=month_part,
        taxi_type=taxi_type,
        file_name=path.name,
        url=url_for(taxi_type, month),
        local_path=path,
        size_bytes=path.stat().st_size,
        checksum_sha256=sha256_file(path),
        row_count=parquet_row_count(path),
    )


def create_manifest(files: list[DownloadedTaxiFile], bronze_bucket: str) -> dict[str, Any]:
    if not files:
        raise ValueError("No files to include in manifest")

    months = [f"{item.year}-{item.month}" for item in files]
    total_size_bytes = sum(item.size_bytes for item in files)
    total_rows = sum(item.row_count for item in files)
    batch_id = f"{months[0]}_{months[-1]}_{len(files)}files"
    bronze_prefix = f"nyc-taxi/scale/{batch_id}"

    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": utc_now(),
        "dataset": "nyc_taxi",
        "taxi_type": files[0].taxi_type,
        "year": "scale",
        "month": batch_id,
        "batch_id": batch_id,
        "source_url": DEFAULT_SOURCE_URL,
        "source_base_url": TLC_BASE_URL,
        "bronze_bucket": bronze_bucket,
        "bronze_prefix": bronze_prefix,
        "file_count": len(files),
        "total_size_bytes": total_size_bytes,
        "total_rows": total_rows,
        "months": months,
        "files": [
            {
                "role": "source",
                "format": "parquet",
                "content_type": "application/vnd.apache.parquet",
                "local_path": item.local_path.as_posix(),
                "file_name": item.file_name,
                "taxi_type": item.taxi_type,
                "year": item.year,
                "month": item.month,
                "row_count": item.row_count,
                "size_bytes": item.size_bytes,
                "checksum_algorithm": "sha256",
                "checksum_sha256": item.checksum_sha256,
                "source_url": item.url,
                "bronze_key": item.bronze_key,
                "bronze_uri": f"s3://{bronze_bucket}/{item.bronze_key}",
            }
            for item in files
        ],
        "notes": [
            "Multi-file NYC Taxi scale manifest built from official TLC Parquet files.",
            "Files are kept in month partitions in bronze.",
            "The manifest-level year/month identify the scale batch, not a calendar month.",
        ],
    }


def write_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        settings = load_settings()
        months = parse_months(args.months)
        if args.limit_files:
            months = months[: args.limit_files]

        output_dir = Path(args.output_dir)
        downloaded: list[DownloadedTaxiFile] = []
        for month in months:
            file_name = file_name_for(args.taxi_type, month)
            destination = output_dir / file_name
            download_file(url_for(args.taxi_type, month), destination, args.force)
            info = inspect_downloaded_file(args.taxi_type, month, destination)
            downloaded.append(info)
            print(
                f"inspected: {file_name} rows={info.row_count} "
                f"size_bytes={info.size_bytes}"
            )

        manifest = create_manifest(downloaded, settings.bronze_bucket)
        manifest_path = Path(args.manifest_path)
        write_manifest(manifest, manifest_path)

        print(f"manifest: {manifest_path}")
        print(f"file_count: {manifest['file_count']}")
        print(f"total_size_bytes: {manifest['total_size_bytes']}")
        print(f"total_rows: {manifest['total_rows']}")
        print(f"bronze_prefix: s3://{manifest['bronze_bucket']}/{manifest['bronze_prefix']}")
        print("download-nyc-taxi-scale ok")
        return 0
    except Exception as exc:
        print(f"download_nyc_taxi_scale failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
