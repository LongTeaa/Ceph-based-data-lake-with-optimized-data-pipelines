#!/usr/bin/env python3
"""Create bronze ingest manifests for NYC Yellow Taxi source files."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import load_dotenv, load_settings


NYC_TAXI_FILENAME_RE = re.compile(
    r"^(?P<taxi_type>yellow|green|fhv|fhvhv)_tripdata_(?P<year>\d{4})-(?P<month>\d{2})\.parquet$"
)
MANIFEST_VERSION = "nyc_taxi_manifest_v1"
DEFAULT_SOURCE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


@dataclass(frozen=True)
class TaxiFileInfo:
    path: Path
    file_name: str
    taxi_type: str
    year: str
    month: str
    size_bytes: int
    checksum_sha256: str

    @property
    def bronze_prefix(self) -> str:
        return f"nyc-taxi/year={self.year}/month={self.month}"

    @property
    def bronze_key(self) -> str:
        return f"{self.bronze_prefix}/{self.file_name}"

    @property
    def manifest_key(self) -> str:
        return f"{self.bronze_prefix}/manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a local NYC Taxi manifest.")
    parser.add_argument(
        "--source-dir",
        default="",
        help="Directory containing NYC Taxi parquet files. Defaults to NYC_TAXI_SOURCE_DIR.",
    )
    parser.add_argument(
        "--file-name",
        default="yellow_tripdata_2025-01.parquet",
        help="NYC Taxi parquet file name inside source-dir.",
    )
    parser.add_argument(
        "--manifest-path",
        default="",
        help="Output manifest path. Defaults to data/source/nyc-taxi/manifests/<file>.manifest.json.",
    )
    return parser.parse_args()


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


def parse_taxi_file(path: Path) -> TaxiFileInfo:
    match = NYC_TAXI_FILENAME_RE.match(path.name)
    if not match:
        raise ValueError(
            f"Unexpected NYC Taxi file name: {path.name}. "
            "Expected yellow_tripdata_YYYY-MM.parquet."
        )
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_file():
        raise ValueError(f"Not a file: {path}")

    return TaxiFileInfo(
        path=path,
        file_name=path.name,
        taxi_type=match.group("taxi_type"),
        year=match.group("year"),
        month=match.group("month"),
        size_bytes=path.stat().st_size,
        checksum_sha256=sha256_file(path),
    )


def source_dir_from_env() -> Path:
    values = load_dotenv()
    return Path(values.get("NYC_TAXI_SOURCE_DIR", "data/source/nyc-taxi"))


def default_manifest_path(source_dir: Path, file_name: str) -> Path:
    return source_dir / "manifests" / f"{Path(file_name).stem}.manifest.json"


def create_manifest(info: TaxiFileInfo, bronze_bucket: str) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": utc_now(),
        "dataset": "nyc_taxi",
        "taxi_type": info.taxi_type,
        "year": info.year,
        "month": info.month,
        "source_url": DEFAULT_SOURCE_URL,
        "bronze_bucket": bronze_bucket,
        "bronze_prefix": info.bronze_prefix,
        "files": [
            {
                "role": "source",
                "format": "parquet",
                "content_type": "application/vnd.apache.parquet",
                "local_path": info.path.as_posix(),
                "file_name": info.file_name,
                "size_bytes": info.size_bytes,
                "checksum_algorithm": "sha256",
                "checksum_sha256": info.checksum_sha256,
                "bronze_key": info.bronze_key,
                "bronze_uri": f"s3://{bronze_bucket}/{info.bronze_key}",
            }
        ],
        "notes": [
            "Original source file is kept immutable in bronze.",
            "Row count and schema inspection are deferred to the Spark phase.",
        ],
    }


def write_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def command_prepare(args: argparse.Namespace) -> int:
    settings = load_settings()
    source_dir = Path(args.source_dir) if args.source_dir else source_dir_from_env()
    source_file = source_dir / args.file_name
    manifest_path = (
        Path(args.manifest_path)
        if args.manifest_path
        else default_manifest_path(source_dir, args.file_name)
    )

    info = parse_taxi_file(source_file)
    manifest = create_manifest(info, settings.bronze_bucket)
    write_manifest(manifest, manifest_path)

    print(f"source: {source_file}")
    print(f"size_bytes: {info.size_bytes}")
    print(f"checksum_sha256: {info.checksum_sha256}")
    print(f"bronze_uri: s3://{settings.bronze_bucket}/{info.bronze_key}")
    print(f"manifest: {manifest_path}")
    print("prepare-nyc-taxi ok")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return command_prepare(args)
    except Exception as exc:
        print(f"nyc_taxi_manifest failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
