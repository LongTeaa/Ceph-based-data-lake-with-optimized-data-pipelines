#!/usr/bin/env python3
"""Generate deterministic binary objects for S3 storage benchmark payloads."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MANIFEST_VERSION = "synthetic_binary_manifest_v1"
SIZE_UNITS = {
    "b": 1,
    "kib": 1024,
    "kb": 1000,
    "mib": 1024 * 1024,
    "mb": 1000 * 1000,
    "gib": 1024 * 1024 * 1024,
    "gb": 1000 * 1000 * 1000,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic binary benchmark objects.")
    parser.add_argument("--object-sizes", default="4KiB,1MiB", help="Comma-separated sizes.")
    parser.add_argument("--count", type=int, default=2, help="Objects per size.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed.")
    parser.add_argument("--output-dir", default="data/source/synthetic/binary", help="Output directory.")
    parser.add_argument("--batch-id", default="", help="Batch id; defaults to sizes/count/seed.")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def parse_sizes(value: str) -> list[int]:
    sizes = [parse_size(item) for item in value.split(",") if item.strip()]
    if not sizes:
        raise ValueError("object-sizes must not be empty")
    return sizes


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def deterministic_bytes(size_bytes: int, seed: int) -> bytes:
    rng = random.Random(seed)
    return bytes(rng.randrange(0, 256) for _ in range(size_bytes))


def write_binary(path: Path, size_bytes: int, seed: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(deterministic_bytes(size_bytes, seed))


def create_manifest(batch_id: str, sizes: list[int], count: int, seed: int, files: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": utc_now(),
        "dataset": "synthetic_binary",
        "batch_id": batch_id,
        "object_sizes": sizes,
        "count_per_size": count,
        "seed": seed,
        "files": files,
    }


def generate_objects(sizes: list[int], count: int, seed: int, output_dir: Path, batch_id: str = "") -> dict[str, Any]:
    if count < 1:
        raise ValueError("count must be >= 1")
    batch = batch_id or "synthetic-binary-sizes=" + "-".join(str(size) for size in sizes) + f"-count={count}-seed={seed}"
    batch_dir = output_dir / batch
    files: list[dict[str, Any]] = []

    for size_index, size in enumerate(sizes):
        for object_index in range(count):
            object_seed = seed + size_index * 100000 + object_index
            path = batch_dir / f"object_size={size}" / f"object_{object_index:04d}.bin"
            write_binary(path, size, object_seed)
            files.append(
                {
                    "role": "object",
                    "format": "binary",
                    "content_type": "application/octet-stream",
                    "local_path": path.as_posix(),
                    "file_name": path.name,
                    "object_size_bytes": size,
                    "seed": object_seed,
                    "size_bytes": path.stat().st_size,
                    "checksum_algorithm": "sha256",
                    "checksum_sha256": sha256_file(path),
                }
            )

    manifest = create_manifest(batch, sizes, count, seed, files)
    manifest_path = batch_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    args = parse_args()
    try:
        manifest = generate_objects(
            sizes=parse_sizes(args.object_sizes),
            count=args.count,
            seed=args.seed,
            output_dir=Path(args.output_dir),
            batch_id=args.batch_id,
        )
        print(f"batch_id: {manifest['batch_id']}")
        print(f"files: {len(manifest['files'])}")
        for item in manifest["files"]:
            print(f"{item['local_path']} size={item['size_bytes']} sha256={item['checksum_sha256']}")
        print("generate_binary_objects ok")
        return 0
    except Exception as exc:
        print(f"generate_binary_objects failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
