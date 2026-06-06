#!/usr/bin/env python3
"""Run an S3 upload/download/checksum smoke test."""

from __future__ import annotations

import argparse
import hashlib
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from s3_common import create_s3_client, load_settings, print_settings_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test S3-compatible storage.")
    parser.add_argument("--health-only", action="store_true", help="Only list buckets.")
    parser.add_argument("--bucket", default="", help="Bucket to use; defaults to SYSTEM_BUCKET.")
    parser.add_argument("--key-prefix", default="smoke-tests", help="Object key prefix.")
    parser.add_argument("--size-bytes", type=int, default=4096, help="Payload size.")
    return parser.parse_args()


def sha256(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def health_check(client) -> None:
    response = client.list_buckets()
    names = [bucket["Name"] for bucket in response.get("Buckets", [])]
    print("reachable buckets: " + (", ".join(names) if names else "(none)"))


def smoke_test(client, bucket: str, key_prefix: str, size_bytes: int) -> None:
    if size_bytes <= 0:
        raise ValueError("--size-bytes must be positive")

    payload = secrets.token_bytes(size_bytes)
    expected_sha = sha256(payload)
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{key_prefix.rstrip('/')}/{now}-{expected_sha[:12]}.bin"

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        Metadata={"sha256": expected_sha},
        ContentType="application/octet-stream",
    )
    print(f"uploaded: s3://{bucket}/{key}")

    head = client.head_object(Bucket=bucket, Key=key)
    metadata_sha = head.get("Metadata", {}).get("sha256")
    if metadata_sha != expected_sha:
        raise RuntimeError("metadata checksum mismatch")

    downloaded = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    actual_sha = sha256(downloaded)
    if actual_sha != expected_sha:
        raise RuntimeError("downloaded payload checksum mismatch")
    print(f"checksum ok: {actual_sha}")

    client.delete_object(Bucket=bucket, Key=key)
    print(f"deleted: s3://{bucket}/{key}")


def main() -> int:
    args = parse_args()
    try:
        settings = load_settings()
        client = create_s3_client(settings)
        print_settings_summary(settings)
        health_check(client)

        if args.health_only:
            print("storage health ok")
            return 0

        bucket = args.bucket or settings.system_bucket
        smoke_test(client, bucket, args.key_prefix, args.size_bytes)
        print("storage-smoke ok")
        return 0
    except SystemExit:
        raise
    except Exception as exc:
        if exc.__class__.__name__ == "EndpointConnectionError":
            print(f"Cannot connect to S3 endpoint: {exc}", file=sys.stderr)
            return 1
        if exc.__class__.__name__ == "ClientError":
            print(f"S3 request failed: {exc}", file=sys.stderr)
            return 1
        print(f"storage-smoke failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
