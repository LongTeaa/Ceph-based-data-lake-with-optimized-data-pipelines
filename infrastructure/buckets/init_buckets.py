#!/usr/bin/env python3
"""Create Data Lake buckets on an S3-compatible endpoint."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from s3_common import create_s3_client, ensure_unique, load_settings, print_settings_summary


def bucket_exists(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except Exception as exc:
        if not hasattr(exc, "response"):
            raise
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return False
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket", "NotFound"}:
            return False
        raise


def create_bucket(client, bucket: str, region: str) -> None:
    kwargs = {"Bucket": bucket}
    if region and region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        client.create_bucket(**kwargs)
    except Exception as exc:
        if not hasattr(exc, "response"):
            raise
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            return
        raise


def main() -> int:
    try:
        settings = load_settings()
        ensure_unique(settings.buckets)
        client = create_s3_client(settings)
        print_settings_summary(settings)

        for bucket in settings.buckets:
            if bucket_exists(client, bucket):
                print(f"exists: {bucket}")
                continue
            create_bucket(client, bucket, settings.region)
            print(f"created: {bucket}")

        print("init-buckets ok")
        return 0
    except SystemExit:
        raise
    except Exception as exc:
        if exc.__class__.__name__ == "EndpointConnectionError":
            print(f"Cannot connect to S3 endpoint: {exc}", file=sys.stderr)
            return 1
        print(f"init-buckets failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
