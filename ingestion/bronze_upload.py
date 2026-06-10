#!/usr/bin/env python3
"""Upload files described by a manifest into the bronze bucket."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "infrastructure" / "buckets"))

from s3_common import create_s3_client, load_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload manifest-described files to bronze.")
    parser.add_argument(
        "--manifest-path",
        required=True,
        help="Local manifest path.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite remote objects when checksum/content differs.",
    )
    return parser.parse_args()


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def remote_object_matches(client, bucket: str, key: str, checksum: str, size_bytes: int) -> bool:
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if hasattr(exc, "response"):
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code", "")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
        raise

    metadata_sha = head.get("Metadata", {}).get("sha256")
    remote_size = head.get("ContentLength")
    if metadata_sha == checksum and remote_size == size_bytes:
        return True

    raise RuntimeError(
        f"Remote object exists but checksum/size differs: s3://{bucket}/{key}. "
        "Use --force to overwrite intentionally."
    )


def upload_file_if_needed(
    client,
    bucket: str,
    key: str,
    path: Path,
    checksum: str,
    content_type: str,
    force: bool,
) -> None:
    size_bytes = path.stat().st_size
    try:
        if remote_object_matches(client, bucket, key, checksum, size_bytes):
            print(f"exists: s3://{bucket}/{key}")
            return
    except RuntimeError:
        if not force:
            raise

    client.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": content_type,
            "Metadata": {"sha256": checksum},
        },
    )
    print(f"uploaded: s3://{bucket}/{key}")


def normalize_manifest_for_compare(manifest: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(manifest)
    normalized.pop("generated_at", None)
    return normalized


def remote_manifest_matches(client, bucket: str, key: str, manifest: dict[str, Any]) -> bool:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if hasattr(exc, "response"):
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code", "")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
        raise

    remote_manifest = json.loads(response["Body"].read().decode("utf-8"))
    if normalize_manifest_for_compare(remote_manifest) == normalize_manifest_for_compare(manifest):
        return True

    raise RuntimeError(
        f"Remote manifest exists but differs semantically: s3://{bucket}/{key}. "
        "Use --force to overwrite intentionally."
    )


def upload_manifest_if_needed(
    client,
    bucket: str,
    key: str,
    manifest: dict[str, Any],
    force: bool,
) -> None:
    body = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    checksum = hashlib.sha256(body).hexdigest()

    try:
        if remote_manifest_matches(client, bucket, key, manifest):
            print(f"exists: s3://{bucket}/{key}")
            return
    except RuntimeError:
        if not force:
            raise

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"sha256": checksum},
    )
    print(f"uploaded: s3://{bucket}/{key}")


def upload_manifest(manifest_path: Path, force: bool) -> None:
    manifest = load_manifest(manifest_path)
    settings = load_settings()
    client = create_s3_client(settings)
    bucket = manifest["bronze_bucket"]

    for item in manifest["files"]:
        upload_file_if_needed(
            client=client,
            bucket=bucket,
            key=item["bronze_key"],
            path=Path(item["local_path"]),
            checksum=item["checksum_sha256"],
            content_type=item["content_type"],
            force=force,
        )

    manifest_key = f"{manifest['bronze_prefix']}/manifest.json"
    upload_manifest_if_needed(client, bucket, manifest_key, manifest, force)


def main() -> int:
    args = parse_args()
    try:
        upload_manifest(Path(args.manifest_path), args.force)
        print("ingest ok")
        return 0
    except Exception as exc:
        print(f"bronze_upload failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
