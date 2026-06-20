"""Shared S3-compatible storage helpers for Phase 1 scripts."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REQUIRED_CONFIG = (
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_REGION",
    "BRONZE_BUCKET",
    "SILVER_BUCKET",
    "GOLD_BUCKET",
    "SYSTEM_BUCKET",
)

PROXY_CONFIG = (
    "NO_PROXY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "no_proxy",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    access_key: str
    secret_key: str
    region: str
    path_style_access: bool
    use_ssl: bool
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str
    system_bucket: str

    @property
    def buckets(self) -> tuple[str, str, str, str]:
        return (
            self.bronze_bucket,
            self.silver_bucket,
            self.gold_bucket,
            self.system_bucket,
        )


def load_dotenv(path: Path = Path(".env")) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_value(name: str, dotenv_values: dict[str, str]) -> str:
    return os.getenv(name) or dotenv_values.get(name, "")


def parse_bool(value: str, default: bool = False) -> bool:
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def apply_proxy_environment(dotenv_values: dict[str, str]) -> None:
    for name in PROXY_CONFIG:
        if name in dotenv_values:
            os.environ[name] = dotenv_values[name]


def load_settings() -> S3Settings:
    dotenv_values = load_dotenv()
    apply_proxy_environment(dotenv_values)
    missing = [name for name in REQUIRED_CONFIG if not get_value(name, dotenv_values)]
    if missing:
        raise ValueError(
            "Missing required S3 config: "
            + ", ".join(missing)
            + ". Create .env from .env.example or export these variables."
        )

    return S3Settings(
        endpoint=get_value("S3_ENDPOINT", dotenv_values),
        access_key=get_value("S3_ACCESS_KEY", dotenv_values),
        secret_key=get_value("S3_SECRET_KEY", dotenv_values),
        region=get_value("S3_REGION", dotenv_values),
        path_style_access=parse_bool(get_value("S3_PATH_STYLE_ACCESS", dotenv_values), True),
        use_ssl=parse_bool(get_value("S3_USE_SSL", dotenv_values), False),
        bronze_bucket=get_value("BRONZE_BUCKET", dotenv_values),
        silver_bucket=get_value("SILVER_BUCKET", dotenv_values),
        gold_bucket=get_value("GOLD_BUCKET", dotenv_values),
        system_bucket=get_value("SYSTEM_BUCKET", dotenv_values),
    )


def require_boto3():
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        print(
            "Missing dependency: boto3. Install dependencies with "
            "`pip install -r requirements.txt` inside your virtual environment.",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    return boto3, Config


def create_s3_client(settings: S3Settings):
    boto3, Config = require_boto3()
    addressing_style = "path" if settings.path_style_access else "auto"
    config = Config(
        signature_version="s3v4",
        s3={"addressing_style": addressing_style},
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=5,
        read_timeout=60,
    )
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        use_ssl=settings.use_ssl,
        config=config,
    )


def print_settings_summary(settings: S3Settings) -> None:
    print(f"S3 endpoint: {settings.endpoint}")
    print(f"S3 region: {settings.region}")
    print(f"S3 path-style access: {settings.path_style_access}")
    print("Buckets: " + ", ".join(settings.buckets))


def ensure_unique(values: Iterable[str]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        raise ValueError("Bucket names must be unique: " + ", ".join(sorted(duplicates)))
