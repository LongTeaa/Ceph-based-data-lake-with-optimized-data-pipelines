#!/usr/bin/env python3
"""Download demo images from Wikimedia Commons and create metadata.

This script is intentionally dependency-free. It uses the Wikimedia Commons
MediaWiki API, stores images under data/source/images/raw, and writes metadata
as both CSV and JSONL under data/source/images/metadata.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import mimetypes
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


API_URL = "https://commons.wikimedia.org/w/api.php"
DEFAULT_QUERIES = [
    "New York taxi",
    "taxi cab",
    "city street traffic",
    "public transport",
    "urban road",
]
DEFAULT_USER_AGENT = (
    "CephDataLakeStudentProject/1.0 "
    "(metadata demo; https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia)"
)
CSV_FIELDS = [
    "image_id",
    "file_name",
    "category",
    "source_url",
    "direct_url",
    "license",
    "license_url",
    "author",
    "content_type",
    "size_bytes",
    "width",
    "height",
    "checksum_sha256",
    "ingested_at",
    "commons_title",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download images from Wikimedia Commons and generate metadata.",
    )
    parser.add_argument("--limit", type=int, default=50, help="Number of images to download.")
    parser.add_argument(
        "--queries",
        nargs="+",
        default=DEFAULT_QUERIES,
        help="Search queries used on Wikimedia Commons.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/source/images/raw",
        help="Directory for downloaded image files.",
    )
    parser.add_argument(
        "--metadata-dir",
        default="data/source/images/metadata",
        help="Directory for metadata CSV/JSONL outputs.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="Seconds to sleep between downloads.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent sent to Wikimedia. Keep this descriptive.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query API and print selected files without downloading.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing image files and metadata.",
    )
    return parser.parse_args()


def http_json(url: str, user_agent: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def http_bytes(url: str, user_agent: str) -> tuple[bytes, str]:
    request = Request(url, headers={"User-Agent": user_agent})
    with urlopen(request, timeout=120) as response:
        content_type = response.headers.get_content_type()
        return response.read(), content_type


def strip_markup(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return " ".join(text.split())


def metadata_value(extmetadata: dict[str, Any], key: str) -> str:
    raw = extmetadata.get(key, {})
    if isinstance(raw, dict):
        return strip_markup(raw.get("value", ""))
    return strip_markup(raw)


def file_extension(title: str, direct_url: str, content_type: str) -> str:
    title_ext = Path(title).suffix.lower()
    if title_ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return ".jpg" if title_ext == ".jpeg" else title_ext

    url_ext = Path(urlparse(direct_url).path).suffix.lower()
    if url_ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return ".jpg" if url_ext == ".jpeg" else url_ext

    guessed = mimetypes.guess_extension(content_type or "")
    if guessed in {".jpg", ".jpeg", ".png", ".webp"}:
        return ".jpg" if guessed == ".jpeg" else guessed

    return ".jpg"


def category_from_query(query: str) -> str:
    query = query.lower()
    if "taxi" in query:
        return "taxi"
    if "traffic" in query:
        return "traffic"
    if "transport" in query:
        return "transport"
    if "road" in query or "street" in query:
        return "street"
    return "other"


def commons_file_page(title: str) -> str:
    return "https://commons.wikimedia.org/wiki/" + title.replace(" ", "_")


def query_commons(search: str, limit: int, user_agent: str) -> list[dict[str, Any]]:
    params = {
        "action": "query",
        "format": "json",
        "generator": "search",
        "gsrnamespace": "6",
        "gsrsearch": search,
        "gsrlimit": str(limit),
        "prop": "imageinfo",
        "iiprop": "url|mime|size|extmetadata",
        "iiurlwidth": "1200",
    }
    url = API_URL + "?" + urlencode(params)
    payload = http_json(url, user_agent)
    pages = payload.get("query", {}).get("pages", {})
    results: list[dict[str, Any]] = []

    for page in pages.values():
        info = (page.get("imageinfo") or [{}])[0]
        direct_url = info.get("url") or info.get("thumburl")
        mime = info.get("mime", "")
        if not direct_url or not str(mime).startswith("image/"):
            continue
        if str(mime).lower() not in {"image/jpeg", "image/png", "image/webp"}:
            continue
        results.append(
            {
                "title": page.get("title", ""),
                "direct_url": direct_url,
                "mime": mime,
                "width": info.get("width", ""),
                "height": info.get("height", ""),
                "extmetadata": info.get("extmetadata", {}),
                "category": category_from_query(search),
            }
        )

    return results


def collect_candidates(args: argparse.Namespace) -> list[dict[str, Any]]:
    seen_titles: set[str] = set()
    candidates: list[dict[str, Any]] = []
    per_query_limit = max(args.limit, 25)

    for query in args.queries:
        for item in query_commons(query, per_query_limit, args.user_agent):
            title = item["title"]
            if title in seen_titles:
                continue
            seen_titles.add(title)
            candidates.append(item)
            if len(candidates) >= args.limit:
                return candidates

    return candidates


def sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def write_metadata(metadata_dir: Path, rows: list[dict[str, Any]]) -> None:
    metadata_dir.mkdir(parents=True, exist_ok=True)

    csv_path = metadata_dir / "images_metadata.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    jsonl_path = metadata_dir / "images_metadata.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def download_images(args: argparse.Namespace, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for idx, item in enumerate(candidates, start=1):
        title = item["title"]
        extmetadata = item["extmetadata"]
        license_name = metadata_value(extmetadata, "LicenseShortName")
        license_url = metadata_value(extmetadata, "LicenseUrl")
        author = metadata_value(extmetadata, "Artist") or metadata_value(extmetadata, "Credit")

        image_id = f"img_{idx:04d}"
        ext = file_extension(title, item["direct_url"], item["mime"])
        file_name = f"{image_id}_{item['category']}{ext}"
        output_path = raw_dir / file_name

        if output_path.exists() and not args.overwrite:
            print(f"skip existing: {output_path}")
            data = output_path.read_bytes()
            content_type = item["mime"]
        else:
            print(f"download {idx:03d}/{len(candidates):03d}: {title}")
            data, content_type = http_bytes(item["direct_url"], args.user_agent)
            output_path.write_bytes(data)
            time.sleep(args.sleep)

        rows.append(
            {
                "image_id": image_id,
                "file_name": file_name,
                "category": item["category"],
                "source_url": commons_file_page(title),
                "direct_url": item["direct_url"],
                "license": license_name,
                "license_url": license_url,
                "author": author,
                "content_type": content_type,
                "size_bytes": len(data),
                "width": item["width"],
                "height": item["height"],
                "checksum_sha256": sha256_bytes(data),
                "ingested_at": ingested_at,
                "commons_title": title,
            }
        )

    return rows


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        print("--limit must be positive", file=sys.stderr)
        return 2

    try:
        candidates = collect_candidates(args)
    except (HTTPError, URLError, TimeoutError) as exc:
        print(f"Failed to query Wikimedia Commons: {exc}", file=sys.stderr)
        return 1

    if len(candidates) < args.limit:
        print(
            f"Only found {len(candidates)} usable images for the configured queries; "
            f"requested {args.limit}.",
            file=sys.stderr,
        )

    if args.dry_run:
        for idx, item in enumerate(candidates, start=1):
            print(f"{idx:03d}: {item['category']} | {item['title']} | {item['direct_url']}")
        return 0

    try:
        rows = download_images(args, candidates)
    except (HTTPError, URLError, TimeoutError) as exc:
        print(f"Failed while downloading images: {exc}", file=sys.stderr)
        return 1

    write_metadata(Path(args.metadata_dir), rows)
    print(f"Downloaded {len(rows)} images into {args.raw_dir}")
    print(f"Wrote metadata to {args.metadata_dir}/images_metadata.csv")
    print(f"Wrote metadata to {args.metadata_dir}/images_metadata.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
