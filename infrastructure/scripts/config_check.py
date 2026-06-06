#!/usr/bin/env python3
"""Validate required environment variables for local project commands."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def load_dotenv(path: Path) -> dict[str, str]:
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


def main(argv: list[str]) -> int:
    required = argv[1:]
    if not required:
        print("No required variables were provided.")
        return 0

    dotenv_values = load_dotenv(Path(".env"))
    missing = [
        key
        for key in required
        if not os.getenv(key) and not dotenv_values.get(key)
    ]

    if missing:
        print("Missing required config: " + ", ".join(missing))
        print("Create .env from .env.example or export the variables in your shell.")
        return 1

    print("config-check ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
