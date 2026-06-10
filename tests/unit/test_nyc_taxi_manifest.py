import json
from pathlib import Path
import tempfile
import unittest

from ingestion.nyc_taxi_manifest import (
    create_manifest,
    default_manifest_path,
    parse_taxi_file,
    sha256_file,
    write_manifest,
)


class NYCTaxiManifestTests(unittest.TestCase):
    def test_parse_taxi_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "yellow_tripdata_2025-01.parquet"
            path.write_bytes(b"sample parquet payload")

            info = parse_taxi_file(path)

        self.assertEqual(info.taxi_type, "yellow")
        self.assertEqual(info.year, "2025")
        self.assertEqual(info.month, "01")
        self.assertEqual(info.file_name, "yellow_tripdata_2025-01.parquet")
        self.assertEqual(info.bronze_key, "nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet")
        self.assertEqual(info.manifest_key, "nyc-taxi/year=2025/month=01/manifest.json")

    def test_parse_rejects_unexpected_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "taxi.parquet"
            path.write_bytes(b"data")

            with self.assertRaises(ValueError):
                parse_taxi_file(path)

    def test_create_and_write_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "yellow_tripdata_2025-01.parquet"
            source.write_bytes(b"abc")
            expected_checksum = sha256_file(source)
            info = parse_taxi_file(source)
            manifest = create_manifest(info, "datalake-bronze")
            manifest_path = default_manifest_path(Path(tmpdir), source.name)
            write_manifest(manifest, manifest_path)

            loaded = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(loaded["manifest_version"], "nyc_taxi_manifest_v1")
        self.assertEqual(loaded["bronze_bucket"], "datalake-bronze")
        self.assertEqual(loaded["files"][0]["checksum_sha256"], expected_checksum)
        self.assertEqual(loaded["files"][0]["bronze_uri"], "s3://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet")


if __name__ == "__main__":
    unittest.main()
