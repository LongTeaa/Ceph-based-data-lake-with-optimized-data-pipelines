import tempfile
import unittest
from pathlib import Path

from ingestion.download_nyc_taxi_scale import (
    DownloadedTaxiFile,
    create_manifest,
    default_months,
    file_name_for,
    parse_months,
    url_for,
)


class DownloadNYCTaxiScaleTests(unittest.TestCase):
    def test_default_months_returns_30_months(self):
        months = default_months()

        self.assertEqual(len(months), 30)
        self.assertEqual(months[0], "2023-01")
        self.assertEqual(months[-1], "2025-06")

    def test_file_name_and_url_use_tlc_pattern(self):
        self.assertEqual(file_name_for("yellow", "2025-01"), "yellow_tripdata_2025-01.parquet")
        self.assertEqual(
            url_for("yellow", "2025-01"),
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
        )

    def test_parse_months_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            parse_months("2025-01,2025-13")

    def test_create_manifest_summarizes_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            first = DownloadedTaxiFile(
                year="2023",
                month="01",
                taxi_type="yellow",
                file_name="yellow_tripdata_2023-01.parquet",
                url="https://example.test/yellow_tripdata_2023-01.parquet",
                local_path=base / "yellow_tripdata_2023-01.parquet",
                size_bytes=100,
                checksum_sha256="a" * 64,
                row_count=10,
            )
            second = DownloadedTaxiFile(
                year="2023",
                month="02",
                taxi_type="yellow",
                file_name="yellow_tripdata_2023-02.parquet",
                url="https://example.test/yellow_tripdata_2023-02.parquet",
                local_path=base / "yellow_tripdata_2023-02.parquet",
                size_bytes=200,
                checksum_sha256="b" * 64,
                row_count=20,
            )

            manifest = create_manifest([first, second], "datalake-bronze")

        self.assertEqual(manifest["file_count"], 2)
        self.assertEqual(manifest["total_size_bytes"], 300)
        self.assertEqual(manifest["total_rows"], 30)
        self.assertEqual(manifest["year"], "scale")
        self.assertEqual(manifest["month"], "2023-01_2023-02_2files")
        self.assertEqual(manifest["bronze_prefix"], "nyc-taxi/scale/2023-01_2023-02_2files")
        self.assertEqual(
            manifest["files"][1]["bronze_key"],
            "nyc-taxi/year=2023/month=02/yellow_tripdata_2023-02.parquet",
        )


if __name__ == "__main__":
    unittest.main()
