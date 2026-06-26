from pathlib import Path
import tempfile
import unittest

from spark.jobs.nyc_taxi_common import (
    batch_from_manifest,
    gold_paths,
    metrics_path,
    missing_required_columns,
    s3_uri,
)


class NYCTaxiSparkCommonTests(unittest.TestCase):
    def test_s3_uri_uses_s3a_scheme_and_normalizes_key(self):
        self.assertEqual(
            s3_uri("datalake-bronze", "/nyc-taxi/year=2025/month=01/file.parquet/"),
            "s3a://datalake-bronze/nyc-taxi/year=2025/month=01/file.parquet",
        )

    def test_batch_from_manifest_builds_bronze_and_silver_paths(self):
        manifest = {
            "dataset": "nyc_taxi",
            "taxi_type": "yellow",
            "year": "2025",
            "month": "01",
            "bronze_bucket": "datalake-bronze",
            "bronze_prefix": "nyc-taxi/year=2025/month=01",
            "files": [
                {
                    "bronze_key": "nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet",
                }
            ],
        }

        batch = batch_from_manifest(manifest, "datalake-silver")

        self.assertEqual(
            batch.source_uri,
            "s3a://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet",
        )
        self.assertEqual(batch.silver_prefix, "nyc-taxi/year=2025/month=01")
        self.assertEqual(batch.silver_uri, "s3a://datalake-silver/nyc-taxi/year=2025/month=01")
        self.assertEqual(
            batch.source_uris,
            ("s3a://datalake-bronze/nyc-taxi/year=2025/month=01/yellow_tripdata_2025-01.parquet",),
        )

    def test_batch_from_manifest_accepts_multi_file_scale_manifest(self):
        manifest = {
            "dataset": "nyc_taxi",
            "taxi_type": "yellow",
            "year": "scale",
            "month": "2023-01_2023-02_2files",
            "bronze_bucket": "datalake-bronze",
            "bronze_prefix": "nyc-taxi/scale/2023-01_2023-02_2files",
            "files": [
                {
                    "bronze_key": "nyc-taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet",
                },
                {
                    "bronze_key": "nyc-taxi/year=2023/month=02/yellow_tripdata_2023-02.parquet",
                },
            ],
        }

        batch = batch_from_manifest(manifest, "datalake-silver")

        self.assertEqual(
            batch.source_uris,
            (
                "s3a://datalake-bronze/nyc-taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet",
                "s3a://datalake-bronze/nyc-taxi/year=2023/month=02/yellow_tripdata_2023-02.parquet",
            ),
        )
        self.assertEqual(batch.source_uri, batch.source_uris[0])
        self.assertEqual(
            batch.silver_uri,
            "s3a://datalake-silver/nyc-taxi/year=scale/month=2023-01_2023-02_2files",
        )

    def test_missing_required_columns_returns_ordered_missing_columns(self):
        missing = missing_required_columns(["tpep_pickup_datetime", "fare_amount"])

        self.assertEqual(
            missing,
            [
                "tpep_dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "PULocationID",
                "DOLocationID",
                "payment_type",
                "tip_amount",
                "total_amount",
            ],
        )

    def test_metrics_path_uses_job_year_and_month(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = metrics_path(Path(tmpdir), "job", "2025", "01")

            self.assertEqual(path, Path(tmpdir) / "job" / "year=2025" / "month=01" / "metrics.json")

    def test_gold_paths_builds_expected_output_uris(self):
        paths = gold_paths("datalake-gold", "2025", "01")

        self.assertEqual(
            paths.daily_metrics_uri,
            "s3a://datalake-gold/daily_trip_metrics/year=2025/month=01",
        )
        self.assertEqual(
            paths.location_metrics_uri,
            "s3a://datalake-gold/location_metrics/year=2025/month=01",
        )
        self.assertEqual(
            paths.payment_metrics_uri,
            "s3a://datalake-gold/payment_metrics/year=2025/month=01",
        )


if __name__ == "__main__":
    unittest.main()
