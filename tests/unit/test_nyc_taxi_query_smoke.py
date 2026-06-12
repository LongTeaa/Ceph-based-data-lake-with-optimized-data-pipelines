from pathlib import Path
import tempfile
import unittest

from spark.jobs.nyc_taxi_query_smoke import load_sql_queries, render_sql


SQL_DIR = Path("spark/sql")


class NYCTaxiQuerySmokeTests(unittest.TestCase):
    def test_standard_sql_query_set_exists(self):
        queries = load_sql_queries(SQL_DIR)

        self.assertEqual(len(queries), 6)
        self.assertEqual(
            [query.name for query in queries],
            [
                "01_daily_revenue.sql",
                "02_top_pickup_locations.sql",
                "03_avg_tip_by_payment.sql",
                "04_hourly_distance_fare.sql",
                "05_selective_pickup_date.sql",
                "06_full_scan_location_aggregation.sql",
            ],
        )

    def test_queries_cover_gold_and_silver_views(self):
        combined_sql = "\n".join(path.read_text(encoding="utf-8") for path in load_sql_queries(SQL_DIR))

        for view_name in [
            "daily_trip_metrics",
            "location_metrics",
            "payment_metrics",
            "silver_trips",
        ]:
            self.assertIn(view_name, combined_sql)

    def test_render_sql_replaces_date_parameters(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "query.sql"
            path.write_text("SELECT DATE '${pickup_date}' AS pickup_date", encoding="utf-8")

            rendered = render_sql(path, {"pickup_date": "2025-01-01"})

        self.assertEqual(rendered, "SELECT DATE '2025-01-01' AS pickup_date")


if __name__ == "__main__":
    unittest.main()
