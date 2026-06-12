import unittest

from benchmark.query.spark_sql_benchmark import percentile, summarize_results


class QueryBenchmarkTests(unittest.TestCase):
    def test_percentile_interpolates_values(self):
        self.assertEqual(percentile([1.0, 2.0, 3.0], 50), 2.0)
        self.assertAlmostEqual(percentile([1.0, 2.0, 3.0, 4.0], 95), 3.85)

    def test_summarize_results_uses_measured_successful_runs(self):
        records = [
            {
                "phase": "warmup",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 10,
                "duration_seconds": 99.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 10,
                "duration_seconds": 1.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 10,
                "duration_seconds": 2.0,
            },
        ]

        summary = summarize_results(records)

        self.assertEqual(summary[0]["query_name"], "q1")
        self.assertEqual(summary[0]["runs"], 2)
        self.assertEqual(summary[0]["rows_returned"], 10)
        self.assertEqual(summary[0]["median_seconds"], 1.5)


if __name__ == "__main__":
    unittest.main()
