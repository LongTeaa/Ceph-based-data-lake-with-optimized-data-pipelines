from decimal import Decimal
import unittest

from benchmark.query.spark_layout_benchmark import (
    benchmark_layouts,
    normalize_row,
    parse_sql_files,
    read_layout,
    summarize_results,
)


class SparkLayoutBenchmarkTests(unittest.TestCase):
    def test_parse_sql_files_rejects_empty_list(self):
        with self.assertRaises(ValueError):
            parse_sql_files(" , ")

    def test_normalize_row_rounds_numeric_values(self):
        row = normalize_row({"b": Decimal("1.123456789"), "a": 2.987654321})

        self.assertEqual(row, {"a": 2.987654, "b": 1.123457})

    def test_summarize_results_groups_by_layout_and_checks_consistency(self):
        records = [
            {
                "phase": "warmup",
                "status": "success",
                "layout": "partitioned",
                "query_name": "q",
                "rows_returned": 1,
                "result_sha256": "ignored",
                "duration_seconds": 99.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "layout": "partitioned",
                "query_name": "q",
                "rows_returned": 1,
                "result_sha256": "same",
                "duration_seconds": 1.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "layout": "non_partitioned",
                "query_name": "q",
                "rows_returned": 1,
                "result_sha256": "same",
                "duration_seconds": 2.0,
            },
        ]

        summary = summarize_results(records)

        self.assertEqual(len(summary), 2)
        self.assertTrue(all(row["result_consistent"] for row in summary))
        self.assertEqual({row["layout"] for row in summary}, {"partitioned", "non_partitioned"})

    def test_summarize_results_marks_inconsistent_hashes(self):
        records = [
            {
                "phase": "measured",
                "status": "success",
                "layout": "partitioned",
                "query_name": "q",
                "rows_returned": 1,
                "result_sha256": "a",
                "duration_seconds": 1.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "layout": "non_partitioned",
                "query_name": "q",
                "rows_returned": 1,
                "result_sha256": "b",
                "duration_seconds": 2.0,
            },
        ]

        summary = summarize_results(records)

        self.assertFalse(any(row["result_consistent"] for row in summary))

    def test_compaction_requires_positive_coalesce_before_loading_settings(self):
        with self.assertRaises(ValueError):
            benchmark_layouts(
                manifest_path="unused",
                sql_files=[],
                output_dir="unused",
                run_id="unused",
                iterations=1,
                warmup=0,
                comparison="compaction",
                coalesce=0,
                keep_layout=False,
            )

    def test_read_layout_rejects_unknown_format(self):
        with self.assertRaises(ValueError):
            read_layout(None, "s3a://bucket/path", "json", None)


if __name__ == "__main__":
    unittest.main()
