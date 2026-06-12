import unittest
from pathlib import Path

from benchmark.query.trino_benchmark import PROJECT_ROOT, container_sql_path, count_csv_rows, summarize_results, trino_command


class TrinoBenchmarkTests(unittest.TestCase):
    def test_count_csv_rows_handles_quoted_values(self):
        output = '"2025-01-01","10","123.45"\n"2025-01-02","20","456.78"\n'

        self.assertEqual(count_csv_rows(output), 2)

    def test_count_csv_rows_ignores_blank_lines(self):
        self.assertEqual(count_csv_rows('\n"1"\n\n'), 1)

    def test_trino_command_builds_execute_command(self):
        command = trino_command(
            compose_file=Path("docker/compose.yml"),
            service="trino",
            server="localhost:8080",
            catalog="lake",
            schema="nyc_taxi",
            execute="SELECT 1",
        )

        self.assertEqual(command[:3], ["docker", "compose", "-f"])
        self.assertEqual(Path(command[3]), Path("docker/compose.yml"))
        self.assertEqual(command[4:6], ["exec", "-T"])
        self.assertIn("--catalog", command)
        self.assertIn("lake", command)
        self.assertIn("--execute", command)
        self.assertIn("SELECT 1", command)

    def test_container_sql_path_maps_repo_sql_to_container_mount(self):
        host_path = PROJECT_ROOT / "docker" / "trino" / "sql" / "nyc_taxi_gold_setup.sql"

        self.assertEqual(container_sql_path(host_path), "/etc/trino/sql/nyc_taxi_gold_setup.sql")

    def test_summarize_results_uses_measured_successful_runs(self):
        records = [
            {
                "phase": "warmup",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 1,
                "duration_seconds": 99.0,
            },
            {
                "phase": "measured",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 1,
                "duration_seconds": 0.5,
            },
            {
                "phase": "measured",
                "status": "success",
                "query_name": "q1",
                "rows_returned": 1,
                "duration_seconds": 1.5,
            },
        ]

        summary = summarize_results(records)

        self.assertEqual(summary[0]["query_name"], "q1")
        self.assertEqual(summary[0]["runs"], 2)
        self.assertEqual(summary[0]["rows_returned"], 1)
        self.assertEqual(summary[0]["median_seconds"], 1.0)


if __name__ == "__main__":
    unittest.main()
