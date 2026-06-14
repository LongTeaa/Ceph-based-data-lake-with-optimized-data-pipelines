import unittest

from benchmark.storage.s3_benchmark import (
    Scenario,
    deterministic_payload,
    make_scenarios,
    parse_operations,
    parse_size,
    summarize_results,
)


class StorageBenchmarkTests(unittest.TestCase):
    def test_parse_size_supports_binary_units(self):
        self.assertEqual(parse_size("4KiB"), 4096)
        self.assertEqual(parse_size("1MiB"), 1024 * 1024)
        self.assertEqual(parse_size("10"), 10)

    def test_parse_size_rejects_unknown_unit(self):
        with self.assertRaises(ValueError):
            parse_size("4XB")

    def test_parse_operations_rejects_unknown_operation(self):
        self.assertEqual(parse_operations("put,get,mixed"), ["put", "get", "mixed"])
        with self.assertRaises(ValueError):
            parse_operations("put,delete")

    def test_deterministic_payload_reuses_seed(self):
        first = deterministic_payload(128, 7)
        second = deterministic_payload(128, 7)
        different = deterministic_payload(128, 8)

        self.assertEqual(first, second)
        self.assertNotEqual(first, different)
        self.assertEqual(len(first), 128)

    def test_make_scenarios_cross_product(self):
        scenarios = make_scenarios([4096, 1024 * 1024], [1, 4], ["put"])

        self.assertEqual(len(scenarios), 4)
        self.assertEqual(scenarios[0], Scenario(operation="put", object_size_bytes=4096, concurrency=1))

    def test_summarize_results_uses_measured_records(self):
        records = [
            {
                "phase": "warmup",
                "scenario_operation": "put",
                "object_size_bytes": 1024,
                "concurrency": 1,
                "status": "success",
                "logical_bytes": 1024,
                "latency_seconds": 9.0,
                "phase_elapsed_seconds": 9.0,
            },
            {
                "phase": "measured",
                "scenario_operation": "put",
                "object_size_bytes": 1024,
                "concurrency": 1,
                "status": "success",
                "logical_bytes": 1024,
                "latency_seconds": 0.1,
                "phase_elapsed_seconds": 0.1,
            },
            {
                "phase": "measured",
                "scenario_operation": "put",
                "object_size_bytes": 1024,
                "concurrency": 1,
                "status": "failed",
                "logical_bytes": 0,
                "latency_seconds": 0.2,
                "phase_elapsed_seconds": 0.2,
            },
        ]

        summary = summarize_results(records)

        self.assertEqual(summary[0]["operation"], "put")
        self.assertEqual(summary[0]["runs"], 2)
        self.assertEqual(summary[0]["errors"], 1)
        self.assertEqual(summary[0]["latency_p50_ms"], 100.0)


if __name__ == "__main__":
    unittest.main()
