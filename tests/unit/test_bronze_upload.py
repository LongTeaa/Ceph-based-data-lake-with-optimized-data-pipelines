import unittest

from ingestion.bronze_upload import normalize_manifest_for_compare


class BronzeUploadTests(unittest.TestCase):
    def test_normalize_manifest_ignores_generated_at(self):
        first = {"generated_at": "2026-01-01T00:00:00+00:00", "dataset": "nyc_taxi"}
        second = {"generated_at": "2026-01-02T00:00:00+00:00", "dataset": "nyc_taxi"}

        self.assertEqual(
            normalize_manifest_for_compare(first),
            normalize_manifest_for_compare(second),
        )


if __name__ == "__main__":
    unittest.main()
