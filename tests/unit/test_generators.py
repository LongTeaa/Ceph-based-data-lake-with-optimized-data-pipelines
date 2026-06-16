import tempfile
import unittest
from pathlib import Path

from generator.generate_binary_objects import (
    deterministic_bytes,
    generate_objects,
    parse_size,
    parse_sizes,
)
from generator.generate_test_records import generate_dataset, generate_records


class SyntheticTabularGeneratorTests(unittest.TestCase):
    def test_records_are_deterministic_for_same_seed(self):
        first = generate_records(rows=12, days=3, seed=7)
        second = generate_records(rows=12, days=3, seed=7)
        different = generate_records(rows=12, days=3, seed=8)

        self.assertEqual(first, second)
        self.assertNotEqual(first, different)
        self.assertEqual(first[0]["quality_case"], "valid")

    def test_dataset_writes_csv_jsonl_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = generate_dataset(
                rows=80,
                days=4,
                seed=11,
                output_dir=Path(tmpdir),
                batch_id="test-batch",
            )

            files = {item["format"]: Path(item["local_path"]) for item in manifest["files"]}
            self.assertTrue(files["csv"].exists())
            self.assertTrue(files["jsonl"].exists())
            self.assertTrue((Path(tmpdir) / "test-batch" / "manifest.json").exists())
            self.assertEqual(manifest["rows"], 80)
            self.assertGreater(manifest["quality_counts"]["negative_amount"], 0)
            self.assertGreater(manifest["quality_counts"]["bad_timestamp"], 0)


class SyntheticBinaryGeneratorTests(unittest.TestCase):
    def test_parse_sizes(self):
        self.assertEqual(parse_size("4KiB"), 4096)
        self.assertEqual(parse_sizes("4KiB,1MiB"), [4096, 1024 * 1024])
        with self.assertRaises(ValueError):
            parse_size("1XB")

    def test_deterministic_bytes_reuses_seed(self):
        self.assertEqual(deterministic_bytes(64, 3), deterministic_bytes(64, 3))
        self.assertNotEqual(deterministic_bytes(64, 3), deterministic_bytes(64, 4))

    def test_generate_objects_writes_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = generate_objects(
                sizes=[16, 32],
                count=2,
                seed=5,
                output_dir=Path(tmpdir),
                batch_id="binary-batch",
            )

            self.assertEqual(len(manifest["files"]), 4)
            self.assertTrue((Path(tmpdir) / "binary-batch" / "manifest.json").exists())
            self.assertEqual(sorted({item["object_size_bytes"] for item in manifest["files"]}), [16, 32])
            self.assertTrue(all(Path(item["local_path"]).exists() for item in manifest["files"]))


if __name__ == "__main__":
    unittest.main()
