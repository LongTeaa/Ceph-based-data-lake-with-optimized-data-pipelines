from pathlib import Path
import tempfile
import unittest

from infrastructure.buckets.s3_common import load_dotenv, parse_bool


class S3CommonTests(unittest.TestCase):
    def test_parse_bool(self):
        self.assertTrue(parse_bool("true"))
        self.assertTrue(parse_bool("1"))
        self.assertTrue(parse_bool("YES"))
        self.assertFalse(parse_bool("false"))
        self.assertFalse(parse_bool(""))
        self.assertTrue(parse_bool("", default=True))

    def test_load_dotenv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / ".env"
            path.write_text(
                "\n".join(
                    [
                        "# comment",
                        "S3_ENDPOINT=http://localhost:9000",
                        "S3_ACCESS_KEY='minioadmin'",
                        'S3_SECRET_KEY="minioadmin123"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            values = load_dotenv(path)

        self.assertEqual(values["S3_ENDPOINT"], "http://localhost:9000")
        self.assertEqual(values["S3_ACCESS_KEY"], "minioadmin")
        self.assertEqual(values["S3_SECRET_KEY"], "minioadmin123")


if __name__ == "__main__":
    unittest.main()
