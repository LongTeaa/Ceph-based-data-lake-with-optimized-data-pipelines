from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from infrastructure.buckets.s3_common import apply_proxy_environment, load_dotenv, parse_bool


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

    def test_apply_proxy_environment_allows_dotenv_to_clear_host_proxy(self):
        with patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://127.0.0.1:9",
                "HTTPS_PROXY": "http://127.0.0.1:9",
                "ALL_PROXY": "http://127.0.0.1:9",
            },
            clear=True,
        ):
            apply_proxy_environment(
                {
                    "NO_PROXY": "localhost,127.0.0.1,192.168.56.101",
                    "HTTP_PROXY": "",
                    "HTTPS_PROXY": "",
                    "ALL_PROXY": "",
                }
            )

            self.assertEqual(os.environ["NO_PROXY"], "localhost,127.0.0.1,192.168.56.101")
            self.assertEqual(os.environ["HTTP_PROXY"], "")
            self.assertEqual(os.environ["HTTPS_PROXY"], "")
            self.assertEqual(os.environ["ALL_PROXY"], "")


if __name__ == "__main__":
    unittest.main()
