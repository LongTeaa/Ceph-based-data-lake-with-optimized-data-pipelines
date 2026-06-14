import json
from pathlib import Path
import unittest


PROMETHEUS_CONFIG = Path("docker/prometheus/prometheus.yml")
GRAFANA_DASHBOARD = Path("docker/grafana/dashboards/data-lake-local-overview.json")
GRAFANA_DATASOURCE = Path("docker/grafana/provisioning/datasources/prometheus.yml")
SPARK_METRICS = Path("docker/spark/metrics.properties")


class MonitoringConfigTests(unittest.TestCase):
    def test_prometheus_scrapes_expected_local_jobs(self):
        source = PROMETHEUS_CONFIG.read_text(encoding="utf-8")

        for job in [
            "job_name: prometheus",
            "job_name: minio",
            "job_name: airflow",
            "job_name: spark-master",
            "job_name: spark-worker",
        ]:
            self.assertIn(job, source)

        self.assertIn("/minio/v2/metrics/cluster", source)
        self.assertIn("/metrics/master/prometheus", source)
        self.assertIn("statsd-exporter:9102", source)

    def test_grafana_dashboard_is_valid_json_with_expected_panels(self):
        dashboard = json.loads(GRAFANA_DASHBOARD.read_text(encoding="utf-8"))

        self.assertEqual("Data Lake Local Overview", dashboard["title"])
        self.assertEqual("data-lake-local-overview", dashboard["uid"])
        panel_titles = {panel["title"] for panel in dashboard["panels"]}

        for title in [
            "Scrape Target Health",
            "Storage Used",
            "S3 Request Rate",
            "Spark Metrics",
            "Airflow Task Failures",
        ]:
            self.assertIn(title, panel_titles)

    def test_grafana_datasource_uses_prometheus_service(self):
        source = GRAFANA_DATASOURCE.read_text(encoding="utf-8")

        self.assertIn("uid: prometheus", source)
        self.assertIn("url: http://prometheus:9090", source)

    def test_spark_prometheus_servlet_is_enabled(self):
        source = SPARK_METRICS.read_text(encoding="utf-8")

        self.assertIn("PrometheusServlet", source)
        self.assertIn("master.sink.prometheusServlet.path=/metrics/master/prometheus", source)
        self.assertIn("*.sink.prometheusServlet.path=/metrics/prometheus", source)


if __name__ == "__main__":
    unittest.main()
