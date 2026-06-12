from pathlib import Path
import unittest


COMPOSE_PATH = Path("docker/compose.yml")


class DockerComposeSourceTests(unittest.TestCase):
    def test_airflow_services_are_declared(self):
        source = COMPOSE_PATH.read_text(encoding="utf-8")

        for service in [
            "postgres:",
            "airflow-init:",
            "airflow-webserver:",
            "airflow-scheduler:",
            "spark-master:",
            "spark-worker:",
            "spark-submit:",
        ]:
            self.assertIn(service, source)

    def test_airflow_services_mount_project_and_dags(self):
        source = COMPOSE_PATH.read_text(encoding="utf-8")

        self.assertIn("- ..:/opt/airflow/project", source)
        self.assertIn("- ../airflow/dags:/opt/airflow/dags", source)
        self.assertIn("DATA_LAKE_PROJECT_ROOT: /opt/airflow/project", source)
        self.assertIn("S3_ENDPOINT: http://minio:9000", source)

    def test_airflow_uses_project_requirements(self):
        source = COMPOSE_PATH.read_text(encoding="utf-8")

        self.assertIn(
            '_PIP_ADDITIONAL_REQUIREMENTS: "-r /opt/airflow/project/docker/airflow/requirements.txt"',
            source,
        )

    def test_spark_standalone_services_are_configured_for_minio(self):
        source = COMPOSE_PATH.read_text(encoding="utf-8")

        self.assertIn("SPARK_MODE: master", source)
        self.assertIn("SPARK_MODE: worker", source)
        self.assertIn("SPARK_MASTER_URL: spark://spark-master:7077", source)
        self.assertIn("S3_ENDPOINT: http://minio:9000", source)
        self.assertIn("- ..:/opt/spark/project", source)
        self.assertIn("dockerfile: docker/spark/Dockerfile", source)


if __name__ == "__main__":
    unittest.main()
