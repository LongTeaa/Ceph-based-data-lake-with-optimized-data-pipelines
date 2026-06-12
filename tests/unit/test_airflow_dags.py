from pathlib import Path
import unittest


DAG_PATH = Path("airflow/dags/nyc_taxi_pipeline.py")


class AirflowDagSourceTests(unittest.TestCase):
    def test_nyc_taxi_dag_has_expected_tasks_and_commands(self):
        source = DAG_PATH.read_text(encoding="utf-8")

        compile(source, str(DAG_PATH), "exec")

        for task_id in [
            "check_config",
            "check_storage",
            "prepare_manifest",
            "upload_bronze",
            "bronze_to_silver",
            "silver_to_gold",
        ]:
            self.assertIn(f'task_id="{task_id}"', source)

        for command_fragment in [
            "infrastructure/scripts/config_check.py",
            "infrastructure/buckets/storage_smoke.py --health-only",
            "ingestion/nyc_taxi_manifest.py",
            "ingestion/bronze_upload.py",
            "spark-submit",
            "--master",
            "spark://spark-master:7077",
            "spark/jobs/nyc_taxi_bronze_to_silver.py",
            "spark/jobs/nyc_taxi_silver_to_gold.py",
        ]:
            self.assertIn(command_fragment, source)

    def test_nyc_taxi_dag_documents_dependency_order(self):
        source = DAG_PATH.read_text(encoding="utf-8")

        self.assertIn(
            "check_config >> check_storage >> prepare_manifest >> upload_bronze",
            source,
        )
        self.assertIn(
            "upload_bronze >> bronze_to_silver >> silver_to_gold",
            source,
        )


if __name__ == "__main__":
    unittest.main()
