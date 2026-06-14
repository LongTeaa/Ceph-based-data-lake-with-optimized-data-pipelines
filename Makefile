.PHONY: help config-check env-check lint test dag-check health up down airflow-up airflow-down airflow-logs airflow-dag-list spark-up spark-down spark-logs spark-submit-silver spark-submit-gold trino-up trino-down trino-logs trino-setup trino-smoke trino-cli monitoring-up monitoring-down monitoring-logs init-buckets storage-smoke generate-test-data prepare-nyc-taxi ingest transform transform-silver transform-gold publish query-smoke benchmark-storage benchmark-query benchmark-trino e2e-smoke

REQUIRED_ENV := S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET
SOURCE ?= data/source/nyc-taxi
FILE ?= yellow_tripdata_2025-01.parquet
MANIFEST ?= data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
OUTPUT_DIR ?= results
TRANSFORM_MODE ?= overwrite
QUERY_BENCHMARK_ITERATIONS ?= 3
QUERY_BENCHMARK_WARMUP ?= 1
QUERY_BENCHMARK_OUTPUT_DIR ?= benchmark/results
BENCHMARK_RUN_ID ?= local-baseline
TRINO_BENCHMARK_ITERATIONS ?= 3
TRINO_BENCHMARK_WARMUP ?= 1
TRINO_BENCHMARK_OUTPUT_DIR ?= benchmark/results

help:
	@echo "Ceph-based Data Lake commands"
	@echo "  make config-check       Validate required environment variables"
	@echo "  make up                 Start local S3-compatible storage"
	@echo "  make airflow-up         Start local Airflow services"
	@echo "  make airflow-down       Stop local Airflow services"
	@echo "  make airflow-dag-list   List Airflow DAGs in the webserver container"
	@echo "  make spark-up           Start local Spark standalone services"
	@echo "  make spark-submit-silver Submit bronze-to-silver job to Spark standalone"
	@echo "  make spark-submit-gold  Submit silver-to-gold job to Spark standalone"
	@echo "  make trino-up           Start local Trino query service"
	@echo "  make trino-smoke        Register and query NYC Taxi gold tables with Trino"
	@echo "  make trino-cli          Open an interactive Trino SQL CLI"
	@echo "  make monitoring-up      Start Prometheus, Grafana, and local metric targets"
	@echo "  make monitoring-down    Stop Prometheus, Grafana, and statsd exporter"
	@echo "  make init-buckets       Create Data Lake buckets"
	@echo "  make storage-smoke      Upload/download/checksum smoke test"
	@echo "  make health             Check S3 endpoint reachability"
	@echo "  make test               Run available tests"
	@echo "  make prepare-nyc-taxi   Create NYC Taxi source manifest"
	@echo "  make ingest             Upload manifest-described files to bronze"
	@echo "  make transform          Transform NYC Taxi bronze to silver and gold"
	@echo "  make transform-silver   Transform NYC Taxi bronze data to silver"
	@echo "  make transform-gold     Aggregate NYC Taxi silver data to gold"
	@echo "  make query-smoke        Run Spark SQL smoke queries on NYC Taxi silver/gold"
	@echo "  make benchmark-query    Benchmark Spark SQL queries on NYC Taxi silver/gold"
	@echo "  make benchmark-trino    Benchmark Trino queries on NYC Taxi gold"
	@echo "  make dag-check          Validate Airflow DAG source files"

config-check:
	@python infrastructure/scripts/config_check.py $(REQUIRED_ENV)

env-check: config-check

lint:
	@python -c "from pathlib import Path; files=['ingestion/download_wikimedia_images.py','ingestion/nyc_taxi_manifest.py','ingestion/bronze_upload.py','spark/jobs/nyc_taxi_common.py','spark/jobs/nyc_taxi_bronze_to_silver.py','spark/jobs/nyc_taxi_silver_to_gold.py','spark/jobs/nyc_taxi_query_smoke.py','benchmark/query/spark_sql_benchmark.py','benchmark/query/trino_benchmark.py','airflow/dags/nyc_taxi_pipeline.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"

test:
	@python -c "from pathlib import Path; files=['ingestion/download_wikimedia_images.py','ingestion/nyc_taxi_manifest.py','ingestion/bronze_upload.py','spark/jobs/nyc_taxi_common.py','spark/jobs/nyc_taxi_bronze_to_silver.py','spark/jobs/nyc_taxi_silver_to_gold.py','spark/jobs/nyc_taxi_query_smoke.py','benchmark/query/spark_sql_benchmark.py','benchmark/query/trino_benchmark.py','airflow/dags/nyc_taxi_pipeline.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"
	@python -m unittest discover -s tests/unit
	@echo "Available syntax and unit checks passed."

dag-check:
	@python -m unittest tests.unit.test_airflow_dags

health: config-check
	@python infrastructure/buckets/storage_smoke.py --health-only

up:
	@docker compose -f docker/compose.yml up -d minio

down:
	@docker compose -f docker/compose.yml down

airflow-up:
	@docker compose -f docker/compose.yml up -d minio postgres spark-master spark-worker airflow-init airflow-webserver airflow-scheduler

airflow-down:
	@docker compose -f docker/compose.yml stop airflow-scheduler airflow-webserver postgres

airflow-logs:
	@docker compose -f docker/compose.yml logs -f airflow-scheduler airflow-webserver

airflow-dag-list:
	@docker compose -f docker/compose.yml exec airflow-webserver airflow dags list

spark-up:
	@docker compose -f docker/compose.yml up -d minio spark-master spark-worker

spark-down:
	@docker compose -f docker/compose.yml stop spark-worker spark-master

spark-logs:
	@docker compose -f docker/compose.yml logs -f spark-master spark-worker

spark-submit-silver:
	@docker compose -f docker/compose.yml run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_bronze_to_silver.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)' --mode '$(TRANSFORM_MODE)'"

spark-submit-gold:
	@docker compose -f docker/compose.yml run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_silver_to_gold.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)' --mode '$(TRANSFORM_MODE)'"

trino-up:
	@docker compose -f docker/compose.yml up -d minio trino

trino-down:
	@docker compose -f docker/compose.yml stop trino

trino-logs:
	@docker compose -f docker/compose.yml logs -f trino

trino-setup:
	@docker compose -f docker/compose.yml exec -T trino trino --server localhost:8080 --file /etc/trino/sql/nyc_taxi_gold_setup.sql

trino-smoke: trino-setup
	@docker compose -f docker/compose.yml exec -T trino trino --server localhost:8080 --file /etc/trino/sql/nyc_taxi_gold_smoke.sql

trino-cli:
	@docker compose -f docker/compose.yml exec trino trino --server localhost:8080 --catalog lake --schema nyc_taxi

monitoring-up:
	@docker compose -f docker/compose.yml up -d minio statsd-exporter spark-master spark-worker prometheus grafana

monitoring-down:
	@docker compose -f docker/compose.yml stop grafana prometheus statsd-exporter

monitoring-logs:
	@docker compose -f docker/compose.yml logs -f prometheus grafana statsd-exporter

init-buckets: config-check
	@python infrastructure/buckets/init_buckets.py

storage-smoke: config-check
	@python infrastructure/buckets/storage_smoke.py

generate-test-data:
	@echo "Not implemented in Phase 0. Planned for Phase 2."

prepare-nyc-taxi:
	@python ingestion/nyc_taxi_manifest.py --source-dir "$(SOURCE)" --file-name "$(FILE)" --manifest-path "$(MANIFEST)"

ingest: config-check
	@python ingestion/bronze_upload.py --manifest-path "$(MANIFEST)"

transform: transform-silver transform-gold

transform-silver: config-check
	@python spark/jobs/nyc_taxi_bronze_to_silver.py --manifest-path "$(MANIFEST)" --output-dir "$(OUTPUT_DIR)" --mode "$(TRANSFORM_MODE)"

transform-gold: config-check
	@python spark/jobs/nyc_taxi_silver_to_gold.py --manifest-path "$(MANIFEST)" --output-dir "$(OUTPUT_DIR)" --mode "$(TRANSFORM_MODE)"

publish:
	@echo "Not implemented in Phase 0. Planned for Phase 3/4."

query-smoke:
	@docker compose -f docker/compose.yml run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_query_smoke.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)'"

benchmark-storage:
	@echo "Not implemented in Phase 0. Planned for Phase 7."

benchmark-query:
	@docker compose -f docker/compose.yml run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 benchmark/query/spark_sql_benchmark.py --manifest-path '$(MANIFEST)' --output-dir '$(QUERY_BENCHMARK_OUTPUT_DIR)' --run-id '$(BENCHMARK_RUN_ID)' --iterations '$(QUERY_BENCHMARK_ITERATIONS)' --warmup '$(QUERY_BENCHMARK_WARMUP)'"

benchmark-trino:
	@python benchmark/query/trino_benchmark.py --output-dir "$(TRINO_BENCHMARK_OUTPUT_DIR)" --run-id "$(BENCHMARK_RUN_ID)" --iterations "$(TRINO_BENCHMARK_ITERATIONS)" --warmup "$(TRINO_BENCHMARK_WARMUP)"

e2e-smoke:
	@echo "Not implemented in Phase 0. Planned after storage and Spark are available."
