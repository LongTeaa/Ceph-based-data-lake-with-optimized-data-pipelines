.PHONY: help config-check env-check lint test dag-check health up down airflow-up airflow-down airflow-logs airflow-dag-list spark-up spark-down spark-logs spark-submit-silver spark-submit-gold trino-up trino-down trino-logs trino-setup trino-smoke trino-cli monitoring-up monitoring-down monitoring-logs init-buckets storage-smoke generate-test-data generate-tabular-data generate-binary-data prepare-nyc-taxi download-nyc-taxi-scale ingest transform transform-silver transform-gold publish query-smoke benchmark-storage benchmark-query benchmark-query-layout benchmark-query-compaction benchmark-query-format benchmark-trino e2e-smoke

REQUIRED_ENV := S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET
COMPOSE := docker compose --env-file .env -f docker/compose.yml
SOURCE ?= data/source/nyc-taxi
FILE ?= yellow_tripdata_2025-01.parquet
MANIFEST ?= data/source/nyc-taxi/manifests/yellow_tripdata_2025-01.manifest.json
NYC_TAXI_SCALE_MONTHS ?=
NYC_TAXI_SCALE_LIMIT_FILES ?= 0
NYC_TAXI_SCALE_OUTPUT_DIR ?= data/source/nyc-taxi/scale
NYC_TAXI_SCALE_MANIFEST ?= data/source/nyc-taxi/manifests/yellow_tripdata_2023-01_2025-06_30files.manifest.json
OUTPUT_DIR ?= results
TRANSFORM_MODE ?= overwrite
QUERY_BENCHMARK_ITERATIONS ?= 3
QUERY_BENCHMARK_WARMUP ?= 1
QUERY_BENCHMARK_OUTPUT_DIR ?= benchmark/results
BENCHMARK_RUN_ID ?= local-baseline
QUERY_LAYOUT_BENCHMARK_ITERATIONS ?= 3
QUERY_LAYOUT_BENCHMARK_WARMUP ?= 1
QUERY_LAYOUT_BENCHMARK_OUTPUT_DIR ?= benchmark/results
QUERY_LAYOUT_BENCHMARK_COALESCE ?= 0
QUERY_COMPACTION_BENCHMARK_COALESCE ?= 1
TRINO_BENCHMARK_ITERATIONS ?= 3
TRINO_BENCHMARK_WARMUP ?= 1
TRINO_BENCHMARK_OUTPUT_DIR ?= benchmark/results
STORAGE_BENCHMARK_OUTPUT_DIR ?= benchmark/results
STORAGE_BENCHMARK_BACKEND ?= local-s3
STORAGE_BENCHMARK_OBJECT_SIZES ?= 4KiB,1MiB
STORAGE_BENCHMARK_CONCURRENCY ?= 1,4
STORAGE_BENCHMARK_OPERATIONS ?= put,get,mixed
STORAGE_BENCHMARK_ITERATIONS ?= 10
STORAGE_BENCHMARK_WARMUP ?= 2
ROWS ?= 10000
DAYS ?= 7
SEED ?= 42
SYNTHETIC_OUTPUT_DIR ?= data/source/synthetic
SYNTHETIC_BATCH_ID ?=
BINARY_OBJECT_SIZES ?= 4KiB,1MiB
BINARY_OBJECT_COUNT ?= 2

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
	@echo "  make generate-test-data Generate deterministic synthetic tabular and binary data"
	@echo "  make prepare-nyc-taxi   Create NYC Taxi source manifest"
	@echo "  make download-nyc-taxi-scale Download 30 NYC Taxi files and create a scale manifest"
	@echo "  make ingest             Upload manifest-described files to bronze"
	@echo "  make transform          Transform NYC Taxi bronze to silver and gold"
	@echo "  make transform-silver   Transform NYC Taxi bronze data to silver"
	@echo "  make transform-gold     Aggregate NYC Taxi silver data to gold"
	@echo "  make query-smoke        Run Spark SQL smoke queries on NYC Taxi silver/gold"
	@echo "  make benchmark-query    Benchmark Spark SQL queries on NYC Taxi silver/gold"
	@echo "  make benchmark-query-layout Compare partitioned and non-partitioned silver layouts"
	@echo "  make benchmark-query-compaction Compare small-file and compacted silver layouts"
	@echo "  make benchmark-query-format Compare CSV and Parquet silver layouts"
	@echo "  make benchmark-trino    Benchmark Trino queries on NYC Taxi gold"
	@echo "  make benchmark-storage  Benchmark S3-compatible PUT/GET/mixed workloads"
	@echo "  make dag-check          Validate Airflow DAG source files"

config-check:
	@python infrastructure/scripts/config_check.py $(REQUIRED_ENV)

env-check: config-check

lint:
	@python -c "from pathlib import Path; files=['generator/generate_test_records.py','generator/generate_binary_objects.py','ingestion/download_wikimedia_images.py','ingestion/nyc_taxi_manifest.py','ingestion/download_nyc_taxi_scale.py','ingestion/bronze_upload.py','spark/jobs/nyc_taxi_common.py','spark/jobs/nyc_taxi_bronze_to_silver.py','spark/jobs/nyc_taxi_silver_to_gold.py','spark/jobs/nyc_taxi_query_smoke.py','benchmark/query/spark_sql_benchmark.py','benchmark/query/spark_layout_benchmark.py','benchmark/query/trino_benchmark.py','benchmark/storage/s3_benchmark.py','airflow/dags/nyc_taxi_pipeline.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"

test:
	@python -c "from pathlib import Path; files=['generator/generate_test_records.py','generator/generate_binary_objects.py','ingestion/download_wikimedia_images.py','ingestion/nyc_taxi_manifest.py','ingestion/download_nyc_taxi_scale.py','ingestion/bronze_upload.py','spark/jobs/nyc_taxi_common.py','spark/jobs/nyc_taxi_bronze_to_silver.py','spark/jobs/nyc_taxi_silver_to_gold.py','spark/jobs/nyc_taxi_query_smoke.py','benchmark/query/spark_sql_benchmark.py','benchmark/query/spark_layout_benchmark.py','benchmark/query/trino_benchmark.py','benchmark/storage/s3_benchmark.py','airflow/dags/nyc_taxi_pipeline.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"
	@python -m unittest discover -s tests/unit
	@echo "Available syntax and unit checks passed."

dag-check:
	@python -m unittest tests.unit.test_airflow_dags

health: config-check
	@python infrastructure/buckets/storage_smoke.py --health-only

up:
	@$(COMPOSE) up -d minio

down:
	@$(COMPOSE) down

airflow-up:
	@$(COMPOSE) up -d postgres spark-master spark-worker airflow-init airflow-webserver airflow-scheduler

airflow-down:
	@$(COMPOSE) stop airflow-scheduler airflow-webserver postgres

airflow-logs:
	@$(COMPOSE) logs -f airflow-scheduler airflow-webserver

airflow-dag-list:
	@$(COMPOSE) exec airflow-webserver airflow dags list

spark-up:
	@$(COMPOSE) up -d spark-master spark-worker

spark-down:
	@$(COMPOSE) stop spark-worker spark-master

spark-logs:
	@$(COMPOSE) logs -f spark-master spark-worker

spark-submit-silver:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_bronze_to_silver.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)' --mode '$(TRANSFORM_MODE)'"

spark-submit-gold:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_silver_to_gold.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)' --mode '$(TRANSFORM_MODE)'"

trino-up:
	@$(COMPOSE) up -d trino

trino-down:
	@$(COMPOSE) stop trino

trino-logs:
	@$(COMPOSE) logs -f trino

trino-setup:
	@$(COMPOSE) exec -T trino trino --server localhost:8080 --file /etc/trino/sql/nyc_taxi_gold_setup.sql

trino-smoke: trino-setup
	@$(COMPOSE) exec -T trino trino --server localhost:8080 --file /etc/trino/sql/nyc_taxi_gold_smoke.sql

trino-cli:
	@$(COMPOSE) exec trino trino --server localhost:8080 --catalog lake --schema nyc_taxi

monitoring-up:
	@$(COMPOSE) up -d statsd-exporter spark-master spark-worker prometheus grafana

monitoring-down:
	@$(COMPOSE) stop grafana prometheus statsd-exporter

monitoring-logs:
	@$(COMPOSE) logs -f prometheus grafana statsd-exporter

init-buckets: config-check
	@python infrastructure/buckets/init_buckets.py

storage-smoke: config-check
	@python infrastructure/buckets/storage_smoke.py

generate-test-data: generate-tabular-data generate-binary-data

generate-tabular-data:
	@python generator/generate_test_records.py --rows "$(ROWS)" --days "$(DAYS)" --seed "$(SEED)" --output-dir "$(SYNTHETIC_OUTPUT_DIR)/tabular" $(if $(SYNTHETIC_BATCH_ID),--batch-id "$(SYNTHETIC_BATCH_ID)")

generate-binary-data:
	@python generator/generate_binary_objects.py --object-sizes "$(BINARY_OBJECT_SIZES)" --count "$(BINARY_OBJECT_COUNT)" --seed "$(SEED)" --output-dir "$(SYNTHETIC_OUTPUT_DIR)/binary" $(if $(SYNTHETIC_BATCH_ID),--batch-id "$(SYNTHETIC_BATCH_ID)")

prepare-nyc-taxi:
	@python ingestion/nyc_taxi_manifest.py --source-dir "$(SOURCE)" --file-name "$(FILE)" --manifest-path "$(MANIFEST)"

download-nyc-taxi-scale: config-check
	@python ingestion/download_nyc_taxi_scale.py --output-dir "$(NYC_TAXI_SCALE_OUTPUT_DIR)" --manifest-path "$(NYC_TAXI_SCALE_MANIFEST)" $(if $(NYC_TAXI_SCALE_MONTHS),--months "$(NYC_TAXI_SCALE_MONTHS)") $(if $(filter-out 0,$(NYC_TAXI_SCALE_LIMIT_FILES)),--limit-files "$(NYC_TAXI_SCALE_LIMIT_FILES)")

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
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/jobs/nyc_taxi_query_smoke.py --manifest-path '$(MANIFEST)' --output-dir '$(OUTPUT_DIR)'"

benchmark-storage:
	@python benchmark/storage/s3_benchmark.py --output-dir "$(STORAGE_BENCHMARK_OUTPUT_DIR)" --run-id "$(BENCHMARK_RUN_ID)" --backend "$(STORAGE_BENCHMARK_BACKEND)" --object-sizes "$(STORAGE_BENCHMARK_OBJECT_SIZES)" --concurrency "$(STORAGE_BENCHMARK_CONCURRENCY)" --operations "$(STORAGE_BENCHMARK_OPERATIONS)" --iterations "$(STORAGE_BENCHMARK_ITERATIONS)" --warmup "$(STORAGE_BENCHMARK_WARMUP)"

benchmark-query:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 benchmark/query/spark_sql_benchmark.py --manifest-path '$(MANIFEST)' --output-dir '$(QUERY_BENCHMARK_OUTPUT_DIR)' --run-id '$(BENCHMARK_RUN_ID)' --iterations '$(QUERY_BENCHMARK_ITERATIONS)' --warmup '$(QUERY_BENCHMARK_WARMUP)'"

benchmark-query-layout:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 benchmark/query/spark_layout_benchmark.py --manifest-path '$(MANIFEST)' --output-dir '$(QUERY_LAYOUT_BENCHMARK_OUTPUT_DIR)' --run-id '$(BENCHMARK_RUN_ID)' --iterations '$(QUERY_LAYOUT_BENCHMARK_ITERATIONS)' --warmup '$(QUERY_LAYOUT_BENCHMARK_WARMUP)' --comparison partition --coalesce '$(QUERY_LAYOUT_BENCHMARK_COALESCE)'"

benchmark-query-compaction:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 benchmark/query/spark_layout_benchmark.py --manifest-path '$(MANIFEST)' --output-dir '$(QUERY_LAYOUT_BENCHMARK_OUTPUT_DIR)' --run-id '$(BENCHMARK_RUN_ID)' --iterations '$(QUERY_LAYOUT_BENCHMARK_ITERATIONS)' --warmup '$(QUERY_LAYOUT_BENCHMARK_WARMUP)' --comparison compaction --coalesce '$(QUERY_COMPACTION_BENCHMARK_COALESCE)'"

benchmark-query-format:
	@$(COMPOSE) run --rm spark-submit "mkdir -p /tmp/spark-ivy/cache /tmp/spark-ivy/jars /tmp/spark-local && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/spark-ivy --packages org.apache.hadoop:hadoop-aws:3.3.4 benchmark/query/spark_layout_benchmark.py --manifest-path '$(MANIFEST)' --output-dir '$(QUERY_LAYOUT_BENCHMARK_OUTPUT_DIR)' --run-id '$(BENCHMARK_RUN_ID)' --iterations '$(QUERY_LAYOUT_BENCHMARK_ITERATIONS)' --warmup '$(QUERY_LAYOUT_BENCHMARK_WARMUP)' --comparison format --coalesce '$(QUERY_LAYOUT_BENCHMARK_COALESCE)'"

benchmark-trino:
	@python benchmark/query/trino_benchmark.py --output-dir "$(TRINO_BENCHMARK_OUTPUT_DIR)" --run-id "$(BENCHMARK_RUN_ID)" --iterations "$(TRINO_BENCHMARK_ITERATIONS)" --warmup "$(TRINO_BENCHMARK_WARMUP)"

e2e-smoke: config-check init-buckets storage-smoke prepare-nyc-taxi ingest spark-submit-silver spark-submit-gold query-smoke
	@echo "e2e-smoke ok"
