.PHONY: help config-check env-check lint test health up down init-buckets storage-smoke generate-test-data prepare-nyc-taxi ingest transform publish query-smoke benchmark-storage benchmark-query e2e-smoke

REQUIRED_ENV := S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET

help:
	@echo "Ceph-based Data Lake commands"
	@echo "  make config-check       Validate required environment variables"
	@echo "  make up                 Start local S3-compatible storage"
	@echo "  make init-buckets       Create Data Lake buckets"
	@echo "  make storage-smoke      Upload/download/checksum smoke test"
	@echo "  make health             Check S3 endpoint reachability"
	@echo "  make test               Run available tests"
	@echo "  make prepare-nyc-taxi   Placeholder for Phase 2"
	@echo "  make ingest             Placeholder for later phases"

config-check:
	@python infrastructure/scripts/config_check.py $(REQUIRED_ENV)

env-check: config-check

lint:
	@python -c "from pathlib import Path; files=['ingestion/download_wikimedia_images.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"

test:
	@python -c "from pathlib import Path; files=['ingestion/download_wikimedia_images.py','infrastructure/scripts/config_check.py','infrastructure/buckets/s3_common.py','infrastructure/buckets/init_buckets.py','infrastructure/buckets/storage_smoke.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('syntax ok')"
	@python -m unittest discover -s tests/unit
	@echo "Phase 1 syntax checks passed."

health: config-check
	@python infrastructure/buckets/storage_smoke.py --health-only

up:
	@docker compose -f docker/compose.yml up -d minio

down:
	@docker compose -f docker/compose.yml down

init-buckets: config-check
	@python infrastructure/buckets/init_buckets.py

storage-smoke: config-check
	@python infrastructure/buckets/storage_smoke.py

generate-test-data:
	@echo "Not implemented in Phase 0. Planned for Phase 2."

prepare-nyc-taxi:
	@echo "Not implemented in Phase 0. Planned for Phase 2."

ingest:
	@echo "Not implemented in Phase 0. Planned for Phase 2/4."

transform:
	@echo "Not implemented in Phase 0. Planned for Phase 3."

publish:
	@echo "Not implemented in Phase 0. Planned for Phase 3/4."

query-smoke:
	@echo "Not implemented in Phase 0. Planned for Phase 5."

benchmark-storage:
	@echo "Not implemented in Phase 0. Planned for Phase 7."

benchmark-query:
	@echo "Not implemented in Phase 0. Planned for Phase 8."

e2e-smoke:
	@echo "Not implemented in Phase 0. Planned after storage and Spark are available."
