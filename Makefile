.PHONY: help config-check env-check lint test health up down init-buckets generate-test-data prepare-nyc-taxi ingest transform publish query-smoke benchmark-storage benchmark-query e2e-smoke

REQUIRED_ENV := S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_REGION BRONZE_BUCKET SILVER_BUCKET GOLD_BUCKET SYSTEM_BUCKET

help:
	@echo "Ceph-based Data Lake commands"
	@echo "  make config-check       Validate required environment variables"
	@echo "  make health             Placeholder health check for Phase 0"
	@echo "  make test               Run available tests"
	@echo "  make prepare-nyc-taxi   Placeholder for Phase 2"
	@echo "  make ingest             Placeholder for later phases"

config-check:
	@python infrastructure/scripts/config_check.py $(REQUIRED_ENV)

env-check: config-check

lint:
	@python -c "from pathlib import Path; compile(Path('ingestion/download_wikimedia_images.py').read_text(encoding='utf-8'), 'ingestion/download_wikimedia_images.py', 'exec'); print('syntax ok')"

test:
	@python -c "from pathlib import Path; compile(Path('ingestion/download_wikimedia_images.py').read_text(encoding='utf-8'), 'ingestion/download_wikimedia_images.py', 'exec'); print('syntax ok')"
	@echo "No test suite yet; Phase 0 syntax checks passed."

health:
	@echo "Phase 0 only: no services are running yet."

up:
	@echo "Not implemented in Phase 0. Service compose files will be added in later phases."

down:
	@echo "Not implemented in Phase 0."

init-buckets:
	@echo "Not implemented in Phase 0. Planned for Phase 1."

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
