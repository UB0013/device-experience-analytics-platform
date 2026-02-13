.PHONY: help up down status health-check produce stream-start batch-run api-start test load-test clean

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ───────────────────────────────────────────────────

up: ## Start all infrastructure services
	docker-compose up -d
	@echo "Waiting for services to start..."
	@sleep 15
	@$(MAKE) health-check

down: ## Stop all services
	docker-compose down

status: ## Show service status
	docker-compose ps

health-check: ## Check health of all services
	python -m src.monitoring.health_check -v

logs: ## Tail all service logs
	docker-compose logs -f --tail=50

# ── Data Pipeline ────────────────────────────────────────────────────

produce: ## Start telemetry event producer (default 1000 events/sec)
	python -m src.ingestion.producer --rate 1000

produce-low: ## Start producer at low rate (100 events/sec)
	python -m src.ingestion.producer --rate 100

produce-high: ## Start producer at high rate (5000 events/sec)
	python -m src.ingestion.producer --rate 5000

produce-stress: ## Start producer at stress-test rate (10000 events/sec)
	python -m src.ingestion.producer --rate 10000

# ── Stream Processing ────────────────────────────────────────────────

stream-start: ## Start Spark Streaming jobs
	spark-submit \
		--master spark://localhost:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		--conf spark.sql.shuffle.partitions=12 \
		src/streaming/stream_processor.py

# ── Batch Processing ─────────────────────────────────────────────────

batch-run: ## Run Spark batch analytics
	spark-submit \
		--master spark://localhost:7077 \
		--conf spark.sql.shuffle.partitions=12 \
		src/batch/batch_processor.py

hive-init: ## Initialize Hive tables
	beeline -u jdbc:hive2://localhost:10000 -f src/batch/hive_tables.sql

# ── API ──────────────────────────────────────────────────────────────

api-start: ## Start FastAPI serving layer
	python -m src.serving.api

api-dev: ## Start API in development mode with auto-reload
	uvicorn src.serving.api:app --host 0.0.0.0 --port 8000 --reload

# ── Testing ──────────────────────────────────────────────────────────

test: ## Run unit tests
	python -m pytest tests/unit -v

test-integration: ## Run integration tests
	python -m pytest tests/integration -v

load-test: ## Run load/scalability tests
	python tests/load/load_test.py

# ── Storage ──────────────────────────────────────────────────────────

hdfs-init: ## Initialize HDFS data lake structure
	python -c "from src.storage.hdfs_manager import HDFSManager; HDFSManager().init_data_lake()"

hdfs-status: ## Show HDFS storage stats
	hdfs dfs -du -s -h /data/*

# ── Utilities ────────────────────────────────────────────────────────

install: ## Install Python dependencies
	pip install -r requirements.txt

clean: ## Remove local data and cache files
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
