SHELL := /bin/bash
PYTHON ?= python3
export PYTHONPATH := $(CURDIR)

.PHONY: help up down demo test test-unit test-java test-smoke benchmark freshness ops demo-kill-tm demo-spike demo-savepoint demo-reprocess clean

help:
	@echo "Real-Time Clickstream Analytics Platform (Kafka 4.4 KRaft + Flink 2.2 + ClickHouse 25.8 + Grafana 13.2)"
	@echo ""
	@echo "Core Lifecycle:"
	@echo "  make up              Build images, boot 9-container cluster, init topics/schemas/DDL, and deploy Flink jobs"
	@echo "  make demo            Stream realistic clickstream + price updates, measure freshness, and verify batch recount"
	@echo "  make test            Run Python unit tests, Flink MiniCluster SQL/DataStream fixture tests, and live smoke checks"
	@echo "  make down            Stop all containers and remove volumes"
	@echo ""
	@echo "Benchmarks & Operations:"
	@echo "  make benchmark       Run Kafka 4.4 codec (none/lz4/zstd) & batching (linger.ms/batch.size) benchmark"
	@echo "  make freshness       Measure end-to-end event creation -> Grafana /api/ds/query freshness (p50 & p95)"
	@echo "  make ops             Snapshot Kafka consumer lag and Flink checkpoint/backpressure telemetry"
	@echo ""
	@echo "Failure & Recovery Demos:"
	@echo "  make demo-kill-tm    Kill a Flink TaskManager under load and prove exact batch recount recovery"
	@echo "  make demo-spike      Push a 10x traffic spike and record backpressure, lag buildup, and zero-lag recovery"
	@echo "  make demo-savepoint  Stop DataStream job with S3 savepoint and deploy upgraded v2 query without state loss"
	@echo "  make demo-reprocess  Truncate serving store and rebuild all 6 tables from Kafka earliest-offset (Kappa)"

up:
	docker compose up -d --build --wait
	$(PYTHON) scripts/init_infra.py
	$(PYTHON) scripts/deploy_jobs.py
	@echo ""
	@echo "Platform ready:"
	@echo "  Grafana Dashboard : http://localhost:3000/d/clickstream-rt/real-time-e-commerce-clickstream-analytics (admin / admin_local)"
	@echo "  Flink Web UI      : http://localhost:8081"
	@echo "  Schema Registry   : http://localhost:8085/apis/ccompat/v7/subjects"
	@echo "  ClickHouse HTTP   : http://localhost:8123"
	@echo "  MinIO Console     : http://localhost:9011"

demo:
	$(PYTHON) -m generator.clickstream_generator \
		--rate 280 \
		--duration-sec 68 \
		--duplicate-rate 0.03 \
		--ooo-rate 0.04 \
		--late-rate 0.015 \
		--flush-watermark \
		--ledger-path /tmp/demo_ledger.jsonl
	$(PYTHON) scripts/verify_isolation_level.py --json-out benchmarks/results/isolation_level.json
	$(PYTHON) scripts/verify_recount.py \
		--ledger-path /tmp/demo_ledger.jsonl \
		--exclude-synthetic-late \
		--json-out benchmarks/results/demo_recount.json
	$(PYTHON) scripts/measure_freshness.py --probes 25
	$(PYTHON) scripts/monitor_ops.py --json-out benchmarks/results/ops_snapshot.json

benchmark:
	$(PYTHON) -m benchmarks.benchmark_producer --messages 40000

freshness:
	$(PYTHON) scripts/measure_freshness.py --probes 25

ops:
	$(PYTHON) scripts/monitor_ops.py --samples 3 --interval-sec 2

demo-kill-tm:
	bash scripts/demos/demo_kill_taskmanager.sh

demo-spike:
	bash scripts/demos/demo_traffic_spike.sh

demo-savepoint:
	bash scripts/demos/demo_savepoint_upgrade.sh

demo-reprocess:
	bash scripts/demos/demo_reprocess_earliest.sh

test-unit:
	$(PYTHON) -m pytest tests/test_generator_and_recount.py tests/test_sql_and_repo_contracts.py

test-java:
	@if command -v mvn >/dev/null 2>&1; then \
		mvn -B -f flink-jobs/pom.xml test; \
	else \
		docker run --rm -v "$$(pwd)":/ws -w /ws/flink-jobs maven:3.9.9-eclipse-temurin-17 mvn -B test; \
	fi

test-smoke:
	$(PYTHON) -m pytest tests/test_live_smoke.py

test: test-unit test-java
	@if curl -sf http://localhost:8081/overview >/dev/null 2>&1; then \
		echo "[test] Live stack detected on localhost:8081 — running end-to-end smoke & recount checks..."; \
		$(PYTHON) -m pytest tests/test_live_smoke.py; \
	else \
		echo "[test] Live Docker stack not running (run 'make up && make demo' first to include live smoke checks)."; \
	fi

down:
	docker compose down -v --remove-orphans
