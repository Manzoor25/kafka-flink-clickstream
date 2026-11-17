from __future__ import annotations

import os

import clickhouse_connect
import pytest
import requests
from confluent_kafka.admin import AdminClient


def _stack_is_live() -> bool:
    if os.getenv("REQUIRE_LIVE_STACK") == "1":
        return True
    try:
        r = requests.get("http://localhost:8081/overview", timeout=2.5)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _stack_is_live(), reason="Live Docker Compose stack is not running")


def test_kafka_4_kraft_cluster_and_topics() -> None:
    bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094")
    admin = AdminClient({"bootstrap.servers": bs})
    md = admin.list_topics(timeout=10)
    assert len(md.brokers) == 3
    expected_partitions = {
        "clicks.raw.v1": 12,
        "products.catalogue.v1": 12,
        "clicks.enriched.v1": 6,
        "sessions.summary.v1": 6,
        "sessions.sql.v1": 6,
        "metrics.minute.tumbling.v1": 3,
        "metrics.minute.hopping.v1": 3,
        "metrics.funnel.v1": 3,
        "metrics.top_products.v1": 3,
        "clicks.late.v1": 3,
    }
    for topic, count in expected_partitions.items():
        assert topic in md.topics, f"Missing Kafka topic {topic}"
        assert len(md.topics[topic].partitions) == count


def test_apicurio_schema_registry_subjects() -> None:
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085/apis/ccompat/v7")
    resp = requests.get(f"{sr_url}/subjects", timeout=10)
    resp.raise_for_status()
    subjects = set(resp.json())
    assert "clicks.raw.v1-value" in subjects
    assert "products.catalogue.v1-value" in subjects


def test_flink_jobs_running_with_completed_minio_checkpoints() -> None:
    flink_url = os.getenv("FLINK_REST_URL", "http://localhost:8081")
    ov = requests.get(f"{flink_url}/jobs/overview", timeout=10).json()["jobs"]
    running = [j for j in ov if j["state"] == "RUNNING"]
    assert len(running) >= 2, f"Expected both SQL and DataStream Flink jobs RUNNING, got: {ov}"
    for job in running:
        ck = requests.get(f"{flink_url}/jobs/{job['jid']}/checkpoints", timeout=10).json()
        completed = int(ck.get("counts", {}).get("completed", 0))
        assert completed >= 1, f"Job {job['name']} has no completed checkpoints yet: {ck}"


def test_clickhouse_serving_tables_and_grafana_query_api() -> None:
    ch = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "clickstream"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"),
    )
    required_non_empty = [
        "clicks_enriched",
        "minute_metrics_tumbling",
        "minute_metrics_hopping",
        "user_sessions",
        "user_sessions_sql",
        "funnel_window_metrics",
        "top_products_window",
    ]
    for tbl in required_non_empty:
        cnt = int(ch.query(f"SELECT count() FROM analytics.{tbl} FINAL").first_row[0])
        assert cnt > 0, f"Expected ClickHouse table analytics.{tbl} to contain rows"

    gf_url = os.getenv("GRAFANA_URL", "http://localhost:3000")
    health = requests.get(f"{gf_url}/api/health", timeout=10)
    health.raise_for_status()

    ds_resp = requests.post(
        f"{gf_url}/api/ds/query",
        auth=("admin", "admin"),
        json={
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-rt"},
                    "queryType": "sql",
                    "format": 1,
                    "rawSql": "SELECT count() AS total FROM analytics.clicks_enriched FINAL",
                }
            ],
            "from": "now-15m",
            "to": "now",
        },
        timeout=10,
    )
    ds_resp.raise_for_status()
    values = ds_resp.json()["results"]["A"]["frames"][0]["data"]["values"]
    assert int(values[0][0]) > 0
