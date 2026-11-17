from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_flink_sql_pipeline_contracts_and_unique_txn_prefixes() -> None:
    sql_text = (ROOT / "flink-sql" / "01_pipeline.sql").read_text(encoding="utf-8")

    # Verify all required streaming SQL primitives from Build #2-#9
    assert "WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND" in sql_text
    assert "ROW_NUMBER() OVER (" in sql_text
    assert "PARTITION BY event_id" in sql_text
    assert "ORDER BY proc_time ASC" in sql_text
    assert "FOR SYSTEM_TIME AS OF c.event_ts" in sql_text
    assert "TUMBLE(TABLE clicks_enriched, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)" in sql_text
    assert "HOP(TABLE clicks_enriched, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)" in sql_text
    assert (
        "SESSION(TABLE clicks_enriched PARTITION BY(user_id), DESCRIPTOR(event_ts), INTERVAL '30' SECOND)" in sql_text
    )
    assert "MATCH_RECOGNIZE" in sql_text
    assert "WHERE rnk <= 10" in sql_text
    assert "'properties.isolation.level' = 'read_committed'" in sql_text

    # Every Kafka sink in STATEMENT SET must use exactly-once and a distinct transactional-id-prefix
    exactly_once_matches = re.findall(r"'sink\.delivery-guarantee'\s*=\s*'exactly-once'", sql_text)
    assert len(exactly_once_matches) == 6

    txn_prefixes = re.findall(r"'sink\.transactional-id-prefix'\s*=\s*'([^']+)'", sql_text)
    assert len(txn_prefixes) == 6
    assert len(set(txn_prefixes)) == 6, f"Duplicate sink transactional prefixes detected: {txn_prefixes}"


def test_clickhouse_kafka_tables_enforce_read_committed() -> None:
    kafka_xml = (ROOT / "docker" / "clickhouse" / "config.d" / "kafka.xml").read_text(encoding="utf-8")
    assert "<isolation_level>read_committed</isolation_level>" in kafka_xml

    ddl = (ROOT / "docker" / "clickhouse" / "kafka_ingestion.sql").read_text(encoding="utf-8")
    kafka_engines = re.findall(r"ENGINE\s*=\s*Kafka", ddl)
    assert len(kafka_engines) == 8


def test_provisioned_grafana_dashboard_schema() -> None:
    dash_path = ROOT / "docker" / "grafana" / "dashboards" / "clickstream-realtime.json"
    dash = json.loads(dash_path.read_text(encoding="utf-8"))
    assert dash["uid"] == "clickstream-rt"
    assert dash["refresh"] == "2s"
    panels = [p for p in dash["panels"] if p.get("type") != "row"]
    assert len(panels) >= 12
    for p in panels:
        assert p["datasource"]["uid"] == "clickhouse-rt"
        assert len(p.get("targets", [])) >= 1
