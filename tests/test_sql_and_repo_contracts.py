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


def test_portfolio_deliverables_and_measured_artifacts_complete() -> None:
    required_files = [
        "LICENSE",
        "README.md",
        "docs/design.md",
        "docs/interview-questions.md",
        "docs/linkedin-article.md",
        "docs/linkedin-post.md",
        "docs/assets/dashboard.png",
        "benchmarks/results/producer_benchmark.json",
        "benchmarks/results/freshness_report.json",
        "benchmarks/results/demo_recount.json",
        "benchmarks/results/isolation_level.json",
        "benchmarks/results/demo_kill_taskmanager.json",
        "benchmarks/results/demo_traffic_spike.json",
        "benchmarks/results/demo_savepoint_upgrade.json",
        "benchmarks/results/demo_reprocess_earliest.json",
        "benchmarks/results/demo_reprocess_recount.json",
    ]
    for rel in required_files:
        fp = ROOT / rel
        assert fp.exists(), f"Missing required deliverable: {rel}"

    assert (ROOT / "docs" / "assets" / "dashboard.png").stat().st_size > 50_000
    assert len(list((ROOT / "docs" / "adr").glob("000*.md"))) >= 7

    # Ensure zero unfilled template placeholders remain across docs
    for md_path in [ROOT / "README.md", *(ROOT / "docs").rglob("*.md")]:
        txt = md_path.read_text(encoding="utf-8")
        assert "{{MEASURED_" not in txt, f"Unfilled placeholder in {md_path}"

    # Verify 10 interview Q&As
    iq_headings = re.findall(
        r"^##\s+\d+\.", (ROOT / "docs" / "interview-questions.md").read_text(encoding="utf-8"), re.M
    )
    assert len(iq_headings) == 10

    # Verify LinkedIn article constraints (working title, 900..1200 words, repo link)
    article = (ROOT / "docs" / "linkedin-article.md").read_text(encoding="utf-8")
    assert article.startswith("# From click to dashboard in seconds: a real-time analytics stack on Kafka and Flink")
    assert "https://github.com/Manzoor25/kafka-flink-clickstream" in article
    word_count = len(article.split())
    assert 900 <= word_count <= 1200, f"LinkedIn article word count {word_count} outside [900, 1200]"

    # Verify LinkedIn teaser post constraints (<1300 chars, 3..5 hashtags)
    post = (ROOT / "docs" / "linkedin-post.md").read_text(encoding="utf-8").strip()
    assert len(post) < 1300, f"LinkedIn post length {len(post)} >= 1300"
    hashtags = re.findall(r"(?<!\S)#[A-Za-z][A-Za-z0-9_]*", post)
    assert 3 <= len(hashtags) <= 5, f"Expected 3..5 hashtags, got {hashtags}"
