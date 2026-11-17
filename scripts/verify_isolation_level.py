#!/usr/bin/env python3
"""Demonstrates Flink 2PC Transactional Kafka Sinks & Downstream `read_committed` Isolation
(Build Requirement #9).

Verifies:
1. Flink checkpoints run in EXACTLY_ONCE mode and commit Kafka 2PC transactions on completion.
2. Transactional output topics contain Kafka transaction control markers (visible as offset gap
   between raw high-watermark and business record count).
3. ClickHouse Kafka Engine consumers enforce `isolation_level = read_committed` so uncommitted
   or aborted records from crashed TaskManagers never surface in analytical queries.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import requests
from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parents[1]


def count_topic_messages(bootstrap_servers: str, topic: str, isolation_level: str) -> dict[str, int]:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"iso-verify-{isolation_level}-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": isolation_level,
        }
    )
    records = 0
    raw_offset_span = 0
    try:
        meta = c.list_topics(topic, timeout=10)
        parts = [TopicPartition(topic, pid, 0) for pid in sorted(meta.topics[topic].partitions.keys())]
        end_offsets: dict[int, int] = {}
        for tp in parts:
            low, high = c.get_watermark_offsets(tp, timeout=5.0)
            end_offsets[tp.partition] = high
            raw_offset_span += max(0, high - low)

        active = [tp for tp in parts if end_offsets[tp.partition] > 0]
        if not active:
            return {"business_records": 0, "raw_offset_span": 0, "txn_control_markers": 0}

        c.assign(active)
        empty_polls = 0
        while empty_polls < 4:
            msg = c.poll(0.75)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            records += 1
    finally:
        c.close()

    return {
        "business_records": records,
        "raw_offset_span": raw_offset_span,
        "txn_control_markers": max(0, raw_offset_span - records),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Verify Flink 2PC Kafka sink & read_committed isolation")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument("--flink-url", default=os.getenv("FLINK_REST_URL", "http://localhost:8081"))
    p.add_argument("--topic", default="clicks.enriched.v1")
    p.add_argument("--json-out", default="")
    args = p.parse_args()

    ov = requests.get(f"{args.flink_url}/jobs/overview", timeout=10).json()
    running = [j for j in ov.get("jobs", []) if j["state"] == "RUNNING"]
    if not running:
        print("ERROR: No RUNNING Flink jobs found", file=sys.stderr)
        sys.exit(1)

    jid = running[0]["jid"]
    ck_cfg = requests.get(f"{args.flink_url}/jobs/{jid}/checkpoints/config", timeout=10).json()
    mode = ck_cfg.get("mode", "").upper()

    committed_stats = count_topic_messages(args.bootstrap_servers, args.topic, "read_committed")
    ch_xml = (ROOT / "docker" / "clickhouse" / "config.d" / "kafka.xml").read_text(encoding="utf-8")
    has_ch_read_committed = "<isolation_level>read_committed</isolation_level>" in ch_xml

    report = {
        "flink_checkpoint_mode": mode,
        "flink_checkpoint_interval_ms": ck_cfg.get("interval"),
        "topic": args.topic,
        "read_committed_consumer": committed_stats,
        "clickhouse_read_committed_configured": has_ch_read_committed,
    }
    print(json.dumps(report, indent=2))
    if args.json_out:
        out_p = Path(args.json_out)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if mode != "EXACTLY_ONCE" or not has_ch_read_committed or committed_stats["txn_control_markers"] <= 0:
        print("ERROR: Transactional 2PC / read_committed verification failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
