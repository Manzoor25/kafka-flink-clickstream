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
import time
import uuid
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]


def count_topic_messages(bootstrap_servers: str, topic: str, isolation_level: str) -> dict[str, int]:
    # Wait >1 Flink checkpoint interval (2.0s) so any in-flight 2PC transaction commits cleanly
    time.sleep(3.0)
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"iso-verify-{isolation_level}-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": isolation_level,
        }
    )
    leo_probe = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"iso-leo-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "isolation.level": "read_uncommitted",
        }
    )
    records = 0
    raw_offset_span = 0
    try:
        meta = c.list_topics(topic, timeout=10)
        parts = [TopicPartition(topic, pid, 0) for pid in sorted(meta.topics[topic].partitions.keys())]
        active: list[TopicPartition] = []
        for tp in parts:
            _low, high = leo_probe.get_watermark_offsets(tp, timeout=5.0)
            if high > 0:
                active.append(tp)

        if not active:
            return {"business_records": 0, "raw_offset_span": 0, "txn_control_markers": 0}

        c.assign(active)
        empty_polls = 0
        while empty_polls < 5:
            msg = c.poll(0.75)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            records += 1

        # Query true Log End Offset (LEO) across partitions after draining committed records
        for tp in parts:
            low, high = leo_probe.get_watermark_offsets(tp, timeout=5.0)
            raw_offset_span += max(0, high - low)
    finally:
        c.close()
        leo_probe.close()

    return {
        "business_records": records,
        "raw_offset_span": raw_offset_span,
        "txn_control_markers": max(0, raw_offset_span - records),
    }


def prove_downstream_isolation_levels(bootstrap_servers: str) -> dict[str, Any]:
    """Empirically proves why downstream consumers of Flink 2PC sinks require read_committed."""
    probe_topic = f"txn.isolation.probe.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futs = admin.create_topics(
        [NewTopic(probe_topic, num_partitions=1, replication_factor=3, config={"min.insync.replicas": "2"})]
    )
    for fut in futs.values():
        fut.result()

    def _drain_probe(level: str) -> list[str]:
        cons = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"probe-{level}-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": level,
            }
        )
        seen: list[str] = []
        try:
            cons.assign([TopicPartition(probe_topic, 0, 0)])
            empty = 0
            while empty < 3:
                m = cons.poll(0.5)
                if m is None:
                    empty += 1
                    continue
                empty = 0
                if not m.error() and m.value():
                    seen.append(m.value().decode("utf-8"))
        finally:
            cons.close()
        return seen

    txn_prod = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "transactional.id": f"isolation-proof-{uuid.uuid4().hex[:8]}",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    txn_prod.init_transactions(15.0)

    # 1. Open transaction, flush dirty record WITHOUT committing, and inspect both isolation levels
    txn_prod.begin_transaction()
    txn_prod.produce(probe_topic, key=b"u1", value=b"dirty-uncommitted-record", partition=0)
    txn_prod.flush(10.0)

    uncommitted_dirty_reads = len(_drain_probe("read_uncommitted"))
    committed_during_inflight = len(_drain_probe("read_committed"))

    # 2. Simulate TaskManager crash abort, then commit a valid checkpoint transaction
    txn_prod.abort_transaction(15.0)
    txn_prod.begin_transaction()
    txn_prod.produce(probe_topic, key=b"u1", value=b"valid-2pc-committed-record", partition=0)
    txn_prod.commit_transaction(15.0)

    committed_after_abort_and_commit = _drain_probe("read_committed")
    try:
        del_futs = admin.delete_topics([probe_topic])
        for fut in del_futs.values():
            fut.result()
    except Exception:
        pass

    return {
        "read_uncommitted_saw_inflight_records": uncommitted_dirty_reads,
        "read_committed_saw_inflight_records": committed_during_inflight,
        "read_committed_records_after_abort_then_commit": committed_after_abort_and_commit,
        "aborted_transaction_hidden": committed_after_abort_and_commit == ["valid-2pc-committed-record"],
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
    isolation_proof = prove_downstream_isolation_levels(args.bootstrap_servers)
    ch_xml = (ROOT / "docker" / "clickhouse" / "config.d" / "kafka.xml").read_text(encoding="utf-8")
    has_ch_read_committed = "<isolation_level>read_committed</isolation_level>" in ch_xml

    report = {
        "flink_checkpoint_mode": mode,
        "flink_checkpoint_interval_ms": ck_cfg.get("interval"),
        "topic": args.topic,
        "read_committed_consumer": committed_stats,
        "downstream_isolation_proof": isolation_proof,
        "clickhouse_read_committed_configured": has_ch_read_committed,
    }
    print(json.dumps(report, indent=2))
    if args.json_out:
        out_p = Path(args.json_out)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        out_p.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if (
        mode != "EXACTLY_ONCE"
        or not has_ch_read_committed
        or committed_stats["txn_control_markers"] <= 0
        or isolation_proof["read_committed_saw_inflight_records"] != 0
        or not isolation_proof["aborted_transaction_hidden"]
    ):
        print("ERROR: Transactional 2PC / read_committed verification failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
