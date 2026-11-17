#!/usr/bin/env python3
"""Operations telemetry probe (Build Requirement #12):
- Kafka consumer group lag per topic across Flink & ClickHouse groups
- Flink JobManager / TaskManager checkpoint health, restoration counts, state size
- Flink vertex-level backpressure ratios (`backpressureLevel`, `ratio`, `busyRatio`)
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

MONITORED_GROUPS = [
    "flink-sql-clickstream-v1",
    "flink-ds-sessions-v1",
    "ch-clicks-enriched-v1",
    "ch-minute-tumbling-v1",
    "ch-user-sessions-v1",
    "ch-funnel-metrics-v1",
]


def collect_consumer_lag(bootstrap_servers: str, groups: list[str]) -> dict[str, dict[str, int]]:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    probe = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"ops-lag-probe-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    lag_report: dict[str, dict[str, int]] = {}
    try:
        for grp in groups:
            try:
                futs = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(grp)])
                grp_info = futs[grp].result(timeout=10)
            except Exception:
                continue

            total_lag = 0
            committed_parts = 0
            for tp in grp_info.topic_partitions:
                if tp.offset < 0:
                    continue
                committed_parts += 1
                _low, high = probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5.0)
                total_lag += max(0, high - tp.offset)

            if committed_parts > 0:
                lag_report[grp] = {"committed_partitions": committed_parts, "total_lag": total_lag}
    finally:
        probe.close()
    return lag_report


def collect_flink_telemetry(flink_url: str) -> list[dict[str, Any]]:
    ov = requests.get(f"{flink_url}/jobs/overview", timeout=10)
    ov.raise_for_status()
    jobs_out: list[dict[str, Any]] = []

    for j in ov.json().get("jobs", []):
        if j["state"] not in ("RUNNING", "RESTARTING", "RECONCILING"):
            continue
        jid = j["jid"]
        detail = requests.get(f"{flink_url}/jobs/{jid}", timeout=10).json()
        ck = requests.get(f"{flink_url}/jobs/{jid}/checkpoints", timeout=10).json()
        counts = ck.get("counts", {})
        latest_ck = (ck.get("latest", {}) or {}).get("completed", {}) or {}

        vertices_bp: list[dict[str, Any]] = []
        max_bp_ratio = 0.0
        max_busy_ratio = 0.0
        for v in detail.get("vertices", []):
            vid = v["id"]
            bp_resp = requests.get(f"{flink_url}/jobs/{jid}/vertices/{vid}/backpressure", timeout=10)
            bp_level = "unknown"
            bp_ratio = 0.0
            busy_ratio = 0.0
            if bp_resp.status_code == 200:
                bp_json = bp_resp.json()
                bp_level = bp_json.get("backpressureLevel", bp_json.get("backpressure-level", "ok"))
                subtasks = bp_json.get("subtasks", [])
                if subtasks:
                    bp_ratio = max(float(s.get("ratio", 0.0)) for s in subtasks)
                    busy_vals = [float(s.get("busyRatio", 0.0)) for s in subtasks if s.get("busyRatio") is not None]
                    if busy_vals:
                        busy_ratio = max(busy_vals)
            max_bp_ratio = max(max_bp_ratio, bp_ratio)
            max_busy_ratio = max(max_busy_ratio, busy_ratio)
            vertices_bp.append(
                {
                    "name": v["name"],
                    "parallelism": v["parallelism"],
                    "backpressure_level": bp_level,
                    "max_subtask_bp_ratio": round(bp_ratio, 3),
                    "max_subtask_busy_ratio": round(busy_ratio, 3),
                }
            )

        jobs_out.append(
            {
                "jid": jid,
                "name": j["name"],
                "state": j["state"],
                "completed_checkpoints": counts.get("completed", 0),
                "restored_checkpoints": counts.get("restored", 0),
                "failed_checkpoints": counts.get("failed", 0),
                "latest_state_size_bytes": latest_ck.get("state_size", 0),
                "latest_end_to_end_duration_ms": latest_ck.get("end_to_end_duration", 0),
                "max_backpressure_ratio": round(max_bp_ratio, 3),
                "max_busy_ratio": round(max_busy_ratio, 3),
                "vertices": vertices_bp,
            }
        )
    return jobs_out


def snapshot(bootstrap_servers: str, flink_url: str) -> dict[str, Any]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "consumer_lag": collect_consumer_lag(bootstrap_servers, MONITORED_GROUPS),
        "flink_jobs": collect_flink_telemetry(flink_url),
    }


def print_snapshot(snap: dict[str, Any]) -> None:
    print(f"=== Ops Telemetry @ {snap['timestamp_utc']} ===")
    for grp, info in snap["consumer_lag"].items():
        print(f"  [kafka-lag] group={grp:28s} partitions={info['committed_partitions']:2d} lag={info['total_lag']}")
    for job in snap["flink_jobs"]:
        print(
            f"  [flink-job] {job['name']} ({job['state']}) | ck_ok={job['completed_checkpoints']} "
            f"ck_restored={job['restored_checkpoints']} state={job['latest_state_size_bytes']:,}B "
            f"max_bp={job['max_backpressure_ratio']:.2f} max_busy={job['max_busy_ratio']:.2f}"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Monitor Kafka consumer lag and Flink backpressure/checkpoints")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument("--flink-url", default=os.getenv("FLINK_REST_URL", "http://localhost:8081"))
    p.add_argument("--samples", type=int, default=1)
    p.add_argument("--interval-sec", type=float, default=2.0)
    p.add_argument("--json-out", default="")
    args = p.parse_args()

    series: list[dict[str, Any]] = []
    for i in range(args.samples):
        s = snapshot(args.bootstrap_servers, args.flink_url)
        series.append(s)
        print_snapshot(s)
        if i + 1 < args.samples:
            time.sleep(args.interval_sec)

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(series, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
