#!/usr/bin/env python3
"""Streaming-vs-Batch Correctness Verifier.

Computes an independent deterministic batch recount from either:
  1. A local JSONL ground-truth ledger (`--ledger-path`), OR
  2. Raw immutable Kafka topics (`clicks.raw.v1` + `products.catalogue.v1` from earliest offset),
and asserts exact equality against ClickHouse `analytics.clicks_enriched FINAL`
(unique event count, unique active users, purchase count, and temporally joined gross revenue).
"""

from __future__ import annotations

import argparse
import bisect
import io
import json
import os
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import clickhouse_connect
import fastavro
from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parents[1]


def compute_batch_totals(
    clicks: list[dict[str, Any]],
    catalogue_revisions: list[dict[str, Any]],
    exclude_synthetic_late: bool = False,
) -> dict[str, Any]:
    """Pure deterministic batch recount matching Flink deduplication + FOR SYSTEM_TIME AS OF join."""
    # Build sorted temporal price timeline per SKU: [(updated_at_ms, price), ...]
    revs_by_sku: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for rev in catalogue_revisions:
        sku = str(rev["product_id"])
        if sku.startswith("SKU-WM-FLUSH-"):
            continue
        revs_by_sku[sku].append((int(rev["updated_at_ms"]), float(rev["price"])))

    ts_index_by_sku: dict[str, list[int]] = {}
    price_list_by_sku: dict[str, list[float]] = {}
    for sku, pairs in revs_by_sku.items():
        # Sort by updated_at_ms; later revision at identical ms wins
        dedup_ts: dict[int, float] = {}
        for ts_ms, price in pairs:
            dedup_ts[ts_ms] = price
        ordered = sorted(dedup_ts.items(), key=lambda x: x[0])
        ts_index_by_sku[sku] = [x[0] for x in ordered]
        price_list_by_sku[sku] = [x[1] for x in ordered]

    seen_event_ids: set[str] = set()
    users: set[str] = set()
    purchases = 0
    gross_revenue = 0.0

    for c in clicks:
        eid = str(c["event_id"])
        if eid.startswith("wm-flush-") or eid in seen_event_ids:
            continue
        seen_event_ids.add(eid)

        if exclude_synthetic_late and bool(c.get("is_synthetic_late", False)):
            continue

        users.add(str(c["user_id"]))
        if c["event_type"] == "purchase":
            purchases += 1
            sku = str(c["product_id"])
            ev_ts = int(c["event_ts_ms"])
            ts_list = ts_index_by_sku.get(sku, [])
            pos = bisect.bisect_right(ts_list, ev_ts) - 1
            unit_price = round(price_list_by_sku[sku][pos], 2) if pos >= 0 else 0.0
            gross_revenue += round(unit_price * int(c["quantity"]), 2)

    return {
        "unique_events": len(seen_event_ids)
        if not exclude_synthetic_late
        else len(users_or_events(clicks, exclude_synthetic_late)),
        "active_users": len(users),
        "purchases": purchases,
        "gross_revenue": round(gross_revenue, 2),
    }


def users_or_events(clicks: list[dict[str, Any]], exclude_synthetic_late: bool) -> set[str]:
    seen: set[str] = set()
    kept: set[str] = set()
    for c in clicks:
        eid = str(c["event_id"])
        if eid.startswith("wm-flush-") or eid in seen:
            continue
        seen.add(eid)
        if exclude_synthetic_late and bool(c.get("is_synthetic_late", False)):
            continue
        kept.add(eid)
    return kept


def load_from_ledger(ledger_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    clicks: list[dict[str, Any]] = []
    cat: list[dict[str, Any]] = []
    for line in ledger_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if obj.get("record_kind") == "catalogue":
            cat.append(obj)
        elif obj.get("record_kind") == "click":
            clicks.append(obj)
    return clicks, cat


def drain_avro_topic(bootstrap_servers: str, topic: str, schema_path: Path) -> list[dict[str, Any]]:
    parsed_schema = fastavro.parse_schema(json.loads(schema_path.read_text(encoding="utf-8")))
    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"batch-recount-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    rows: list[dict[str, Any]] = []
    try:
        meta = c.list_topics(topic, timeout=10)
        parts = [TopicPartition(topic, pid, 0) for pid in sorted(meta.topics[topic].partitions.keys())]
        end_offsets = {}
        for tp in parts:
            _low, high = c.get_watermark_offsets(tp, timeout=5.0)
            end_offsets[tp.partition] = high
        active_parts = [tp for tp in parts if end_offsets[tp.partition] > 0]
        if not active_parts:
            return rows
        c.assign(active_parts)
        done: set[int] = set()
        empty_polls = 0
        while len(done) < len(active_parts) and empty_polls < 6:
            msg = c.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                continue
            val = msg.value()
            if val and len(val) > 5 and val[0] == 0:
                rec = fastavro.schemaless_reader(io.BytesIO(val[5:]), parsed_schema)
                rows.append(dict(rec))
            if msg.offset() + 1 >= end_offsets[msg.partition()]:
                done.add(msg.partition())
    finally:
        c.close()
    return rows


def query_clickhouse_totals(
    host: str,
    port: int,
    user: str,
    password: str,
    exclude_synthetic_late: bool,
    event_id_subset: set[str] | None = None,
) -> dict[str, Any]:
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
    where_clauses = ["1 = 1"]
    if exclude_synthetic_late:
        where_clauses.append("is_synthetic_late = 0")
    where_sql = " AND ".join(where_clauses)

    if event_id_subset is not None:
        # Filter strictly to the target ledger's unique event_ids when multiple runs share a live cluster
        rows = client.query(
            f"SELECT event_id, user_id, event_type, toFloat64(gross_revenue) "
            f"FROM analytics.clicks_enriched FINAL WHERE {where_sql}"
        ).result_rows
        matched = [r for r in rows if r[0] in event_id_subset]
        return {
            "unique_events": len(matched),
            "active_users": len({r[1] for r in matched}),
            "purchases": sum(1 for r in matched if r[2] == "purchase"),
            "gross_revenue": round(sum(float(r[3]) for r in matched), 2),
        }

    row = client.query(
        f"""
        SELECT
            count() AS unique_events,
            uniqExact(user_id) AS active_users,
            countIf(event_type = 'purchase') AS purchases,
            round(toFloat64(coalesce(sum(gross_revenue), 0)), 2) AS gross_revenue
        FROM analytics.clicks_enriched FINAL
        WHERE {where_sql}
        """
    ).first_row
    return {
        "unique_events": int(row[0]),
        "active_users": int(row[1]),
        "purchases": int(row[2]),
        "gross_revenue": round(float(row[3]), 2),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Verify Flink/ClickHouse streaming totals against exact batch recount")
    p.add_argument("--ledger-path", default="", help="Optional JSONL ledger path (if omitted, recounts raw Kafka)")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument("--clickhouse-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--clickhouse-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8123")))
    p.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "clickstream"))
    p.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"))
    p.add_argument("--exclude-synthetic-late", action="store_true")
    p.add_argument("--wait-timeout-sec", type=float, default=35.0)
    p.add_argument("--json-out", default="")
    args = p.parse_args()

    subset_ids: set[str] | None = None
    if args.ledger_path:
        clicks, ledger_cat = load_from_ledger(Path(args.ledger_path))
        try:
            kafka_cat = drain_avro_topic(
                args.bootstrap_servers, "products.catalogue.v1", ROOT / "schemas" / "product_catalogue.avsc"
            )
            cat = kafka_cat if kafka_cat else ledger_cat
        except Exception:
            cat = ledger_cat
        subset_ids = users_or_events(clicks, args.exclude_synthetic_late)
        source_desc = f"ledger:{args.ledger_path}"
    else:
        clicks = drain_avro_topic(args.bootstrap_servers, "clicks.raw.v1", ROOT / "schemas" / "click_event.avsc")
        cat = drain_avro_topic(
            args.bootstrap_servers, "products.catalogue.v1", ROOT / "schemas" / "product_catalogue.avsc"
        )
        source_desc = "kafka:clicks.raw.v1+products.catalogue.v1"

    batch = compute_batch_totals(clicks, cat, exclude_synthetic_late=args.exclude_synthetic_late)

    deadline = time.monotonic() + args.wait_timeout_sec
    stream = {}
    while time.monotonic() < deadline:
        stream = query_clickhouse_totals(
            args.clickhouse_host,
            args.clickhouse_port,
            args.clickhouse_user,
            args.clickhouse_password,
            args.exclude_synthetic_late,
            subset_ids,
        )
        if stream["unique_events"] >= batch["unique_events"] and batch["unique_events"] > 0:
            break
        time.sleep(1.0)

    ev_diff = stream["unique_events"] - batch["unique_events"]
    usr_diff = stream["active_users"] - batch["active_users"]
    pur_diff = stream["purchases"] - batch["purchases"]
    rev_diff = round(stream["gross_revenue"] - batch["gross_revenue"], 2)

    passed = ev_diff == 0 and usr_diff == 0 and pur_diff == 0 and abs(rev_diff) <= 0.01

    report = {
        "source": source_desc,
        "batch_recount": batch,
        "streaming_clickhouse": stream,
        "diff": {
            "unique_events": ev_diff,
            "active_users": usr_diff,
            "purchases": pur_diff,
            "gross_revenue": rev_diff,
        },
        "passed": passed,
    }
    print(json.dumps(report, indent=2))
    if args.json_out:
        out_file = Path(args.json_out)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
