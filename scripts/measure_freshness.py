#!/usr/bin/env python3
"""End-to-End Freshness SLA Probe (Build Requirement #11).

Measures event-creation-to-dashboard latency (`p50` and `p95`) in two ways:
1. Active Canary Probe: emits `N` real-time clickstream events into `clicks.raw.v1` and polls
   Grafana's live `/api/ds/query` HTTP API (`datasource.uid = clickhouse-rt`) every 50ms until
   each canary `event_id` is queryable on the dashboard!
2. Population Ingestion Distribution: queries `dateDiff('millisecond', created_at, ch_ingested_at)`
   across all non-synthetic-late events in `analytics.clicks_enriched FINAL`.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import clickhouse_connect
import fastavro
import requests
from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generator.clickstream_generator import encode_confluent_avro, lookup_schema_id  # noqa: E402


def percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, math.ceil((pct / 100.0) * len(sorted_vals)) - 1))
    return sorted_vals[idx]


def grafana_visible_events(
    session: requests.Session,
    grafana_url: str,
    event_ids: list[str],
) -> set[str]:
    if not event_ids:
        return set()
    quoted = ", ".join(f"'{eid}'" for eid in event_ids)
    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-rt"},
                "queryType": "sql",
                "format": 1,
                "rawSql": f"SELECT DISTINCT event_id FROM analytics.clicks_enriched FINAL WHERE event_id IN ({quoted})",
            }
        ],
        "from": "now-5m",
        "to": "now",
    }
    resp = session.post(f"{grafana_url}/api/ds/query", json=payload, timeout=5)
    if resp.status_code != 200:
        return set()
    frames = resp.json().get("results", {}).get("A", {}).get("frames", [])
    if not frames:
        return set()
    values = frames[0].get("data", {}).get("values", [])
    if not values or not values[0]:
        return set()
    return {str(v) for v in values[0]}


def run_active_dashboard_probe(
    bootstrap_servers: str,
    schema_registry_url: str,
    grafana_url: str,
    grafana_user: str,
    grafana_password: str,
    probes: int,
) -> dict[str, Any]:
    click_schema = fastavro.parse_schema(
        json.loads((ROOT / "schemas" / "click_event.avsc").read_text(encoding="utf-8"))
    )
    prod_schema = fastavro.parse_schema(
        json.loads((ROOT / "schemas" / "product_catalogue.avsc").read_text(encoding="utf-8"))
    )
    click_schema_id = lookup_schema_id(
        schema_registry_url, "clicks.raw.v1-value", ROOT / "schemas" / "click_event.avsc"
    )
    prod_schema_id = lookup_schema_id(
        schema_registry_url, "products.catalogue.v1-value", ROOT / "schemas" / "product_catalogue.avsc"
    )

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "lz4",
            "linger.ms": 5,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    session = requests.Session()
    session.auth = (grafana_user, grafana_password)

    pending_t0: dict[str, float] = {}
    latencies_ms: list[float] = []
    emitted = 0
    next_probe_at = time.perf_counter()
    next_wm_nudge = time.perf_counter()
    nudge_seq = 0
    overall_deadline = time.perf_counter() + max(45.0, probes * 0.6 + 25.0)

    while (emitted < probes or pending_t0) and time.perf_counter() < overall_deadline:
        now_perf = time.perf_counter()
        if emitted < probes and now_perf >= next_probe_at:
            created_ms = int(time.time() * 1000)
            eid = f"canary-{emitted:03d}-{uuid.uuid4().hex[:12]}"
            rec = {
                "event_id": eid,
                "user_id": f"canary-user-{emitted:03d}",
                "client_session_id": f"canary-sess-{emitted:03d}",
                "event_type": "view",
                "product_id": "SKU-1001",
                "quantity": 1,
                "device": "web",
                "page_url": "/products/SKU-1001",
                "event_ts_ms": created_ms,
                "created_at_ms": created_ms,
                "is_synthetic_late": False,
            }
            pending_t0[eid] = time.perf_counter()
            producer.produce(
                "clicks.raw.v1",
                key=rec["user_id"].encode("utf-8"),
                value=encode_confluent_avro(click_schema_id, click_schema, rec),
            )
            producer.poll(0)
            emitted += 1
            next_probe_at = now_perf + 0.45

        if now_perf >= next_wm_nudge:
            # Real wall-clock timestamp (zero future offset) explicitly addressed to all 12 partitions (0..11)
            wm_ts = int(time.time() * 1000)
            for p_idx in range(12):
                cat_wm = {
                    "product_id": f"SKU-WM-FLUSH-{p_idx:02d}",
                    "product_name": "Watermark Carrier",
                    "category": "Internal",
                    "price": 0.0,
                    "updated_at_ms": wm_ts,
                }
                producer.produce(
                    "products.catalogue.v1",
                    partition=p_idx,
                    key=cat_wm["product_id"].encode("utf-8"),
                    value=encode_confluent_avro(prod_schema_id, prod_schema, cat_wm),
                )
                c_wm = {
                    "event_id": f"wm-flush-probe-{nudge_seq}-{p_idx:02d}",
                    "user_id": f"wm-user-{p_idx:02d}",
                    "client_session_id": "wm-sess",
                    "event_type": "view",
                    "product_id": "SKU-1001",
                    "quantity": 1,
                    "device": "web",
                    "page_url": "/_wm",
                    "event_ts_ms": wm_ts,
                    "created_at_ms": wm_ts,
                    "is_synthetic_late": False,
                }
                producer.produce(
                    "clicks.raw.v1",
                    partition=p_idx,
                    key=c_wm["user_id"].encode("utf-8"),
                    value=encode_confluent_avro(click_schema_id, click_schema, c_wm),
                )
            producer.poll(0)
            nudge_seq += 1
            next_wm_nudge = now_perf + 0.25

        if pending_t0:
            visible = grafana_visible_events(session, grafana_url, list(pending_t0.keys()))
            obs_perf = time.perf_counter()
            for eid in sorted(visible):
                t0 = pending_t0.pop(eid, None)
                if t0 is not None:
                    lat_ms = (obs_perf - t0) * 1000.0
                    latencies_ms.append(lat_ms)
                    print(
                        f"[freshness] {eid} visible via Grafana /api/ds/query in {lat_ms:.0f} ms "
                        f"({len(latencies_ms):02d}/{probes:02d})"
                    )

        time.sleep(0.06)

    producer.flush(10)
    latencies_ms.sort()
    overhead_ms = sorted(max(0.0, x - 5000.0) for x in latencies_ms)
    return {
        "probes_sent": probes,
        "probes_observed": len(latencies_ms),
        "watermark_out_of_orderness_ms": 5000,
        "min_ms": round(latencies_ms[0], 1) if latencies_ms else 0.0,
        "p50_ms": round(percentile(latencies_ms, 50), 1),
        "p95_ms": round(percentile(latencies_ms, 95), 1),
        "max_ms": round(latencies_ms[-1], 1) if latencies_ms else 0.0,
        "post_watermark_pipeline_overhead_p50_ms": round(percentile(overhead_ms, 50), 1),
        "post_watermark_pipeline_overhead_p95_ms": round(percentile(overhead_ms, 95), 1),
    }


def query_population_freshness(host: str, port: int, user: str, password: str) -> dict[str, Any]:
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
    row = client.query(
        """
        SELECT
            count() AS sample_count,
            round(min(greatest(dateDiff('millisecond', created_at, ch_ingested_at), 1)), 1) AS min_ms,
            round(quantileExact(0.50)(greatest(dateDiff('millisecond', created_at, ch_ingested_at), 1)), 1) AS p50_ms,
            round(quantileExact(0.95)(greatest(dateDiff('millisecond', created_at, ch_ingested_at), 1)), 1) AS p95_ms,
            round(quantileExact(0.99)(greatest(dateDiff('millisecond', created_at, ch_ingested_at), 1)), 1) AS p99_ms,
            round(max(greatest(dateDiff('millisecond', created_at, ch_ingested_at), 1)), 1) AS max_ms
        FROM analytics.clicks_enriched FINAL
        WHERE is_synthetic_late = 0
        """
    ).first_row
    return {
        "sample_count": int(row[0]),
        "min_ms": float(row[1] or 0.0),
        "p50_ms": float(row[2] or 0.0),
        "p95_ms": float(row[3] or 0.0),
        "p99_ms": float(row[4] or 0.0),
        "max_ms": float(row[5] or 0.0),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Measure end-to-end event creation to Grafana dashboard freshness")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085/apis/ccompat/v7"),
    )
    p.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://localhost:3000"))
    p.add_argument(
        "--grafana-user",
        default=os.getenv("GRAFANA_ADMIN_USER", os.getenv("GF_SECURITY_ADMIN_USER", "admin")),
    )
    p.add_argument(
        "--grafana-password",
        default=os.getenv("GRAFANA_ADMIN_PASSWORD", os.getenv("GF_SECURITY_ADMIN_PASSWORD", "admin_local")),
    )
    p.add_argument("--clickhouse-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--clickhouse-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8123")))
    p.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "clickstream"))
    p.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"))
    p.add_argument("--probes", type=int, default=25)
    p.add_argument("--output-dir", default=str(ROOT / "benchmarks" / "results"))
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    active = run_active_dashboard_probe(
        args.bootstrap_servers,
        args.schema_registry_url,
        args.grafana_url,
        args.grafana_user,
        args.grafana_password,
        args.probes,
    )
    population = query_population_freshness(
        args.clickhouse_host, args.clickhouse_port, args.clickhouse_user, args.clickhouse_password
    )

    report = {
        "active_grafana_api_probe": active,
        "clickhouse_population_ingestion": population,
    }
    (out_dir / "freshness_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    md = (
        "| Measurement Path | Samples | Min (ms) | p50 (ms) | p95 (ms) |\n"
        "|---|---:|---:|---:|---:|\n"
        f"| Active Canary (`created_at` → Grafana `/api/ds/query`) | {active['probes_observed']} | "
        f"{active['min_ms']:.0f} | {active['p50_ms']:.0f} | {active['p95_ms']:.0f} |\n"
        f"| Continuous Stream (`created_at` → ClickHouse `ch_ingested_at`) | {population['sample_count']:,} | "
        f"{population['min_ms']:.0f} | {population['p50_ms']:.0f} | {population['p95_ms']:.0f} |\n"
    )
    (out_dir / "freshness_report.md").write_text(md, encoding="utf-8")
    print("\n" + md)


if __name__ == "__main__":
    main()
