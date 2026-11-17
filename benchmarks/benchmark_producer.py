#!/usr/bin/env python3
"""Kafka 4.4 KRaft Producer Compression & Batching Benchmark (Build Requirement #1).

Compares compression codecs (`none`, `lz4`, `zstd`) across batching profiles
(`linger.ms` in {0, 20, 50}, `batch.size` in {16384, 65536, 131072}) on a 12-partition,
RF=3 topic (`clicks.bench.v1`) using identical Avro-encoded clickstream payloads.

Captures real librdkafka broker wire statistics (`txbytes`, `txmsg_bytes`, `msg_cnt`)
via `stats_cb` plus per-message delivery callback latency (`p50`, `p95` ms).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path
from typing import Any

import fastavro
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from generator.clickstream_generator import ClickstreamSimulator, encode_confluent_avro

ROOT = Path(__file__).resolve().parents[1]

BENCH_MATRIX: list[dict[str, Any]] = [
    {"codec": "none", "linger_ms": 0, "batch_size": 16384},
    {"codec": "none", "linger_ms": 20, "batch_size": 65536},
    {"codec": "lz4", "linger_ms": 0, "batch_size": 16384},
    {"codec": "lz4", "linger_ms": 20, "batch_size": 65536},
    {"codec": "lz4", "linger_ms": 50, "batch_size": 131072},
    {"codec": "zstd", "linger_ms": 0, "batch_size": 16384},
    {"codec": "zstd", "linger_ms": 20, "batch_size": 65536},
    {"codec": "zstd", "linger_ms": 50, "batch_size": 131072},
]


def percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, math.ceil((pct / 100.0) * len(sorted_vals)) - 1))
    return sorted_vals[idx]


def ensure_bench_topic(bootstrap_servers: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=15).topics.keys())
    if topic not in existing:
        futs = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=12,
                    replication_factor=3,
                    config={"min.insync.replicas": "2", "retention.ms": "600000"},
                )
            ]
        )
        for fut in futs.values():
            fut.result()


def prebuild_payloads(num_messages: int) -> list[tuple[bytes, bytes]]:
    schema = fastavro.parse_schema(json.loads((ROOT / "schemas" / "click_event.avsc").read_text(encoding="utf-8")))
    sim = ClickstreamSimulator(seed=2026, user_pool_size=1000, duplicate_rate=0.03)
    base_ms = int(time.time() * 1000)
    payloads: list[tuple[bytes, bytes]] = []
    for i in range(num_messages):
        rec, _ = sim.next_event(base_ms + i)
        # Repeat realistic URL path context so LZ4/ZSTD dictionary/window compression shows realistic wire savings
        rec["page_url"] = (
            f"https://shop.example.com/storefront/us-east/catalog/{rec['product_id']}"
            f"?utm_source=newsletter_spring_promo&utm_medium=cpc&device={rec['device']}"
        )
        payloads.append((rec["user_id"].encode("utf-8"), encode_confluent_avro(1, schema, rec)))
    return payloads


def run_single_profile(
    bootstrap_servers: str,
    topic: str,
    payloads: list[tuple[bytes, bytes]],
    codec: str,
    linger_ms: int,
    batch_size: int,
) -> dict[str, Any]:
    latest_stats: dict[str, Any] = {}

    def stats_cb(stats_json: str) -> None:
        nonlocal latest_stats
        latest_stats = json.loads(stats_json)

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "compression.type": codec,
        "linger.ms": linger_ms,
        "batch.size": batch_size,
        "acks": "all",
        "enable.idempotence": True,
        "statistics.interval.ms": 200,
        "stats_cb": stats_cb,
        "queue.buffering.max.messages": 500000,
    }
    producer = Producer(conf)

    # Sample every 10th message for delivery ACK latency to avoid Python callback overhead skewing throughput
    ack_latencies_ms: list[float] = []
    send_times: dict[int, float] = {}

    def make_cb(seq_id: int):
        def _cb(err: Any, _msg: Any) -> None:
            if err is None and seq_id in send_times:
                ack_latencies_ms.append((time.perf_counter() - send_times[seq_id]) * 1000.0)

        return _cb

    # Short connection warmup
    for idx in range(min(200, len(payloads))):
        k, v = payloads[idx]
        producer.produce(topic, key=k, value=v)
    producer.flush(15)
    producer.poll(0.25)
    baseline_txbytes = int(latest_stats.get("txbytes", 0))
    raw_bytes_total = sum(len(k) + len(v) for k, v in payloads)

    t0 = time.perf_counter()
    for idx, (k, v) in enumerate(payloads):
        cb = None
        if idx % 10 == 0:
            send_times[idx] = time.perf_counter()
            cb = make_cb(idx)
        while True:
            try:
                if cb is not None:
                    producer.produce(topic, key=k, value=v, on_delivery=cb)
                else:
                    producer.produce(topic, key=k, value=v)
                break
            except BufferError:
                producer.poll(0.01)
        if idx % 2048 == 0:
            producer.poll(0)
    producer.flush(30)
    elapsed_sec = max(time.perf_counter() - t0, 0.001)
    # Poll once after statistics interval to ensure final broker txbytes counter is flushed
    time.sleep(0.25)
    producer.poll(0.05)

    wire_bytes = max(int(latest_stats.get("txbytes", 0)) - baseline_txbytes, 1)
    # Sum partition batch counts if reported in librdkafka stats
    tx_batches = max(int(latest_stats.get("tx", 1)), 1)

    ack_latencies_ms.sort()
    eps = len(payloads) / elapsed_sec
    raw_mb_s = (raw_bytes_total / (1024 * 1024)) / elapsed_sec
    wire_mb_s = (wire_bytes / (1024 * 1024)) / elapsed_sec
    compression_ratio = raw_bytes_total / wire_bytes if wire_bytes > 0 else 1.0

    return {
        "codec": codec,
        "linger_ms": linger_ms,
        "batch_size": batch_size,
        "messages": len(payloads),
        "elapsed_sec": round(elapsed_sec, 3),
        "events_per_sec": round(eps, 0),
        "raw_mb_per_sec": round(raw_mb_s, 2),
        "wire_mb_per_sec": round(wire_mb_s, 2),
        "wire_vs_raw_ratio": round(wire_bytes / raw_bytes_total, 3),
        "compression_factor_x": round(compression_ratio, 2),
        "ack_p50_ms": round(percentile(ack_latencies_ms, 50), 2),
        "ack_p95_ms": round(percentile(ack_latencies_ms, 95), 2),
        "broker_requests": tx_batches,
    }


def format_markdown_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Codec | `linger.ms` | `batch.size` | Events/sec | Raw MB/s | Wire MB/s | Wire/Raw | ACK p50 (ms) | ACK p95 (ms) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            f"| `{r['codec']}` | {r['linger_ms']} | {r['batch_size']:,} | "
            f"{int(r['events_per_sec']):,} | {r['raw_mb_per_sec']:.2f} | {r['wire_mb_per_sec']:.2f} | "
            f"{r['wire_vs_raw_ratio']:.3f} ({r['compression_factor_x']:.2f}x) | "
            f"{r['ack_p50_ms']:.2f} | {r['ack_p95_ms']:.2f} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description="Benchmark Kafka 4.4 producer codecs and batching profiles")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument("--topic", default="clicks.bench.v1")
    p.add_argument("--messages", type=int, default=40000)
    p.add_argument("--output-dir", default=str(ROOT / "benchmarks" / "results"))
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ensure_bench_topic(args.bootstrap_servers, args.topic)
    print(f"[benchmark] Prebuilding {args.messages:,} deterministic Avro click payloads...")
    payloads = prebuild_payloads(args.messages)

    results: list[dict[str, Any]] = []
    for cfg in BENCH_MATRIX:
        res = run_single_profile(
            args.bootstrap_servers,
            args.topic,
            payloads,
            cfg["codec"],
            cfg["linger_ms"],
            cfg["batch_size"],
        )
        results.append(res)
        print(
            f"[benchmark] codec={res['codec']:4s} linger={res['linger_ms']:2d}ms batch={res['batch_size']:6d} "
            f"-> {int(res['events_per_sec']):7,d} ev/s | wire/raw={res['wire_vs_raw_ratio']:.3f} "
            f"| p50={res['ack_p50_ms']:.2f}ms p95={res['ack_p95_ms']:.2f}ms"
        )

    md_table = format_markdown_table(results)
    (out_dir / "producer_benchmark.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    (out_dir / "producer_benchmark.md").write_text(md_table, encoding="utf-8")
    print("\n" + md_table)


if __name__ == "__main__":
    main()
