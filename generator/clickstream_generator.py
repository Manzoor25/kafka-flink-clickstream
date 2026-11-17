#!/usr/bin/env python3
"""Realistic e-commerce clickstream & temporal catalogue event generator.

Features:
- Stateful user journeys following a 4-step Markov funnel (view -> add_to_cart -> checkout -> purchase)
- Zipf-weighted SKU popularity across 24 products
- Configurable traffic spikes (--spike-multiplier / scheduled spike windows)
- Configurable exact-payload duplicate event injection (--duplicate-rate) to verify Flink event_id deduplication
- Configurable out-of-order (within-watermark) and extreme late-event (--late-rate) injection
- Periodic compacted-topic price updates (`products.catalogue.v1`) to verify Flink temporal joins
- Optional JSONL ground-truth ledger (`--ledger-path`) for streaming-vs-batch recount assertions
"""

from __future__ import annotations

import argparse
import collections
import io
import json
import os
import random
import struct
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fastavro
import requests
from confluent_kafka import Producer

from generator.catalog import INITIAL_CATALOGUE, generate_price_mutation

ROOT = Path(__file__).resolve().parents[1]
DEVICES = ["web", "ios", "android"]
DEVICE_WEIGHTS = [0.54, 0.27, 0.19]


def encode_confluent_avro(schema_id: int, parsed_schema: Any, record: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def lookup_schema_id(registry_ccompat_url: str, subject: str, fallback_schema_path: Path) -> int:
    resp = requests.get(f"{registry_ccompat_url}/subjects/{subject}/versions/latest", timeout=10)
    if resp.status_code == 200:
        return int(resp.json()["id"])
    raw_schema = fallback_schema_path.read_text(encoding="utf-8")
    reg = requests.post(
        f"{registry_ccompat_url}/subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": raw_schema},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    reg.raise_for_status()
    return int(reg.json()["id"])


@dataclass
class ActiveJourney:
    user_id: str
    client_session_id: str
    device: str
    product_id: str
    quantity: int
    stage_idx: int = 0  # 0=view, 1=add_to_cart, 2=checkout, 3=purchase
    extra_views_left: int = 0
    next_emit_ms: int = 0


@dataclass
class ClickstreamSimulator:
    seed: int = 42
    user_pool_size: int = 600
    duplicate_rate: float = 0.03
    within_watermark_ooo_rate: float = 0.04
    extreme_late_rate: float = 0.015
    rng: random.Random = field(init=False)
    active_journeys: list[ActiveJourney] = field(default_factory=list)
    recent_events: collections.deque[dict[str, Any]] = field(default_factory=lambda: collections.deque(maxlen=512))
    products: list[dict[str, Any]] = field(default_factory=lambda: [dict(p) for p in INITIAL_CATALOGUE])
    product_weights: list[float] = field(init=False)
    min_non_late_ts_ms: int | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.product_weights = [float(p["weight"]) for p in self.products]

    def _pick_product(self) -> str:
        chosen = self.rng.choices(self.products, weights=self.product_weights, k=1)[0]
        return str(chosen["product_id"])

    def _start_journey(self, now_ms: int) -> ActiveJourney:
        user_num = self.rng.randint(1, self.user_pool_size)
        user_id = f"user-{user_num:04d}"
        sess_suffix = self.rng.getrandbits(32)
        client_session_id = f"sess-{user_num:04d}-{sess_suffix:08x}"
        device = self.rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
        product_id = self._pick_product()
        quantity = self.rng.choices([1, 2, 3], weights=[0.78, 0.17, 0.05], k=1)[0]
        extra_views = self.rng.choices([0, 1, 2], weights=[0.55, 0.30, 0.15], k=1)[0]
        return ActiveJourney(
            user_id=user_id,
            client_session_id=client_session_id,
            device=device,
            product_id=product_id,
            quantity=quantity,
            stage_idx=0,
            extra_views_left=extra_views,
            next_emit_ms=now_ms,
        )

    def _deterministic_uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def next_event(self, now_ms: int) -> tuple[dict[str, Any], bool]:
        """Generates the next clickstream record. Returns (record, is_duplicate)."""
        if self.recent_events and self.rng.random() < self.duplicate_rate:
            orig = dict(self.rng.choice(self.recent_events))
            # Duplicate retry preserves original event_id, user_id, event_ts_ms, and payload
            return orig, True

        ready_indices = [idx for idx, j in enumerate(self.active_journeys) if j.next_emit_ms <= now_ms]
        if ready_indices:
            idx = self.rng.choice(ready_indices)
            journey = self.active_journeys[idx]
        else:
            journey = self._start_journey(now_ms)
            self.active_journeys.append(journey)
            idx = len(self.active_journeys) - 1

        stages = ["view", "add_to_cart", "checkout", "purchase"]
        event_type = stages[journey.stage_idx]
        sku = journey.product_id
        qty = journey.quantity if event_type != "view" else 1

        route_map = {
            "view": f"/products/{sku}",
            "add_to_cart": f"/cart/add/{sku}",
            "checkout": "/checkout",
            "purchase": "/checkout/complete",
        }

        if self.min_non_late_ts_ms is None:
            self.min_non_late_ts_ms = now_ms

        event_ts_ms = now_ms
        is_synthetic_late = False
        late_roll = self.rng.random()
        if late_roll < self.extreme_late_rate:
            # Backdate by 45s-75s (> 5s watermark + 15s allowed lateness) so DataStream side-output catches it
            event_ts_ms = now_ms - self.rng.randint(45_000, 75_000)
            is_synthetic_late = True
        elif late_roll < (self.extreme_late_rate + self.within_watermark_ooo_rate):
            # Mild out-of-order arrival within the 5-second bounded-out-of-orderness watermark
            event_ts_ms = max(self.min_non_late_ts_ms, now_ms - self.rng.randint(400, 3_800))

        record = {
            "event_id": self._deterministic_uuid(),
            "user_id": journey.user_id,
            "client_session_id": journey.client_session_id,
            "event_type": event_type,
            "product_id": sku,
            "quantity": qty,
            "device": journey.device,
            "page_url": route_map[event_type],
            "event_ts_ms": event_ts_ms,
            "created_at_ms": now_ms,
            "is_synthetic_late": is_synthetic_late,
        }
        self.recent_events.append(record)

        # Advance or terminate Markov funnel state
        think_ms = self.rng.randint(300, 2_200)
        if journey.stage_idx == 0 and journey.extra_views_left > 0:
            journey.extra_views_left -= 1
            if self.rng.random() < 0.5:
                journey.product_id = self._pick_product()
            journey.next_emit_ms = now_ms + think_ms
        elif journey.stage_idx == 0:
            if self.rng.random() < 0.36:
                journey.stage_idx = 1
                journey.next_emit_ms = now_ms + think_ms
            else:
                self.active_journeys.pop(idx)
        elif journey.stage_idx == 1:
            if self.rng.random() < 0.60:
                journey.stage_idx = 2
                journey.next_emit_ms = now_ms + think_ms
            else:
                self.active_journeys.pop(idx)
        elif journey.stage_idx == 2:
            if self.rng.random() < 0.70:
                journey.stage_idx = 3
                journey.next_emit_ms = now_ms + think_ms
            else:
                self.active_journeys.pop(idx)
        else:
            self.active_journeys.pop(idx)

        return record, False


def run_producer(args: argparse.Namespace) -> dict[str, Any]:
    click_schema = fastavro.parse_schema(
        json.loads((ROOT / "schemas" / "click_event.avsc").read_text(encoding="utf-8"))
    )
    prod_schema = fastavro.parse_schema(
        json.loads((ROOT / "schemas" / "product_catalogue.avsc").read_text(encoding="utf-8"))
    )
    click_schema_id = lookup_schema_id(
        args.schema_registry_url, "clicks.raw.v1-value", ROOT / "schemas" / "click_event.avsc"
    )
    prod_schema_id = lookup_schema_id(
        args.schema_registry_url, "products.catalogue.v1-value", ROOT / "schemas" / "product_catalogue.avsc"
    )

    producer_conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "compression.type": args.compression,
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "acks": "all",
        "enable.idempotence": True,
        "queue.buffering.max.messages": 500000,
    }
    producer = Producer(producer_conf)

    sim = ClickstreamSimulator(
        seed=args.seed,
        user_pool_size=args.user_pool,
        duplicate_rate=args.duplicate_rate,
        within_watermark_ooo_rate=args.ooo_rate,
        extreme_late_rate=args.late_rate,
    )
    live_catalogue = {p["product_id"]: dict(p) for p in INITIAL_CATALOGUE}

    ledger_fp = open(args.ledger_path, "w", encoding="utf-8") if args.ledger_path else None
    if ledger_fp:
        # Record initial baseline catalogue snapshot in ledger for deterministic point-in-time batch recount
        seed_effective_ms = int((time.time() - 3500) * 1000)
        for item in live_catalogue.values():
            ledger_fp.write(
                json.dumps(
                    {
                        "record_kind": "catalogue",
                        "product_id": item["product_id"],
                        "product_name": item["product_name"],
                        "category": item["category"],
                        "price": float(item["price"]),
                        "updated_at_ms": seed_effective_ms,
                    }
                )
                + "\n"
            )

    start_monotonic = time.monotonic()
    last_cat_update = start_monotonic
    last_wm_carrier = start_monotonic
    last_report = start_monotonic

    total_emitted = 0
    duplicates_emitted = 0
    late_emitted = 0
    price_mutations = 0
    delivery_errors = 0

    def on_delivery(err: Any, _msg: Any) -> None:
        nonlocal delivery_errors
        if err is not None:
            delivery_errors += 1

    try:
        while True:
            elapsed = time.monotonic() - start_monotonic
            if args.duration_sec > 0 and elapsed >= args.duration_sec:
                break
            if args.max_events > 0 and total_emitted >= args.max_events:
                break

            in_spike = args.spike_after_sec >= 0 and args.spike_after_sec <= elapsed < (
                args.spike_after_sec + args.spike_duration_sec
            )
            effective_rate = args.rate * (args.spike_multiplier if in_spike else 1.0)

            slice_sec = 0.05
            batch_target = max(1, int(round(effective_rate * slice_sec)))
            slice_start = time.monotonic()

            now_ms = int(time.time() * 1000)
            if (slice_start - last_cat_update) >= args.catalogue_update_interval_sec:
                mutation = generate_price_mutation(sim.rng, live_catalogue, now_ms)
                payload = encode_confluent_avro(prod_schema_id, prod_schema, mutation)
                producer.produce(
                    "products.catalogue.v1",
                    key=mutation["product_id"].encode("utf-8"),
                    value=payload,
                    on_delivery=on_delivery,
                )
                if ledger_fp:
                    ledger_fp.write(json.dumps({"record_kind": "catalogue", **mutation}) + "\n")
                price_mutations += 1
                last_cat_update = slice_start

            if (slice_start - last_wm_carrier) >= 1.0:
                # Keep all 12 compacted dimension partitions' event-time watermarks advancing continuously
                for p_idx in range(12):
                    cat_carrier = {
                        "product_id": f"SKU-WM-FLUSH-{p_idx:02d}",
                        "product_name": "Watermark Carrier",
                        "category": "Internal",
                        "price": 0.0,
                        "updated_at_ms": now_ms,
                    }
                    producer.produce(
                        "products.catalogue.v1",
                        partition=p_idx,
                        key=cat_carrier["product_id"].encode("utf-8"),
                        value=encode_confluent_avro(prod_schema_id, prod_schema, cat_carrier),
                        on_delivery=on_delivery,
                    )
                last_wm_carrier = slice_start

            for _ in range(batch_target):
                if args.max_events > 0 and total_emitted >= args.max_events:
                    break
                now_ms = int(time.time() * 1000)
                rec, is_dup = sim.next_event(now_ms)
                payload = encode_confluent_avro(click_schema_id, click_schema, rec)
                while True:
                    try:
                        producer.produce(
                            args.topic,
                            key=rec["user_id"].encode("utf-8"),
                            value=payload,
                            on_delivery=on_delivery,
                        )
                        break
                    except BufferError:
                        producer.poll(0.05)

                if ledger_fp:
                    ledger_fp.write(json.dumps({"record_kind": "click", "is_duplicate": is_dup, **rec}) + "\n")

                total_emitted += 1
                if is_dup:
                    duplicates_emitted += 1
                if rec["is_synthetic_late"]:
                    late_emitted += 1

            producer.poll(0)

            if (time.monotonic() - last_report) >= 5.0:
                actual_eps = total_emitted / max(time.monotonic() - start_monotonic, 0.001)
                mode = f"SPIKE x{args.spike_multiplier:g}" if in_spike else "NORMAL"
                print(
                    f"[generator] mode={mode} emitted={total_emitted} "
                    f"(dups={duplicates_emitted}, extreme_late={late_emitted}, price_updates={price_mutations}) "
                    f"avg_rate={actual_eps:.1f} ev/s"
                )
                last_report = time.monotonic()

            spent = time.monotonic() - slice_start
            if spent < slice_sec:
                time.sleep(slice_sec - spent)

        if args.flush_watermark:
            if args.flush_offset_sec <= 0.0:
                # Advance real wall-clock past the 5s bounded-out-of-orderness window without emitting future timestamps
                time.sleep(5.2)
            flush_ts_ms = int(time.time() * 1000) + int(round(args.flush_offset_sec * 1000.0))
            run_tag = uuid.uuid4().hex[:8]
            for p_idx in range(12):
                cat_sentinel = {
                    "product_id": f"SKU-WM-FLUSH-{p_idx:02d}",
                    "product_name": "Watermark Carrier",
                    "category": "Internal",
                    "price": 0.0,
                    "updated_at_ms": flush_ts_ms,
                }
                producer.produce(
                    "products.catalogue.v1",
                    partition=p_idx,
                    key=cat_sentinel["product_id"].encode("utf-8"),
                    value=encode_confluent_avro(prod_schema_id, prod_schema, cat_sentinel),
                )
                click_sentinel = {
                    "event_id": f"wm-flush-{run_tag}-{p_idx:02d}",
                    "user_id": f"wm-user-{p_idx:02d}",
                    "client_session_id": f"wm-sess-{p_idx:02d}",
                    "event_type": "view",
                    "product_id": "SKU-1001",
                    "quantity": 1,
                    "device": "web",
                    "page_url": "/_wm",
                    "event_ts_ms": flush_ts_ms,
                    "created_at_ms": int(time.time() * 1000),
                    "is_synthetic_late": False,
                }
                producer.produce(
                    args.topic,
                    partition=p_idx,
                    key=click_sentinel["user_id"].encode("utf-8"),
                    value=encode_confluent_avro(click_schema_id, click_schema, click_sentinel),
                )

        producer.flush(30)
    finally:
        if ledger_fp:
            ledger_fp.close()

    total_sec = max(time.monotonic() - start_monotonic, 0.001)
    summary = {
        "total_emitted": total_emitted,
        "unique_emitted_approx": total_emitted - duplicates_emitted,
        "duplicates_emitted": duplicates_emitted,
        "extreme_late_emitted": late_emitted,
        "price_mutations": price_mutations,
        "delivery_errors": delivery_errors,
        "duration_sec": round(total_sec, 3),
        "avg_events_per_sec": round(total_emitted / total_sec, 2),
    }
    print(f"[generator] Completed: {json.dumps(summary)}")
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Synthetic e-commerce clickstream & temporal catalogue generator")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    p.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085/apis/ccompat/v7"),
    )
    p.add_argument("--topic", default="clicks.raw.v1")
    p.add_argument("--rate", type=float, default=250.0, help="Baseline target events/second")
    p.add_argument("--duration-sec", type=float, default=65.0, help="Run duration in seconds (0 = until max-events)")
    p.add_argument("--max-events", type=int, default=0, help="Optional cap on total emitted events")
    p.add_argument("--user-pool", type=int, default=600)
    p.add_argument("--duplicate-rate", type=float, default=0.03, help="Fraction of duplicate event_id retries")
    p.add_argument("--ooo-rate", type=float, default=0.04, help="Fraction of mild (<4s) out-of-order events")
    p.add_argument("--late-rate", type=float, default=0.015, help="Fraction of extreme (>45s) late events")
    p.add_argument("--spike-after-sec", type=float, default=-1.0, help="Start traffic spike after N seconds (-1=off)")
    p.add_argument("--spike-duration-sec", type=float, default=15.0)
    p.add_argument("--spike-multiplier", type=float, default=10.0)
    p.add_argument("--catalogue-update-interval-sec", type=float, default=4.0)
    p.add_argument("--compression", default="lz4", choices=["none", "lz4", "zstd", "snappy", "gzip"])
    p.add_argument("--linger-ms", type=int, default=20)
    p.add_argument("--batch-size", type=int, default=65536)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--ledger-path", default="", help="Optional JSONL ledger path for deterministic batch recount")
    p.add_argument("--flush-watermark", action="store_true", help="Advance watermarks across all partitions on exit")
    p.add_argument(
        "--flush-offset-sec",
        type=float,
        default=0.0,
        help="Optional future offset seconds added on --flush-watermark (0.0 waits 5.2s real wall-clock)",
    )
    return p


if __name__ == "__main__":
    run_producer(build_arg_parser().parse_args())
