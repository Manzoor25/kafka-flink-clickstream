from __future__ import annotations

import io
import json
import random
from pathlib import Path

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from generator.catalog import INITIAL_CATALOGUE, generate_price_mutation
from generator.clickstream_generator import ClickstreamSimulator
from scripts.verify_recount import compute_batch_totals, load_from_ledger

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "flink-jobs" / "src" / "test" / "resources" / "fixtures"


def test_avro_schemas_roundtrip_click_and_catalogue() -> None:
    click_schema = parse_schema(json.loads((ROOT / "schemas" / "click_event.avsc").read_text(encoding="utf-8")))
    cat_schema = parse_schema(json.loads((ROOT / "schemas" / "product_catalogue.avsc").read_text(encoding="utf-8")))

    sim = ClickstreamSimulator(seed=42, duplicate_rate=0.0)
    ev, is_dup = sim.next_event(now_ms=1_775_000_010_000)
    assert not is_dup

    buf = io.BytesIO()
    schemaless_writer(buf, click_schema, ev)
    buf.seek(0)
    decoded = schemaless_reader(buf, click_schema)
    assert decoded["event_id"] == ev["event_id"]
    assert decoded["event_type"] == "view"

    rng = random.Random(42)
    current = {p["product_id"]: dict(p) for p in INITIAL_CATALOGUE}
    mutated = generate_price_mutation(rng, current, updated_at_ms=1_775_000_020_000)
    buf2 = io.BytesIO()
    schemaless_writer(buf2, cat_schema, mutated)
    buf2.seek(0)
    decoded_cat = schemaless_reader(buf2, cat_schema)
    assert decoded_cat["product_id"] == mutated["product_id"]
    assert abs(float(decoded_cat["price"]) - float(mutated["price"])) < 1e-6


def test_simulator_respects_funnel_order_and_emits_duplicates() -> None:
    sim = ClickstreamSimulator(seed=7, user_pool_size=40, duplicate_rate=0.10, extreme_late_rate=0.05)
    by_session: dict[str, list[str]] = {}
    dup_count = 0
    late_count = 0
    ts = 1_775_000_000_000
    for _ in range(400):
        ev, is_dup = sim.next_event(ts)
        ts += 250
        if is_dup:
            dup_count += 1
            continue
        if ev["is_synthetic_late"]:
            late_count += 1
        by_session.setdefault(ev["client_session_id"], []).append(ev["event_type"])

    rank = {"view": 0, "add_to_cart": 1, "checkout": 2, "purchase": 3}
    for seq in by_session.values():
        assert seq[0] == "view"
        assert [rank[s] for s in seq] == sorted(rank[s] for s in seq)
    assert dup_count > 0
    assert late_count > 0
    assert any("purchase" in seq for seq in by_session.values())


def test_separate_simulator_runs_never_collide_event_ids_by_default() -> None:
    sim_a = ClickstreamSimulator(seed=42, duplicate_rate=0.0)
    sim_b = ClickstreamSimulator(seed=42, duplicate_rate=0.0)
    ids_a = {sim_a.next_event(1_775_000_000_000 + i)[0]["event_id"] for i in range(200)}
    ids_b = {sim_b.next_event(1_775_000_050_000 + i)[0]["event_id"] for i in range(200)}
    assert ids_a.isdisjoint(ids_b)

    det_1 = ClickstreamSimulator(seed=2026, duplicate_rate=0.0, deterministic_ids=True)
    det_2 = ClickstreamSimulator(seed=2026, duplicate_rate=0.0, deterministic_ids=True)
    assert [det_1.next_event(1_000 + i)[0]["event_id"] for i in range(25)] == [
        det_2.next_event(1_000 + i)[0]["event_id"] for i in range(25)
    ]


def test_batch_recount_matches_flink_sql_expected_fixture() -> None:
    clicks = [
        json.loads(line)
        for line in (FIXTURES / "clicks_input.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    catalogue = [
        json.loads(line)
        for line in (FIXTURES / "catalogue_input.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    expected = json.loads((FIXTURES / "expected_sql_outputs.json").read_text(encoding="utf-8"))

    totals_all = compute_batch_totals(clicks, catalogue, exclude_synthetic_late=False)
    assert totals_all["unique_events"] == expected["enriched"]["unique_event_count"] == 11
    assert totals_all["active_users"] == expected["tumbling_1m"]["active_users"] == 4
    assert totals_all["purchases"] == expected["tumbling_1m"]["purchases"] == 2
    assert totals_all["gross_revenue"] == expected["enriched"]["total_gross_revenue"] == 550.00


def test_ledger_loader_deduplicates_and_applies_temporal_price_changes(tmp_path: Path) -> None:
    sku0 = dict(INITIAL_CATALOGUE[0])
    ledger_file = tmp_path / "run.jsonl"
    entries = [
        {
            "record_kind": "catalogue",
            "product_id": sku0["product_id"],
            "product_name": sku0["product_name"],
            "category": sku0["category"],
            "price": float(sku0["price"]),
            "updated_at_ms": 1_000,
        },
        {
            "record_kind": "click",
            "event_id": "e1",
            "user_id": "u1",
            "client_session_id": "s1",
            "event_type": "purchase",
            "product_id": sku0["product_id"],
            "quantity": 1,
            "device": "web",
            "page_url": "/checkout/complete",
            "event_ts_ms": 2_000,
            "created_at_ms": 2_000,
            "is_synthetic_late": False,
        },
        # Duplicate retry with identical event_id must be deduplicated
        {
            "record_kind": "click",
            "event_id": "e1",
            "user_id": "u1",
            "client_session_id": "s1",
            "event_type": "purchase",
            "product_id": sku0["product_id"],
            "quantity": 1,
            "device": "web",
            "page_url": "/checkout/complete",
            "event_ts_ms": 2_000,
            "created_at_ms": 2_500,
            "is_synthetic_late": False,
        },
        # Price increases to 250.00 at t=3_000
        {
            "record_kind": "catalogue",
            "product_id": sku0["product_id"],
            "product_name": sku0["product_name"],
            "category": sku0["category"],
            "price": 250.00,
            "updated_at_ms": 3_000,
        },
        {
            "record_kind": "click",
            "event_id": "e2",
            "user_id": "u2",
            "client_session_id": "s2",
            "event_type": "purchase",
            "product_id": sku0["product_id"],
            "quantity": 2,
            "device": "ios",
            "page_url": "/checkout/complete",
            "event_ts_ms": 3_500,
            "created_at_ms": 3_500,
            "is_synthetic_late": False,
        },
    ]
    ledger_file.write_text("\n".join(json.dumps(x) for x in entries) + "\n", encoding="utf-8")
    clicks, cat = load_from_ledger(ledger_file)
    totals = compute_batch_totals(clicks, cat)
    assert totals["unique_events"] == 2
    assert totals["active_users"] == 2
    assert totals["purchases"] == 2
    assert totals["gross_revenue"] == round(float(sku0["price"]) + 2 * 250.00, 2)
