#!/usr/bin/env python3
"""Idempotent platform bootstrapper:
1. Creates S3 bucket `flink-state` (checkpoints + savepoints) in MinIO.
2. Creates Kafka 4.4 KRaft topics with explicit partition counts, RF=3, min.insync.replicas=2,
   and log compaction on `products.catalogue.v1`.
3. Registers Avro schemas in Apicurio Schema Registry (Confluent v7 API) with BACKWARD compatibility.
4. Applies ClickHouse Kafka Engine + Materialized View DDL (`docker/clickhouse/kafka_ingestion.sql`).
5. Seeds initial versioned product catalogue into `products.catalogue.v1` across partitions.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import struct
import sys
import time
from pathlib import Path
from typing import Any

import boto3
import clickhouse_connect
import fastavro
import requests
from botocore.client import Config as BotoConfig
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TOPICS: list[dict[str, Any]] = [
    {
        "name": "clicks.raw.v1",
        "partitions": 12,
        "rf": 3,
        "config": {
            "min.insync.replicas": "2",
            "retention.ms": str(7 * 24 * 3600 * 1000),
            "compression.type": "producer",
        },
    },
    {
        "name": "products.catalogue.v1",
        "partitions": 12,
        "rf": 3,
        "config": {
            "cleanup.policy": "compact",
            "min.insync.replicas": "2",
            "min.cleanable.dirty.ratio": "0.01",
            "min.compaction.lag.ms": "3600000",
            "segment.ms": "10000",
            "delete.retention.ms": "60000",
        },
    },
    {
        "name": "clicks.enriched.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "metrics.minute.tumbling.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "metrics.minute.hopping.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "analytics.sessions.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "analytics.sessions.sql.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "analytics.funnel.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "analytics.top_products.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
    {
        "name": "clicks.late.v1",
        "partitions": 6,
        "rf": 3,
        "config": {"min.insync.replicas": "2"},
    },
]


def ensure_minio_bucket(endpoint: str, access_key: str, secret_key: str, bucket: str = "flink-state") -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"[minio] Created bucket s3://{bucket}")
    else:
        print(f"[minio] Bucket s3://{bucket} already exists")


def ensure_kafka_topics(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    meta = admin.list_topics(timeout=15)
    existing = set(meta.topics.keys())
    to_create = [
        NewTopic(t["name"], num_partitions=t["partitions"], replication_factor=t["rf"], config=t["config"])
        for t in TOPICS
        if t["name"] not in existing
    ]
    if to_create:
        futures = admin.create_topics(to_create, operation_timeout=30)
        for name, fut in futures.items():
            fut.result()
            print(f"[kafka] Created topic {name}")
    else:
        print(f"[kafka] All {len(TOPICS)} topics already exist")


def register_schema(registry_ccompat_url: str, subject: str, schema_path: Path) -> int:
    raw_schema = schema_path.read_text(encoding="utf-8")
    # Enforce BACKWARD compatibility on the subject
    cfg_resp = requests.put(
        f"{registry_ccompat_url}/config/{subject}",
        json={"compatibility": "BACKWARD"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    if cfg_resp.status_code not in (200, 204, 404):
        cfg_resp.raise_for_status()

    resp = requests.post(
        f"{registry_ccompat_url}/subjects/{subject}/versions",
        json={"schemaType": "AVRO", "schema": raw_schema},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    resp.raise_for_status()
    schema_id = int(resp.json()["id"])
    # Re-apply subject compatibility after initial subject creation
    requests.put(
        f"{registry_ccompat_url}/config/{subject}",
        json={"compatibility": "BACKWARD"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    print(f"[schema-registry] Registered {subject} -> schema_id={schema_id} (BACKWARD)")
    return schema_id


def apply_clickhouse_ddl(host: str, port: int, user: str, password: str) -> None:
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)
    for sql_file in [
        ROOT / "docker" / "clickhouse" / "init" / "01_base_tables.sql",
        ROOT / "docker" / "clickhouse" / "kafka_ingestion.sql",
    ]:
        content = sql_file.read_text(encoding="utf-8")
        statements = [s.strip() for s in content.split(";") if s.strip()]
        for stmt in statements:
            lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("--")]
            cleaned = "\n".join(lines).strip()
            if cleaned:
                client.command(cleaned)
        print(f"[clickhouse] Applied {sql_file.name}")


def encode_confluent_avro(schema_id: int, parsed_schema: Any, record: dict[str, Any]) -> bytes:
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def seed_product_catalogue(bootstrap_servers: str, product_schema_id: int) -> None:
    from generator.catalog import INITIAL_CATALOGUE

    schema_dict = json.loads((ROOT / "schemas" / "product_catalogue.avsc").read_text(encoding="utf-8"))
    parsed = fastavro.parse_schema(schema_dict)
    producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 5, "acks": "all"})
    # Seed initial effective timestamp 1 hour before wall clock so historical/late clicks match cleanly
    effective_ms = int((time.time() - 3600) * 1000)
    for item in INITIAL_CATALOGUE:
        rec = {
            "product_id": item["product_id"],
            "product_name": item["product_name"],
            "category": item["category"],
            "price": float(item["price"]),
            "updated_at_ms": effective_ms,
        }
        payload = encode_confluent_avro(product_schema_id, parsed, rec)
        producer.produce(
            "products.catalogue.v1",
            key=item["product_id"].encode("utf-8"),
            value=payload,
        )
    # Seed explicit watermark carriers across all 12 partitions at current wall-clock so no partition starts empty
    now_ms = int(time.time() * 1000)
    for p_idx in range(12):
        wm_rec = {
            "product_id": f"SKU-WM-FLUSH-{p_idx:02d}",
            "product_name": "Watermark Carrier",
            "category": "Internal",
            "price": 0.0,
            "updated_at_ms": now_ms,
        }
        producer.produce(
            "products.catalogue.v1",
            partition=p_idx,
            key=wm_rec["product_id"].encode("utf-8"),
            value=encode_confluent_avro(product_schema_id, parsed, wm_rec),
        )
    producer.flush(15)
    print(f"[catalogue] Seeded {len(INITIAL_CATALOGUE)} products into compacted topic products.catalogue.v1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Kafka, MinIO, Schema Registry, and ClickHouse")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094"),
    )
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085/apis/ccompat/v7"),
    )
    parser.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9010"))
    parser.add_argument("--minio-user", default=os.getenv("MINIO_ROOT_USER", "localminio"))
    parser.add_argument("--minio-password", default=os.getenv("MINIO_ROOT_PASSWORD", "localminio123"))
    parser.add_argument("--clickhouse-host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--clickhouse-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "clickstream"))
    parser.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"))
    parser.add_argument("--skip-seed", action="store_true")
    args = parser.parse_args()

    ensure_minio_bucket(args.minio_endpoint, args.minio_user, args.minio_password)
    ensure_kafka_topics(args.bootstrap_servers)
    register_schema(args.schema_registry_url, "clicks.raw.v1-value", ROOT / "schemas" / "click_event.avsc")
    prod_schema_id = register_schema(
        args.schema_registry_url, "products.catalogue.v1-value", ROOT / "schemas" / "product_catalogue.avsc"
    )
    apply_clickhouse_ddl(args.clickhouse_host, args.clickhouse_port, args.clickhouse_user, args.clickhouse_password)
    if not args.skip_seed:
        seed_product_catalogue(args.bootstrap_servers, prod_schema_id)


if __name__ == "__main__":
    main()
