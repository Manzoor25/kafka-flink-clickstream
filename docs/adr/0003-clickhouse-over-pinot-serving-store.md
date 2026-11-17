# ADR-0003: ClickHouse 25.8 LTS over Apache Pinot for the Real-Time Serving Store

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Processed clickstream events and windowed aggregates in Kafka must land in a low-latency OLAP serving store that powers a dashboard refreshing every few seconds. The two candidates are **ClickHouse** and **Apache Pinot**.

| Criterion | ClickHouse 25.8 LTS | Apache Pinot |
|---|---|---|
| Local / CI Footprint | 1 container (~350 MB RAM, native C++ binary) | 4 JVM services: Controller, Broker, Server, ZooKeeper (~4–6 GB RAM) |
| Kafka Ingestion | Built-in `Kafka` table engine + Materialized Views (`stream_flush_interval_ms=500`) | Built-in real-time LLC consumer + segment flush |
| Transactional Kafka | Supports `librdkafka` `<isolation_level>read_committed</isolation_level>` directly in XML config | Consumer properties support `isolation.level`, heavier table/schema JSON management |
| Idempotent Upserts / Replays | `ReplacingMergeTree(version_ms)` + `FINAL` handles replays and Flink updates cleanly | Native real-time key upsert requires strict partition co-location & memory-resident primary key index |
| Ultra-high QPS point lookups | Very good (hundreds to low thousands QPS per node) | Superior at 10k+ QPS user-facing slices via Star-Tree & inverted indexes |

## Decision

Select **ClickHouse `25.8.33.6-alpine`** (25.8 LTS) as the real-time serving store:
- Configure `<kafka><isolation_level>read_committed</isolation_level></kafka>` in `/etc/clickhouse-server/config.d/kafka.xml`.
- Ingest from Flink's transactional Kafka output topics through ClickHouse `Kafka` engine tables (`stream_flush_interval_ms = 500`) into `ReplacingMergeTree` analytical tables via Materialized Views.
- Query analytical tables with `FINAL` (or `argMax` rollups) from Grafana using `grafana-clickhouse-datasource` (`4.23.5`).

## Consequences

- **Positive:** Sub-second Kafka-to-query visibility (~500 ms flush), zero external ingestion daemons, deterministic idempotency across TaskManager crashes and earliest-offset Kappa replays, and fast startup (<5 s) inside a single laptop or CI runner alongside 3 Kafka brokers and Flink.
- **Negative:** `SELECT ... FINAL` merges uncompacted parts at read time. At multi-billion-row production scale, partition by `toYYYYMMDD(window_start)`, keep active hot partitions small, or pre-aggregate with `AggregatingMergeTree`. If the workload shifts to 20,000+ QPS customer-facing embedded widgets needing Star-Tree indexes, Apache Pinot becomes preferable.
