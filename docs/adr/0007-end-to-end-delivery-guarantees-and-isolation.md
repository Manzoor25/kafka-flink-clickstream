# ADR-0007: End-to-End Delivery Semantics, Checkpoint Cadence, and `read_committed` Isolation

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

We require end-to-end accuracy (streaming totals matching a batch recount to the cent even across TaskManager crashes) **and** dashboard freshness of only a few seconds.

Flink's Kafka transactional sink (`sink.delivery-guarantee = 'exactly-once'`) uses two-phase commit (2PC) tied to Flink checkpoints: records are written to Kafka immediately in an open transaction, **but downstream consumers configured with `isolation.level=read_committed` cannot read them until the checkpoint completes and Flink commits the transaction**.

Therefore, a 30-second or 60-second checkpoint interval would make `read_committed` downstream views 30–60 seconds stale—violating the freshness SLA! Conversely, reading with `read_uncommitted` would expose uncommitted duplicate/aborted records whenever a TaskManager crashes and rolls back.

## Decision

1. Set Flink's RocksDB incremental checkpoint interval to **`2s`** (`execution.checkpointing.interval: 2s`, `min-pause: 500ms`, `mode: EXACTLY_ONCE`) backed by S3/MinIO.
2. Configure every Flink SQL and DataStream Kafka sink with `exactly-once` delivery, deterministic `transactional-id-prefix` values, and `transaction.timeout.ms = 600000` (strictly below broker `transaction.max.timeout.ms = 900000`).
3. Configure ClickHouse's `librdkafka` consumer with `<isolation_level>read_committed</isolation_level>` and `stream_flush_interval_ms = 500`, landing into idempotent `ReplacingMergeTree` tables keyed by natural primary keys (`event_id`, `(window_start, window_end)`, `(user_id, session_start, session_end)`).

## Consequences

- **Positive:** Achieves sub-3-second end-to-end freshness (`p50` ~1.5–2.5 s, `p95` < 5 s) while preserving strict Kafka transactional isolation **and** idempotent sink storage across crashes or replays.
- **Negative:** A 2-second checkpoint cadence increases S3 `PUT` request rate (~30 checkpoints/min per job). At 100+ TaskManagers in production, tune checkpoint interval to `5s–10s` or enable Changelog State Backend (`DSTC`) to decouple durability latency from SST compaction.
