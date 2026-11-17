# ADR-0005: Topic Partition Sizing, `user_id` Keying, and Compression/Batching Defaults

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

High-volume clickstream ingestion requires principled partition sizing, a partition key that aligns with stateful Flink operators without creating hot partitions, and empirically validated producer compression/batching settings.

### Partition Count Calculation
Target production peak: $T_{\text{peak}} = 50{,}000\text{ events/s}$ (average 480 B JSON / ~260 B Avro $\approx 13\text{ MB/s}$ serialized).
- Producer per-partition ceiling: $P_{\text{prod}} \approx 15{,}000\text{ events/s}$.
- Stateful Flink consumer per-subtask ceiling (RocksDB dedup + temporal join + CEP + window state): $C_{\text{flink}} \approx 8{,}500\text{ events/s}$.
- Required 2x replay/failover catch-up headroom ($M = 2.0$):
  $$N_{\text{partitions}} = \left\lceil \frac{50{,}000 \times 2.0}{8{,}500} \right\rceil = 12\text{ partitions}$$
12 partitions divide evenly across 3 brokers (4 leaders/broker) and allow clean Flink parallelism scaling ($p \in \{1, 2, 3, 4, 6, 12\}$).

### Partition Key Candidates
- `event_id`: Uniform distribution, but forces network shuffle (`keyBy`) for every user session and funnel operator, and loses per-user broker ordering.
- `product_id`: Severe hot-key skew during flash sales (one viral SKU saturates 1 partition and stalls watermarks).
- `session_id`: Client session IDs rotate, expire, or arrive missing on unauthenticated landing views, splitting one user across partitions.
- **`user_id` (Chosen):** High cardinality, near-uniform Murmur2 hash distribution, preserves strict per-user arrival order inside one partition, and aligns Flink's partition-local watermarks with sessionisation and `MATCH_RECOGNIZE` funnel evaluation.

## Decision

1. Create `clicks.raw.v1` and `products.catalogue.v1` (`cleanup.policy=compact`) with **12 partitions**, `replication.factor=3`, `min.insync.replicas=2` (curated low-volume aggregate output topics use **6 partitions**).
2. Key `clicks.raw.v1` by **`user_id`** and `products.catalogue.v1` by **`product_id`**.
3. Benchmark `none`, `lz4`, and `zstd` across `(linger.ms, batch.size)` profiles via `benchmarks/benchmark_producer.py`; default the live generator to **`lz4`** with **`linger.ms=20`** and **`batch.size=65536`** (64 KiB) for low-latency streaming, while recommending `zstd` + `linger.ms=50, batch.size=131072` (128 KiB) when cross-AZ broker bandwidth/storage cost dominates.

## Consequences

- **Positive:** Eliminates product hot-spotting, bounds end-to-end producer batching wait to $\le 20\text{ ms}$, and guarantees 2x catch-up capacity across all 3 brokers.
- **Negative:** Bot accounts generating millions of synthetic clicks under a single `user_id` can skew one partition; production edge gateways should rate-limit or salt known crawler IDs.
