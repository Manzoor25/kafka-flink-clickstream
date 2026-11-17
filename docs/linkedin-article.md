# From click to dashboard in seconds: a real-time analytics stack on Kafka and Flink

Most real-time analytics demos cheat in at least one of three ways. They read JSON off a single Kafka broker with `acks=1`, they join clickstreams against a mutable lookup table that silently rewrites yesterday's revenue whenever a product goes on sale, or they skip failure testing altogether and hope nobody kills a worker mid-window.

I built an open-source, end-to-end e-commerce clickstream analytics platform on **Apache Kafka 4.4.0** and **Apache Flink 2.2.1** to solve the problem properly—and then tried to break it with duplicates, out-of-order mobile clicks, mid-stream price changes, a $10\times$ traffic spike, an abrupt `SIGKILL` of an active Flink TaskManager, a stateful query upgrade, and a full historical replay from offset zero.

The entire stack, chaos scripts, and measured artifacts live here: **https://github.com/Manzoor25/kafka-flink-clickstream**

## The problem

An e-commerce operations team wants one live dashboard within seconds of reality: active shoppers, sessions, four-step funnel conversion (`view -> add_to_cart -> checkout -> purchase`) with step drop-offs, top products, and revenue per minute.

Making those numbers fast is easy. Making them **accurate** under real network conditions requires handling five failure modes at once:

1. **Duplicate retries:** Mobile SDKs and gateways retry timed-out requests, inflating conversion and revenue if counted naively.
2. **Out-of-order and late clicks:** Cellular handoffs reorder events by seconds; backgrounded tabs flush clicks a minute late.
3. **Promotional price changes:** If a SKU drops from $\$200$ to $\$150$ at `12:04:30`, a `12:04:10` purchase must keep its $\$200$ price—even on historical replay.
4. **Dirty crash reads:** When a stream worker dies mid-checkpoint, consumers ignoring Kafka transaction markers read uncommitted records and double-count after recovery.
5. **Dual-codebase drift:** Running separate streaming and nightly batch pipelines guarantees two conflicting definitions of a session.

## The design

Everything runs locally and in CI across **9 Docker containers**:

- **Ingestion & governance:** A 3-broker **Apache Kafka 4.4.0** KRaft cluster (`replication.factor=3`, `min.insync.replicas=2`) with **Apicurio Registry 3.3.4** enforcing `BACKWARD`-compatible Confluent binary Avro schemas. `clicks.raw.v1` uses 12 partitions keyed by `user_id` to preserve per-shopper order without single-SKU flash-sale hotspots; `products.catalogue.v1` is a 12-partition compacted changelog keyed by `product_id`.
- **Stateful stream compute:** **Apache Flink 2.2.1** using the **RocksDB** state backend with 2-second incremental SST checkpoints to **MinIO S3** and FLIP-76 hybrid unaligned checkpoints (`aligned-checkpoint-timeout: 5s`). A Flink SQL job (`01_pipeline.sql`) handles 5-second bounded-out-of-orderness watermarks, `ROW_NUMBER()` deduplication by `event_id`, point-in-time temporal joins (`FOR SYSTEM_TIME AS OF c.event_ts`) against the compacted `upsert-kafka` catalogue, 1-minute tumbling windows, 5-minute hopping windows (1-minute slide), `SESSION` window TVFs, `MATCH_RECOGNIZE` four-step funnels, and `WindowRank` Top-10 products. Alongside SQL, a Java DataStream API job (`SessionAnalyticsJob`) runs 30-second event-time session windows with 15-second allowed lateness and routes extreme late arrivals via side output to `clicks.late.v1`.
- **Transactional handoff & serving:** Every Flink sink commits to Kafka via two-phase commit (`exactly-once`). **ClickHouse 25.8 LTS** consumes those topics with `isolation_level=read_committed` into deterministic `ReplacingMergeTree` tables, powering a provisioned 13-panel **Grafana 13.2** dashboard (`2s` refresh, `SELECT ... FINAL`).

## The one thing that surprised me

The sharpest surprise surfaced when I measured continuous creation-to-ClickHouse freshness across the temporal join.

Canary probes reached Grafana in ~10 seconds, yet population `p50` across a 68-second run jumped to **66.6 seconds**—even with CPU idle and `table.exec.source.idle-timeout = '2 s'`.

The culprit was an interaction between **partitioned watermarks** and **sparse compacted dimensions**. Our catalogue generator emitted one random SKU update every 4 seconds across 12 partitions. Whenever one partition on a Flink subtask saw an update, that subtask stayed active—never triggering the subtask-level idle timeout—while its three quiet catalogue partitions kept their split watermarks parked at seed time! Because `TemporalRowTimeJoinOperator` buffers clicks until `min(WM_clicks, WM_catalogue)` passes `click.event_ts`, three silent dimension partitions held back 17,000 clicks in RocksDB.

Advancing watermarks across all 12 catalogue partitions every second (via low partition counts on quiet dimension topics or lightweight partition heartbeats) dropped full-burst population `p50` from **66.6 seconds** to **16.44 seconds**—and steady-state dashboard `p50` to **10 seconds** (`10.73 s` via Grafana's query API), right above our 5-second watermark plus 2-second 2PC commit floor.

## Measured results

Every number below was measured automatically on this repository's 9-container Docker stack in CI:

- **Producer codec & batching benchmark (`40,000` Avro events across 12 partitions, `acks=all`):** Uncompressed `linger.ms=0` ran at **76,304 events/s** (`p95` delivery latency **320.97 ms**), while `lz4` at `linger.ms=0` managed only **36,277 events/s** (`p95` **589.11 ms**). Enabling `lz4` batching (`linger.ms=20, batch.size=64 KiB`) jumped throughput **8.8x** to **318,790 events/s** while shrinking broker wire bytes to **0.320** of raw Avro (**3.12x** compression, `p50` **15.15 ms**, `p95` **22.84 ms**). `zstd` (`linger.ms=50, batch.size=128 KiB`) sustained **229,236 events/s** and cut wire bytes to **0.189** (**5.29x** compression, `p95` **53.16 ms**).
- **Exact correctness & 2PC isolation:** Across **17,755** unique non-late events (**1,108** purchases, **$314,270.45** temporally priced revenue) with 3% injected duplicates and 4% out-of-order jitter, ClickHouse streaming totals matched an independent raw-ledger batch recount with **`0` event drift and `$0.00` revenue drift**. In our live aborted-transaction test, a `read_uncommitted` consumer observed **`1`** dirty uncommitted record while `read_committed` observed **`0`**.
- **End-to-end dashboard freshness:** Active canary probes from event creation to Grafana's `/api/ds/query` HTTP endpoint measured **`p50 = 10.73 s`** and **`p95 = 13.71 s`** (**5.73 s** `p50` pipeline overhead beyond the deliberate 5.0-second watermark holdback). Continuous population ingestion across **17,780** events measured **`p50 = 16.44 s`** and **`p95 = 27.37 s`**.
- **Four failure & operational drills:**
  1. *TaskManager `SIGKILL` under 300 ev/s load:* Flink restored incremental RocksDB state from MinIO S3 in **18.98 s**, fenced uncommitted 2PC transactions, and matched the batch recount across all **7,912** unique events (**`$0.00` drift**).
  2. *$10\times$ traffic spike (`450 -> 4,500 ev/s`):* Peak consumer lag reached **51,929** messages (peak operator busy ratio **0.793**, backpressure ratio **0.482**) with zero failed checkpoints, then drained cleanly back to **`0`** source lag.
  3. *Zero-state-loss upgrade (`v1 -> v2`):* Stopping `SessionAnalyticsJob` with an S3 savepoint and deploying `v2` preserved **2,278** `v1` sessions and emitted **467** upgraded `v2` sessions (**261** buyer/cart-abandoner intent-classified) without dropping open state.
  4. *Kappa replay from `earliest-offset`:* Truncating all 6 SQL-fed ClickHouse tables to `0` and replaying **72,037** unique non-late historical events (**$1,253,294.80** revenue) from offset zero rebuilt all 6 tables deterministically with **`$0.00` recount drift**—showing why an immutable log plus deterministic event-time SQL replaces a separate nightly batch layer.

## What I did not measure

All measurements above ran on a single-host `ubuntu-24.04` environment running all 9 containers over a local Docker bridge network. I did **not** measure cross-availability-zone replication latency across separate physical machines, multi-hour RocksDB SST compaction behavior under sustained tens-of-thousands-of-events-per-second NVMe load, real AWS S3 multipart checkpoint tail latency, or ClickHouse `FINAL` query latency under hundreds of concurrent Grafana dashboard users.

If you want to inspect the SQL, DataStream sessioniser, ADRs, or run the four chaos drills yourself (`make up && make demo`), check out the repository: **https://github.com/Manzoor25/kafka-flink-clickstream**
