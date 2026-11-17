# 10 Customer & Staff-Level Interview Questions on This Architecture

Below are ten high-signal design, failure-mode, and trade-off questions an interviewer, principal data architect, or e-commerce customer would ask about this Kafka + Flink + ClickHouse clickstream stack—along with concrete answers grounded in this implementation.

---

## 1. Why partition `clicks.raw.v1` by `user_id` instead of `product_id`, `session_id`, or `event_id`—and how do you handle hot keys?

**Answer:**
Partition key selection trades off **broker write balance**, **per-entity ordering**, and **Flink shuffle cost**:
- **`product_id` fails during flash sales:** When a flagship SKU drops in price by 40%, 30–50% of site traffic targets a single `product_id`, saturating one broker partition and stalling partition-aligned watermarks.
- **`event_id` destroys locality:** Every event gets a random UUID, giving uniform broker spread, but loses per-user arrival ordering and forces a full network shuffle (`keyBy`) before sessionisation and funnel pattern matching.
- **`client_session_id` fragments anonymous-to-logged-in journeys:** Mobile WebViews and expired browser tabs rotate session tokens mid-visit (`user_id` stays stable once authenticated or cookied).
- **`user_id` (chosen in [ADR-0005](adr/0005-topic-partitioning-key-and-compression.md))** has high cardinality across hundreds of thousands of shoppers, distributes uniformly under Kafka's Murmur2 hash, preserves causal per-user event ordering inside a single partition, and aligns partition-level watermarks with `SESSION` windows.

**Mitigating `user_id` skew (bots & crawlers):** If an unauthenticated traffic scraper emits 20,000 clicks/sec under `user_id = "anonymous"` or a single synthetic account, one partition still hotspots. In production, the edge ingestion API enforces per-identity token-bucket rate limits and routes null/unauthenticated identifiers to a salted fallback key (`anon-<random_0..11>`) before producing to Kafka.

---

## 2. You combined an event-time temporal join (`FOR SYSTEM_TIME AS OF`) with `exactly-once` 2PC Kafka sinks. How do those two choices set a mathematical floor on dashboard freshness?

**Answer:**
Many teams assume non-windowed enrichment (`clicks LEFT JOIN catalogue`) streams through in tens of milliseconds. That is only true for **processing-time lookup joins** (`FOR SYSTEM_TIME AS OF PROCTIME()`), which sacrifice historical determinism on replay.

When you use an **event-time temporal join** (`FOR SYSTEM_TIME AS OF c.event_ts`) **plus** Flink **two-phase commit (2PC)** Kafka sinks (`sink.delivery-guarantee = 'exactly-once'`), every click must clear three sequential gates before appearing on Grafana:
1. **Watermark Holdback ($W$):** Flink's `TemporalRowTimeJoinOperator` buffers each click in RocksDB `leftState` until $\min(\text{WM}_{\text{clicks}}, \text{WM}_{\text{catalogue}}) \ge c.\text{event\_ts}$. If it emitted earlier, a delayed price mutation on `products.catalogue.v1` with `updated_at <= c.event_ts` could arrive within the out-of-orderness bound and invalidate the joined price. With `WATERMARK = event_ts - INTERVAL '5' SECOND`, this adds **$5.0\text{ s}$** of deliberate event-time holdback.
2. **2PC Checkpoint Commit ($\approx C / 2$ average, $C$ worst case):** Flink writes enriched records into an open Kafka transaction immediately, but downstream consumers running `isolation.level=read_committed` are blocked at the Last Stable Offset (`LSO`) until the next Flink checkpoint barrier completes and writes the Kafka `COMMIT` marker. With `execution.checkpointing.interval = 2s`, this adds **$\sim 1.0\text{ s}$** on average (up to $2.0\text{ s}$).
3. **Serving Flush ($\approx F / 2$ average):** ClickHouse's `Kafka` engine flushes batches into `MergeTree` every `stream_flush_interval_ms = 500` (**$\sim 0.25\text{ s}$** average).

$$\text{Expected } p_{50}\text{ Freshness} \approx W + \frac{C}{2} + \frac{F}{2} = 5.0\text{ s} + 1.0\text{ s} + 0.25\text{ s} \approx 6.25\text{ s}$$

If a customer demands `< 2.5 s` per-click latency instead of `~6–7 s`, you must narrow the out-of-orderness bound $W$ (for example to `1s`) and checkpoint cadence $C$ (to `1s`), accepting that clicks delayed by `1–5 s` in mobile networks will miss the watermark.

---

## 3. When a TaskManager dies mid-stream, how do Flink, Kafka, and ClickHouse prevent duplicate revenue—and why isn't Kafka `exactly-once` sufficient on its own?

**Answer:**
End-to-end correctness requires **two cooperating mechanisms** across the Flink $\rightarrow$ Kafka $\rightarrow$ ClickHouse boundary ([ADR-0007](adr/0007-end-to-end-delivery-guarantees-and-isolation.md)):

1. **Flink $\rightarrow$ Kafka (Transactional 2PC + `read_committed`):**
   Suppose `flink-tm-2` processes 900 clicks after Checkpoint `N`, writes them into an uncommitted Kafka transaction on `clicks.enriched.v1`, and then dies (`docker kill flink-tm-2` in `make demo-kill-tm`). When `flink-tm-1` restores Checkpoint `N` from MinIO S3 (`s3://flink-state/checkpoints/...`), Flink fences the dead producer epoch and aborts the dangling transaction, writing an `ABORT` control marker into Kafka. Because ClickHouse configures `<isolation_level>read_committed</isolation_level>`, `librdkafka` skips the 900 aborted records completely and only reads the re-computed records committed in Checkpoint `N+1`.
2. **Why `exactly-once` Kafka is NOT enough at the ClickHouse hop:**
   ClickHouse's `Kafka` table engine provides **at-least-once** ingestion into local `MergeTree` parts (if ClickHouse crashes after inserting a block into `MergeTree` but before committing consumer offsets to Kafka, it replays that block on restart). Furthermore, Flink SQL retractable/upsert streams (such as Top-N rankings or late session merges) emit updated versions of the same logical key.
   Therefore, every ClickHouse destination table uses **`ReplacingMergeTree(version_ms)`** keyed by natural deterministic keys (`event_id`, `(window_start, window_end)`, `(user_id, session_start, session_end)`), and dashboard queries read with `FINAL`. Even if a replay (`make demo-reprocess`) writes the entire history a second time, `ReplacingMergeTree` collapses identical keys deterministically so revenue never double-counts.

---

## 4. Why did you pick ClickHouse over Apache Pinot—and when would you reverse that choice?

**Answer:**
Recorded in [ADR-0003](adr/0003-clickhouse-over-pinot-serving-store.md):
- **Why ClickHouse wins here:** A single native C++ ClickHouse container (~350 MB RAM) natively consumes transactional `read_committed` Kafka topics via Materialized Views every `500 ms`, supports idempotent key-level deduplication/updates via `ReplacingMergeTree ... FINAL` without strict partition co-location constraints, and executes multi-table analytical SQL joins/quantiles in single-digit milliseconds alongside a 9-container local/CI stack. Running Apache Pinot requires four separate JVM components (Controller, Broker, Server, ZooKeeper) consuming 4–6 GB RAM before ingesting a single click.
- **When to switch to Apache Pinot:** If this internal ops dashboard (dozens of concurrent analysts at `2s` refresh) turned into a **customer-facing merchant analytics portal serving 15,000+ concurrent QPS** with strict `< 20 ms` `p99` SLAs over billions of rows. At high concurrency, ClickHouse's read-time `FINAL` merge overhead becomes CPU-expensive, whereas Pinot's Star-Tree pre-aggregation indexes, inverted/range indexes, and partitioned memory-mapped real-time upserts sustain 10k+ QPS predictably.

---

## 5. What happens to `FOR SYSTEM_TIME AS OF` when the compacted `products.catalogue.v1` topic has no price updates for an hour—or during a full historical replay?

**Answer:**
This is one of the sharpest production traps in Flink SQL:
1. **Live quiet-dimension watermark stall (and the short `idle-timeout` trap):** `products.catalogue.v1` has 12 partitions, while real SKU prices only change occasionally. Any partition without recent updates holds its split watermark at its last event timestamp, freezing $\min(\text{WM}_{\text{clicks}}, \text{WM}_{\text{catalogue}})$ and buffering incoming clicks in RocksDB! Worse, naively setting `table.exec.source.idle-timeout` to `2 s` has two traps: (a) Flink SQL's source idle-timeout operates at the *subtask* output level, so if 1 of 4 assigned catalogue partitions updates every 4s, the subtask never goes idle while its 3 quiet partitions still hold back `WatermarkOutputMultiplexer`; and (b) during TaskManager failover, `2 s` is shorter than S3 RocksDB checkpoint restore jitter, prematurely marking a restoring click subtask `IDLE` and letting faster subtasks advance `TemporalRowTimeJoinOperator` cleanup ahead of backlog clicks. We solve both cleanly by emitting lightweight 1-second partition watermark carriers explicitly across all 12 catalogue partitions (`partition=0..11`, `SKU-WM-FLUSH-00..11`) and setting `table.exec.source.idle-timeout = '30 s'` as a safe outage backstop.
2. **Historical Kappa replay skew (`earliest-offset`):** During `make demo-reprocess`, `products.catalogue.v1` contains dozens of compacted price records while `clicks.raw.v1` contains tens of thousands of clicks. Without cross-source watermark coordination, the tiny catalogue stream can race to current timestamps in milliseconds. Flink's `TemporalRowTimeJoinOperator` stores full versioned history `MapState<Timestamp, RowData>` per `product_id` and only evicts superseded versions older than the **combined operator watermark** $\min(\text{WM}_{\text{left}}, \text{WM}_{\text{right}})$. For multi-terabyte multi-day backfills in production, you must also enable **FLIP-217 source watermark alignment** (`scan.watermark.alignment.group` + `scan.watermark.alignment.max-drift`) so fast partitions pause `poll()` when they drift more than e.g. `30 s` ahead of slower click partitions.

---

## 6. How is RocksDB state bounded across deduplication, temporal joins, `MATCH_RECOGNIZE`, and windows—and what is the memory/disk footprint at 50,000 events/sec?

**Answer:**
Unbounded streaming state is a guaranteed OOM/disk-full outage. Every stateful operator has an explicit retention bound (detailed math in [`docs/design.md`](design.md)):
- **`event_id` Deduplication (`KeepFirstRow` / `EventIdDeduplicator`):** Configured with a `24-hour` state TTL (`SET 'table.exec.state.ttl' = '86400 s'` in SQL and `StateTtlConfig.newBuilder(Duration.ofHours(24))` in DataStream). At local/moderate workloads this keeps full-day retry protection effortless; at $50{,}000\text{ events/s}$ sustained peak, tuning the dedup TTL to $1\text{ hour}$ ($1.8 \times 10^8$ unique IDs $\times \sim 96\text{ B}$ RocksDB key+metadata) bounds compressed SST state to $\approx \mathbf{17.3\text{ GB}}$ across the cluster (~$1.44\text{ GB}$ per partition), versus $\sim 415\text{ GB}$ on NVMe for a full 24-hour window. If a producer retries an identical `event_id` after the Flink TTL expires, Flink passes it through, **but** ClickHouse's `ReplacingMergeTree` on `ORDER BY (event_id)` still deduplicates it at the serving layer (`clicks_enriched`), leaving only closed 1-minute window aggregates subject to a rare ultra-late retry.
- **`MATCH_RECOGNIZE` Funnel:** Bounded by `WITHIN INTERVAL '10' MINUTE` + `1 h` idle state TTL; abandoned partial funnels expire automatically after 10 minutes of event time.
- **Temporal Catalogue State:** Proportional to `|SKUs| x active_revisions_within_watermark` ($100{,}000\text{ SKUs} \times 500\text{ B} \approx \mathbf{50\text{ MB}}$—tiny).
- **Window State (`1m` Tumble, `5m/1m` Hop, `30s` Session):** Pre-aggregated accumulators cleared as soon as `watermark >= window_end + allowedLateness` (`< 500 MB` at 50k ev/s).

---

## 7. Why run both Flink SQL (`01_pipeline.sql`) and a Java DataStream job (`SessionAnalyticsJob.java`) instead of 100% SQL?

**Answer:**
Flink SQL expresses 85% of analytical logic—`TUMBLE`/`HOP`/`SESSION` TVFs, `ROW_NUMBER` deduplication, `FOR SYSTEM_TIME AS OF` temporal joins, `MATCH_RECOGNIZE` CEP funnels, and `WindowRank` Top-N—in 250 declarative lines that compile via Calcite into optimized Tungsten/Blink binary row operators.

However, pure Flink SQL hits three sharp operational limits where the **DataStream API** shines (`SessionAnalyticsJob.java`):
1. **Dead-letter / Late Event Side Outputs:** Flink SQL silently drops records arriving past `watermark + allowedLateness` (increasing only an internal metric counter). In `SessionAnalyticsJob`, `.sideOutputLateData(LATE_EVENTS_TAG)` captures every dropped late `ClickEvent`, computes its exact `lateness_ms = currentWatermark - event_ts_ms`, and streams structured audit records to `clicks.late.v1` $\rightarrow$ `analytics.late_events_audit`.
2. **Explicit Operator UIDs for Evolvable Savepoints:** In DataStream, attaching `.uid("deduplicate-by-event-id")` and `.uid("user-session-window-agg")` guarantees deterministic state mapping across topology edits (`v1` $\rightarrow$ `v2` in `make demo-savepoint`). In Flink SQL, changing a query plan often alters planner-generated operator hashes unless compiled and frozen via `COMPILE PLAN` JSON.
3. **Fine-grained RocksDB Compaction & Custom Triggers:** DataStream exposes `StateTtlConfig.cleanupInRocksdbCompactFilter(1000)` and custom early/late firing triggers per key.

---

## 8. Walk through upgrading a live Flink job without losing in-flight sessions. What kinds of changes break savepoint restore?

**Answer:**
Demonstrated end to end in `scripts/demos/demo_savepoint_upgrade.sh` (`make demo-savepoint`):
1. Execute atomic drain/stop with savepoint:
   `flink stop --savepointPath s3://flink-state/savepoints <jobId>`
   Flink injects a savepoint barrier at the Kafka sources, snapshots all open `EventTimeSessionWindows` accumulators, RocksDB `seen_event` keys, and exact Kafka partition offsets to MinIO S3 in canonical format, commits the final 2PC Kafka transactions, and stops cleanly.
2. Submit the upgraded binary (`--job-version v2`, which adds buyer/cart-abandoner intent classification) from that S3 URI with a fresh transactional prefix:
   `flink run -d -s s3://flink-state/savepoints/savepoint-... -c io.clickstream.flink.SessionAnalyticsJob ... --job-version v2`
3. Flink matches `.uid(...)` hashes in the savepoint metadata to the new job graph, seeds RocksDB state for open user sessions, resumes from the exact saved Kafka offsets, and emits `v2` sessions seamlessly once their 30-second inactivity gap expires.

**What breaks savepoint compatibility:**
- Changing or omitting an existing stateful operator's `.uid(...)` (unless `--allowNonRestoredState` intentionally drops that state).
- Changing the `keyBy(...)` data type (changes RocksDB key-group Murmur hash partitioning).
- Incompatible state schema mutations (e.g., removing/retyping POJO fields without registering a custom `TypeSerializerSnapshot` migration, or switching between incompatible SQL plans without a compiled JSON plan).

---

## 9. During a 10x traffic spike, how do consumer lag and Flink backpressure interact, and why must you watch checkpoint alignment duration alongside lag?

**Answer:**
Demonstrated in `scripts/demos/demo_traffic_spike.sh` (`make demo-spike`, surging $10\times$ from $450$ to $4{,}500\text{ events/s}$):
- **Credit-based backpressure propagation:** When RocksDB point lookups (`dedup` + `temporal join` + `CEP`) or Kafka 2PC sinks hit high `busyTimeMsPerSecond`, downstream task input buffers fill, revoking network credits from upstream `Source: clicks_raw` subtasks (`backPressuredTimeMsPerSecond` rises).
- **Kafka absorbs the shock safely:** Once source subtasks stop polling `clicks.raw.v1`, memory inside Flink stays flat—excess traffic accumulates safely on disk inside Kafka's 3-broker replicated log (`consumer_lag` climbs). When the burst subsides, Flink drains `flink-*` source lag back to `0`.
- **The hidden danger—aligned checkpoint barriers:** With `EXACTLY_ONCE` aligned checkpoints every `2s`, checkpoint barriers travel inline inside the data channels. Under heavy backpressure (`backpressure_ratio > 0.85`), barriers queue behind full network buffers, causing checkpoint alignment durations to spike. If checkpoints exceed `execution.checkpointing.timeout` (`2min`), checkpoints fail and 2PC Kafka transactions stop committing (`read_committed` dashboard freezes!). **How we solve this (`docker/flink/config.yaml`):** we enable **FLIP-76 hybrid unaligned checkpoints** (`execution.checkpointing.unaligned.enabled: true` + `aligned-checkpoint-timeout: 5s`), keeping steady-state checkpoints lightweight and aligned (`< 500 ms`) while allowing barriers delayed past `5 s` during spikes or 60k-event Kappa backfills to overtake in-flight channel buffers cleanly. In production, combine this with reactive TaskManager autoscaling up to all 12 Kafka partitions via the Flink Kubernetes Operator.

---

## 10. Why does earliest-offset replay (`make demo-reprocess`) eliminate a separate nightly Lambda batch pipeline—and what are the limits of pure Kafka-backed Kappa architecture?

**Answer:**
In a classic **Lambda architecture**, teams maintain two codebases: a Flink/Spark Streaming speed layer for real-time dashboards and a nightly Spark/Airflow/dbt batch job to "correct" numbers the next morning. In practice, two codebases in two SQL dialects inevitably define session boundaries, bot filters, or temporal price joins slightly differently—creating permanent reconciliation drift between live and historical numbers.

In our **Kappa architecture** (`scripts/demos/demo_reprocess_earliest.sh`), Kafka holds the immutable raw fact stream (`clicks.raw.v1`) and compacted versioned dimension log (`products.catalogue.v1`). When we truncate all six ClickHouse analytical tables to `0` and restart the **exact same `01_pipeline.sql` DAG** with `scan.startup.mode = 'earliest-offset'`, deterministic event-time watermarks, `event_id` deduplication, and `FOR SYSTEM_TIME AS OF` temporal joins reconstruct every window, funnel stage, and revenue dollar identically (`verify_recount.py` diff = `0`).

**Where pure Kafka retention hits its limit in production:** Keeping 2–3 years of raw clickstream on NVMe broker disks at $50{,}000\text{ events/s}$ ($\sim 1.1\text{ TB/day}$ raw, $\sim 350\text{ GB/day}$ compressed) is expensive, and replaying 400 TB sequentially through 12 Kafka partitions takes hours. Production Kappa solves this with **Kafka Tiered Storage (KIP-405)** to S3—or **Apache Iceberg / Apache Paimon** lakehouse tables synced from `clicks.raw.v1`—where Flink's unified Hybrid Source (`HybridSource`) reads multi-month historical Parquet splits from S3 at massive parallelism and seamlessly hands off at a specific watermark/offset to live Kafka partitions using the exact same Flink SQL query.
