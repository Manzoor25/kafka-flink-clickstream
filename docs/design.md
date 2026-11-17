# System Design: Real-Time E-Commerce Clickstream Analytics Platform

## 1. Functional & Non-Functional Requirements

### Functional Requirements
An e-commerce storefront requires a single operational dashboard covering six live views:
1. **Active Shoppers & Event Velocity:** Distinct active users, clicks/sec, and gross revenue across rolling time slices.
2. **Per-Minute Window Aggregates:** 1-minute tumbling windows (`TUMBLE`) for discrete minute-over-minute reporting and 5-minute hopping windows sliding every 1 minute (`HOP`) for smoothed trend lines and conversion ratios.
3. **User Sessionisation:** Dynamic event-time sessions closing after **30 seconds** of per-user inactivity (`SESSION` Window TVF in Flink SQL and `EventTimeSessionWindows` in the Java DataStream API), capturing session duration, page depth, conversion flag, device tier, and revenue.
4. **4-Step Causal Funnel & Drop-Offs:** Ordered within-session progression (`view -> add_to_cart -> checkout -> purchase` within 10 minutes) computed with Flink SQL `MATCH_RECOGNIZE`, emitting step counts, step-to-step drop-off percentages, and overall conversion rate.
5. **Windowed Top-N Products:** Top 10 SKUs per 1-minute tumbling window ordered by gross revenue and view volume (`WindowRank`).
6. **Point-in-Time Catalogue Price Enrichment:** Every purchase and view joins against a compacted `products.catalogue.v1` changelog topic (`upsert-kafka`) via `FOR SYSTEM_TIME AS OF c.event_ts`, picking up the exact unit price valid at the millisecond the click occurred—never retroactively mutating historical revenue when a promotion starts later.

### Non-Functional Targets
| Dimension | Design Target | Verification Mechanism |
|---|---|---|
| **End-to-End Freshness** | Dashboard within seconds of event creation (`~6.25 s` theoretical floor from `5 s` watermark + `2 s` 2PC commit; `< 15 s p50` / `< 20 s p95` active SLA on single-host 9-container CI) | `scripts/measure_freshness.py` polling Grafana `/api/ds/query` |
| **Correctness Under Faults** | `0` revenue/count drift vs deterministic raw-log batch recount after abrupt TaskManager failure | `scripts/demos/demo_kill_taskmanager.sh` + `scripts/verify_recount.py` |
| **Burst Absorption** | Survive a $10\times$ traffic surge without task failure or dropped checkpoints; drain consumer lag to `0` | `scripts/demos/demo_traffic_spike.sh` + `scripts/monitor_ops.py` |
| **Zero-State-Loss Evolution** | Upgrade live stateful logic (`v1 -> v2`) via S3 savepoints while retaining in-flight user sessions | `scripts/demos/demo_savepoint_upgrade.sh` |
| **Batch-Layer Elimination** | Deterministically rebuild all serving tables from Kafka `earliest-offset` using the identical SQL DAG | `scripts/demos/demo_reprocess_earliest.sh` |

---

## 2. Throughput, Partition Sizing & Producer Compression Maths

### 2.1 Partition Count Derivation ([ADR-0005](adr/0005-topic-partitioning-key-and-compression.md))
We size `clicks.raw.v1` for a production e-commerce peak of:
- **Peak event rate ($T_{\text{peak}}$):** $50{,}000\text{ events/s}$
- **Serialized payload ($S_{\text{avro}}$):** $\sim 255\text{ B}$ Confluent-framed binary Avro ($\sim 485\text{ B}$ equivalent JSON), yielding $\sim 12.8\text{ MB/s}$ uncompressed ingress ($38.4\text{ MB/s}$ across `replication.factor = 3`).
- **Single-partition Kafka producer/broker ceiling ($P_{\text{broker}}$):** $> 15{,}000\text{ events/s}$ with `acks=all`, `min.insync.replicas=2`, and `lz4` batching.
- **Single Flink subtask stateful ceiling ($C_{\text{flink}}$):** Each subtask maintains RocksDB `event_id` deduplication state, `TemporalRowTimeJoinOperator` left/right buffers, `MATCH_RECOGNIZE` NFA state, and 4 concurrent window operators, conservatively sustaining $C_{\text{flink}} \approx 8{,}500\text{ events/s/core}$.
- **Failover & Kappa Replay Catch-Up Multiplier ($M$):** After a TaskManager crash or savepoint upgrade, the pipeline must catch up at $2.0\times$ peak ingress ($100{,}000\text{ events/s}$).

$$N_{\text{partitions}} = \max\left(\left\lceil \frac{T_{\text{peak}} \cdot M}{P_{\text{broker}}} \right\rceil,\; \left\lceil \frac{T_{\text{peak}} \cdot M}{C_{\text{flink}}} \right\rceil\right) = \max\left(\left\lceil \frac{100{,}000}{15{,}000} \right\rceil,\; \left\lceil \frac{100{,}000}{8{,}500} \right\rceil\right) = \max(7, 12) = \mathbf{12\text{ partitions}}$$

**Why 12 is structurally clean:**
- **Even 3-broker leadership:** $12 / 3 = 4$ leader partitions and $8$ follower replicas per KRaft broker (`kafka-1`, `kafka-2`, `kafka-3`).
- **Highly composite Flink parallelism:** $12$ is divisible by $\{1, 2, 3, 4, 6, 12\}$, allowing local/CI deployment at `parallelism = 3` (4 partitions/subtask) and linear scale-out to `parallelism = 12` in production without repartitioning Kafka.
- **Co-partitioned catalogue changelog:** `products.catalogue.v1` (`cleanup.policy=compact`, keyed by `product_id`) also uses **12 partitions**, while pre-aggregated output topics (`metrics.minute.*`, `analytics.funnel.v1`, `analytics.top_products.v1`, `analytics.sessions.*`) use **6 partitions** (`2` leaders/broker) since window aggregation compresses event volume by $> 100\times$.

### 2.2 Partition Key Choice
- `clicks.raw.v1` is keyed by **`user_id`**: high cardinality prevents single-SKU flash-sale hot partitions (unlike `product_id`), keeps a shopper's cross-tab journey on a single partition (unlike rotating `client_session_id` tokens), and preserves per-user timestamp monotonicity inside each partition (unlike random `event_id`).
- `products.catalogue.v1` is keyed by **`product_id`**, required by Kafka log compaction and Flink's `upsert-kafka` primary key contract.

### 2.3 Measured Codec (`none`, `lz4`, `zstd`) & Batching (`linger.ms`, `batch.size`) Benchmark
Measured via `benchmarks/benchmark_producer.py` (`40,000` Confluent-framed Avro clickstream records per profile across **12 partitions**, `replication.factor = 3`, `min.insync.replicas = 2`, `acks = all`, `enable.idempotence = true` on the 3-broker Kafka 4.4.0 KRaft cluster):

| Codec | `linger.ms` | `batch.size` | Events/sec | Raw MB/s | Wire MB/s | Wire / Raw | ACK `p50` (ms) | ACK `p95` (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `none` | 0 | 16,384 | 76,304 | 17.69 | 18.49 | 1.045 (`0.96x`) | 215.75 | 320.97 |
| `none` | 20 | 65,536 | 326,281 | 75.66 | 78.84 | 1.042 (`0.96x`) | 15.28 | 25.06 |
| `lz4` | 0 | 16,384 | 36,277 | 8.41 | 3.14 | 0.373 (`2.68x`) | 126.05 | 589.11 |
| **`lz4` (Default)** | **20** | **65,536** | **318,790** | **73.92** | **23.67** | **0.320 (`3.12x`)** | **15.15** | **22.84** |
| `lz4` | 50 | 131,072 | 303,165 | 70.30 | 22.44 | 0.319 (`3.13x`) | 18.27 | 31.53 |
| `zstd` | 0 | 16,384 | 25,251 | 5.86 | 1.31 | 0.223 (`4.47x`) | 508.31 | 1,225.44 |
| `zstd` | 20 | 65,536 | 282,235 | 65.45 | 12.74 | 0.195 (`5.14x`) | 16.58 | 42.20 |
| **`zstd` (Storage-Opt)** | **50** | **131,072** | **229,236** | **53.16** | **10.04** | **0.189 (`5.29x`)** | **28.41** | **53.16** |

**Key Engineering Takeaways:**
1. **`linger.ms=0` starves dictionary/block compression and hammers broker I/O:** At `linger.ms=0`, per-batch message counts stay tiny, hurting `zstd` ACK `p95` (`1,225.44 ms`) and limiting `lz4` to `36,277 ev/s`. Adding just **`20 ms` of batching (`64 KiB` batch)** increases `lz4` throughput **`8.8x`** (`318,790 ev/s`, $>6\times$ our `50,000 ev/s` target peak from a single Python thread!) while dropping `p95` ACK latency from `589.11 ms` to **`22.84 ms`**.
2. **`lz4` vs `zstd` trade-off:** `lz4 (20 ms, 64 KiB)` cuts broker wire and disk replication traffic by **`3.12x`** (`0.320` wire/raw) with the lowest `p95` delivery latency (`22.84 ms`), making it our default interactive streaming profile. `zstd (50 ms, 128 KiB)` achieves **`5.29x` wire compression** (`0.189` wire/raw—saving an additional `41%` cross-AZ replication and disk bytes over `lz4`) while still sustaining `229,236 ev/s`.

---

## 3. Event-Time Semantics, Watermarks & Explicit Lateness Policy

Real mobile and browser clickstreams suffer from three distinct timing anomalies modeled in `generator/clickstream_generator.py`:
1. **At-least-once network retries (`3–5%` duplicates):** Identical `event_id` emitted twice with a newer `created_at_ms`.
2. **Mild network jitter (`~4%` out-of-order by `0.4–3.8 s`):** Events arriving slightly out of timestamp order across mobile/Wi-Fi handoffs.
3. **Offline/background-tab flushes (`~1.5%` extreme late by `45–75 s`):** Mobile tabs backgrounded during browsing and flushed a minute later.

### Three-Tier Lateness Policy
| Stream Operator | Watermark Strategy | Allowed Lateness | Behavior Beyond Lateness Bound |
|---|---|---|---|
| **Sources (`clicks_raw`, `products_catalogue`)** | `event_ts - INTERVAL '5' SECOND` + `idle-timeout = 30 s` + 1 s dimension watermark carriers | Absorbs up to `5 s` out-of-orderness cleanly | Keeps all 12 catalogue partitions advancing every second without prematurely marking restoring subtasks idle during S3 checkpoint failover |
| **SQL Temporal Join & Window TVFs (`01_pipeline.sql`)** | Driven by `min(WM_clicks, WM_catalogue)` | `0 s` post-watermark on TVFs (`5 s` total jitter window) | Late arrivals past watermark are excluded from closed immutable `1m` financial/funnel windows |
| **DataStream `SessionAnalyticsJob` (`EventTimeSessionWindows`)** | `BoundedOutOfOrderness(5 s)` + `withIdleness(30 s)` | **`+15 s` (`allowedLateness`)** on `30 s` session windows (`20 s` total tolerance) | Updates & re-emits merged session summaries to ClickHouse `ReplacingMergeTree`; events $> 20\text{ s}$ late are routed via `.sideOutputLateData(RAW_LATE_EVENTS_TAG)` to **`clicks.late.v1` $\rightarrow$ `analytics.late_events_audit`** (zero silent data loss) |

---

## 4. RocksDB State Size Estimates

All stateful operators run on Flink's **Embedded RocksDB State Backend** with **incremental SST checkpoints** to MinIO S3 (`s3://flink-state/checkpoints`). Below are steady-state disk/memory estimates at **local demo scale ($300\text{ ev/s}$)** vs **target production peak ($50{,}000\text{ ev/s}$)**:

| Operator | State Key & Structure | Retention / Cleanup | Footprint @ $300\text{ ev/s}$ | Footprint @ $50{,}000\text{ ev/s}$ |
|---|---|---|---|---|
| **1. `event_id` Dedup (`KeepFirstRow` / `ValueState<Boolean>`)** | `UUID (36 B)` + RocksDB key-group/TTL header $\approx 96\text{ B/entry}$ | `24 h` default (`1 h` tuned at 50k ev/s via compaction filter) | $2.59\text{M keys} \approx \mathbf{249\text{ MB}}$ (`24 h`) | $180\text{M keys} \approx \mathbf{17.3\text{ GB}}$ (`1 h` TTL) |
| **2. Temporal Catalogue Join (`TemporalRowTimeJoinOperator`)** | `rightState`: `MapState<ts, ProductRow>` ($\sim 450\text{ B/rev}$); `leftState`: buffered clicks within $5\text{ s}$ watermark ($\sim 320\text{ B/click}$) | `rightState` pruned past operator WM (retaining latest active price/SKU); `leftState` drained every watermark tick | $12\text{ SKUs} + 1{,}500\text{ clicks} \approx \mathbf{0.6\text{ MB}}$ | $100\text{k SKUs} + 250\text{k clicks} \approx \mathbf{125\text{ MB}}$ |
| **3. `MATCH_RECOGNIZE` 4-Step Funnel** | Per-`client_session_id` NFA state + partial event refs ($\sim 420\text{ B}$ per open session across 4 branches) | `WITHIN INTERVAL '10' MINUTE` + idle TTL | $\sim 3{,}000\text{ active sessions} \approx \mathbf{5\text{ MB}}$ | $\sim 450\text{k active sessions} \approx \mathbf{756\text{ MB}}$ |
| **4. `TUMBLE` (`1m`) & `HOP` (`5m/1m`) Windows** | Window slice accumulators + `DistinctAccumulator(user_id)` map ($\sim 48\text{ B/distinct user/slice}$) | Purged immediately when `WM >= window_end` | $\sim 600\text{ users} \times 6\text{ slices} \approx \mathbf{0.2\text{ MB}}$ | $\sim 250\text{k users} \times 6\text{ slices} \approx \mathbf{72\text{ MB}}$ |
| **5. `WindowRank` Top-10 Products** | Per-`(window, product_id)` sum + Top-10 min-heap ($\sim 180\text{ B/SKU}$) | Purged at `1m` window close | $12\text{ SKUs} \approx \mathbf{< 0.1\text{ MB}}$ | $50\text{k active SKUs/min} \approx \mathbf{9\text{ MB}}$ |
| **6. Session Windows (`30s` gap + `15s` lateness)** | `ProcessWindowFunction` buffered click list / session accumulators per active user | Purged at `max_ts + 30s + 15s` | $\sim 500\text{ open sessions} \approx \mathbf{1.2\text{ MB}}$ | $\sim 300\text{k open sessions} \approx \mathbf{290\text{ MB}}$ |
| **Total Cluster RocksDB State** | Incremental SST files on local NVMe + S3 | Continuous RocksDB background compaction | **$\approx 256\text{ MB}$** | **$\approx 18.6\text{ GB}$ (`1.55 GB` / partition)** |

With **incremental checkpointing** enabled (`execution.checkpointing.incremental: true`), each 2-second checkpoint uploads only newly flushed RocksDB SST deltas (`KB` to low `MB`) rather than full multi-gigabyte snapshots.

---

## 5. End-to-End Delivery Guarantees Per Hop ([ADR-0007](adr/0007-end-to-end-delivery-guarantees-and-isolation.md))

| Hop | Producer / Writer | Storage / Transport | Consumer / Reader | Effective Guarantee |
|---|---|---|---|---|
| **1. Edge $\rightarrow$ Raw Kafka** | Python Generator (`enable.idempotence=True`, `acks=all`, intentional `3–5%` app-level retry simulation) | `clicks.raw.v1` (`RF=3`, `min.insync.replicas=2`) | Flink `KafkaSource` (offsets stored in checkpoint state) | **At-least-once** at wire level; **deduplicated to exact-once** inside Flink by `event_id` |
| **2. Flink Internal State** | RocksDB incremental checkpoints (`interval = 2s`, `EXACTLY_ONCE` barrier alignment) | MinIO S3 (`s3://flink-state/checkpoints`) | Restored automatically on TaskManager failover | **Exactly-once state consistency** |
| **3. Flink $\rightarrow$ Curated Kafka** | Flink `KafkaSink` / SQL Kafka Sink (`sink.delivery-guarantee = 'exactly-once'`, unique `transactional-id-prefix`) | 8 curated Kafka topics with 2PC `COMMIT`/`ABORT` control markers | ClickHouse `Kafka` engine + verifiers with **`isolation.level = read_committed`** | **Exactly-once committed visibility** (uncommitted records from crashed TaskManagers are fenced, aborted, and skipped) |
| **4. Curated Kafka $\rightarrow$ ClickHouse** | ClickHouse `Kafka` engine (`stream_flush_interval_ms = 500`) + Materialized Views | `ReplacingMergeTree(version_ms)` ordered by natural deterministic PKs | Grafana (`grafana-clickhouse-datasource`) querying **`SELECT ... FINAL`** | **Effectively-once (idempotent serving)**—even at-least-once ClickHouse flushes or full Kappa replays collapse onto identical primary keys |

### End-to-End Freshness Measurements
Measured end-to-end by `scripts/measure_freshness.py` (`benchmarks/results/freshness_report.json`):
- **Active Creation $\rightarrow$ Grafana `/api/ds/query` HTTP Probe (`25/25` probes observed):**
  - **`min = 6,827.7 ms` (`6.83 s`)**, **`p50 = 10,728.7 ms` (`10.73 s`)**, **`p95 = 13,714.6 ms` (`13.71 s`)**, `max = 14,238.6 ms` (`14.24 s`).
  - Subtracting the deliberate **`5,000 ms` (`5.0 s`) event-time temporal-join watermark holdback**, pure multi-hop transport + RocksDB state + 2PC commit + ClickHouse flush + Grafana SQL query overhead is **`p50 = 5.73 s`** and **`p95 = 8.71 s`** on a shared 4-vCPU single-host Docker bridge running all 9 containers.
  - On the live provisioned Grafana dashboard (`docs/assets/dashboard.png`), rolling 45-second steady-state freshness renders at **`10 s` (`p50`)** and **`14 s` (`p95`)**.
- **Full-Workload Population Ingestion (`17,780` non-late events across the initial 68-second multi-job load):**
  - `min = 5.69 s`, **`p50 = 16.44 s`**, **`p95 = 27.37 s`**, `p99 = 29.51 s`.

### Failure & Recovery Verification Summary
| Failure Drill | Fault / Operational Action | Measured Outcome |
|---|---|---|
| **`make demo-kill-tm`** | `docker kill` active Flink TaskManager mid-stream (`300 ev/s` + duplicates + price changes) | Restored RocksDB incremental checkpoint `#68` from MinIO S3 in **`18.98 s`**, aborted uncommitted 2PC Kafka batch, **`7,912 == 7,912`** unique events, **`$120,436.95 == $120,436.95`** (**`$0.00` drift**) |
| **`make demo-spike`** | $10\times$ surge from `450 ev/s` $\rightarrow$ `4,500 ev/s` across 12 partitions | Peak total lag **`51,929`** (`flink-sql = 32,276`, `flink-ds = 14,066`), peak busy **`0.793`**, peak backpressure **`0.482`**, zero failed checkpoints (FLIP-76 `aligned-checkpoint-timeout: 5s`), drained to **`0` lag** |
| **`make demo-savepoint`** | Atomic `flink stop --savepointPath s3://flink-state/savepoints` on `v1`, deploy `--job-version v2` | Restored S3 savepoint `savepoint-991c20-d81561cdc375`; preserved **`2,278`** `v1` sessions and closed **`467`** `v2` sessions (**`261`** intent-classified) without state loss |
| **`make demo-reprocess`** | Graceful `flink stop` (committing 2PC `LSO`), `TRUNCATE` all 6 ClickHouse SQL tables (`72,358 -> 0`), replay `earliest-offset` | Rebuilt all 6 serving tables (`72,359` enriched clicks, `2,914` SQL sessions, `78` Top-N rows); raw-Kafka batch recount **`72,037 == 72,037`** non-late events, **`$1,253,294.80 == $1,253,294.80`** (**`$0.00` drift**) |

---

## 6. Serving Store Choice & Architectural Trade-Offs

### Why ClickHouse 25.8 LTS over Apache Pinot ([ADR-0003](adr/0003-clickhouse-over-pinot-serving-store.md))
- **Operational Density:** ClickHouse runs as a single ~350 MB native C++ process with built-in `librdkafka` `read_committed` consumers and Materialized Views, keeping our entire 9-container stack comfortable inside `< 7 GB` RAM on a developer laptop or standard GitHub Actions runner. Apache Pinot requires four JVM services (Controller, Broker, Server, ZooKeeper) totaling 4–6 GB RAM on its own.
- **Idempotent Replay & Late Window Updates:** `ReplacingMergeTree(version_ms)` + `FINAL` handles late session window updates (`allowedLateness = 15s`) and full `earliest-offset` Kappa replays out of the box without Pinot's strict partition-co-located memory-resident upsert index constraints.

### Key Architectural Trade-Offs Made
1. **5-Second Event-Time Temporal Join Holdback vs Ultra-Low Per-Click Latency:** Choosing `FOR SYSTEM_TIME AS OF c.event_ts` with a `5s` bounded-out-of-orderness watermark guarantees 100% deterministic point-in-time pricing on replays and tolerates up to `5s` of mobile jitter, at the cost of holding enriched clicks for `5.0s` before 2PC commit (`~6–7s` total dashboard freshness). Switching to a processing-time lookup join (`PROCTIME()`) would drop per-click latency below `2s` but break replay determinism.
2. **2-Second Aligned 2PC Checkpoints vs S3 Request Rate:** A `2s` checkpoint cadence keeps `read_committed` downstream latency tight (`~1s` average wait for `COMMIT`), at the expense of 30 checkpoint barrier cycles per minute.
3. **`SELECT ... FINAL` Read-Time Merge vs Write-Time Pre-Aggregation:** `FINAL` guarantees exact deduplicated answers immediately after ingestion without waiting for background `MergeTree` compactions, trading modest read CPU that works effortlessly up to tens of millions of rows per partition.

---

## 7. What Would Change for Production

1. **Kubernetes & Stateful Autoscaling:** Replace Docker Compose with **Strimzi Kafka Operator** (separate KRaft controller quorum + rack-aware NVMe brokers) and the **Apache Flink Kubernetes Operator** managing `FlinkDeployment` CRDs with reactive autoscaling (`parallelism` scaling cleanly from `3` $\rightarrow$ `6` $\rightarrow$ `12` across partition boundaries) and **Unaligned Checkpoints** enabled for heavy spikes.
2. **Object Storage & Lakehouse Tiering (Hybrid Kappa):** Replace frozen community MinIO ([ADR-0002](adr/0002-flink-2-2-rocksdb-checkpoints-on-minio.md)) with AWS S3 / GCS / ADLS, enable **Kafka Tiered Storage (KIP-405)**, and sink `clicks.raw.v1` + `products.catalogue.v1` into **Apache Iceberg** or **Apache Paimon** so multi-month historical replays read columnar Parquet splits from S3 via Flink `HybridSource` before switching seamlessly to live Kafka tailing.
3. **Security, Governance & Multi-Zone HA:** Enable **mTLS + SASL/SCRAM or OAuthBearer** across Kafka, Schema Registry, Flink, and ClickHouse; enforce **`BACKWARD_TRANSITIVE`** compatibility rules in CI prior to producer deployment; run **ClickHouse Keeper + `ReplicatedReplacingMergeTree`** across 3 availability zones; and replace static credentials with Kubernetes Workload Identity / Vault secrets.
4. **Observability & Alerting:** Scrape JMX/Prometheus metrics from Kafka (`UnderReplicatedPartitions`, `RequestHandlerAvgIdlePercent`), Flink (`lastCheckpointDuration`, `backPressuredTimeMsPerSecond`, `currentInputWatermark` lag, `numLateRecordsDropped`), and ClickHouse (`KafkaAssignedPartitions`, `BackgroundPoolTask`) into Prometheus + Alertmanager with PagerDuty SLO burn alerts when `p95` freshness exceeds `10 s`.
5. **SQL Plan Stability:** Use Flink SQL's **`COMPILE PLAN` / `EXECUTE PLAN`** JSON artifacts in Git so SQL query upgrades preserve deterministic operator UIDs across Flink minor-version upgrades just like our DataStream `.uid(...)` savepoints.

---

## 8. Architecture Decision Records Index

- [ADR-0001: Apache Kafka 4.4.0 in 3-Broker KRaft Mode](adr/0001-kafka-4-kraft-three-broker-topology.md)
- [ADR-0002: Apache Flink 2.2.1, RocksDB Incremental State, and S3 Checkpoints](adr/0002-flink-2-2-rocksdb-checkpoints-on-minio.md)
- [ADR-0003: ClickHouse 25.8 LTS over Apache Pinot for the Real-Time Serving Store](adr/0003-clickhouse-over-pinot-serving-store.md)
- [ADR-0004: Apicurio Schema Registry 3.3.4 with Confluent-Compatible Avro Wire Contracts](adr/0004-apicurio-registry-avro-wire-contracts.md)
- [ADR-0005: Topic Partition Sizing, `user_id` Keying, and Compression/Batching Defaults](adr/0005-topic-partitioning-key-and-compression.md)
- [ADR-0006: Grafana 13.2 Provisioned Dashboard-as-Code over Apache Superset](adr/0006-grafana-over-superset-provisioned-dashboard.md)
- [ADR-0007: End-to-End Delivery Semantics, Checkpoint Cadence, and `read_committed` Isolation](adr/0007-end-to-end-delivery-guarantees-and-isolation.md)
