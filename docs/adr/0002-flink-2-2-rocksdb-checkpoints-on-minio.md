# ADR-0002: Apache Flink 2.2.1, RocksDB Incremental State, and S3 Checkpoints

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

We need stateful stream processing supporting SQL (`MATCH_RECOGNIZE`, Window TVFs, `FOR SYSTEM_TIME AS OF` temporal joins, deduplication) and a Java DataStream API job (`EventTimeSessionWindows` + late side outputs), backed by RocksDB incremental checkpoints and savepoints on S3-compatible object storage (MinIO).

Two supply-chain constraints surfaced during version verification:
1. **Flink Core vs Connector Release Train:** Although Apache Flink `2.3.0` is published upstream, Flink connectors release asynchronously; the latest GA Kafka SQL/DataStream connector on Maven Central is **`5.0.0-2.2`** (targeting Flink `2.2.x`).
2. **MinIO Container Distribution:** Upstream deleted `minio/minio` from Docker Hub in September 2026 and restricted `quay.io/minio/minio` (`401 Unauthorized`).

## Decision

1. Pin **`flink:2.2.1-scala_2.12-java17`** paired with **`flink-sql-connector-kafka:5.0.0-2.2`** and **`flink-sql-avro-confluent-registry:2.2.1`**.
2. Configure Flink 2.x standard YAML (`conf/config.yaml`) with `state.backend.type: rocksdb`, `execution.checkpointing.incremental: true`, `execution.checkpointing.mode: EXACTLY_ONCE`, `execution.checkpointing.unaligned.enabled: true` with `aligned-checkpoint-timeout: 5s` (FLIP-76), and `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.2.1.jar`. Checkpoints stay aligned in steady-state traffic (`< 500 ms` barrier alignment) and automatically switch to unaligned checkpoints if backpressure delays barrier alignment past `5 s` during $10\times$ traffic spikes or full `earliest-offset` Kappa backfills.
3. Run **1 JobManager + 2 TaskManagers** (6 slots each, 12 total with `cluster.evenly-spread-out-slots: true`, running 2 jobs at `parallelism=3`) so killing one TaskManager under load demonstrates live failover onto surviving standby slots and S3 checkpoint recovery without external cluster autoscaling.
4. Pin **`alpine/minio:RELEASE.2025-10-15T17-29-55Z`** for local S3 checkpoint/savepoint storage and bootstrap buckets via Python `boto3` (`s3v4` path-style) rather than the removed `minio/mc` image.

## Consequences

- **Positive:** 100% binary compatibility between Flink 2.2 core and official Kafka 5.0.0 connectors; RocksDB incremental SST uploads keep checkpoint durations sub-second as dedup/window state grows; cold `docker compose pull` works reliably on clean machines and CI.
- **Negative / Production Delta:** RocksDB incurs serialization overhead on every state read/write compared to `hashmap`. In production, replace frozen community MinIO with managed object storage (AWS S3 / GCS / ADLS) or actively maintained Apache-2.0 S3 stores (SeaweedFS / Ceph RGW).
