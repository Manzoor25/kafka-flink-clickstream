# ADR-0001: Apache Kafka 4.4.0 in 3-Node Combined KRaft Mode

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

The platform requires a fault-tolerant 3-broker Apache Kafka 4.x cluster running locally under Docker Compose and in CI without ZooKeeper. Apache Kafka 4.x completely removed ZooKeeper metadata support in favor of KRaft (`KIP-833`), and `apache/kafka:4.4.0` is the latest upstream GA release.

In KRaft, nodes can run either dedicated roles (`controller` vs `broker` — 6 containers minimum for a 3-voter + 3-broker cluster) or combined roles (`process.roles=broker,controller` — 3 containers).

## Decision

1. Pin **`apache/kafka:4.4.0`** across three nodes (`kafka-1`, `kafka-2`, `kafka-3`) with `KAFKA_PROCESS_ROLES=broker,controller` and a 3-voter quorum (`1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`).
2. Configure dual listeners on every broker (`INTERNAL://kafka-N:19092` for container-to-container traffic, `EXTERNAL://localhost:29092..29094` for host Python scripts and benchmarks, plus `CONTROLLER://kafka-N:9093`).
3. Enforce `replication.factor=3`, `min.insync.replicas=2`, `offsets.topic.replication.factor=3`, `transaction.state.log.replication.factor=3`, and `transaction.state.log.min.isr=2`.

## Consequences

- **Positive:** Eliminates 3 extra JVM controller containers (~1.5 GB RAM saved in Docker Compose and GitHub Actions runners) while preserving genuine Raft quorum election, ISR replication, multi-broker partition leadership, and Kafka transaction coordinator failover.
- **Negative / Production Delta:** Combined mode couples metadata Raft heartbeats with broker page-cache/GC pressure. In production above ~30 MB/s or >1,000 partitions, split the 3 KRaft controllers onto dedicated small instances (`m7g.large`) separate from NVMe-backed broker nodes.
