# Real-Time E-Commerce Clickstream Analytics on Apache Kafka 4.4 & Apache Flink 2.2

End-to-end, reproducible real-time clickstream analytics platform built on **Apache Kafka 4.4.0 (3-broker KRaft)**, **Apache Flink 2.2.1** (Flink SQL + Java DataStream API, RocksDB incremental checkpoints on MinIO S3), **Apicurio Schema Registry 3.3.4**, **ClickHouse 25.8**, and **Grafana 13.2**.

## Quickstart

```bash
make up     # Boot 9-container stack, register Avro schemas, create ClickHouse pipelines & deploy Flink jobs
make demo   # Stream realistic e-commerce traffic, verify 2PC isolation, run batch recount & measure freshness
make test   # Run Python contract tests, Flink MiniCluster fixed-file tests, and live smoke checks
make down   # Stop all containers and clean volumes
```

Full architecture diagrams, measured benchmarks (`lz4` vs `zstd`, `linger.ms`/`batch.size`, `p50`/`p95` dashboard freshness), failure demo walkthroughs, and ADRs are maintained in [`docs/`](docs/).
