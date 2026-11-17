# ADR-0004: Apicurio Registry 3.3.4 with Avro Confluent-Compatible Wire Contracts

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Clickstream producers and Flink streaming jobs evolve independently. Untyped JSON on high-volume ingress topics wastes network/broker bandwidth by repeating field names on every event and lets breaking schema changes fail at runtime inside Flink operators rather than at publish time. We also want the entire repository stack to remain permissively open-source friendly without Confluent Community License restrictions.

## Decision

1. Deploy **Apicurio Registry `3.3.4`** (`apicurio/apicurio-registry:3.3.4`, Apache-2.0 licensed CNCF project) exposing the Confluent Schema Registry v7 API at `http://schema-registry:8080/apis/ccompat/v7`.
2. Encode ingress topics (`clicks.raw.v1` and compacted `products.catalogue.v1`) in **Apache Avro** (`avro-confluent` format in Flink SQL; Confluent wire framing `[0x00][4-byte schema ID][avro payload]` in Python) with **`BACKWARD`** compatibility enforced per subject.
3. Encode Flink's downstream curated output topics (`clicks.enriched.v1`, `metrics.minute.tumbling.v1`, `metrics.minute.hopping.v1`, `analytics.sessions.v1`, `analytics.funnel.v1`, `analytics.top_products.v1`, `clicks.late.v1`) as structured JSON (`JSONEachRow`) with registered JSON Schemas so ClickHouse `Kafka` tables and CLI inspection tools consume them natively without external UDF decoders.

## Consequences

- **Positive:** Cuts raw ingress payload size by ~45% before compression, enforces `BACKWARD` schema evolution gates before bad events enter Kafka, and uses standard Flink `flink-sql-avro-confluent-registry` without vendor lock-in.
- **Negative:** Adds one stateless HTTP dependency on startup (schema lookups are cached client-side by schema ID after first fetch). In production, back Apicurio Registry with PostgreSQL or Kafka SQL storage (`kafkasql`) instead of in-memory storage.
