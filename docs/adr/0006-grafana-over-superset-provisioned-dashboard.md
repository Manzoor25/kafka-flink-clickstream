# ADR-0006: Grafana 13.2.6 Provisioned as Code over Apache Superset

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

The business requires a live operational dashboard refreshing every few seconds ("never more than a few seconds stale") committed entirely as code in Git so `make up` brings up a ready-to-view dashboard with zero manual UI clicks. The options are **Grafana** and **Apache Superset**.

## Decision

Choose **Grafana `13.2.6`** with the official **`grafana-clickhouse-datasource` (`4.23.5`)** baked into the container image and declarative YAML/JSON provisioning mounted under `/etc/grafana/provisioning/`:
- Sub-5-second auto-refresh (`"refresh": "2s"`) is native in Grafana, whereas Superset's dashboard auto-refresh and metadata database (`PostgreSQL` + `Redis` + init migrations) add heavy container startup latency and prefer 10s+ polling intervals.
- Grafana datasources and dashboard JSON files load declaratively from disk on startup without database export/import ZIP workflows.

## Consequences

- **Positive:** Single lightweight container, 100% deterministic GitOps provisioning, native `2s` live refresh, and direct SQL panels over ClickHouse `FINAL` views.
- **Negative:** Business analysts wanting self-service drag-and-drop ad-hoc SQL exploration get richer BI semantic-layer features in Apache Superset.
