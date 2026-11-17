| Measurement Path | Samples | Min (ms) | p50 (ms) | p95 (ms) |
|---|---:|---:|---:|---:|
| Active Canary (`created_at` → Grafana `/api/ds/query`) | 25 | 6828 | 10729 | 13715 |
| Continuous Stream (`created_at` → ClickHouse `ch_ingested_at`) | 17,780 | 5692 | 16435 | 27372 |
