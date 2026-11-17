FROM grafana/grafana:13.2.6

# Pre-install pinned official ClickHouse datasource plugin so container boot is air-gapped & deterministic
RUN grafana cli plugins install grafana-clickhouse-datasource 4.23.5
