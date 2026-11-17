What actually happens to revenue accuracy when you `docker kill` a Flink TaskManager mid-stream while product prices are changing?

I built a complete, runnable real-time e-commerce clickstream platform on Apache Kafka 4.4 (3-broker KRaft), Apache Flink 2.2 (SQL + Java DataStream API + RocksDB S3 checkpoints), Apicurio Schema Registry, ClickHouse 25.8, and Grafana 13.2—and tested every failure mode end to end:

• Compression & batching: `lz4` (3.12x, 318.8k ev/s) & `zstd` (5.29x, 229.2k ev/s) vs uncompressed Avro on 12 KRaft partitions
• Point-in-time pricing: `FOR SYSTEM_TIME AS OF` temporal join on compacted `upsert-kafka` + `event_id` dedup + `MATCH_RECOGNIZE` 4-step funnel
• End-to-end freshness: 10.73s p50 / 13.71s p95 creation-to-Grafana API (5.73s pipeline overhead above our 5s watermark holdback)
• Chaos drills: TaskManager SIGKILL recovery in 18.98s with $0.00 recount drift, 10x spike lag (51.9k -> 0), S3 savepoint v1->v2 upgrade, and 72k-event Kappa replay from offset 0 ($0.00 drift)

Full code, 7 ADRs, benchmarks, and `make up && make demo`:
https://github.com/Manzoor25/kafka-flink-clickstream

#DataEngineering #ApacheKafka #ApacheFlink #StreamProcessing #ClickHouse
