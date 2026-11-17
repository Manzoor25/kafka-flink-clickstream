-- ClickHouse real-time Kafka Engine consumers + Materialized Views
-- Executed after Kafka topics exist. Consumer isolation_level=read_committed is enforced via config.d/kafka.xml.

CREATE TABLE IF NOT EXISTS analytics.kafka_clicks_enriched
(
    event_id           String,
    user_id            String,
    client_session_id  String,
    event_type         String,
    product_id         String,
    product_name       Nullable(String),
    category           Nullable(String),
    unit_price         Nullable(Float64),
    quantity           Int32,
    gross_revenue      Float64,
    device             String,
    event_ts_ms        Int64,
    created_at_ms      Int64,
    is_synthetic_late  Bool,
    flink_emit_ts      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'clicks.enriched.v1',
    kafka_group_name = 'ch-clicks-enriched-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_clicks_enriched TO analytics.clicks_enriched AS
SELECT
    event_id,
    user_id,
    client_session_id,
    event_type,
    product_id,
     coalesce(product_name, 'Unknown') AS product_name,
    coalesce(category, 'Uncategorized') AS category,
    toDecimal64(coalesce(unit_price, 0.0), 2) AS unit_price,
    quantity,
    toDecimal64(gross_revenue, 2) AS gross_revenue,
    device,
    fromUnixTimestamp64Milli(event_ts_ms, 'UTC') AS event_ts,
    fromUnixTimestamp64Milli(created_at_ms, 'UTC') AS created_at,
    if(is_synthetic_late, 1, 0) AS is_synthetic_late,
    parseDateTime64BestEffortOrZero(flink_emit_ts, 3, 'UTC') AS flink_emit_ts,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_clicks_enriched;

CREATE TABLE IF NOT EXISTS analytics.kafka_minute_metrics_tumbling
(
    window_start   String,
    window_end     String,
    active_users   UInt64,
    total_events   UInt64,
    views          UInt64,
    add_to_carts   UInt64,
    checkouts      UInt64,
    purchases      UInt64,
    gross_revenue  Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'metrics.minute.tumbling.v1',
    kafka_group_name = 'ch-minute-tumbling-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_minute_metrics_tumbling TO analytics.minute_metrics_tumbling AS
SELECT
    parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
    active_users,
    total_events,
    views,
    add_to_carts,
    checkouts,
    purchases,
    toDecimal64(gross_revenue, 2) AS gross_revenue,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_minute_metrics_tumbling;

CREATE TABLE IF NOT EXISTS analytics.kafka_minute_metrics_hopping
(
    window_start         String,
    window_end           String,
    active_users_5m      UInt64,
    purchases_5m         UInt64,
    gross_revenue_5m     Float64,
    conversion_rate_pct  Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'metrics.minute.hopping.v1',
    kafka_group_name = 'ch-minute-hopping-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_minute_metrics_hopping TO analytics.minute_metrics_hopping AS
SELECT
    parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
    active_users_5m,
    purchases_5m,
    toDecimal64(gross_revenue_5m, 2) AS gross_revenue_5m,
    conversion_rate_pct,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_minute_metrics_hopping;

CREATE TABLE IF NOT EXISTS analytics.kafka_user_sessions
(
    session_id         String,
    user_id            String,
    session_start_ms   Int64,
    session_end_ms     Int64,
    duration_sec       Float64,
    event_count        UInt64,
    distinct_products  UInt64,
    view_count         UInt64,
    cart_count         UInt64,
    checkout_count     UInt64,
    purchase_count     UInt64,
    converted          Bool,
    primary_device     String,
    job_version        String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'analytics.sessions.v1',
    kafka_group_name = 'ch-user-sessions-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_user_sessions TO analytics.user_sessions AS
SELECT
    session_id,
    user_id,
    fromUnixTimestamp64Milli(session_start_ms, 'UTC') AS session_start,
    fromUnixTimestamp64Milli(session_end_ms, 'UTC') AS session_end,
    duration_sec,
    event_count,
    distinct_products,
    view_count,
    cart_count,
    checkout_count,
    purchase_count,
    if(converted, 1, 0) AS converted,
    primary_device,
    job_version,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_user_sessions;

CREATE TABLE IF NOT EXISTS analytics.kafka_user_sessions_sql
(
    user_id          String,
    session_start    String,
    session_end      String,
    duration_sec     Int64,
    event_count      UInt64,
    purchases        UInt64,
    session_revenue  Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'analytics.sessions.sql.v1',
    kafka_group_name = 'ch-user-sessions-sql-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_user_sessions_sql TO analytics.user_sessions_sql AS
SELECT
    user_id,
    parseDateTime64BestEffort(session_start, 3, 'UTC') AS session_start,
    parseDateTime64BestEffort(session_end, 3, 'UTC') AS session_end,
    duration_sec,
    event_count,
    purchases,
    toDecimal64(session_revenue, 2) AS session_revenue,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_user_sessions_sql;

CREATE TABLE IF NOT EXISTS analytics.kafka_funnel_window_metrics
(
    window_start                      String,
    window_end                        String,
    step_1_views                      UInt64,
    step_2_carts                      UInt64,
    step_3_checkouts                  UInt64,
    step_4_purchases                  UInt64,
    dropoff_view_to_cart_pct           Nullable(Float64),
    dropoff_cart_to_checkout_pct      Nullable(Float64),
    dropoff_checkout_to_purchase_pct  Nullable(Float64),
    overall_conversion_pct            Nullable(Float64),
    funnel_revenue                    Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'analytics.funnel.v1',
    kafka_group_name = 'ch-funnel-metrics-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_funnel_window_metrics TO analytics.funnel_window_metrics AS
SELECT
    parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
    step_1_views,
    step_2_carts,
    step_3_checkouts,
    step_4_purchases,
    coalesce(dropoff_view_to_cart_pct, 0.0) AS dropoff_view_to_cart_pct,
    coalesce(dropoff_cart_to_checkout_pct, 0.0) AS dropoff_cart_to_checkout_pct,
    coalesce(dropoff_checkout_to_purchase_pct, 0.0) AS dropoff_checkout_to_purchase_pct,
    coalesce(overall_conversion_pct, 0.0) AS overall_conversion_pct,
    toDecimal64(funnel_revenue, 2) AS funnel_revenue,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_funnel_window_metrics;

CREATE TABLE IF NOT EXISTS analytics.kafka_top_products_window
(
    window_start  String,
    window_end    String,
    rnk           UInt32,
    product_id    String,
    product_name  String,
    category      String,
    views         UInt64,
    purchases     UInt64,
    revenue       Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'analytics.top_products.v1',
    kafka_group_name = 'ch-top-products-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_top_products_window TO analytics.top_products_window AS
SELECT
    parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
    rnk,
    product_id,
    product_name,
    category,
    views,
    purchases,
    toDecimal64(revenue, 2) AS revenue,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_top_products_window;

CREATE TABLE IF NOT EXISTS analytics.kafka_late_events
(
    event_id        String,
    user_id         String,
    event_type      String,
    product_id      String,
    event_ts_ms     Int64,
    watermark_ms    Int64,
    lateness_ms     Int64,
    detected_at_ms  Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092,kafka-3:19092',
    kafka_topic_list = 'clicks.late.v1',
    kafka_group_name = 'ch-late-events-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_late_events TO analytics.late_events AS
SELECT
    event_id,
    user_id,
    event_type,
    product_id,
    fromUnixTimestamp64Milli(event_ts_ms, 'UTC') AS event_ts,
    fromUnixTimestamp64Milli(watermark_ms, 'UTC') AS watermark_ts,
    lateness_ms,
    fromUnixTimestamp64Milli(detected_at_ms, 'UTC') AS detected_at,
    now64(3, 'UTC') AS ch_ingested_at
FROM analytics.kafka_late_events;
