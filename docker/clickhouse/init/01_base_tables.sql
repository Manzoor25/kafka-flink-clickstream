CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Enriched, deduplicated, temporally price-joined clickstream events
CREATE TABLE IF NOT EXISTS analytics.clicks_enriched
(
    event_id           String,
    user_id            String,
    client_session_id  String,
    event_type         LowCardinality(String),
    product_id         LowCardinality(String),
    product_name       String,
    category           LowCardinality(String),
    unit_price         Decimal(10, 2),
    quantity           Int32,
    gross_revenue      Decimal(12, 2),
    device             LowCardinality(String),
    event_ts           DateTime64(3, 'UTC'),
    created_at         DateTime64(3, 'UTC'),
    is_synthetic_late  UInt8,
    flink_emit_ts      DateTime64(3, 'UTC'),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (event_id);

-- 2. Tumbling 1-minute window metrics from Flink SQL
CREATE TABLE IF NOT EXISTS analytics.minute_metrics_tumbling
(
    window_start       DateTime64(3, 'UTC'),
    window_end         DateTime64(3, 'UTC'),
    active_users       UInt64,
    total_events       UInt64,
    views              UInt64,
    add_to_carts       UInt64,
    checkouts          UInt64,
    purchases          UInt64,
    gross_revenue      Decimal(14, 2),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (window_start, window_end);

-- 3. Hopping 5-minute window (1-minute slide) metrics from Flink SQL
CREATE TABLE IF NOT EXISTS analytics.minute_metrics_hopping
(
    window_start          DateTime64(3, 'UTC'),
    window_end            DateTime64(3, 'UTC'),
    active_users_5m       UInt64,
    purchases_5m          UInt64,
    gross_revenue_5m      Decimal(14, 2),
    conversion_rate_pct   Float64,
    ch_ingested_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (window_start, window_end);

-- 4. Sessionised user journeys from Flink DataStream API job (SessionAnalyticsJob)
CREATE TABLE IF NOT EXISTS analytics.user_sessions
(
    session_id         String,
    user_id            String,
    session_start      DateTime64(3, 'UTC'),
    session_end        DateTime64(3, 'UTC'),
    duration_sec       Float64,
    event_count        UInt64,
    distinct_products  UInt64,
    view_count         UInt64,
    cart_count         UInt64,
    checkout_count     UInt64,
    purchase_count     UInt64,
    converted          UInt8,
    primary_device     LowCardinality(String),
    job_version        LowCardinality(String),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (user_id, session_start, session_id);

-- 5. Flink SQL SESSION TVF summaries
CREATE TABLE IF NOT EXISTS analytics.user_sessions_sql
(
    user_id            String,
    session_start      DateTime64(3, 'UTC'),
    session_end        DateTime64(3, 'UTC'),
    duration_sec       Int64,
    event_count        UInt64,
    purchases          UInt64,
    session_revenue    Decimal(14, 2),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (user_id, session_start, session_end);

-- 6. MATCH_RECOGNIZE 4-step funnel conversion and step drop-offs per window
CREATE TABLE IF NOT EXISTS analytics.funnel_window_metrics
(
    window_start                      DateTime64(3, 'UTC'),
    window_end                        DateTime64(3, 'UTC'),
    step_1_views                      UInt64,
    step_2_carts                      UInt64,
    step_3_checkouts                  UInt64,
    step_4_purchases                  UInt64,
    dropoff_view_to_cart_pct          Float64,
    dropoff_cart_to_checkout_pct      Float64,
    dropoff_checkout_to_purchase_pct  Float64,
    overall_conversion_pct            Float64,
    funnel_revenue                    Decimal(14, 2),
    ch_ingested_at                    DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (window_start, window_end);

-- 7. Window Top-N products from Flink SQL
CREATE TABLE IF NOT EXISTS analytics.top_products_window
(
    window_start       DateTime64(3, 'UTC'),
    window_end         DateTime64(3, 'UTC'),
    rnk                UInt32,
    product_id         LowCardinality(String),
    product_name       String,
    category           LowCardinality(String),
    views              UInt64,
    purchases          UInt64,
    revenue            Decimal(14, 2),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (window_start, window_end, rnk, product_id);

-- 8. Late events routed to DataStream side output beyond watermark + allowedLateness
CREATE TABLE IF NOT EXISTS analytics.late_events
(
    event_id           String,
    user_id            String,
    event_type         LowCardinality(String),
    product_id         LowCardinality(String),
    event_ts           DateTime64(3, 'UTC'),
    watermark_ts       DateTime64(3, 'UTC'),
    lateness_ms        Int64,
    detected_at        DateTime64(3, 'UTC'),
    ch_ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ch_ingested_at)
ORDER BY (event_id);
