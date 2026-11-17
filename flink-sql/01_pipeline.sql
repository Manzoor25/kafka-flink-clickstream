-- =============================================================================
-- Real-Time Clickstream Analytics Pipeline (Apache Flink 2.2 SQL)
-- =============================================================================
-- Implements Build Requirements #2, #3, #4, #5 (SQL TVF), #6, #7, #8, and #9:
--   2. Kafka Avro source with 5s bounded-out-of-orderness watermarks & idle-timeout
--   3. Event-ID deduplication (RocksDB state with 24h TTL)
--   4. Tumbling (1m) and Hopping (5m window, 1m slide) aggregates
--   5. Sessionisation via Flink 2.2 SESSION Window TVF (30s inactivity gap)
--   6. 4-step Funnel Conversion & per-step drop-off via MATCH_RECOGNIZE CEP
--   7. Window Top-N (Top 10 products per 1m window via WindowRank)
--   8. Point-in-time Temporal Join (`FOR SYSTEM_TIME AS OF`) against compacted
--      `products.catalogue.v1` (`upsert-kafka`)
--   9. Two-phase-commit (`exactly-once`) transactional Kafka sinks for downstream
--      `isolation.level = read_committed` consumers
-- =============================================================================

SET 'pipeline.name' = '${PIPELINE_NAME}';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '30 s';
SET 'table.exec.state.ttl' = '86400 s';

-- -----------------------------------------------------------------------------
-- 1. Sources: Raw Clickstream (Kafka + Avro) & Versioned Catalogue (Upsert-Kafka)
-- -----------------------------------------------------------------------------
CREATE TABLE clicks_raw (
    event_id STRING,
    user_id STRING,
    client_session_id STRING,
    event_type STRING,
    product_id STRING,
    quantity INT,
    device STRING,
    page_url STRING,
    event_ts_ms BIGINT,
    created_at_ms BIGINT,
    is_synthetic_late BOOLEAN,
    proc_time AS PROCTIME(),
    event_ts AS TO_TIMESTAMP_LTZ(event_ts_ms, 3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks.raw.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.group.id' = '${CONSUMER_GROUP}',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = '${SCAN_STARTUP_MODE}',
    'format' = 'avro-confluent',
    'avro-confluent.url' = '${SCHEMA_REGISTRY_URL}'
);

CREATE TABLE products_catalogue (
    product_id STRING NOT NULL,
    product_name STRING,
    category STRING,
    price DOUBLE,
    updated_at_ms BIGINT,
    updated_at AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'products.catalogue.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.isolation.level' = 'read_committed',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = '${SCHEMA_REGISTRY_URL}',
    'value.fields-include' = 'ALL'
);

-- @SECTION:CORE_VIEWS_START
-- -----------------------------------------------------------------------------
-- 2. Deduplication by event_id (KeepFirstRow -> insert-only stream preserving rowtime)
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW clicks_deduped AS
SELECT
    event_id,
    user_id,
    client_session_id,
    event_type,
    product_id,
    quantity,
    device,
    page_url,
    event_ts_ms,
    created_at_ms,
    is_synthetic_late,
    event_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
    FROM clicks_raw
    WHERE event_id NOT LIKE 'wm-flush-%'
)
WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- 3. Event-Time Temporal Join (`FOR SYSTEM_TIME AS OF`) against Product Catalogue
--    Picks up the exact product price valid at `c.event_ts`.
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW clicks_enriched AS
SELECT
    c.event_id,
    c.user_id,
    c.client_session_id,
    c.event_type,
    c.product_id,
    COALESCE(p.product_name, 'Unknown SKU') AS product_name,
    COALESCE(p.category, 'Uncategorized') AS category,
    ROUND(COALESCE(p.price, 0.0), 2) AS unit_price,
    c.quantity,
    CASE
        WHEN c.event_type = 'purchase' THEN ROUND(ROUND(COALESCE(p.price, 0.0), 2) * c.quantity, 2)
        ELSE 0.0
    END AS gross_revenue,
    c.device,
    c.event_ts_ms,
    c.created_at_ms,
    c.is_synthetic_late,
    c.event_ts
FROM clicks_deduped AS c
LEFT JOIN products_catalogue FOR SYSTEM_TIME AS OF c.event_ts AS p
    ON c.product_id = p.product_id;

-- -----------------------------------------------------------------------------
-- 4. MATCH_RECOGNIZE 4-Step Funnel Stages (Causal Pattern Matching within 10m)
--    Evaluates unique sessions reaching each sequential milestone:
--      Stage 1: view
--      Stage 2: view -> add_to_cart
--      Stage 3: view -> add_to_cart -> checkout
--      Stage 4: view -> add_to_cart -> checkout -> purchase
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW funnel_stage_matches AS
SELECT client_session_id, stage, stage_revenue, match_ts
FROM clicks_enriched
MATCH_RECOGNIZE (
    PARTITION BY client_session_id
    ORDER BY event_ts
    MEASURES
        1 AS stage,
        CAST(0.0 AS DOUBLE) AS stage_revenue,
        MATCH_ROWTIME(event_ts) AS match_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V)
    DEFINE
        V AS V.event_type = 'view'
)
UNION ALL
SELECT client_session_id, stage, stage_revenue, match_ts
FROM clicks_enriched
MATCH_RECOGNIZE (
    PARTITION BY client_session_id
    ORDER BY event_ts
    MEASURES
        2 AS stage,
        CAST(0.0 AS DOUBLE) AS stage_revenue,
        MATCH_ROWTIME(event_ts) AS match_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V E1*? C) WITHIN INTERVAL '10' MINUTE
    DEFINE
        V AS V.event_type = 'view',
        E1 AS E1.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart'
)
UNION ALL
SELECT client_session_id, stage, stage_revenue, match_ts
FROM clicks_enriched
MATCH_RECOGNIZE (
    PARTITION BY client_session_id
    ORDER BY event_ts
    MEASURES
        3 AS stage,
        CAST(0.0 AS DOUBLE) AS stage_revenue,
        MATCH_ROWTIME(event_ts) AS match_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V E1*? C E2*? K) WITHIN INTERVAL '10' MINUTE
    DEFINE
        V AS V.event_type = 'view',
        E1 AS E1.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart',
        E2 AS E2.event_type <> 'checkout',
        K AS K.event_type = 'checkout'
)
UNION ALL
SELECT client_session_id, stage, stage_revenue, match_ts
FROM clicks_enriched
MATCH_RECOGNIZE (
    PARTITION BY client_session_id
    ORDER BY event_ts
    MEASURES
        4 AS stage,
        P.gross_revenue AS stage_revenue,
        MATCH_ROWTIME(event_ts) AS match_ts
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (V E1*? C E2*? K E3*? P) WITHIN INTERVAL '10' MINUTE
    DEFINE
        V AS V.event_type = 'view',
        E1 AS E1.event_type <> 'add_to_cart',
        C AS C.event_type = 'add_to_cart',
        E2 AS E2.event_type <> 'checkout',
        K AS K.event_type = 'checkout',
        E3 AS E3.event_type <> 'purchase',
        P AS P.event_type = 'purchase'
);
-- @SECTION:CORE_VIEWS_END

-- -----------------------------------------------------------------------------
-- 5. Transactional (`exactly-once`) Kafka Sink Tables
-- -----------------------------------------------------------------------------
CREATE TABLE clicks_enriched_sink (
    event_id STRING,
    user_id STRING,
    client_session_id STRING,
    event_type STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    unit_price DOUBLE,
    quantity INT,
    gross_revenue DOUBLE,
    device STRING,
    event_ts_ms BIGINT,
    created_at_ms BIGINT,
    is_synthetic_late BOOLEAN,
    flink_emit_ts STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks.enriched.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-enriched',
    'format' = 'json'
);

CREATE TABLE minute_metrics_tumbling_sink (
    window_start STRING,
    window_end STRING,
    active_users BIGINT,
    total_events BIGINT,
    `views` BIGINT,
    add_to_carts BIGINT,
    checkouts BIGINT,
    purchases BIGINT,
    gross_revenue DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'metrics.minute.tumbling.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-tumbling',
    'format' = 'json'
);

CREATE TABLE minute_metrics_hopping_sink (
    window_start STRING,
    window_end STRING,
    active_users_5m BIGINT,
    purchases_5m BIGINT,
    gross_revenue_5m DOUBLE,
    conversion_rate_pct DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'metrics.minute.hopping.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-hopping',
    'format' = 'json'
);

CREATE TABLE user_sessions_sql_sink (
    user_id STRING,
    session_start STRING,
    session_end STRING,
    duration_sec BIGINT,
    event_count BIGINT,
    purchases BIGINT,
    session_revenue DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.sessions.sql.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-sessions-sql',
    'format' = 'json'
);

CREATE TABLE funnel_window_metrics_sink (
    window_start STRING,
    window_end STRING,
    step_1_views BIGINT,
    step_2_carts BIGINT,
    step_3_checkouts BIGINT,
    step_4_purchases BIGINT,
    dropoff_view_to_cart_pct DOUBLE,
    dropoff_cart_to_checkout_pct DOUBLE,
    dropoff_checkout_to_purchase_pct DOUBLE,
    overall_conversion_pct DOUBLE,
    funnel_revenue DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.funnel.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-funnel',
    'format' = 'json'
);

CREATE TABLE top_products_window_sink (
    window_start STRING,
    window_end STRING,
    rnk INT,
    product_id STRING,
    product_name STRING,
    category STRING,
    `views` BIGINT,
    purchases BIGINT,
    revenue DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'analytics.top_products.v1',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.transaction.timeout.ms' = '300000',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = '${TXN_PREFIX}-topn',
    'format' = 'json'
);

-- -----------------------------------------------------------------------------
-- 6. Single-DAG Multi-Sink Execution (`STATEMENT SET`)
-- -----------------------------------------------------------------------------
EXECUTE STATEMENT SET
BEGIN

-- (A) Enriched per-click stream (Freshness SLA + sub-second live panels)
INSERT INTO clicks_enriched_sink
-- @QUERY:ENRICHED_START
SELECT
    event_id,
    user_id,
    client_session_id,
    event_type,
    product_id,
    product_name,
    category,
    unit_price,
    quantity,
    gross_revenue,
    device,
    event_ts_ms,
    created_at_ms,
    is_synthetic_late,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS flink_emit_ts
FROM clicks_enriched
-- @QUERY:ENRICHED_END
;

-- (B) 1-Minute Tumbling Window Metrics
INSERT INTO minute_metrics_tumbling_sink
-- @QUERY:TUMBLING_START
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_end,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'view') AS `views`,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_carts,
    COUNT(*) FILTER (WHERE event_type = 'checkout') AS checkouts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    ROUND(COALESCE(SUM(gross_revenue), 0.0), 2) AS gross_revenue
FROM TABLE(
    TUMBLE(TABLE clicks_enriched, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end
-- @QUERY:TUMBLING_END
;

-- (C) 5-Minute Hopping Window (1-Minute Slide) Rolling Metrics
INSERT INTO minute_metrics_hopping_sink
-- @QUERY:HOPPING_START
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_end,
    COUNT(DISTINCT user_id) AS active_users_5m,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases_5m,
    ROUND(COALESCE(SUM(gross_revenue), 0.0), 2) AS gross_revenue_5m,
    CASE
        WHEN COUNT(DISTINCT user_id) > 0 THEN
            ROUND(
                CAST(COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id ELSE NULL END) AS DOUBLE)
                * 100.0 / CAST(COUNT(DISTINCT user_id) AS DOUBLE),
                2
            )
        ELSE 0.0
    END AS conversion_rate_pct
FROM TABLE(
    HOP(TABLE clicks_enriched, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end
-- @QUERY:HOPPING_END
;

-- (D) Flink 2.2 SQL SESSION Window TVF (30-Second Inactivity Gap)
INSERT INTO user_sessions_sql_sink
-- @QUERY:SESSIONS_SQL_START
SELECT
    user_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS session_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS session_end,
    TIMESTAMPDIFF(SECOND, window_start, window_end) AS duration_sec,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
    ROUND(COALESCE(SUM(gross_revenue), 0.0), 2) AS session_revenue
FROM TABLE(
    SESSION(TABLE clicks_enriched PARTITION BY(user_id), DESCRIPTOR(event_ts), INTERVAL '30' SECOND)
)
GROUP BY user_id, window_start, window_end
-- @QUERY:SESSIONS_SQL_END
;

-- (E) MATCH_RECOGNIZE 4-Step Funnel Conversion & Drop-Off per 1m Window
INSERT INTO funnel_window_metrics_sink
-- @QUERY:FUNNEL_START
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_end,
    s1 AS step_1_views,
    s2 AS step_2_carts,
    s3 AS step_3_checkouts,
    s4 AS step_4_purchases,
    CASE WHEN s1 > 0 AND s1 >= s2 THEN ROUND(CAST(s1 - s2 AS DOUBLE) * 100.0 / CAST(s1 AS DOUBLE), 2) ELSE 0.0 END AS dropoff_view_to_cart_pct,
    CASE WHEN s2 > 0 AND s2 >= s3 THEN ROUND(CAST(s2 - s3 AS DOUBLE) * 100.0 / CAST(s2 AS DOUBLE), 2) ELSE 0.0 END AS dropoff_cart_to_checkout_pct,
    CASE WHEN s3 > 0 AND s3 >= s4 THEN ROUND(CAST(s3 - s4 AS DOUBLE) * 100.0 / CAST(s3 AS DOUBLE), 2) ELSE 0.0 END AS dropoff_checkout_to_purchase_pct,
    CASE WHEN s1 > 0 THEN ROUND(CAST(s4 AS DOUBLE) * 100.0 / CAST(s1 AS DOUBLE), 2) ELSE 0.0 END AS overall_conversion_pct,
    ROUND(COALESCE(funnel_rev, 0.0), 2) AS funnel_revenue
FROM (
    SELECT
        window_start,
        window_end,
        COUNT(DISTINCT client_session_id) FILTER (WHERE stage = 1) AS s1,
        COUNT(DISTINCT client_session_id) FILTER (WHERE stage = 2) AS s2,
        COUNT(DISTINCT client_session_id) FILTER (WHERE stage = 3) AS s3,
        COUNT(DISTINCT client_session_id) FILTER (WHERE stage = 4) AS s4,
        SUM(stage_revenue) FILTER (WHERE stage = 4) AS funnel_rev
    FROM TABLE(
        TUMBLE(TABLE funnel_stage_matches, DESCRIPTOR(match_ts), INTERVAL '1' MINUTE)
    )
    GROUP BY window_start, window_end
)
-- @QUERY:FUNNEL_END
;

-- (F) Window Top-N (Top 10 Products per 1m Window via WindowRank)
INSERT INTO top_products_window_sink
-- @QUERY:TOPN_START
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_start,
    DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS window_end,
    CAST(rnk AS INT) AS rnk,
    product_id,
    product_name,
    category,
    `views`,
    purchases,
    ROUND(revenue, 2) AS revenue
FROM (
    SELECT
        window_start,
        window_end,
        product_id,
        product_name,
        category,
        `views`,
        purchases,
        revenue,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end
            ORDER BY revenue DESC, `views` DESC, product_id ASC
        ) AS rnk
    FROM (
        SELECT
            window_start,
            window_end,
            product_id,
            COALESCE(MAX(CASE WHEN product_name <> 'Unknown SKU' THEN product_name END), 'Unknown SKU') AS product_name,
            COALESCE(MAX(CASE WHEN category <> 'Uncategorized' THEN category END), 'Uncategorized') AS category,
            COUNT(*) FILTER (WHERE event_type = 'view') AS `views`,
            COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
            COALESCE(SUM(gross_revenue), 0.0) AS revenue
        FROM TABLE(
            TUMBLE(TABLE clicks_enriched, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, product_id
    )
)
WHERE rnk <= 10
-- @QUERY:TOPN_END
;

END;
