package io.clickstream.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Executes the production Flink SQL transformations from {@code flink-sql/01_pipeline.sql}
 * against fixed JSONL input files ({@code clicks_input.jsonl} and {@code catalogue_input.jsonl})
 * and asserts exact parity with {@code expected_sql_outputs.json}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Deduplication of duplicate {@code event_id} ("ev-1")</li>
 *   <li>Point-in-time {@code FOR SYSTEM_TIME AS OF} temporal join across a mid-minute price drop ($200 -> $150)</li>
 *   <li>1-minute {@code TUMBLE} window aggregates</li>
 *   <li>5-minute {@code HOP} window rolling aggregates</li>
 *   <li>30-second {@code SESSION} window TVF aggregates</li>
 *   <li>4-step {@code MATCH_RECOGNIZE} funnel conversion &amp; step-by-step drop-off percentages</li>
 *   <li>3-level {@code WindowRank} Top-N product rankings</li>
 * </ul>
 */
class FlinkSqlPipelineFixtureTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void productionSqlMatchesExpectedOutputsAgainstFixedFiles() throws Exception {
        String pipelineSql = loadProductionPipelineSql();
        JsonNode expected;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/expected_sql_outputs.json")) {
            expected = MAPPER.readTree(in);
        }

        URL clicksUrl = getClass().getResource("/fixtures/clicks_input.jsonl");
        URL catUrl = getClass().getResource("/fixtures/catalogue_input.jsonl");
        String clicksPath = Path.of(clicksUrl.toURI()).toUri().toString();
        String catPath = Path.of(catUrl.toURI()).toUri().toString();

        StreamTableEnvironment tEnv = createTestTableEnv(clicksPath, catPath, pipelineSql);

        // 1. Verify deduplication + temporal join point-in-time pricing on clicks_enriched
        List<Row> enrichedRows = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:ENRICHED_START", "@QUERY:ENRICHED_END"));
        JsonNode expEnriched = expected.get("enriched");
        assertEquals(expEnriched.get("unique_event_count").asInt(), enrichedRows.size(), "Duplicate ev-1 must be removed");

        Map<String, Row> enrichedById = new HashMap<>();
        double totalGrossRevenue = 0.0;
        for (Row r : enrichedRows) {
            enrichedById.put((String) r.getField(0), r);
            totalGrossRevenue += ((Number) r.getField(9)).doubleValue();
        }
        // ev-4 occurred at 00:00:20Z (before 00:00:30Z promo) -> $200 * 2 = $400
        assertEquals(expEnriched.get("ev4_unit_price_before_promo").asDouble(), ((Number) enrichedById.get("ev-4").getField(7)).doubleValue(), 0.01);
        assertEquals(expEnriched.get("ev4_gross_revenue").asDouble(), ((Number) enrichedById.get("ev-4").getField(9)).doubleValue(), 0.01);
        // ev-8 occurred at 00:00:48Z (after 00:00:30Z promo) -> $150 * 1 = $150
        assertEquals(expEnriched.get("ev8_unit_price_after_promo").asDouble(), ((Number) enrichedById.get("ev-8").getField(7)).doubleValue(), 0.01);
        assertEquals(expEnriched.get("ev8_gross_revenue").asDouble(), ((Number) enrichedById.get("ev-8").getField(9)).doubleValue(), 0.01);
        assertEquals(expEnriched.get("total_gross_revenue").asDouble(), totalGrossRevenue, 0.01);

        // 2. Verify 1-minute TUMBLE window metrics
        List<Row> tumblingRows = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:TUMBLING_START", "@QUERY:TUMBLING_END"));
        assertEquals(1, tumblingRows.size());
        Row tumble = tumblingRows.get(0);
        JsonNode expTumble = expected.get("tumbling_1m");
        assertEquals(expTumble.get("window_start").asText(), tumble.getField(0));
        assertEquals(expTumble.get("window_end").asText(), tumble.getField(1));
        assertEquals(expTumble.get("active_users").asLong(), ((Number) tumble.getField(2)).longValue());
        assertEquals(expTumble.get("total_events").asLong(), ((Number) tumble.getField(3)).longValue());
        assertEquals(expTumble.get("views").asLong(), ((Number) tumble.getField(4)).longValue());
        assertEquals(expTumble.get("add_to_carts").asLong(), ((Number) tumble.getField(5)).longValue());
        assertEquals(expTumble.get("checkouts").asLong(), ((Number) tumble.getField(6)).longValue());
        assertEquals(expTumble.get("purchases").asLong(), ((Number) tumble.getField(7)).longValue());
        assertEquals(expTumble.get("gross_revenue").asDouble(), ((Number) tumble.getField(8)).doubleValue(), 0.01);

        // 3. Verify 5-minute HOP window emits 5 sliding windows covering minute 00:00..00:01
        List<Row> hoppingRows = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:HOPPING_START", "@QUERY:HOPPING_END"));
        assertEquals(5, hoppingRows.size(), "5m window with 1m slide produces 5 overlapping windows for 1 minute of events");
        for (Row hop : hoppingRows) {
            assertEquals(4L, ((Number) hop.getField(2)).longValue());
            assertEquals(2L, ((Number) hop.getField(3)).longValue());
            assertEquals(550.00, ((Number) hop.getField(4)).doubleValue(), 0.01);
            assertEquals(50.00, ((Number) hop.getField(5)).doubleValue(), 0.01);
        }

        // 4. Verify MATCH_RECOGNIZE 4-step funnel & step-by-step drop-off percentages
        List<Row> funnelRows = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:FUNNEL_START", "@QUERY:FUNNEL_END"));
        assertEquals(1, funnelRows.size());
        Row funnel = funnelRows.get(0);
        JsonNode expFunnel = expected.get("funnel_1m");
        assertEquals(expFunnel.get("step_1_views").asLong(), ((Number) funnel.getField(2)).longValue());
        assertEquals(expFunnel.get("step_2_carts").asLong(), ((Number) funnel.getField(3)).longValue());
        assertEquals(expFunnel.get("step_3_checkouts").asLong(), ((Number) funnel.getField(4)).longValue());
        assertEquals(expFunnel.get("step_4_purchases").asLong(), ((Number) funnel.getField(5)).longValue());
        assertEquals(expFunnel.get("dropoff_view_to_cart_pct").asDouble(), ((Number) funnel.getField(6)).doubleValue(), 0.01);
        assertEquals(expFunnel.get("dropoff_cart_to_checkout_pct").asDouble(), ((Number) funnel.getField(7)).doubleValue(), 0.01);
        assertEquals(expFunnel.get("dropoff_checkout_to_purchase_pct").asDouble(), ((Number) funnel.getField(8)).doubleValue(), 0.01);
        assertEquals(expFunnel.get("overall_conversion_pct").asDouble(), ((Number) funnel.getField(9)).doubleValue(), 0.01);
        assertEquals(expFunnel.get("funnel_revenue").asDouble(), ((Number) funnel.getField(10)).doubleValue(), 0.01);

        // 5. Verify WindowRank Top-N products
        List<Row> topNRows = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:TOPN_START", "@QUERY:TOPN_END"));
        JsonNode expTopN = expected.get("top_products_1m");
        assertEquals(expTopN.size(), topNRows.size());
        for (int i = 0; i < expTopN.size(); i++) {
            JsonNode expItem = expTopN.get(i);
            Row actual = topNRows.get(i);
            assertEquals(expItem.get("rnk").asInt(), ((Number) actual.getField(2)).intValue());
            assertEquals(expItem.get("product_id").asText(), actual.getField(3));
            assertEquals(expItem.get("views").asLong(), ((Number) actual.getField(6)).longValue());
            assertEquals(expItem.get("purchases").asLong(), ((Number) actual.getField(7)).longValue());
            assertEquals(expItem.get("revenue").asDouble(), ((Number) actual.getField(8)).doubleValue(), 0.01);
        }

        // 6. Verify SQL SESSION Window TVF summaries
        List<Row> sqlSessions = collectRows(tEnv, extractSection(pipelineSql, "@QUERY:SESSIONS_SQL_START", "@QUERY:SESSIONS_SQL_END"));
        JsonNode expSessions = expected.get("sessions_sql");
        assertEquals(expSessions.size(), sqlSessions.size());
        for (Row sess : sqlSessions) {
            String uid = (String) sess.getField(0);
            JsonNode expUser = expSessions.get(uid);
            assertEquals(expUser.get("event_count").asLong(), ((Number) sess.getField(4)).longValue());
            assertEquals(expUser.get("purchases").asLong(), ((Number) sess.getField(5)).longValue());
            assertEquals(expUser.get("session_revenue").asDouble(), ((Number) sess.getField(6)).doubleValue(), 0.01);
        }
    }

    private static StreamTableEnvironment createTestTableEnv(
            String clicksUri, String catalogueUri, String pipelineSql) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");

        tEnv.executeSql("""
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
                    'connector' = 'filesystem',
                    'path' = '%s',
                    'format' = 'json'
                )
                """.formatted(clicksUri));

        tEnv.executeSql("""
                CREATE TABLE catalogue_raw_file (
                    product_id STRING NOT NULL,
                    product_name STRING,
                    category STRING,
                    price DOUBLE,
                    updated_at_ms BIGINT,
                    updated_at AS TO_TIMESTAMP_LTZ(updated_at_ms, 3),
                    WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = '%s',
                    'format' = 'json'
                )
                """.formatted(catalogueUri));

        // Convert bounded JSONL catalogue history into a Flink Versioned View (equivalent to upsert-kafka PK)
        tEnv.executeSql("""
                CREATE TEMPORARY VIEW products_catalogue AS
                SELECT product_id, product_name, category, price, updated_at_ms, updated_at
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS rn
                    FROM catalogue_raw_file
                )
                WHERE rn = 1
                """);

        String coreViewsBlock = extractSection(pipelineSql, "@SECTION:CORE_VIEWS_START", "@SECTION:CORE_VIEWS_END");
        for (String viewStmt : SqlPipelineRunner.splitStatements(SqlPipelineRunner.stripLineComments(coreViewsBlock))) {
            tEnv.executeSql(viewStmt);
        }
        return tEnv;
    }

    private static List<Row> collectRows(StreamTableEnvironment tEnv, String selectSql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(selectSql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static String loadProductionPipelineSql() throws Exception {
        Path p = Path.of("..", "flink-sql", "01_pipeline.sql");
        if (!Files.exists(p)) {
            p = Path.of("flink-sql", "01_pipeline.sql");
        }
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    private static String extractSection(String text, String startMarker, String endMarker) {
        int s = text.indexOf(startMarker);
        int e = text.indexOf(endMarker);
        if (s < 0 || e < 0 || e <= s) {
            throw new IllegalArgumentException("Missing SQL section markers: " + startMarker + " .. " + endMarker);
        }
        int lineEnd = text.indexOf('\n', s);
        return text.substring(lineEnd + 1, e).trim();
    }
}
