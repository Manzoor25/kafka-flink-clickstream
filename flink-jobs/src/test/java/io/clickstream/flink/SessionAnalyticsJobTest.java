package io.clickstream.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.clickstream.flink.model.ClickEvent;
import io.clickstream.flink.model.LateClickAudit;
import io.clickstream.flink.model.UserSessionSummary;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * Verifies Flink 2.2 DataStream SessionAnalyticsJob:
 *  - RocksDB/Keyed ValueState deduplication by event_id
 *  - EventTimeSessionWindows merging & summary calculation
 *  - Extreme late event routing (> watermark + allowedLateness) to side output
 *  - Schema parity between schemas/click_event.avsc and classpath resource
 */
class SessionAnalyticsJobTest {

    @Test
    void avroResourceMatchesCanonicalRepoSchema() throws Exception {
        Path repoSchema = Path.of("..", "schemas", "click_event.avsc");
        if (Files.exists(repoSchema)) {
            String canonical = Files.readString(repoSchema, StandardCharsets.UTF_8).replace("\r\n", "\n");
            try (var in = getClass().getResourceAsStream("/schemas/click_event.avsc")) {
                String bundled = new String(in.readAllBytes(), StandardCharsets.UTF_8).replace("\r\n", "\n");
                assertEquals(canonical, bundled, "Bundled click_event.avsc must match schemas/click_event.avsc");
            }
        }
    }

    @Test
    void deduplicatesMergesSessionsAndRoutesLateSideOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long t0 = 1_767_225_600_000L;
        List<ClickEvent> events = List.of(
                // user-A session: view, duplicate view (must drop), add_to_cart, checkout, purchase
                new ClickEvent("e1", "user-A", "sA", "view", "SKU-1001", 1, "web", "/", t0 + 1_000, t0 + 1_000, false),
                new ClickEvent("e1", "user-A", "sA", "view", "SKU-1001", 1, "web", "/", t0 + 1_000, t0 + 1_050, false),
                new ClickEvent("e2", "user-A", "sA", "add_to_cart", "SKU-1001", 1, "web", "/", t0 + 4_000, t0 + 4_000, false),
                new ClickEvent("e3", "user-A", "sA", "checkout", "SKU-1001", 1, "web", "/", t0 + 8_000, t0 + 8_000, false),
                new ClickEvent("e4", "user-A", "sA", "purchase", "SKU-1001", 1, "web", "/", t0 + 11_000, t0 + 11_000, false),
                // Advance watermark far ahead (+120s -> watermark = t0 + 115s, well past user-A window + 15s lateness)
                new ClickEvent("e5", "user-B", "sB", "view", "SKU-1002", 1, "ios", "/", t0 + 120_000, t0 + 120_000, false),
                // Extreme late event at t0 + 2s arrives when watermark is t0 + 115s -> must hit late side output!
                new ClickEvent("e-late-99", "user-A", "sA", "view", "SKU-1003", 1, "web", "/", t0 + 2_000, t0 + 121_000, true),
                // Advance watermark once more so user-B session closes
                new ClickEvent("e6", "user-C", "sC", "view", "SKU-1004", 1, "android", "/", t0 + 200_000, t0 + 200_000, false)
        );

        WatermarkStrategy<ClickEvent> wm = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getEventTsMs());

        DataStream<ClickEvent> src = env.fromData(events).assignTimestampsAndWatermarks(wm);
        SessionAnalyticsJob.PipelineOutputs out = SessionAnalyticsJob.buildTopology(
                src,
                10L, // 10s session gap
                5L,  // 5s allowed lateness
                Duration.ofHours(1),
                "v2");

        // Collect sessions
        List<UserSessionSummary> sessions = new ArrayList<>();
        try (CloseableIterator<UserSessionSummary> it = out.sessions().executeAndCollect()) {
            it.forEachRemaining(sessions::add);
        }

        UserSessionSummary userASession = sessions.stream()
                .filter(s -> "user-A".equals(s.getUserId()))
                .findFirst()
                .orElseThrow();

        assertEquals(4L, userASession.getEventCount(), "Duplicate event e1 and extreme late event must not inflate user-A session");
        assertEquals(1L, userASession.getViewCount());
        assertEquals(1L, userASession.getCartCount());
        assertEquals(1L, userASession.getCheckoutCount());
        assertEquals(1L, userASession.getPurchaseCount());
        assertTrue(userASession.isConverted());
        assertEquals("web:buyer", userASession.getPrimaryDevice(), "v2 upgrade logic enriches buyer device tier");

        // Re-run focusing on side output collection
        DataStream<ClickEvent> src2 = env.fromData(events).assignTimestampsAndWatermarks(wm);
        SessionAnalyticsJob.PipelineOutputs out2 = SessionAnalyticsJob.buildTopology(
                src2, 10L, 5L, Duration.ofHours(1), "v1");
        out2.sessions().sinkTo(new DiscardingSink<>());

        List<LateClickAudit> lateAudits = Collections.synchronizedList(new ArrayList<>());
        try (CloseableIterator<LateClickAudit> lateIt = out2.lateAudits().executeAndCollect()) {
            lateIt.forEachRemaining(lateAudits::add);
        }

        assertEquals(1, lateAudits.size(), "Expected exactly 1 extreme late event routed to side output");
        assertEquals("e-late-99", lateAudits.get(0).getEventId());
        assertTrue(lateAudits.get(0).getLatenessMs() >= 100_000L);
    }
}
