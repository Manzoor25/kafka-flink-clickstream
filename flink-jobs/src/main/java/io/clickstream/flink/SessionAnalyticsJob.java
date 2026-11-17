package io.clickstream.flink;

import io.clickstream.flink.model.ClickEvent;
import io.clickstream.flink.model.LateClickAudit;
import io.clickstream.flink.model.UserSessionSummary;
import io.clickstream.flink.serde.ConfluentAvroClickDeserializer;
import io.clickstream.flink.serde.JsonKafkaSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink 2.2 DataStream API Job: Stateful Event-ID Deduplication, Event-Time Sessionisation,
 * Allowed-Lateness Retractions/Updates, Side-Output Late Event Auditing, and Savepoint Upgrades.
 *
 * <p>All stateful operators carry stable explicit UIDs so savepoints taken under {@code --job-version v1}
 * restore cleanly into {@code --job-version v2} (Failure Demo #3) without losing RocksDB state.
 */
public class SessionAnalyticsJob {

    public static final OutputTag<ClickEvent> RAW_LATE_EVENTS_TAG =
            new OutputTag<ClickEvent>("raw-late-events") {};

    public record PipelineOutputs(
            SingleOutputStreamOperator<UserSessionSummary> sessions,
            DataStream<LateClickAudit> lateAudits) {}

    public static PipelineOutputs buildTopology(
            DataStream<ClickEvent> timestampedSource,
            long sessionGapSec,
            long allowedLatenessSec,
            Duration dedupTtl,
            String jobVersion) {

        SingleOutputStreamOperator<ClickEvent> deduped = timestampedSource
                .keyBy(ClickEvent::getEventId)
                .process(new EventIdDeduplicator(dedupTtl))
                .name("DeduplicateByEventId")
                .uid("deduplicate-by-event-id");

        SingleOutputStreamOperator<UserSessionSummary> sessionSummaries = deduped
                .keyBy(ClickEvent::getUserId)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionGapSec)))
                .allowedLateness(Duration.ofSeconds(allowedLatenessSec))
                .sideOutputLateData(RAW_LATE_EVENTS_TAG)
                .process(new SessionWindowAggregator(jobVersion))
                .name("UserSessionWindowAggregator-" + jobVersion)
                .uid("user-session-window-agg");

        DataStream<LateClickAudit> lateAudits = sessionSummaries
                .getSideOutput(RAW_LATE_EVENTS_TAG)
                .process(new LateEventEnricher())
                .name("LateEventSideOutputEnricher")
                .uid("enrich-late-audit");

        return new PipelineOutputs(sessionSummaries, lateAudits);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);
        String brokers = cli.getOrDefault("kafka-brokers", "kafka-1:19092,kafka-2:19092,kafka-3:19092");
        String groupId = cli.getOrDefault("consumer-group", "flink-ds-sessions-v1");
        String startupMode = cli.getOrDefault("scan-startup-mode", "earliest");
        String jobVersion = cli.getOrDefault("job-version", "v1");
        String txnPrefix = cli.getOrDefault("txn-prefix", "ds-rt-" + jobVersion);
        long sessionGapSec = Long.parseLong(cli.getOrDefault("session-gap-sec", "30"));
        long allowedLatenessSec = Long.parseLong(cli.getOrDefault("allowed-lateness-sec", "15"));
        int parallelism = Integer.parseInt(cli.getOrDefault("parallelism", "4"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        OffsetsInitializer offsets = "latest".equalsIgnoreCase(startupMode)
                ? OffsetsInitializer.latest()
                : OffsetsInitializer.earliest();

        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics("clicks.raw.v1")
                .setGroupId(groupId)
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new ConfluentAvroClickDeserializer())
                .build();

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTs) -> event.getEventTsMs())
                .withIdleness(Duration.ofSeconds(2));

        DataStream<ClickEvent> sourceStream = env
                .fromSource(source, watermarkStrategy, "KafkaClicksRawSource")
                .uid("kafka-clicks-raw-source");

        PipelineOutputs outputs = buildTopology(
                sourceStream,
                sessionGapSec,
                allowedLatenessSec,
                Duration.ofHours(24),
                jobVersion);

        Properties txnProps = new Properties();
        txnProps.setProperty("transaction.timeout.ms", "300000");

        JsonKafkaSerializer<UserSessionSummary> sessionJson = new JsonKafkaSerializer<>();
        KafkaSink<UserSessionSummary> sessionSink = KafkaSink.<UserSessionSummary>builder()
                .setBootstrapServers(brokers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(txnPrefix + "-sessions")
                .setKafkaProducerConfig(txnProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserSessionSummary>builder()
                                .setTopic("analytics.sessions.v1")
                                .setKeySerializationSchema(s -> s.getUserId().getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(sessionJson)
                                .build())
                .build();

        JsonKafkaSerializer<LateClickAudit> lateJson = new JsonKafkaSerializer<>();
        KafkaSink<LateClickAudit> lateSink = KafkaSink.<LateClickAudit>builder()
                .setBootstrapServers(brokers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(txnPrefix + "-late")
                .setKafkaProducerConfig(txnProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<LateClickAudit>builder()
                                .setTopic("clicks.late.v1")
                                .setKeySerializationSchema(l -> l.getUserId().getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(lateJson)
                                .build())
                .build();

        outputs.sessions().sinkTo(sessionSink).name("KafkaSessionsSink").uid("sessions-kafka-sink");
        outputs.lateAudits().sinkTo(lateSink).name("KafkaLateEventsSink").uid("late-events-kafka-sink");

        env.execute("clickstream-datastream-sessions-" + jobVersion);
    }

    /**
     * Stateful event_id deduplicator backed by RocksDB ValueState with automatic TTL expiration.
     */
    public static class EventIdDeduplicator extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {
        private static final long serialVersionUID = 1L;
        private final Duration ttl;
        private transient ValueState<Boolean> seenState;

        public EventIdDeduplicator(Duration ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(OpenContext openContext) {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(ttl)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("event-seen", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);
            this.seenState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(ClickEvent value, Context ctx, Collector<ClickEvent> out) throws Exception {
            if (value.getEventId() != null && value.getEventId().startsWith("wm-flush-")) {
                return;
            }
            if (seenState.value() == null) {
                seenState.update(Boolean.TRUE);
                out.collect(value);
            }
        }
    }

    /**
     * Evaluates merged event-time session windows. Under jobVersion="v2" (savepoint upgrade demo),
     * enriches primaryDevice with intent tier tags while reusing identical operator state.
     */
    public static class SessionWindowAggregator
            extends ProcessWindowFunction<ClickEvent, UserSessionSummary, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final String jobVersion;

        public SessionWindowAggregator(String jobVersion) {
            this.jobVersion = jobVersion;
        }

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<ClickEvent> elements,
                Collector<UserSessionSummary> out) {
            long minTs = Long.MAX_VALUE;
            long maxTs = Long.MIN_VALUE;
            long total = 0;
            long views = 0;
            long carts = 0;
            long checkouts = 0;
            long purchases = 0;
            Set<String> products = new HashSet<>();
            Map<String, Integer> deviceCounts = new HashMap<>();

            for (ClickEvent ev : elements) {
                total++;
                minTs = Math.min(minTs, ev.getEventTsMs());
                maxTs = Math.max(maxTs, ev.getEventTsMs());
                if (ev.getProductId() != null && !ev.getProductId().isEmpty()) {
                    products.add(ev.getProductId());
                }
                String dev = ev.getDevice() == null || ev.getDevice().isEmpty() ? "web" : ev.getDevice();
                deviceCounts.merge(dev, 1, Integer::sum);

                switch (ev.getEventType()) {
                    case "view" -> views++;
                    case "add_to_cart" -> carts++;
                    case "checkout" -> checkouts++;
                    case "purchase" -> purchases++;
                    default -> {
                    }
                }
            }

            if (total == 0) {
                return;
            }

            String primaryDevice = deviceCounts.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse("web");

            // Query evolution for Failure Demo #3 (savepoint upgrade v1 -> v2)
            if (jobVersion.startsWith("v2") && (purchases > 0 || checkouts > 0 || carts > 0)) {
                String tier = purchases > 0 ? "buyer" : (checkouts > 0 ? "checkout-abandon" : "cart-abandon");
                primaryDevice = primaryDevice + ":" + tier;
            }

            double durationSec = Math.round(((maxTs - minTs) / 1000.0) * 100.0) / 100.0;
            String sessionId = userId + "-" + minTs;

            out.collect(new UserSessionSummary(
                    sessionId,
                    userId,
                    minTs,
                    maxTs,
                    durationSec,
                    total,
                    products.size(),
                    views,
                    carts,
                    checkouts,
                    purchases,
                    purchases > 0,
                    primaryDevice,
                    jobVersion));
        }
    }

    /**
     * Annotates side-output events dropped by the session window with exact watermark lateness.
     */
    public static class LateEventEnricher extends ProcessFunction<ClickEvent, LateClickAudit> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(ClickEvent ev, Context ctx, Collector<LateClickAudit> out) {
            long wm = ctx.timerService().currentWatermark();
            long lateness = wm > Long.MIN_VALUE ? Math.max(0L, wm - ev.getEventTsMs()) : 0L;
            out.collect(new LateClickAudit(
                    ev.getEventId(),
                    ev.getUserId(),
                    ev.getEventType(),
                    ev.getProductId(),
                    ev.getEventTsMs(),
                    wm,
                    lateness,
                    System.currentTimeMillis()));
        }
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            if (token.startsWith("--")) {
                String key = token.substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    out.put(key, args[++i]);
                } else {
                    out.put(key, "true");
                }
            }
        }
        return out;
    }
}
