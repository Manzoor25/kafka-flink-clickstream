package io.clickstream.flink;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Executes parameterized Flink 2.2 SQL pipelines (including SET directives, DDL, TEMPORARY VIEWs,
 * and multi-sink EXECUTE STATEMENT SET blocks) cleanly via {@code flink run -d}.
 */
public class SqlPipelineRunner {

    private static final Pattern SET_PATTERN =
            Pattern.compile("(?is)^SET\\s+'([^']+)'\\s*=\\s*'([^']*)'\\s*$");
    private static final Pattern STATEMENT_SET_PATTERN =
            Pattern.compile("(?is)EXECUTE\\s+STATEMENT\\s+SET\\s+BEGIN(.*)END\\s*;?\\s*$");

    public static void main(String[] args) throws Exception {
        Map<String, String> cli = SessionAnalyticsJob.parseArgs(args);
        String sqlFile = cli.getOrDefault("sql-file", "/opt/flink/sql/01_pipeline.sql");

        Map<String, String> vars = Map.of(
                "PIPELINE_NAME", cli.getOrDefault("pipeline-name", "clickstream-realtime-sql-pipeline"),
                "PARALLELISM", cli.getOrDefault("parallelism", "4"),
                "KAFKA_BROKERS", cli.getOrDefault("kafka-brokers", "kafka-1:19092,kafka-2:19092,kafka-3:19092"),
                "SCHEMA_REGISTRY_URL", cli.getOrDefault("schema-registry-url", "http://schema-registry:8080/apis/ccompat/v7"),
                "CONSUMER_GROUP", cli.getOrDefault("consumer-group", "flink-sql-clickstream-v1"),
                "SCAN_STARTUP_MODE", cli.getOrDefault("scan-startup-mode", "earliest-offset"),
                "TXN_PREFIX", cli.getOrDefault("txn-prefix", "sql-rt-v1"));

        String rawSql = Files.readString(Path.of(sqlFile), StandardCharsets.UTF_8);
        for (Map.Entry<String, String> entry : vars.entrySet()) {
            rawSql = rawSql.replace("${" + entry.getKey() + "}", entry.getValue());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        executeSqlScript(tEnv, rawSql);
    }

    public static TableResult executeSqlScript(StreamTableEnvironment tEnv, String substitutedSql) {
        String stripped = stripLineComments(substitutedSql).trim();
        Matcher ssMatcher = STATEMENT_SET_PATTERN.matcher(stripped);

        String preamble = stripped;
        String statementSetBody = null;
        if (ssMatcher.find()) {
            preamble = stripped.substring(0, ssMatcher.start()).trim();
            statementSetBody = ssMatcher.group(1).trim();
        }

        for (String stmt : splitStatements(preamble)) {
            Matcher setMatcher = SET_PATTERN.matcher(stmt);
            if (setMatcher.matches()) {
                tEnv.getConfig().getConfiguration().setString(setMatcher.group(1), setMatcher.group(2));
            } else {
                tEnv.executeSql(stmt);
            }
        }

        if (statementSetBody != null) {
            StatementSet statementSet = tEnv.createStatementSet();
            for (String insertStmt : splitStatements(statementSetBody)) {
                statementSet.addInsertSql(insertStmt);
            }
            return statementSet.execute();
        }
        return null;
    }

    public static String stripLineComments(String sql) {
        StringBuilder sb = new StringBuilder();
        for (String line : sql.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("--")) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    public static List<String> splitStatements(String block) {
        List<String> out = new ArrayList<>();
        for (String part : block.split(";")) {
            String s = part.trim();
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return out;
    }
}
