#!/usr/bin/env bash
# =============================================================================
# Failure Demo 4: Reprocess from Earliest Offset (Kappa Architecture Replay)
# =============================================================================
# Scenario:
#   Suppose a downstream serving schema migration or metric bug requires
#   rebuilding every analytical table from scratch. Instead of maintaining a
#   separate Spark/Hive nightly batch pipeline (Lambda architecture), we wipe
#   ClickHouse's SQL serving tables to 0 rows and replay `clicks.raw.v1` +
#   compacted `products.catalogue.v1` from offset 0 (`earliest-offset`) through
#   the exact same `flink-sql/01_pipeline.sql` DAG.
#
# Expected Output:
#   1. All 6 SQL-populated ClickHouse serving tables are truncated to 0 rows.
#   2. Flink SQL job restarts with `--scan-startup-mode earliest-offset` and a
#      fresh consumer group, replaying the entire immutable Kafka history.
#   3. Every serving table (`clicks_enriched`, `minute_metrics_tumbling`,
#      `minute_metrics_hopping`, `user_sessions_sql`, `funnel_window_metrics`,
#      `top_products_window`) is deterministically rebuilt, and `verify_recount.py`
#      confirms exact parity against raw Kafka.
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
OUT_JSON="$ROOT_DIR/benchmarks/results/demo_reprocess_earliest.json"
mkdir -p "$ROOT_DIR/benchmarks/results"

echo "=== [Demo 4] Recording pre-wipe ClickHouse table counts & cancelling SQL job ==="
PRE_COUNTS=$($PYTHON_BIN - <<'PY'
import json, os, requests, subprocess, clickhouse_connect

ov = requests.get("http://localhost:8081/jobs/overview", timeout=10).json()["jobs"]
for j in ov:
    if j["state"] == "RUNNING" and "sql" in j["name"]:
        subprocess.run(["docker", "exec", "flink-jobmanager", "/opt/flink/bin/flink", "cancel", j["jid"]], check=True, stdout=subprocess.DEVNULL)

ch = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "clickstream"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"),
)
tables = [
    "clicks_enriched",
    "minute_metrics_tumbling",
    "minute_metrics_hopping",
    "user_sessions_sql",
    "funnel_window_metrics",
    "top_products_window",
]
pre = {}
for t in tables:
    pre[t] = int(ch.query(f"SELECT count() FROM analytics.{t} FINAL").first_row[0])
    ch.command(f"TRUNCATE TABLE analytics.{t}")
post_wipe = {t: int(ch.query(f"SELECT count() FROM analytics.{t} FINAL").first_row[0]) for t in tables}
print(json.dumps({"before_wipe": pre, "after_truncate": post_wipe}))
PY
)
echo "[Demo 4] Truncated serving store: $PRE_COUNTS"

EPOCH_TAG=$(date +%s)
echo "=== [Demo 4] Submitting Kappa full historical replay from earliest-offset ==="
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c io.clickstream.flink.SqlPipelineRunner \
  /opt/flink/usrlib/clickstream-flink-jobs.jar \
  --sql-file /opt/flink/sql/01_pipeline.sql \
  --pipeline-name clickstream-realtime-sql-pipeline \
  --parallelism 3 \
  --scan-startup-mode earliest-offset \
  --consumer-group "flink-sql-kappa-replay-${EPOCH_TAG}" \
  --txn-prefix "sql-kappa-${EPOCH_TAG}"

# Wait for the replayed SQL job to reach RUNNING and catch up on historical partitions before advancing final watermark
$PYTHON_BIN - "flink-sql-kappa-replay-${EPOCH_TAG}" <<'PY'
import os, sys, time, requests
from scripts.monitor_ops import collect_consumer_lag

grp = sys.argv[1]
bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094")
deadline = time.time() + 60.0
while time.time() < deadline:
    try:
        ov = requests.get("http://localhost:8081/jobs/overview", timeout=5).json().get("jobs", [])
        sql_jobs = [j for j in ov if j["state"] == "RUNNING" and "sql" in j["name"]]
        if sql_jobs:
            lag = collect_consumer_lag(bs, [grp]).get(grp, {})
            if int(lag.get("committed_partitions", 0)) >= 12 and int(lag.get("total_lag", 999999)) == 0:
                break
    except Exception:
        pass
    time.sleep(1.5)
PY

$PYTHON_BIN -m generator.clickstream_generator \
  --rate 10 \
  --max-events 1 \
  --duration-sec 0 \
  --duplicate-rate 0.0 \
  --ooo-rate 0.0 \
  --late-rate 0.0 \
  --flush-watermark \
  --flush-offset-sec 65.0 >/dev/null

echo "=== [Demo 4] Verifying full metric reconstruction & exact raw-Kafka batch recount ==="
$PYTHON_BIN scripts/verify_recount.py \
  --exclude-synthetic-late \
  --wait-timeout-sec 60 \
  --json-out "$ROOT_DIR/benchmarks/results/demo_reprocess_recount.json"

$PYTHON_BIN - "$PRE_COUNTS" "$OUT_JSON" <<'PY'
import json, os, pathlib, sys, time, clickhouse_connect

counts_info = json.loads(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])

ch = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "clickstream"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"),
)
tables = [
    "clicks_enriched",
    "minute_metrics_tumbling",
    "minute_metrics_hopping",
    "user_sessions_sql",
    "funnel_window_metrics",
    "top_products_window",
]
rebuilt = {}
for _ in range(20):
    rebuilt = {t: int(ch.query(f"SELECT count() FROM analytics.{t} FINAL").first_row[0]) for t in tables}
    if all(rebuilt[t] > 0 for t in tables):
        break
    time.sleep(1.5)

report = {
    "before_wipe": counts_info["before_wipe"],
    "after_truncate": counts_info["after_truncate"],
    "rebuilt_from_earliest_offset": rebuilt,
    "all_tables_rebuilt": all(rebuilt[t] > 0 for t in tables),
    "why_kappa_replaces_batch_layer": (
        "Kafka retains the immutable raw clickstream log and compacted versioned product catalogue. "
        "Because Flink evaluates deterministic event-time watermarks, KeepFirstRow event_id deduplication, "
        "and FOR SYSTEM_TIME AS OF temporal joins, replaying from earliest-offset through the identical "
        "SQL DAG reconstructs every historical window, funnel conversion rate, and session summary without "
        "maintaining a separate Spark/Hive batch codebase that could drift semantically."
    ),
}
out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
if not report["all_tables_rebuilt"]:
    raise SystemExit(f"Expected all 6 analytical tables > 0 after Kappa replay, got: {rebuilt}")
print("[Demo 4] PASSED: All 6 serving tables rebuilt from Kafka earliest-offset via single Kappa codebase!")
PY
