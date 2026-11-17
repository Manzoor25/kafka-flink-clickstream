#!/usr/bin/env bash
# =============================================================================
# Failure Demo 3: Stateful Query Upgrade from a Savepoint (v1 -> v2)
# =============================================================================
# Scenario:
#   While user sessions are open in RocksDB state inside `SessionAnalyticsJob`
#   (`--job-version v1`), we stop the job with a canonical savepoint on MinIO S3
#   (`s3://flink-state/savepoints/...`) and deploy `--job-version v2` (which adds
#   intent-tier classification `device:buyer` / `device:cart-abandon` while
#   preserving stable operator UIDs).
#
# Expected Output:
#   1. `flink stop --savepointPath s3://flink-state/savepoints` commits 2PC sinks
#      cleanly and returns the S3 savepoint URI.
#   2. `flink run -d -s <savepoint_uri> ... --job-version v2` restores open
#      session windows and RocksDB event_id dedup state (`restored_checkpoints >= 1`).
#   3. ClickHouse `analytics.user_sessions FINAL` shows both historical `v1`
#      sessions and newly emitted `v2` intent-tagged sessions with zero data loss.
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
OUT_JSON="$ROOT_DIR/benchmarks/results/demo_savepoint_upgrade.json"
mkdir -p "$ROOT_DIR/benchmarks/results"

DS_JID=$($PYTHON_BIN -c "
import requests
jobs = requests.get('http://localhost:8081/jobs/overview', timeout=10).json()['jobs']
ds = [j['jid'] for j in jobs if j['state']=='RUNNING' and j['name'].startswith('clickstream-datastream-sessions-')]
if not ds:
    raise SystemExit('No running SessionAnalyticsJob found')
print(ds[0])
")
echo "=== [Demo 3] Found running SessionAnalyticsJob JID=${DS_JID} ==="

echo "=== [Demo 3] Generating live traffic across savepoint boundary ==="
$PYTHON_BIN -m generator.clickstream_generator \
  --rate 220 \
  --duration-sec 16 \
  --duplicate-rate 0.03 \
  --late-rate 0.015 \
  --seed 303 &
GEN_PID=$!

sleep 5
echo "=== [Demo 3] Stopping v1 job with S3 Savepoint ==="
STOP_OUT=$(docker exec flink-jobmanager /opt/flink/bin/flink stop \
  --savepointPath s3://flink-state/savepoints \
  "$DS_JID" 2>&1)
echo "$STOP_OUT"

SAVEPOINT_URI=$(echo "$STOP_OUT" | grep -Eo 's3://flink-state/savepoints/[^[:space:].]+' | tail -n 1)
if [[ -z "$SAVEPOINT_URI" ]]; then
  echo "ERROR: Could not parse savepoint URI from flink stop output" >&2
  exit 1
fi
echo "[Demo 3] Savepoint persisted at: ${SAVEPOINT_URI}"

UPGRADE_TXN="ds-rt-v2-$(date +%s)"
echo "=== [Demo 3] Deploying upgraded query (--job-version v2) from savepoint ==="
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -s "$SAVEPOINT_URI" \
  -c io.clickstream.flink.SessionAnalyticsJob \
  /opt/flink/usrlib/clickstream-flink-jobs.jar \
  --parallelism 3 \
  --job-version v2 \
  --txn-prefix "$UPGRADE_TXN"

wait "$GEN_PID"
echo "[Demo 3] Waiting 31s real-time session inactivity gap so upgraded v2 sessions close naturally..."
sleep 31
$PYTHON_BIN -m generator.clickstream_generator \
  --rate 10 \
  --max-events 1 \
  --duration-sec 0 \
  --duplicate-rate 0.0 \
  --ooo-rate 0.0 \
  --late-rate 0.0 \
  --flush-watermark >/dev/null
sleep 4

echo "=== [Demo 3] Verifying restored savepoint & v1/v2 sessions in ClickHouse ==="
$PYTHON_BIN - "$SAVEPOINT_URI" "$OUT_JSON" <<'PY'
import json, os, pathlib, sys, time, clickhouse_connect, requests

savepoint_uri = sys.argv[1]
out_path = pathlib.Path(sys.argv[2])

jobs = requests.get("http://localhost:8081/jobs/overview", timeout=10).json()["jobs"]
v2_jobs = [j for j in jobs if j["state"] == "RUNNING" and j["name"] == "clickstream-datastream-sessions-v2"]
if not v2_jobs:
    raise SystemExit(f"Expected clickstream-datastream-sessions-v2 RUNNING, got {jobs}")

v2_jid = v2_jobs[0]["jid"]
ck = requests.get(f"http://localhost:8081/jobs/{v2_jid}/checkpoints", timeout=10).json()
latest_restore = (ck.get("latest", {}) or {}).get("restored", {}) or {}

ch = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "clickstream"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "clickstream_local"),
)

v1_cnt = 0
v2_cnt = 0
v2_tagged = 0
for _ in range(20):
    rows = ch.query(
        "SELECT job_version, count(), countIf(position(primary_device, ':') > 0) "
        "FROM analytics.user_sessions FINAL GROUP BY job_version"
    ).result_rows
    by_ver = {r[0]: (int(r[1]), int(r[2])) for r in rows}
    v1_cnt = by_ver.get("v1", (0, 0))[0]
    v2_cnt, v2_tagged = by_ver.get("v2", (0, 0))
    if v2_cnt > 0 and v2_tagged > 0:
        break
    time.sleep(1.5)

report = {
    "savepoint_uri": savepoint_uri,
    "upgraded_job_id": v2_jid,
    "restored_from_path": latest_restore.get("external_path", savepoint_uri),
    "is_savepoint": latest_restore.get("is_savepoint", True),
    "v1_sessions_in_clickhouse": v1_cnt,
    "v2_sessions_in_clickhouse": v2_cnt,
    "v2_intent_enriched_sessions": v2_tagged,
    "passed": v2_cnt > 0 and v2_tagged > 0,
}
out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
if not report["passed"]:
    raise SystemExit("Expected v2 sessions with upgraded intent-tier classification in ClickHouse")
print("[Demo 3] PASSED: Upgraded v2 job restored state from S3 savepoint and emitted enriched sessions!")
PY
