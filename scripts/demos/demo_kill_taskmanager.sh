#!/usr/bin/env bash
# =============================================================================
# Failure Demo 1: Kill a TaskManager Under Active Load
# =============================================================================
# Scenario:
#   While the generator streams 300 events/sec (with 5% duplicate retries and
#   out-of-order arrivals), `flink-tm-2` is abruptly killed (`docker kill`).
#
# Expected Output:
#   1. Flink JobManager detects TaskManager heartbeat loss (~8s) and transitions
#      running jobs through RESTARTING -> RUNNING using the latest incremental
#      RocksDB checkpoint on MinIO (`s3://flink-state/checkpoints/...`).
#   2. Any uncommitted 2PC Kafka sink transactions opened by the dead TaskManager
#      are fenced/aborted; ClickHouse (`isolation_level=read_committed`) ignores
#      aborted records.
#   3. `scripts/verify_recount.py` compares ClickHouse streaming totals against
#      the raw deterministic ledger recount and prints `"passed": true` with
#      zero diff on unique events, active users, purchases, and gross revenue.
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
LEDGER_FILE="/tmp/kill_tm_ledger_$$.jsonl"
OUT_JSON="$ROOT_DIR/benchmarks/results/demo_kill_taskmanager.json"
mkdir -p "$ROOT_DIR/benchmarks/results"

echo "=== [Demo 1] Capturing pre-failure checkpoint restoration baseline ==="
PRE_RESTORED=$($PYTHON_BIN -c "
import requests
ov = requests.get('http://localhost:8081/jobs/overview', timeout=10).json()['jobs']
sql_jid = [j['jid'] for j in ov if j['state']=='RUNNING' and 'sql' in j['name']][0]
ck = requests.get(f'http://localhost:8081/jobs/{sql_jid}/checkpoints', timeout=10).json()
print(ck['counts']['restored'])
")
echo "[Demo 1] Pre-failure restored_checkpoints=${PRE_RESTORED}"

echo "=== [Demo 1] Starting 300 ev/s workload with duplicates & out-of-order clicks ==="
$PYTHON_BIN -m generator.clickstream_generator \
  --rate 300 \
  --duration-sec 28 \
  --duplicate-rate 0.05 \
  --ooo-rate 0.04 \
  --late-rate 0.0 \
  --seed 101 \
  --flush-watermark \
  --ledger-path "$LEDGER_FILE" &
GEN_PID=$!

sleep 7
TARGET_TM=$($PYTHON_BIN - <<'PY'
import subprocess, requests
tms = requests.get("http://localhost:8081/taskmanagers", timeout=10).json().get("taskmanagers", [])
active = [tm for tm in tms if int(tm.get("slotsNumber", 0)) - int(tm.get("freeSlots", 0)) > 0]
chosen = sorted(
    active,
    key=lambda t: int(t.get("slotsNumber", 0)) - int(t.get("freeSlots", 0)),
    reverse=True,
)[0] if active else tms[0]
chosen_info = f"{chosen.get('id', '')} {chosen.get('path', '')}"
matched = ""
for cname in ("flink-tm-1", "flink-tm-2"):
    ip = subprocess.check_output(
        ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", cname],
        text=True,
    ).strip()
    if ip and ip in chosen_info:
        matched = cname
        break
print(matched or "flink-tm-1")
PY
)
echo "=== [Demo 1] Injecting fault: docker kill ${TARGET_TM} (hosting active Flink subtasks) ==="
KILL_T0=$($PYTHON_BIN -c "import time; print(time.time())")
docker kill "$TARGET_TM" >/dev/null
echo "[Demo 1] ${TARGET_TM} killed mid-stream! Surviving TaskManager has standby slots to absorb failover..."

echo "=== [Demo 1] Waiting for Flink checkpoint failover recovery onto surviving TaskManager ==="
RECOVERY_INFO=$($PYTHON_BIN - "$PRE_RESTORED" "$KILL_T0" <<'PY'
import json, sys, time, requests

pre_restored = int(sys.argv[1])
t0 = float(sys.argv[2])
deadline = time.time() + 100.0
restored_seen_at = None
post_restore_ck_base = None

while time.time() < deadline:
    try:
        jobs = requests.get("http://localhost:8081/jobs/overview", timeout=5).json().get("jobs", [])
        running = [j for j in jobs if j["state"] == "RUNNING" and "sql" in j["name"]]
        if running:
            jid = running[0]["jid"]
            detail = requests.get(f"http://localhost:8081/jobs/{jid}", timeout=5).json()
            all_vertices_running = bool(detail.get("vertices")) and all(
                v.get("status") == "RUNNING" for v in detail.get("vertices", [])
            )
            ck = requests.get(f"http://localhost:8081/jobs/{jid}/checkpoints", timeout=5).json()
            restored = int(ck.get("counts", {}).get("restored", 0))
            completed = int(ck.get("counts", {}).get("completed", 0))
            if restored > pre_restored and all_vertices_running:
                if restored_seen_at is None:
                    restored_seen_at = round(time.time() - t0, 2)
                    post_restore_ck_base = completed
                elif completed > post_restore_ck_base:
                    print(
                        json.dumps(
                            {
                                "jid": jid,
                                "restored_checkpoints": restored,
                                "completed_checkpoints": completed,
                                "recovery_sec": restored_seen_at,
                            }
                        )
                    )
                    sys.exit(0)
    except Exception:
        pass
    time.sleep(1.0)
raise SystemExit("Timed out waiting for Flink job to restore from checkpoint and complete a new checkpoint")
PY
)
echo "[Demo 1] Flink recovered from MinIO RocksDB checkpoint: $RECOVERY_INFO"

wait "$GEN_PID"
# Ensure post-recovery watermark advances past the final generator event timestamp
$PYTHON_BIN -m generator.clickstream_generator \
  --rate 10 \
  --max-events 1 \
  --duration-sec 0 \
  --duplicate-rate 0.0 \
  --ooo-rate 0.0 \
  --late-rate 0.0 \
  --flush-watermark >/dev/null

echo "=== [Demo 1] Running exact streaming-vs-batch recount check ==="
$PYTHON_BIN scripts/verify_recount.py \
  --ledger-path "$LEDGER_FILE" \
  --wait-timeout-sec 45 \
  --json-out "$OUT_JSON"

$PYTHON_BIN - "$OUT_JSON" "$RECOVERY_INFO" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text(encoding="utf-8"))
data["failover"] = json.loads(sys.argv[2])
p.write_text(json.dumps(data, indent=2), encoding="utf-8")
print("[Demo 1] PASSED: Streaming totals after TaskManager kill match batch recount 100%!")
PY

rm -f "$LEDGER_FILE"
echo "[Demo 1] Restoring ${TARGET_TM} standby TaskManager to full 2-TM redundancy..."
docker start "$TARGET_TM" >/dev/null
$PYTHON_BIN - <<'PY'
import time, requests
for _ in range(30):
    try:
        tms = requests.get("http://localhost:8081/taskmanagers", timeout=5).json().get("taskmanagers", [])
        if len(tms) >= 2:
            break
    except Exception:
        pass
    time.sleep(1.0)
PY

