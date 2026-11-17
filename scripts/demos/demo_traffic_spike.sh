#!/usr/bin/env bash
# =============================================================================
# Failure Demo 2: Push a 10x Traffic Spike (Backpressure, Lag & Recovery)
# =============================================================================
# Scenario:
#   Baseline traffic starts at 350 events/sec, surges 10x to 3,500 events/sec
#   for 14 seconds, then returns to 350 events/sec.
#
# Expected Output:
#   1. `scripts/monitor_ops.py` samples Flink vertex busy/backpressure ratios
#      and Kafka consumer group lag every 2 seconds across the burst.
#   2. Peak consumer lag and operator busy/backpressure ratios rise visibly
#      during the 10x burst without dropped checkpoints or failed tasks.
#   3. After the burst subsides, Flink & ClickHouse drain consumer lag back to 0.
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SPIKE_SERIES_JSON="$ROOT_DIR/benchmarks/results/demo_traffic_spike_series.json"
SPIKE_SUMMARY_JSON="$ROOT_DIR/benchmarks/results/demo_traffic_spike.json"
mkdir -p "$ROOT_DIR/benchmarks/results"

echo "=== [Demo 2] Launching 1-second telemetry watcher (lag + backpressure) ==="
export PYTHONPATH="$ROOT_DIR"
$PYTHON_BIN scripts/monitor_ops.py \
  --samples 32 \
  --interval-sec 1.0 \
  --json-out "$SPIKE_SERIES_JSON" &
MON_PID=$!

echo "=== [Demo 2] Starting generator: 450 ev/s baseline -> 4,500 ev/s (10x spike) -> 450 ev/s recovery ==="
$PYTHON_BIN -m generator.clickstream_generator \
  --rate 450 \
  --duration-sec 26 \
  --spike-after-sec 4 \
  --spike-duration-sec 14 \
  --spike-multiplier 10.0 \
  --duplicate-rate 0.03 \
  --late-rate 0.0 \
  --seed 202 \
  --flush-watermark

wait "$MON_PID"

echo "=== [Demo 2] Verifying Flink raw-topic consumer lag drained to 0 post-spike ==="
$PYTHON_BIN - "$SPIKE_SERIES_JSON" "$SPIKE_SUMMARY_JSON" <<'PY'
import json, os, pathlib, sys, time
from scripts.monitor_ops import collect_consumer_lag, MONITORED_GROUPS

series = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
peak_lag_by_group = {}
peak_bp = 0.0
peak_busy = 0.0

for snap in series:
    for grp, info in snap.get("consumer_lag", {}).items():
        peak_lag_by_group[grp] = max(peak_lag_by_group.get(grp, 0), int(info["total_lag"]))
    for job in snap.get("flink_jobs", []):
        peak_bp = max(peak_bp, float(job.get("max_backpressure_ratio", 0.0)))
        peak_busy = max(peak_busy, float(job.get("max_busy_ratio", 0.0)))

bs = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29093,localhost:29094")
final_lag = {}
raw_flink_lag = 999999
for _ in range(70):
    lag_now = collect_consumer_lag(bs, MONITORED_GROUPS)
    final_lag = {g: v["total_lag"] for g, v in lag_now.items()}
    # Non-transactional source groups (flink-*) drain to exact 0; transactional 2PC topics
    # retain 1 trailing COMMIT control-batch offset per active partition at partition tail.
    raw_flink_lag = sum(v for g, v in final_lag.items() if g.startswith("flink-"))
    if raw_flink_lag == 0:
        break
    time.sleep(1.5)

summary = {
    "baseline_rate_eps": 450,
    "spike_rate_eps": 4500,
    "spike_multiplier": 10.0,
    "peak_consumer_lag_by_group": peak_lag_by_group,
    "peak_total_lag": sum(peak_lag_by_group.values()),
    "peak_flink_backpressure_ratio": round(peak_bp, 3),
    "peak_flink_busy_ratio": round(peak_busy, 3),
    "post_recovery_lag_by_group": final_lag,
    "post_recovery_flink_source_lag": raw_flink_lag,
    "recovered_to_zero_lag": raw_flink_lag == 0,
}
pathlib.Path(sys.argv[2]).write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
if not summary["recovered_to_zero_lag"]:
    raise SystemExit(f"Post-spike Flink source lag did not drain to zero: {final_lag}")
print("[Demo 2] PASSED: 10x spike absorbed and Flink consumer lag drained cleanly to 0!")
PY
