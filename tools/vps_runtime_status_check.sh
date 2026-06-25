#!/usr/bin/env bash
set -euo pipefail

cd /root/NurtacCoreEngineClaude

echo "=================================================="
echo "NURTAC CORE ENGINE CLAUDE — VPS RUNTIME STATUS"
echo "Generated: $(date -u)"
echo "=================================================="

echo
echo "=== 1) SYSTEM RESOURCE ==="
free -m || true
df -h /root || true

echo
echo "=== 2) PYTHON ENGINE PROCESSES ==="
ps aux | grep -E "main.py|production_supervisor|evidence_engine.py|observer_engine.py|detector_engine.py|decision_gate.py|smart_money_engine.py|scenario_engine.py|paper_trade_engine.py|historical|calibration|validator.py|regime_engine.py|market_context_engine.py|liquidation_engine.py" | grep -v grep || echo "NO PYTHON ENGINE PROCESSES FOUND"

echo
echo "=== 3) SYSTEMD / TIMERS ==="
systemctl list-units --type=service --all | grep -i "nurtac\|core\|paper\|calibration\|historical" || true
echo
systemctl list-timers --all | grep -i "nurtac\|paper\|calibration\|historical" || true

echo
echo "=== 4) KEY FILE FRESHNESS ==="
python3 - <<'PY'
import time
from pathlib import Path

files = [
"data/system_health.json",
"data/combined_1s_dna_btcusdt.jsonl",
"data/rolling_3s_dna.jsonl",
"data/rolling_5s_dna.jsonl",
"data/rolling_15s_dna.jsonl",
"data/decision_gate_output.jsonl",
"data/evidence_stream.jsonl",
"data/setups.jsonl",
"data/observations.jsonl",
"data/qualified_setups.jsonl",
"data/paper_trades.jsonl",
"data/paper_closed.jsonl",
"data/historical_outcomes.jsonl",
"data/historical_outcome_observations.jsonl",
"data/calibration_profiles.json",
"data/hypothesis_outcomes.jsonl",
]

now = time.time()
for f in files:
    p = Path(f)
    if not p.exists():
        print(f"MISSING | {f}")
        continue
    age = now - p.stat().st_mtime
    size = p.stat().st_size / 1024 / 1024
    status = "FRESH" if age < 60 else "STALE" if age < 600 else "OLD"
    print(f"{status:6} | age={age:8.1f}s | size={size:8.2f}MB | {f}")
PY

echo
echo "=== 5) 120s GROWTH TEST ==="
python3 - <<'PY'
import time
from pathlib import Path

files = [
"data/combined_1s_dna_btcusdt.jsonl",
"data/decision_gate_output.jsonl",
"data/evidence_stream.jsonl",
"data/setups.jsonl",
"data/observations.jsonl",
"data/qualified_setups.jsonl",
"data/paper_trades.jsonl",
"data/paper_closed.jsonl",
"data/hypothesis_outcomes.jsonl",
]

start = {}
for f in files:
    p = Path(f)
    start[f] = p.stat().st_size if p.exists() else -1

print("Waiting 120 seconds...")
time.sleep(120)

for f in files:
    p = Path(f)
    end = p.stat().st_size if p.exists() else -1
    delta = end - start[f]
    growing = "GROWING" if delta > 0 else "NOT_GROWING"
    print(f"{growing:12} | delta={delta:10} bytes | {f}")
PY

echo
echo "=== 6) QUICK VERDICT ==="
python3 - <<'PY'
import subprocess, time
from pathlib import Path

proc = subprocess.getoutput(
    "ps aux | grep -E 'main.py|production_supervisor|evidence_engine.py|observer_engine.py|detector_engine.py|decision_gate.py' | grep -v grep"
)

critical = [
"data/combined_1s_dna_btcusdt.jsonl",
"data/evidence_stream.jsonl",
"data/setups.jsonl",
"data/observations.jsonl",
]

now = time.time()
fresh = []
stale = []

for f in critical:
    p = Path(f)
    if not p.exists():
        stale.append(f + " MISSING")
    else:
        age = now - p.stat().st_mtime
        if age < 120:
            fresh.append(f)
        else:
            stale.append(f + f" age={age:.1f}s")

if not proc.strip():
    print("ROOT_STATUS: ENGINE_STOPPED")
elif stale:
    print("ROOT_STATUS: PARTIAL_RUNTIME")
else:
    print("ROOT_STATUS: LIVE_RUNNING")

print()
print("Fresh critical files:", len(fresh))
for x in fresh:
    print("  OK:", x)

print()
print("Stale/missing critical files:", len(stale))
for x in stale:
    print("  PROBLEM:", x)
PY

echo
echo "=================================================="
echo "STATUS CHECK COMPLETE"
echo "=================================================="
