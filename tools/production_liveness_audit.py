#!/usr/bin/env python3
import os, json, time, subprocess
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "production_liveness_report.json"

WATCH = {
    "layer0": "combined_1s_dna_btcusdt.jsonl",
    "rolling_3s": "rolling_3s_dna.jsonl",
    "rolling_5s": "rolling_5s_dna.jsonl",
    "rolling_15s": "rolling_15s_dna.jsonl",
    "baseline": "historical_baseline_dna.jsonl",
    "decision_gate": "decision_gate_output.jsonl",
    "smart_money_1s": "structure_1s.jsonl",
    "evidence": "evidence_stream.jsonl",
    "setup": "setups.jsonl",
    "observer": "qualified_setups.jsonl",
    "paper_trade": "paper_trades.jsonl",
    "market_context": "market_context.jsonl",
}

def stat_file(name):
    p = DATA / name
    if not p.exists():
        return {"exists": False, "size": 0, "mtime": 0}
    s = p.stat()
    return {"exists": True, "size": s.st_size, "mtime": s.st_mtime}

def shell(cmd):
    return subprocess.getoutput(cmd)

before = {k: stat_file(v) for k, v in WATCH.items()}
time.sleep(60)
after = {k: stat_file(v) for k, v in WATCH.items()}

files = {}
alive = []
stalled = []
missing = []

for key, fname in WATCH.items():
    b = before[key]
    a = after[key]
    delta = a["size"] - b["size"]

    status = "alive"
    if not a["exists"]:
        status = "missing_output"
        missing.append(key)
    elif delta <= 0:
        status = "stalled"
        stalled.append(key)
    else:
        alive.append(key)

    files[key] = {
        "file": str(DATA / fname),
        "exists": a["exists"],
        "size_before": b["size"],
        "size_after": a["size"],
        "delta_bytes_60s": delta,
        "mtime": a["mtime"],
        "status": status,
    }

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "summary": {
        "alive_count": len(alive),
        "stalled_count": len(stalled),
        "missing_count": len(missing),
        "alive": alive,
        "stalled": stalled,
        "missing": missing,
    },
    "files": files,
    "memory_top": shell("ps aux --sort=-%mem | head -15"),
    "supervisor_status": shell("systemctl status nurtac-supervisor --no-pager | head -25"),
    "recent_errors": shell("journalctl -u nurtac-supervisor --since '20 minutes ago' --no-pager | grep -Ei 'error|exception|traceback|crash|oom|killed|failed' | tail -80"),
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
print("REPORT:", OUT)
