#!/usr/bin/env python3
import os, time, json, subprocess
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

WATCH = [
    "one_second_combined_dna.jsonl",
    "rolling_3s.jsonl",
    "rolling_5s.jsonl",
    "rolling_15s.jsonl",
    "baseline_context.jsonl",
    "market_context.jsonl",
    "decision_gate_output.jsonl",
    "setups.jsonl",
    "qualified_setups.jsonl",
    "paper_trades.jsonl",
]

def stat_file(name):
    p = DATA / name
    if not p.exists():
        return {"exists": False, "size": 0, "mtime": 0}
    s = p.stat()
    return {"exists": True, "size": s.st_size, "mtime": s.st_mtime}

def ps_memory():
    out = subprocess.getoutput("ps aux --sort=-%mem | head -12")
    return out

def journal_errors():
    cmd = "journalctl -u nurtac-supervisor --since '20 minutes ago' --no-pager | grep -Ei 'error|exception|traceback|crash|killed|oom|failed' | tail -40"
    return subprocess.getoutput(cmd)

print("=== LIVE PIPELINE AUDIT START ===")
print("root=", ROOT)

before = {f: stat_file(f) for f in WATCH}
time.sleep(60)
after = {f: stat_file(f) for f in WATCH}

report = {
    "checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    "files": {},
    "stalled_files": [],
    "growing_files": [],
    "missing_files": [],
    "memory_top": ps_memory(),
    "recent_errors": journal_errors(),
}

for f in WATCH:
    b, a = before[f], after[f]
    delta = a["size"] - b["size"]
    report["files"][f] = {
        "exists": a["exists"],
        "size_before": b["size"],
        "size_after": a["size"],
        "delta_bytes_60s": delta,
        "mtime_after": a["mtime"],
    }
    if not a["exists"]:
        report["missing_files"].append(f)
    elif delta > 0:
        report["growing_files"].append(f)
    else:
        report["stalled_files"].append(f)

out = DATA / "live_pipeline_audit_report.json"
out.write_text(json.dumps(report, indent=2), encoding="utf-8")

print(json.dumps(report, indent=2))
print("report=", out)
