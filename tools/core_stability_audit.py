#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "core_stability_report.json"

FILES = [
    "setups.jsonl",
    "observations.jsonl",
    "qualified_setups.jsonl",
    "paper_trades.jsonl",
    "evidence_stream.jsonl",
    "smart_money_dna.jsonl",
    "structure_1s.jsonl",
    "rolling_3s_dna.jsonl",
    "rolling_5s_dna.jsonl",
    "rolling_15s_dna.jsonl",
]

SERVICES = [
    "nurtac-supervisor",
    "nurtac-observer",
    "nurtac-paper",
    "nurtac-setup-guardian",
    "nurtac-data-archiver.timer",
]

def sh(cmd):
    return subprocess.getoutput(cmd)

def stat_file(name):
    p = DATA / name
    if not p.exists():
        return {"exists": False, "size_mb": 0, "mtime": None}
    st = p.stat()
    return {
        "exists": True,
        "size_mb": round(st.st_size / 1024 / 1024, 2),
        "mtime": st.st_mtime,
    }

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "services": {s: sh(f"systemctl is-active {s} 2>/dev/null || true") for s in SERVICES},
    "files": {f: stat_file(f) for f in FILES},
    "cursor_state": sh("ls -lh data/cursors 2>/dev/null || true"),
    "guardian_state_exists": (DATA / "setup_guardian_state.json").exists(),
    "guardian_reports_tail": sh("tail -5 data/setup_guardian_reports.jsonl 2>/dev/null || true"),
    "memory_top": sh("ps aux --sort=-%mem | head -12"),
    "recent_oom": sh("journalctl --since '30 minutes ago' --no-pager | grep -Ei 'oom|killed|memorymax' | tail -50 || true"),
    "disk": sh("df -h /root / 2>/dev/null | tail -5"),
    "archiver_report": sh("cat data/data_archiver_report.json 2>/dev/null || true"),
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
print("REPORT:", OUT)
