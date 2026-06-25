#!/usr/bin/env python3
import json
import subprocess
import time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "cursor_gap_audit_report.json"

def tail_jsonl(name, n=50):
    p = DATA / name
    if not p.exists():
        return []
    rows = []
    for line in subprocess.getoutput(f"tail -{n} {p}").splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

setups = tail_jsonl("setups.jsonl", 50)
obs = tail_jsonl("observations.jsonl", 300)
qualified = tail_jsonl("qualified_setups.jsonl", 100)
paper = tail_jsonl("paper_trades.jsonl", 100)

obs_ids = {x.get("source_setup_id") for x in obs if x.get("source_setup_id")}
qualified_ids = {x.get("source_setup_id") for x in qualified if x.get("source_setup_id")}
paper_ids = {x.get("source_setup_id") for x in paper if x.get("source_setup_id")}

setup_ids = [x.get("setup_id") for x in setups if x.get("setup_id")]

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "counts": {
        "recent_setups": len(setup_ids),
        "seen_by_observer": len([x for x in setup_ids if x in obs_ids]),
        "qualified": len([x for x in setup_ids if x in qualified_ids]),
        "paper_recorded": len([x for x in setup_ids if x in paper_ids]),
    },
    "observer_missed_recent_setup_ids": [x for x in setup_ids if x not in obs_ids],
    "qualified_not_paper_ids": [x for x in qualified_ids if x not in paper_ids],
    "cursor_files": subprocess.getoutput("ls -lh data/cursors 2>/dev/null || true"),
    "observer_status": subprocess.getoutput("systemctl status nurtac-observer --no-pager | head -25"),
    "paper_status": subprocess.getoutput("systemctl status nurtac-paper --no-pager | head -25"),
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
print("REPORT:", OUT)
