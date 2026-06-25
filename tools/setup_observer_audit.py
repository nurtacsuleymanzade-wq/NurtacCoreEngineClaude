#!/usr/bin/env python3
import json, time, subprocess
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "setup_observer_audit_report.json"

def read_last_jsonl(path, n=20):
    p = DATA / path
    if not p.exists():
        return []
    lines = subprocess.getoutput(f"tail -{n} {p}").splitlines()
    rows = []
    for line in lines:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

setups = read_last_jsonl("setups.jsonl", 20)
qualified = read_last_jsonl("qualified_setups.jsonl", 20)
observations = read_last_jsonl("observations.jsonl", 20)

setup_ids = [x.get("setup_id") for x in setups if x.get("setup_id")]
qualified_sources = [x.get("source_setup_id") for x in qualified if x.get("source_setup_id")]

unqualified_recent = [sid for sid in setup_ids if sid not in qualified_sources]

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "last_setup_count_checked": len(setups),
    "last_qualified_count_checked": len(qualified),
    "last_observation_count_checked": len(observations),
    "recent_setup_ids": setup_ids,
    "recent_qualified_source_ids": qualified_sources,
    "recent_setups_not_qualified": unqualified_recent,
    "last_setups": setups[-5:],
    "last_qualified": qualified[-5:],
    "last_observations": observations[-5:],
    "observer_journal": subprocess.getoutput(
        "journalctl -u nurtac-supervisor --since '15 minutes ago' --no-pager | "
        "grep -Ei 'OBS|QUALIFIED|INVALIDATED|EXPIRED|observer|ERROR|TRACEBACK|exception' | tail -120"
    )
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
print("REPORT:", OUT)
