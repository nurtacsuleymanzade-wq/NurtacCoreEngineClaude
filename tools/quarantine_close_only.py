#!/usr/bin/env python3
"""
Phase 1A — Quarantine close_only records.
Input:  data/paper_closed.jsonl
Output: data/paper_closed_verified.jsonl   (verified_high_low only)
        data/paper_closed_quarantine.jsonl (close_only, for reference)
        data/quarantine_report.json
"""
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

IN_FILE = DATA / "paper_closed.jsonl"
CLEAN_FILE = DATA / "paper_closed_verified.jsonl"
QUARANTINE_FILE = DATA / "paper_closed_quarantine.jsonl"
REPORT_FILE = DATA / "quarantine_report.json"

# RAM-safe: tail -5000 only
raw = subprocess.getoutput(f"tail -5000 {IN_FILE}")
rows = []
for line in raw.splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        rows.append(json.loads(line))
    except Exception:
        continue

verified, quarantined, skipped = [], [], []

for r in rows:
    if r.get("record_type") != "paper_trade_closed":
        skipped.append(r)
        continue
    quality = (r.get("hit_candle") or {}).get("price_source_quality", "unknown")
    if quality == "verified_high_low":
        verified.append(r)
    elif quality == "close_only":
        quarantined.append(r)
    else:
        skipped.append(r)

CLEAN_FILE.write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in verified) + "\n",
    encoding="utf-8"
)
QUARANTINE_FILE.write_text(
    "\n".join(json.dumps(r, ensure_ascii=False) for r in quarantined) + "\n",
    encoding="utf-8"
)

report = {
    "run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "input_rows_read": len(rows),
    "verified_high_low": len(verified),
    "close_only_quarantined": len(quarantined),
    "other_skipped": len(skipped),
    "clean_file": str(CLEAN_FILE),
    "quarantine_file": str(QUARANTINE_FILE),
    "expected_verified": 54,
    "expected_quarantined": 27,
    "counts_match_expected": (len(verified) == 54 and len(quarantined) == 27)
}
REPORT_FILE.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(report, indent=2, ensure_ascii=False))
