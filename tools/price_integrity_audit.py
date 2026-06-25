#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
from collections import Counter

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

PAPER = DATA / "paper_trades.jsonl"
PAPER_CLOSED = DATA / "paper_closed.jsonl"

FILES = [
    DATA / "combined_1s_dna_btcusdt.jsonl",
    DATA / "one_second_combined_dna.jsonl",
    DATA / "rolling_3s_dna.jsonl",
    DATA / "rolling_5s_dna.jsonl",
    DATA / "rolling_15s_dna.jsonl",
    DATA / "aligned_1m_candle_dna.jsonl",
]

OUT_JSON = DATA / "price_integrity_audit_report.json"
OUT_MD = DATA / "price_integrity_audit_report.md"

def read_jsonl(path, n=50000):
    if not path.exists():
        return []
    rows = []
    out = subprocess.getoutput(f"tail -{int(n)} {path}")
    for line in out.splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def sid(row):
    return row.get("setup_id") or row.get("source_setup_id") or row.get("qualified_setup_id")

price_sources = []

for f in FILES:
    rows = read_jsonl(f, 500)
    if not rows:
        price_sources.append({"file": str(f), "exists_with_rows": False})
        continue

    top_high = any("high" in r for r in rows)
    top_low = any("low" in r for r in rows)
    nested_high = False
    nested_low = False

    for r in rows:
        for k in ["ohlc", "candle", "summary", "price_dna", "trade_dna"]:
            obj = r.get(k)
            if isinstance(obj, dict):
                nested_high = nested_high or ("high" in obj)
                nested_low = nested_low or ("low" in obj)

    price_sources.append({
        "file": str(f),
        "exists_with_rows": True,
        "rows_checked": len(rows),
        "top_level_high": top_high,
        "top_level_low": top_low,
        "nested_high": nested_high,
        "nested_low": nested_low
    })

paper_rows = read_jsonl(PAPER, 100000) + read_jsonl(PAPER_CLOSED, 100000)
closed = [r for r in paper_rows if r.get("record_type") == "paper_trade_closed"]

quality_counter = Counter()
same_ts = Counter()
close_only_sids = []

for r in closed:
    q = ((r.get("hit_candle") or {}).get("price_source_quality")) or "unknown"
    quality_counter[q] += 1

    if q == "close_only":
        close_only_sids.append(sid(r))

    ts = r.get("closed_ts")
    if ts:
        same_ts[str(ts)] += 1

duplicates = [
    {"closed_ts": ts, "count": count}
    for ts, count in same_ts.items()
    if count > 1
]

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "price_sources": price_sources,
    "paper_closed_count": len(closed),
    "price_source_quality_distribution": dict(quality_counter),
    "close_only_count": len(close_only_sids),
    "close_only_sample": close_only_sids[:20],
    "duplicate_closed_timestamp_groups": duplicates,
    "largest_duplicate_group": max([d["count"] for d in duplicates], default=1),
    "decision": "Evidence only. No patch applied."
}

OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False))

md = [
    "# Price Integrity Audit",
    "",
    "## Price sources"
]
for item in price_sources:
    md.append(f"- {item}")

md += [
    "",
    "## Paper Close",
    f"- closed={len(closed)}",
    f"- quality={dict(quality_counter)}",
    f"- close_only={len(close_only_sids)}",
    "",
    "## Duplicate close timestamps"
]
for item in duplicates:
    md.append(f"- {item}")

OUT_MD.write_text("\n".join(md))

print(json.dumps(report, indent=2, ensure_ascii=False))
