#!/usr/bin/env python3
"""
Phase 3 — Event Sequence Win Rate Analysis
Groups events within 5-second windows.
Measures WR for combinations.
"""
import json, subprocess
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "edge_sequence_analysis.json"

WINDOW_MS = 5000
MIN_N = 10

# RAM-safe: tail only
raw = subprocess.getoutput(f"tail -10000 {DATA / 'historical_outcome_observations.jsonl'}")
rows = []
input_rows = 0
unmeasured_rows_skipped = 0
for line in raw.splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        r = json.loads(line)
        input_rows += 1
        reference = r.get("reference") or {}
        ts = reference.get("price_ts") or r.get("event_window_start_ts") or r.get("ts") or 0
        event_type = r.get("event_type") or "unknown"
        side = r.get("side") or "neutral"
        sig = f"{event_type}_{side}"
        outcome_60 = (r.get("outcomes") or {}).get("60s") or {}
        directional_result = outcome_60.get("directional_result", "unknown")
        result_60 = ("correct" if directional_result == "favorable" else
                     "incorrect" if directional_result in ("unfavorable", "flat") else
                     "unknown")
        if result_60 == "unknown":
            unmeasured_rows_skipped += 1
            continue
        rows.append({"ts": ts, "sig": sig, "result": result_60})
    except Exception:
        continue

rows.sort(key=lambda x: x["ts"])

# Group into windows
groups = []
i = 0
while i < len(rows):
    group = [rows[i]]
    j = i + 1
    while j < len(rows) and rows[j]["ts"] - rows[i]["ts"] <= WINDOW_MS:
        group.append(rows[j])
        j += 1
    groups.append(group)
    i = j

# Combos
combo_stats = defaultdict(lambda: Counter())
single_stats = defaultdict(lambda: Counter())

for group in groups:
    sigs = tuple(sorted(set(r["sig"] for r in group)))
    outcomes = [r["result"] for r in group]
    majority = Counter(outcomes).most_common(1)[0][0] if outcomes else "unknown"
    
    # Single signals
    for s in sigs:
        single_stats[s][majority] += 1
    
    # Combinations (2+)
    if len(sigs) >= 2:
        combo_stats[sigs][majority] += 1

def make_stats(stats_dict):
    results = []
    for sigs, counts in stats_dict.items():
        total = sum(counts.values())
        if total < MIN_N:
            continue
        correct = counts.get("correct", 0)
        wr = round(correct / total * 100, 1) if total > 0 else 0
        results.append({
            "signals": list(sigs) if isinstance(sigs, tuple) else [sigs],
            "n": total,
            "correct": correct,
            "wr_60s": wr
        })
    return sorted(results, key=lambda x: x["wr_60s"], reverse=True)

combo_results = make_stats(combo_stats)
single_results = make_stats(single_stats)

output = {
    "input_rows": input_rows,
    "unmeasured_rows_skipped": unmeasured_rows_skipped,
    "total_rows": len(rows),
    "total_groups": len(groups),
    "single_signal_stats": single_results[:20],
    "combo_stats_top20": combo_results[:20],
    "combo_stats_bottom10": sorted(combo_results, key=lambda x: x["wr_60s"])[:10],
    "best_combo": combo_results[0] if combo_results else None,
    "min_n_threshold": MIN_N
}

OUT.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Done. {len(combo_results)} combos found with n>={MIN_N}")
print("\nTOP 5 COMBOS:")
for r in combo_results[:5]:
    print(f"  {r['signals']} → WR={r['wr_60s']}% (n={r['n']})")
