#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Probability Surface Builder
READ:  data/hypothesis_outcomes.jsonl (tail -2000)
WRITE: data/probability_surface.json
Günlük systemd timer ile çalışır. Trade açmaz.
"""

import json
import math
import subprocess
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA_DIR = ROOT / "data"
INPUT_FILE = DATA_DIR / "hypothesis_outcomes.jsonl"
OUTPUT_FILE = DATA_DIR / "probability_surface.json"
SCAN_LIMIT = 2000
MIN_N = 30


def _wilson_lower(wins: int, n: int) -> float:
    if n == 0:
        return 0.0
    z = 1.645
    p = wins / n
    d = 1 + z**2 / n
    c = (p + z**2 / (2 * n)) / d
    m = (z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))) / d
    return max(0.0, c - m)


def build() -> None:
    if not INPUT_FILE.exists():
        print("[PROB] hypothesis_outcomes.jsonl bulunamadı")
        return

    raw = subprocess.getoutput(f"tail -{SCAN_LIMIT} {INPUT_FILE} 2>/dev/null")
    rows = []
    for line in raw.splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

    if len(rows) < 50:
        print(f"[PROB] Yetersiz veri: {len(rows)} satır (min 50)")
        return

    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        det = r.get("detector", "")
        h = r.get("horizon_label", "")
        if det and h:
            groups[det][h].append(r)

    detectors = {}
    for det, horizons in groups.items():
        detectors[det] = {}
        for h, recs in horizons.items():
            n = len(recs)
            wins = sum(1 for r in recs if r.get("direction_correct"))
            wr = wins / n if n else 0
            wl = _wilson_lower(wins, n)
            mfe = sum(r.get("max_favorable_excursion_pct", 0) for r in recs) / n if n else 0
            mae = sum(r.get("max_adverse_excursion_pct", 0) for r in recs) / n if n else 0
            tp_r = sum(1 for r in recs if r.get("tp_proxy_hit")) / n if n else 0

            detectors[det][h] = {
                "n": n,
                "wr": round(wr, 4),
                "wilson_lower": round(wl, 4),
                "mfe_avg": round(mfe, 4),
                "mae_avg": round(mae, 4),
                "mfe_mae_ratio": round(mfe / mae, 3) if mae > 0 else 99.0,
                "tp_proxy_rate": round(tp_r, 4),
                "reliable": n >= MIN_N,
                "grade": (
                    "A"
                    if wl >= 0.60 and n >= MIN_N
                    else "B"
                    if wl >= 0.50 and n >= MIN_N
                    else "C"
                    if n >= MIN_N
                    else "INSUFFICIENT"
                ),
            }

    best = sorted(
        [
            {"detector": d, "horizon": h, **{k: v for k, v in s.items() if k != "reliable"}}
            for d, hs in detectors.items()
            for h, s in hs.items()
            if s["reliable"] and s["wr"] > 0.50
        ],
        key=lambda x: x["wilson_lower"],
        reverse=True,
    )

    scalp_ok = list(
        {
            d
            for d, hs in detectors.items()
            for h, s in hs.items()
            if h in ("30s", "1m") and s["wr"] > 0.55 and s["reliable"]
        }
    )
    swing_no = list(
        {
            d
            for d, hs in detectors.items()
            if all(s["wr"] < 0.50 for s in hs.values() if s["reliable"])
        }
    )

    result = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_observations": len(rows),
        "detectors": detectors,
        "best_combinations": best[:10],
        "scalp_recommended": scalp_ok,
        "swing_not_recommended": swing_no,
    }

    OUTPUT_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"[PROB] Yazıldı: {len(rows)} obs | best: {[b['detector'] + '_' + b['horizon'] for b in best[:3]]}")


if __name__ == "__main__":
    build()
