"""
NurtacCoreEngineClaude — Layer-14: Edge Matrix Engine

Reads:  data/historical_outcome_observations.jsonl
Writes: data/edge_matrix.jsonl  (single-line JSON snapshot)

For every (event_type, side) combination, computes the 60s-horizon
win rate over all observed outcomes and classifies it into an
"edge" bucket. Combinations with too few observations (n < MIN_N)
are dropped — not enough samples to trust the win rate.

This file is consumed by evidence_engine.py to apply small score
adjustments to detector signals based on their own historical
track record (self-learning feedback loop).

No Binance API/WebSocket calls. No mock data. Only reads existing
JSONL files. Never crashes, never writes invalid records.

Usage:
  python edge_matrix_engine.py --mode batch
  python edge_matrix_engine.py --mode live
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ───────────────────────────────────────────────────────────────────────
DATA_DIR     = Path("data")
HALT_FILE    = DATA_DIR / "SYSTEM_HALT"
SOURCE_FILE  = DATA_DIR / "historical_outcome_observations.jsonl"
OUTPUT_FILE  = DATA_DIR / "edge_matrix.jsonl"

HORIZON         = "60s"   # outcome horizon used for win-rate calculation
MIN_N           = 30      # minimum observation count to trust a win rate
RECOMPUTE_SLEEP = 60.0    # seconds between recomputes in live mode

EDGE_THRESHOLDS = [
    (0.60, "strong"),
    (0.55, "moderate"),
    (0.50, "neutral"),
]


# ── Helpers ──────────────────────────────────────────────────────────────────────
def _check_system_halt() -> bool:
    if HALT_FILE.exists():
        print("[EDGE] SYSTEM_HALT detected — exiting", flush=True)
        return True
    return False


def _read_all_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return records


def _classify_edge(wr: float) -> str:
    for threshold, label in EDGE_THRESHOLDS:
        if wr >= threshold:
            return label
    return "negative"


def _safe_write_jsonl_single(path: Path, data: dict) -> None:
    """Write a single-line JSON snapshot atomically (tmp + replace)."""
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False))
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)
    except OSError:
        pass


# ── Core computation ─────────────────────────────────────────────────────────────
def compute_edge_matrix(records: list[dict]) -> dict:
    """Group observations by event_type+side, compute 60s win rate per group.

    A win = directional_result == "favorable" at the HORIZON outcome.
    "unfavorable" and "flat" both count as non-wins (denominator only).
    Observations missing the HORIZON outcome (still pending / not yet
    measured) are skipped entirely — they contribute to neither side.
    """
    counts: dict[str, dict[str, int]] = {}

    for rec in records:
        event_type = rec.get("event_type")
        side       = rec.get("side")
        if not event_type or not side:
            continue

        outcomes = rec.get("outcomes") or {}
        horizon_outcome = outcomes.get(HORIZON)
        if not horizon_outcome:
            continue

        dr = horizon_outcome.get("directional_result")
        if dr not in ("favorable", "unfavorable", "flat"):
            continue

        key = f"{event_type}_{side}"
        bucket = counts.setdefault(key, {"total": 0, "win": 0})
        bucket["total"] += 1
        if dr == "favorable":
            bucket["win"] += 1

    matrix: dict = {}
    for key, bucket in counts.items():
        n = bucket["total"]
        if n < MIN_N:
            continue
        wr = bucket["win"] / n
        matrix[key] = {
            "wr":   round(wr, 4),
            "n":    n,
            "edge": _classify_edge(wr),
        }

    return matrix


def _print_summary(matrix: dict) -> None:
    if not matrix:
        print("[EDGE] No combination reached MIN_N — edge_matrix empty", flush=True)
        return
    print(f"[EDGE] edge_matrix updated — {len(matrix)} combination(s):", flush=True)
    for key, v in sorted(matrix.items(), key=lambda kv: -kv[1]["n"]):
        print(f"[EDGE]   {key:<32} wr={v['wr']:.4f}  n={v['n']:<5}  edge={v['edge']}", flush=True)


# ── Batch mode ───────────────────────────────────────────────────────────────────
def run_batch() -> None:
    if _check_system_halt():
        return
    print("NurtacCoreEngineClaude — Layer-14 Edge Matrix Engine (batch)")
    print(f"Source: {SOURCE_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    records = _read_all_jsonl(SOURCE_FILE)
    if not records:
        print(f"[EDGE] No records in {SOURCE_FILE} — nothing to compute")
        return

    matrix = compute_edge_matrix(records)
    _safe_write_jsonl_single(OUTPUT_FILE, matrix)
    _print_summary(matrix)


# ── Live mode ────────────────────────────────────────────────────────────────────
async def run_live() -> None:
    """Live mode: periodically recompute the edge matrix from the full
    observations file and rewrite the snapshot."""
    print("NurtacCoreEngineClaude — Layer-14 Edge Matrix Engine (live)")
    print(f"Source: {SOURCE_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    while True:
        if HALT_FILE.exists():
            print("[EDGE] SYSTEM_HALT detected — exiting", flush=True)
            return

        records = _read_all_jsonl(SOURCE_FILE)
        if records:
            matrix = compute_edge_matrix(records)
            _safe_write_jsonl_single(OUTPUT_FILE, matrix)
            _print_summary(matrix)
        else:
            print(f"[EDGE] Waiting for {SOURCE_FILE}...", flush=True)

        for _ in range(int(RECOMPUTE_SLEEP)):
            if HALT_FILE.exists():
                print("[EDGE] SYSTEM_HALT detected — exiting", flush=True)
                return
            await asyncio.sleep(1.0)


# run_edge is the name production_supervisor.py looks for first.
async def run_edge() -> None:
    await run_live()


# ── Entry point ──────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer-14 Edge Matrix Engine"
    )
    parser.add_argument(
        "--mode", choices=["batch", "live"], default="batch",
        help="batch: one pass over the observations file; live: recompute periodically",
    )
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch()
    else:
        try:
            asyncio.run(run_live())
        except KeyboardInterrupt:
            print("\n[EDGE] Stopping.")


if __name__ == "__main__":
    main()
