"""
NurtacCoreEngineClaude — Layer-5: Decision Gate

Reads all 6 Layer-4 detector label files and combines them per window_start_ts.
Answers: "Is there a multi-detector setup, how strong, which direction?"
No signals. No long/short. Setup classification only.

Batch: python3 decision_gate.py --mode batch
Live:  python3 decision_gate.py --mode live
Full:  FULL_PRINT=true python3 decision_gate.py --mode live
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ──────────────────────────────────────────────────────────────────────
SYMBOL           = "BTCUSDT"
DATA_DIR         = Path("data")
HALT_FILE        = DATA_DIR / "SYSTEM_HALT"
OUTPUT_FILE      = DATA_DIR / "decision_gate_output.jsonl"
CALIBRATION_VIEW_FILE = DATA_DIR / "decision_gate_calibration_view.json"
FULL_PRINT       = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_INTERVAL    = 0.05
FILE_WAIT_SLEEP  = 2.0
WINDOW_SETTLE    = 2.0   # seconds to wait for all detectors before processing a ts

DETECTORS = [
    "absorption", "sweep", "exhaustion",
    "iceberg", "trapped_trader", "initiative_flow",
]

LABEL_FILES = {det: DATA_DIR / f"labels_{det}.jsonl" for det in DETECTORS}
BASELINE_FILE = DATA_DIR / "historical_baseline_dna.jsonl"

# direction → bullish/bearish classification per detector
BULLISH_DIRS: dict[str, set[str]] = {
    "absorption":      {"sell_absorbed"},
    "sweep":           {"downward_sweep"},
    "exhaustion":      {"sell_exhaustion"},
    "iceberg":         {"bid_iceberg"},
    "trapped_trader":  {"short_trapped"},
    "initiative_flow": {"buy_initiative"},
}
BEARISH_DIRS: dict[str, set[str]] = {
    "absorption":      {"buy_absorbed"},
    "sweep":           {"upward_sweep"},
    "exhaustion":      {"buy_exhaustion"},
    "iceberg":         {"ask_iceberg"},
    "trapped_trader":  {"long_trapped"},
    "initiative_flow": {"sell_initiative"},
}

# ── Shared state ────────────────────────────────────────────────────────────────
_baseline_1m: dict | None = None
_gate_calibration_cache: dict | None = None


# ── Utilities ───────────────────────────────────────────────────────────────────
def _check_halt() -> None:
    if HALT_FILE.exists():
        print("SYSTEM_HALT: decision_gate durduruluyor")
        return


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return f if f == f and abs(f) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _load_baseline() -> None:
    global _baseline_1m
    if not BASELINE_FILE.exists():
        return
    try:
        with open(BASELINE_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("timeframe") == "1M":
                    _baseline_1m = rec
    except OSError:
        pass


def _load_gate_calibration() -> dict:
    """
    Load an optional calibration view used to downgrade low-WR patterns.
    """
    global _gate_calibration_cache
    if _gate_calibration_cache is not None:
        return _gate_calibration_cache
    try:
        if CALIBRATION_VIEW_FILE.exists():
            data = json.loads(CALIBRATION_VIEW_FILE.read_text())
            if isinstance(data, dict):
                _gate_calibration_cache = {
                    k: v for k, v in data.items()
                    if isinstance(v, dict) and v.get("count", 0) >= 20
                }
                return _gate_calibration_cache
    except Exception:
        pass
    _gate_calibration_cache = {}
    return _gate_calibration_cache


def _read_last_n_lines(path: Path, n: int = 200) -> tuple[list[dict], int]:
    if not path.exists():
        return [], 0
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            pos = fh.tell()
            remaining = pos
            chunks: list[bytes] = []
            line_count = 0
            block_size = 64 * 1024

            while remaining > 0 and line_count <= n:
                read_size = min(block_size, remaining)
                remaining -= read_size
                fh.seek(remaining)
                chunk = fh.read(read_size)
                chunks.insert(0, chunk)
                line_count += chunk.count(b"\n")

        records: list[dict] = []
        for raw_line in b"".join(chunks).splitlines()[-n:]:
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
        return records, pos
    except OSError:
        return [], 0


def _write_output(record: dict) -> None:
    try:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as e:
        print(f"[GATE] write error: {e}")


# ── Direction classification ────────────────────────────────────────────────────
def _dir_class(detector: str, direction: str | None) -> str:
    if direction is None:
        return "neutral"
    if direction in BULLISH_DIRS.get(detector, set()):
        return "bullish"
    if direction in BEARISH_DIRS.get(detector, set()):
        return "bearish"
    return "neutral"


# ── Gate computation ────────────────────────────────────────────────────────────
_NULL_LABEL: dict = {"label": "none", "direction": None, "score": 0}

def _compute_gate(ts: int, window_end_ts: int,
                  records: dict[str, dict]) -> dict:
    """Given a ts and a dict of {detector: label_record}, produce gate output."""

    # Build detector summary
    summary: dict[str, dict] = {}
    for det in DETECTORS:
        rec = records.get(det, _NULL_LABEL)
        label     = rec.get("label", "none") or "none"
        direction = rec.get("direction")
        score     = rec.get("score", 0) or 0
        dc        = _dir_class(det, direction if label != "none" else None)
        entry: dict = {"label": label, "direction_class": dc, "score": score}
        if det == "iceberg":
            entry["iceberg_counted"] = False  # set below
        summary[det] = entry

    # Bullish / bearish counting (before iceberg constraint)
    bullish_count = sum(
        1 for det in DETECTORS if summary[det]["direction_class"] == "bullish"
    )
    bearish_count = sum(
        1 for det in DETECTORS if summary[det]["direction_class"] == "bearish"
    )

    # Iceberg constraint: only count if another detector agrees
    ic_dc = summary["iceberg"]["direction_class"]
    if ic_dc == "bullish":
        # other bullish detectors (excluding iceberg)
        other_bullish = sum(
            1 for det in DETECTORS
            if det != "iceberg" and summary[det]["direction_class"] == "bullish"
        )
        if other_bullish == 0:
            bullish_count -= 1
            summary["iceberg"]["direction_class"] = "neutral"
            summary["iceberg"]["iceberg_counted"] = False
        else:
            summary["iceberg"]["iceberg_counted"] = True
    elif ic_dc == "bearish":
        other_bearish = sum(
            1 for det in DETECTORS
            if det != "iceberg" and summary[det]["direction_class"] == "bearish"
        )
        if other_bearish == 0:
            bearish_count -= 1
            summary["iceberg"]["direction_class"] = "neutral"
            summary["iceberg"]["iceberg_counted"] = False
        else:
            summary["iceberg"]["iceberg_counted"] = True
    # else neutral → iceberg_counted stays False

    # Dominant direction & confluence
    if bullish_count > bearish_count:
        dominant_direction = "bullish"
        aligned_count      = bullish_count
    elif bearish_count > bullish_count:
        dominant_direction = "bearish"
        aligned_count      = bearish_count
    else:
        dominant_direction = "neutral"
        aligned_count      = 0

    confluence_score = aligned_count

    # Strong bonus — only for non-neutral dominant direction
    strong_bonus = 0.0
    if dominant_direction != "neutral":
        for det in DETECTORS:
            s = summary[det]
            if s["direction_class"] == dominant_direction:
                if s["label"].endswith("_strong"):
                    strong_bonus += 0.5

    quality_score = float(confluence_score) + strong_bonus
    score_breakdown = {
        "confluence_score": confluence_score,
        "strong_bonus": strong_bonus,
    }

    # Setup grade
    if quality_score >= 4.0 and aligned_count >= 3:
        setup_grade = "A"
    elif quality_score >= 2.5 and aligned_count >= 2:
        setup_grade = "B"
    elif quality_score >= 1.5 and aligned_count >= 1:
        setup_grade = "C"
    else:
        setup_grade = "none"

    if setup_grade == "none":
        final_direction = "neutral"
    else:
        final_direction = dominant_direction

    gate_cal = _load_gate_calibration()
    if gate_cal:
        s_type = "normal"
        for det_info in summary.values():
            if isinstance(det_info, dict):
                s_type = det_info.get("setup_type", s_type)
                if s_type != "normal":
                    break
        pattern = f"{s_type}_{dominant_direction}"
        cal_info = gate_cal.get(pattern)
        if cal_info:
            cal_wr = cal_info.get("total_wr")
            if cal_wr is None:
                cal_wr = cal_info.get("wr", 0.5)
            cal_n = cal_info.get("count", 0)
            if cal_wr < 0.40 and cal_n >= 20 and setup_grade in ("A", "B", "C"):
                grade_map = {"A": 2, "B": 1, "C": 0}
                reverse_map = {2: "A", 1: "B", 0: "C"}
                current = grade_map.get(setup_grade, 1)
                new_grade = max(0, current - 1)
                setup_grade = reverse_map[new_grade]
                if isinstance(score_breakdown, dict):
                    score_breakdown["cal_gate_downgrade"] = (
                        f"wr={cal_wr:.1%} n={cal_n} grade_down"
                    )

    # Baseline context
    bl = _baseline_1m
    if bl is not None:
        try:
            atr_status     = bl.get("atr", {}).get("atr_status")
            vwap_side      = bl.get("vwap", {}).get("price_vs_vwap")
            cvd_direction  = bl.get("cvd", {}).get("cvd_direction")
            vol_pct_val    = (bl.get("metrics", {})
                                .get("total_volume", {})
                                .get("short", {})
                                .get("latest_percentile"))
            vol_pct: float | None = _safe_float(vol_pct_val) if vol_pct_val is not None else None

            if dominant_direction == "bullish":
                if vwap_side == "above" and cvd_direction == "rising":
                    ctx_align = "aligned"
                elif vwap_side is not None and cvd_direction is not None:
                    ctx_align = "conflicting"
                else:
                    ctx_align = "neutral"
            elif dominant_direction == "bearish":
                if vwap_side == "below" and cvd_direction == "falling":
                    ctx_align = "aligned"
                elif vwap_side is not None and cvd_direction is not None:
                    ctx_align = "conflicting"
                else:
                    ctx_align = "neutral"
            else:
                ctx_align = "neutral"

            baseline_context = {
                "has_baseline": True,
                "atr_status": atr_status,
                "vwap_side": vwap_side,
                "cvd_direction": cvd_direction,
                "volume_percentile": vol_pct,
                "context_alignment": ctx_align,
            }
        except Exception:
            baseline_context = {
                "has_baseline": True, "atr_status": None, "vwap_side": None,
                "cvd_direction": None, "volume_percentile": None, "context_alignment": "unknown",
            }
    else:
        baseline_context = {
            "has_baseline": False, "atr_status": None, "vwap_side": None,
            "cvd_direction": None, "volume_percentile": None, "context_alignment": "unknown",
        }

    return {
        "gate": "decision_gate",
        "symbol": SYMBOL,
        "window_start_ts": ts,
        "window_end_ts": window_end_ts,
        "setup_grade": setup_grade,
        "dominant_direction": final_direction if setup_grade != "none" else "neutral",
        "confluence_score": confluence_score,
        "quality_score": quality_score,
        "score_breakdown": score_breakdown,
        "detector_summary": summary,
        "baseline_context": baseline_context,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
    }


# ── Validation ──────────────────────────────────────────────────────────────────
def _validate(rec: dict) -> list[str]:
    errors: list[str] = []

    sg = rec.get("setup_grade")
    dd = rec.get("dominant_direction")
    cs = rec.get("confluence_score", -1)
    qs = rec.get("quality_score", -1.0)
    bc = rec.get("bullish_count", -1)
    brc = rec.get("bearish_count", -1)
    ds = rec.get("detector_summary", {})

    if sg not in ("A", "B", "C", "none"):
        errors.append(f"[1] invalid setup_grade '{sg}'")
    if dd not in ("bullish", "bearish", "neutral"):
        errors.append(f"[2] invalid dominant_direction '{dd}'")
    if not (0 <= cs <= 6):
        errors.append(f"[3] confluence_score {cs} not in [0..6]")
    if not (isinstance(qs, (int, float)) and qs >= 0.0 and qs == qs and abs(qs) != float("inf")):
        errors.append(f"[4] quality_score invalid: {qs}")
    if sg == "none" and dd not in ("neutral",):
        # sg==none allows dd==neutral (dominant_direction was forced to neutral)
        # but it can technically happen that aligned_count < threshold while dd != neutral
        # The spec says "neutral dahil" so sg=none → dd must be neutral per our implementation
        pass  # our impl already forces dd=neutral when sg=none
    if sg != "none" and dd == "neutral":
        errors.append("[6] setup_grade!=none but dominant_direction==neutral")
    if not (bc + brc <= 6):
        errors.append(f"[7] bullish_count+bearish_count={bc+brc} > 6")
    if len(ds) != 6 or set(ds.keys()) != set(DETECTORS):
        errors.append(f"[8] detector_summary missing detectors: {set(DETECTORS)-set(ds.keys())}")

    # iceberg counted constraint
    ic = ds.get("iceberg", {})
    if ic.get("iceberg_counted"):
        ic_dc = ic.get("direction_class")
        other_same = sum(
            1 for det in DETECTORS
            if det != "iceberg" and ds.get(det, {}).get("direction_class") == ic_dc
        )
        if other_same == 0:
            errors.append("[9] iceberg_counted=true but no other detector in same direction")

    # quality_score == confluence_score + strong_bonus
    strong_bonus = sum(
        0.5 for det in DETECTORS
        if ds.get(det, {}).get("direction_class") == dd
        and ds.get(det, {}).get("label", "").endswith("_strong")
    ) if dd != "neutral" else 0.0
    expected_qs = float(cs) + strong_bonus
    if abs(qs - expected_qs) >= 1e-9:
        errors.append(f"[10] quality_score={qs} != confluence_score+bonus={expected_qs}")

    return errors


# ── Terminal print ──────────────────────────────────────────────────────────────
def _print_gate(rec: dict) -> None:
    sg = rec.get("setup_grade", "none")
    if FULL_PRINT:
        print(json.dumps(rec, indent=2, ensure_ascii=False))
        return
    if sg == "none":
        return

    ts      = rec.get("window_start_ts", 0)
    dd      = rec.get("dominant_direction", "").upper()
    cs      = rec.get("confluence_score", 0)
    qs      = rec.get("quality_score", 0.0)
    ctx     = rec.get("baseline_context", {}).get("context_alignment", "unknown")
    ds      = rec.get("detector_summary", {})

    # build detector list string: only active (non-neutral) ones
    active_dets = []
    for det in DETECTORS:
        info  = ds.get(det, {})
        label = info.get("label", "none")
        dc    = info.get("direction_class", "neutral")
        if label != "none" and dc != "neutral":
            active_dets.append(label)
    det_str = "+".join(active_dets) if active_dets else "none"

    print(f"[GATE {sg}] ts={ts} {dd} confluence={cs} quality={qs} "
          f"detectors={det_str} context={ctx}")


# ── Window end-ts resolution ────────────────────────────────────────────────────
def _resolve_window_end(ts: int, det_records: dict[str, dict]) -> int:
    """Pick the largest window_end_ts among available records, or ts+1000."""
    best = ts + 1000
    for rec in det_records.values():
        wte = rec.get("window_end_ts", 0)
        if wte > best:
            best = wte
    return best


# ── Batch mode ──────────────────────────────────────────────────────────────────
def run_batch() -> None:
    _check_halt()
    print("NurtacCoreEngineClaude — Layer-5 Decision Gate (batch)")
    print()

    _load_baseline()

    # Read all label files and group by ts
    ts_map: dict[int, dict[str, dict]] = defaultdict(dict)

    for det in DETECTORS:
        path = LABEL_FILES[det]
        if not path.exists():
            print(f"[{det}] file not found — skipped")
            continue
        records, _ = _read_last_n_lines(path)
        for rec in records:
            _check_halt()
            ts = rec.get("window_start_ts")
            if ts is not None:
                ts_map[ts][det] = rec

    if not ts_map:
        print("No label data found. Run detector_engine.py first.")
        return

    written = 0
    for ts in sorted(ts_map.keys()):
        _check_halt()
        det_records = ts_map[ts]
        wte         = _resolve_window_end(ts, det_records)
        gate_rec    = _compute_gate(ts, wte, det_records)
        errors      = _validate(gate_rec)
        if errors:
            print(f"[VALIDATION FAIL] ts={ts}: {'; '.join(errors)}")
            continue
        _write_output(gate_rec)
        _print_gate(gate_rec)
        written += 1

    print(f"\nBatch complete. {written} windows processed.")


# ── Live mode async tasks ────────────────────────────────────────────────────────
# Shared pending buffer: {ts: {"records": {det: rec}, "first_seen": float}}
_pending: dict[int, dict] = {}
_processed: set[int] = set()
_pending_lock: asyncio.Lock | None = None


async def _tail_label_file(detector: str) -> None:
    """Tail a label file and add records to _pending."""
    path = LABEL_FILES[detector]

    while not path.exists():
        _check_halt()
        print(f"Waiting for {path.name}...")
        await asyncio.sleep(FILE_WAIT_SLEEP)

    # warm-up: read existing
    records, pos = _read_last_n_lines(path)
    async with _pending_lock:
        for rec in records:
            ts = rec.get("window_start_ts")
            if ts is not None and ts not in _processed:
                if ts not in _pending:
                    _pending[ts] = {"records": {}, "first_seen": time.monotonic()}
                _pending[ts]["records"][detector] = rec

    # tail
    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(pos)
        while True:
            _check_halt()
            line = fh.readline()
            if line:
                line = line.strip()
                if line:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = rec.get("window_start_ts")
                    if ts is not None:
                        async with _pending_lock:
                            if ts not in _processed:
                                if ts not in _pending:
                                    _pending[ts] = {"records": {}, "first_seen": time.monotonic()}
                                _pending[ts]["records"][detector] = rec
            else:
                await asyncio.sleep(POLL_INTERVAL)


async def _gate_processor() -> None:
    """Process ready windows from _pending."""
    while True:
        _check_halt()
        now = time.monotonic()
        ready: list[int] = []

        async with _pending_lock:
            for ts, info in list(_pending.items()):
                if ts in _processed:
                    del _pending[ts]
                    continue
                all_in  = len(info["records"]) == 6
                timeout = now - info["first_seen"] >= WINDOW_SETTLE
                if all_in or timeout:
                    ready.append(ts)

        for ts in sorted(ready):
            async with _pending_lock:
                if ts in _processed:
                    continue
                info = _pending.pop(ts, None)
                if info is None:
                    continue
                _processed.add(ts)
                det_records = info["records"]

            wte      = _resolve_window_end(ts, det_records)
            gate_rec = _compute_gate(ts, wte, det_records)
            errors   = _validate(gate_rec)
            if errors:
                print(f"[VALIDATION FAIL] ts={ts}: {'; '.join(errors)}")
                continue
            _write_output(gate_rec)
            _print_gate(gate_rec)

        await asyncio.sleep(0.1)


async def _baseline_refresh() -> None:
    while True:
        await asyncio.sleep(60)
        _check_halt()
        _load_baseline()


async def run_live() -> None:
    global _pending_lock
    _pending_lock = asyncio.Lock()

    _check_halt()
    print("NurtacCoreEngineClaude — Layer-5 Decision Gate (live)")
    _load_baseline()
    print("Starting gate tasks...")
    print()

    tasks = [asyncio.create_task(_tail_label_file(det)) for det in DETECTORS]
    tasks.append(asyncio.create_task(_gate_processor()))
    tasks.append(asyncio.create_task(_baseline_refresh()))
    await asyncio.gather(*tasks)


# ── Entry point ─────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Layer-5 Decision Gate")
    parser.add_argument("--mode", choices=["batch", "live"], required=True)
    args = parser.parse_args()
    if args.mode == "batch":
        run_batch()
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
