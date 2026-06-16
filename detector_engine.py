"""
NurtacCoreEngineClaude — Layer-4: Detector Engine

6 parallel detectors read Layer-0/1/2/3 JSONL outputs and produce labels.
No Binance API/WebSocket. No mock data. No signals. Labels only.

Batch: python3 detector_engine.py --mode batch
Live:  python3 detector_engine.py --mode live
Full:  FULL_PRINT=true python3 detector_engine.py --mode live
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import deque, defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ──────────────────────────────────────────────────────────────────────
SYMBOL           = "BTCUSDT"
DATA_DIR         = Path("data")
HALT_FILE        = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT       = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_INTERVAL    = 0.05
FILE_WAIT_SLEEP  = 2.0
BASELINE_REFRESH = 60

INPUT_1S   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
INPUT_3S   = DATA_DIR / "rolling_3s_dna.jsonl"
INPUT_5S   = DATA_DIR / "rolling_5s_dna.jsonl"
INPUT_15S  = DATA_DIR / "rolling_15s_dna.jsonl"
INPUT_BASE = DATA_DIR / "historical_baseline_dna.jsonl"

OUTPUT_FILES = {
    "absorption":      DATA_DIR / "labels_absorption.jsonl",
    "sweep":           DATA_DIR / "labels_sweep.jsonl",
    "exhaustion":      DATA_DIR / "labels_exhaustion.jsonl",
    "iceberg":         DATA_DIR / "labels_iceberg.jsonl",
    "trapped_trader":  DATA_DIR / "labels_trapped_trader.jsonl",
    "initiative_flow": DATA_DIR / "labels_initiative_flow.jsonl",
}

MAX_SCORE = {
    "absorption": 5, "sweep": 4, "exhaustion": 5,
    "iceberg": 5, "trapped_trader": 5, "initiative_flow": 6,
}

VALID_LABELS = {
    "absorption":      {"none", "absorption_candidate", "absorption_strong"},
    "sweep":           {"none", "sweep_candidate", "sweep_strong"},
    "exhaustion":      {"none", "exhaustion_candidate", "exhaustion_strong"},
    "iceberg":         {"none", "iceberg_candidate", "iceberg_strong"},
    "trapped_trader":  {"none", "trapped_candidate", "trapped_strong"},
    "initiative_flow": {"none", "initiative_candidate", "initiative_strong"},
}

PRINT_SHORT = {
    "absorption": "absorption", "sweep": "sweep", "exhaustion": "exhaustion",
    "iceberg": "iceberg", "trapped_trader": "trapped", "initiative_flow": "initiative",
}

# ── Shared state ────────────────────────────────────────────────────────────────
_baseline_cache: dict[str, dict] = {}
_baseline_last_load: float = 0.0
_buf_5s: deque[dict]  = deque(maxlen=20)
_buf_15s: deque[dict] = deque(maxlen=10)


# ── Core utilities ──────────────────────────────────────────────────────────────
def _check_halt() -> None:
    if HALT_FILE.exists():
        print("SYSTEM_HALT: detector_engine durduruluyor")
        return


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        if f != f or f == float("inf") or f == float("-inf"):
            return default
        return f
    except (TypeError, ValueError):
        return default


def _load_baselines() -> None:
    global _baseline_last_load
    if not INPUT_BASE.exists():
        return
    tmp: dict[str, dict] = {}
    try:
        with open(INPUT_BASE, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                tf = rec.get("timeframe", "")
                if tf:
                    tmp[tf] = rec
    except OSError:
        return
    _baseline_cache.update(tmp)
    _baseline_last_load = time.monotonic()


def get_baseline(timeframe: str) -> dict | None:
    return _baseline_cache.get(timeframe)


def _metric(bl: dict | None, metric: str, field: str = "latest_percentile",
            window: str = "short") -> float:
    if bl is None:
        return 0.0
    try:
        return _safe_float(bl["metrics"][metric][window][field])
    except (KeyError, TypeError):
        return 0.0


def _atr(bl: dict | None, default: float = 1.0) -> float:
    if bl is None:
        return default
    try:
        v = _safe_float(bl["atr"]["atr"], default)
        return v if v > 0 else default
    except (KeyError, TypeError):
        return default


def get_matching_5s_window(ts: int) -> dict | None:
    best, best_dist = None, float("inf")
    for entry in _buf_5s:
        wstart = entry.get("window_start_ts", 0)
        wend   = entry.get("window_end_ts", 0)
        if wstart <= ts < wend:
            return entry
        dist = min(abs(wstart - ts), abs(wend - ts))
        if dist < best_dist:
            best_dist, best = dist, entry
    return best


def get_matching_15s_window(ts: int) -> dict | None:
    best, best_dist = None, float("inf")
    for entry in _buf_15s:
        wstart = entry.get("window_start_ts", 0)
        wend   = entry.get("window_end_ts", 0)
        if wstart <= ts < wend:
            return entry
        dist = min(abs(wstart - ts), abs(wend - ts))
        if dist < best_dist:
            best_dist, best = dist, entry
    return best


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


def _write_label(detector: str, record: dict) -> None:
    path = OUTPUT_FILES[detector]
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as e:
        print(f"[{detector.upper()}] write error: {e}")


def _validate_output(rec: dict, detector: str) -> list[str]:
    errors: list[str] = []
    ts    = rec.get("window_start_ts", 0)
    label = rec.get("label", "")
    score = rec.get("score", -1)
    sc    = rec.get("score_components", {})

    if rec.get("detector") not in VALID_LABELS:
        errors.append("[1] invalid detector name")
    if rec.get("mode") != "label":
        errors.append("[2] mode != 'label'")
    if not (rec.get("window_start_ts", 0) < rec.get("window_end_ts", 0)):
        errors.append(f"[3] ts ordering violation ts={ts}")
    if label not in VALID_LABELS.get(detector, set()):
        errors.append(f"[4] invalid label '{label}'")
    if label == "none" and rec.get("direction") is not None:
        errors.append("[5] label=none but direction!=null")
    if label != "none" and rec.get("direction") is None:
        errors.append("[6] label!=none but direction=null")
    if score < 0:
        errors.append("[7] score < 0")
    for k, v in sc.items():
        if not isinstance(v, bool):
            errors.append(f"[8] score_components.{k} not bool")
    if sum(1 for v in sc.values() if v) != score:
        errors.append(f"[9] sum(score_components)={sum(1 for v in sc.values() if v)} != score={score}")
    for k, v in rec.get("measurements", {}).items():
        if isinstance(v, float) and (v != v or abs(v) == float("inf")):
            errors.append(f"[10] measurements.{k} is NaN/inf")

    if detector == "absorption":
        if not (0 <= score <= 5):
            errors.append(f"[abs] score {score} not in [0..5]")
        ratio = rec.get("measurements", {}).get("range_atr_ratio", 0.0)
        if isinstance(ratio, float) and ratio < 0:
            errors.append("[abs] range_atr_ratio < 0")
    elif detector == "sweep":
        if not (0 <= score <= 4):
            errors.append(f"[sw] score {score} not in [0..4]")
        m  = rec.get("measurements", {})
        uw = m.get("upper_wick", 0.0)
        lw = m.get("lower_wick", 0.0)
        cr = m.get("candle_range", 0.0)
        if isinstance(uw, float) and isinstance(lw, float) and isinstance(cr, float):
            if uw + lw > cr + 1e-9:
                errors.append("[sw] upper+lower wick > range")
    elif detector == "exhaustion":
        if not (0 <= score <= 5):
            errors.append(f"[ex] score {score} not in [0..5]")
    elif detector == "iceberg":
        if not (0 <= score <= 5):
            errors.append(f"[ic] score {score} not in [0..5]")
        if rec.get("measurements", {}).get("recurrence_count", 0) < 0:
            errors.append("[ic] recurrence_count < 0")
    elif detector == "trapped_trader":
        if not (0 <= score <= 5):
            errors.append(f"[tr] score {score} not in [0..5]")
    elif detector == "initiative_flow":
        if not (0 <= score <= 6):
            errors.append(f"[if] score {score} not in [0..6]")
        active = rec.get("measurements", {}).get("active_seconds", 0)
        if isinstance(active, (int, float)) and not (0 <= active <= 5):
            errors.append(f"[if] active_seconds {active} not in [0..5]")

    return errors


def _process_and_emit(detector: str, rec: dict, batch_mode: bool,
                      last_holder: list | None = None) -> None:
    errors = _validate_output(rec, detector)
    if errors:
        print(f"[VALIDATION FAIL] {detector} ts={rec.get('window_start_ts')}: {'; '.join(errors)}")
        return
    if batch_mode:
        if last_holder is not None:
            last_holder.clear()
            last_holder.append(rec)
    else:
        _write_label(detector, rec)
        _print_label(rec)


def _print_label(rec: dict) -> None:
    label = rec.get("label", "none")
    if FULL_PRINT:
        print(json.dumps(rec, indent=2, ensure_ascii=False))
        return
    if label == "none":
        return
    det    = rec.get("detector", "")
    ts     = rec.get("window_start_ts", 0)
    score  = rec.get("score", 0)
    maxs   = MAX_SCORE.get(det, 0)
    direct = rec.get("direction", "") or ""
    short  = PRINT_SHORT.get(det, det)
    print(f"[LABEL {short}] ts={ts} {label.upper()} {direct} score={score}/{maxs}")


# ── Absorption Detector ─────────────────────────────────────────────────────────
def _detect_absorption(row: dict) -> dict:
    bl     = get_baseline("1S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    cdna   = row.get("candle_dna") or {}
    fdna   = row.get("footprint_dna") or {}
    ddna   = row.get("depth_dna") or {}
    atr_v  = _atr(bl)

    _null_sc = {
        "IS_HIGH_VOLUME": False, "SELL_OR_BUY_PRESSURE": False,
        "SMALL_RANGE": False, "COUNTER_SIDE_DOMINANT": False, "DEPTH_CONFIRMS": False,
    }
    _null_m = {"volume_percentile": 0.0, "candle_range": 0.0, "atr": atr_v, "range_atr_ratio": 0.0}

    def null_rec():
        return {
            "detector": "absorption", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": dict(_null_sc), "measurements": dict(_null_m),
            "data_quality": {"has_trade": False, "has_baseline": has_bl, "completeness": None},
        }

    if not cdna.get("has_trade"):
        return null_rec()

    delta_v   = cdna.get("delta")
    buy_vol   = cdna.get("buy_volume")
    sell_vol  = cdna.get("sell_volume")
    total_vol = cdna.get("total_volume")
    high_p    = (cdna.get("high") or {}).get("price")
    low_p     = (cdna.get("low") or {}).get("price")
    close_p   = (cdna.get("close") or {}).get("price")
    levels    = fdna.get("price_levels") or []

    if any(v is None for v in [delta_v, buy_vol, sell_vol, total_vol, high_p, low_p, close_p]):
        return null_rec()
    if len(levels) < 1:
        return null_rec()

    total_vol = _safe_float(total_vol)
    if total_vol == 0:
        return null_rec()

    buy_vol  = _safe_float(buy_vol)
    sell_vol = _safe_float(sell_vol)
    high_p   = _safe_float(high_p)
    low_p    = _safe_float(low_p)

    vol_pct        = _metric(bl, "total_volume", "latest_percentile")
    IS_HIGH_VOLUME = vol_pct >= 70.0
    SELL_PRESSURE  = sell_vol > total_vol * 0.65
    BUY_PRESSURE   = buy_vol  > total_vol * 0.65
    PRESSURE       = SELL_PRESSURE or BUY_PRESSURE

    candle_range = high_p - low_p
    SMALL_RANGE  = candle_range < atr_v * 0.30

    best_lvl, best_tv = None, -1.0
    for lvl in levels:
        tv = _safe_float(lvl.get("total_volume"))
        if tv > best_tv:
            best_tv, best_lvl = tv, lvl

    if best_lvl is not None:
        top_buy  = _safe_float(best_lvl.get("buy_volume"))
        top_sell = _safe_float(best_lvl.get("sell_volume"))
        COUNTER_SIDE_DOMINANT = (
            (SELL_PRESSURE and top_buy > top_sell) or
            (BUY_PRESSURE  and top_sell > top_buy)
        )
    else:
        COUNTER_SIDE_DOMINANT = False

    has_depth    = bool(ddna.get("has_depth"))
    dom_side     = (ddna.get("dominant_side") or "").upper()
    DEPTH_CONFIRMS = has_depth and (
        (SELL_PRESSURE and dom_side == "BID") or
        (BUY_PRESSURE  and dom_side == "ASK")
    )

    sc = {
        "IS_HIGH_VOLUME": IS_HIGH_VOLUME, "SELL_OR_BUY_PRESSURE": PRESSURE,
        "SMALL_RANGE": SMALL_RANGE, "COUNTER_SIDE_DOMINANT": COUNTER_SIDE_DOMINANT,
        "DEPTH_CONFIRMS": DEPTH_CONFIRMS,
    }
    score = sum(1 for v in sc.values() if v)

    if score >= 4:
        label = "absorption_strong"
    elif score == 3:
        label = "absorption_candidate"
    else:
        label = "none"

    direction = None
    if label != "none":
        direction = "sell_absorbed" if SELL_PRESSURE else "buy_absorbed"

    range_atr = (candle_range / atr_v) if atr_v > 0 else 0.0

    return {
        "detector": "absorption", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {
            "volume_percentile": vol_pct, "candle_range": candle_range,
            "atr": atr_v, "range_atr_ratio": range_atr,
        },
        "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
    }


# ── Sweep Detector ──────────────────────────────────────────────────────────────
def _detect_sweep(row: dict) -> dict:
    bl     = get_baseline("1S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    cdna   = row.get("candle_dna") or {}
    atr_v  = _atr(bl)

    _null_sc = {"HAS_SWEEP": False, "PRESSURE": False, "SMALL_BODY": False, "HIGH_VOLUME": False}
    _null_m  = {
        "upper_wick": 0.0, "lower_wick": 0.0, "body": 0.0,
        "candle_range": 0.0, "atr": atr_v, "upper_wick_ratio": 0.0, "lower_wick_ratio": 0.0,
    }

    def null_rec():
        return {
            "detector": "sweep", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": dict(_null_sc), "measurements": dict(_null_m),
            "data_quality": {
                "has_trade": bool(cdna.get("has_trade")), "has_baseline": has_bl, "completeness": None,
            },
        }

    if not cdna.get("has_trade"):
        return null_rec()

    open_p    = (cdna.get("open")  or {}).get("price")
    high_p    = (cdna.get("high")  or {}).get("price")
    low_p     = (cdna.get("low")   or {}).get("price")
    close_p   = (cdna.get("close") or {}).get("price")
    total_vol = _safe_float(cdna.get("total_volume"))

    if any(v is None for v in [open_p, high_p, low_p, close_p]) or total_vol <= 0:
        return null_rec()

    open_p  = _safe_float(open_p)
    high_p  = _safe_float(high_p)
    low_p   = _safe_float(low_p)
    close_p = _safe_float(close_p)
    buy_vol  = _safe_float(cdna.get("buy_volume"))
    sell_vol = _safe_float(cdna.get("sell_volume"))

    body         = abs(close_p - open_p)
    upper_wick   = high_p - max(open_p, close_p)
    lower_wick   = min(open_p, close_p) - low_p
    candle_range = high_p - low_p

    if candle_range == 0:
        return null_rec()

    uwr = upper_wick / candle_range
    lwr = lower_wick / candle_range
    vol_pct = _metric(bl, "total_volume", "latest_percentile")

    HAS_UPPER = uwr >= 0.60 and upper_wick >= atr_v * 0.15
    HAS_LOWER = lwr >= 0.60 and lower_wick >= atr_v * 0.15

    if HAS_UPPER and HAS_LOWER:
        if lower_wick > upper_wick:
            HAS_UPPER = False
        else:
            HAS_LOWER = False

    SMALL_BODY  = body < candle_range * 0.25
    HIGH_VOLUME = vol_pct >= 60.0

    if HAS_UPPER:
        PRESSURE  = buy_vol > total_vol * 0.55
        direction = "upward_sweep"
        sc = {"HAS_SWEEP": True, "PRESSURE": PRESSURE, "SMALL_BODY": SMALL_BODY, "HIGH_VOLUME": HIGH_VOLUME}
    elif HAS_LOWER:
        PRESSURE  = sell_vol > total_vol * 0.55
        direction = "downward_sweep"
        sc = {"HAS_SWEEP": True, "PRESSURE": PRESSURE, "SMALL_BODY": SMALL_BODY, "HIGH_VOLUME": HIGH_VOLUME}
    else:
        sc = {"HAS_SWEEP": False, "PRESSURE": False, "SMALL_BODY": False, "HIGH_VOLUME": False}
        direction = None

    score = sum(1 for v in sc.values() if v)

    if score >= 3:
        label = "sweep_strong"
    elif score == 2:
        label = "sweep_candidate"
    else:
        label = "none"

    if label == "none":
        direction = None

    return {
        "detector": "sweep", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {
            "upper_wick": upper_wick, "lower_wick": lower_wick, "body": body,
            "candle_range": candle_range, "atr": atr_v,
            "upper_wick_ratio": uwr, "lower_wick_ratio": lwr,
        },
        "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
    }


# ── Exhaustion Detector ─────────────────────────────────────────────────────────
def _detect_exhaustion(row: dict) -> dict:
    bl     = get_baseline("3S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    mb     = row.get("micro_behavior") or {}
    vol    = row.get("volume") or {}

    _null_sc = {
        "PRIOR_DIRECTIONAL": False, "DELTA_WEAKENING": False,
        "TRADE_DECLINING": False, "DELTA_Z_CONFIRMS": False, "MULTI_TF_CONFIRMS": False,
    }

    def null_rec():
        return {
            "detector": "exhaustion", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": dict(_null_sc),
            "measurements": {"delta_sequence": [], "trade_count_sequence": [], "delta_z": 0.0},
            "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
        }

    deltas = mb.get("delta_sequence")
    trades = mb.get("trade_count_sequence")
    if not deltas or len(deltas) != 3 or not trades or len(trades) != 3:
        return null_rec()

    deltas = [_safe_float(d) for d in deltas]
    trades = [int(t) if t is not None else 0 for t in trades]

    prior_buy  = deltas[0] > 0 and deltas[1] > 0
    prior_sell = deltas[0] < 0 and deltas[1] < 0
    PRIOR_DIRECTIONAL = prior_buy or prior_sell

    if prior_buy:
        DELTA_WEAKENING    = deltas[2] < deltas[1] * 0.50
        direction_candidate = "buy_exhaustion"
    elif prior_sell:
        DELTA_WEAKENING    = abs(deltas[2]) < abs(deltas[1]) * 0.50
        direction_candidate = "sell_exhaustion"
    else:
        DELTA_WEAKENING    = False
        direction_candidate = None

    TRADE_DECLINING = (trades[1] > 0) and (trades[2] < trades[1] * 0.60)

    delta_z = _metric(bl, "delta", "z_score")
    DELTA_Z_CONFIRMS = abs(delta_z) < 1.0 and abs(deltas[1]) > abs(deltas[2])

    vol_delta   = _safe_float(vol.get("delta"))
    rolling_5s  = get_matching_5s_window(wts)
    if rolling_5s is not None:
        r5d = _safe_float((rolling_5s.get("volume") or {}).get("delta"))
        if prior_buy:
            MULTI_TF_CONFIRMS = r5d < vol_delta
        elif prior_sell:
            MULTI_TF_CONFIRMS = r5d > vol_delta
        else:
            MULTI_TF_CONFIRMS = False
    else:
        MULTI_TF_CONFIRMS = False

    sc = {
        "PRIOR_DIRECTIONAL": PRIOR_DIRECTIONAL, "DELTA_WEAKENING": DELTA_WEAKENING,
        "TRADE_DECLINING": TRADE_DECLINING, "DELTA_Z_CONFIRMS": DELTA_Z_CONFIRMS,
        "MULTI_TF_CONFIRMS": MULTI_TF_CONFIRMS,
    }
    score = sum(1 for v in sc.values() if v)

    if score >= 4:
        label = "exhaustion_strong"
    elif score == 3:
        label = "exhaustion_candidate"
    else:
        label = "none"

    direction = direction_candidate if label != "none" else None

    return {
        "detector": "exhaustion", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {"delta_sequence": deltas, "trade_count_sequence": trades, "delta_z": delta_z},
        "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
    }


# ── Iceberg Detector ────────────────────────────────────────────────────────────
def _detect_iceberg(row: dict, buffer: deque) -> dict:
    bl     = get_baseline("1S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    has_tr = bool((row.get("candle_dna") or {}).get("has_trade"))
    buflen = len(buffer)

    _null_sc = {
        "HAS_RECURRING_LEVEL": False, "LEVEL_VOLUME_SIGNIFICANT": False,
        "PRICE_CONTAINED": False, "HIGH_VOLUME_CTX": False, "STRONG_RECURRENCE": False,
    }

    def null_rec(sc=None):
        return {
            "detector": "iceberg", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": sc or dict(_null_sc),
            "measurements": {"recurrence_count": 0, "candidate_price": None, "total_at_level": 0.0, "buffer_size": buflen},
            "data_quality": {"has_trade": has_tr, "has_baseline": has_bl, "completeness": buflen / 5.0},
        }

    if buflen < 3:
        return null_rec()

    level_appearances: dict[float, set] = defaultdict(set)
    for window in buffer:
        fp = window.get("footprint_dna") or {}
        for lvl in (fp.get("price_levels") or []):
            p = lvl.get("price")
            if p is not None:
                rounded = round(_safe_float(p), 1)
                level_appearances[rounded].add(window.get("window_start_ts", 0))

    recurring = {p: ts for p, ts in level_appearances.items() if len(ts) >= 3}
    HAS_RECURRING = len(recurring) > 0

    if HAS_RECURRING:
        candidate_price  = max(recurring, key=lambda p: len(recurring[p]))
        recurrence_count = len(recurring[candidate_price])

        level_vols, level_buy, level_sell = [], [], []
        for window in buffer:
            fp = window.get("footprint_dna") or {}
            for lvl in (fp.get("price_levels") or []):
                if round(_safe_float(lvl.get("price", 0)), 1) == candidate_price:
                    level_vols.append(_safe_float(lvl.get("total_volume")))
                    level_buy.append(_safe_float(lvl.get("buy_volume")))
                    level_sell.append(_safe_float(lvl.get("sell_volume")))

        total_at  = sum(level_vols)
        buy_at    = sum(level_buy)
        sell_at   = sum(level_sell)

        r5 = get_matching_5s_window(wts)
        if r5 is not None:
            tv5   = _safe_float((r5.get("volume") or {}).get("total_volume"))
            avg_v = (tv5 / 5.0) if tv5 > 0 else 0.001
        else:
            avg_v = 0.001
        LEVEL_VOL_SIG = total_at > avg_v * 0.40

        atr_v     = _atr(bl)
        last_win  = list(buffer)[-1]
        last_cl   = (last_win.get("candle_dna") or {}).get("close") or {}
        last_close = last_cl.get("price") if isinstance(last_cl, dict) else None

        if last_close is not None:
            pm = abs(_safe_float(last_close) - candidate_price)
            PRICE_CONTAINED = pm < atr_v * 0.20
        else:
            PRICE_CONTAINED = False

        vol_pct         = _metric(bl, "total_volume", "latest_percentile")
        HIGH_VOL_CTX    = vol_pct >= 65.0
        STRONG_RECUR    = recurrence_count >= 4
        iceberg_side    = "bid_iceberg" if sell_at > buy_at else "ask_iceberg"
        total_at_out    = float(total_at)
        cand_out        = candidate_price
    else:
        recurrence_count = 0
        total_at_out     = 0.0
        buy_at = sell_at = 0.0
        LEVEL_VOL_SIG    = PRICE_CONTAINED = HIGH_VOL_CTX = STRONG_RECUR = False
        iceberg_side     = None
        cand_out         = None

    sc = {
        "HAS_RECURRING_LEVEL": HAS_RECURRING, "LEVEL_VOLUME_SIGNIFICANT": LEVEL_VOL_SIG,
        "PRICE_CONTAINED": PRICE_CONTAINED, "HIGH_VOLUME_CTX": HIGH_VOL_CTX,
        "STRONG_RECURRENCE": STRONG_RECUR,
    }
    score = sum(1 for v in sc.values() if v)

    if score >= 4:
        label = "iceberg_strong"
    elif score == 3:
        label = "iceberg_candidate"
    else:
        label = "none"

    direction = iceberg_side if label != "none" else None

    return {
        "detector": "iceberg", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {
            "recurrence_count": recurrence_count, "candidate_price": cand_out,
            "total_at_level": total_at_out, "buffer_size": buflen,
        },
        "data_quality": {"has_trade": has_tr, "has_baseline": has_bl, "completeness": buflen / 5.0},
    }


# ── Trapped Trader Detector ─────────────────────────────────────────────────────
def _detect_trapped_trader(row: dict) -> dict:
    bl     = get_baseline("3S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    mb     = row.get("micro_behavior") or {}
    atr_v  = _atr(bl, 1.0)

    _null_sc = {
        "PRIOR_TREND": False, "REVERSAL": False, "REVERSAL_HIGH_VOLUME": False,
        "DELTA_SWING_SIGNIFICANT": False, "PRICE_COMMITTED": False,
    }

    def null_rec():
        return {
            "detector": "trapped_trader", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": dict(_null_sc),
            "measurements": {
                "delta_sequence": [], "price_sequence": [],
                "delta_swing": 0.0, "price_return": 0.0, "atr": atr_v,
            },
            "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
        }

    deltas = mb.get("delta_sequence")
    prices = mb.get("price_sequence")
    if not deltas or len(deltas) != 3 or not prices or len(prices) != 3:
        return null_rec()
    if any(p is None for p in prices):
        return null_rec()

    deltas = [_safe_float(d) for d in deltas]
    prices = [_safe_float(p) for p in prices]

    prior_buy  = deltas[0] > 0 and deltas[1] > 0 and prices[1] > prices[0]
    prior_sell = deltas[0] < 0 and deltas[1] < 0 and prices[1] < prices[0]
    PRIOR_TREND = prior_buy or prior_sell

    if prior_buy:
        REVERSAL   = deltas[2] < 0 and prices[2] < prices[1]
        trapped_d  = "long_trapped"
    elif prior_sell:
        REVERSAL   = deltas[2] > 0 and prices[2] > prices[1]
        trapped_d  = "short_trapped"
    else:
        REVERSAL   = False
        trapped_d  = None

    vol_pct              = _metric(bl, "total_volume", "latest_percentile")
    REVERSAL_HIGH_VOL    = vol_pct >= 65.0

    delta_swing = abs(deltas[2] - deltas[1])
    DELTA_SWING = delta_swing > atr_v * 0.10 if atr_v > 0 else False

    price_ret       = abs(prices[2] - prices[1])
    PRICE_COMMITTED = price_ret >= atr_v * 0.05 if atr_v > 0 else False

    sc = {
        "PRIOR_TREND": PRIOR_TREND, "REVERSAL": REVERSAL,
        "REVERSAL_HIGH_VOLUME": REVERSAL_HIGH_VOL,
        "DELTA_SWING_SIGNIFICANT": DELTA_SWING, "PRICE_COMMITTED": PRICE_COMMITTED,
    }
    score = sum(1 for v in sc.values() if v)

    if score >= 4:
        label = "trapped_strong"
    elif score == 3:
        label = "trapped_candidate"
    else:
        label = "none"

    if not REVERSAL:
        label = "none"

    direction = trapped_d if label != "none" else None

    return {
        "detector": "trapped_trader", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {
            "delta_sequence": deltas, "price_sequence": prices,
            "delta_swing": delta_swing, "price_return": price_ret, "atr": atr_v,
        },
        "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
    }


# ── Initiative Flow Detector ────────────────────────────────────────────────────
def _detect_initiative_flow(row: dict) -> dict:
    bl     = get_baseline("5S")
    has_bl = bl is not None
    wts    = row.get("window_start_ts", 0)
    wte    = row.get("window_end_ts", 0)
    mb     = row.get("micro_behavior") or {}
    tf_d   = row.get("trade_flow") or {}

    _null_sc = {
        "DELTA_CONSISTENT": False, "PRICE_ADVANCING": False, "ACTIVE_SECONDS_HIGH": False,
        "VOLUME_HIGH": False, "DELTA_Z_SIGNIFICANT": False, "MTF_CONFIRMS": False,
    }

    def null_rec():
        return {
            "detector": "initiative_flow", "mode": "label", "symbol": SYMBOL,
            "window_start_ts": wts, "window_end_ts": wte,
            "label": "none", "direction": None, "score": 0,
            "score_components": dict(_null_sc),
            "measurements": {
                "delta_sequence": [], "price_sequence": [],
                "active_seconds": 0, "volume_percentile": 0.0, "delta_z": 0.0,
            },
            "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
        }

    deltas = mb.get("delta_sequence")
    prices = mb.get("price_sequence")
    active = tf_d.get("active_seconds")

    if not deltas or len(deltas) != 5 or not prices or len(prices) != 5:
        return null_rec()

    deltas = [_safe_float(d) if d is not None else 0.0 for d in deltas]
    prices_c: list[float] = []
    for i, p in enumerate(prices):
        if p is None:
            prices_c.append(prices_c[i - 1] if prices_c else 0.0)
        else:
            prices_c.append(_safe_float(p))

    buy_init  = all(d > 0 for d in deltas)
    sell_init = all(d < 0 for d in deltas)
    DELTA_CONSISTENT = buy_init or sell_init

    if buy_init:
        PRICE_ADVANCING = prices_c[4] > prices_c[0]
        flow_dir        = "buy_initiative"
    elif sell_init:
        PRICE_ADVANCING = prices_c[4] < prices_c[0]
        flow_dir        = "sell_initiative"
    else:
        PRICE_ADVANCING = False
        flow_dir        = None

    active_v           = int(active) if active is not None else 0
    ACTIVE_HIGH        = active_v >= 4
    vol_pct            = _metric(bl, "total_volume", "latest_percentile")
    VOLUME_HIGH        = vol_pct >= 70.0
    delta_z            = _metric(bl, "delta", "z_score")
    DELTA_Z_SIG        = abs(delta_z) >= 1.5

    r15 = get_matching_15s_window(wts)
    if r15 is not None and DELTA_CONSISTENT:
        r15_delta = _safe_float((r15.get("volume") or {}).get("delta"))
        r15_side  = ((r15.get("depth_flow") or {}).get("dominant_side") or "").lower()
        if buy_init:
            MTF_CONFIRMS = r15_delta > 0 and r15_side == "bid"
        elif sell_init:
            MTF_CONFIRMS = r15_delta < 0 and r15_side == "ask"
        else:
            MTF_CONFIRMS = False
    else:
        MTF_CONFIRMS = False

    sc = {
        "DELTA_CONSISTENT": DELTA_CONSISTENT, "PRICE_ADVANCING": PRICE_ADVANCING,
        "ACTIVE_SECONDS_HIGH": ACTIVE_HIGH, "VOLUME_HIGH": VOLUME_HIGH,
        "DELTA_Z_SIGNIFICANT": DELTA_Z_SIG, "MTF_CONFIRMS": MTF_CONFIRMS,
    }
    score = sum(1 for v in sc.values() if v)

    if score >= 5:
        label = "initiative_strong"
    elif score >= 3:
        label = "initiative_candidate"
    else:
        label = "none"

    if not DELTA_CONSISTENT:
        label    = "none"
        flow_dir = None

    direction = flow_dir if label != "none" else None

    active_clamped = max(0, min(int(active_v), 5))

    return {
        "detector": "initiative_flow", "mode": "label", "symbol": SYMBOL,
        "window_start_ts": wts, "window_end_ts": wte,
        "label": label, "direction": direction, "score": score,
        "score_components": sc,
        "measurements": {
            "delta_sequence": deltas, "price_sequence": prices_c,
            "active_seconds": active_clamped, "volume_percentile": vol_pct, "delta_z": delta_z,
        },
        "data_quality": {"has_trade": True, "has_baseline": has_bl, "completeness": None},
    }


# ── Batch mode ──────────────────────────────────────────────────────────────────
def run_batch() -> None:
    _check_halt()
    print("NurtacCoreEngineClaude — Layer-4 Detector Engine (batch)")
    print()

    _load_baselines()

    # Pre-fill cross-reference buffers
    recs_5s, _ = _read_last_n_lines(INPUT_5S)
    if not recs_5s:
        print(f"Waiting for {INPUT_5S}...")
    for r in recs_5s:
        _buf_5s.append(r)

    recs_15s, _ = _read_last_n_lines(INPUT_15S)
    if not recs_15s:
        print(f"Waiting for {INPUT_15S}...")
    for r in recs_15s:
        _buf_15s.append(r)

    # ── 1S-based detectors (absorption, sweep, iceberg) ──
    recs_1s, _ = _read_last_n_lines(INPUT_1S)
    if not recs_1s:
        print(f"Waiting for {INPUT_1S}...")

    last_abs   = [None]
    last_sw    = [None]
    last_ic    = [None]
    iceberg_buf: deque[dict] = deque(maxlen=5)

    for row in recs_1s:
        _check_halt()

        rec_a = _detect_absorption(row)
        errs  = _validate_output(rec_a, "absorption")
        if errs:
            print(f"[VALIDATION FAIL] absorption ts={rec_a.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_abs[0] = rec_a

        rec_s = _detect_sweep(row)
        errs  = _validate_output(rec_s, "sweep")
        if errs:
            print(f"[VALIDATION FAIL] sweep ts={rec_s.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_sw[0] = rec_s

        iceberg_buf.append(row)
        rec_i = _detect_iceberg(row, iceberg_buf)
        errs  = _validate_output(rec_i, "iceberg")
        if errs:
            print(f"[VALIDATION FAIL] iceberg ts={rec_i.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_ic[0] = rec_i

    for det, holder in [("absorption", last_abs), ("sweep", last_sw), ("iceberg", last_ic)]:
        if holder[0] is not None:
            _write_label(det, holder[0])
            _print_label(holder[0])
            print(f"[{det.upper()}] batch done — last ts={holder[0].get('window_start_ts')}")
        else:
            print(f"[{det.upper()}] no valid output produced")

    # ── 3S-based detectors (exhaustion, trapped_trader) ──
    recs_3s, _ = _read_last_n_lines(INPUT_3S)
    if not recs_3s:
        print(f"Waiting for {INPUT_3S}...")

    last_ex = [None]
    last_tr = [None]

    for row in recs_3s:
        _check_halt()

        rec_e = _detect_exhaustion(row)
        errs  = _validate_output(rec_e, "exhaustion")
        if errs:
            print(f"[VALIDATION FAIL] exhaustion ts={rec_e.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_ex[0] = rec_e

        rec_t = _detect_trapped_trader(row)
        errs  = _validate_output(rec_t, "trapped_trader")
        if errs:
            print(f"[VALIDATION FAIL] trapped_trader ts={rec_t.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_tr[0] = rec_t

    for det, holder in [("exhaustion", last_ex), ("trapped_trader", last_tr)]:
        if holder[0] is not None:
            _write_label(det, holder[0])
            _print_label(holder[0])
            print(f"[{det.upper()}] batch done — last ts={holder[0].get('window_start_ts')}")
        else:
            print(f"[{det.upper()}] no valid output produced")

    # ── 5S-based detector (initiative_flow) ──
    last_if = [None]

    for row in recs_5s:
        _check_halt()
        rec_f = _detect_initiative_flow(row)
        errs  = _validate_output(rec_f, "initiative_flow")
        if errs:
            print(f"[VALIDATION FAIL] initiative_flow ts={rec_f.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            last_if[0] = rec_f

    if last_if[0] is not None:
        _write_label("initiative_flow", last_if[0])
        _print_label(last_if[0])
        print(f"[INITIATIVE_FLOW] batch done — last ts={last_if[0].get('window_start_ts')}")
    else:
        print("[INITIATIVE_FLOW] no valid output produced")

    print("\nBatch complete.")


# ── Live mode async tasks ────────────────────────────────────────────────────────
async def _tail_file(path: Path, start_pos: int):
    """Async generator: yields parsed dicts from a tailed JSONL file."""
    while not path.exists():
        _check_halt()
        print(f"Waiting for {path.name}...")
        await asyncio.sleep(FILE_WAIT_SLEEP)

    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(start_pos)
        while True:
            _check_halt()
            line = fh.readline()
            if line:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass
            else:
                await asyncio.sleep(POLL_INTERVAL)


async def _task_absorption() -> None:
    recs, pos = _read_last_n_lines(INPUT_1S)
    for row in recs:
        _check_halt()
        rec  = _detect_absorption(row)
        errs = _validate_output(rec, "absorption")
        if errs:
            print(f"[VALIDATION FAIL] absorption ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("absorption", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_1S, pos):
        rec  = _detect_absorption(row)
        errs = _validate_output(rec, "absorption")
        if errs:
            print(f"[VALIDATION FAIL] absorption ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("absorption", rec)
            _print_label(rec)


async def _task_sweep() -> None:
    recs, pos = _read_last_n_lines(INPUT_1S)
    for row in recs:
        _check_halt()
        rec  = _detect_sweep(row)
        errs = _validate_output(rec, "sweep")
        if errs:
            print(f"[VALIDATION FAIL] sweep ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("sweep", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_1S, pos):
        rec  = _detect_sweep(row)
        errs = _validate_output(rec, "sweep")
        if errs:
            print(f"[VALIDATION FAIL] sweep ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("sweep", rec)
            _print_label(rec)


async def _task_iceberg() -> None:
    buf: deque[dict] = deque(maxlen=5)
    recs, pos = _read_last_n_lines(INPUT_1S)
    for row in recs:
        _check_halt()
        buf.append(row)
        rec  = _detect_iceberg(row, buf)
        errs = _validate_output(rec, "iceberg")
        if errs:
            print(f"[VALIDATION FAIL] iceberg ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("iceberg", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_1S, pos):
        buf.append(row)
        rec  = _detect_iceberg(row, buf)
        errs = _validate_output(rec, "iceberg")
        if errs:
            print(f"[VALIDATION FAIL] iceberg ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("iceberg", rec)
            _print_label(rec)


async def _task_exhaustion() -> None:
    recs, pos = _read_last_n_lines(INPUT_3S)
    for row in recs:
        _check_halt()
        rec  = _detect_exhaustion(row)
        errs = _validate_output(rec, "exhaustion")
        if errs:
            print(f"[VALIDATION FAIL] exhaustion ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("exhaustion", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_3S, pos):
        rec  = _detect_exhaustion(row)
        errs = _validate_output(rec, "exhaustion")
        if errs:
            print(f"[VALIDATION FAIL] exhaustion ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("exhaustion", rec)
            _print_label(rec)


async def _task_trapped_trader() -> None:
    recs, pos = _read_last_n_lines(INPUT_3S)
    for row in recs:
        _check_halt()
        rec  = _detect_trapped_trader(row)
        errs = _validate_output(rec, "trapped_trader")
        if errs:
            print(f"[VALIDATION FAIL] trapped_trader ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("trapped_trader", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_3S, pos):
        rec  = _detect_trapped_trader(row)
        errs = _validate_output(rec, "trapped_trader")
        if errs:
            print(f"[VALIDATION FAIL] trapped_trader ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("trapped_trader", rec)
            _print_label(rec)


async def _task_initiative_flow() -> None:
    recs, pos = _read_last_n_lines(INPUT_5S)
    for row in recs:
        _check_halt()
        _buf_5s.append(row)
        rec  = _detect_initiative_flow(row)
        errs = _validate_output(rec, "initiative_flow")
        if errs:
            print(f"[VALIDATION FAIL] initiative_flow ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("initiative_flow", rec)
            _print_label(rec)
    async for row in _tail_file(INPUT_5S, pos):
        _buf_5s.append(row)
        rec  = _detect_initiative_flow(row)
        errs = _validate_output(rec, "initiative_flow")
        if errs:
            print(f"[VALIDATION FAIL] initiative_flow ts={rec.get('window_start_ts')}: {'; '.join(errs)}")
        else:
            _write_label("initiative_flow", rec)
            _print_label(rec)


async def _task_15s_buffer() -> None:
    recs, pos = _read_last_n_lines(INPUT_15S)
    for row in recs:
        _buf_15s.append(row)
    async for row in _tail_file(INPUT_15S, pos):
        _buf_15s.append(row)


async def _task_baseline_refresh() -> None:
    while True:
        await asyncio.sleep(BASELINE_REFRESH)
        _check_halt()
        _load_baselines()


async def run_live() -> None:
    _check_halt()
    print("NurtacCoreEngineClaude — Layer-4 Detector Engine (live)")
    print("Warming up baselines and cross-reference buffers...")

    _load_baselines()

    # Seed 5S/15S buffers before detectors start
    for r in _read_last_n_lines(INPUT_5S)[0]:
        _buf_5s.append(r)
    for r in _read_last_n_lines(INPUT_15S)[0]:
        _buf_15s.append(r)

    print("Warm-up complete. Starting 6 detector tasks + helpers...")
    print()

    await asyncio.gather(
        _task_absorption(),
        _task_sweep(),
        _task_iceberg(),
        _task_exhaustion(),
        _task_trapped_trader(),
        _task_initiative_flow(),
        _task_15s_buffer(),
        _task_baseline_refresh(),
    )


# ── Entry point ─────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Layer-4 Detector Engine")
    parser.add_argument("--mode", choices=["batch", "live"], required=True)
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch()
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
