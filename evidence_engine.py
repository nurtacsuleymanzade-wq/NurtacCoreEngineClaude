"""
NurtacCoreEngineClaude — Layer-7: Evidence Accumulator + Setup Generator

Primary trigger: data/combined_1s_dna_btcusdt.jsonl
Reads:  gate, structure (1S/1M/5M), 6 detectors, baseline
Writes: data/evidence_stream.jsonl
        data/setups.jsonl

Rules:
  - No Binance API/WebSocket calls
  - No mock data
  - Only reads existing JSONL files
  - No real orders — setup records only
  - Never crash, never write invalid records
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ───────────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.05

EVIDENCE_FILE   = DATA_DIR / "evidence_stream.jsonl"
SETUPS_FILE     = DATA_DIR / "setups.jsonl"
BIAS_CTX_FILE   = DATA_DIR / "bias_context.jsonl"
VOL_PROFILE_1M  = DATA_DIR / "volume_profile_1m.jsonl"
EDGE_MATRIX_FILE = DATA_DIR / "edge_matrix.jsonl"
CALIBRATION_FILE = DATA_DIR / "calibration_profiles.json"

PRIMARY_FILE   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
GATE_FILE      = DATA_DIR / "decision_gate_output.jsonl"
STRUCT_1S_FILE = DATA_DIR / "structure_1s.jsonl"
STRUCT_1M_FILE = DATA_DIR / "structure_1m.jsonl"
STRUCT_5M_FILE = DATA_DIR / "structure_5m.jsonl"
BASELINE_FILE  = DATA_DIR / "historical_baseline_dna.jsonl"
REGIME_FILE    = DATA_DIR / "regime_context.jsonl"
LIQ_CLUSTER_FILE = DATA_DIR / "liquidation_clusters.jsonl"
ORDERBOOK_WALL_FILE = DATA_DIR / "orderbook_walls.jsonl"
WHALE_SUMMARY_FILE = DATA_DIR / "whale_trade_summary.jsonl"
ORDERBOOK_STATS_FILE = DATA_DIR / "orderbook_stats.jsonl"
WHALE_ORDER_FILE = DATA_DIR / "whale_orders.jsonl"
MAX_PAIN_FILE = DATA_DIR / "max_pain.json"

DETECTOR_FILES = {
    "absorption":      DATA_DIR / "labels_absorption.jsonl",
    "sweep":           DATA_DIR / "labels_sweep.jsonl",
    "exhaustion":      DATA_DIR / "labels_exhaustion.jsonl",
    "initiative_flow": DATA_DIR / "labels_initiative_flow.jsonl",
    "trapped_trader":  DATA_DIR / "labels_trapped_trader.jsonl",
    "iceberg":         DATA_DIR / "labels_iceberg.jsonl",
}

EDGE_BONUS_STRONG   = 1.0   # added to a detector's score bucket when edge_matrix says "strong"
EDGE_PENALTY_NEGATIVE = 1.0 # subtracted when edge_matrix says "negative"

MIN_LONG_SCORE_NORMAL = 4.0   # L1_LOW minimum
MIN_LONG_SCORE_FLASH  = 12.0

# Kalite tierlari
TIER_L1_LOW     = 4.0
TIER_L2_MEDIUM  = 6.0
TIER_L3_GOOD    = 8.0
TIER_L4_PREMIUM = 11.0

def _get_tier(score: float) -> str:
    if score >= TIER_L4_PREMIUM: return "L4_PREMIUM"
    if score >= TIER_L3_GOOD:    return "L3_GOOD_A+"
    if score >= TIER_L2_MEDIUM:  return "L2_MEDIUM"
    if score >= TIER_L1_LOW:     return "L1_LOW"
    return "BELOW_MIN"
DOMINANT_GAP_MIN      = 2.0
ATR_MULTIPLIER_SL     = 1.5
ATR_MULTIPLIER_TP1    = 1.5
ATR_MULTIPLIER_TP2    = 3.0
ATR_MULTIPLIER_TP3    = 4.5
NORMAL_COOLDOWN_MS    = 30_000
FLASH_COOLDOWN_MS     = 15_000
MAX_OPEN_SETUPS       = 3
LIVE_CACHE_MAX        = 300

# ── Helpers ───────────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_last_n_lines(path, n: int = 200) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        raw = subprocess.getoutput(f"tail -{int(n)} {path}")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    except Exception:
        pass
    return records

def _read_last_n_jsonl(path: Path, maxlen: int) -> list[dict]:
    """Read only last N lines from JSONL file (memory-efficient warm-up)."""
    return _read_last_n_lines(path, maxlen)


def _count_recent_spoof_cancellations(records, ts: int) -> int:
    cutoff = ts - 300_000
    return sum(
        1 for record in records
        if record.get("event") == "CANCELLED"
        and cutoff <= int(record.get("ts", 0) or 0) <= ts
    )

def _read_vol_profile_1m() -> dict | None:
    """Read last line of volume_profile_1m.jsonl."""
    if not VOL_PROFILE_1M.exists():
        return None
    try:
        last_line = subprocess.getoutput(f"tail -1 {VOL_PROFILE_1M}").strip()
        if not last_line:
            return None
        return json.loads(last_line)
    except Exception:
        return None

def _read_bias_context() -> tuple[str, float]:
    """Read last line of bias_context.jsonl. Returns (dominant_bias, bias_gap)."""
    if not BIAS_CTX_FILE.exists():
        return "neutral", 0.0
    try:
        last_line = subprocess.getoutput(f"tail -1 {BIAS_CTX_FILE}").strip()
        if not last_line:
            return "neutral", 0.0
        rec = json.loads(last_line)
        dom = rec.get("dominant_bias", "neutral")
        gap = float(rec.get("bias_gap", 0.0))
        return dom, gap
    except Exception:
        return "neutral", 0.0

def _read_edge_matrix() -> dict:
    """Read the single-line edge_matrix.jsonl snapshot written by
    edge_matrix_engine.py. Returns {} if missing/empty/invalid —
    callers must treat a missing edge_matrix as "no adjustment"."""
    if not EDGE_MATRIX_FILE.exists():
        return {}
    try:
        last_line = subprocess.getoutput(f"tail -1 {EDGE_MATRIX_FILE}").strip()
        if not last_line:
            return {}
        data = json.loads(last_line)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _read_calibration_profile() -> dict:
    if not CALIBRATION_FILE.exists():
        return {}
    try:
        data = json.loads(CALIBRATION_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}

def _lower_tier(tier: str) -> str:
    lower = {
        "L4_PREMIUM": "L3_GOOD_A+",
        "L3_GOOD_A+": "L2_MEDIUM",
        "L2_MEDIUM": "L1_LOW",
        "L1_LOW": "BELOW_MIN",
    }
    return lower.get(tier, tier)

def _calibration_adjustment(pattern: str, regime: dict, tier: str) -> tuple[float, bool, float | None]:
    profile = _read_calibration_profile()
    pattern_profile = (profile.get("patterns") or {}).get(pattern) or {}
    if not pattern_profile:
        return 0.0, False, None
    candidates = [
        ((pattern_profile.get("by_regime") or {}).get(regime.get("trend_regime")) or {}).get("wr"),
        ((pattern_profile.get("by_session") or {}).get(regime.get("session")) or {}).get("wr"),
        ((pattern_profile.get("by_tier") or {}).get(tier) or {}).get("wr"),
        pattern_profile.get("total_wr"),
    ]
    wr = next((_sf(value, -1.0) for value in candidates if value is not None), -1.0)
    if wr < 0:
        return 0.0, False, None
    if wr >= 0.65:
        return 1.0, False, wr
    if wr < 0.50:
        return 0.0, True, wr
    return 0.0, False, wr

# Detector direction string -> which side of the market that signal pertains to.
# Mirrors historical_outcome_engine._SIDE_MAP so edge_matrix keys line up
# exactly with what was recorded for each (event_type, side) combination.
_DETECTOR_SIDE_MAP: dict[str, str] = {
    "sell_absorbed":   "sell",
    "downward_sweep":  "sell",
    "sell_exhaustion": "sell",
    "ask_iceberg":     "sell",
    "long_trapped":    "sell",
    "sell_initiative": "sell",
    "buy_absorbed":    "buy",
    "upward_sweep":    "buy",
    "buy_exhaustion":  "buy",
    "bid_iceberg":     "buy",
    "short_trapped":   "buy",
    "buy_initiative":  "buy",
}

def _edge_adjustment(edge_matrix: dict, event_type: str, direction: str | None) -> float:
    """Look up edge_matrix[f'{event_type}_{side}'] and return the score
    adjustment: +EDGE_BONUS_STRONG for "strong" edge, -EDGE_PENALTY_NEGATIVE
    for "negative" edge, 0.0 otherwise (moderate/neutral/unknown/missing)."""
    side = _DETECTOR_SIDE_MAP.get(direction or "")
    if not side or not edge_matrix:
        return 0.0
    entry = edge_matrix.get(f"{event_type}_{side}")
    if not entry:
        return 0.0
    edge = entry.get("edge")
    if edge == "strong":
        return EDGE_BONUS_STRONG
    if edge == "negative":
        return -EDGE_PENALTY_NEGATIVE
    return 0.0

def _build_exact_index(records: list[dict]) -> dict[int, dict]:
    """Index records by window_start_ts. Latest record wins on duplicate ts."""
    idx: dict[int, dict] = {}
    for rec in records:
        ts = rec.get("window_start_ts", rec.get("ts"))
        if ts is not None:
            idx[int(ts)] = rec
    return idx

def _cache_put(cache: dict[int, dict], ts: int, rec: dict) -> None:
    cache[ts] = rec
    overflow = len(cache) - LIVE_CACHE_MAX
    if overflow > 0:
        for old_ts in sorted(cache)[:overflow]:
            cache.pop(old_ts, None)

def _get_latest_at_or_before(idx: dict[int, dict], ts: int) -> dict | None:
    """Return the record with the largest ts <= given ts."""
    candidates = [k for k in idx if k <= ts]
    if not candidates:
        return None
    return idx[max(candidates)]

def _write_jsonl(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

# ── Setup state ────────────────────────────────────────────────────────────────────
class SetupState:
    def __init__(self):
        self.open_setups: list[dict] = []
        self.last_normal_long_ts:  int = 0
        self.last_normal_short_ts: int = 0
        self.last_flash_long_ts:   int = 0
        self.last_flash_short_ts:  int = 0
        self.emitted_ids: set[str] = set()

# ── Evidence computation ───────────────────────────────────────────────────────────
def _score_detectors(
    det_recs: dict[str, dict | None],
    edge_matrix: dict | None = None,
) -> tuple[float, float, dict]:
    """Compute detector contribution to long/short scores.

    edge_matrix (from edge_matrix_engine.py / data/edge_matrix.jsonl) is
    consulted per detector signal: when the historical 60s win rate for
    that exact (event_type, side) is "strong", +EDGE_BONUS_STRONG is added
    to whichever score bucket the signal feeds; when "negative",
    -EDGE_PENALTY_NEGATIVE is applied instead. This is how the system
    learns from its own track record over time.
    """
    edge_matrix = edge_matrix or {}
    long_add  = 0.0
    short_add = 0.0
    comps: dict[str, dict] = {}

    def _strength(label: str | None, is_strong: bool) -> float:
        if label is None:
            return 0.0
        if "strong" in label:
            return 2.0 if is_strong else 1.5
        if "candidate" in label:
            return 1.0 if is_strong else 0.75
        return 0.0

    # Absorption
    rec = det_recs.get("absorption")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")
    edge_adj = _edge_adjustment(edge_matrix, "absorption", drn)
    if drn == "sell_absorbed":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        long_add += v + edge_adj
    elif drn == "buy_absorbed":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        short_add += v + edge_adj
    comps["absorption"] = {"label": lbl, "direction": drn, "edge_adjustment": edge_adj}

    # Sweep
    rec = det_recs.get("sweep")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")
    edge_adj = _edge_adjustment(edge_matrix, "sweep", drn)
    if drn == "downward_sweep":
        v = 1.5 if lbl and "strong" in lbl else (0.75 if lbl and "candidate" in lbl else 0.0)
        long_add += v + edge_adj
    elif drn == "upward_sweep":
        v = 1.5 if lbl and "strong" in lbl else (0.75 if lbl and "candidate" in lbl else 0.0)
        short_add += v + edge_adj
    comps["sweep"] = {"label": lbl, "direction": drn, "edge_adjustment": edge_adj}

    # Exhaustion
    rec = det_recs.get("exhaustion")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")
    edge_adj = _edge_adjustment(edge_matrix, "exhaustion", drn)
    if drn == "sell_exhaustion":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        long_add += v + edge_adj
    elif drn == "buy_exhaustion":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        short_add += v + edge_adj
    comps["exhaustion"] = {"label": lbl, "direction": drn, "edge_adjustment": edge_adj}

    # Initiative flow
    rec = det_recs.get("initiative_flow")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")
    edge_adj = _edge_adjustment(edge_matrix, "initiative_flow", drn)
    if drn == "buy_initiative":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        long_add += v + edge_adj
    elif drn == "sell_initiative":
        v = 2.0 if lbl and "strong" in lbl else (1.0 if lbl and "candidate" in lbl else 0.0)
        short_add += v + edge_adj
    comps["initiative_flow"] = {"label": lbl, "direction": drn, "edge_adjustment": edge_adj}

    # Trapped trader
    rec = det_recs.get("trapped_trader")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")
    edge_adj = _edge_adjustment(edge_matrix, "trapped_trader", drn)
    if drn == "short_trapped":
        v = 1.5 if lbl and "strong" in lbl else (0.75 if lbl and "candidate" in lbl else 0.0)
        long_add += v + edge_adj
    elif drn == "long_trapped":
        v = 1.5 if lbl and "strong" in lbl else (0.75 if lbl and "candidate" in lbl else 0.0)
        short_add += v + edge_adj
    comps["trapped_trader"] = {"label": lbl, "direction": drn, "edge_adjustment": edge_adj}

    # Iceberg — constraint: only counts if at least 1 other bullish/bearish detector fires
    rec = det_recs.get("iceberg")
    lbl = (rec or {}).get("label", "none")
    drn = (rec or {}).get("direction")

    _bull_dets = {
        "absorption": "sell_absorbed", "sweep": "downward_sweep",
        "exhaustion": "sell_exhaustion", "initiative_flow": "buy_initiative",
        "trapped_trader": "short_trapped",
    }
    _bear_dets = {
        "absorption": "buy_absorbed", "sweep": "upward_sweep",
        "exhaustion": "buy_exhaustion", "initiative_flow": "sell_initiative",
        "trapped_trader": "long_trapped",
    }
    other_bull = sum(
        1 for d, expected_dir in _bull_dets.items()
        if (det_recs.get(d) or {}).get("direction") == expected_dir
    )
    other_bear = sum(
        1 for d, expected_dir in _bear_dets.items()
        if (det_recs.get(d) or {}).get("direction") == expected_dir
    )
    iceberg_counted = False
    edge_adj = 0.0
    if drn == "bid_iceberg" and other_bull >= 1:
        v = 1.0 if lbl and "strong" in lbl else (0.5 if lbl and "candidate" in lbl else 0.0)
        edge_adj = _edge_adjustment(edge_matrix, "iceberg", drn)
        long_add += v + edge_adj
        iceberg_counted = v > 0
    elif drn == "ask_iceberg" and other_bear >= 1:
        v = 1.0 if lbl and "strong" in lbl else (0.5 if lbl and "candidate" in lbl else 0.0)
        edge_adj = _edge_adjustment(edge_matrix, "iceberg", drn)
        short_add += v + edge_adj
        iceberg_counted = v > 0
    comps["iceberg"] = {
        "label": lbl, "direction": drn,
        "iceberg_counted": iceberg_counted, "edge_adjustment": edge_adj,
    }

    return long_add, short_add, comps

def compute_evidence(
    primary: dict,
    gate:    dict | None,
    s1s:     dict | None,
    s1m:     dict | None,
    s5m:     dict | None,
    det_recs: dict[str, dict | None],
    baseline: dict | None,
    liquidation: dict | None = None,
    walls: dict | None = None,
    whale_summary: dict | None = None,
    orderbook_stats: dict | None = None,
    spoofing_cancel_count: int = 0,
) -> dict:
    """Compute long_score and short_score for one 1S bar."""
    long_score  = 0.0
    short_score = 0.0
    comps: dict[str, dict] = {}
    score_breakdown: dict[str, float] = {}

    # ── A. Candle DNA ─────────────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    cdna = primary.get("candle_dna") or {}
    ddna = primary.get("depth_dna") or {}
    delta        = _sf(cdna.get("delta"), 0.0)
    buy_vol      = _sf(cdna.get("buy_volume"), 0.0)
    sell_vol     = _sf(cdna.get("sell_volume"), 0.0)
    total_vol    = _sf(cdna.get("total_volume"), 0.0)
    depth_side   = ddna.get("dominant_side", "")   # "BID" or "ASK"

    d_pos  = delta > 0
    d_neg  = delta < 0
    d_spos = d_pos and total_vol > 0 and buy_vol  > total_vol * 0.70
    d_sneg = d_neg and total_vol > 0 and sell_vol > total_vol * 0.70
    d_bid  = depth_side == "BID"
    d_ask  = depth_side == "ASK"

    if d_pos:  long_score  += 1.0
    if d_neg:  short_score += 1.0
    if d_spos: long_score  += 1.0
    if d_sneg: short_score += 1.0
    if d_bid:  long_score  += 1.0
    if d_ask:  short_score += 1.0

    comps["candle_dna"] = {
        "delta_positive":       d_pos,
        "delta_negative":       d_neg,
        "delta_strong_positive": d_spos,
        "delta_strong_negative": d_sneg,
        "depth_bid_dominant":   d_bid,
        "depth_ask_dominant":   d_ask,
    }
    score_breakdown["candle_dna_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["candle_dna_short"] = round(short_score - _pre_short, 4)

    # ── B. Gate ───────────────────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    gate_grade = None
    gate_dir   = None
    g_comp: dict[str, bool] = {}
    if gate:
        gate_grade = gate.get("setup_grade")
        gate_dir   = gate.get("dominant_direction")
        for grade, pts in [("A", 3.0), ("B", 2.0), ("C", 1.0)]:
            gb = gate_grade == grade and gate_dir == "bullish"
            bb = gate_grade == grade and gate_dir == "bearish"
            g_comp[f"gate_bullish_{grade}"] = gb
            g_comp[f"gate_bearish_{grade}"] = bb
            if gb: long_score  += pts
            if bb: short_score += pts
    g_comp["grade"]     = gate_grade
    g_comp["direction"] = gate_dir
    comps["gate"] = g_comp
    score_breakdown["gate_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["gate_short"] = round(short_score - _pre_short, 4)

    # ── C. Smart Money ────────────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    # 1S structure
    s1s_trend = (s1s or {}).get("trend") or {}
    s1s_bos   = (s1s or {}).get("bos") or {}
    micro_bos       = s1s_bos.get("micro_bos")
    s1s_dir         = s1s_trend.get("direction", "unknown")
    choch_confirmed = s1s_trend.get("choch_confirmed")
    msb             = s1s_trend.get("msb")

    sm1s: dict[str, bool] = {}
    sm1s["micro_bos_bullish"] = micro_bos == "bullish"
    sm1s["micro_bos_bearish"] = micro_bos == "bearish"
    sm1s["trend_uptrend"]     = s1s_dir == "uptrend"
    sm1s["trend_downtrend"]   = s1s_dir == "downtrend"
    sm1s["choch_bullish"]     = choch_confirmed == "bullish"
    sm1s["choch_bearish"]     = choch_confirmed == "bearish"
    sm1s["msb_bullish"]       = msb == "bullish"
    sm1s["msb_bearish"]       = msb == "bearish"

    if sm1s["micro_bos_bullish"]: long_score  += 1.0
    if sm1s["micro_bos_bearish"]: short_score += 1.0
    if sm1s["trend_uptrend"]:     long_score  += 1.0
    if sm1s["trend_downtrend"]:   short_score += 1.0
    if sm1s["choch_bullish"]:     long_score  += 1.0
    if sm1s["choch_bearish"]:     short_score += 1.0
    if sm1s["msb_bullish"]:       long_score  += 1.0
    if sm1s["msb_bearish"]:       short_score += 1.0
    comps["smart_money_1s"] = sm1s

    # 1M structure
    s1m_trend  = (s1m or {}).get("trend") or {}
    s1m_bos    = (s1m or {}).get("bos") or {}
    macro_bos  = s1m_bos.get("macro_bos")
    macro_tf   = s1m_bos.get("macro_bos_tf")
    s1m_dir    = s1m_trend.get("direction", "unknown")

    sm1m: dict[str, bool] = {}
    sm1m["macro_bos_bullish"] = macro_bos == "bullish" and macro_tf == "1M"
    sm1m["macro_bos_bearish"] = macro_bos == "bearish" and macro_tf == "1M"
    sm1m["trend_uptrend_1m"]  = s1m_dir == "uptrend"
    sm1m["trend_downtrend_1m"] = s1m_dir == "downtrend"

    if sm1m["macro_bos_bullish"]: long_score  += 1.0
    if sm1m["macro_bos_bearish"]: short_score += 1.0
    if sm1m["trend_uptrend_1m"]:  long_score  += 1.0
    if sm1m["trend_downtrend_1m"]: short_score += 1.0
    comps["smart_money_1m"] = sm1m

    # 5M structure
    s5m_trend = (s5m or {}).get("trend") or {}
    s5m_dir   = s5m_trend.get("direction", "unknown")

    sm5m: dict[str, bool] = {}
    sm5m["trend_uptrend_5m"]  = s5m_dir == "uptrend"
    sm5m["trend_downtrend_5m"] = s5m_dir == "downtrend"

    if sm5m["trend_uptrend_5m"]:  long_score  += 1.0
    if sm5m["trend_downtrend_5m"]: short_score += 1.0
    comps["smart_money_5m"] = sm5m
    score_breakdown["smart_money_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["smart_money_short"] = round(short_score - _pre_short, 4)

    # ── D. Detectors ──────────────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    edge_matrix = _read_edge_matrix()
    det_long, det_short, det_comps = _score_detectors(det_recs, edge_matrix)
    long_score  += det_long
    short_score += det_short
    comps["detectors"] = det_comps
    score_breakdown["detector_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["detector_short"] = round(short_score - _pre_short, 4)

    # ── E. Baseline ───────────────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    bl1m      = None
    bl_vwap   = None
    bl_cvd    = None
    bl_atr_st = None
    if baseline:
        bl_vwap   = (baseline.get("vwap") or {}).get("price_vs_vwap")
        bl_cvd    = (baseline.get("cvd") or {}).get("cvd_direction")
        bl_atr_st = (baseline.get("atr") or {}).get("atr_status")

    bl_comp: dict = {}
    bl_comp["vwap_above"]    = bl_vwap == "above"
    bl_comp["vwap_below"]    = bl_vwap == "below"
    bl_comp["cvd_rising"]    = bl_cvd  == "rising"
    bl_comp["cvd_falling"]   = bl_cvd  == "falling"
    bl_comp["atr_extreme_high"] = bl_atr_st == "extreme_high"

    if bl_comp["vwap_above"]:  long_score  += 1.0
    if bl_comp["vwap_below"]:  short_score += 1.0
    if bl_comp["cvd_rising"]:  long_score  += 1.0
    if bl_comp["cvd_falling"]: short_score += 1.0
    comps["baseline"] = bl_comp

    # ATR extreme_high → scale down both scores
    if bl_comp["atr_extreme_high"]:
        long_score  *= 0.85
        short_score *= 0.85
    score_breakdown["baseline_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["baseline_short"] = round(short_score - _pre_short, 4)

    # ── F. Market Context Bias ────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    bias_dom, bias_gap_val = _read_bias_context()
    mc_long  = 0.0
    mc_short = 0.0
    if bias_dom == "long":
        mc_long = 2.0 if bias_gap_val >= 2.0 else 1.0
    elif bias_dom == "short":
        mc_short = 2.0 if bias_gap_val >= 2.0 else 1.0
    long_score  += mc_long
    short_score += mc_short
    comps["market_context_bias"] = {
        "dominant_bias":  bias_dom,
        "bias_gap":       round(bias_gap_val, 4),
        "long_contribution":  mc_long,
        "short_contribution": mc_short,
    }
    score_breakdown["market_context_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["market_context_short"] = round(short_score - _pre_short, 4)

    # ── G. Volume Profile Bias ────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    vp_rec = _read_vol_profile_1m()
    vp_long  = 0.0
    vp_short = 0.0
    vp_comp: dict = {"available": vp_rec is not None}
    if vp_rec:
        bh  = vp_rec.get("bias_hint") or {}
        ms  = vp_rec.get("market_state") or {}
        fa  = ms.get("failed_auction") or {}
        loc_bias   = bh.get("location_bias", "neutral")
        shape_bias = bh.get("shape_bias", "neutral")
        state_bias = bh.get("state_bias", "neutral")
        if loc_bias == "long":
            vp_long  += 1.5
        elif loc_bias == "short":
            vp_short += 1.5
        if shape_bias == "long":
            vp_long  += 1.0
        elif shape_bias == "short":
            vp_short += 1.0
        if state_bias == "long":
            vp_long  += 1.0
        elif state_bias == "short":
            vp_short += 1.0
        if fa.get("detected"):
            fa_dir = fa.get("direction", "")
            if fa_dir == "failed_auction_above":
                vp_short += 2.0
            elif fa_dir == "failed_auction_below":
                vp_long  += 2.0
        vp_comp["location_bias"]  = loc_bias
        vp_comp["shape_bias"]     = shape_bias
        vp_comp["state_bias"]     = state_bias
        vp_comp["failed_auction"] = fa.get("direction") if fa.get("detected") else None
        vp_comp["long_contribution"]  = vp_long
        vp_comp["short_contribution"] = vp_short

    long_score  += vp_long
    short_score += vp_short
    comps["volume_profile_bias"] = vp_comp
    score_breakdown["volume_profile_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["volume_profile_short"] = round(short_score - _pre_short, 4)

    # ── H. Scenario Bias ──────────────────────────────────────────────────────
    _pre_long, _pre_short = long_score, short_score
    sc_long  = 0.0
    sc_short = 0.0
    sc_comp: dict = {"available": False}
    _sc_file = DATA_DIR / "scenarios.jsonl"
    if _sc_file.exists():
        try:
            _sc_last = subprocess.getoutput(f"tail -1 {_sc_file}").strip()
            if _sc_last:
                _sc_rec = json.loads(_sc_last)
                _dom    = _sc_rec.get("dominant_scenario")
                _dom_dir = _sc_rec.get("dominant_direction", "neutral")
                _active = _sc_rec.get("active_scenarios") or []
                sc_comp["available"] = True
                sc_comp["dominant_scenario"] = _dom
                sc_comp["dominant_direction"] = _dom_dir

                # dominant_scenario confirmed
                _confirmed = [s for s in _active if s.get("status") == "confirmed"]
                _dom_status = None
                for _s in _active:
                    if _s.get("scenario_name") == _dom:
                        _dom_status = _s.get("status")
                        break

                if _dom and _dom_status == "confirmed":
                    if _dom_dir == "bullish":
                        sc_long  += 3.0
                    elif _dom_dir == "bearish":
                        sc_short += 3.0
                elif _dom and _dom_status == "developing":
                    if _dom_dir == "bullish":
                        sc_long  += 1.5
                    elif _dom_dir == "bearish":
                        sc_short += 1.5

                # multi-scenario alignment bonus
                if len(_confirmed) >= 2:
                    _conf_dirs = {s.get("direction") for s in _confirmed
                                  if s.get("direction") not in (None, "neutral")}
                    if len(_conf_dirs) == 1:
                        _aligned_dir = next(iter(_conf_dirs))
                        if _aligned_dir == "bullish":
                            sc_long  += 1.0
                        elif _aligned_dir == "bearish":
                            sc_short += 1.0

                sc_comp["long_contribution"]  = sc_long
                sc_comp["short_contribution"] = sc_short
        except Exception:
            pass

    long_score  += sc_long
    short_score += sc_short
    comps["scenario_bias"] = sc_comp
    score_breakdown["scenario_long"]  = round(long_score  - _pre_long,  4)
    score_breakdown["scenario_short"] = round(short_score - _pre_short, 4)

    # ── I. Liquidation magnets and market-depth intelligence ───────────────
    current_price = _sf((cdna.get("close") or {}).get("price"), 0.0)
    liquidation = liquidation or {}
    nearby_long = liquidation.get("nearby_long_clusters") or []
    nearby_short = liquidation.get("nearby_short_clusters") or []
    magnet_below = next((
        cluster for cluster in nearby_long
        if _sf(cluster.get("usd_at_risk")) > 20
        and cluster.get("intensity_label") in ("HOT", "WARM")
        and _sf(cluster.get("price")) < current_price
    ), None)
    magnet_above = next((
        cluster for cluster in nearby_short
        if _sf(cluster.get("usd_at_risk")) > 20
        and cluster.get("intensity_label") in ("HOT", "WARM")
        and _sf(cluster.get("price")) > current_price
    ), None)
    has_liq_magnet_below = magnet_below is not None
    has_liq_magnet_above = magnet_above is not None
    if magnet_above:
        boost = 1.5 if magnet_above.get("cascade_capable") else 1.0
        tier = magnet_above.get("leverage_tier", "?")
        long_score += boost
        score_breakdown[f"liq_magnet_long_{tier}"] = boost
    if magnet_below:
        boost = 1.5 if magnet_below.get("cascade_capable") else 1.0
        tier = magnet_below.get("leverage_tier", "?")
        short_score += boost
        score_breakdown[f"liq_magnet_short_{tier}"] = boost

    walls = walls or {}
    nearest_ask = walls.get("nearest_ask_wall") or {}
    nearest_bid = walls.get("nearest_bid_wall") or {}
    wall_blocking_long = (
        bool(nearest_ask)
        and _sf(nearest_ask.get("distance_pct"), 999.0) < 0.5
        and _sf(nearest_ask.get("qty_btc")) > 20
    )
    wall_blocking_short = (
        bool(nearest_bid)
        and _sf(nearest_bid.get("distance_pct"), 999.0) < 0.5
        and _sf(nearest_bid.get("qty_btc")) > 20
    )
    walls_reliable = spoofing_cancel_count < 3
    if wall_blocking_long and walls_reliable:
        long_score -= 1.0
        score_breakdown["wall_blocking_long"] = -1.0
    if wall_blocking_short and walls_reliable:
        short_score -= 1.0
        score_breakdown["wall_blocking_short"] = -1.0

    whale_summary = whale_summary or {}
    whale_pressure = whale_summary.get("whale_pressure")
    whale_strength = whale_summary.get("pressure_strength")
    whale_boost = 1.0 if whale_strength == "STRONG" else 0.5
    if whale_pressure == "buy":
        long_score += whale_boost
        score_breakdown["whale_pressure_long"] = whale_boost
        if whale_strength == "STRONG":
            short_score -= 1.0
            score_breakdown["whale_counter_short"] = -1.0
    elif whale_pressure == "sell":
        short_score += whale_boost
        score_breakdown["whale_pressure_short"] = whale_boost
        if whale_strength == "STRONG":
            long_score -= 1.0
            score_breakdown["whale_counter_long"] = -1.0

    orderbook_stats = orderbook_stats or {}
    bid_pct = _sf(orderbook_stats.get("bid_pct"), 50.0)
    if bid_pct >= 65:
        long_score += 1.0
        score_breakdown["ob_imbalance_long"] = 1.0
    elif bid_pct <= 35:
        short_score += 1.0
        score_breakdown["ob_imbalance_short"] = 1.0

    active_whale_orders = orderbook_stats.get("active_whale_orders") or {}
    institutional_walls = active_whale_orders.get("INSTITUTIONAL") or []
    institutional_ask_walls = [
        wall for wall in institutional_walls
        if wall.get("side") == "ask"
        and _sf(wall.get("price")) > current_price
    ]
    institutional_bid_walls = [
        wall for wall in institutional_walls
        if wall.get("side") == "bid"
        and 0 < _sf(wall.get("price")) < current_price
    ]
    if current_price > 0 and institutional_ask_walls:
        nearest_ask_wall = min(
            institutional_ask_walls, key=lambda wall: _sf(wall.get("price")),
        )
        ask_distance_pct = (
            (_sf(nearest_ask_wall.get("price")) - current_price)
            / current_price * 100
        )
        if ask_distance_pct < 1.0:
            long_score -= 2.0
            score_breakdown["institutional_wall_blocking_long"] = -2.0
        short_score += 1.5
        score_breakdown["institutional_support_short"] = 1.5
    if current_price > 0 and institutional_bid_walls:
        nearest_bid_wall = max(
            institutional_bid_walls, key=lambda wall: _sf(wall.get("price")),
        )
        bid_distance_pct = (
            (current_price - _sf(nearest_bid_wall.get("price")))
            / current_price * 100
        )
        if bid_distance_pct < 1.0:
            short_score -= 2.0
            score_breakdown["institutional_wall_blocking_short"] = -2.0
        long_score += 1.5
        score_breakdown["institutional_support_long"] = 1.5
    comps["liquidation_context"] = {
        "available": bool(liquidation),
        "cascade_risk": liquidation.get("cascade_risk"),
        "liq_magnet_below": has_liq_magnet_below,
        "liq_magnet_above": has_liq_magnet_above,
        "walls_available": bool(walls),
        "wall_blocking_long": wall_blocking_long,
        "wall_blocking_short": wall_blocking_short,
        "walls_reliable": walls_reliable,
        "spoofing_cancel_count_5m": spoofing_cancel_count,
        "whale_pressure": whale_pressure,
        "whale_pressure_strength": whale_strength,
        "ob_pressure": orderbook_stats.get("ob_pressure"),
        "bid_pct": bid_pct,
        "institutional_wall_count": len(institutional_walls),
        "institutional_ask_wall_count": len(institutional_ask_walls),
        "institutional_bid_wall_count": len(institutional_bid_walls),
        "nearest_liq_long": nearby_long[0].get("price") if nearby_long else None,
        "nearest_liq_short": nearby_short[0].get("price") if nearby_short else None,
    }

    # ── J. Deribit max pain and options/futures regime ─────────────────────
    try:
        max_pain = (
            json.loads(MAX_PAIN_FILE.read_text(encoding="utf-8"))
            if MAX_PAIN_FILE.exists() else {}
        )
    except Exception:
        max_pain = {}
    mp_bias = max_pain.get("mp_bias", "neutral")
    mp_proximity = max_pain.get("expiry_proximity", "LOW")
    options_futures = max_pain.get("options_futures_ratio") or {}
    signal_confidence = options_futures.get("signal_confidence", "HIGH")

    if signal_confidence == "LOW":
        long_score *= 0.5
        short_score *= 0.5
        score_breakdown["options_dominant_penalty"] = -0.5

    mp_boost = 1.0 if mp_proximity == "HIGH" else 0.5
    if mp_bias == "bullish":
        long_score += mp_boost
        short_score -= mp_boost * 0.5
        score_breakdown["max_pain_long"] = mp_boost
        score_breakdown["max_pain_counter_short"] = -(mp_boost * 0.5)
    elif mp_bias == "bearish":
        short_score += mp_boost
        long_score -= mp_boost * 0.5
        score_breakdown["max_pain_short"] = mp_boost
        score_breakdown["max_pain_counter_long"] = -(mp_boost * 0.5)
    comps["max_pain_context"] = {
        "available": bool(max_pain),
        "max_pain_price": max_pain.get("max_pain_price"),
        "bias": mp_bias,
        "expiry_proximity": mp_proximity,
        "options_futures_regime": options_futures.get("regime"),
        "signal_confidence": signal_confidence,
    }

    # Clamp to ≥ 0 (multiplication shouldn't go negative, but guard)
    long_score  = max(0.0, long_score)
    short_score = max(0.0, short_score)

    # ── Dominant side ─────────────────────────────────────────────────────────────
    score_gap = abs(long_score - short_score)
    if long_score > short_score and score_gap >= DOMINANT_GAP_MIN:
        dominant = "long"
    elif short_score > long_score and score_gap >= DOMINANT_GAP_MIN:
        dominant = "short"
    else:
        dominant = "neutral"

    ts = int(primary.get("window_start_ts", 0))
    wte = int(primary.get("window_end_ts", ts + 1000))

    return {
        "engine":          "evidence_accumulator",
        "symbol":          SYMBOL,
        "window_start_ts": ts,
        "window_end_ts":   wte,
        "close_price":     _sf(((primary.get("candle_dna") or {}).get("close") or {}).get("price"), 0.0),
        "long_score":      round(long_score,  4),
        "short_score":     round(short_score, 4),
        "dominant_side":   dominant,
        "score_gap":       round(score_gap, 4),
        "score_breakdown": score_breakdown,
        "evidence_components": comps,
        "data_sources_available": {
            "gate":        gate is not None,
            "structure_1s": s1s is not None,
            "structure_1m": s1m is not None,
            "structure_5m": s5m is not None,
            "baseline":    baseline is not None,
            "liquidation_clusters": bool(liquidation),
            "orderbook_walls": bool(walls),
            "whale_trade_summary": bool(whale_summary),
            "orderbook_stats": bool(orderbook_stats),
            "max_pain": bool(max_pain),
        },
    }

# ── Validation ────────────────────────────────────────────────────────────────────
def _validate_evidence(rec: dict) -> list[str]:
    errors: list[str] = []
    ls = _sf(rec.get("long_score"),  -1.0)
    ss = _sf(rec.get("short_score"), -1.0)
    if ls < 0 or ls != ls or abs(ls) == float("inf"):
        errors.append(f"[1] long_score invalid: {ls}")
    if ss < 0 or ss != ss or abs(ss) == float("inf"):
        errors.append(f"[1] short_score invalid: {ss}")
    if rec.get("dominant_side") not in ("long", "short", "neutral"):
        errors.append(f"[2] dominant_side invalid: {rec.get('dominant_side')}")
    expected_gap = round(abs(ls - ss), 4)
    if abs(_sf(rec.get("score_gap"), -999) - expected_gap) > 1e-6:
        errors.append(f"[3] score_gap mismatch: {rec.get('score_gap')} vs {expected_gap}")
    return errors

def _validate_setup(rec: dict) -> list[str]:
    errors: list[str] = []
    direction = rec.get("direction")
    stype     = rec.get("setup_type")

    if direction not in ("long", "short"):
        errors.append(f"[4] direction invalid: {direction}")
    if stype not in ("normal", "flash"):
        errors.append(f"[5] setup_type invalid: {stype}")

    entry = _sf((rec.get("entry") or {}).get("price"), 0.0)
    sl    = _sf((rec.get("sl")   or {}).get("price"), 0.0)
    tp1   = _sf((rec.get("tp1")  or {}).get("price"), 0.0)
    tp2   = _sf((rec.get("tp2")  or {}).get("price"), 0.0)
    tp3   = _sf((rec.get("tp3")  or {}).get("price"), 0.0)

    if direction == "long":
        if not (sl < entry < tp1 < tp2 < tp3):
            errors.append(f"[6] long price order: sl={sl} entry={entry} tp1={tp1} tp2={tp2} tp3={tp3}")
    elif direction == "short":
        if not (sl > entry > tp1 > tp2 > tp3):
            errors.append(f"[7] short price order: sl={sl} entry={entry} tp1={tp1} tp2={tp2} tp3={tp3}")

    if not (_sf(rec.get("atr_used"), 0.0) > 0):
        errors.append(f"[8] atr_used <= 0: {rec.get('atr_used')}")

    tc = rec.get("trigger_conditions") or {}
    if not all(isinstance(v, bool) for v in tc.values()):
        errors.append(f"[9] trigger_conditions not all bool: {tc}")

    if stype == "normal":
        if len(tc) != 7 or not all(tc.values()):
            errors.append(f"[10] normal setup trigger_conditions not all True: {tc}")
    elif stype == "flash":
        if len(tc) != 5:
            errors.append(f"[11] flash setup must have 5 conditions, got {len(tc)}")

    return errors

# ── ATR resolution ─────────────────────────────────────────────────────────────────
def _resolve_atr(baseline: dict | None, s1s: dict | None) -> float:
    if baseline:
        val = _sf((baseline.get("atr") or {}).get("atr"), 0.0)
        if val > 0:
            return val
    if s1s:
        val = _sf(s1s.get("atr_used"), 0.0)
        if val > 0:
            return val
    return 1.0

def _calc_entry_timing(ev: dict, s1s: dict, atr: float) -> str:
    """Return how far price has travelled from the last BOS, in ATR units."""
    bos_price = (s1s or {}).get("bos", {}).get("last_bos_price")
    current = ev.get("close_price", 0)
    if not bos_price or not atr or atr == 0:
        return "unknown"
    distance = abs(current - bos_price) / atr
    if distance < 0.5:
        return "early"
    if distance < 1.0:
        return "mid"
    if distance < 2.0:
        return "late"
    return "extended"

# ── Setup generation ───────────────────────────────────────────────────────────────
def _check_active_ob_fvg(structure: dict | None, direction: str) -> tuple[int, int, list[dict]]:
    """Return (active_ob_count, active_fvg_count, active_bull_obs)."""
    if structure is None:
        return 0, 0, []
    obs  = structure.get("order_blocks") or []
    fvgs = structure.get("fvg") or []

    prefix_ob  = "bullish_ob"  if direction == "long" else "bearish_ob"
    prefix_fvg = "bullish_fvg" if direction == "long" else "bearish_fvg"

    active_obs = [ob for ob in obs
                  if ob.get("ob_type") == prefix_ob and ob.get("status") == "active"]
    active_fvgs = [f for f in fvgs
                   if f.get("fvg_type") == prefix_fvg and f.get("status") == "active"]
    return len(active_obs), len(active_fvgs), active_obs

def _refine_sl(sl_price: float, direction: str, active_obs: list[dict], atr: float) -> float:
    if not active_obs:
        return sl_price
    if direction == "long":
        lowest_ob_low = min(ob["ob_low"] for ob in active_obs)
        return max(sl_price, lowest_ob_low - atr * 0.1)
    else:
        highest_ob_high = max(ob["ob_high"] for ob in active_obs)
        return min(sl_price, highest_ob_high + atr * 0.1)

def try_generate_setup(
    ev: dict,
    gate: dict | None,
    s1s: dict | None,
    s1m: dict | None,
    s5m: dict | None,
    det_recs: dict[str, dict | None],
    baseline: dict | None,
    regime: dict | None,
    primary: dict,
    state: SetupState,
    setups_fh,
) -> None:
    """Check setup conditions and write to setups.jsonl if triggered."""
    ts  = ev["window_start_ts"]
    wte = ev["window_end_ts"]
    dominant = ev["dominant_side"]
    ls  = ev["long_score"]
    ss  = ev["short_score"]

    atr = _resolve_atr(baseline, s1s)
    regime = regime or {}
    entry_timing = _calc_entry_timing(ev, s1s or {}, atr)

    cdna        = primary.get("candle_dna") or {}
    entry_price = _sf((cdna.get("close") or {}).get("price"), 0.0)
    if entry_price <= 0:
        return

    gate_grade = (gate or {}).get("setup_grade")
    gate_dir   = (gate or {}).get("dominant_direction")
    s1s_trend  = (s1s or {}).get("trend") or {}
    s1s_bos    = (s1s or {}).get("bos") or {}
    s1m_trend  = (s1m or {}).get("trend") or {}
    s5m_trend  = (s5m or {}).get("trend") or {}
    micro_bos  = s1s_bos.get("micro_bos")
    choch_conf = s1s_trend.get("choch_confirmed")
    msb_val    = s1s_trend.get("msb")
    trend_1s   = s1s_trend.get("direction", "unknown")
    trend_1m   = s1m_trend.get("direction", "unknown")
    trend_5m   = s5m_trend.get("direction", "unknown")

    initiative_rec = det_recs.get("initiative_flow") or {}
    initiative_lbl = initiative_rec.get("label", "none")
    initiative_dir = initiative_rec.get("direction")

    def _make_setup(direction: str, stype: str, tc: dict) -> dict | None:
        if not regime.get("trade_allowed", True) or entry_timing == "extended":
            return None
        sid = f"{ts}_{direction}_{stype}"
        if sid in state.emitted_ids:
            return None
        if len(state.open_setups) >= MAX_OPEN_SETUPS:
            return None

        # Cooldown check
        if stype == "normal":
            cooldown = NORMAL_COOLDOWN_MS
            last_ts = (state.last_normal_long_ts if direction == "long"
                       else state.last_normal_short_ts)
        else:
            cooldown = FLASH_COOLDOWN_MS
            last_ts = (state.last_flash_long_ts if direction == "long"
                       else state.last_flash_short_ts)
        if ts - last_ts < cooldown:
            return None

        # Prices
        if direction == "long":
            sl    = entry_price - atr * ATR_MULTIPLIER_SL
            tp1   = entry_price + atr * ATR_MULTIPLIER_TP1
            tp2   = entry_price + atr * ATR_MULTIPLIER_TP2
            tp3   = entry_price + atr * ATR_MULTIPLIER_TP3
        else:
            sl    = entry_price + atr * ATR_MULTIPLIER_SL
            tp1   = entry_price - atr * ATR_MULTIPLIER_TP1
            tp2   = entry_price - atr * ATR_MULTIPLIER_TP2
            tp3   = entry_price - atr * ATR_MULTIPLIER_TP3

        # SL refinement
        _, _, active_obs = _check_active_ob_fvg(s1s, direction)
        sl = _refine_sl(sl, direction, active_obs, atr)

        ob_count  = len((s1s or {}).get("order_blocks") or [])
        fvg_count = len((s1s or {}).get("fvg") or [])
        active_ob_n, active_fvg_n, _ = _check_active_ob_fvg(s1s, direction)
        if direction == "short" and initiative_dir == "sell_initiative":
            pattern_key = "initiative_flow_sell"
        elif direction == "long" and initiative_dir == "buy_initiative":
            pattern_key = "initiative_flow_buy"
        else:
            pattern_key = f"{stype}_{direction}"
        base_score = ss if direction == "short" else ls
        base_tier = _get_tier(base_score)
        score_boost, downgrade, calibration_wr = _calibration_adjustment(
            pattern_key, regime, base_tier,
        )
        calibrated_score = base_score + score_boost
        calibrated_tier = _get_tier(calibrated_score)
        if downgrade:
            calibrated_tier = _lower_tier(calibrated_tier)
        calibrated_long = calibrated_score if direction == "long" else ls
        calibrated_short = calibrated_score if direction == "short" else ss
        setup_breakdown = dict(ev.get("score_breakdown", {}))
        setup_breakdown["calibration"] = score_boost

        return {
            "engine":          "setup_generator",
            "setup_id":        sid,
            "symbol":          SYMBOL,
            "setup_type":      stype,
            "direction":       direction,
            "window_start_ts": ts,
            "window_end_ts":   wte,
            "entry": {
                "price":            round(entry_price, 4),
                "triggered_at_ts":  ts,
                "timeframe_context": "1S",
            },
            "sl":  {"price": round(sl,  4), "atr_multiplier": ATR_MULTIPLIER_SL},
            "tp1": {"price": round(tp1, 4), "rr": 1.0},
            "tp2": {"price": round(tp2, 4), "rr": 2.0},
            "tp3": {"price": round(tp3, 4), "rr": 3.0},
            "atr_used": round(atr, 4),
            "trigger_conditions": tc,
            "direction_score": round(calibrated_score, 4),
            "quality_tier": calibrated_tier,
            "pattern_key": pattern_key,
            "calibration": {
                "observed_wr": calibration_wr,
                "score_boost": score_boost,
                "tier_downgraded": downgrade,
            },
            "regime_context": {
                "trend_regime": regime.get("trend_regime"),
                "volatility_class": regime.get("volatility_class"),
                "session": regime.get("session"),
                "trade_allowed": regime.get("trade_allowed", True),
                "compatible_setups": regime.get("compatible_setups", []),
            },
            "entry_timing": entry_timing,
            "scores": {
                "long_score":  round(calibrated_long, 4),
                "short_score": round(calibrated_short, 4),
                "score_gap":   round(abs(calibrated_long - calibrated_short), 4),
            },
            "score_breakdown": setup_breakdown,
            "context": {
                "gate_grade":      gate_grade,
                "micro_bos":       micro_bos,
                "macro_bos":       (s1m or {}).get("bos", {}).get("macro_bos"),
                "trend_1s":        trend_1s,
                "trend_1m":        trend_1m,
                "trend_5m":        trend_5m,
                "choch_confirmed": choch_conf,
                "msb":             msb_val,
                "active_ob_count": active_ob_n,
                "active_fvg_count": active_fvg_n,
            },
            "market_depth": {
                "whale_pressure": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("whale_pressure")),
                "ob_imbalance": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("ob_pressure")),
                "cascade_risk": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("cascade_risk")),
                "bid_pct": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("bid_pct")),
                "nearest_liq_long": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("nearest_liq_long")),
                "nearest_liq_short": (ev.get("evidence_components", {}).get(
                    "liquidation_context", {}).get("nearest_liq_short")),
            },
            "status": "open",
        }

    def _emit(setup: dict) -> None:
        errs = _validate_setup(setup)
        if errs:
            print(f"[EV] SETUP VALIDATION ERROR ts={ts}: {errs}", flush=True)
            return
        _write_jsonl(setups_fh, setup)
        state.emitted_ids.add(setup["setup_id"])
        state.open_setups.append(setup)
        d = setup["direction"]
        st = setup["setup_type"]
        if st == "normal":
            if d == "long":  state.last_normal_long_ts  = ts
            else:            state.last_normal_short_ts = ts
        else:
            if d == "long":  state.last_flash_long_ts   = ts
            else:            state.last_flash_short_ts  = ts
        # Terminal output
        _print_setup(setup, ev)

    # ── Try FLASH LONG ────────────────────────────────────────────────────────────
    flash_active_ob_n, flash_active_fvg_n, _ = _check_active_ob_fvg(s1s, "long")
    fl_tc = {
        "F1_score_high":       ls >= MIN_LONG_SCORE_FLASH,
        "F2_gate_A":           gate_grade == "A",
        "F3_micro_bos_bullish": micro_bos == "bullish",
        "F4_initiative_strong": (initiative_lbl == "initiative_strong" and
                                 initiative_dir == "buy_initiative"),
        "F5_ob_or_fvg":        (flash_active_ob_n + flash_active_fvg_n) > 0,
    }
    if all(fl_tc.values()):
        setup = _make_setup("long", "flash", fl_tc)
        if setup:
            _emit(setup)
            return

    # ── Try FLASH SHORT ───────────────────────────────────────────────────────────
    flash_s_ob_n, flash_s_fvg_n, _ = _check_active_ob_fvg(s1s, "short")
    fs_tc = {
        "F1_score_high":        ss >= MIN_LONG_SCORE_FLASH,
        "F2_gate_A":            gate_grade == "A",
        "F3_micro_bos_bearish": micro_bos == "bearish",
        "F4_initiative_strong": (initiative_lbl == "initiative_strong" and
                                 initiative_dir == "sell_initiative"),
        "F5_ob_or_fvg":         (flash_s_ob_n + flash_s_fvg_n) > 0,
    }
    if all(fs_tc.values()):
        setup = _make_setup("short", "flash", fs_tc)
        if setup:
            _emit(setup)
            return

    # ── Try NORMAL LONG ───────────────────────────────────────────────────────────
    nl_ob_n, nl_fvg_n, _ = _check_active_ob_fvg(s1s, "long")
    S6_long = (nl_ob_n + nl_fvg_n) > 0
    S7_long = not (trend_1m == "downtrend" and trend_5m == "downtrend")

    nl_tc = {
        "S1_dominant_side": dominant == "long",
        "S2_min_score":     ls >= MIN_LONG_SCORE_NORMAL,
        "S3_gate_grade":    True,
        "S4_1s_trend":      trend_1s in ("uptrend", "ranging"),
        "S5_bos_or_1m_trend": (micro_bos == "bullish" or trend_1m == "uptrend" or trend_1s != "downtrend"),
        "S6_ob_or_fvg":     S6_long,
        "S7_not_counter_trend": S7_long,
    }
    if all(nl_tc.values()):
        setup = _make_setup("long", "normal", nl_tc)
        if setup:
            _emit(setup)
            return

    # ── Try NORMAL SHORT ──────────────────────────────────────────────────────────
    ns_ob_n, ns_fvg_n, _ = _check_active_ob_fvg(s1s, "short")
    S6_short = (ns_ob_n + ns_fvg_n) > 0
    S7_short = not (trend_1m == "uptrend" and trend_5m == "uptrend")

    s1s_bos2 = (s1s or {}).get("bos") or {}
    micro_bos_s = s1s_bos2.get("micro_bos")

    ns_tc = {
        "S1_dominant_side":     dominant == "short",
        "S2_min_score":         ss >= MIN_LONG_SCORE_NORMAL,
        "S3_gate_grade":    True,
        "S4_1s_trend":          trend_1s in ("downtrend", "ranging"),
        "S5_bos_or_1m_trend":   (micro_bos_s == "bearish" or trend_1m == "downtrend" or trend_1s != "uptrend"),
        "S6_ob_or_fvg":         S6_short,
        "S7_not_counter_trend": S7_short,
    }
    if all(ns_tc.values()):
        setup = _make_setup("short", "normal", ns_tc)
        if setup:
            _emit(setup)

def _print_setup(setup: dict, ev: dict) -> None:
    d   = setup["direction"].upper()
    st  = setup["setup_type"].upper().ljust(6)
    ts  = setup["window_start_ts"] // 1000
    ep  = setup["entry"]["price"]
    sl  = setup["sl"]["price"]
    tp1 = setup["tp1"]["price"]
    tp2 = setup["tp2"]["price"]
    ls  = ev["long_score"]
    ss  = ev["short_score"]
    g   = (setup.get("context") or {}).get("gate_grade", "?")
    bos = (setup.get("context") or {}).get("micro_bos", "?")
    print(
        f"[SETUP {d.ljust(5)} {st}] ts={ts} "
        f"entry={ep:.1f} sl={sl:.1f} tp1={tp1:.1f} tp2={tp2:.1f} "
        f"score={ls:.1f}/{ss:.1f} gate={g} bos={bos}",
        flush=True
    )
    if FULL_PRINT:
        print(json.dumps(setup, indent=2), flush=True)

# ── Batch mode ─────────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[EV] Batch mode — loading all input files", flush=True)

    primary_recs = _read_last_n_lines(PRIMARY_FILE, 200)
    gate_idx     = _build_exact_index(_read_last_n_lines(GATE_FILE, 200))
    s1s_idx      = _build_exact_index(_read_last_n_lines(STRUCT_1S_FILE, 200))
    s1m_idx      = _build_exact_index(_read_last_n_lines(STRUCT_1M_FILE, 200))
    s5m_idx      = _build_exact_index(_read_last_n_lines(STRUCT_5M_FILE, 200))
    det_idxs     = {d: _build_exact_index(_read_last_n_lines(p, 200))
                    for d, p in DETECTOR_FILES.items()}
    regime_idx  = {
        int(rec.get("ts", 0)): rec
        for rec in _read_last_n_lines(REGIME_FILE, 200)
        if rec.get("ts") is not None
    }
    liq_idx = _build_exact_index(_read_last_n_lines(LIQ_CLUSTER_FILE, 200))
    wall_idx = _build_exact_index(_read_last_n_lines(ORDERBOOK_WALL_FILE, 200))
    whale_summary_idx = _build_exact_index(_read_last_n_lines(WHALE_SUMMARY_FILE, 100))
    orderbook_stats_idx = _build_exact_index(_read_last_n_lines(ORDERBOOK_STATS_FILE, 100))
    whale_order_records = _read_last_n_lines(WHALE_ORDER_FILE, 2000)

    # Baseline: latest record per timeframe
    baseline_1m: dict | None = None
    for rec in _read_last_n_lines(BASELINE_FILE, 200):
        if rec.get("timeframe") == "1M":
            baseline_1m = rec

    state  = SetupState()
    n_ev   = 0
    n_skip = 0

    with (open(EVIDENCE_FILE, "a", encoding="utf-8") as ev_fh,
          open(SETUPS_FILE,   "a", encoding="utf-8") as se_fh):

        for raw in primary_recs:
            if HALT_FILE.exists():
                print("[EV] SYSTEM_HALT — aborting batch", flush=True)
                return
            cdna = raw.get("candle_dna") or {}
            if not cdna.get("has_trade"):
                n_skip += 1
                continue

            ts  = raw.get("window_start_ts")
            if ts is None:
                continue
            ts = int(ts)

            gate    = gate_idx.get(ts)
            s1s     = _get_latest_at_or_before(s1s_idx, ts)
            s1m     = _get_latest_at_or_before(s1m_idx, ts)
            s5m     = _get_latest_at_or_before(s5m_idx, ts)
            det_rec = {d: det_idxs[d].get(ts) for d in DETECTOR_FILES}
            regime  = _get_latest_at_or_before(regime_idx, ts)
            liquidation = _get_latest_at_or_before(liq_idx, ts)
            walls = _get_latest_at_or_before(wall_idx, ts)
            whale_summary = _get_latest_at_or_before(whale_summary_idx, ts)
            orderbook_stats = _get_latest_at_or_before(orderbook_stats_idx, ts)
            spoofing_cancel_count = _count_recent_spoof_cancellations(
                whale_order_records, ts,
            )

            ev = compute_evidence(
                raw, gate, s1s, s1m, s5m, det_rec, baseline_1m,
                liquidation, walls, whale_summary, orderbook_stats,
                spoofing_cancel_count,
            )

            errs = _validate_evidence(ev)
            if errs:
                print(f"[EV] EVIDENCE VALIDATION ERROR ts={ts}: {errs}", flush=True)
                continue

            _write_jsonl(ev_fh, ev)
            n_ev += 1

            try_generate_setup(ev, gate, s1s, s1m, s5m, det_rec, baseline_1m, regime,
                               raw, state, se_fh)

    print(f"[EV] Batch done: {n_ev} evidence records written "
          f"({n_skip} no-trade bars skipped), "
          f"{len(state.emitted_ids)} setups generated", flush=True)

# ── Live mode ──────────────────────────────────────────────────────────────────────
class LiveCtx:
    """Shared state for live tasks."""
    def __init__(self):
        self.gate: dict[int, dict]  = {}
        self.s1s:  dict[int, dict]  = {}
        self.s1m:  dict[int, dict]  = {}
        self.s5m:  dict[int, dict]  = {}
        self.dets: dict[str, dict[int, dict]] = {d: {} for d in DETECTOR_FILES}
        self.regime: dict[int, dict] = {}
        self.liquidation: dict[int, dict] = {}
        self.walls: dict[int, dict] = {}
        self.whale_summary: dict[int, dict] = {}
        self.orderbook_stats: dict[int, dict] = {}
        self.whale_orders: dict[int, dict] = {}
        self.baseline_1m: dict | None = None
        self.setup_state = SetupState()

def _inode(path: Path) -> int | None:
    """rotate_data.sh gibi araçlar dosyayı `tail | mv` ile değiştirdiğinde
    inode değişir. Açık bir file handle eski (silinmiş) inode'a bağlı kalır
    ve hiçbir zaman yeni veri görmez — sessizce donar. Bu yüzden periyodik
    olarak inode karşılaştırması yapıp gerekirse dosyayı yeniden açıyoruz."""
    try:
        return os.stat(path).st_ino
    except OSError:
        return None

async def _tail_secondary_file(
    path: Path, cache: dict[int, dict],
    label: str, ctx: LiveCtx = None, is_baseline: bool = False
) -> None:
    """Tail a secondary file and update cache or baseline.

    Dosya rotate edilirse (inode değişirse) handle'ı kapatıp yeniden açar,
    böylece dosya sessizce takılı kalmaz."""
    while not path.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    f = open(path, "r", encoding="utf-8")
    inode = os.fstat(f.fileno()).st_ino
    try:
        # Warm-up backlog'u (dosyadaki mevcut tüm satırlar) ana thread'i bloke
        # etmeden thread pool'da oku — büyük dosyalarda (örn. historical_baseline_dna,
        # 90MB+) event loop'u uzun süre dondurmasın.
        loop = asyncio.get_event_loop()
        _raw = await loop.run_in_executor(
            None, lambda: subprocess.getoutput(f"tail -{LIVE_CACHE_MAX} {path}"),
        )
        backlog = [l + "\n" for l in _raw.splitlines()]
        for line in backlog:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if is_baseline:
                    if rec.get("timeframe") == "1M" and ctx:
                        ctx.baseline_1m = rec
                else:
                    ts = rec.get("window_start_ts", rec.get("ts"))
                    if ts is not None:
                        _cache_put(cache, int(ts), rec)
            except (json.JSONDecodeError, Exception):
                pass
        f.seek(0, 2)

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                cur_inode = _inode(path)
                if cur_inode is not None and cur_inode != inode:
                    print(f"[EV] {label}: {path.name} rotate edildi (inode değişti), yeniden açılıyor", flush=True)
                    f.close()
                    f = open(path, "r", encoding="utf-8")
                    inode = os.fstat(f.fileno()).st_ino
                    continue
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if is_baseline:
                    if rec.get("timeframe") == "1M" and ctx:
                        ctx.baseline_1m = rec
                else:
                    ts = rec.get("window_start_ts", rec.get("ts"))
                    if ts is not None:
                        _cache_put(cache, int(ts), rec)
            except (json.JSONDecodeError, Exception):
                pass
    finally:
        f.close()

async def _primary_task(ctx: LiveCtx) -> None:
    """Process primary file and write evidence + setups."""
    while not PRIMARY_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    # Warm-up: skip existing records (process but don't write)
    # _read_last_n_jsonl tüm dosyayı satır satır tarar — thread pool'da çalıştır
    # ki event loop'u bloke etmesin.
    loop = asyncio.get_event_loop()
    primary_existing = await loop.run_in_executor(None, _read_last_n_jsonl, PRIMARY_FILE, 300)
    print(f"[EV] Warm-up: {len(primary_existing)} existing primary records", flush=True)

    ev_fh = open(EVIDENCE_FILE, "a", encoding="utf-8")
    se_fh = open(SETUPS_FILE,   "a", encoding="utf-8")
    pf    = open(PRIMARY_FILE,  "r", encoding="utf-8")
    pf_inode = os.fstat(pf.fileno()).st_ino

    # Seek to end
    pf.seek(0, 2)

    try:
        while True:
            if HALT_FILE.exists():
                print("[EV] SYSTEM_HALT — stopping", flush=True)
                return

            line = pf.readline()
            if not line:
                # rotate_data.sh `tail | mv` ile dosyayı değiştirebilir; bu durumda
                # elimizdeki fd eski (silinmiş) inode'a saplanır ve hiçbir zaman yeni
                # veri görmez — sessizce donar. inode değiştiyse yeniden aç.
                cur_inode = _inode(PRIMARY_FILE)
                if cur_inode is not None and cur_inode != pf_inode:
                    print("[EV] PRIMARY_FILE rotate edildi (inode değişti), yeniden açılıyor", flush=True)
                    pf.close()
                    pf = open(PRIMARY_FILE, "r", encoding="utf-8")
                    pf_inode = os.fstat(pf.fileno()).st_ino
                    continue
                # ev_fh/se_fh de aynı şekilde rotate edilebilir (append mod ile açık
                # olan handle'lar da eski inode'a yazmaya devam eder, dışarıdan
                # görünmez olur). Aynı kontrolü onlar için de yap.
                if _inode(EVIDENCE_FILE) is not None and _inode(EVIDENCE_FILE) != os.fstat(ev_fh.fileno()).st_ino:
                    print("[EV] EVIDENCE_FILE rotate edildi, yeniden açılıyor", flush=True)
                    ev_fh.close()
                    ev_fh = open(EVIDENCE_FILE, "a", encoding="utf-8")
                if _inode(SETUPS_FILE) is not None and _inode(SETUPS_FILE) != os.fstat(se_fh.fileno()).st_ino:
                    print("[EV] SETUPS_FILE rotate edildi, yeniden açılıyor", flush=True)
                    se_fh.close()
                    se_fh = open(SETUPS_FILE, "a", encoding="utf-8")
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue

            cdna = raw.get("candle_dna") or {}
            if not cdna.get("has_trade"):
                continue

            ts = raw.get("window_start_ts")
            if ts is None:
                continue
            ts = int(ts)

            gate    = ctx.gate.get(ts)
            s1s     = _get_latest_at_or_before(ctx.s1s, ts)
            s1m     = _get_latest_at_or_before(ctx.s1m, ts)
            s5m     = _get_latest_at_or_before(ctx.s5m, ts)
            det_rec = {d: ctx.dets[d].get(ts) for d in DETECTOR_FILES}
            regime  = _get_latest_at_or_before(ctx.regime, ts)
            liquidation = _get_latest_at_or_before(ctx.liquidation, ts)
            walls = _get_latest_at_or_before(ctx.walls, ts)
            whale_summary = _get_latest_at_or_before(ctx.whale_summary, ts)
            orderbook_stats = _get_latest_at_or_before(ctx.orderbook_stats, ts)
            spoofing_cancel_count = _count_recent_spoof_cancellations(
                ctx.whale_orders.values(), ts,
            )

            ev = compute_evidence(
                raw, gate, s1s, s1m, s5m, det_rec, ctx.baseline_1m,
                liquidation, walls, whale_summary, orderbook_stats,
                spoofing_cancel_count,
            )

            errs = _validate_evidence(ev)
            if errs:
                print(f"[EV] VALIDATION ERROR ts={ts}: {errs}", flush=True)
                continue

            _write_jsonl(ev_fh, ev)
            try_generate_setup(ev, gate, s1s, s1m, s5m, det_rec, ctx.baseline_1m, regime,
                               raw, ctx.setup_state, se_fh)
    finally:
        pf.close()
        ev_fh.close()
        se_fh.close()

async def run_live() -> None:
    ctx = LiveCtx()
    tasks = [
        asyncio.create_task(_primary_task(ctx), name="ev-primary"),
        asyncio.create_task(_tail_secondary_file(GATE_FILE,     ctx.gate, "gate"), name="ev-gate"),
        asyncio.create_task(_tail_secondary_file(STRUCT_1S_FILE, ctx.s1s, "s1s"), name="ev-s1s"),
        asyncio.create_task(_tail_secondary_file(STRUCT_1M_FILE, ctx.s1m, "s1m"), name="ev-s1m"),
        asyncio.create_task(_tail_secondary_file(STRUCT_5M_FILE, ctx.s5m, "s5m"), name="ev-s5m"),
        asyncio.create_task(_tail_secondary_file(REGIME_FILE, ctx.regime, "regime"), name="ev-regime"),
        asyncio.create_task(_tail_secondary_file(LIQ_CLUSTER_FILE, ctx.liquidation, "liquidation"), name="ev-liquidation"),
        asyncio.create_task(_tail_secondary_file(ORDERBOOK_WALL_FILE, ctx.walls, "walls"), name="ev-walls"),
        asyncio.create_task(_tail_secondary_file(WHALE_SUMMARY_FILE, ctx.whale_summary, "whale-summary"), name="ev-whale-summary"),
        asyncio.create_task(_tail_secondary_file(ORDERBOOK_STATS_FILE, ctx.orderbook_stats, "orderbook-stats"), name="ev-orderbook-stats"),
        asyncio.create_task(_tail_secondary_file(WHALE_ORDER_FILE, ctx.whale_orders, "whale-orders"), name="ev-whale-orders"),
        asyncio.create_task(_tail_secondary_file(BASELINE_FILE,  {},      "bl",
                                                 ctx=ctx, is_baseline=True),       name="ev-bl"),
    ]
    for det_name, path in DETECTOR_FILES.items():
        tasks.append(asyncio.create_task(
            _tail_secondary_file(path, ctx.dets[det_name], det_name),
            name=f"ev-{det_name}"
        ))
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("[EV] Tasks cancelled", flush=True)
        for t in tasks:
            if not t.done():
                t.cancel()
        raise
    except Exception as e:
        # Hiçbir exception sessizce yutulmasın — logla, traceback'i yazdır,
        # diğer alt task'ları da iptal et ve yeniden fırlat (supervisor restart edebilsin).
        print(f"[EV] FATAL: {e}", flush=True)
        import traceback
        traceback.print_exc()
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

# ── Entry point ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Evidence Engine — Layer 7")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[EV] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        return

    if args.mode == "batch":
        run_batch()
    else:
        print("[EV] Starting live mode", flush=True)
        asyncio.run(run_live())

if __name__ == "__main__":
    main()
