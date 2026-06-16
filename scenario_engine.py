"""
NurtacCoreEngineClaude — Layer-9: Scenario Engine

Primary trigger: data/combined_1s_dna_btcusdt.jsonl
Reads:  all detector/structure/profile/gate/baseline JSONL files
Writes: data/scenarios.jsonl
        data/scenario_memory.jsonl

Rules:
  - No Binance API/WebSocket calls
  - Only reads existing JSONL files
  - No real orders — scenario detection only
  - Never crash, never write invalid records
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.05

SCENARIOS_FILE = DATA_DIR / "scenarios.jsonl"
MEMORY_FILE    = DATA_DIR / "scenario_memory.jsonl"
MAX_MEMORY     = 100

PRIMARY_FILE   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
STRUCT_1S_FILE = DATA_DIR / "structure_1s.jsonl"
STRUCT_1M_FILE = DATA_DIR / "structure_1m.jsonl"
STRUCT_5M_FILE = DATA_DIR / "structure_5m.jsonl"
VOL_1M_FILE    = DATA_DIR / "volume_profile_1m.jsonl"
VOL_SES_FILE   = DATA_DIR / "volume_profile_session.jsonl"
GATE_FILE      = DATA_DIR / "decision_gate_output.jsonl"
BASELINE_FILE  = DATA_DIR / "historical_baseline_dna.jsonl"
BIAS_FILE      = DATA_DIR / "bias_context.jsonl"

DETECTOR_FILES = {
    "absorption":      DATA_DIR / "labels_absorption.jsonl",
    "sweep":           DATA_DIR / "labels_sweep.jsonl",
    "exhaustion":      DATA_DIR / "labels_exhaustion.jsonl",
    "initiative_flow": DATA_DIR / "labels_initiative_flow.jsonl",
    "trapped_trader":  DATA_DIR / "labels_trapped_trader.jsonl",
    "iceberg":         DATA_DIR / "labels_iceberg.jsonl",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_last_line(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        last = ""
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    last = line.strip()
        if not last:
            return None
        return json.loads(last)
    except Exception:
        return None

def _read_last_n_lines(path, n: int = 200) -> list[dict]:
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
                    pass
    except OSError:
        pass
    return records

def _read_last_n_jsonl(path: Path, maxlen: int) -> list[dict]:
    """Read only last N lines from JSONL file (memory-efficient warm-up)."""
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            last_records = deque(maxlen=maxlen)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last_records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            records = list(last_records)
    except OSError:
        pass
    return records

def _build_index(records: list[dict]) -> dict[int, dict]:
    idx: dict[int, dict] = {}
    for rec in records:
        ts = rec.get("window_start_ts") or rec.get("ts")
        if ts is not None:
            idx[int(ts)] = rec
    return idx

def _latest_at_or_before(idx: dict[int, dict], ts: int) -> dict | None:
    candidates = [k for k in idx if k <= ts]
    if not candidates:
        return None
    return idx[max(candidates)]

def _write_jsonl(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

def get_atr(baseline: dict | None) -> float:
    if baseline:
        val = _sf((baseline.get("atr") or {}).get("atr"), 0.0)
        if val > 0:
            return val
    return 1.0

# ── Scenario definitions ──────────────────────────────────────────────────────

def _score_status(score: int, confirmed_thresh: int, developing_thresh: int) -> str:
    if score >= confirmed_thresh:
        return "confirmed"
    if score >= developing_thresh:
        return "developing"
    return "none"

def _s1_long_trap(primary: dict, s1s: dict | None, det: dict[str, dict | None],
                  vp1m: dict | None) -> dict:
    cdna   = primary.get("candle_dna") or {}
    bos    = (s1s or {}).get("bos") or {}
    sweep  = det.get("sweep") or {}
    absrp  = det.get("absorption") or {}
    loc    = (vp1m or {}).get("location") or {}

    c1 = bos.get("micro_bos") == "bullish"
    c2 = (sweep.get("label") in ("sweep_strong", "sweep_candidate") and
          sweep.get("direction") == "upward_sweep")
    c3 = absrp.get("direction") == "buy_absorbed"
    c4 = _sf(cdna.get("delta"), 0.0) < 0
    c5 = loc.get("position") in ("above_value", "at_vah")

    score = (1 if c1 else 0) + (2 if c2 else 0) + (2 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    return {
        "scenario_id":    "S1",
        "scenario_name":  "LONG_TRAP",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      "bearish" if status != "none" else None,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "above_value or at_vah — long trap zone",
            "aggression":   "buyers (trapped)",
            "absorption":   "sellers absorbing buy pressure",
            "exhaustion":   "buyers exhausted above value",
            "trap":         "long traders trapped above swing high",
            "acceptance":   None,
            "continuation": "bearish continuation likely",
            "invalidation": "new HH confirmed and held above VAH",
            "target":       "VAL or previous swing low",
        },
    }

def _s2_short_trap(primary: dict, s1s: dict | None, det: dict[str, dict | None],
                   vp1m: dict | None) -> dict:
    cdna   = primary.get("candle_dna") or {}
    bos    = (s1s or {}).get("bos") or {}
    sweep  = det.get("sweep") or {}
    absrp  = det.get("absorption") or {}
    loc    = (vp1m or {}).get("location") or {}

    c1 = bos.get("micro_bos") == "bearish"
    c2 = (sweep.get("label") in ("sweep_strong", "sweep_candidate") and
          sweep.get("direction") == "downward_sweep")
    c3 = absrp.get("direction") == "sell_absorbed"
    c4 = _sf(cdna.get("delta"), 0.0) > 0
    c5 = loc.get("position") in ("below_value", "at_val")

    score = (1 if c1 else 0) + (2 if c2 else 0) + (2 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    return {
        "scenario_id":    "S2",
        "scenario_name":  "SHORT_TRAP",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      "bullish" if status != "none" else None,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "below_value or at_val — short trap zone",
            "aggression":   "sellers (trapped)",
            "absorption":   "buyers absorbing sell pressure",
            "exhaustion":   None,
            "trap":         "short traders trapped below swing low",
            "acceptance":   None,
            "continuation": "bullish continuation likely",
            "invalidation": "new LL confirmed and held below VAL",
            "target":       "VAH or previous swing high",
        },
    }

def _s3_institutional_accumulation(primary: dict, det: dict[str, dict | None],
                                   vp1m: dict | None, baseline: dict | None) -> dict:
    cdna      = primary.get("candle_dna") or {}
    ms        = (vp1m or {}).get("market_state") or {}
    prof      = (vp1m or {}).get("profile") or {}
    iceberg   = det.get("iceberg") or {}
    total_vol = _sf(cdna.get("total_volume"), 0.0)
    delta_abs = abs(_sf(cdna.get("delta"), 0.0))

    bl_mean_vol = 0.0
    if baseline:
        metrics = baseline.get("metrics") or {}
        tv_metrics = metrics.get("total_volume") or {}
        bl_mean_vol = _sf(tv_metrics.get("mean"), 0.0)

    c1 = ms.get("balance_zone") is True
    c2 = iceberg.get("label") in ("iceberg_strong", "iceberg_candidate")
    c3 = (bl_mean_vol > 0 and total_vol > bl_mean_vol)
    c4 = (total_vol > 0 and delta_abs < total_vol * 0.20)
    c5 = prof.get("profile_shape") in ("normal_distribution", "thin_profile")

    score = (2 if c1 else 0) + (2 if c2 else 0) + (1 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    return {
        "scenario_id":    "S3",
        "scenario_name":  "INSTITUTIONAL_ACCUMULATION",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      "neutral" if status != "none" else None,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "inside_value — balance zone accumulation",
            "aggression":   "institutions (hidden)",
            "absorption":   "both sides — controlled range",
            "exhaustion":   None,
            "trap":         None,
            "acceptance":   None,
            "continuation": "breakout imminent — direction unknown until confirmed",
            "invalidation": "sustained move outside range without return",
            "target":       "range high (if bullish) or range low (if bearish)",
        },
    }

def _s4_failed_auction(primary: dict, vp1m: dict | None) -> dict:
    cdna = primary.get("candle_dna") or {}
    ms   = (vp1m or {}).get("market_state") or {}
    fa   = ms.get("failed_auction") or {}

    c1 = fa.get("detected") is True
    c2 = bool(fa.get("direction"))
    c3 = cdna.get("has_trade") is True

    score = (3 if c1 else 0) + (2 if c2 else 0) + (1 if c3 else 0)
    status = _score_status(score, 5, 3)

    fa_dir = fa.get("direction", "")
    if status != "none":
        direction = "bearish" if fa_dir == "failed_auction_above" else \
                    "bullish" if fa_dir == "failed_auction_below" else "neutral"
    else:
        direction = None

    return {
        "scenario_id":    "S4",
        "scenario_name":  "FAILED_AUCTION",
        "status":         status,
        "score":          score,
        "max_score":      6,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": False, "C5": False},
        "market_questions": {
            "location":     "returned to value after failed auction",
            "aggression":   "auction participants rejected",
            "absorption":   None,
            "exhaustion":   None,
            "trap":         None,
            "acceptance":   None,
            "continuation": "return to POC then opposite value boundary",
            "invalidation": "price accepts outside value (3+ bars)",
            "target":       "opposite value area boundary",
        },
    }

def _s5_breakout_continuation(s1m: dict | None, det: dict[str, dict | None],
                               gate: dict | None, vp1m: dict | None,
                               bias: dict | None) -> dict:
    s1m_bos   = (s1m or {}).get("bos") or {}
    macro_bos = s1m_bos.get("macro_bos")
    ms        = (vp1m or {}).get("market_state") or {}
    imb_dir   = ms.get("imbalance_direction")
    gate_grade = (gate or {}).get("setup_grade")
    gate_dir   = (gate or {}).get("dominant_direction")
    dom_bias   = (bias or {}).get("dominant_bias", "neutral")
    flow       = det.get("initiative_flow") or {}
    flow_lbl   = flow.get("label", "none")
    flow_dir   = flow.get("direction")

    def _dir_match(val: str | None, target: str) -> bool:
        if target == "bullish":
            return val in ("bullish", "buy_initiative", "long")
        if target == "bearish":
            return val in ("bearish", "sell_initiative", "short")
        return False

    c1 = macro_bos in ("bullish", "bearish")
    c2 = (flow_lbl in ("initiative_strong", "initiative_candidate") and
          c1 and _dir_match(flow_dir, macro_bos))
    c3 = (gate_grade in ("A", "B") and c1 and _dir_match(gate_dir, macro_bos))
    c4 = (ms.get("imbalance_zone") is True and
          c1 and _dir_match(imb_dir, macro_bos))
    c5 = c1 and _dir_match(dom_bias, macro_bos)

    score = (2 if c1 else 0) + (2 if c2 else 0) + (1 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    direction = macro_bos if (status != "none" and macro_bos in ("bullish", "bearish")) else None

    return {
        "scenario_id":    "S5",
        "scenario_name":  "BREAKOUT_CONTINUATION",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "outside value — breakout confirmed",
            "aggression":   "initiative buyers/sellers",
            "absorption":   None,
            "exhaustion":   None,
            "trap":         None,
            "acceptance":   None,
            "continuation": "high probability — all aligned",
            "invalidation": "price returns inside value area",
            "target":       "next HVN or swing high/low",
        },
    }

def _s6_exhaustion_reversal(primary: dict, s1s: dict | None,
                             det: dict[str, dict | None], vp1m: dict | None,
                             trade_count_seq: list[int]) -> dict:
    cdna    = primary.get("candle_dna") or {}
    trend   = (s1s or {}).get("trend") or {}
    exhaust = det.get("exhaustion") or {}
    flow    = det.get("initiative_flow") or {}
    bh      = (vp1m or {}).get("bias_hint") or {}
    shape   = bh.get("shape_bias", "neutral")

    exh_lbl = exhaust.get("label", "none")
    exh_dir = exhaust.get("direction")
    flow_lbl = flow.get("label", "none")
    flow_dir = flow.get("direction")
    trend_str = trend.get("strength")
    trend_dir = trend.get("direction", "unknown")

    c1 = exh_lbl in ("exhaustion_strong", "exhaustion_candidate")
    c2 = trend_str in ("strong", "weak")
    c3 = (flow_lbl != "none" and
          ((exh_dir == "buy_exhaustion" and flow_dir == "sell_initiative") or
           (exh_dir == "sell_exhaustion" and flow_dir == "buy_initiative")))
    c4 = ((trend_dir == "uptrend" and shape == "b_shape") or
          (trend_dir == "downtrend" and shape == "p_shape") or
          (trend_dir in ("uptrend", "downtrend") and shape not in ("neutral", None) and
           shape != ("p_shape" if trend_dir == "uptrend" else "b_shape")))
    c5 = (len(trade_count_seq) >= 3 and
          trade_count_seq[2] < trade_count_seq[0])

    score = (2 if c1 else 0) + (1 if c2 else 0) + (2 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    if status != "none" and exh_dir:
        direction = "bullish" if exh_dir == "sell_exhaustion" else "bearish"
    else:
        direction = None

    return {
        "scenario_id":    "S6",
        "scenario_name":  "EXHAUSTION_REVERSAL",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "trend extreme — exhaustion detected",
            "aggression":   "trend participants (exhausted)",
            "absorption":   None,
            "exhaustion":   "trend participants exhausted",
            "trap":         None,
            "acceptance":   None,
            "continuation": "reversal candidate — wait for confirmation",
            "invalidation": "new extreme in trend direction with volume",
            "target":       "previous swing / value boundary",
        },
    }

def _s7_reclaim(primary: dict, s1s: dict | None, det: dict[str, dict | None],
                vp1m: dict | None, prev_location: str | None) -> dict:
    cdna    = primary.get("candle_dna") or {}
    bos     = (s1s or {}).get("bos") or {}
    absrp   = det.get("absorption") or {}
    loc_rec = (vp1m or {}).get("location") or {}
    cur_pos = loc_rec.get("position")

    delta     = _sf(cdna.get("delta"), 0.0)
    micro_bos = bos.get("micro_bos")
    absrp_dir = absrp.get("direction")

    position_changed = (prev_location is not None and
                        cur_pos != prev_location and
                        cur_pos is not None)

    is_bullish_reclaim = (prev_location == "below_value" and cur_pos == "inside_value") or \
                         (prev_location == "inside_value" and cur_pos == "above_value")
    is_bearish_reclaim = (prev_location == "above_value" and cur_pos == "inside_value") or \
                         (prev_location == "inside_value" and cur_pos == "below_value")

    c1 = position_changed and (is_bullish_reclaim or is_bearish_reclaim)
    c2 = micro_bos in ("bullish", "bearish")
    c3 = ((is_bullish_reclaim and delta > 0) or (is_bearish_reclaim and delta < 0))
    c4 = ((is_bullish_reclaim and absrp_dir == "sell_absorbed") or
          (is_bearish_reclaim and absrp_dir == "buy_absorbed"))
    c5 = False  # requires multi-bar history — conservative default

    score = (2 if c1 else 0) + (1 if c2 else 0) + (1 if c3 else 0) + \
            (2 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    if status != "none":
        direction = "bullish" if is_bullish_reclaim else "bearish"
        rt = "vah_reclaim" if is_bullish_reclaim else "val_reclaim"
    else:
        direction = None
        rt = None

    return {
        "scenario_id":    "S7",
        "scenario_name":  "RECLAIM",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "reclaim_type":   rt,
        "market_questions": {
            "location":     f"reclaimed level — {direction or 'unknown'} reclaim",
            "aggression":   "reclaim participants",
            "absorption":   None,
            "exhaustion":   None,
            "trap":         None,
            "acceptance":   None,
            "continuation": "likely — reclaim is strong signal",
            "invalidation": "lose reclaim level again",
            "target":       "next level in reclaim direction",
        },
    }

def _s8_liquidity_sweep(primary: dict, s1m: dict | None,
                        det: dict[str, dict | None]) -> dict:
    cdna    = primary.get("candle_dna") or {}
    swing   = (s1m or {}).get("swing") or {}
    sweep   = det.get("sweep") or {}
    trapped = det.get("trapped_trader") or {}
    delta   = _sf(cdna.get("delta"), 0.0)

    sweep_lbl = sweep.get("label", "none")
    sweep_dir = sweep.get("direction")
    trap_lbl  = trapped.get("label", "none")
    trap_dir  = trapped.get("direction")

    last_high = swing.get("last_swing_high") or {}
    last_low  = swing.get("last_swing_low") or {}
    has_eqh = last_high.get("type") == "EQH"
    has_eql = last_low.get("type") == "EQL"

    c1 = sweep_lbl in ("sweep_strong", "sweep_candidate")
    c2 = has_eqh or has_eql
    c3 = ((has_eqh and sweep_dir == "upward_sweep") or
          (has_eql and sweep_dir == "downward_sweep"))
    c4 = (trap_lbl != "none" and
          ((sweep_dir == "upward_sweep" and trap_dir == "long_trapped") or
           (sweep_dir == "downward_sweep" and trap_dir == "short_trapped")))
    c5 = ((sweep_dir == "upward_sweep" and delta < 0) or
          (sweep_dir == "downward_sweep" and delta > 0))

    score = (2 if c1 else 0) + (2 if c2 else 0) + (1 if c3 else 0) + \
            (1 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    if status != "none" and sweep_dir:
        direction = "bearish" if sweep_dir == "upward_sweep" else "bullish"
        st = "equal_high_swept" if sweep_dir == "upward_sweep" else "equal_low_swept"
    else:
        direction = None
        st = None

    return {
        "scenario_id":    "S8",
        "scenario_name":  "LIQUIDITY_SWEEP",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "sweep_target":   st,
        "market_questions": {
            "location":     f"swept {'EQH' if sweep_dir == 'upward_sweep' else 'EQL'} — liquidity taken",
            "aggression":   "smart money swept retail stops",
            "absorption":   None,
            "exhaustion":   None,
            "trap":         "retail traders stopped out",
            "acceptance":   None,
            "continuation": "reversal after sweep — high probability",
            "invalidation": "sweep continues beyond equal level",
            "target":       "opposite liquidity pool",
        },
    }

def _s9_balance_breakout_anticipation(s1m: dict | None, gate: dict | None,
                                      bias: dict | None, vp1m: dict | None,
                                      det: dict[str, dict | None]) -> dict:
    ms        = (vp1m or {}).get("market_state") or {}
    trend     = (s1m or {}).get("trend") or {}
    iceberg   = det.get("iceberg") or {}
    gate_grade = (gate or {}).get("setup_grade")
    dom_bias   = (bias or {}).get("dominant_bias", "neutral")
    choch_ph   = trend.get("choch_phase")

    c1 = ms.get("balance_zone") is True
    c2 = gate_grade in ("A", "B", "C")
    c3 = dom_bias != "neutral"
    c4 = choch_ph in ("phase1_bullish", "phase1_bearish")
    c5 = iceberg.get("label") not in (None, "none")

    score = (2 if c1 else 0) + (1 if c2 else 0) + (1 if c3 else 0) + \
            (2 if c4 else 0) + (1 if c5 else 0)
    status = _score_status(score, 5, 3)

    if status != "none" and dom_bias in ("long", "short"):
        direction = "bullish" if dom_bias == "long" else "bearish"
    else:
        direction = None

    return {
        "scenario_id":    "S9",
        "scenario_name":  "BALANCE_BREAKOUT_ANTICIPATION",
        "status":         status,
        "score":          score,
        "max_score":      7,
        "direction":      direction,
        "conditions_met": {"C1": c1, "C2": c2, "C3": c3, "C4": c4, "C5": c5},
        "market_questions": {
            "location":     "inside balance — pre-breakout phase",
            "aggression":   "not yet — accumulation phase",
            "absorption":   None,
            "exhaustion":   None,
            "trap":         None,
            "acceptance":   None,
            "continuation": f"breakout expected — direction {dom_bias}",
            "invalidation": "sustained auction in opposite direction",
            "target":       "range boundary + measured move",
        },
    }

# ── Validation ────────────────────────────────────────────────────────────────
def _validate_output(rec: dict) -> list[str]:
    errors: list[str] = []
    valid_dirs = {"bullish", "bearish", "neutral", None}
    valid_statuses = {"confirmed", "developing"}

    for sc in rec.get("active_scenarios", []):
        sid = sc.get("scenario_id", "?")
        if sc.get("score", 0) > sc.get("max_score", 0):
            errors.append(f"[1] {sid} score > max_score")
        if sc.get("direction") not in valid_dirs:
            errors.append(f"[2] {sid} invalid direction: {sc.get('direction')}")
        if sc.get("status") not in valid_statuses:
            errors.append(f"[3] {sid} invalid status: {sc.get('status')}")
        cm = sc.get("conditions_met") or {}
        if len(cm) != 5:
            errors.append(f"[4] {sid} conditions_met must have 5 keys")
        mq = sc.get("market_questions") or {}
        for key in ("location", "continuation", "invalidation", "target"):
            if not mq.get(key):
                errors.append(f"[6] {sid} market_questions.{key} is null/missing")

    if rec.get("dominant_direction") not in ("bullish", "bearish", "neutral"):
        errors.append(f"[7] invalid dominant_direction: {rec.get('dominant_direction')}")

    return errors

# ── Core evaluation ────────────────────────────────────────────────────────────
def evaluate_scenarios(
    primary: dict,
    s1s: dict | None,
    s1m: dict | None,
    s5m: dict | None,
    vp1m: dict | None,
    vp_ses: dict | None,
    gate: dict | None,
    det: dict[str, dict | None],
    bias: dict | None,
    baseline: dict | None,
    prev_location: str | None,
    trade_count_seq: list[int],
) -> dict:
    ts  = int(primary.get("window_start_ts", 0))
    wte = int(primary.get("window_end_ts", ts + 1000))
    cdna = primary.get("candle_dna") or {}
    close_obj = cdna.get("close") or {}
    cur_price = _sf(close_obj.get("price"), None) if isinstance(close_obj, dict) else _sf(cdna.get("close"), None)

    vp_loc   = (vp1m or {}).get("location") or {}
    vp_prof  = (vp1m or {}).get("profile") or {}
    ms       = (vp1m or {}).get("market_state") or {}
    s1s_trend = (s1s or {}).get("trend") or {}
    s1m_trend = (s1m or {}).get("trend") or {}
    bos_1s   = (s1s or {}).get("bos") or {}

    all_results = [
        _s1_long_trap(primary, s1s, det, vp1m),
        _s2_short_trap(primary, s1s, det, vp1m),
        _s3_institutional_accumulation(primary, det, vp1m, baseline),
        _s4_failed_auction(primary, vp1m),
        _s5_breakout_continuation(s1m, det, gate, vp1m, bias),
        _s6_exhaustion_reversal(primary, s1s, det, vp1m, trade_count_seq),
        _s7_reclaim(primary, s1s, det, vp1m, prev_location),
        _s8_liquidity_sweep(primary, s1m, det),
        _s9_balance_breakout_anticipation(s1m, gate, bias, vp1m, det),
    ]

    active = [r for r in all_results if r["status"] != "none"]

    confirmed = [r for r in active if r["status"] == "confirmed"]
    developing = [r for r in active if r["status"] == "developing"]

    if confirmed:
        dominant = max(confirmed, key=lambda x: x["score"])
    elif developing:
        dominant = max(developing, key=lambda x: x["score"])
    else:
        dominant = None

    if dominant:
        dom_name = dominant["scenario_name"]
        # Check conflicting confirmed scenarios
        if len(confirmed) >= 2:
            dirs = {r["direction"] for r in confirmed if r["direction"] not in (None, "neutral")}
            dom_dir = "neutral" if len(dirs) > 1 else (dominant["direction"] or "neutral")
        else:
            dom_dir = dominant["direction"] or "neutral"
    else:
        dom_name = None
        dom_dir  = "neutral"

    ctx = {
        "current_price": cur_price,
        "poc":           _sf(vp_prof.get("poc"), None) if vp_prof else None,
        "location":      vp_loc.get("position"),
        "profile_shape": vp_prof.get("profile_shape"),
        "trend_1s":      s1s_trend.get("direction"),
        "trend_1m":      s1m_trend.get("direction"),
        "gate_grade":    (gate or {}).get("setup_grade"),
        "market_bias":   (bias or {}).get("dominant_bias"),
    }

    return {
        "engine":             "scenario_engine",
        "symbol":             "BTCUSDT",
        "window_start_ts":    ts,
        "window_end_ts":      wte,
        "active_scenarios":   active,
        "dominant_scenario":  dom_name,
        "dominant_direction": dom_dir,
        "scenario_count":     len(active),
        "context_snapshot":   ctx,
    }

# ── Scenario memory ───────────────────────────────────────────────────────────
def _update_memory(active: list[dict], ts: int, entry_price: float | None,
                   mem_fh) -> None:
    for sc in active:
        if sc["status"] == "confirmed":
            rec = {
                "ts":              ts,
                "scenario_name":   sc["scenario_name"],
                "direction":       sc.get("direction"),
                "entry_price":     entry_price,
                "score":           sc["score"],
                "status_at_record": "confirmed",
            }
            mem_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    mem_fh.flush()
    try:
        os.fsync(mem_fh.fileno())
    except Exception:
        pass

def _trim_memory() -> None:
    if not MEMORY_FILE.exists():
        return
    lines = []
    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            lines = [l for l in f if l.strip()]
    except OSError:
        return
    if len(lines) <= MAX_MEMORY:
        return
    keep = lines[-MAX_MEMORY:]
    try:
        with open(MEMORY_FILE, "w", encoding="utf-8") as f:
            f.writelines(keep)
        os.fsync(f.fileno())
    except OSError:
        pass

# ── Terminal output ───────────────────────────────────────────────────────────
def _print_result(result: dict, prev_dominant: str | None) -> None:
    if FULL_PRINT:
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return

    ts = result["window_start_ts"] // 1000
    for sc in result.get("active_scenarios", []):
        name   = sc["scenario_name"]
        status = sc["status"]
        score  = sc["score"]
        maxs   = sc["max_score"]
        mq     = sc.get("market_questions") or {}
        d      = sc.get("direction", "?")

        if status == "confirmed" and name != prev_dominant:
            loc  = mq.get("location", "")
            trap = mq.get("trap", "")
            inv  = mq.get("invalidation", "")
            tgt  = mq.get("target", "")
            print(
                f"[SCENARIO CONFIRMED] ts={ts} {name} score={score}/{maxs}\n"
                f"  location={loc} trap={trap}\n"
                f"  invalidation={inv} target={tgt}",
                flush=True,
            )
        elif status == "developing":
            print(
                f"[SCENARIO DEVELOPING] ts={ts} {name} score={score}/{maxs} "
                f"direction={d}",
                flush=True,
            )

# ── Data loading for batch ────────────────────────────────────────────────────
def _load_baseline_1s(baseline_recs: list[dict]) -> dict | None:
    for r in reversed(baseline_recs):
        if r.get("timeframe") == "1S":
            return r
    return None

def _load_baseline_1m(baseline_recs: list[dict]) -> dict | None:
    for r in reversed(baseline_recs):
        if r.get("timeframe") == "1M":
            return r
    return None

# ── Batch mode ────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[SCEN] Batch mode — loading input files (warm-up limits)", flush=True)

    # Warm-up: load only last N lines per file (memory-efficient)
    primary_recs = _read_last_n_jsonl(PRIMARY_FILE, maxlen=300)
    s1s_idx      = _build_index(_read_last_n_jsonl(STRUCT_1S_FILE, maxlen=300))
    s1m_idx      = _build_index(_read_last_n_jsonl(STRUCT_1M_FILE, maxlen=100))
    s5m_idx      = _build_index(_read_last_n_jsonl(STRUCT_5M_FILE, maxlen=100))
    vp1m_idx     = _build_index(_read_last_n_jsonl(VOL_1M_FILE, maxlen=100))
    vp_ses_idx   = _build_index(_read_last_n_jsonl(VOL_SES_FILE, maxlen=100))
    gate_idx     = _build_index(_read_last_n_jsonl(GATE_FILE, maxlen=100))
    det_idxs     = {d: _build_index(_read_last_n_jsonl(p, maxlen=100)) for d, p in DETECTOR_FILES.items()}

    bl_recs    = _read_last_n_jsonl(BASELINE_FILE, maxlen=100)
    baseline_1s = _load_baseline_1s(bl_recs)
    baseline_1m = _load_baseline_1m(bl_recs)
    bias_rec   = _read_last_line(BIAS_FILE)

    prev_location: str | None = None
    trade_count_buf: list[int] = []
    prev_dominant: str | None  = None
    n_written = 0

    with (open(SCENARIOS_FILE, "a", encoding="utf-8") as sc_fh,
          open(MEMORY_FILE,    "a", encoding="utf-8") as mem_fh):

        for raw in primary_recs:
            if HALT_FILE.exists():
                print("[SCEN] SYSTEM_HALT — aborting batch", flush=True)
                return

            cdna = raw.get("candle_dna") or {}
            if not cdna.get("has_trade"):
                continue

            ts  = raw.get("window_start_ts")
            if ts is None:
                continue
            ts = int(ts)

            s1s  = s1s_idx.get(ts)
            s1m  = _latest_at_or_before(s1m_idx, ts)
            s5m  = _latest_at_or_before(s5m_idx, ts)
            vp1m = _latest_at_or_before(vp1m_idx, ts)
            vp_s = _latest_at_or_before(vp_ses_idx, ts)
            gate = gate_idx.get(ts)
            det  = {d: det_idxs[d].get(ts) for d in DETECTOR_FILES}

            tc = int(cdna.get("trade_count") or 0)
            trade_count_buf.append(tc)
            if len(trade_count_buf) > 3:
                trade_count_buf.pop(0)

            result = evaluate_scenarios(
                raw, s1s, s1m, s5m, vp1m, vp_s, gate, det,
                bias_rec, baseline_1s, prev_location, trade_count_buf,
            )

            errs = _validate_output(result)
            if errs:
                print(f"[SCEN] VALIDATION ERROR ts={ts}: {errs}", flush=True)
                continue

            _write_jsonl(sc_fh, result)
            n_written += 1

            entry_price = None
            close_obj = cdna.get("close")
            if isinstance(close_obj, dict):
                entry_price = close_obj.get("price")
            elif close_obj is not None:
                entry_price = _sf(close_obj, None)

            _update_memory(result["active_scenarios"], ts, entry_price, mem_fh)

            cur_loc = ((vp1m or {}).get("location") or {}).get("position")
            prev_location = cur_loc

            _print_result(result, prev_dominant)
            prev_dominant = result.get("dominant_scenario")

    _trim_memory()
    print(f"[SCEN] Batch done: {n_written} scenario records written", flush=True)

# ── Live mode ─────────────────────────────────────────────────────────────────
class LiveCtx:
    def __init__(self):
        self.s1s:  dict[int, dict] = {}
        self.s1m:  dict[int, dict] = {}
        self.s5m:  dict[int, dict] = {}
        self.vp1m: dict[int, dict] = {}
        self.vp_s: dict[int, dict] = {}
        self.gate: dict[int, dict] = {}
        self.dets: dict[str, dict[int, dict]] = {d: {} for d in DETECTOR_FILES}
        self.baseline_1s: dict | None = None
        self.bias:        dict | None = None
        self.prev_location: str | None = None
        self.trade_count_buf: list[int] = []
        self.prev_dominant: str | None = None

async def _tail_index(path: Path, cache: dict[int, dict], label: str) -> None:
    while not path.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(path, "r", encoding="utf-8") as f:
        # Warm-up backlog'u thread pool'da oku — büyük dosyalarda event loop'u
        # bloke etmesin (tüm diğer engine task'ları o süre boyunca donar).
        loop = asyncio.get_event_loop()
        backlog = await loop.run_in_executor(None, f.readlines)
        for line in backlog:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = rec.get("window_start_ts")
                if ts is not None:
                    cache[int(ts)] = rec
            except Exception:
                pass

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = rec.get("window_start_ts")
                if ts is not None:
                    cache[int(ts)] = rec
            except Exception:
                pass

async def _tail_baseline(ctx: LiveCtx) -> None:
    while not BASELINE_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
        # historical_baseline_dna.jsonl 90MB+ olabiliyor — thread pool'da oku.
        loop = asyncio.get_event_loop()
        backlog = await loop.run_in_executor(None, f.readlines)
        for line in backlog:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("timeframe") == "1S":
                    ctx.baseline_1s = rec
            except Exception:
                pass

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("timeframe") == "1S":
                    ctx.baseline_1s = rec
            except Exception:
                pass

async def _tail_bias(ctx: LiveCtx) -> None:
    while not BIAS_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(BIAS_FILE, "r", encoding="utf-8") as f:
        loop = asyncio.get_event_loop()
        backlog = await loop.run_in_executor(None, f.readlines)
        for line in backlog:
            line = line.strip()
            if line:
                try:
                    ctx.bias = json.loads(line)
                except Exception:
                    pass

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                ctx.bias = json.loads(line)
            except Exception:
                pass

async def _primary_task(ctx: LiveCtx) -> None:
    while not PRIMARY_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    # _read_last_n_jsonl tüm dosyayı tarar — thread pool'da çalıştır.
    loop = asyncio.get_event_loop()
    existing = await loop.run_in_executor(None, _read_last_n_jsonl, PRIMARY_FILE, 3600)
    print(f"[SCEN] Warm-up: {len(existing)} existing primary records", flush=True)

    _trim_counter = [0]

    with (open(SCENARIOS_FILE, "a", encoding="utf-8") as sc_fh,
          open(MEMORY_FILE,    "a", encoding="utf-8") as mem_fh,
          open(PRIMARY_FILE,   "r", encoding="utf-8") as pf):

        pf.seek(0, 2)

        while True:
            if HALT_FILE.exists():
                print("[SCEN] SYSTEM_HALT — stopping", flush=True)
                return

            line = pf.readline()
            if not line:
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

            s1s  = ctx.s1s.get(ts)
            s1m  = _latest_at_or_before(ctx.s1m, ts)
            s5m  = _latest_at_or_before(ctx.s5m, ts)
            vp1m = _latest_at_or_before(ctx.vp1m, ts)
            vp_s = _latest_at_or_before(ctx.vp_s, ts)
            gate = ctx.gate.get(ts)
            det  = {d: ctx.dets[d].get(ts) for d in DETECTOR_FILES}

            tc = int(cdna.get("trade_count") or 0)
            ctx.trade_count_buf.append(tc)
            if len(ctx.trade_count_buf) > 3:
                ctx.trade_count_buf.pop(0)

            result = evaluate_scenarios(
                raw, s1s, s1m, s5m, vp1m, vp_s, gate, det,
                ctx.bias, ctx.baseline_1s, ctx.prev_location, ctx.trade_count_buf,
            )

            errs = _validate_output(result)
            if errs:
                print(f"[SCEN] VALIDATION ERROR ts={ts}: {errs}", flush=True)
                continue

            _write_jsonl(sc_fh, result)

            entry_price = None
            close_obj = cdna.get("close")
            if isinstance(close_obj, dict):
                entry_price = close_obj.get("price")
            elif close_obj is not None:
                try:
                    entry_price = float(close_obj)
                except Exception:
                    pass

            _update_memory(result["active_scenarios"], ts, entry_price, mem_fh)

            cur_loc = ((vp1m or {}).get("location") or {}).get("position")
            ctx.prev_location = cur_loc

            _print_result(result, ctx.prev_dominant)
            ctx.prev_dominant = result.get("dominant_scenario")

            _trim_counter[0] += 1
            if _trim_counter[0] % 500 == 0:
                _trim_memory()

async def run_live() -> None:
    ctx = LiveCtx()
    tasks = [
        asyncio.create_task(_primary_task(ctx),                          name="sc-primary"),
        asyncio.create_task(_tail_index(STRUCT_1S_FILE, ctx.s1s, "s1s"), name="sc-s1s"),
        asyncio.create_task(_tail_index(STRUCT_1M_FILE, ctx.s1m, "s1m"), name="sc-s1m"),
        asyncio.create_task(_tail_index(STRUCT_5M_FILE, ctx.s5m, "s5m"), name="sc-s5m"),
        asyncio.create_task(_tail_index(VOL_1M_FILE,   ctx.vp1m, "vp1m"), name="sc-vp1m"),
        asyncio.create_task(_tail_index(VOL_SES_FILE,  ctx.vp_s, "vpses"), name="sc-vpses"),
        asyncio.create_task(_tail_index(GATE_FILE,     ctx.gate, "gate"), name="sc-gate"),
        asyncio.create_task(_tail_baseline(ctx),                          name="sc-bl"),
        asyncio.create_task(_tail_bias(ctx),                              name="sc-bias"),
    ]
    for det_name, path in DETECTOR_FILES.items():
        tasks.append(asyncio.create_task(
            _tail_index(path, ctx.dets[det_name], det_name),
            name=f"sc-{det_name}",
        ))
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("[SCEN] Tasks cancelled", flush=True)

# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Scenario Engine — Layer 9")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[SCEN] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        return

    if args.mode == "batch":
        run_batch()
    else:
        print("[SCEN] Starting live mode", flush=True)
        asyncio.run(run_live())

if __name__ == "__main__":
    main()
