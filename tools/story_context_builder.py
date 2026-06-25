#!/usr/bin/env python3
"""
story_context_builder.py
Builds the story_context block for historical_outcome observations.
Called by historical_outcome_engine.py at observation write time.
All data sourced from existing engine outputs — never invented.
Missing data → null (never fabricated).
"""
import json, subprocess
from pathlib import Path
from typing import Optional

DATA = Path("/root/NurtacCoreEngineClaude/data")

def _tail_jsonl(filepath: Path, n: int = 2000) -> list:
    """RAM-safe: read last n lines of a JSONL file."""
    if not filepath.exists():
        return []
    raw = subprocess.getoutput(f"tail -{n} {filepath}")
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows

def _find_latest_before(rows: list, event_ts: int, ts_field: str = "ts") -> Optional[dict]:
    """Find the most recent record where record[ts_field] <= event_ts."""
    best = None
    for r in rows:
        r_ts = r.get(ts_field) or r.get("window_start_ts") or r.get("timestamp") or 0
        if r_ts <= event_ts:
            if best is None or r_ts > (best.get(ts_field) or 0):
                best = r
    return best

def _find_signals_in_window(event_ts: int, window_ms: int = 3000) -> list:
    """Find all detector signals within ±window_ms of event_ts."""
    label_files = [
        DATA / "labels_absorption.jsonl",
        DATA / "labels_sweep.jsonl",
        DATA / "labels_exhaustion.jsonl",
        DATA / "labels_iceberg.jsonl",
        DATA / "labels_trapped_trader.jsonl",
        DATA / "labels_initiative_flow.jsonl",
    ]
    signals = []
    for f in label_files:
        rows = _tail_jsonl(f, 500)
        for r in rows:
            r_ts = r.get("window_start_ts") or r.get("ts") or 0
            if abs(r_ts - event_ts) <= window_ms:
                label = r.get("label") or r.get("event_type") or "unknown"
                if label and label != "none":
                    signals.append({
                        "signal": label,
                        "ts_offset_ms": r_ts - event_ts
                    })
    return signals

def build_story_context(event_ts: int, event_type: str, direction: str) -> dict:
    """
    Build story_context for a given event timestamp.
    Returns dict — never raises exception.
    Missing fields are null.
    """
    try:
        # --- Signals ---
        co_signals = _find_signals_in_window(event_ts)
        same_dir = [s for s in co_signals if direction in s["signal"]]
        opp = [s for s in co_signals if s not in same_dir]

        # --- Structure ---
        str_1s = _find_latest_before(_tail_jsonl(DATA / "structure_1s.jsonl", 500), event_ts)
        str_1m = _find_latest_before(_tail_jsonl(DATA / "structure_1m.jsonl", 500), event_ts)
        str_5m = _find_latest_before(_tail_jsonl(DATA / "structure_5m.jsonl", 500), event_ts)

        def get_trend(s): return (s or {}).get("trend") or (s or {}).get("trend_direction")
        def get_bos(s): return bool((s or {}).get("bos_detected") or (s or {}).get("is_bos"))
        def has_ob(s): return len((s or {}).get("order_blocks") or []) > 0
        def has_fvg(s): return len((s or {}).get("fair_value_gaps") or []) > 0

        trends = [get_trend(str_1s), get_trend(str_1m), get_trend(str_5m)]
        trends = [t for t in trends if t and t != "unknown"]

        mtf = "unknown"
        if len(set(trends)) == 1 and trends:
            mtf = "aligned"
        elif len(trends) > 1:
            mtf = "conflict" if len(set(trends)) > 1 else "aligned"

        # --- Scenario ---
        scen_rows = _tail_jsonl(DATA / "scenarios.jsonl", 200)
        scen = _find_latest_before(scen_rows, event_ts)
        
        # Find best/confirmed scenario
        active_scen = None
        if scen:
            scenarios_data = scen.get("scenarios") or {}
            for s_id, s_data in scenarios_data.items():
                if isinstance(s_data, dict) and s_data.get("status") == "confirmed":
                    active_scen = {"id": s_id, "name": s_data.get("name"), 
                                   "direction": s_data.get("direction"),
                                   "status": "confirmed",
                                   "score": s_data.get("score")}
                    break

        # --- Market Context ---
        mc_rows = _tail_jsonl(DATA / "market_context.jsonl", 100)
        mc = _find_latest_before(mc_rows, event_ts)
        bias_rows = _tail_jsonl(DATA / "bias_context.jsonl", 50) if (DATA / "bias_context.jsonl").exists() else []
        bias = _find_latest_before(bias_rows, event_ts)

        # --- Momentum (from 1S DNA) ---
        dna_rows = _tail_jsonl(DATA / "combined_1s_dna_btcusdt.jsonl", 200)
        dna = _find_latest_before(dna_rows, event_ts, ts_field="window_start_ts")

        return {
            "signal_picture": {
                "primary_signal": event_type,
                "primary_signal_direction": direction,
                "co_active_signals": [s["signal"] for s in co_signals],
                "co_active_count": len(co_signals),
                "co_active_same_direction": len(same_dir),
                "co_active_opposing": len(opp)
            },
            "structure_picture": {
                "trend_1s": get_trend(str_1s),
                "bos_1s": get_bos(str_1s),
                "trend_1m": get_trend(str_1m),
                "bos_1m": get_bos(str_1m),
                "active_ob_1m": has_ob(str_1m),
                "active_fvg_1m": has_fvg(str_1m),
                "trend_5m": get_trend(str_5m),
                "bos_5m": get_bos(str_5m),
                "mtf_alignment": mtf
            },
            "scenario_picture": {
                "active_scenario_id": active_scen["id"] if active_scen else None,
                "active_scenario_name": active_scen["name"] if active_scen else None,
                "scenario_direction": active_scen["direction"] if active_scen else None,
                "scenario_status": active_scen["status"] if active_scen else None,
                "scenario_score": active_scen["score"] if active_scen else None
            },
            "market_context_picture": {
                "dominant_bias": (bias or {}).get("dominant_bias") or (mc or {}).get("dominant_bias"),
                "funding_rate": (mc or {}).get("funding_rate"),
                "oi_direction": (mc or {}).get("oi_direction"),
                "ls_ratio": (mc or {}).get("global_ls_ratio"),
                "liquidation_cascade_active": (mc or {}).get("liquidation_cascade_active")
            },
            "momentum_picture": {
                "delta_direction": "positive" if ((dna or {}).get("delta") or 0) > 0 else "negative" if ((dna or {}).get("delta") or 0) < 0 else "neutral",
                "aggression_side": (dna or {}).get("aggressor_side"),
                "volume_total": (dna or {}).get("total_volume")
            }
        }
    except Exception as e:
        return {"error": str(e), "event_ts": event_ts, "event_type": event_type}


def build_paper_result(event_id: str) -> dict:
    """
    Find paper trade result for this event/setup.
    Reads paper_closed_verified.jsonl (verified_high_low only).
    Returns dict with nulls if not found.
    """
    try:
        closed_rows = _tail_jsonl(DATA / "paper_closed_verified.jsonl", 200)
        for r in closed_rows:
            if (r.get("setup_id") == event_id or 
                r.get("source_setup_id") == event_id or
                r.get("qualified_setup_id") == event_id):
                return {
                    "setup_id": r.get("setup_id"),
                    "outcome": r.get("outcome"),
                    "pnl_r": r.get("pnl_r"),
                    "close_reason": r.get("close_reason"),
                    "price_source_quality": (r.get("hit_candle") or {}).get("price_source_quality"),
                    "duration_seconds": r.get("duration_seconds"),
                    "tp1_hit": r.get("tp1_hit"),
                    "tp2_hit": r.get("tp2_hit"),
                    "tp3_hit": r.get("tp3_hit")
                }
        return {
            "setup_id": None, "outcome": None, "pnl_r": None,
            "close_reason": None, "price_source_quality": None,
            "duration_seconds": None, "tp1_hit": None,
            "tp2_hit": None, "tp3_hit": None
        }
    except Exception as e:
        return {"error": str(e)}
