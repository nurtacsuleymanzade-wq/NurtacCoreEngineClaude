#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Multi-TF Outlook Engine
READ: macro + regime + liq + probability + max_pain → outlooks
WRITE: data/multitf_outlook.json (overwrite)
Her 15 dakikada çalışır. Trade açmaz. Sadece bağlam üretir.
"""

import json
import subprocess
import time
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "multitf_outlook.json"


def _tail1(fname: str) -> dict:
    try:
        r = subprocess.getoutput(f"tail -1 {DATA_DIR/fname} 2>/dev/null")
        return json.loads(r) if r.strip() else {}
    except Exception:
        return {}


def _read(fname: str) -> dict:
    try:
        return json.loads((DATA_DIR / fname).read_text())
    except Exception:
        return {}


def _px(v) -> float:
    if isinstance(v, dict):
        return float(v.get("price") or 0)
    return float(v or 0)


def _score(signal_val, target_dir: str) -> float:
    if not signal_val:
        return 0.0
    s = str(signal_val).lower()
    bear_words = {"bear", "short", "down", "negative", "sell", "outflow", "genuine_bear"}
    bull_words = {"bull", "long", "up", "positive", "buy", "inflow", "genuine_bull"}
    is_bear = any(w in s for w in bear_words)
    is_bull = any(w in s for w in bull_words)
    if target_dir == "bearish":
        if is_bear:
            return +1.0
        if is_bull:
            return -1.0
    elif target_dir == "bullish":
        if is_bull:
            return +1.0
        if is_bear:
            return -1.0
    return 0.0


def _build_horizon(signals: dict, weights: dict) -> dict:
    bull_score = 0.0
    bear_score = 0.0
    agrees_bear = []
    agrees_bull = []
    for sig, val in signals.items():
        w = weights.get(sig, 0.3)
        b = _score(val, "bearish") * w
        u = _score(val, "bullish") * w
        if b > 0:
            agrees_bear.append(sig)
            bear_score += b
        if u > 0:
            agrees_bull.append(sig)
            bull_score += u
    total = bull_score + bear_score
    if total == 0:
        return {"bias": "neutral", "confidence": 0.40, "signals_agree": [], "signals_disagree": []}
    if bear_score >= bull_score:
        conf = round(min(0.90, 0.40 + (bear_score - bull_score) / max(total, 1) * 0.5), 3)
        return {
            "bias": "bearish",
            "confidence": conf,
            "signals_agree": agrees_bear[:4],
            "signals_disagree": agrees_bull[:2],
        }
    conf = round(min(0.90, 0.40 + (bull_score - bear_score) / max(total, 1) * 0.5), 3)
    return {
        "bias": "bullish",
        "confidence": conf,
        "signals_agree": agrees_bull[:4],
        "signals_disagree": agrees_bear[:2],
    }


def build_outlook() -> None:
    mc = _read("macro_context.json")
    regime = _tail1("regime_context.jsonl")
    liq = _tail1("liquidation_clusters.jsonl")
    bias = _tail1("bias_context.jsonl")
    ps = _read("probability_surface.json")
    mp = _read("max_pain.json")

    dna = _tail1("combined_1s_dna_btcusdt.jsonl")
    cdna = dna.get("candle_dna") or {}
    price = _px(cdna.get("close")) or float(cdna.get("last_trade_price") or 0)
    if price <= 0:
        price = float(bias.get("current_price") or 0)

    signals = {
        "macro_move": mc.get("move_type", ""),
        "macro_bias": mc.get("directional_bias", ""),
        "smart_money": mc.get("smart_money_bias", ""),
        "top_trader": mc.get("divergence_signal", ""),
        "etf_signal": mc.get("etf_signal", ""),
        "coinbase": mc.get("coinbase_signal", ""),
        "basis": mc.get("basis_signal", ""),
        "oi_regime": mc.get("price_oi_regime", ""),
        "regime": regime.get("trend_regime", ""),
        "funding": bias.get("dominant_bias", ""),
        "max_pain": mp.get("mp_bias", ""),
    }

    hot_long = [c.get("price") for c in liq.get("hot_long_clusters", [])[:3] if c.get("price")]
    hot_short = [c.get("price") for c in liq.get("hot_short_clusters", [])[:3] if c.get("price")]
    mp_price = mp.get("max_pain_price")
    best_edge = (ps.get("best_combinations") or [{}])[0]

    o1h = _build_horizon(
        signals,
        {
            "macro_bias": 1.0,
            "smart_money": 1.0,
            "top_trader": 0.8,
            "regime": 0.8,
            "funding": 0.6,
            "etf_signal": 0.5,
            "coinbase": 0.5,
            "max_pain": 0.4,
        },
    )
    o4h = _build_horizon(
        signals,
        {
            "macro_move": 1.0,
            "macro_bias": 1.0,
            "smart_money": 0.8,
            "regime": 1.0,
            "etf_signal": 0.7,
            "top_trader": 0.6,
            "max_pain": 0.6,
            "funding": 0.4,
        },
    )
    o1d = _build_horizon(
        signals,
        {
            "macro_move": 1.0,
            "etf_signal": 1.0,
            "max_pain": 0.8,
            "smart_money": 0.5,
            "top_trader": 0.5,
            "basis": 0.4,
        },
    )

    result = {
        "updated_at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "current_price": round(price, 2),
        "session": regime.get("session", "?"),
        "regime": signals["regime"],
        "outlooks": {"1H": o1h, "4H": o4h, "1D": o1d},
        "raw_signals": {k: v for k, v in signals.items() if v},
        "key_levels": {
            "liq_long_clusters": hot_long,
            "liq_short_clusters": hot_short,
            "max_pain": mp_price,
        },
        "best_edge": best_edge,
    }

    OUTPUT_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(
        f"[OUTLOOK] 1H={o1h['bias']}({o1h['confidence']:.0%}) "
        f"4H={o4h['bias']}({o4h['confidence']:.0%}) "
        f"1D={o1d['bias']}({o1d['confidence']:.0%})",
        flush=True,
    )


def run_live(interval_s: int = 900) -> None:
    print("[OUTLOOK] Multi-TF Outlook Engine başlatıldı", flush=True)
    while True:
        try:
            build_outlook()
        except Exception as e:
            print(f"[OUTLOOK] Hata: {e}", flush=True)
        time.sleep(interval_s)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["live", "once"], default="live")
    args = p.parse_args()
    if args.mode == "once":
        build_outlook()
    else:
        run_live()
