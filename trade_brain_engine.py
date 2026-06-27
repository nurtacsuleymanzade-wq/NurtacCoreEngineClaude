#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Trade Brain Engine (L10.5)
Profesyonel order flow trader gibi düşünür.
Her saniye 9 soruyu cevaplar, LONG/SHORT/NEUTRAL karar verir.
Karar yeterince güçlüyse trade_brain_setups.jsonl'e setup yazar.
Observer bu dosyayı okuyarak Trade Brain setup'larını takip eder.
"""

import json
import subprocess
import time
from pathlib import Path

DATA = Path("/root/NurtacCoreEngineClaude/data")
SYMBOL = "BTCUSDT"
POLL_SLEEP = 1.0
MIN_CONFIDENCE = 0.60
COOLDOWN_S = 120
BRAIN_FILE = DATA / "trade_brain_output.jsonl"
BRAIN_SETUPS = DATA / "trade_brain_setups.jsonl"


def _read_best_recent(path: str | Path, window_s: int = 30) -> dict:
    """Son window_s saniyede gelen en güçlü (non-none) kaydı döndür.
    ts=0 olan satırlar için dosya mtime kullanılır."""
    import subprocess, time
    from pathlib import Path as _Path
    NOW = time.time()
    p = _Path(path)
    if not p.exists():
        return {}
    # Dosya son değişim zamanını al
    file_mtime = p.stat().st_mtime
    file_age = NOW - file_mtime
    # Dosya çok eskiyse skip
    if file_age > 60:
        return {}
    best = {}
    best_score = -1
    try:
        raw = subprocess.getoutput(f"tail -50 {path} 2>/dev/null")
        lines = raw.splitlines()
        total = len(lines)
        for i, line in enumerate(lines):
            try:
                r = json.loads(line)
                lbl = r.get("label", "none")
                if lbl in (None, "none", ""):
                    continue
                score = r.get("score", 0) or 0
                if score > best_score:
                    best_score = score
                    best = r
            except:
                pass
    except:
        pass
    return best

def _read_json(path: str | Path) -> dict:
    p = Path(path)
    try:
        if str(p).endswith(".json"):
            return json.loads(p.read_text())
        raw = subprocess.getoutput(f"tail -1 {p} 2>/dev/null")
        return json.loads(raw) if raw.strip() else {}
    except Exception:
        return {}


def _sf(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _write_jsonl(path: Path, record: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def analyze_market() -> dict:
    ts = int(time.time() * 1000)

    zone = _read_json(DATA / "zone_context.json")
    vp = _read_json(DATA / "volume_profile.json")
    s1s = _read_json(DATA / "structure_1s.jsonl")
    s1m = _read_json(DATA / "structure_1m.jsonl")
    scen = _read_json(DATA / "scenarios.jsonl")
    bias = _read_json(DATA / "bias_context.jsonl")
    regime = _read_json(DATA / "regime_context.jsonl")
    liq = _read_json(DATA / "liquidation_clusters.jsonl")
    ev = _read_json(DATA / "evidence_stream.jsonl")
    gate = _read_json(DATA / "decision_gate_output.jsonl")
    dna = _read_json(DATA / "combined_1s_dna_btcusdt.jsonl")
    init_r = _read_best_recent(DATA / "labels_initiative_flow.jsonl")
    abs_r = _read_best_recent(DATA / "labels_absorption.jsonl")
    trap_r = _read_best_recent(DATA / "labels_trapped_trader.jsonl")

    close_dict = (dna.get("candle_dna") or {}).get("close") or {}
    current_price = _sf(close_dict.get("price") if isinstance(close_dict, dict) else close_dict)
    if current_price <= 0:
        return {}

    delta = _sf((dna.get("candle_dna") or {}).get("delta"))
    trend_1s = (s1s.get("trend") or {}).get("direction", "ranging")
    trend_1m = (s1m.get("trend") or {}).get("direction", "ranging")
    micro_bos = (s1s.get("bos") or {}).get("micro_bos")
    price_loc = zone.get("price_location", "neutral")
    dom_scenario = scen.get("dominant_scenario")
    dom_scen_dir = scen.get("dominant_direction", "neutral")
    active_scens = scen.get("active_scenarios") or []
    scen_count = _sf(scen.get("scenario_count"), 0)
    # geliştirme/confirm edilmiş senaryolardan en yüksek skorlu olanı kullan
    if (not dom_scenario or dom_scenario in ("none", "")) and active_scens:
        developing = [
            s for s in active_scens
            if s.get("status") in ("developing", "confirmed")
        ]
        if developing:
            best = max(developing, key=lambda x: _sf(x.get("score"), 0))
            dom_scenario = best.get("scenario_name") or best.get("name")
            if not dom_scen_dir or dom_scen_dir == "neutral":
                dom_scen_dir = best.get("direction") or "neutral"
    dom_bias = bias.get("dominant_bias", "neutral")
    cascade_risk = liq.get("cascade_risk", "none")
    long_score = _sf(ev.get("long_score"))
    short_score = _sf(ev.get("short_score"))
    gate_grade = gate.get("setup_grade", "none")

    scores = {}

    if price_loc in ("demand", "below_value", "at_val", "below_poc"):
        scores["Q1_market_location"] = (0.8, 0.2, f"Discount zone: {price_loc}")
    elif price_loc in ("supply", "above_value", "at_vah", "above_poc"):
        scores["Q1_market_location"] = (0.2, 0.8, f"Premium zone: {price_loc}")
    elif price_loc == "fvg":
        scores["Q1_market_location"] = (0.5, 0.5, "FVG — nötr")
    else:
        scores["Q1_market_location"] = (0.5, 0.5, f"Neutral: {price_loc}")

    if trend_1s == "uptrend" and trend_1m == "uptrend":
        scores["Q2_trend"] = (0.85, 0.15, "1s+1m uptrend")
    elif trend_1s == "downtrend" and trend_1m == "downtrend":
        scores["Q2_trend"] = (0.15, 0.85, "1s+1m downtrend")
    elif trend_1m == "uptrend":
        scores["Q2_trend"] = (0.65, 0.35, "1m uptrend")
    elif trend_1m == "downtrend":
        scores["Q2_trend"] = (0.35, 0.65, "1m downtrend")
    else:
        scores["Q2_trend"] = (0.5, 0.5, "ranging")

    nearby_long = liq.get("nearby_long_clusters") or []
    nearby_short = liq.get("nearby_short_clusters") or []
    if cascade_risk in ("HIGH", "CRITICAL"):
        if len(nearby_short) > len(nearby_long):
            scores["Q3_liquidity"] = (0.25, 0.75, f"CASCADE {cascade_risk} short clusters")
        else:
            scores["Q3_liquidity"] = (0.75, 0.25, f"CASCADE {cascade_risk} long clusters")
    elif nearby_long and not nearby_short:
        scores["Q3_liquidity"] = (0.35, 0.65, "Long liq nearby — sweep risk")
    elif nearby_short and not nearby_long:
        scores["Q3_liquidity"] = (0.65, 0.35, "Short liq nearby — sweep risk")
    else:
        scores["Q3_liquidity"] = (0.5, 0.5, "Dengeli likidite")

    acc_zone_raw = vp.get("acceptance_zone", {})
    price_vs_poc = vp.get("price_vs_poc", "at")
    if micro_bos == "bullish":
        scores["Q4_acceptance"] = (0.75, 0.25, "BOS bullish — acceptance yukari")
    elif micro_bos == "bearish":
        scores["Q4_acceptance"] = (0.25, 0.75, "BOS bearish — acceptance asagi")
    elif price_vs_poc == "above":
        scores["Q4_acceptance"] = (0.65, 0.35, "Above POC — acceptance")
    elif price_vs_poc == "below":
        scores["Q4_acceptance"] = (0.35, 0.65, "Below POC — rejection")
    else:
        scores["Q4_acceptance"] = (0.5, 0.5, f"At POC — neutral {acc_zone_raw}")

    init_dir = init_r.get("direction")
    if init_dir == "buy_initiative":
        scores["Q5_aggression"] = (0.8, 0.2, "Buy initiative")
    elif init_dir == "sell_initiative":
        scores["Q5_aggression"] = (0.2, 0.8, "Sell initiative")
    elif delta > 0:
        scores["Q5_aggression"] = (0.6, 0.4, f"Delta pozitif: {delta:.1f}")
    elif delta < 0:
        scores["Q5_aggression"] = (0.4, 0.6, f"Delta negatif: {delta:.1f}")
    else:
        scores["Q5_aggression"] = (0.5, 0.5, "Nötr delta")

    abs_dir = abs_r.get("direction")
    if abs_dir in ("buy_absorption", "buy_absorbed"):
        scores["Q6_absorption"] = (0.75, 0.25, "Buy absorption — güçlü alıcı")
    elif abs_dir in ("sell_absorption", "sell_absorbed"):
        scores["Q6_absorption"] = (0.25, 0.75, "Sell absorption — güçlü satıcı")
    else:
        scores["Q6_absorption"] = (0.5, 0.5, "Absorption yok")

    trap_lbl = trap_r.get("label", "none")
    if trap_lbl == "long_trapped":
        scores["Q7_trapped"] = (0.2, 0.8, "Long trapped — short devam")
    elif trap_lbl == "short_trapped":
        scores["Q7_trapped"] = (0.8, 0.2, "Short trapped — long devam")
    else:
        scores["Q7_trapped"] = (0.5, 0.5, "Trapped yok")

    total_score = long_score + short_score
    if total_score > 0:
        long_prob = long_score / total_score
        short_prob = short_score / total_score
        scores["Q8_probability"] = (
            long_prob,
            short_prob,
            f"Evidence L={long_score:.1f} S={short_score:.1f} gate={gate_grade}",
        )
    else:
        scores["Q8_probability"] = (0.5, 0.5, "Evidence yok")

    scenario_long_map = {
        "STOP_HUNT_RECLAIM": (0.75, 0.25),
        "SHORT_TRAP": (0.80, 0.20),
        "REVERSAL": (0.65, 0.35),
        "INSTITUTIONAL_ACCUMULATION": (0.70, 0.30),
        "BREAKOUT_CONTINUATION": (0.60, 0.40) if dom_scen_dir == "bullish" else (0.40, 0.60),
    }
    scenario_short_map = {
        "LONG_TRAP": (0.20, 0.80),
        "MOMENTUM_FADE": (0.30, 0.70),
        "LIQUIDITY_SWEEP": (0.35, 0.65),
        "RANGE_CONTINUATION": (0.5, 0.5),
    }
    if dom_scen_dir == "bullish":
        scenario_bias = (0.65, 0.35)
    elif dom_scen_dir == "bearish":
        scenario_bias = (0.35, 0.65)
    else:
        scenario_bias = (0.5, 0.5)
    if dom_scenario in scenario_long_map:
        scores["Q9_market_intent"] = (*scenario_long_map[dom_scenario], f"Senaryo: {dom_scenario}")
    elif dom_scenario in scenario_short_map:
        scores["Q9_market_intent"] = (*scenario_short_map[dom_scenario], f"Senaryo: {dom_scenario}")
    elif dom_scen_dir != "neutral":
        scores["Q9_market_intent"] = (*scenario_bias, f"Yon: {dom_scen_dir} (senaryo yok)")
    else:
        scores["Q9_market_intent"] = (0.5, 0.5, "Senaryo ve yon yok")

    total_long = sum(v[0] for v in scores.values()) / len(scores)
    total_short = sum(v[1] for v in scores.values()) / len(scores)

    if total_long >= MIN_CONFIDENCE and total_long > total_short:
        decision = "long"
        confidence = total_long
    elif total_short >= MIN_CONFIDENCE and total_short > total_long:
        decision = "short"
        confidence = total_short
    else:
        decision = "neutral"
        confidence = max(total_long, total_short)

    return {
        "engine": "trade_brain",
        "ts": ts,
        "symbol": SYMBOL,
        "current_price": round(current_price, 2),
        "decision": decision,
        "confidence": round(confidence, 3),
        "long_prob": round(total_long, 3),
        "short_prob": round(total_short, 3),
        "questions": {
            k: {"long": v[0], "short": v[1], "reason": v[2]} for k, v in scores.items()
        },
        "context": {
            "trend_1s": trend_1s,
            "trend_1m": trend_1m,
            "price_loc": price_loc,
            "micro_bos": micro_bos,
            "scenario": dom_scenario,
            "scenario_direction": dom_scen_dir,
            "scenario_count": int(scen_count),
            "scenario_snapshot": scen,
            "gate_grade": gate_grade,
            "dom_bias": dom_bias,
            "cascade": cascade_risk,
            "session": regime.get("session"),
        },
    }


def maybe_emit_setup(result: dict, last_emit: dict) -> dict:
    decision = result.get("decision", "neutral")
    confidence = result.get("confidence", 0)
    ts = result.get("ts", 0)
    price = result.get("current_price", 0)

    if decision == "neutral" or confidence < MIN_CONFIDENCE or price <= 0:
        return last_emit

    last_ts = last_emit.get(decision, 0)
    if ts - last_ts < COOLDOWN_S * 1000:
        return last_emit

    try:
        bl = json.loads(
            subprocess.getoutput(
                "tail -1 /root/NurtacCoreEngineClaude/data/historical_baseline_dna.jsonl 2>/dev/null"
            )
        )
        atr = float(bl.get("atr", 30.0))
    except Exception:
        atr = 30.0

    atr = max(atr, 5.0)
    if decision == "long":
        sl = price - atr * 1.5
        tp1 = price + atr * 1.0
        tp2 = price + atr * 2.0
        tp3 = price + atr * 3.0
    else:
        sl = price + atr * 1.5
        tp1 = price - atr * 1.0
        tp2 = price - atr * 2.0
        tp3 = price - atr * 3.0

    setup = {
        "engine": "trade_brain",
        "setup_id": f"TB_{ts}_{decision}",
        "symbol": SYMBOL,
        "setup_type": "normal",
        "direction": decision,
        "window_start_ts": ts,
        "window_end_ts": ts + 1000,
        "entry": {"price": round(price, 4), "triggered_at_ts": ts, "timeframe_context": "1S"},
        "sl": {"price": round(sl, 4), "atr_multiplier": 1.5},
        "tp1": {"price": round(tp1, 4), "rr": 1.0},
        "tp2": {"price": round(tp2, 4), "rr": 2.0},
        "tp3": {"price": round(tp3, 4), "rr": 3.0},
        "atr_used": round(atr, 4),
        "quality_tier": "L2_MEDIUM",
        "direction_score": round(confidence * 10, 2),
        "confidence": round(confidence, 3),
        "brain_questions": result.get("questions", {}),
        "context": result.get("context", {}),
        "source_scenario": result.get("context", {}).get("scenario"),
        "source_scenario_direction": result.get("context", {}).get("scenario_direction"),
        "scenario_snapshot": result.get("context", {}).get("scenario_snapshot"),
        "status": "open",
    }

    _write_jsonl(BRAIN_SETUPS, setup)
    last_emit[decision] = ts
    print(
        f"[TB] SETUP {decision.upper()} conf={confidence:.2f} "
        f"entry={price:.2f} sl={sl:.2f} tp1={tp1:.2f}",
        flush=True,
    )
    return last_emit


def main() -> None:
    print("[TB] Trade Brain Engine başlatıldı", flush=True)
    print(f"[TB] MIN_CONFIDENCE={MIN_CONFIDENCE} COOLDOWN={COOLDOWN_S}s", flush=True)

    last_emit = {"long": 0, "short": 0}
    try:
        if BRAIN_SETUPS.exists():
            for line in BRAIN_SETUPS.read_text().splitlines()[-20:]:
                try:
                    r = json.loads(line)
                    d = r.get("direction")
                    ts_ms = int(r.get("window_start_ts", 0) or 0)
                    if d in last_emit and ts_ms > last_emit[d]:
                        last_emit[d] = ts_ms
                except Exception:
                    pass
    except Exception:
        pass
    last_print = 0

    while True:
        try:
            result = analyze_market()
            if not result:
                time.sleep(POLL_SLEEP)
                continue

            _write_jsonl(BRAIN_FILE, result)

            now = time.time()
            if now - last_print >= 30:
                print(
                    f"[TB] {result['decision'].upper():<7} conf={result['confidence']:.2f} "
                    f"price={result['current_price']:.2f}",
                    flush=True,
                )
                last_print = now

            last_emit = maybe_emit_setup(result, last_emit)
        except Exception as e:
            print(f"[TB] Hata: {e}", flush=True)

        time.sleep(POLL_SLEEP)


if __name__ == "__main__":
    main()
