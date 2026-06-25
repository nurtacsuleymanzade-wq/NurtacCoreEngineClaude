#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Zone Engine
structure_1s + volume_profile -> zone_context.json
Her 30 saniyede calisir. Trade acmaz.
READ: data/structure_1s.jsonl (tail -1)
      data/volume_profile.json
      data/combined_1s_dna_btcusdt.jsonl (tail -1, fiyat)
WRITE: data/zone_context.json (overwrite)
"""

import json
import subprocess
import time
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_FILE = DATA_DIR / "zone_context.json"


def _tail1(fname: str) -> dict:
    try:
        path = DATA_DIR / fname
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not raw:
            return {}
        return json.loads(raw[-1])
    except Exception:
        return {}


def _px(v) -> float:
    if isinstance(v, dict):
        return float(v.get("price") or 0)
    return float(v or 0)


def _get_price() -> float:
    d = _tail1("combined_1s_dna_btcusdt.jsonl")
    cdna = d.get("candle_dna") or d.get("candle") or {}
    close = cdna.get("close")
    return _px(close) or float(cdna.get("last_trade_price") or 0)


def _distance_pct(price: float, zone_mid: float) -> float:
    if zone_mid <= 0:
        return 999.0
    return round(abs(price - zone_mid) / zone_mid * 100, 4)


def _normalize_ob(ob: dict) -> dict | None:
    if not isinstance(ob, dict):
        return None
    high = float(ob.get("ob_high") or ob.get("high") or ob.get("price") or 0)
    low = float(ob.get("ob_low") or ob.get("low") or (high * 0.999 if high > 0 else 0))
    if high <= 0 or low <= 0:
        return None
    ob_type = str(ob.get("ob_type") or ob.get("type") or "").lower()
    strength = str(ob.get("strength") or ob.get("grade") or "MEDIUM").upper()
    if "bearish" in ob_type or "supply" in ob_type:
        zone_type = "supply"
    elif "bullish" in ob_type or "demand" in ob_type:
        zone_type = "demand"
    else:
        zone_type = "neutral"
    return {
        "low": round(low, 2),
        "high": round(high, 2),
        "strength": strength,
        "zone_type": zone_type,
        "price_inside": False,
    }


def _normalize_fvg(fvg: dict) -> dict | None:
    if not isinstance(fvg, dict):
        return None
    high = float(fvg.get("gap_high") or fvg.get("high") or 0)
    low = float(fvg.get("gap_low") or fvg.get("low") or 0)
    if high <= 0 or low <= 0:
        return None
    fvg_type = str(fvg.get("fvg_type") or fvg.get("type") or "").lower()
    if "bearish" in fvg_type:
        kind = "bearish"
    elif "bullish" in fvg_type:
        kind = "bullish"
    else:
        kind = "neutral"
    return {
        "type": kind,
        "low": round(low, 2),
        "high": round(high, 2),
        "fill_pct": 0.0,
        "distance_pct": 999.0,
        "price_inside": False,
    }


def build_zone_context() -> None:
    price = _get_price()
    if price <= 0:
        print("[ZONE] Fiyat verisi yok", flush=True)
        return

    s1s = _tail1("structure_1s.jsonl")
    try:
        vp = json.loads((DATA_DIR / "volume_profile.json").read_text())
    except Exception:
        vp = {}

    order_blocks = s1s.get("order_blocks") or []
    fvg_list = s1s.get("fvg") or []
    ts = int(s1s.get("ts") or s1s.get("window_start_ts") or 0)

    demand_zones = []
    supply_zones = []
    for ob in order_blocks:
        norm = _normalize_ob(ob)
        if not norm:
            continue
        if norm["zone_type"] == "demand":
            demand_zones.append(norm)
        elif norm["zone_type"] == "supply":
            supply_zones.append(norm)

    nearest_demand = None
    min_dem_dist = 999.0
    for zone in demand_zones:
        mid = (zone["high"] + zone["low"]) / 2
        dist = _distance_pct(price, mid)
        if dist < min_dem_dist:
            min_dem_dist = dist
            nearest_demand = {
                "low": zone["low"],
                "high": zone["high"],
                "strength": zone["strength"],
                "distance_pct": dist,
                "price_inside": zone["low"] <= price <= zone["high"],
            }

    nearest_supply = None
    min_sup_dist = 999.0
    for zone in supply_zones:
        mid = (zone["high"] + zone["low"]) / 2
        dist = _distance_pct(price, mid)
        if dist < min_sup_dist:
            min_sup_dist = dist
            nearest_supply = {
                "low": zone["low"],
                "high": zone["high"],
                "strength": zone["strength"],
                "distance_pct": dist,
                "price_inside": zone["low"] <= price <= zone["high"],
            }

    active_fvg = None
    min_fvg_dist = 999.0
    for fvg in fvg_list:
        norm = _normalize_fvg(fvg)
        if not norm:
            continue
        mid = (norm["high"] + norm["low"]) / 2
        dist = _distance_pct(price, mid)
        span = max(norm["high"] - norm["low"], 0.0001)
        if norm["type"] == "bullish":
            fill_pct = max(0.0, min(100.0, (price - norm["low"]) / span * 100))
        elif norm["type"] == "bearish":
            fill_pct = max(0.0, min(100.0, (norm["high"] - price) / span * 100))
        else:
            fill_pct = 0.0
        if dist < min_fvg_dist and fill_pct < 100:
            min_fvg_dist = dist
            active_fvg = {
                "type": norm["type"],
                "low": norm["low"],
                "high": norm["high"],
                "fill_pct": round(fill_pct, 1),
                "price_inside": norm["low"] <= price <= norm["high"],
            }

    poc = float(vp.get("poc_price") or 0)
    vah = float(vp.get("vah") or 0)
    val = float(vp.get("val") or 0)
    above_poc = price > poc if poc > 0 else None
    in_value_area = val <= price <= vah if (val > 0 and vah > 0) else None

    in_demand = bool(nearest_demand and nearest_demand.get("price_inside"))
    in_supply = bool(nearest_supply and nearest_supply.get("price_inside"))
    in_fvg = bool(active_fvg and active_fvg.get("price_inside"))

    if in_supply:
        price_location = "supply"
    elif in_demand:
        price_location = "demand"
    elif in_fvg:
        price_location = "fvg"
    elif above_poc:
        price_location = "above_poc"
    else:
        price_location = "neutral"

    result = {
        "ts": ts,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "current_price": round(price, 2),
        "price_location": price_location,
        "nearest_demand": nearest_demand,
        "nearest_supply": nearest_supply,
        "active_fvg": active_fvg,
        "above_poc": above_poc,
        "in_value_area": in_value_area,
        "poc_price": round(poc, 1) if poc else None,
        "vah": round(vah, 1) if vah else None,
        "val": round(val, 1) if val else None,
    }

    OUTPUT_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(
        f"[ZONE] price={price:.0f} location={price_location} "
        f"POC={'above' if above_poc else 'below'} "
        f"VA={'in' if in_value_area else 'out'}",
        flush=True,
    )


def run_live(interval_s: int = 30) -> None:
    print("[ZONE] Zone Engine baslatildi", flush=True)
    while True:
        try:
            build_zone_context()
        except Exception as e:
            print(f"[ZONE] Hata: {e}", flush=True)
        time.sleep(interval_s)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["live", "once"], default="live")
    args = p.parse_args()
    if args.mode == "once":
        build_zone_context()
    else:
        run_live()
