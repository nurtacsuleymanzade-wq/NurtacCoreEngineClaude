#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Volume Profile Engine
aligned_1m_candle_dna.jsonl -> volume_profile.json
Her 5 dakikada calisir. Mevcut sisteme etki etmez.
READ: data/aligned_1m_candle_dna.jsonl (tail -500)
WRITE: data/volume_profile.json (overwrite)
"""

import json
import subprocess
import time
from pathlib import Path

DATA_DIR = Path("data")
INPUT_1M = DATA_DIR / "aligned_1m_candle_dna.jsonl"
OUTPUT_FILE = DATA_DIR / "volume_profile.json"
SCAN_LIMIT = 500


def _px(v) -> float:
    """Güvenli fiyat çıkarma. Dict içinde price bekler."""
    if isinstance(v, dict):
        return float(v.get("price") or 0)
    return float(v or 0)


def _num(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _read_recent_candles() -> list[dict]:
    if not INPUT_1M.exists():
        return []
    raw = subprocess.getoutput(f"tail -{SCAN_LIMIT} {INPUT_1M} 2>/dev/null")
    candles: list[dict] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        ohlc = rec.get("ohlc") or {}
        volume = rec.get("volume") or {}
        profile = rec.get("profile") or {}
        o = _px(ohlc.get("open"))
        h = _px(ohlc.get("high"))
        l = _px(ohlc.get("low"))
        c = _px(ohlc.get("close"))
        v = _num(volume.get("total_volume"))
        if v <= 0:
            v = _num(volume.get("buy_volume")) + _num(volume.get("sell_volume"))
        if v <= 0:
            v = 1.0
        ts = int(rec.get("window_start_ts") or 0)
        if o > 0 and h > 0 and l > 0 and c > 0 and ts > 0:
            candles.append(
                {
                    "ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "profile": profile,
                }
            )
    return candles


def build_profile() -> None:
    candles = _read_recent_candles()
    if len(candles) < 10:
        print(f"[VP] Yetersiz mum: {len(candles)}", flush=True)
        return

    last = candles[-1]
    profile = last.get("profile") or {}

    poc_price = _num(profile.get("poc"))
    vah = _num(profile.get("vah"))
    val = _num(profile.get("val"))

    if poc_price <= 0 or vah <= 0 or val <= 0:
        print("[VP] Profil alanlari eksik", flush=True)
        return

    current_price = _num(last.get("close"))
    hvn_raw = profile.get("hvn") or []
    lvn_raw = profile.get("lvn") or []
    hvn = []
    lvn = []
    for node in hvn_raw:
        if isinstance(node, dict):
            px = _num(node.get("price"))
            if px > 0:
                hvn.append(px)
        else:
            px = _num(node)
            if px > 0:
                hvn.append(px)
    for node in lvn_raw:
        if isinstance(node, dict):
            px = _num(node.get("price"))
            if px > 0:
                lvn.append(px)
        else:
            px = _num(node)
            if px > 0:
                lvn.append(px)

    price_vs_poc = "above" if current_price > poc_price else "below" if current_price < poc_price else "at"
    price_vs_vah = "above" if current_price > vah else "below" if current_price < vah else "at"
    price_vs_val = "above" if current_price > val else "below" if current_price < val else "at"

    result = {
        "ts": last["ts"],
        "updated_at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "candles_used": len(candles),
        "poc_price": round(poc_price, 1),
        "vah": round(vah, 1),
        "val": round(val, 1),
        "hvn": [round(p, 1) for p in sorted(set(hvn), reverse=True)[:5]],
        "lvn": [round(p, 1) for p in sorted(set(lvn))[:5]],
        "price_vs_poc": price_vs_poc,
        "price_vs_vah": price_vs_vah,
        "price_vs_val": price_vs_val,
        "current_price": round(current_price, 2),
        "acceptance_zone": {
            "low": round(val, 1),
            "high": round(vah, 1),
        },
    }

    OUTPUT_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(
        f"[VP] POC={poc_price:.0f} VAH={vah:.0f} VAL={val:.0f} "
        f"price={current_price:.0f} vs_poc={price_vs_poc}",
        flush=True,
    )


def run_live(interval_s: int = 300) -> None:
    print("[VP] Volume Profile Engine baslatildi", flush=True)
    while True:
        try:
            build_profile()
        except Exception as e:
            print(f"[VP] Hata: {e}", flush=True)
        time.sleep(interval_s)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "once"], default="live")
    args = parser.parse_args()
    if args.mode == "once":
        build_profile()
    else:
        run_live()
