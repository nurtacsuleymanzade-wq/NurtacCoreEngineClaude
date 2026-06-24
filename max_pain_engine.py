#!/usr/bin/env python3
"""Deribit BTC options max-pain and options/futures OI engine."""

import argparse
import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path


DATA_DIR = Path("data")
MAX_PAIN_FILE = DATA_DIR / "max_pain.json"
BIAS_CONTEXT_FILE = DATA_DIR / "bias_context.jsonl"
MARKET_CONTEXT_FILE = DATA_DIR / "market_context.jsonl"
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"


def fetch_deribit(endpoint: str, params: dict | None = None) -> dict:
    """Call a Deribit public REST endpoint."""
    url = f"{DERIBIT_BASE}/{endpoint}?" + urllib.parse.urlencode(params or {})
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.loads(response.read())
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"[MAX_PAIN] Deribit fetch error: {exc}", flush=True)
        return {}


def calc_max_pain(current_price: float) -> dict:
    """Calculate max pain from BTC options expiring within seven days."""
    response = fetch_deribit(
        "get_instruments",
        {"currency": "BTC", "kind": "option", "expired": "false"},
    )
    instruments = response.get("result", [])
    if not isinstance(instruments, list) or not instruments:
        return {}

    now_ms = int(time.time() * 1000)
    week_ms = 7 * 24 * 3600 * 1000
    weekly = [
        item for item in instruments
        if 0 < int(item.get("expiration_timestamp", 0)) - now_ms <= week_ms
    ]
    if not weekly:
        weekly = instruments

    summary_response = fetch_deribit(
        "get_book_summary_by_currency",
        {"currency": "BTC", "kind": "option"},
    )
    summary_rows = summary_response.get("result", [])
    summaries = {
        item.get("instrument_name"): item
        for item in summary_rows if isinstance(item, dict) and item.get("instrument_name")
    } if isinstance(summary_rows, list) else {}

    strike_data: dict[float, dict[str, float]] = {}
    options_oi_total = 0.0
    for instrument in weekly:
        name = instrument.get("instrument_name")
        strike = float(instrument.get("strike", 0) or 0)
        option_type = instrument.get("option_type", "")
        if not name or strike <= 0:
            continue

        summary = summaries.get(name, {})
        oi_btc = float(summary.get("open_interest", 0) or 0)
        oi_usd = oi_btc * current_price
        options_oi_total += oi_usd
        bucket = strike_data.setdefault(
            strike, {"call_oi_usd": 0.0, "put_oi_usd": 0.0},
        )
        if option_type == "call":
            bucket["call_oi_usd"] += oi_usd
        elif option_type == "put":
            bucket["put_oi_usd"] += oi_usd

    if not strike_data or current_price <= 0:
        return {}

    strikes = sorted(strike_data)
    min_pain = float("inf")
    max_pain_price = current_price
    for expiry_price in strikes:
        total_pain = 0.0
        for strike, data in strike_data.items():
            call_payout = max(0.0, expiry_price - strike)
            put_payout = max(0.0, strike - expiry_price)
            total_pain += (
                call_payout * data.get("call_oi_usd", 0.0) / current_price
                + put_payout * data.get("put_oi_usd", 0.0) / current_price
            )
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_price = expiry_price

    distance_pct = (current_price - max_pain_price) / current_price * 100
    if distance_pct > 2:
        mp_bias = "bearish"
    elif distance_pct < -2:
        mp_bias = "bullish"
    else:
        mp_bias = "neutral"

    weekday = time.gmtime().tm_wday
    expiry_proximity = (
        "HIGH" if weekday in (3, 4) else "MEDIUM" if weekday == 2 else "LOW"
    )
    return {
        "engine": "max_pain_engine",
        "ts": int(time.time() * 1000),
        "current_price": current_price,
        "max_pain_price": max_pain_price,
        "distance_pct": round(distance_pct, 2),
        "mp_bias": mp_bias,
        "expiry_proximity": expiry_proximity,
        "options_oi_usd_m": round(options_oi_total / 1e6, 1),
        "strike_count": len(strike_data),
        "weekly_options_count": len(weekly),
    }


def _read_latest_jsonl(path: Path) -> dict:
    raw = subprocess.getoutput(f"tail -1 {path}")
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def calc_options_futures_ratio(options_oi_usd: float) -> dict:
    """Calculate the options-to-futures open-interest ratio."""
    bias_context = _read_latest_jsonl(BIAS_CONTEXT_FILE)
    market_context = _read_latest_jsonl(MARKET_CONTEXT_FILE)
    futures_oi_usd = float(bias_context.get("open_interest_usd", 0) or 0)
    if futures_oi_usd <= 0:
        oi_btc = float(
            market_context.get("oi_value", 0)
            or (bias_context.get("market_context") or {}).get("oi_value", 0)
            or 0
        )
        current_price = float(market_context.get("current_price", 0) or 0)
        futures_oi_usd = oi_btc * current_price
    if futures_oi_usd <= 0:
        return {}

    ratio = options_oi_usd / futures_oi_usd
    if ratio < 0.5:
        regime, signal_confidence = "FUTURES_DOMINANT", "HIGH"
    elif ratio < 0.9:
        regime, signal_confidence = "MIXED", "MEDIUM"
    else:
        regime, signal_confidence = "OPTIONS_DOMINANT", "LOW"
    return {
        "options_oi_usd_m": round(options_oi_usd / 1e6, 1),
        "futures_oi_usd_m": round(futures_oi_usd / 1e6, 1),
        "ratio": round(ratio, 3),
        "regime": regime,
        "signal_confidence": signal_confidence,
    }


def run_once(current_price: float) -> None:
    print(f"[MAX_PAIN] Calculating... price=${current_price:,.0f}", flush=True)
    result = calc_max_pain(current_price)
    if not result:
        print("[MAX_PAIN] No data from Deribit", flush=True)
        return

    result["options_futures_ratio"] = calc_options_futures_ratio(
        float(result.get("options_oi_usd_m", 0) or 0) * 1e6,
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MAX_PAIN_FILE.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        f"[MAX_PAIN] max_pain=${result.get('max_pain_price', 0):,.0f} "
        f"bias={result.get('mp_bias')} "
        f"distance={result.get('distance_pct', 0):+.1f}% "
        f"expiry_proximity={result.get('expiry_proximity')}",
        flush=True,
    )


def get_price() -> float:
    try:
        bias_context = _read_latest_jsonl(BIAS_CONTEXT_FILE)
        market_context = _read_latest_jsonl(MARKET_CONTEXT_FILE)
        return float(
            bias_context.get("current_price", 0)
            or market_context.get("current_price", 0)
            or 0
        )
    except Exception:
        return 0.0


def run_live(mode: str) -> None:
    if mode == "once":
        price = get_price()
        if price > 0:
            run_once(price)
        else:
            print("[MAX_PAIN] Price not available", flush=True)
        return

    print("[MAX_PAIN] Live mode — saatlik güncelleme", flush=True)
    while True:
        try:
            price = get_price()
            if price > 0:
                run_once(price)
            else:
                print("[MAX_PAIN] Price not available, retry in 60s", flush=True)
                time.sleep(60)
                continue
        except Exception as exc:
            print(f"[MAX_PAIN] Error: {exc}", flush=True)
        time.sleep(3600)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("live", "once"), default="live")
    args = parser.parse_args()
    run_live(args.mode)


if __name__ == "__main__":
    main()
