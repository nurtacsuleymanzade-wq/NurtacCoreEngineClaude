#!/usr/bin/env python3
"""Market regime, volatility, and UTC session context engine."""

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
ONE_SECOND_FILE = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
ALIGNED_1H_FILE = DATA_DIR / "aligned_1h_candle_dna.jsonl"
ALIGNED_4H_FILE = DATA_DIR / "aligned_4h_candle_dna.jsonl"
BASELINE_FILE = DATA_DIR / "historical_baseline_dna.jsonl"
OUTPUT_FILE = DATA_DIR / "regime_context.jsonl"
HALT_FILE = DATA_DIR / "SYSTEM_HALT"

REGIME_SETUP_COMPAT = {
    "TRENDING_UP": ["BREAKOUT_CONTINUATION_long", "RECLAIM_long"],
    "TRENDING_DOWN": [
        "BREAKOUT_CONTINUATION_short",
        "RECLAIM_short",
        "initiative_flow_sell",
    ],
    "RANGING": ["REVERSAL_long", "REVERSAL_short", "STOP_HUNT_RECLAIM"],
    "BREAKOUT": ["BREAKOUT_CONTINUATION_long", "BREAKOUT_CONTINUATION_short"],
    "VOLATILE": [],
}

SESSION_SETUP_COMPAT = {
    "ASIA": ["REVERSAL", "RECLAIM"],
    "LONDON": ["BREAKOUT_CONTINUATION"],
    "NY_LONDON": ["BREAKOUT_CONTINUATION", "initiative_flow"],
    "NEW_YORK": ["BREAKOUT_CONTINUATION", "REVERSAL"],
    "OFF_HOURS": [],
}


def _tail_records(path: Path, count: int = 50) -> list[dict]:
    """Read only a bounded tail and tolerate malformed or concatenated JSON."""
    if not path.exists():
        return []
    raw = subprocess.getoutput(f"tail -{count} {path}")
    decoder = json.JSONDecoder()
    records: list[dict] = []
    offset = 0
    while offset < len(raw):
        while offset < len(raw) and raw[offset].isspace():
            offset += 1
        if offset >= len(raw):
            break
        try:
            record, end = decoder.raw_decode(raw, offset)
        except json.JSONDecodeError:
            next_line = raw.find("\n", offset)
            if next_line < 0:
                break
            offset = next_line + 1
            continue
        if isinstance(record, dict):
            records.append(record)
        offset = end
    return records


def _price(record: dict, field: str) -> float:
    value = (record.get("ohlc", {}).get(field, {}) or {}).get("price", 0.0)
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _compact_candles(path: Path) -> list[dict]:
    candles: list[dict] = []
    seen: set[int] = set()
    for record in _tail_records(path, 50):
        ts = int(record.get("window_start_ts", 0) or 0)
        candle = {
            "ts": ts,
            "high": _price(record, "high"),
            "low": _price(record, "low"),
            "close": _price(record, "close"),
        }
        if ts and ts not in seen and all(candle.get(k, 0) > 0 for k in ("high", "low", "close")):
            candles.append(candle)
            seen.add(ts)
    return sorted(candles, key=lambda item: item.get("ts", 0))[-50:]


def _direction(candles: list[dict]) -> str:
    if len(candles) < 3:
        return "flat"
    recent = candles[-3:]
    highs = [item.get("high", 0.0) for item in recent]
    lows = [item.get("low", 0.0) for item in recent]
    if highs[0] < highs[1] < highs[2] and lows[0] < lows[1] < lows[2]:
        return "up"
    if highs[0] > highs[1] > highs[2] and lows[0] > lows[1] > lows[2]:
        return "down"
    return "flat"


def _is_breakout(candles: list[dict]) -> bool:
    if len(candles) < 5:
        return False
    window = candles[-20:]
    previous = window[:-1]
    latest = window[-1]
    ranges = [item.get("high", 0.0) - item.get("low", 0.0) for item in previous]
    average_range = sum(ranges) / len(ranges) if ranges else 0.0
    outside_range = (
        latest.get("close", 0.0) > max(item.get("high", 0.0) for item in previous)
        or latest.get("close", 0.0) < min(item.get("low", 0.0) for item in previous)
    )
    latest_range = latest.get("high", 0.0) - latest.get("low", 0.0)
    return bool(average_range > 0 and outside_range and latest_range > average_range * 1.5)


def _trend_strength(one_hour: list[dict], four_hour: list[dict]) -> float:
    comparisons: list[bool] = []
    for candles in (one_hour[-6:], four_hour[-6:]):
        for previous, current in zip(candles, candles[1:]):
            comparisons.extend([
                current.get("high", 0.0) != previous.get("high", 0.0),
                current.get("low", 0.0) != previous.get("low", 0.0),
            ])
    if not comparisons:
        return 0.0
    one_direction = []
    for candles in (one_hour[-6:], four_hour[-6:]):
        for previous, current in zip(candles, candles[1:]):
            high_move = current.get("high", 0.0) - previous.get("high", 0.0)
            low_move = current.get("low", 0.0) - previous.get("low", 0.0)
            one_direction.append(high_move * low_move > 0)
    return round(sum(one_direction) / len(one_direction), 3) if one_direction else 0.0


def _trend_regime(one_hour: list[dict], four_hour: list[dict]) -> tuple[str, float]:
    if _is_breakout(one_hour):
        return "BREAKOUT", _trend_strength(one_hour, four_hour)
    direction_1h = _direction(one_hour)
    direction_4h = _direction(four_hour)
    if direction_1h == direction_4h == "up":
        regime = "TRENDING_UP"
    elif direction_1h == direction_4h == "down":
        regime = "TRENDING_DOWN"
    else:
        regime = "RANGING"
    return regime, _trend_strength(one_hour, four_hour)


def _atr_context() -> tuple[float, str]:
    records = _tail_records(BASELINE_FILE, 50)
    baseline = next(
        (record for record in reversed(records) if record.get("timeframe") == "1S"),
        records[-1] if records else {},
    )
    atr = baseline.get("atr", {}) or {}
    try:
        zscore = float(atr.get("atr_z_score_medium", 0.0) or 0.0)
    except (TypeError, ValueError):
        zscore = 0.0
    if zscore < -0.5:
        volatility = "LOW_VOL"
    elif zscore <= 1.0:
        volatility = "NORMAL_VOL"
    elif zscore <= 2.0:
        volatility = "HIGH_VOL"
    else:
        volatility = "SPIKE"
    return round(zscore, 4), volatility


def _session(ts_ms: int) -> str:
    hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour
    if hour < 8:
        return "ASIA"
    if hour < 12:
        return "LONDON"
    if hour < 17:
        return "NY_LONDON"
    if hour < 21:
        return "NEW_YORK"
    return "OFF_HOURS"


def _compatible_setups(regime: str, session: str) -> list[str]:
    regime_setups = REGIME_SETUP_COMPAT.get(regime, [])
    session_families = SESSION_SETUP_COMPAT.get(session, [])
    if not session_families:
        return []
    return [
        setup for setup in regime_setups
        if any(family in setup for family in session_families)
    ]


def build_context(ts_ms: int) -> dict:
    one_hour = _compact_candles(ALIGNED_1H_FILE)
    four_hour = _compact_candles(ALIGNED_4H_FILE)
    regime, strength = _trend_regime(one_hour, four_hour)
    atr_zscore, volatility = _atr_context()
    session = _session(ts_ms)
    compatible = _compatible_setups(regime, session)
    block_reason = None
    if volatility == "SPIKE":
        block_reason = "VOLATILE"
    elif session == "OFF_HOURS":
        block_reason = "OFF_HOURS"
    trade_allowed = block_reason is None
    return {
        "engine": "regime_engine",
        "ts": ts_ms,
        "trend_regime": regime,
        "volatility_class": volatility,
        "session": session,
        "atr_zscore": atr_zscore,
        "trend_strength": strength,
        "compatible_setups": compatible,
        "trade_allowed": trade_allowed,
        "block_reason": block_reason,
    }


def _latest_one_second_ts() -> int:
    records = _tail_records(ONE_SECOND_FILE, 1)
    if not records:
        return 0
    record = records[-1]
    return int(record.get("window_start_ts", record.get("ts", 0)) or 0)


def run_live() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("[REGIME] Starting live mode", flush=True)
    last_ts = 0
    while not HALT_FILE.exists():
        ts_ms = _latest_one_second_ts()
        if ts_ms and ts_ms != last_ts:
            context = build_context(ts_ms)
            with OUTPUT_FILE.open("a", encoding="utf-8") as output:
                output.write(json.dumps(context, separators=(",", ":")) + "\n")
            last_ts = ts_ms
            print(
                f"[REGIME] ts={ts_ms} {context.get('trend_regime')} "
                f"{context.get('volatility_class')} {context.get('session')}",
                flush=True,
            )
        time.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Market regime context engine")
    parser.add_argument("--mode", choices=["live"], default="live")
    parser.parse_args()
    run_live()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
