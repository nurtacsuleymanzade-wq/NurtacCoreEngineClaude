"""
NurtacCoreEngineClaude — Layer-2: Aligned Candle Engine

Reads data/combined_1s_dna_btcusdt.jsonl (Layer-0 output) and produces
UTC-aligned, non-overlapping candle DNA for 1M, 5M, 15M, 1H, 4H, 1D.

Hierarchy (each level closes and feeds the next):
  1S (Layer-0) -> 1M -> 5M -> 15M -> 1H -> 4H -> 1D

Run alongside Layer-0 and optionally Layer-1:
  Terminal 1: python3 main.py
  Terminal 2: python3 rolling_window_engine.py   # optional
  Terminal 3: python3 aligned_candle_engine.py
"""

import json
import os
import time
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
INPUT_FILE = os.path.join("data", "combined_1s_dna_btcusdt.jsonl")

OUTPUT_FILES = {
    "1M":  os.path.join("data", "aligned_1m_candle_dna.jsonl"),
    "5M":  os.path.join("data", "aligned_5m_candle_dna.jsonl"),
    "15M": os.path.join("data", "aligned_15m_candle_dna.jsonl"),
    "1H":  os.path.join("data", "aligned_1h_candle_dna.jsonl"),
    "4H":  os.path.join("data", "aligned_4h_candle_dna.jsonl"),
    "1D":  os.path.join("data", "aligned_1d_candle_dna.jsonl"),
}

# How many source units close each timeframe
EXPECTED_COUNT = {"1M": 60, "5M": 5, "15M": 3, "1H": 4, "4H": 4, "1D": 6}

# Millisecond duration of each timeframe
TIMEFRAME_MS = {
    "1M":  60_000,
    "5M":  300_000,
    "15M": 900_000,
    "1H":  3_600_000,
    "4H":  14_400_000,
    "1D":  86_400_000,
}

# Source timeframe label for each output timeframe
SOURCE_TF = {
    "1M": "1S", "5M": "1M", "15M": "5M", "1H": "15M", "4H": "1H", "1D": "4H",
}

# Bucket divisor: floor(source_ts / div) * div → output bucket start
BUCKET_DIV = {
    "1M":  60_000,
    "5M":  300_000,
    "15M": 900_000,
    "1H":  3_600_000,
    "4H":  14_400_000,
    "1D":  86_400_000,
}

# Cascade chain: which timeframe feeds which next
_NEXT_TF = {"1M": "5M", "5M": "15M", "15M": "1H", "1H": "4H", "4H": "1D"}

FULL_PRINT         = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_INTERVAL      = 0.05
FILE_WAIT_INTERVAL = 0.5


# ── Pure helpers ──────────────────────────────────────────────────────────────
def _delta_state(delta: float) -> str:
    if delta > 0:
        return "positive"
    if delta < 0:
        return "negative"
    return "neutral"


def _make_price_levels(fp_map: dict) -> list:
    result = []
    for price in sorted(fp_map, reverse=True):
        e  = fp_map[price]
        bv = e["buy_volume"]
        sv = e["sell_volume"]
        tv = bv + sv
        d  = bv - sv
        result.append({
            "price":        price,
            "buy_volume":   bv,
            "sell_volume":  sv,
            "total_volume": tv,
            "delta":        d,
            "delta_state":  _delta_state(d),
            "trade_count":  e["trade_count"],
        })
    return result


def _aggregate_fp(units: list, fp_key: str) -> list:
    """Merge price_levels from a list of source units.
    fp_key is 'footprint_dna' for 1S sources, 'footprint' for aligned-candle sources.
    """
    fp_map: dict = {}
    for u in units:
        for lv in u[fp_key]["price_levels"]:
            p = lv["price"]
            e = fp_map.setdefault(p, {"buy_volume": 0.0, "sell_volume": 0.0, "trade_count": 0})
            e["buy_volume"]  += lv["buy_volume"]
            e["sell_volume"] += lv["sell_volume"]
            e["trade_count"] += lv["trade_count"]
    return _make_price_levels(fp_map)


def _make_depth_flow(bid: int, ask: int) -> dict:
    total   = bid + ask
    balance = bid - ask
    return {
        "bid_update_count": bid,
        "ask_update_count": ask,
        "dominant_side": "bid" if bid > ask else ("ask" if ask > bid else "neutral"),
        "balance":       balance,
        "imbalance":     balance / total if total else 0.0,
        "ratio":         bid / ask if ask else None,
    }


# ── Profile: POC / VAH / VAL / HVN / LVN ─────────────────────────────────────
def _compute_profile(price_levels: list, close_price: Optional[float]) -> dict:
    if not price_levels:
        return {
            "poc": None, "vah": None, "val": None,
            "value_area_volume": 0.0, "value_area_ratio": 0.70,
            "hvn": [], "lvn": [],
        }

    total_vol = sum(lv["total_volume"] for lv in price_levels)

    # POC: max total_volume; tie -> closest to close (null -> highest price)
    max_vol    = max(lv["total_volume"] for lv in price_levels)
    candidates = [lv for lv in price_levels if lv["total_volume"] == max_vol]
    if len(candidates) == 1:
        poc_lv = candidates[0]
    elif close_price is not None:
        poc_lv = min(candidates, key=lambda lv: abs(lv["price"] - close_price))
    else:
        poc_lv = candidates[0]  # already highest price (list is descending)
    poc_price = poc_lv["price"]

    # Value area (70%) — price_levels sorted descending; index 0 = highest price
    poc_idx = next(i for i, lv in enumerate(price_levels) if lv["price"] == poc_price)
    va_vol  = poc_lv["total_volume"]
    target  = total_vol * 0.70
    hi_idx  = poc_idx
    lo_idx  = poc_idx
    va_set  = {poc_idx}

    while va_vol < target:
        can_up   = hi_idx > 0
        can_down = lo_idx < len(price_levels) - 1
        if not can_up and not can_down:
            break

        up_vol   = price_levels[hi_idx - 1]["total_volume"] if can_up   else -1.0
        down_vol = price_levels[lo_idx + 1]["total_volume"] if can_down else -1.0

        if up_vol > down_vol:
            hi_idx -= 1
            va_set.add(hi_idx)
            va_vol += up_vol
        elif down_vol > up_vol:
            lo_idx += 1
            va_set.add(lo_idx)
            va_vol += down_vol
        else:
            # Tie: pick closer to close; null or equal distance -> upper (higher price)
            if can_up and can_down:
                up_p   = price_levels[hi_idx - 1]["price"]
                down_p = price_levels[lo_idx + 1]["price"]
                if close_price is None or abs(up_p - close_price) <= abs(down_p - close_price):
                    hi_idx -= 1; va_set.add(hi_idx); va_vol += up_vol
                else:
                    lo_idx += 1; va_set.add(lo_idx); va_vol += down_vol
            elif can_up:
                hi_idx -= 1; va_set.add(hi_idx); va_vol += up_vol
            else:
                lo_idx += 1; va_set.add(lo_idx); va_vol += down_vol

    va_prices = [price_levels[i]["price"] for i in va_set]
    vah, val  = max(va_prices), min(va_prices)

    avg_vol = total_vol / len(price_levels)
    hvn = [{"price": lv["price"], "total_volume": lv["total_volume"]}
           for lv in price_levels if lv["total_volume"] >= avg_vol * 1.5]
    lvn = [{"price": lv["price"], "total_volume": lv["total_volume"]}
           for lv in price_levels if lv["total_volume"] <= avg_vol * 0.5]

    return {
        "poc": poc_price, "vah": vah, "val": val,
        "value_area_volume": va_vol, "value_area_ratio": 0.70,
        "hvn": hvn, "lvn": lvn,
    }


# ── Candle builders ───────────────────────────────────────────────────────────
def _build_1m_candle(records: list, bucket: int) -> dict:
    """Build aligned 1M candle from exactly 60 1S combined DNA records."""
    # OHLC — source: candle_dna; "has_trade unit" = has_trade == True
    open_v = close_v = high_v = low_v = None
    for r in records:
        cd = r["candle_dna"]
        if not cd["has_trade"]:
            continue
        if open_v is None:
            open_v = cd["open"]
        close_v = cd["close"]
        if high_v is None or cd["high"]["price"] > high_v["price"]:
            high_v = cd["high"]
        if low_v  is None or cd["low"]["price"]  < low_v["price"]:
            low_v  = cd["low"]

    # Volume — source: candle_dna
    bv = sum(r["candle_dna"]["buy_volume"]  for r in records)
    sv = sum(r["candle_dna"]["sell_volume"] for r in records)
    tv = bv + sv
    d  = bv - sv

    # Trade flow — source: candle_dna
    tc  = sum(r["candle_dna"]["trade_count"] for r in records)
    act = sum(1 for r in records if r["candle_dna"]["has_trade"])
    tcs = [r["candle_dna"]["trade_count"] for r in records]

    # Footprint — source: footprint_dna
    pl = _aggregate_fp(records, "footprint_dna")

    # Depth — source: depth_dna (has_depth=false -> counts are 0, already stored as 0)
    bid = sum(r["depth_dna"]["bid_update_count"] for r in records)
    ask = sum(r["depth_dna"]["ask_update_count"] for r in records)

    cp = close_v["price"] if close_v is not None else None

    return {
        "symbol":           SYMBOL,
        "timeframe":        "1M",
        "window_start_ts":  bucket,
        "window_end_ts":    bucket + TIMEFRAME_MS["1M"],
        "source_count":     60,
        "source_timeframe": "1S",
        "ohlc": {"open": open_v, "high": high_v, "low": low_v, "close": close_v},
        "volume": {
            "buy_volume":   bv,
            "sell_volume":  sv,
            "total_volume": tv,
            "delta":        d,
            "delta_state":  _delta_state(d),
        },
        "trade_flow": {
            "trade_count":               tc,
            "active_units":              act,
            "empty_units":               60 - act,
            "avg_trade_count_per_unit":  tc / 60,
            "max_trade_count_unit":      max(tcs),
            "min_trade_count_unit":      min(tcs),
        },
        "footprint": {"price_levels": pl},
        "profile":   _compute_profile(pl, cp),
        "depth_flow": _make_depth_flow(bid, ask),
        "source_refs": {
            "source_window_start_ts": [r["window_start_ts"] for r in records],
            "source_timeframe": "1S",
        },
    }


def _build_higher_candle(units: list, timeframe: str, bucket: int) -> dict:
    """Build aligned 5M/15M/1H/4H/1D candle from lower aligned candles."""
    src_tf   = SOURCE_TF[timeframe]
    expected = EXPECTED_COUNT[timeframe]

    # "has_trade unit" for 5M+ = ohlc.close != null
    # OHLC — source: unit.ohlc; first/last active wins
    open_v = close_v = high_v = low_v = None
    for u in units:
        if u["ohlc"]["close"] is None:
            continue
        if open_v is None:
            open_v = u["ohlc"]["open"]
        close_v = u["ohlc"]["close"]
        h = u["ohlc"]["high"]
        l = u["ohlc"]["low"]
        if h is not None and (high_v is None or h["price"] > high_v["price"]):
            high_v = h
        if l is not None and (low_v  is None or l["price"] < low_v["price"]):
            low_v  = l

    # Volume — source: unit.volume
    bv = sum(u["volume"]["buy_volume"]  for u in units)
    sv = sum(u["volume"]["sell_volume"] for u in units)
    tv = bv + sv
    d  = bv - sv

    # Trade flow — source: unit.trade_flow; active = ohlc.close != null
    tc  = sum(u["trade_flow"]["trade_count"] for u in units)
    act = sum(1 for u in units if u["ohlc"]["close"] is not None)
    tcs = [u["trade_flow"]["trade_count"] for u in units]

    # Footprint — source: unit.footprint.price_levels
    pl = _aggregate_fp(units, "footprint")

    # Depth — source: unit.depth_flow
    bid = sum(u["depth_flow"]["bid_update_count"] for u in units)
    ask = sum(u["depth_flow"]["ask_update_count"] for u in units)

    cp = close_v["price"] if close_v is not None else None

    return {
        "symbol":           SYMBOL,
        "timeframe":        timeframe,
        "window_start_ts":  bucket,
        "window_end_ts":    bucket + TIMEFRAME_MS[timeframe],
        "source_count":     expected,
        "source_timeframe": src_tf,
        "ohlc": {"open": open_v, "high": high_v, "low": low_v, "close": close_v},
        "volume": {
            "buy_volume":   bv,
            "sell_volume":  sv,
            "total_volume": tv,
            "delta":        d,
            "delta_state":  _delta_state(d),
        },
        "trade_flow": {
            "trade_count":               tc,
            "active_units":              act,
            "empty_units":               expected - act,
            "avg_trade_count_per_unit":  tc / expected,
            "max_trade_count_unit":      max(tcs),
            "min_trade_count_unit":      min(tcs),
        },
        "footprint": {"price_levels": pl},
        "profile":   _compute_profile(pl, cp),
        "depth_flow": _make_depth_flow(bid, ask),
        "source_refs": {
            "source_window_start_ts": [u["window_start_ts"] for u in units],
            "source_timeframe": src_tf,
        },
    }


# ── Validation ────────────────────────────────────────────────────────────────
def _validate(obj: dict) -> list[str]:
    errors   = []
    tf       = obj["timeframe"]
    wts      = obj["window_start_ts"]
    expected = EXPECTED_COUNT[tf]
    vol      = obj["volume"]
    tflow    = obj["trade_flow"]
    prof     = obj["profile"]
    ohlc     = obj["ohlc"]

    if obj["source_count"] != expected:
        errors.append(f"[1] source_count={obj['source_count']} != {expected} at ts={wts}")

    sw = obj["source_refs"]["source_window_start_ts"]
    if len(sw) != expected:
        errors.append(f"[2] len(source_window_start_ts)={len(sw)} != {expected} at ts={wts}")

    if abs(vol["buy_volume"] + vol["sell_volume"] - vol["total_volume"]) > 1e-9:
        errors.append(f"[3] total_volume mismatch at ts={wts}")

    if abs(vol["buy_volume"] - vol["sell_volume"] - vol["delta"]) > 1e-9:
        errors.append(f"[4] delta mismatch at ts={wts}")

    fp_buy = sum(lv["buy_volume"] for lv in obj["footprint"]["price_levels"])
    if abs(fp_buy - vol["buy_volume"]) >= 1e-9:
        errors.append(f"[5] footprint buy_volume sum mismatch at ts={wts}")

    fp_sell = sum(lv["sell_volume"] for lv in obj["footprint"]["price_levels"])
    if abs(fp_sell - vol["sell_volume"]) >= 1e-9:
        errors.append(f"[6] footprint sell_volume sum mismatch at ts={wts}")

    if tflow["active_units"] + tflow["empty_units"] != expected:
        errors.append(f"[7] active_units+empty_units != source_count at ts={wts}")

    if ohlc["high"] is not None and ohlc["low"] is not None:
        if ohlc["high"]["price"] < ohlc["low"]["price"]:
            errors.append(f"[8] high.price < low.price at ts={wts}")

    if prof["poc"] is not None:
        prices = {lv["price"] for lv in obj["footprint"]["price_levels"]}
        if prof["poc"] not in prices:
            errors.append(f"[9] poc={prof['poc']} not in price_levels at ts={wts}")

    if prof["vah"] is not None and prof["val"] is not None:
        if prof["vah"] < prof["val"]:
            errors.append(f"[10] vah={prof['vah']} < val={prof['val']} at ts={wts}")

    expected_end = wts + TIMEFRAME_MS[tf]
    if obj["window_end_ts"] != expected_end:
        errors.append(f"[11] window_end_ts={obj['window_end_ts']} != {expected_end} at ts={wts}")

    return errors


# ── File I/O ──────────────────────────────────────────────────────────────────
def _append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _print_summary(obj: dict) -> None:
    tf    = obj["timeframe"]
    close = obj["ohlc"]["close"]
    cp    = close["price"] if close is not None else "null"
    prof  = obj["profile"]
    print(
        f"[ALIGNED {tf}] ts={obj['window_start_ts']}-{obj['window_end_ts']} "
        f"close={cp} trades={obj['trade_flow']['trade_count']} "
        f"delta={obj['volume']['delta']} "
        f"poc={prof['poc']} vah={prof['vah']} val={prof['val']}"
    )


# ── File tail generator ───────────────────────────────────────────────────────
def _follow_jsonl(path: str):
    while not os.path.exists(path):
        print(f"Waiting for {path} to appear...")
        time.sleep(FILE_WAIT_INTERVAL)
    print(f"Opening {path} — reading history then following live updates...")
    with open(path, "r", encoding="utf-8") as fh:
        while True:
            line = fh.readline()
            if line:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"[WARN] Skipping malformed JSON line: {exc}")
            else:
                time.sleep(POLL_INTERVAL)


# ── Aligned candle engine (hierarchical state machine) ────────────────────────
class AlignedCandleEngine:
    """
    State per timeframe: {bucket, buffer}
    - 1M: fills from 1S records; closes when new minute arrives with full 60-record buffer
    - 5M+: fills from lower aligned candles; closes immediately when count is reached
    Partial periods (startup) are discarded silently.
    """

    def __init__(self) -> None:
        self._s = {tf: {"bucket": None, "buffer": []} for tf in EXPECTED_COUNT}

    def on_1s(self, record: dict) -> None:
        ts     = record["window_start_ts"]
        bucket = (ts // BUCKET_DIV["1M"]) * BUCKET_DIV["1M"]
        state  = self._s["1M"]

        if state["bucket"] is None:
            state["bucket"] = bucket
            state["buffer"].append(record)
            return

        if bucket == state["bucket"]:
            state["buffer"].append(record)
            return

        # Minute boundary crossed — check if previous buffer is complete
        if len(state["buffer"]) == EXPECTED_COUNT["1M"]:
            self._emit("1M", state["buffer"], state["bucket"])
        # else: partial minute at startup → discard silently

        state["bucket"] = bucket
        state["buffer"] = [record]

    def _feed(self, timeframe: str, unit: dict) -> None:
        """Feed a completed lower-level candle into the accumulator for timeframe."""
        ts     = unit["window_start_ts"]
        bucket = (ts // BUCKET_DIV[timeframe]) * BUCKET_DIV[timeframe]
        state  = self._s[timeframe]

        if state["bucket"] is None or bucket != state["bucket"]:
            # New period — discard any partial buffer from previous period
            state["bucket"] = bucket
            state["buffer"] = []

        state["buffer"].append(unit)

        if len(state["buffer"]) == EXPECTED_COUNT[timeframe]:
            self._emit(timeframe, state["buffer"], bucket)
            state["buffer"] = []
            state["bucket"] = None

    def _emit(self, timeframe: str, buffer: list, bucket: int) -> None:
        """Build, validate, write, print, and cascade a completed candle."""
        obj = (_build_1m_candle(buffer, bucket) if timeframe == "1M"
               else _build_higher_candle(buffer, timeframe, bucket))

        errors = _validate(obj)
        if errors:
            print(f"[VALIDATION FAIL] {timeframe} window_start_ts={bucket}")
            for e in errors:
                print(f"  {e}")
            return

        _append_jsonl(OUTPUT_FILES[timeframe], obj)
        if FULL_PRINT:
            print(json.dumps(obj, indent=2, ensure_ascii=False))
        else:
            _print_summary(obj)

        # Cascade to next timeframe
        next_tf = _NEXT_TF.get(timeframe)
        if next_tf:
            self._feed(next_tf, obj)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    os.makedirs("data", exist_ok=True)
    print("NurtacCoreEngineClaude — Layer-2 Aligned Candle Engine")
    print(f"Input : {INPUT_FILE}")
    print(f"FULL_PRINT={'true' if FULL_PRINT else 'false'}")
    print()

    engine = AlignedCandleEngine()
    for record in _follow_jsonl(INPUT_FILE):
        engine.on_1s(record)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down.")
