"""
NurtacCoreEngineClaude — Layer-1: Rolling Window Engine

Reads data/combined_1s_dna_btcusdt.jsonl produced by Layer-0 (main.py)
and produces sliding/overlapping rolling window DNA for 3S, 5S, 15S.

Run alongside Layer-0:
  Terminal 1: python3 main.py
  Terminal 2: python3 rolling_window_engine.py

Set FULL_PRINT=true for full JSON output instead of summary lines.
"""

import json
import os
import sys
import time
from collections import deque
from typing import Optional

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL      = "BTCUSDT"
INPUT_FILE  = os.path.join("data", "combined_1s_dna_btcusdt.jsonl")
OUTPUT_3S   = os.path.join("data", "rolling_3s_dna.jsonl")
OUTPUT_5S   = os.path.join("data", "rolling_5s_dna.jsonl")
OUTPUT_15S  = os.path.join("data", "rolling_15s_dna.jsonl")
DQ_LOG_FILE = os.path.join("data", "data_quality_log.jsonl")
HALT_FILE   = os.path.join("data", "SYSTEM_HALT")

WINDOW_SIZES   = [3, 5, 15]
OUTPUT_FILES   = {3: OUTPUT_3S, 5: OUTPUT_5S, 15: OUTPUT_15S}

FULL_PRINT         = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_INTERVAL      = 0.05   # seconds between readline retries when no new data
FILE_WAIT_INTERVAL = 0.5    # seconds between checks when input file doesn't exist yet


# ── SYSTEM_HALT check ────────────────────────────────────────────────────────
def _check_system_halt() -> None:
    if os.path.exists(HALT_FILE):
        try:
            with open(HALT_FILE, "r", encoding="utf-8") as fh:
                halt_info = json.loads(fh.read().strip())
            reason = halt_info.get("reason", "unknown")
        except Exception:
            reason = "unknown"
        print(f"SYSTEM_HALT tespit edildi, {reason}, program durduruluyor")
        return


# ── Data quality log ──────────────────────────────────────────────────────────
def _log_quality(event_type: str, detail: dict) -> None:
    entry = {
        "ts":         int(time.time() * 1000),
        "source":     "layer1",
        "event_type": event_type,
        "detail":     detail,
    }
    try:
        os.makedirs("data", exist_ok=True)
        with open(DQ_LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError:
        pass


# ── Pure helpers ──────────────────────────────────────────────────────────────
def _delta_state(delta: float) -> str:
    if delta > 0:
        return "positive"
    if delta < 0:
        return "negative"
    return "neutral"


# ── Rolling window builder ────────────────────────────────────────────────────
def build_rolling_output(window: list[dict], window_size: int) -> dict:
    """Aggregate window_size 1S combined DNA records into one rolling DNA object."""

    # ── OHLC ─────────────────────────────────────────────────────────────────
    open_val  = None
    close_val = None
    high_val  = None
    low_val   = None

    for rec in window:
        cdna = rec["candle_dna"]
        if not cdna["has_trade"]:
            continue

        if open_val is None:
            open_val = cdna["open"]
        close_val = cdna["close"]

        # Strictly greater — first occurrence wins on tie
        h_price = cdna["high"]["price"]
        if high_val is None or h_price > high_val["price"]:
            high_val = cdna["high"]

        # Strictly less — first occurrence wins on tie
        l_price = cdna["low"]["price"]
        if low_val is None or l_price < low_val["price"]:
            low_val = cdna["low"]

    # ── Volume ────────────────────────────────────────────────────────────────
    buy_vol   = sum(r["candle_dna"]["buy_volume"]  for r in window)
    sell_vol  = sum(r["candle_dna"]["sell_volume"] for r in window)
    total_vol = buy_vol + sell_vol
    delta     = buy_vol - sell_vol

    # ── Trade flow ────────────────────────────────────────────────────────────
    trade_count  = sum(r["candle_dna"]["trade_count"] for r in window)
    active_secs  = sum(1 for r in window if r["candle_dna"]["has_trade"])
    empty_secs   = window_size - active_secs
    avg_tps      = trade_count / window_size
    tc_values    = [r["candle_dna"]["trade_count"] for r in window]
    max_tc       = max(tc_values)
    min_tc       = min(tc_values)

    # ── Footprint aggregation (merge price levels across all 1S) ─────────────
    fp_map: dict[float, dict] = {}
    for rec in window:
        for lv in rec["footprint_dna"]["price_levels"]:
            p  = lv["price"]
            entry = fp_map.setdefault(p, {"buy_volume": 0.0, "sell_volume": 0.0, "trade_count": 0})
            entry["buy_volume"]  += lv["buy_volume"]
            entry["sell_volume"] += lv["sell_volume"]
            entry["trade_count"] += lv["trade_count"]

    price_levels: list[dict] = []
    for price in sorted(fp_map, reverse=True):
        lv = fp_map[price]
        bv = lv["buy_volume"]
        sv = lv["sell_volume"]
        tv = bv + sv
        d  = bv - sv
        price_levels.append({
            "price":        price,
            "buy_volume":   bv,
            "sell_volume":  sv,
            "total_volume": tv,
            "delta":        d,
            "delta_state":  _delta_state(d),
            "trade_count":  lv["trade_count"],
        })

    # ── Depth flow ────────────────────────────────────────────────────────────
    bid_count = sum(r["depth_dna"]["bid_update_count"] for r in window)
    ask_count = sum(r["depth_dna"]["ask_update_count"] for r in window)
    total_d   = bid_count + ask_count
    balance_d = bid_count - ask_count
    imbalance = balance_d / total_d if total_d else 0.0
    ratio_d   = bid_count / ask_count if ask_count else None

    if bid_count > ask_count:
        dominant = "bid"
    elif ask_count > bid_count:
        dominant = "ask"
    else:
        dominant = "neutral"

    # ── Micro behavior ────────────────────────────────────────────────────────
    delta_seq: list[float]          = [r["candle_dna"]["delta"] for r in window]
    tc_seq: list[int]               = tc_values
    dom_seq: list[str]              = [r["depth_dna"]["dominant_side"].lower() for r in window]

    price_seq: list[Optional[float]] = []
    for r in window:
        cdna = r["candle_dna"]
        if cdna["has_trade"]:
            price_seq.append(cdna["close"]["price"])
        else:
            price_seq.append(cdna.get("carry_forward_price"))

    # ── Source refs ───────────────────────────────────────────────────────────
    source_windows  = [r["window_start_ts"] for r in window]
    window_start_ts = window[0]["window_start_ts"]
    window_end_ts   = window[-1]["window_end_ts"]

    return {
        "symbol":              SYMBOL,
        "window_type":         f"{window_size}S",
        "window_size_seconds": window_size,
        "window_start_ts":     window_start_ts,
        "window_end_ts":       window_end_ts,
        "source_1s_count":     window_size,

        "ohlc": {
            "open":  open_val,
            "high":  high_val,
            "low":   low_val,
            "close": close_val,
        },

        "volume": {
            "buy_volume":   buy_vol,
            "sell_volume":  sell_vol,
            "total_volume": total_vol,
            "delta":        delta,
            "delta_state":  _delta_state(delta),
        },

        "trade_flow": {
            "trade_count":                trade_count,
            "active_seconds":             active_secs,
            "empty_seconds":              empty_secs,
            "avg_trade_count_per_second": avg_tps,
            "max_trade_count_1s":         max_tc,
            "min_trade_count_1s":         min_tc,
        },

        "footprint": {
            "price_levels": price_levels,
        },

        "depth_flow": {
            "bid_update_count": bid_count,
            "ask_update_count": ask_count,
            "dominant_side":    dominant,
            "balance":          balance_d,
            "imbalance":        imbalance,
            "ratio":            ratio_d,
        },

        "micro_behavior": {
            "delta_sequence":         delta_seq,
            "price_sequence":         price_seq,
            "trade_count_sequence":   tc_seq,
            "dominant_side_sequence": dom_seq,
        },

        "source_refs": {
            "first_1s_ts":    source_windows[0],
            "last_1s_ts":     source_windows[-1],
            "source_windows": source_windows,
        },
    }


# ── Validation ────────────────────────────────────────────────────────────────
def validate_output(obj: dict) -> list[str]:
    """Returns a list of error strings. Empty list means valid."""
    errors: list[str] = []
    n   = obj["source_1s_count"]
    wts = obj["window_start_ts"]
    vol = obj["volume"]
    tf  = obj["trade_flow"]
    mb  = obj["micro_behavior"]

    # [1] source_1s_count == window_size_seconds
    if n != obj["window_size_seconds"]:
        errors.append(
            f"[1] source_1s_count={n} != window_size_seconds={obj['window_size_seconds']} "
            f"at window_start_ts={wts}"
        )

    # [2] len(source_windows) == source_1s_count
    sw = obj["source_refs"]["source_windows"]
    if len(sw) != n:
        errors.append(
            f"[2] len(source_windows)={len(sw)} != source_1s_count={n} "
            f"at window_start_ts={wts}"
        )

    # [3] total_volume == buy_volume + sell_volume
    tv_check = vol["buy_volume"] + vol["sell_volume"]
    if abs(tv_check - vol["total_volume"]) > 1e-9:
        errors.append(
            f"[3] total_volume={vol['total_volume']} != buy+sell={tv_check} "
            f"at window_start_ts={wts}"
        )

    # [4] delta == buy_volume - sell_volume
    d_check = vol["buy_volume"] - vol["sell_volume"]
    if abs(d_check - vol["delta"]) > 1e-9:
        errors.append(
            f"[4] delta={vol['delta']} != buy-sell={d_check} "
            f"at window_start_ts={wts}"
        )

    # [5] Σ footprint buy_volume == volume.buy_volume
    fp_buy = sum(lv["buy_volume"] for lv in obj["footprint"]["price_levels"])
    if abs(fp_buy - vol["buy_volume"]) >= 1e-9:
        errors.append(
            f"[5] footprint buy_volume sum={fp_buy} != volume.buy_volume={vol['buy_volume']} "
            f"at window_start_ts={wts}"
        )

    # [6] Σ footprint sell_volume == volume.sell_volume
    fp_sell = sum(lv["sell_volume"] for lv in obj["footprint"]["price_levels"])
    if abs(fp_sell - vol["sell_volume"]) >= 1e-9:
        errors.append(
            f"[6] footprint sell_volume sum={fp_sell} != volume.sell_volume={vol['sell_volume']} "
            f"at window_start_ts={wts}"
        )

    # [7] active_seconds + empty_seconds == source_1s_count
    if tf["active_seconds"] + tf["empty_seconds"] != n:
        errors.append(
            f"[7] active_seconds={tf['active_seconds']} + empty_seconds={tf['empty_seconds']} "
            f"!= source_1s_count={n} at window_start_ts={wts}"
        )

    # [8–11] sequence lengths
    seq_checks = [
        (8,  "delta_sequence",         mb["delta_sequence"]),
        (9,  "price_sequence",         mb["price_sequence"]),
        (10, "trade_count_sequence",   mb["trade_count_sequence"]),
        (11, "dominant_side_sequence", mb["dominant_side_sequence"]),
    ]
    for check_num, field_name, seq in seq_checks:
        if len(seq) != n:
            errors.append(
                f"[{check_num}] len({field_name})={len(seq)} != source_1s_count={n} "
                f"at window_start_ts={wts}"
            )

    return errors


# ── File I/O ──────────────────────────────────────────────────────────────────
def _append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _print_summary(obj: dict) -> None:
    wt    = obj["window_type"]
    wst   = obj["window_start_ts"]
    wet   = obj["window_end_ts"]
    close = obj["ohlc"]["close"]
    close_price = close["price"] if close is not None else "null"
    tc    = obj["trade_flow"]["trade_count"]
    delta = obj["volume"]["delta"]
    lvls  = len(obj["footprint"]["price_levels"])
    dom   = obj["depth_flow"]["dominant_side"]
    print(
        f"[ROLLING {wt}] ts={wst}-{wet} close={close_price} "
        f"trades={tc} delta={delta} levels={lvls} dominant={dom}"
    )


# ── Tail generator ────────────────────────────────────────────────────────────
def follow_jsonl(path: str):
    """Yield parsed JSON objects from a JSONL file.

    Waits for the file to appear, reads all existing lines from the beginning,
    then follows new lines as they are written (tail -f behaviour).
    """
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


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    os.makedirs("data", exist_ok=True)
    _check_system_halt()
    _log_quality("engine_started", {"script": "rolling_window_engine.py"})

    print("NurtacCoreEngineClaude — Layer-1 Rolling Window Engine")
    print(f"Input : {INPUT_FILE}")
    print(f"Output: {OUTPUT_3S}, {OUTPUT_5S}, {OUTPUT_15S}")
    print(f"FULL_PRINT={'true' if FULL_PRINT else 'false'}")
    print()

    # Single shared buffer; maxlen=15 covers all three window sizes
    buf: deque[dict] = deque(maxlen=15)
    last_ts: Optional[int] = None

    for record in follow_jsonl(INPUT_FILE):
        _check_system_halt()
        ts = record["window_start_ts"]

        # Gap detection: source timestamps must be consecutive 1-second steps
        if last_ts is not None and ts != last_ts + 1000:
            gap_s = (ts - last_ts) // 1000 - 1
            print(
                f"[GAP] Source gap detected: prev_ts={last_ts} curr_ts={ts} "
                f"missing={gap_s}s"
            )
            _log_quality("gap_detected", {
                "prev_ts":       last_ts,
                "curr_ts":       ts,
                "gap_seconds":   gap_s,
            })
        last_ts = ts

        buf.append(record)
        n = len(buf)

        for window_size in WINDOW_SIZES:
            if n < window_size:
                continue

            window = list(buf)[-window_size:]
            obj    = build_rolling_output(window, window_size)
            errors = validate_output(obj)

            if errors:
                print(
                    f"[VALIDATION FAIL] {obj['window_type']} "
                    f"window_start_ts={obj['window_start_ts']}"
                )
                for err in errors:
                    print(f"  {err}")
                _log_quality("gap_detected", {
                    "window_type":     obj["window_type"],
                    "window_start_ts": obj["window_start_ts"],
                    "errors":          errors,
                })
                continue

            _append_jsonl(OUTPUT_FILES[window_size], obj)

            if FULL_PRINT:
                print(json.dumps(obj, indent=2, ensure_ascii=False))
            else:
                _print_summary(obj)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down.")
