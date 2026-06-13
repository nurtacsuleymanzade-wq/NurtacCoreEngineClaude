"""
NurtacCoreEngineClaude — Core Engine Layer

Streams Binance USDⓈ-M Futures live data from two WebSocket endpoints
and produces four DNA objects per 1-second window:
  • Candle DNA
  • Footprint DNA
  • Depth DNA
  • Combined 1S DNA

Known limitation: The depth@100ms stream measures the *flow* of order book
updates, not the full order book state. True order book state requires a
REST snapshot + incremental diff sync — this is out of scope for this layer.

Note: Prices and quantities arrive from Binance as strings and are cast to
float. For precision-critical downstream use, migrate to Decimal.
Float is acceptable for this MVP layer.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import websockets

# ── Constants ────────────────────────────────────────────────────────────────
SYMBOL        = "BTCUSDT"
TRADE_WS_URL  = "wss://fstream.binance.com/ws/btcusdt@trade"
DEPTH_WS_URL  = "wss://fstream.binance.com/ws/btcusdt@depth@100ms"

DATA_DIR       = "data"
CANDLE_FILE    = os.path.join(DATA_DIR, "candle_dna_btcusdt.jsonl")
FOOTPRINT_FILE = os.path.join(DATA_DIR, "footprint_dna_btcusdt.jsonl")
DEPTH_FILE     = os.path.join(DATA_DIR, "depth_dna_btcusdt.jsonl")
COMBINED_FILE  = os.path.join(DATA_DIR, "combined_1s_dna_btcusdt.jsonl")

# Exponential backoff in seconds; last value is used for all subsequent retries
BACKOFF_DELAYS = [1, 2, 4, 8, 16, 30]

# Grace period: ms after window_end_ts before finalizing (absorbs network jitter)
GRACE_PERIOD_MS = 300


# ── Side enums ───────────────────────────────────────────────────────────────
# Trade side (BUY/SELL) and depth side (BID/ASK/NEUTRAL) are intentionally
# separate enums — no shared "side" helper function is used.

class TradeSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class DepthSide(str, Enum):
    BID     = "BID"
    ASK     = "ASK"
    NEUTRAL = "NEUTRAL"


# ── Shared state ─────────────────────────────────────────────────────────────
@dataclass
class SharedState:
    # Per-event-time buckets keyed by window_start_ts.
    # window_start_ts = floor(event_time_ms / 1000) * 1000
    buckets:           dict  = field(default_factory=dict)
    # Recently-finalized window_start_ts values for late-event detection.
    # Pruned to the last 30 seconds on each finalization.
    finalized_windows: set   = field(default_factory=set)
    trade_connected:   bool  = False
    depth_connected:   bool  = False
    first_trade_seen:  bool  = False
    first_depth_seen:  bool  = False
    last_close_price:  Optional[float] = None


_state = SharedState()
_lock  = asyncio.Lock()


# ── Pure computation helpers ──────────────────────────────────────────────────
def _delta_state(delta: float) -> str:
    if delta > 0:
        return "positive"
    if delta < 0:
        return "negative"
    return "neutral"


def _parse_trade_event(msg: dict) -> dict:
    """Parse a raw Binance trade WebSocket message into internal format.

    m=true  → buyer is maker → taker is SELLER (aggressive sell)
    m=false → seller is maker → taker is BUYER  (aggressive buy)
    """
    return {
        "price":   float(msg["p"]),
        "qty":     float(msg["q"]),
        "time_ms": int(msg["T"]),
        "side":    TradeSide.SELL if msg["m"] else TradeSide.BUY,
    }


# ── DNA builders ─────────────────────────────────────────────────────────────
def build_candle_dna(
    trades: list[dict],
    window_start_ts: int,
    window_end_ts: int,
    carry_forward_price: Optional[float],
    stream_disconnected: bool,
) -> Optional[dict]:
    """
    Returns None only when no trade has ever been seen and the window
    is empty (pre-warm-up edge case). After warm-up this never returns None.
    """
    has_trade = bool(trades)

    if has_trade:
        # Initialize all OHLC fields as None — never 0.0.
        # The first valid trade (price > 0, guaranteed by stream-level filter)
        # atomically sets open/high/low/close. Subsequent trades update close
        # and conditionally update high/low. No intermediate 0.0 state exists.
        open_v  = None
        high_v  = None
        low_v   = None
        close_v = None
        buy_vol  = 0.0
        sell_vol = 0.0

        for t in trades:
            p  = t["price"]
            ev = {"price": p, "time": t["time_ms"], "side": t["side"].value}

            if open_v is None:
                # First trade: open, high, low all start from this same event
                open_v = high_v = low_v = ev
            else:
                if p > high_v["price"]:
                    high_v = ev
                if p < low_v["price"]:
                    low_v  = ev

            close_v = ev  # every trade updates close

            if t["side"] is TradeSide.BUY:
                buy_vol  += t["qty"]
            else:
                sell_vol += t["qty"]

        total_vol = buy_vol + sell_vol
        d         = buy_vol - sell_vol

        # Validation: prices must be > 0 (should always pass after stream-level filter)
        if (open_v is None or open_v["price"] <= 0
                or high_v["price"] <= 0
                or low_v["price"]  <= 0
                or close_v["price"] <= 0):
            print(f"[WARN] Candle validation failed: non-positive price in OHLC "
                  f"at window_start_ts={window_start_ts}")

        return {
            "symbol":              SYMBOL,
            "window_start_ts":     window_start_ts,
            "window_end_ts":       window_end_ts,
            "open":                open_v,
            "high":                high_v,
            "low":                 low_v,
            "close":               close_v,
            "trade_count":         len(trades),
            "buy_volume":          buy_vol,
            "sell_volume":         sell_vol,
            "total_volume":        total_vol,
            "delta":               d,
            "delta_state":         _delta_state(d),
            "last_trade_price":    close_v["price"],
            "has_trade":           True,
            "carry_forward":       False,
            "carry_forward_price": None,
            "stream_disconnected": stream_disconnected,
        }

    # No trades in this window
    if carry_forward_price is None:
        # System has never seen a trade; output suppressed (pre-warm-up only)
        return None

    return {
        "symbol":              SYMBOL,
        "window_start_ts":     window_start_ts,
        "window_end_ts":       window_end_ts,
        "open":  None,
        "high":  None,
        "low":   None,
        "close": None,
        "trade_count":         0,
        "buy_volume":          0.0,
        "sell_volume":         0.0,
        "total_volume":        0.0,
        "delta":               0.0,
        "delta_state":         "neutral",
        "last_trade_price":    None,
        "has_trade":           False,
        "carry_forward":       True,
        "carry_forward_price": carry_forward_price,
        "stream_disconnected": stream_disconnected,
    }


def build_footprint_dna(
    trades: list[dict],
    window_start_ts: int,
    window_end_ts: int,
) -> dict:
    has_trade    = bool(trades)
    price_levels: list[dict] = []

    if has_trade:
        lm: dict[float, dict] = {}
        for t in trades:
            p  = t["price"]
            lv = lm.setdefault(p, {"buy_volume": 0.0, "sell_volume": 0.0, "trade_count": 0})
            lv["trade_count"] += 1
            if t["side"] is TradeSide.BUY:
                lv["buy_volume"]  += t["qty"]
            else:
                lv["sell_volume"] += t["qty"]

        for price in sorted(lm, reverse=True):
            lv = lm[price]
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

    return {
        "symbol":          SYMBOL,
        "window_start_ts": window_start_ts,
        "window_end_ts":   window_end_ts,
        "has_trade":       has_trade,
        "price_levels":    price_levels,
    }


def build_depth_dna(
    depth_events: list[dict],
    window_start_ts: int,
    window_end_ts: int,
    stream_disconnected: bool,
) -> dict:
    has_depth = bool(depth_events)
    bid_count = ask_count = 0
    last_bid  = last_ask  = None

    for ev in depth_events:
        bids = ev["bids"]
        asks = ev["asks"]
        bid_count += len(bids)
        ask_count += len(asks)
        if bids:
            last_bid = {
                "price": float(bids[-1][0]),
                "qty":   float(bids[-1][1]),
                "time":  ev["time_ms"],
            }
        if asks:
            last_ask = {
                "price": float(asks[-1][0]),
                "qty":   float(asks[-1][1]),
                "time":  ev["time_ms"],
            }

    total     = bid_count + ask_count
    balance   = bid_count - ask_count
    imbalance = balance / total if total else 0.0
    ratio     = bid_count / ask_count if ask_count else None

    if bid_count > ask_count:
        dominant = DepthSide.BID.value
    elif ask_count > bid_count:
        dominant = DepthSide.ASK.value
    else:
        dominant = DepthSide.NEUTRAL.value

    return {
        "symbol":              SYMBOL,
        "window_start_ts":     window_start_ts,
        "window_end_ts":       window_end_ts,
        "has_depth":           has_depth,
        "bid_update_count":    bid_count,
        "ask_update_count":    ask_count,
        "last_bid_update":     last_bid,
        "last_ask_update":     last_ask,
        "dominant_side":       dominant,
        "balance":             balance,
        "imbalance":           imbalance,
        "ratio":               ratio,
        "stream_disconnected": stream_disconnected,
    }


def build_combined_dna(
    candle: dict,
    footprint: dict,
    depth: dict,
    window_start_ts: int,
    window_end_ts: int,
    stream_disconnected: bool,
) -> dict:
    # Fields that live only at the top level of combined; stripped from sub-objects
    _excl_candle = {"symbol", "window_start_ts", "window_end_ts", "stream_disconnected"}
    _excl_fp     = {"symbol", "window_start_ts", "window_end_ts"}
    _excl_depth  = {"symbol", "window_start_ts", "window_end_ts", "stream_disconnected"}

    return {
        "symbol":              SYMBOL,
        "window_start_ts":     window_start_ts,
        "window_end_ts":       window_end_ts,
        "stream_disconnected": stream_disconnected,
        "candle_dna":    {k: v for k, v in candle.items()    if k not in _excl_candle},
        "footprint_dna": {k: v for k, v in footprint.items() if k not in _excl_fp},
        "depth_dna":     {k: v for k, v in depth.items()     if k not in _excl_depth},
    }


# ── File I/O ──────────────────────────────────────────────────────────────────
def _append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _print_dna(label: str, obj: dict) -> None:
    sep = "-" * 54
    print(f"\n{sep}\n{label}\n{sep}")
    print(json.dumps(obj, indent=2, ensure_ascii=False))


# ── WebSocket streams with exponential backoff ────────────────────────────────
async def _run_trade_stream() -> None:
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(TRADE_WS_URL) as ws:
                async with _lock:
                    _state.trade_connected = True
                backoff_idx = 0
                print("[Trade] Connected.")
                async for raw in ws:
                    ev = _parse_trade_event(json.loads(raw))
                    if ev["price"] <= 0:
                        # Skip anomalous zero/negative price events (Binance data guard)
                        continue
                    # Bucket by event's own timestamp T, not by arrival time
                    wts = (ev["time_ms"] // 1000) * 1000
                    async with _lock:
                        if wts in _state.finalized_windows:
                            print(
                                f"[LATE EVENT] Trade skipped: "
                                f"event_time={ev['time_ms']}, "
                                f"window_start_ts={wts} already finalized"
                            )
                        else:
                            bucket = _state.buckets.setdefault(
                                wts, {"trades": [], "depths": []}
                            )
                            bucket["trades"].append(ev)
                            if not _state.first_trade_seen:
                                _state.first_trade_seen = True
                                print("[Trade] First event received.")
        except Exception as exc:
            async with _lock:
                _state.trade_connected = False
            delay = BACKOFF_DELAYS[min(backoff_idx, len(BACKOFF_DELAYS) - 1)]
            print(f"[Trade] Disconnected ({exc}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
            backoff_idx = min(backoff_idx + 1, len(BACKOFF_DELAYS) - 1)


async def _run_depth_stream() -> None:
    backoff_idx = 0
    while True:
        try:
            async with websockets.connect(DEPTH_WS_URL) as ws:
                async with _lock:
                    _state.depth_connected = True
                backoff_idx = 0
                print("[Depth] Connected.")
                async for raw in ws:
                    msg     = json.loads(raw)
                    time_ms = int(msg["E"])
                    ev      = {"time_ms": time_ms, "bids": msg["b"], "asks": msg["a"]}
                    # Bucket by event's own timestamp E, not by arrival time
                    wts = (time_ms // 1000) * 1000
                    async with _lock:
                        if wts in _state.finalized_windows:
                            print(
                                f"[LATE EVENT] Depth skipped: "
                                f"event_time={time_ms}, "
                                f"window_start_ts={wts} already finalized"
                            )
                        else:
                            bucket = _state.buckets.setdefault(
                                wts, {"trades": [], "depths": []}
                            )
                            bucket["depths"].append(ev)
                            if not _state.first_depth_seen:
                                _state.first_depth_seen = True
                                print("[Depth] First event received.")
        except Exception as exc:
            async with _lock:
                _state.depth_connected = False
            delay = BACKOFF_DELAYS[min(backoff_idx, len(BACKOFF_DELAYS) - 1)]
            print(f"[Depth] Disconnected ({exc}). Retrying in {delay}s...")
            await asyncio.sleep(delay)
            backoff_idx = min(backoff_idx + 1, len(BACKOFF_DELAYS) - 1)


# ── Event-time-bucketed scheduler ────────────────────────────────────────────
async def _scheduler() -> None:
    """Finalize windows based on event timestamps, not wall-clock drain time.

    Each 50 ms poll finds all pending buckets whose grace period has expired
    (wall_clock >= window_end_ts + GRACE_PERIOD_MS) and emits them in order.
    Gap windows between consecutive emitted windows are filled with carry-forward
    records so the JSONL stream remains gapless.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    # Block until at least one trade AND one depth event have been received
    print("Warm-up: waiting for first trade and depth event...")
    while True:
        async with _lock:
            ready = _state.first_trade_seen and _state.first_depth_seen
        if ready:
            break
        await asyncio.sleep(0.05)
    print("Warm-up complete. Starting event-time scheduler.")

    # Tracks the last window_start_ts that was successfully finalized.
    # Once set, all subsequent windows (including gaps) are emitted in order.
    last_finalized_ts: Optional[int] = None

    while True:
        await asyncio.sleep(0.05)

        now_ms = int(time.time() * 1000)

        # Highest window_start_ts whose grace period has fully expired:
        #   wts + 1000 + GRACE_PERIOD_MS <= now_ms
        #   wts <= now_ms - 1000 - GRACE_PERIOD_MS
        check_to = ((now_ms - 1000 - GRACE_PERIOD_MS) // 1000) * 1000

        async with _lock:
            bucket_keys = set(_state.buckets.keys())
            t_disc = not _state.trade_connected
            d_disc = not _state.depth_connected

        if last_finalized_ts is not None:
            # Emit every window from (last_finalized_ts + 1000) up to check_to.
            # Windows with no bucket entry become carry-forward records.
            check_from = last_finalized_ts + 1000
            windows_to_finalize = (
                list(range(check_from, check_to + 1000, 1000))
                if check_to >= check_from else []
            )
        else:
            # First pass: start from the earliest ready bucket and fill
            # any gaps between ready buckets (no gap-filling before the first
            # bucket since there is no prior context).
            ready_buckets = sorted(wts for wts in bucket_keys if wts <= check_to)
            if ready_buckets:
                windows_to_finalize = list(
                    range(ready_buckets[0], ready_buckets[-1] + 1000, 1000)
                )
            else:
                windows_to_finalize = []

        for wts in windows_to_finalize:
            window_end_ts = wts + 1000

            async with _lock:
                # Pop the bucket (returns empty dict if it was a gap window)
                bucket = _state.buckets.pop(wts, {"trades": [], "depths": []})
                # Mark as finalized for late-event detection
                _state.finalized_windows.add(wts)
                # Prune the finalized set to the last 30 seconds
                cutoff = wts - 30_000
                _state.finalized_windows = {
                    w for w in _state.finalized_windows if w > cutoff
                }
                carry  = _state.last_close_price
                t_disc = not _state.trade_connected
                d_disc = not _state.depth_connected

            trades = bucket["trades"]
            depths = bucket["depths"]

            candle = build_candle_dna(trades, wts, window_end_ts, carry, t_disc)
            if candle is None:
                # Pre-warm-up gap; no carry price yet — skip silently
                last_finalized_ts = wts
                continue

            if candle["has_trade"]:
                async with _lock:
                    _state.last_close_price = candle["close"]["price"]

            footprint = build_footprint_dna(trades, wts, window_end_ts)
            depth     = build_depth_dna(depths, wts, window_end_ts, d_disc)
            combined  = build_combined_dna(
                candle, footprint, depth,
                wts, window_end_ts,
                t_disc or d_disc,
            )

            _print_dna("CANDLE DNA",      candle)
            _print_dna("FOOTPRINT DNA",   footprint)
            _print_dna("DEPTH DNA",       depth)
            _print_dna("COMBINED 1S DNA", combined)

            _append_jsonl(CANDLE_FILE,    candle)
            _append_jsonl(FOOTPRINT_FILE, footprint)
            _append_jsonl(DEPTH_FILE,     depth)
            _append_jsonl(COMBINED_FILE,  combined)

            last_finalized_ts = wts


# ── Entry point ───────────────────────────────────────────────────────────────
async def main() -> None:
    await asyncio.gather(
        _run_trade_stream(),
        _run_depth_stream(),
        _scheduler(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down.")
