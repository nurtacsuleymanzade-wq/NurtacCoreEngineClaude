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
import math
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
    trade_buffer:     list = field(default_factory=list)
    depth_buffer:     list = field(default_factory=list)
    trade_connected:  bool = False
    depth_connected:  bool = False
    first_trade_seen: bool = False
    first_depth_seen: bool = False
    last_close_price: Optional[float] = None


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
        open_t = trades[0]

        # Scan all trades including open for high/low; first occurrence wins on tie
        high_price = open_t["price"]
        low_price  = open_t["price"]
        high_t     = open_t
        low_t      = open_t
        buy_vol    = 0.0
        sell_vol   = 0.0

        for t in trades:
            p = t["price"]
            if p > high_price:
                high_price, high_t = p, t
            if p < low_price:
                low_price, low_t   = p, t
            if t["side"] is TradeSide.BUY:
                buy_vol  += t["qty"]
            else:
                sell_vol += t["qty"]

        total_vol = buy_vol + sell_vol
        d         = buy_vol - sell_vol
        close_t   = trades[-1]

        return {
            "symbol":              SYMBOL,
            "window_start_ts":     window_start_ts,
            "window_end_ts":       window_end_ts,
            "open":  {"price": open_t["price"],  "time": open_t["time_ms"],  "side": open_t["side"].value},
            "high":  {"price": high_t["price"],  "time": high_t["time_ms"],  "side": high_t["side"].value},
            "low":   {"price": low_t["price"],   "time": low_t["time_ms"],   "side": low_t["side"].value},
            "close": {"price": close_t["price"], "time": close_t["time_ms"], "side": close_t["side"].value},
            "trade_count":         len(trades),
            "buy_volume":          buy_vol,
            "sell_volume":         sell_vol,
            "total_volume":        total_vol,
            "delta":               d,
            "delta_state":         _delta_state(d),
            "last_trade_price":    close_t["price"],
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
                    async with _lock:
                        _state.trade_buffer.append(ev)
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
                    msg = json.loads(raw)
                    ev  = {
                        "time_ms": int(msg["E"]),
                        "bids":    msg["b"],
                        "asks":    msg["a"],
                    }
                    async with _lock:
                        _state.depth_buffer.append(ev)
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


# ── 1-second drift-compensated scheduler ─────────────────────────────────────
async def _scheduler() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    # Block until at least one trade AND one depth event have been received
    print("Warm-up: waiting for first trade and depth event...")
    while True:
        async with _lock:
            ready = _state.first_trade_seen and _state.first_depth_seen
        if ready:
            break
        await asyncio.sleep(0.05)
    print("Warm-up complete. Starting 1-second scheduler.")

    # Align to next whole-second boundary; scheduler self-corrects for drift
    now       = time.time()
    next_tick = math.ceil(now)
    await asyncio.sleep(next_tick - now)

    while True:
        current_tick = next_tick
        next_tick    = current_tick + 1.0

        window_end_ts   = int(round(current_tick * 1000))
        window_start_ts = window_end_ts - 1000

        # Atomically snapshot and drain both buffers
        async with _lock:
            trades = list(_state.trade_buffer)
            depths = list(_state.depth_buffer)
            _state.trade_buffer.clear()
            _state.depth_buffer.clear()
            t_disc = not _state.trade_connected
            d_disc = not _state.depth_connected
            carry  = _state.last_close_price

        candle = build_candle_dna(trades, window_start_ts, window_end_ts, carry, t_disc)

        if candle is None:
            # Theoretically unreachable after warm-up; skip silently
            await asyncio.sleep(max(0.0, next_tick - time.time()))
            continue

        # Update carry-forward price for future no-trade windows
        if candle["has_trade"]:
            async with _lock:
                _state.last_close_price = candle["close"]["price"]

        footprint = build_footprint_dna(trades, window_start_ts, window_end_ts)
        depth     = build_depth_dna(depths, window_start_ts, window_end_ts, d_disc)
        combined  = build_combined_dna(
            candle, footprint, depth,
            window_start_ts, window_end_ts,
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

        # Drift-compensated sleep until next boundary
        await asyncio.sleep(max(0.0, next_tick - time.time()))


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
