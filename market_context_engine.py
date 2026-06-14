"""
NurtacCoreEngineClaude — Market Context Engine

Binance Public API (no API key) → bias context for BTCUSDT futures.
REST:  OI, Funding Rate, L/S Ratios, Taker Volume (via klines)
WS:    Liquidation stream (!forceOrder@arr)
Local: data/combined_1s_dna_btcusdt.jsonl (futures delta)

Outputs:
  data/market_context.jsonl
  data/liquidation_events.jsonl
  data/liquidation_heatmap.jsonl
  data/bias_context.jsonl

Rules:
  - Only Binance Public API — no API key required
  - No mock data, no orders
  - Bias/context output only — no signals
"""

import argparse
import asyncio
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ───────────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"

MARKET_CTX_FILE   = DATA_DIR / "market_context.jsonl"
LIQ_EVENTS_FILE   = DATA_DIR / "liquidation_events.jsonl"
LIQ_HEATMAP_FILE  = DATA_DIR / "liquidation_heatmap.jsonl"
BIAS_CTX_FILE     = DATA_DIR / "bias_context.jsonl"
QUALITY_LOG_FILE  = DATA_DIR / "data_quality_log.jsonl"
PRIMARY_1S_FILE   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"

OI_INTERVAL      = 30    # seconds
RATE_INTERVAL    = 300   # 5 min
HEATMAP_INTERVAL = 60    # seconds
CASCADE_WINDOW   = 30_000  # ms
CASCADE_MIN      = 3
HEATMAP_WINDOW   = 3600   # seconds (1 hour)
PRICE_LEVEL      = 10.0   # $ granularity for heatmap

BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS   = "wss://fstream.binance.com/ws/!forceOrder@arr"

_last_req_ts = 0.0  # monotonic time of last REST request

# ── Helpers ───────────────────────────────────────────────────────────────────────
def _sf(v, d: float = 0.0) -> float:
    if v is None:
        return d
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else d
    except (TypeError, ValueError):
        return d

def _write_jsonl(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

def _log_quality(msg: str) -> None:
    try:
        ts = int(time.time() * 1000)
        rec = {"engine": "market_context_engine", "ts": ts, "event": "api_error", "detail": msg}
        with open(QUALITY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError:
        pass

def _tail_file(path: Path, n: int) -> list[str]:
    """Return last n non-empty lines from a file (efficient chunked read)."""
    CHUNK = 65536
    lines: list[str] = []
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            offset = max(0, size - CHUNK * max(1, n // 60))
            f.seek(offset)
            buf = f.read().decode("utf-8", errors="replace")
            for line in reversed(buf.splitlines()):
                line = line.strip()
                if line:
                    lines.append(line)
                    if len(lines) >= n:
                        break
    except OSError:
        return []
    return list(reversed(lines))

# ── REST helper ───────────────────────────────────────────────────────────────────
async def _rest_get(url: str, timeout: int = 10) -> dict | list | None:
    global _last_req_ts
    elapsed = time.monotonic() - _last_req_ts
    if elapsed < 0.1:
        await asyncio.sleep(0.1 - elapsed)

    def _do() -> dict | list | str | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                return "RATE_LIMIT"
            _log_quality(f"HTTP {e.code} {url}")
            return None
        except Exception as ex:
            _log_quality(f"request failed {url}: {ex}")
            return None

    _last_req_ts = time.monotonic()
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _do)

    if result == "RATE_LIMIT":
        print(f"[MCE] Rate limit — sleeping 60s", flush=True)
        await asyncio.sleep(60)
        return await _rest_get(url, timeout)
    return result

# ── State ─────────────────────────────────────────────────────────────────────────
class MktState:
    def __init__(self):
        # OI
        self.oi_prev:    float | None = None
        self.oi_curr:    float | None = None
        self.oi_trend    = "oi_neutral"
        self.oi_long     = 0.0
        self.oi_short    = 0.0
        self.oi_chg_pct  = 0.0
        # Funding
        self.funding_rate  = 0.0
        self.funding_state = "neutral"
        # L/S
        self.global_ls_ratio     = 0.5
        self.top_trader_ls_ratio = 0.5
        self.global_state        = "balanced"
        self.top_trader_state    = "neutral"
        # Taker (from klines)
        self.taker_buy_vol  = 0.0
        self.taker_sell_vol = 0.0
        self.taker_buy_ratio = 0.5
        self.taker_state     = "balanced"
        # Futures delta
        self.futures_delta_60s = 0.0
        self.current_price     = 0.0
        self.price_chg_pct     = 0.0
        self.alignment         = "diverging"
        # Liquidations
        self.liq_history: deque = deque()   # (ts, side, price, qty) — last 1h
        self.liq_recent:  deque = deque()   # last 30s
        self.cascade_detected  = False
        self.cascade_direction: str | None = None
        # Heatmap
        self.max_pain_price:       float | None = None
        self.dist_to_max_pain_pct: float | None = None
        self.heatmap_bias   = "balanced"
        self._heatmap_levels: list[dict] = []

# ── 1S file reader ────────────────────────────────────────────────────────────────
def _read_1s_data() -> tuple[float, float, float]:
    """Returns (futures_delta_60s, current_price, price_change_pct)."""
    lines = _tail_file(PRIMARY_1S_FILE, 62)
    if not lines:
        return 0.0, 0.0, 0.0
    delta_sum = 0.0
    closes: list[float] = []
    for raw in lines:
        try:
            rec  = json.loads(raw)
            cdna = rec.get("candle_dna") or {}
            delta_sum += _sf(cdna.get("delta"), 0.0)
            cp = _sf((cdna.get("close") or {}).get("price"), 0.0)
            if cp > 0:
                closes.append(cp)
        except Exception:
            pass
    cur  = closes[-1] if closes else 0.0
    prev = closes[-2] if len(closes) >= 2 else cur
    pc   = ((cur - prev) / prev * 100) if prev > 0 else 0.0
    return delta_sum, cur, pc

# ── REST polling functions ────────────────────────────────────────────────────────
async def _poll_oi(state: MktState) -> None:
    url  = f"{BINANCE_REST}/fapi/v1/openInterest?symbol={SYMBOL}"
    data = await _rest_get(url)
    if not isinstance(data, dict):
        return
    oi = _sf(data.get("openInterest"), 0.0)
    if oi <= 0:
        return
    state.oi_prev = state.oi_curr
    state.oi_curr = oi
    if state.oi_prev is None:
        return
    chg = (oi - state.oi_prev) / state.oi_prev * 100 if state.oi_prev > 0 else 0.0
    state.oi_chg_pct = chg
    delta, cur, pc = _read_1s_data()
    state.futures_delta_60s = delta
    state.current_price     = cur
    state.price_chg_pct     = pc
    if abs(chg) <= 0.1:
        state.oi_trend, state.oi_long, state.oi_short = "oi_neutral", 0.0, 0.0
    elif chg > 0.1:
        if pc > 0:
            state.oi_trend, state.oi_long, state.oi_short = "oi_rising_price_rising",  1.0, 0.0
        else:
            state.oi_trend, state.oi_long, state.oi_short = "oi_rising_price_falling",  0.0, 1.0
    else:
        if pc > 0:
            state.oi_trend, state.oi_long, state.oi_short = "oi_falling_price_rising",  0.5, 0.0
        else:
            state.oi_trend, state.oi_long, state.oi_short = "oi_falling_price_falling", 0.0, 0.5

async def _poll_funding(state: MktState) -> None:
    url  = f"{BINANCE_REST}/fapi/v1/fundingRate?symbol={SYMBOL}&limit=1"
    data = await _rest_get(url)
    if not isinstance(data, list) or not data:
        return
    fr = _sf(data[0].get("fundingRate"), 0.0)
    # Clamp to [-1, 1] (spec rule 4)
    if abs(fr) > 1.0:
        _log_quality(f"funding_rate out of range: {fr}, clamping")
        fr = max(-1.0, min(1.0, fr))
    state.funding_rate = fr

async def _poll_ls_ratios(state: MktState) -> None:
    urls = [
        f"{BINANCE_REST}/futures/data/globalLongShortAccountRatio"
        f"?symbol={SYMBOL}&period=5m&limit=1",
        f"{BINANCE_REST}/futures/data/topLongShortAccountRatio"
        f"?symbol={SYMBOL}&period=5m&limit=1",
    ]
    for url, attr in zip(urls, ["global_ls_ratio", "top_trader_ls_ratio"]):
        data = await _rest_get(url)
        if not isinstance(data, list) or not data:
            continue
        val = _sf(data[0].get("longAccount"), 0.5)
        # Clamp to [0, 1] (spec rule 5)
        val = max(0.0, min(1.0, val))
        setattr(state, attr, val)

async def _poll_taker(state: MktState) -> None:
    """Get taker buy/sell from klines (takerBuySellVol endpoint deprecated)."""
    url  = f"{BINANCE_REST}/fapi/v1/klines?symbol={SYMBOL}&interval=5m&limit=1"
    data = await _rest_get(url)
    if not isinstance(data, list) or not data:
        return
    kl = data[0]
    try:
        total_vol    = _sf(kl[5],  0.0)
        taker_buy    = _sf(kl[9],  0.0)
        taker_sell   = max(0.0, total_vol - taker_buy)
        state.taker_buy_vol  = taker_buy
        state.taker_sell_vol = taker_sell
        total = taker_buy + taker_sell
        # Clamp to [0, 1] (spec rule 6)
        state.taker_buy_ratio = max(0.0, min(1.0, taker_buy / total if total > 0 else 0.5))
    except (IndexError, TypeError):
        pass

# ── Liquidation handler ───────────────────────────────────────────────────────────
def _handle_liq(state: MktState, side: str, price: float, qty: float,
                ts: int, liq_fh) -> None:
    if price <= 0 or qty <= 0:
        return
    event = {
        "engine": "market_context_engine", "symbol": SYMBOL,
        "ts": ts, "liq_side": side, "liq_price": price, "liq_qty": qty,
    }
    _write_jsonl(liq_fh, event)

    # History (1 hour)
    state.liq_history.append((ts, side, price, qty))
    cutoff_h = ts - HEATMAP_WINDOW * 1000
    while state.liq_history and state.liq_history[0][0] < cutoff_h:
        state.liq_history.popleft()

    # Recent (30s for cascade)
    state.liq_recent.append((ts, side, price, qty))
    cutoff_c = ts - CASCADE_WINDOW
    while state.liq_recent and state.liq_recent[0][0] < cutoff_c:
        state.liq_recent.popleft()

    sell_n = sum(1 for _, s, _, _ in state.liq_recent if s == "SELL")
    buy_n  = sum(1 for _, s, _, _ in state.liq_recent if s == "BUY")

    if sell_n >= CASCADE_MIN and state.cascade_direction != "short":
        state.cascade_detected  = True
        state.cascade_direction = "short"
        tot = sum(q for _, s, _, q in state.liq_recent if s == "SELL")
        print(f"[LIQ CASCADE] ts={ts//1000} direction=SHORT "
              f"qty={tot:.3f} price={price:.1f}\n"
              f"  {sell_n} long liquidations in 30s", flush=True)
    elif buy_n >= CASCADE_MIN and state.cascade_direction != "long":
        state.cascade_detected  = True
        state.cascade_direction = "long"
        tot = sum(q for _, s, _, q in state.liq_recent if s == "BUY")
        print(f"[LIQ CASCADE] ts={ts//1000} direction=LONG "
              f"qty={tot:.3f} price={price:.1f}\n"
              f"  {buy_n} short liquidations in 30s", flush=True)
    elif sell_n < CASCADE_MIN and buy_n < CASCADE_MIN:
        state.cascade_detected  = False
        state.cascade_direction = None

# ── Heatmap ───────────────────────────────────────────────────────────────────────
def _build_heatmap(state: MktState, now_ts: int) -> tuple[str, float | None, float | None]:
    """Return (heatmap_bias, max_pain, dist_pct). Updates state._heatmap_levels."""
    cutoff = now_ts - HEATMAP_WINDOW * 1000
    levels: dict[float, dict] = defaultdict(lambda: {
        "long_liq_count": 0, "long_liq_vol": 0.0,
        "short_liq_count": 0, "short_liq_vol": 0.0, "total_liq_vol": 0.0,
    })
    for ts, side, price, qty in state.liq_history:
        if ts < cutoff:
            continue
        lv = math.floor(price / PRICE_LEVEL) * PRICE_LEVEL
        if side == "SELL":
            levels[lv]["long_liq_count"] += 1
            levels[lv]["long_liq_vol"]   += qty
        else:
            levels[lv]["short_liq_count"] += 1
            levels[lv]["short_liq_vol"]   += qty
        levels[lv]["total_liq_vol"] += qty

    state._heatmap_levels = [
        {"level": lv, **d} for lv, d in sorted(levels.items())
    ]

    if not levels:
        return "balanced", None, None

    max_pain = max(levels, key=lambda lv: levels[lv]["total_liq_vol"])
    total_long  = sum(v["long_liq_vol"]  for v in levels.values())
    total_short = sum(v["short_liq_vol"] for v in levels.values())

    if total_long > total_short * 1.5:
        bias = "long_heavy"
    elif total_short > total_long * 1.5:
        bias = "short_heavy"
    else:
        bias = "balanced"

    dist = None
    if state.current_price > 0:
        dist = round((state.current_price - max_pain) / max_pain * 100, 4)

    return bias, max_pain, dist

# ── Bias computation ───────────────────────────────────────────────────────────────
def _compute_bias(state: MktState, now_ts: int) -> dict | None:
    """Compute and return bias_context record. Returns None on validation failure."""
    long_b  = 0.0
    short_b = 0.0

    # OI contribution
    long_b  += state.oi_long
    short_b += state.oi_short

    # Funding
    fr = state.funding_rate
    if fr > 0.0005:
        fs, fl, fsh = "extreme_positive", 0.0, 1.0
    elif fr > 0.0001:
        fs, fl, fsh = "positive",         0.0, 0.5
    elif fr < -0.0005:
        fs, fl, fsh = "extreme_negative", 1.0, 0.0
    elif fr < -0.0001:
        fs, fl, fsh = "negative",         0.5, 0.0
    else:
        fs, fl, fsh = "neutral",          0.0, 0.0
    state.funding_state = fs
    long_b  += fl
    short_b += fsh

    # Global L/S (contrarian)
    gls = state.global_ls_ratio
    if gls > 0.60:
        g_st, gl, gs = "crowded_long",  0.0, 0.5
    elif gls < 0.40:
        g_st, gl, gs = "crowded_short", 0.5, 0.0
    else:
        g_st, gl, gs = "balanced",      0.0, 0.0
    state.global_state = g_st

    # Top trader L/S (follow)
    tls = state.top_trader_ls_ratio
    if tls > 0.65:
        t_st, tl, ts_ = "smart_long",  1.0, 0.0
    elif tls < 0.35:
        t_st, tl, ts_ = "smart_short", 0.0, 1.0
    else:
        t_st, tl, ts_ = "neutral",     0.0, 0.0
    state.top_trader_state = t_st

    ls_long  = gl + tl
    ls_short = gs + ts_
    long_b  += ls_long
    short_b += ls_short

    # Taker
    tbr = state.taker_buy_ratio
    if tbr > 0.60:
        tk_st, tkl, tks = "aggressive_buy",  1.0, 0.0
    elif tbr > 0.55:
        tk_st, tkl, tks = "mild_buy",        0.5, 0.0
    elif tbr < 0.40:
        tk_st, tkl, tks = "aggressive_sell", 0.0, 1.0
    elif tbr < 0.45:
        tk_st, tkl, tks = "mild_sell",       0.0, 0.5
    else:
        tk_st, tkl, tks = "balanced",        0.0, 0.0
    state.taker_state = tk_st
    long_b  += tkl
    short_b += tks

    # Spot-Futures alignment
    fd = state.futures_delta_60s
    if fd > 0 and tbr > 0.55:
        aln, sfl, sfs = "aligned_bullish", 1.0, 0.0
    elif fd < 0 and tbr < 0.45:
        aln, sfl, sfs = "aligned_bearish", 0.0, 1.0
    else:
        aln, sfl, sfs = "diverging",       0.0, 0.0
    state.alignment = aln
    long_b  += sfl
    short_b += sfs

    # Liquidation (recent 30s)
    cutoff = now_ts - CASCADE_WINDOW
    recent = [(t, s, p, q) for t, s, p, q in state.liq_recent if t >= cutoff]
    sell_n = sum(1 for _, s, _, _ in recent if s == "SELL")
    buy_n  = sum(1 for _, s, _, _ in recent if s == "BUY")
    liq_l = min(buy_n  * 0.3, 1.5)
    liq_s = min(sell_n * 0.3, 1.5)
    if state.cascade_detected:
        if state.cascade_direction == "long":
            liq_l += 1.0
        elif state.cascade_direction == "short":
            liq_s += 1.0
    long_b  += liq_l
    short_b += liq_s

    # Heatmap
    hm_bias, max_pain, dist = _build_heatmap(state, now_ts)
    state.heatmap_bias         = hm_bias
    state.max_pain_price       = max_pain
    state.dist_to_max_pain_pct = dist

    hm_l = 0.0
    hm_s = 0.0
    if hm_bias == "short_heavy":
        hm_l = 0.5
    elif hm_bias == "long_heavy":
        hm_s = 0.5
    if dist is not None:
        if dist > 0.3:
            hm_s += 0.3
        elif dist < -0.3:
            hm_l += 0.3
    long_b  += hm_l
    short_b += hm_s

    long_b  = max(0.0, round(long_b,  4))
    short_b = max(0.0, round(short_b, 4))
    gap     = round(abs(long_b - short_b), 4)

    if long_b > short_b and gap >= 1.0:
        dominant = "long"
    elif short_b > long_b and gap >= 1.0:
        dominant = "short"
    else:
        dominant = "neutral"

    oi_chg = 0.0
    if state.oi_prev and state.oi_prev > 0 and state.oi_curr:
        oi_chg = round((state.oi_curr - state.oi_prev) / state.oi_prev * 100, 6)

    rec = {
        "engine":        "market_context_engine",
        "symbol":        SYMBOL,
        "ts":            now_ts,
        "long_bias":     long_b,
        "short_bias":    short_b,
        "dominant_bias": dominant,
        "bias_gap":      gap,
        "components": {
            "oi": {
                "oi_value":           state.oi_curr,
                "oi_change_pct":      oi_chg,
                "oi_trend":           state.oi_trend,
                "long_contribution":  state.oi_long,
                "short_contribution": state.oi_short,
            },
            "funding": {
                "funding_rate":       round(fr, 8),
                "funding_state":      fs,
                "long_contribution":  fl,
                "short_contribution": fsh,
            },
            "long_short_ratio": {
                "global_ls_ratio":     round(gls, 4),
                "top_trader_ls_ratio": round(tls, 4),
                "global_state":        g_st,
                "top_trader_state":    t_st,
                "long_contribution":   ls_long,
                "short_contribution":  ls_short,
            },
            "taker_volume": {
                "taker_buy_ratio":    round(tbr, 4),
                "taker_state":        tk_st,
                "long_contribution":  tkl,
                "short_contribution": tks,
            },
            "spot_futures": {
                "futures_delta_60s":  round(fd, 6),
                "alignment":          aln,
                "long_contribution":  sfl,
                "short_contribution": sfs,
            },
            "liquidation": {
                "recent_long_liqs":   buy_n,
                "recent_short_liqs":  sell_n,
                "cascade_detected":   state.cascade_detected,
                "cascade_direction":  state.cascade_direction,
                "long_contribution":  round(liq_l, 4),
                "short_contribution": round(liq_s, 4),
            },
            "heatmap": {
                "max_pain_price":           max_pain,
                "distance_to_max_pain_pct": dist,
                "heatmap_bias":             hm_bias,
                "long_contribution":        round(hm_l, 4),
                "short_contribution":       round(hm_s, 4),
            },
        },
        "market_context": {
            "oi_value":            state.oi_curr,
            "funding_rate":        round(fr, 8),
            "global_ls_ratio":     round(gls, 4),
            "top_trader_ls_ratio": round(tls, 4),
            "taker_buy_ratio":     round(tbr, 4),
            "max_pain_price":      max_pain,
        },
    }

    errs = _validate_bias(rec)
    if errs:
        print(f"[MCE] BIAS VALIDATION ERROR: {errs}", flush=True)
        return None
    return rec

# ── Validation ─────────────────────────────────────────────────────────────────────
def _validate_bias(rec: dict) -> list[str]:
    errors: list[str] = []
    lb = _sf(rec.get("long_bias"),  -1.0)
    sb = _sf(rec.get("short_bias"), -1.0)
    if lb < 0 or lb != lb or abs(lb) == float("inf"):
        errors.append(f"[1] long_bias invalid: {lb}")
    if sb < 0 or sb != sb or abs(sb) == float("inf"):
        errors.append(f"[1] short_bias invalid: {sb}")
    if rec.get("dominant_bias") not in ("long", "short", "neutral"):
        errors.append(f"[2] dominant_bias invalid: {rec.get('dominant_bias')}")
    exp_gap = round(abs(lb - sb), 4)
    if abs(_sf(rec.get("bias_gap"), -1) - exp_gap) > 1e-3:
        errors.append(f"[3] bias_gap mismatch: {rec.get('bias_gap')} vs {exp_gap}")
    gls = _sf((rec.get("components") or {}).get("long_short_ratio", {}).get("global_ls_ratio"), -1)
    if not (0.0 <= gls <= 1.0):
        errors.append(f"[5] global_ls_ratio out of range: {gls}")
    tbr = _sf((rec.get("components") or {}).get("taker_volume", {}).get("taker_buy_ratio"), -1)
    if not (0.0 <= tbr <= 1.0):
        errors.append(f"[6] taker_buy_ratio out of range: {tbr}")
    for lv in (rec.get("components") or {}).get("heatmap", {}).get("price_levels") or []:
        if _sf(lv.get("total_liq_vol"), 0.0) < 0:
            errors.append(f"[7] negative total_liq_vol at level {lv.get('level')}")
    return errors

# ── Heatmap snapshot writer ────────────────────────────────────────────────────────
def _write_heatmap(state: MktState, now_ts: int, hm_fh) -> None:
    hm_bias, max_pain, dist = _build_heatmap(state, now_ts)
    levels = state._heatmap_levels
    if max_pain is not None:
        # Validate rule 8: max_pain_price must be in price_levels
        level_keys = {lv["level"] for lv in levels}
        if max_pain not in level_keys:
            print(f"[MCE] HEATMAP ERROR: max_pain {max_pain} not in levels", flush=True)
            return
    rec = {
        "ts":                    now_ts,
        "symbol":                SYMBOL,
        "window_seconds":        HEATMAP_WINDOW,
        "current_price":         state.current_price,
        "max_pain_price":        max_pain,
        "distance_to_max_pain_pct": dist,
        "price_levels":          levels,
        "heatmap_bias":          hm_bias,
    }
    _write_jsonl(hm_fh, rec)

# ── Market context snapshot writer ────────────────────────────────────────────────
def _write_market_ctx(state: MktState, now_ts: int, ctx_fh) -> None:
    rec = {
        "engine":             "market_context_engine",
        "symbol":             SYMBOL,
        "ts":                 now_ts,
        "oi_value":           state.oi_curr,
        "oi_change_pct":      round(state.oi_chg_pct, 6),
        "funding_rate":       round(state.funding_rate, 8),
        "global_ls_ratio":    round(state.global_ls_ratio, 4),
        "top_trader_ls_ratio": round(state.top_trader_ls_ratio, 4),
        "taker_buy_ratio":    round(state.taker_buy_ratio, 4),
        "taker_buy_vol":      round(state.taker_buy_vol, 4),
        "taker_sell_vol":     round(state.taker_sell_vol, 4),
        "futures_delta_60s":  round(state.futures_delta_60s, 6),
        "current_price":      state.current_price,
    }
    _write_jsonl(ctx_fh, rec)

# ── Terminal output ────────────────────────────────────────────────────────────────
def _maybe_print_bias(rec: dict) -> None:
    dom = rec.get("dominant_bias", "neutral")
    if dom == "neutral" and not FULL_PRINT:
        return
    ts   = rec["ts"] // 1000
    lb   = rec["long_bias"]
    sb   = rec["short_bias"]
    gap  = rec["bias_gap"]
    comp = rec.get("components") or {}
    oi   = comp.get("oi", {}).get("oi_trend", "?")
    fr   = comp.get("funding", {}).get("funding_state", "?")
    tt   = comp.get("long_short_ratio", {}).get("top_trader_state", "?")
    tk   = comp.get("taker_volume", {}).get("taker_state", "?")
    cas  = comp.get("liquidation", {}).get("cascade_direction")
    mp   = comp.get("heatmap", {}).get("max_pain_price")
    dist = comp.get("heatmap", {}).get("distance_to_max_pain_pct")
    mp_s = f"{mp:.0f}" if mp else "null"
    dist_s = f"{dist:.2f}%" if dist is not None else "null"
    print(f"[MARKET CTX] ts={ts} LONG_BIAS={lb:.2f} SHORT_BIAS={sb:.2f} gap={gap:.2f}",
          flush=True)
    print(f"  oi={oi} funding={fr} ls={tt} taker={tk} "
          f"cascade={cas} maxpain={mp_s} dist={dist_s}", flush=True)
    if FULL_PRINT:
        print(json.dumps(rec, indent=2), flush=True)

# ── Batch mode ─────────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[MCE] Batch mode — polling Binance APIs once", flush=True)
    state = MktState()

    async def _batch_inner():
        await _poll_funding(state)
        await _poll_ls_ratios(state)
        await _poll_taker(state)
        await _poll_oi(state)  # also reads 1S file
        now_ts = int(time.time() * 1000)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with (open(MARKET_CTX_FILE,  "a", encoding="utf-8") as ctx_fh,
              open(LIQ_EVENTS_FILE,  "a", encoding="utf-8") as liq_fh,
              open(LIQ_HEATMAP_FILE, "a", encoding="utf-8") as hm_fh,
              open(BIAS_CTX_FILE,    "a", encoding="utf-8") as bias_fh):

            _write_market_ctx(state, now_ts, ctx_fh)
            _write_heatmap(state, now_ts, hm_fh)

            bias_rec = _compute_bias(state, now_ts)
            if bias_rec:
                _write_jsonl(bias_fh, bias_rec)
                _maybe_print_bias(bias_rec)
            else:
                print("[MCE] Batch: bias validation failed", flush=True)

        print(f"[MCE] Batch done — OI={state.oi_curr} FR={state.funding_rate:.8f} "
              f"gLS={state.global_ls_ratio:.4f} ttLS={state.top_trader_ls_ratio:.4f} "
              f"taker={state.taker_buy_ratio:.4f}", flush=True)

    asyncio.run(_batch_inner())

# ── Live mode ──────────────────────────────────────────────────────────────────────
async def _oi_loop(state: MktState, ctx_fh, bias_fh) -> None:
    while True:
        if HALT_FILE.exists():
            return
        await _poll_oi(state)
        now_ts = int(time.time() * 1000)
        _write_market_ctx(state, now_ts, ctx_fh)
        bias_rec = _compute_bias(state, now_ts)
        if bias_rec:
            _write_jsonl(bias_fh, bias_rec)
            _maybe_print_bias(bias_rec)
        await asyncio.sleep(OI_INTERVAL)

async def _rate_loop(state: MktState) -> None:
    await asyncio.sleep(5)  # slight offset
    while True:
        if HALT_FILE.exists():
            return
        await _poll_funding(state)
        await _poll_ls_ratios(state)
        await _poll_taker(state)
        await asyncio.sleep(RATE_INTERVAL)

async def _heatmap_loop(state: MktState, hm_fh) -> None:
    while True:
        if HALT_FILE.exists():
            return
        await asyncio.sleep(HEATMAP_INTERVAL)
        now_ts = int(time.time() * 1000)
        if state.liq_history:
            _write_heatmap(state, now_ts, hm_fh)

async def _liq_ws_task(state: MktState, liq_fh) -> None:
    try:
        import websockets as _ws
    except ImportError:
        print("[MCE] websockets not installed — liquidation WS disabled", flush=True)
        return

    backoff = 1
    while True:
        if HALT_FILE.exists():
            return
        try:
            async with _ws.connect(
                BINANCE_WS, ping_interval=20, ping_timeout=10
            ) as ws:
                print("[MCE] Liquidation WebSocket connected", flush=True)
                backoff = 1
                async for msg_raw in ws:
                    if HALT_FILE.exists():
                        return
                    try:
                        data = json.loads(msg_raw)
                        events = data if isinstance(data, list) else [data]
                        for ev in events:
                            if not isinstance(ev, dict):
                                continue
                            order = ev.get("o") or {}
                            if order.get("s") != SYMBOL:
                                continue
                            side  = order.get("S", "")
                            price = _sf(order.get("ap") or order.get("p"), 0.0)
                            qty   = _sf(order.get("l") or order.get("q"), 0.0)
                            ts    = int(ev.get("E", time.time() * 1000))
                            _handle_liq(state, side, price, qty, ts, liq_fh)
                    except Exception:
                        pass
        except Exception as ex:
            print(f"[MCE] WS error: {ex} — reconnect in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

async def _run_live() -> None:
    state = MktState()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Initial poll before opening files
    await _poll_funding(state)
    await _poll_ls_ratios(state)
    await _poll_taker(state)
    print("[MCE] Initial polls done — starting live loops", flush=True)

    with (open(MARKET_CTX_FILE,  "a", encoding="utf-8") as ctx_fh,
          open(LIQ_EVENTS_FILE,  "a", encoding="utf-8") as liq_fh,
          open(LIQ_HEATMAP_FILE, "a", encoding="utf-8") as hm_fh,
          open(BIAS_CTX_FILE,    "a", encoding="utf-8") as bias_fh):

        tasks = [
            asyncio.create_task(_oi_loop(state, ctx_fh, bias_fh),   name="mce-oi"),
            asyncio.create_task(_rate_loop(state),                   name="mce-rate"),
            asyncio.create_task(_heatmap_loop(state, hm_fh),         name="mce-heatmap"),
            asyncio.create_task(_liq_ws_task(state, liq_fh),         name="mce-liq"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("[MCE] Tasks cancelled", flush=True)

# ── Entry point ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Market Context Engine")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if HALT_FILE.exists():
        print("[MCE] SYSTEM_HALT at startup — refusing to start", flush=True)
        sys.exit(1)

    if args.mode == "batch":
        run_batch()
    else:
        print("[MCE] Starting live mode", flush=True)
        asyncio.run(_run_live())

if __name__ == "__main__":
    main()
