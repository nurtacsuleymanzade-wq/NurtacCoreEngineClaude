"""
NurtacCoreEngineClaude — Layer-6: Smart Money Engine

Reads: data/combined_1s_dna_btcusdt.jsonl (1S)
       data/aligned_1m_candle_dna.jsonl  (1M)
       data/aligned_5m_candle_dna.jsonl  (5M)
       data/aligned_15m_candle_dna.jsonl (15M)
       data/aligned_1h_candle_dna.jsonl  (1H)
       data/historical_baseline_dna.jsonl (ATR source)
Writes: data/structure_1s.jsonl
        data/structure_1m.jsonl
        data/structure_5m.jsonl
        data/structure_15m.jsonl
        data/structure_1h.jsonl

Rules:
  - No Binance API/WebSocket calls
  - No mock data
  - Only reads existing JSONL files
  - No signals/long-short — only structural labels
  - Never crash, never write invalid records
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ───────────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
DATA_DIR   = Path("data")
HALT_FILE  = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.05

BASELINE_FILE = DATA_DIR / "historical_baseline_dna.jsonl"

TIMEFRAME_CONFIG = {
    "1S":  (DATA_DIR / "combined_1s_dna_btcusdt.jsonl",  DATA_DIR / "structure_1s.jsonl"),
    "1M":  (DATA_DIR / "aligned_1m_candle_dna.jsonl",    DATA_DIR / "structure_1m.jsonl"),
    "5M":  (DATA_DIR / "aligned_5m_candle_dna.jsonl",    DATA_DIR / "structure_5m.jsonl"),
    "15M": (DATA_DIR / "aligned_15m_candle_dna.jsonl",   DATA_DIR / "structure_15m.jsonl"),
    "1H":  (DATA_DIR / "aligned_1h_candle_dna.jsonl",    DATA_DIR / "structure_1h.jsonl"),
}

EQH_EQL_ATR_RATIO = 0.05   # equal level threshold = ATR * 0.05
MAX_OBS  = 3
MAX_FVGS = 5
MIN_VALID_BARS = 7          # minimum valid OHLC bars before swing detection

# ── Data classes ─────────────────────────────────────────────────────────────────
class Bar:
    __slots__ = ("ts", "o", "h", "l", "c", "wte")
    def __init__(self, ts: int, o, h, l, c, wte: int):
        self.ts  = ts
        self.o   = o
        self.h   = h
        self.l   = l
        self.c   = c
        self.wte = wte

class SwingPt:
    __slots__ = ("ts", "price", "stype", "is_high")
    def __init__(self, ts: int, price: float, stype: str, is_high: bool):
        self.ts      = ts
        self.price   = price
        self.stype   = stype   # "HH","LH","EQH" or "HL","LL","EQL"
        self.is_high = is_high

# ── Per-timeframe state ───────────────────────────────────────────────────────────
class TFState:
    def __init__(self, tf: str, baseline_atr: float):
        self.tf   = tf
        # Full bar buffer (for ts continuity; includes null-OHLC bars)
        self.all_buf: deque[Bar] = deque(maxlen=100)
        # Valid-OHLC bar buffer (for swing detection)
        self.buf: deque[Bar]     = deque(maxlen=50)
        # Swing history (trim to last 30 each)
        self.highs: list[SwingPt] = []
        self.lows:  list[SwingPt] = []
        # Trend
        self.trend          = "unknown"
        self.trend_strength = "unclear"
        # CHoCH state machine
        self.choch_phase: str | None    = None    # "phase1_bullish"|"phase1_bearish"
        self.choch_phase_ts: int | None = None
        self.choch_confirmed: str | None = None   # "bullish"|"bearish"
        self.msb: str | None            = None    # "bullish"|"bearish"
        self._msb_armed                 = False   # prevent repeated MSB for same event
        # Order blocks (max 3)
        self.obs: list[dict]  = []
        # Fair Value Gaps (max 5)
        self.fvgs: list[dict] = []
        # BOS tracking
        self.last_bos_high_ts: int | None = None  # swing high ts last used for BOS
        self.last_bos_low_ts:  int | None = None
        self.last_bos: str | None         = None  # most recent BOS direction
        # Equal levels
        self.equal_high: float | None = None
        self.equal_low:  float | None = None
        # ATR
        self.atr              = baseline_atr if baseline_atr > 0.0 else 1.0
        self._bar_ranges: list[float] = []
        # Counters / output helpers
        self.confirmed_fractals_count = 0

# ── Baseline loading ──────────────────────────────────────────────────────────────
def _load_baselines() -> dict[str, float]:
    """Return {timeframe: atr_value} from historical_baseline_dna.jsonl."""
    result: dict[str, float] = {}
    if not BASELINE_FILE.exists():
        return result
    try:
        with open(BASELINE_FILE, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                tf = rec.get("timeframe", "")
                try:
                    val = float(rec["atr"]["atr"])
                    if val > 0:
                        result[tf] = val
                except (KeyError, TypeError, ValueError):
                    pass
    except OSError:
        pass
    return result

# ── Parsing ───────────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    """Safe float conversion."""
    if v is None:
        return default
    try:
        f = float(v)
        if f != f or abs(f) == float("inf"):
            return default
        return f
    except (TypeError, ValueError):
        return default

def _parse_1s(raw: dict) -> Bar | None:
    ts = raw.get("window_start_ts")
    if ts is None:
        return None
    wte = raw.get("window_end_ts") or (ts + 1_000)
    cdna = raw.get("candle_dna") or {}
    o = (cdna.get("open")  or {}).get("price")
    h = (cdna.get("high")  or {}).get("price")
    l = (cdna.get("low")   or {}).get("price")
    c = (cdna.get("close") or {}).get("price")
    return Bar(int(ts), o, h, l, c, int(wte))

def _parse_aligned(raw: dict) -> Bar | None:
    ts = raw.get("window_start_ts")
    if ts is None:
        return None
    wte = raw.get("window_end_ts") or (ts + 60_000)
    ohlc = raw.get("ohlc") or {}
    o = (ohlc.get("open")  or {}).get("price")
    h = (ohlc.get("high")  or {}).get("price")
    l = (ohlc.get("low")   or {}).get("price")
    c = (ohlc.get("close") or {}).get("price")
    return Bar(int(ts), o, h, l, c, int(wte))

def _is_valid_ohlc(bar: Bar) -> bool:
    return (bar.o is not None and bar.h is not None and
            bar.l is not None and bar.c is not None)

# ── ATR ───────────────────────────────────────────────────────────────────────────
def _update_atr(state: TFState, bar: Bar) -> None:
    if bar.h is not None and bar.l is not None:
        tr = _sf(bar.h) - _sf(bar.l)
        if tr > 0:
            state._bar_ranges.append(tr)
            if len(state._bar_ranges) > 50:
                state._bar_ranges.pop(0)
    # If baseline ATR is the default 1.0 but we have bar data → compute
    if len(state._bar_ranges) >= 1:
        last14 = state._bar_ranges[-14:]
        computed = sum(last14) / len(last14)
        # Only override the 1.0 fallback, not a real baseline
        if state.atr == 1.0 or computed > 0:
            if state.atr == 1.0:
                state.atr = computed

# ── Swing classification ──────────────────────────────────────────────────────────
def _classify_swing_high(state: TFState, price: float) -> str:
    if not state.highs:
        return "HH"
    prev = state.highs[-1].price
    diff = abs(price - prev)
    if diff < state.atr * EQH_EQL_ATR_RATIO:
        return "EQH"
    return "HH" if price > prev else "LH"

def _classify_swing_low(state: TFState, price: float) -> str:
    if not state.lows:
        return "HL"
    prev = state.lows[-1].price
    diff = abs(price - prev)
    if diff < state.atr * EQH_EQL_ATR_RATIO:
        return "EQL"
    return "HL" if price > prev else "LL"

# ── Fractal detection ─────────────────────────────────────────────────────────────
def _try_confirm_fractal(state: TFState) -> bool:
    """
    Try to confirm fractal at buf[-4] (3-bar lag).
    Requires len(buf) >= 7.
    Returns True if at least one new fractal was confirmed.
    """
    buf = state.buf
    if len(buf) < MIN_VALID_BARS:
        return False

    b = list(buf)[-7:]
    # b[0..2] = left, b[3] = candidate, b[4..6] = right
    cand  = b[3]
    left  = b[:3]
    right = b[4:]

    # Guard: all 6 surrounding bars must have valid H/L
    if any(x.h is None or x.l is None for x in left + right):
        return False
    if cand.h is None or cand.l is None:
        return False

    ch = _sf(cand.h)
    cl = _sf(cand.l)

    new_fractal = False

    # Fractal HIGH
    already_h = any(s.ts == cand.ts and s.is_high for s in state.highs)
    if not already_h:
        if all(_sf(x.h) <= ch for x in left) and all(_sf(x.h) <= ch for x in right):
            stype = _classify_swing_high(state, ch)
            state.highs.append(SwingPt(cand.ts, ch, stype, True))
            if len(state.highs) > 30:
                state.highs.pop(0)
            state.confirmed_fractals_count += 1
            new_fractal = True

    # Fractal LOW
    already_l = any(s.ts == cand.ts and not s.is_high for s in state.lows)
    if not already_l:
        if all(_sf(x.l, 1e18) >= cl for x in left) and all(_sf(x.l, 1e18) >= cl for x in right):
            stype = _classify_swing_low(state, cl)
            state.lows.append(SwingPt(cand.ts, cl, stype, False))
            if len(state.lows) > 30:
                state.lows.pop(0)
            state.confirmed_fractals_count += 1
            new_fractal = True

    return new_fractal

# ── Trend ─────────────────────────────────────────────────────────────────────────
def _update_trend(state: TFState) -> None:
    if len(state.highs) < 2 or len(state.lows) < 2:
        state.trend = "unknown"
        state.trend_strength = "unclear"
        return

    last_h_type = state.highs[-1].stype
    last_l_type = state.lows[-1].stype

    if last_h_type == "HH" and last_l_type == "HL":
        state.trend = "uptrend"
    elif last_h_type == "LH" and last_l_type == "LL":
        state.trend = "downtrend"
    else:
        state.trend = "ranging"

    # Strength: last 3 swing events
    all_swings = sorted(state.highs[-15:] + state.lows[-15:], key=lambda s: s.ts)
    if len(all_swings) >= 3:
        last3 = all_swings[-3:]
        bullish = {"HH", "HL"}
        bearish = {"LH", "LL"}
        if all(s.stype in bullish for s in last3):
            state.trend_strength = "strong"
        elif all(s.stype in bearish for s in last3):
            state.trend_strength = "strong"
        elif (last3[-1].stype in bullish and last3[-2].stype in bullish):
            state.trend_strength = "weak"
        elif (last3[-1].stype in bearish and last3[-2].stype in bearish):
            state.trend_strength = "weak"
        else:
            state.trend_strength = "unclear"
    else:
        state.trend_strength = "unclear"

# ── BOS ───────────────────────────────────────────────────────────────────────────
def _check_bos(state: TFState, bar: Bar) -> str | None:
    """Check for Break of Structure on the current bar. Returns direction or None."""
    if bar.c is None:
        return None
    close = _sf(bar.c)

    # Bullish BOS: close breaks above last swing high not yet used
    if state.highs:
        sh = state.highs[-1]
        if sh.ts != state.last_bos_high_ts and close > sh.price:
            # Require at least 1 swing low BEFORE this swing high
            if any(sl.ts < sh.ts for sl in state.lows):
                state.last_bos_high_ts = sh.ts
                return "bullish"

    # Bearish BOS: close breaks below last swing low not yet used
    if state.lows:
        sl = state.lows[-1]
        if sl.ts != state.last_bos_low_ts and close < sl.price:
            if any(sh.ts < sl.ts for sh in state.highs):
                state.last_bos_low_ts = sl.ts
                return "bearish"

    return None

# ── Order Block ───────────────────────────────────────────────────────────────────
def _create_ob(state: TFState, bar: Bar, bos_dir: str) -> None:
    """Create Order Block triggered by BOS. OB = last opposite candle in buffer."""
    is_bullish = (bos_dir == "bullish")
    ob_bar = None
    for b in reversed(list(state.buf)[:-1]):
        if b.o is None or b.c is None or b.h is None or b.l is None:
            continue
        o, c = _sf(b.o), _sf(b.c)
        if is_bullish and c < o:      # last bearish candle
            ob_bar = b
            break
        elif not is_bullish and c > o: # last bullish candle
            ob_bar = b
            break

    if ob_bar is None:
        return

    ob_high = _sf(ob_bar.h)
    ob_low  = _sf(ob_bar.l)
    if ob_high <= ob_low or ob_high <= 0 or ob_low <= 0:
        return

    ob = {
        "ob_type":    "bullish_ob" if is_bullish else "bearish_ob",
        "ob_high":    ob_high,
        "ob_low":     ob_low,
        "created_ts": bar.ts,
        "status":     "active",
    }
    state.obs.append(ob)
    if len(state.obs) > MAX_OBS:
        state.obs.pop(0)

def _update_ob_status(state: TFState, bar: Bar) -> list[dict]:
    """Update OB statuses based on current bar. Returns newly-breaker OBs."""
    if bar.h is None or bar.l is None or bar.c is None:
        return []
    h, l, c = _sf(bar.h), _sf(bar.l), _sf(bar.c)
    new_breakers = []
    for ob in state.obs:
        if ob["status"] == "breaker":
            continue
        oh, ol = ob["ob_high"], ob["ob_low"]
        # Mitigation: price enters OB zone
        if ob["status"] == "active" and l <= oh and h >= ol:
            ob["status"] = "mitigated"
        # Breaker: price fully passes through
        was_breaker = False
        if ob["ob_type"] == "bullish_ob" and c < ol:
            ob["status"] = "breaker"
            was_breaker = True
        elif ob["ob_type"] == "bearish_ob" and c > oh:
            ob["status"] = "breaker"
            was_breaker = True
        if was_breaker:
            new_breakers.append(ob)
    return new_breakers

# ── Fair Value Gap ────────────────────────────────────────────────────────────────
def _detect_fvg(state: TFState, bar: Bar) -> None:
    """Detect FVG using buf[-3] (i-1), buf[-2] (i), buf[-1] = bar (i+1)."""
    if len(state.buf) < 3:
        return
    bars = list(state.buf)
    b_prev = bars[-3]   # bar[i-1]
    # b_mid  = bars[-2]   # bar[i]  — not directly used
    b_curr = bars[-1]   # bar[i+1] = current bar

    if (b_prev.h is None or b_prev.l is None or
            b_curr.h is None or b_curr.l is None):
        return

    prev_h = _sf(b_prev.h)
    prev_l = _sf(b_prev.l)
    curr_h = _sf(b_curr.h)
    curr_l = _sf(b_curr.l)

    # Bullish FVG: gap between prev_h and curr_l
    if prev_h < curr_l:
        gap_low, gap_high = prev_h, curr_l
        if gap_high > gap_low > 0:
            fvg = {
                "fvg_type":   "bullish_fvg",
                "gap_high":   gap_high,
                "gap_low":    gap_low,
                "created_ts": bar.ts,
                "status":     "active",
            }
            state.fvgs.append(fvg)
            if len(state.fvgs) > MAX_FVGS:
                state.fvgs.pop(0)
    # Bearish FVG: gap between curr_h and prev_l
    elif prev_l > curr_h:
        gap_high, gap_low = prev_l, curr_h
        if gap_high > gap_low > 0:
            fvg = {
                "fvg_type":   "bearish_fvg",
                "gap_high":   gap_high,
                "gap_low":    gap_low,
                "created_ts": bar.ts,
                "status":     "active",
            }
            state.fvgs.append(fvg)
            if len(state.fvgs) > MAX_FVGS:
                state.fvgs.pop(0)

def _update_fvg_status(state: TFState, bar: Bar) -> None:
    """Update FVG statuses based on current bar."""
    if bar.h is None or bar.l is None:
        return
    h, l = _sf(bar.h), _sf(bar.l)
    for fvg in state.fvgs:
        if fvg["status"] == "filled":
            continue
        gh, gl = fvg["gap_high"], fvg["gap_low"]
        # Mitigated: price enters gap
        if fvg["status"] == "active" and l < gh and h > gl:
            fvg["status"] = "mitigated"
        # Filled: price fully covers gap
        if fvg["status"] == "mitigated":
            if fvg["fvg_type"] == "bullish_fvg" and l <= gl:
                fvg["status"] = "filled"
            elif fvg["fvg_type"] == "bearish_fvg" and h >= gh:
                fvg["status"] = "filled"

# ── CHoCH ────────────────────────────────────────────────────────────────────────
def _update_choch(state: TFState) -> None:
    """Update CHoCH state machine based on current swing history."""
    highs, lows = state.highs, state.lows
    if not highs or not lows:
        return

    last_h = highs[-1]
    last_l = lows[-1]

    # Phase 1 detection (only if not already in a phase)
    if state.choch_phase is None:
        # Bullish CHoCH phase 1: downtrend + new HH
        if state.trend == "downtrend" and last_h.stype == "HH":
            if state.choch_phase_ts != last_h.ts:
                state.choch_phase    = "phase1_bullish"
                state.choch_phase_ts = last_h.ts
                state._msb_armed     = False
        # Bearish CHoCH phase 1: uptrend + new LL
        elif state.trend == "uptrend" and last_l.stype == "LL":
            if state.choch_phase_ts != last_l.ts:
                state.choch_phase    = "phase1_bearish"
                state.choch_phase_ts = last_l.ts
                state._msb_armed     = False
        return

    p1_ts = state.choch_phase_ts

    if state.choch_phase == "phase1_bullish":
        post_h = [s for s in highs if s.ts > p1_ts]
        post_l = [s for s in lows  if s.ts > p1_ts]
        # Cancellation: old-trend (bearish) swing appears
        all_post = sorted(post_h + post_l, key=lambda s: s.ts)
        for s in all_post:
            if s.stype in ("LH", "LL"):
                state.choch_phase    = None
                state.choch_phase_ts = None
                return
            if not s.is_high and s.stype == "HL":
                # Phase 2 confirmed
                state.choch_confirmed = "bullish"
                state.choch_phase     = None
                state.choch_phase_ts  = None
                return

    elif state.choch_phase == "phase1_bearish":
        post_h = [s for s in highs if s.ts > p1_ts]
        post_l = [s for s in lows  if s.ts > p1_ts]
        all_post = sorted(post_h + post_l, key=lambda s: s.ts)
        for s in all_post:
            if s.stype in ("HH", "HL"):
                state.choch_phase    = None
                state.choch_phase_ts = None
                return
            if s.is_high and s.stype == "LH":
                # Phase 2 confirmed
                state.choch_confirmed = "bearish"
                state.choch_phase     = None
                state.choch_phase_ts  = None
                return

# ── MSB ──────────────────────────────────────────────────────────────────────────
def _check_msb(state: TFState, bos_dir: str | None) -> None:
    """MSB = CHoCH confirmed + same-direction BOS."""
    if state._msb_armed:
        return
    if (state.choch_confirmed is not None and
            bos_dir is not None and
            bos_dir == state.choch_confirmed):
        state.msb        = bos_dir
        state._msb_armed = True

# ── Equal levels ──────────────────────────────────────────────────────────────────
def _update_equal_levels(state: TFState) -> None:
    """Detect EQH and EQL — last 2 consecutive swing highs/lows within ATR*0.05."""
    thresh = state.atr * EQH_EQL_ATR_RATIO
    if len(state.highs) >= 2:
        h1, h2 = state.highs[-1].price, state.highs[-2].price
        state.equal_high = h1 if abs(h1 - h2) < thresh else None
    else:
        state.equal_high = None

    if len(state.lows) >= 2:
        l1, l2 = state.lows[-1].price, state.lows[-2].price
        state.equal_low = l1 if abs(l1 - l2) < thresh else None
    else:
        state.equal_low = None

# ── Output builder ────────────────────────────────────────────────────────────────
def _build_output(state: TFState, bar: Bar, bos_dir: str | None) -> dict:
    tf = state.tf
    last_sh = state.highs[-1] if state.highs else None
    last_sl = state.lows[-1]  if state.lows  else None

    bos_micro    = bos_dir if tf == "1S" else None
    bos_macro    = bos_dir if tf != "1S" else None
    macro_bos_tf = tf      if tf != "1S" else None

    return {
        "engine":          "smart_money_engine",
        "symbol":          SYMBOL,
        "timeframe":       tf,
        "window_start_ts": bar.ts,
        "window_end_ts":   bar.wte,
        "swing": {
            "last_swing_high": (
                {"price": last_sh.price, "ts": last_sh.ts, "type": last_sh.stype}
                if last_sh else None
            ),
            "last_swing_low": (
                {"price": last_sl.price, "ts": last_sl.ts, "type": last_sl.stype}
                if last_sl else None
            ),
            "confirmed_fractals_count": state.confirmed_fractals_count,
        },
        "trend": {
            "direction":      state.trend,
            "strength":       state.trend_strength,
            "choch_phase":    state.choch_phase,
            "choch_confirmed": state.choch_confirmed,
            "msb":            state.msb,
        },
        "bos": {
            "micro_bos":    bos_micro,
            "macro_bos":    bos_macro,
            "macro_bos_tf": macro_bos_tf,
        },
        "order_blocks": [dict(ob) for ob in state.obs],
        "fvg":          [dict(f)  for f  in state.fvgs],
        "equal_levels": {
            "equal_high": state.equal_high,
            "equal_low":  state.equal_low,
        },
        "atr_used": state.atr,
    }

# ── Validation ───────────────────────────────────────────────────────────────────
VALID_TFS         = {"1S", "1M", "5M", "15M", "1H"}
VALID_TREND_DIRS  = {"uptrend", "downtrend", "ranging", "unknown"}
VALID_STRENGTHS   = {"strong", "weak", "unclear", "unknown"}
VALID_CHOCH_PH    = {None, "phase1_bullish", "phase1_bearish"}
VALID_CHOCH_CONF  = {None, "bullish", "bearish"}
VALID_MSB         = {None, "bullish", "bearish"}
VALID_BOS         = {None, "bullish", "bearish"}
VALID_OB_TYPES    = {"bullish_ob", "bearish_ob"}
VALID_OB_STATUS   = {"active", "mitigated", "breaker"}
VALID_FVG_TYPES   = {"bullish_fvg", "bearish_fvg"}
VALID_FVG_STATUS  = {"active", "mitigated", "filled"}

def _validate(rec: dict) -> list[str]:
    errors: list[str] = []

    # [1] timeframe valid
    if rec.get("timeframe") not in VALID_TFS:
        errors.append(f"[1] invalid timeframe: {rec.get('timeframe')}")

    # [2] ts ordering
    wts = rec.get("window_start_ts", 0)
    wte = rec.get("window_end_ts", 0)
    if not (isinstance(wts, int) and isinstance(wte, int) and wts < wte):
        errors.append(f"[2] ts order: {wts} >= {wte}")

    # [3] trend.direction valid
    trend = rec.get("trend", {})
    if trend.get("direction") not in VALID_TREND_DIRS:
        errors.append(f"[3] invalid trend direction: {trend.get('direction')}")

    # [4] trend.strength valid
    if trend.get("strength") not in VALID_STRENGTHS:
        errors.append(f"[4] invalid trend strength: {trend.get('strength')}")

    # [5] bos values valid
    bos = rec.get("bos", {})
    if bos.get("micro_bos") not in VALID_BOS:
        errors.append(f"[5a] invalid micro_bos: {bos.get('micro_bos')}")
    if bos.get("macro_bos") not in VALID_BOS:
        errors.append(f"[5b] invalid macro_bos: {bos.get('macro_bos')}")

    # [6] order_blocks max 3
    obs = rec.get("order_blocks", [])
    if len(obs) > MAX_OBS:
        errors.append(f"[6] order_blocks count {len(obs)} > {MAX_OBS}")

    # [7] fvg max 5
    fvgs = rec.get("fvg", [])
    if len(fvgs) > MAX_FVGS:
        errors.append(f"[7] fvg count {len(fvgs)} > {MAX_FVGS}")

    # [8] ob_high > ob_low
    for ob in obs:
        if ob.get("ob_high", 0) <= ob.get("ob_low", 0):
            errors.append(f"[8] ob_high <= ob_low: {ob}")
        if ob.get("ob_type") not in VALID_OB_TYPES:
            errors.append(f"[8b] invalid ob_type: {ob.get('ob_type')}")
        if ob.get("status") not in VALID_OB_STATUS:
            errors.append(f"[8c] invalid ob status: {ob.get('status')}")

    # [9] gap_high > gap_low
    for fvg in fvgs:
        if fvg.get("gap_high", 0) <= fvg.get("gap_low", 0):
            errors.append(f"[9] gap_high <= gap_low: {fvg}")
        if fvg.get("fvg_type") not in VALID_FVG_TYPES:
            errors.append(f"[9b] invalid fvg_type: {fvg.get('fvg_type')}")
        if fvg.get("status") not in VALID_FVG_STATUS:
            errors.append(f"[9c] invalid fvg status: {fvg.get('status')}")

    # [10] atr_used > 0
    if not (_sf(rec.get("atr_used", 0)) > 0):
        errors.append(f"[10] atr_used <= 0: {rec.get('atr_used')}")

    return errors

# ── JSONL write helper ────────────────────────────────────────────────────────────
def _write_jsonl(fh, rec: dict) -> None:
    line = json.dumps(rec, ensure_ascii=False)
    fh.write(line + "\n")
    fh.flush()
    os.fsync(fh.fileno())

# ── Terminal print ────────────────────────────────────────────────────────────────
def _maybe_print(state: TFState, bar: Bar, bos_dir: str | None,
                 new_obs: int, new_breakers: list[dict]) -> None:
    tf    = state.tf
    ts_s  = bar.ts // 1000

    if bos_dir:
        sym = "^" if bos_dir == "bullish" else "v"
        print(f"[SME][{tf}] BOS {sym}{bos_dir.upper()} ts={ts_s} "
              f"close={_sf(bar.c):.1f} trend={state.trend}", flush=True)

    if state.choch_confirmed and not state.msb:
        print(f"[SME][{tf}] CHoCH {state.choch_confirmed.upper()} confirmed ts={ts_s}",
              flush=True)

    if state.msb:
        print(f"[SME][{tf}] MSB {state.msb.upper()} ts={ts_s}", flush=True)

    if new_obs:
        ob = state.obs[-1]
        print(f"[SME][{tf}] NEW OB {ob['ob_type']} "
              f"[{ob['ob_low']:.1f}–{ob['ob_high']:.1f}] ts={ts_s}", flush=True)

    for ob in new_breakers:
        print(f"[SME][{tf}] BREAKER {ob['ob_type']} "
              f"[{ob['ob_low']:.1f}–{ob['ob_high']:.1f}] ts={ts_s}", flush=True)

    if FULL_PRINT:
        print(f"[SME][{tf}] ts={ts_s} trend={state.trend}/{state.trend_strength} "
              f"fractals={state.confirmed_fractals_count} "
              f"obs={len(state.obs)} fvgs={len(state.fvgs)}", flush=True)

# ── Main ingest (one bar) ─────────────────────────────────────────────────────────
def ingest(state: TFState, bar: Bar, fh) -> bool:
    """
    Process one bar. Write validated output to fh.
    Returns True if output was written.
    """
    state.all_buf.append(bar)

    # Skip null OHLC bars — no output, but ts is tracked in all_buf
    if not _is_valid_ohlc(bar):
        return False

    state.buf.append(bar)
    _update_atr(state, bar)
    _try_confirm_fractal(state)
    _update_trend(state)

    bos_dir = _check_bos(state, bar)
    if bos_dir:
        state.last_bos = bos_dir

    prev_ob_count = len(state.obs)
    if bos_dir:
        _create_ob(state, bar, bos_dir)

    _update_choch(state)
    _check_msb(state, bos_dir)
    _detect_fvg(state, bar)

    # OB status — capture breakers
    prev_obs_statuses = [ob["status"] for ob in state.obs]
    new_breakers = _update_ob_status(state, bar)
    _update_fvg_status(state, bar)
    _update_equal_levels(state)

    rec = _build_output(state, bar, bos_dir)

    errors = _validate(rec)
    if errors:
        print(f"[SME][{state.tf}] VALIDATION ERROR ts={bar.ts}: {errors}", flush=True)
        return False

    _write_jsonl(fh, rec)
    new_obs = len(state.obs) - prev_ob_count
    _maybe_print(state, bar, bos_dir, new_obs, new_breakers)
    return True

# ── File reading helpers ──────────────────────────────────────────────────────────
def _parse_line(tf: str, raw: dict) -> Bar | None:
    if tf == "1S":
        return _parse_1s(raw)
    return _parse_aligned(raw)

def _read_all_lines(path: Path) -> list[dict]:
    """Read all valid JSON lines from a JSONL file."""
    records: list[dict] = []
    if not path.exists():
        return records
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    except OSError:
        pass
    return records

# ── Batch mode ────────────────────────────────────────────────────────────────────
def _run_batch_tf(tf: str, in_path: Path, out_path: Path,
                  baseline_atr: float) -> None:
    print(f"[SME][{tf}] Batch mode: reading {in_path}", flush=True)
    records = _read_all_lines(in_path)
    if not records:
        print(f"[SME][{tf}] No data in {in_path}", flush=True)
        return

    state = TFState(tf, baseline_atr)
    outputs: list[dict] = []

    for raw in records:
        if HALT_FILE.exists():
            print(f"[SME][{tf}] SYSTEM_HALT detected — aborting batch", flush=True)
            return
        bar = _parse_line(tf, raw)
        if bar is None:
            continue
        if not _is_valid_ohlc(bar):
            state.all_buf.append(bar)
            continue

        state.buf.append(bar)
        state.all_buf.append(bar)
        _update_atr(state, bar)
        _try_confirm_fractal(state)
        _update_trend(state)

        bos_dir = _check_bos(state, bar)
        if bos_dir:
            state.last_bos = bos_dir
            _create_ob(state, bar, bos_dir)

        _update_choch(state)
        _check_msb(state, bos_dir)
        _detect_fvg(state, bar)
        _update_ob_status(state, bar)
        _update_fvg_status(state, bar)
        _update_equal_levels(state)

        rec = _build_output(state, bar, bos_dir)
        errors = _validate(rec)
        if errors:
            print(f"[SME][{tf}] BATCH VALIDATION ERROR ts={bar.ts}: {errors}",
                  flush=True)
            continue
        outputs.append(rec)

    if not outputs:
        print(f"[SME][{tf}] Batch: no valid outputs", flush=True)
        return

    # Write ONLY the last valid output in batch mode
    last_rec = outputs[-1]
    with open(out_path, "a", encoding="utf-8") as fh:
        _write_jsonl(fh, last_rec)

    print(f"[SME][{tf}] Batch done: {len(records)} bars processed, "
          f"{len(outputs)} valid outputs, last written to {out_path}", flush=True)
    print(f"[SME][{tf}] Last trend={last_rec['trend']['direction']}/"
          f"{last_rec['trend']['strength']} "
          f"fractals={last_rec['swing']['confirmed_fractals_count']} "
          f"obs={len(last_rec['order_blocks'])} fvgs={len(last_rec['fvg'])}",
          flush=True)

def run_batch(baselines: dict[str, float]) -> None:
    for tf, (in_path, out_path) in TIMEFRAME_CONFIG.items():
        atr = baselines.get(tf, 1.0)
        _run_batch_tf(tf, in_path, out_path, atr)

# ── Live mode ─────────────────────────────────────────────────────────────────────
async def _live_tf_task(tf: str, in_path: Path, out_path: Path,
                        baseline_atr: float) -> None:
    """Asyncio task: tail-f the input file and process new bars."""
    print(f"[SME][{tf}] Live task started — waiting for {in_path}", flush=True)

    # Wait for file to exist
    while not in_path.exists():
        if HALT_FILE.exists():
            print(f"[SME][{tf}] SYSTEM_HALT — exiting", flush=True)
            return
        await asyncio.sleep(2.0)

    state = TFState(tf, baseline_atr)

    # Warm-up: process existing data (do NOT write to output)
    existing = _read_all_lines(in_path)
    print(f"[SME][{tf}] Warm-up: {len(existing)} existing records", flush=True)
    for raw in existing:
        bar = _parse_line(tf, raw)
        if bar is None:
            continue
        if not _is_valid_ohlc(bar):
            state.all_buf.append(bar)
            continue
        state.buf.append(bar)
        state.all_buf.append(bar)
        _update_atr(state, bar)
        _try_confirm_fractal(state)
        _update_trend(state)
        bos_dir = _check_bos(state, bar)
        if bos_dir:
            state.last_bos = bos_dir
            _create_ob(state, bar, bos_dir)
        _update_choch(state)
        _check_msb(state, bos_dir)
        _detect_fvg(state, bar)
        _update_ob_status(state, bar)
        _update_fvg_status(state, bar)
        _update_equal_levels(state)

    print(f"[SME][{tf}] Warm-up done: trend={state.trend}/{state.trend_strength} "
          f"fractals={state.confirmed_fractals_count} "
          f"obs={len(state.obs)} fvgs={len(state.fvgs)}", flush=True)

    # Tail-f loop
    with (open(in_path,  "r", encoding="utf-8") as inf,
          open(out_path, "a", encoding="utf-8") as outf):

        # Seek to end (skip already-processed lines)
        inf.seek(0, 2)

        while True:
            if HALT_FILE.exists():
                print(f"[SME][{tf}] SYSTEM_HALT — stopping live task", flush=True)
                return

            line = inf.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue

            line = line.strip()
            if not line:
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue

            bar = _parse_line(tf, raw)
            if bar is None:
                continue

            ingest(state, bar, outf)

# ── Entry point ───────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Smart Money Engine — Layer 6")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[SME] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        sys.exit(1)

    baselines = _load_baselines()
    print(f"[SME] Loaded baselines: {baselines}", flush=True)

    if args.mode == "batch":
        run_batch(baselines)
        return

    # Live mode: 5 parallel asyncio tasks
    async def _main_live():
        tasks = []
        for tf, (in_path, out_path) in TIMEFRAME_CONFIG.items():
            atr = baselines.get(tf, 1.0)
            tasks.append(asyncio.create_task(
                _live_tf_task(tf, in_path, out_path, atr),
                name=f"sme-{tf}"
            ))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("[SME] Tasks cancelled — shutting down", flush=True)

    print("[SME] Starting live mode (5 timeframe tasks)", flush=True)
    asyncio.run(_main_live())

if __name__ == "__main__":
    main()
