"""
NurtacCoreEngineClaude — Layer-8: Volume Profile Engine

Reads: data/combined_1s_dna_btcusdt.jsonl
       data/aligned_1m_candle_dna.jsonl
       data/aligned_5m_candle_dna.jsonl
Writes: data/volume_profile_1s.jsonl
        data/volume_profile_1m.jsonl
        data/volume_profile_5m.jsonl
        data/volume_profile_session.jsonl
        data/volume_memory.jsonl

Rules:
  - No Binance API/WebSocket calls
  - Only reads existing JSONL files
  - No signal/decision output — volume memory and location only
  - Never crash, never write invalid records
"""

import argparse
import asyncio
import json
import math
import os
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
DATA_DIR   = Path("data")
HALT_FILE  = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.05

FILE_1S      = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
FILE_1M      = DATA_DIR / "aligned_1m_candle_dna.jsonl"
FILE_5M      = DATA_DIR / "aligned_5m_candle_dna.jsonl"

OUT_1S       = DATA_DIR / "volume_profile_1s.jsonl"
OUT_1M       = DATA_DIR / "volume_profile_1m.jsonl"
OUT_5M       = DATA_DIR / "volume_profile_5m.jsonl"
OUT_SESSION  = DATA_DIR / "volume_profile_session.jsonl"
OUT_MEMORY   = DATA_DIR / "volume_memory.jsonl"

PRICE_STEP      = 0.1          # BTC granularity
ROLLING_1S_SEC  = 300          # 5 min window for 1S profile
ROLLING_1M_BARS = 30           # 30 x 1M
ROLLING_5M_BARS = 24           # 24 x 5M = 2h
VALUE_AREA_PCT  = 0.70
OUTPUT_1S_EVERY = 10           # seconds
OUTPUT_SES_EVERY= 60           # seconds
ATR_PROXIMITY_POC = 0.05
ATR_PROXIMITY_VAH = 0.1


# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default

def _quantize(price: float) -> float:
    return round(round(price / PRICE_STEP) * PRICE_STEP, 10)

def _now_ms() -> int:
    return int(time.time() * 1000)

def _utc_day_start_ms() -> int:
    t = time.gmtime()
    import calendar
    return int(calendar.timegm((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0)) * 1000)

def _read_all_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
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

def _read_last_n_jsonl(path: Path, maxlen: int) -> list[dict]:
    """Read only last N lines from JSONL file (memory-efficient warm-up)."""
    if not path.exists():
        return []
    records = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            last_records = deque(maxlen=maxlen)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last_records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            records = list(last_records)
    except OSError:
        pass
    return records

def _write_jsonl(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        pass

def _append_jsonl(path: Path, rec: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            _write_jsonl(f, rec)
    except OSError:
        pass


# ── Level data structure ───────────────────────────────────────────────────────
def _empty_level() -> dict:
    return {
        "total_volume": 0.0,
        "buy_volume":   0.0,
        "sell_volume":  0.0,
        "delta":        0.0,
        "tpo_count":    0,
        "first_visit_ts": None,
        "last_visit_ts":  None,
    }

def _merge_level(dst: dict, tv: float, bv: float, sv: float, ts: int) -> None:
    dst["total_volume"] += tv
    dst["buy_volume"]   += bv
    dst["sell_volume"]  += sv
    dst["delta"]        += (bv - sv)
    dst["tpo_count"]    += 1
    if dst["first_visit_ts"] is None or ts < dst["first_visit_ts"]:
        dst["first_visit_ts"] = ts
    if dst["last_visit_ts"] is None or ts > dst["last_visit_ts"]:
        dst["last_visit_ts"] = ts

def _build_merged_profile(contributions: list[dict]) -> dict[float, dict]:
    """Merge a list of {level: {tv,bv,sv,ts}} dicts into one profile."""
    merged: dict[float, dict] = {}
    for contrib in contributions:
        for level_str, ld in contrib.items():
            level = float(level_str)
            if level not in merged:
                merged[level] = _empty_level()
            _merge_level(
                merged[level],
                ld["tv"], ld["bv"], ld["sv"], ld["ts"]
            )
    return merged

def _extract_1s_levels(record: dict) -> dict[float, dict]:
    """Extract price level contributions from a 1S combined record."""
    out: dict[float, dict] = {}
    fp = record.get("footprint_dna") or {}
    if not fp.get("has_trade"):
        return out
    ts = int(record.get("window_start_ts", 0))
    for pl in fp.get("price_levels") or []:
        price = _sf(pl.get("price"), 0.0)
        if price <= 0:
            continue
        level = _quantize(price)
        bv = _sf(pl.get("buy_volume"))
        sv = _sf(pl.get("sell_volume"))
        tv = bv + sv
        if level not in out:
            out[level] = {"tv": 0.0, "bv": 0.0, "sv": 0.0, "ts": ts}
        out[level]["tv"] += tv
        out[level]["bv"] += bv
        out[level]["sv"] += sv
    return out

def _extract_aligned_levels(record: dict) -> dict[float, dict]:
    """Extract price level contributions from a 1M/5M aligned candle record."""
    out: dict[float, dict] = {}
    fp = record.get("footprint") or {}
    ts = int(record.get("window_start_ts", 0))
    for pl in fp.get("price_levels") or []:
        price = _sf(pl.get("price"), 0.0)
        if price <= 0:
            continue
        level = _quantize(price)
        bv = _sf(pl.get("buy_volume"))
        sv = _sf(pl.get("sell_volume"))
        tv = bv + sv
        if level not in out:
            out[level] = {"tv": 0.0, "bv": 0.0, "sv": 0.0, "ts": ts}
        out[level]["tv"] += tv
        out[level]["bv"] += bv
        out[level]["sv"] += sv
    return out


# ── POC / VAH / VAL ───────────────────────────────────────────────────────────
def _compute_poc_vah_val(
    profile: dict[float, dict],
    current_close: float | None = None
) -> tuple[float | None, float | None, float | None, float, list[float]]:
    """Returns (poc, vah, val, value_area_volume, value_area_levels)."""
    if not profile:
        return None, None, None, 0.0, []

    total_vol = sum(ld["total_volume"] for ld in profile.values())
    if total_vol <= 0:
        return None, None, None, 0.0, []

    # POC: highest volume level; ties broken by nearest to current_close
    max_vol = max(ld["total_volume"] for ld in profile.values())
    poc_candidates = [lvl for lvl, ld in profile.items()
                      if abs(ld["total_volume"] - max_vol) < 1e-12]
    if current_close is not None and len(poc_candidates) > 1:
        poc = min(poc_candidates, key=lambda p: abs(p - current_close))
    else:
        poc = poc_candidates[0]

    # VAH / VAL: expand from POC until 70% of total volume covered
    target = total_vol * VALUE_AREA_PCT
    sorted_levels = sorted(profile.keys())
    poc_idx = sorted_levels.index(poc)

    va_set = {poc}
    va_vol = profile[poc]["total_volume"]

    lo_idx = poc_idx - 1
    hi_idx = poc_idx + 1

    while va_vol < target:
        lo_vol = profile[sorted_levels[lo_idx]]["total_volume"] if lo_idx >= 0 else -1
        hi_vol = profile[sorted_levels[hi_idx]]["total_volume"] if hi_idx < len(sorted_levels) else -1

        if lo_vol < 0 and hi_vol < 0:
            break
        if hi_vol >= lo_vol:
            va_set.add(sorted_levels[hi_idx])
            va_vol += hi_vol
            hi_idx += 1
        else:
            va_set.add(sorted_levels[lo_idx])
            va_vol += lo_vol
            lo_idx -= 1

    va_levels = sorted(va_set)
    vah = va_levels[-1] if va_levels else None
    val = va_levels[0]  if va_levels else None

    return poc, vah, val, va_vol, va_levels


# ── Profile shape ─────────────────────────────────────────────────────────────
def _compute_profile_shape(
    profile: dict[float, dict],
    poc: float | None,
    vah: float | None,
    val: float | None,
    atr: float,
) -> str:
    if poc is None or vah is None or val is None or not profile:
        return "unknown"

    va_range = vah - val
    if va_range <= 0:
        return "unknown"

    # Thin / Elongated first
    if va_range < atr * 1.0:
        return "thin_profile"
    if va_range > atr * 3.0:
        return "elongated_profile"

    # Relative POC position within VA
    poc_pos = (poc - val) / va_range  # 0.0 = bottom, 1.0 = top

    # Bimodal: find two separated high-volume clusters
    sorted_levels = sorted(profile.keys())
    volumes = [profile[l]["total_volume"] for l in sorted_levels]
    avg_vol = sum(volumes) / len(volumes) if volumes else 0
    # Find local maxima
    peaks = []
    for i in range(1, len(sorted_levels) - 1):
        if volumes[i] > volumes[i-1] and volumes[i] > volumes[i+1]:
            peaks.append((sorted_levels[i], volumes[i]))
    if len(peaks) >= 2:
        peaks_sorted = sorted(peaks, key=lambda x: -x[1])
        p1, p2 = peaks_sorted[0], peaks_sorted[1]
        mid_price = (p1[0] + p2[0]) / 2.0
        # Volume at mid (trough between peaks)
        mid_candidates = [l for l in sorted_levels
                          if min(p1[0], p2[0]) < l < max(p1[0], p2[0])]
        if mid_candidates:
            mid_vol = min(profile[l]["total_volume"] for l in mid_candidates)
            if mid_vol < min(p1[1], p2[1]) * 0.4:
                return "bimodal"

    # P-shape: POC in lower 30%
    if poc_pos <= 0.30:
        return "p_shape"
    # B-shape: POC in upper 30%
    if poc_pos >= 0.70:
        return "b_shape"

    # Normal distribution: POC near center with symmetric distribution
    mid_pos = 0.5
    if abs(poc_pos - mid_pos) / mid_pos < 0.20:
        return "normal_distribution"

    return "unknown"


# ── Location ──────────────────────────────────────────────────────────────────
def _compute_location(
    close: float | None,
    poc: float | None,
    vah: float | None,
    val: float | None,
    atr: float,
) -> dict:
    if close is None or poc is None or vah is None or val is None:
        return {
            "position": "inside_value",
            "distance_to_poc_pct": None,
            "distance_to_vah_pct": None,
            "distance_to_val_pct": None,
            "near_prev_session_poc": False,
            "prev_session_poc": None,
        }

    dist_poc = (close - poc) / poc * 100.0 if poc else None
    dist_vah = (close - vah) / vah * 100.0 if vah else None
    dist_val = (close - val) / val * 100.0 if val else None

    # Proximity checks (ATR-based)
    at_poc = abs(close - poc) < atr * ATR_PROXIMITY_POC
    at_vah = abs(close - vah) < atr * ATR_PROXIMITY_VAH
    at_val = abs(close - val) < atr * ATR_PROXIMITY_VAH

    if at_poc:
        position = "at_poc"
    elif at_vah:
        position = "at_vah"
    elif at_val:
        position = "at_val"
    elif close > vah:
        position = "above_value"
    elif close < val:
        position = "below_value"
    else:
        position = "inside_value"

    return {
        "position": position,
        "distance_to_poc_pct": round(dist_poc, 4) if dist_poc is not None else None,
        "distance_to_vah_pct": round(dist_vah, 4) if dist_vah is not None else None,
        "distance_to_val_pct": round(dist_val, 4) if dist_val is not None else None,
        "near_prev_session_poc": False,
        "prev_session_poc": None,
    }


# ── HVN / LVN ─────────────────────────────────────────────────────────────────
def _compute_hvn_lvn(profile: dict[float, dict]) -> tuple[list[dict], list[dict]]:
    if not profile:
        return [], []
    volumes = [ld["total_volume"] for ld in profile.values()]
    avg = sum(volumes) / len(volumes)
    hvn, lvn = [], []
    for level, ld in sorted(profile.items(), key=lambda x: -x[1]["total_volume"]):
        tv = ld["total_volume"]
        if tv >= avg * 1.5:
            hvn.append({"price": level, "total_volume": round(tv, 8)})
        elif tv <= avg * 0.4:
            lvn.append({"price": level, "total_volume": round(tv, 8)})
    hvn.sort(key=lambda x: -x["total_volume"])
    lvn.sort(key=lambda x: x["total_volume"])
    return hvn[:10], lvn[:10]


# ── Market state ──────────────────────────────────────────────────────────────
class MarketStateTracker:
    """Tracks acceptance, failed auction, balance/imbalance state."""

    def __init__(self):
        # For 1M bars: track last 10 for balance, last 5 for imbalance
        self.recent_1m_closes: deque[float] = deque(maxlen=10)
        self.recent_1m_highs:  deque[float] = deque(maxlen=10)
        self.recent_1m_lows:   deque[float] = deque(maxlen=10)
        self.recent_1s_closes: deque[tuple[int, float]] = deque(maxlen=20)
        # For acceptance tracking: consecutive bars outside value
        self.outside_count_above = 0
        self.outside_count_below = 0
        self.last_vah: float | None = None
        self.last_val: float | None = None
        self.last_poc: float | None = None
        self.prev_poc_for_auction: float | None = None
        # Failed auction
        self.auction_above_price: float | None = None
        self.auction_below_price: float | None = None
        self.failed_auction_above = False
        self.failed_auction_below = False
        self.fa_above_price: float | None = None
        self.fa_below_price: float | None = None

    def update_1s(self, close: float, ts: int) -> None:
        self.recent_1s_closes.append((ts, close))

    def update_1m(self, close: float, high: float, low: float) -> None:
        self.recent_1m_closes.append(close)
        self.recent_1m_highs.append(high)
        self.recent_1m_lows.append(low)

    def compute_state(
        self,
        close: float | None,
        poc: float | None,
        vah: float | None,
        val: float | None,
        atr: float,
    ) -> dict:
        if close is None or poc is None or vah is None or val is None:
            return {
                "balance_zone": False,
                "imbalance_zone": False,
                "imbalance_direction": None,
                "acceptance": {"accepted_outside": False, "accepted_direction": None},
                "failed_auction": {"detected": False, "direction": None, "auction_price": None},
            }

        # ── Acceptance / Failed Auction ────────────────────────────────────────
        if self.last_vah is not None and self.last_val is not None:
            prev_vah = self.last_vah
            prev_val = self.last_val

            if close > prev_vah:
                self.outside_count_above += 1
                self.outside_count_below = 0
                # Track where the auction started
                if self.auction_above_price is None:
                    self.auction_above_price = close
                # Clear below auction
                self.failed_auction_below = False
                self.auction_below_price = None
            elif close < prev_val:
                self.outside_count_below += 1
                self.outside_count_above = 0
                if self.auction_below_price is None:
                    self.auction_below_price = close
                self.failed_auction_above = False
                self.auction_above_price = None
            else:
                # Back inside value
                if self.outside_count_above > 0 and self.outside_count_above < 3:
                    self.failed_auction_above = True
                    self.fa_above_price = self.auction_above_price
                elif self.outside_count_below > 0 and self.outside_count_below < 3:
                    self.failed_auction_below = True
                    self.fa_below_price = self.auction_below_price
                else:
                    self.failed_auction_above = False
                    self.failed_auction_below = False
                self.outside_count_above = 0
                self.outside_count_below = 0
                self.auction_above_price = None
                self.auction_below_price = None

        accepted_outside = self.outside_count_above >= 3 or self.outside_count_below >= 3
        accepted_dir = None
        if self.outside_count_above >= 3:
            accepted_dir = "above"
        elif self.outside_count_below >= 3:
            accepted_dir = "below"

        failed_detected = self.failed_auction_above or self.failed_auction_below
        failed_dir = None
        failed_price = None
        if self.failed_auction_above:
            failed_dir = "failed_auction_above"
            failed_price = self.fa_above_price
        elif self.failed_auction_below:
            failed_dir = "failed_auction_below"
            failed_price = self.fa_below_price

        # ── Balance / Imbalance ────────────────────────────────────────────────
        balance_zone = False
        imbalance_zone = False
        imbalance_direction = None

        closes = list(self.recent_1m_closes)
        if len(closes) >= 5:
            # Imbalance: last 5 bars all same direction
            last5 = closes[-5:]
            if all(last5[i+1] > last5[i] for i in range(4)):
                imbalance_zone = True
                imbalance_direction = "bullish"
            elif all(last5[i+1] < last5[i] for i in range(4)):
                imbalance_zone = True
                imbalance_direction = "bearish"

        if len(closes) >= 10 and poc is not None and self.last_poc is not None:
            poc_drift = abs(poc - self.last_poc)
            last10_in_va = all(val <= c <= vah for c in closes[-10:])
            if last10_in_va and poc_drift < atr * 0.3:
                balance_zone = True

        # Store for next iteration
        self.last_vah = vah
        self.last_val = val
        self.last_poc = poc

        return {
            "balance_zone": balance_zone,
            "imbalance_zone": imbalance_zone,
            "imbalance_direction": imbalance_direction,
            "acceptance": {
                "accepted_outside": accepted_outside,
                "accepted_direction": accepted_dir,
            },
            "failed_auction": {
                "detected": failed_detected,
                "direction": failed_dir,
                "auction_price": failed_price,
            },
        }


# ── Bias hint ─────────────────────────────────────────────────────────────────
def _compute_bias(
    position: str,
    shape: str,
    market_state: dict,
) -> dict:
    # Location bias
    loc_map = {
        "above_value": "long",
        "below_value": "short",
        "inside_value": "neutral",
        "at_poc": "neutral",
        "at_vah": "neutral",
        "at_val": "neutral",
    }
    location_bias = loc_map.get(position, "neutral")

    # Refine at_vah / at_val based on failed auction
    fa = market_state.get("failed_auction", {})
    if fa.get("detected"):
        if fa.get("direction") == "failed_auction_above":
            location_bias = "short"
        elif fa.get("direction") == "failed_auction_below":
            location_bias = "long"

    # Shape bias
    shape_map = {
        "p_shape": "long",
        "b_shape": "short",
        "normal_distribution": "neutral",
        "bimodal": "neutral",
        "thin_profile": "neutral",
        "elongated_profile": "neutral",
        "unknown": "neutral",
    }
    shape_bias = shape_map.get(shape, "neutral")

    # State bias
    state_bias = "neutral"
    reasoning_parts = []
    if market_state.get("imbalance_zone"):
        d = market_state.get("imbalance_direction")
        if d == "bullish":
            state_bias = "long"
        elif d == "bearish":
            state_bias = "short"
    if market_state.get("balance_zone"):
        state_bias = "neutral"
    if fa.get("detected"):
        if fa.get("direction") == "failed_auction_above":
            state_bias = "short"
        elif fa.get("direction") == "failed_auction_below":
            state_bias = "long"

    # Combined
    votes = [location_bias, shape_bias, state_bias]
    long_votes  = votes.count("long")
    short_votes = votes.count("short")
    if long_votes == 3:
        combined = "long"
    elif short_votes == 3:
        combined = "short"
    elif long_votes >= 2 and short_votes == 0:
        combined = "long"
    elif short_votes >= 2 and long_votes == 0:
        combined = "short"
    else:
        combined = "neutral"

    # Reasoning string
    parts = []
    if shape != "unknown":
        parts.append(shape)
    if position not in ("inside_value",):
        parts.append(position)
    if fa.get("detected") and fa.get("direction"):
        parts.append(fa["direction"])
    if market_state.get("balance_zone"):
        parts.append("balance_zone")
    if market_state.get("imbalance_zone"):
        d = market_state.get("imbalance_direction") or ""
        parts.append(f"imbalance_{d}")
    reasoning = " + ".join(parts) if parts else "neutral"

    return {
        "location_bias": location_bias,
        "shape_bias":    shape_bias,
        "state_bias":    state_bias,
        "combined_hint": combined,
        "reasoning":     reasoning,
    }


# ── Validation ─────────────────────────────────────────────────────────────────
def _validate_output(rec: dict) -> list[str]:
    errors = []
    p = rec.get("profile", {})
    poc = p.get("poc")
    vah = p.get("vah")
    val = p.get("val")

    # 1. val <= poc <= vah
    if poc is not None and vah is not None and val is not None:
        if not (val <= poc <= vah):
            errors.append(f"[1] val={val} poc={poc} vah={vah} order violated")

    # 2. value_area_volume <= total_volume
    va_vol = _sf(p.get("value_area_volume"))
    tot_vol = _sf(p.get("total_volume"))
    if va_vol > tot_vol + 1e-9:
        errors.append(f"[2] value_area_volume={va_vol} > total_volume={tot_vol}")

    # 3. buy + sell == total (with tolerance)
    bv = _sf(p.get("buy_volume"))
    sv = _sf(p.get("sell_volume"))
    if abs(bv + sv - tot_vol) > 1e-9 * max(1.0, tot_vol):
        errors.append(f"[3] buy={bv}+sell={sv} != total={tot_vol}")

    # 4. profile_shape valid
    valid_shapes = {"normal_distribution","p_shape","b_shape","bimodal",
                    "thin_profile","elongated_profile","unknown"}
    if p.get("profile_shape") not in valid_shapes:
        errors.append(f"[4] profile_shape invalid: {p.get('profile_shape')}")

    # 5. position valid
    valid_pos = {"above_value","inside_value","below_value","at_vah","at_val","at_poc"}
    loc = rec.get("location", {})
    if loc.get("position") not in valid_pos:
        errors.append(f"[5] position invalid: {loc.get('position')}")

    # 6. distances finite
    for k in ("distance_to_poc_pct","distance_to_vah_pct","distance_to_val_pct"):
        v = loc.get(k)
        if v is not None and not math.isfinite(v):
            errors.append(f"[6] {k} is NaN/inf")

    # 7. combined_hint valid
    bh = rec.get("bias_hint", {})
    if bh.get("combined_hint") not in ("long","short","neutral"):
        errors.append(f"[7] combined_hint invalid: {bh.get('combined_hint')}")

    # 8. failed_auction direction consistency
    ms = rec.get("market_state", {})
    fa = ms.get("failed_auction", {})
    if fa.get("detected") and not fa.get("direction"):
        errors.append("[8] failed_auction detected but direction is null")

    # 9. hvn_levels volumes
    for h in p.get("hvn_levels", []):
        tv = _sf(h.get("total_volume"))
        if tv <= 0:
            errors.append(f"[9] hvn_level with non-positive volume: {h}")

    # 10. price_levels sorted descending
    pls = p.get("price_levels", [])
    for i in range(len(pls) - 1):
        if pls[i]["total_volume"] < pls[i+1]["total_volume"]:
            errors.append(f"[10] price_levels not sorted desc at index {i}")
            break

    return errors


# ── Output builder ─────────────────────────────────────────────────────────────
def _build_output(
    profile_type: str,
    profile: dict[float, dict],
    current_price: float | None,
    bars_included: int,
    ts: int,
    market_state: dict,
    atr: float,
    prev_session_poc: float | None = None,
) -> dict:
    poc, vah, val, va_vol, va_levels = _compute_poc_vah_val(profile, current_price)

    total_vol = sum(ld["total_volume"] for ld in profile.values())
    buy_vol   = sum(ld["buy_volume"]   for ld in profile.values())
    sell_vol  = sum(ld["sell_volume"]  for ld in profile.values())
    delta     = buy_vol - sell_vol

    shape = _compute_profile_shape(profile, poc, vah, val, atr)
    hvn, lvn = _compute_hvn_lvn(profile)

    # Price levels sorted desc by total_volume
    price_levels_out = []
    for level in sorted(profile.keys(), key=lambda l: -profile[l]["total_volume"]):
        ld = profile[level]
        price_levels_out.append({
            "price":        level,
            "total_volume": round(ld["total_volume"], 8),
            "buy_volume":   round(ld["buy_volume"],   8),
            "sell_volume":  round(ld["sell_volume"],  8),
            "delta":        round(ld["delta"],        8),
            "tpo_count":    ld["tpo_count"],
        })

    loc = _compute_location(current_price, poc, vah, val, atr)

    # Inject prev_session_poc
    if prev_session_poc is not None and current_price is not None:
        near = abs(current_price - prev_session_poc) < atr * 2.0
        loc["near_prev_session_poc"] = near
        loc["prev_session_poc"] = prev_session_poc
    else:
        loc["near_prev_session_poc"] = False
        loc["prev_session_poc"] = prev_session_poc

    bias = _compute_bias(loc["position"], shape, market_state)

    return {
        "engine":        "volume_profile_engine",
        "symbol":        SYMBOL,
        "profile_type":  profile_type,
        "ts":            ts,
        "current_price": current_price,
        "bars_included": bars_included,
        "profile": {
            "poc":               poc,
            "vah":               vah,
            "val":               val,
            "value_area_volume": round(va_vol,   8),
            "total_volume":      round(total_vol, 8),
            "buy_volume":        round(buy_vol,   8),
            "sell_volume":       round(sell_vol,  8),
            "delta":             round(delta,     8),
            "profile_shape":     shape,
            "hvn_levels":        hvn,
            "lvn_levels":        lvn,
            "price_levels":      price_levels_out,
        },
        "location":     loc,
        "market_state": market_state,
        "bias_hint":    bias,
    }


# ── Terminal output ────────────────────────────────────────────────────────────
def _print_1m(rec: dict) -> None:
    if FULL_PRINT:
        print(json.dumps(rec, ensure_ascii=False), flush=True)
        return
    p = rec.get("profile", {})
    loc = rec.get("location", {})
    ms = rec.get("market_state", {})
    bh = rec.get("bias_hint", {})
    fa = ms.get("failed_auction", {})
    fa_str = fa.get("direction", "none") if fa.get("detected") else "none"
    print(
        f"[VOL PROFILE 1M] ts={rec['ts']} poc={p.get('poc')} "
        f"vah={p.get('vah')} val={p.get('val')}\n"
        f"  shape={p.get('profile_shape')} location={loc.get('position')} "
        f"balance={ms.get('balance_zone')}\n"
        f"  hint={bh.get('combined_hint','').upper()} failed_auction={fa_str}",
        flush=True
    )

def _print_session(rec: dict) -> None:
    if FULL_PRINT:
        print(json.dumps(rec, ensure_ascii=False), flush=True)
        return
    p = rec.get("profile", {})
    bh = rec.get("bias_hint", {})
    print(
        f"[VOL PROFILE SESSION] ts={rec['ts']} poc={p.get('poc')} "
        f"bars={rec.get('bars_included')}\n"
        f"  shape={p.get('profile_shape')} hint={bh.get('combined_hint','').upper()}",
        flush=True
    )

def _print_failed_auction(rec: dict) -> None:
    ms = rec.get("market_state", {})
    fa = ms.get("failed_auction", {})
    if not fa.get("detected"):
        return
    direction = fa.get("direction", "")
    price = fa.get("auction_price")
    hint = "SHORT BIAS" if "above" in direction else "LONG BIAS"
    print(
        f"[FAILED AUCTION] ts={rec['ts']} direction={direction} price={price}\n"
        f"  returned_to_value=true → {hint}",
        flush=True
    )


# ── ATR estimation ─────────────────────────────────────────────────────────────
def _estimate_atr(recent_1m_highs: list[float], recent_1m_lows: list[float]) -> float:
    if not recent_1m_highs or not recent_1m_lows:
        return 50.0  # BTC default fallback
    ranges = [h - l for h, l in zip(recent_1m_highs, recent_1m_lows) if h > l]
    if not ranges:
        return 50.0
    return sum(ranges) / len(ranges)


# ── Volume Memory ──────────────────────────────────────────────────────────────
def _save_memory(memory_type: str, price: float, strength: float,
                 first_ts: int, last_ts: int, timeframe: str, notes: str) -> None:
    rec = {
        "ts": _now_ms(),
        "symbol": SYMBOL,
        "memory_type": memory_type,
        "price": price,
        "strength": round(strength, 4),
        "first_seen_ts": first_ts,
        "last_confirmed_ts": last_ts,
        "timeframe": timeframe,
        "notes": notes,
    }
    _append_jsonl(OUT_MEMORY, rec)

def _load_prev_session_poc() -> float | None:
    """Return the most recent session_poc from volume_memory.jsonl."""
    recs = _read_all_jsonl(OUT_MEMORY)
    for rec in reversed(recs):
        if rec.get("memory_type") == "session_poc":
            return _sf(rec.get("price")) or None
    return None


# ── Shared async context ───────────────────────────────────────────────────────
class VolProfileCtx:
    def __init__(self):
        # Rolling 1S: deque of (ts_s, {level_str: {tv,bv,sv,ts}})
        self.rolling_1s: deque[tuple[int, dict]] = deque()
        self.rolling_1s_last_output: float = 0.0

        # Rolling 1M: deque of contribution dicts
        self.rolling_1m: deque[dict] = deque(maxlen=ROLLING_1M_BARS)
        self.rolling_1m_ts: deque[int] = deque(maxlen=ROLLING_1M_BARS)

        # Rolling 5M: deque of contribution dicts
        self.rolling_5m: deque[dict] = deque(maxlen=ROLLING_5M_BARS)
        self.rolling_5m_ts: deque[int] = deque(maxlen=ROLLING_5M_BARS)

        # Session profile
        self.session_profile: dict[float, dict] = {}
        self.session_bars: int = 0
        self.session_day_start_ms: int = _utc_day_start_ms()
        self.session_last_output: float = 0.0

        # Shared state
        self.current_close: float | None = None
        self.current_ts: int = 0

        # ATR
        self.recent_highs: deque[float] = deque(maxlen=14)
        self.recent_lows:  deque[float] = deque(maxlen=14)

        # Market state trackers
        self.ms_1s  = MarketStateTracker()
        self.ms_1m  = MarketStateTracker()
        self.ms_5m  = MarketStateTracker()
        self.ms_ses = MarketStateTracker()

        # Previous session POC
        self.prev_session_poc: float | None = None

        # Output file handles (opened in run)
        self.fh_1s:  object = None
        self.fh_1m:  object = None
        self.fh_5m:  object = None
        self.fh_ses: object = None

        # 1M last close tracking for market state
        self.last_1m_close: float | None = None
        self.last_5m_close: float | None = None


# ── Profile flush functions ────────────────────────────────────────────────────
def _flush_1s(ctx: VolProfileCtx, ts: int) -> None:
    profile = _build_merged_profile([c for _, c in ctx.rolling_1s])
    if not profile:
        return
    atr = _estimate_atr(list(ctx.recent_highs), list(ctx.recent_lows))
    ms = ctx.ms_1s.compute_state(
        ctx.current_close, None, None, None, atr
    )
    if profile:
        poc, vah, val, _, _ = _compute_poc_vah_val(profile, ctx.current_close)
        ms = ctx.ms_1s.compute_state(ctx.current_close, poc, vah, val, atr)

    rec = _build_output("1S", profile, ctx.current_close,
                        len(ctx.rolling_1s), ts, ms, atr, ctx.prev_session_poc)
    errs = _validate_output(rec)
    if errs:
        print(f"[VP] 1S validation error: {errs}", flush=True)
        return
    if ctx.fh_1s:
        _write_jsonl(ctx.fh_1s, rec)

def _flush_1m(ctx: VolProfileCtx, ts: int) -> None:
    all_contribs = list(ctx.rolling_1m)
    profile = _build_merged_profile(all_contribs)
    if not profile:
        return
    atr = _estimate_atr(list(ctx.recent_highs), list(ctx.recent_lows))
    poc, vah, val, _, _ = _compute_poc_vah_val(profile, ctx.current_close)
    ms = ctx.ms_1m.compute_state(ctx.current_close, poc, vah, val, atr)

    rec = _build_output("1M", profile, ctx.current_close,
                        len(ctx.rolling_1m), ts, ms, atr, ctx.prev_session_poc)
    errs = _validate_output(rec)
    if errs:
        print(f"[VP] 1M validation error: {errs}", flush=True)
        return
    if ctx.fh_1m:
        _write_jsonl(ctx.fh_1m, rec)
    _print_1m(rec)
    if ms.get("failed_auction", {}).get("detected"):
        _print_failed_auction(rec)

def _flush_5m(ctx: VolProfileCtx, ts: int) -> None:
    all_contribs = list(ctx.rolling_5m)
    profile = _build_merged_profile(all_contribs)
    if not profile:
        return
    atr = _estimate_atr(list(ctx.recent_highs), list(ctx.recent_lows))
    poc, vah, val, _, _ = _compute_poc_vah_val(profile, ctx.current_close)
    ms = ctx.ms_5m.compute_state(ctx.current_close, poc, vah, val, atr)

    rec = _build_output("5M", profile, ctx.current_close,
                        len(ctx.rolling_5m), ts, ms, atr, ctx.prev_session_poc)
    errs = _validate_output(rec)
    if errs:
        print(f"[VP] 5M validation error: {errs}", flush=True)
        return
    if ctx.fh_5m:
        _write_jsonl(ctx.fh_5m, rec)

def _flush_session(ctx: VolProfileCtx, ts: int) -> None:
    if not ctx.session_profile:
        return
    atr = _estimate_atr(list(ctx.recent_highs), list(ctx.recent_lows))
    poc, vah, val, _, _ = _compute_poc_vah_val(ctx.session_profile, ctx.current_close)
    ms = ctx.ms_ses.compute_state(ctx.current_close, poc, vah, val, atr)

    rec = _build_output("session", ctx.session_profile, ctx.current_close,
                        ctx.session_bars, ts, ms, atr, ctx.prev_session_poc)
    errs = _validate_output(rec)
    if errs:
        print(f"[VP] session validation error: {errs}", flush=True)
        return
    if ctx.fh_ses:
        _write_jsonl(ctx.fh_ses, rec)
    _print_session(rec)


# ── Session reset ──────────────────────────────────────────────────────────────
def _maybe_reset_session(ctx: VolProfileCtx, ts_ms: int) -> None:
    day_start = _utc_day_start_ms()
    if day_start <= ctx.session_day_start_ms:
        return
    # New UTC day — save memory and reset
    if ctx.session_profile:
        p = ctx.session_profile
        poc, vah, val, _, _ = _compute_poc_vah_val(p, ctx.current_close)
        atr = _estimate_atr(list(ctx.recent_highs), list(ctx.recent_lows))
        total_vol = sum(ld["total_volume"] for ld in p.values())
        if poc is not None:
            _save_memory("session_poc", poc, total_vol, ctx.session_day_start_ms, ts_ms,
                         "session", f"session ended poc={poc}")
            ctx.prev_session_poc = poc
        if vah is not None:
            _save_memory("session_vah", vah, total_vol * 0.5, ctx.session_day_start_ms, ts_ms,
                         "session", f"session ended vah={vah}")
        if val is not None:
            _save_memory("session_val", val, total_vol * 0.5, ctx.session_day_start_ms, ts_ms,
                         "session", f"session ended val={val}")

    ctx.session_profile = {}
    ctx.session_bars = 0
    ctx.session_day_start_ms = day_start
    print(f"[VP] Session reset for new UTC day", flush=True)


# ── 1S task ────────────────────────────────────────────────────────────────────
async def _task_1s(ctx: VolProfileCtx, batch_recs: list[dict] | None = None) -> None:
    """Process 1S records for rolling 1S profile and session profile."""
    is_batch = batch_recs is not None

    def _process_1s_record(raw: dict) -> None:
        cdna = raw.get("candle_dna") or {}
        if not (raw.get("footprint_dna") or {}).get("has_trade"):
            return
        ts_ms = int(raw.get("window_start_ts", 0))
        ts_s  = ts_ms // 1000

        close_d = cdna.get("close") or {}
        close = _sf(close_d.get("price") if isinstance(close_d, dict) else close_d, 0.0)
        if close <= 0:
            return

        ctx.current_close = close
        ctx.current_ts = ts_ms

        # Session reset check
        _maybe_reset_session(ctx, ts_ms)

        # Extract levels
        levels = _extract_1s_levels(raw)
        if not levels:
            return

        # Rolling 1S: add with timestamp (seconds)
        ctx.rolling_1s.append((ts_s, levels))

        # Remove entries older than ROLLING_1S_SEC
        cutoff = ts_s - ROLLING_1S_SEC
        while ctx.rolling_1s and ctx.rolling_1s[0][0] < cutoff:
            ctx.rolling_1s.popleft()

        # Session
        for level, ld in levels.items():
            if level not in ctx.session_profile:
                ctx.session_profile[level] = _empty_level()
            _merge_level(ctx.session_profile[level], ld["tv"], ld["bv"], ld["sv"], ts_ms)
        ctx.session_bars += 1

        # Market state for session
        ctx.ms_ses.update_1s(close, ts_ms)

    if is_batch:
        for raw in batch_recs:
            if HALT_FILE.exists():
                return
            _process_1s_record(raw)
            ts_ms = int(raw.get("window_start_ts", 0))
            ts_s = ts_ms // 1000
            # Output every OUTPUT_1S_EVERY seconds in batch
            if ts_s % OUTPUT_1S_EVERY == 0:
                _flush_1s(ctx, ts_ms)
            if ts_s % OUTPUT_SES_EVERY == 0:
                _flush_session(ctx, ts_ms)
        return

    # Live mode
    while not FILE_1S.exists():
        if HALT_FILE.exists():
            return
        print(f"[VP] Waiting for {FILE_1S}...", flush=True)
        await asyncio.sleep(2.0)

    with open(FILE_1S, "r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                # Periodic outputs
                now = time.time()
                ts_ms = ctx.current_ts or _now_ms()
                if now - ctx.rolling_1s_last_output >= OUTPUT_1S_EVERY:
                    ctx.rolling_1s_last_output = now
                    _flush_1s(ctx, ts_ms)
                if now - ctx.session_last_output >= OUTPUT_SES_EVERY:
                    ctx.session_last_output = now
                    _flush_session(ctx, ts_ms)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue
            _process_1s_record(raw)


# ── 1M task ────────────────────────────────────────────────────────────────────
async def _task_1m(ctx: VolProfileCtx, batch_recs: list[dict] | None = None) -> None:
    is_batch = batch_recs is not None

    def _process_1m_record(raw: dict) -> None:
        ts_ms = int(raw.get("window_start_ts", 0))
        ohlc = raw.get("ohlc") or {}
        close = _sf(ohlc.get("close"), 0.0)
        high  = _sf(ohlc.get("high"),  0.0)
        low   = _sf(ohlc.get("low"),   0.0)
        if close > 0:
            ctx.current_close = close
            ctx.current_ts = ts_ms
        if high > 0 and low > 0:
            ctx.recent_highs.append(high)
            ctx.recent_lows.append(low)
            ctx.ms_1m.update_1m(close, high, low)

        levels = _extract_aligned_levels(raw)
        if not levels:
            return

        ctx.rolling_1m.append(levels)
        ctx.rolling_1m_ts.append(ts_ms)
        _flush_1m(ctx, ts_ms)

    if is_batch:
        for raw in batch_recs:
            if HALT_FILE.exists():
                return
            _process_1m_record(raw)
        return

    while not FILE_1M.exists():
        if HALT_FILE.exists():
            return
        print(f"[VP] Waiting for {FILE_1M}...", flush=True)
        await asyncio.sleep(2.0)

    with open(FILE_1M, "r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
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
            _process_1m_record(raw)


# ── 5M task ────────────────────────────────────────────────────────────────────
async def _task_5m(ctx: VolProfileCtx, batch_recs: list[dict] | None = None) -> None:
    is_batch = batch_recs is not None

    def _process_5m_record(raw: dict) -> None:
        ts_ms = int(raw.get("window_start_ts", 0))
        ohlc = raw.get("ohlc") or {}
        close = _sf(ohlc.get("close"), 0.0)
        high  = _sf(ohlc.get("high"),  0.0)
        low   = _sf(ohlc.get("low"),   0.0)
        if high > 0 and low > 0:
            ctx.ms_5m.update_1m(close, high, low)

        levels = _extract_aligned_levels(raw)
        if not levels:
            return

        ctx.rolling_5m.append(levels)
        ctx.rolling_5m_ts.append(ts_ms)
        _flush_5m(ctx, ts_ms)

    if is_batch:
        for raw in batch_recs:
            if HALT_FILE.exists():
                return
            _process_5m_record(raw)
        return

    while not FILE_5M.exists():
        if HALT_FILE.exists():
            return
        print(f"[VP] Waiting for {FILE_5M}...", flush=True)
        await asyncio.sleep(2.0)

    with open(FILE_5M, "r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
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
            _process_5m_record(raw)


# ── Batch mode ─────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[VP] Batch mode — loading input files (warm-up limits)", flush=True)

    # Warm-up: load only last N lines per file (memory-efficient)
    recs_1s = _read_last_n_jsonl(FILE_1S, maxlen=3600)
    recs_1m = _read_last_n_jsonl(FILE_1M, maxlen=1000)
    recs_5m = _read_last_n_jsonl(FILE_5M, maxlen=1000)

    print(f"[VP] Loaded: 1S={len(recs_1s)} 1M={len(recs_1m)} 5M={len(recs_5m)}", flush=True)

    ctx = VolProfileCtx()
    ctx.prev_session_poc = _load_prev_session_poc()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with (open(OUT_1S,      "a", encoding="utf-8") as fh_1s,
          open(OUT_1M,      "a", encoding="utf-8") as fh_1m,
          open(OUT_5M,      "a", encoding="utf-8") as fh_5m,
          open(OUT_SESSION, "a", encoding="utf-8") as fh_ses):

        ctx.fh_1s  = fh_1s
        ctx.fh_1m  = fh_1m
        ctx.fh_5m  = fh_5m
        ctx.fh_ses = fh_ses

        # Process 1M and 5M first to build ATR
        asyncio.run(_task_1m(ctx, recs_1m))
        asyncio.run(_task_5m(ctx, recs_5m))
        # Then 1S (uses ATR from above)
        asyncio.run(_task_1s(ctx, recs_1s))

        # Final session flush
        if ctx.session_profile:
            _flush_session(ctx, ctx.current_ts or _now_ms())

    print("[VP] Batch done.", flush=True)


# ── Live mode ──────────────────────────────────────────────────────────────────
async def run_live() -> None:
    ctx = VolProfileCtx()
    ctx.prev_session_poc = _load_prev_session_poc()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with (open(OUT_1S,      "a", encoding="utf-8") as fh_1s,
          open(OUT_1M,      "a", encoding="utf-8") as fh_1m,
          open(OUT_5M,      "a", encoding="utf-8") as fh_5m,
          open(OUT_SESSION, "a", encoding="utf-8") as fh_ses):

        ctx.fh_1s  = fh_1s
        ctx.fh_1m  = fh_1m
        ctx.fh_5m  = fh_5m
        ctx.fh_ses = fh_ses

        tasks = [
            asyncio.create_task(_task_1s(ctx), name="vp-1s"),
            asyncio.create_task(_task_1m(ctx), name="vp-1m"),
            asyncio.create_task(_task_5m(ctx), name="vp-5m"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("[VP] Tasks cancelled", flush=True)


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    if HALT_FILE.exists():
        print("[VP] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Volume Profile Engine — Layer 8")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch()
    else:
        print("[VP] Starting live mode", flush=True)
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
