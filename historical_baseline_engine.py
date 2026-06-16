"""
NurtacCoreEngineClaude — Layer-3: Historical Baseline + Context Metrics Engine

Reads Layer-0/1/2 JSONL DNA files and produces reference metrics:
ATR, VWAP, CVD, and percentile/z-score baseline statistics for each timeframe.

No Binance API/WebSocket calls are made. No signals or trade decisions are generated.

Usage:
  python historical_baseline_engine.py --mode batch
  python historical_baseline_engine.py --mode live
  FULL_PRINT=true python historical_baseline_engine.py --mode batch
"""

import argparse
import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from typing import Optional

# Ensure UTF-8 terminal output on Windows (cp1252 can't encode Turkish chars)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ─────────────────────────────────────────────────────────────────────
SYMBOL = "BTCUSDT"

DATA_DIR    = "data"
HALT_FILE   = os.path.join(DATA_DIR, "SYSTEM_HALT")
OUTPUT_FILE = os.path.join(DATA_DIR, "historical_baseline_dna.jsonl")

SOURCE_FILES: dict[str, str] = {
    "1S":  os.path.join(DATA_DIR, "combined_1s_dna_btcusdt.jsonl"),
    "3S":  os.path.join(DATA_DIR, "rolling_3s_dna.jsonl"),
    "5S":  os.path.join(DATA_DIR, "rolling_5s_dna.jsonl"),
    "15S": os.path.join(DATA_DIR, "rolling_15s_dna.jsonl"),
    "1M":  os.path.join(DATA_DIR, "aligned_1m_candle_dna.jsonl"),
    "5M":  os.path.join(DATA_DIR, "aligned_5m_candle_dna.jsonl"),
    "15M": os.path.join(DATA_DIR, "aligned_15m_candle_dna.jsonl"),
    "1H":  os.path.join(DATA_DIR, "aligned_1h_candle_dna.jsonl"),
    "4H":  os.path.join(DATA_DIR, "aligned_4h_candle_dna.jsonl"),
    "1D":  os.path.join(DATA_DIR, "aligned_1d_candle_dna.jsonl"),
}

BASELINE_WINDOWS: dict[str, int] = {"short": 20, "medium": 100, "long": 200}
ATR_PERIOD   = 14
MS_PER_DAY   = 86_400_000

METRIC_NAMES: list[str] = [
    "range", "total_volume", "buy_volume", "sell_volume",
    "delta", "absolute_delta", "trade_count",
    "footprint_price_level_count",
    "bid_update_count", "ask_update_count",
    "depth_balance", "depth_imbalance", "close_price",
]

FULL_PRINT      = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_INTERVAL   = 0.05   # seconds between readline retries (live mode)
FILE_WAIT_SLEEP = 0.5    # seconds between file-existence checks (live mode)


# ── SYSTEM_HALT check ─────────────────────────────────────────────────────────
def _check_system_halt() -> bool:
    """Check if SYSTEM_HALT exists. Return True if halt is set."""
    if os.path.exists(HALT_FILE):
        try:
            with open(HALT_FILE, "r", encoding="utf-8") as fh:
                info = json.loads(fh.read().strip())
            reason = info.get("reason", "unknown")
        except Exception:
            reason = "unknown"
        print(f"[HBE] SYSTEM_HALT tespit edildi: {reason}")
        return True
    return False


# ── Normalization ──────────────────────────────────────────────────────────────
def _price(v) -> Optional[float]:
    """Extract .price from an OHLC event object, or None."""
    if v is None:
        return None
    if isinstance(v, dict):
        return v.get("price")
    return float(v)


def _normalize_1s(raw: dict) -> dict:
    """Normalize a Layer-0 combined_1s_dna record."""
    cd = raw.get("candle_dna", {})
    fd = raw.get("footprint_dna", {})
    dd = raw.get("depth_dna", {})
    return {
        "symbol":                      raw.get("symbol", SYMBOL),
        "timeframe":                   "1S",
        "window_start_ts":             raw["window_start_ts"],
        "window_end_ts":               raw["window_end_ts"],
        "open_price":                  _price(cd.get("open")),
        "high_price":                  _price(cd.get("high")),
        "low_price":                   _price(cd.get("low")),
        "close_price":                 _price(cd.get("close")),
        "buy_volume":                  float(cd.get("buy_volume", 0.0)),
        "sell_volume":                 float(cd.get("sell_volume", 0.0)),
        "total_volume":                float(cd.get("total_volume", 0.0)),
        "delta":                       float(cd.get("delta", 0.0)),
        "trade_count":                 int(cd.get("trade_count", 0)),
        "footprint_price_level_count": len(fd.get("price_levels", [])),
        "bid_update_count":            int(dd.get("bid_update_count", 0)),
        "ask_update_count":            int(dd.get("ask_update_count", 0)),
        "depth_balance":               int(dd.get("balance", 0)),
        "depth_imbalance":             float(dd.get("imbalance", 0.0)),
    }


def _normalize_layer1_or_layer2(raw: dict, timeframe: str) -> dict:
    """Normalize a Layer-1 rolling or Layer-2 aligned record."""
    ohlc  = raw.get("ohlc", {})
    vol   = raw.get("volume", {})
    tf_   = raw.get("trade_flow", {})
    fp    = raw.get("footprint", raw.get("footprint_dna", {}))
    df    = raw.get("depth_flow", {})
    return {
        "symbol":                      raw.get("symbol", SYMBOL),
        "timeframe":                   timeframe,
        "window_start_ts":             raw["window_start_ts"],
        "window_end_ts":               raw["window_end_ts"],
        "open_price":                  _price(ohlc.get("open")),
        "high_price":                  _price(ohlc.get("high")),
        "low_price":                   _price(ohlc.get("low")),
        "close_price":                 _price(ohlc.get("close")),
        "buy_volume":                  float(vol.get("buy_volume", 0.0)),
        "sell_volume":                 float(vol.get("sell_volume", 0.0)),
        "total_volume":                float(vol.get("total_volume", 0.0)),
        "delta":                       float(vol.get("delta", 0.0)),
        "trade_count":                 int(tf_.get("trade_count", 0)),
        "footprint_price_level_count": len(fp.get("price_levels", [])),
        "bid_update_count":            int(df.get("bid_update_count", 0)),
        "ask_update_count":            int(df.get("ask_update_count", 0)),
        "depth_balance":               int(df.get("balance", 0)),
        "depth_imbalance":             float(df.get("imbalance", 0.0)),
    }


def normalize_record(raw: dict, timeframe: str) -> dict:
    if timeframe == "1S":
        return _normalize_1s(raw)
    return _normalize_layer1_or_layer2(raw, timeframe)


# ── Statistics helpers ─────────────────────────────────────────────────────────
def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std_pop(values: list[float], mean: float) -> float:
    """Population standard deviation (ddof=0)."""
    n = len(values)
    if n < 2:
        return 0.0
    return math.sqrt(sum((v - mean) ** 2 for v in values) / n)


def _percentile_linear(sv: list[float], p: float) -> float:
    """Linear interpolation percentile (matches numpy.percentile 'linear' method)."""
    n = len(sv)
    if n == 0:
        return 0.0
    if n == 1:
        return sv[0]
    i  = (p / 100.0) * (n - 1)
    lo = int(i)
    hi = lo + 1
    if hi >= n:
        return sv[-1]
    return sv[lo] + (i - lo) * (sv[hi] - sv[lo])


def _latest_percentile(sv: list[float], latest: float) -> float:
    """Percentage of sorted values <= latest, via binary search."""
    n = len(sv)
    if n < 2:
        return 50.0
    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if sv[mid] <= latest:
            lo = mid + 1
        else:
            hi = mid
    return lo / n * 100.0


def _metric_stats(values: list[float], latest: float) -> dict:
    """Full statistics for a metric window."""
    n = len(values)
    if n == 0:
        return {
            "sample_count": 0, "mean": 0.0, "median": 0.0,
            "min": 0.0, "max": 0.0, "std": 0.0,
            "p10": 0.0, "p25": 0.0, "p50": 0.0, "p75": 0.0, "p90": 0.0,
            "latest": latest, "latest_percentile": 50.0, "z_score": 0.0,
        }

    sv   = sorted(values)
    mean = _mean(values)
    std  = _std_pop(values, mean)
    z    = (latest - mean) / std if std > 0 else 0.0
    if not math.isfinite(z):
        z = 0.0
    lp = _latest_percentile(sv, latest) if n >= 2 else 50.0

    return {
        "sample_count":      n,
        "mean":              round(mean, 8),
        "median":            round(_percentile_linear(sv, 50.0), 8),
        "min":               sv[0],
        "max":               sv[-1],
        "std":               round(std, 8),
        "p10":               round(_percentile_linear(sv, 10.0), 8),
        "p25":               round(_percentile_linear(sv, 25.0), 8),
        "p50":               round(_percentile_linear(sv, 50.0), 8),
        "p75":               round(_percentile_linear(sv, 75.0), 8),
        "p90":               round(_percentile_linear(sv, 90.0), 8),
        "latest":            latest,
        "latest_percentile": round(lp, 4),
        "z_score":           round(z, 6),
    }


# ── Per-timeframe state ────────────────────────────────────────────────────────
class TimeframeState:
    """All mutable state for one timeframe's baseline computation."""

    def __init__(self, timeframe: str, source_file: str) -> None:
        self.timeframe   = timeframe
        self.source_file = source_file

        # Rolling record buffer; maxlen == largest window
        self.records: deque[dict] = deque(maxlen=BASELINE_WINDOWS["long"])
        # True-Range values for ATR; only appended when high/low both non-null
        self.tr_values: deque[float] = deque(maxlen=BASELINE_WINDOWS["long"])

        self.prev_close: Optional[float] = None
        self.first_ts: Optional[int]     = None
        self.last_ts: Optional[int]      = None

        # VWAP session
        self._vwap_day: Optional[int] = None  # UTC day index
        self._vwap_num: float         = 0.0   # Σ(typical_price × volume)
        self._vwap_den: float         = 0.0   # Σ(volume)
        self._vwap_session_start: Optional[int] = None

        # CVD session
        self._cvd_day: Optional[int] = None
        self._cvd: float             = 0.0
        self._prev_cvd: float        = 0.0
        self._cvd_session_start: Optional[int] = None

    # ── Session helpers ────────────────────────────────────────────────────────
    def _maybe_reset_session(self, ts: int) -> None:
        """Reset VWAP and CVD accumulators when the UTC day changes."""
        day = ts // MS_PER_DAY
        if self._vwap_day != day:
            self._vwap_day          = day
            self._vwap_session_start = day * MS_PER_DAY
            self._vwap_num          = 0.0
            self._vwap_den          = 0.0
        if self._cvd_day != day:
            self._cvd_day           = day
            self._cvd_session_start = day * MS_PER_DAY
            self._cvd               = 0.0
            self._prev_cvd          = 0.0

    # ── True Range ────────────────────────────────────────────────────────────
    def _true_range(self, rec: dict) -> Optional[float]:
        h = rec["high_price"]
        l = rec["low_price"]
        if h is None or l is None:
            return None
        if self.prev_close is None:
            return h - l
        return max(h - l, abs(h - self.prev_close), abs(l - self.prev_close))

    # ── ATR output ────────────────────────────────────────────────────────────
    def _atr_output(self, current_tr: Optional[float]) -> dict:
        tr_list = list(self.tr_values)
        n       = len(tr_list)
        cur_tr  = current_tr if current_tr is not None else 0.0

        atr_window = tr_list[-ATR_PERIOD:] if n >= ATR_PERIOD else tr_list
        atr_val    = sum(atr_window) / len(atr_window) if atr_window else cur_tr
        atr_n      = len(atr_window)

        def _win_stats(size: int) -> tuple[float, float]:
            w = tr_list[-size:] if len(tr_list) >= size else tr_list
            if len(w) < 2:
                return 50.0, 0.0
            sv   = sorted(w)
            mean = _mean(w)
            std  = _std_pop(w, mean)
            pctl = _latest_percentile(sv, atr_val)
            z    = (atr_val - mean) / std if std > 0 else 0.0
            if not math.isfinite(z):
                z = 0.0
            return round(pctl, 4), round(z, 6)

        short_p, _        = _win_stats(BASELINE_WINDOWS["short"])
        medium_p, medium_z = _win_stats(BASELINE_WINDOWS["medium"])
        long_p,  _        = _win_stats(BASELINE_WINDOWS["long"])

        if   medium_p >= 90: status = "extreme_high"
        elif medium_p >= 75: status = "high"
        elif medium_p <= 10: status = "extreme_low"
        elif medium_p <= 25: status = "low"
        else:                status = "normal"

        return {
            "atr_period":            ATR_PERIOD,
            "current_tr":            round(cur_tr, 8),
            "atr":                   round(atr_val, 8),
            "atr_sample_count":      atr_n,
            "atr_percentile_short":  short_p,
            "atr_percentile_medium": medium_p,
            "atr_percentile_long":   long_p,
            "atr_z_score_medium":    medium_z,
            "atr_status":            status,
        }

    # ── VWAP output ───────────────────────────────────────────────────────────
    def _vwap_output(self, rec: dict) -> dict:
        h = rec["high_price"]
        l = rec["low_price"]
        c = rec["close_price"]
        v = rec["total_volume"]

        if h is not None and l is not None and c is not None:
            tp            = (h + l + c) / 3.0
            self._vwap_num += tp * v
            self._vwap_den += v

        if self._vwap_den == 0.0:
            svwap, pvc, dist, dist_pct = None, "unknown", None, None
        else:
            svwap = self._vwap_num / self._vwap_den
            if c is None:
                pvc, dist, dist_pct = "unknown", None, None
            elif c > svwap:
                pvc  = "above"
                dist = c - svwap
                dist_pct = dist / svwap * 100
            elif c < svwap:
                pvc  = "below"
                dist = c - svwap
                dist_pct = dist / svwap * 100
            else:
                pvc, dist, dist_pct = "at", 0.0, 0.0

        return {
            "session_start_ts":     self._vwap_session_start,
            "session_vwap":         round(svwap, 8) if svwap is not None else None,
            "price_vs_vwap":        pvc,
            "distance_to_vwap":     round(dist, 8)     if dist     is not None else None,
            "distance_to_vwap_pct": round(dist_pct, 6) if dist_pct is not None else None,
        }

    # ── CVD output ────────────────────────────────────────────────────────────
    def _cvd_output(self, rec: dict) -> dict:
        delta          = rec["delta"]
        self._prev_cvd = self._cvd
        self._cvd      = self._prev_cvd + delta
        change         = self._cvd - self._prev_cvd  # == delta

        if   change > 0: direction = "rising"
        elif change < 0: direction = "falling"
        else:            direction = "flat"

        return {
            "session_start_ts": self._cvd_session_start,
            "cvd":              round(self._cvd, 8),
            "cvd_change":       round(change, 8),
            "cvd_direction":    direction,
        }

    # ── Metrics output ────────────────────────────────────────────────────────
    def _metrics_output(self, rec: dict) -> dict:
        """Compute all 13 metrics across short/medium/long windows."""
        recs = list(self.records)  # includes the current record (already appended)
        h    = rec["high_price"]
        l    = rec["low_price"]

        # Latest values for the current record
        latest_range = (h - l) if h is not None and l is not None else 0.0
        latest: dict[str, float] = {
            "range":                        latest_range,
            "total_volume":                 rec["total_volume"],
            "buy_volume":                   rec["buy_volume"],
            "sell_volume":                  rec["sell_volume"],
            "delta":                        rec["delta"],
            "absolute_delta":               abs(rec["delta"]),
            "trade_count":                  float(rec["trade_count"]),
            "footprint_price_level_count":  float(rec["footprint_price_level_count"]),
            "bid_update_count":             float(rec["bid_update_count"]),
            "ask_update_count":             float(rec["ask_update_count"]),
            "depth_balance":                float(rec["depth_balance"]),
            "depth_imbalance":              rec["depth_imbalance"],
            "close_price":                  rec["close_price"] if rec["close_price"] is not None else 0.0,
        }

        def _series(field: str) -> list[float]:
            if field == "range":
                return [
                    (r["high_price"] - r["low_price"])
                    if r["high_price"] is not None and r["low_price"] is not None
                    else 0.0
                    for r in recs
                ]
            if field == "absolute_delta":
                return [abs(r["delta"]) for r in recs]
            if field == "close_price":
                return [r["close_price"] for r in recs if r["close_price"] is not None]
            return [float(r.get(field, 0)) for r in recs]

        result: dict = {}
        for m in METRIC_NAMES:
            series = _series(m)
            lv     = latest[m]
            win_stats: dict = {}
            for win_name, win_size in BASELINE_WINDOWS.items():
                window = series[-win_size:] if len(series) > win_size else series
                win_stats[win_name] = _metric_stats(window, lv)
            result[m] = win_stats
        return result

    # ── Main ingestion ────────────────────────────────────────────────────────
    def ingest(self, raw: dict) -> Optional[dict]:
        """Process one raw source record. Returns baseline output or None on failure."""
        rec = normalize_record(raw, self.timeframe)
        ts  = rec["window_start_ts"]

        if self.first_ts is None:
            self.first_ts = ts
        self.last_ts = ts

        self._maybe_reset_session(ts)

        # Compute TR before appending (uses prev_close from LAST record)
        tr = self._true_range(rec)
        if tr is not None:
            self.tr_values.append(tr)

        # Append record (metrics will include it as "latest")
        self.records.append(rec)

        atr_out  = self._atr_output(tr)
        vwap_out = self._vwap_output(rec)   # updates _vwap_num/_vwap_den
        cvd_out  = self._cvd_output(rec)    # updates _cvd/_prev_cvd
        metrics  = self._metrics_output(rec)

        # Update prev_close after TR computation
        if rec["close_price"] is not None:
            self.prev_close = rec["close_price"]

        output: dict = {
            "symbol":           SYMBOL,
            "layer":            "Layer-3",
            "engine":           "HistoricalBaselineEngine",
            "timeframe":        self.timeframe,
            "source_file":      self.source_file,
            "generated_at_ts":  int(time.time() * 1000),
            "record_window": {
                "first_ts":     self.first_ts,
                "last_ts":      self.last_ts,
                "record_count": len(self.records),
            },
            "baseline_windows": BASELINE_WINDOWS,
            "metrics":          metrics,
            "atr":              atr_out,
            "vwap":             vwap_out,
            "cvd":              cvd_out,
        }

        errors = _validate(output)
        if errors:
            print(f"[BASELINE VALIDATION FAIL] {self.timeframe} ts={ts}:")
            for e in errors:
                print(f"  {e}")
            return None

        return output


# ── Validation ────────────────────────────────────────────────────────────────
def _validate(obj: dict) -> list[str]:
    errors: list[str] = []
    tf = obj.get("timeframe", "")

    # [1] timeframe non-empty
    if not tf:
        errors.append("[1] timeframe is empty")

    # [2] record_count > 0
    rc = obj.get("record_window", {}).get("record_count", 0)
    if rc <= 0:
        errors.append(f"[2] record_count={rc} must be > 0")

    # [3] all metrics present
    metrics = obj.get("metrics", {})
    for m in METRIC_NAMES:
        if m not in metrics:
            errors.append(f"[3] metrics.{m} missing")

    # [4] atr required fields
    atr = obj.get("atr", {})
    for f in ("atr_period", "current_tr", "atr", "atr_status"):
        if f not in atr:
            errors.append(f"[4] atr.{f} missing")

    # [5] vwap required fields
    vwap = obj.get("vwap", {})
    for f in ("session_vwap", "price_vs_vwap"):
        if f not in vwap:
            errors.append(f"[5] vwap.{f} missing")

    # [6] cvd required fields
    cvd = obj.get("cvd", {})
    for f in ("cvd", "cvd_direction"):
        if f not in cvd:
            errors.append(f"[6] cvd.{f} missing")

    # [7] latest_percentile in [0, 100]
    for m_name, m_data in metrics.items():
        for win_name, win_data in m_data.items():
            lp = win_data.get("latest_percentile", 50.0)
            if not (0.0 <= lp <= 100.0):
                errors.append(
                    f"[7] metrics.{m_name}.{win_name}.latest_percentile={lp} not in [0,100]"
                )

    # [8] std >= 0
    for m_name, m_data in metrics.items():
        for win_name, win_data in m_data.items():
            std = win_data.get("std", 0.0)
            if std < 0:
                errors.append(f"[8] metrics.{m_name}.{win_name}.std={std} < 0")

    # [9] z_score is finite
    for m_name, m_data in metrics.items():
        for win_name, win_data in m_data.items():
            z = win_data.get("z_score", 0.0)
            if not math.isfinite(z):
                errors.append(f"[9] metrics.{m_name}.{win_name}.z_score={z} is NaN/inf")

    return errors


# ── File I/O ──────────────────────────────────────────────────────────────────
def _append_output(obj: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _print_summary(obj: dict) -> None:
    atr_out  = obj["atr"]
    vwap_out = obj["vwap"]
    cvd_out  = obj["cvd"]
    vol_mid  = obj["metrics"].get("total_volume", {}).get("medium", {})
    dlt_mid  = obj["metrics"].get("delta",        {}).get("medium", {})
    print(
        f"[BASELINE {obj['timeframe']}] "
        f"records={obj['record_window']['record_count']} "
        f"atr={atr_out['atr']:.4f} atr_status={atr_out['atr_status']} "
        f"vwap={vwap_out['session_vwap']} "
        f"cvd={cvd_out['cvd']:.2f} "
        f"volume_pctl={vol_mid.get('latest_percentile', 0.0):.1f} "
        f"delta_z={dlt_mid.get('z_score', 0.0):.3f}"
    )


# ── Shared: read all lines from a file ────────────────────────────────────────
def _read_all_lines(path: str) -> tuple[list[dict], int]:
    """Return (records, byte_position_after_last_line)."""
    if not os.path.exists(path):
        return [], 0
    records: list[dict] = []
    pos = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped:
                try:
                    records.append(json.loads(stripped))
                except json.JSONDecodeError:
                    pass
        pos = fh.tell()
    return records, pos


# ── Batch mode ────────────────────────────────────────────────────────────────
def run_batch() -> None:
    if _check_system_halt():
        return
    print("NurtacCoreEngineClaude — Layer-3 Baseline Engine (batch)")
    print(f"Output: {OUTPUT_FILE}")
    print()

    for tf, path in SOURCE_FILES.items():
        records, _ = _read_all_lines(path)
        if not records:
            print(f"[{tf}] No records in {path} — skipped")
            continue

        state       = TimeframeState(tf, path)
        last_output = None

        for raw in records:
            out = state.ingest(raw)
            if out is not None:
                last_output = out

        if last_output is not None:
            _append_output(last_output)
            if FULL_PRINT:
                print(json.dumps(last_output, indent=2, ensure_ascii=False))
            else:
                _print_summary(last_output)
        else:
            print(f"[{tf}] All records failed validation — nothing written")


# ── Live mode ─────────────────────────────────────────────────────────────────
async def _tail_follow(
    tf: str,
    path: str,
    state: TimeframeState,
    start_pos: int,
    write_lock: asyncio.Lock,
) -> None:
    """Tail-follow one JSONL file starting from start_pos (after warm-up)."""
    while not os.path.exists(path):
        if _check_system_halt():
            return
        print(f"[{tf}] Waiting for {path}...")
        await asyncio.sleep(FILE_WAIT_SLEEP)

    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(0, 2)  # Always start from real EOF
        while True:
            if os.path.exists(HALT_FILE):
                print(f"[{tf}] SYSTEM_HALT detected, stopping task.")
                return

            line = fh.readline()
            if line:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue

                out = state.ingest(raw)
                if out is not None:
                    async with write_lock:
                        _append_output(out)
                    if FULL_PRINT:
                        print(json.dumps(out, indent=2, ensure_ascii=False))
                    else:
                        _print_summary(out)
            else:
                await asyncio.sleep(POLL_INTERVAL)


async def run_live() -> None:
    """Live mode: warm-up then tail-follow all source files."""
    print("NurtacCoreEngineClaude — Layer-3 Baseline Engine (live)")
    print(f"Output: {OUTPUT_FILE}")

    # Wait for at least one source file to exist
    print("Waiting for source files to appear...")
    while True:
        if os.path.exists(HALT_FILE):
            print("[HBE] SYSTEM_HALT — exiting")
            return

        # Check if any source file exists
        any_exists = any(os.path.exists(path) for path in SOURCE_FILES.values())
        if any_exists:
            break

        await asyncio.sleep(1.0)

    if _check_system_halt():
        return
    print("Warming up — reading existing records...")
    print()

    states: dict[str, TimeframeState] = {}
    positions: dict[str, int]         = {}

    for tf, path in SOURCE_FILES.items():
        state         = TimeframeState(tf, path)
        records, pos  = _read_all_lines(path)
        for raw in records:
            state.ingest(raw)
        if records:
            print(f"[{tf}] Warm-up: {len(records)} records ingested, file pos={pos}")
        states[tf]    = state
        positions[tf] = pos

    print("\nWarm-up complete. Starting live tail-follow tasks...")
    print()

    write_lock = asyncio.Lock()
    tasks = [
        asyncio.create_task(
            _tail_follow(tf, path, states[tf], positions[tf], write_lock),
            name=f"baseline-{tf}",
        )
        for tf, path in SOURCE_FILES.items()
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer-3 Historical Baseline + Context Metrics Engine"
    )
    parser.add_argument(
        "--mode", choices=["batch", "live"], required=True,
        help="batch: one pass over all files; live: warm-up then tail-follow",
    )
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch()
    else:
        try:
            asyncio.run(run_live())
        except KeyboardInterrupt:
            print("\n[BASELINE] Stopping.")


if __name__ == "__main__":
    main()
