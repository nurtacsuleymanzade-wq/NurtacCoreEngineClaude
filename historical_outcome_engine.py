"""
NurtacCoreEngineClaude — Layer-11: Historical Outcome Engine

Reads all signal sources, opens forward-horizon observations per event,
measures outcomes as prices arrive, writes calibration profiles.

NO scoring, NO confidence, NO thresholds, NO predictions.
Only measures and records what happened.

Outputs:
  data/historical_outcome_observations.jsonl
  data/historical_outcome_open_positions.json
  data/calibration_profiles.json
  data/historical_outcome_health.json
  data/historical_outcome_errors.jsonl
"""

import argparse
import asyncio
import bisect
import hashlib
import json
import os
import statistics
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
DATA_DIR   = Path("data")
HALT_FILE  = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.5

HORIZONS_MS: list[int] = [30_000, 60_000, 180_000, 300_000, 900_000, 3_600_000]
HORIZON_LABELS: dict[int, str] = {
    30_000:    "30s",
    60_000:    "60s",
    180_000:   "180s",
    300_000:   "300s",
    900_000:   "900s",
    3_600_000: "3600s",
}
MAX_PRICE_INDEX_AGE_MS = 14_400_000  # 4 hours
MAX_OPEN_OBS           = 500
COMPOSITE_WINDOW_MS    = 2_000
CALIBRATION_INTERVAL_S = 60.0
HEALTH_INTERVAL_S      = 30.0
PERSIST_INTERVAL_S     = 30.0
MIN_SAMPLE_LABEL       = 30          # reporting label threshold (not a threshold for trade)
MAX_COMPLETED_IN_RAM   = 10_000     # rolling window for calibration

# ── Output files ──────────────────────────────────────────────────────────────
OBS_FILE        = DATA_DIR / "historical_outcome_observations.jsonl"
POSITIONS_FILE  = DATA_DIR / "historical_outcome_open_positions.json"
PROFILES_FILE   = DATA_DIR / "calibration_profiles.json"
HEALTH_FILE     = DATA_DIR / "historical_outcome_health.json"
ERRORS_FILE     = DATA_DIR / "historical_outcome_errors.jsonl"

# ── Input files ───────────────────────────────────────────────────────────────
PRIMARY_FILE   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
EVIDENCE_FILE  = DATA_DIR / "evidence_stream.jsonl"
BASELINE_FILE  = DATA_DIR / "historical_baseline_dna.jsonl"

STRUCT_FILES = {
    "structure_1s": DATA_DIR / "structure_1s.jsonl",
    "structure_1m": DATA_DIR / "structure_1m.jsonl",
    "structure_5m": DATA_DIR / "structure_5m.jsonl",
}
SCENARIO_FILE  = DATA_DIR / "scenarios.jsonl"
VP1M_FILE      = DATA_DIR / "volume_profile_1m.jsonl"
BIAS_FILE      = DATA_DIR / "bias_context.jsonl"

DETECTOR_KEYS = [
    "absorption", "sweep", "exhaustion",
    "initiative_flow", "trapped_trader", "iceberg",
]
DETECTOR_FILES: dict[str, Path] = {
    k: DATA_DIR / f"labels_{k}.jsonl" for k in DETECTOR_KEYS
}

OBS_QUAL_FILE = DATA_DIR / "qualified_setups.jsonl"
OBS_RAW_FILE  = DATA_DIR / "observations.jsonl"

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_all_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
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

def _safe_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)
    except OSError:
        pass

def _append_jsonl(path: Path, rec: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        pass

def _append_fh(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

def _log_error(msg: str) -> None:
    print(f"[HOE ERROR] {msg}", flush=True)
    _append_jsonl(ERRORS_FILE, {"ts": int(time.time() * 1000), "error": msg})

# ── Price index ───────────────────────────────────────────────────────────────
class PriceIndex:
    """Sorted timestamp → price index with O(log n) lookups."""

    def __init__(self):
        self._ts:  list[int]   = []
        self._px:  list[float] = []

    def add(self, ts: int, price: float) -> None:
        idx = bisect.bisect_left(self._ts, ts)
        if idx < len(self._ts) and self._ts[idx] == ts:
            self._px[idx] = price
        else:
            self._ts.insert(idx, ts)
            self._px.insert(idx, price)

    def at_or_before(self, ts: int) -> tuple[int, float] | None:
        idx = bisect.bisect_right(self._ts, ts) - 1
        if idx < 0:
            return None
        return self._ts[idx], self._px[idx]

    def at_or_after(self, ts: int) -> tuple[int, float] | None:
        idx = bisect.bisect_left(self._ts, ts)
        if idx >= len(self._ts):
            return None
        return self._ts[idx], self._px[idx]

    def range_prices(self, start_ts: int, end_ts: int) -> list[float]:
        lo = bisect.bisect_left(self._ts, start_ts)
        hi = bisect.bisect_right(self._ts, end_ts)
        return self._px[lo:hi]

    def trim_before(self, cutoff_ts: int) -> None:
        idx = bisect.bisect_left(self._ts, cutoff_ts)
        if idx > 0:
            self._ts = self._ts[idx:]
            self._px = self._px[idx:]

    def latest_ts(self) -> int | None:
        return self._ts[-1] if self._ts else None

    def count(self) -> int:
        return len(self._ts)


# ── Direction/side helpers ────────────────────────────────────────────────────
_SIDE_MAP: dict[str, str] = {
    "sell_absorbed":   "sell",
    "downward_sweep":  "sell",
    "sell_exhaustion": "sell",
    "ask_iceberg":     "sell",
    "long_trapped":    "sell",
    "sell_initiative": "sell",
    "buy_absorbed":    "buy",
    "upward_sweep":    "buy",
    "buy_exhaustion":  "buy",
    "bid_iceberg":     "buy",
    "short_trapped":   "buy",
    "buy_initiative":  "buy",
}

_UP_KW   = ["bullish", "buy", "long", "upward", "sell_absorbed",
             "sell_exhaustion", "short_trapped", "bid_iceberg", "buy_initiative"]
_DOWN_KW = ["bearish", "sell", "short", "downward", "buy_absorbed",
             "buy_exhaustion", "long_trapped", "ask_iceberg", "sell_initiative"]

def direction_to_side(d: str | None) -> str:
    if not d:
        return "neutral"
    return _SIDE_MAP.get(d, "neutral")

def label_to_direction(d: str | None) -> str:
    if not d:
        return "unknown"
    s = d.lower()
    for kw in _UP_KW:
        if kw in s:
            return "up"
    for kw in _DOWN_KW:
        if kw in s:
            return "down"
    return "unknown"

def dominant_side_to_side(ds: str | None) -> str:
    if ds == "long":   return "buy"
    if ds == "short":  return "sell"
    return "neutral"

def dominant_to_direction(ds: str | None) -> str:
    if ds == "long":  return "up"
    if ds == "short": return "down"
    return "unknown"


# ── Event ID & Pattern Signature ──────────────────────────────────────────────
def make_event_id(source: str, symbol: str, timeframe: str,
                  window_start_ts: int, event_type: str, side: str) -> str:
    raw = f"{source}|{symbol}|{timeframe}|{window_start_ts}|{event_type}|{side}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def make_pattern_signature(symbol: str, timeframe: str,
                            components: list[str]) -> str:
    raw = symbol + "|" + timeframe + "|" + "|".join(sorted(components))
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def make_pattern_key(symbol: str, timeframe: str, source: str,
                     event_type: str, side: str) -> str:
    return f"{symbol}|{timeframe}|{source}:{event_type}:{side}"


# ── Event normalization ───────────────────────────────────────────────────────
def _base_event(source: str, event_id: str, timeframe: str,
                window_start_ts: int, window_end_ts: int | None,
                event_type: str, side: str, direction: str,
                raw: dict) -> dict:
    components = [source, event_type, side, direction]
    sig = make_pattern_signature(SYMBOL, timeframe, components)
    key = make_pattern_key(SYMBOL, timeframe, source, event_type, side)
    return {
        "source":              source,
        "event_id":            event_id,
        "symbol":              SYMBOL,
        "timeframe":           timeframe,
        "window_start_ts":     window_start_ts,
        "window_end_ts":       window_end_ts,
        "event_type":          event_type,
        "side":                side,
        "direction":           direction,
        "pattern_signature":   sig,
        "pattern_key":         key,
        "pattern_components":  components,
        "calibration_status":  "observed_not_scored",
        "source_refs":         {},
        "data_quality":        {},
        "raw":                 raw,
    }

def normalize_detector(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    lbl = row.get("label", "none")
    if lbl == "none" or not lbl:
        return None
    det = row.get("detector", "unknown")
    drn = row.get("direction")
    side = direction_to_side(drn)
    dirn = label_to_direction(drn)
    ts   = row.get("window_start_ts")
    wte  = row.get("window_end_ts")
    if ts is None:
        return None
    event_id = make_event_id("detector", SYMBOL, "1S", int(ts), det, side)
    ev = _base_event("detector", event_id, "1S", int(ts), wte,
                     det, side, dirn, row)
    ev["label"] = lbl
    ev["score"] = _sf(row.get("score"), 0.0)
    return ev

def normalize_evidence(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    dom = row.get("dominant_side", "neutral")
    if dom == "neutral" or not dom:
        return None
    ts  = row.get("window_start_ts")
    wte = row.get("window_end_ts")
    if ts is None:
        return None
    side = dominant_side_to_side(dom)
    dirn = dominant_to_direction(dom)
    event_id = make_event_id("evidence", SYMBOL, "1S", int(ts),
                             "evidence_packet", side)
    ev = _base_event("evidence", event_id, "1S", int(ts), wte,
                     "evidence_packet", side, dirn, row)
    ev["long_score"]  = _sf(row.get("long_score"), 0.0)
    ev["short_score"] = _sf(row.get("short_score"), 0.0)
    ev["score_gap"]   = _sf(row.get("score_gap"), 0.0)
    return ev

def _struct_event_type(row: dict) -> str:
    bos   = row.get("bos") or {}
    trend = row.get("trend") or {}
    mbos  = bos.get("micro_bos")
    mabos = bos.get("macro_bos")
    choch = trend.get("choch_confirmed")
    msb   = trend.get("msb")
    if mbos  == "bullish": return "micro_bos_bullish"
    if mbos  == "bearish": return "micro_bos_bearish"
    if mabos == "bullish": return "macro_bos_bullish"
    if mabos == "bearish": return "macro_bos_bearish"
    if choch == "bullish": return "choch_bullish"
    if choch == "bearish": return "choch_bearish"
    if msb   == "bullish": return "msb_bullish"
    if msb   == "bearish": return "msb_bearish"
    return "structure_update"

def normalize_structure(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    etype = _struct_event_type(row)
    if etype == "structure_update":
        return None
    tf  = row.get("timeframe", "1S")
    ts  = row.get("window_start_ts")
    wte = row.get("window_end_ts")
    if ts is None:
        return None
    side = "buy" if "bullish" in etype else "sell"
    dirn = "up"  if "bullish" in etype else "down"
    event_id = make_event_id("smart_money", SYMBOL, tf, int(ts), etype, side)
    return _base_event("smart_money", event_id, tf, int(ts), wte,
                       etype, side, dirn, row)

def normalize_scenario(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    dom = row.get("dominant_scenario")
    if not dom:
        return None
    ts  = row.get("window_start_ts")
    wte = row.get("window_end_ts")
    if ts is None:
        return None
    dom_dir = row.get("dominant_direction", "neutral")
    side    = dominant_side_to_side(
                  "long" if dom_dir == "bullish" else
                  "short" if dom_dir == "bearish" else "neutral")
    dirn    = dominant_to_direction(
                  "long" if dom_dir == "bullish" else
                  "short" if dom_dir == "bearish" else "neutral")
    act = row.get("active_scenarios", [])
    status = act[0].get("status") if act else "none"
    event_id = make_event_id("scenario", SYMBOL, "1S", int(ts), dom, side)
    ev = _base_event("scenario", event_id, "1S", int(ts), wte,
                     dom, side, dirn, row)
    ev["status"] = status
    return ev

def normalize_observer(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    qt = row.get("qualification_ts")
    if qt is None:
        return None
    d    = row.get("direction", "long")
    side = "buy" if d == "long" else "sell"
    dirn = "up"  if d == "long" else "down"
    event_id = make_event_id("observer", SYMBOL, "1S", int(qt),
                             "qualified_setup", side)
    ev = _base_event("observer", event_id, "1S", int(qt), None,
                     "qualified_setup", side, dirn, row)
    ev["watch_side"] = d
    return ev


# ── Outcome helpers ───────────────────────────────────────────────────────────
def _compute_outcome(ref_price: float, future_price: float,
                     future_ts: int, target_ts: int,
                     side: str, path_prices: list[float]) -> dict:
    raw_return = (future_price - ref_price) / ref_price if ref_price != 0 else 0.0

    if side == "buy":
        sar = raw_return
    elif side == "sell":
        sar = -raw_return
    else:
        sar = None

    if sar is None:
        dr = "unknown"
    elif sar > 0:
        dr = "favorable"
    elif sar < 0:
        dr = "unfavorable"
    else:
        dr = "flat"

    max_fav   = None
    max_adv   = None
    if path_prices and side in ("buy", "sell"):
        if side == "buy":
            returns = [(p - ref_price) / ref_price for p in path_prices if ref_price != 0]
        else:
            returns = [(ref_price - p) / ref_price for p in path_prices if ref_price != 0]
        if returns:
            max_fav = max(returns)
            max_adv = min(returns)

    return {
        "future_price":                    round(future_price, 6),
        "future_price_ts":                 future_ts,
        "future_price_delay_ms":           future_ts - target_ts,
        "raw_return":                      round(raw_return, 8),
        "side_adjusted_return":            round(sar, 8) if sar is not None else None,
        "directional_result":              dr,
        "max_favorable_return_until_horizon": round(max_fav, 8) if max_fav is not None else None,
        "max_adverse_return_until_horizon":   round(max_adv, 8) if max_adv is not None else None,
    }


def _validate_observation(rec: dict) -> list[str]:
    errors: list[str] = []
    ref = rec.get("reference") or {}
    rp  = _sf(ref.get("price"), 0.0)
    rpt = ref.get("price_ts", 0)
    ewt = rec.get("event_window_start_ts", 0)
    val = rec.get("validation") or {}

    if rp <= 0:
        errors.append("reference_price <= 0")
    if rpt > ewt:
        errors.append(f"future_leakage: ref_ts={rpt} > event_ts={ewt}")
    if val.get("future_leakage_detected", True):
        errors.append("future_leakage_detected=true")

    scores = rec.get("scores") or {}
    for k, v in scores.items():
        if v is not None:
            errors.append(f"score {k} must be null, got {v}")

    if rec.get("calibration_status") != "observed_not_scored":
        errors.append("calibration_status != observed_not_scored")

    for h_label, outcome in (rec.get("outcomes") or {}).items():
        if outcome is None:
            continue
        fpts = outcome.get("future_price_ts", 0)
        h_ms = next((k for k, v in HORIZON_LABELS.items() if v == h_label), None)
        if h_ms is not None:
            target = ewt + h_ms
            if fpts < target:
                errors.append(f"{h_label}: future_price_ts {fpts} < target {target}")
        rr = outcome.get("raw_return")
        if rr is not None:
            if rr != rr or abs(rr) == float("inf"):
                errors.append(f"{h_label}: raw_return is NaN/inf")
        dr = outcome.get("directional_result")
        if dr not in ("favorable", "unfavorable", "flat", "unknown"):
            errors.append(f"{h_label}: invalid directional_result {dr}")

    return errors


# ── Engine state ──────────────────────────────────────────────────────────────
class EngineState:
    def __init__(self):
        self.price_index    = PriceIndex()
        self.open_obs:      dict[str, dict] = {}  # obs_id → obs
        self.processed_ids: set[str]        = set()
        self.completed_count: int = 0
        self.completed_for_profiling: list[dict] = []  # rolling window
        self.event_counts:  dict[str, int]  = defaultdict(int)
        self.last_price_ts: int   = 0
        self.last_event_ts: int   = 0
        self.missing_inputs: list[str] = []
        self.warnings:       list[str] = []
        self.errors_list:    list[str] = []
        # Composite buffer: ts_bucket → list of events (for composite patterns)
        self.composite_buf: dict[int, list[dict]] = defaultdict(list)

    def composite_bucket(self, ts: int) -> int:
        """Round ts to COMPOSITE_WINDOW_MS bucket."""
        return (ts // COMPOSITE_WINDOW_MS) * COMPOSITE_WINDOW_MS


# ── Open an observation ───────────────────────────────────────────────────────
def open_observation(state: EngineState, event: dict, obs_fh=None) -> None:
    eid = event["event_id"]
    if eid in state.processed_ids:
        return

    ts = event["window_start_ts"]
    ref = state.price_index.at_or_before(ts)
    if ref is None:
        state.warnings.append(f"no_ref_price_for_event_at_{ts}")
        return

    ref_ts, ref_price = ref
    if ref_price <= 0:
        state.warnings.append(f"ref_price<=0 at ts={ts}")
        return

    # Check future leakage
    if ref_ts > ts:
        _log_error(f"FUTURE LEAKAGE DETECTED: ref_ts={ref_ts} > event_ts={ts}")
        return

    state.processed_ids.add(eid)
    state.event_counts[event["source"]] += 1
    state.last_event_ts = max(state.last_event_ts, ts)

    # Enforce max open observations
    if len(state.open_obs) >= MAX_OPEN_OBS:
        oldest_id = min(state.open_obs,
                        key=lambda k: state.open_obs[k]["event_window_start_ts"])
        _close_observation(state, state.open_obs[oldest_id], obs_fh,
                           all_measured=False, reason="max_obs_exceeded")
        state.open_obs.pop(oldest_id, None)

    obs_id = str(uuid.uuid4())
    obs = {
        "observation_id":       obs_id,
        "event_id":             eid,
        "pattern_signature":    event["pattern_signature"],
        "pattern_key":          event["pattern_key"],
        "pattern_components":   event["pattern_components"],
        "source":               event["source"],
        "symbol":               SYMBOL,
        "timeframe":            event["timeframe"],
        "event_type":           event["event_type"],
        "side":                 event["side"],
        "direction":            event["direction"],
        "event_window_start_ts": ts,
        "event_window_end_ts":  event.get("window_end_ts"),
        "reference_price":      ref_price,
        "reference_price_ts":   ref_ts,
        "horizons_pending":     list(HORIZONS_MS),
        "outcomes":             {},
        "price_path":           [],    # list of [ts, price]
        "source_event":         {k: v for k, v in event.items()
                                 if k not in ("raw", "source_event")},
        "data_quality":         {},
    }
    state.open_obs[obs_id] = obs

    # Register in composite buffer
    bucket = state.composite_bucket(ts)
    state.composite_buf[bucket].append({
        "obs_id":     obs_id,
        "event_type": event["event_type"],
        "source":     event["source"],
        "side":       event["side"],
    })


# ── Update price paths and check horizons ─────────────────────────────────────
def update_and_check(state: EngineState, ts: int, price: float,
                     obs_fh) -> None:
    """Called on each new price. Updates paths and resolves completed horizons."""
    state.price_index.add(ts, price)
    state.last_price_ts = max(state.last_price_ts, ts)

    # Trim old price index entries
    cutoff = ts - MAX_PRICE_INDEX_AGE_MS
    if cutoff > 0:
        state.price_index.trim_before(cutoff)

    # Update open observations
    expired_ids: list[str] = []
    for obs_id, obs in list(state.open_obs.items()):
        ref_ts   = obs["reference_price_ts"]
        ref_price = obs["reference_price"]
        ewt       = obs["event_window_start_ts"]
        side      = obs["side"]

        # Add to price path (ts, price) if within max horizon
        if ts <= ewt + HORIZONS_MS[-1]:
            obs["price_path"].append([ts, price])

        # Check horizons
        done_horizons: list[int] = []
        for h_ms in list(obs["horizons_pending"]):
            target_ts = ewt + h_ms
            if ts >= target_ts:
                future = state.price_index.at_or_after(target_ts)
                if future is not None:
                    f_ts, f_price = future
                    # path prices up to horizon
                    path_prices = [p for (t, p) in obs["price_path"] if t <= target_ts]
                    outcome = _compute_outcome(
                        ref_price, f_price, f_ts, target_ts, side, path_prices
                    )
                    h_label = HORIZON_LABELS[h_ms]
                    obs["outcomes"][h_label] = outcome
                    done_horizons.append(h_ms)

        for h_ms in done_horizons:
            obs["horizons_pending"].remove(h_ms)

        # Close if all horizons done
        if not obs["horizons_pending"]:
            _close_observation(state, obs, obs_fh, all_measured=True)
            expired_ids.append(obs_id)
            continue

        # Timeout: event_ts + 3600000ms passed and still pending
        if ts >= ewt + HORIZONS_MS[-1] + 1000:
            _close_observation(state, obs, obs_fh, all_measured=False,
                               reason="timeout")
            expired_ids.append(obs_id)

    for oid in expired_ids:
        state.open_obs.pop(oid, None)


# ── Close an observation ──────────────────────────────────────────────────────
def _close_observation(state: EngineState, obs: dict, obs_fh,
                       all_measured: bool, reason: str = "") -> None:
    ref_price = obs["reference_price"]
    ref_ts    = obs["reference_price_ts"]
    ewt       = obs["event_window_start_ts"]

    # Fill missing horizons with null
    for h_ms in obs["horizons_pending"]:
        h_label = HORIZON_LABELS[h_ms]
        obs["outcomes"].setdefault(h_label, None)

    # Validate
    completed_rec = {
        "layer":              "Layer-11",
        "engine":             "HistoricalOutcomeEngine",
        "record_type":        "historical_outcome_observation",
        "calibration_status": "observed_not_scored",
        "observation_id":     obs["observation_id"],
        "event_id":           obs["event_id"],
        "pattern_signature":  obs["pattern_signature"],
        "pattern_key":        obs["pattern_key"],
        "pattern_components": obs["pattern_components"],
        "source":             obs["source"],
        "symbol":             SYMBOL,
        "timeframe":          obs["timeframe"],
        "event_type":         obs["event_type"],
        "side":               obs["side"],
        "direction":          obs["direction"],
        "event_window_start_ts": ewt,
        "event_window_end_ts":   obs.get("event_window_end_ts"),
        "reference": {
            "price":    round(ref_price, 6),
            "price_ts": ref_ts,
        },
        "outcomes":     obs["outcomes"],
        "source_event": obs.get("source_event", {}),
        "data_quality": obs.get("data_quality", {}),
        "scores": {
            "confidence":       None,
            "strength_score":   None,
            "edge_score":       None,
            "probability_score": None,
            "threshold":        None,
        },
        "validation": {
            "reference_price_valid":  ref_price > 0,
            "all_horizons_measured":  all_measured,
            "future_leakage_detected": ref_ts > ewt,
            "errors":                 [],
        },
    }

    errs = _validate_observation(completed_rec)
    if errs:
        completed_rec["validation"]["errors"] = errs
        for e in errs:
            _log_error(f"obs={obs['observation_id']} {e}")

    # Write to file
    if obs_fh is not None:
        _append_fh(obs_fh, completed_rec)
    else:
        _append_jsonl(OBS_FILE, completed_rec)

    state.completed_count += 1

    # Keep in RAM for calibration (rolling window)
    state.completed_for_profiling.append(completed_rec)
    if len(state.completed_for_profiling) > MAX_COMPLETED_IN_RAM:
        state.completed_for_profiling.pop(0)

    _print_completed(completed_rec)


def _print_completed(rec: dict) -> None:
    if FULL_PRINT:
        print(json.dumps(rec, ensure_ascii=False), flush=True)
        return
    oid  = rec.get("observation_id", "?")[:8]
    src  = rec.get("source", "?")
    et   = rec.get("event_type", "?")
    side = rec.get("side", "?")
    outs = rec.get("outcomes") or {}

    def _dr(label: str) -> str:
        o = outs.get(label)
        return o.get("directional_result", "?")[:3] if o else "N/A"

    print(
        f"[HOE COMPLETED] obs_id={oid} source={src} event={et}\n"
        f"  side={side} 30s={_dr('30s')} 60s={_dr('60s')} "
        f"300s={_dr('300s')}",
        flush=True,
    )


# ── Calibration profiles ──────────────────────────────────────────────────────
def compute_calibration_profiles(state: EngineState) -> None:
    completed = state.completed_for_profiling
    if not completed:
        _safe_write_json(PROFILES_FILE, {
            "layer": "Layer-11", "engine": "HistoricalOutcomeEngine",
            "record_type": "calibration_profile_summary",
            "calibration_status": "observed_not_scored",
            "generated_at": time.time(),
            "total_observations": 0, "total_groups": 0, "groups": [],
            "scores": _null_scores(),
        })
        return

    # Group by key
    groups: dict[str, dict] = {}
    for rec in completed:
        key = (rec.get("symbol",""), rec.get("timeframe",""),
               rec.get("source",""), rec.get("event_type",""),
               rec.get("side",""), rec.get("direction",""),
               rec.get("pattern_signature",""))
        k = "|".join(key)
        if k not in groups:
            groups[k] = {
                "symbol":            rec.get("symbol",""),
                "timeframe":         rec.get("timeframe",""),
                "source":            rec.get("source",""),
                "event_type":        rec.get("event_type",""),
                "side":              rec.get("side",""),
                "direction":         rec.get("direction",""),
                "pattern_signature": rec.get("pattern_signature",""),
                "pattern_key":       rec.get("pattern_key",""),
                "samples":           [],
            }
        groups[k]["samples"].append(rec)

    group_list: list[dict] = []
    for gdata in groups.values():
        samples = gdata.pop("samples")
        n = len(samples)
        gdata["sample_count"] = n
        gdata["sample_status"] = ("insufficient_data"
                                  if n < MIN_SAMPLE_LABEL
                                  else "observed_sample")
        horizons_stats: dict[str, dict] = {}
        for h_label in HORIZON_LABELS.values():
            fav = unf = flat = unk = 0
            raw_rets: list[float] = []
            sar_rets: list[float] = []
            max_favs: list[float] = []
            max_advs: list[float] = []
            for s in samples:
                oc = (s.get("outcomes") or {}).get(h_label)
                if oc is None:
                    unk += 1
                    continue
                dr = oc.get("directional_result", "unknown")
                if dr == "favorable":   fav += 1
                elif dr == "unfavorable": unf += 1
                elif dr == "flat":      flat += 1
                else:                   unk  += 1
                rr = oc.get("raw_return")
                if rr is not None and rr == rr:
                    raw_rets.append(rr)
                sar = oc.get("side_adjusted_return")
                if sar is not None and sar == sar:
                    sar_rets.append(sar)
                mf = oc.get("max_favorable_return_until_horizon")
                if mf is not None and mf == mf:
                    max_favs.append(mf)
                ma = oc.get("max_adverse_return_until_horizon")
                if ma is not None and ma == ma:
                    max_advs.append(ma)

            def _avg(lst: list) -> float | None:
                return round(sum(lst)/len(lst), 8) if lst else None
            def _med(lst: list) -> float | None:
                return round(statistics.median(lst), 8) if lst else None

            horizons_stats[h_label] = {
                "favorable_count":   fav,
                "unfavorable_count": unf,
                "flat_count":        flat,
                "unknown_count":     unk,
                "avg_raw_return":              _avg(raw_rets),
                "median_raw_return":           _med(raw_rets),
                "avg_side_adjusted_return":    _avg(sar_rets),
                "median_side_adjusted_return": _med(sar_rets),
                "avg_max_favorable_return":    _avg(max_favs),
                "avg_max_adverse_return":      _avg(max_advs),
            }
        gdata["horizons"] = horizons_stats
        gdata["scores"]   = _null_scores()
        group_list.append(gdata)

    out = {
        "layer":              "Layer-11",
        "engine":             "HistoricalOutcomeEngine",
        "record_type":        "calibration_profile_summary",
        "calibration_status": "observed_not_scored",
        "generated_at":       time.time(),
        "total_observations": len(completed),
        "total_groups":       len(group_list),
        "groups":             group_list,
        "scores":             _null_scores(),
    }
    _safe_write_json(PROFILES_FILE, out)
    state.profiles_written = getattr(state, "profiles_written", 0) + 1


def _null_scores() -> dict:
    return {
        "confidence":       None,
        "strength_score":   None,
        "edge_score":       None,
        "probability_score": None,
        "threshold":        None,
    }


# ── Health ────────────────────────────────────────────────────────────────────
def write_health(state: EngineState) -> None:
    counts = {
        "detector_labels_absorption":     state.event_counts.get("detector_absorption", 0)
                                          + state.event_counts.get("detector", 0),
        "detector_labels_sweep":          state.event_counts.get("detector_sweep", 0),
        "detector_labels_exhaustion":     state.event_counts.get("detector_exhaustion", 0),
        "detector_labels_initiative_flow": state.event_counts.get("detector_initiative_flow", 0),
        "detector_labels_trapped_trader": state.event_counts.get("detector_trapped_trader", 0),
        "detector_labels_iceberg":        state.event_counts.get("detector_iceberg", 0),
        "evidence_stream":                state.event_counts.get("evidence", 0),
        "structure_1s":                   state.event_counts.get("smart_money_1S", 0),
        "structure_1m":                   state.event_counts.get("smart_money_1M", 0),
        "structure_5m":                   state.event_counts.get("smart_money_5M", 0),
        "scenarios":                      state.event_counts.get("scenario", 0),
        "observer_qualified_setups":      state.event_counts.get("observer", 0),
    }
    health = {
        "status":                 "alive",
        "prices_indexed":         state.price_index.count(),
        "input_events_processed": counts,
        "open_observations":      len(state.open_obs),
        "completed_observations": state.completed_count,
        "profiles_written":       getattr(state, "profiles_written", 0),
        "last_price_ts":          state.last_price_ts,
        "last_event_ts":          state.last_event_ts,
        "missing_inputs":         state.missing_inputs,
        "warnings":               state.warnings[-20:],  # last 20
        "errors":                 state.errors_list[-20:],
    }
    _safe_write_json(HEALTH_FILE, health)


# ── Persist / restore open positions ─────────────────────────────────────────
def persist_open_positions(state: EngineState) -> None:
    data = {
        "saved_at":    time.time(),
        "observation_count": len(state.open_obs),
        "observations": {},
    }
    for oid, obs in state.open_obs.items():
        # Don't include full price_path to save space
        o = {k: v for k, v in obs.items() if k != "price_path"}
        o["price_path_len"] = len(obs["price_path"])
        data["observations"][oid] = o
    _safe_write_json(POSITIONS_FILE, data)

def restore_open_positions(state: EngineState) -> int:
    if not POSITIONS_FILE.exists():
        return 0
    try:
        with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0
    restored = 0
    for oid, obs in (data.get("observations") or {}).items():
        obs["price_path"] = []  # price_path lost on restart
        state.open_obs[oid] = obs
        state.processed_ids.add(obs.get("event_id", ""))
        restored += 1
    return restored

def load_processed_ids(state: EngineState) -> int:
    """Re-read observations file to rebuild processed_ids set."""
    n = 0
    for rec in _read_all_jsonl(OBS_FILE):
        eid = rec.get("event_id")
        if eid:
            state.processed_ids.add(eid)
            n += 1
    return n


# ── Event source counters ─────────────────────────────────────────────────────
def _count_key(source: str, timeframe: str) -> str:
    if source == "smart_money":
        return f"{source}_{timeframe}"
    return source


# ── Batch mode ────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[HOE] Batch mode — loading all input files", flush=True)
    state = EngineState()

    # Check missing inputs
    all_input_files = (
        [PRIMARY_FILE, EVIDENCE_FILE] +
        list(DETECTOR_FILES.values()) +
        list(STRUCT_FILES.values()) +
        [SCENARIO_FILE]
    )
    for p in all_input_files:
        if not p.exists():
            state.missing_inputs.append(str(p))

    # Load all prices
    print("[HOE] Price index yükleniyor...", flush=True)
    primary_recs = _read_all_jsonl(PRIMARY_FILE)
    prices: list[tuple[int, float]] = []
    for row in primary_recs:
        ts  = row.get("window_start_ts")
        cdna = row.get("candle_dna") or {}
        co   = cdna.get("close")
        px   = _sf(co.get("price") if isinstance(co, dict) else co, 0.0)
        if ts is not None and px > 0:
            prices.append((int(ts), px))
    prices.sort(key=lambda x: x[0])
    print(f"[HOE] {len(prices)} fiyat satırı yüklendi", flush=True)

    # Restore open observations from previous run
    restored = restore_open_positions(state)
    n_prev   = load_processed_ids(state)
    print(f"[HOE] {n_prev} tamamlanmış event_id yüklendi, "
          f"{restored} açık observation restore edildi", flush=True)

    # Load all events from all sources
    all_events: list[dict] = []

    # Detector events
    for det_key, det_path in DETECTOR_FILES.items():
        for row in _read_all_jsonl(det_path):
            ev = normalize_detector(row)
            if ev:
                all_events.append(ev)

    # Evidence events
    for row in _read_all_jsonl(EVIDENCE_FILE):
        ev = normalize_evidence(row)
        if ev:
            all_events.append(ev)

    # Structure events
    for sf_key, sf_path in STRUCT_FILES.items():
        for row in _read_all_jsonl(sf_path):
            ev = normalize_structure(row)
            if ev:
                all_events.append(ev)

    # Scenario events
    for row in _read_all_jsonl(SCENARIO_FILE):
        ev = normalize_scenario(row)
        if ev:
            all_events.append(ev)

    # Observer events (optional)
    if OBS_QUAL_FILE.exists():
        for row in _read_all_jsonl(OBS_QUAL_FILE):
            ev = normalize_observer(row)
            if ev:
                all_events.append(ev)
    else:
        state.warnings.append("observer_input_missing")

    # Sort events by timestamp
    all_events.sort(key=lambda x: x["window_start_ts"])
    print(f"[HOE] {len(all_events)} event yüklendi", flush=True)

    # Compute composite patterns
    ts_to_events: dict[int, list[dict]] = defaultdict(list)
    for ev in all_events:
        ts_to_events[ev["window_start_ts"]].append(ev)

    composite_events: list[dict] = []
    for ts_val, evs in ts_to_events.items():
        if len(evs) >= 2:
            types = sorted(set(e["event_type"] for e in evs))
            sig   = make_pattern_signature(SYMBOL, "1S", types)
            key   = f"{SYMBOL}|1S|" + "+".join(types)
            sides = [e["side"] for e in evs]
            c_side = sides[0] if len(set(sides)) == 1 else "neutral"
            eid   = make_event_id("composite", SYMBOL, "1S", ts_val,
                                  "+".join(types), c_side)
            wte   = evs[0].get("window_end_ts")
            c_ev  = {
                "source":             "composite",
                "event_id":           eid,
                "symbol":             SYMBOL,
                "timeframe":          "1S",
                "window_start_ts":    ts_val,
                "window_end_ts":      wte,
                "event_type":         "+".join(types),
                "side":               c_side,
                "direction":          "unknown",
                "pattern_signature":  sig,
                "pattern_key":        key,
                "pattern_components": types,
                "calibration_status": "observed_not_scored",
                "source_refs":        {},
                "data_quality":       {},
                "raw":                {},
            }
            composite_events.append(c_ev)

    all_events.extend(composite_events)
    all_events.sort(key=lambda x: x["window_start_ts"])

    # Merge prices and events chronologically
    pi = 0  # price index
    ei = 0  # event index
    last_status_t = time.time()
    last_calib_t  = time.time()
    last_health_t = time.time()
    last_persist_t = time.time()

    with open(OBS_FILE, "a", encoding="utf-8") as obs_fh:
        while pi < len(prices) or ei < len(all_events):
            if HALT_FILE.exists():
                print("[HOE] SYSTEM_HALT — aborting batch", flush=True)
                break

            next_price_ts = prices[pi][0] if pi < len(prices) else float("inf")
            next_event_ts = (all_events[ei]["window_start_ts"]
                             if ei < len(all_events) else float("inf"))

            if next_price_ts <= next_event_ts:
                ts, px = prices[pi]
                update_and_check(state, ts, px, obs_fh)
                pi += 1
            else:
                ev = all_events[ei]
                open_observation(state, ev, obs_fh)
                ei += 1

            # Periodic operations
            now = time.time()
            if now - last_calib_t >= CALIBRATION_INTERVAL_S:
                compute_calibration_profiles(state)
                last_calib_t = now
            if now - last_health_t >= HEALTH_INTERVAL_S:
                write_health(state)
                last_health_t = now
            if now - last_persist_t >= PERSIST_INTERVAL_S:
                persist_open_positions(state)
                last_persist_t = now
            if now - last_status_t >= 60.0:
                print(f"[HOE STATUS] open={len(state.open_obs)} "
                      f"completed={state.completed_count} "
                      f"last_price_ts={state.last_price_ts}", flush=True)
                last_status_t = now

        # Force-close remaining open observations
        for obs_id, obs in list(state.open_obs.items()):
            _close_observation(state, obs, obs_fh,
                               all_measured=False, reason="batch_end")
        state.open_obs.clear()

    compute_calibration_profiles(state)
    write_health(state)
    persist_open_positions(state)

    print(f"[HOE] Batch done: prices={len(prices)} events={len(all_events)} "
          f"completed={state.completed_count}", flush=True)


# ── Live mode (asyncio) ───────────────────────────────────────────────────────
async def _tail_price(state: EngineState, obs_fh) -> None:
    """Tail primary price file."""
    while not PRIMARY_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(PRIMARY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row  = json.loads(line)
                ts   = row.get("window_start_ts")
                cdna = row.get("candle_dna") or {}
                co   = cdna.get("close")
                px   = _sf(co.get("price") if isinstance(co, dict) else co, 0.0)
                if ts is not None and px > 0:
                    update_and_check(state, int(ts), px, obs_fh)
            except Exception:
                pass

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
                row  = json.loads(line)
                ts   = row.get("window_start_ts")
                cdna = row.get("candle_dna") or {}
                co   = cdna.get("close")
                px   = _sf(co.get("price") if isinstance(co, dict) else co, 0.0)
                if ts is not None and px > 0:
                    update_and_check(state, int(ts), px, obs_fh)
            except Exception:
                pass


async def _tail_events(path: Path, norm_fn, state: EngineState,
                       obs_fh, required: bool = True) -> None:
    """Generic event file tailer."""
    if not path.exists() and not required:
        state.warnings.append(f"observer_input_missing")
        return

    while not path.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = norm_fn(json.loads(line))
                if ev:
                    open_observation(state, ev, obs_fh)
            except Exception:
                pass

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
                ev = norm_fn(json.loads(line))
                if ev:
                    open_observation(state, ev, obs_fh)
            except Exception:
                pass


async def _periodic(state: EngineState) -> None:
    """Periodic health/calibration/persist tasks."""
    last_calib   = time.time()
    last_health  = time.time()
    last_persist = time.time()
    last_status  = time.time()

    while not HALT_FILE.exists():
        await asyncio.sleep(5.0)
        now = time.time()
        if now - last_calib >= CALIBRATION_INTERVAL_S:
            compute_calibration_profiles(state)
            last_calib = now
        if now - last_health >= HEALTH_INTERVAL_S:
            write_health(state)
            last_health = now
        if now - last_persist >= PERSIST_INTERVAL_S:
            persist_open_positions(state)
            last_persist = now
        if now - last_status >= 60.0:
            print(f"[HOE STATUS] open={len(state.open_obs)} "
                  f"completed={state.completed_count} "
                  f"profiles={getattr(state,'profiles_written',0)}\n"
                  f"  last_price_ts={state.last_price_ts}", flush=True)
            last_status = now


async def run_live() -> None:
    state = EngineState()

    # Check missing inputs
    for p in ([PRIMARY_FILE, EVIDENCE_FILE] +
              list(DETECTOR_FILES.values()) +
              list(STRUCT_FILES.values()) + [SCENARIO_FILE]):
        if not p.exists():
            state.missing_inputs.append(str(p))

    # Restore
    restored = restore_open_positions(state)
    n_prev   = load_processed_ids(state)
    print(f"[HOE] Historical Outcome Engine başlatıldı", flush=True)
    print(f"[HOE] {n_prev} tamamlanmış event, "
          f"{restored} açık observation restore edildi", flush=True)

    write_health(state)

    with open(OBS_FILE, "a", encoding="utf-8") as obs_fh:
        tasks = [
            asyncio.create_task(_tail_price(state, obs_fh), name="hoe-price"),
            asyncio.create_task(_periodic(state),            name="hoe-periodic"),
            asyncio.create_task(
                _tail_events(EVIDENCE_FILE, normalize_evidence, state, obs_fh),
                name="hoe-evidence"),
            asyncio.create_task(
                _tail_events(SCENARIO_FILE, normalize_scenario, state, obs_fh),
                name="hoe-scenario"),
            asyncio.create_task(
                _tail_events(OBS_QUAL_FILE, normalize_observer,
                             state, obs_fh, required=False),
                name="hoe-observer"),
        ]
        # Detector tasks
        for det_key, det_path in DETECTOR_FILES.items():
            tasks.append(asyncio.create_task(
                _tail_events(det_path, normalize_detector, state, obs_fh),
                name=f"hoe-det-{det_key}",
            ))
        # Structure tasks
        for sf_key, sf_path in STRUCT_FILES.items():
            tasks.append(asyncio.create_task(
                _tail_events(sf_path, normalize_structure, state, obs_fh),
                name=f"hoe-{sf_key}",
            ))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("[HOE] Tasks cancelled", flush=True)

    compute_calibration_profiles(state)
    write_health(state)
    persist_open_positions(state)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Historical Outcome Engine — Layer 11")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[HOE] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        sys.exit(1)

    if args.mode == "batch":
        run_batch()
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
