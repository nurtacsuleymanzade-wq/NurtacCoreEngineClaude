"""
NurtacCoreEngineClaude — Layer-10: Observer + Setup Qualifier

Primary trigger: data/combined_1s_dna_btcusdt.jsonl
Also monitors:   data/setups.jsonl  (new Layer-7 setups)
Reads:   setups (L7), scenarios (L9), structure 1S/1M, volume profile 1M,
         gate, bias, detector labels (absorption/initiative_flow/sweep), baseline
Writes:  data/observations.jsonl
         data/qualified_setups.jsonl

Rules:
  - No Binance API/WebSocket calls
  - Only reads existing JSONL files
  - No real orders — qualification records only
  - Never crash, never write invalid records
"""

import argparse
import asyncio
import json
import os
import sys
from collections import deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"
POLL_SLEEP = 0.05

OBSERVATIONS_FILE = DATA_DIR / "observations.jsonl"
QUALIFIED_FILE    = DATA_DIR / "qualified_setups.jsonl"

PRIMARY_FILE   = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
SETUPS_FILE    = DATA_DIR / "setups.jsonl"
LIQ_SETUPS_FILE = DATA_DIR / "liquidation_setups.jsonl"
TRADE_BRAIN_SETUPS_FILE = DATA_DIR / "trade_brain_setups.jsonl"
SCENARIOS_FILE = DATA_DIR / "scenarios.jsonl"

STRUCT_1S_FILE = DATA_DIR / "structure_1s.jsonl"
STRUCT_1M_FILE = DATA_DIR / "structure_1m.jsonl"
VOL_1M_FILE    = DATA_DIR / "volume_profile_1m.jsonl"
ZONE_CTX_FILE  = DATA_DIR / "zone_context.json"
VP_FILE        = DATA_DIR / "volume_profile.json"
GATE_FILE      = DATA_DIR / "decision_gate_output.jsonl"
BIAS_FILE      = DATA_DIR / "bias_context.jsonl"
BASELINE_FILE  = DATA_DIR / "historical_baseline_dna.jsonl"
REGIME_FILE    = DATA_DIR / "regime_context.jsonl"

DETECTOR_FILES = {
    "absorption":      DATA_DIR / "labels_absorption.jsonl",
    "initiative_flow": DATA_DIR / "labels_initiative_flow.jsonl",
    "sweep":           DATA_DIR / "labels_sweep.jsonl",
}

MAX_OPEN_SETUPS       = 20
SETUP_LIFETIME_MS     = 300_000   # 5 min
WAITING_TIMEOUT_MS    = 120_000   # 2 min - setup olgunlassin
DEVELOPING_TIMEOUT_MS = 120_000   # 2 min
VOLUME_BOOST          = 0.5
ATR_TOUCH_FACTOR      = 0.1       # ATR * 0.1 = touch range for HOLD
LIVE_CACHE_MAX        = 120
MIN_BRAIN_SOFT_CONFIRMATIONS = 2
BRAIN_MODE = "paper_learning_v0"

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_last_n_lines(path, n: int = 200) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        import subprocess as _sp
        raw = _sp.getoutput(f"tail -{int(n)} {path}")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    except Exception:
        pass
    return records

def _read_last_n_jsonl(path: Path, maxlen: int) -> list[dict]:
    """Read only last N lines using tail (truly RAM-safe)."""
    import subprocess as _sp
    if not path.exists():
        return []
    records: list[dict] = []
    try:
        raw = _sp.getoutput(f"tail -{maxlen} {path}")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    except Exception:
        pass
    return records

def _build_index(records: list[dict]) -> dict[int, dict]:
    idx: dict[int, dict] = {}
    for rec in records:
        ts = rec.get("window_start_ts") or rec.get("ts")
        if ts is not None:
            idx[int(ts)] = rec
    return idx

def _cache_put(cache: dict[int, dict], ts: int, rec: dict) -> None:
    cache[ts] = rec
    overflow = len(cache) - LIVE_CACHE_MAX
    if overflow > 0:
        for old_ts in sorted(cache)[:overflow]:
            cache.pop(old_ts, None)

def _latest_at_or_before(idx: dict[int, dict], ts: int) -> dict | None:
    candidates = [k for k in idx if k <= ts]
    if not candidates:
        return None
    return idx[max(candidates)]

def _latest_within_window(
    idx: dict[int, dict], ts: int, window_ms: int, field: str
) -> dict | None:
    """Return the latest record in [ts - window_ms, ts] with a non-empty field."""
    lower = ts - window_ms
    candidates = [
        k for k, rec in idx.items()
        if lower <= k <= ts and rec.get(field) not in (None, "", "none", "None")
    ]
    if not candidates:
        return None
    return idx[max(candidates)]

def _write_jsonl(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

def _load_baseline(recs: list[dict], timeframe: str) -> dict | None:
    for r in reversed(recs):
        if r.get("timeframe") == timeframe:
            return r
    return None

def _price_from_cdna(cdna: dict) -> float:
    co = cdna.get("close")
    if isinstance(co, dict):
        return _sf(co.get("price"), 0.0)
    return _sf(co, 0.0)

def _bl_mean_vol(baseline_1s: dict | None) -> float:
    if not baseline_1s:
        return 0.0
    metrics = baseline_1s.get("metrics") or {}
    tv = metrics.get("total_volume") or {}
    if isinstance(tv, dict):
        v = tv.get("mean")
        if v is not None:
            return _sf(v, 0.0)
    # Try short window
    bw = baseline_1s.get("baseline_windows") or {}
    return 0.0

# ── State machine ─────────────────────────────────────────────────────────────
# QUALIFIED is observational, not terminal. Only invalidation/expiry stop tracking.
TERMINAL_STATES = frozenset({"INVALIDATED", "EXPIRED"})

class ObservedSetup:
    """Tracks observation state for one Layer-7 setup."""

    def __init__(self, setup: dict, opened_ts: int):
        self.setup_id     = setup.get("setup_id", f"{opened_ts}_unknown")
        self.direction    = setup.get("direction", "long")
        self.setup_type   = setup.get("setup_type", "normal")
        self.quality_tier = setup.get("quality_tier", "L1_LOW")
        self.pattern_key  = setup.get("pattern_key", f"{self.setup_type}_{self.direction}")
        self.entry_timing = setup.get("entry_timing", "unknown")
        self.source_setup = setup
        self.opened_ts    = opened_ts
        self.waiting_timeout_ms = (
            30_000 if self.setup_type == "liq_cascade_reversal"
            else WAITING_TIMEOUT_MS
        )

        e  = setup.get("entry") or {}
        sl = setup.get("sl")    or {}

        self.entry_price = _sf(e.get("price"),   0.0)
        self.sl_price    = _sf(sl.get("price"),  0.0)
        self.tp1_orig    = _sf((setup.get("tp1") or {}).get("price"), 0.0)
        self.tp2_orig    = _sf((setup.get("tp2") or {}).get("price"), 0.0)
        self.tp3_orig    = _sf((setup.get("tp3") or {}).get("price"), 0.0)
        self.atr_used    = _sf(setup.get("atr_used"), 1.0)

        self.state = "WAITING"
        self.transition_history: list[dict] = []

        # Event flags
        self.has_pullback_start    = False
        self.has_pullback_complete = False
        self.has_hold              = False
        self.has_breakout          = False
        self.has_reclaim           = False
        self.has_follow_through    = False

        # Rolling state
        self.delta_buf: list[float] = []
        self.close_buf: list[float] = []
        self.prev_location: str | None = None
        self.prev_close:    float | None = None
        self.prev_poc:      float | None = None

        # HOLD tracking
        self.hold_level:      float | None = None
        self.hold_touches:    int = 0
        self.hold_bars_clear: int = 0

        # BREAKOUT tracking
        self.breakout_confirmed  = False
        self.breakout_ts:        int | None = None
        self.breakout_level:     float | None = None
        self.post_breakout_bars  = 0
        self.follow_through_bars = 0

        # Phase timing
        self.developing_start_ts: int | None = None

    def is_active(self) -> bool:
        return self.state not in TERMINAL_STATES

    # ── Internal helpers ──────────────────────────────────────────────────────
    def _obs_write(self, ts: int, event: str, old_state: str, new_state: str,
                   cur_price: float, details: str, obs_fh) -> None:
        """Write one observation record."""
        details_l = (details or "").lower()
        f_gates = {
            "f1_bos": "bos" in details_l or "breakout" in details_l or "reclaim" in details_l,
            "f2_volume": "vol" in details_l or "volume" in details_l,
            "f3_regime": "regime" in details_l,
            "f4_structure": "structure" in details_l or "acceptance" in details_l,
            "f5_confirmation": "confirm" in details_l or "follow-through" in details_l or "follow_through" in details_l,
        }
        blocking_gate = ""
        if new_state == "INVALIDATED":
            if "regime" in details_l:
                blocking_gate = "REGIME_MISMATCH"
            elif "breakout" in details_l or "weak" in details_l:
                blocking_gate = "BREAKOUT_WEAK"
            else:
                blocking_gate = event
        elif new_state == "QUALIFYING":
            blocking_gate = "QUALIFYING"
        elif new_state == "DEVELOPING":
            blocking_gate = "DEVELOPING"
        observer_reason = details or ""
        rec = {
            "engine":          "observer_engine",
            "ts":              ts,
            "symbol":          SYMBOL,
            "source_setup_id": self.setup_id,
            "event_type":      event,
            "state_before":    old_state,
            "state_after":     new_state,
            "current_price":   cur_price,
            "details":         details,
            "f_gates":         f_gates,
            "blocking_gate":   blocking_gate,
            "observer_reason":  observer_reason,
            "bos_ok":          f_gates["f1_bos"],
            "vol_ok":          f_gates["f2_volume"],
            "regime_ok":       f_gates["f3_regime"],
        }
        _write_jsonl(obs_fh, rec)

    def _transition(self, ts: int, new_state: str, event: str, details: str,
                    cur_price: float, obs_fh) -> None:
        old = self.state
        self.state = new_state
        self.transition_history.append({
            "ts": ts, "event": event,
            "state_before": old, "state_after": new_state,
            "details": details,
        })
        self._obs_write(ts, event, old, new_state, cur_price, details, obs_fh)
        _print_obs_transition(self.setup_id, ts, event, old, new_state,
                              cur_price, details, self.direction)

    def _note_event(self, ts: int, event: str, cur_price: float,
                    details: str, obs_fh) -> None:
        """Record event without state change (no-state-change observation)."""
        self.transition_history.append({
            "ts": ts, "event": event,
            "state_before": self.state, "state_after": self.state,
            "details": details,
        })
        self._obs_write(ts, event, self.state, self.state, cur_price, details, obs_fh)

    # ── Main bar processor ────────────────────────────────────────────────────
    def process_bar(
        self, ts: int, primary: dict,
        s1s: dict | None, s1m: dict | None, vp1m: dict | None,
        gate: dict | None, scenario: dict | None,
        det: dict[str, dict | None], bias: dict | None,
        baseline_1s: dict | None, regime: dict | None,
        obs_fh, qual_fh,
    ) -> bool:
        if not self.is_active():
            return False

        # ── Unpack price first (needed for lifetime/invalidation messages) ───────
        cdna      = primary.get("candle_dna") or {}
        cur_price = _price_from_cdna(cdna)

        # ── Global lifetime ───────────────────────────────────────────────────
        if ts >= self.opened_ts + SETUP_LIFETIME_MS:
            self._transition(ts, "EXPIRED", "LIFETIME_EXPIRED",
                             "300s max lifetime reached", cur_price, obs_fh)
            return False

        # ── Unpack remaining bar fields ───────────────────────────────────────
        delta     = _sf(cdna.get("delta"), 0.0)
        total_vol = _sf(cdna.get("total_volume"), 0.0)

        # zone_context.json (new format) → cur_loc
        _zone = {}
        try:
            if ZONE_CTX_FILE.exists():
                _zone = json.loads(ZONE_CTX_FILE.read_text())
        except Exception:
            pass
        # volume_profile.json (new format) → poc/vah/val
        _vp = {}
        try:
            if VP_FILE.exists():
                _vp = json.loads(VP_FILE.read_text())
        except Exception:
            pass
        # Fallback: eski vp1m formatı
        vp_loc  = (vp1m or {}).get("location") or {}
        vp_prof = (vp1m or {}).get("profile")  or {}
        # cur_loc: zone_context öncelikli
        cur_loc = (
            _zone.get("price_location")
            or (vp_loc.get("location") or vp_loc).get("position")
        )
        # poc/vah/val: volume_profile.json öncelikli
        poc     = _sf(_vp.get("poc_price") or vp_prof.get("poc"), 0.0)
        vah     = _sf(_vp.get("vah")       or vp_prof.get("vah"), 0.0)
        val     = _sf(_vp.get("val")       or vp_prof.get("val"), 0.0)
        prof_shape = vp_prof.get("profile_shape")

        bos_1s    = (s1s or {}).get("bos") or {}
        trend_1s  = ((s1s or {}).get("trend") or {})
        micro_bos = bos_1s.get("micro_bos")
        # F2 için recent window: live mode'da s1s zaten latest_at_or_before ile geliyor
        trend_1s_dir = trend_1s.get("direction", "unknown")

        s1m_trend    = ((s1m or {}).get("trend") or {})
        trend_1m_dir = s1m_trend.get("direction", "unknown")

        flow_rec  = (det.get("initiative_flow") or {})
        flow_dir  = flow_rec.get("direction")
        sweep_rec = (det.get("sweep") or {})
        sweep_lbl = sweep_rec.get("label", "none")
        absrp_rec = (det.get("absorption") or {})

        mean_vol  = _bl_mean_vol(baseline_1s)
        dom_bias  = (bias or {}).get("dominant_bias", "neutral")
        gate_grade = (gate or {}).get("setup_grade")

        dom_scen_dir  = (scenario or {}).get("dominant_direction", "neutral")
        dom_scen_name = (scenario or {}).get("dominant_scenario")
        cascade_scenario = next((
            item for item in (scenario or {}).get("active_scenarios", [])
            if item.get("scenario_name", item.get("name"))
            == "LIQ_CASCADE_REVERSAL"
        ), None)
        cascade_direction = (
            cascade_scenario.get("direction") if cascade_scenario else None
        )
        if cascade_scenario and self.state == "WAITING":
            self.waiting_timeout_ms = 30_000
        scen_is_confirmed = any(
            s.get("status") == "confirmed" and s.get("scenario_name") == dom_scen_name
            for s in (scenario or {}).get("active_scenarios", [])
        ) if dom_scen_name else False

        # Update rolling buffers
        self.delta_buf.append(delta)
        if len(self.delta_buf) > 5:
            self.delta_buf.pop(0)
        self.close_buf.append(cur_price)
        if len(self.close_buf) > 5:
            self.close_buf.pop(0)

        # ── 1. Check invalidation ─────────────────────────────────────────────
        if self._check_invalidation(ts, cur_price, dom_scen_dir,
                                    scen_is_confirmed, obs_fh):
            return False

        # ── 2. Phase timeouts ─────────────────────────────────────────────────
        if self.state == "WAITING" and ts >= self.opened_ts + self.waiting_timeout_ms:
            timeout_seconds = self.waiting_timeout_ms // 1000
            self._transition(ts, "EXPIRED", "WAITING_TIMEOUT",
                             f"No events in {timeout_seconds}s", cur_price, obs_fh)
            return False

        if (self.state == "DEVELOPING" and self.developing_start_ts is not None and
                ts >= self.developing_start_ts + DEVELOPING_TIMEOUT_MS):
            self._transition(ts, "EXPIRED", "DEVELOPING_TIMEOUT",
                             "No qualifying events in 120s", cur_price, obs_fh)
            return False

        # ── 3. Detect events ──────────────────────────────────────────────────
        events = self._detect_events(
            ts, cur_price, delta, total_vol,
            poc, vah, val, micro_bos, cur_loc,
            sweep_lbl, flow_dir, mean_vol,
        )

        # ── 4. Apply events ───────────────────────────────────────────────────
        for ev_type, details in events:
            if not self.is_active():
                break
            self._apply_event(ts, ev_type, details, cur_price, obs_fh)

        # ── 5. Check qualification ────────────────────────────────────────────
        if self.is_active() and self.state == "QUALIFYING":
            qual = self._build_qual_criteria(delta, micro_bos, trend_1s_dir,
                                             cur_loc, dom_scen_dir, dom_bias,
                                             regime or {}, cascade_direction)
            rejection = None
            if not qual.get("F0_regime_compatible", True):
                rejection = "REGIME_MISMATCH"
            elif not qual.get("F_session_ok", True):
                rejection = "SESSION_MISMATCH"
            elif not qual.get("F_timing_ok", True):
                rejection = "EXTENDED_ENTRY"
            if rejection:
                self._transition(ts, "INVALIDATED", rejection,
                                 rejection, cur_price, obs_fh)
                return False
            # F0 ve session kritik — bunlar False ise gerçekten uyumsuz
            hard_fail = (
                not qual.get("F0_regime_compatible", True) or
                not qual.get("F_session_ok", True)
            )
            if hard_fail:
                pass  # INVALIDATED zaten yukarıda handle edildi
            else:
                brain = self._build_brain_decision(qual)
                if brain["decision"] == "APPROVED":
                    # Minimal paper-trade v0: allows observation-backed paper trades for learning; not final live trading threshold.
                    self._emit_qualified(
                        ts, cur_price, poc, vah, val, prof_shape,
                        trend_1s_dir, trend_1m_dir, dom_scen_name,
                        gate_grade, dom_bias, qual,
                        flow_rec, absrp_rec, regime or {}, qual_fh, obs_fh,
                        brain=brain,
                    )
                    return False
                else:
                    self._obs_write(
                        ts, "F_GATE_PARTIAL",
                        "QUALIFYING", "QUALIFYING",
                        cur_price,
                        f"BRAIN_WAIT reason={brain.get('reason')} "
                        f"hard_gates_ok={brain.get('hard_gates_ok')} "
                        f"soft_confirmations={brain.get('soft_confirmations')}/"
                        f"{brain.get('required_soft_confirmations')} "
                        f"failed_soft={brain.get('failed_soft_gates', [])}",
                        obs_fh,
                    )
                    # Setup QUALIFYING'de kalır, bir sonraki bar'da tekrar değerlendirilir

        # ── 6. Update rolling state ───────────────────────────────────────────
        self.prev_location = cur_loc
        self.prev_close    = cur_price
        if poc > 0:
            self.prev_poc = poc

        return self.is_active()

    # ── Invalidation ──────────────────────────────────────────────────────────
    def _check_invalidation(self, ts: int, cur_price: float, dom_scen_dir: str,
                             scen_confirmed: bool, obs_fh) -> bool:
        reason: str | None = None

        # Pre-entry SL kontrolü kaldırıldı.
        # SL yönetimi paper_trade_engine'in sorumluluğundadır.
        # Observer sadece gözlemler; trade açılmadan SL invalide etmez.
        if self.sl_price > 0 and cur_price > 0:
            sl_breached = (
                (self.direction == "long" and cur_price < self.sl_price) or
                (self.direction == "short" and cur_price > self.sl_price)
            )
            if sl_breached:
                print(
                    f"[OBS] SL_NOTE {self.setup_id}: "
                    f"pre-entry price={cur_price:.2f} sl={self.sl_price:.2f} "
                    f"(gözlem devam ediyor, trade açılmadı)",
                    flush=True,
                )

        if scen_confirmed:
            if self.direction == "long" and dom_scen_dir == "bearish":
                reason = "confirmed bearish scenario invalidates long setup"
            elif self.direction == "short" and dom_scen_dir == "bullish":
                reason = "confirmed bullish scenario invalidates short setup"

        if reason:
            self._transition(ts, "INVALIDATED", "INVALIDATED",
                             reason, cur_price, obs_fh)
            return True
        return False

    # ── Event detection ───────────────────────────────────────────────────────
    def _detect_events(
        self, ts: int, cur_price: float, delta: float, total_vol: float,
        poc: float, vah: float, val: float,
        micro_bos: str | None, cur_loc: str | None,
        sweep_lbl: str, flow_dir: str | None, mean_vol: float,
    ) -> list[tuple[str, str]]:
        events: list[tuple[str, str]] = []
        atr_t = max(self.atr_used * ATR_TOUCH_FACTOR, 0.5)

        # ── RECLAIM ───────────────────────────────────────────────────────────
        if self.prev_location and cur_loc and self.prev_location != cur_loc:
            if self.direction == "long":
                if (self.prev_location in ("below_value", "at_val", "demand") and
                        cur_loc in ("inside_value", "above_value", "at_vah", "at_poc",
                                    "fvg", "neutral", "above_poc")):
                    events.append(("RECLAIM_VALUE",
                                   f"location {self.prev_location}→{cur_loc}"))
            else:
                if (self.prev_location in ("above_value", "at_vah", "supply") and
                        cur_loc in ("inside_value", "below_value", "at_val", "at_poc",
                                    "fvg", "neutral", "below_poc")):
                    events.append(("RECLAIM_VALUE",
                                   f"location {self.prev_location}→{cur_loc}"))

        if poc > 0 and self.prev_poc and self.prev_close and self.prev_close > 0:
            if self.direction == "long" and self.prev_close < poc and cur_price >= poc:
                events.append(("RECLAIM_POC",
                               f"close {self.prev_close:.2f}→{cur_price:.2f} poc={poc:.2f}"))
            elif self.direction == "short" and self.prev_close > poc and cur_price <= poc:
                events.append(("RECLAIM_POC",
                               f"close {self.prev_close:.2f}→{cur_price:.2f} poc={poc:.2f}"))

        # ── PULLBACK ──────────────────────────────────────────────────────────
        if not self.has_pullback_start:
            if self.direction == "long" and flow_dir == "buy_initiative" and delta < 0:
                events.append(("PULLBACK_IN_PROGRESS",
                               f"buy_initiative + delta={delta:.4f}"))
            elif self.direction == "short" and flow_dir == "sell_initiative" and delta > 0:
                events.append(("PULLBACK_IN_PROGRESS",
                               f"sell_initiative + delta={delta:.4f}"))
        elif not self.has_pullback_complete:
            if self.direction == "long" and delta > 0:
                events.append(("PULLBACK_COMPLETE",
                               f"delta resumed positive: {delta:.4f}"))
            elif self.direction == "short" and delta < 0:
                events.append(("PULLBACK_COMPLETE",
                               f"delta resumed negative: {delta:.4f}"))

        # ── HOLD ──────────────────────────────────────────────────────────────
        if not self.has_hold and cur_price > 0:
            # Watch level: long=support(VAL/POC), short=resistance(VAH/POC)
            if self.direction == "long":
                watch = val if val > 0 else (poc if poc > 0 else 0.0)
            else:
                watch = vah if vah > 0 else (poc if poc > 0 else 0.0)

            if watch > 0:
                if self.hold_level is None:
                    self.hold_level = watch

                in_touch = abs(cur_price - self.hold_level) <= atr_t
                held = ((self.direction == "long" and cur_price >= self.hold_level) or
                        (self.direction == "short" and cur_price <= self.hold_level))

                if in_touch:
                    self.hold_touches += 1
                if held:
                    self.hold_bars_clear += 1
                else:
                    self.hold_bars_clear = 0

                if self.hold_touches >= 2 and self.hold_bars_clear >= 3:
                    events.append(("HOLD_CONFIRMED",
                                   f"level={self.hold_level:.2f} "
                                   f"touches={self.hold_touches} "
                                   f"bars_clear={self.hold_bars_clear}"))

        # ── BREAKOUT ──────────────────────────────────────────────────────────
        if not self.breakout_confirmed and cur_price > 0:
            tgt = vah if self.direction == "long" else val
            if tgt > 0:
                broke = ((self.direction == "long" and cur_price > tgt) or
                         (self.direction == "short" and cur_price < tgt))
                if broke:
                    vol_ok = mean_vol <= 0 or total_vol >= mean_vol * VOLUME_BOOST
                    # BOS koşulu kaldırıldı — F kapıları zaten yön/bias/delta filtreler
                    if vol_ok:
                        events.append(("BREAKOUT_CONFIRMED",
                                       f"close={cur_price:.2f} tgt={tgt:.2f} "
                                       f"vol={total_vol:.4f}"))
                        self.breakout_confirmed = True
                        self.breakout_ts    = ts
                        self.breakout_level = tgt
                    else:
                        events.append(("BREAKOUT_WEAK",
                                       f"close={cur_price:.2f} tgt={tgt:.2f} "
                                       f"vol_ok={vol_ok} bos_ok=removed"))

        elif self.breakout_confirmed:
            self.post_breakout_bars += 1
            in_dir = ((self.direction == "long" and delta > 0) or
                      (self.direction == "short" and delta < 0))
            if in_dir:
                self.follow_through_bars += 1
            else:
                self.follow_through_bars = 0

            if self.follow_through_bars >= 2 and not self.has_follow_through:
                events.append(("FOLLOW_THROUGH_STRONG",
                               f"bars={self.follow_through_bars}"))
            elif self.follow_through_bars == 1:
                events.append(("FOLLOW_THROUGH_WEAK", "1 bar follow-through"))

            # FAILED_BREAKOUT: returned to broken level within 5 bars
            if self.post_breakout_bars <= 5 and self.breakout_level is not None:
                returned = ((self.direction == "long" and
                             cur_price < self.breakout_level) or
                            (self.direction == "short" and
                             cur_price > self.breakout_level))
                if returned:
                    events.append(("FAILED_BREAKOUT",
                                   f"price {cur_price:.2f} returned to "
                                   f"{self.breakout_level:.2f}"))

        # ── REJECTION ─────────────────────────────────────────────────────────
        if sweep_lbl not in ("none", None) and cur_price > 0:
            ref = poc if poc > 0 else (vah if self.direction == "short" else val)
            if ref > 0 and abs(cur_price - ref) <= atr_t * 3:
                events.append(("REJECTION_CONFIRMED",
                               f"sweep={sweep_lbl} near {ref:.2f}"))

        return events

    # ── Event application ─────────────────────────────────────────────────────
    def _apply_event(self, ts: int, ev: str, details: str,
                     cur_price: float, obs_fh) -> None:
        # Terminal events
        if ev == "FAILED_BREAKOUT":
            self._transition(ts, "INVALIDATED", ev, details, cur_price, obs_fh)
            return

        # Events that don't change state but are recorded
        if ev in ("BREAKOUT_WEAK", "REJECTION_CONFIRMED"):
            self._note_event(ts, ev, cur_price, details, obs_fh)
            return

        # PULLBACK_IN_PROGRESS → DEVELOPING
        if ev == "PULLBACK_IN_PROGRESS":
            self.has_pullback_start = True
            if self.state == "WAITING":
                self._transition(ts, "DEVELOPING", ev, details, cur_price, obs_fh)
                self.developing_start_ts = ts
            return

        # RECLAIM → DEVELOPING or QUALIFYING
        if ev in ("RECLAIM_VALUE", "RECLAIM_POC"):
            self.has_reclaim = True
            if self.state == "WAITING":
                self._transition(ts, "DEVELOPING", ev, details, cur_price, obs_fh)
                self.developing_start_ts = ts
            elif self.state == "DEVELOPING":
                self._transition(ts, "QUALIFYING", ev, details, cur_price, obs_fh)
            return

        # HOLD_CONFIRMED → DEVELOPING or QUALIFYING
        if ev == "HOLD_CONFIRMED":
            self.has_hold = True
            if self.state == "WAITING":
                self._transition(ts, "DEVELOPING", ev, details, cur_price, obs_fh)
                self.developing_start_ts = ts
            elif self.state == "DEVELOPING":
                self._transition(ts, "QUALIFYING", ev, details, cur_price, obs_fh)
            return

        # PULLBACK_COMPLETE → QUALIFYING
        if ev == "PULLBACK_COMPLETE":
            self.has_pullback_complete = True
            if self.state == "DEVELOPING":
                self._transition(ts, "QUALIFYING", ev, details, cur_price, obs_fh)
            return

        # BREAKOUT_CONFIRMED → DEVELOPING or QUALIFYING
        if ev == "BREAKOUT_CONFIRMED":
            self.has_breakout = True
            if self.state == "WAITING":
                self._transition(ts, "DEVELOPING", ev, details, cur_price, obs_fh)
                self.developing_start_ts = ts
            elif self.state == "DEVELOPING":
                self._transition(ts, "QUALIFYING", ev, details, cur_price, obs_fh)
            return

        # FOLLOW_THROUGH → QUALIFYING (if already has breakout or reclaim)
        if ev in ("FOLLOW_THROUGH_STRONG", "FOLLOW_THROUGH_WEAK"):
            self.has_follow_through = True
            if self.state == "DEVELOPING" and (self.has_breakout or self.has_reclaim):
                self._transition(ts, "QUALIFYING", ev, details, cur_price, obs_fh)
            return

    # ── Qualification criteria ────────────────────────────────────────────────
    def _build_qual_criteria(
        self, delta: float, micro_bos: str | None, trend_1s: str,
        cur_loc: str | None, dom_scen_dir: str, dom_bias: str,
        regime: dict, cascade_direction: str | None = None,
    ) -> dict[str, bool]:
        regime_ok = regime.get("trade_allowed", True) if regime else True
        compatible = regime.get("compatible_setups", []) if regime else []
        setup_type_str = f"{self.setup_type}_{self.direction}"
        f0 = regime_ok and (
            not compatible or any(c in setup_type_str for c in compatible)
        )
        session = regime.get("session", "UNKNOWN") if regime else "UNKNOWN"
        bad_sessions = set()  # BTC 7/24 - tüm session'lar açık
        f_session = session not in bad_sessions or self.setup_type in ["REVERSAL", "RECLAIM"]
        f_timing = self.entry_timing not in ("extended",)
        if self.direction == "long":
            macro_ctx = {"dominant_bias": dom_bias}
            macro_bullish = macro_ctx.get("dominant_bias") == "long"
            trend_1m_up = ((s1m or {}).get("trend", {}).get("direction") == "uptrend")
            f1 = (delta > 0) or (macro_bullish and trend_1m_up)
            f2 = (trend_1s == "uptrend" or micro_bos == "bullish" or trend_1m_dir == "uptrend")
            # zone_engine: demand/supply/fvg/neutral/above_poc  
            f3 = cur_loc in ("demand", "fvg", "neutral", "at_poc", "above_poc",
                             "inside_value", "above_value", "at_vah")  # her iki format
            f4 = dom_scen_dir in ("bullish", "neutral")
            f5 = dom_bias in ("long", "neutral")
        else:
            macro_ctx = {"dominant_bias": dom_bias}
            macro_bearish = macro_ctx.get("dominant_bias") == "short"
            trend_1m_down = ((s1m or {}).get("trend", {}).get("direction") == "downtrend")
            f1 = (delta < 0) or (macro_bearish and trend_1m_down)
            f2 = (trend_1s == "downtrend" or micro_bos == "bearish" or trend_1m_dir == "downtrend")
            # zone_engine: demand/supply/fvg/neutral/above_poc
            # Eski: inside_value/below_value/at_val/at_poc (artık kullanılmıyor)
            f3 = cur_loc in ("demand", "fvg", "neutral", "at_poc", "below_poc",
                             "inside_value", "below_value", "at_val")  # her iki format
            f4 = dom_scen_dir in ("bearish", "neutral")
            f5 = dom_bias in ("short", "neutral")

        if self.setup_type.startswith("liq_"):
            f3 = True
        if cascade_direction is not None:
            f4 = (
                cascade_direction == "bullish"
                if self.direction == "long"
                else cascade_direction == "bearish"
            )

        return {
            "F0_regime_compatible": f0,
            "F1_delta_aligned":     f1,
            "F2_structure_aligned": f2,
            "F3_location_valid":    f3,
            "F4_scenario_aligned":  f4,
            "F5_bias_aligned":      f5,
            "F_session_ok":         f_session,
            "F_timing_ok":          f_timing,
        }

    def _build_brain_decision(self, qual: dict) -> dict:
        soft_keys = [
            "F1_delta_aligned",
            "F2_structure_aligned",
            "F3_location_valid",
            "F4_scenario_aligned",
            "F5_bias_aligned",
        ]
        hard_gates_ok = all([
            bool(qual.get("F0_regime_compatible", False)),
            bool(qual.get("F_session_ok", False)),
            bool(qual.get("F_timing_ok", False)),
        ])
        passed_soft_gates = [k for k in soft_keys if qual.get(k, False)]
        failed_soft_gates = [k for k in soft_keys if not qual.get(k, False)]
        soft_confirmations = len(passed_soft_gates)
        approved = hard_gates_ok and soft_confirmations >= MIN_BRAIN_SOFT_CONFIRMATIONS
        if approved:
            decision = "APPROVED"
            reason = (
                f"approved hard_gates_ok={hard_gates_ok} "
                f"soft_confirmations={soft_confirmations}/"
                f"{MIN_BRAIN_SOFT_CONFIRMATIONS} "
                f"passed_soft={passed_soft_gates}"
            )
        else:
            decision = "WAIT"
            reason = (
                f"wait hard_gates_ok={hard_gates_ok} "
                f"soft_confirmations={soft_confirmations}/"
                f"{MIN_BRAIN_SOFT_CONFIRMATIONS} "
                f"failed_soft={failed_soft_gates}"
            )
        return {
            "decision": decision,
            "hard_gates_ok": hard_gates_ok,
            "soft_confirmations": soft_confirmations,
            "required_soft_confirmations": MIN_BRAIN_SOFT_CONFIRMATIONS,
            "passed_soft_gates": passed_soft_gates,
            "failed_soft_gates": failed_soft_gates,
            "reason": reason,
            "brain_mode": BRAIN_MODE,
        }

    # ── Emit qualified setup ──────────────────────────────────────────────────
    def _emit_qualified(
        self, ts: int, cur_price: float, poc: float, vah: float, val: float,
        prof_shape: str | None, trend_1s: str, trend_1m: str,
        dom_scen_name: str | None, gate_grade: str | None, dom_bias: str,
        qual: dict[str, bool], flow_rec: dict, absrp_rec: dict,
        regime: dict, qual_fh, obs_fh, brain: dict | None = None,
    ) -> None:
        time_to_qualify = max(0, (ts - self.opened_ts) // 1000)
        qsid = f"QS_{self.setup_id}"

        # Compute prices from qualify-time close
        atr = self.atr_used if self.atr_used > 0 else 1.0
        ep  = cur_price

        if self.direction == "long":
            sl  = ep - atr * 1.5
            tp1 = ep + atr * 1.5
            tp2 = ep + atr * 3.0
            tp3 = ep + atr * 4.5
            # Use more conservative of computed vs original SL
            if self.sl_price > 0 and self.sl_price < ep:
                sl = max(sl, self.sl_price)
        else:
            sl  = ep + atr * 1.5
            tp1 = ep - atr * 1.5
            tp2 = ep - atr * 3.0
            tp3 = ep - atr * 4.5
            if self.sl_price > 0 and self.sl_price > ep:
                sl = min(sl, self.sl_price)

        sl_dist = abs(ep - sl)

        # Derive best primary target
        if self.direction == "long":
            primary_tgt = vah if (vah > ep and vah > 0) else tp1
            tgt_rationale = "VAH" if (vah > ep and vah > 0) else "ATR TP1"
        else:
            primary_tgt = val if (val < ep and val > 0) else tp1
            tgt_rationale = "VAL" if (val < ep and val > 0) else "ATR TP1"

        context = {
            "current_price": round(cur_price, 4),
            "poc":           round(poc, 4) if poc > 0 else None,
            "location":      None,  # cur_loc not in scope here; caller sets via qual
            "profile_shape": prof_shape,
            "trend_1s":      trend_1s,
            "trend_1m":      trend_1m,
            "active_scenario": dom_scen_name,
            "gate_grade":    gate_grade,
            "market_bias":   dom_bias,
        }

        mq_dir_word = "below" if self.direction == "long" else "above"
        loc_desc = f"poc={poc:.2f}" if poc > 0 else "inside_value"

        qual_rec = {
            "engine":               "observer_engine",
            "qualified_setup_id":   qsid,
            "source_setup_id":      self.setup_id,
            "symbol":               SYMBOL,
            "direction":            self.direction,
            "setup_type":           self.setup_type,
            "quality_tier":         self.quality_tier,
            "pattern_key":          self.pattern_key,
            "regime_at_qualification": regime.get("trend_regime"),
            "session_at_qualification": regime.get("session"),
            "entry_timing": self.entry_timing,
            "volatility_at_qualification": regime.get("volatility_class"),
            "qualification_ts":     ts,
            "source_setup_ts":      self.opened_ts,
            "time_to_qualify_seconds": time_to_qualify,
            "entry": {
                "price":             round(ep, 4),
                "recommended_entry": round(ep, 4),
                "original_entry":    round(self.entry_price, 4),
            },
            "risk": {
                "sl_price":          round(sl, 4),
                "atr_used":          round(atr, 4),
                "sl_atr_multiplier": 1.5,
            },
            "targets": {
                "tp1": round(tp1, 4), "tp1_rr": 1.0,
                "tp2": round(tp2, 4), "tp2_rr": 2.0,
                "tp3": round(tp3, 4), "tp3_rr": 3.0,
                "primary_target":   round(primary_tgt, 4),
                "target_rationale": tgt_rationale,
            },
            "transition_history":      self.transition_history,
            "qualification_criteria":  qual,
            "brain_decision":          (brain or {}).get("decision", "APPROVED"),
            "brain_soft_confirmations": (brain or {}).get("soft_confirmations", 0),
            "brain_required_min_confirmations": (brain or {}).get(
                "required_soft_confirmations", MIN_BRAIN_SOFT_CONFIRMATIONS
            ),
            "brain_reason":            (brain or {}).get("reason", ""),
            "brain_mode":              (brain or {}).get("brain_mode", BRAIN_MODE),
            "context_at_qualification": context,
            "market_questions": {
                "location":     f"price at {ep:.2f} — {loc_desc}",
                "aggression":   flow_rec.get("direction") or "unknown",
                "absorption":   absrp_rec.get("direction") or "unknown",
                "exhaustion":   "none",
                "trap":         "none",
                "acceptance":   f"price accepted at {loc_desc}",
                "continuation": f"high — qualified after {time_to_qualify}s of observation",
                "invalidation": f"close {mq_dir_word} {sl:.2f}",
                "target":       f"tp1={tp1:.2f} tp2={tp2:.2f} tp3={tp3:.2f}",
                "risk_reward":  f"SL={sl_dist:.2f} TP1=1:1 TP2=1:2 TP3=1:3",
            },
            "status": "open",
        }

        errs = _validate_qualified(qual_rec)
        if errs:
            print(f"[OBS] QUALIFIED VALIDATION ERROR: {errs}", flush=True)
            return

        self.state = "QUALIFIED"
        self.transition_history.append({
            "ts": ts, "event": "QUALIFIED",
            "state_before": "QUALIFYING", "state_after": "QUALIFIED",
            "details": f"all F1-F5 criteria met",
        })
        _write_jsonl(qual_fh, qual_rec)
        _write_jsonl(obs_fh, {
            "engine": "observer_engine",
            "ts": ts,
            "symbol": SYMBOL,
            "source_setup_id": self.setup_id,
            "event_type": "QUALIFIED",
            "state_before": "QUALIFYING",
            "state_after": "QUALIFIED",
            "regime_at_qualification": regime.get("trend_regime"),
            "session_at_qualification": regime.get("session"),
            "entry_timing": self.entry_timing,
            "volatility_at_qualification": regime.get("volatility_class"),
            "details": "all regime and F1-F5 criteria met",
            "brain_decision": (brain or {}).get("decision", "APPROVED"),
            "brain_reason": (brain or {}).get("reason", "all hard gates met and soft confirmations satisfied"),
            "soft_confirmations": (brain or {}).get("soft_confirmations", 0),
            "hard_gates_ok": (brain or {}).get("hard_gates_ok", True),
            "f_gates": {
                "f1_bos": True,
                "f2_volume": True,
                "f3_regime": True,
                "f4_structure": True,
                "f5_confirmation": True,
            },
            "blocking_gate": "",
            "observer_reason": "all regime and F1-F5 criteria met",
            "bos_ok": True,
            "vol_ok": True,
            "regime_ok": True,
        })
        _print_qualified(qual_rec, self.atr_used)


# ── Validation ────────────────────────────────────────────────────────────────
def _validate_qualified(rec: dict) -> list[str]:
    errors: list[str] = []
    d  = rec.get("direction")
    ep = _sf((rec.get("entry") or {}).get("price"), 0.0)
    sl = _sf((rec.get("risk")    or {}).get("sl_price"), 0.0)
    t1 = _sf((rec.get("targets") or {}).get("tp1"), 0.0)
    t2 = _sf((rec.get("targets") or {}).get("tp2"), 0.0)
    t3 = _sf((rec.get("targets") or {}).get("tp3"), 0.0)

    if d not in ("long", "short"):
        errors.append(f"[1] invalid direction: {d}")
    if d == "long":
        if not (sl < ep < t1 < t2 < t3):
            errors.append(f"[2] long price order violation: sl={sl} ep={ep} t1={t1} t2={t2} t3={t3}")
    elif d == "short":
        if not (sl > ep > t1 > t2 > t3):
            errors.append(f"[3] short price order violation: sl={sl} ep={ep} t1={t1} t2={t2} t3={t3}")

    ttq = rec.get("time_to_qualify_seconds")
    if not (isinstance(ttq, int) and ttq >= 0):
        errors.append(f"[4] invalid time_to_qualify_seconds: {ttq}")

    qual = rec.get("qualification_criteria") or {}
    if len(qual) != 8 or not all(isinstance(v, bool) for v in qual.values()):
        errors.append(f"[5] qualification_criteria must have 8 bool values")

    th = rec.get("transition_history") or []
    for i, entry in enumerate(th):
        sb = entry.get("state_before")
        sa = entry.get("state_after")
        # Allow state==state transitions for observation notes
        if sb is None or sa is None:
            errors.append(f"[6] transition_history[{i}] missing state fields")

    if rec.get("status") != "open":
        errors.append(f"[7] status must be 'open'")

    return errors


# ── Terminal output ───────────────────────────────────────────────────────────
def _print_obs_transition(setup_id: str, ts: int, event: str,
                           old_state: str, new_state: str,
                           cur_price: float, details: str, direction: str) -> None:
    if FULL_PRINT:
        return  # full JSON printed by caller
    t = ts // 1000

    if new_state == "DEVELOPING":
        print(f"[OBS DEVELOPING] setup_id={setup_id} ts={t} {event}\n"
              f"  price={cur_price:.2f} details={details}", flush=True)

    elif new_state == "QUALIFYING":
        print(f"[OBS QUALIFYING] setup_id={setup_id} ts={t} {event}\n"
              f"  stage complete, awaiting final criteria", flush=True)

    elif new_state == "INVALIDATED":
        print(f"[INVALIDATED] setup_id={setup_id} ts={t} {direction.upper()}\n"
              f"  reason={details}", flush=True)

    elif new_state == "EXPIRED":
        print(f"[EXPIRED] setup_id={setup_id} ts={t} {direction.upper()}\n"
              f"  reason={details}", flush=True)


def _print_qualified(rec: dict, atr: float) -> None:
    sid   = rec.get("qualified_setup_id", "?")
    ts    = (rec.get("qualification_ts") or 0) // 1000
    d     = rec.get("direction", "?").upper()
    ep    = (rec.get("entry") or {}).get("price", 0.0)
    sl    = (rec.get("risk")  or {}).get("sl_price", 0.0)
    tgts  = rec.get("targets") or {}
    tp1   = tgts.get("tp1", 0.0)
    tp2   = tgts.get("tp2", 0.0)
    tp3   = tgts.get("tp3", 0.0)
    scen  = (rec.get("context_at_qualification") or {}).get("active_scenario", "none")
    ttq   = rec.get("time_to_qualify_seconds", 0)

    if FULL_PRINT:
        print(json.dumps(rec, ensure_ascii=False), flush=True)
    else:
        print(
            f"[QUALIFIED SETUP] setup_id={sid} ts={ts} {d}\n"
            f"  entry={ep:.2f} sl={sl:.2f} tp1={tp1:.2f} tp2={tp2:.2f} tp3={tp3:.2f}\n"
            f"  scenario={scen} time_to_qualify={ttq}s",
            flush=True,
        )


# ── Tracker (manages all active state machines) ───────────────────────────────
class SetupTracker:
    """Manages all active ObservedSetup state machines."""

    def __init__(self):
        self.active: dict[str, ObservedSetup] = {}   # setup_id → ObservedSetup
        self.seen_ids: set[str] = set()              # already tracked

    def admit(self, setup: dict, ts: int, obs_fh) -> None:
        """Add a new setup to tracking (if not already tracked)."""
        sid = setup.get("setup_id", "")
        if not sid or sid in self.seen_ids:
            return
        # quality_block: Observer tüm setup'ları gözlemler
        # Kalite filtresi paper_trade_engine'de (_risk_gate) uygulanır
        qb   = (setup.get("score_breakdown") or {}).get("quality_block")
        tier = setup.get("quality_tier", "L1_LOW")
        if qb:
            print(f"[OBS] NOTE quality_block {tier} {sid}: {qb} (gözleniyor)", flush=True)
        self.seen_ids.add(sid)

        # Enforce max concurrent setups
        if len(self.active) >= MAX_OPEN_SETUPS:
            # Expire the oldest
            oldest_id = min(self.active, key=lambda k: self.active[k].opened_ts)
            old = self.active.pop(oldest_id)
            old._transition(ts, "EXPIRED", "MAX_SETUPS_EXCEEDED",
                            "Max concurrent setups reached", 0.0, obs_fh)

        self.active[sid] = ObservedSetup(setup, ts)
        if str(setup.get("setup_type", "")).startswith("liq_"):
            print(
                f"[OBS LIQ SETUP] tracking {setup.get('setup_type')} "
                f"setup_id={sid} timeout_ms={self.active.get(sid).waiting_timeout_ms}",
                flush=True,
            )

    def process_bar(self, ts: int, primary: dict, s1s: dict | None,
                    s1m: dict | None, vp1m: dict | None, gate: dict | None,
                    scenario: dict | None, det: dict, bias: dict | None,
                    baseline_1s: dict | None, regime: dict | None,
                    obs_fh, qual_fh) -> None:
        """Process one primary bar through all active state machines."""
        # Warn if state machines exceed safe threshold
        if len(self.active) > 8:
            print(f"[OBS WARNING] State machines: {len(self.active)} (max {MAX_OPEN_SETUPS})", flush=True)

        done: list[str] = []
        for sid, sm in list(self.active.items()):
            still_active = sm.process_bar(
                ts, primary, s1s, s1m, vp1m, gate, scenario,
                det, bias, baseline_1s, regime, obs_fh, qual_fh,
            )
            if not still_active:
                done.append(sid)
        for sid in done:
            self.active.pop(sid, None)


# ── Batch mode ────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[OBS] Batch mode — loading input files (warm-up limits)", flush=True)

    # Warm-up: load only last N lines per file (memory-efficient)
    primary_recs = _read_last_n_jsonl(PRIMARY_FILE, maxlen=300)
    import time as _time
    _warmup_cutoff = int(_time.time() * 1000) - 120_000
    setup_recs = [
        r for r in _read_last_n_jsonl(SETUPS_FILE, maxlen=100)
        if int(r.get("window_start_ts") or 0) >= _warmup_cutoff
    ]
    liq_setup_recs = [
        r for r in _read_last_n_jsonl(LIQ_SETUPS_FILE, maxlen=50)
        if int(r.get("window_start_ts") or 0) >= _warmup_cutoff
    ]
    s1s_idx      = _build_index(_read_last_n_jsonl(STRUCT_1S_FILE, maxlen=300))
    s1m_idx      = _build_index(_read_last_n_jsonl(STRUCT_1M_FILE, maxlen=100))
    vp1m_idx     = _build_index(_read_last_n_jsonl(VOL_1M_FILE, maxlen=100))
    gate_idx     = _build_index(_read_last_n_jsonl(GATE_FILE, maxlen=100))
    scen_idx     = _build_index(_read_last_n_jsonl(SCENARIOS_FILE, maxlen=100))
    bias_idx     = _build_index(_read_last_n_jsonl(BIAS_FILE, maxlen=100))
    regime_idx   = _build_index(_read_last_n_jsonl(REGIME_FILE, maxlen=100))
    det_idxs     = {d: _build_index(_read_last_n_jsonl(p, maxlen=100))
                    for d, p in DETECTOR_FILES.items()}

    bl_recs      = _read_last_n_jsonl(BASELINE_FILE, maxlen=100)
    baseline_1s  = _load_baseline(bl_recs, "1S")

    # Index setups by window_start_ts for lookup
    setups_by_ts: dict[int, list[dict]] = {}
    for s in setup_recs + liq_setup_recs:
        st = s.get("window_start_ts")
        if st is not None:
            setups_by_ts.setdefault(int(st), []).append(s)

    tracker   = SetupTracker()
    n_primary = 0
    n_obs_pre = 0  # count observations before opening

    with (open(OBSERVATIONS_FILE, "a", encoding="utf-8") as obs_fh,
          open(QUALIFIED_FILE,    "a", encoding="utf-8") as qual_fh):

        for raw in primary_recs:
            if HALT_FILE.exists():
                print("[OBS] SYSTEM_HALT — aborting batch", flush=True)
                return

            cdna = raw.get("candle_dna") or {}
            if not cdna.get("has_trade"):
                continue

            ts = raw.get("window_start_ts")
            if ts is None:
                continue
            ts = int(ts)

            # Admit any setups that arrived at this timestamp
            for s in setups_by_ts.get(ts, []):
                tracker.admit(s, ts, obs_fh)

            s1s  = _latest_at_or_before(s1s_idx, ts)
            s1s_bos = _latest_within_window(s1s_idx, ts, 30_000, "micro_bos")
            if s1s_bos is not None:
                s1s = s1s_bos
            s1m  = _latest_at_or_before(s1m_idx, ts)
            vp1m = _latest_at_or_before(vp1m_idx, ts)
            gate = gate_idx.get(ts)
            scen = _latest_at_or_before(scen_idx, ts)
            bias = _latest_at_or_before(bias_idx, ts)
            regime = _latest_at_or_before(regime_idx, ts)
            det  = {d: det_idxs[d].get(ts) for d in DETECTOR_FILES}

            tracker.process_bar(ts, raw, s1s, s1m, vp1m, gate, scen,
                                det, bias, baseline_1s, regime, obs_fh, qual_fh)
            n_primary += 1

    n_setups = len(setup_recs) + len(liq_setup_recs)
    print(f"[OBS] Batch done: {n_primary} primary bars processed, "
          f"{n_setups} setups from L7, "
          f"{len(tracker.seen_ids)} setups tracked", flush=True)


# ── Live mode ─────────────────────────────────────────────────────────────────
class LiveCtx:
    def __init__(self):
        self.s1s:  dict[int, dict] = {}
        self.s1m:  dict[int, dict] = {}
        self.vp1m: dict[int, dict] = {}
        self.gate: dict[int, dict] = {}
        self.scen: dict[int, dict] = {}
        self.bias: dict[int, dict] = {}
        self.regime: dict[int, dict] = {}
        self.dets: dict[str, dict[int, dict]] = {d: {} for d in DETECTOR_FILES}
        self.baseline_1s: dict | None = None
        self.tracker = SetupTracker()


async def _tail_index(path: Path, cache: dict[int, dict], label: str) -> None:
    while not path.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(path, "r", encoding="utf-8") as f:
        loop = asyncio.get_event_loop()
        backlog = await loop.run_in_executor(None, _read_last_n_jsonl, path, LIVE_CACHE_MAX)
        for rec in backlog:
            ts = rec.get("window_start_ts") or rec.get("ts")
            if ts is not None:
                _cache_put(cache, int(ts), rec)
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
                rec = json.loads(line)
                ts = rec.get("window_start_ts") or rec.get("ts")
                if ts is not None:
                    _cache_put(cache, int(ts), rec)
            except Exception:
                pass


async def _tail_baseline(ctx: LiveCtx) -> None:
    while not BASELINE_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(BASELINE_FILE, "r", encoding="utf-8") as f:
        loop = asyncio.get_event_loop()
        backlog = await loop.run_in_executor(None, _read_last_n_jsonl, BASELINE_FILE, 100)
        for rec in backlog:
            if rec.get("timeframe") == "1S":
                ctx.baseline_1s = rec
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
                rec = json.loads(line)
                if rec.get("timeframe") == "1S":
                    ctx.baseline_1s = rec
            except Exception:
                pass


async def _tail_setups(
    ctx: LiveCtx, obs_fh, setups_file: Path, warmup_maxlen: int = 0,
) -> None:
    while not setups_file.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(setups_file, "r", encoding="utf-8") as f:
        if warmup_maxlen > 0:
            loop = asyncio.get_event_loop()
            warmup = await loop.run_in_executor(
                None, _read_last_n_jsonl, setups_file, warmup_maxlen,
            )
            for setup in warmup:
                ts = setup.get("window_start_ts")
                if ts is not None:
                    ctx.tracker.admit(setup, int(ts), obs_fh)
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
                setup = json.loads(line)
                ts = setup.get("window_start_ts")
                if ts is not None:
                    ctx.tracker.admit(setup, int(ts), obs_fh)
            except Exception:
                pass


async def _primary_task(ctx: LiveCtx) -> None:
    while not PRIMARY_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    loop = asyncio.get_event_loop()
    existing = await loop.run_in_executor(None, _read_last_n_jsonl, PRIMARY_FILE, 100)
    print(f"[OBS] Warm-up: {len(existing)} existing primary records", flush=True)

    with (open(OBSERVATIONS_FILE, "a", encoding="utf-8") as obs_fh,
          open(QUALIFIED_FILE,    "a", encoding="utf-8") as qual_fh,
          open(PRIMARY_FILE,      "r", encoding="utf-8") as pf):

        pf.seek(0, 2)

        while True:
            if HALT_FILE.exists():
                print("[OBS] SYSTEM_HALT — stopping", flush=True)
                return

            line = pf.readline()
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

            cdna = raw.get("candle_dna") or {}
            if not cdna.get("has_trade"):
                continue

            ts = raw.get("window_start_ts")
            if ts is None:
                continue
            ts = int(ts)

            s1s  = ctx.s1s.get(ts)
            s1m  = _latest_at_or_before(ctx.s1m, ts)
            vp1m = _latest_at_or_before(ctx.vp1m, ts)
            gate = ctx.gate.get(ts)
            scen = _latest_at_or_before(ctx.scen, ts)
            bias = _latest_at_or_before(ctx.bias, ts)
            regime = _latest_at_or_before(ctx.regime, ts)
            det  = {d: ctx.dets[d].get(ts) for d in DETECTOR_FILES}

            ctx.tracker.process_bar(ts, raw, s1s, s1m, vp1m, gate, scen,
                                    det, bias, ctx.baseline_1s, regime, obs_fh, qual_fh)


async def run_live() -> None:
    ctx = LiveCtx()

    async def _tail_setup_file(setups_file: Path, warmup_maxlen: int) -> None:
        with open(OBSERVATIONS_FILE, "a", encoding="utf-8") as obs_fh:
            await _tail_setups(
                ctx, obs_fh, setups_file, warmup_maxlen=warmup_maxlen,
            )

    tasks = [
        asyncio.create_task(_primary_task(ctx),                           name="ob-primary"),
        asyncio.create_task(_tail_index(STRUCT_1S_FILE, ctx.s1s,  "s1s"), name="ob-s1s"),
        asyncio.create_task(_tail_index(STRUCT_1M_FILE, ctx.s1m,  "s1m"), name="ob-s1m"),
        asyncio.create_task(_tail_index(VOL_1M_FILE,   ctx.vp1m, "vp1m"), name="ob-vp1m"),
        asyncio.create_task(_tail_index(GATE_FILE,     ctx.gate,  "gate"), name="ob-gate"),
        asyncio.create_task(_tail_index(SCENARIOS_FILE, ctx.scen, "scen"), name="ob-scen"),
        asyncio.create_task(_tail_index(BIAS_FILE,     ctx.bias,  "bias"), name="ob-bias"),
        asyncio.create_task(_tail_index(REGIME_FILE, ctx.regime, "regime"), name="ob-regime"),
        asyncio.create_task(_tail_baseline(ctx),                           name="ob-bl"),
    ]
    for det_name, path in DETECTOR_FILES.items():
        tasks.append(asyncio.create_task(
            _tail_index(path, ctx.dets[det_name], det_name),
            name=f"ob-{det_name}",
        ))

    tasks.append(asyncio.create_task(
        _tail_setup_file(SETUPS_FILE, 0), name="ob-setups",
    ))
    tasks.append(asyncio.create_task(
        _tail_setup_file(LIQ_SETUPS_FILE, 50), name="obs-liq-setups",
    ))
    tasks.append(asyncio.create_task(
        _tail_setup_file(TRADE_BRAIN_SETUPS_FILE, 0), name="ob-trade-brain-setups",
    ))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("[OBS] Tasks cancelled", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Observer Engine — Layer 10")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[OBS] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        return

    if args.mode == "batch":
        run_batch()
    else:
        print("[OBS] Starting live mode", flush=True)
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
