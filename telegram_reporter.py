"""
NurtacCoreEngineClaude — Layer-13: Telegram Reporter (v2)

New design: Only SIGNAL messages + 15min REPORT
- Signals: final_setup PREMIUM/STANDARD + bias >= 60%
- Reports: 15min interval with trade stats
- No noise: gap_detected, structure_event, cooldown_skip, etc.

No Binance API. No real orders. Reads only JSONL files.
Supports graceful fallback to terminal if tokens not set.
"""

import argparse
import asyncio
import datetime
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict, deque
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    env_path = Path(".env")
    if env_path.exists():
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("TELEGRAM_BOT_TOKEN=") and not TELEGRAM_TOKEN:
                    TELEGRAM_TOKEN = line.split("=", 1)[1].strip() or None
                elif line.startswith("TELEGRAM_CHAT_ID=") and not TELEGRAM_CHAT_ID:
                    TELEGRAM_CHAT_ID = line.split("=", 1)[1].strip() or None
        except Exception:
            pass
TELEGRAM_CONFIGURED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage" if TELEGRAM_TOKEN else None
POLL_SLEEP = 0.5

# ── Timezones ──────────────────────────────────────────────────────────────────
UTC4 = datetime.timezone(datetime.timedelta(hours=4))

def get_time_utc4() -> str:
    return datetime.datetime.now(UTC4).strftime("%H:%M")

def get_time_utc4_full() -> str:
    return datetime.datetime.now(UTC4).strftime("%Y-%m-%d %H:%M UTC+4")

# ── Input files ────────────────────────────────────────────────────────────────
FINAL_SETUPS_FILE = DATA_DIR / "final_setups.jsonl"
QUALIFIED_SETUPS_FILE = DATA_DIR / "qualified_setups.jsonl"
TRADE_BRAIN_FILE = DATA_DIR / "trade_brain_setups.jsonl"
OBSERVATIONS_FILE = DATA_DIR / "observations.jsonl"
BIAS_FILE = DATA_DIR / "bias_context.jsonl"
PAPER_TRADES_FILE = DATA_DIR / "paper_trades.jsonl"
PAPER_TRADES_OPEN_FILE = DATA_DIR / "paper_trades_open.json"
EDGE_MATRIX_FILE = DATA_DIR / "edge_matrix.jsonl"
OUTCOME_FILE = DATA_DIR / "historical_outcome_observations.jsonl"
PRIMARY_FILE = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"

# ── Output files ───────────────────────────────────────────────────────────────
LOG_FILE = DATA_DIR / "telegram_log.jsonl"
HEALTH_FILE = DATA_DIR / "telegram_health.json"
SENT_IDS_FILE = DATA_DIR / "telegram_sent_ids.json"
BASELINE_DNA_FILE = DATA_DIR / "historical_baseline_dna.jsonl"
SCENARIOS_FILE = DATA_DIR / "scenarios.jsonl"
TRADE_BRAIN_OUTPUT_FILE = DATA_DIR / "trade_brain_output.jsonl"
HISTORICAL_OUTCOMES_FILE = DATA_DIR / "historical_outcomes.jsonl"
HYPOTHESIS_OUTCOMES_FILE = DATA_DIR / "hypothesis_outcomes.jsonl"
PROBABILITY_SURFACE_FILE = DATA_DIR / "probability_surface.json"
CALIBRATION_PROFILES_FILE = DATA_DIR / "calibration_profiles.json"
EDGE_MATRIX_FILE = DATA_DIR / "edge_matrix.json"
DECISION_GATE_OUTPUT_FILE = DATA_DIR / "decision_gate_output.jsonl"
STRUCTURE_1S_FILE = DATA_DIR / "structure_1s.jsonl"
STRUCTURE_1M_FILE = DATA_DIR / "structure_1m.jsonl"
VOLUME_PROFILE_FILE = DATA_DIR / "volume_profile.json"
ZONE_CONTEXT_FILE = DATA_DIR / "zone_context.json"
LIQUIDATION_CLUSTERS_FILE = DATA_DIR / "liquidation_clusters.jsonl"
BIAS_CONTEXT_FILE = DATA_DIR / "bias_context.jsonl"
REGIME_CONTEXT_FILE = DATA_DIR / "regime_context.jsonl"

# ── Helpers ────────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_last_jsonl(path: Path, maxlen: int = 100) -> list[dict]:
    """Read last N records from JSONL file (memory-efficient)."""
    if not path.exists():
        return []
    records: deque = deque(maxlen=maxlen)
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    return list(records)

def _read_json(path: Path) -> dict | None:
    """Read single JSON file."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def load_json(path: Path) -> dict | None:
    return _read_json(path)

def load_recent_jsonl(path: Path, limit: int = 500) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    return rows[-limit:]

def _read_all_jsonl(path: Path, limit: int | None = None) -> list[dict]:
    return load_recent_jsonl(path, limit or 500)

def _send_telegram(text: str, parse_mode: str = "HTML") -> bool:
    """Send message to Telegram. Return True if successful or no token."""
    if not TELEGRAM_CONFIGURED:
        print(f"[TELEGRAM] NOT CONFIGURED: {text}", flush=True)
        return False

    try:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(TELEGRAM_API, data=data)
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200
    except Exception as e:
        print(f"[TELEGRAM] HATA: {e}", flush=True)
        return False

def _log_message(msg_type: str, content: str, metadata: dict | None = None) -> None:
    """Log sent message to file."""
    try:
        record = {
            "ts": int(time.time() * 1000),
            "type": msg_type,
            "content_length": len(content),
            **(metadata or {}),
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _safe_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)
    except Exception:
        pass

def _load_sent_ids() -> set[str]:
    try:
        data = json.loads(SENT_IDS_FILE.read_text())
        if isinstance(data, list):
            return {str(x) for x in data if x is not None}
        if isinstance(data, dict):
            ids = data.get("sent_ids") or data.get("ids") or []
            if isinstance(ids, list):
                return {str(x) for x in ids if x is not None}
    except Exception:
        bak = SENT_IDS_FILE.with_suffix(f".json.bak.{int(time.time())}")
        try:
            if SENT_IDS_FILE.exists():
                SENT_IDS_FILE.replace(bak)
        except Exception:
            pass
    return set()

def _write_sent_ids(sent_ids: set[str]) -> None:
    _safe_write_json(SENT_IDS_FILE, {"sent_ids": sorted(sent_ids), "updated_at": int(time.time() * 1000)})

def _write_health(**payload) -> None:
    base = {
        "status": "alive",
        "version": "v4",
        "last_blocker": None,
        "last_error": None,
        "last_sent_trade_id": None,
        "last_status": "waiting_new_paper_trade",
        "last_seen_trade_id": None,
        "current_open_count": 0,
        "configured": TELEGRAM_CONFIGURED,
        "context_enrichment": "missing",
        "missing_context_sources": [],
        "last_message_version": "v4",
        "last_quality_score": "partial",
        "warnings": [],
    }
    base.update(payload)
    _safe_write_json(HEALTH_FILE, base)

def _pick_str(*values, default: str = "not_available") -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "null", "nan"}:
            return text
    return default

def _pick_dict(*values) -> dict:
    for value in values:
        if isinstance(value, dict) and value:
            return value
    return {}

def _safe_ts(value) -> int | None:
    try:
        ts = int(float(value))
    except Exception:
        return None
    return ts * 1000 if 0 < ts < 10_000_000_000 else ts

def _record_ts(record: dict) -> int | None:
    for key in ("ts", "open_ts", "entry_ts", "created_at", "recorded_at_ts", "window_end_ts", "window_start_ts", "qualification_ts", "source_setup_ts"):
        ts = _safe_ts(record.get(key))
        if ts:
            return ts
    return None

def find_by_setup_id(records: list[dict], setup_id: str | None, source_setup_id: str | None = None) -> dict | None:
    lookup = {str(x) for x in (setup_id, source_setup_id) if x}
    if not lookup:
        return None
    for rec in reversed(records):
        rec_ids = {
            str(rec.get("setup_id") or ""),
            str(rec.get("source_setup_id") or ""),
            str(rec.get("qualified_setup_id") or ""),
            str(rec.get("trade_id") or ""),
        }
        if rec_ids & lookup:
            return rec
    return None

def _nearest_by_ts(records: list[dict], target_ts: int | None, window_s: int = 120) -> dict | None:
    if not records or not target_ts:
        return None
    best = None
    best_delta = None
    for rec in records:
        ts = _record_ts(rec)
        if not ts:
            continue
        delta = abs(ts - target_ts)
        if delta <= window_s * 1000 and (best_delta is None or delta < best_delta):
            best = rec
            best_delta = delta
    return best

def _trade_ts(trade: dict) -> int | None:
    for key in ("open_ts", "entry_ts", "ts", "created_at", "opened_ts"):
        ts = _safe_ts(trade.get(key))
        if ts:
            return ts
    return None

def enrich_trade_context(trade: dict) -> dict:
    trade_id = str(trade.get("trade_id") or trade.get("id") or "")
    setup_id = str(trade.get("setup_id") or trade.get("source_setup_id") or trade.get("qualified_setup_id") or "")
    source_setup_id = str(trade.get("source_setup_id") or trade.get("setup_id") or "")
    qualified_setup_id = str(trade.get("qualified_setup_id") or "")
    direction = str(trade.get("direction") or trade.get("side") or "").upper() or "unknown"
    entry = _sf(trade.get("entry_price") or trade.get("open_price") or trade.get("entry") or (trade.get("entry") or {}).get("price"))
    sl = _sf(trade.get("sl_price") or trade.get("stop_loss") or trade.get("sl") or (trade.get("risk") or {}).get("sl_price"))
    tp1 = _sf(trade.get("tp1") or trade.get("tp1_price") or (trade.get("targets") or {}).get("tp1"))
    tp2 = _sf(trade.get("tp2") or trade.get("tp2_price") or (trade.get("targets") or {}).get("tp2"))
    tp3 = _sf(trade.get("tp3") or trade.get("tp3_price") or (trade.get("targets") or {}).get("tp3"))

    trade_ts = _trade_ts(trade)
    qualified_records = load_recent_jsonl(QUALIFIED_SETUPS_FILE, 800)
    brain_records = load_recent_jsonl(TRADE_BRAIN_FILE, 1200)
    brain_output_records = load_recent_jsonl(TRADE_BRAIN_OUTPUT_FILE, 2000)
    observation_records = load_recent_jsonl(OBSERVATIONS_FILE, 2000)
    evidence_records = load_recent_jsonl(DATA_DIR / "evidence_stream.jsonl", 2000)
    scenario_records = load_recent_jsonl(SCENARIOS_FILE, 1200)
    decision_gate_records = load_recent_jsonl(DECISION_GATE_OUTPUT_FILE, 1200)
    struct1s_records = load_recent_jsonl(STRUCTURE_1S_FILE, 1200)
    struct1m_records = load_recent_jsonl(STRUCTURE_1M_FILE, 1200)
    liq_records = load_recent_jsonl(LIQUIDATION_CLUSTERS_FILE, 1200)
    bias_records = load_recent_jsonl(BIAS_CONTEXT_FILE, 1200)
    regime_records = load_recent_jsonl(REGIME_CONTEXT_FILE, 400)

    qualified = find_by_setup_id(qualified_records, setup_id, source_setup_id)
    if not qualified:
        qualified = _nearest_by_ts(qualified_records, trade_ts, 120)

    resolved_source_setup_id = str((qualified or {}).get("source_setup_id") or source_setup_id or setup_id or "")
    brain = find_by_setup_id(brain_records, resolved_source_setup_id or setup_id, resolved_source_setup_id)
    if not brain:
        brain = _nearest_by_ts(brain_records, trade_ts, 120)
    brain_out = _nearest_by_ts(brain_output_records, trade_ts, 120) or find_by_setup_id(brain_output_records, resolved_source_setup_id or setup_id, resolved_source_setup_id)
    obs = find_by_setup_id(observation_records, resolved_source_setup_id or setup_id, resolved_source_setup_id)
    if not obs:
        obs = _nearest_by_ts(observation_records, trade_ts, 120)
    evidence = _nearest_by_ts(evidence_records, trade_ts, 120)
    scenario = find_by_setup_id(scenario_records, resolved_source_setup_id or setup_id, resolved_source_setup_id)
    if not scenario:
        scenario = _nearest_by_ts(scenario_records, trade_ts, 120)
    decision_gate = _nearest_by_ts(decision_gate_records, trade_ts, 120)
    struct1s = _nearest_by_ts(struct1s_records, trade_ts, 120)
    struct1m = _nearest_by_ts(struct1m_records, trade_ts, 120)
    liq = _nearest_by_ts(liq_records, trade_ts, 120)
    bias = _nearest_by_ts(bias_records, trade_ts, 120)
    regime = _nearest_by_ts(regime_records, trade_ts, 120)
    volume_profile = load_json(VOLUME_PROFILE_FILE) or {}
    zone_context = load_json(ZONE_CONTEXT_FILE) or {}
    probability_surface = load_json(PROBABILITY_SURFACE_FILE) or {}
    calibration_profiles = load_json(CALIBRATION_PROFILES_FILE) or {}
    edge_matrix = load_json(EDGE_MATRIX_FILE) or {}
    baseline_dna = _nearest_by_ts(load_recent_jsonl(BASELINE_DNA_FILE, 20), trade_ts, 120)

    source_context = _pick_dict(
        trade.get("context_at_open"),
        trade.get("context"),
        trade.get("source_context"),
        (qualified or {}).get("context_at_qualification"),
        (brain or {}).get("context"),
        (brain or {}).get("context_snapshot"),
        (brain_out or {}).get("context"),
        (scenario or {}).get("context_snapshot"),
        (obs or {}).get("source_context"),
    )
    scenario_snapshot = _pick_dict(
        (brain or {}).get("scenario_snapshot"),
        (brain_out or {}).get("context", {}).get("scenario_snapshot"),
        (scenario or {}).get("context_snapshot"),
        source_context.get("scenario_snapshot"),
        trade.get("scenario_snapshot"),
    )

    q9_reason = _pick_str(
        (brain or {}).get("brain_questions", {}).get("Q9_market_intent", {}).get("reason"),
        (brain_out or {}).get("questions", {}).get("Q9_market_intent", {}).get("reason"),
        (brain_out or {}).get("q9_reason"),
        (qualified or {}).get("q9_reason"),
        default="no_q9_context",
    )
    scenario_name = _pick_str(
        scenario_snapshot.get("dominant_scenario"),
        source_context.get("scenario"),
        source_context.get("active_scenario"),
        (qualified or {}).get("scenario"),
        default="no_active_scenario",
    )
    scenario_direction = _pick_str(
        scenario_snapshot.get("dominant_direction"),
        source_context.get("scenario_direction"),
        source_context.get("dom_bias"),
    )
    scenario_status = _pick_str(
        scenario_snapshot.get("status"),
        (scenario or {}).get("status"),
        (brain or {}).get("status"),
        (qualified or {}).get("status"),
    )
    scenario_count = scenario_snapshot.get("scenario_count")
    if scenario_count is None:
        scenario_count = (scenario or {}).get("scenario_count")
    active_scenarios = scenario_snapshot.get("active_scenarios")
    if active_scenarios is None:
        active_scenarios = (scenario or {}).get("active_scenarios")
    active_scenarios = active_scenarios if isinstance(active_scenarios, list) and active_scenarios else []

    observer_state = _pick_str((obs or {}).get("state_after"), (obs or {}).get("state_before"), (obs or {}).get("state"))
    observer_event = _pick_str((obs or {}).get("event_type"))
    observer_reason = _pick_str((obs or {}).get("observer_reason"), (obs or {}).get("details"))

    confidence = (brain or {}).get("confidence")
    if confidence is None:
        confidence = (brain_out or {}).get("confidence")
    if confidence is None:
        confidence = (qualified or {}).get("confidence")
    confidence_text = f"{confidence:.3f}" if isinstance(confidence, (int, float)) else "not_available"

    if entry > 0 and sl > 0 and tp1 > 0:
        dist = abs(entry - sl)
        if dist == 0:
            rr_text = "invalid"
            warning = "entry_equals_sl"
        elif (direction == "LONG" and sl >= entry) or (direction == "SHORT" and sl <= entry):
            rr_text = f"{abs(tp1 - entry) / dist:.2f}"
            warning = "invalid_sl_geometry"
        else:
            rr_text = f"{abs(tp1 - entry) / dist:.2f}"
            warning = "none"
    else:
        rr_text = "invalid"
        warning = "missing_entry_or_sl_or_tp1"

    delta = _pick_str(
        (evidence or {}).get("candle_dna", {}).get("delta"),
        (evidence or {}).get("delta"),
        (decision_gate or {}).get("baseline_context", {}).get("cvd_direction"),
        (bias or {}).get("dominant_bias"),
    )
    trend_1s = _pick_str(source_context.get("trend_1s"), (struct1s or {}).get("trend", {}).get("direction"))
    trend_1m = _pick_str(source_context.get("trend_1m"), (struct1m or {}).get("trend", {}).get("direction"))
    micro_bos = _pick_str(source_context.get("micro_bos"), source_context.get("macro_bos"), (struct1s or {}).get("bos", {}).get("micro_bos"), (struct1m or {}).get("bos", {}).get("micro_bos"))
    gate_grade = _pick_str(source_context.get("gate_grade"), (decision_gate or {}).get("setup_grade"), (qualified or {}).get("quality_tier"))
    price_loc = _pick_str(source_context.get("price_loc"), source_context.get("location"), zone_context.get("price_location"))
    poc_relation = _pick_str(volume_profile.get("price_vs_poc"), "not_available")
    zone = _pick_str(zone_context.get("price_location"), (zone_context.get("active_fvg") or {}).get("type"), (zone_context.get("nearest_supply") or {}).get("strength"), (zone_context.get("nearest_demand") or {}).get("strength"))
    bias_text = _pick_str(source_context.get("market_bias"), source_context.get("dom_bias"), (bias or {}).get("dominant_bias"))
    cascade_risk = _pick_str((liq or {}).get("cascade_risk"), (liquidation := (bias or {}).get("components", {}).get("liquidation", {})).get("cascade_direction"))
    session = _pick_str((qualified or {}).get("session_at_qualification"), source_context.get("session"), (regime or {}).get("session"))
    liquidity = _pick_str((liq or {}).get("cascade_risk"), (liq or {}).get("long_dominant_price"), (liq or {}).get("short_dominant_price"))
    atrit = _pick_str(
        (decision_gate or {}).get("detector_summary", {}).get("absorption", {}).get("label"),
        (decision_gate or {}).get("detector_summary", {}).get("initiative_flow", {}).get("label"),
        (decision_gate or {}).get("detector_summary", {}).get("trapped_trader", {}).get("label"),
    )

    key_evidence = []
    if isinstance(evidence, dict):
        if evidence.get("dominant_side"):
            key_evidence.append(f"dominant_side={evidence.get('dominant_side')}")
        if evidence.get("score_breakdown"):
            key_evidence.append(f"score_breakdown={evidence.get('score_breakdown')}")
    if isinstance(decision_gate, dict) and decision_gate.get("score_breakdown"):
        key_evidence.append(f"gate={decision_gate.get('setup_grade')} / {decision_gate.get('score_breakdown')}")
    if volume_profile:
        key_evidence.append(f"poc={volume_profile.get('poc_price')} {volume_profile.get('price_vs_poc')}")
    if baseline_dna:
        metrics = (baseline_dna.get("metrics") or {}).get("range", {}).get("long", {})
        if metrics:
            key_evidence.append(f"baseline_latest_pctl={metrics.get('latest_percentile')} z={metrics.get('z_score')}")

    def _num(v):
        try:
            f = float(v)
            return f if f == f else None
        except Exception:
            return None

    def _normalize_side(v):
        return str(v or "").strip().lower()

    historical_source = "not_available"
    hist_wr = "not_available"
    hist_n = "not_available"
    hist_wilson = "not_available"
    hist_reliability = "uncalibrated"
    best = None
    for detector, horizons in (probability_surface.get("detectors") or {}).items():
        if isinstance(horizons, dict):
            for horizon, stats in horizons.items():
                if isinstance(stats, dict) and stats.get("n") is not None:
                    n = stats.get("n", 0)
                    if best is None or n > best[0]:
                        best = (n, detector, horizon, stats)
    if best:
        _, detector, horizon, stats = best
        hist_wr = stats.get("wr", "not_available")
        hist_n = stats.get("n", "not_available")
        hist_wilson = stats.get("wilson_lower", "not_available")
        historical_source = f"probability_surface:{detector}/{horizon}"
        hist_reliability = "calibrated" if isinstance(hist_n, int) and hist_n >= 30 else "low_sample"
    elif calibration_profiles.get("by_setup_type"):
        selected = None
        for key, stats in calibration_profiles.get("by_setup_type", {}).items():
            if isinstance(stats, dict) and stats.get("sample_count") is not None:
                n = stats.get("sample_count", 0)
                if selected is None or n > selected[0]:
                    selected = (n, key, stats)
        if selected:
            _, key, stats = selected
            hist_wr = stats.get("win_rate_observed", "not_available")
            hist_n = stats.get("sample_count", "not_available")
            hist_wilson = stats.get("wilson_lower", "not_available")
            historical_source = f"calibration_profiles:{key}"
            hist_reliability = "calibrated" if isinstance(hist_n, int) and hist_n >= 30 else "low_sample"
    elif edge_matrix:
        hist_wr = edge_matrix.get("wr", "not_available")
        hist_n = edge_matrix.get("n", "not_available")
        hist_wilson = edge_matrix.get("wilson_lower", "not_available")
        historical_source = "edge_matrix"
        hist_reliability = "uncalibrated" if not isinstance(hist_n, int) or hist_n < 30 else "calibrated"
    if historical_source == "not_available" and baseline_dna:
        historical_source = "historical_baseline_dna"
        hist_reliability = "uncalibrated"

    brain_conf = _num(confidence)
    brain_decision = _normalize_side((qualified or {}).get("brain_decision") or (brain or {}).get("decision") or (brain_out or {}).get("decision"))
    brain_supported = bool(brain_conf is not None and brain_conf >= 0.55 and brain_decision in {"approved", "long", "short"})

    historical_supported = False
    historical_note = "No reliable historical sample found."
    hist_wr_num = _num(hist_wr)
    hist_n_num = _num(hist_n)
    hist_wilson_num = _num(hist_wilson)
    if hist_wr_num is not None and hist_n_num is not None:
        if hist_wr_num == 0.0:
            historical_note = "Historical edge does not support this setup yet."
        elif hist_n_num < 30:
            historical_note = "Low sample size."
            hist_reliability = "low_sample"
        elif hist_wr_num > 0.50 and hist_reliability == "calibrated" and (hist_wilson_num is None or hist_wilson_num > 0.0):
            historical_supported = True
            historical_note = "Historical edge supports this setup."
        else:
            historical_note = "Historical edge is weak or uncalibrated."
    elif hist_wr == "not_available" or hist_n == "not_available":
        historical_note = "No reliable historical sample found."

    scenario_count_num = _num(scenario_count) or 0.0
    scenario_supported = bool(
        scenario_name not in {"no_active_scenario", "not_available"}
        and scenario_direction not in {"neutral", "not_available", ""}
        and scenario_count_num > 0
    )
    scenario_note = "No active scenario confirmed." if scenario_name == "no_active_scenario" else ("Scenario active." if scenario_supported else "Scenario context is unconfirmed.")

    delta_l = _normalize_side(delta)
    trend_1m_l = _normalize_side(trend_1m)
    gate_grade_l = _normalize_side(gate_grade)
    price_loc_l = _normalize_side(price_loc)
    bias_l = _normalize_side(bias_text)
    cascade_l = _normalize_side(cascade_risk)
    atrit_l = _normalize_side(atrit)
    dominant_side_l = _normalize_side((evidence or {}).get("dominant_side"))
    orderflow_support_count = 0
    if direction.lower() == "short":
        if delta_l.startswith("-") or delta_l in {"negative", "bearish", "short"}:
            orderflow_support_count += 1
        if trend_1m_l in {"downtrend", "bearish"}:
            orderflow_support_count += 1
        if gate_grade_l in {"a", "b", "a+", "b+", "premium", "strong"}:
            orderflow_support_count += 1
        if price_loc_l in {"premium", "above_poc", "supply", "fvg"}:
            orderflow_support_count += 1
        if bias_l in {"short", "bearish", "neutral"}:
            orderflow_support_count += 1
        if cascade_l in {"short", "bearish", "high"}:
            orderflow_support_count += 1
        if atrit_l in {"sell", "bearish", "short", "initiative", "trap"}:
            orderflow_support_count += 1
    elif direction.lower() == "long":
        if delta_l.startswith("+") or delta_l in {"positive", "bullish", "long"}:
            orderflow_support_count += 1
        if trend_1m_l in {"uptrend", "bullish"}:
            orderflow_support_count += 1
        if gate_grade_l in {"a", "b", "a+", "b+", "premium", "strong"}:
            orderflow_support_count += 1
        if price_loc_l in {"discount", "below_poc", "demand", "fvg"}:
            orderflow_support_count += 1
        if bias_l in {"long", "bullish", "neutral"}:
            orderflow_support_count += 1
        if cascade_l in {"long", "bullish", "high"}:
            orderflow_support_count += 1
        if atrit_l in {"buy", "bullish", "long", "initiative", "trap"}:
            orderflow_support_count += 1
    if dominant_side_l in {"short", "bearish"} and direction.lower() == "short":
        orderflow_support_count -= 1
    if dominant_side_l in {"long", "bullish"} and direction.lower() == "long":
        orderflow_support_count -= 1
    orderflow_supported = orderflow_support_count >= 2
    if dominant_side_l and direction.lower() in {"short", "long"} and dominant_side_l not in {direction.lower(), "neutral"}:
        key_evidence.append(f"dominant_side_conflict={dominant_side_l}")

    observer_supported = observer_state == "QUALIFIED"

    probability_ctx = {
        "long_prob": (brain_out or {}).get("long_prob", (brain or {}).get("confidence")) if isinstance((brain_out or {}).get("long_prob", None), (int, float)) or isinstance((brain or {}).get("confidence"), (int, float)) else "not_available",
        "short_prob": (brain_out or {}).get("short_prob") if isinstance((brain_out or {}).get("short_prob", None), (int, float)) else "not_available",
        "source": "not_available",
        "reliability": hist_reliability,
    }
    if probability_ctx["long_prob"] == "not_available" and isinstance(confidence, (int, float)):
        probability_ctx["long_prob"] = confidence
    if probability_ctx["short_prob"] == "not_available" and isinstance(probability_ctx["long_prob"], (int, float)):
        probability_ctx["short_prob"] = round(max(0.0, 1.0 - float(probability_ctx["long_prob"])), 3)
    if historical_source.startswith("probability_surface"):
        probability_ctx["source"] = "probability_surface"
    elif historical_source == "edge_matrix":
        probability_ctx["source"] = "edge_matrix"
    elif historical_source.startswith("calibration_profiles"):
        probability_ctx["source"] = "calibration_profiles"
    elif historical_source == "historical_baseline_dna":
        probability_ctx["source"] = "historical_baseline_dna"

    position_size = _pick_str(
        (trade.get("sim") or {}).get("contracts"),
        (trade.get("sim") or {}).get("position_usd"),
        (trade.get("sim") or {}).get("risk_usd"),
    )
    market_regime = _pick_str(
        (regime or {}).get("trend_regime"),
        (qualified or {}).get("regime_at_qualification"),
        (brain_out or {}).get("context", {}).get("market_regime"),
    )
    risk_ctx = {
        "rr": rr_text,
        "warning": warning,
        "risk_usd": (trade.get("sim") or {}).get("risk_usd", "not_available"),
        "position_size": position_size,
        "leverage": (trade.get("sim") or {}).get("leverage_approx", "not_available"),
        "entry_equals_sl": warning == "entry_equals_sl",
        "invalid_sl_geometry": warning == "invalid_sl_geometry",
    }

    risk_supported = bool(
        _num(rr_text) is not None
        and float(rr_text) >= 1.0
        and warning == "none"
        and not risk_ctx.get("entry_equals_sl")
        and not risk_ctx.get("invalid_sl_geometry")
    )

    support_flags = {
        "brain_supported": brain_supported,
        "observer_supported": observer_supported,
        "risk_supported": risk_supported,
        "orderflow_supported": orderflow_supported,
        "scenario_supported": scenario_supported,
        "historical_supported": historical_supported,
    }
    support_count = sum(1 for v in support_flags.values() if v)
    if support_count >= 5 and (historical_supported or scenario_supported):
        trade_quality_score = "strong"
    elif support_count >= 3:
        trade_quality_score = "moderate"
    else:
        trade_quality_score = "partial"
    honesty_warnings = []
    if not scenario_supported and not historical_supported:
        honesty_warnings.append("uncalibrated_no_scenario_no_historical_edge")
    if not historical_supported:
        honesty_warnings.append("historical_edge_not_supportive")
    if not scenario_supported:
        honesty_warnings.append("scenario_not_active")

    missing = []
    if not qualified:
        missing.append("qualified_setups.jsonl")
    if not brain:
        missing.append("trade_brain_setups.jsonl")
    if not brain_out:
        missing.append("trade_brain_output.jsonl")
    if not obs:
        missing.append("observations.jsonl")
    if not evidence:
        missing.append("evidence_stream.jsonl")
    if not scenario:
        missing.append("scenarios.jsonl")
    if historical_source == "not_available":
        missing.extend(["probability_surface.json", "edge_matrix.json", "calibration_profiles.json"])
    if not decision_gate:
        missing.append("decision_gate_output.jsonl")
    if not struct1s:
        missing.append("structure_1s.jsonl")
    if not struct1m:
        missing.append("structure_1m.jsonl")
    if not volume_profile:
        missing.append("volume_profile.json")
    if not zone_context:
        missing.append("zone_context.json")
    if not liq:
        missing.append("liquidation_clusters.jsonl")
    if not bias:
        missing.append("bias_context.jsonl")
    if not regime:
        missing.append("regime_context.jsonl")

    context_source = "enriched" if not missing else ("partial" if len(missing) < 6 else "missing")

    return {
        "trade_id": trade_id or "not_available",
        "setup_id": setup_id or "not_available",
        "source_setup_id": source_setup_id or "not_available",
        "qualified_setup_id": qualified_setup_id or "not_available",
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr_text": rr_text,
        "warning": warning,
        "confidence": confidence_text,
        "decision": (qualified or {}).get("brain_decision") or (brain or {}).get("decision") or (brain_out or {}).get("decision") or "unknown",
        "q9_reason": q9_reason,
        "scenario": scenario_name,
        "scenario_direction": scenario_direction,
        "scenario_status": scenario_status,
        "scenario_context": {"dominant": scenario_name, "direction": scenario_direction, "status": scenario_status, "score": scenario_snapshot.get("dominant_score") or scenario_snapshot.get("score") or "not_available", "active_count": scenario_count if scenario_count is not None else "not_available", "active_scenarios": active_scenarios, "q9": q9_reason},
        "order_flow": {"delta": delta, "trend_1s": trend_1s, "trend_1m": trend_1m, "micro_bos": micro_bos, "gate_grade": gate_grade, "price_loc": price_loc, "poc_relation": poc_relation, "zone": zone, "dom_bias": bias_text, "cascade": cascade_risk, "session": session, "liquidity": liquidity, "atrit": atrit},
        "observer": {"state": observer_state, "event": observer_event, "reason": observer_reason, "f_gates": (obs or {}).get("f_gates") or (decision_gate or {}).get("detector_summary") or {}, "age": int((time.time() * 1000 - trade_ts) / 1000) if trade_ts else "not_available"},
        "historical_edge": {"wr": hist_wr, "n": hist_n, "wilson_lower": hist_wilson, "source": historical_source, "reliability": hist_reliability},
        "probability": probability_ctx,
        "calibration": {
            "source": historical_source if historical_source != "not_available" else "uncalibrated",
            "status": "measured" if historical_source.startswith(("probability_surface", "calibration_profiles")) else "uncalibrated",
            "sample_count": hist_n,
        },
        "risk": risk_ctx,
        "market_regime": market_regime,
        "position_size": position_size,
        "trade_quality_score": trade_quality_score,
        "quality_points": support_count,
        "support_flags": support_flags,
        "support_count": support_count,
        "key_evidence": key_evidence,
        "context_source": context_source,
        "missing_context_sources": missing,
        "warnings": ([warning] if warning != "none" else []) + honesty_warnings,
        "historical_supported": historical_supported,
        "scenario_supported": scenario_supported,
        "orderflow_supported": orderflow_supported,
        "observer_supported": observer_supported,
        "risk_supported": risk_supported,
        "brain_supported": brain_supported,
        "historical_note": historical_note,
        "scenario_note": scenario_note,
    }

def format_v3_message(trade: dict, ctx: dict) -> str:
    def _historical_support_label(h: dict, supported: bool) -> str:
        wr = h.get("wr")
        n = h.get("n")
        try:
            n_num = float(n)
        except Exception:
            n_num = None
        if supported:
            return "YES"
        if wr == "not_available" or n == "not_available":
            return "UNKNOWN"
        if n_num is not None and n_num < 30:
            return "WEAK"
        return "NO"

    warning_text = ", ".join(ctx.get("warnings") or []) if ctx.get("warnings") else "none"
    if ctx["rr_text"] == "invalid" and warning_text == "none":
        warning_text = "missing_rr_inputs"
    trade_ts = _trade_ts(trade)
    time_text = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(trade_ts / 1000)) if trade_ts else "not_available"
    scenario = ctx.get("scenario_context") or {}
    observer = ctx.get("observer") or {}
    historical = ctx.get("historical_edge") or {}
    probability = ctx.get("probability") or {}
    calibration = ctx.get("calibration") or {}
    risk = ctx.get("risk") or {}
    ai_expl = ctx.get("key_evidence") or []
    quality_context = "uncalibrated" if ctx.get("trade_quality_score") == "partial" else ctx.get("confidence")
    historical_supported = bool(ctx.get("historical_supported"))
    scenario_supported = bool(ctx.get("scenario_supported"))
    orderflow_supported = bool(ctx.get("orderflow_supported"))
    observer_supported = bool(ctx.get("observer_supported"))
    risk_supported = bool(ctx.get("risk_supported"))
    brain_supported = bool(ctx.get("brain_supported"))

    historical_note = ctx.get("historical_note") or "No reliable historical sample found."
    scenario_note = ctx.get("scenario_note") or "Scenario context is unconfirmed."
    result_lines = []
    if observer_supported:
        result_lines.append("Observer QUALIFIED.")
    if risk_supported:
        result_lines.append("Risk valid.")
    if orderflow_supported:
        result_lines.append("Order flow is partly aligned.")
    if scenario_supported:
        result_lines.append("Scenario is active.")
    else:
        result_lines.append("Scenario desteği yok.")
    if not historical_supported:
        result_lines.append("Historical edge this setup is not yet supporting / uncalibrated.")
    else:
        result_lines.append("Historical edge supports this setup.")
    if not any([brain_supported, observer_supported, risk_supported, orderflow_supported, scenario_supported, historical_supported]):
        result_lines.append("This trade was opened by the technical pipeline; edge is limited/uncalibrated.")
    elif not historical_supported and not scenario_supported:
        result_lines.append("This trade was opened by the technical pipeline; edge is limited/uncalibrated.")
    return (
        "🧠 NurtacCoreEngine — Trade Brain Report V4\n\n"
        "📌 Trade\n"
        f"Symbol: {SYMBOL}\n"
        f"Direction: {ctx['direction']}\n"
        f"Entry: {ctx['entry']:.2f}\n"
        f"SL: {ctx['sl']:.2f}\n"
        f"TP1: {ctx['tp1']:.2f}\n"
        f"TP2: {ctx['tp2']:.2f}\n"
        f"TP3: {ctx['tp3']:.2f}\n"
        f"RR: {ctx['rr_text']}\n"
        f"Setup ID: {ctx['qualified_setup_id'] if ctx['qualified_setup_id'] != 'not_available' else ctx['setup_id']}\n"
        f"Trade ID: {ctx['trade_id']}\n"
        f"Time UTC: {time_text}\n\n"
        "🧠 Trade Brain\n"
        f"Decision: {ctx['decision']}\n"
        f"Confidence: {ctx['confidence']}\n"
        f"Q9: {ctx['q9_reason']}\n"
        "Explanation:\n"
        f"- {ctx['decision']}\n"
        f"- {ctx['q9_reason']}\n"
        f"- {ctx['observer']['state']}\n\n"
        "📊 Order Flow\n"
        f"Delta: {ctx['order_flow']['delta']}\n"
        f"Trend 1S: {ctx['order_flow']['trend_1s']}\n"
        f"Trend 1M: {ctx['order_flow']['trend_1m']}\n"
        f"Micro BOS: {ctx['order_flow']['micro_bos']}\n"
        f"Gate: {ctx['order_flow']['gate_grade']}\n"
        f"Price Location: {ctx['order_flow']['price_loc']}\n"
        f"POC: {ctx['order_flow']['poc_relation']}\n"
        f"Zone: {ctx['order_flow']['zone']}\n"
        f"Bias: {ctx['order_flow']['dom_bias']}\n"
        f"Cascade Risk: {ctx['order_flow']['cascade']}\n"
        f"Session: {ctx['order_flow']['session']}\n"
        f"Liquidity: {ctx['order_flow']['liquidity']}\n"
        f"Absorption/Initiative/Trap: {ctx['order_flow']['atrit']}\n"
        "Key Evidence:\n"
        + "\n".join([f"- {x}" for x in (ctx.get("key_evidence") or [])[:3]]) + "\n\n"
        "🎯 Scenario\n"
        f"Dominant: {scenario.get('dominant')}\n"
        f"Direction: {scenario.get('direction')}\n"
        f"Status: {scenario.get('status')}\n"
        f"Score: {scenario.get('score')}\n"
        f"Active Count: {scenario.get('active_count')}\n"
        f"Active Scenarios: {scenario.get('active_scenarios') if scenario.get('active_scenarios') else 'no_active_scenario'}\n"
        f"Q9: {scenario.get('q9')}\n\n"
        "📈 Historical Edge\n"
        f"WR: {historical.get('wr')}\n"
        f"N: {historical.get('n')}\n"
        f"Wilson LB: {historical.get('wilson_lower')}\n"
        f"Source: {historical.get('source')}\n"
        f"Reliability: {historical.get('reliability')}\n\n"
        f"Support: {_historical_support_label(historical, historical_supported)}\n"
        f"Note: {historical_note}\n\n"
        "📊 Probability\n"
        f"Long Prob: {probability.get('long_prob')}\n"
        f"Short Prob: {probability.get('short_prob')}\n"
        f"Source: {probability.get('source')}\n"
        f"Reliability: {probability.get('reliability')}\n\n"
        "🧾 Calibration\n"
        f"Source: {calibration.get('source')}\n"
        f"Status: {calibration.get('status')}\n"
        f"Sample Count: {calibration.get('sample_count')}\n\n"
        "🛡 Risk\n"
        f"RR: {risk.get('rr')}\n"
        f"Risk USD: {risk.get('risk_usd')}\n"
        f"Position Size: {ctx.get('position_size')}\n"
        f"Leverage: {risk.get('leverage')}\n"
        f"Warning: {risk.get('warning')}\n\n"
        "👁 Observer\n"
        f"State: {observer.get('state')}\n"
        f"Event: {observer.get('event')}\n"
        f"F Gates: {observer.get('f_gates')}\n"
        f"Age: {observer.get('age')}\n\n"
        "🧠 AI Explanation\n"
        + "\n".join([f"- {x}" for x in ai_expl[:3]]) + "\n\n"
        "⭐ Trade Quality\n"
        f"Score: {ctx.get('trade_quality_score')}\n"
        f"Confidence: {quality_context if isinstance(quality_context, str) else ctx['confidence']}\n"
        f"Context: {ctx.get('context_source')}\n"
        f"Warnings: {warning_text}\n"
        f"Missing Sources: {ctx.get('missing_context_sources')}\n\n"
        "🏷 Market Regime\n"
        f"Regime: {ctx.get('market_regime')}\n"
        f"Session: {ctx.get('order_flow', {}).get('session')}\n\n"
        "✅ Sonuç\n"
        + "\n".join(result_lines)
    )

def _join_context(trade: dict) -> dict:
    return enrich_trade_context(trade)

def _paper_open_to_message(trade: dict) -> str | None:
    ctx = _join_context(trade)
    if ctx["direction"] not in ("LONG", "SHORT", "unknown"):
        return None
    if ctx["entry"] <= 0 or ctx["sl"] <= 0 or ctx["tp1"] <= 0:
        return None
    return format_v3_message(trade, ctx)

# ── Signal Message ─────────────────────────────────────────────────────────────
def format_setup_message(setup: dict) -> str:
    """
    Qualified setup'tan Telegram mesajı oluştur.
    INPUT:  qualified_setups.jsonl kaydı (dict)
    OUTPUT: Telegram mesaj string
    YASAK:  trade açmaz, dosya yazmaz
    """
    direction  = str(setup.get("direction", "?")).upper()
    tier       = setup.get("quality_tier", "?")
    score      = setup.get("direction_score", 0)
    entry_d    = setup.get("entry") or {}
    sl_d       = setup.get("sl")    or {}
    tp1_d      = setup.get("tp1")   or {}
    tp2_d      = setup.get("tp2")   or {}
    tp3_d      = setup.get("tp3")   or {}
    entry = float(entry_d.get("price") or 0)
    sl    = float(sl_d.get("price")    or 0)
    tp1   = float(tp1_d.get("price")   or 0)
    tp2   = float(tp2_d.get("price")   or 0)
    tp3   = float(tp3_d.get("price")   or 0)
    sl_pct  = abs(sl - entry) / entry * 100 if entry > 0 else 0
    sl_sign = "+" if sl > entry else "-"
    regime_ctx = setup.get("regime_context") or {}
    regime     = regime_ctx.get("trend_regime", "?")
    session    = regime_ctx.get("session", "?")
    macro_ctx  = setup.get("macro_context") or {}
    move_type  = macro_ctx.get("move_type", "?")
    sm_bias    = macro_ctx.get("smart_money_bias", "?")
    etf_sig    = macro_ctx.get("etf_signal", "?")
    cb_sig     = macro_ctx.get("coinbase_signal", "?")
    tt_div     = macro_ctx.get("divergence_signal", "?")
    mp_price   = macro_ctx.get("max_pain_price", "?")
    mp_bias    = macro_ctx.get("max_pain_bias", "?")
    bd         = setup.get("score_breakdown") or {}
    cal_boost  = bd.get("calibration", 0)
    qblock     = bd.get("quality_block", "")
    sim        = setup.get("sim") or {}
    risk_usd   = sim.get("risk_usd", "?")
    dir_emoji  = "📈" if direction == "LONG" else "📉"
    return (
        f"{dir_emoji} *SETUP: {direction} {tier}*\n"
        f"{'─'*25}\n"
        f"💰 Entry: `${entry:,.2f}`\n"
        f"🛡 SL: `${sl:,.2f}` ({sl_sign}{sl_pct:.2f}%)\n"
        f"🎯 TP1: `${tp1:,.0f}` | TP2: `${tp2:,.0f}` | TP3: `${tp3:,.0f}`\n"
        f"{'─'*25}\n"
        f"⚡ Score: `{score}` | Risk: `${risk_usd}`\n"
        f"📍 Rejim: `{regime}` | Session: `{session}`\n"
        f"🌊 Macro: `{move_type}` | SM: `{sm_bias}`\n"
        f"🐋 Top Trader: `{tt_div}` | ETF: `{etf_sig}`\n"
        f"💸 Coinbase: `{cb_sig}` | MaxPain: `${mp_price}` ({mp_bias})\n"
        f"{'─'*25}\n"
        f"{'⚠️ Block: ' + qblock if qblock else '✅ Kalite: OK'}\n"
        f"📊 Cal boost: `{cal_boost}`"
    )

# ── 15 Minute Report ───────────────────────────────────────────────────────────
def format_15min_report(trades: list[dict], current_price: float | None = None) -> str:
    """Format 15-minute periodic report."""
    if not trades:
        return ""

    # Count trades in last 15 minutes
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (15 * 60 * 1000)

    recent = [t for t in trades if _sf(t.get("entry_ts", 0)) > cutoff_ms]
    open_trades = [t for t in recent if not t.get("closed")]
    closed_trades = [t for t in recent if t.get("closed")]

    long_count = sum(1 for t in recent if t.get("direction", "").upper() == "LONG")
    short_count = sum(1 for t in recent if t.get("direction", "").upper() == "SHORT")

    tp_hits = sum(1 for t in closed_trades if t.get("close_reason", "").startswith("TP"))
    sl_hits = sum(1 for t in closed_trades if t.get("close_reason") == "SL")

    # Win rate for period
    period_wr = 0
    if closed_trades:
        wins = sum(1 for t in closed_trades if _sf(t.get("pnl_r", 0)) > 0)
        period_wr = int((wins / len(closed_trades)) * 100)

    # Collect all R values
    all_rs = [_sf(t.get("pnl_r", 0)) for t in closed_trades if "pnl_r" in t]
    avg_r = sum(all_rs) / len(all_rs) if all_rs else 0

    # Overall stats (all trades)
    total_trades = len([t for t in trades if "pnl_r" in t])
    total_wr = 0
    if total_trades > 0:
        wins = sum(1 for t in trades if _sf(t.get("pnl_r", 0)) > 0)
        total_wr = int((wins / total_trades) * 100)

    time_str = get_time_utc4_full()
    price_str = f"${current_price:.2f}" if current_price else "N/A"

    msg = f"""📋 15 DK RAPOR
━━━━━━━━━━━━━━━━━━━
⏰ {time_str}
💰 BTC: {price_str}

🔄 Bu periyotta:
  Açılan: {len(recent)} trade ({long_count}L / {short_count}S)
  TP: {tp_hits} | SL: {sl_hits} | Açık: {len(open_trades)}
  WR: %{period_wr}

📊 Genel istatistik:
  Toplam: {total_trades} trade
  WR: %{total_wr}
  Avg R: {avg_r:.2f}R
"""
    return msg

# ── Paper Trade Close Message ──────────────────────────────────────────────────
def format_close_message(trade: dict) -> str | None:
    """Format message when trade closes (TP/SL hit)."""
    direction = trade.get("direction", "").upper()
    if direction not in ["LONG", "SHORT"]:
        return None

    reason = trade.get("close_reason", "CLOSED")
    entry = _sf(trade.get("entry_price", 0))
    close = _sf(trade.get("close_price", 0))
    pnl_r = _sf(trade.get("pnl_r", 0))

    entry_ts = trade.get("entry_ts")
    close_ts = trade.get("close_ts")
    duration_min = 0
    if entry_ts and close_ts:
        duration_min = int((close_ts - entry_ts) / 1000 / 60)

    emoji = "✅" if pnl_r > 0 else "❌"
    msg = f"""{emoji} {reason} — {direction} {SYMBOL}
━━━━━━━━━━━━━━━━━━━
⏰ {get_time_utc4()} | Süre: {duration_min}dk
📍 Entry: ${entry:.2f} → Close: ${close:.2f}
💰 PnL: {pnl_r:+.2f}R
"""
    return msg

# ── Main Loop ──────────────────────────────────────────────────────────────────
async def run_live() -> None:
    """Live mode: watch for signals and send Telegram messages."""
    print("[TELEGRAM] Live mode — waiting for signals", flush=True)

    sent_signal_ids = set()
    sent_trade_ids = _load_sent_ids()
    last_15min_report = time.time()
    last_seen_trade_id = None

    while not HALT_FILE.exists():
        try:
            open_state = _read_json(PAPER_TRADES_OPEN_FILE) or {}
            open_trades = open_state.get("trades") if isinstance(open_state, dict) else []
            if not isinstance(open_trades, list):
                open_trades = []
            current_open = len(open_trades)
            latest_open = open_trades[-1] if open_trades else None
            if latest_open:
                trade_id = str(latest_open.get("trade_id") or latest_open.get("id") or "")
                last_seen_trade_id = trade_id or last_seen_trade_id
                if trade_id and trade_id not in sent_trade_ids:
                    msg = _paper_open_to_message(latest_open)
                    if msg:
                        ok = _send_telegram(msg)
                        if ok:
                            sent_trade_ids.add(trade_id)
                            _write_sent_ids(sent_trade_ids)
                            _log_message("paper_open", msg, {"trade_id": trade_id, "setup_id": latest_open.get("source_setup_id") or latest_open.get("qualified_setup_id")})
                            ctx = enrich_trade_context(latest_open)
                            _write_health(
                                version="v4",
                                last_message_version="v4",
                                last_status="sent",
                                last_sent_trade_id=trade_id,
                                last_seen_trade_id=trade_id,
                                current_open_count=current_open,
                                context_enrichment=ctx.get("context_source", "missing"),
                                missing_context_sources=ctx.get("missing_context_sources", []),
                                last_quality_score=ctx.get("trade_quality_score", "partial"),
                                warnings=ctx.get("warnings", []),
                                honesty_version="v4_truth_audit",
                                support_flags=ctx.get("support_flags", {}),
                                support_count=ctx.get("support_count", 0),
                                message_honesty="honest",
                            )
                            print(f"[TELEGRAM] Paper open sent: {trade_id}", flush=True)
                        else:
                            _write_health(version="v4", last_message_version="v4", last_status="telegram_api_error", last_blocker="telegram_api_error", last_error="sendMessage failed", last_seen_trade_id=trade_id, current_open_count=current_open, honesty_version="v4_truth_audit", message_honesty="honest")
                    else:
                        _write_health(version="v4", last_message_version="v4", last_status="paper_schema_mismatch", last_blocker="paper_schema_mismatch", last_error="missing entry/sl/tp1", last_seen_trade_id=trade_id, current_open_count=current_open, honesty_version="v4_truth_audit", message_honesty="honest")
            else:
                _write_health(version="v4", last_message_version="v4", last_status="waiting_new_paper_trade", last_blocker="waiting_new_paper_trade", last_seen_trade_id=last_seen_trade_id, current_open_count=0, honesty_version="v4_truth_audit", message_honesty="honest")

            # Check for trade closures
            trades = _read_last_jsonl(PAPER_TRADES_FILE, maxlen=100)
            for trade in trades:
                trade_id = trade.get("trade_id", "")
                if trade_id and trade.get("closed") and trade_id not in sent_trade_ids:
                    msg = format_close_message(trade)
                    if msg:
                        if _send_telegram(msg):
                            sent_trade_ids.add(trade_id)
                            _log_message("close", msg, {"trade_id": trade_id})
                            print(f"[TELEGRAM] Close sent: {trade_id}", flush=True)

            # 15-minute report
            now = time.time()
            if now - last_15min_report >= 15 * 60:
                # Get current price
                primary_records = _read_last_jsonl(PRIMARY_FILE, maxlen=1)
                current_price = None
                if primary_records:
                    cdna = primary_records[-1].get("candle_dna", {})
                    close_price = cdna.get("close", {})
                    if isinstance(close_price, dict):
                        current_price = _sf(close_price.get("price"))
                    else:
                        current_price = _sf(close_price)

                msg = format_15min_report(trades, current_price)
                if msg and _send_telegram(msg):
                    _log_message("report_15min", msg, {})
                    print("[TELEGRAM] 15min report sent", flush=True)

                last_15min_report = now

        except asyncio.CancelledError:
            raise
        except Exception as e:
            _write_health(version="v4", last_message_version="v4", last_status="telegram_reporter_error", last_blocker="reporter_not_started", last_error=str(e), honesty_version="v4_truth_audit", message_honesty="honest")
            print(f"[TELEGRAM] Error in live loop: {e}", flush=True)

        await asyncio.sleep(POLL_SLEEP)

# ── Entry point ────────────────────────────────────────────────────────────────
async def main() -> None:
    """Async entry point."""
    if HALT_FILE.exists():
        print("[TELEGRAM] SYSTEM_HALT exists — exiting", flush=True)
        return

    print("[TELEGRAM] === NurtacCoreEngineClaude Telegram Reporter (v2) ===", flush=True)
    print(f"[TELEGRAM] Token configured: {TELEGRAM_CONFIGURED}", flush=True)
    print(f"[TELEGRAM] Starting live mode...", flush=True)
    if not TELEGRAM_CONFIGURED:
        _write_health(version="v4", last_message_version="v4", last_status="telegram_config_missing", last_blocker="telegram_config_missing", configured=False, honesty_version="v4_truth_audit", message_honesty="honest")
    else:
        _write_health(version="v4", last_message_version="v4", last_status="waiting_new_paper_trade", configured=True, honesty_version="v4_truth_audit", message_honesty="honest")

    await run_live()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram Reporter — Layer 13")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    if args.mode == "live":
        asyncio.run(main())


def format_analyst_report() -> str:
    """
    Günlük analist raporu oluştur.
    READ: data/multitf_outlook.json, data/probability_surface.json,
          data/macro_context.json, data/max_pain.json, data/bias_context.jsonl
    WRITE: Telegram mesaj string (döndürür, göndermez)
    """
    import subprocess, json, time
    from pathlib import Path

    DATA = Path("data")

    def _read(fname):
        try:
            return json.loads((DATA / fname).read_text())
        except Exception:
            return {}

    def _tail1(fname):
        try:
            r = subprocess.getoutput(f"tail -1 {DATA/fname} 2>/dev/null")
            return json.loads(r) if r.strip() else {}
        except Exception:
            return {}

    outlook = _read("multitf_outlook.json")
    ps = _read("probability_surface.json")
    mc = _read("macro_context.json")
    mp = _read("max_pain.json")
    bias = _tail1("bias_context.jsonl")

    price = float(outlook.get("current_price") or bias.get("current_price") or 0)
    now = time.strftime("%d %B %H:%M UTC", time.gmtime())
    regime = outlook.get("regime", "?")
    session = outlook.get("session", "?")
    outlooks = outlook.get("outlooks", {})
    levels = outlook.get("key_levels", {})

    def _ol(h: str) -> str:
        o = outlooks.get(h, {})
        b = o.get("bias", "?")
        conf = o.get("confidence", 0)
        agrees = ", ".join(o.get("signals_agree", [])[:2])
        em = "📉" if b == "bearish" else "📈" if b == "bullish" else "↔️"
        line = f"{em} {h} → {b.capitalize()} (%{conf*100:.0f})"
        if agrees:
            line += f" — {agrees}"
        return line

    best = (ps.get("best_combinations") or [{}])[0]
    best_str = (
        f"{best.get('detector','?')} → {best.get('horizon','?')} "
        f"WR: %{best.get('wr',0)*100:.0f} "
        f"(N={best.get('n',0)}, Wilson: {best.get('wilson_lower',0):.2f})"
    ) if best else "Veri yetersiz"

    scalp_ok = ", ".join(ps.get("scalp_recommended", [])) or "Belirsiz"
    swing_no = ", ".join(ps.get("swing_not_recommended", [])) or "Yok"
    liq_long = [f"${p:,.0f}" for p in (levels.get("liq_long_clusters") or [])[:3]]
    liq_short = [f"${p:,.0f}" for p in (levels.get("liq_short_clusters") or [])[:3]]
    mp_price = levels.get("max_pain") or mp.get("max_pain_price", "?")
    move_type = mc.get("move_type", "?")
    reliability = mc.get("signal_reliability", "?")

    return (
        f"🧠 NURTAC ANALİST RAPORU\n"
        f"{'━'*24}\n"
        f"📅 {now} | BTC: ${price:,.0f}\n\n"
        f"📊 DURUM: {regime} | {session}\n"
        f"Makro: {move_type} ({reliability})\n\n"
        f"🔮 ÖNGÖRÜ\n"
        + "\n".join(_ol(h) for h in ["1H", "4H", "1D"] if h in outlooks)
        + f"\n\n⚡ EN GÜÇLÜ SİNYAL\n{best_str}\n\n"
        f"💧 KRİTİK SEVİYELER\n"
        f"Liq (Long): {liq_long}\n"
        f"Liq (Short): {liq_short}\n"
        f"Max Pain: ${mp_price}\n\n"
        f"📈 EDGE DURUMU\n"
        f"Scalp önerilen: {scalp_ok}\n"
        f"Swing önerilmeyen: {swing_no}\n"
        f"{'━'*24}"
    )


def send_analyst_report() -> bool:
    """
    format_analyst_report() çağırır ve Telegram'a gönderir.
    Mevcut _send_telegram fonksiyonunu kullanır.
    """
    try:
        msg = format_analyst_report()
        return _send_telegram(msg, parse_mode="HTML")
    except Exception as e:
        print(f"[TG] Analyst report error: {e}", flush=True)
        return False
