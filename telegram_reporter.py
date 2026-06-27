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

def _read_all_jsonl(path: Path, limit: int | None = None) -> list[dict]:
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
    return rows[-limit:] if limit is not None else rows

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
        "last_blocker": None,
        "last_error": None,
        "last_sent_trade_id": None,
        "last_status": "waiting_new_paper_trade",
        "last_seen_trade_id": None,
        "current_open_count": 0,
        "configured": TELEGRAM_CONFIGURED,
    }
    base.update(payload)
    _safe_write_json(HEALTH_FILE, base)

def _join_context(trade: dict) -> dict:
    trade_id = str(trade.get("trade_id") or trade.get("id") or "")
    setup_id = str(trade.get("setup_id") or trade.get("source_setup_id") or trade.get("qualified_setup_id") or "")
    direction = str(trade.get("direction") or trade.get("side") or "").upper() or "unknown"
    entry = _sf(trade.get("entry_price") or trade.get("open_price") or trade.get("entry") or (trade.get("entry") or {}).get("price"))
    sl = _sf(trade.get("sl_price") or trade.get("stop_loss") or trade.get("sl") or (trade.get("risk") or {}).get("sl_price"))
    tp1 = _sf(trade.get("tp1") or trade.get("tp1_price") or (trade.get("targets") or {}).get("tp1"))
    tp2 = _sf(trade.get("tp2") or trade.get("tp2_price") or (trade.get("targets") or {}).get("tp2"))
    tp3 = _sf(trade.get("tp3") or trade.get("tp3_price") or (trade.get("targets") or {}).get("tp3"))
    source_setup_id = str(trade.get("source_setup_id") or trade.get("setup_id") or "")
    qualified_setup_id = str(trade.get("qualified_setup_id") or "")
    warning = "none"

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

    qualified = None
    if setup_id or source_setup_id or qualified_setup_id:
        lookup_ids = {setup_id, source_setup_id, qualified_setup_id}
        for rec in reversed(_read_all_jsonl(QUALIFIED_SETUPS_FILE, limit=200)):
            rid = str(rec.get("source_setup_id") or rec.get("qualified_setup_id") or rec.get("setup_id") or "")
            if rid in lookup_ids:
                qualified = rec
                break

    brain = None
    if setup_id or source_setup_id or qualified_setup_id:
        lookup_ids = {setup_id, source_setup_id, qualified_setup_id}
        for rec in reversed(_read_all_jsonl(TRADE_BRAIN_FILE, limit=400)):
            rid = str(
                rec.get("source_setup_id")
                or rec.get("setup_id")
                or rec.get("qualified_setup_id")
                or ""
            )
            if rid in lookup_ids:
                brain = rec
                break

    obs = None
    if setup_id or source_setup_id or qualified_setup_id:
        lookup_ids = {setup_id, source_setup_id, qualified_setup_id}
        for rec in reversed(_read_all_jsonl(OBSERVATIONS_FILE, limit=800)):
            rid = str(rec.get("source_setup_id") or rec.get("setup_id") or "")
            if rid in lookup_ids:
                obs = rec
                break

    source_context = _pick_dict(
        trade.get("context_at_open")
        or trade.get("context")
        or trade.get("source_context")
        or (qualified or {}).get("context_at_qualification")
        or (brain or {}).get("context")
        or (brain or {}).get("context_snapshot")
        or (qualified or {}).get("context_at_qualification")
        or (obs or {}).get("source_context")
        or {}
    )
    scenario_snapshot = _pick_dict(
        (brain or {}).get("scenario_snapshot"),
        source_context.get("scenario_snapshot"),
        (trade or {}).get("scenario_snapshot"),
    )
    q9_reason = _pick_str(
        (brain or {}).get("brain_questions", {}).get("Q9_market_intent", {}).get("reason"),
        (brain or {}).get("q9_reason"),
        (qualified or {}).get("q9_reason"),
        default="no_q9_context",
    )
    scenario = _pick_str(
        scenario_snapshot.get("dominant_scenario"),
        source_context.get("scenario"),
        source_context.get("active_scenario"),
        (qualified or {}).get("scenario"),
        default="no_scenario",
    )
    scenario_direction = _pick_str(
        scenario_snapshot.get("dominant_direction"),
        source_context.get("scenario_direction"),
        source_context.get("dom_bias"),
        default="unknown",
    )
    scenario_status = _pick_str(
        scenario_snapshot.get("status"),
        (brain or {}).get("status"),
        (qualified or {}).get("status"),
        default="not_available",
    )
    observer_state = _pick_str(
        (obs or {}).get("state_after"),
        (obs or {}).get("state_before"),
        (obs or {}).get("state"),
        default="not_available",
    )
    observer_event = _pick_str((obs or {}).get("event_type"), default="not_available")
    observer_reason = _pick_str((obs or {}).get("observer_reason"), (obs or {}).get("details"), default="not_available")
    confidence = (brain or {}).get("confidence")
    if confidence is None:
        confidence = (qualified or {}).get("confidence")
    confidence_text = f"{confidence:.3f}" if isinstance(confidence, (int, float)) else "not_available"
    if entry > 0 and sl > 0 and tp1 > 0:
        dist = abs(entry - sl)
        if dist == 0:
            rr_text = "invalid"
            warning = "entry_equals_sl"
        else:
            rr_text = f"{abs(tp1 - entry) / dist:.2f}"
    else:
        rr_text = "invalid"
        warning = "missing_entry_or_sl_or_tp1"

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
        "decision": (qualified or {}).get("brain_decision") or (brain or {}).get("decision") or "unknown",
        "q9_reason": q9_reason,
        "scenario": scenario or "no_scenario",
        "scenario_direction": scenario_direction,
        "scenario_status": scenario_status,
        "order_flow": {
            "trend_1s": _pick_str(source_context.get("trend_1s"), default="unknown"),
            "trend_1m": _pick_str(source_context.get("trend_1m"), default="unknown"),
            "micro_bos": _pick_str(source_context.get("micro_bos"), source_context.get("macro_bos"), default="not_available"),
            "gate_grade": _pick_str(source_context.get("gate_grade"), default="unknown"),
            "price_loc": _pick_str(source_context.get("price_loc"), source_context.get("location"), source_context.get("profile_shape"), default="unknown"),
            "dom_bias": _pick_str(source_context.get("dom_bias"), source_context.get("market_bias"), default="unknown"),
            "cascade": _pick_str(source_context.get("cascade"), default="not_available"),
            "session": _pick_str((qualified or {}).get("session_at_qualification"), source_context.get("session"), default="unknown"),
        },
        "observer": {
            "state": observer_state,
            "event": observer_event,
            "reason": observer_reason,
        },
        "explanation": [
            f"1. Trade Brain decision = {(qualified or {}).get('brain_decision') or (brain or {}).get('decision') or 'unknown'}.",
            f"2. Q9 reason = {q9_reason}.",
            f"3. Observer state = {observer_state}, event = {observer_event}.",
        ],
        "context_source": "enriched" if (qualified or brain or obs) else "missing",
    }

def _paper_open_to_message(trade: dict) -> str | None:
    ctx = _join_context(trade)
    if ctx["direction"] not in ("LONG", "SHORT", "unknown"):
        return None
    if ctx["entry"] <= 0 or ctx["sl"] <= 0 or ctx["tp1"] <= 0:
        return None
    warning_text = ctx["warning"]
    if ctx["rr_text"] == "invalid" and warning_text == "none":
        warning_text = "missing_rr_inputs"
    return (
        "🧠 NurtacCoreEngine Paper Trade Açıldı\n\n"
        f"Symbol: {SYMBOL}\n"
        f"Direction: {ctx['direction']}\n"
        f"Entry: {ctx['entry']:.2f}\n"
        f"SL: {ctx['sl']:.2f}\n"
        f"TP1: {ctx['tp1']:.2f}\n"
        f"TP2: {ctx['tp2']:.2f}\n"
        f"TP3: {ctx['tp3']:.2f}\n"
        f"RR: {ctx['rr_text']}\n\n"
        f"Confidence: {ctx['confidence']}\n"
        f"Decision: {ctx['decision']}\n"
        f"Scenario: {ctx['scenario']}\n"
        f"Scenario Direction: {ctx['scenario_direction']}\n"
        f"Scenario Status: {ctx['scenario_status']}\n"
        f"Q9: {ctx['q9_reason']}\n\n"
        "Order Flow:\n"
        f"* Trend 1S: {ctx['order_flow']['trend_1s']}\n"
        f"* Trend 1M: {ctx['order_flow']['trend_1m']}\n"
        f"* Micro BOS: {ctx['order_flow']['micro_bos']}\n"
        f"* Gate: {ctx['order_flow']['gate_grade']}\n"
        f"* Price Location: {ctx['order_flow']['price_loc']}\n"
        f"* Bias: {ctx['order_flow']['dom_bias']}\n"
        f"* Cascade: {ctx['order_flow']['cascade']}\n"
        f"* Session: {ctx['order_flow']['session']}\n\n"
        "Observer:\n"
        f"* State: {ctx['observer']['state']}\n"
        f"* Event: {ctx['observer']['event']}\n"
        f"* Setup ID: {ctx['source_setup_id']}\n"
        f"* Qualified ID: {ctx['qualified_setup_id']}\n\n"
        "Trade Brain Explanation:\n"
        "Bu trade açıldı çünkü:\n"
        f"{ctx['explanation'][0]}\n"
        f"{ctx['explanation'][1]}\n"
        f"{ctx['explanation'][2]}\n\n"
        "Health:\n"
        f"* Context source: {ctx['context_source']}\n"
        f"* Warning: {warning_text}\n"
    )

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
                            _write_health(last_status="sent", last_sent_trade_id=trade_id, last_seen_trade_id=trade_id, current_open_count=current_open)
                            print(f"[TELEGRAM] Paper open sent: {trade_id}", flush=True)
                        else:
                            _write_health(last_status="telegram_api_error", last_blocker="telegram_api_error", last_error="sendMessage failed", last_seen_trade_id=trade_id, current_open_count=current_open)
                    else:
                        _write_health(last_status="paper_schema_mismatch", last_blocker="paper_schema_mismatch", last_error="missing entry/sl/tp1", last_seen_trade_id=trade_id, current_open_count=current_open)
            else:
                _write_health(last_status="waiting_new_paper_trade", last_blocker="waiting_new_paper_trade", last_seen_trade_id=last_seen_trade_id, current_open_count=0)

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
            _write_health(last_status="telegram_reporter_error", last_blocker="reporter_not_started", last_error=str(e))
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
        _write_health(last_status="telegram_config_missing", last_blocker="telegram_config_missing", configured=False)
    else:
        _write_health(last_status="waiting_new_paper_trade", configured=True)

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
