"""
NurtacCoreEngineClaude — Layer-13: Telegram Reporter

Monitors all signal/trade/system events and sends curated Telegram reports.
No Binance API. No real orders. Reads only JSONL files.

Supports graceful fallback to terminal if TELEGRAM_BOT_TOKEN/CHAT_ID not set.
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
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_CONFIGURED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage" if TELEGRAM_TOKEN else None
POLL_SLEEP = 0.5
HEALTH_INTERVAL_S = 30.0
MSG_QUEUE_MAX = 50
RETRY_DELAYS = [2, 4, 8]  # seconds

# Cooldowns (seconds)
COOLDOWN_STRUCTURE = 300  # 5 min
COOLDOWN_GATE_A = 30
COOLDOWN_SCENARIO = 180  # 3 min
COOLDOWN_WATCHDOG = 300  # 5 min
COOLDOWN_VALIDATOR = 300  # 5 min

# ── Input files ───────────────────────────────────────────────────────────────
TRADES_FILE = DATA_DIR / "paper_trades.jsonl"
TRADES_OPEN_FILE = DATA_DIR / "paper_trades_open.json"
SUMMARY_FILE = DATA_DIR / "paper_trade_summary.json"
SETUPS_FILE = DATA_DIR / "qualified_setups.jsonl"
SCENARIOS_FILE = DATA_DIR / "scenarios.jsonl"
STRUCTURE_1S_FILE = DATA_DIR / "structure_1s.jsonl"
STRUCTURE_1M_FILE = DATA_DIR / "structure_1m.jsonl"
GATE_FILE = DATA_DIR / "decision_gate_output.jsonl"
QUALITY_FILE = DATA_DIR / "data_quality_log.jsonl"
VALIDATOR_FILE = DATA_DIR / "validation_report.jsonl"
OUTCOME_HEALTH_FILE = DATA_DIR / "historical_outcome_health.json"
PRIMARY_FILE = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"

# ── Output files ──────────────────────────────────────────────────────────────
LOG_FILE = DATA_DIR / "telegram_log.jsonl"
HEALTH_FILE = DATA_DIR / "telegram_health.json"

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def ts_to_human(ms: int | None) -> str:
    if ms is None:
        return "unknown"
    try:
        dt = datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "invalid_ts"

def tp_icon(hit: bool) -> str:
    return "✅" if hit else "⬜"

def format_r(r: float | None) -> str:
    if r is None:
        return "N/A"
    sign = "+" if r >= 0 else ""
    return f"{sign}{r:.2f}R"

def _read_all_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(path, "r", encoding=enc) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return records
        except (OSError, UnicodeDecodeError):
            records.clear()
    return records

def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _append_jsonl(path: Path, rec: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        pass

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

# ── Message queue and rate limiting ───────────────────────────────────────────
class MessageQueue:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=MSG_QUEUE_MAX)
        self.cooldowns: dict[str, float] = {}
        self.sent_count = 0
        self.failed_count = 0
        self.processed_ids: set[str] = set()
        self.message_hashes: dict[str, float] = {}  # hash -> timestamp mapping
        self.last_hourly_sent_hour: int = -1  # track last sent hour

    def can_send(self, cooldown_key: str, cooldown_sec: int) -> bool:
        now = time.time()
        last = self.cooldowns.get(cooldown_key, 0)
        return now - last >= cooldown_sec

    def record_send(self, cooldown_key: str) -> None:
        self.cooldowns[cooldown_key] = time.time()
        self.sent_count += 1

    async def enqueue(self, msg_type: str, text: str, cooldown_key: str | None = None) -> bool:
        if self.queue.full():
            self.queue.get_nowait()  # remove oldest
        await self.queue.put((msg_type, text, cooldown_key))
        return True

    def has_processed(self, obj_id: str) -> bool:
        return obj_id in self.processed_ids

    def mark_processed(self, obj_id: str) -> None:
        self.processed_ids.add(obj_id)

    def is_duplicate_message(self, text: str, cooldown_sec: int = 60) -> bool:
        import hashlib
        msg_hash = hashlib.md5(text.encode()).hexdigest()
        now = time.time()
        if msg_hash in self.message_hashes:
            last_sent = self.message_hashes[msg_hash]
            if now - last_sent < cooldown_sec:
                return True
        self.message_hashes[msg_hash] = now
        return False

# ── State ─────────────────────────────────────────────────────────────────────
class ReporterState:
    def __init__(self):
        self.mq = MessageQueue()
        self.last_trade_id: str | None = None
        self.last_setup_id: str | None = None
        self.last_scenario: str | None = None
        self.last_structure_1m: dict | None = None
        self.last_gate_ts: int = 0
        self.last_quality_ts: int = 0
        self.last_validator_ts: int = 0
        self.current_price: float | None = None
        self.active_scenario: str | None = None
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.last_hourly_hour = -1
        self.last_daily_day = -1

# ── Telegram send ─────────────────────────────────────────────────────────────
async def send_telegram_message(text: str, retry_count: int = 0) -> tuple[bool, str | None]:
    if not TELEGRAM_CONFIGURED:
        return False, "not_configured"

    try:
        payload = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }).encode("utf-8")

        req = urllib.request.Request(
            TELEGRAM_API,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                return True, None
            return False, result.get("description", "unknown")
    except Exception as e:
        if retry_count < len(RETRY_DELAYS):
            delay = RETRY_DELAYS[retry_count]
            await asyncio.sleep(delay)
            return await send_telegram_message(text, retry_count + 1)
        return False, str(e)

# ── Message worker ───────────────────────────────────────────────────────────
async def message_worker(state: ReporterState) -> None:
    last_send_time = 0.0
    while not HALT_FILE.exists():
        try:
            msg_type, text, cooldown_key = await asyncio.wait_for(
                state.mq.queue.get(), timeout=1.0
            )
        except asyncio.TimeoutError:
            continue

        # Rate limit: 1 msg/sec
        now = time.time()
        if now - last_send_time < 1.0:
            await asyncio.sleep(1.0 - (now - last_send_time))
        last_send_time = time.time()

        # Duplicate check (60 sec cooldown on content)
        if state.mq.is_duplicate_message(text, cooldown_sec=60):
            _log_message(msg_type, "skipped_duplicate", None, text[:100], 0)
            continue

        # Cooldown check
        if cooldown_key and not state.mq.can_send(cooldown_key, 30):  # simplified check
            _log_message(msg_type, "skipped_cooldown", None, text[:100], 0)
            msg_preview = f"[TELEGRAM] SKIPPED {msg_type} (cooldown active)"
            if TELEGRAM_CONFIGURED:
                print(msg_preview, flush=True)
            continue

        # Try send
        success, error = await send_telegram_message(text)
        if success:
            state.mq.record_send(cooldown_key or msg_type)
            _log_message(msg_type, "sent", None, text[:100], 0)
            print(f"[TELEGRAM] SENT {msg_type}", flush=True)
        else:
            state.mq.failed_count += 1
            _log_message(msg_type, "failed", error, text[:100], 0)
            print(f"[TELEGRAM] FAILED {msg_type}: {error}", flush=True)

        if not TELEGRAM_CONFIGURED:
            print(f"[TELEGRAM] NOT CONFIGURED — message:\n{text}", flush=True)

# ── Message formatters ────────────────────────────────────────────────────────
def format_setup_opened(setup: dict) -> str:
    ep = _sf((setup.get("entry") or {}).get("recommended_entry"), 0.0)
    sl = _sf((setup.get("risk") or {}).get("sl_price"), 0.0)
    t1 = _sf((setup.get("targets") or {}).get("tp1"), 0.0)
    t2 = _sf((setup.get("targets") or {}).get("tp2"), 0.0)
    t3 = _sf((setup.get("targets") or {}).get("tp3"), 0.0)
    d = setup.get("direction", "?").upper()
    ctx = setup.get("context_at_qualification") or {}
    gg = ctx.get("gate_grade", "?")
    sc = ctx.get("active_scenario") or "none"
    tr1s = ctx.get("trend_1s") or "?"
    tr1m = ctx.get("trend_1m") or "?"
    loc = ctx.get("location") or "?"
    ts = setup.get("qualification_ts", 0)
    ts_h = ts_to_human(ts)

    rr = 1.0
    if sl > 0 and ep > 0:
        if d == "LONG":
            rr = (t3 - ep) / (ep - sl) if (ep - sl) > 0 else 1.0
        else:
            rr = (ep - t3) / (sl - ep) if (sl - ep) > 0 else 1.0

    sl_dist = abs(ep - sl)
    return f"""<b>🟢 SETUP AÇILDI</b>
━━━━━━━━━━━━━━━━━━━
<b>{d}</b> | BTCUSDT | Gate {gg}
📍 Entry: <b>{ep:.2f}</b>
🛑 SL: {sl:.2f} | 🎯 TP1: {t1:.2f} TP2: {t2:.2f} TP3: {t3:.2f}
📐 Risk: {sl_dist:.2f} | R:R = 1:{rr:.1f}
🧩 Senaryo: {sc}
📊 Trend 1S: {tr1s} | 1M: {tr1m}
🏔️ Location: {loc}
⏱️ {ts_h}"""

def format_trade_close(trade: dict) -> str:
    res = trade.get("results") or {}
    ctx = trade.get("context_at_open") or {}
    d = trade.get("direction", "?").upper()
    outcome = res.get("outcome", "?").upper()
    reason = trade.get("close_reason", "?")
    pr = _sf(res.get("pnl_r"), 0.0)
    pp = _sf(res.get("pnl_pct"), 0.0)
    dur = _sf(trade.get("duration_seconds"), 0.0)
    bars = trade.get("bars_held", 0)
    mfr = _sf(res.get("max_favorable_r"), 0.0)
    mar = _sf(res.get("max_adverse_r"), 0.0)
    sc = ctx.get("scenario") or "?"
    gg = ctx.get("gate_grade") or "?"
    ms = trade.get("milestones") or {}
    tp1 = tp_icon(ms.get("tp1_hit", False))
    tp2 = tp_icon(ms.get("tp2_hit", False))
    tp3 = tp_icon(ms.get("tp3_hit", False))

    sign = "+" if pr >= 0 else ""
    emoji = "✅" if outcome in ("WIN", "TIMEOUT_WIN") else "❌" if "LOSS" in outcome else "⚖️"

    if outcome == "BREAKEVEN":
        return f"""<b>{emoji} TRADE KAPANDI — BREAKEVEN</b>
━━━━━━━━━━━━━━━━━━━
<b>{d}</b> | TP1 sonrası stop tetiklendi
💰 P&L: 0.00R
⏱️ Süre: {dur:.0f}s
🎯 TP1: {tp1} TP2: {tp2}"""
    else:
        return f"""<b>{emoji} TRADE KAPANDI — {outcome}</b>
━━━━━━━━━━━━━━━━━━━
<b>{d}</b> | {reason}
💰 P&L: <b>{sign}{pr:.2f}R</b> | {sign}{pp:.4f}%
⏱️ Süre: {dur:.0f}s | {bars} bar
🎯 TP1:{tp1} TP2:{tp2} TP3:{tp3}
📈 Max Favorable: +{mfr:.2f}R
📉 Max Adverse: -{mar:.2f}R
🧩 Senaryo: {sc} | Gate: {gg}"""

def format_structure_event(row: dict, current_price: float) -> tuple[str, str]:
    ts = row.get("window_start_ts", 0)
    ts_h = ts_to_human(ts)
    bos = row.get("bos") or {}
    trend = row.get("trend") or {}

    event_type = "unknown"
    emoji = "❓"
    if trend.get("choch_confirmed") == "bullish":
        event_type = "choch_confirmed_bullish"
        emoji = "🔄🟢"
    elif trend.get("choch_confirmed") == "bearish":
        event_type = "choch_confirmed_bearish"
        emoji = "🔄🔴"
    elif trend.get("msb") == "bullish":
        event_type = "msb_bullish"
        emoji = "💥🟢"
    elif trend.get("msb") == "bearish":
        event_type = "msb_bearish"
        emoji = "💥🔴"
    elif bos.get("macro_bos") == "bullish":
        event_type = "macro_bos_bullish"
        emoji = "⬆️"
    elif bos.get("macro_bos") == "bearish":
        event_type = "macro_bos_bearish"
        emoji = "⬇️"

    trend_dir = trend.get("trend_direction", "?")
    trend_str = trend.get("trend_strength", "?")
    poc = _sf(row.get("poc"), 0.0)
    vah = _sf(row.get("vah"), 0.0)
    val = _sf(row.get("val"), 0.0)

    text = f"""<b>{emoji} YAPISAL EVENT — 1M</b>
━━━━━━━━━━━━━━━━━━━
{emoji} <b>{event_type}</b>
💹 BTC: {current_price:.2f}
📊 POC: {poc:.2f} | VAH: {vah:.2f} | VAL: {val:.2f}
🧩 Trend: {trend_dir} ({trend_str})
⏱️ {ts_h}"""

    return event_type, text

def format_gate_a(row: dict) -> str:
    ts = row.get("window_start_ts", 0)
    ts_h = ts_to_human(ts)
    dom_dir = row.get("dominant_direction", "?")
    conf = _sf(row.get("confluence_score"), 0.0)
    qual = _sf(row.get("quality_score"), 0.0)
    ctx_align = row.get("context_alignment", "unknown")

    # Detector summary from detector_summary field
    active_detectors = []
    for det_name, det_data in row.get("detector_summary", {}).items():
        if det_data.get("label") != "none":
            label = det_data.get("label", "")
            active_detectors.append(f"{det_name}:{label}")

    if active_detectors:
        detector_line = " + ".join(active_detectors)
    else:
        detector_line = "detectors: none"

    return f"""<b>🏆 GATE A SETUP</b>
━━━━━━━━━━━━━━━━━━━
<b>{dom_dir}</b> | Confluence: {conf:.1f}
Quality: {qual:.1f}
Detectors: {detector_line}
📊 Context: {ctx_align}
⏱️ {ts_h}"""

def format_scenario_change(row: dict) -> str:
    ts = row.get("window_start_ts", 0)
    ts_h = ts_to_human(ts)
    dom_scen = row.get("dominant_scenario", "?")
    dom_dir = row.get("dominant_direction", "?").upper()
    act_scens = row.get("active_scenarios") or []
    active = next((s for s in act_scens if s.get("scenario_name") == dom_scen), {})
    score = _sf(active.get("score"), 0.0)
    max_score = _sf(active.get("max_score"), 1.0)
    mq = active.get("market_questions") or {}

    loc = mq.get("location", "?")
    agg = mq.get("aggression", "?")
    cont = mq.get("continuation", "?")
    inval = mq.get("invalidation", "?")
    tgt = mq.get("target", "?")

    return f"""<b>🎭 SENARYO: {dom_scen}</b>
━━━━━━━━━━━━━━━━━━━
Yön: <b>{dom_dir}</b> | Score: {score:.0f}/{max_score:.0f}
📍 {loc}
⚔️ Aggression: {agg}
🛡️ Continuation: {cont}
⚠️ Invalidation: {inval}
🎯 Target: {tgt}
⏱️ {ts_h}"""

def format_hourly_summary(state: ReporterState) -> str:
    dt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    hour = dt.hour
    summary_data = _read_json(SUMMARY_FILE) or {}
    total_data = summary_data.get("total", {})

    trades = total_data.get("trades", 0)
    wins = total_data.get("wins", 0)
    losses = total_data.get("losses", 0)
    wr = total_data.get("win_rate", 0.0)
    avg_r = total_data.get("avg_pnl_r", 0.0)
    tot_r = total_data.get("total_pnl_r", 0.0)
    pf = total_data.get("profit_factor", None) or "N/A"

    max_win = total_data.get("max_win_r", 0.0)
    max_loss = total_data.get("max_loss_r", 0.0)

    outcome_health = _read_json(OUTCOME_HEALTH_FILE) or {}
    completed_obs = outcome_health.get("completed_observations", 0)
    open_obs = outcome_health.get("open_observations", 0)

    # Format price: show N/A if not available
    price_str = f"{state.current_price:.2f}" if state.current_price and state.current_price > 0 else "N/A"

    # Format max loss: show N/A if no trades, otherwise show signed number
    if trades == 0:
        max_loss_str = "N/A"
    else:
        max_loss_str = f"{max_loss:+.2f}R"

    return f"""<b>📊 SAATLIK ÖZET</b>
━━━━━━━━━━━━━━━━━━━
⏰ {hour:02d}:00 UTC

📈 <b>İşlemler:</b>
  Toplam: {trades} | Win: {wins} | Loss: {losses}
  Win Rate: <b>{wr:.1f}%</b>
  Avg R: {avg_r:+.2f} | Total R: {tot_r:+.2f}
  Profit Factor: {pf}

🏆 <b>En İyi:</b> +{max_win:.2f}R
💸 <b>En Kötü:</b> {max_loss_str}

📚 <b>Öğrenme:</b>
  Tamamlanan: {completed_obs}
  Açık: {open_obs}

💹 <b>Güncel BTC:</b> {price_str}
🧩 <b>Aktif Senaryo:</b> {state.active_scenario or "none"}"""

def format_daily_summary(state: ReporterState) -> str:
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    date_str = now.strftime("%Y-%m-%d")
    summary_data = _read_json(SUMMARY_FILE) or {}
    total_data = summary_data.get("total", {})

    trades = total_data.get("trades", 0)
    wr = total_data.get("win_rate", 0.0)
    tot_r = total_data.get("total_pnl_r", 0.0)

    by_dir = summary_data.get("by_direction", {})
    long_data = by_dir.get("long", {})
    short_data = by_dir.get("short", {})
    long_trades = long_data.get("trades", 0)
    long_wr = long_data.get("win_rate", 0.0)
    short_trades = short_data.get("trades", 0)
    short_wr = short_data.get("win_rate", 0.0)

    by_scenario = summary_data.get("by_scenario", {})
    best_scen = max((k for k, v in by_scenario.items() if v.get("trades", 0) > 0),
                    key=lambda k: by_scenario[k].get("win_rate", 0), default="none")

    by_gg = summary_data.get("by_gate_grade", {})
    best_gg = max((k for k, v in by_gg.items() if v.get("trades", 0) > 0),
                  key=lambda k: by_gg[k].get("win_rate", 0), default="?")

    outcome_health = _read_json(OUTCOME_HEALTH_FILE) or {}
    total_obs = outcome_health.get("completed_observations", 0)

    return f"""<b>📅 GÜNLÜK ÖZET</b>
━━━━━━━━━━━━━━━━━━━
📆 {date_str} UTC

📈 <b>Günlük İşlemler:</b>
  Toplam: {trades} | Win Rate: {wr:.1f}%
  Total R: {tot_r:+.2f}

📊 <b>Yön Dağılımı:</b>
  Long: {long_trades} (Win: {long_wr:.0f}%)
  Short: {short_trades} (Win: {short_wr:.0f}%)

🏷️ <b>En İyi Senaryo:</b> {best_scen}
⚙️ <b>En İyi Gate:</b> {best_gg}

🧠 <b>Öğrenme Birikimi:</b>
  Toplam Observation: {total_obs}"""

def format_system_halt() -> str:
    try:
        with open(HALT_FILE, "r", encoding="utf-8") as f:
            halt_data = json.load(f)
    except Exception:
        halt_data = {}
    reason = halt_data.get("reason", "unknown")
    ts = halt_data.get("ts", 0)
    ts_h = ts_to_human(ts)
    return f"""<b>🛑 SİSTEM DURDURULDU — SYSTEM HALT</b>
━━━━━━━━━━━━━━━━━━━
⚠️ Kritik uyuşmazlık tespit edildi.
Sebep: {reason}
Zaman: {ts_h}
Manuel müdahale gerekiyor."""

def format_watchdog_alarm(row: dict) -> str:
    ts = row.get("ts", 0)
    ts_h = ts_to_human(ts)
    evt_type = row.get("event_type", "?")
    src = row.get("source", "?")
    detail = row.get("detail", "")
    return f"""<b>⚠️ SİSTEM ALARMI</b>
━━━━━━━━━━━━━━━━━━━
🔌 {evt_type}
Kaynak: {src}
Detay: {detail}
⏱️ {ts_h}"""

def format_validator_alert(row: dict) -> str:
    ts = row.get("ts", 0)
    ts_h = ts_to_human(ts)
    tf = row.get("timeframe", "?")
    diff_pct = _sf(row.get("diff_pct"), 0.0)
    our = _sf(row.get("our_close"), 0.0)
    binance = _sf(row.get("binance_close"), 0.0)
    return f"""<b>🚨 VALİDATOR KRİTİK UYARI</b>
━━━━━━━━━━━━━━━━━━━
Timeframe: {tf}
Close Diff: {diff_pct:.4f}%
Bizim: {our:.2f} | Binance: {binance:.2f}
⏱️ {ts_h}"""

# ── Logging ───────────────────────────────────────────────────────────────────
def _log_message(msg_type: str, status: str, response: str | None,
                 preview: str, retry: int) -> None:
    rec = {
        "ts": time.time(),
        "message_type": msg_type,
        "status": status,
        "telegram_response": response,
        "message_preview": preview,
        "retry_count": retry,
    }
    _append_jsonl(LOG_FILE, rec)

# ── Health ────────────────────────────────────────────────────────────────────
def write_health(state: ReporterState) -> None:
    _safe_write_json(HEALTH_FILE, {
        "status": "alive",
        "telegram_configured": TELEGRAM_CONFIGURED,
        "messages_sent": state.mq.sent_count,
        "messages_failed": state.mq.failed_count,
        "messages_queued": state.mq.queue.qsize(),
        "last_sent_ts": None,  # updated on each send
        "last_message_type": None,
        "cooldowns_active": {k: round(v, 1) for k, v in state.mq.cooldowns.items()},
        "warnings": state.warnings[-20:],
        "errors": state.errors[-20:],
    })

# ── File tailers ──────────────────────────────────────────────────────────────
async def _tail_file(path: Path, callback, state: ReporterState,
                     required: bool = False) -> None:
    """Generic file tailer."""
    while not path.exists():
        if HALT_FILE.exists():
            return
        if required:
            state.warnings.append(f"{path.name}_missing")
        await asyncio.sleep(1.0)

    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                await callback(json.loads(line), state)
            except Exception as e:
                state.errors.append(f"{path.name}: {str(e)[:50]}")

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
                await callback(json.loads(line), state)
            except Exception as e:
                state.errors.append(f"{path.name}: {str(e)[:50]}")

# ── Callbacks for each file ───────────────────────────────────────────────────
async def on_trade_close(row: dict, state: ReporterState) -> None:
    trade_id = row.get("trade_id", "")
    if state.mq.has_processed(trade_id):
        return
    state.mq.mark_processed(trade_id)
    state.last_trade_id = trade_id
    text = format_trade_close(row)
    await state.mq.enqueue("trade_close", text)

async def on_setup_open(row: dict, state: ReporterState) -> None:
    setup_id = row.get("qualified_setup_id", "")
    if state.mq.has_processed(setup_id):
        return
    state.mq.mark_processed(setup_id)
    state.last_setup_id = setup_id
    text = format_setup_opened(row)
    await state.mq.enqueue("setup_open", text)

async def on_structure_1m(row: dict, state: ReporterState) -> None:
    ts = row.get("window_start_ts", 0)
    bos = row.get("bos") or {}
    trend = row.get("trend") or {}

    has_event = (
        trend.get("choch_confirmed") or
        trend.get("msb") or
        bos.get("macro_bos")
    )
    if not has_event:
        return

    event_key, text = format_structure_event(row, state.current_price)
    cooldown_key = f"structure_{event_key}"

    if state.mq.can_send(cooldown_key, COOLDOWN_STRUCTURE):
        await state.mq.enqueue("structure_event", text, cooldown_key)
        state.mq.record_send(cooldown_key)

async def on_gate_a(row: dict, state: ReporterState) -> None:
    ts = row.get("window_start_ts", 0)
    grade = row.get("setup_grade", "")
    if grade != "A":
        return
    if state.mq.has_processed(f"gate_{ts}"):
        return
    state.mq.mark_processed(f"gate_{ts}")
    text = format_gate_a(row)
    await state.mq.enqueue("gate_a", text, "gate_a")

async def on_scenario_change(row: dict, state: ReporterState) -> None:
    dom_scen = row.get("dominant_scenario")
    if not dom_scen or state.last_scenario == dom_scen:
        return
    state.last_scenario = dom_scen
    state.active_scenario = dom_scen

    acts = row.get("active_scenarios") or []
    is_confirmed = any(s.get("status") == "confirmed" for s in acts
                       if s.get("scenario_name") == dom_scen)
    if not is_confirmed:
        return

    text = format_scenario_change(row)
    await state.mq.enqueue("scenario_change", text, f"scenario_{dom_scen}")

async def on_quality_log(row: dict, state: ReporterState) -> None:
    evt_type = row.get("event_type", "")
    if evt_type not in ("stream_disconnected", "gap_detected"):
        return

    # Skip small gaps (< 5 seconds) — they're normal
    if evt_type == "gap_detected":
        detail = row.get("detail") or {}
        gap_seconds = detail.get("gap_seconds", 0)
        if isinstance(gap_seconds, (int, float)) and gap_seconds < 5:
            return  # Small gaps are noise, skip them

    ts = row.get("ts", 0)
    if state.mq.has_processed(f"quality_{ts}"):
        return
    state.mq.mark_processed(f"quality_{ts}")
    text = format_watchdog_alarm(row)
    await state.mq.enqueue("watchdog_alarm", text, f"watchdog_{evt_type}")

async def on_validator(row: dict, state: ReporterState) -> None:
    status = row.get("status", "")
    if status != "critical":
        return
    ts = row.get("ts", 0)
    if state.mq.has_processed(f"validator_{ts}"):
        return
    state.mq.mark_processed(f"validator_{ts}")
    text = format_validator_alert(row)
    await state.mq.enqueue("validator_alert", text, "validator")

# ── Current price updater ─────────────────────────────────────────────────────
async def _tail_price(state: ReporterState) -> None:
    while not PRIMARY_FILE.exists():
        await asyncio.sleep(1.0)
    with open(PRIMARY_FILE, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                cdna = row.get("candle_dna") or {}
                co = cdna.get("close")
                has_trade = row.get("has_trade", True)

                if isinstance(co, dict):
                    px = _sf(co.get("price"), None)
                else:
                    px = _sf(co, None)

                # If null or has_trade=false, check carry_forward_price
                if px is None or px <= 0 or not has_trade:
                    cfp = row.get("carry_forward_price")
                    px = _sf(cfp, None)

                if px is not None and px > 0:
                    state.current_price = px
            except Exception:
                pass
        while not HALT_FILE.exists():
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                cdna = row.get("candle_dna") or {}
                co = cdna.get("close")
                has_trade = row.get("has_trade", True)

                if isinstance(co, dict):
                    px = _sf(co.get("price"), None)
                else:
                    px = _sf(co, None)

                # If null or has_trade=false, check carry_forward_price
                if px is None or px <= 0 or not has_trade:
                    cfp = row.get("carry_forward_price")
                    px = _sf(cfp, None)

                if px is not None and px > 0:
                    state.current_price = px
            except Exception:
                pass

# ── Periodic tasks ────────────────────────────────────────────────────────────
async def _periodic(state: ReporterState) -> None:
    last_health = time.time()
    last_hourly = time.time()
    last_daily = time.time()

    while not HALT_FILE.exists():
        await asyncio.sleep(5.0)
        now = time.time()

        if now - last_health >= HEALTH_INTERVAL_S:
            write_health(state)
            last_health = now

        # Hourly summary (only once per hour, on hour change)
        dt_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        current_hour = dt_now.hour
        if current_hour != state.last_hourly_hour:
            text = format_hourly_summary(state)
            await state.mq.enqueue("hourly_summary", text)
            state.last_hourly_hour = current_hour
            # Track in message queue to prevent re-sending in same hour
            state.mq.last_hourly_sent_hour = current_hour
            last_hourly = now

        # Daily summary
        if dt_now.day != state.last_daily_day:
            text = format_daily_summary(state)
            await state.mq.enqueue("daily_summary", text)
            state.last_daily_day = dt_now.day
            last_daily = now

# ── Halt monitoring ───────────────────────────────────────────────────────────
async def _check_halt(state: ReporterState) -> None:
    halt_sent = False
    while True:
        if HALT_FILE.exists() and not halt_sent:
            text = format_system_halt()
            await state.mq.enqueue("system_halt", text)
            halt_sent = True
        if HALT_FILE.exists():
            await asyncio.sleep(10.0)
            # After sending, exit
            sys.exit(1)
        await asyncio.sleep(1.0)

# ── Batch mode ────────────────────────────────────────────────────────────────
async def run_batch() -> None:
    print("[TELEGRAM] Batch mode — sending summary only", flush=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = ReporterState()

    state.current_price = 0.0
    summary_data = _read_json(SUMMARY_FILE) or {}
    total_data = summary_data.get("total", {})
    if total_data.get("trades", 0) > 0:
        text = format_hourly_summary(state)
        success, _ = await send_telegram_message(text)
        if success:
            print("[TELEGRAM] Batch summary sent", flush=True)
        else:
            print("[TELEGRAM] Batch summary failed", flush=True)

    write_health(state)

# ── Live mode ─────────────────────────────────────────────────────────────────
async def run_live() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = ReporterState()

    if not TELEGRAM_CONFIGURED:
        state.warnings.append("telegram_not_configured")
        print("[TELEGRAM] Not configured — messages to terminal only", flush=True)

    write_health(state)

    tasks = [
        asyncio.create_task(_tail_file(TRADES_FILE, on_trade_close, state),
                           name="tg-trades"),
        asyncio.create_task(_tail_file(SETUPS_FILE, on_setup_open, state),
                           name="tg-setups"),
        asyncio.create_task(_tail_file(STRUCTURE_1M_FILE, on_structure_1m, state),
                           name="tg-struct"),
        asyncio.create_task(_tail_file(GATE_FILE, on_gate_a, state),
                           name="tg-gate"),
        asyncio.create_task(_tail_file(SCENARIOS_FILE, on_scenario_change, state),
                           name="tg-scenarios"),
        asyncio.create_task(_tail_file(QUALITY_FILE, on_quality_log, state),
                           name="tg-quality"),
        asyncio.create_task(_tail_file(VALIDATOR_FILE, on_validator, state),
                           name="tg-validator"),
        asyncio.create_task(_tail_price(state), name="tg-price"),
        asyncio.create_task(_periodic(state), name="tg-periodic"),
        asyncio.create_task(_check_halt(state), name="tg-halt"),
        asyncio.create_task(message_worker(state), name="tg-worker"),
    ]

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("[TELEGRAM] Interrupted", flush=True)

    write_health(state)

# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram Reporter — Layer 13")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[TELEGRAM] SYSTEM_HALT exists — exiting", flush=True)
        sys.exit(1)

    if args.mode == "batch":
        asyncio.run(run_batch())
    else:
        asyncio.run(run_live())

if __name__ == "__main__":
    main()
