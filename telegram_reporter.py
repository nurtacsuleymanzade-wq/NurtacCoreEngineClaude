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
BIAS_FILE = DATA_DIR / "bias_context.jsonl"
PAPER_TRADES_FILE = DATA_DIR / "paper_trades.jsonl"
PAPER_TRADES_OPEN_FILE = DATA_DIR / "paper_trades_open.json"
EDGE_MATRIX_FILE = DATA_DIR / "edge_matrix.jsonl"
OUTCOME_FILE = DATA_DIR / "historical_outcome_observations.jsonl"
PRIMARY_FILE = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"

# ── Output files ───────────────────────────────────────────────────────────────
LOG_FILE = DATA_DIR / "telegram_log.jsonl"

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

def _send_telegram(text: str, parse_mode: str = "HTML") -> bool:
    """Send message to Telegram. Return True if successful or no token."""
    if not TELEGRAM_CONFIGURED:
        print(text)  # Print to terminal as fallback
        return True

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

# ── Signal Message ─────────────────────────────────────────────────────────────
def format_signal_message(setup: dict) -> str | None:
    """Format SIGNAL message from final_setup record."""
    # Check: PREMIUM or STANDARD quality
    quality = setup.get("quality", "").upper()
    if quality not in ["PREMIUM", "STANDARD"]:
        return None

    # Check: long_bias or short_bias >= 60%
    long_bias = _sf(setup.get("long_bias", 0))
    short_bias = _sf(setup.get("short_bias", 0))
    direction = None
    bias_pct = 0

    if long_bias >= 60:
        direction = "LONG"
        bias_pct = int(long_bias)
    elif short_bias >= 60:
        direction = "SHORT"
        bias_pct = int(short_bias)
    else:
        return None

    # Extract data
    ts = setup.get("window_start_ts")
    price = _sf(setup.get("price", 0))
    timeframe = setup.get("dominant_timeframe", "unknown")
    entry = _sf(setup.get("entry", 0))
    sl = _sf(setup.get("sl", 0))
    tp1 = _sf(setup.get("tp1", 0))
    tp2 = _sf(setup.get("tp2", 0))
    tp3 = _sf(setup.get("tp3", 0))

    # Calculate percentages
    sl_pct = abs((sl - entry) / entry * 100) if entry > 0 else 0
    tp1_pct = abs((tp1 - entry) / entry * 100) if entry > 0 else 0
    tp2_pct = abs((tp2 - entry) / entry * 100) if entry > 0 else 0
    tp3_pct = abs((tp3 - entry) / entry * 100) if entry > 0 else 0

    # Reason chain (from setup metadata)
    reasons = []
    if setup.get("smart_money_reason"):
        reasons.append(f"- {setup['smart_money_reason']}")
    if setup.get("detector_reason"):
        reasons.append(f"- {setup['detector_reason']}")
    if setup.get("baseline_reason"):
        reasons.append(f"- {setup['baseline_reason']}")
    if setup.get("market_context_reason"):
        reasons.append(f"- {setup['market_context_reason']}")
    if setup.get("volume_profile_reason"):
        reasons.append(f"- {setup['volume_profile_reason']}")

    reason_text = "\n".join(reasons[:5]) if reasons else "- N/A"

    # Historical win rate
    similar_count = int(setup.get("similar_setups_count", 0))
    historical_wr = _sf(setup.get("historical_win_rate", 0))

    # Build message
    emoji = "🟢" if direction == "LONG" else "🔴"
    msg = f"""{emoji} {direction} SİNYAL — {SYMBOL}
━━━━━━━━━━━━━━━━━━━
⏰ {get_time_utc4()} | 💰 ${price:.2f}
📊 Timeframe: {timeframe}
🏆 Kalite: {quality} ({bias_pct}% {direction} bias)

📍 Entry:  ${entry:.2f}
🛡 SL:     ${sl:.2f}  ({sl_pct:.2f}%)
🎯 TP1:    ${tp1:.2f}  ({tp1_pct:.2f}%)
🎯 TP2:    ${tp2:.2f}  ({tp2_pct:.2f}%)
🎯 TP3:    ${tp3:.2f}  ({tp3_pct:.2f}%)

📈 Sebep-sonuç zinciri:
{reason_text}

🔢 Geçmiş: {similar_count} benzer setup → %{int(historical_wr)} WR
"""
    return msg

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
    sent_trade_ids = set()
    last_15min_report = time.time()

    while not HALT_FILE.exists():
        try:
            # Check for new signals
            final_setups = _read_last_jsonl(FINAL_SETUPS_FILE, maxlen=50)
            for setup in final_setups:
                signal_id = f"{setup.get('window_start_ts', 0)}_{setup.get('setup_id', '')}"
                if signal_id not in sent_signal_ids:
                    msg = format_signal_message(setup)
                    if msg:
                        if _send_telegram(msg):
                            sent_signal_ids.add(signal_id)
                            _log_message("signal", msg, {"setup_id": signal_id})
                            print(f"[TELEGRAM] Signal sent: {signal_id}", flush=True)

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

    await run_live()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram Reporter — Layer 13")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    if args.mode == "live":
        asyncio.run(main())
