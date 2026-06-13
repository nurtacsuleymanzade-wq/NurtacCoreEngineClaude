"""
NurtacCoreEngineClaude — Watchdog

Polls every 30 seconds and checks:
  1. Layer-0 data freshness (last combined_1s_dna record age)
  2. Disk usage of the data/ directory
  3. Stream disconnect / reconnect state from data_quality_log.jsonl

Alerts go to the terminal. If TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are
set in the environment, alerts are also sent via Telegram (at most once per
5 minutes per alert type to prevent spam). If the env vars are not set,
Telegram is silently skipped.

Run in its own terminal:
  python3 watchdog.py

  # Optional Telegram alerts:
  # export TELEGRAM_BOT_TOKEN=<your bot token>
  # export TELEGRAM_CHAT_ID=<your chat id>
  # python3 watchdog.py
"""

import json
import os
import sys
import time
from typing import Optional

# Ensure UTF-8 terminal output on Windows (cp1252 can't encode Turkish chars)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR          = "data"
COMBINED_FILE     = os.path.join(DATA_DIR, "combined_1s_dna_btcusdt.jsonl")
DQ_LOG_FILE       = os.path.join(DATA_DIR, "data_quality_log.jsonl")

POLL_INTERVAL_S   = 30          # seconds between each watchdog cycle
DATA_STALE_MS     = 15_000      # alert if last record is older than this
DISK_WARN_PCT     = 85.0        # alert if disk usage exceeds this %
TELEGRAM_COOLDOWN = 300         # seconds; minimum gap between same-type Telegram alerts


TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# Per-alert-type last-sent timestamps for Telegram spam prevention
_tg_last_sent: dict[str, float] = {}


# ── Telegram helper ───────────────────────────────────────────────────────────
def _send_telegram(alert_type: str, message: str) -> None:
    if not (_REQUESTS_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return

    now = time.time()
    if now - _tg_last_sent.get(alert_type, 0.0) < TELEGRAM_COOLDOWN:
        return  # cooldown active — silent skip

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        _requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
        _tg_last_sent[alert_type] = now
    except Exception:
        pass  # never crash the watchdog over a Telegram failure


def _alert(alert_type: str, message: str) -> None:
    print(f"[WATCHDOG ALERT] {message}")
    _send_telegram(alert_type, f"[NurtacCoreEngine ALERT] {message}")


# ── Check helpers ─────────────────────────────────────────────────────────────
def _check_data_freshness() -> None:
    if not os.path.exists(COMBINED_FILE):
        _alert("no_combined_file", f"combined_1s_dna_btcusdt.jsonl does not exist in {DATA_DIR}/")
        return

    last_line = ""
    try:
        with open(COMBINED_FILE, "rb") as fh:
            # Read last non-empty line efficiently
            fh.seek(0, 2)
            size = fh.tell()
            if size == 0:
                _alert("empty_combined_file", "combined_1s_dna_btcusdt.jsonl is empty")
                return
            # Scan backwards for the last newline
            buf_size = min(4096, size)
            fh.seek(-buf_size, 2)
            tail = fh.read(buf_size).decode("utf-8", errors="replace")
            lines = [l for l in tail.splitlines() if l.strip()]
            last_line = lines[-1] if lines else ""
    except OSError as exc:
        _alert("read_error", f"Cannot read {COMBINED_FILE}: {exc}")
        return

    if not last_line:
        _alert("empty_combined_file", "combined_1s_dna_btcusdt.jsonl has no readable lines")
        return

    try:
        rec = json.loads(last_line)
        wts = rec["window_start_ts"]
    except (json.JSONDecodeError, KeyError):
        _alert("parse_error", f"Cannot parse last line of combined_1s_dna_btcusdt.jsonl")
        return

    now_ms = int(time.time() * 1000)
    age_ms = now_ms - wts
    if age_ms > DATA_STALE_MS:
        _alert(
            "data_stale",
            f"Layer-0 veri akışı {age_ms // 1000}+ saniye önce durdu, "
            f"son ts={wts}",
        )


def _check_disk_usage() -> None:
    try:
        stat = os.statvfs(DATA_DIR) if hasattr(os, "statvfs") else None
    except OSError:
        stat = None

    if stat is None:
        # Windows fallback using shutil
        try:
            import shutil
            total, used, free = shutil.disk_usage(DATA_DIR)
            pct = used / total * 100 if total else 0.0
        except Exception:
            return
    else:
        total = stat.f_blocks * stat.f_frsize
        free  = stat.f_bavail * stat.f_frsize
        used  = total - free
        pct   = used / total * 100 if total else 0.0

    if pct >= DISK_WARN_PCT:
        _alert(
            "disk_usage",
            f"VPS disk kullanımı {pct:.1f}% — "
            f"{DISK_WARN_PCT}% eşiğini aştı. Disk doluyor!",
        )


def _check_stream_state() -> None:
    if not os.path.exists(DQ_LOG_FILE):
        return

    cutoff_ms = int(time.time() * 1000) - 30_000  # last 30 seconds

    disconnected_streams: set = set()
    try:
        with open(DQ_LOG_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("ts", 0) < cutoff_ms:
                    continue
                et     = entry.get("event_type", "")
                stream = entry.get("detail", {}).get("stream", "unknown")
                if et == "stream_disconnected":
                    disconnected_streams.add(stream)
                elif et == "stream_reconnected":
                    disconnected_streams.discard(stream)
    except OSError:
        return

    for stream in disconnected_streams:
        _alert(
            f"stream_disconnected_{stream}",
            f"Stream '{stream}' bağlantısı kesilmiş ve henüz yeniden bağlanmadı "
            f"(son 30 saniye içinde)",
        )


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    print("NurtacCoreEngineClaude — Watchdog")
    print(f"Poll interval : {POLL_INTERVAL_S}s")
    print(f"Data stale    : >{DATA_STALE_MS // 1000}s")
    print(f"Disk warn     : >{DISK_WARN_PCT}%")
    print(
        f"Telegram      : {'configured' if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID else 'not configured (silent)'}"
    )
    print()

    while True:
        _check_data_freshness()
        _check_disk_usage()
        _check_stream_state()
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nWatchdog stopped.")
