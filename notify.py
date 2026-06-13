"""
NurtacCoreEngineClaude — Shared Telegram notifier.

Imported by validator.py and watchdog.py. Silently skips if
TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are not set, or if the
requests library is not installed.
"""

import os
import time
import threading

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_COOLDOWN  = 300  # seconds; minimum gap between same-type messages

_tg_last_sent: dict[str, float] = {}
_tg_lock = threading.Lock()


def send_telegram(alert_type: str, message: str) -> None:
    """Send a Telegram message with per-type cooldown."""
    if not (_REQUESTS_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    now = time.time()
    with _tg_lock:
        if now - _tg_last_sent.get(alert_type, 0.0) < TELEGRAM_COOLDOWN:
            return
        _tg_last_sent[alert_type] = now
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        _requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
    except Exception:
        pass
