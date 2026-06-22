"""
NurtacCoreEngineClaude — Validator Engine

Tail-follows the 6 aligned candle JSONL files (1M, 5M, 15M, 1H, 4H, 1D).
For each new closed candle, fetches the corresponding Binance kline via
REST and compares close/high/low. Results are written to
data/validation_report.jsonl.

HALT conditions:
  (a) Any 1D candle reaches status="critical"
  (b) 3 consecutive critical candles for the same timeframe

On HALT: creates data/SYSTEM_HALT and exits. The other engines check for
this file each iteration and shut down gracefully.

Run in its own terminal:
  python validator.py

Optional Telegram alerts:
  set TELEGRAM_BOT_TOKEN=<token>
  set TELEGRAM_CHAT_ID=<chat_id>
  python validator.py
"""

import json
import os
import sys
import time
import threading
from typing import Optional

# Ensure UTF-8 terminal output on Windows (cp1252 can't encode Turkish chars)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library not installed. Run: pip install requests")
    sys.exit(1)

from notify import send_telegram

# ── Config ─────────────────────────────────────────────────────────────────────
DATA_DIR    = "data"
HALT_FILE   = os.path.join(DATA_DIR, "SYSTEM_HALT")
REPORT_FILE = os.path.join(DATA_DIR, "validation_report.jsonl")

ALIGNED_FILES = {
    "1M":  os.path.join(DATA_DIR, "aligned_1m_candle_dna.jsonl"),
    "5M":  os.path.join(DATA_DIR, "aligned_5m_candle_dna.jsonl"),
    "15M": os.path.join(DATA_DIR, "aligned_15m_candle_dna.jsonl"),
    "1H":  os.path.join(DATA_DIR, "aligned_1h_candle_dna.jsonl"),
    "4H":  os.path.join(DATA_DIR, "aligned_4h_candle_dna.jsonl"),
    "1D":  os.path.join(DATA_DIR, "aligned_1d_candle_dna.jsonl"),
}

BINANCE_INTERVAL = {
    "1M": "1m", "5M": "5m", "15M": "15m",
    "1H": "1h", "4H": "4h", "1D":  "1d",
}

BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"

THRESHOLDS = {
    "1M":  {"warning": 0.05,  "critical": 0.5},
    "5M":  {"warning": 0.04,  "critical": 0.4},
    "15M": {"warning": 0.03,  "critical": 0.3},
    "1H":  {"warning": 0.02,  "critical": 0.2},
    "4H":  {"warning": 0.05,  "critical": 0.5},
    "1D":  {"warning": 0.01,  "critical": 0.1},
}

POLL_INTERVAL      = 0.05
FILE_WAIT_INTERVAL = 0.5
RETRY_DELAYS       = [1, 2, 4]  # seconds between successive retry attempts

# Per-timeframe consecutive-critical streak counters (written by one thread each)
_consecutive_critical: dict[str, int] = {tf: 0 for tf in ALIGNED_FILES}

_report_lock  = threading.Lock()
_halt_lock    = threading.Lock()
_halt_written = False


# ── Binance REST ───────────────────────────────────────────────────────────────
def _fetch_binance_kline(
    timeframe: str, start_ms: int, end_ms: int
) -> Optional[list]:
    """Fetch one Binance kline. Returns raw kline array or None after retries."""
    params = {
        "symbol":    "BTCUSDT",
        "interval":  BINANCE_INTERVAL[timeframe],
        "startTime": start_ms,
        "endTime":   end_ms - 1,  # endTime inclusive; -1 avoids spilling into next bar
        "limit":     1,
    }
    all_delays = [0] + RETRY_DELAYS  # first attempt has no pre-sleep
    for attempt, delay in enumerate(all_delays):
        if delay:
            time.sleep(delay)
        try:
            resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data:
                return data[0]
        except Exception as exc:
            if attempt < len(RETRY_DELAYS):
                print(
                    f"[VALIDATOR] Binance fetch attempt {attempt + 1} failed "
                    f"({exc}), retrying in {RETRY_DELAYS[attempt]}s..."
                )
    return None


# ── Thread-safe report writer ──────────────────────────────────────────────────
def _write_report(entry: dict) -> None:
    with _report_lock:
        with open(REPORT_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())


# ── HALT trigger ───────────────────────────────────────────────────────────────
def _trigger_halt(reason: str, trigger_entry: dict) -> None:
    global _halt_written
    with _halt_lock:
        if _halt_written:
            return
        halt_content = {
            "ts":      int(time.time() * 1000),
            "reason":  reason,
            "trigger": trigger_entry,
        }
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            with open(HALT_FILE, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(halt_content, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        except OSError as exc:
            print(f"[VALIDATOR] ERROR: Could not write SYSTEM_HALT: {exc}")
            return
        _halt_written = True

    msg = f"SYSTEM HALT TETİKLENDİ: {reason}"
    print(f"[VALIDATOR] {msg}")
    send_telegram("system_halt", f"[NurtacCoreEngine] {msg}")


# ── Per-candle comparison ──────────────────────────────────────────────────────
def _validate_candle(timeframe: str, candle: dict) -> None:
    """Compare one aligned candle against the Binance kline and log the result."""
    wts = candle["window_start_ts"]
    wet = candle["window_end_ts"]

    # Skip very incomplete candles (startup partial windows) to avoid false alarms
    completeness = candle.get("data_completeness", 1.0)
    if completeness < 0.9:
        print(
            f"[VALIDATOR] {timeframe} ts={wts} skipped "
            f"(data_completeness={completeness:.2f} < 0.90)"
        )
        return

    ohlc = candle.get("ohlc", {})
    our_close = ohlc.get("close")
    our_high  = ohlc.get("high")
    our_low   = ohlc.get("low")

    # OHLC objects are {"price": float, ...} at all timeframes
    our_close_p = our_close["price"] if our_close is not None else None
    our_high_p  = our_high["price"]  if our_high  is not None else None
    our_low_p   = our_low["price"]   if our_low   is not None else None

    if our_close_p is None or our_high_p is None or our_low_p is None:
        print(f"[VALIDATOR] {timeframe} ts={wts} skipped (null OHLC — no trades)")
        return

    kline = _fetch_binance_kline(timeframe, wts, wet)

    entry: dict = {
        "ts":              int(time.time() * 1000),
        "timeframe":       timeframe,
        "window_start_ts": wts,
        "window_end_ts":   wet,
        "ours": {
            "close": our_close_p,
            "high":  our_high_p,
            "low":   our_low_p,
        },
    }

    if kline is None:
        entry["binance"]  = None
        entry["diff_pct"] = None
        entry["status"]   = "binance_unreachable"
        _write_report(entry)
        print(f"[VALIDATOR] {timeframe} ts={wts} status=binance_unreachable")
        return

    # kline layout: [openTime, open, high, low, close, volume, closeTime, ...]
    bnc_high  = float(kline[2])
    bnc_low   = float(kline[3])
    bnc_close = float(kline[4])

    def _pct(ours: float, bnc: float) -> float:
        return abs(ours - bnc) / bnc * 100 if bnc else 0.0

    diff_close = _pct(our_close_p, bnc_close)
    diff_high  = _pct(our_high_p,  bnc_high)
    diff_low   = _pct(our_low_p,   bnc_low)
    max_diff   = max(diff_close, diff_high, diff_low)

    thresh = THRESHOLDS[timeframe]
    if max_diff > thresh["critical"]:
        status = "critical"
    elif max_diff > thresh["warning"]:
        status = "warning"
    else:
        status = "ok"

    entry["binance"]  = {"close": bnc_close, "high": bnc_high, "low": bnc_low}
    entry["diff_pct"] = {"close": diff_close, "high": diff_high, "low": diff_low}
    entry["status"]   = status

    _write_report(entry)
    print(
        f"[VALIDATOR] {timeframe} ts={wts} "
        f"close={diff_close:.4f}% high={diff_high:.4f}% "
        f"low={diff_low:.4f}% status={status}"
    )

    # Update consecutive-critical streak
    if status == "critical":
        _consecutive_critical[timeframe] += 1
    else:
        _consecutive_critical[timeframe] = 0

    # HALT condition (a): any 1D critical — standalone trigger
    if status == "critical" and timeframe == "1D":
        _trigger_halt(
            f"1D candle critical divergence: max_diff={max_diff:.4f}%",
            entry,
        )
        return

    # HALT condition (b): 3 consecutive critical for the same timeframe
    if _consecutive_critical[timeframe] >= 3:
        _trigger_halt(
            f"{timeframe} 3 ardisik critical: "
            f"streak={_consecutive_critical[timeframe]} max_diff={max_diff:.4f}%",
            entry,
        )


# ── Per-timeframe tail follower thread ────────────────────────────────────────
def _follow_timeframe(timeframe: str) -> None:
    path = ALIGNED_FILES[timeframe]

    while not os.path.exists(path):
        if os.path.exists(HALT_FILE):
            return
        print(f"[VALIDATOR] Waiting for {path}...")
        time.sleep(FILE_WAIT_INTERVAL)

    print(f"[VALIDATOR] {timeframe}: following {path}")
    with open(path, "r", encoding="utf-8") as fh:
        while True:
            if os.path.exists(HALT_FILE):
                print(f"[VALIDATOR] {timeframe}: SYSTEM_HALT detected, stopping thread.")
                return

            line = fh.readline()
            if line:
                line = line.rstrip("\n")
                if not line:
                    continue
                try:
                    candle = json.loads(line)
                except json.JSONDecodeError:
                    continue
                _validate_candle(timeframe, candle)
            else:
                time.sleep(POLL_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    # Refuse to start if SYSTEM_HALT already exists
    if os.path.exists(HALT_FILE):
        try:
            with open(HALT_FILE, "r", encoding="utf-8") as fh:
                halt_info = json.loads(fh.read().strip())
            reason = halt_info.get("reason", "unknown")
        except Exception:
            reason = "unknown"
        print(f"SYSTEM_HALT tespit edildi, {reason}, program durduruluyor")
        sys.exit(1)

    print("NurtacCoreEngineClaude — Validator Engine")
    print(f"Report : {REPORT_FILE}")
    print(f"Halt   : {HALT_FILE}")
    print(f"Tracks : {', '.join(ALIGNED_FILES)}")
    print()

    threads = [
        threading.Thread(
            target=_follow_timeframe,
            args=(tf,),
            daemon=True,
            name=f"validator-{tf}",
        )
        for tf in ALIGNED_FILES
    ]
    for t in threads:
        t.start()

    try:
        while True:
            if os.path.exists(HALT_FILE):
                print("[VALIDATOR] SYSTEM_HALT detected. Exiting.")
                sys.exit(1)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[VALIDATOR] Stopping.")


if __name__ == "__main__":
    main()
