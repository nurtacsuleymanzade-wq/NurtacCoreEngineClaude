#!/usr/bin/env python3
"""Process-isolated live supervisor with bounded self-healing."""

import json
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
VENV = str(ROOT / ".venv/bin/python3")
WATCHDOG_STATUS_FILE = DATA / "supervisor_watchdog.json"

# Batch jobs (paper close, historical outcome, calibration, archive) belong to
# timers. Keeping them out of the hot supervisor prevents batch memory spikes
# from taking down the live decision pipeline.
ENGINES = [
    ("health_monitor", "tools/health_monitor.py"),
    ("rolling_window", "rolling_window_engine.py"),
    ("aligned_candle", "aligned_candle_engine.py"),
    ("smart_money", "smart_money_engine.py --mode live"),
    ("historical_baseline", "historical_baseline_engine.py --mode live"),
    ("volume_profile", "volume_profile_engine.py --mode live"),
    ("regime", "regime_engine.py --mode live"),
    ("market_context", "market_context_engine.py --mode live"),
    ("macro_context", "macro_context_engine.py --mode live"),
    ("max_pain", "max_pain_engine.py --mode live"),
    ("detector", "detector_engine.py --mode live"),
    ("decision_gate", "decision_gate.py --mode live"),
    ("liquidation", "liquidation_engine.py --mode live"),
    ("scenario", "scenario_engine.py --mode live"),
    ("evidence", "evidence_engine.py"),
    ("observer", "observer_engine.py --mode live"),
    ("paper_trade", "paper_trade_engine.py --mode live"),
    ("telegram_reporter", "telegram_reporter.py --mode live"),
    ("validator", "validator.py"),
]

ENGINE_COMMANDS = dict(ENGINES)

# Only continuously produced files are restart signals. Event-driven files such
# as setups/observations may be old during quiet markets and must never trigger
# a restart loop.
WATCHDOG_RULES = {
    DATA / "combined_1s_dna_btcusdt.jsonl": (15, None),
    DATA / "rolling_3s_dna.jsonl": (20, "rolling_window"),
    DATA / "aligned_1m_candle_dna.jsonl": (120, "aligned_candle"),
    DATA / "historical_baseline_dna.jsonl": (45, "historical_baseline"),
    DATA / "volume_profile_1s.jsonl": (45, "volume_profile"),
    DATA / "structure_1s.jsonl": (30, "smart_money"),
    DATA / "regime_context.jsonl": (30, "regime"),
    DATA / "market_context.jsonl": (60, "market_context"),
    DATA / "macro_context.json": (900, "macro_context"),
    DATA / "max_pain.json": (4200, "max_pain"),
    DATA / "labels_initiative_flow.jsonl": (30, "detector"),
    DATA / "decision_gate_output.jsonl": (30, "decision_gate"),
    DATA / "liquidation_clusters.jsonl": (90, "liquidation"),
    DATA / "scenarios.jsonl": (30, "scenario"),
    DATA / "evidence_stream.jsonl": (30, "evidence"),
    DATA / "system_health.json": (120, "health_monitor"),
}

WATCHDOG_INTERVAL_S = 20
WATCHDOG_GRACE_S = 90
WATCHDOG_FAILURES_REQUIRED = 3
WATCHDOG_RESTART_COOLDOWN_S = 300
MAX_RESTARTS_PER_10M = 3
START_INTERVAL_S = 2

procs: dict[str, subprocess.Popen] = {}
stale_strikes: dict[str, int] = {}
last_watchdog_restart: dict[str, float] = {}
restart_history: dict[str, list[float]] = {}
started_at = time.time()
shutting_down = False


def get_available_mb() -> int:
    out = subprocess.getoutput("free -m | grep '^Mem:'").split()
    try:
        return int(out[6])
    except (IndexError, TypeError, ValueError):
        return 0


def _restart_budget_available(name: str) -> bool:
    now = time.time()
    recent = [ts for ts in restart_history.get(name, []) if now - ts < 600]
    restart_history[name] = recent
    return len(recent) < MAX_RESTARTS_PER_10M


def start(name: str, script: str, *, restart: bool = False) -> subprocess.Popen | None:
    if restart and not _restart_budget_available(name):
        print(f"[SUP] {name} restart suppressed: {MAX_RESTARTS_PER_10M}/10m limit", flush=True)
        return None
    try:
        proc = subprocess.Popen([VENV] + script.split(), cwd=str(ROOT))
    except Exception as exc:
        print(f"[SUP] {name} start failed: {exc}", flush=True)
        return None
    procs[name] = proc
    if restart:
        restart_history.setdefault(name, []).append(time.time())
    print(f"[SUP] {name} started pid={proc.pid} RAM={get_available_mb()}MB", flush=True)
    return proc


def _stop_process(name: str, timeout_s: float = 5.0) -> bool:
    proc = procs.get(name)
    if proc is None or proc.poll() is not None:
        return True
    proc.terminate()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.1)
    print(f"[SUP] {name} did not stop after terminate; restart deferred", flush=True)
    return False


def restart_engine(name: str, reason: str) -> bool:
    script = ENGINE_COMMANDS.get(name)
    if script is None or not _restart_budget_available(name):
        return False
    print(f"[WATCHDOG] restarting {name}: {reason}", flush=True)
    if not _stop_process(name):
        return False
    time.sleep(1)
    return start(name, script, restart=True) is not None


def _file_age(path: Path, now: float) -> float | None:
    try:
        return max(0.0, now - path.stat().st_mtime)
    except OSError:
        return None


def _write_watchdog_status(payload: dict) -> None:
    tmp = WATCHDOG_STATUS_FILE.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(WATCHDOG_STATUS_FILE)
    except OSError as exc:
        print(f"[WATCHDOG] status write failed: {exc}", flush=True)


def watchdog_check() -> list[str]:
    now = time.time()
    restarted: list[str] = []
    links: dict[str, dict] = {}

    for path, (max_age, engine_name) in WATCHDOG_RULES.items():
        age = _file_age(path, now)
        stale = age is None or age > max_age
        key = str(path)
        stale_strikes[key] = stale_strikes.get(key, 0) + 1 if stale else 0
        links[path.name] = {
            "age_s": round(age, 1) if age is not None else None,
            "max_age_s": max_age,
            "stale": stale,
            "strikes": stale_strikes[key],
            "engine": engine_name,
        }

        if not stale or engine_name is None:
            continue
        if stale_strikes[key] < WATCHDOG_FAILURES_REQUIRED:
            continue
        if now - last_watchdog_restart.get(engine_name, 0) < WATCHDOG_RESTART_COOLDOWN_S:
            continue
        if restart_engine(engine_name, f"{path.name} age={age}"):
            last_watchdog_restart[engine_name] = now
            stale_strikes[key] = 0
            restarted.append(engine_name)
            break  # Avoid a cascading multi-engine restart in one pass.

    _write_watchdog_status({
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now)),
        "grace_complete": now - started_at >= WATCHDOG_GRACE_S,
        "links": links,
        "restarted": restarted,
    })
    return restarted


def shutdown(_sig=None, _frame=None) -> None:
    global shutting_down
    shutting_down = True
    print("[SUP] shutting down children", flush=True)
    for name in list(procs):
        _stop_process(name, timeout_s=3.0)
    raise SystemExit(0)


def main() -> None:
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    print(f"[SUP] starting {len(ENGINES)} isolated live engines", flush=True)
    for name, script in ENGINES:
        start(name, script)
        time.sleep(START_INTERVAL_S)

    print("[SUP] live engines started; watchdog active", flush=True)
    last_watchdog = 0.0
    while not shutting_down:
        for name, script in ENGINES:
            proc = procs.get(name)
            if proc is None or proc.poll() is not None:
                start(name, script, restart=True)

        now = time.time()
        if now - started_at >= WATCHDOG_GRACE_S and now - last_watchdog >= WATCHDOG_INTERVAL_S:
            watchdog_check()
            last_watchdog = now
        time.sleep(5)


if __name__ == "__main__":
    main()
