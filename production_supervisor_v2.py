#!/usr/bin/env python3
"""
Supervisor v2: Her engine ayrı subprocess olarak çalışır.
RAM izolasyonu sağlar. OOM bir engine'i öldürürse diğerleri etkilenmez.
"""
import subprocess, time, os, signal, sys
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
VENV = str(ROOT / ".venv/bin/python3")

# Sadece core pipeline — RAM bütçesi: ~1.5GB toplam
ENGINES = [
    ("rolling_window_engine",   "rolling_window_engine.py",   60),
    ("aligned_candle_engine",   "aligned_candle_engine.py",   60),
    ("historical_baseline",     "historical_baseline_engine.py", 90),
    ("detector_engine",         "detector_engine.py",         30),
    ("evidence_engine",         "evidence_engine.py",         45),
    ("trade_brain_engine",      "trade_brain_engine.py",      50),
    ("observer_engine",         "observer_engine.py --mode live", 60),
]

processes = {}

def start_engine(name, script, delay):
    time.sleep(delay / 10)
    cmd = [VENV] + script.split()
    p = subprocess.Popen(cmd, cwd=str(ROOT))
    processes[name] = p
    print(f"[SUP] Started {name} (pid={p.pid})", flush=True)
    return p

def check_and_restart(name, script, delay):
    p = processes.get(name)
    if p is None or p.poll() is not None:
        print(f"[SUP] Restarting {name}...", flush=True)
        time.sleep(5)
        processes[name] = start_engine(name, script, 0)

def shutdown(sig, frame):
    print("[SUP] Shutting down...", flush=True)
    for name, p in processes.items():
        p.terminate()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

print(f"[SUP] Starting {len(ENGINES)} engines as separate processes", flush=True)

for name, script, delay in ENGINES:
    start_engine(name, script, delay)
    time.sleep(2)

print("[SUP] All engines started. Monitoring...", flush=True)

while True:
    for name, script, delay in ENGINES:
        check_and_restart(name, script, delay)
    time.sleep(30)
