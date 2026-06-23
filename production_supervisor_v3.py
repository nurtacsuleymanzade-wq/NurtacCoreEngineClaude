#!/usr/bin/env python3
"""
Supervisor v3: Sadece 3 core engine.
Toplam RAM hedefi: <1.5GB
"""
import subprocess, time, signal, sys
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
VENV = str(ROOT / ".venv/bin/python3")

# SADECE 3 ENGINE — toplam ~800MB hedef
ENGINES = [
    ("rolling_window", "rolling_window_engine.py"),
    ("detector",       "detector_engine.py --mode live"),
    ("evidence",       "evidence_engine.py"),
]

procs = {}

def start(name, script):
    p = subprocess.Popen([VENV] + script.split(), cwd=str(ROOT))
    procs[name] = p
    print(f"[SUP] {name} started pid={p.pid}", flush=True)

def shutdown(sig, frame):
    for p in procs.values(): p.terminate()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

for name, script in ENGINES:
    start(name, script)
    time.sleep(3)

print("[SUP] Core engines running", flush=True)

while True:
    for name, script in ENGINES:
        if procs[name].poll() is not None:
            print(f"[SUP] Restarting {name}", flush=True)
            time.sleep(5)
            start(name, script)
    time.sleep(30)
