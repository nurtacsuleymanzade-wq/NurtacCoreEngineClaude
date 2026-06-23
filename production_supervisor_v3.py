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
    ("smart_money", "smart_money_engine.py --mode live"),
    ("aligned_candle", "aligned_candle_engine.py"),
    ("historical_baseline", "historical_baseline_engine.py --mode live"),
    ("observer", "observer_engine.py --mode live"),
    ("validator", "validator.py"),
    ("market_context", "market_context_engine.py"),
    ("detector",       "detector_engine.py --mode live"),
    ("evidence",       "evidence_engine.py"),
]

procs = {}

def get_available_mb():
    import subprocess as _sub
    out = _sub.getoutput("free -m | grep '^Mem:'").split()
    return int(out[6]) if len(out) > 6 else 9999

def start(name, script):
    # RAM kontrolü — 800MB altındaysa bekle
    avail = get_available_mb()
    waited = 0
    while avail < 800 and waited < 60:
        print(f"[SUP] RAM low ({avail}MB), waiting 10s before starting {name}...", flush=True)
        time.sleep(10)
        waited += 10
        avail = get_available_mb()
    p = subprocess.Popen([VENV] + script.split(), cwd=str(ROOT))
    procs[name] = p
    print(f"[SUP] {name} started pid={p.pid} (RAM:{avail}MB)", flush=True)
    time.sleep(5)  # Engine'in başlaması için ekstra bekleme

def shutdown(sig, frame):
    for p in procs.values(): p.terminate()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

for name, script in ENGINES:
    start(name, script)
    time.sleep(3)

print("[SUP] Core engines running", flush=True)
import subprocess as _sp
_hp = _sp.Popen([VENV, "tools/health_monitor.py"], cwd=str(ROOT))
print(f"[SUP] Health monitor pid={_hp.pid}", flush=True)

while True:
    for name, script in ENGINES:
        if procs[name].poll() is not None:
            print(f"[SUP] Restarting {name}", flush=True)
            time.sleep(5)
            start(name, script)
    time.sleep(30)
