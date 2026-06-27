#!/usr/bin/env python3
"""
NurtacCoreEngineClaude — Watchdog Engine
Her 30 saniyede kritik dosyaların tazeliğini kontrol eder.
Stale dosya varsa nurtac-supervisor'ı restart eder.
"""
import json
import subprocess
import time
from pathlib import Path

DATA = Path("/root/NurtacCoreEngineClaude/data")

# Dosya -> max izin verilen yaş (saniye)
CHECKS = {
    "combined_1s_dna_btcusdt.jsonl": 10,
    "decision_gate_output.jsonl": 15,
    "evidence_stream.jsonl": 15,
    "structure_1s.jsonl": 15,
    "regime_context.jsonl": 15,
    "scenarios.jsonl": 15,
    "setups.jsonl": 300,
    "observations.jsonl": 300,
    "trade_brain_output.jsonl": 30,
    "bias_context.jsonl": 60,
    "liquidation_clusters.jsonl": 120,
}

RESTART_COOLDOWN = 300  # 5 dakika arayla restart
last_restart = 0


def file_age(fname: str) -> float:
    p = DATA / fname
    if not p.exists():
        return 9999.0
    try:
        raw = subprocess.getoutput(f"tail -1 {p} 2>/dev/null")
        r = json.loads(raw) if raw.strip() else {}
        ts = r.get("ts") or r.get("window_start_ts") or 0
        if ts and isinstance(ts, (int, float)):
            age = time.time() - ts / 1000
            return round(age, 1)
    except Exception:
        pass
    return round(time.time() - p.stat().st_mtime, 1)


def check_and_restart():
    global last_restart

    stale = []
    for fname, max_age in CHECKS.items():
        age = file_age(fname)
        if age > max_age:
            stale.append(f"{fname}({age:.0f}s>{max_age}s)")

    if not stale:
        print(f"[WD] OK — tüm {len(CHECKS)} dosya taze", flush=True)
        return

    now = time.time()
    if now - last_restart < RESTART_COOLDOWN:
        remaining = round(RESTART_COOLDOWN - (now - last_restart))
        print(f"[WD] STALE: {stale} — cooldown {remaining}s kaldı", flush=True)
        return

    print(f"[WD] !! STALE DETECTED: {stale}", flush=True)
    print("[WD] nurtac-supervisor restart ediliyor...", flush=True)

    result = subprocess.run(
        ["systemctl", "restart", "nurtac-supervisor"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        last_restart = now
        print(
            f"[WD] ✓ Restart başarılı @ {time.strftime('%H:%M:%S UTC', time.gmtime())}",
            flush=True,
        )
    else:
        print(f"[WD] ✗ Restart başarısız: {result.stderr}", flush=True)


if __name__ == "__main__":
    print("[WD] Watchdog Engine başlatıldı", flush=True)
    print(f"[WD] İzlenen dosya: {len(CHECKS)}", flush=True)
    print(f"[WD] Kontrol aralığı: 30s | Restart cooldown: {RESTART_COOLDOWN}s", flush=True)

    while True:
        try:
            check_and_restart()
        except Exception as e:
            print(f"[WD] Hata: {e}", flush=True)
        time.sleep(30)
