#!/usr/bin/env python3
"""
RAM Lifecycle Manager — Her 30 saniyede çalışır.
RAM durumuna göre servisleri otomatik yönetir.
Manuel müdahale gerektirmez.
"""
import subprocess, time, json
from pathlib import Path

LOG = Path("/root/NurtacCoreEngineClaude/data/ram_lifecycle_log.jsonl")

# RAM eşikleri (MB)
LEVEL_OK       = 2500   # available > 2500MB → tüm servisler açık
LEVEL_WARN     = 1500   # available < 1500MB → batch servisleri durdur
LEVEL_CRITICAL = 800    # available < 800MB  → heavy engingleri de durdur
LEVEL_EMERGENCY = 400   # available < 400MB  → supervisor'ı yeniden başlat

BATCH_SERVICES = [
    "nurtac-paper-close-engine.timer",
    "nurtac-historical-outcome-feed.timer",
    "nurtac-calibration-feed.timer",
    "nurtac-curated-telegram.timer",
    "nurtac-paper-live-closer.timer",
]

HEAVY_PROCESSES = [
    "paper_close_engine.py",
    "setup_guardian_reporter.py",
    "watchdog.py",
]

def run(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def get_available_mb():
    out = subprocess.getoutput("free -m | grep '^Mem:'").split()
    try:
        return int(out[6])  # available column
    except:
        return 9999

def service_active(svc):
    r = subprocess.getoutput(f"systemctl is-active {svc} 2>/dev/null")
    return r.strip() == "active"

def stop_batch():
    for svc in BATCH_SERVICES:
        run(f"systemctl stop {svc} 2>/dev/null")

def start_batch():
    for svc in BATCH_SERVICES:
        run(f"systemctl start {svc} 2>/dev/null")

def stop_heavy_procs():
    for proc in HEAVY_PROCESSES:
        run(f"pkill -f {proc} 2>/dev/null")

def restart_supervisor():
    run("systemctl restart nurtac-supervisor")

def log_event(level, available_mb, action):
    event = {
        "ts": int(time.time()*1000),
        "level": level,
        "available_mb": available_mb,
        "action": action
    }
    with open(LOG, "a") as f:
        f.write(json.dumps(event) + "\n")
    print(f"[RAM-LC] {level}: {available_mb}MB available → {action}")

def main():
    available = get_available_mb()

    if available >= LEVEL_OK:
        # Her şey normal — batch servisleri başlat (durdurulmuşlarsa)
        if not service_active(BATCH_SERVICES[0]):
            start_batch()
            log_event("OK", available, "batch_services_resumed")
        else:
            log_event("OK", available, "no_action")

    elif available >= LEVEL_WARN:
        # Uyarı — sadece izle, henüz bir şey yapma
        log_event("WARN", available, "monitoring")

    elif available >= LEVEL_CRITICAL:
        # Kritik — batch servisleri durdur
        stop_batch()
        log_event("CRITICAL", available, "batch_services_stopped")

    elif available >= LEVEL_EMERGENCY:
        # Çok kritik — ağır processleri de öldür
        stop_batch()
        stop_heavy_procs()
        log_event("VERY_CRITICAL", available, "heavy_procs_killed")

    else:
        # Acil — supervisor'ı yeniden başlat (memory temizlenir)
        stop_batch()
        stop_heavy_procs()
        restart_supervisor()
        log_event("EMERGENCY", available, "supervisor_restarted")

if __name__ == "__main__":
    main()
