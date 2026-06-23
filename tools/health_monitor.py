#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

DATA = Path("/root/NurtacCoreEngineClaude/data")
HEALTH_FILE = DATA / "system_health.json"

def get_ram():
    parts = subprocess.getoutput("free -m | grep '^Mem:'").split()
    return int(parts[6])

def get_age(filepath, ts_field="ts"):
    try:
        raw = subprocess.getoutput(f"tail -1 {filepath}").strip()
        r = json.loads(raw)
        ts = r.get(ts_field) or r.get("window_start_ts") or 0
        return round(time.time() - ts/1000, 1) if ts else None
    except:
        return None

def run():
    available = get_ram()
    halt = (DATA / "SYSTEM_HALT").exists()
    ev_age = get_age(DATA / "evidence_stream.jsonl")
    
    status = "OK"
    alerts = []
    if available < 500: status = "CRITICAL"; alerts.append(f"RAM:{available}MB")
    elif available < 1000: status = "WARN"; alerts.append(f"RAM LOW:{available}MB")
    if halt: status = "CRITICAL"; alerts.append("SYSTEM_HALT")
    if ev_age and ev_age > 30: status = "WARN" if status=="OK" else status; alerts.append(f"evidence_stale:{ev_age:.0f}s")
    
    report = {
        "status": status, "alerts": alerts,
        "ram_available_mb": available,
        "system_halt": halt,
        "evidence_age_s": ev_age,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    }
    HEALTH_FILE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[HEALTH] {status} RAM:{available}MB evidence:{ev_age}s halt:{halt}")

if __name__ == "__main__":
    while True:
        try: run()
        except Exception as e: print(f"[HEALTH] err: {e}")
        time.sleep(60)
