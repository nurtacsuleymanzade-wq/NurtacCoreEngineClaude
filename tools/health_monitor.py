#!/usr/bin/env python3
"""Low-overhead host and pipeline health snapshot."""

import json
import subprocess
import time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
HEALTH_FILE = DATA / "system_health.json"
CGROUP_MEMORY = Path("/sys/fs/cgroup/system.slice/nurtac-supervisor.service/memory.current")
CGROUP_MEMORY_MAX = Path("/sys/fs/cgroup/system.slice/nurtac-supervisor.service/memory.max")

CHAIN_RULES = {
    "L0_dna_age_s": (DATA / "combined_1s_dna_btcusdt.jsonl", 15, True),
    "L1_rolling_age_s": (DATA / "rolling_3s_dna.jsonl", 20, True),
    "L2_candle_age_s": (DATA / "aligned_1m_candle_dna.jsonl", 120, True),
    "L3_baseline_age_s": (DATA / "historical_baseline_dna.jsonl", 45, True),
    "L4_detector_age_s": (DATA / "labels_initiative_flow.jsonl", 30, True),
    "L5_gate_age_s": (DATA / "decision_gate_output.jsonl", 30, True),
    "L6_structure_age_s": (DATA / "structure_1s.jsonl", 30, True),
    "L7_evidence_age_s": (DATA / "evidence_stream.jsonl", 30, True),
    "L7_setup_age_s": (DATA / "setups.jsonl", 30, False),
    "L9_scenario_age_s": (DATA / "scenarios.jsonl", 30, True),
    "L10_obs_age_s": (DATA / "observations.jsonl", 60, False),
    "liq_cluster_age_s": (DATA / "liquidation_clusters.jsonl", 90, True),
    "whale_age_s": (DATA / "whale_orders.jsonl", 60, False),
    "regime_age_s": (DATA / "regime_context.jsonl", 30, True),
    "macro_age_s": (DATA / "macro_context.json", 900, True),
}


def _memory() -> tuple[int, int]:
    lines = subprocess.getoutput("free -m").splitlines()
    available = 0
    swap_used = 0
    for line in lines:
        parts = line.split()
        if parts and parts[0] == "Mem:":
            try:
                available = int(parts[6])
            except (IndexError, TypeError, ValueError):
                pass
        elif parts and parts[0] == "Swap:":
            try:
                swap_used = int(parts[2])
            except (IndexError, TypeError, ValueError):
                pass
    return available, swap_used


def _read_int(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return int(raw) if raw != "max" else None
    except (OSError, TypeError, ValueError):
        return None


def _record_age(path: Path, now: float) -> float | None:
    try:
        raw = subprocess.getoutput(f"tail -1 {path} 2>/dev/null").strip()
        payload = json.loads(raw)
        ts = payload.get("ts") or payload.get("window_start_ts")
        if ts:
            return round(max(0.0, now - float(ts) / 1000), 1)
        return round(max(0.0, now - path.stat().st_mtime), 1)
    except Exception:
        try:
            return round(max(0.0, now - path.stat().st_mtime), 1)
        except OSError:
            return None


def _build_chain_health(now: float) -> dict:
    chain: dict[str, object] = {}
    broken: list[str] = []
    statuses: dict[str, dict] = {}
    for key, (path, max_age, continuous) in CHAIN_RULES.items():
        age = _record_age(path, now)
        stale = age is None or age > max_age
        chain[key] = age if age is not None else 9999.0
        statuses[key] = {
            "max_age_s": max_age,
            "continuous": continuous,
            "stale": stale,
        }
        if continuous and stale:
            broken.append(key)
    chain["chain_broken"] = bool(broken)
    chain["broken_links"] = broken
    chain["status"] = statuses
    return chain


def _write_report(report: dict) -> None:
    tmp = HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    tmp.replace(HEALTH_FILE)


def run() -> None:
    now = time.time()
    available, swap_used = _memory()
    halt = (DATA / "SYSTEM_HALT").exists()
    chain = _build_chain_health(now)
    cgroup_current = _read_int(CGROUP_MEMORY)
    cgroup_max = _read_int(CGROUP_MEMORY_MAX)
    cgroup_pct = (
        round(cgroup_current / cgroup_max * 100, 1)
        if cgroup_current is not None and cgroup_max else None
    )

    status = "OK"
    alerts: list[str] = []
    if available < 500:
        status = "CRITICAL"
        alerts.append(f"RAM:{available}MB")
    elif available < 1000:
        status = "WARN"
        alerts.append(f"RAM_LOW:{available}MB")
    if swap_used > 1500:
        status = "CRITICAL"
        alerts.append(f"SWAP_CRITICAL:{swap_used}MB")
    elif swap_used > 500:
        status = "WARN" if status == "OK" else status
        alerts.append(f"SWAP_HIGH:{swap_used}MB")
    if cgroup_pct is not None and cgroup_pct > 95:
        status = "CRITICAL"
        alerts.append(f"CGROUP_MEMORY:{cgroup_pct}%")
    elif cgroup_pct is not None and cgroup_pct > 85:
        status = "WARN" if status == "OK" else status
        alerts.append(f"CGROUP_MEMORY:{cgroup_pct}%")
    if halt:
        status = "CRITICAL"
        alerts.append("SYSTEM_HALT")
    if chain.get("chain_broken"):
        status = "WARN" if status == "OK" else status
        alerts.append("CHAIN_BROKEN")

    report = {
        "status": status,
        "alerts": alerts,
        "ram_available_mb": available,
        "swap_used_mb": swap_used,
        "supervisor_memory_mb": round(cgroup_current / 1048576, 1) if cgroup_current else None,
        "supervisor_memory_max_mb": round(cgroup_max / 1048576, 1) if cgroup_max else None,
        "supervisor_memory_pct": cgroup_pct,
        "system_halt": halt,
        "evidence_age_s": chain.get("L7_evidence_age_s"),
        "chain": chain,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now)),
    }
    _write_report(report)
    print(
        f"[HEALTH] {status} RAM:{available}MB swap:{swap_used}MB "
        f"chain_broken={chain.get('chain_broken')} halt:{halt}",
        flush=True,
    )


if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as exc:
            print(f"[HEALTH] err: {exc}", flush=True)
        time.sleep(60)
