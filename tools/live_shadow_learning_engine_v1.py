#!/usr/bin/env python3
import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

PAPER_TRADES = DATA / "paper_trades.jsonl"
STATE_FILE = DATA / "live_shadow_learning_state.json"
HEALTH = DATA / "live_shadow_learning_health.json"
AUDIT = DATA / "live_shadow_learning_audit.jsonl"
REPORT = DATA / "live_shadow_learning_report.md"
SNAPSHOT = DATA / "live_shadow_learning_snapshot.json"
SNAPSHOT_JSONL = DATA / "live_shadow_learning_snapshot.jsonl"

PROD_DIFF_FILES = [
    ROOT / "trade_brain_engine.py",
    ROOT / "trade_brain_v2.py",
    ROOT / "paper_trade_engine.py",
    ROOT / "observer_engine.py",
    ROOT / "telegram_reporter.py",
]

PIPELINE_STEPS = [
    ("outcome_join", ["python3", "tools/trade_decision_outcome_join.py", "--mode", "batch"]),
    ("outcome_explanation", ["python3", "tools/trade_outcome_explanation_engine.py", "--mode", "batch"]),
    ("evidence_scoring", ["python3", "tools/evidence_scoring_engine.py", "--mode", "batch"]),
    ("probability_surface", ["python3", "tools/probability_surface_engine.py", "--mode", "batch"]),
    ("calibration_v2", ["python3", "tools/calibration_engine_v2.py", "--mode", "batch"]),
    ("learning_candidate", ["python3", "tools/trade_brain_learning_engine_v1.py", "--mode", "batch"]),
    ("weight_simulation", ["python3", "tools/weight_simulation_approval_pipeline.py", "--mode", "batch"]),
    ("learning_promotion", ["python3", "tools/learning_promotion_engine_v1.py", "--mode", "batch"]),
    ("weight_registry", ["python3", "tools/weight_registry_builder_v1.py", "--mode", "batch"]),
    ("trade_brain_v2", ["python3", "tools/trade_brain_v2.py", "--mode", "batch"]),
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _tail_text(path: Path, lines: int = 20) -> str:
    if not path.exists():
        return ""
    data = path.read_text().splitlines()[-lines:]
    return "\n".join(data)


def _sf(v: Any, default: float | None = None) -> float | None:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _find_last_closed_trade() -> dict[str, Any] | None:
    rows = _read_jsonl(PAPER_TRADES)
    for trade in reversed(rows):
        tid = trade.get("trade_id")
        if not tid:
            continue
        close_ts = _sf(trade.get("close_ts"), None)
        outcome = trade.get("outcome") or trade.get("results", {}).get("outcome")
        close_reason = trade.get("close_reason")
        if close_ts is None and not outcome and not close_reason:
            continue
        if trade.get("status") in {"open", "pending"} and close_ts is None and not outcome:
            continue
        return trade
    return None


def _load_state() -> dict[str, Any]:
    state = _read_json(STATE_FILE)
    if not state:
        state = {
            "engine": "live_shadow_learning_engine_v1",
            "last_seen_trade_id": None,
            "last_seen_close_ts": None,
            "last_processed_trade_id": None,
            "last_processed_close_ts": None,
            "processed_trade_ids": [],
            "last_run_ts": None,
            "mode": "once",
        }
    return state


def _save_state(state: dict[str, Any]) -> None:
    state["processed_trade_ids"] = list(dict.fromkeys((state.get("processed_trade_ids") or [])[-1000:]))
    _write_json(STATE_FILE, state)


def _run_step(step: str, cmd: list[str]) -> dict[str, Any]:
    start = time.time()
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=120)
        status = "success" if proc.returncode == 0 else "failed"
        rc = proc.returncode
        stdout_tail = (proc.stdout or "")[-1000:]
        stderr_tail = (proc.stderr or "")[-1000:]
        warning = None if status == "success" else f"{step}_return_code_{rc}"
    except FileNotFoundError:
        status = "skipped"
        rc = 127
        stdout_tail = ""
        stderr_tail = ""
        warning = f"{step}_tool_missing"
    except subprocess.TimeoutExpired as e:
        status = "failed"
        rc = 124
        stdout_tail = (e.stdout or "")[-1000:] if isinstance(e.stdout, str) else ""
        stderr_tail = (e.stderr or "")[-1000:] if isinstance(e.stderr, str) else ""
        warning = f"{step}_timeout"
    return {
        "step": step,
        "status": status,
        "return_code": rc,
        "duration_ms": int((time.time() - start) * 1000),
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "warning": warning,
    }


def _safety_diff_check() -> list[str]:
    warnings = []
    try:
        diff = subprocess.run(
            ["git", "diff", "--", "trade_brain_engine.py", "trade_brain_v2.py", "paper_trade_engine.py", "observer_engine.py", "telegram_reporter.py"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if diff.stdout.strip():
            warnings.append("production_file_modified_unexpectedly")
    except Exception:
        warnings.append("safety_diff_check_failed")
    return warnings


def _write_snapshot(snapshot: dict[str, Any]) -> None:
    _write_json(SNAPSHOT, snapshot)
    _write_jsonl(SNAPSHOT_JSONL, snapshot)


def _report(snapshot: dict[str, Any], steps: list[dict[str, Any]]) -> None:
    trade = snapshot.get("trade") or {}
    lines = [
        "# Live Shadow Learning Report",
        "",
        "## Status",
        f"- shadow_learning_status: {snapshot.get('shadow_learning_status')}",
        f"- live_weight_changed: {snapshot.get('live_weight_changed')}",
        f"- trade_brain_changed: {snapshot.get('trade_brain_changed')}",
        f"- trade_logic_changed: {snapshot.get('trade_logic_changed')}",
        "",
        "## Last Closed Trade",
        f"- trade_id: {trade.get('trade_id')}",
        f"- symbol: {trade.get('symbol')}",
        f"- direction: {trade.get('direction')}",
        f"- outcome: {trade.get('outcome')}",
        f"- close_reason: {trade.get('close_reason')}",
        f"- pnl_r: {trade.get('pnl_r')}",
        "",
        "## Pipeline Execution",
        "\n".join(f"- {s['step']}: {s['status']} rc={s['return_code']}" for s in steps) or "- none",
        "",
        "## Outcome Join",
        f"- updated: {snapshot['learning_outputs'].get('join_updated')}",
        "",
        "## Outcome Explanation",
        f"- updated: {snapshot['learning_outputs'].get('outcome_explanation_updated')}",
        "",
        "## Evidence Scoring",
        f"- updated: {snapshot['learning_outputs'].get('evidence_scores_updated')}",
        "",
        "## Probability Surface",
        f"- updated: {snapshot['learning_outputs'].get('probability_surface_updated')}",
        "",
        "## Calibration",
        f"- updated: {snapshot['learning_outputs'].get('calibration_updated')}",
        "",
        "## Learning Candidates",
        f"- updated: {snapshot['learning_outputs'].get('learning_candidates_updated')}",
        "",
        "## Weight Simulation",
        f"- updated: {snapshot['learning_outputs'].get('weight_simulation_updated')}",
        "",
        "## Learning Promotion",
        f"- updated: {snapshot['learning_outputs'].get('promotion_registry_updated')}",
        "",
        "## Weight Registry",
        f"- updated: {snapshot['learning_outputs'].get('weight_registry_updated')}",
        "",
        "## Trade Brain V2 Shadow Decision",
        f"- updated: {snapshot['learning_outputs'].get('trade_brain_v2_updated')}",
        "",
        "## Safety",
        "- The engine is measurement-only.",
        "- Production decision behavior is unchanged.",
        "",
        "## No Live Weight Change Guarantee",
        "No live Trade Brain weights were changed.",
        "This is shadow learning only.",
        "Production decision behavior is unchanged.",
        "",
        "## Next Recommended Step",
        "- Keep the loop running or rerun once after the next closed paper trade.",
    ]
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_once(force: bool = False) -> dict[str, Any]:
    ts = int(time.time() * 1000)
    state = _load_state()
    last_trade = _find_last_closed_trade()
    if not last_trade:
        snapshot = {
            "engine": "live_shadow_learning_engine_v1",
            "generated_ts": ts,
            "mode": "once",
            "processed_new_trade": False,
            "trade": None,
            "pipeline_steps": [],
            "learning_outputs": {
                "join_updated": False,
                "outcome_explanation_updated": False,
                "evidence_scores_updated": False,
                "probability_surface_updated": False,
                "calibration_updated": False,
                "learning_candidates_updated": False,
                "weight_simulation_updated": False,
                "promotion_registry_updated": False,
                "weight_registry_updated": False,
                "trade_brain_v2_updated": False,
            },
            "shadow_learning_status": "no_closed_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "warnings": ["no_closed_trade"],
        }
        _write_snapshot(snapshot)
        _write_json(HEALTH, {
            "status": "no_new_trade",
            "last_run_ts": ts,
            "mode": "once",
            "last_seen_trade_id": state.get("last_seen_trade_id"),
            "last_processed_trade_id": state.get("last_processed_trade_id"),
            "processed_new_trade": False,
            "steps_total": 0,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "last_blocker": "no_closed_trade",
            "warnings": ["no_closed_trade"],
        })
        _write_jsonl(AUDIT, {
            "ts": ts,
            "engine": "live_shadow_learning_engine_v1",
            "mode": "once",
            "trade_id": None,
            "processed_new_trade": False,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "shadow_learning_status": "no_closed_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "warnings": ["no_closed_trade"],
        })
        _report(snapshot, [])
        state["last_run_ts"] = ts
        _save_state(state)
        return snapshot

    trade_id = str(last_trade.get("trade_id"))
    close_ts = _sf(last_trade.get("close_ts"), None)
    if not force and trade_id in set(state.get("processed_trade_ids") or []):
        snapshot = {
            "engine": "live_shadow_learning_engine_v1",
            "generated_ts": ts,
            "mode": "once",
            "processed_new_trade": False,
            "trade": {
                "trade_id": trade_id,
                "symbol": last_trade.get("symbol"),
                "direction": last_trade.get("direction"),
                "open_ts": last_trade.get("open_ts"),
                "close_ts": close_ts,
                "outcome": last_trade.get("outcome") or last_trade.get("results", {}).get("outcome"),
                "close_reason": last_trade.get("close_reason"),
                "pnl_r": last_trade.get("results", {}).get("pnl_r"),
            },
            "pipeline_steps": [],
            "learning_outputs": {
                "join_updated": False,
                "outcome_explanation_updated": False,
                "evidence_scores_updated": False,
                "probability_surface_updated": False,
                "calibration_updated": False,
                "learning_candidates_updated": False,
                "weight_simulation_updated": False,
                "promotion_registry_updated": False,
                "weight_registry_updated": False,
                "trade_brain_v2_updated": False,
            },
            "shadow_learning_status": "no_new_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "warnings": ["duplicate_trade_skipped"],
        }
        _write_snapshot(snapshot)
        _write_json(HEALTH, {
            "status": "no_new_trade",
            "last_run_ts": ts,
            "mode": "once",
            "last_seen_trade_id": trade_id,
            "last_processed_trade_id": state.get("last_processed_trade_id"),
            "processed_new_trade": False,
            "steps_total": 0,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "last_blocker": "duplicate_trade_skipped",
            "warnings": ["duplicate_trade_skipped"],
        })
        _write_jsonl(AUDIT, {
            "ts": ts,
            "engine": "live_shadow_learning_engine_v1",
            "mode": "once",
            "trade_id": trade_id,
            "processed_new_trade": False,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "shadow_learning_status": "no_new_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "warnings": ["duplicate_trade_skipped"],
        })
        _report(snapshot, [])
        state["last_seen_trade_id"] = trade_id
        state["last_seen_close_ts"] = close_ts
        state["last_run_ts"] = ts
        _save_state(state)
        return snapshot

    if close_ts is None:
        trade_status = "open"
    else:
        trade_status = "closed"
    if trade_status != "closed":
        snapshot = {
            "engine": "live_shadow_learning_engine_v1",
            "generated_ts": ts,
            "mode": "once",
            "processed_new_trade": False,
            "trade": {
                "trade_id": trade_id,
                "symbol": last_trade.get("symbol"),
                "direction": last_trade.get("direction"),
                "open_ts": last_trade.get("open_ts"),
                "close_ts": close_ts,
                "outcome": last_trade.get("outcome") or last_trade.get("results", {}).get("outcome"),
                "close_reason": last_trade.get("close_reason"),
                "pnl_r": last_trade.get("results", {}).get("pnl_r"),
            },
            "pipeline_steps": [],
            "learning_outputs": {
                "join_updated": False,
                "outcome_explanation_updated": False,
                "evidence_scores_updated": False,
                "probability_surface_updated": False,
                "calibration_updated": False,
                "learning_candidates_updated": False,
                "weight_simulation_updated": False,
                "promotion_registry_updated": False,
                "weight_registry_updated": False,
                "trade_brain_v2_updated": False,
            },
            "shadow_learning_status": "no_closed_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "warnings": ["no_closed_trade"],
        }
        _write_snapshot(snapshot)
        _write_json(HEALTH, {
            "status": "no_new_trade",
            "last_run_ts": ts,
            "mode": "once",
            "last_seen_trade_id": trade_id,
            "last_processed_trade_id": state.get("last_processed_trade_id"),
            "processed_new_trade": False,
            "steps_total": 0,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "trade_logic_changed": False,
            "last_blocker": "no_closed_trade",
            "warnings": ["no_closed_trade"],
        })
        _write_jsonl(AUDIT, {
            "ts": ts,
            "engine": "live_shadow_learning_engine_v1",
            "mode": "once",
            "trade_id": trade_id,
            "processed_new_trade": False,
            "steps_success": 0,
            "steps_failed": 0,
            "steps_skipped": 0,
            "shadow_learning_status": "no_closed_trade",
            "live_weight_changed": False,
            "trade_brain_changed": False,
            "warnings": ["no_closed_trade"],
        })
        _report(snapshot, [])
        state["last_seen_trade_id"] = trade_id
        state["last_seen_close_ts"] = close_ts
        state["last_run_ts"] = ts
        _save_state(state)
        return snapshot

    steps = []
    updates = {
        "join_updated": False,
        "outcome_explanation_updated": False,
        "evidence_scores_updated": False,
        "probability_surface_updated": False,
        "calibration_updated": False,
        "learning_candidates_updated": False,
        "weight_simulation_updated": False,
        "promotion_registry_updated": False,
        "weight_registry_updated": False,
        "trade_brain_v2_updated": False,
    }
    warnings = []
    for step_name, cmd in PIPELINE_STEPS:
        if step_name == "evidence_scoring" and not (ROOT / "tools/evidence_scoring_engine.py").exists():
            steps.append({
                "step": step_name,
                "status": "skipped",
                "return_code": 0,
                "duration_ms": 0,
                "stdout_tail": "",
                "stderr_tail": "",
                "warning": "evidence_scoring_tool_missing",
            })
            warnings.append("evidence_scoring_tool_missing")
            continue
        result = _run_step(step_name, cmd)
        steps.append(result)
        if result["status"] != "success" and result["warning"]:
            warnings.append(result["warning"])
        if step_name == "outcome_join":
            updates["join_updated"] = result["status"] == "success"
        elif step_name == "outcome_explanation":
            updates["outcome_explanation_updated"] = result["status"] == "success"
        elif step_name == "evidence_scoring":
            updates["evidence_scores_updated"] = result["status"] == "success"
        elif step_name == "probability_surface":
            updates["probability_surface_updated"] = result["status"] == "success"
        elif step_name == "calibration_v2":
            updates["calibration_updated"] = result["status"] == "success"
        elif step_name == "learning_candidate":
            updates["learning_candidates_updated"] = result["status"] == "success"
        elif step_name == "weight_simulation":
            updates["weight_simulation_updated"] = result["status"] == "success"
        elif step_name == "learning_promotion":
            updates["promotion_registry_updated"] = result["status"] == "success"
        elif step_name == "weight_registry":
            updates["weight_registry_updated"] = result["status"] == "success"
        elif step_name == "trade_brain_v2":
            updates["trade_brain_v2_updated"] = result["status"] == "success"

    diff_warnings = _safety_diff_check()
    warnings.extend(diff_warnings)
    shadow_status = "updated"
    if any(s["status"] == "failed" for s in steps):
        shadow_status = "partial"
    if diff_warnings:
        shadow_status = "failed"

    snapshot = {
        "engine": "live_shadow_learning_engine_v1",
        "generated_ts": ts,
        "mode": "once",
        "processed_new_trade": True,
        "trade": {
            "trade_id": trade_id,
            "symbol": last_trade.get("symbol"),
            "direction": last_trade.get("direction"),
            "open_ts": last_trade.get("open_ts"),
            "close_ts": close_ts,
            "outcome": last_trade.get("outcome") or last_trade.get("results", {}).get("outcome"),
            "close_reason": last_trade.get("close_reason"),
            "pnl_r": last_trade.get("results", {}).get("pnl_r"),
        },
        "pipeline_steps": steps,
        "learning_outputs": updates,
        "shadow_learning_status": shadow_status,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "trade_logic_changed": False,
        "warnings": warnings,
    }
    _write_snapshot(snapshot)

    success_count = sum(1 for s in steps if s["status"] == "success")
    failed_count = sum(1 for s in steps if s["status"] == "failed")
    skipped_count = sum(1 for s in steps if s["status"] == "skipped")
    health = {
        "status": "alive" if not failed_count else "partial" if success_count else "failed",
        "last_run_ts": ts,
        "mode": "once",
        "last_seen_trade_id": trade_id,
        "last_processed_trade_id": trade_id,
        "processed_new_trade": True,
        "steps_total": len(steps),
        "steps_success": success_count,
        "steps_failed": failed_count,
        "steps_skipped": skipped_count,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "trade_logic_changed": False,
        "last_blocker": warnings[0] if warnings else None,
        "warnings": warnings,
    }
    _write_json(HEALTH, health)
    _write_jsonl(AUDIT, {
        "ts": ts,
        "engine": "live_shadow_learning_engine_v1",
        "mode": "once",
        "trade_id": trade_id,
        "processed_new_trade": True,
        "steps_success": success_count,
        "steps_failed": failed_count,
        "steps_skipped": skipped_count,
        "shadow_learning_status": shadow_status,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "warnings": warnings,
    })
    _report(snapshot, steps)
    state["last_seen_trade_id"] = trade_id
    state["last_seen_close_ts"] = close_ts
    state["last_processed_trade_id"] = trade_id
    state["last_processed_close_ts"] = close_ts
    state.setdefault("processed_trade_ids", []).append(trade_id)
    state["last_run_ts"] = ts
    _save_state(state)
    return snapshot


def run_loop(interval: int, force: bool = False) -> None:
    try:
        while True:
            run_once(force=force)
            time.sleep(interval)
    except KeyboardInterrupt:
        return


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "loop"], default="once")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.mode == "once":
        run_once(force=args.force)
    else:
        run_loop(args.interval, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
