#!/usr/bin/env python3
import argparse
import json
import re
import time
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
CONFIG = ROOT / "config"
APPROVAL_FILE = CONFIG / "weight_update_approval.json"
APPROVAL_CANDIDATES = DATA / "weight_approval_candidates.json"
SIM_RESULTS = DATA / "weight_simulation_results.json"
LEARNING_JSON = DATA / "trade_brain_learning_candidates.json"
TRADE_BRAIN = ROOT / "trade_brain_engine.py"

HEALTH = DATA / "controlled_weight_update_health.json"
AUDIT = DATA / "controlled_weight_update_audit.jsonl"
STAGED = DATA / "staged_trade_brain_weights.json"
DIFF = DATA / "staged_trade_brain_weights_diff.json"
ROLLBACK = DATA / "weight_update_rollback_manifest.json"
REPORT = DATA / "controlled_weight_update_report.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _grep_weights() -> list[str]:
    if not TRADE_BRAIN.exists():
        return []
    try:
        text = TRADE_BRAIN.read_text()
    except Exception:
        return []
    pats = [
        r"MIN_CONFIDENCE\s*=\s*([0-9.]+)",
        r"COOLDOWN_S\s*=\s*([0-9.]+)",
        r"Q\d+_[A-Za-z_]+",
        r"\"([a-z_]+_long|[a-z_]+_short)\"",
    ]
    found = []
    for pat in pats:
        for m in re.finditer(pat, text):
            found.append(m.group(0))
    return found[:200]


def _candidate_map() -> dict[str, dict[str, Any]]:
    raw = _read_json(APPROVAL_CANDIDATES)
    rows = (
        raw.get("approved_ready", [])
        + raw.get("candidate", [])
        + raw.get("watch", [])
        + raw.get("reject", [])
    )
    out = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row.get("candidate_id") or row.get("id") or row.get("evidence")
        if key:
            out[str(key)] = row
    return out


def _simulation_map() -> dict[str, dict[str, Any]]:
    raw = _read_json(SIM_RESULTS)
    rows = raw.get("results", []) if isinstance(raw, dict) else []
    return {str(r.get("evidence") or ""): r for r in rows if r.get("evidence")}


def _approval_state() -> tuple[bool, dict[str, Any], list[str]]:
    if not APPROVAL_FILE.exists():
        return False, {}, ["approval_file_missing"]
    approval = _read_json(APPROVAL_FILE)
    warnings = []
    if approval.get("approval_mode") != "stage_only":
        warnings.append("approval_mode_not_stage_only")
    if approval.get("allow_live_apply") is not False:
        warnings.append("allow_live_apply_not_false")
    if not approval.get("candidate_ids"):
        warnings.append("candidate_ids_empty")
    return True, approval, warnings


def _safe_weight_value(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def build_batch() -> dict[str, Any]:
    ts = int(time.time() * 1000)
    approval_found, approval, approval_warnings = _approval_state()
    candidate_index = _candidate_map()
    sim_index = _simulation_map()
    weight_refs = _grep_weights()

    staged_weights: dict[str, Any] = {}
    staged_changes = []
    blocked = []
    approved_candidate_ids = []
    approve_ready_seen = 0

    if approval_found and approval.get("approval_mode") == "stage_only" and approval.get("allow_live_apply") is False:
        for cid in approval.get("candidate_ids") or []:
            if cid not in candidate_index:
                blocked.append({"candidate_id": cid, "reason": "candidate_not_found"})
                continue
            cand = candidate_index[cid]
            if cand.get("recommendation") != "approve_ready":
                blocked.append({"candidate_id": cid, "reason": "candidate_not_approve_ready"})
                continue
            approve_ready_seen += 1
            sim = sim_index.get(cid) or {}
            if not sim.get("trade_count"):
                blocked.append({"candidate_id": cid, "reason": "simulation_missing"})
                continue
            proposed = _safe_weight_value(cand.get("suggested_weight"))
            if proposed is None:
                blocked.append({"candidate_id": cid, "reason": "proposed_weight_null"})
                continue
            current = _safe_weight_value(cand.get("current_weight"))
            delta = (proposed - current) if current is not None else None
            delta_pct = ((delta / current) * 100.0) if current not in (None, 0) and delta is not None else None
            max_delta = _safe_weight_value(approval.get("max_weight_delta_pct"))
            if max_delta is not None and delta_pct is not None and abs(delta_pct) > max_delta:
                blocked.append({"candidate_id": cid, "reason": "delta_exceeds_max_weight_delta_pct"})
                continue
            if cand.get("observed_risks") and len(cand.get("observed_risks") or []) > 3:
                blocked.append({"candidate_id": cid, "reason": "high_risk_flags"})
                continue
            if cand.get("recommendation") == "insufficient_data":
                blocked.append({"candidate_id": cid, "reason": "insufficient_data"})
                continue
            approved_candidate_ids.append(cid)
            staged_weights[cid] = {
                "evidence": cid,
                "current": current,
                "proposed": proposed,
                "delta": round(delta, 4) if delta is not None else None,
                "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
                "candidate_id": cid,
                "reason": cand.get("reason"),
                "simulation": {
                    "baseline_wr": cand.get("baseline_wr"),
                    "candidate_wr": cand.get("candidate_wr"),
                    "pf_delta": cand.get("pf_delta"),
                    "expectancy_delta": cand.get("expectancy_delta"),
                    "drawdown_delta": cand.get("drawdown_delta"),
                },
                "safety": {
                    "manual_approval_required": True,
                    "live_update_allowed": False,
                    "stage_only": True,
                },
            }
            staged_changes.append({
                "evidence": cid,
                "current": current,
                "proposed": proposed,
                "delta": round(delta, 4) if delta is not None else None,
                "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
                "approval_status": "approve_ready",
                "safe_to_stage": True,
                "safe_to_apply_live": False,
            })

    live_changed = False
    trade_brain_changed = False
    if staged_weights:
        staged_profile = {
            "engine": "controlled_weight_update_engine_v1",
            "mode": "stage_only",
            "generated_ts": ts,
            "approval": {
                "approval_id": approval.get("approval_id"),
                "approved_by": approval.get("approved_by"),
                "allow_live_apply": False,
            },
            "source_candidates": approved_candidate_ids,
            "weights": staged_weights,
            "active": False,
            "applied_to_live": False,
            "weight_source_canonicalized": bool(weight_refs),
        }
    else:
        staged_profile = {
            "engine": "controlled_weight_update_engine_v1",
            "mode": "stage_only",
            "generated_ts": ts,
            "approval": {
                "approval_id": approval.get("approval_id"),
                "approved_by": approval.get("approved_by"),
                "allow_live_apply": False,
            },
            "source_candidates": [],
            "weights": {},
            "active": False,
            "applied_to_live": False,
            "weight_source_canonicalized": bool(weight_refs),
        }

    diff = {
        "generated_ts": ts,
        "changes_count": len(staged_changes),
        "changes": staged_changes,
        "blocked": blocked,
        "warnings": approval_warnings,
    }

    rollback = {
        "generated_ts": ts,
        "rollback_available": True,
        "previous_weights_snapshot": _grep_weights(),
        "staged_weights_snapshot": staged_profile,
        "restore_instruction": "No live weights were changed; delete staged profile to rollback stage.",
        "live_changed": live_changed,
    }

    health_status = "alive"
    if not approval_found:
        health_status = "no_update"
    if approval_warnings:
        health_status = "blocked"

    health = {
        "status": health_status,
        "last_run_ts": ts,
        "mode": "stage_only",
        "approval_file_found": approval_found,
        "approval_valid": approval_found and not approval_warnings,
        "candidates_seen": len(candidate_index),
        "approve_ready_seen": approve_ready_seen,
        "staged_count": len(staged_changes),
        "blocked_count": len(blocked),
        "live_changed": live_changed,
        "trade_brain_changed": trade_brain_changed,
        "last_blocker": approval_warnings[0] if approval_warnings else None,
        "warnings": approval_warnings if approval_warnings else ([] if staged_changes else ["no_update"]),
    }

    _write_json(STAGED, staged_profile)
    _write_json(DIFF, diff)
    _write_json(ROLLBACK, rollback)
    _write_json(HEALTH, health)
    _write_jsonl(AUDIT, {
        "ts": ts,
        "approval_id": approval.get("approval_id"),
        "mode": "stage_only",
        "candidates_seen": len(candidate_index),
        "approve_ready_seen": approve_ready_seen,
        "approved_candidate_ids": approved_candidate_ids,
        "staged_count": len(staged_changes),
        "blocked_count": len(blocked),
        "live_changed": live_changed,
        "trade_brain_changed": trade_brain_changed,
        "warnings": health["warnings"],
    })

    report = [
        "# Controlled Weight Update Report",
        "",
        "## Status",
        f"- status: {health_status}",
        "- No live Trade Brain weights were changed.",
        "",
        "## Git / Version",
        f"- trade_brain_engine_present: {TRADE_BRAIN.exists()}",
        f"- live_weight_source_canonicalized: {bool(weight_refs)}",
        "",
        "## Approval",
        f"- approval_file_found: {approval_found}",
        f"- approval_valid: {health['approval_valid']}",
        f"- approval_id: {approval.get('approval_id')}",
        f"- approval_mode: {approval.get('approval_mode')}",
        f"- allow_live_apply: {approval.get('allow_live_apply')}",
        "",
        "## Candidates Seen",
        f"- candidates_seen: {len(candidate_index)}",
        f"- approve_ready_seen: {approve_ready_seen}",
        "",
        "## Candidates Staged",
        f"- staged_count: {len(staged_changes)}",
        "",
        "## Candidates Blocked",
        f"- blocked_count: {len(blocked)}",
        "",
        "## Weight Diff",
        json.dumps(diff, ensure_ascii=False)[:3000],
        "",
        "## Simulation Evidence",
        json.dumps({"weight_approval_candidates": _read_json(APPROVAL_CANDIDATES), "weight_simulation_results": _read_json(SIM_RESULTS)}, ensure_ascii=False)[:3000],
        "",
        "## Safety Checks",
        "- stage_only enforced",
        "- live_update_allowed=false enforced",
        "- manual approval required",
        "",
        "## Rollback Plan",
        rollback["restore_instruction"],
        "",
        "## Live Update Status",
        f"- live_changed: {live_changed}",
        f"- trade_brain_changed: {trade_brain_changed}",
        "",
        "## Next Recommended Step",
        "- Create a manual approval file with approve_ready candidate IDs if you want a staged profile on a future run.",
        "",
        "No live Trade Brain weights were changed.",
    ]
    REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
    return health


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="batch", choices=["batch"])
    args = parser.parse_args()
    if args.mode == "batch":
        build_batch()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
