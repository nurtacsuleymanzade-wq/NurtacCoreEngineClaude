#!/usr/bin/env python3
import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
CONFIG = ROOT / "config"

LEARNING_JSON = DATA / "trade_brain_learning_candidates.json"
LEARNING_JSONL = DATA / "trade_brain_learning_candidates.jsonl"
SIM_RESULTS = DATA / "weight_simulation_results.json"
SIM_RESULTS_JSONL = DATA / "weight_simulation_results.jsonl"
APPROVAL_CANDIDATES = DATA / "weight_approval_candidates.json"
CONTROL_HEALTH = DATA / "controlled_weight_update_health.json"
STAGED_PROFILE = DATA / "staged_trade_brain_weights.json"
STAGED_DIFF = DATA / "staged_trade_brain_weights_diff.json"
ROLLBACK = DATA / "weight_update_rollback_manifest.json"
PROMO_APPROVAL = CONFIG / "learning_promotion_approval.json"

REGISTRY = DATA / "learning_promotion_registry.json"
REGISTRY_JSONL = DATA / "learning_promotion_registry.jsonl"
AUDIT = DATA / "learning_promotion_audit.jsonl"
MANIFEST = DATA / "learning_promotion_manifest.json"
HEALTH = DATA / "learning_promotion_health.json"
REPORT = DATA / "learning_promotion_report.md"
TRADE_BRAIN = ROOT / "trade_brain_engine.py"


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


def _approval_state() -> tuple[bool, dict[str, Any], list[str]]:
    if not PROMO_APPROVAL.exists():
        return False, {}, ["promotion_approval_missing"]
    doc = _read_json(PROMO_APPROVAL)
    warnings = []
    if doc.get("approval_mode") != "promote_staged_only":
        warnings.append("approval_mode_invalid")
    if doc.get("allow_live_weight_change") is not False:
        warnings.append("allow_live_weight_change_not_false")
    if not doc.get("candidate_ids"):
        warnings.append("candidate_ids_empty")
    return True, doc, warnings


def _flatten_candidates() -> list[dict[str, Any]]:
    doc = _read_json(LEARNING_JSON)
    rows = []
    for bucket in ("increase_candidates", "decrease_candidates", "hold_candidates", "disable_candidates", "insufficient_data_candidates"):
        rows.extend(doc.get(bucket, []) or [])
    if not rows:
        rows.extend(_read_jsonl(LEARNING_JSONL))
    return [r for r in rows if isinstance(r, dict) and r.get("evidence")]


def _candidate_index(candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for c in candidates:
        key = str(c.get("candidate_id") or c.get("evidence"))
        out[key] = c
    return out


def _simulation_index() -> dict[str, dict[str, Any]]:
    raw = _read_json(SIM_RESULTS)
    rows = raw.get("results", []) if isinstance(raw, dict) else []
    return {str(r.get("evidence") or ""): r for r in rows if r.get("evidence")}


def _approval_candidate_index() -> dict[str, dict[str, Any]]:
    raw = _read_json(APPROVAL_CANDIDATES)
    rows = (raw.get("approved_ready", []) or []) + (raw.get("candidate", []) or []) + (raw.get("watch", []) or []) + (raw.get("reject", []) or [])
    out = {}
    for r in rows:
        if isinstance(r, dict):
            key = str(r.get("candidate_id") or r.get("id") or r.get("evidence") or "")
            if key:
                out[key] = r
    return out


def _stage_index() -> dict[str, dict[str, Any]]:
    staged = _read_json(STAGED_PROFILE)
    weights = staged.get("weights") or {}
    out = {}
    for _, item in weights.items():
        if isinstance(item, dict):
            key = str(item.get("candidate_id") or item.get("evidence") or "")
            if key:
                out[key] = item
    return out


def _status_for(cid: str, cand: dict[str, Any], sim: dict[str, Any], appr: dict[str, Any], stage: dict[str, Any], promo_approval: dict[str, Any], promo_valid: bool) -> tuple[str, str, dict[str, bool], dict[str, Any], dict[str, Any]]:
    source = {
        "learning_candidate": bool(cand),
        "simulation_result": bool(sim),
        "approval_candidate": bool(appr),
        "staged_profile": bool(stage),
        "manual_promotion_approval": promo_valid and cid in set(promo_approval.get("candidate_ids") or []),
    }
    sim_status = appr.get("recommendation") if appr else "unknown"
    simulation = {
        "baseline_wr": sim.get("baseline_wr") if sim else cand.get("baseline_wr"),
        "candidate_wr": sim.get("candidate_wr") if sim else cand.get("candidate_wr"),
        "wr_delta": sim.get("wr_delta") if sim else cand.get("wr_delta"),
        "pf_delta": sim.get("pf_delta") if sim else cand.get("pf_delta"),
        "expectancy_delta": sim.get("expectancy_delta") if sim else cand.get("expectancy_delta"),
        "drawdown_delta": sim.get("drawdown_delta") if sim else cand.get("drawdown_delta"),
        "approval_status": sim_status,
    }
    staged_weight = {
        "current": stage.get("current") if stage else cand.get("current_weight"),
        "proposed": stage.get("proposed") if stage else cand.get("suggested_weight"),
        "delta": stage.get("delta") if stage else None,
        "delta_pct": stage.get("delta_pct") if stage else None,
    }
    return source, simulation, staged_weight


def build_batch() -> dict[str, Any]:
    ts = int(time.time() * 1000)
    approval_found, promo_approval, promo_warnings = _approval_state()
    candidates = _flatten_candidates()
    cand_index = _candidate_index(candidates)
    sim_index = _simulation_index()
    appr_index = _approval_candidate_index()
    stage_index = _stage_index()
    control_health = _read_json(CONTROL_HEALTH)

    promoted_ids = []
    blocked_ids = []
    entries = []
    manual_approved_count = 0
    staged_count = 0
    approve_ready_count = 0
    promoted_count = 0
    blocked_count = 0
    rejected_count = 0
    expired_count = 0

    promo_valid = approval_found and not promo_warnings
    allowed_ids = set(promo_approval.get("candidate_ids") or [])

    for cid, cand in cand_index.items():
        sim = sim_index.get(str(cand.get("evidence") or cid), {})
        appr = appr_index.get(cid) or appr_index.get(str(cand.get("evidence") or ""))
        stage = stage_index.get(cid) or stage_index.get(str(cand.get("evidence") or ""))
        source, simulation, staged_weight = _status_for(cid, cand, sim, appr or {}, stage or {}, promo_approval, promo_valid)

        previous_status = "candidate"
        if sim:
            previous_status = "simulated"
        if appr and (appr.get("recommendation") == "approve_ready" or appr.get("approval_status") == "approve_ready"):
            previous_status = "approve_ready"
            approve_ready_count += 1
        if stage:
            previous_status = "staged"
            staged_count += 1
        if cid in allowed_ids and promo_valid and stage and (appr and (appr.get("recommendation") == "approve_ready" or appr.get("approval_status") == "approve_ready")):
            lifecycle_status = "promoted"
            promoted_count += 1
            promoted_ids.append(cid)
        elif cid in allowed_ids and promo_valid and not stage:
            lifecycle_status = "blocked"
            blocked_count += 1
            blocked_ids.append(cid)
        elif cand.get("recommendation") == "reject" or cand.get("recommendation") == "watch":
            lifecycle_status = "rejected"
            rejected_count += 1
        elif cand.get("recommendation") == "insufficient_data":
            lifecycle_status = "blocked"
            blocked_count += 1
        else:
            lifecycle_status = "candidate"

        if promo_valid and cid in allowed_ids:
            manual_approved_count += 1
        if cand.get("expired") or (cand.get("generated_ts") and ts - int(cand["generated_ts"]) > 7 * 24 * 3600 * 1000):
            lifecycle_status = "expired"
            expired_count += 1

        warnings = []
        if not source["simulation_result"]:
            warnings.append("simulation_missing")
        if not source["approval_candidate"]:
            warnings.append("approval_candidate_missing")
        if not source["staged_profile"]:
            warnings.append("staged_profile_missing")
        if not promo_valid:
            warnings.extend(promo_warnings or ["promotion_approval_missing"])
        if cand.get("recommendation") == "insufficient_data":
            warnings.append("insufficient_data")

        if lifecycle_status == "candidate" and not sim:
            lifecycle_status = "blocked"
            blocked_count += 1

        entries.append({
            "candidate_id": cid,
            "evidence": cand.get("evidence"),
            "direction": cand.get("direction"),
            "lifecycle_status": lifecycle_status,
            "previous_status": previous_status,
            "source": source,
            "simulation": simulation,
            "staged_weight": staged_weight,
            "promotion": {
                "eligible": bool(cid in allowed_ids and promo_valid and stage and appr and (appr.get("recommendation") == "approve_ready" or appr.get("approval_status") == "approve_ready")),
                "manual_approved": bool(cid in allowed_ids and promo_valid),
                "promoted": lifecycle_status == "promoted",
                "promotion_id": promo_approval.get("approval_id") if lifecycle_status == "promoted" else None,
                "promotion_reason": "lifecycle_only_promotion" if lifecycle_status == "promoted" else "not_eligible",
                "blocked_reason": None if lifecycle_status == "promoted" else ("promotion_approval_missing" if not promo_valid else None),
            },
            "safety": {
                "live_update_allowed": False,
                "manual_approval_required": True,
                "stage_only": True,
                "rollback_available": True,
            },
            "warnings": warnings,
        })

    total = len(entries)
    summary = {
        "total_candidates": total,
        "candidate_count": sum(1 for e in entries if e["lifecycle_status"] == "candidate"),
        "simulated_count": sum(1 for e in entries if e["lifecycle_status"] in {"simulated", "approve_ready", "manual_approved", "staged", "promoted"}),
        "approve_ready_count": approve_ready_count,
        "manual_approved_count": manual_approved_count,
        "staged_count": staged_count,
        "promoted_count": promoted_count,
        "rejected_count": rejected_count,
        "expired_count": expired_count,
        "blocked_count": blocked_count,
    }

    registry = {
        "engine": "learning_promotion_engine_v1",
        "generated_ts": ts,
        "mode": "batch",
        "live_weight_change_allowed": False,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "candidates": entries,
        "summary": summary,
        "warnings": ([] if promo_valid else promo_warnings) + (["staged_profile_missing"] if not _read_json(STAGED_PROFILE) else []) + (["controlled_weight_update_blocked"] if control_health.get("status") == "blocked" else []),
    }

    manifest = {
        "promotion_manifest_id": f"promotion_{ts}",
        "generated_ts": ts,
        "promotion_mode": "lifecycle_only",
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "promoted_candidates": promoted_ids,
        "blocked_candidates": blocked_ids,
        "registry_snapshot": str(REGISTRY),
        "rollback_manifest": str(ROLLBACK),
        "next_required_action": "manual review / wait for approve_ready candidates / create approval file",
    }

    health = {
        "status": "alive" if promoted_count or approve_ready_count or summary["simulated_count"] else "no_promotion",
        "last_run_ts": ts,
        "approval_file_found": approval_found,
        "approval_valid": promo_valid,
        "candidates_seen": total,
        "approve_ready_seen": approve_ready_count,
        "staged_seen": staged_count,
        "manual_approved_seen": manual_approved_count,
        "promoted_count": promoted_count,
        "blocked_count": blocked_count,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "last_blocker": promo_warnings[0] if promo_warnings else None,
        "warnings": registry["warnings"],
    }

    _write_json(REGISTRY, registry)
    _write_jsonl(REGISTRY_JSONL, registry)
    _write_json(MANIFEST, manifest)
    _write_json(HEALTH, health)
    _write_jsonl(AUDIT, {
        "ts": ts,
        "engine": "learning_promotion_engine_v1",
        "approval_file_found": approval_found,
        "approval_valid": promo_valid,
        "candidates_seen": total,
        "approve_ready_seen": approve_ready_count,
        "staged_seen": staged_count,
        "manual_approved_seen": manual_approved_count,
        "promoted_count": promoted_count,
        "blocked_count": blocked_count,
        "live_weight_changed": False,
        "trade_brain_changed": False,
        "warnings": health["warnings"],
    })

    report = [
        "# Learning Promotion Engine Report",
        "",
        "## Status",
        f"- status: {health['status']}",
        "- No live Trade Brain weights were changed.",
        "- Promotion means lifecycle promotion only, not production application.",
        "",
        "## Git / Version",
        f"- trade_brain_engine_changed: False",
        f"- controlled_update_status: {control_health.get('status', 'unknown')}",
        "",
        "## Input Availability",
        f"- learning_candidates: {len(candidates)}",
        f"- weight_approval_candidates: {len(appr_index)}",
        f"- staged_profile_present: {bool(_read_json(STAGED_PROFILE))}",
        f"- promotion_approval_found: {approval_found}",
        f"- promotion_approval_valid: {promo_valid}",
        "",
        "## Candidate Lifecycle Summary",
        f"- candidate_count: {summary['candidate_count']}",
        f"- simulated_count: {summary['simulated_count']}",
        f"- approve_ready_count: {summary['approve_ready_count']}",
        f"- manual_approved_count: {summary['manual_approved_count']}",
        f"- staged_count: {summary['staged_count']}",
        f"- promoted_count: {summary['promoted_count']}",
        f"- rejected_count: {summary['rejected_count']}",
        f"- expired_count: {summary['expired_count']}",
        f"- blocked_count: {summary['blocked_count']}",
        "",
        "## Approve-Ready Candidates",
        "\n".join(f"- {e['candidate_id']} {e['evidence']} {e['lifecycle_status']}" for e in entries if e["lifecycle_status"] == "approve_ready") or "- none",
        "",
        "## Staged Candidates",
        "\n".join(f"- {e['candidate_id']} {e['evidence']} {e['lifecycle_status']}" for e in entries if e["lifecycle_status"] == "staged") or "- none",
        "",
        "## Manual Approval",
        "\n".join(f"- {e['candidate_id']} {e['evidence']} {e['promotion']['manual_approved']}" for e in entries if e["promotion"]["manual_approved"]) or "- none",
        "",
        "## Promoted Candidates",
        "\n".join(f"- {e['candidate_id']} {e['evidence']} {e['promotion']['promoted']}" for e in entries if e["promotion"]["promoted"]) or "- none",
        "",
        "## Blocked Candidates",
        "\n".join(f"- {e['candidate_id']} {e['warnings']}" for e in entries if e["lifecycle_status"] == "blocked") or "- none",
        "",
        "## Safety Checks",
        "- trade_brain_engine.py unchanged",
        "- live weights unchanged",
        "- staged profiles remain non-active",
        "- promotion approval requires promote_staged_only and allow_live_weight_change=false",
        "",
        "## Rollback / Archive Plan",
        "- No live state to rollback; delete registry snapshot to revert lifecycle view.",
        f"- Rollback manifest: {ROLLBACK.name}",
        "",
        "## No Live Weight Change Guarantee",
        "No live Trade Brain weights were changed.",
        "",
        "## Next Recommended Step",
        "- Wait for an approve-ready candidate and a valid promote_staged_only approval before expecting any promoted lifecycle entries.",
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
