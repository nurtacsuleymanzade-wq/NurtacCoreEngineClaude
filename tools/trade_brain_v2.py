#!/usr/bin/env python3
import argparse
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any
import sys

sys.path.insert(0, str(Path("/root/NurtacCoreEngineClaude")))

from weight_registry import load_weight_registry, validate_registry, registry_is_safe_for_live, get_weight


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
TRADE_BRAIN = ROOT / "trade_brain_engine.py"

WEIGHT_REGISTRY = DATA / "trade_brain_weight_registry.json"
PROB_SURFACE = DATA / "probability_surface.json"
CALIBRATION = DATA / "calibration_profiles.json"
PROMO_REGISTRY = DATA / "learning_promotion_registry.json"
LEARNING = DATA / "trade_brain_learning_candidates.json"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
AUDIT_SOURCE = DATA / "trade_brain_decision_audit.jsonl"

OUT_JSON = DATA / "trade_brain_v2_output.json"
OUT_AUDIT = DATA / "trade_brain_v2_decision_audit.jsonl"
HEALTH = DATA / "trade_brain_v2_health.json"
REPORT = DATA / "trade_brain_v2_report.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_jsonl(path: Path, tail: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text().splitlines()
    if tail is not None:
        lines = lines[-tail:]
    rows = []
    for line in lines:
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


def _sf(v: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _wilson(wins: int, losses: int, z: float = 1.96) -> float | None:
    n = wins + losses
    if n <= 0:
        return None
    phat = wins / n
    denom = 1 + (z * z) / n
    center = phat + (z * z) / (2 * n)
    margin = z * math.sqrt((phat * (1 - phat) / n) + (z * z / (4 * n * n)))
    return max(0.0, (center - margin) / denom)


def _pf(pnls: list[float]) -> float | None:
    gains = sum(p for p in pnls if p > 0)
    losses = sum(abs(p) for p in pnls if p < 0)
    if losses <= 0:
        return float("inf") if gains > 0 else None
    return gains / losses


def _load_history() -> list[dict[str, Any]]:
    joins = _read_jsonl(JOIN_FILE)
    audits = _read_jsonl(AUDIT_SOURCE, tail=5000)
    audit_by_trade = {str(a.get("trade_id") or a.get("ts") or ""): a for a in audits if isinstance(a, dict)}
    rows = []
    for j in joins:
        tid = str(j.get("trade_id") or "")
        audit = j.get("decision_audit") or audit_by_trade.get(tid) or {}
        rows.append({
            "trade_id": tid,
            "direction": str(j.get("direction") or "neutral").lower(),
            "pnl_r": _sf(j.get("pnl_r"), None),
            "duration_seconds": _sf(j.get("duration_seconds"), None),
            "learning_eligible": bool(j.get("learning_eligible")),
            "decision_audit": audit,
            "outcome": str(j.get("outcome") or "unknown").lower(),
            "close_reason": str(j.get("close_reason") or "unknown").lower(),
        })
    return rows


def _feature_weights(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    weights = registry.get("weights") or {}
    if isinstance(weights, dict) and weights:
        return weights
    profile = ((registry.get("profiles") or {}).get("current_discovered_weights") or {}).get("weights") or {}
    return profile if isinstance(profile, dict) else {}


def _score_evidence(history: list[dict[str, Any]], weights: dict[str, dict[str, Any]], surface: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    evidence_names = sorted(set(weights.keys()) | {"iceberg", "absorption", "scenario", "gate", "market_context", "volume_profile", "baseline", "smart_money", "detector", "macro", "etf", "coinbase", "liq_magnet", "whale", "order_block", "imbalance"})
    evidence_rows = []
    warnings = []
    surface_meta = surface.get("detectors") or {}
    for name in evidence_names:
        meta = weights.get(name) or {}
        current = _sf(meta.get("value"), None)
        if current is None:
            current = get_weight(name, default=None)
        exact_weight = current is not None
        base_matches = [r for r in history if name in json.dumps(r.get("decision_audit") or {}).lower()]
        if not base_matches:
            base_matches = history[:]
            warnings.append(f"fallback_history_for_{name}")
        pnls = [r["pnl_r"] for r in base_matches if r["pnl_r"] is not None]
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        wr = wins / (wins + losses) if (wins + losses) else None
        wlb = _wilson(wins, losses)
        pf = _pf(pnls)
        avg_r = mean(pnls) if pnls else None
        surf = surface_meta.get(name) or {}
        population = _sf(surf.get("n"), len(base_matches)) or len(base_matches)
        surf_wr = _sf(surf.get("wr"), wr)
        surf_wilson = _sf(surf.get("wilson_lower"), wlb)
        cal = {}
        if name in surface_meta:
            cal["surface_grade"] = surf.get("grade")
            cal["surface_reliable"] = surf.get("reliable")
        evidence_rows.append({
            "name": name,
            "weight": current,
            "source": meta.get("source", "unknown"),
            "confidence": meta.get("confidence", "unknown" if current is None else "exact"),
            "direction": meta.get("direction", "unknown"),
            "category": meta.get("category", name.split("_", 1)[0] if "_" in name else "unknown"),
            "population": population,
            "observed_wr": round(wr, 4) if wr is not None else None,
            "wilson_lower": round(wlb, 4) if wlb is not None else None,
            "profit_factor": round(pf, 4) if pf not in (None, float("inf")) else pf,
            "average_r": round(avg_r, 4) if avg_r is not None else None,
            "surface": {
                "wr": round(surf_wr, 4) if surf_wr is not None else None,
                "wilson_lower": round(surf_wilson, 4) if surf_wilson is not None else None,
                "population": population,
                "confidence": _sf(surf.get("confidence"), None),
                "reliability": surf.get("reliable") if "reliable" in surf else None,
            },
            "calibration": cal,
            "source_refs": meta.get("line_refs") or [],
        })
    return evidence_rows, {"warnings": warnings}, warnings


def build_batch() -> dict[str, Any]:
    ts = int(time.time() * 1000)
    registry = load_weight_registry()
    registry_errors = validate_registry(registry)
    safe_for_live = registry_is_safe_for_live(registry)
    surface = _read_json(PROB_SURFACE)
    calibration = _read_json(CALIBRATION)
    promo = _read_json(PROMO_REGISTRY)
    learning = _read_json(LEARNING)
    history = _load_history()
    weights = _feature_weights(registry)

    evidence_rows, warn_block, warnings = _score_evidence(history, weights, surface)
    weight_component = sum(((_sf(e["weight"], 0.0) or 0.0) * (e["surface"]["wr"] or 0.5)) for e in evidence_rows if e["weight"] is not None)
    historical_component = mean([e["observed_wr"] or 0.0 for e in evidence_rows]) if evidence_rows else 0.0
    probability_component = mean([e["surface"]["wr"] or 0.0 for e in evidence_rows]) if evidence_rows else 0.0
    calibration_component = 0.0
    calibration_loaded = bool(calibration)
    if calibration_loaded:
        overall = calibration.get("overall") or {}
        calibration_component = _sf(overall.get("win_rate_observed"), 0.0) / 100.0 if overall else 0.0
    historical_loaded = bool(history)

    raw_score = round(weight_component + historical_component + probability_component, 4)
    calibration_bonus = round(calibration_component, 4)
    probability_bonus = round(probability_component, 4)
    historical_bonus = round(historical_component, 4)
    penalty = -round(len([w for w in warnings if "fallback" in w]) * 0.1, 4)
    weighted_score = round(raw_score + calibration_bonus + penalty, 4)
    final_score = round(weighted_score + historical_bonus + probability_bonus + calibration_bonus + penalty, 4)

    if final_score >= 0.7:
        decision = "LONG"
    elif final_score <= 0.3:
        decision = "SHORT"
    else:
        decision = "NEUTRAL"

    confidence_components = {
        "weight_component": round(min(1.0, max(0.0, weight_component / max(len(evidence_rows), 1))), 4),
        "probability_component": round(min(1.0, max(0.0, probability_component)), 4),
        "historical_component": round(min(1.0, max(0.0, historical_component)), 4),
        "calibration_component": round(min(1.0, max(0.0, calibration_bonus)), 4),
        "penalty_component": round(penalty, 4),
    }
    confidence_base = (
        0.34 * confidence_components["weight_component"]
        + 0.28 * confidence_components["probability_component"]
        + 0.20 * confidence_components["historical_component"]
        + 0.18 * confidence_components["calibration_component"]
    )
    confidence = round(max(0.0, min(1.0, confidence_base + max(-0.15, penalty * 0.05))), 4)

    explanations = []
    for e in sorted(evidence_rows, key=lambda x: (x["surface"]["wr"] or 0.0, x["observed_wr"] or 0.0), reverse=True)[:12]:
        note = f"{e['name']}: weight={e['weight']} wr={e['surface']['wr']} hist_wr={e['observed_wr']} cal={e['calibration'].get('surface_grade', 'n/a')}"
        explanations.append(note)

    explanation = (
        f"{decision} because registry weights, probability surface, historical edge, and calibration all contributed. "
        f"Weighted score={weighted_score}, final score={final_score}."
    )

    output = {
        "decision": decision,
        "confidence": confidence,
        "raw_score": raw_score,
        "weighted_score": weighted_score,
        "historical_bonus": historical_bonus,
        "probability_bonus": probability_bonus,
        "calibration_bonus": calibration_bonus,
        "penalty": penalty,
        "final_score": final_score,
        "evidence": evidence_rows,
        "confidence_components": confidence_components,
        "explanation": explanation,
        "warnings": registry_errors + warnings + (["registry_missing"] if not registry else []),
    }

    _write_json(OUT_JSON, output)
    _write_jsonl(OUT_AUDIT, {
        "ts": ts,
        "engine": "trade_brain_v2",
        "registry_loaded": bool(registry),
        "probability_loaded": bool(surface),
        "calibration_loaded": calibration_loaded,
        "historical_loaded": historical_loaded,
        "evidence_count": len(evidence_rows),
        "final_score": final_score,
        "decision": decision,
        "confidence": confidence,
        "fallback_used": any(w.startswith("fallback") for w in warnings),
        "trade_logic_changed": False,
        "live_weight_changed": False,
        "warnings": output["warnings"],
    })

    health = {
        "status": "alive" if registry and surface and calibration and history else "blocked",
        "registry_loaded": bool(registry),
        "probability_loaded": bool(surface),
        "calibration_loaded": calibration_loaded,
        "historical_loaded": historical_loaded,
        "fallback_used": any(w.startswith("fallback") for w in warnings),
        "trade_logic_changed": False,
        "live_weight_changed": False,
    }
    _write_json(HEALTH, health)

    report = [
        "# Trade Brain V2 Report",
        "",
        "## Status",
        f"- status: {health['status']}",
        "",
        "## Registry",
        f"- loaded: {bool(registry)}",
        f"- safe_for_live: {safe_for_live}",
        "",
        "## Probability",
        f"- loaded: {bool(surface)}",
        "",
        "## Calibration",
        f"- loaded: {calibration_loaded}",
        "",
        "## Historical",
        f"- loaded: {historical_loaded}",
        f"- trades: {len(history)}",
        "",
        "## Evidence",
        "\n".join(f"- {e['name']} weight={e['weight']} surface_wr={e['surface']['wr']} hist_wr={e['observed_wr']}" for e in evidence_rows[:20]) or "- none",
        "",
        "## Confidence",
        json.dumps(confidence_components, ensure_ascii=False),
        "",
        "## Decision",
        f"- decision: {decision}",
        f"- confidence: {confidence}",
        f"- final_score: {final_score}",
        "",
        "## Warnings",
        "\n".join(f"- {w}" for w in output["warnings"]) or "- none",
        "",
        "## Fallback",
        f"- used: {health['fallback_used']}",
        "",
        "## Next Step",
        "- This is a parallel V2 engine only; do not wire it into live trading without a separate approval phase.",
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
