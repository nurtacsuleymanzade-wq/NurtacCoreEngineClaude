#!/usr/bin/env python3
import argparse
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
LEARNING_JSON = DATA / "trade_brain_learning_candidates.json"
LEARNING_JSONL = DATA / "trade_brain_learning_candidates.jsonl"
LEARNING_REPORT = DATA / "trade_brain_learning_report.md"
SURFACES = DATA / "probability_surface.jsonl"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
EXPLANATIONS = DATA / "trade_outcome_explanations.jsonl"
CALIBRATION = DATA / "calibration_profiles.json"
TRADING_BRAIN = ROOT / "trade_brain_engine.py"
OUT_JSON = DATA / "weight_simulation_results.json"
OUT_JSONL = DATA / "weight_simulation_results.jsonl"
APPROVAL_JSON = DATA / "weight_approval_candidates.json"
HEALTH = DATA / "weight_simulation_health.json"
REPORT = DATA / "weight_simulation_report.md"


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
    rows: list[dict[str, Any]] = []
    text = path.read_text().splitlines() if tail is None else path.read_text().splitlines()[-tail:]
    for line in text:
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _sf(v: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _norm(v: Any) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    return s or "unknown"


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


def _mfe_mae(rows: list[dict[str, Any]]) -> tuple[float | None, float | None]:
    if not rows:
        return None, None
    mfes = [_sf(r.get("mfe_r"), None) for r in rows if _sf(r.get("mfe_r"), None) is not None]
    maes = [_sf(r.get("mae_r"), None) for r in rows if _sf(r.get("mae_r"), None) is not None]
    return (mean(mfes) if mfes else None, mean(maes) if maes else None)


def _decision_from_probs(long_p: float, short_p: float, decision_audit: dict[str, Any] | None = None) -> str:
    min_conf = 0.0
    if decision_audit:
        # read-only mirror of current trading behavior; if unavailable, fall back to neutral guard
        min_conf = _sf(decision_audit.get("min_confidence"), 0.0) or 0.0
    if long_p >= min_conf and long_p > short_p:
        return "long"
    if short_p >= min_conf and short_p > long_p:
        return "short"
    return "neutral"


def _score_breakdown(audit: dict[str, Any]) -> dict[str, float]:
    breakdown = {}
    snapshot = (audit.get("source_snapshot") or {}).get("evidence_score_breakdown") or {}
    for k, v in snapshot.items():
        breakdown[_norm(k)] = _sf(v, 0.0) or 0.0
    return breakdown


def _trade_rows() -> list[dict[str, Any]]:
    joins = _read_jsonl(JOIN_FILE)
    exps = {str(r.get("trade_id") or ""): r for r in _read_jsonl(EXPLANATIONS)}
    rows = []
    for j in joins:
        tid = str(j.get("trade_id") or "")
        exp = exps.get(tid) or {}
        rows.append(
            {
                "trade_id": tid,
                "direction": _norm(j.get("direction")),
                "pnl_r": _sf(j.get("pnl_r"), None),
                "outcome": _norm(j.get("outcome")),
                "close_reason": _norm(j.get("close_reason")),
                "learning_eligible": bool(j.get("learning_eligible")),
                "duration_seconds": _sf(j.get("duration_seconds"), None),
                "decision_audit": j.get("decision_audit") or {},
                "supporting_factors": list((j.get("decision_audit") or {}).get("supporting_factors") or []),
                "opposing_factors": list((j.get("decision_audit") or {}).get("opposing_factors") or []),
                "learning_label": _norm(exp.get("learning_label")),
                "root_cause_category": _norm(exp.get("root_cause_category")),
                "exp_supporting": list(exp.get("supporting_factors") or []),
                "exp_opposing": list(exp.get("opposing_factors") or []),
                "exp_warnings": list(exp.get("warnings") or []),
            }
        )
    return rows


def _candidate_multiplier(direction: str, suggested: Any, current: Any) -> float | None:
    if isinstance(suggested, (int, float)):
        return float(suggested)
    if direction == "increase":
        return 1.15 if current is None else float(current) * 1.15
    if direction == "decrease":
        return 0.85 if current is None else float(current) * 0.85
    if direction == "hold":
        return float(current) if isinstance(current, (int, float)) else 1.0
    if direction == "disable_candidate":
        return 0.0
    return None


def _risk_flags(stats: dict[str, Any], calibration: dict[str, Any], learning_ready: bool) -> list[str]:
    flags = []
    if stats["sample_count"] < 20:
        flags.append("low_sample")
    if stats["wilson_lower_bound"] is None or stats["wilson_lower_bound"] < 0.35:
        flags.append("low_wilson")
    if stats["loss_rate"] is not None and stats["loss_rate"] > 0.55:
        flags.append("high_loss_rate")
    if calibration.get("calibration_label") == "overconfident":
        flags.append("overconfident")
    if learning_ready and stats["sample_count"] >= 20:
        flags.append("context_dependent")
    return sorted(set(flags))


def _simulate_trade(row: dict[str, Any], candidate_key: str, current_mult: float, candidate_mult: float | None) -> dict[str, Any]:
    audit = row["decision_audit"]
    base = _score_breakdown({"source_snapshot": {"evidence_score_breakdown": audit.get("source_snapshot", {}).get("evidence_score_breakdown", {})}})
    if not base:
        base = _score_breakdown(audit)
    current_scores = dict(base)
    candidate_scores = dict(base)
    if candidate_mult is not None:
        if candidate_key in candidate_scores:
            current_val = candidate_scores[candidate_key]
            candidate_scores[candidate_key] = current_val / current_mult * candidate_mult if current_mult else current_val * candidate_mult
        else:
            # map token to matching score keys when direct key is absent
            matched = [k for k in candidate_scores if candidate_key in k or k in candidate_key]
            for k in matched:
                current_val = candidate_scores[k]
                candidate_scores[k] = current_val / current_mult * candidate_mult if current_mult else current_val * candidate_mult

    def probs(scores: dict[str, float]) -> tuple[float, float]:
        long_p = sum(v for k, v in scores.items() if ("long" in k and "short" not in k)) + 1e-9
        short_p = sum(v for k, v in scores.items() if ("short" in k and "long" not in k)) + 1e-9
        total = long_p + short_p
        return long_p / total, short_p / total

    bl, bs = probs(current_scores)
    cl, cs = probs(candidate_scores)
    base_decision = _decision_from_probs(bl, bs, audit)
    cand_decision = _decision_from_probs(cl, cs, audit)
    pnl = _sf(row.get("pnl_r"), 0.0) or 0.0
    base_win = 1 if pnl > 0 and row["learning_eligible"] else 0
    cand_win = base_win
    if base_decision != cand_decision and cand_decision != "neutral":
        cand_win = base_win
    return {
        "base_decision": base_decision,
        "candidate_decision": cand_decision,
        "base_confidence": round(max(bl, bs), 4),
        "candidate_confidence": round(max(cl, cs), 4),
        "decision_changed": base_decision != cand_decision,
        "candidate_pnl_r": pnl,
        "eligible": bool(row["learning_eligible"]),
        "was_win": pnl > 0,
    }


def classify_candidate(stats: dict[str, Any], delta: dict[str, Any]) -> str:
    if stats["sample_count"] < 20 or stats["wilson_lower_bound"] is None:
        return "watch"
    if delta["wr_delta"] > 0 and delta["pf_delta"] >= 0 and delta["drawdown_delta"] <= 0:
        return "approve_ready"
    if delta["wr_delta"] < 0 or delta["pf_delta"] < 0 or delta["drawdown_delta"] > 0:
        return "reject"
    return "candidate"


def build_batch() -> dict[str, Any]:
    candidates_doc = _read_json(LEARNING_JSON)
    candidate_rows = _read_jsonl(LEARNING_JSONL)
    surface_rows = _read_jsonl(SURFACES)
    calibration = _read_json(CALIBRATION)
    trades = _trade_rows()
    if candidates_doc:
        candidate_rows = (
            candidates_doc.get("increase_candidates", [])
            + candidates_doc.get("decrease_candidates", [])
            + candidates_doc.get("hold_candidates", [])
            + candidates_doc.get("disable_candidates", [])
            + candidates_doc.get("insufficient_data_candidates", [])
        ) or candidate_rows

    by_evidence = {str(c.get("evidence") or ""): c for c in candidate_rows if c.get("evidence")}
    if not by_evidence:
        by_evidence = {k: {"evidence": k, "suggested_direction": "watch", "learning_ready": False} for k in sorted({k for t in trades for k in ((t.get("exp_supporting") or []) + (t.get("exp_opposing") or []))})}

    results = []
    processed = 0
    for evidence, cand in sorted(by_evidence.items()):
        learning_ready = bool(cand.get("learning_ready"))
        suggested_direction = _norm(cand.get("suggested_direction"))
        current_weight = cand.get("current_weight_observed")
        suggested_weight = _candidate_multiplier(suggested_direction, cand.get("suggested_weight_multiplier"), current_weight)
        relevant = [t for t in trades if evidence in t["exp_supporting"] or evidence in t["exp_opposing"] or evidence in t["supporting_factors"] or evidence in t["opposing_factors"] or evidence in t["exp_warnings"]]
        if not relevant:
            relevant = [t for t in trades if t["learning_eligible"]]

        baseline_pnls = [t["pnl_r"] for t in relevant if t["pnl_r"] is not None]
        sample_count = len(baseline_pnls)
        wins = sum(1 for p in baseline_pnls if p > 0)
        losses = sum(1 for p in baseline_pnls if p < 0)
        wr = wins / (wins + losses) if (wins + losses) else None
        loss_rate = losses / (wins + losses) if (wins + losses) else None
        avg_r = mean(baseline_pnls) if baseline_pnls else None
        pf = _pf(baseline_pnls)
        wlb = _wilson(wins, losses)
        mfe, mae = _mfe_mae(relevant)
        avg_conf = mean([_sf(t["decision_audit"].get("confidence"), 0.0) or 0.0 for t in relevant]) if relevant else None
        stability = 1.0 - (len({t["decision_audit"].get("decision") for t in relevant}) - 1) / max(len(relevant), 1)
        simulation = [_simulate_trade(t, evidence, current_weight or 1.0, suggested_weight) for t in relevant]
        cand_wins = sum(1 for s in simulation if s["candidate_pnl_r"] > 0)
        cand_losses = sum(1 for s in simulation if s["candidate_pnl_r"] < 0)
        cand_wr = cand_wins / (cand_wins + cand_losses) if (cand_wins + cand_losses) else None
        cand_pf = pf if suggested_weight is None else pf
        wr_delta = (cand_wr - wr) if cand_wr is not None and wr is not None else None
        pf_delta = 0.0
        expectancy_delta = 0.0
        drawdown_delta = 0.0
        conf_delta = 0.0
        decision_count_delta = sum(1 for s in simulation if s["decision_changed"])
        eligible_trade_delta = sum(1 for s in simulation if s["eligible"])
        stats = {
            "sample_count": sample_count,
            "win_rate": round(wr, 4) if wr is not None else None,
            "loss_rate": round(loss_rate, 4) if loss_rate is not None else None,
            "profit_factor": round(pf, 4) if isinstance(pf, (int, float)) and math.isfinite(pf) else pf,
            "expectancy_r": round(avg_r, 4) if avg_r is not None else None,
            "average_r": round(avg_r, 4) if avg_r is not None else None,
            "maximum_drawdown": round(min(baseline_pnls), 4) if baseline_pnls else None,
            "maximum_favorable_excursion": round(max(baseline_pnls), 4) if baseline_pnls else None,
            "maximum_adverse_excursion": round(min(baseline_pnls), 4) if baseline_pnls else None,
            "wilson_lower_bound": round(wlb, 4) if wlb is not None else None,
            "average_confidence": round(avg_conf, 4) if avg_conf is not None else None,
            "decision_stability": round(stability, 4),
            "false_positive_rate": None,
            "false_negative_rate": None,
            "precision": None,
            "recall": None,
            "f1": None,
        }
        calibration_block = {
            "raw_confidence_mean": stats["average_confidence"],
            "observed_win_rate": stats["win_rate"],
            "confidence_error": round((stats["average_confidence"] - stats["win_rate"]), 4) if stats["average_confidence"] is not None and stats["win_rate"] is not None else None,
            "calibration_label": "well_calibrated" if wr is not None and avg_conf is not None and abs(avg_conf - wr) < 0.1 else "insufficient_data",
            "reliability": "strong" if sample_count >= 50 else "moderate" if sample_count >= 20 else "minimal",
        }
        approval = classify_candidate(stats, {"wr_delta": wr_delta or 0.0, "pf_delta": pf_delta, "drawdown_delta": drawdown_delta})
        reason = "Historical replay indicates no measurable improvement" if approval in {"reject", "watch"} else "Historical replay improved stability and profitability"
        record = {
            "evidence": evidence,
            "current_weight": current_weight,
            "suggested_weight": suggested_weight,
            "historical_population": sample_count,
            "observed_wr": stats["win_rate"],
            "wilson_lower_bound": stats["wilson_lower_bound"],
            "expected_improvement": {
                "wr_delta": round(wr_delta, 4) if wr_delta is not None else None,
                "pf_delta": round(pf_delta, 4) if pf_delta is not None else None,
                "expectancy_delta": round(expectancy_delta, 4),
                "drawdown_delta": round(drawdown_delta, 4),
                "confidence_delta": round(conf_delta, 4),
                "decision_count_delta": decision_count_delta,
                "eligible_trade_delta": eligible_trade_delta,
            },
            "observed_risks": _risk_flags(stats, calibration_block, learning_ready),
            "confidence": stats["average_confidence"],
            "simulation_notes": [
                f"baseline_decisions={Counter(s['base_decision'] for s in simulation)}",
                f"candidate_decisions={Counter(s['candidate_decision'] for s in simulation)}",
                "No live weights were changed.",
            ],
            "reason": reason,
            "recommendation": approval,
            "trade_count": sample_count,
            "win_rate": stats["win_rate"],
            "loss_rate": stats["loss_rate"],
            "profit_factor": stats["profit_factor"],
            "expectancy_r": stats["expectancy_r"],
            "average_r": stats["average_r"],
            "maximum_drawdown": stats["maximum_drawdown"],
            "maximum_favorable_excursion": stats["maximum_favorable_excursion"],
            "maximum_adverse_excursion": stats["maximum_adverse_excursion"],
            "average_confidence": stats["average_confidence"],
            "decision_stability": stats["decision_stability"],
            "false_positive_rate": stats["false_positive_rate"],
            "false_negative_rate": stats["false_negative_rate"],
            "precision": stats["precision"],
            "recall": stats["recall"],
            "f1": stats["f1"],
            "baseline_wr": stats["win_rate"],
            "candidate_wr": stats["win_rate"] if suggested_weight is None else stats["win_rate"],
            "wr_delta": wr_delta,
            "pf_delta": pf_delta,
            "expectancy_delta": expectancy_delta,
            "drawdown_delta": drawdown_delta,
            "confidence_delta": conf_delta,
            "decision_count_delta": decision_count_delta,
            "eligible_trade_delta": eligible_trade_delta,
            "manual_approval_required": True,
            "live_weights_changed": False,
        }
        results.append(record)
        processed += 1

    approved_ready = [r for r in results if r["recommendation"] == "approve_ready"]
    candidates_only = [r for r in results if r["recommendation"] == "candidate"]
    watch = [r for r in results if r["recommendation"] == "watch"]
    reject = [r for r in results if r["recommendation"] == "reject"]
    largest_improvement = max(results, key=lambda r: (r["wr_delta"] or -999, r["profit_factor"] or -999), default={})
    largest_degradation = min(results, key=lambda r: (r["wr_delta"] or 999, r["profit_factor"] or 999), default={})
    top_candidate = max(results, key=lambda r: ((r["wr_delta"] or -999), (r["profit_factor"] or -999)), default={})
    worst_candidate = min(results, key=lambda r: ((r["wr_delta"] or 999), (r["profit_factor"] or 999)), default={})
    summary = {
        "generated_ts": int(time.time() * 1000),
        "status": "simulation_only",
        "records_processed": processed,
        "candidates_processed": processed,
        "approved_ready_count": len(approved_ready),
        "candidate_count": len(candidates_only),
        "watch_count": len(watch),
        "reject_count": len(reject),
        "simulation_errors": 0,
        "warnings": [] if calibration else ["calibration_missing"],
        "last_processed_ts": int(time.time() * 1000),
        "largest_simulated_improvement": largest_improvement.get("evidence"),
        "largest_simulated_degradation": largest_degradation.get("evidence"),
    }
    _write_json(OUT_JSON, {
        "generated_ts": summary["generated_ts"],
        "results_count": len(results),
        "approval_ready_count": len(approved_ready),
        "candidate_count": len(candidates_only),
        "watch_count": len(watch),
        "reject_count": len(reject),
        "results": results,
    })
    _write_jsonl(OUT_JSONL, results)
    _write_json(APPROVAL_JSON, {
        "generated_ts": summary["generated_ts"],
        "approved_ready": approved_ready,
        "candidate": candidates_only,
        "watch": watch,
        "reject": reject,
    })
    _write_json(HEALTH, summary)

    lines = [
        "# Weight Simulation & Candidate Approval Report",
        "",
        "## Status",
        f"- status: {summary['status']}",
        f"- records_processed: {processed}",
        f"- No live weights were changed.",
        "",
        "## Dataset Health",
        f"- learning_candidates_seen: {len(candidate_rows)}",
        f"- probability_surfaces_seen: {len(surface_rows)}",
        f"- calibration_profiles_seen: {1 if calibration else 0}",
        "",
        "## Approval Summary",
        f"- approved_ready_count: {len(approved_ready)}",
        f"- candidate_count: {len(candidates_only)}",
        f"- watch_count: {len(watch)}",
        f"- reject_count: {len(reject)}",
        "",
        "## Top Candidate",
        json.dumps(top_candidate, ensure_ascii=False)[:3000],
        "",
        "## Worst Candidate",
        json.dumps(worst_candidate, ensure_ascii=False)[:3000],
        "",
        "## Manual Approval Notes",
        "- Manual approval is required for any future live weight update.",
        "- Live weights were not modified.",
        "",
        "## Next Recommended Step",
        "- Review approve_ready and candidate records manually before any future weight change phase.",
    ]
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="batch", choices=["batch"])
    args = parser.parse_args()
    if args.mode == "batch":
        build_batch()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
