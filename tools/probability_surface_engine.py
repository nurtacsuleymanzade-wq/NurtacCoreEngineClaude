#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
EXPLANATIONS_FILE = DATA / "trade_outcome_explanations.jsonl"
EVIDENCE_SCORES_FILE = DATA / "evidence_scores.jsonl"
QUALIFIED_FILE = DATA / "qualified_setups.jsonl"
AUDIT_FILE = DATA / "trade_brain_decision_audit.jsonl"
SURFACE_FILE = DATA / "probability_surface.jsonl"
SUMMARY_FILE = DATA / "probability_surface_summary.json"
HEALTH_FILE = DATA / "probability_surface_health.json"


def _read_jsonl(path: Path, tail: int = 8000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    raw = subprocess.getoutput(f"tail -n {tail} {path}")
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _write_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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


def _latest_by_trade_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        tid = row.get("trade_id")
        if tid:
            out[str(tid)] = row
    return out


def _load_existing_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    for row in _read_jsonl(path):
        sid = row.get("surface_id")
        if sid:
            text = str(sid)
            ids.add(text.split(":", 1)[0])
    return ids


def _wilson_lower_bound(wins: int, losses: int, z: float = 1.96) -> float | None:
    n = wins + losses
    if n <= 0:
        return None
    phat = wins / n
    denom = 1.0 + (z * z) / n
    center = phat + (z * z) / (2.0 * n)
    margin = z * math.sqrt((phat * (1.0 - phat) / n) + (z * z) / (4.0 * n * n))
    return max(0.0, (center - margin) / denom)


def _profit_factor(rows: list[dict[str, Any]]) -> float | None:
    gains = 0.0
    losses = 0.0
    for row in rows:
        pnl = _sf(row.get("pnl_r"), None)
        if pnl is None:
            continue
        if pnl > 0:
            gains += pnl
        elif pnl < 0:
            losses += abs(pnl)
    if losses <= 0:
        return None if gains <= 0 else float("inf")
    return gains / losses


def _feature_candidates(join_row: dict[str, Any], explanation: dict[str, Any], audit: dict[str, Any], qualified: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    obs = explanation.get("scenario_support") or {}
    observer = explanation.get("observer_state") or {}
    paper_context = {}
    paper = explanation.get("paper") or {}
    if paper:
        paper_context = paper.get("context_at_open") or {}
    evidence_blob = evidence or {}
    gate = (join_row.get("decision_audit") or {}).get("decision_reason") or ""
    feature_vector = {
        "gate": _norm((join_row.get("decision_audit") or {}).get("quality") or (join_row.get("decision_audit") or {}).get("decision")),
        "scenario": _norm(obs.get("active_scenario")),
        "trend1m": _norm(paper_context.get("trend_1m")),
        "trend1s": _norm(paper_context.get("trend_1s")),
        "bias": _norm(paper_context.get("market_bias")),
        "price_location": _norm(paper_context.get("price_location") or paper_context.get("location") or (evidence_blob.get("context") or {}).get("price_location")),
        "volume_profile": _norm((evidence_blob.get("volume_profile_bias") or {}).get("shape_bias") or (paper_context.get("profile_shape"))),
        "delta_sign": _norm((evidence_blob.get("candle_dna") or {}).get("delta_state")),
        "cvd_sign": _norm((evidence_blob.get("baseline") or {}).get("cvd_direction")),
        "order_block": _norm((evidence_blob.get("detectors") or {}).get("trapped_trader", {}).get("label") or (evidence_blob.get("detectors") or {}).get("initiative_flow", {}).get("label")),
        "fvg": _norm((evidence_blob.get("detectors") or {}).get("sweep", {}).get("label")),
        "liquidity_grab": _norm((evidence_blob.get("detectors") or {}).get("sweep", {}).get("label") or (evidence_blob.get("liquidation_context") or {}).get("cascade_risk")),
        "iceberg": _norm((evidence_blob.get("detectors") or {}).get("iceberg", {}).get("label")),
        "absorption": _norm((evidence_blob.get("detectors") or {}).get("absorption", {}).get("label")),
        "sweep": _norm((evidence_blob.get("detectors") or {}).get("sweep", {}).get("label")),
        "whale_pressure": _norm((evidence_blob.get("liquidation_context") or {}).get("whale_pressure")),
        "session": _norm((qualified.get("session_at_qualification") or paper_context.get("session") or (evidence_blob.get("context") or {}).get("session"))),
        "historical_edge": _norm((join_row.get("decision_audit") or {}).get("quality")),
        "market_context": _norm((evidence_blob.get("market_context_bias") or {}).get("dominant_bias")),
        "macro_context": _norm((evidence_blob.get("macro_context") or {}).get("directional_bias")),
        "gate_text": _norm(gate),
        "observer_status": _norm(observer.get("status")),
    }
    feature_vector["dynamic_keys"] = sorted([k for k, v in feature_vector.items() if v not in {"unknown", "", None}])
    return feature_vector


def _feature_similarity(a: dict[str, Any], b: dict[str, Any]) -> float:
    keys = sorted((set(a) | set(b)) - {"dynamic_keys"})
    if not keys:
        return 0.0
    same = 0
    total = 0
    for k in keys:
        av = a.get(k)
        bv = b.get(k)
        if av in (None, "", "unknown") and bv in (None, "", "unknown"):
            continue
        total += 1
        if av == bv:
            same += 1
    return round(same / total, 4) if total else 0.0


def _population_label(join_row: dict[str, Any], explanation: dict[str, Any]) -> str:
    if explanation.get("learning_eligible"):
        if _norm(explanation.get("trade_quality")) in {"strong", "good"}:
            return "Only High Quality"
        return "Only Learning Eligible"
    if explanation.get("scenario_support", {}).get("active_scenario"):
        return "Only Scenario Supported"
    if explanation.get("observer_state", {}).get("qualified_setup_id"):
        return "Only Qualified"
    return "All Trades"


def _match_mode(score: float, exact_count: int) -> str:
    if exact_count > 0:
        return "exact"
    if score >= 0.95:
        return "near_exact"
    if score >= 0.90:
        return "high_similarity"
    if score >= 0.85:
        return "partial"
    return "broad"


def _confidence(score: float, wlb: float | None, sample_count: int, learning_ready: bool, reliability: float) -> float:
    components = [x for x in [score, wlb, reliability] if x is not None]
    if not components:
        return 0.0
    base = sum(components) / len(components)
    sample_factor = min(1.0, math.log1p(sample_count) / math.log1p(max(sample_count, 2)))
    learning_factor = 1.0 if learning_ready else 0.75
    return round(max(0.0, min(1.0, base * sample_factor * learning_factor)), 4)


def _reliability(sample_count: int, exact_count: int, learning_ready: bool, avg_score: float) -> float:
    if sample_count <= 0:
        return 0.0
    match_factor = exact_count / sample_count
    sample_factor = min(1.0, math.log1p(sample_count) / math.log1p(sample_count + 1))
    learning_factor = 1.0 if learning_ready else 0.7
    return round(max(0.0, min(1.0, (match_factor + sample_factor + avg_score) / 3.0 * learning_factor)), 4)


def _aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) > 0)
    losses = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) < 0)
    pnl = [x for x in (_sf(r.get("pnl_r"), None) for r in rows) if x is not None]
    hold = [x for x in (_sf(r.get("duration_seconds"), None) for r in rows) if x is not None]
    wr = round(wins / (wins + losses), 4) if (wins + losses) else None
    return {
        "sample_count": len(rows),
        "wins": wins,
        "losses": losses,
        "win_rate": wr,
        "average_r": round(mean(pnl), 4) if pnl else None,
        "profit_factor": _profit_factor(rows),
        "average_hold_time": round(mean(hold), 2) if hold else None,
        "wilson_lower_bound": _wilson_lower_bound(wins, losses),
    }


def _best_matches(target: dict[str, Any], candidates: list[dict[str, Any]], limit: int = 25) -> list[dict[str, Any]]:
    scored = []
    for cand in candidates:
        score = _feature_similarity(target["feature_vector"], cand["feature_vector"])
        if score <= 0:
            continue
        scored.append((score, cand))
    scored.sort(key=lambda x: (x[0], x[1].get("win_rate") or 0.0, x[1].get("sample_count") or 0), reverse=True)
    return [{"score": s, **c} for s, c in scored[:limit]]


def _summaries(surfaces: list[dict[str, Any]]) -> dict[str, Any]:
    if not surfaces:
        return {
            "total_surfaces": 0,
            "learning_ready_surfaces": 0,
            "highest_confidence_surface": None,
            "largest_sample_surface": None,
            "best_wr_surface": None,
            "worst_wr_surface": None,
            "best_profit_factor_surface": None,
            "largest_population": None,
            "average_confidence": 0.0,
        }
    by_conf = max(surfaces, key=lambda r: r.get("confidence") or 0.0)
    by_sample = max(surfaces, key=lambda r: r.get("sample_count") or 0)
    by_wr = max(surfaces, key=lambda r: r.get("win_rate") or 0.0)
    worst_wr = min(surfaces, key=lambda r: r.get("win_rate") if r.get("win_rate") is not None else 999.0)
    by_pf = max(surfaces, key=lambda r: r.get("profit_factor") if r.get("profit_factor") not in (None, float("inf")) else -1.0)
    largest_population = max(surfaces, key=lambda r: (r.get("nearest_population") or {}).get("sample_count") or 0)
    confidences = [r.get("confidence") or 0.0 for r in surfaces]
    return {
        "total_surfaces": len(surfaces),
        "learning_ready_surfaces": sum(1 for r in surfaces if r.get("learning_ready")),
        "highest_confidence_surface": by_conf,
        "largest_sample_surface": by_sample,
        "best_wr_surface": by_wr,
        "worst_wr_surface": worst_wr,
        "best_profit_factor_surface": by_pf,
        "largest_population": largest_population,
        "average_confidence": round(mean(confidences), 4) if confidences else 0.0,
    }


def build_batch() -> dict[str, Any]:
    join_rows = _read_jsonl(JOIN_FILE)
    explanations = _latest_by_trade_id(_read_jsonl(EXPLANATIONS_FILE))
    audits = _latest_by_trade_id(_read_jsonl(AUDIT_FILE))
    qualified_rows = _latest_by_trade_id(_read_jsonl(QUALIFIED_FILE))
    evidence_rows = _latest_by_trade_id(_read_jsonl(EVIDENCE_SCORES_FILE))
    existing = _load_existing_ids(SURFACE_FILE)
    surfaces: list[dict[str, Any]] = []
    written = 0
    warnings: list[str] = []
    latest_surface = None

    # Historical population is the join file itself; this keeps the engine
    # descriptive and prevents any forward-looking leakage.
    population_rows = [r for r in join_rows if str(r.get("trade_id") or "") in explanations]

    for join_row in population_rows:
        trade_id = str(join_row.get("trade_id") or "")
        if not trade_id or trade_id in existing:
            continue
        explanation = explanations.get(trade_id) or {}
        audit = audits.get(trade_id) or join_row.get("decision_audit") or {}
        qualified = qualified_rows.get(str(join_row.get("source_setup_id") or "")) or {}
        evidence = evidence_rows.get(str(join_row.get("source_setup_id") or "")) or {}

        feature_vector = _feature_candidates(join_row, explanation, audit, qualified, evidence)
        population_label = _population_label(join_row, explanation)

        historical = []
        for past in population_rows:
            if str(past.get("trade_id") or "") == trade_id:
                continue
            past_exp = explanations.get(str(past.get("trade_id") or "")) or {}
            past_audit = audits.get(str(past.get("trade_id") or "")) or past.get("decision_audit") or {}
            past_qualified = qualified_rows.get(str(past.get("source_setup_id") or "")) or {}
            past_evidence = evidence_rows.get(str(past.get("source_setup_id") or "")) or {}
            historical.append({
                "trade_id": past.get("trade_id"),
                "feature_vector": _feature_candidates(past, past_exp, past_audit, past_qualified, past_evidence),
                "pnl_r": past.get("pnl_r"),
                "duration_seconds": past.get("duration_seconds"),
                "learning_eligible": bool(past.get("learning_eligible")),
                "outcome": past.get("outcome"),
            })

        exact_matches = [x for x in historical if x["feature_vector"] == feature_vector]
        best_matches = _best_matches({"feature_vector": feature_vector}, historical)
        nearest_population = {
            "population": population_label,
            "match_mode": _match_mode(best_matches[0]["score"] if best_matches else 0.0, len(exact_matches)),
            "top_matches": best_matches[:10],
        }
        matched_rows = [row for row in historical if (row["trade_id"] in {m["trade_id"] for m in exact_matches})]
        if not matched_rows:
            matched_rows = [historical[i] for i in range(min(len(best_matches), len(historical)))] if historical else []
        if not matched_rows and best_matches:
            matched_rows = [historical[0]]
        matched_rows = matched_rows or historical

        stats = _aggregate([
            {
                "pnl_r": m.get("pnl_r"),
                "duration_seconds": m.get("duration_seconds"),
            }
            for m in matched_rows
        ])
        wlb = stats.get("wilson_lower_bound")
        exact_count = len(exact_matches)
        avg_score = round(sum(m.get("score", 0.0) for m in best_matches[:10]) / len(best_matches[:10]), 4) if best_matches[:10] else 0.0
        reliability = _reliability(stats["sample_count"], exact_count, bool(explanation.get("learning_eligible")), avg_score)
        confidence = _confidence(best_matches[0]["score"] if best_matches else 0.0, wlb, stats["sample_count"], bool(explanation.get("learning_eligible")), reliability)
        learning_ready = bool(explanation.get("learning_eligible")) and stats["sample_count"] >= 3 and wlb is not None
        record = {
            "surface_id": f"{trade_id}:{int(time.time() * 1000)}",
            "generated_ts": int(time.time() * 1000),
            "trade_direction": join_row.get("direction"),
            "feature_vector": feature_vector,
            "nearest_population": nearest_population,
            "sample_count": stats["sample_count"],
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_rate": stats["win_rate"],
            "average_r": stats["average_r"],
            "profit_factor": stats["profit_factor"],
            "wilson_lower_bound": wlb,
            "confidence": confidence,
            "reliability": reliability,
            "learning_ready": learning_ready,
            "warnings": warnings,
            "explanation": (
                f"Most similar historical setups: {stats['sample_count']} samples, "
                f"{round((stats['win_rate'] or 0.0) * 100, 2) if stats['win_rate'] is not None else 'n/a'}% WR, "
                f"{round((wlb or 0.0) * 100, 2) if wlb is not None else 'n/a'}% Wilson LB, "
                f"Average R = {stats['average_r'] if stats['average_r'] is not None else 'n/a'}, "
                f"Historical confidence {'high' if confidence >= 0.75 else 'medium-high' if confidence >= 0.5 else 'low'}"
            ),
            "population_label": population_label,
            "top_matches": nearest_population["top_matches"],
            "engine": "probability_surface_engine",
            "note": "Append-only probability surface snapshot. No trading logic changed.",
        }
        _write_jsonl(SURFACE_FILE, record)
        existing.add(trade_id)
        surfaces.append(record)
        latest_surface = record
        written += 1

    all_surfaces = _read_jsonl(SURFACE_FILE, tail=20000)
    summary = _summaries(all_surfaces)
    _write_json(SUMMARY_FILE, summary)
    health = {
        "status": "alive",
        "records_written": written,
        "surfaces_generated": len(surfaces),
        "largest_population": (summary.get("largest_population") or {}).get("population_label"),
        "latest_surface": latest_surface,
        "warnings": warnings,
        "last_blocker": None,
    }
    _write_json(HEALTH_FILE, health)
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
