#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
import time
from collections import defaultdict, Counter
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
EVIDENCE_SCORES = DATA / "evidence_scores.jsonl"
EVIDENCE_SUMMARY = DATA / "evidence_score_summary.json"
SURFACES = DATA / "probability_surface.jsonl"
SURFACE_SUMMARY = DATA / "probability_surface_summary.json"
CALIBRATION_JSON = DATA / "calibration_profiles_v2.json"
CALIBRATION_JSONL = DATA / "calibration_profiles_v2.jsonl"
EXPLANATIONS = DATA / "trade_outcome_explanations.jsonl"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
AUDITS = DATA / "trade_brain_decision_audit.jsonl"
OUT_JSON = DATA / "trade_brain_learning_candidates.json"
OUT_JSONL = DATA / "trade_brain_learning_candidates.jsonl"
HEALTH = DATA / "trade_brain_learning_health.json"
REPORT = DATA / "trade_brain_learning_report.md"

DEFAULT_EVIDENCES = [
    "iceberg", "absorption", "initiative_flow", "exhaustion", "sweep", "trapped_trader",
    "delta_positive", "delta_negative", "cvd_positive", "cvd_negative", "bullish_bos",
    "bearish_bos", "micro_bos_bullish", "micro_bos_bearish", "fvg", "order_block",
    "breaker_block", "liquidity_grab", "volume_spike", "poc_reclaim", "value_area_high",
    "value_area_low", "whale_pressure", "coinbase_premium", "funding", "etf_flow",
    "scenario", "gate", "bias", "market_context", "macro_context", "historical_edge",
]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_jsonl(path: Path, tail: int = 30000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    raw = subprocess.getoutput(f"tail -n {tail} {path}")
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    return rows


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


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


def _pf(rows: list[dict[str, Any]]) -> float | None:
    gains = 0.0
    losses = 0.0
    for r in rows:
        pnl = _sf(r.get("pnl_r"), None)
        if pnl is None:
            continue
        if pnl > 0:
            gains += pnl
        elif pnl < 0:
            losses += abs(pnl)
    if losses <= 0:
        return float("inf") if gains > 0 else None
    return gains / losses


def _load_evidence_scores() -> list[dict[str, Any]]:
    if EVIDENCE_SCORES.exists():
        return _read_jsonl(EVIDENCE_SCORES)
    audits = _read_jsonl(AUDITS)
    out = []
    for row in audits:
        snap = (row.get("source_snapshot") or {}).get("evidence_score_breakdown") or {}
        if not snap:
            continue
        out.append({
            "trade_id": row.get("trade_id") or row.get("ts"),
            "direction": "long" if _sf(row.get("long_score"), 0.0) >= _sf(row.get("short_score"), 0.0) else "short",
            "pnl_r": None,
            "duration_seconds": None,
            "evidence_scores": snap,
            "raw_confidence": _sf(row.get("confidence"), None),
            "quality": row.get("quality"),
            "decision": row.get("decision"),
            "neutral_factors": list(row.get("neutral_factors") or []),
            "supporting_factors": list(row.get("supporting_factors") or []),
            "opposing_factors": list(row.get("opposing_factors") or []),
        })
    return out


def _join_map() -> dict[str, dict[str, Any]]:
    return {str(r.get("trade_id") or ""): r for r in _read_jsonl(JOIN_FILE)}


def _exp_map() -> dict[str, dict[str, Any]]:
    return {str(r.get("trade_id") or ""): r for r in _read_jsonl(EXPLANATIONS)}


def _surface_rows() -> list[dict[str, Any]]:
    return _read_jsonl(SURFACES)


def _surface_summary() -> dict[str, Any]:
    return _read_json(SURFACE_SUMMARY)


def _calibration_summary() -> dict[str, Any]:
    return _read_json(CALIBRATION_JSON)


def _evidence_tokens(row: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    scores = row.get("evidence_scores") or {}
    for k, v in scores.items():
        out[_norm(k)] = _sf(v, 0.0) or 0.0
    decision_audit = row.get("decision_audit") or {}
    for f in decision_audit.get("supporting_factors") or []:
        out[_norm(f)] = out.get(_norm(f), 0.0) + 1.0
    for f in decision_audit.get("opposing_factors") or []:
        out[_norm(f)] = out.get(_norm(f), 0.0) - 1.0
    for f in decision_audit.get("neutral_factors") or []:
        out.setdefault(_norm(f), 0.0)
    return out


def _direction_from_key(key: str) -> str:
    k = key.lower()
    if any(x in k for x in ("bull", "long", "positive", "buy", "above", "premium", "up")):
        return "long"
    if any(x in k for x in ("bear", "short", "negative", "sell", "below", "down")):
        return "short"
    if k in {"gate", "scenario", "bias", "market_context", "macro_context", "historical_edge"}:
        return "both"
    return "unknown"


def _rel_class(sample_count: int, wlb: float | None, learning_ready: bool) -> str:
    if sample_count < 3:
        return "insufficient"
    if sample_count < 8:
        return "low_sample"
    if wlb is None or wlb < 0.3:
        return "minimal"
    if sample_count < 30 or wlb < 0.45:
        return "moderate"
    return "strong" if learning_ready else "moderate"


def _band(raw: float | None) -> str:
    if raw is None or raw < 0.35:
        return "low"
    if raw < 0.6:
        return "medium"
    return "high"


def _multiplier(direction: str) -> float | None:
    return {
        "increase": 1.15,
        "decrease": 0.85,
        "hold": 1.0,
        "disable_candidate": 0.25,
        "insufficient_data": None,
    }.get(direction)


def _classify(sample_count: int, wr: float | None, avg_r: float | None, wlb: float | None, learning_ready: bool, raw_conf: float | None, loss_rate: float | None) -> str:
    if sample_count < 3 or wlb is None or not learning_ready:
        return "insufficient_data"
    if wr is None or avg_r is None or raw_conf is None or loss_rate is None:
        return "hold"
    if sample_count >= 12 and wr < 0.4 and avg_r < 0 and (wlb < 0.35 if wlb is not None else True):
        return "disable_candidate"
    if wr >= 0.6 and avg_r > 0 and (raw_conf - wr) <= 0.08:
        return "increase"
    if wr <= 0.45 and avg_r <= 0:
        return "decrease"
    return "hold"


def _context_bucket(rows: list[dict[str, Any]], key_fn) -> dict[str, dict[str, Any]]:
    buckets = defaultdict(list)
    for r in rows:
        buckets[key_fn(r)].append(r)
    out = {}
    for k, arr in buckets.items():
        if not k:
            continue
        wins = sum(1 for r in arr if _sf(r.get("pnl_r"), 0.0) > 0)
        losses = sum(1 for r in arr if _sf(r.get("pnl_r"), 0.0) < 0)
        out[k] = {
            "n": len(arr),
            "wr": round(wins / (wins + losses), 4) if (wins + losses) else None,
            "avg_r": round(mean([_sf(r.get("pnl_r"), 0.0) for r in arr]), 4) if arr else None,
            "wilson": _wilson(wins, losses),
        }
    return out


def _join_ctx(join: dict[str, Any], *keys: str) -> str:
    cur: Any = join
    for key in keys:
        if not isinstance(cur, dict):
            return "unknown"
        cur = cur.get(key)
    return _norm(cur)


def _exp_ctx(exp: dict[str, Any], *keys: str) -> str:
    cur: Any = exp
    for key in keys:
        if not isinstance(cur, dict):
            return "unknown"
        cur = cur.get(key)
    return _norm(cur)


def _surface_ctx(surface: dict[str, Any], *keys: str) -> str:
    cur: Any = surface
    for key in keys:
        if not isinstance(cur, dict):
            return "unknown"
        cur = cur.get(key)
    return _norm(cur)


def build_batch() -> dict[str, Any]:
    joins = _join_map()
    exps = _exp_map()
    surfaces = _surface_rows()
    cal_summary = _calibration_summary()
    cal_rows = _read_jsonl(CALIBRATION_JSONL)
    evidence_rows = _load_evidence_scores()

    trade_rows: dict[str, dict[str, Any]] = {}
    trade_evidence: dict[str, dict[str, float]] = {}
    trade_context: dict[str, dict[str, str]] = {}
    trade_surface: dict[str, dict[str, Any]] = {}
    trade_label: dict[str, str] = {}
    trade_learning_ready: dict[str, bool] = {}
    trade_raw_conf: dict[str, float | None] = {}
    trade_tokens: dict[str, set[str]] = {}

    for ev in evidence_rows:
        tid = str(ev.get("trade_id") or "")
        if not tid:
            continue
        join = joins.get(tid) or {}
        exp = exps.get(tid) or {}
        trade_rows[tid] = join
        trade_evidence.setdefault(tid, {}).update(_evidence_tokens(ev))
        trade_context[tid] = {
            "session": _join_ctx(join, "context", "session"),
            "regime": _join_ctx(join, "context", "bias"),
            "scenario": _exp_ctx(exp, "scenario_support", "active_scenario"),
        }
        trade_surface[tid] = next((s for s in surfaces if str(s.get("surface_id") or "").startswith(tid)), {})
        trade_label[tid] = _norm(exp.get("learning_label"))
        trade_learning_ready[tid] = bool(join.get("learning_eligible")) and trade_label[tid] != "not_enough_data"
        trade_raw_conf[tid] = _sf(join.get("decision_audit", {}).get("confidence"), _sf(ev.get("raw_confidence"), None))

    for tid, tokens in list(trade_evidence.items()):
        fv = (trade_surface.get(tid) or {}).get("feature_vector") or {}
        keys = set(DEFAULT_EVIDENCES) | set(tokens.keys()) | {str(k).lower() for k in fv.keys()}
        trade_tokens[tid] = {k for k in keys if k}

    token_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    token_trade_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    token_context_rows: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

    for tid, tokens in trade_tokens.items():
        join = trade_rows.get(tid) or {}
        exp = exps.get(tid) or {}
        pnl = _sf(join.get("pnl_r"), None)
        dur = _sf(join.get("duration_seconds"), None)
        raw_conf = trade_raw_conf.get(tid)
        for token in tokens:
            token_rows[token].append({
                "trade_id": tid,
                "pnl_r": pnl,
                "duration_seconds": dur,
                "raw_confidence": raw_conf,
                "learning_ready": trade_learning_ready.get(tid, False),
                "learning_label": trade_label.get(tid, "not_enough_data"),
            })
            token_trade_rows[token].append(join)
            token_context_rows[token]["session"].append({"context": trade_context.get(tid, {}).get("session"), "pnl_r": pnl})
            token_context_rows[token]["regime"].append({"context": trade_context.get(tid, {}).get("regime"), "pnl_r": pnl})
            token_context_rows[token]["scenario"].append({"context": trade_context.get(tid, {}).get("scenario"), "pnl_r": pnl})

    candidates: list[dict[str, Any]] = []
    for evidence in sorted(token_rows):
        rows = token_rows[evidence]
        trade_rows_for_evidence = token_trade_rows[evidence]
        sample_count = len(rows)
        wins = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) > 0)
        losses = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) < 0)
        total = wins + losses
        wr = wins / total if total else None
        loss_rate = losses / total if total else None
        avg_r = mean([_sf(r.get("pnl_r"), 0.0) for r in rows]) if rows else None
        hold = mean([_sf(r.get("duration_seconds"), 0.0) for r in rows]) if rows else None
        wlb = _wilson(wins, losses)
        pf = _pf(rows)
        raws = [_sf(r.get("raw_confidence"), None) for r in rows if _sf(r.get("raw_confidence"), None) is not None]
        raw_mean = mean(raws) if raws else None
        learning_ready = any(_sf(r.get("learning_ready"), 0.0) for r in rows) and sample_count >= 3
        label_counts = Counter(r.get("learning_label") for r in rows)
        cal_label = "insufficient_data"
        if sample_count >= 3 and raw_mean is not None and wr is not None:
            diff = raw_mean - wr
            if diff >= 0.1:
                cal_label = "overconfident"
            elif diff <= -0.1:
                cal_label = "underconfident"
            elif sample_count >= 8:
                cal_label = "well_calibrated"
        reliability = _rel_class(sample_count, wlb, learning_ready)
        suggested = _classify(sample_count, wr, avg_r, wlb, learning_ready, raw_mean, loss_rate)
        if not learning_ready:
            suggested = "insufficient_data"
        elif sample_count >= 12 and wr is not None and avg_r is not None and wr < 0.4 and avg_r < 0 and (wlb is None or wlb < 0.35):
            suggested = "disable_candidate"
        elif sample_count >= 5 and wr is not None and avg_r is not None and wr >= 0.6 and avg_r > 0 and (wlb is None or wlb >= 0.35) and (raw_mean is None or raw_mean - wr <= 0.08):
            suggested = "increase"
        elif sample_count >= 5 and wr is not None and avg_r is not None and wr <= 0.45 and avg_r <= 0:
            suggested = "decrease"
        elif suggested == "hold" and sample_count < 3:
            suggested = "insufficient_data"

        context_session = _context_bucket(token_context_rows[evidence]["session"], lambda r: _norm(r.get("context")))
        context_regime = _context_bucket(token_context_rows[evidence]["regime"], lambda r: _norm(r.get("context")))
        context_scenario = _context_bucket(token_context_rows[evidence]["scenario"], lambda r: _norm(r.get("context")))

        works_best_in = []
        fails_in = []
        for bucket in (context_session, context_regime, context_scenario):
            for k, v in bucket.items():
                if v.get("wr") is not None and v["wr"] >= 0.6 and k not in works_best_in:
                    works_best_in.append(k)
                if v.get("wr") is not None and v["wr"] <= 0.4 and k not in fails_in:
                    fails_in.append(k)

        risk_flags = []
        if sample_count < 5:
            risk_flags.append("low_sample")
        if wlb is None or wlb < 0.35:
            risk_flags.append("low_wilson")
        if loss_rate is not None and loss_rate >= 0.55:
            risk_flags.append("high_loss_rate")
        if raw_mean is not None and wr is not None and raw_mean - wr >= 0.1:
            risk_flags.append("overconfident")
            risk_flags.append("overconfident_context")
        context_values = {
            trade_context.get(r.get("trade_id") or "", {}).get("session", "unknown")
            for r in trade_rows_for_evidence
        } | {
            trade_context.get(r.get("trade_id") or "", {}).get("regime", "unknown")
            for r in trade_rows_for_evidence
        } | {
            trade_context.get(r.get("trade_id") or "", {}).get("scenario", "unknown")
            for r in trade_rows_for_evidence
        }
        if len({v for v in context_values if v != "unknown"}) > 1:
            risk_flags.append("context_dependent")
        if any(label_counts.get(x, 0) for x in ("bad_but_lucky", "good_but_unlucky")):
            risk_flags.extend([x for x in ("bad_but_lucky", "good_but_unlucky") if label_counts.get(x, 0)])
        if trade_label and label_counts.get("not_enough_data", 0) == sample_count:
            risk_flags.append("not_enough_data")
        risk_flags = sorted(set(risk_flags))

        candidate = {
            "evidence": evidence,
            "direction": _direction_from_key(evidence),
            "current_weight_observed": None,
            "suggested_weight_multiplier": _multiplier(suggested),
            "suggested_direction": suggested,
            "reason": "measurement_only_no_weight_update",
            "stats": {
                "sample_count": sample_count,
                "win_rate": round(wr, 4) if wr is not None else None,
                "loss_rate": round(loss_rate, 4) if loss_rate is not None else None,
                "average_r": round(avg_r, 4) if avg_r is not None else None,
                "wilson_lower_bound": round(wlb, 4) if wlb is not None else None,
                "profit_factor": round(pf, 4) if pf not in (None, float("inf")) else pf,
                "average_hold_seconds": round(hold, 2) if hold is not None else None,
            },
            "calibration": {
                "raw_confidence_mean": round(raw_mean, 4) if raw_mean is not None else None,
                "observed_win_rate": round(wr, 4) if wr is not None else None,
                "confidence_error": round((raw_mean - wr), 4) if raw_mean is not None and wr is not None else None,
                "calibration_label": cal_label,
                "reliability": reliability,
            },
            "context_constraints": {
                "works_best_in": works_best_in[:5],
                "fails_in": fails_in[:5],
                "session_effect": context_session,
                "regime_effect": context_regime,
                "scenario_effect": context_scenario,
            },
            "risk_flags": risk_flags,
            "learning_ready": bool(learning_ready and suggested != "insufficient_data"),
            "manual_approval_required": True,
            "live_update_allowed": False,
        }
        candidate["evidence"] = evidence
        candidates.append(candidate)

    def _sort_key(c: dict[str, Any]) -> tuple[Any, ...]:
        wr = c["stats"]["win_rate"] if c["stats"]["win_rate"] is not None else -1
        avg_r = c["stats"]["average_r"] if c["stats"]["average_r"] is not None else -999
        return (c["learning_ready"], c["stats"]["sample_count"], wr, avg_r)

    increase = [c for c in candidates if c["suggested_direction"] == "increase"]
    decrease = [c for c in candidates if c["suggested_direction"] == "decrease"]
    hold = [c for c in candidates if c["suggested_direction"] == "hold"]
    disable = [c for c in candidates if c["suggested_direction"] == "disable_candidate"]
    insufficient = [c for c in candidates if c["suggested_direction"] == "insufficient_data"]
    top_positive = sorted([c for c in candidates if (c["stats"]["average_r"] or -999) > 0], key=_sort_key, reverse=True)[:10]
    top_negative = sorted([c for c in candidates if (c["stats"]["average_r"] or 999) < 0], key=_sort_key)[:10]
    context_dep = [c for c in candidates if "context_dependent" in c["risk_flags"]][:20]
    overconfident = [c for c in candidates if "overconfident" in c["risk_flags"]][:20]
    underconfident = [c for c in candidates if c["calibration"]["calibration_label"] == "underconfident"][:20]

    eligible = sum(1 for c in candidates if c["learning_ready"])
    global_status = "not_ready" if eligible < 3 else "minimal" if eligible < 8 else "ready"
    summary = {
        "generated_ts": int(time.time() * 1000),
        "status": "measurement_only",
        "live_update_allowed": False,
        "manual_approval_required": True,
        "candidates_count": len(candidates),
        "learning_ready_count": eligible,
        "increase_candidates": increase[:20],
        "decrease_candidates": decrease[:20],
        "hold_candidates": hold[:20],
        "disable_candidates": disable[:20],
        "insufficient_data_candidates": insufficient[:20],
        "top_positive_evidence": top_positive,
        "top_negative_evidence": top_negative,
        "context_dependent_evidence": context_dep,
        "global_learning_readiness": {
            "status": global_status,
            "reason": "conservative reporting-only policy",
            "eligible_trade_count": eligible,
            "minimum_required_for_live_update": 20,
        },
        "warnings": [] if cal_summary else ["calibration_v2_missing"],
    }
    _write_json(OUT_JSON, summary)
    _write_jsonl(OUT_JSONL, summary)

    label_counts = Counter(c["suggested_direction"] for c in candidates)
    health = {
        "status": "alive",
        "last_run_ts": int(time.time() * 1000),
        "mode": "batch",
        "measurement_only": True,
        "live_update_allowed": False,
        "manual_approval_required": True,
        "evidence_seen": len(candidates),
        "candidates_count": len(candidates),
        "learning_ready_count": eligible,
        "increase_count": label_counts.get("increase", 0),
        "decrease_count": label_counts.get("decrease", 0),
        "hold_count": label_counts.get("hold", 0),
        "disable_count": label_counts.get("disable_candidate", 0),
        "insufficient_data_count": label_counts.get("insufficient_data", 0),
        "last_blocker": None,
        "warnings": [] if surfaces else ["probability_surface_missing"],
    }
    _write_json(HEALTH, health)

    def section(title: str, lines: list[str]) -> list[str]:
        return ["", f"## {title}"] + (lines or ["- none"])

    report = [
        "# Trade Brain Learning Candidate Report",
        "",
    ]
    report += section("Status", ["measurement_only; live_update_allowed=false; manual_approval_required=true"])
    report += section("Dataset Health", [
        f"- evidence_seen: {len(candidates)}",
        f"- probability_surfaces_seen: {len(surfaces)}",
        f"- calibration_profiles_seen: {len(cal_rows)}",
        "No live weights were changed.",
    ])
    report += section("Global Learning Readiness", [
        f"- status: {global_status}",
        f"- eligible_trade_count: {eligible}",
        "- policy: conservative reporting-only",
    ])
    report += section("Increase Candidates", [f"- {c['evidence']} ({c['stats']['sample_count']} samples, dir={c['direction']})" for c in increase[:20]])
    report += section("Decrease Candidates", [f"- {c['evidence']} ({c['stats']['sample_count']} samples, dir={c['direction']})" for c in decrease[:20]])
    report += section("Hold Candidates", [f"- {c['evidence']} ({c['stats']['sample_count']} samples, dir={c['direction']})" for c in hold[:20]])
    report += section("Disable Candidates", [f"- {c['evidence']} ({c['stats']['sample_count']} samples, dir={c['direction']})" for c in disable[:20]])
    report += section("Insufficient Data", [f"- {c['evidence']} ({c['stats']['sample_count']} samples, dir={c['direction']})" for c in insufficient[:20]])
    report += section("Context-Dependent Evidence", [f"- {c['evidence']} ({c['stats']['sample_count']} samples)" for c in context_dep[:20]])
    report += section("Overconfident Evidence", [f"- {c['evidence']} ({c['stats']['sample_count']} samples)" for c in overconfident[:20]])
    report += section("Underconfident Evidence", [f"- {c['evidence']} ({c['stats']['sample_count']} samples)" for c in underconfident[:20]])
    report += section("Top Positive Evidence", [f"- {c['evidence']} ({c['stats']['average_r']})" for c in top_positive[:10]])
    report += section("Top Negative Evidence", [f"- {c['evidence']} ({c['stats']['average_r']})" for c in top_negative[:10]])
    report += section("Risk Flags", ["- low_sample", "- low_wilson", "- overconfident", "- high_loss_rate", "- context_dependent"])
    report += section("Manual Approval Notes", ["No live weights were changed.", "manual_approval_required=true", "live_update_allowed=false"])
    report += section("Next Recommended Step", ["Review candidates manually before any future weight update phase."])
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
