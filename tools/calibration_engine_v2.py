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
SURFACES = DATA / "probability_surface.jsonl"
SURFACE_SUMMARY = DATA / "probability_surface_summary.json"
EVIDENCE_SCORES = DATA / "evidence_scores.jsonl"
EVIDENCE_SUMMARY = DATA / "evidence_score_summary.json"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
EXPLANATIONS = DATA / "trade_outcome_explanations.jsonl"
AUDITS = DATA / "trade_brain_decision_audit.jsonl"
OUT_JSON = DATA / "calibration_profiles_v2.json"
OUT_JSONL = DATA / "calibration_profiles_v2.jsonl"
HEALTH = DATA / "calibration_health_v2.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_jsonl(path: Path, tail: int = 20000) -> list[dict[str, Any]]:
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


def _wilson_lower_bound(wins: int, losses: int, z: float = 1.96) -> float | None:
    n = wins + losses
    if n <= 0:
        return None
    phat = wins / n
    denom = 1 + (z * z) / n
    center = phat + (z * z) / (2 * n)
    margin = z * math.sqrt((phat * (1 - phat) / n) + (z * z / (4 * n * n)))
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
        return float("inf") if gains > 0 else None
    return gains / losses


def _signature_from_vector(vector: dict[str, Any]) -> str:
    keys = sorted(k for k, v in vector.items() if k != "dynamic_keys" and _norm(v) != "unknown")
    parts = [f"{k}={_norm(vector.get(k))}" for k in keys]
    return "|".join(parts) if parts else "empty"


def _feature_vector_from_surface(surface: dict[str, Any]) -> dict[str, Any]:
    fv = dict(surface.get("feature_vector") or {})
    fv.pop("dynamic_keys", None)
    return fv


def _confidence_band(v: float | None) -> str:
    if v is None or v < 0.35:
        return "low"
    if v < 0.60:
        return "medium"
    return "high"


def _calibration_label(raw_conf: float | None, wr: float | None, n: int, learning_ready: bool) -> str:
    if n < 3 or raw_conf is None or wr is None:
        return "insufficient_data"
    if not learning_ready:
        return "insufficient_data"
    diff = raw_conf - wr
    if diff >= 0.10:
        return "overconfident"
    if diff <= -0.10:
        return "underconfident"
    return "well_calibrated"


def _reliability_class(n: int, wlb: float | None, learning_ready: bool) -> str:
    if n < 3:
        return "insufficient"
    if n < 8:
        return "low_sample"
    if wlb is None or wlb < 0.30:
        return "minimal"
    if n < 30 or wlb < 0.45:
        return "moderate"
    return "strong" if learning_ready else "moderate"


def _calibrated_confidence(raw_conf: float | None, wr: float | None, wlb: float | None, avg_r: float | None, pf: float | None, match_score: float | None, learning_ready: bool) -> float | None:
    vals = [v for v in [raw_conf, wr, wlb, match_score] if v is not None]
    if not vals:
        return None
    base = sum(vals) / len(vals)
    edge_bonus = 0.0
    if avg_r is not None:
        edge_bonus += max(-0.05, min(0.05, avg_r / 20.0))
    if pf is not None and math.isfinite(pf):
        edge_bonus += max(-0.05, min(0.05, (pf - 1.0) / 20.0))
    if not learning_ready:
        base *= 0.85
    return round(max(0.0, min(1.0, base + edge_bonus)), 4)


def _population_rows() -> list[dict[str, Any]]:
    rows = _read_jsonl(SURFACES)
    if rows:
        return rows
    # Fallback uses explanation/join if surfaces are unavailable.
    explanations = _read_jsonl(EXPLANATIONS)
    joins = {str(r.get("trade_id") or ""): r for r in _read_jsonl(JOIN_FILE)}
    audits = {str(r.get("trade_id") or ""): r for r in _read_jsonl(AUDITS)}
    out = []
    for exp in explanations:
        tid = str(exp.get("trade_id") or "")
        join = joins.get(tid) or {}
        audit = audits.get(tid) or join.get("decision_audit") or {}
        out.append({
            "surface_id": tid,
            "generated_ts": int(time.time() * 1000),
            "trade_direction": exp.get("direction") or join.get("direction"),
            "feature_vector": exp.get("feature_vector") or {
                "decision": _norm(audit.get("decision")),
                "quality": _norm(audit.get("quality")),
                "close_reason": _norm(join.get("close_reason")),
            },
            "nearest_population": {"population": "fallback"},
            "sample_count": 1,
            "wins": 1 if _sf(join.get("pnl_r"), 0.0) > 0 else 0,
            "losses": 1 if _sf(join.get("pnl_r"), 0.0) < 0 else 0,
            "win_rate": 1.0 if _sf(join.get("pnl_r"), 0.0) > 0 else 0.0,
            "average_r": _sf(join.get("pnl_r"), None),
            "profit_factor": None,
            "wilson_lower_bound": 0.0,
            "confidence": 0.0,
            "reliability": "insufficient",
            "learning_ready": bool(join.get("learning_eligible")),
            "warnings": [],
        })
    return out


def _group_key(surface: dict[str, Any]) -> tuple[str, str]:
    fv = _feature_vector_from_surface(surface)
    return surface.get("trade_direction") or "unknown", _signature_from_vector(fv)


def _evidence_candidates(surfaces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence_rows = _read_jsonl(EVIDENCE_SCORES)
    if not evidence_rows:
        return []
    by_layer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence_rows:
        for k, v in (row.get("evidence_components") or {}).items():
            if isinstance(v, dict):
                label = v.get("label")
                if label:
                    by_layer[k].append(row)
    out = []
    for layer, rows in by_layer.items():
        wins = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) > 0)
        losses = sum(1 for r in rows if _sf(r.get("pnl_r"), 0.0) < 0)
        wr = wins / (wins + losses) if (wins + losses) else None
        wlb = _wilson_lower_bound(wins, losses)
        direction = "hold"
        if wr is not None and wlb is not None:
            if wr > 0.55 and wlb > 0.45:
                direction = "increase"
            elif wr < 0.45 or (wlb is not None and wlb < 0.35):
                direction = "decrease"
        out.append({
            "evidence": layer,
            "observed_wr": round(wr, 4) if wr is not None else None,
            "n": wins + losses,
            "wilson": wlb,
            "suggested_direction": direction,
            "reason": "measurement_only_no_weight_update",
        })
    return out[:25]


def _global_metrics(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    total_n = sum(p["population"]["sample_count"] for p in profiles)
    wrs = [p["population"]["win_rate"] for p in profiles if p["population"]["win_rate"] is not None]
    raw_confs = [p["calibration"]["raw_confidence_mean"] for p in profiles if p["calibration"]["raw_confidence_mean"] is not None]
    cal_confs = [p["calibration"]["calibrated_confidence"] for p in profiles if p["calibration"]["calibrated_confidence"] is not None]
    errs = [p["calibration"]["confidence_error"] for p in profiles if p["calibration"]["confidence_error"] is not None]
    return {
        "total_n": total_n,
        "global_wr": round(sum(wrs) / len(wrs), 4) if wrs else None,
        "avg_raw_confidence": round(sum(raw_confs) / len(raw_confs), 4) if raw_confs else None,
        "avg_calibrated_confidence": round(sum(cal_confs) / len(cal_confs), 4) if cal_confs else None,
        "global_confidence_error": round(sum(errs) / len(errs), 4) if errs else None,
    }


def build_batch() -> dict[str, Any]:
    surfaces = _population_rows()
    surface_summary = _read_json(SURFACE_SUMMARY)
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for s in surfaces:
        groups[_group_key(s)].append(s)

    profiles: list[dict[str, Any]] = []
    last_profile_id = None
    for (direction, signature), bucket in groups.items():
        sample_count = len(bucket)
        wins = sum(1 for r in bucket if _sf(r.get("pnl_r"), 0.0) > 0)
        losses = sum(1 for r in bucket if _sf(r.get("pnl_r"), 0.0) < 0)
        win_rate = wins / (wins + losses) if (wins + losses) else None
        avg_r = mean([_sf(r.get("average_r"), 0.0) for r in bucket]) if bucket else None
        pf = _profit_factor(bucket)
        wlb = _wilson_lower_bound(wins, losses)
        raw_conf = mean([_sf(r.get("confidence"), 0.0) for r in bucket]) if bucket else None
        learning_ready = any(bool(r.get("learning_ready")) for r in bucket)
        match_scores = [max([_sf(m.get("score"), 0.0) for m in (r.get("top_matches") or [])] or [0.0]) for r in bucket]
        match_score = mean(match_scores) if match_scores else None
        reliability = _reliability_class(sample_count, wlb, learning_ready)
        calibration_label = _calibration_label(raw_conf, win_rate, sample_count, learning_ready)
        band = _confidence_band(raw_conf)
        calibrated = _calibrated_confidence(raw_conf, win_rate, wlb, avg_r, pf, match_score, learning_ready)
        error = round((raw_conf - win_rate), 4) if raw_conf is not None and win_rate is not None else None

        profile_id = f"{direction}:{signature[:48]}:{int(time.time() * 1000)}"
        feature_vector = {}
        for s in bucket:
            feature_vector.update(_feature_vector_from_surface(s))

        evidence_candidates = _evidence_candidates(bucket)
        profile = {
            "profile_id": profile_id,
            "generated_ts": int(time.time() * 1000),
            "feature_signature": signature,
            "feature_vector": feature_vector,
            "direction": direction,
            "population": {
                "sample_count": sample_count,
                "wins": wins,
                "losses": losses,
                "win_rate": round(win_rate, 4) if win_rate is not None else None,
                "average_r": round(avg_r, 4) if avg_r is not None else None,
                "profit_factor": pf,
                "wilson_lower_bound": wlb,
            },
            "probability_surface": {
                "surface_id": (bucket[0].get("surface_id") if bucket else None),
                "match_score": round(match_score, 4) if match_score is not None else None,
                "nearest_population": (bucket[0].get("nearest_population") if bucket else {}),
                "reliability": bucket[0].get("reliability") if bucket else None,
                "learning_ready": learning_ready,
            },
            "calibration": {
                "raw_confidence_mean": round(raw_conf, 4) if raw_conf is not None else None,
                "observed_win_rate": round(win_rate, 4) if win_rate is not None else None,
                "confidence_error": error,
                "calibrated_confidence": calibrated,
                "calibration_label": calibration_label,
                "confidence_band": band,
            },
            "evidence_adjustment_candidates": evidence_candidates,
            "learning_ready": learning_ready and sample_count >= 3,
            "warnings": [],
        }
        profiles.append(profile)
        _write_jsonl(OUT_JSONL, profile)
        last_profile_id = profile_id

    summaries = {
        "generated_ts": int(time.time() * 1000),
        "profiles_count": len(profiles),
        "learning_ready_profiles": sum(1 for p in profiles if p.get("learning_ready")),
        "best_profiles": sorted(profiles, key=lambda p: (p["population"]["win_rate"] or 0, p["population"]["sample_count"]), reverse=True)[:10],
        "worst_profiles": sorted(profiles, key=lambda p: (p["population"]["win_rate"] if p["population"]["win_rate"] is not None else 1), reverse=False)[:10],
        "overconfident_profiles": [p for p in profiles if p["calibration"]["calibration_label"] == "overconfident"][:10],
        "underconfident_profiles": [p for p in profiles if p["calibration"]["calibration_label"] == "underconfident"][:10],
        "well_calibrated_profiles": [p for p in profiles if p["calibration"]["calibration_label"] == "well_calibrated"][:10],
        "insufficient_data_profiles": [p for p in profiles if p["calibration"]["calibration_label"] == "insufficient_data"][:10],
        "global_calibration": _global_metrics(profiles),
    }
    _write_json(OUT_JSON, summaries)

    label_counts = Counter(p["calibration"]["calibration_label"] for p in profiles)
    health = {
        "status": "alive",
        "last_run_ts": int(time.time() * 1000),
        "probability_surfaces_seen": len(surfaces),
        "profiles_written": len(profiles),
        "learning_ready_profiles": summaries["learning_ready_profiles"],
        "overconfident_count": label_counts.get("overconfident", 0),
        "underconfident_count": label_counts.get("underconfident", 0),
        "well_calibrated_count": label_counts.get("well_calibrated", 0),
        "insufficient_data_count": label_counts.get("insufficient_data", 0),
        "last_profile_id": last_profile_id,
        "last_blocker": None,
        "warnings": [] if surface_summary else ["probability_surface_summary_missing"],
    }
    _write_json(HEALTH, health)
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
