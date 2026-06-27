#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
SUMMARY_JSON = DATA / "trade_decision_attribution_summary.json"
REPORT_MD = DATA / "trade_decision_attribution_report.md"


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


def _sf(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _norm(v: Any) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    return s or "unknown"


def _dedupe_by_trade_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = {}
    for row in rows:
        tid = row.get("trade_id")
        if not tid:
            continue
        seen[str(tid)] = row
    return list(seen.values())


def _is_learning_eligible(row: dict[str, Any]) -> bool:
    return bool(row.get("learning_eligible"))


def _invalid_timestamp(row: dict[str, Any]) -> bool:
    warnings = [str(w) for w in (row.get("warnings") or [])]
    validation = row.get("decision_audit", {}).get("warnings") or []
    duration = _sf(row.get("duration_seconds"), 0.0)
    open_ts = _sf(row.get("open_ts"), None)
    close_ts = _sf(row.get("close_ts"), None)
    if duration < 0:
        return True
    if open_ts is not None and close_ts is not None and close_ts < open_ts:
        return True
    if any("negative" in w for w in warnings + [str(x) for x in validation]):
        return True
    return False


def _outcome(row: dict[str, Any]) -> str:
    outcome = _norm(row.get("outcome"))
    pnl = _sf(row.get("pnl_r"), None)
    close_reason = _norm(row.get("close_reason"))
    if outcome in ("win", "loss"):
        return outcome
    if pnl is not None:
        if pnl > 0:
            return "win"
        if pnl < 0:
            return "loss"
    if close_reason in ("tp1_hit", "tp2_hit", "tp3_hit"):
        return "win"
    if close_reason == "sl_hit":
        return "loss"
    return "unknown"


def _bucket_confidence(conf: float) -> str:
    if conf < 0.50:
        return "<0.50"
    if conf < 0.55:
        return "0.50-0.55"
    if conf < 0.60:
        return "0.55-0.60"
    if conf < 0.65:
        return "0.60-0.65"
    if conf < 0.70:
        return "0.65-0.70"
    return ">=0.70"


def _rate(num: int, den: int) -> float:
    return round((num / den) * 100.0, 2) if den else 0.0


def _avg(values: list[float]) -> float:
    return round(mean(values), 4) if values else 0.0


def _quality_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = {}
    by_quality = defaultdict(list)
    for row in rows:
        q = _norm(row.get("decision_audit", {}).get("quality"))
        by_quality[q].append(row)
    for q, items in by_quality.items():
        wins = sum(1 for r in items if _outcome(r) == "win")
        losses = sum(1 for r in items if _outcome(r) == "loss")
        out[q] = {
            "n": len(items),
            "win": wins,
            "loss": losses,
            "wr": _rate(wins, wins + losses),
            "avg_pnl_r": _avg([_sf(r.get("pnl_r")) for r in items]),
            "avg_confidence": _avg([_sf(r.get("decision_audit", {}).get("confidence")) for r in items]),
        }
    return out


def _bucket_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = {}
    by_bucket = defaultdict(list)
    for row in rows:
        b = _bucket_confidence(_sf(row.get("decision_audit", {}).get("confidence")))
        by_bucket[b].append(row)
    for b, items in by_bucket.items():
        wins = sum(1 for r in items if _outcome(r) == "win")
        losses = sum(1 for r in items if _outcome(r) == "loss")
        out[b] = {
            "n": len(items),
            "win": wins,
            "loss": losses,
            "wr": _rate(wins, wins + losses),
            "avg_pnl_r": _avg([_sf(r.get("pnl_r")) for r in items]),
        }
    return out


def _warning_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    acc = defaultdict(list)
    for row in rows:
        outcome = _outcome(row)
        for w in (row.get("warnings") or []):
            acc[str(w)].append((row, outcome))
        for w in (row.get("decision_audit", {}).get("warnings") or []):
            acc[str(w)].append((row, outcome))
    out = {}
    for w, pairs in acc.items():
        items = [r for r, _ in pairs]
        wins = sum(1 for _, o in pairs if o == "win")
        losses = sum(1 for _, o in pairs if o == "loss")
        out[w] = {
            "n": len(items),
            "win": wins,
            "loss": losses,
            "wr": _rate(wins, wins + losses),
            "loss_rate": _rate(losses, wins + losses),
            "avg_pnl_r": _avg([_sf(r.get("pnl_r")) for r in items]),
        }
    return out


def _factor_stats(rows: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    appeared = defaultdict(list)
    correct = defaultdict(int)
    failed = defaultdict(int)
    confirmed = defaultdict(int)
    ignored = defaultdict(int)
    for row in rows:
        outcome = _outcome(row)
        pnl = _sf(row.get("pnl_r"))
        factor_outcome = row.get("factor_outcome") or {}
        if kind == "supporting":
            for f in factor_outcome.get("supporting_correct") or []:
                appeared[f].append(row)
                correct[f] += 1
            for f in factor_outcome.get("supporting_failed") or []:
                appeared[f].append(row)
                failed[f] += 1
        elif kind == "opposing":
            for f in factor_outcome.get("opposing_correct") or []:
                appeared[f].append(row)
                correct[f] += 1
            for f in factor_outcome.get("opposing_failed") or []:
                appeared[f].append(row)
                failed[f] += 1
        else:
            for f in factor_outcome.get("contradictions_confirmed") or []:
                appeared[f].append(row)
                confirmed[f] += 1
            for f in factor_outcome.get("contradictions_ignored") or []:
                appeared[f].append(row)
                ignored[f] += 1

    out = {}
    keys = set(appeared) | set(correct) | set(failed) | set(confirmed) | set(ignored)
    for f in keys:
        items = appeared.get(f, [])
        if kind == "contradiction":
            total_confirmed = confirmed.get(f, 0)
            total_ignored = ignored.get(f, 0)
            denom = total_confirmed + total_ignored
            out[f] = {
                "appeared_n": len(items),
                "confirmed_n": total_confirmed,
                "ignored_n": total_ignored,
                "confirmed_rate": _rate(total_confirmed, denom),
                "avg_pnl_r_when_present": _avg([_sf(r.get("pnl_r")) for r in items]),
            }
        else:
            total_correct = correct.get(f, 0)
            total_failed = failed.get(f, 0)
            denom = total_correct + total_failed
            out[f] = {
                "appeared_n": len(items),
                "correct_n": total_correct,
                "failed_n": total_failed,
                "correctness_rate": _rate(total_correct, denom),
                "avg_pnl_r_when_present": _avg([_sf(r.get("pnl_r")) for r in items]),
            }
    return out


def _top_bottom(mapping: dict[str, Any], key: str, min_n: int = 10) -> tuple[list[tuple[str, Any]], list[tuple[str, Any]]]:
    items = [(k, v) for k, v in mapping.items() if v.get("n", v.get("appeared_n", 0)) >= min_n]
    if not items:
        return [], []
    top = sorted(items, key=lambda kv: (kv[1].get(key, 0), kv[1].get("avg_pnl_r", 0)), reverse=True)
    bottom = sorted(items, key=lambda kv: (kv[1].get(key, 0), kv[1].get("avg_pnl_r", 0)))
    return top[:10], bottom[:10]


def build_summary() -> tuple[dict[str, Any], str]:
    raw = _read_jsonl(JOIN_FILE)
    all_records = _dedupe_by_trade_id(raw)
    all_n = len(all_records)
    learning_eligible_records = [r for r in all_records if _is_learning_eligible(r)]
    invalid_timestamp_records = [r for r in all_records if _invalid_timestamp(r)]
    matched_records = [r for r in all_records if (r.get("audit_match") or {}).get("matched")]
    unmatched_records = [r for r in all_records if not (r.get("audit_match") or {}).get("matched")]

    wins = sum(1 for r in all_records if _outcome(r) == "win")
    losses = sum(1 for r in all_records if _outcome(r) == "loss")
    unknown = sum(1 for r in all_records if _outcome(r) == "unknown")
    eligible_wins = sum(1 for r in learning_eligible_records if _outcome(r) == "win")
    eligible_losses = sum(1 for r in learning_eligible_records if _outcome(r) == "loss")

    quality_stats = _quality_stats(learning_eligible_records)
    confidence_stats = _bucket_stats(learning_eligible_records)
    warning_stats = _warning_stats(all_records)
    supporting_stats = _factor_stats(learning_eligible_records, "supporting")
    opposing_stats = _factor_stats(learning_eligible_records, "opposing")
    contradiction_stats = _factor_stats(learning_eligible_records, "contradiction")

    best_quality = sorted(
        quality_stats.items(),
        key=lambda kv: (kv[1].get("wr", 0), kv[1].get("avg_pnl_r", 0)),
        reverse=True,
    )[:3]
    worst_quality = sorted(
        quality_stats.items(),
        key=lambda kv: (kv[1].get("wr", 0), kv[1].get("avg_pnl_r", 0)),
    )[:3]

    top_supporting = sorted(
        [(k, v) for k, v in supporting_stats.items() if v.get("appeared_n", 0) >= 10],
        key=lambda kv: (kv[1].get("correctness_rate", 0), kv[1].get("appeared_n", 0)),
        reverse=True,
    )[:10]
    worst_supporting = sorted(
        [(k, v) for k, v in supporting_stats.items() if v.get("appeared_n", 0) >= 10],
        key=lambda kv: (kv[1].get("correctness_rate", 0), kv[1].get("appeared_n", 0)),
    )[:10]
    dangerous_warnings = sorted(
        [(k, v) for k, v in warning_stats.items() if v.get("n", 0) >= 10],
        key=lambda kv: (kv[1].get("loss_rate", 0), kv[1].get("n", 0)),
        reverse=True,
    )[:10]
    confirmed_contradictions = sorted(
        [(k, v) for k, v in contradiction_stats.items() if v.get("appeared_n", 0) >= 10],
        key=lambda kv: (kv[1].get("confirmed_rate", 0), kv[1].get("appeared_n", 0)),
        reverse=True,
    )[:10]

    eligible_n = len(learning_eligible_records)
    if eligible_n >= 500:
        learning_status = "ready"
        reason = "eligible sample is large enough for stronger attribution analysis"
    elif eligible_n >= 100:
        learning_status = "minimal"
        reason = "eligible sample exists but remains only minimal for cautious learning"
    else:
        learning_status = "insufficient"
        reason = "eligible sample is too small for robust learning"

    critical_findings = []
    critical_findings.append(f"Learning eligible subset is {eligible_n}, status={learning_status}.")
    if invalid_timestamp_records:
        critical_findings.append(f"Negative duration / invalid timestamp records excluded from learning: {len(invalid_timestamp_records)}.")
    if warning_stats.get("decision_lost", {}).get("loss_rate", 0) >= 50:
        critical_findings.append("decision_lost is strongly associated with losses.")
    if warning_stats.get("historical_not_supportive", {}).get("loss_rate", 0) >= 50:
        critical_findings.append("historical_not_supportive has high loss pressure.")
    if quality_stats.get("weak", {}).get("wr", 0) <= 25 and quality_stats.get("weak", {}).get("n", 0) > 0:
        critical_findings.append("weak decision quality underperforms materially.")
    if not supporting_stats:
        critical_findings.append("Supporting factor attribution remains sparse in the current join data.")
    if not opposing_stats:
        critical_findings.append("Opposing factor attribution remains sparse in the current join data.")
    if len(critical_findings) < 5:
        critical_findings.append("Join-based attribution is still conservative and not causal.")
    if any(_sf(r.get("pnl_r")) == 0 for r in all_records):
        critical_findings.append("Some trades report pnl_r=0 while still resolving as losses; review downstream outcome encoding.")

    summary = {
        "engine": "trade_decision_attribution_summary",
        "dataset": {
            "all_records_n": all_n,
            "learning_eligible_n": eligible_n,
            "invalid_timestamp_n": len(invalid_timestamp_records),
            "matched_n": len(matched_records),
            "unmatched_n": len(unmatched_records),
            "win_n": wins,
            "loss_n": losses,
            "unknown_n": unknown,
            "learning_win_n": eligible_wins,
            "learning_loss_n": eligible_losses,
            "negative_duration_n": len([r for r in all_records if _invalid_timestamp(r)]),
        },
        "learning_readiness": {
            "eligible_n": eligible_n,
            "minimum_needed": 100,
            "status": learning_status,
            "reason": reason,
        },
        "outcome_summary": {
            "all_records": {
                "n": all_n,
                "win": wins,
                "loss": losses,
                "unknown": unknown,
                "wr": _rate(wins, wins + losses),
                "avg_pnl_r": _avg([_sf(r.get("pnl_r")) for r in all_records]),
            },
            "learning_eligible_records": {
                "n": eligible_n,
                "win": eligible_wins,
                "loss": eligible_losses,
                "wr": _rate(eligible_wins, eligible_wins + eligible_losses),
                "avg_pnl_r": _avg([_sf(r.get("pnl_r")) for r in learning_eligible_records]),
            },
        },
        "decision_quality_performance": quality_stats,
        "confidence_bucket_performance": confidence_stats,
        "warning_loss_analysis": warning_stats,
        "supporting_factor_performance": supporting_stats,
        "opposing_factor_performance": opposing_stats,
        "contradiction_analysis": contradiction_stats,
        "top_positive_factors": {
            "supporting": top_supporting,
            "contradictions": confirmed_contradictions,
        },
        "worst_factors": {
            "supporting": worst_supporting,
            "warnings": dangerous_warnings,
        },
        "most_dangerous_warnings": dangerous_warnings,
        "critical_findings": critical_findings[:10],
    }

    lines = []
    lines.append("# Trade Decision Attribution Summary")
    lines.append("")
    lines.append("## Dataset Health")
    lines.append(f"- All records: {all_n}")
    lines.append(f"- Learning eligible: {eligible_n}")
    lines.append(f"- Matched: {len(matched_records)}")
    lines.append(f"- Unmatched: {len(unmatched_records)}")
    lines.append(f"- Negative duration count: {len(invalid_timestamp_records)}")
    lines.append("")
    lines.append("## Learning Eligible Subset")
    lines.append(f"- Win rate: {_rate(eligible_wins, eligible_wins + eligible_losses)}%")
    lines.append(f"- Avg pnl_r: {_avg([_sf(r.get('pnl_r')) for r in learning_eligible_records])}")
    lines.append("- Negative duration records were excluded from learning analysis.")
    lines.append("")
    lines.append("## Outcome Summary")
    lines.append(f"- All-record WR: {_rate(wins, wins + losses)}%")
    lines.append(f"- Learning-subset WR: {_rate(eligible_wins, eligible_wins + eligible_losses)}%")
    lines.append("")
    lines.append("## Decision Quality Performance")
    for q, stats in sorted(quality_stats.items(), key=lambda kv: kv[0]):
        lines.append(f"- {q}: n={stats['n']} wr={stats['wr']} avg_pnl_r={stats['avg_pnl_r']} avg_confidence={stats['avg_confidence']}")
    lines.append("")
    lines.append("## Confidence Bucket Performance")
    for b, stats in sorted(confidence_stats.items(), key=lambda kv: kv[0]):
        lines.append(f"- {b}: n={stats['n']} wr={stats['wr']} avg_pnl_r={stats['avg_pnl_r']}")
    lines.append("")
    lines.append("## Warning Loss Analysis")
    for w, stats in sorted(warning_stats.items(), key=lambda kv: kv[1].get("loss_rate", 0), reverse=True):
        lines.append(f"- {w}: n={stats['n']} loss_rate={stats['loss_rate']} wr={stats['wr']} avg_pnl_r={stats['avg_pnl_r']}")
    lines.append("")
    lines.append("## Supporting Factor Performance")
    for f, stats in sorted(supporting_stats.items(), key=lambda kv: kv[1].get("correctness_rate", 0), reverse=True):
        lines.append(f"- {f}: appeared={stats['appeared_n']} correctness_rate={stats['correctness_rate']} avg_pnl_r={stats['avg_pnl_r_when_present']}")
    lines.append("")
    lines.append("## Opposing Factor Performance")
    for f, stats in sorted(opposing_stats.items(), key=lambda kv: kv[1].get("correctness_rate", 0), reverse=True):
        lines.append(f"- {f}: appeared={stats['appeared_n']} correctness_rate={stats['correctness_rate']} avg_pnl_r={stats['avg_pnl_r_when_present']}")
    lines.append("")
    lines.append("## Contradiction Analysis")
    for f, stats in sorted(contradiction_stats.items(), key=lambda kv: kv[1].get("confirmed_rate", 0), reverse=True):
        lines.append(f"- {f}: appeared={stats['appeared_n']} confirmed_rate={stats['confirmed_rate']} avg_pnl_r={stats['avg_pnl_r_when_present']}")
    lines.append("")
    lines.append("## Top Positive Factors")
    for f, stats in top_supporting:
        lines.append(f"- {f}: correctness_rate={stats['correctness_rate']} appeared={stats['appeared_n']}")
    lines.append("")
    lines.append("## Worst Factors")
    for f, stats in worst_supporting:
        lines.append(f"- {f}: correctness_rate={stats['correctness_rate']} appeared={stats['appeared_n']}")
    lines.append("")
    lines.append("## Most Dangerous Warnings")
    for f, stats in dangerous_warnings:
        lines.append(f"- {f}: loss_rate={stats['loss_rate']} n={stats['n']}")
    lines.append("")
    lines.append("## Learning Readiness")
    lines.append(f"- Status: {learning_status}")
    lines.append(f"- Reason: {reason}")
    lines.append("")
    lines.append("## Critical Findings")
    for item in critical_findings[:10]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Next Recommended Step")
    lines.append("- Fix negative-duration lifecycle bugs before any stronger learning or weight updates.")
    lines.append("- Expand the learning eligible subset before trusting factor-level calibration.")

    return summary, "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="batch", choices=["batch"])
    args = parser.parse_args()
    summary, report = build_summary()
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    REPORT_MD.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
