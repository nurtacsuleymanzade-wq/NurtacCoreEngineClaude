#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
PAPER_TRADES = DATA / "paper_trades.jsonl"
QUALIFIED_SETUPS = DATA / "qualified_setups.jsonl"
OBSERVATIONS = DATA / "observations.jsonl"
EVIDENCE_STREAM = DATA / "evidence_stream.jsonl"
OUTPUT_FILE = DATA / "trade_outcome_explanations.jsonl"
HEALTH_FILE = DATA / "trade_outcome_explanation_health.json"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    raw = subprocess.getoutput(f"tail -n 5000 {path}")
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


def _write_json(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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


def _load_existing_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    for row in _read_jsonl(path):
        tid = row.get("trade_id")
        if tid:
            ids.add(str(tid))
    return ids


def _latest_by_trade_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        tid = row.get("trade_id")
        if tid:
            out[str(tid)] = row
    return out


def _match_trade_context(
    join_row: dict[str, Any],
    paper_rows: dict[str, dict[str, Any]],
    qualified_rows: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    source_setup_id = str(join_row.get("source_setup_id") or "")
    symbol = join_row.get("symbol")

    paper = paper_rows.get(str(join_row.get("trade_id") or "")) or {}
    qualified = {}
    for row in qualified_rows:
        if str(row.get("qualified_setup_id") or "") == source_setup_id or str(row.get("source_setup_id") or "") == source_setup_id:
            qualified = row
            break

    observation = {}
    for row in reversed(observations[-1000:]):
        if row.get("symbol") != symbol:
            continue
        if str(row.get("source_setup_id") or "") == source_setup_id or str(row.get("qualified_setup_id") or "") == str(qualified.get("qualified_setup_id") or ""):
            observation = row
            break

    evidence = {}
    for row in reversed(evidence_rows[-2000:]):
        if row.get("symbol") != symbol:
            continue
        evidence = row
        break

    return {
        "paper": paper,
        "qualified": qualified,
        "observation": observation,
        "evidence": evidence,
    }


def _decision_open_reason(join_row: dict[str, Any], ctx: dict[str, Any]) -> str:
    decision = _norm((join_row.get("decision_audit") or {}).get("decision"))
    confidence = _sf((join_row.get("decision_audit") or {}).get("confidence"), 0.0) or 0.0
    quality = _norm((join_row.get("decision_audit") or {}).get("quality"))
    supported = list((join_row.get("decision_audit") or {}).get("supporting_factors") or [])
    opposing = list((join_row.get("decision_audit") or {}).get("opposing_factors") or [])
    warnings = list((join_row.get("decision_audit") or {}).get("warnings") or [])
    lines = [
        f"Trade Brain {decision}ed." if decision in ("buy", "sell", "long", "short") else f"Trade Brain {decision}.",
        f"Observer {('QUALIFIED' if ctx.get('qualified') else 'UNQUALIFIED')}.",
        f"Gate={(ctx.get('observation') or {}).get('context_at_qualification', {}).get('gate_grade') or (ctx.get('paper') or {}).get('context_at_open', {}).get('gate_grade') or 'unknown'}.",
        f"Quality={quality.upper()}.",
        f"Confidence={confidence:.3f}.",
    ]
    if supported:
        lines.append("Supporting evidence present.")
    if opposing:
        lines.append("Opposing evidence present.")
    if warnings:
        lines.append("Trade Brain warnings existed.")
    context = (ctx.get("paper") or {}).get("context_at_open") or {}
    if context.get("market_bias"):
        lines.append(f"Market context {str(context.get('market_bias')).lower()}.")
    if (join_row.get("decision_audit") or {}).get("decision_reason"):
        lines.append(str((join_row.get("decision_audit") or {}).get("decision_reason")))
    return " ".join(lines)


def _close_reason(join_row: dict[str, Any]) -> str:
    reason = _norm(join_row.get("close_reason"))
    if reason in {"sl_hit", "stop_loss", "stop"}:
        return "SL hit"
    if reason in {"tp1_hit", "tp2_hit", "tp3_hit", "tp", "take_profit"}:
        return "TP1 hit" if reason == "tp1_hit" else "TP hit"
    if reason in {"timeout", "time_out"}:
        return "Timeout"
    if reason in {"manual", "manual_close"}:
        return "Manual close"
    if reason in {"force_close", "forced", "force"}:
        return "Force close"
    return reason.replace("_", " ").title() if reason else "Unknown"


def _win_loss_lines(join_row: dict[str, Any], ctx: dict[str, Any]) -> tuple[list[str], list[str]]:
    supporting = list((join_row.get("decision_audit") or {}).get("supporting_factors") or [])
    opposing = list((join_row.get("decision_audit") or {}).get("opposing_factors") or [])
    contradictions = list((join_row.get("decision_audit") or {}).get("contradictions") or [])
    warnings = list((join_row.get("decision_audit") or {}).get("warnings") or [])
    evidence = ctx.get("evidence") or {}
    scenario = (ctx.get("observation") or {}).get("context_at_qualification", {}).get("active_scenario")
    historical_support = _norm(join_row.get("decision_audit", {}).get("quality"))

    wins: list[str] = []
    losses: list[str] = []
    if _norm(join_row.get("outcome")) in {"win", "timeout_win"} or _sf(join_row.get("pnl_r"), 0.0) > 0:
        wins.extend([f"✓ {f}" for f in supporting[:8]])
        if not wins:
            wins.append("✓ realized outcome matched the approved direction")
        if scenario:
            wins.append("✓ scenario confirmed")
        if _norm(evidence.get("dominant_side")) != "neutral":
            wins.append(f"✓ evidence dominant side {evidence.get('dominant_side')}")
    else:
        if historical_support in {"weak", "none", "unknown"}:
            losses.append("✗ historical edge unsupported")
        losses.extend([f"✗ {f}" for f in opposing[:8]])
        if not losses:
            losses.append("✗ price failed to follow the approved direction")
        losses.extend([f"✗ contradiction: {c}" for c in contradictions[:5]])
        losses.extend([f"✗ warning: {w}" for w in warnings[:5]])
        if not scenario:
            losses.append("✗ no active scenario")
        if _norm(join_row.get("decision_audit", {}).get("decision")) == "neutral":
            losses.append("✗ low probability surface")
    return wins, losses


def _root_cause(join_row: dict[str, Any], ctx: dict[str, Any]) -> str:
    outcome = _norm(join_row.get("outcome"))
    reason = _norm(join_row.get("close_reason"))
    quality = _norm((join_row.get("decision_audit") or {}).get("quality"))
    scenario = (ctx.get("observation") or {}).get("context_at_qualification", {}).get("active_scenario")
    paper_context = (ctx.get("paper") or {}).get("context_at_open") or {}
    if reason in {"sl_hit", "stop_loss", "stop"}:
        if quality in {"weak", "poor"}:
            return "Historical"
        if not scenario:
            return "Scenario"
        if paper_context.get("market_bias") and _norm(join_row.get("direction")) != _norm(paper_context.get("market_bias")):
            return "Structure"
        return "Risk"
    if reason in {"tp1_hit", "tp2_hit", "tp3_hit"} or outcome in {"win", "timeout_win"}:
        if scenario:
            return "Scenario"
        if quality in {"strong", "good"}:
            return "Execution"
        return "Liquidity"
    if "liquid" in json.dumps(ctx.get("evidence") or {}).lower():
        return "Liquidity"
    if "structure" in json.dumps(ctx.get("observation") or {}).lower():
        return "Structure"
    return "Unknown"


def _learning_label(join_row: dict[str, Any], root_cause: str) -> str:
    outcome = _norm(join_row.get("outcome"))
    decision = _norm((join_row.get("decision_audit") or {}).get("decision"))
    confidence = _sf((join_row.get("decision_audit") or {}).get("confidence"), 0.0) or 0.0
    high_conf = confidence >= 0.55
    if outcome in {"unknown", "open"}:
        return "not_enough_data"
    if outcome in {"win", "timeout_win"}:
        if decision in {"neutral", "unknown"} or not high_conf:
            return "bad_but_lucky"
        if root_cause in {"Liquidity", "Execution"}:
            return "good_but_unlucky"
        return "good_decision"
    if decision in {"neutral", "unknown"}:
        return "good_but_unlucky"
    if high_conf:
        return "bad_decision"
    return "good_but_unlucky"


def build_batch() -> dict[str, Any]:
    join_rows = _read_jsonl(JOIN_FILE)
    paper_rows = _latest_by_trade_id(_read_jsonl(PAPER_TRADES))
    qualified_rows = _read_jsonl(QUALIFIED_SETUPS)
    observations = _read_jsonl(OBSERVATIONS)
    evidence_rows = _read_jsonl(EVIDENCE_STREAM)
    existing_ids = _load_existing_ids(OUTPUT_FILE)
    written = 0
    warnings: list[str] = []
    latest = None

    for join_row in join_rows:
        trade_id = str(join_row.get("trade_id") or "")
        if not trade_id or trade_id in existing_ids:
            continue
        if _norm(join_row.get("outcome")) in {"open", "unknown"} and _sf(join_row.get("pnl_r"), None) is None:
            continue

        ctx = _match_trade_context(join_row, paper_rows, qualified_rows, observations, evidence_rows)
        root_cause = _root_cause(join_row, ctx)
        learning_label = _learning_label(join_row, root_cause)
        wins, losses = _win_loss_lines(join_row, ctx)
        summary = []
        if _sf(join_row.get("pnl_r"), 0.0) is not None and _sf(join_row.get("pnl_r"), 0.0) > 0:
            summary.append("Trade opened with supportive conditions and closed in profit.")
        else:
            summary.append("Trade opened with approval but later failed to sustain the expected move.")
        if losses:
            summary.append(" ".join(losses[:2]))
        elif wins:
            summary.append(" ".join(wins[:2]))

        record = {
            "trade_id": trade_id,
            "symbol": join_row.get("symbol"),
            "direction": join_row.get("direction"),
            "decision": (join_row.get("decision_audit") or {}).get("decision"),
            "confidence": (join_row.get("decision_audit") or {}).get("confidence"),
            "trade_quality": (join_row.get("decision_audit") or {}).get("quality"),
            "outcome": join_row.get("outcome"),
            "close_reason": join_row.get("close_reason"),
            "learning_eligible": bool(join_row.get("learning_eligible")),
            "historical_support": {
                "quality": (join_row.get("decision_audit") or {}).get("quality"),
                "warnings": list((join_row.get("decision_audit") or {}).get("warnings") or []),
            },
            "scenario_support": {
                "qualified_setup": bool(ctx.get("qualified")),
                "active_scenario": (ctx.get("observation") or {}).get("context_at_qualification", {}).get("active_scenario"),
                "scenario_direction": (ctx.get("observation") or {}).get("context_at_qualification", {}).get("market_bias"),
            },
            "observer_state": {
                "qualified_setup_id": (ctx.get("qualified") or {}).get("qualified_setup_id"),
                "qualification_ts": (ctx.get("qualified") or {}).get("qualification_ts"),
                "status": (ctx.get("qualified") or {}).get("status"),
            },
            "entry_price": join_row.get("entry_price"),
            "exit_price": (ctx.get("paper") or {}).get("close_price"),
            "pnl_r": join_row.get("pnl_r"),
            "hold_seconds": join_row.get("duration_seconds"),
            "trade_open_reason": _decision_open_reason(join_row, ctx),
            "trade_close_reason": _close_reason(join_row),
            "why_won": wins,
            "why_lost": losses,
            "supporting_factors": list((join_row.get("decision_audit") or {}).get("supporting_factors") or []),
            "opposing_factors": list((join_row.get("decision_audit") or {}).get("opposing_factors") or []),
            "contradictions": list((join_row.get("decision_audit") or {}).get("contradictions") or []),
            "warnings": list((join_row.get("decision_audit") or {}).get("warnings") or []),
            "human_readable_summary": None,
            "root_cause_category": root_cause,
            "learning_label": learning_label,
            "created_at": int(__import__("time").time()),
            "engine": "trade_outcome_explanation_engine",
            "note": "Append-only explanation record. No trading logic changed.",
        }
        record["human_readable_summary"] = (
            f"Trade opened with {'strong' if record['trade_quality'] in {'strong', 'good'} else 'limited'} "
            f"support but closed via {record['trade_close_reason'].lower()}."
            if _sf(join_row.get("pnl_r"), 0.0) <= 0
            else f"Trade opened with supportive evidence and closed profitably before the thesis broke."
        )
        _write_jsonl(OUTPUT_FILE, record)
        existing_ids.add(trade_id)
        written += 1
        latest = record

    health = {
        "status": "alive" if written >= 0 else "degraded",
        "records_written": written,
        "latest_trade_id": (latest or {}).get("trade_id"),
        "latest_outcome": (latest or {}).get("outcome"),
        "latest_learning_label": (latest or {}).get("learning_label"),
        "latest_root_cause": (latest or {}).get("root_cause_category"),
        "warnings": warnings,
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
