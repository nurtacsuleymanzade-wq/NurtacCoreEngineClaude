#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
PAPER_TRADES = DATA / "paper_trades.jsonl"
AUDITS = DATA / "trade_brain_decision_audit.jsonl"
JOIN_FILE = DATA / "trade_decision_outcome_join.jsonl"
HEALTH_FILE = DATA / "trade_decision_outcome_join_health.json"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _write_jsonl(path: Path, record: dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_json(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sf(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _norm(v: Any) -> str:
    if not v:
        return "unknown"
    return str(v).lower()


def _trade_outcome(trade: dict[str, Any]) -> str:
    res = trade.get("results") or {}
    out = (res.get("outcome") or trade.get("outcome") or trade.get("validation", {}).get("outcome") or "unknown")
    return _norm(out)


def _close_reason(trade: dict[str, Any]) -> str:
    return _norm(trade.get("close_reason") or trade.get("closeReason") or "unknown")


def _learning_eligible(trade: dict[str, Any], warnings: list[str]) -> bool:
    validation = trade.get("validation") or {}
    errors = validation.get("errors") or []
    duration = _sf(trade.get("duration_seconds"), None)
    close_ts = _sf(trade.get("close_ts"), None)
    open_ts = _sf(trade.get("open_ts"), None)
    if duration is not None and duration < 0:
        return False
    if close_ts is not None and open_ts is not None and close_ts < open_ts:
        return False
    if any("duration_seconds < 0" in str(e) for e in errors):
        return False
    if any("negative" in str(w).lower() for w in warnings):
        return False
    return True


def _normalize_direction(v: Any) -> str:
    v = _norm(v)
    if v in ("long", "bullish", "bull", "buy", "uptrend", "up"):
        return "long"
    if v in ("short", "bearish", "bear", "sell", "downtrend", "down"):
        return "short"
    return "unknown"


def _load_existing_ids(path: Path) -> set[str]:
    ids = set()
    for r in _read_jsonl(path):
        tid = r.get("trade_id")
        if tid:
            ids.add(str(tid))
    return ids


def _best_audit_for_trade(trade: dict[str, Any], audits: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str, float]:
    trade_open_ts = _sf(trade.get("open_ts"), None)
    trade_dir = _normalize_direction(trade.get("direction"))
    source_setup_id = str(trade.get("source_setup_id") or "")
    setup_id = str(trade.get("setup_id") or "")
    qualified_setup_id = str(trade.get("qualified_setup_id") or "")
    candidates = []
    for audit in audits:
        audit_ts = _sf(audit.get("ts"), None)
        if audit_ts is None:
            continue
        score = 0.0
        method = "time_nearest"
        audit_text = json.dumps(audit, ensure_ascii=False)
        if source_setup_id and source_setup_id in audit_text:
            score += 1000.0
            method = "source_setup_id"
        elif setup_id and setup_id in audit_text:
            score += 900.0
            method = "setup_id"
        elif qualified_setup_id and qualified_setup_id in audit_text:
            score += 800.0
            method = "qualified_setup_id"
        if trade_open_ts is not None:
            age = abs(audit_ts - trade_open_ts) / 1000.0
            if age <= 120:
                score += max(0.0, 120.0 - age)
            else:
                score -= age
            if _normalize_direction(audit.get("decision")) == trade_dir:
                score += 25.0
        candidates.append((score, method, audit, audit_ts))
    if not candidates:
        return None, "not_found", 0.0
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    return best[2], best[1], best[3]


def _factor_outcome(trade: dict[str, Any], audit: dict[str, Any] | None, outcome: str, confidence: float, warnings: list[str]) -> dict[str, list[str]]:
    supporting = list((audit or {}).get("supporting_factors") or [])
    opposing = list((audit or {}).get("opposing_factors") or [])
    contradictions = list((audit or {}).get("contradictions") or [])
    neutral = list((audit or {}).get("neutral_factors") or [])
    if outcome in ("open", "unknown"):
        return {
            "supporting_correct": [],
            "supporting_failed": [],
            "opposing_correct": [],
            "opposing_failed": [],
            "contradictions_confirmed": [],
            "contradictions_ignored": [],
            "neutral_notes": neutral or ["outcome_pending"],
        }
    if outcome == "win":
        return {
            "supporting_correct": supporting,
            "supporting_failed": [],
            "opposing_correct": [],
            "opposing_failed": opposing,
            "contradictions_confirmed": [],
            "contradictions_ignored": contradictions,
            "neutral_notes": neutral,
        }
    return {
        "supporting_correct": [],
        "supporting_failed": supporting,
        "opposing_correct": opposing,
        "opposing_failed": [],
        "contradictions_confirmed": contradictions,
        "contradictions_ignored": [],
        "neutral_notes": neutral,
    }


def join_batch() -> dict[str, Any]:
    paper_trades = _read_jsonl(PAPER_TRADES)
    audits = _read_jsonl(AUDITS)
    existing_ids = _load_existing_ids(JOIN_FILE)
    joined_new = 0
    duplicate_skipped = 0
    matched = 0
    unmatched = 0
    learning_eligible_count = 0
    negative_duration_count = 0
    last_joined_trade_id = None
    warnings = []

    for trade in paper_trades:
        trade_id = str(trade.get("trade_id") or "")
        if not trade_id:
            continue
        if trade_id in existing_ids:
            duplicate_skipped += 1
            continue

        outcome = _trade_outcome(trade)
        close_reason = _close_reason(trade)
        open_ts = _sf(trade.get("open_ts"), None)
        close_ts = _sf(trade.get("close_ts"), None)
        duration_seconds = _sf(trade.get("duration_seconds"), None)
        if duration_seconds is not None and duration_seconds < 0:
            negative_duration_count += 1
        if close_ts is not None and open_ts is not None and close_ts < open_ts:
            negative_duration_count += 1
        trade_warnings = []
        if duration_seconds is not None and duration_seconds < 0:
            trade_warnings.append("duration_seconds_negative")
        if close_ts is not None and open_ts is not None and close_ts < open_ts:
            trade_warnings.append("paper_trade_negative_duration")
        if "duration_seconds < 0" in " ".join(map(str, (trade.get("validation") or {}).get("errors") or [])):
            trade_warnings.append("validation_negative_duration")
        learning_eligible = _learning_eligible(trade, trade_warnings)
        if learning_eligible:
            learning_eligible_count += 1

        audit, match_method, audit_ts = _best_audit_for_trade(trade, audits)
        matched_flag = audit is not None
        if matched_flag:
            matched += 1
            audit_age = None
            if open_ts is not None and audit_ts is not None:
                audit_age = (audit_ts - open_ts) / 1000.0
        else:
            unmatched += 1
            audit_age = None
            match_method = "not_found"

        confidence = _sf((audit or {}).get("confidence"))
        decision = (audit or {}).get("decision", "neutral")
        factor_outcome = _factor_outcome(trade, audit, outcome, confidence, trade_warnings)
        confidence_correct = True
        was_high_confidence = confidence >= 0.55
        if outcome == "win":
            confidence_correct = confidence >= 0.55
        elif outcome == "loss":
            confidence_correct = confidence < 0.55
            trade_warnings.append("decision_lost")
        else:
            trade_warnings.append("outcome_pending")

        if matched_flag:
            record = {
                "engine": "trade_decision_outcome_join",
                "symbol": trade.get("symbol", "BTCUSDT"),
                "join_ts": int(__import__("time").time() * 1000),
                "trade_id": trade_id,
                "setup_id": trade.get("setup_id"),
                "source_setup_id": trade.get("source_setup_id"),
                "direction": trade.get("direction"),
                "open_ts": open_ts,
                "close_ts": close_ts,
                "duration_seconds": duration_seconds,
                "outcome": outcome,
                "close_reason": close_reason,
                "pnl_r": _sf((trade.get("results") or {}).get("pnl_r")),
                "mfe_r": _sf((trade.get("results") or {}).get("mfe_r")),
                "mae_r": _sf((trade.get("results") or {}).get("mae_r")),
                "audit_match": {
                    "matched": True,
                    "match_method": match_method,
                    "audit_ts": audit_ts,
                    "audit_age_seconds_from_open": audit_age,
                },
                "decision_audit": {
                    "decision": decision,
                    "confidence": confidence,
                    "quality": (audit or {}).get("quality"),
                    "decision_reason": (audit or {}).get("decision_reason"),
                    "supporting_factors": (audit or {}).get("supporting_factors") or [],
                    "opposing_factors": (audit or {}).get("opposing_factors") or [],
                    "neutral_factors": (audit or {}).get("neutral_factors") or [],
                    "contradictions": (audit or {}).get("contradictions") or [],
                    "warnings": (audit or {}).get("warnings") or [],
                },
                "factor_outcome": factor_outcome,
                "confidence_evaluation": {
                    "confidence": confidence,
                    "was_high_confidence": was_high_confidence,
                    "confidence_correct": confidence_correct,
                    "note": "confidence evaluated conservatively against realized outcome",
                },
                "attribution_mode": "conservative_outcome_join_v1",
                "learning_eligible": learning_eligible,
                "warnings": trade_warnings,
            }
            _write_jsonl(JOIN_FILE, record)
            existing_ids.add(trade_id)
            joined_new += 1
            last_joined_trade_id = trade_id
        else:
            warnings.append(f"unmatched_trade_id={trade_id}")

    health = {
        "status": "alive",
        "last_run_ts": int(__import__("time").time() * 1000),
        "paper_trades_seen": len(paper_trades),
        "audit_records_seen": len(audits),
        "joined_new_count": joined_new,
        "duplicate_skipped_count": duplicate_skipped,
        "matched_count": matched,
        "unmatched_count": unmatched,
        "learning_eligible_count": learning_eligible_count,
        "negative_duration_count": negative_duration_count,
        "last_joined_trade_id": last_joined_trade_id,
        "last_blocker": None,
        "warnings": warnings,
    }
    _write_json(HEALTH_FILE, health)
    return health


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="batch", choices=["batch"])
    args = parser.parse_args()
    if args.mode == "batch":
        join_batch()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
