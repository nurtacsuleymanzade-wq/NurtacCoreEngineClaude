#!/usr/bin/env python3
import argparse
import json
import math
import subprocess
import time
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Iterable


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

TRADE_FILES = [
    DATA / "paper_trades.jsonl",
    DATA / "qualified_setups.jsonl",
    DATA / "setups.jsonl",
    DATA / "trade_brain_setups.jsonl",
    DATA / "observations.jsonl",
    DATA / "trade_decision_outcome_join.jsonl",
    DATA / "trade_brain_decision_audit.jsonl",
]

DETECTOR_FILES = [
    DATA / "labels_initiative_flow.jsonl",
    DATA / "labels_absorption.jsonl",
    DATA / "labels_sweep.jsonl",
    DATA / "labels_exhaustion.jsonl",
    DATA / "labels_iceberg.jsonl",
    DATA / "labels_trapped_trader.jsonl",
]

OUT_REPORT = DATA / "edge_analytics_v2_report.md"
OUT_SUMMARY = DATA / "edge_analytics_v2_summary.json"
OUT_HEALTH = DATA / "edge_analytics_v2_health.json"


def safe_json_loads(line: str) -> dict[str, Any] | None:
    line = line.strip()
    if not line:
        return None
    try:
        obj = json.loads(line)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def iter_jsonl_tail(path: Path, n: int) -> Iterable[dict[str, Any]]:
    if not path.exists() or n <= 0:
        return
    cmd = ["tail", "-n", str(int(n)), str(path)]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    except Exception:
        return
    assert proc.stdout is not None
    for line in proc.stdout:
        row = safe_json_loads(line)
        if row is not None:
            yield row
    proc.stdout.close()
    proc.wait()


def tail_jsonl(path: Path, n: int) -> list[dict[str, Any]]:
    return list(iter_jsonl_tail(path, n))


def fnum(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def norm(v: Any) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    return s or "unknown"


def wilson_lb(wins: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1 + z * z / n
    center = phat + z * z / (2 * n)
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)
    return max(0.0, (center - margin) / denom)


def profit_factor(rows: list[dict[str, Any]]) -> float:
    gross_win = 0.0
    gross_loss = 0.0
    for row in rows:
        r = fnum(row.get("r_value"), None)
        if r is None:
            r = fnum(row.get("pnl_r"), None)
        if r is None:
            r = fnum((row.get("results") or {}).get("pnl_r"), None)
        if r is None:
            r = fnum((row.get("result") or {}).get("pnl_r"), None)
        if r > 0:
            gross_win += r
        elif r < 0:
            gross_loss += abs(r)
    return round(gross_win / gross_loss, 4) if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0)


def _extract_outcome(row: dict[str, Any]) -> str:
    candidates = [
        row.get("outcome"),
        (row.get("results") or {}).get("outcome"),
        (row.get("result") or {}).get("outcome"),
        row.get("close_reason"),
        (row.get("paper") or {}).get("closed", {}).get("outcome"),
    ]
    for c in candidates:
        s = norm(c)
        if s in ("win", "loss"):
            return s
        if s in ("tp1_hit", "tp2_hit", "tp3_hit"):
            return "win"
        if s in ("sl_hit", "stop_loss", "stoploss"):
            return "loss"
    pnl_candidates = [
        fnum(row.get("pnl_r"), None),
        fnum((row.get("results") or {}).get("pnl_r"), None),
        fnum((row.get("result") or {}).get("pnl_r"), None),
    ]
    for pnl in pnl_candidates:
        if pnl is None:
            continue
        if pnl > 0:
            return "win"
        if pnl < 0:
            return "loss"
    return "unknown"


def _safe_ts(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _context_from(*rows: dict[str, Any] | None) -> dict[str, Any]:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("context_at_open", "context_at_qualification", "context"):
            ctx = row.get(key)
            if isinstance(ctx, dict) and ctx:
                return ctx
    return {}


def _source_type(row: dict[str, Any]) -> str:
    sid = norm(row.get("source_setup_id"))
    pkey = norm(row.get("pattern_key"))
    setup_id = norm(row.get("setup_id"))
    if "tb_" in sid or "tb_" in setup_id or "trade_brain" in pkey or "tb_" in pkey:
        return "trade_brain"
    return "evidence"


def _pattern_key(*rows: dict[str, Any] | None) -> str:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("pattern_key", "setup_pattern_key"):
            v = row.get(key)
            if v:
                return norm(v)
    return "unknown"


def _direction(*rows: dict[str, Any] | None) -> str:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("direction", "side"):
            v = norm(row.get(key))
            if v in ("long", "short"):
                return v
    return "unknown"


def summarize_group(rows: list[dict[str, Any]], key: str, min_n: int) -> list[dict[str, Any]]:
    bucket = defaultdict(list)
    for row in rows:
        bucket[norm(row.get(key))].append(row)
    out = []
    for k, items in bucket.items():
        if len(items) < min_n:
            continue
        wins = sum(1 for r in items if _extract_outcome(r) == "win")
        losses = sum(1 for r in items if _extract_outcome(r) == "loss")
        n = wins + losses
        avg_r = round(sum(fnum(r.get("_r"), fnum(r.get("r_value"), fnum(r.get("pnl_r"), 0.0))) for r in items) / len(items), 4)
        out.append({
            "key": k,
            "n": len(items),
            "wins": wins,
            "losses": losses,
            "wr": round((wins / n) * 100, 2) if n else 0.0,
            "wilson_lb": round(wilson_lb(wins, n) * 100, 2) if n else 0.0,
            "avg_r": avg_r,
            "pf": profit_factor(items),
        })
    out.sort(key=lambda x: (x["wr"], x["wilson_lb"], x["avg_r"]), reverse=True)
    return out


def _parse_trade_join(path: Path, n: int) -> dict[str, dict[str, Any]]:
    rows = {}
    for row in iter_jsonl_tail(path, n):
        tid = row.get("trade_id")
        if tid:
            rows[str(tid)] = row
    return rows


def _load_reference_maps(tail: int) -> dict[str, dict[str, dict[str, Any]]]:
    maps: dict[str, dict[str, dict[str, Any]]] = {}
    for path in [DATA / "qualified_setups.jsonl", DATA / "setups.jsonl", DATA / "trade_brain_setups.jsonl", DATA / "observations.jsonl", DATA / "trade_brain_decision_audit.jsonl"]:
        m = {}
        for row in iter_jsonl_tail(path, tail):
            for key in ("trade_id", "setup_id", "qualified_setup_id", "source_setup_id"):
                v = row.get(key)
                if v:
                    m[str(v)] = row
        maps[path.name] = m
    return maps


def _trade_match_refs(trade: dict[str, Any], refs: dict[str, dict[str, dict[str, Any]]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    sid = trade.get("source_setup_id")
    pkey = trade.get("pattern_key")
    trade_id = trade.get("trade_id")
    q = refs.get("qualified_setups.jsonl", {})
    s = refs.get("setups.jsonl", {})
    tb = refs.get("trade_brain_setups.jsonl", {})
    oa = refs.get("observations.jsonl", {})
    da = refs.get("trade_brain_decision_audit.jsonl", {})
    candidates = [sid, pkey, trade_id]
    qualified = setup = brain = None
    for cand in candidates:
        if not cand:
            continue
        cand = str(cand)
        qualified = qualified or q.get(cand)
        setup = setup or s.get(cand)
        brain = brain or tb.get(cand) or oa.get(cand) or da.get(cand)
    return qualified, setup, brain


def _is_clean_trade(trade: dict[str, Any]) -> bool:
    if not trade.get("trade_id"):
        return False
    outcome = _extract_outcome(trade)
    if outcome not in ("win", "loss"):
        return False
    open_ts = _safe_ts(trade.get("open_ts"))
    close_ts = _safe_ts(trade.get("close_ts"))
    dur = _safe_ts(trade.get("duration_seconds"))
    if open_ts is not None and close_ts is not None and close_ts < open_ts:
        return False
    if dur is not None and dur < 0:
        return False
    validation_errors = trade.get("validation", {}).get("errors") or []
    if any("duration_seconds < 0" in str(e) for e in validation_errors):
        return False
    return True


def _detector_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    non_none = [r for r in rows if norm(r.get("label")) not in ("none", "unknown", "")] 
    strong = [r for r in rows if "strong" in norm(r.get("label"))]
    candidate = [r for r in rows if "candidate" in norm(r.get("label"))]
    labels = Counter()
    dirs = Counter()
    for r in rows:
        labels[norm(r.get("label"))] += 1
        d = norm(r.get("direction"))
        if d != "unknown":
            dirs[d] += 1
    return {
        "total_tail_lines": total,
        "non_none_count": len(non_none),
        "non_none_rate": round((len(non_none) / total) * 100, 2) if total else 0.0,
        "strong_count": len(strong),
        "candidate_count": len(candidate),
        "top_labels": labels.most_common(5),
        "top_directions": dirs.most_common(5),
    }


def _near_trade_attribution(trades: list[dict[str, Any]], detector_rows: dict[str, list[dict[str, Any]]], window_ms: int = 30_000) -> list[dict[str, Any]]:
    stats = defaultdict(list)
    for trade in trades:
        open_ts = _safe_ts(trade.get("open_ts"))
        if open_ts is None:
            continue
        for det_name, rows in detector_rows.items():
            for row in rows:
                start = _safe_ts(row.get("window_start_ts"))
                end = _safe_ts(row.get("window_end_ts"))
                if start is None or end is None:
                    continue
                if end < open_ts - window_ms or start > open_ts + window_ms:
                    continue
                label = norm(row.get("label"))
                direction = norm(row.get("direction"))
                key = f"{det_name}:{label}:{direction}"
                stats[key].append(trade)
                break
    out = []
    for key, items in stats.items():
        wins = sum(1 for r in items if _extract_outcome(r) == "win")
        losses = sum(1 for r in items if _extract_outcome(r) == "loss")
        n = wins + losses
        out.append({
            "key": key,
            "n": n,
            "wins": wins,
            "losses": losses,
            "wr": round((wins / n) * 100, 2) if n else 0.0,
            "wilson_lb": round(wilson_lb(wins, n) * 100, 2) if n else 0.0,
            "avg_r": round(sum(fnum(r.get("_r"), fnum(r.get("r_value"), fnum(r.get("pnl_r"), 0.0))) for r in items) / len(items), 4) if items else 0.0,
        })
    out.sort(key=lambda x: (x["wr"], x["wilson_lb"], x["avg_r"]), reverse=True)
    return out


def _edge_candidates(group_rows: list[dict[str, Any]], min_n: int = 10) -> list[dict[str, Any]]:
    out = []
    for row in group_rows:
        if row["n"] >= min_n and row["wr"] >= 55.0 and row["wilson_lb"] >= 35.0 and row["avg_r"] > 0:
            out.append(row)
    return out


def _render_section(title: str, rows: list[dict[str, Any]], fields: list[str], limit: int = 10) -> str:
    lines = []
    if not rows:
        lines.append("No rows.")
        return "\n".join(lines)
    for row in rows[:limit]:
        parts = [f"{f}={row.get(f)}" for f in fields]
        lines.append("- " + ", ".join(parts))
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tail", type=int, default=5000)
    ap.add_argument("--detector-tail", type=int, default=2000)
    args = ap.parse_args()

    generated_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    files_seen = [str(p) for p in TRADE_FILES + DETECTOR_FILES if p.exists()]
    missing_files = [str(p) for p in TRADE_FILES + DETECTOR_FILES if not p.exists()]

    refs = _load_reference_maps(args.tail)
    paper_trades = tail_jsonl(DATA / "paper_trades.jsonl", args.tail)
    qualified = tail_jsonl(DATA / "qualified_setups.jsonl", args.tail)
    setups = tail_jsonl(DATA / "setups.jsonl", args.tail)
    trade_brain = tail_jsonl(DATA / "trade_brain_setups.jsonl", args.tail)
    observations = tail_jsonl(DATA / "observations.jsonl", args.tail)
    join_rows = tail_jsonl(DATA / "trade_decision_outcome_join.jsonl", args.tail)
    brain_audit = tail_jsonl(DATA / "trade_brain_decision_audit.jsonl", args.tail)
    detector_rows = {p.stem: tail_jsonl(p, args.detector_tail) for p in DETECTOR_FILES}

    clean_trades = []
    dirty_trades = []
    enriched = []
    for trade in paper_trades:
        qualified_row, setup_row, brain_row = _trade_match_refs(trade, refs)
        ctx = _context_from(trade, qualified_row, setup_row, brain_row)
        row = {
            **trade,
            "_qualified": qualified_row,
            "_setup": setup_row,
            "_brain": brain_row,
            "_context": ctx,
            "_source_type": _source_type(trade),
            "_pattern_key": _pattern_key(trade, qualified_row, setup_row, brain_row),
            "_direction": _direction(trade, qualified_row, setup_row, brain_row),
            "_session": norm(ctx.get("session") or (trade.get("regime_context") or {}).get("session") or (qualified_row or {}).get("session_at_qualification")),
            "_regime": norm(ctx.get("regime") or (trade.get("regime_context") or {}).get("trend_regime") or (qualified_row or {}).get("regime_at_qualification")),
            "_trend_1m": norm(ctx.get("trend_1m") or (trade.get("regime_context") or {}).get("trend_1m") or (qualified_row or {}).get("context_at_qualification", {}).get("trend_1m")),
            "_location": norm(ctx.get("location") or (trade.get("context_at_open") or {}).get("location") or (qualified_row or {}).get("context_at_qualification", {}).get("location") or (setup_row or {}).get("context", {}).get("price_loc")),
            "_quality_tier": norm(trade.get("quality_tier") or (qualified_row or {}).get("quality_tier") or (setup_row or {}).get("quality_tier")),
            "_close_reason": norm(trade.get("close_reason")),
            "_r": fnum((trade.get("results") or {}).get("pnl_r"), fnum(trade.get("pnl_r"), 0.0)),
        }
        if _is_clean_trade(trade):
            clean_trades.append(row)
        else:
            dirty_trades.append(row)
        enriched.append(row)

    groups = {
        "pattern_key": summarize_group(clean_trades, "_pattern_key", 1),
        "source_type": summarize_group(clean_trades, "_source_type", 1),
        "direction": summarize_group(clean_trades, "_direction", 1),
        "session": summarize_group(clean_trades, "_session", 1),
        "regime": summarize_group(clean_trades, "_regime", 1),
        "trend_1m": summarize_group(clean_trades, "_trend_1m", 1),
        "location": summarize_group(clean_trades, "_location", 1),
        "quality_tier": summarize_group(clean_trades, "_quality_tier", 1),
        "close_reason": summarize_group(clean_trades, "_close_reason", 1),
    }

    detector_stats = {name: _detector_stats(rows) for name, rows in detector_rows.items()}
    detector_attribution = _near_trade_attribution(clean_trades, detector_rows)
    potential_edges = _edge_candidates(detector_attribution)
    execution_candidates = [x for x in detector_attribution if x["n"] >= 10 and x["wr"] >= 55.0 and x["wilson_lb"] >= 35.0 and x["avg_r"] > 0]
    observation_only_candidates = [dict(x, reason="high_frequency_low_edge") for x in detector_attribution if x["n"] >= 10 and x["wr"] < 50.0 and x["avg_r"] <= 0]

    wins = sum(1 for r in clean_trades if _extract_outcome(r) == "win")
    losses = sum(1 for r in clean_trades if _extract_outcome(r) == "loss")
    n_closed = wins + losses
    overall_wr = round((wins / n_closed) * 100, 2) if n_closed else 0.0
    overall_wilson = round(wilson_lb(wins, n_closed) * 100, 2) if n_closed else 0.0
    overall_avg_r = round(sum(r["_r"] for r in clean_trades) / len(clean_trades), 4) if clean_trades else 0.0
    overall_pf = profit_factor(clean_trades)

    summary = {
        "generated_ts": generated_ts,
        "tail": args.tail,
        "detector_tail": args.detector_tail,
        "paper_trades_seen": len(paper_trades),
        "clean_trades": len(clean_trades),
        "dirty_trades": len(dirty_trades),
        "wins": wins,
        "losses": losses,
        "overall_wr": overall_wr,
        "overall_wilson": overall_wilson,
        "overall_avg_r": overall_avg_r,
        "overall_pf": overall_pf,
        "best_patterns": groups["pattern_key"][:10],
        "best_sources": groups["source_type"][:10],
        "best_directions": groups["direction"][:10],
        "best_sessions": groups["session"][:10],
        "detector_stats": detector_stats,
        "detector_attribution": detector_attribution[:50],
        "potential_edge_candidates": potential_edges[:25],
        "observation_only_candidates": observation_only_candidates[:25],
        "execution_candidates": execution_candidates[:25],
        "warnings": [f"missing:{p}" for p in missing_files],
    }

    health = {
        "status": "alive",
        "last_run_ts": generated_ts,
        "tail": args.tail,
        "detector_tail": args.detector_tail,
        "files_seen": files_seen,
        "missing_files": missing_files,
        "paper_trades_seen": len(paper_trades),
        "clean_trades": len(clean_trades),
        "dirty_trades": len(dirty_trades),
        "potential_edges": len(potential_edges),
        "execution_candidates": len(execution_candidates),
        "observation_only_candidates": len(observation_only_candidates),
        "memory_safe": True,
        "last_blocker": None,
        "warnings": summary["warnings"],
    }

    top10 = sorted(clean_trades, key=lambda r: _safe_ts(r.get("open_ts")) or 0, reverse=True)[:10]
    report = [
        "# Edge Analytics v2 Report",
        "",
        "## Status",
        f"- status: alive",
        f"- generated_ts: {generated_ts}",
        "",
        "## Data Window",
        f"- tail: {args.tail}",
        f"- detector_tail: {args.detector_tail}",
        f"- paper_trades_seen: {len(paper_trades)}",
        f"- clean_trades: {len(clean_trades)}",
        f"- dirty_trades: {len(dirty_trades)}",
        "",
        "## General Trade Summary",
        f"- wins: {wins}",
        f"- losses: {losses}",
        f"- overall_wr: {overall_wr}",
        f"- overall_wilson: {overall_wilson}",
        f"- overall_avg_r: {overall_avg_r}",
        f"- overall_pf: {overall_pf}",
        "",
        "## Clean vs Dirty Trades",
        f"- clean: {len(clean_trades)}",
        f"- dirty: {len(dirty_trades)}",
        "",
        "## Setup Source Distribution",
        _render_section("Setup Source Distribution", groups["source_type"], ["key", "n", "wins", "losses", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Most Produced Patterns",
        _render_section("Most Produced Patterns", sorted(groups["pattern_key"], key=lambda x: x["n"], reverse=True), ["key", "n", "wr", "avg_r", "pf"]),
        "",
        "## Pattern Performance",
        _render_section("Pattern Performance", groups["pattern_key"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Source Type Performance",
        _render_section("Source Type Performance", groups["source_type"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Direction Performance",
        _render_section("Direction Performance", groups["direction"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Session Performance",
        _render_section("Session Performance", groups["session"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Regime Performance",
        _render_section("Regime Performance", groups["regime"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Trend 1M Performance",
        _render_section("Trend 1M Performance", groups["trend_1m"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Location Performance",
        _render_section("Location Performance", groups["location"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Quality Tier Performance",
        _render_section("Quality Tier Performance", groups["quality_tier"], ["key", "n", "wr", "wilson_lb", "avg_r", "pf"]),
        "",
        "## Detector Production Stats",
    ]
    for name, st in detector_stats.items():
        report.append(f"- {name}: {json.dumps(st, ensure_ascii=False)}")
    report.extend([
        "",
        "## Detector Near-Trade Attribution",
    ])
    for row in detector_attribution[:30]:
        report.append(f"- {row['key']}: n={row['n']} wr={row['wr']} wilson={row['wilson_lb']} avg_r={row['avg_r']}")
    report.extend([
        "",
        "## Potential Edge Candidates",
    ])
    for row in potential_edges[:20]:
        report.append(f"- {row['key']}: n={row['n']} wr={row['wr']} wilson={row['wilson_lb']} avg_r={row['avg_r']}")
    report.extend([
        "",
        "## Observation-Only Candidates",
    ])
    for row in observation_only_candidates[:20]:
        report.append(f"- {row['key']}: n={row['n']} wr={row['wr']} wilson={row['wilson_lb']} avg_r={row['avg_r']}")
    report.extend([
        "",
        "## Execution Candidates",
    ])
    for row in execution_candidates[:20]:
        report.append(f"- {row['key']}: n={row['n']} wr={row['wr']} wilson={row['wilson_lb']} avg_r={row['avg_r']}")
    report.extend([
        "",
        "## Top 10 Recent Clean Trades",
    ])
    for trade in top10:
        report.append(f"- trade_id={trade.get('trade_id')} source={trade.get('_source_type')} pattern={trade.get('_pattern_key')} dir={trade.get('_direction')} outcome={_extract_outcome(trade)} r={trade.get('_r')} open_ts={trade.get('open_ts')}")
    report.extend([
        "",
        "## Warnings",
    ])
    for w in summary["warnings"]:
        report.append(f"- {w}")
    report.extend([
        "",
        "## Next Recommended Step",
        "- Review patterns with positive WR and positive avg_r only on a larger window before any execution change.",
    ])

    OUT_SUMMARY.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    OUT_HEALTH.write_text(json.dumps(health, indent=2, ensure_ascii=False), encoding="utf-8")
    OUT_REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")

    print(f"paper_trades_seen={len(paper_trades)} clean={len(clean_trades)} dirty={len(dirty_trades)}")
    print(f"overall_wr={overall_wr} overall_wilson={overall_wilson} overall_avg_r={overall_avg_r} overall_pf={overall_pf}")
    print(f"report={OUT_REPORT}")
    print(f"summary={OUT_SUMMARY}")
    print(f"health={OUT_HEALTH}")


if __name__ == "__main__":
    main()
