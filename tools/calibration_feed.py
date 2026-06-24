#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

HIST = DATA / "historical_outcomes.jsonl"

OUT_PROFILE = DATA / "calibration_profiles.json"
OUT_JSONL = DATA / "calibration_feed.jsonl"
OUT_REPORT = DATA / "calibration_feed_report.json"
OUT_DECISION_GATE_VIEW = DATA / "decision_gate_calibration_view.json"

def read_jsonl_tail(path, n=200000):
    if not path.exists():
        return []
    rows = []
    out = subprocess.getoutput(f"tail -{int(n)} {path}")
    for line in out.splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def fnum(v):
    try:
        return float(v)
    except Exception:
        return None

def outcome_class(row):
    outcome = str(row.get("outcome") or "").upper()
    pnl_r = fnum(row.get("pnl_r"))

    if outcome == "WIN" or (pnl_r is not None and pnl_r > 0):
        return "WIN"
    if outcome == "LOSS" or (pnl_r is not None and pnl_r < 0):
        return "LOSS"
    return "UNKNOWN"

def pack(counter, pnl_values, duration_values):
    wins = counter.get("WIN", 0)
    losses = counter.get("LOSS", 0)
    unknown = counter.get("UNKNOWN", 0)
    closed = wins + losses
    total = closed + unknown

    return {
        "sample_count": total,
        "closed_count": closed,
        "wins": wins,
        "losses": losses,
        "unknown": unknown,
        "win_rate_observed": round(wins / closed * 100, 2) if closed else None,
        "avg_pnl_r": round(sum(pnl_values) / len(pnl_values), 4) if pnl_values else None,
        "avg_duration_seconds": round(sum(duration_values) / len(duration_values), 2) if duration_values else None,
        "status": "measured" if closed else "insufficient_closed_samples",
    }

def setup_type_key(row):
    return "|".join([
        str(row.get("direction") or "unknown"),
        str(row.get("setup_type") or "unknown"),
        str(row.get("quality_tier") or "UNKNOWN"),
        str((row.get("context") or {}).get("gate_grade") or "gate_unknown"),
        str((row.get("context") or {}).get("trend_1s") or "1s_unknown"),
        str((row.get("context") or {}).get("trend_1m") or "1m_unknown"),
    ])

def layer_keys(row):
    out = []
    for item in row.get("top_contributors") or []:
        layer = item.get("layer")
        if layer:
            out.append(layer)
    return out

def trigger_keys(row):
    return [str(x) for x in (row.get("trigger_conditions_true") or [])]

def pattern_key(row):
    explicit = row.get("pattern_key")
    if explicit:
        return str(explicit)
    setup_type = str(row.get("setup_type") or "unknown")
    direction = str(row.get("direction") or "unknown")
    return f"{setup_type}_{direction}"

def regime_key(row):
    regime = row.get("regime_at_qualification")
    if regime:
        return str(regime)
    regime_context = row.get("regime_context") or {}
    return str(regime_context.get("trend_regime") or "UNKNOWN")

def session_key(row):
    session = row.get("session_at_qualification")
    if session:
        return str(session)
    regime_context = row.get("regime_context") or {}
    return str(regime_context.get("session") or "UNKNOWN")

def tier_key(row):
    tier = str(row.get("quality_tier") or "UNKNOWN")
    return tier.replace("L3_GOOD_A_PLUS", "L3_GOOD_A+")

def compact_wr(rows):
    outcomes = [outcome_class(row) for row in rows]
    closed = [outcome for outcome in outcomes if outcome in ("WIN", "LOSS")]
    wins = sum(1 for outcome in closed if outcome == "WIN")
    return {
        "count": len(closed),
        "wr": round(wins / len(closed), 3) if closed else None,
    }

def compact_breakdown(rows, key_fn):
    buckets = defaultdict(list)
    for row in rows:
        buckets[key_fn(row)].append(row)
    return {key: compact_wr(bucket) for key, bucket in buckets.items()}

def build_pattern_profiles(rows):
    buckets = defaultdict(list)
    for row in rows:
        buckets[pattern_key(row)].append(row)
    profiles = {}
    for key, bucket in buckets.items():
        total = compact_wr(bucket)
        profiles[key] = {
            "pattern_key": key,
            "total_wr": total.get("wr"),
            "count": total.get("count", 0),
            "by_regime": compact_breakdown(bucket, regime_key),
            "by_session": compact_breakdown(bucket, session_key),
            "by_tier": compact_breakdown(bucket, tier_key),
        }
    return profiles

def build_group_stats(rows, key_fn):
    buckets = defaultdict(list)
    for r in rows:
        key = key_fn(r)
        if not key:
            continue
        buckets[key].append(r)

    result = {}
    for key, arr in buckets.items():
        c = Counter(outcome_class(r) for r in arr)
        pnl = [fnum(r.get("pnl_r")) for r in arr if fnum(r.get("pnl_r")) is not None]
        dur = [fnum(r.get("duration_seconds")) for r in arr if fnum(r.get("duration_seconds")) is not None]
        result[key] = pack(c, pnl, dur)
    return result

def build_layer_stats(rows):
    buckets = defaultdict(list)
    for r in rows:
        for layer in layer_keys(r):
            buckets[layer].append(r)

    result = {}
    for layer, arr in buckets.items():
        c = Counter(outcome_class(r) for r in arr)
        pnl = [fnum(r.get("pnl_r")) for r in arr if fnum(r.get("pnl_r")) is not None]
        dur = [fnum(r.get("duration_seconds")) for r in arr if fnum(r.get("duration_seconds")) is not None]
        result[layer] = pack(c, pnl, dur)
    return result

def build_trigger_stats(rows):
    buckets = defaultdict(list)
    for r in rows:
        for trig in trigger_keys(r):
            buckets[trig].append(r)

    result = {}
    for trig, arr in buckets.items():
        c = Counter(outcome_class(r) for r in arr)
        pnl = [fnum(r.get("pnl_r")) for r in arr if fnum(r.get("pnl_r")) is not None]
        dur = [fnum(r.get("duration_seconds")) for r in arr if fnum(r.get("duration_seconds")) is not None]
        result[trig] = pack(c, pnl, dur)
    return result

def rank_best_worst(stats):
    measured = [
        (k, v) for k, v in stats.items()
        if v.get("closed_count", 0) > 0 and v.get("win_rate_observed") is not None
    ]

    best = sorted(
        measured,
        key=lambda x: (x[1].get("win_rate_observed") or 0, x[1].get("avg_pnl_r") or 0, x[1].get("closed_count") or 0),
        reverse=True
    )[:10]

    worst = sorted(
        measured,
        key=lambda x: (x[1].get("win_rate_observed") or 0, x[1].get("avg_pnl_r") or 0)
    )[:10]

    return {
        "best": [{"key": k, **v} for k, v in best],
        "worst": [{"key": k, **v} for k, v in worst],
    }

def main():
    rows = read_jsonl_tail(HIST, 200000)

    c = Counter(outcome_class(r) for r in rows)
    pnl = [fnum(r.get("pnl_r")) for r in rows if fnum(r.get("pnl_r")) is not None]
    dur = [fnum(r.get("duration_seconds")) for r in rows if fnum(r.get("duration_seconds")) is not None]

    overall = pack(c, pnl, dur)

    by_setup_type = build_group_stats(rows, setup_type_key)
    by_quality = build_group_stats(rows, lambda r: r.get("quality_tier") or "UNKNOWN")
    by_direction = build_group_stats(rows, lambda r: r.get("direction") or "unknown")
    by_close_reason = build_group_stats(rows, lambda r: r.get("close_reason") or "unknown")
    by_layer = build_layer_stats(rows)
    by_trigger = build_trigger_stats(rows)
    patterns = build_pattern_profiles(rows)

    profile = {
        "engine": "calibration_feed",
        "record_type": "calibration_profile",
        "created_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "source": str(HIST),
        "overall": overall,
        "by_setup_type": by_setup_type,
        "by_quality": by_quality,
        "by_direction": by_direction,
        "by_close_reason": by_close_reason,
        "by_layer": by_layer,
        "by_trigger": by_trigger,
        "patterns": patterns,
        "by_regime": compact_breakdown(rows, regime_key),
        "by_session": compact_breakdown(rows, session_key),
        "by_tier": compact_breakdown(rows, tier_key),
        "rankings": {
            "setup_type": rank_best_worst(by_setup_type),
            "quality": rank_best_worst(by_quality),
            "layer": rank_best_worst(by_layer),
            "trigger": rank_best_worst(by_trigger),
        },
        "guardrails": {
            "trade_decision": False,
            "hardcoded_probability": False,
            "hardcoded_confidence": False,
            "description": "Measured historical outcome profile only. Decision Gate may read this as calibration evidence, not as direct trade signal."
        }
    }

    OUT_PROFILE.write_text(json.dumps(profile, indent=2, ensure_ascii=False), encoding="utf-8")

    with OUT_JSONL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(profile, ensure_ascii=False) + "\n")

    decision_gate_view = {
        "created_at_utc": profile["created_at_utc"],
        "source_profile": str(OUT_PROFILE),
        "overall": overall,
        "best_setup_types": profile["rankings"]["setup_type"]["best"],
        "worst_setup_types": profile["rankings"]["setup_type"]["worst"],
        "best_layers": profile["rankings"]["layer"]["best"],
        "worst_layers": profile["rankings"]["layer"]["worst"],
        "status": "ready_as_evidence" if overall.get("closed_count", 0) > 0 else "waiting_for_closed_historical_outcomes",
        "decision_gate_may_use_as": "calibration_evidence_only",
    }

    OUT_DECISION_GATE_VIEW.write_text(json.dumps(decision_gate_view, indent=2, ensure_ascii=False), encoding="utf-8")

    report = {
        "checked_at_utc": profile["created_at_utc"],
        "historical_rows_seen": len(rows),
        "closed_count": overall.get("closed_count"),
        "overall": overall,
        "setup_type_count": len(by_setup_type),
        "layer_count": len(by_layer),
        "trigger_count": len(by_trigger),
        "decision_gate_view": str(OUT_DECISION_GATE_VIEW),
        "profile": str(OUT_PROFILE),
        "jsonl": str(OUT_JSONL),
        "status": decision_gate_view["status"],
    }

    OUT_REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
