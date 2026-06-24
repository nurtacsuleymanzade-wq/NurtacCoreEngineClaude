#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

SETUPS = DATA / "setups.jsonl"
BIRTH = DATA / "setup_birth_reports.jsonl"
LIFECYCLE = DATA / "setup_lifecycle.jsonl"
PAPER_CLOSED = DATA / "paper_closed.jsonl"
PAPER_TRADES = DATA / "paper_trades.jsonl"

OUT = DATA / "historical_outcomes.jsonl"
OUT_LATEST = DATA / "historical_outcomes_latest.json"
REPORT = DATA / "historical_outcome_feed_report.json"
TAIL_LIMIT = 10000

def read_jsonl_tail(path, n=TAIL_LIMIT):
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

def sid_of(row):
    return row.get("setup_id") or row.get("source_setup_id") or row.get("qualified_setup_id")

def fnum(v):
    try:
        return float(v)
    except Exception:
        return None

def index_latest(rows):
    out = {}
    for r in rows:
        sid = sid_of(r)
        if sid:
            out[sid] = r
    return out

def existing_ids(path):
    ids = set()
    for r in read_jsonl_tail(path, TAIL_LIMIT):
        sid = sid_of(r)
        if sid:
            ids.add(sid)
    return ids

def is_closed(row):
    return (
        row.get("record_type") == "paper_trade_closed"
        or row.get("status") == "closed"
        or row.get("outcome") is not None
        or row.get("closed_ts") is not None
    )


def is_learning_eligible(row):
    quality = (row.get("hit_candle") or {}).get("price_source_quality")
    return is_closed(row) and quality == "verified_high_low"

def quality_tier(score):
    if score is None:
        return "UNKNOWN"
    if score >= 11.0:
        return "L4_PREMIUM"
    if score >= 8.0:
        return "L3_GOOD_A_PLUS"
    if score >= 6.0:
        return "L2_MEDIUM"
    if score >= 4.0:
        return "L1_LOW"
    return "L0_WEAK"

def direction_score(setup_or_birth, direction):
    scores = setup_or_birth.get("scores") or {}
    if direction == "long":
        return fnum(scores.get("long_score"))
    if direction == "short":
        return fnum(scores.get("short_score"))
    return None

def build_outcome(closed, setup, birth, lifecycle):
    sid = sid_of(closed)
    direction = closed.get("direction") or setup.get("direction") or birth.get("direction")
    score = direction_score(birth or setup or {}, direction)

    top_contributors = birth.get("top_contributors") or []
    context = birth.get("context") or {}
    triggers = birth.get("true_triggers") or []

    pnl_r = fnum(closed.get("pnl_r"))
    mfe = fnum(closed.get("mfe"))
    mae = fnum(closed.get("mae"))

    outcome = closed.get("outcome")
    regime_context = setup.get("regime_context") or {}
    if not outcome:
        if pnl_r is not None and pnl_r > 0:
            outcome = "WIN"
        elif pnl_r is not None and pnl_r < 0:
            outcome = "LOSS"
        else:
            outcome = "UNKNOWN"

    return {
        "engine": "historical_outcome_feed",
        "record_type": "historical_outcome",
        "setup_id": sid,
        "source_setup_id": closed.get("source_setup_id") or sid,

        "direction": direction,
        "setup_type": setup.get("setup_type") or birth.get("setup_type"),
        "quality_tier": setup.get("quality_tier") or quality_tier(score),
        "pattern_key": setup.get("pattern_key") or f"{setup.get('setup_type', 'unknown')}_{direction or 'unknown'}",
        "regime_at_qualification": regime_context.get("trend_regime"),
        "session_at_qualification": regime_context.get("session"),
        "volatility_at_qualification": regime_context.get("volatility_class"),
        "entry_timing": setup.get("entry_timing"),
        "direction_score": score,

        "outcome": outcome,
        "pnl_r": pnl_r,
        "mfe": mfe,
        "mae": mae,
        "duration_seconds": closed.get("duration_seconds"),
        "close_reason": closed.get("close_reason"),

        "entry_price": closed.get("entry_price"),
        "sl_price": closed.get("sl_price"),
        "tp_price": closed.get("tp_price"),
        "opened_ts": closed.get("opened_ts"),
        "closed_ts": closed.get("closed_ts"),
        "hit_candle": closed.get("hit_candle"),

        "trigger_conditions_true": triggers,
        "top_contributors": top_contributors,
        "context": context,

        "lifecycle_state": lifecycle.get("current_state"),
        "observer_last_event": ((lifecycle.get("observer") or {}).get("last_event") or {}).get("event_type"),

        "created_at": int(time.time()),
        "note": "Historical outcome record. Descriptive only; not a trade decision."
    }

def summarize(outcomes):
    c = Counter()
    by_tier = defaultdict(Counter)
    by_direction = defaultdict(Counter)
    by_layer = defaultdict(Counter)

    for o in outcomes:
        outcome = str(o.get("outcome") or "UNKNOWN").upper()
        tier = o.get("quality_tier") or "UNKNOWN"
        direction = o.get("direction") or "unknown"

        c[outcome] += 1
        by_tier[tier][outcome] += 1
        by_direction[direction][outcome] += 1

        for item in o.get("top_contributors") or []:
            layer = item.get("layer")
            if layer:
                by_layer[layer][outcome] += 1

    def pack(counter):
        total = sum(counter.values())
        wins = counter.get("WIN", 0)
        losses = counter.get("LOSS", 0)
        closed = wins + losses
        return {
            "total": total,
            "win": wins,
            "loss": losses,
            "unknown": counter.get("UNKNOWN", 0),
            "win_rate": round(wins / closed * 100, 2) if closed else None,
        }

    return {
        "total": len(outcomes),
        "overall": pack(c),
        "by_tier": {k: pack(v) for k, v in by_tier.items()},
        "by_direction": {k: pack(v) for k, v in by_direction.items()},
        "by_layer": {k: pack(v) for k, v in by_layer.items()},
    }

def main():
    setups = index_latest(read_jsonl_tail(SETUPS, TAIL_LIMIT))
    births = index_latest(read_jsonl_tail(BIRTH, TAIL_LIMIT))
    lifecycles = index_latest(read_jsonl_tail(LIFECYCLE, TAIL_LIMIT))

    closed_rows = []
    for r in read_jsonl_tail(PAPER_CLOSED, TAIL_LIMIT) + read_jsonl_tail(PAPER_TRADES, TAIL_LIMIT):
        if is_learning_eligible(r):
            closed_rows.append(r)

    done = existing_ids(OUT)
    new_outcomes = []
    skipped_existing = 0
    skipped_missing_sid = 0

    with OUT.open("a", encoding="utf-8") as f:
        for closed in closed_rows:
            sid = sid_of(closed)
            if not sid:
                skipped_missing_sid += 1
                continue
            if sid in done:
                skipped_existing += 1
                continue

            setup = setups.get(sid, {})
            birth = births.get(sid, {})
            lifecycle = lifecycles.get(sid, {})

            outcome = build_outcome(closed, setup, birth, lifecycle)
            f.write(json.dumps(outcome, ensure_ascii=False) + "\n")
            new_outcomes.append(outcome)
            done.add(sid)

    all_outcomes = read_jsonl_tail(OUT, TAIL_LIMIT)
    summary = summarize(all_outcomes)

    latest_payload = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "summary": summary,
        "latest_outcomes": all_outcomes[-50:],
        "files": {
            "historical_outcomes": str(OUT),
            "paper_closed": str(PAPER_CLOSED),
            "paper_trades": str(PAPER_TRADES),
        }
    }

    OUT_LATEST.write_text(json.dumps(latest_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    report = {
        "checked_at_utc": latest_payload["checked_at_utc"],
        "closed_rows_seen": len(closed_rows),
        "new_outcomes_written": len(new_outcomes),
        "skipped_existing": skipped_existing,
        "skipped_missing_sid": skipped_missing_sid,
        "historical_total": len(all_outcomes),
        "summary": summary,
        "output_jsonl": str(OUT),
        "output_latest": str(OUT_LATEST),
    }

    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
