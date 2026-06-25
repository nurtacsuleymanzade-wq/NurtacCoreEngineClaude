#!/usr/bin/env python3
import json, subprocess, time, os
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "system_state_deep_audit_report.json"
OUT_MD = DATA / "system_state_deep_audit_report.md"

FILES = {
    "1s_dna": DATA / "combined_1s_dna_btcusdt.jsonl",
    "rolling_3s": DATA / "rolling_3s_dna.jsonl",
    "rolling_5s": DATA / "rolling_5s_dna.jsonl",
    "rolling_15s": DATA / "rolling_15s_dna.jsonl",
    "aligned_1m": DATA / "aligned_1m_candle_dna.jsonl",
    "evidence": DATA / "evidence_stream.jsonl",
    "setups": DATA / "setups.jsonl",
    "qualified": DATA / "qualified_setups.jsonl",
    "observations": DATA / "observations.jsonl",
    "paper_trades": DATA / "paper_trades.jsonl",
    "paper_closed": DATA / "paper_closed.jsonl",
    "historical": DATA / "historical_outcomes.jsonl",
    "calibration": DATA / "calibration_profiles.json",
    "decision_gate_calibration": DATA / "decision_gate_calibration_view.json",
    "setup_dna": DATA / "setup_dna_latest.jsonl",
    "birth_reports": DATA / "setup_birth_reports.jsonl",
    "lifecycle": DATA / "setup_lifecycle.jsonl",
}

def sh(cmd):
    return subprocess.getoutput(cmd)

def read_jsonl_tail(path, n=50000):
    if not path.exists():
        return []
    rows=[]
    out=sh(f"tail -{int(n)} {path}")
    for line in out.splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def stat_file(path):
    if not path.exists():
        return {"exists": False}
    st=path.stat()
    return {
        "exists": True,
        "size_bytes": st.st_size,
        "size_mb": round(st.st_size/1024/1024, 3),
        "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(st.st_mtime)),
    }

def sid(r):
    return r.get("setup_id") or r.get("source_setup_id") or r.get("qualified_setup_id")

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

def score_of(row):
    d=row.get("direction")
    if d=="long":
        return fnum(row.get("score_long") or row.get("direction_score"))
    if d=="short":
        return fnum(row.get("score_short") or row.get("direction_score"))
    return fnum(row.get("direction_score"))

def tier(score):
    if score is None: return "UNKNOWN"
    if score >= 11: return "L4_PREMIUM"
    if score >= 8: return "L3_GOOD_A_PLUS"
    if score >= 6: return "L2_MEDIUM"
    if score >= 4: return "L1_LOW"
    return "L0_WEAK"

def outcome_class(r):
    o=str(r.get("outcome") or "").upper()
    pnl=fnum(r.get("pnl_r"))
    if o=="WIN" or (pnl is not None and pnl>0): return "WIN"
    if o=="LOSS" or (pnl is not None and pnl<0): return "LOSS"
    return "UNKNOWN"

def price_quality(r):
    return ((r.get("hit_candle") or {}).get("price_source_quality")) or "unknown"

def pack_outcomes(rows):
    c=Counter(outcome_class(r) for r in rows)
    pnl=[fnum(r.get("pnl_r")) for r in rows if fnum(r.get("pnl_r")) is not None]
    dur=[fnum(r.get("duration_seconds")) for r in rows if fnum(r.get("duration_seconds")) is not None]
    wins=c.get("WIN",0); losses=c.get("LOSS",0); closed=wins+losses
    return {
        "total": len(rows),
        "wins": wins,
        "losses": losses,
        "unknown": c.get("UNKNOWN",0),
        "win_rate": round(wins/closed*100,2) if closed else None,
        "avg_pnl_r": round(sum(pnl)/len(pnl),4) if pnl else None,
        "avg_duration_seconds": round(sum(dur)/len(dur),2) if dur else None,
    }

def group_stats(rows, keyfn):
    buckets=defaultdict(list)
    for r in rows:
        buckets[keyfn(r)].append(r)
    return {k: pack_outcomes(v) for k,v in sorted(buckets.items(), key=lambda x: str(x[0]))}

def setup_type_key(r):
    ctx=r.get("context") or {}
    return "|".join([
        str(r.get("direction") or "unknown"),
        str(r.get("setup_type") or "unknown"),
        str(r.get("quality_tier") or tier(fnum(r.get("direction_score")))),
        str(ctx.get("gate_grade") or "gate_unknown"),
        str(ctx.get("trend_1s") or "1s_unknown"),
        str(ctx.get("trend_1m") or "1m_unknown"),
    ])

def top_contributor_layers(rows):
    c=Counter()
    points=defaultdict(float)
    for r in rows:
        for item in r.get("top_contributors") or []:
            layer=item.get("layer")
            if layer:
                c[layer]+=1
                try:
                    points[layer]+=float(item.get("points") or item.get("score") or item.get("contribution") or 0)
                except Exception:
                    pass
    return {
        "count": dict(c.most_common()),
        "points_sum": {k: round(v,4) for k,v in sorted(points.items(), key=lambda x:x[1], reverse=True)}
    }

def runtime():
    return {
        "free_h": sh("free -h"),
        "top_mem": sh("ps aux --sort=-%mem | head -20"),
        "timers": sh("systemctl list-timers --all | grep nurtac || true"),
        "running_services": sh("systemctl --type=service --state=running | grep nurtac || true"),
        "disk": sh("df -h / /root /root/NurtacCoreEngineClaude 2>/dev/null"),
    }

# Load rows
setups=read_jsonl_tail(FILES["setups"], 100000)
setup_dna=read_jsonl_tail(FILES["setup_dna"], 100000)
birth=read_jsonl_tail(FILES["birth_reports"], 100000)
paper=read_jsonl_tail(FILES["paper_trades"], 100000)
closed=read_jsonl_tail(FILES["paper_closed"], 100000)
historical=read_jsonl_tail(FILES["historical"], 100000)
obs=read_jsonl_tail(FILES["observations"], 100000)
qualified=read_jsonl_tail(FILES["qualified"], 100000)

# If historical empty, use verified paper_closed as fallback for analysis
analysis_outcomes = historical if historical else [r for r in closed if r.get("record_type")=="paper_trade_closed"]

setup_quality=Counter()
setup_direction=Counter()
setup_state=Counter()
score_bands=Counter()

for r in setup_dna:
    sc=score_of(r)
    setup_quality[tier(sc)] += 1
    setup_direction[r.get("direction") or "unknown"] += 1
    setup_state[r.get("state") or "unknown"] += 1
    if sc is None:
        score_bands["UNKNOWN"] += 1
    elif sc < 8:
        score_bands["<8"] += 1
    elif sc < 8.25:
        score_bands["8.00-8.24"] += 1
    elif sc < 8.5:
        score_bands["8.25-8.49"] += 1
    elif sc < 8.75:
        score_bands["8.50-8.74"] += 1
    elif sc < 9:
        score_bands["8.75-8.99"] += 1
    elif sc < 10:
        score_bands["9.00-9.99"] += 1
    elif sc < 11:
        score_bands["10.00-10.99"] += 1
    else:
        score_bands["11+"] += 1

pq=Counter(price_quality(r) for r in closed if r.get("record_type")=="paper_trade_closed")
closed_ts=Counter(str(r.get("closed_ts")) for r in closed if r.get("closed_ts"))
dup_ts={k:v for k,v in closed_ts.items() if v>1}

layers=top_contributor_layers(setup_dna)

result={
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "files": {k: stat_file(v) for k,v in FILES.items()},
    "counts": {
        "setups": len(setups),
        "setup_dna": len(setup_dna),
        "birth_reports": len(birth),
        "observations": len(obs),
        "qualified": len(qualified),
        "paper_rows": len(paper),
        "paper_closed_rows": len(closed),
        "historical_outcomes": len(historical),
    },
    "setup_summary": {
        "quality_distribution": dict(setup_quality),
        "direction_distribution": dict(setup_direction),
        "state_distribution": dict(setup_state),
        "score_bands": dict(score_bands),
        "layer_contributors": layers,
    },
    "outcome_learning": {
        "source_used": "historical_outcomes.jsonl" if historical else "paper_closed.jsonl_fallback",
        "overall": pack_outcomes(analysis_outcomes),
        "by_direction": group_stats(analysis_outcomes, lambda r: r.get("direction") or "unknown"),
        "by_quality": group_stats(analysis_outcomes, lambda r: r.get("quality_tier") or "UNKNOWN"),
        "by_setup_type": group_stats(analysis_outcomes, setup_type_key),
        "by_close_reason": group_stats(analysis_outcomes, lambda r: r.get("close_reason") or "unknown"),
    },
    "price_integrity": {
        "paper_closed_quality": dict(pq),
        "duplicate_closed_timestamp_groups": dup_ts,
        "largest_duplicate_group": max(dup_ts.values(), default=1),
        "close_only_present": pq.get("close_only",0) > 0,
    },
    "ecosystem": {
        "layer_flow": [
            "Layer-0 combined_1s_dna_btcusdt.jsonl",
            "Layer-1 rolling_3s/5s/15s",
            "Layer-2 aligned_1m_candle_dna",
            "Layer-3 context/baseline/market_context",
            "Layer-4 detector labels",
            "Layer-5 evidence_stream",
            "Layer-6 smart_money / observer / volume_profile",
            "Layer-7 setups",
            "Layer-13 paper",
            "Historical Outcome",
            "Calibration",
            "Telegram curated summary"
        ],
        "known_missing_outputs": [
            "dominant_timeframe",
            "window_contribution",
            "timeframe_agreement/conflict",
            "regime label",
            "veto_reason",
            "runtime health self-healing",
            "causality chain"
        ]
    },
    "runtime": runtime(),
}

OUT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

md=[]
md.append("# NurtacCoreEngineClaude Deep State Audit\n")
md.append(f"Checked: {result['checked_at_utc']}\n")
md.append("## Counts")
for k,v in result["counts"].items():
    md.append(f"- {k}: {v}")
md.append("\n## Setup Quality")
md.append(json.dumps(result["setup_summary"], indent=2, ensure_ascii=False))
md.append("\n## Outcome Learning")
md.append(json.dumps(result["outcome_learning"], indent=2, ensure_ascii=False))
md.append("\n## Price Integrity")
md.append(json.dumps(result["price_integrity"], indent=2, ensure_ascii=False))
md.append("\n## Runtime")
md.append(result["runtime"]["free_h"])
md.append(result["runtime"]["top_mem"])
md.append(result["runtime"]["timers"])
OUT_MD.write_text("\n".join(md), encoding="utf-8")

print(json.dumps({
    "report": str(OUT),
    "markdown": str(OUT_MD),
    "counts": result["counts"],
    "setup_quality": result["setup_summary"]["quality_distribution"],
    "outcome_overall": result["outcome_learning"]["overall"],
    "price_integrity": result["price_integrity"],
}, indent=2, ensure_ascii=False))
