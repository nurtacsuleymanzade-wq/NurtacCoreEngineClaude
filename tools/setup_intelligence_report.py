#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

LIFECYCLE = DATA / "setup_lifecycle.jsonl"
TIMELINE = DATA / "setup_timeline.jsonl"
BIRTH = DATA / "setup_birth_reports.jsonl"
HEALTH = DATA / "setup_guardian_health.json"

OUT_SUMMARY = DATA / "setup_intelligence_summary.json"
OUT_DNA = DATA / "setup_dna_latest.jsonl"
OUT_WEEKLY = DATA / "setup_weekly_report.json"
OUT_TEXT = DATA / "setup_intelligence_report.txt"

def read_jsonl(path, limit=None):
    if not path.exists():
        return []
    cmd = f"tail -{int(limit)} {path}" if limit else f"cat {path}"
    rows = []
    for line in subprocess.getoutput(cmd).splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def fnum(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def classify_result(lc):
    result = lc.get("result") or {}
    paper = lc.get("paper") or {}
    closed = paper.get("closed") or result or {}
    outcome = str(closed.get("outcome") or "").lower()
    pnl_r = fnum(closed.get("pnl_r"))

    if "win" in outcome or pnl_r > 0:
        return "win"
    if "loss" in outcome or pnl_r < 0:
        return "loss"
    if lc.get("current_state") in ("invalidated", "expired", "waiting_timeout"):
        return lc.get("current_state")
    return "open"

def setup_key(lc):
    birth = lc.get("birth") or {}
    ctx = birth.get("context") or {}
    direction = lc.get("direction") or birth.get("direction") or "unknown"
    stype = lc.get("setup_type") or birth.get("setup_type") or "unknown"
    gate = ctx.get("gate_grade") or "none"
    t1 = ctx.get("trend_1s") or "unknown"
    t1m = ctx.get("trend_1m") or "unknown"
    bos = ctx.get("micro_bos") or ctx.get("macro_bos") or "none"
    return f"{direction}|{stype}|gate={gate}|1s={t1}|1m={t1m}|bos={bos}"

def setup_dna(lc):
    birth = lc.get("birth") or {}
    ctx = birth.get("context") or {}
    contrib = birth.get("top_contributors") or []
    scores = birth.get("scores") or {}
    result = lc.get("result") or {}
    paper = lc.get("paper") or {}
    closed = paper.get("closed") or result or {}

    return {
        "setup_id": lc.get("setup_id"),
        "state": lc.get("current_state"),
        "direction": lc.get("direction"),
        "setup_type": lc.get("setup_type"),
        "score_long": scores.get("long_score"),
        "score_short": scores.get("short_score"),
        "score_gap": scores.get("score_gap"),
        "gate": ctx.get("gate_grade"),
        "trend_1s": ctx.get("trend_1s"),
        "trend_1m": ctx.get("trend_1m"),
        "trend_5m": ctx.get("trend_5m"),
        "micro_bos": ctx.get("micro_bos"),
        "macro_bos": ctx.get("macro_bos"),
        "active_ob": ctx.get("active_ob_count"),
        "active_fvg": ctx.get("active_fvg_count"),
        "top_contributors": contrib,
        "outcome": closed.get("outcome"),
        "pnl_r": closed.get("pnl_r"),
        "mfe": closed.get("mfe"),
        "mae": closed.get("mae"),
        "duration_seconds": closed.get("duration_seconds"),
        "classification": classify_result(lc),
    }

def main():
    lifecycles = read_jsonl(LIFECYCLE, 5000)
    latest_by_id = {}

    for lc in lifecycles:
        sid = lc.get("setup_id")
        if sid:
            latest_by_id[sid] = lc

    latest = list(latest_by_id.values())
    total = len(latest)

    result_counts = Counter(classify_result(x) for x in latest)
    closed = [x for x in latest if classify_result(x) in ("win", "loss")]
    wins = result_counts.get("win", 0)
    losses = result_counts.get("loss", 0)
    win_rate = round(wins / len(closed) * 100, 2) if closed else None

    by_type = defaultdict(list)
    layer_stats = defaultdict(lambda: {"count": 0, "wins": 0, "losses": 0, "pnl_sum": 0.0})

    dna_rows = []
    for lc in latest:
        key = setup_key(lc)
        by_type[key].append(lc)

        dna = setup_dna(lc)
        dna_rows.append(dna)

        cls = dna["classification"]
        for item in dna.get("top_contributors") or []:
            layer = item.get("layer")
            if not layer:
                continue
            layer_stats[layer]["count"] += 1
            if cls == "win":
                layer_stats[layer]["wins"] += 1
            elif cls == "loss":
                layer_stats[layer]["losses"] += 1
            layer_stats[layer]["pnl_sum"] += fnum(dna.get("pnl_r"))

    type_summary = {}
    for k, arr in by_type.items():
        c = Counter(classify_result(x) for x in arr)
        closed_n = c.get("win", 0) + c.get("loss", 0)
        type_summary[k] = {
            "count": len(arr),
            "wins": c.get("win", 0),
            "losses": c.get("loss", 0),
            "win_rate": round(c.get("win", 0) / closed_n * 100, 2) if closed_n else None,
            "open": c.get("open", 0),
            "invalidated": c.get("invalidated", 0),
            "expired": c.get("expired", 0),
            "waiting_timeout": c.get("waiting_timeout", 0),
        }

    layer_summary = {}
    for layer, s in layer_stats.items():
        closed_n = s["wins"] + s["losses"]
        layer_summary[layer] = {
            "count": s["count"],
            "wins": s["wins"],
            "losses": s["losses"],
            "win_rate": round(s["wins"] / closed_n * 100, 2) if closed_n else None,
            "avg_pnl_r": round(s["pnl_sum"] / s["count"], 4) if s["count"] else None,
        }

    summary = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "total_setups": total,
        "result_counts": dict(result_counts),
        "closed_count": len(closed),
        "win_rate": win_rate,
        "by_setup_type": type_summary,
        "layer_contribution_stats": layer_summary,
        "latest_dna_count": len(dna_rows),
        "note": "This is descriptive historical intelligence, not trade decision, not hardcoded probability.",
    }

    OUT_SUMMARY.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    OUT_WEEKLY.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    with OUT_DNA.open("w", encoding="utf-8") as f:
        for row in dna_rows[-500:]:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    best_layer = None
    worst_layer = None
    eligible = {k: v for k, v in layer_summary.items() if v["win_rate"] is not None}
    if eligible:
        best_layer = max(eligible.items(), key=lambda x: x[1]["win_rate"])
        worst_layer = min(eligible.items(), key=lambda x: x[1]["win_rate"])

    text = f"""📚 SETUP INTELLIGENCE REPORT

Total setups: {total}
Closed: {len(closed)}
Wins: {wins}
Losses: {losses}
Win rate: {win_rate}

States:
{json.dumps(dict(result_counts), ensure_ascii=False)}

Best layer:
{best_layer}

Weakest layer:
{worst_layer}

Files:
- {OUT_SUMMARY}
- {OUT_DNA}
- {OUT_WEEKLY}

Not:
Bu rapor karar üretmez. Sadece Guardian'ın gördüğü setup geçmişini açıklar.
"""
    OUT_TEXT.write_text(text, encoding="utf-8")
    print(text)

if __name__ == "__main__":
    main()
