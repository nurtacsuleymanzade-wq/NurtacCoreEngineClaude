#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
OUT = DATA / "setup_lifecycle_report.json"

def tail_jsonl(name, n=200):
    p = DATA / name
    if not p.exists():
        return []
    rows = []
    for line in subprocess.getoutput(f"tail -{n} {p}").splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

setups = tail_jsonl("setups.jsonl", 200)
obs = tail_jsonl("observations.jsonl", 500)
qualified = tail_jsonl("qualified_setups.jsonl", 200)
paper = tail_jsonl("paper_trades.jsonl", 200)

obs_by_setup = {}
for o in obs:
    sid = o.get("source_setup_id")
    if sid:
        obs_by_setup.setdefault(sid, []).append(o)

qualified_by_setup = {
    q.get("source_setup_id"): q for q in qualified if q.get("source_setup_id")
}

paper_by_setup = {
    p.get("source_setup_id"): p for p in paper if p.get("source_setup_id")
}

lifecycle = []
for s in setups[-50:]:
    sid = s.get("setup_id")
    events = obs_by_setup.get(sid, [])
    q = qualified_by_setup.get(sid)
    pt = paper_by_setup.get(sid)

    if pt:
        state = "paper_closed_or_recorded"
    elif q:
        state = "qualified_no_paper"
    elif events:
        last_event = events[-1].get("event_type")
        if last_event in ("INVALIDATED", "EXPIRED"):
            state = last_event.lower()
        else:
            state = "observing_not_qualified"
    else:
        state = "setup_not_seen_by_observer"

    lifecycle.append({
        "setup_id": sid,
        "direction": s.get("direction"),
        "setup_type": s.get("setup_type"),
        "setup_ts": s.get("window_start_ts"),
        "setup_status": s.get("status"),
        "observer_event_count": len(events),
        "last_observer_event": events[-1].get("event_type") if events else None,
        "qualified": bool(q),
        "paper_recorded": bool(pt),
        "lifecycle_state": state
    })

summary = {}
for x in lifecycle:
    summary[x["lifecycle_state"]] = summary.get(x["lifecycle_state"], 0) + 1

report = {
    "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
    "summary": summary,
    "lifecycle_last_50_setups": lifecycle,
    "decision": "If many setup_not_seen_by_observer, observer tail/cursor is broken. If observing_not_qualified dominates, qualification criteria are too strict or market did not confirm. If qualified_no_paper appears, paper trade input is broken."
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
print("REPORT:", OUT)
