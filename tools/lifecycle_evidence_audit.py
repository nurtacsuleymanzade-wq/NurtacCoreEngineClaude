#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path
from collections import Counter

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

SETUPS = DATA / "setups.jsonl"
OBS = DATA / "observations.jsonl"
QUAL = DATA / "qualified_setups.jsonl"
PAPER = DATA / "paper_trades.jsonl"
HIST_OBS = DATA / "historical_outcome_observations.jsonl"
HIST = DATA / "historical_outcomes.jsonl"
CALIB = DATA / "calibration_profiles.json"

OUT_JSON = DATA / "lifecycle_evidence_audit_report.json"
OUT_MD = DATA / "lifecycle_evidence_audit_report.md"

def read_jsonl(path, tail=50000):
    if not path.exists():
        return []
    rows = []
    out = subprocess.getoutput(f"tail -{int(tail)} {path}")
    for line in out.splitlines():
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows

def sid_of(row):
    return (
        row.get("setup_id")
        or row.get("source_setup_id")
        or row.get("qualified_setup_id")
    )

def index_many(rows):
    out = {}
    for r in rows:
        sid = sid_of(r)
        if sid:
            out.setdefault(sid, []).append(r)
    return out

def is_closed_paper(r):
    return (
        r.get("status") == "closed"
        or r.get("record_type") == "paper_trade_closed"
        or r.get("outcome") is not None
        or r.get("close_reason") is not None
        or r.get("closed_ts") is not None
    )

def paper_state(rows):
    opened = [r for r in rows if r.get("status") == "open" or r.get("record_type") == "paper_trade_open"]
    closed = [r for r in rows if is_closed_paper(r)]
    return opened, closed

def obs_state(rows):
    events = [r.get("event_type") for r in rows if r.get("event_type")]
    if any(e == "INVALIDATED" for e in events):
        return "invalidated"
    if any(e == "EXPIRED" for e in events):
        return "expired"
    if any(e == "WAITING_TIMEOUT" for e in events):
        return "waiting_timeout"
    if rows:
        return "observed"
    return "not_seen"

def hist_state(rows):
    if not rows:
        return False
    return True

def main():
    setups = read_jsonl(SETUPS)
    obs = read_jsonl(OBS)
    qual = read_jsonl(QUAL)
    paper = read_jsonl(PAPER)
    hist_obs = read_jsonl(HIST_OBS)
    hist = read_jsonl(HIST)

    obs_i = index_many(obs)
    qual_i = index_many(qual)
    paper_i = index_many(paper)
    hist_obs_i = index_many(hist_obs)
    hist_i = index_many(hist)

    report_rows = []
    anomalies = []

    for s in setups:
        sid = sid_of(s)
        if not sid:
            continue

        o_rows = obs_i.get(sid, [])
        q_rows = qual_i.get(sid, [])
        p_rows = paper_i.get(sid, [])
        hobs_rows = hist_obs_i.get(sid, [])
        h_rows = hist_i.get(sid, [])

        p_open, p_closed = paper_state(p_rows)
        o_state = obs_state(o_rows)

        row = {
            "setup_id": sid,
            "direction": s.get("direction"),
            "setup_type": s.get("setup_type"),
            "setup_ts": s.get("window_start_ts") or s.get("qualification_ts") or s.get("ts"),
            "observer_seen": bool(o_rows),
            "observer_events_count": len(o_rows),
            "observer_state": o_state,
            "qualified": bool(q_rows),
            "qualified_count": len(q_rows),
            "paper_opened": bool(p_open),
            "paper_open_count": len(p_open),
            "paper_closed": bool(p_closed),
            "paper_closed_count": len(p_closed),
            "historical_observation_written": bool(hobs_rows),
            "historical_outcome_written": bool(h_rows),
            "paper_open_without_observer": bool(p_open) and not o_rows,
            "paper_open_without_qualified": bool(p_open) and not q_rows,
            "paper_open_after_terminal_observer": bool(p_open) and o_state in ("invalidated", "expired", "waiting_timeout"),
            "closed_without_historical": bool(p_closed) and not (hobs_rows or h_rows),
            "last_observer_event": o_rows[-1].get("event_type") if o_rows else None,
            "last_paper_status": p_rows[-1].get("status") if p_rows else None,
            "last_paper_record_type": p_rows[-1].get("record_type") if p_rows else None,
            "last_paper_outcome": p_rows[-1].get("outcome") if p_rows else None,
        }
        report_rows.append(row)

        if row["paper_open_without_observer"]:
            anomalies.append({"setup_id": sid, "type": "PAPER_OPEN_WITHOUT_OBSERVER"})
        if row["paper_open_without_qualified"]:
            anomalies.append({"setup_id": sid, "type": "PAPER_OPEN_WITHOUT_QUALIFIED"})
        if row["paper_open_after_terminal_observer"]:
            anomalies.append({"setup_id": sid, "type": "PAPER_OPEN_AFTER_TERMINAL_OBSERVER", "observer_state": o_state})
        if row["closed_without_historical"]:
            anomalies.append({"setup_id": sid, "type": "PAPER_CLOSED_WITHOUT_HISTORICAL"})

    c = Counter()
    for r in report_rows:
        c["setups"] += 1
        c["observer_seen"] += int(r["observer_seen"])
        c["qualified"] += int(r["qualified"])
        c["paper_opened"] += int(r["paper_opened"])
        c["paper_closed"] += int(r["paper_closed"])
        c["historical_observation_written"] += int(r["historical_observation_written"])
        c["historical_outcome_written"] += int(r["historical_outcome_written"])
        c["paper_open_without_observer"] += int(r["paper_open_without_observer"])
        c["paper_open_without_qualified"] += int(r["paper_open_without_qualified"])
        c["paper_open_after_terminal_observer"] += int(r["paper_open_after_terminal_observer"])
        c["closed_without_historical"] += int(r["closed_without_historical"])

    files = {
        "setups": str(SETUPS),
        "observations": str(OBS),
        "qualified_setups": str(QUAL),
        "paper_trades": str(PAPER),
        "historical_outcome_observations": str(HIST_OBS),
        "historical_outcomes": str(HIST),
        "calibration_profiles": str(CALIB),
    }

    result = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "purpose": "Evidence audit for Observer -> Paper -> Historical -> Calibration lifecycle",
        "files": files,
        "summary": dict(c),
        "anomaly_count": len(anomalies),
        "anomalies": anomalies,
        "rows": report_rows,
        "decision": "Patch yok. This report is evidence only. Fixes must target proven anomalies."
    }

    OUT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    md = []
    md.append("# Lifecycle Evidence Audit Report\n")
    md.append(f"Checked at UTC: {result['checked_at_utc']}\n")
    md.append("## Summary\n")
    for k, v in dict(c).items():
        md.append(f"- {k}: {v}")
    md.append(f"- anomaly_count: {len(anomalies)}\n")
    md.append("## Critical anomalies\n")
    for a in anomalies[:100]:
        md.append(f"- {a}")
    md.append("\n## Per setup table\n")
    md.append("| setup_id | observer | qualified | paper_open | paper_closed | historical | anomaly |")
    md.append("|---|---:|---:|---:|---:|---:|---|")
    for r in report_rows:
        anomaly_flags = []
        if r["paper_open_without_observer"]:
            anomaly_flags.append("open_without_observer")
        if r["paper_open_without_qualified"]:
            anomaly_flags.append("open_without_qualified")
        if r["paper_open_after_terminal_observer"]:
            anomaly_flags.append("open_after_terminal_observer")
        if r["closed_without_historical"]:
            anomaly_flags.append("closed_without_historical")
        md.append(
            f"| {r['setup_id']} | {r['observer_state']} | {r['qualified']} | "
            f"{r['paper_opened']} | {r['paper_closed']} | "
            f"{r['historical_observation_written'] or r['historical_outcome_written']} | "
            f"{', '.join(anomaly_flags)} |"
        )

    OUT_MD.write_text("\n".join(md), encoding="utf-8")

    print(json.dumps({
        "summary": dict(c),
        "anomaly_count": len(anomalies),
        "json_report": str(OUT_JSON),
        "md_report": str(OUT_MD)
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
