#!/usr/bin/env python3
import os, json, time, urllib.request, urllib.parse, subprocess
from pathlib import Path

DATA = Path("data")

STATE_FILE = DATA / "setup_guardian_state.json"

SETUPS = DATA / "setups.jsonl"
OBS = DATA / "observations.jsonl"
QUAL = DATA / "qualified_setups.jsonl"
PAPER = DATA / "paper_trades.jsonl"

BIRTH_REPORT_FILE = DATA / "setup_birth_reports.jsonl"
TEXT_REPORT_FILE = DATA / "setup_guardian_reports.jsonl"
LIFECYCLE_FILE = DATA / "setup_lifecycle.jsonl"
LIFECYCLE_EVENTS_FILE = DATA / "setup_lifecycle_events.jsonl"
TIMELINE_FILE = DATA / "setup_timeline.jsonl"
HEALTH_FILE = DATA / "setup_guardian_health.json"

MAX_TRACKED = 500
INTERVAL = 10

def now_ts():
    return int(time.time())

def load_state():
    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            st = {}
    else:
        st = {}

    st.setdefault("offsets", {})
    st.setdefault("tracked", {})
    st.setdefault("sent", {})
    st.setdefault("lifecycle", {})
    st.setdefault("rows_processed", {})
    st.setdefault("started_at", now_ts())
    return st

def save_state(st):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STATE_FILE)

def file_key(path):
    return str(path)

def read_new_jsonl(path, st, source_name):
    if not path.exists():
        return []

    key = file_key(path)
    offsets = st.setdefault("offsets", {})
    cur = offsets.setdefault(key, {"inode": None, "offset": 0})

    stat = path.stat()
    if cur.get("inode") != stat.st_ino or int(cur.get("offset") or 0) > stat.st_size:
        cur["inode"] = stat.st_ino
        cur["offset"] = 0

    rows = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        f.seek(int(cur.get("offset") or 0))
        for line in f:
            try:
                row = json.loads(line)
                rows.append(row)
            except Exception:
                pass
        cur["offset"] = f.tell()
        cur["updated_at"] = time.time()

    st.setdefault("rows_processed", {})
    st["rows_processed"][source_name] = st["rows_processed"].get(source_name, 0) + len(rows)
    return rows

def append_jsonl(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def fnum(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def get_price(obj):
    if isinstance(obj, dict):
        return obj.get("price")
    return None

def top_contributors(score_breakdown, direction):
    suffix = "_long" if direction == "long" else "_short"
    items = []
    for k, v in (score_breakdown or {}).items():
        if k.endswith(suffix):
            val = fnum(v)
            if val > 0:
                items.append({"layer": k.replace(suffix, ""), "contribution": val})
    items.sort(key=lambda x: x["contribution"], reverse=True)
    return items[:8]

def true_triggers(trigger_conditions):
    return [k for k, v in (trigger_conditions or {}).items() if v is True]

def make_birth_report(setup):
    direction = setup.get("direction")
    entry = setup.get("entry") or {}
    sl = setup.get("sl") or {}
    tp1 = setup.get("tp1") or {}
    tp2 = setup.get("tp2") or {}
    tp3 = setup.get("tp3") or {}

    return {
        "report_type": "setup_birth_report",
        "setup_id": setup.get("setup_id"),
        "direction": direction,
        "setup_type": setup.get("setup_type"),
        "entry": entry,
        "risk": {"sl": sl},
        "targets": {"tp1": tp1, "tp2": tp2, "tp3": tp3},
        "scores": setup.get("scores") or {},
        "trigger_conditions": setup.get("trigger_conditions") or {},
        "score_breakdown": setup.get("score_breakdown") or {},
        "context": setup.get("context") or {},
        "top_contributors": top_contributors(setup.get("score_breakdown") or {}, direction),
        "true_triggers": true_triggers(setup.get("trigger_conditions") or {}),
        "telegram_sent": False,
        "created_at": now_ts(),
        "raw_setup": setup,
    }

def base_lifecycle(setup, birth_report):
    sid = setup.get("setup_id")
    return {
        "setup_id": sid,
        "current_state": "opened",
        "direction": setup.get("direction"),
        "setup_type": setup.get("setup_type"),
        "opened_at": now_ts(),
        "last_update_at": now_ts(),
        "birth": birth_report,
        "observer": {
            "last_event": None,
            "events_count": 0,
            "events": [],
        },
        "qualification": None,
        "paper": {
            "opened": None,
            "closed": None,
            "events": [],
        },
        "result": None,
        "timeline": [],
    }

def setup_id_from_row(row):
    return (
        row.get("setup_id")
        or row.get("source_setup_id")
        or row.get("qualified_setup_id")
    )

def format_birth_report(report):
    ctx = report["context"]
    scores = report["scores"]
    entry = report["entry"]
    sl = report["risk"].get("sl") or {}
    targets = report["targets"]

    triggers = "\n".join([f"✅ {x}" for x in report["true_triggers"]]) or "trigger yok"
    contrib = "\n".join([f"• {x['layer']}: +{x['contribution']}" for x in report["top_contributors"]]) or "• katkı bulunamadı"
    direction = (report.get("direction") or "?").upper()

    return f"""🟢 SETUP OLUŞTU

ID: {report.get("setup_id")}
Yön: {direction}
Tip: {report.get("setup_type")}

Entry: {entry.get("price")}
SL: {sl.get("price")}
TP1/TP2/TP3: {(targets.get("tp1") or {}).get("price")} / {(targets.get("tp2") or {}).get("price")} / {(targets.get("tp3") or {}).get("price")}

Skor:
Long: {scores.get("long_score")}
Short: {scores.get("short_score")}
Gap: {scores.get("score_gap")}

Neden oluştu?
{triggers}

Katman katkıları:
{contrib}

Context:
Gate: {ctx.get("gate_grade")}
1S trend: {ctx.get("trend_1s")}
1M trend: {ctx.get("trend_1m")}
5M trend: {ctx.get("trend_5m")}
Micro BOS: {ctx.get("micro_bos")}
Macro BOS: {ctx.get("macro_bos")}
Active OB: {ctx.get("active_ob_count")}
Active FVG: {ctx.get("active_fvg_count")}
"""

def format_observer_event(sid, event, new_state):
    ev = event.get("event_type")
    return f"""👁️ SETUP UPDATE

ID: {sid}
Yeni durum: {new_state}
Son event: {ev}
State: {event.get("state_before")} → {event.get("state_after")}
Price: {event.get("current_price")}
Details: {event.get("details")}
"""

def format_terminal_event(sid, event, new_state):
    ev = event.get("event_type")
    title = "⚠️ SETUP INVALIDATED" if new_state == "invalidated" else "⌛ SETUP EXPIRED" if new_state == "expired" else "⚠️ SETUP WAITING TIMEOUT"
    return f"""{title}

ID: {sid}
Event: {ev}
Price: {event.get("current_price")}
Details: {event.get("details")}
"""

def format_qualified(sid, q):
    entry = q.get("entry") or {}
    sl = q.get("sl") or {}
    tp1 = q.get("tp1") or {}
    tp2 = q.get("tp2") or {}
    tp3 = q.get("tp3") or {}
    return f"""✅ SETUP QUALIFIED

ID: {sid}
Yön: {q.get("direction")}
Qualification ts: {q.get("qualification_ts")}
Time to qualify: {q.get("time_to_qualify_seconds")}s

Entry: {get_price(entry)}
SL: {get_price(sl)}
TP1/TP2/TP3: {get_price(tp1)} / {get_price(tp2)} / {get_price(tp3)}
"""

def format_paper(sid, p, state):
    if state == "paper_closed":
        return f"""📊 PAPER TRADE CLOSED

Setup ID: {sid}
Outcome: {p.get("outcome")}
PnL R: {p.get("pnl_r")}
MFE: {p.get("mfe")}
MAE: {p.get("mae")}
Duration: {p.get("duration_seconds")}
Close reason: {p.get("close_reason")}
"""
    return f"""📄 PAPER TRADE EVENT

Setup ID: {sid}
State: {state}
Direction: {p.get("direction")}
Entry: {p.get("entry_price") or get_price(p.get("entry") or {})}
SL: {p.get("sl_price") or get_price(p.get("sl") or {})}
TP: {p.get("tp_price") or get_price(p.get("tp") or {})}
"""

def send_telegram(text):
    """
    Guardian Telegram'a direkt mesaj atmaz.
    Tüm Telegram trafiği tools/curated_telegram_reporter.py tarafından filtrelenir.
    """
    print("[GUARDIAN TG SUPPRESSED]\n" + str(text)[:800], flush=True)
    return False

def log_text(kind, setup_id, text):
    append_jsonl(TEXT_REPORT_FILE, {
        "ts": now_ts(),
        "kind": kind,
        "setup_id": setup_id,
        "text": text,
    })

def emit_lifecycle_event(st, sid, previous_state, new_state, event_type, source_file, details, text=None):
    key = f"{sid}:{event_type}:{new_state}"
    if st["sent"].get(key):
        return

    telegram_sent = send_telegram(text) if text else False

    row = {
        "report_type": "setup_lifecycle_event",
        "setup_id": sid,
        "previous_state": previous_state,
        "new_state": new_state,
        "event_type": event_type,
        "event_ts": now_ts(),
        "source_file": source_file,
        "details": details,
        "telegram_sent": telegram_sent,
    }

    append_jsonl(LIFECYCLE_EVENTS_FILE, row)
    append_jsonl(TIMELINE_FILE, {
        "ts": row["event_ts"],
        "setup_id": sid,
        "event": event_type,
        "previous_state": previous_state,
        "new_state": new_state,
        "source": source_file,
        "details": details,
    })

    if text:
        log_text(event_type.lower(), sid, text)

    st["sent"][key] = True

def update_lifecycle_cache(st, sid, lifecycle):
    lifecycle["last_update_at"] = now_ts()
    st["lifecycle"][sid] = lifecycle
    append_jsonl(LIFECYCLE_FILE, lifecycle)

def ensure_lifecycle(st, sid):
    return st["lifecycle"].get(sid)

def handle_setup(st, setup):
    sid = setup.get("setup_id")
    if not sid:
        return

    key = f"{sid}:birth"
    if st["sent"].get(key):
        return

    birth = make_birth_report(setup)
    lifecycle = base_lifecycle(setup, birth)

    st["tracked"][sid] = {
        "setup_id": sid,
        "current_state": "opened",
        "last_event_ts": now_ts(),
        "last_reported_state": "opened",
    }

    if len(st["tracked"]) > MAX_TRACKED:
        oldest = list(st["tracked"].keys())[0]
        st["tracked"].pop(oldest, None)

    text = format_birth_report(birth)
    sent = send_telegram(text)
    birth["telegram_sent"] = bool(sent)

    append_jsonl(BIRTH_REPORT_FILE, birth)
    log_text("setup_birth_report", sid, text)

    emit_lifecycle_event(
        st, sid, "unknown", "opened", "BIRTH",
        "setups.jsonl", birth, None
    )

    st["sent"][key] = True
    update_lifecycle_cache(st, sid, lifecycle)

def handle_observation(st, obs):
    sid = obs.get("source_setup_id") or obs.get("setup_id")
    if not sid:
        return

    lifecycle = ensure_lifecycle(st, sid)
    if not lifecycle:
        return

    ev = obs.get("event_type") or "OBSERVATION"
    previous = lifecycle.get("current_state", "unknown")

    if ev == "INVALIDATED":
        new_state = "invalidated"
        text = format_terminal_event(sid, obs, new_state)
    elif ev == "EXPIRED":
        new_state = "expired"
        text = format_terminal_event(sid, obs, new_state)
    elif ev == "WAITING_TIMEOUT":
        new_state = "waiting_timeout"
        text = format_terminal_event(sid, obs, new_state)
    else:
        new_state = "observing"
        text = format_observer_event(sid, obs, new_state)

    lifecycle["current_state"] = new_state
    lifecycle["observer"]["last_event"] = obs
    lifecycle["observer"]["events_count"] = int(lifecycle["observer"].get("events_count") or 0) + 1
    lifecycle["observer"]["events"] = (lifecycle["observer"].get("events") or [])[-20:] + [obs]
    lifecycle["timeline"].append({"ts": now_ts(), "event": ev, "state": new_state, "source": "observations.jsonl"})

    emit_lifecycle_event(st, sid, previous, new_state, ev, "observations.jsonl", obs, text)
    update_lifecycle_cache(st, sid, lifecycle)

def handle_qualified(st, q):
    sid = q.get("source_setup_id") or q.get("setup_id")
    if not sid:
        return

    lifecycle = ensure_lifecycle(st, sid)
    if not lifecycle:
        return

    previous = lifecycle.get("current_state", "unknown")
    new_state = "qualified"
    lifecycle["current_state"] = new_state
    lifecycle["qualification"] = q
    lifecycle["timeline"].append({"ts": now_ts(), "event": "QUALIFIED", "state": new_state, "source": "qualified_setups.jsonl"})

    text = format_qualified(sid, q)
    emit_lifecycle_event(st, sid, previous, new_state, "QUALIFIED", "qualified_setups.jsonl", q, text)
    update_lifecycle_cache(st, sid, lifecycle)

def handle_paper(st, p):
    sid = p.get("source_setup_id") or p.get("setup_id")
    if not sid:
        return

    lifecycle = ensure_lifecycle(st, sid)
    if not lifecycle:
        return

    previous = lifecycle.get("current_state", "unknown")

    outcome = p.get("outcome")
    close_reason = p.get("close_reason")
    if outcome is not None or close_reason is not None or p.get("closed_ts") is not None:
        new_state = "paper_closed"
        lifecycle["paper"]["closed"] = p
        lifecycle["result"] = p
    else:
        new_state = "paper_open"
        lifecycle["paper"]["opened"] = p

    lifecycle["current_state"] = new_state
    lifecycle["paper"]["events"] = (lifecycle["paper"].get("events") or [])[-20:] + [p]
    lifecycle["timeline"].append({"ts": now_ts(), "event": new_state.upper(), "state": new_state, "source": "paper_trades.jsonl"})

    text = format_paper(sid, p, new_state)
    emit_lifecycle_event(st, sid, previous, new_state, new_state.upper(), "paper_trades.jsonl", p, text)
    update_lifecycle_cache(st, sid, lifecycle)

def write_health(st):
    payload = {
        "status": "alive",
        "ts": now_ts(),
        "tracked_count": len(st.get("tracked", {})),
        "lifecycle_count": len(st.get("lifecycle", {})),
        "rows_processed": st.get("rows_processed", {}),
        "offsets": st.get("offsets", {}),
        "outputs": {
            "birth_reports": str(BIRTH_REPORT_FILE),
            "lifecycle": str(LIFECYCLE_FILE),
            "lifecycle_events": str(LIFECYCLE_EVENTS_FILE),
            "timeline": str(TIMELINE_FILE),
            "text_reports": str(TEXT_REPORT_FILE),
        },
        "memory_guard": "systemd MemoryMax=150M",
    }
    tmp = HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(HEALTH_FILE)

def main():
    DATA.mkdir(exist_ok=True)
    st = load_state()
    print("[SETUP GUARDIAN LIFECYCLE] started", flush=True)

    while True:
        try:
            for setup in read_new_jsonl(SETUPS, st, "setups"):
                handle_setup(st, setup)

            for obs in read_new_jsonl(OBS, st, "observations"):
                handle_observation(st, obs)

            for q in read_new_jsonl(QUAL, st, "qualified"):
                handle_qualified(st, q)

            for p in read_new_jsonl(PAPER, st, "paper"):
                handle_paper(st, p)

            write_health(st)
            save_state(st)

        except Exception as e:
            print("[SETUP GUARDIAN LIFECYCLE ERROR]", repr(e), flush=True)

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()

# ==============================
# 4/4 SETUP INTELLIGENCE HELPERS
# ==============================

