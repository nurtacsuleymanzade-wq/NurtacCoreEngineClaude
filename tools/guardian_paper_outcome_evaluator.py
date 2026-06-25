#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

BIRTH = DATA / "setup_birth_reports.jsonl"
PAPER = DATA / "paper_trades.jsonl"
REPORT = DATA / "guardian_paper_outcome_report.json"

PRICE_CANDIDATES = [
    DATA / "combined_1s_dna_btcusdt.jsonl",
    DATA / "one_second_combined_dna.jsonl",
    DATA / "rolling_3s_dna.jsonl",
    DATA / "rolling_5s_dna.jsonl",
]

def read_jsonl_tail(path, n=30000):
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

def get_price(obj):
    if isinstance(obj, dict):
        return fnum(obj.get("price"))
    return fnum(obj)

def get_ts(row):
    for k in ("window_start_ts", "ts", "T", "E", "open_time"):
        if row.get(k) is not None:
            try:
                return int(row.get(k))
            except Exception:
                pass
    return None

def candle_prices(row):
    close = fnum(row.get("close") or row.get("price") or row.get("last_price"))
    high = fnum(row.get("high")) or close
    low = fnum(row.get("low")) or close
    return high, low, close

def setup_from_birth(row):
    setup = row.get("raw_setup") or {}
    sid = row.get("setup_id") or setup.get("setup_id")
    direction = row.get("direction") or setup.get("direction")
    entry = get_price((row.get("entry") or setup.get("entry") or {}))
    sl = get_price(((row.get("risk") or {}).get("sl") or setup.get("sl") or {}))
    targets = row.get("targets") or {}
    tp1 = get_price(targets.get("tp1") or setup.get("tp1") or {})
    ts = setup.get("window_start_ts") or setup.get("qualification_ts") or setup.get("ts")
    try:
        ts = int(ts)
    except Exception:
        ts = None
    return {
        "setup_id": sid,
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp": tp1,
        "setup_ts": ts,
        "raw": setup or row,
    }

def existing_paper_ids():
    ids = set()
    if not PAPER.exists():
        return ids
    for row in read_jsonl_tail(PAPER, 20000):
        sid = row.get("source_setup_id") or row.get("setup_id")
        if sid:
            ids.add(sid)
    return ids

def evaluate(setup, candles):
    sid = setup["setup_id"]
    direction = setup["direction"]
    entry = setup["entry"]
    sl = setup["sl"]
    tp = setup["tp"]
    setup_ts = setup["setup_ts"]

    if not sid or direction not in ("long", "short") or not entry or not sl or not tp or not setup_ts:
        return None

    after = []
    for c in candles:
        ts = get_ts(c)
        if ts is not None and ts >= setup_ts:
            after.append(c)

    if not after:
        return None

    mfe = 0.0
    mae = 0.0
    close_row = None
    close_reason = None
    outcome = None

    for c in after:
        ts = get_ts(c)
        high, low, close = candle_prices(c)
        if high is None or low is None:
            continue

        if direction == "long":
            mfe = max(mfe, high - entry)
            mae = min(mae, low - entry)
            if low <= sl:
                close_row, close_reason, outcome = c, "SL_HIT", "LOSS"
                break
            if high >= tp:
                close_row, close_reason, outcome = c, "TP1_HIT", "WIN"
                break
        else:
            mfe = max(mfe, entry - low)
            mae = min(mae, entry - high)
            if high >= sl:
                close_row, close_reason, outcome = c, "SL_HIT", "LOSS"
                break
            if low <= tp:
                close_row, close_reason, outcome = c, "TP1_HIT", "WIN"
                break

    risk = abs(entry - sl)
    pnl_r = None
    mfe_r = None
    mae_r = None
    duration = None

    if risk and risk > 0:
        if outcome == "WIN":
            pnl_r = round(abs(tp - entry) / risk, 4)
        elif outcome == "LOSS":
            pnl_r = -1.0
        mfe_r = round(mfe / risk, 4)
        mae_r = round(mae / risk, 4)

    if close_row:
        close_ts = get_ts(close_row)
        duration = int((close_ts - setup_ts) / 1000) if close_ts and close_ts > setup_ts else None
    else:
        return {
            "engine": "guardian_paper_replay",
            "record_type": "paper_trade_open",
            "source_setup_id": sid,
            "setup_id": sid,
            "direction": direction,
            "entry_price": entry,
            "sl_price": sl,
            "tp_price": tp,
            "opened_ts": setup_ts,
            "status": "open",
            "outcome": None,
            "pnl_r": None,
            "mfe": mfe_r,
            "mae": mae_r,
            "duration_seconds": None,
            "close_reason": None,
            "created_at": int(time.time()),
        }

    return {
        "engine": "guardian_paper_replay",
        "record_type": "paper_trade_closed",
        "source_setup_id": sid,
        "setup_id": sid,
        "direction": direction,
        "entry_price": entry,
        "sl_price": sl,
        "tp_price": tp,
        "opened_ts": setup_ts,
        "closed_ts": get_ts(close_row),
        "status": "closed",
        "outcome": outcome,
        "pnl_r": pnl_r,
        "mfe": mfe_r,
        "mae": mae_r,
        "duration_seconds": duration,
        "close_reason": close_reason,
        "created_at": int(time.time()),
    }

def main():
    births = read_jsonl_tail(BIRTH, 5000)
    setups = [setup_from_birth(x) for x in births]

    price_file = next((p for p in PRICE_CANDIDATES if p.exists()), None)
    candles = read_jsonl_tail(price_file, 50000) if price_file else []

    done = existing_paper_ids()
    written = 0
    skipped = 0

    with PAPER.open("a", encoding="utf-8") as f:
        for s in setups:
            sid = s.get("setup_id")
            if not sid or sid in done:
                skipped += 1
                continue
            row = evaluate(s, candles)
            if not row:
                skipped += 1
                continue
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            done.add(sid)
            written += 1

    report = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "price_file": str(price_file) if price_file else None,
        "birth_setups_seen": len(setups),
        "paper_rows_written": written,
        "skipped": skipped,
        "note": "Guardian paper replay uses existing setup Entry/SL/TP. It does not create trade decisions."
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
