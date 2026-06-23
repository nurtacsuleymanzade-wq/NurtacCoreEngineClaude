#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

PAPER = DATA / "paper_trades.jsonl"
CLOSED_JSONL = DATA / "paper_closed.jsonl"
CLOSED_JSON = DATA / "paper_closed.json"
REPORT = DATA / "paper_close_engine_report.json"

PRICE_FILES = [
    DATA / "rolling_3s_dna.jsonl",
    DATA / "rolling_5s_dna.jsonl",
    DATA / "rolling_15s_dna.jsonl",
    DATA / "aligned_1m_candle_dna.jsonl",
    DATA / "combined_1s_dna_btcusdt.jsonl",
]

def read_jsonl_tail(path, n=5000):
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

def append_jsonl(path, row):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def fnum(v):
    try:
        if isinstance(v, dict):
            if "price" in v:
                return float(v["price"])
            return None
        return float(v)
    except Exception:
        return None

def get_nested(row, keys):
    for k in keys:
        if row.get(k) is not None:
            return row.get(k)

    for nested in ("ohlc", "candle", "summary", "price_dna", "trade_dna", "window", "bar"):
        obj = row.get(nested)
        if isinstance(obj, dict):
            for k in keys:
                if obj.get(k) is not None:
                    return obj.get(k)

    return None

def get_ts(row):
    v = get_nested(row, ["window_start_ts", "ts", "T", "E", "open_time", "time"])
    try:
        return int(v)
    except Exception:
        return None

def extract_hlc(row):
    high = fnum(get_nested(row, ["high", "h"]))
    low = fnum(get_nested(row, ["low", "l"]))
    close = fnum(get_nested(row, ["close", "c", "price", "last_price", "last_trade_price"]))

    if high is None or low is None:
        return None, None, close, "missing_high_low"

    return high, low, close, "verified_high_low"

def load_price_rows():
    candidates = []

    for pf in PRICE_FILES:
        rows = read_jsonl_tail(pf, 120000)
        usable = []
        missing_hl = 0

        for r in rows:
            ts = get_ts(r)
            high, low, close, quality = extract_hlc(r)

            if ts is None:
                continue

            if quality != "verified_high_low":
                missing_hl += 1
                continue

            usable.append((ts, high, low, close, quality, str(pf), r))

        if usable:
            usable.sort(key=lambda x: x[0])
            candidates.append({
                "file": str(pf),
                "usable_rows": len(usable),
                "missing_high_low_rows": missing_hl,
                "rows": usable,
            })

    if not candidates:
        return None, [], []

    # En yüksek çözünürlük sırası PRICE_FILES sırasıdır; ilk usable kaynak seçilir.
    chosen = candidates[0]
    source_report = [
        {k: v for k, v in c.items() if k != "rows"}
        for c in candidates
    ]
    return chosen["file"], chosen["rows"], source_report

def sid_of(row):
    return row.get("source_setup_id") or row.get("setup_id") or row.get("qualified_setup_id")

def is_closed(row):
    return (
        row.get("status") == "closed"
        or row.get("record_type") == "paper_trade_closed"
        or row.get("outcome") is not None
        or row.get("closed_ts") is not None
        or row.get("close_reason") is not None
    )

def is_valid_closed(row):
    return is_closed(row) and ((row.get("hit_candle") or {}).get("price_source_quality") == "verified_high_low")

def is_open(row):
    return (
        row.get("status") == "open"
        or row.get("record_type") == "paper_trade_open"
    ) and row.get("outcome") is None

def already_valid_closed_ids():
    ids = set()
    for r in read_jsonl_tail(PAPER, 100000) + read_jsonl_tail(CLOSED_JSONL, 100000):
        sid = sid_of(r)
        if sid and is_valid_closed(r):
            ids.add(sid)
    return ids

def open_paper_rows():
    valid_closed = already_valid_closed_ids()
    opens = {}

    for r in read_jsonl_tail(PAPER, 100000):
        sid = sid_of(r)
        if not sid or sid in valid_closed:
            continue

        # ignore old invalid close_only closed records; they are not valid source of truth
        if is_open(r):
            opens[sid] = r

    return opens

def evaluate(open_row, price_rows):
    sid = sid_of(open_row)
    direction = str(open_row.get("direction") or "").lower()
    entry = fnum(open_row.get("entry_price") or open_row.get("entry"))
    sl = fnum(open_row.get("sl_price") or open_row.get("sl"))
    tp = fnum(open_row.get("tp_price") or open_row.get("tp"))

    try:
        opened_ts = int(open_row.get("opened_ts") or open_row.get("setup_ts") or 0)
    except Exception:
        opened_ts = 0

    if direction not in ("long", "short") or entry is None or sl is None or tp is None or not opened_ts:
        return None

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    first_seen_ts = None
    last_seen_ts = None
    mfe_abs = 0.0
    mae_abs = 0.0

    for ts, high, low, close, quality, source_file, raw in price_rows:
        if ts < opened_ts:
            continue

        if first_seen_ts is None:
            first_seen_ts = ts
        last_seen_ts = ts

        if direction == "long":
            mfe_abs = max(mfe_abs, high - entry)
            mae_abs = min(mae_abs, low - entry)
            sl_hit = low <= sl
            tp_hit = high >= tp
        else:
            mfe_abs = max(mfe_abs, entry - low)
            mae_abs = min(mae_abs, entry - high)
            sl_hit = high >= sl
            tp_hit = low <= tp

        if sl_hit and tp_hit:
            outcome = "LOSS"
            reason = "SL_AND_TP_SAME_CANDLE_SL_FIRST"
        elif sl_hit:
            outcome = "LOSS"
            reason = "SL_HIT"
        elif tp_hit:
            outcome = "WIN"
            reason = "TP1_HIT"
        else:
            continue

        pnl_r = round(abs(tp - entry) / risk, 4) if outcome == "WIN" else -1.0
        duration = int((ts - opened_ts) / 1000) if ts >= opened_ts else None

        return {
            "engine": "paper_close_engine",
            "record_type": "paper_trade_closed",
            "source_setup_id": sid,
            "setup_id": sid,
            "status": "closed",
            "direction": direction,
            "entry_price": entry,
            "sl_price": sl,
            "tp_price": tp,
            "opened_ts": opened_ts,
            "closed_ts": ts,
            "outcome": outcome,
            "pnl_r": pnl_r,
            "mfe": round(mfe_abs / risk, 4),
            "mae": round(mae_abs / risk, 4),
            "duration_seconds": duration,
            "close_reason": reason,
            "hit_candle": {
                "ts": ts,
                "high": high,
                "low": low,
                "close": close,
                "price_source_quality": "verified_high_low",
                "source_file": source_file,
            },
            "created_at": int(time.time()),
        }

    return {
        "record_type": "paper_close_pending",
        "source_setup_id": sid,
        "setup_id": sid,
        "status": "open",
        "direction": direction,
        "entry_price": entry,
        "sl_price": sl,
        "tp_price": tp,
        "opened_ts": opened_ts,
        "first_price_ts_seen": first_seen_ts,
        "last_price_ts_seen": last_seen_ts,
        "mfe": round(mfe_abs / risk, 4),
        "mae": round(mae_abs / risk, 4),
        "reason": "NO_VERIFIED_HIGH_LOW_TP_OR_SL_HIT_IN_AVAILABLE_PRICE_WINDOW",
        "created_at": int(time.time()),
    }

def main():
    price_file, price_rows, source_report = load_price_rows()
    opens = open_paper_rows()

    closed = []
    pending = []

    if not price_rows:
        summary = {
            "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "status": "NO_VERIFIED_HIGH_LOW_SOURCE",
            "price_sources": source_report,
            "open_checked": len(opens),
            "closed_written": 0,
            "pending_count": len(opens),
        }
        REPORT.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        CLOSED_JSON.write_text(json.dumps({"latest_closed": [], "pending": [], "summary": summary}, indent=2, ensure_ascii=False))
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    for sid, row in opens.items():
        res = evaluate(row, price_rows)
        if not res:
            continue

        if res.get("record_type") == "paper_trade_closed":
            append_jsonl(CLOSED_JSONL, res)
            append_jsonl(PAPER, res)
            closed.append(res)
        else:
            pending.append(res)

    summary = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "status": "OK_VERIFIED_HIGH_LOW_ONLY",
        "price_file": price_file,
        "price_rows": len(price_rows),
        "price_sources": source_report,
        "open_checked": len(opens),
        "closed_written": len(closed),
        "pending_count": len(pending),
        "closed_sample": closed[:10],
        "pending_sample": pending[:10],
        "guardrail": "close_only cannot create WIN/LOSS",
    }

    CLOSED_JSON.write_text(json.dumps({
        "latest_closed": closed[-100:],
        "pending": pending[-100:],
        "summary": summary,
    }, indent=2, ensure_ascii=False))

    REPORT.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
