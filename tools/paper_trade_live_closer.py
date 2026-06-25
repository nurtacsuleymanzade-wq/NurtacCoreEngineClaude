#!/usr/bin/env python3
import json, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

PAPER = DATA / "paper_trades.jsonl"
PRICE_FILES = [
    DATA / "combined_1s_dna_btcusdt.jsonl",
    DATA / "one_second_combined_dna.jsonl",
]
REPORT = DATA / "paper_live_closer_report.json"

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

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

def get_ts(row):
    for k in ("window_start_ts", "ts", "T", "E", "open_time"):
        if row.get(k) is not None:
            try:
                return int(row.get(k))
            except Exception:
                pass
    return None

def get_price_row(row):
    close = fnum(row.get("close") or row.get("price") or row.get("last_price"))
    high = fnum(row.get("high")) or close
    low = fnum(row.get("low")) or close
    return high, low, close

def latest_price_rows():
    for pf in PRICE_FILES:
        rows = read_jsonl_tail(pf, 8000)
        if rows:
            return pf, rows
    return None, []

def main():
    paper_rows = read_jsonl_tail(PAPER, 20000)
    closed_ids = set()
    open_rows = {}

    for r in paper_rows:
        sid = r.get("source_setup_id") or r.get("setup_id")
        if not sid:
            continue
        if r.get("status") == "closed" or r.get("record_type") == "paper_trade_closed" or r.get("outcome") is not None:
            closed_ids.add(sid)
        elif sid not in closed_ids:
            open_rows[sid] = r

    price_file, prices = latest_price_rows()
    written = 0
    checked = 0

    with PAPER.open("a", encoding="utf-8") as f:
        for sid, p in list(open_rows.items()):
            if sid in closed_ids:
                continue

            direction = p.get("direction")
            entry = fnum(p.get("entry_price"))
            sl = fnum(p.get("sl_price"))
            tp = fnum(p.get("tp_price"))
            opened_ts = int(p.get("opened_ts") or 0)

            if not direction or entry is None or sl is None or tp is None:
                continue

            checked += 1
            mfe = 0.0
            mae = 0.0
            close_row = None
            outcome = None
            reason = None

            for row in prices:
                ts = get_ts(row)
                if opened_ts and ts and ts < opened_ts:
                    continue

                high, low, close = get_price_row(row)
                if high is None or low is None:
                    continue

                if direction == "long":
                    mfe = max(mfe, high - entry)
                    mae = min(mae, low - entry)
                    if low <= sl:
                        outcome, reason, close_row = "LOSS", "SL_HIT", row
                        break
                    if high >= tp:
                        outcome, reason, close_row = "WIN", "TP1_HIT", row
                        break
                elif direction == "short":
                    mfe = max(mfe, entry - low)
                    mae = min(mae, entry - high)
                    if high >= sl:
                        outcome, reason, close_row = "LOSS", "SL_HIT", row
                        break
                    if low <= tp:
                        outcome, reason, close_row = "WIN", "TP1_HIT", row
                        break

            if not close_row:
                continue

            risk = abs(entry - sl)
            pnl_r = None
            mfe_r = None
            mae_r = None
            if risk > 0:
                pnl_r = round(abs(tp-entry)/risk, 4) if outcome == "WIN" else -1.0
                mfe_r = round(mfe/risk, 4)
                mae_r = round(mae/risk, 4)

            close_ts = get_ts(close_row)
            duration = int((close_ts-opened_ts)/1000) if close_ts and opened_ts and close_ts > opened_ts else None

            out = {
                "engine": "paper_live_closer",
                "record_type": "paper_trade_closed",
                "source_setup_id": sid,
                "setup_id": sid,
                "direction": direction,
                "entry_price": entry,
                "sl_price": sl,
                "tp_price": tp,
                "opened_ts": opened_ts,
                "closed_ts": close_ts,
                "status": "closed",
                "outcome": outcome,
                "pnl_r": pnl_r,
                "mfe": mfe_r,
                "mae": mae_r,
                "duration_seconds": duration,
                "close_reason": reason,
                "created_at": int(time.time()),
            }
            f.write(json.dumps(out, ensure_ascii=False) + "\\n")
            written += 1

    report = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "price_file": str(price_file) if price_file else None,
        "open_checked": checked,
        "closed_written": written,
        "open_remaining": max(0, len(open_rows)-written),
    }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
