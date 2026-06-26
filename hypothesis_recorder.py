#!/usr/bin/env python3
"""
NurtacCoreEngineClaude - Shadow Hypothesis Recorder v1

READ-ONLY. TRADE AÇMAZ. MEVCUT SİSTEME ETKİ ETMEZ.

Her detector eventi için multi-horizon MFE/MAE outcome takibi.
1 event -> 5 flat satır (30s/60s/180s/300s/900s).

Çalıştırma: python3 hypothesis_recorder.py --mode live
Timer: Her 30 saniyede yeni detector eventleri tarar.
"""

import argparse
import json
import time
from collections import deque
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_FILE = DATA_DIR / "hypothesis_outcomes.jsonl"
CURSOR_FILE = DATA_DIR / "hypothesis_cursors.json"
PRICE_FILE = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"

HORIZONS = [
    (30, "30s"),
    (60, "1m"),
    (180, "3m"),
    (300, "5m"),
    (900, "15m"),
]

TP_PROXY_PCT = 0.15
SL_PROXY_PCT = 0.10

DETECTORS = {
    "initiative_flow": DATA_DIR / "labels_initiative_flow.jsonl",
    "absorption": DATA_DIR / "labels_absorption.jsonl",
    "sweep": DATA_DIR / "labels_sweep.jsonl",
    "exhaustion": DATA_DIR / "labels_exhaustion.jsonl",
    "iceberg": DATA_DIR / "labels_iceberg.jsonl",
    "trapped_trader": DATA_DIR / "labels_trapped_trader.jsonl",
}

BULLISH_LABELS = {
    "buy_initiative",
    "buy_absorbed",
    "upward_sweep",
    "buy_exhaustion",
    "bid_iceberg",
    "short_trapped",
}
BEARISH_LABELS = {
    "sell_initiative",
    "sell_absorbed",
    "downward_sweep",
    "sell_exhaustion",
    "ask_iceberg",
    "long_trapped",
}

MAX_PRICE_BUFFER = 1800
SCAN_LIMIT = 2000


def _append_jsonl(path: Path, record: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[HYP] Write error: {e}", flush=True)


def _load_cursors() -> dict:
    try:
        if CURSOR_FILE.exists():
            return json.loads(CURSOR_FILE.read_text())
    except Exception:
        pass
    return {}


def _save_cursors(cursors: dict) -> None:
    try:
        CURSOR_FILE.write_text(json.dumps(cursors, indent=2))
    except Exception as e:
        print(f"[HYP] Cursor save error: {e}", flush=True)


def _direction(label: str) -> str | None:
    if label in BULLISH_LABELS:
        return "long"
    if label in BEARISH_LABELS:
        return "short"
    return None


def _data_quality(detector: str, label: str, price: float) -> str:
    if detector in ("initiative_flow", "sweep"):
        return "A"
    if detector in ("absorption", "iceberg"):
        return "B"
    return "C"


class PriceBuffer:
    def __init__(self):
        self._buf: deque[tuple[int, float, float, float]] = deque(maxlen=MAX_PRICE_BUFFER)
        self._cursor: int = 0

    def update(self) -> None:
        if not PRICE_FILE.exists():
            return
        try:
            size = PRICE_FILE.stat().st_size
            if size <= self._cursor:
                return

            with open(PRICE_FILE, "r", encoding="utf-8", errors="replace") as f:
                f.seek(self._cursor)
                count = 0
                while count < SCAN_LIMIT:
                    line = f.readline()
                    if not line:
                        break
                    count += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        ts = int(d.get("ts") or d.get("window_start_ts") or 0)
                        cdna = d.get("candle_dna") or {}
                        def _px(v):
                            if isinstance(v, dict): return float(v.get("price") or 0)
                            return float(v or 0)
                        close = _px(cdna.get("close")) or _px(cdna.get("last_trade_price"))
                        high  = _px(cdna.get("high"))  or close
                        low   = _px(cdna.get("low"))   or close
                        if ts > 0 and close > 0:
                            self._buf.append((ts, close, high, low))
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                self._cursor = f.tell()
        except Exception as e:
            print(f"[HYP] PriceBuffer update error: {e}", flush=True)

    def current_price(self) -> float:
        return self._buf[-1][1] if self._buf else 0.0

    def prices_between(self, from_ts: int, to_ts: int) -> list[tuple[int, float, float, float]]:
        return [(ts, c, h, l) for ts, c, h, l in self._buf if from_ts <= ts <= to_ts]


class HypothesisTracker:
    def __init__(self):
        self._active: dict[str, dict] = {}
        self._seen: set[str] = set()
        self._total = 0
        self._emitted = 0

    def admit(
        self,
        detector: str,
        label: str,
        ts: int,
        entry_price: float,
        became_setup: bool,
        became_qualified: bool,
        became_trade: bool,
    ) -> None:
        direction = _direction(label)
        if direction is None:
            return

        hyp_id = f"{ts}_{detector}_{label}"
        if hyp_id in self._seen:
            return
        self._seen.add(hyp_id)

        self._active[hyp_id] = {
            "hypothesis_id": hyp_id,
            "source_event_ts": ts,
            "detector": detector,
            "label": label,
            "direction": direction,
            "entry_price": round(entry_price, 4),
            "became_setup": became_setup,
            "became_qualified": became_qualified,
            "became_trade": became_trade,
            "data_quality": _data_quality(detector, label, entry_price),
            "pending": list(HORIZONS),
            "max_price": entry_price,
            "min_price": entry_price,
            "time_to_mfe_s": 0,
            "time_to_mae_s": 0,
        }

        self._total += 1
        print(
            f"[HYP] +{detector:<20} {label:<20} {direction:5} @ {entry_price:.2f} [active={len(self._active)}]",
            flush=True,
        )

    def tick(self, price_buf: PriceBuffer) -> None:
        if not self._active:
            return

        now_ts = int(time.time() * 1000)
        completed_hyps = []

        for hyp_id, h in list(self._active.items()):
            entry_ts = h["source_event_ts"]
            entry_price = h["entry_price"]
            direction = h["direction"]

            age_s = (now_ts - entry_ts) / 1000
            prices = price_buf.prices_between(entry_ts, now_ts)

            for bar_ts, close, high, low in prices:
                bar_age_s = (bar_ts - entry_ts) / 1000
                if high > h["max_price"]:
                    h["max_price"] = high
                    h["time_to_mfe_s"] = round(bar_age_s)
                if low < h["min_price"]:
                    h["min_price"] = low
                    h["time_to_mae_s"] = round(bar_age_s)

            still_pending = []
            for horizon_s, horizon_label in h["pending"]:
                if age_s < horizon_s:
                    still_pending.append((horizon_s, horizon_label))
                    continue

                current = price_buf.current_price()
                if current <= 0:
                    still_pending.append((horizon_s, horizon_label))
                    continue

                if direction == "long":
                    close_move_pct = (current - entry_price) / entry_price * 100
                    mfe_pct = (h["max_price"] - entry_price) / entry_price * 100
                    mae_pct = (entry_price - h["min_price"]) / entry_price * 100
                    correct = current > entry_price
                    tp_hit = mfe_pct >= TP_PROXY_PCT
                    sl_hit = mae_pct >= SL_PROXY_PCT
                else:
                    close_move_pct = (entry_price - current) / entry_price * 100
                    mfe_pct = (entry_price - h["min_price"]) / entry_price * 100
                    mae_pct = (h["max_price"] - entry_price) / entry_price * 100
                    correct = current < entry_price
                    tp_hit = mfe_pct >= TP_PROXY_PCT
                    sl_hit = mae_pct >= SL_PROXY_PCT

                mfe_mae_ratio = round(mfe_pct / mae_pct, 3) if mae_pct > 0 else 99.0

                row = {
                    "hypothesis_id": hyp_id,
                    "source_event_ts": entry_ts,
                    "horizon_s": horizon_s,
                    "horizon_label": horizon_label,
                    "recorded_at_ts": now_ts,
                    "detector": h["detector"],
                    "label": h["label"],
                    "direction": direction,
                    "entry_price": entry_price,
                    "close_price": round(current, 4),
                    "close_move_pct": round(close_move_pct, 4),
                    "direction_correct": correct,
                    "max_favorable_excursion_pct": round(mfe_pct, 4),
                    "max_adverse_excursion_pct": round(mae_pct, 4),
                    "mfe_mae_ratio": mfe_mae_ratio,
                    "time_to_mfe_s": h["time_to_mfe_s"],
                    "time_to_mae_s": h["time_to_mae_s"],
                    "tp_proxy_hit": tp_hit,
                    "sl_proxy_hit": sl_hit,
                    "became_setup": h["became_setup"],
                    "became_qualified": h["became_qualified"],
                    "became_trade": h["became_trade"],
                    "data_quality": h["data_quality"],
                }

                _append_jsonl(OUTPUT_FILE, row)
                self._emitted += 1

                print(
                    f"[HYP] ✓ {h['detector']:<20} {horizon_label:>4} {'OK' if correct else 'NO':2} MFE={mfe_pct:+.2f}% MAE={mae_pct:.2f}% ratio={mfe_mae_ratio:.2f}",
                    flush=True,
                )

            h["pending"] = still_pending
            if not still_pending:
                completed_hyps.append(hyp_id)

        for hyp_id in completed_hyps:
            self._active.pop(hyp_id, None)

        cutoff_ts = now_ts - 1_200_000
        stale = [k for k, v in self._active.items() if v["source_event_ts"] < cutoff_ts]
        for k in stale:
            self._active.pop(k, None)

    def stats(self) -> dict:
        return {
            "total_admitted": self._total,
            "total_emitted": self._emitted,
            "active_pending": len(self._active),
        }


def _scan_new_detector_events(
    detector_name: str,
    path: Path,
    cursor: int,
    setups_index: set[int],
    qualified_index: set[int],
    trades_index: set[int],
) -> tuple[list[dict], int]:
    if not path.exists():
        return [], cursor

    try:
        size = path.stat().st_size
        if size <= cursor:
            return [], cursor

        events = []
        new_cursor = cursor

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(cursor)
            count = 0
            while count < SCAN_LIMIT:
                line = f.readline()
                if not line:
                    break
                count += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    ts = int(d.get("ts") or d.get("window_start_ts") or 0)
                    label     = str(d.get("label", "") or "")
                    direction = str(d.get("direction", "") or "")
                    # label alani "initiative_candidate" gibi generic
                    # direction alani "sell_initiative" gibi BULLISH/BEARISH'te olan deger
                    # Hipotez icin direction'i kullan, label fallback
                    track_label = direction if direction and direction != "None" else label
                    if ts > 0 and track_label and track_label not in ("none", "", "None"):
                        events.append(
                            {
                                "ts":               ts,
                                "label":            track_label,
                                "orig_label":       label,
                                "became_setup":     ts in setups_index,
                                "became_qualified": ts in qualified_index,
                                "became_trade":     ts in trades_index,
                            }
                        )
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
            new_cursor = f.tell()

        return events, new_cursor
    except Exception as e:
        print(f"[HYP] Scan error {detector_name}: {e}", flush=True)
        return [], cursor


def _build_ts_index(path: Path, tail_n: int = 500) -> set[int]:
    import subprocess

    result = set()
    try:
        raw = subprocess.getoutput(f"tail -{tail_n} {path} 2>/dev/null")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                ts = int(d.get("window_start_ts") or d.get("ts") or d.get("open_ts") or 0)
                if ts > 0:
                    result.add(ts)
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
    except Exception:
        pass
    return result


def run_once(tracker: HypothesisTracker, price_buf: PriceBuffer, cursors: dict) -> None:
    price_buf.update()
    current_price = price_buf.current_price()
    if current_price <= 0:
        print("[HYP] Fiyat verisi yok - bekleniyor", flush=True)
        return

    setups_idx = _build_ts_index(DATA_DIR / "setups.jsonl", 500)
    qualified_idx = _build_ts_index(DATA_DIR / "qualified_setups.jsonl", 200)
    trades_idx = _build_ts_index(DATA_DIR / "paper_trades_open.json", 50)

    for det_name, det_path in DETECTORS.items():
        cur = int(cursors.get(det_name, 0) or 0)
        events, new_cur = _scan_new_detector_events(
            det_name, det_path, cur, setups_idx, qualified_idx, trades_idx
        )
        cursors[det_name] = new_cur

        for ev in events:
            tracker.admit(
                detector=det_name,
                label=ev["label"],
                ts=ev["ts"],
                entry_price=current_price,
                became_setup=ev["became_setup"],
                became_qualified=ev["became_qualified"],
                became_trade=ev["became_trade"],
                record=ev.get("record"),
            )

    tracker.tick(price_buf)
    stats = tracker.stats()
    print(
        f"[HYP] tick | price={current_price:.2f} active={stats['active_pending']} total={stats['total_admitted']} emitted={stats['total_emitted']}",
        flush=True,
    )


def run_live(interval_s: int = 30) -> None:
    print("[HYP] Shadow Hypothesis Recorder v1 başlatıldı", flush=True)
    print(f"[HYP] Horizons: {[h[1] for h in HORIZONS]}", flush=True)
    print(f"[HYP] Tarama aralığı: {interval_s}s", flush=True)
    print(f"[HYP] Çıktı: {OUTPUT_FILE}", flush=True)
    print("[HYP] READ-ONLY. TRADE AÇMAZ. MEVCUT SİSTEME ETKİ ETMEZ.", flush=True)

    tracker = HypothesisTracker()
    price_buf = PriceBuffer()
    cursors = _load_cursors()

    while True:
        try:
            run_once(tracker, price_buf, cursors)
            _save_cursors(cursors)
        except Exception as e:
            print(f"[HYP] Loop error: {e}", flush=True)

        time.sleep(interval_s)


def run_test() -> None:
    print("[HYP] Test modu", flush=True)

    tracker = HypothesisTracker()
    price_buf = PriceBuffer()

    fake_ts = int(time.time() * 1000) - 1000
    price_buf._buf.append((fake_ts, 62000.0, 62100.0, 61900.0))

    old_ts = int(time.time() * 1000) - 930_000
    price_buf._buf.append((old_ts, 62000.0, 62200.0, 61800.0))

    tracker.admit(
        detector="initiative_flow",
        label="sell_initiative",
        ts=old_ts,
        entry_price=62000.0,
        became_setup=False,
        became_qualified=False,
        became_trade=False,
    )

    for i in range(0, 930, 30):
        t = old_ts + i * 1000
        price_buf._buf.append((t, 62000.0 - i * 0.5, 62000.0 - i * 0.4, 62000.0 - i * 0.6))

    tracker.tick(price_buf)

    stats = tracker.stats()
    print(f"[HYP] Test stats: {stats}", flush=True)
    if OUTPUT_FILE.exists():
        lines = OUTPUT_FILE.read_text().strip().splitlines()
        print(f"[HYP] Output rows: {len(lines)}", flush=True)
        if lines:
            last = json.loads(lines[-1])
            print(
                f"[HYP] Last row: horizon={last.get('horizon_label')} correct={last.get('direction_correct')} mfe={last.get('max_favorable_excursion_pct')} mae={last.get('max_adverse_excursion_pct')}",
                flush=True,
            )
    print("[HYP] Test TAMAMLANDI", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shadow Hypothesis Recorder v1")
    parser.add_argument("--mode", choices=["live", "test"], default="live")
    parser.add_argument("--interval", type=int, default=30, help="Tarama aralığı saniye (default: 30)")
    args = parser.parse_args()

    if args.mode == "test":
        run_test()
    else:
        run_live(interval_s=args.interval)
