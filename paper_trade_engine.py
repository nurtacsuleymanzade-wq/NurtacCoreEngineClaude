"""
NurtacCoreEngineClaude — Layer-12: Paper Trade Lifecycle Engine

Reads qualified setups from Layer-10, simulates paper trades against
live price feed, tracks milestones (TP1/TP2/TP3/SL), computes PnL.

NO real orders. NO real money. Simulation only.
NO Binance API. Only reads existing JSONL files.

Outputs:
  data/paper_trades.jsonl          (append — closed trades)
  data/paper_trades_open.json      (overwrite — current open trades)
  data/paper_trade_summary.json    (overwrite — cumulative stats)
  data/paper_trade_health.json     (overwrite — health snapshot)
"""

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Constants ─────────────────────────────────────────────────────────────────
SYMBOL    = "BTCUSDT"
DATA_DIR  = Path("data")
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
FULL_PRINT = os.environ.get("FULL_PRINT", "false").lower() == "true"

MAX_OPEN_TRADES   = 3
HEALTH_INTERVAL_S = 30.0
OPEN_INTERVAL_S   = 10.0
SUMMARY_INTERVAL_S = 10.0
POLL_SLEEP        = 0.5
INITIAL_BALANCE_USD = 500.0

TIER_RISK_PCT = {
    "L1_LOW": 0.01,
    "L2_MEDIUM": 0.02,
    "L3_GOOD_A+": 0.03,
    "L4_PREMIUM": 0.05,
}

# ── Timeout bars per timeframe/setup_type ────────────────────────────────────
def _max_bars(setup_type: str, timeframe_source: str) -> int:
    if setup_type == "flash" or timeframe_source == "1S":
        return 300
    if timeframe_source == "1M":
        return 1800
    if timeframe_source == "5M":
        return 5400
    return 900

# ── File paths ────────────────────────────────────────────────────────────────
SETUPS_FILE   = DATA_DIR / "qualified_setups.jsonl"
PRIMARY_FILE  = DATA_DIR / "combined_1s_dna_btcusdt.jsonl"
BASELINE_FILE = DATA_DIR / "historical_baseline_dna.jsonl"
SCENARIO_FILE = DATA_DIR / "scenarios.jsonl"
VP1M_FILE     = DATA_DIR / "volume_profile_1m.jsonl"
BIAS_FILE     = DATA_DIR / "bias_context.jsonl"

TRADES_FILE   = DATA_DIR / "paper_trades.jsonl"
OPEN_FILE     = DATA_DIR / "paper_trades_open.json"
SUMMARY_FILE  = DATA_DIR / "paper_trade_summary.json"
HEALTH_FILE   = DATA_DIR / "paper_trade_health.json"
PORTFOLIO_FILE = DATA_DIR / "portfolio_sim.json"

# ── Helpers ───────────────────────────────────────────────────────────────────
def _sf(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return f if (f == f and abs(f) != float("inf")) else default
    except (TypeError, ValueError):
        return default

def _read_all_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(path, "r", encoding=enc) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return records
        except (OSError, UnicodeDecodeError):
            records.clear()
    return records

def _safe_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)
    except OSError:
        pass

def _append_fh(fh, rec: dict) -> None:
    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    fh.flush()
    os.fsync(fh.fileno())

def _append_jsonl(path: Path, rec: dict) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        pass

def calc_position_size(balance: float, tier: str, sl_pct: float,
                       entry_price: float = 0.0) -> dict:
    risk_pct = TIER_RISK_PCT.get(tier, 0.01)
    risk_usd = balance * risk_pct
    if sl_pct <= 0 or entry_price <= 0:
        return {
            "risk_usd": round(risk_usd, 2),
            "position_usd": 0.0,
            "contracts": 0.0,
            "leverage_approx": 0.0,
        }
    position_usd = risk_usd / sl_pct
    contracts = position_usd / entry_price
    return {
        "risk_usd": round(risk_usd, 2),
        "position_usd": round(position_usd, 2),
        "contracts": round(contracts, 6),
        "leverage_approx": round(position_usd / balance, 1) if balance > 0 else 0.0,
    }

def calc_pnl_usd(trade: dict, close_price: float) -> dict:
    entry = _sf(trade.get("entry_price", trade.get("open_price", 0)), 0.0)
    contracts = _sf((trade.get("sim") or {}).get("contracts"), 0.0)
    risk_usd = _sf((trade.get("sim") or {}).get("risk_usd"), 0.0)
    direction = trade.get("direction")
    if entry <= 0 or close_price <= 0 or direction not in ("long", "short"):
        return {"pnl_usd": 0.0, "pnl_pct": 0.0, "rr_actual": 0.0}
    if direction == "long":
        pnl_pct = (close_price - entry) / entry
    else:
        pnl_pct = (entry - close_price) / entry
    pnl_usd = contracts * entry * pnl_pct
    rr = pnl_usd / risk_usd if risk_usd > 0 else 0.0
    return {
        "pnl_usd": round(pnl_usd, 2),
        "pnl_pct": round(pnl_pct * 100, 3),
        "rr_actual": round(rr, 2),
    }

def _risk_gate(setup: dict) -> tuple[bool, str, float]:
    """
    Trade açılmadan önce RR ve geometri kontrolü.
    INPUT: qualified setup dict
    OUTPUT: (geçti: bool, sebep: str, rr: float)
    """
    direction = setup.get("direction", "")
    entry_d = setup.get("entry") or {}
    sl_d = setup.get("sl") or {}
    tp1_d = setup.get("tp1") or {}

    risk_obj = setup.get("risk") or {}
    tgt_obj  = setup.get("targets") or {}
    entry = float(entry_d.get("price") or entry_d.get("recommended_entry") or 0)
    sl    = float((setup.get("risk") or {}).get("sl_price") or sl_d.get("price") or 0)
    tp1   = float((setup.get("targets") or {}).get("tp1") or tp1_d.get("price") or 0)

    if entry <= 0 or sl <= 0 or tp1 <= 0:
        return False, f"MISSING_PRICES entry={entry} sl={sl} tp1={tp1}", 0.0

    risk = abs(sl - entry)
    reward = abs(tp1 - entry)

    if risk < 0.01:
        return False, f"SL_TOO_CLOSE risk={risk:.4f}", 0.0

    rr = round(reward / risk, 3)

    # SL yanlış tarafta mı?
    if direction == "long" and sl >= entry:
        return False, f"SL_WRONG_SIDE sl={sl} >= entry={entry}", rr
    if direction == "short" and sl <= entry:
        return False, f"SL_WRONG_SIDE sl={sl} <= entry={entry}", rr

    # TP yanlış tarafta mı?
    if direction == "long" and tp1 <= entry:
        return False, f"TP_WRONG_SIDE tp1={tp1} <= entry={entry}", rr
    if direction == "short" and tp1 >= entry:
        return False, f"TP_WRONG_SIDE tp1={tp1} >= entry={entry}", rr

    # Minimum RR kontrolü
    if rr < 1.0:
        return False, f"RR_TOO_LOW rr={rr} < 1.0", rr

    return True, "OK", rr

# ── Timeframe source inference ────────────────────────────────────────────────
def _infer_timeframe(setup: dict) -> str:
    st  = setup.get("setup_type", "")
    ctx = setup.get("context_at_qualification") or {}
    if st == "flash":
        return "1S"
    if ctx.get("trend_1s"):
        return "1S"
    return "1M"

# ── Engine state ──────────────────────────────────────────────────────────────
class TradeState:
    def __init__(self):
        self.open_trades:         dict[str, dict] = {}
        self.processed_setup_ids: set[str]        = set()
        self.completed_trades:    list[dict]      = []
        self.total_opened:  int  = 0
        self.total_closed:  int  = 0
        self.last_trade_open_ts:  int | None = None
        self.last_trade_close_ts: int | None = None
        self.last_price_ts:       int | None = None
        self.current_price: float | None = None
        self.missing_inputs: list[str] = []
        self.warnings:       list[str] = []
        self.errors:         list[str] = []
        # Streak tracking
        self.consec_wins:         int = 0
        self.consec_losses:       int = 0
        self.max_consec_wins:     int = 0
        self.max_consec_losses:   int = 0

def _simulated_trades(state: TradeState) -> list[dict]:
    return [
        trade for trade in state.completed_trades
        if isinstance(trade.get("sim"), dict)
        and trade.get("sim", {}).get("pnl_usd") is not None
    ]

def _current_balance(state: TradeState) -> float:
    pnl = sum(_sf(trade.get("sim", {}).get("pnl_usd"), 0.0)
              for trade in _simulated_trades(state))
    return round(INITIAL_BALANCE_USD + pnl, 2)

def _portfolio_group(trades: list[dict]) -> dict:
    wins = sum(1 for trade in trades if _sf(trade.get("sim", {}).get("pnl_usd"), 0.0) > 0)
    pnl = sum(_sf(trade.get("sim", {}).get("pnl_usd"), 0.0) for trade in trades)
    count = len(trades)
    return {
        "trades": count,
        "wr": round(wins / count * 100, 1) if count else 0.0,
        "pnl_usd": round(pnl, 2),
    }

def _write_portfolio(state: TradeState) -> None:
    trades = _simulated_trades(state)
    pnls = [_sf(trade.get("sim", {}).get("pnl_usd"), 0.0) for trade in trades]
    rrs = [_sf(trade.get("sim", {}).get("rr_actual"), 0.0) for trade in trades]
    wins = sum(1 for pnl in pnls if pnl > 0)
    losses = sum(1 for pnl in pnls if pnl < 0)
    breakeven = len(pnls) - wins - losses

    equity = INITIAL_BALANCE_USD
    peak = equity
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    grouped: dict[str, dict[str, list[dict]]] = {
        "by_tier": defaultdict(list),
        "by_regime": defaultdict(list),
        "by_session": defaultdict(list),
    }
    for trade in trades:
        sim = trade.get("sim") or {}
        context = trade.get("context_at_open") or {}
        grouped.get("by_tier", {})[sim.get("tier", "L1_LOW")].append(trade)
        grouped.get("by_regime", {})[context.get("regime") or "UNKNOWN"].append(trade)
        grouped.get("by_session", {})[context.get("session") or "UNKNOWN"].append(trade)

    total_pnl = round(sum(pnls), 2)
    payload = {
        "initial_balance": INITIAL_BALANCE_USD,
        "current_balance": round(INITIAL_BALANCE_USD + total_pnl, 2),
        "total_pnl_usd": total_pnl,
        "total_pnl_pct": round(total_pnl / INITIAL_BALANCE_USD * 100, 2),
        "total_trades": len(trades),
        "winning_trades": wins,
        "losing_trades": losses,
        "breakeven_trades": breakeven,
        "win_rate_pct": round(wins / len(trades) * 100, 1) if trades else 0.0,
        "avg_rr": round(sum(rrs) / len(rrs), 2) if rrs else 0.0,
        "max_drawdown_usd": round(max_drawdown, 2),
        "best_trade_usd": round(max(pnls), 2) if pnls else 0.0,
        "worst_trade_usd": round(min(pnls), 2) if pnls else 0.0,
        "by_tier": {key: _portfolio_group(value) for key, value in grouped.get("by_tier", {}).items()},
        "by_regime": {key: _portfolio_group(value) for key, value in grouped.get("by_regime", {}).items()},
        "by_session": {key: _portfolio_group(value) for key, value in grouped.get("by_session", {}).items()},
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }
    _safe_write_json(PORTFOLIO_FILE, payload)

# ── Open a paper trade ────────────────────────────────────────────────────────

def is_qualified_setup_record(setup: dict) -> bool:
    """
    Lifecycle Integrity Guard:
    Paper trade may open only from qualified setup records.

    Required:
    - qualified_setup_id OR source_setup_id exists
    - record came from qualified_setups.jsonl semantics
    - no terminal observer state
    """
    if not isinstance(setup, dict):
        return False

    qid = setup.get("qualified_setup_id")
    sid = setup.get("source_setup_id") or setup.get("setup_id")

    if not sid:
        return False

    # Qualified record must carry qualified id or qualification timestamp.
    if not qid and setup.get("qualification_ts") is None:
        return False

    terminal = str(
        setup.get("observer_state")
        or setup.get("state")
        or setup.get("status")
        or ""
    ).lower()

    forbidden = {"expired", "invalidated", "waiting_timeout", "timeout", "rejected"}
    if terminal in forbidden:
        return False

    return True

def try_open_trade(state: TradeState, setup: dict, trades_fh) -> bool:
    # C1: status
    status = setup.get("status", "open")
    if status != "open":
        return False

    # Check if setup is too old (> 24 hours)
    now_ms = int(time.time() * 1000)
    qual_ts = setup.get("qualification_ts", 0)
    if qual_ts and (now_ms - qual_ts) > 86400000:  # 24 hours in ms
        age_s = (now_ms - qual_ts) / 1000
        print(f"[PAPER SKIP] Setup too old: {age_s:.0f}s", flush=True)
        return False

    # C2: not already processed
    src_id = setup.get("qualified_setup_id") or setup.get("source_setup_id") or setup.get("setup_id", "")
    if not src_id or src_id in state.processed_setup_ids:
        return False

    # C4: direction valid
    direction = setup.get("direction")
    if direction not in ("long", "short"):
        return False
    if not is_qualified_setup_record(setup):
        return
    # C5: entry price > 0
    entry_obj  = setup.get("entry") or {}
    open_price = _sf(entry_obj.get("recommended_entry"), 0.0)
    if open_price <= 0:
        return False

    # C3: max open trades
    if len(state.open_trades) >= MAX_OPEN_TRADES:
        print("[PAPER] Max open trades reached", flush=True)
        return False

    state.processed_setup_ids.add(src_id)

    risk_obj = setup.get("risk") or {}
    tgt_obj  = setup.get("targets") or {}
    ctx_obj  = setup.get("context_at_qualification") or {}

    sl_price  = _sf(risk_obj.get("sl_price"),  0.0)
    atr_used  = _sf(risk_obj.get("atr_used"),  0.0)
    tp1_price = _sf(tgt_obj.get("tp1"),        0.0)
    tp2_price = _sf(tgt_obj.get("tp2"),        0.0)
    tp3_price = _sf(tgt_obj.get("tp3"),        0.0)
    rg_pass, rg_reason, rg_rr = _risk_gate(setup)
    if not rg_pass:
        print(f"[PAPER] RISK_GATE BLOCK: {rg_reason}", flush=True)
        return False
    setup_type      = setup.get("setup_type", "normal")
    quality_tier    = setup.get("quality_tier", "L1_LOW")
    timeframe_source = _infer_timeframe(setup)
    open_ts   = setup.get("qualification_ts") or int(time.time() * 1000)
    sl_pct = abs(open_price - sl_price) / open_price if open_price > 0 else 0.0
    sim = calc_position_size(
        _current_balance(state), quality_tier, sl_pct, open_price,
    )
    sim["tier"] = quality_tier

    trade = {
        "trade_id":        str(uuid.uuid4()),
        "source_setup_id": src_id,
        "symbol":          SYMBOL,
        "direction":       direction,
        "setup_type":      setup_type,
        "pattern_key":     setup.get("pattern_key", f"{setup_type}_{direction}"),
        "timeframe_source": timeframe_source,

        "open_ts":    open_ts,
        "open_price": open_price,
        "entry_price": open_price,

        "sl_price":   sl_price,
        "sl_original": sl_price,
        "tp1_price":  tp1_price,
        "tp2_price":  tp2_price,
        "tp3_price":  tp3_price,
        "atr_used":   atr_used,

        "status":       "open",
        "close_ts":     None,
        "close_price":  None,
        "close_reason": None,

        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "sl_hit":  False,

        "stop_moved_to_breakeven": False,
        "max_favorable_price": None,
        "max_adverse_price":   None,

        "pnl_r":   None,
        "pnl_pct": None,
        "outcome": None,
        "sim": sim,

        "duration_seconds": None,
        "bars_held":        0,

        "context_at_open": {
            "scenario":      ctx_obj.get("active_scenario"),
            "gate_grade":    ctx_obj.get("gate_grade"),
            "location":      ctx_obj.get("location"),
            "trend_1s":      ctx_obj.get("trend_1s"),
            "trend_1m":      ctx_obj.get("trend_1m"),
            "market_bias":   ctx_obj.get("market_bias"),
            "profile_shape": ctx_obj.get("profile_shape"),
            "regime":        setup.get("regime_at_qualification"),
            "session":       setup.get("session_at_qualification"),
            "volatility":    setup.get("volatility_at_qualification"),
            "entry_timing":  setup.get("entry_timing"),
        },

        "market_questions": setup.get("market_questions") or {},
    }

    # Check SL validity
    if sl_price is None or sl_price <= 0:
        print("[PAPER SKIP] Invalid SL", flush=True)
        return False

    # Check SL distance (warn if too wide)
    sl_dist = abs(open_price - sl_price)
    if atr_used > 0 and sl_dist > atr_used * 2.0:
        print(f"[PAPER WARNING] SL too wide: {sl_dist:.2f} vs ATR {atr_used:.2f}", flush=True)
        # Log but proceed (don't reject)

    # Open validation
    errors = _validate_open(trade)
    if errors:
        state.warnings.extend(errors)

    state.open_trades[trade["trade_id"]] = trade
    state.total_opened += 1
    state.last_trade_open_ts = open_ts

    _print_open(trade)
    return True

# ── Update trade on new price bar ─────────────────────────────────────────────
def update_trades(state: TradeState, ts: int, price: float | None,
                  trades_fh) -> None:
    if price is None or price <= 0:
        return

    state.last_price_ts   = ts
    state.current_price   = price
    to_close: list[tuple[str, str, float]] = []  # (trade_id, reason, close_price)

    for trade_id, trade in state.open_trades.items():
        direction   = trade["direction"]
        open_price  = trade["open_price"]
        sl_price    = trade["sl_price"]
        tp1_price   = trade["tp1_price"]
        tp2_price   = trade["tp2_price"]
        tp3_price   = trade["tp3_price"]

        # bars_held
        trade["bars_held"] += 1

        # max/min favorable/adverse
        if trade["max_favorable_price"] is None:
            trade["max_favorable_price"] = price
            trade["max_adverse_price"]   = price
        else:
            if direction == "long":
                trade["max_favorable_price"] = max(trade["max_favorable_price"], price)
                trade["max_adverse_price"]   = min(trade["max_adverse_price"], price)
            else:
                trade["max_favorable_price"] = min(trade["max_favorable_price"], price)
                trade["max_adverse_price"]   = max(trade["max_adverse_price"], price)

        close_reason: str | None = None
        close_price: float | None = None

        # ── SL check (highest priority) ────────────────────────────────────
        if direction == "long"  and price <= sl_price:
            close_reason = "sl_hit"
            close_price  = sl_price
        elif direction == "short" and price >= sl_price:
            close_reason = "sl_hit"
            close_price  = sl_price

        if close_reason:
            trade["sl_hit"] = True
            to_close.append((trade_id, close_reason, close_price))
            continue

        # ── TP1 check ──────────────────────────────────────────────────────
        if not trade["tp1_hit"]:
            tp1_hit = False
            if direction == "long"  and price >= tp1_price:
                tp1_hit = True
            elif direction == "short" and price <= tp1_price:
                tp1_hit = True

            if tp1_hit:
                trade["tp1_hit"] = True
                trade["stop_moved_to_breakeven"] = True
                trade["sl_price"] = open_price   # move to breakeven
                _print_tp1(trade, price)

        # ── TP2 check ──────────────────────────────────────────────────────
        if trade["tp1_hit"] and not trade["tp2_hit"]:
            tp2_hit = False
            if direction == "long"  and price >= tp2_price:
                tp2_hit = True
            elif direction == "short" and price <= tp2_price:
                tp2_hit = True

            if tp2_hit:
                trade["tp2_hit"] = True
                _print_tp2(trade, price)

        # ── TP3 check ──────────────────────────────────────────────────────
        if trade["tp2_hit"] and not trade["tp3_hit"]:
            tp3_hit = False
            if direction == "long"  and price >= tp3_price:
                tp3_hit = True
            elif direction == "short" and price <= tp3_price:
                tp3_hit = True

            if tp3_hit:
                trade["tp3_hit"] = True
                close_reason = "tp3_hit"
                close_price  = tp3_price
                to_close.append((trade_id, close_reason, close_price))
                continue

        # ── Breakeven stop ─────────────────────────────────────────────────
        if trade["stop_moved_to_breakeven"]:
            be_hit = False
            if direction == "long"  and price <= open_price:
                be_hit = True
            elif direction == "short" and price >= open_price:
                be_hit = True

            if be_hit:
                close_reason = "breakeven_stop"
                close_price  = open_price
                to_close.append((trade_id, close_reason, close_price))
                continue

        # ── Timeout ────────────────────────────────────────────────────────
        max_b = _max_bars(trade["setup_type"], trade["timeframe_source"])
        if trade["bars_held"] >= max_b:
            to_close.append((trade_id, "timeout", price))

    # Close trades collected this bar
    for trade_id, reason, cp in to_close:
        trade = state.open_trades.pop(trade_id, None)
        if trade is None:
            continue
        _close_trade(state, trade, ts, cp, reason, trades_fh)


# ── Close a trade ─────────────────────────────────────────────────────────────
def _close_trade(state: TradeState, trade: dict, close_ts: int,
                 close_price: float, reason: str, trades_fh) -> None:
    direction  = trade["direction"]
    open_price = trade["open_price"]
    sl_original = trade.get("sl_original", trade["sl_price"])

    trade["close_ts"]    = close_ts
    trade["close_price"] = close_price
    trade["close_reason"] = reason
    trade["duration_seconds"] = (close_ts - trade["open_ts"]) / 1000.0

    # pnl_pct
    if direction == "long":
        pnl_pct = (close_price - open_price) / open_price * 100
    else:
        pnl_pct = (open_price - close_price) / open_price * 100

    # pnl_r
    risk_dist = abs(open_price - sl_original) if sl_original > 0 else 1.0
    if direction == "long":
        pnl_r = (close_price - open_price) / risk_dist
    else:
        pnl_r = (open_price - close_price) / risk_dist

    # max favorable/adverse R
    mfp = trade["max_favorable_price"]
    map_ = trade["max_adverse_price"]

    def _fr(fav_p: float | None) -> float | None:
        if fav_p is None:
            return None
        if direction == "long":
            return (fav_p - open_price) / risk_dist
        else:
            return (open_price - fav_p) / risk_dist

    def _ar(adv_p: float | None) -> float | None:
        if adv_p is None:
            return None
        if direction == "long":
            return (open_price - adv_p) / risk_dist
        else:
            return (adv_p - open_price) / risk_dist

    max_fav_r = _fr(mfp)
    max_adv_r = _ar(map_)

    trade["pnl_pct"] = round(pnl_pct, 6)
    trade["pnl_r"]   = round(pnl_r, 6)
    sim_result = calc_pnl_usd(trade, close_price)
    trade.setdefault("sim", {}).update(sim_result)

    # outcome
    if reason == "sl_hit":
        outcome = "loss"
    elif reason == "tp3_hit":
        outcome = "win"
    elif reason == "breakeven_stop":
        outcome = "breakeven"
    elif reason == "timeout":
        if pnl_r > 0.001:
            outcome = "timeout_win"
        elif pnl_r < -0.001:
            outcome = "timeout_loss"
        else:
            outcome = "timeout_flat"
    else:
        outcome = "timeout_flat"

    trade["outcome"] = outcome

    # Streak tracking
    if outcome in ("win", "timeout_win"):
        state.consec_wins  += 1
        state.consec_losses = 0
    elif outcome in ("loss", "timeout_loss"):
        state.consec_losses += 1
        state.consec_wins   = 0
    else:
        state.consec_wins   = 0
        state.consec_losses = 0
    state.max_consec_wins   = max(state.max_consec_wins,   state.consec_wins)
    state.max_consec_losses = max(state.max_consec_losses, state.consec_losses)

    # Validation
    errors = _validate_close(trade)

    # Completed record schema
    rec = {
        "engine":            "paper_trade_engine",
        "layer":             "Layer-12",
        "trade_id":          trade["trade_id"],
        "source_setup_id":   trade["source_setup_id"],
        "symbol":            SYMBOL,
        "direction":         direction,
        "setup_type":        trade["setup_type"],
        "pattern_key":       trade.get("pattern_key"),
        "timeframe_source":  trade["timeframe_source"],

        "open_ts":           trade["open_ts"],
        "open_price":        trade["open_price"],
        "close_ts":          close_ts,
        "close_price":       close_price,
        "close_reason":      reason,
        "duration_seconds":  trade["duration_seconds"],
        "bars_held":         trade["bars_held"],

        "levels": {
            "sl_price":     trade["sl_price"],
            "tp1_price":    trade["tp1_price"],
            "tp2_price":    trade["tp2_price"],
            "tp3_price":    trade["tp3_price"],
            "sl_original":  sl_original,
            "atr_used":     trade["atr_used"],
        },

        "milestones": {
            "tp1_hit":               trade["tp1_hit"],
            "tp2_hit":               trade["tp2_hit"],
            "tp3_hit":               trade["tp3_hit"],
            "sl_hit":                trade["sl_hit"],
            "stop_moved_to_breakeven": trade["stop_moved_to_breakeven"],
        },

        "results": {
            "outcome":             outcome,
            "pnl_pct":             round(pnl_pct, 6),
            "pnl_r":               round(pnl_r, 6),
            "max_favorable_r":     round(max_fav_r, 6) if max_fav_r is not None else None,
            "max_adverse_r":       round(max_adv_r, 6) if max_adv_r is not None else None,
            "max_favorable_price": round(mfp, 4) if mfp is not None else None,
            "max_adverse_price":   round(map_, 4) if map_ is not None else None,
        },

        "context_at_open":  trade["context_at_open"],
        "market_questions": trade["market_questions"],
        "sim":              trade.get("sim", {}),

        "validation": {
            "sl_valid":           sl_original > 0,
            "tp_sequence_valid":  not bool([e for e in errors if "tp_sequence" in e]),
            "pnl_consistent":     not bool([e for e in errors if "pnl" in e]),
            "errors":             errors,
        },
    }

    # Write
    if trades_fh is not None:
        _append_fh(trades_fh, rec)
    else:
        _append_jsonl(TRADES_FILE, rec)

    state.completed_trades.append(rec)
    state.total_closed += 1
    state.last_trade_close_ts = close_ts

    _print_close(rec)
    _write_summary(state)
    _write_portfolio(state)


# ── Summary computation ───────────────────────────────────────────────────────
def _group_stats(trades: list[dict]) -> dict:
    if not trades:
        return _empty_stats()

    outcomes = [(t.get("results") or t).get("outcome", "unknown") for t in trades]
    pnl_rs   = [_sf((t.get("results") or t).get("pnl_r")) for t in trades]
    durs     = [_sf(t.get("duration_seconds")) for t in trades]

    wins      = sum(1 for o in outcomes if o in ("win", "timeout_win"))
    losses    = sum(1 for o in outcomes if o in ("loss", "timeout_loss"))
    bkvns     = sum(1 for o in outcomes if o == "breakeven")
    timeouts  = sum(1 for o in outcomes if o and "timeout" in o)
    n         = len(trades)

    gross_wins   = sum(r for r in pnl_rs if r > 0)
    gross_losses = sum(r for r in pnl_rs if r < 0)
    pf = round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else None

    return {
        "trades":               n,
        "wins":                 wins,
        "losses":               losses,
        "breakevens":           bkvns,
        "timeouts":             timeouts,
        "win_rate":             round(wins / n * 100, 2) if n else 0.0,
        "avg_pnl_r":            round(sum(pnl_rs) / n, 4) if n else 0.0,
        "total_pnl_r":          round(sum(pnl_rs), 4),
        "avg_duration_seconds": round(sum(durs) / n, 1) if n else 0.0,
        "max_win_r":            round(max(pnl_rs), 4) if pnl_rs else 0.0,
        "max_loss_r":           round(min(pnl_rs), 4) if pnl_rs else 0.0,
        "profit_factor":        pf,
    }

def _empty_stats() -> dict:
    return {
        "trades": 0, "wins": 0, "losses": 0, "breakevens": 0, "timeouts": 0,
        "win_rate": 0.0, "avg_pnl_r": 0.0, "total_pnl_r": 0.0,
        "avg_duration_seconds": 0.0, "max_win_r": 0.0, "max_loss_r": 0.0,
        "profit_factor": None,
    }

def _write_summary(state: TradeState) -> None:
    trades = state.completed_trades
    total  = _group_stats(trades)

    by_dir: dict[str, list] = defaultdict(list)
    by_st:  dict[str, list] = defaultdict(list)
    by_tf:  dict[str, list] = defaultdict(list)
    by_sc:  dict[str, list] = defaultdict(list)
    by_gg:  dict[str, list] = defaultdict(list)
    by_cr:  dict[str, int]  = defaultdict(int)

    for t in trades:
        by_dir[t.get("direction", "unknown")].append(t)
        by_st[t.get("setup_type", "unknown")].append(t)
        by_tf[t.get("timeframe_source", "unknown")].append(t)
        sc = (t.get("context_at_open") or {}).get("scenario") or "unknown"
        by_sc[sc].append(t)
        gg = (t.get("context_at_open") or {}).get("gate_grade") or "unknown"
        by_gg[gg].append(t)
        cr = (t.get("results") or t).get("outcome", "unknown")
        by_cr[cr] += 1

    out = {
        "engine":    "paper_trade_engine",
        "symbol":    SYMBOL,
        "generated_at": time.time(),

        "total":          total,
        "by_direction":   {k: _group_stats(v) for k, v in by_dir.items()},
        "by_setup_type":  {k: _group_stats(v) for k, v in by_st.items()},
        "by_timeframe":   {k: _group_stats(v) for k, v in by_tf.items()},
        "by_scenario":    {k: _group_stats(v) for k, v in by_sc.items()},
        "by_gate_grade":  {k: _group_stats(v) for k, v in by_gg.items()},
        "by_close_reason": dict(by_cr),

        "current_open_trades":     len(state.open_trades),
        "consecutive_wins":        state.consec_wins,
        "consecutive_losses":      state.consec_losses,
        "max_consecutive_wins":    state.max_consec_wins,
        "max_consecutive_losses":  state.max_consec_losses,

        "scores": {
            "confidence": None,
            "edge_score": None,
        },
    }
    _safe_write_json(SUMMARY_FILE, out)


# ── Open positions file ───────────────────────────────────────────────────────
def _write_open_positions(state: TradeState) -> None:
    items = []
    for trade in state.open_trades.values():
        cp    = state.current_price or 0.0
        op    = trade["open_price"]
        sl_o  = trade.get("sl_original", trade["sl_price"])
        risk  = abs(op - sl_o) if sl_o > 0 else 1.0
        d     = trade["direction"]
        cur_r = ((cp - op) / risk) if d == "long" else ((op - cp) / risk)
        items.append({
            "trade_id":                 trade["trade_id"],
            "source_setup_id":          trade["source_setup_id"],
            "direction":                d,
            "open_ts":                  trade["open_ts"],
            "open_price":               trade["open_price"],
            "current_price":            round(cp, 4),
            "bars_held":                trade["bars_held"],
            "current_pnl_r":            round(cur_r, 4),
            "tp1_hit":                  trade["tp1_hit"],
            "tp2_hit":                  trade["tp2_hit"],
            "sl_price":                 trade["sl_price"],
            "tp1_price":                trade["tp1_price"],
            "tp2_price":                trade["tp2_price"],
            "tp3_price":                trade["tp3_price"],
            "stop_moved_to_breakeven":  trade["stop_moved_to_breakeven"],
            "scenario":                 (trade["context_at_open"] or {}).get("scenario"),
            "gate_grade":               (trade["context_at_open"] or {}).get("gate_grade"),
            "sim":                      trade.get("sim", {}),
        })
    _safe_write_json(OPEN_FILE, {
        "generated_at": time.time(),
        "open_count":   len(items),
        "trades":       items,
    })


# ── Health ────────────────────────────────────────────────────────────────────
def _write_health(state: TradeState) -> None:
    _safe_write_json(HEALTH_FILE, {
        "status":               "alive",
        "total_trades_opened":  state.total_opened,
        "total_trades_closed":  state.total_closed,
        "current_open":         len(state.open_trades),
        "last_trade_open_ts":   state.last_trade_open_ts,
        "last_trade_close_ts":  state.last_trade_close_ts,
        "last_price_ts":        state.last_price_ts,
        "missing_inputs":       state.missing_inputs,
        "warnings":             state.warnings[-20:],
        "errors":               state.errors[-20:],
    })


# ── Validation helpers ────────────────────────────────────────────────────────
def _validate_open(trade: dict) -> list[str]:
    errs: list[str] = []
    op = trade["open_price"]
    sl = trade["sl_price"]
    t1 = trade["tp1_price"]
    t2 = trade["tp2_price"]
    t3 = trade["tp3_price"]
    d  = trade["direction"]
    atr = trade["atr_used"]

    if op <= 0:
        errs.append("open_price <= 0")
    if atr <= 0:
        errs.append("atr_used <= 0")
    if d == "long":
        if not (sl < op):
            errs.append(f"tp_sequence: LONG sl={sl} must be < open={op}")
        if t1 > 0 and not (op < t1):
            errs.append(f"tp_sequence: LONG open={op} must be < tp1={t1}")
        if t1 > 0 and t2 > 0 and not (t1 < t2):
            errs.append(f"tp_sequence: LONG tp1={t1} must be < tp2={t2}")
        if t2 > 0 and t3 > 0 and not (t2 < t3):
            errs.append(f"tp_sequence: LONG tp2={t2} must be < tp3={t3}")
    elif d == "short":
        if not (sl > op):
            errs.append(f"tp_sequence: SHORT sl={sl} must be > open={op}")
        if t1 > 0 and not (op > t1):
            errs.append(f"tp_sequence: SHORT open={op} must be > tp1={t1}")
        if t1 > 0 and t2 > 0 and not (t1 > t2):
            errs.append(f"tp_sequence: SHORT tp1={t1} must be > tp2={t2}")
        if t2 > 0 and t3 > 0 and not (t2 > t3):
            errs.append(f"tp_sequence: SHORT tp2={t2} must be > tp3={t3}")
    return errs

def _validate_close(trade: dict) -> list[str]:
    errs: list[str] = []
    cp  = trade.get("close_price", 0.0) or 0.0
    dur = trade.get("duration_seconds", -1) or -1
    pr  = trade.get("pnl_r")
    out = trade.get("outcome", "")
    d   = trade["direction"]
    op  = trade["open_price"]
    sl_o = trade.get("sl_original", trade["sl_price"])

    if cp <= 0:
        errs.append("close_price <= 0")
    if dur < 0:
        errs.append("duration_seconds < 0")
    if pr is None or pr != pr or abs(pr) == float("inf"):
        errs.append("pnl_r NaN or inf")
    valid_outcomes = {"win","loss","breakeven","timeout_win","timeout_loss","timeout_flat"}
    if out not in valid_outcomes:
        errs.append(f"invalid outcome: {out}")

    reason = trade.get("close_reason", "")
    if reason == "tp3_hit" and out == "win":
        if d == "long" and cp < op:
            errs.append(f"pnl: LONG WIN but close {cp} < open {op}")
        if d == "short" and cp > op:
            errs.append(f"pnl: SHORT WIN but close {cp} > open {op}")
    if reason == "sl_hit" and out == "loss":
        if d == "long" and cp > sl_o:
            errs.append(f"pnl: LONG LOSS but close {cp} > sl {sl_o}")
        if d == "short" and cp < sl_o:
            errs.append(f"pnl: SHORT LOSS but close {cp} < sl {sl_o}")

    return errs


# ── Restart recovery ──────────────────────────────────────────────────────────
def restore_state(state: TradeState) -> None:
    # Load processed IDs from closed trades
    for rec in _read_all_jsonl(TRADES_FILE):
        sid = rec.get("source_setup_id")
        if sid:
            state.processed_setup_ids.add(sid)
        state.completed_trades.append(rec)
    state.total_closed = len(state.completed_trades)

    # Load open trade setup IDs (prevent re-opening)
    if OPEN_FILE.exists():
        try:
            with open(OPEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
        for t in (data.get("trades") or []):
            sid = t.get("source_setup_id")
            if sid:
                state.processed_setup_ids.add(sid)

    n = len(state.completed_trades)
    print(f"[PAPER] Restored {n} completed trades, "
          f"{len(state.processed_setup_ids)} setup_ids", flush=True)


# ── Terminal output ───────────────────────────────────────────────────────────
def _print_open(trade: dict) -> None:
    if FULL_PRINT:
        print(json.dumps(trade, ensure_ascii=False), flush=True)
        return
    ctx  = trade["context_at_open"] or {}
    tid  = trade["trade_id"][:8]
    d    = trade["direction"].upper()
    op   = trade["open_price"]
    sl   = trade["sl_price"]
    t1   = trade["tp1_price"]
    t2   = trade["tp2_price"]
    t3   = trade["tp3_price"]
    sc   = ctx.get("scenario") or "none"
    gg   = ctx.get("gate_grade") or "?"
    print(
        f"[PAPER OPEN] trade_id={tid} {d} entry={op}\n"
        f"  sl={sl} tp1={t1} tp2={t2} tp3={t3}\n"
        f"  scenario={sc} gate={gg}",
        flush=True,
    )

def _print_tp1(trade: dict, price: float) -> None:
    if FULL_PRINT:
        return
    tid = trade["trade_id"][:8]
    d   = trade["direction"].upper()
    print(
        f"[PAPER TP1] trade_id={tid} {d} price={price}\n"
        f"  stop→breakeven={trade['open_price']}",
        flush=True,
    )

def _print_tp2(trade: dict, price: float) -> None:
    if FULL_PRINT:
        return
    tid = trade["trade_id"][:8]
    d   = trade["direction"].upper()
    print(f"[PAPER TP2] trade_id={tid} {d} price={price}", flush=True)

def _print_close(rec: dict) -> None:
    if FULL_PRINT:
        print(json.dumps(rec, ensure_ascii=False), flush=True)
        return
    tid  = rec["trade_id"][:8]
    d    = rec["direction"].upper()
    res  = rec["results"]
    out  = res["outcome"].upper()
    cp   = rec["close_price"]
    rr   = rec["close_reason"]
    pr   = res["pnl_r"]
    pp   = res["pnl_pct"]
    dur  = rec["duration_seconds"]
    ms   = rec["milestones"]
    tp1c = "✓" if ms["tp1_hit"] else "✗"
    tp2c = "✓" if ms["tp2_hit"] else "✗"
    tp3c = "✓" if ms["tp3_hit"] else "✗"
    sign = "+" if pr >= 0 else ""
    print(
        f"[PAPER CLOSE] trade_id={tid} {d} {out}\n"
        f"  close={cp} reason={rr}\n"
        f"  pnl_r={sign}{pr:.2f} pnl_pct={sign}{pp:.4f}% duration={dur:.0f}s\n"
        f"  tp1={tp1c} tp2={tp2c} tp3={tp3c}",
        flush=True,
    )


# ── Price extraction from combined DNA bar ────────────────────────────────────
def _extract_price(row: dict) -> float | None:
    cdna = row.get("candle_dna") or {}
    co   = cdna.get("close")
    if isinstance(co, dict):
        px = _sf(co.get("price"), 0.0)
    else:
        px = _sf(co, 0.0)
    if px > 0:
        return px
    # fallback: carry_forward_price
    cfp = row.get("carry_forward_price")
    if cfp is not None:
        v = _sf(cfp, 0.0)
        if v > 0:
            return v
    return None


# ── Batch mode ────────────────────────────────────────────────────────────────
def run_batch() -> None:
    print("[PAPER] Batch mode — loading input files", flush=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = TradeState()

    # Check inputs
    for p in [SETUPS_FILE, PRIMARY_FILE]:
        if not p.exists():
            state.missing_inputs.append(str(p))

    restore_state(state)

    # Load all prices
    price_rows = _read_all_jsonl(PRIMARY_FILE)
    prices: list[tuple[int, float]] = []
    for row in price_rows:
        ts = row.get("window_start_ts")
        px = _extract_price(row)
        if ts is not None and px is not None:
            prices.append((int(ts), px))
    prices.sort(key=lambda x: x[0])
    print(f"[PAPER] {len(prices)} price bars loaded", flush=True)

    # Load all setups
    setup_rows = _read_all_jsonl(SETUPS_FILE)
    setups = sorted(
        [r for r in setup_rows if r.get("qualification_ts")],
        key=lambda r: r["qualification_ts"]
    )
    print(f"[PAPER] {len(setups)} qualified setups loaded", flush=True)

    last_health  = time.time()
    last_open_wr = time.time()
    last_summary_print = time.time()
    si = 0  # setup index

    with open(TRADES_FILE, "a", encoding="utf-8") as trades_fh:
        for ts, price in prices:
            if HALT_FILE.exists():
                print("[PAPER] SYSTEM_HALT — aborting", flush=True)
                break

            # Open setups whose qualification_ts <= current bar ts
            while si < len(setups) and setups[si]["qualification_ts"] <= ts:
                try_open_trade(state, setups[si], trades_fh)
                si += 1

            # Update open trades with this price bar
            update_trades(state, ts, price, trades_fh)

            now = time.time()
            if now - last_health >= HEALTH_INTERVAL_S:
                _write_health(state)
                last_health = now
            if now - last_open_wr >= OPEN_INTERVAL_S:
                _write_open_positions(state)
                last_open_wr = now
            if now - last_summary_print >= 60.0:
                _print_summary(state)
                last_summary_print = now

        # Open remaining setups (after price data ends)
        while si < len(setups):
            try_open_trade(state, setups[si], None)
            si += 1

        # Force-close remaining open trades at last price
        if prices:
            last_ts, last_px = prices[-1]
            for trade in list(state.open_trades.values()):
                _close_trade(state, trade, last_ts, last_px,
                             "timeout", trades_fh)
            state.open_trades.clear()

    _write_health(state)
    _write_open_positions(state)
    _write_summary(state)
    _write_portfolio(state)
    _print_summary(state)
    print(f"[PAPER] Batch done — opened={state.total_opened} "
          f"closed={state.total_closed}", flush=True)


def _print_summary(state: TradeState) -> None:
    t = state.completed_trades
    n = len(t)
    if n == 0:
        print(f"[PAPER SUMMARY] trades=0 open={len(state.open_trades)}", flush=True)
        return
    pnls = [_sf((r.get("results") or r).get("pnl_r")) for r in t]
    wins = sum(1 for r in t if (r.get("results") or r).get("outcome","") in ("win","timeout_win"))
    wr   = wins / n * 100
    avg  = sum(pnls) / n
    tot  = sum(pnls)
    print(
        f"[PAPER SUMMARY] trades={n} win_rate={wr:.1f}%\n"
        f"  avg_r={avg:.2f} total_r={tot:.2f} open={len(state.open_trades)}",
        flush=True,
    )


# ── Live mode (asyncio) ───────────────────────────────────────────────────────
async def _tail_setups(state: TradeState, trades_fh) -> None:
    while not SETUPS_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(SETUPS_FILE, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                try_open_trade(state, json.loads(line), trades_fh)
            except Exception:
                pass

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                try_open_trade(state, json.loads(line), trades_fh)
            except Exception:
                pass


async def _tail_prices(state: TradeState, trades_fh) -> None:
    while not PRIMARY_FILE.exists():
        if HALT_FILE.exists():
            return
        await asyncio.sleep(1.0)

    with open(PRIMARY_FILE, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                ts  = row.get("window_start_ts")
                px  = _extract_price(row)
                if ts is not None and px is not None:
                    update_trades(state, int(ts), px, trades_fh)
            except Exception:
                pass

        while True:
            if HALT_FILE.exists():
                return
            line = f.readline()
            if not line:
                await asyncio.sleep(POLL_SLEEP)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                ts  = row.get("window_start_ts")
                px  = _extract_price(row)
                if ts is not None and px is not None:
                    update_trades(state, int(ts), px, trades_fh)
            except Exception:
                pass


async def _periodic(state: TradeState) -> None:
    last_health     = time.time()
    last_open       = time.time()
    last_summary_pr = time.time()

    while not HALT_FILE.exists():
        await asyncio.sleep(5.0)
        now = time.time()
        if now - last_health >= HEALTH_INTERVAL_S:
            _write_health(state)
            last_health = now
        if now - last_open >= OPEN_INTERVAL_S:
            _write_open_positions(state)
            last_open = now
        if now - last_summary_pr >= 60.0:
            _print_summary(state)
            last_summary_pr = now


async def run_live() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = TradeState()

    for p in [SETUPS_FILE, PRIMARY_FILE]:
        if not p.exists():
            state.missing_inputs.append(str(p))

    restore_state(state)
    _write_health(state)
    _write_open_positions(state)
    _write_summary(state)
    _write_portfolio(state)
    print("[PAPER] Paper Trade Engine başlatıldı (live mode)", flush=True)

    with open(TRADES_FILE, "a", encoding="utf-8") as trades_fh:
        await asyncio.gather(
            asyncio.create_task(_tail_setups(state,  trades_fh), name="paper-setups"),
            asyncio.create_task(_tail_prices(state,  trades_fh), name="paper-prices"),
            asyncio.create_task(_periodic(state),                name="paper-periodic"),
        )

    _write_health(state)
    _write_open_positions(state)
    _write_summary(state)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Paper Trade Engine — Layer 12")
    parser.add_argument("--mode", choices=["batch", "live"], default="live")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if HALT_FILE.exists():
        print("[PAPER] SYSTEM_HALT exists at startup — refusing to start", flush=True)
        return

    if args.mode == "batch":
        run_batch()
    else:
        asyncio.run(run_live())


if __name__ == "__main__":
    main()
