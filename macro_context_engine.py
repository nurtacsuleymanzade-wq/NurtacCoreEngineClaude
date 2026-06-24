#!/usr/bin/env python3
"""
NurtacCoreEngineClaude - Macro Intelligence Engine

Builds a macro context snapshot from:
  - Binance spot/futures price + open interest
  - Coinbase spot premium
  - Kraken spot delta
  - Yahoo Finance ETF charts for IBIT / FBTC / GBTC
  - Local bias context for funding/OI proxy fallback

Output:
  data/macro_context.json
"""

import argparse
import json
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

DATA_DIR = Path("data")
MACRO_FILE = DATA_DIR / "macro_context.json"
MACRO_CACHE_FILE = DATA_DIR / "macro_context_cache.json"
HALT_FILE = DATA_DIR / "SYSTEM_HALT"
UPDATE_INTERVAL = 300
FETCH_TIMEOUT = 10

BINANCE_SPOT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
BINANCE_FUTURES_URL = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"
BINANCE_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
COINBASE_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
KRAKEN_URL = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"


def _fetch_json(url: str) -> dict | list | None:
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "NurtacBot/1.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        print(f"[MACRO] Fetch error {url[:72]}...: {exc}", flush=True)
        return None


def _read_latest_jsonl(path: Path) -> dict:
    if not path.exists():
        return {}
    raw = subprocess.getoutput(f"tail -1 {path}")
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_cache() -> dict:
    if not MACRO_CACHE_FILE.exists():
        return {}
    try:
        payload = json.loads(MACRO_CACHE_FILE.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def fetch_spot_futures_basis() -> dict:
    spot_r = _fetch_json(BINANCE_SPOT_URL)
    futures_r = _fetch_json(BINANCE_FUTURES_URL)

    spot_price = 0.0
    futures_price = 0.0
    if isinstance(spot_r, dict):
        try:
            spot_price = float(spot_r.get("price", 0) or 0)
        except Exception:
            spot_price = 0.0
    if isinstance(futures_r, dict):
        try:
            futures_price = float(futures_r.get("price", 0) or 0)
        except Exception:
            futures_price = 0.0

    if spot_price <= 0 or futures_price <= 0:
        fallback = _read_latest_jsonl(DATA_DIR / "market_context.jsonl")
        spot_price = float(fallback.get("current_price", 0) or 0)
        if spot_price <= 0:
            spot_price = float(_read_latest_jsonl(DATA_DIR / "bias_context.jsonl").get("current_price", 0) or 0)
        futures_price = spot_price + float(fallback.get("basis_usd", 0) or 0) if spot_price > 0 else 0.0

    if spot_price <= 0 or futures_price <= 0:
        return {}

    basis_usd = futures_price - spot_price
    basis_pct = (basis_usd / spot_price) * 100 if spot_price > 0 else 0.0
    basis_signal = "CONTANGO" if basis_pct > 0.1 else "BACKWARDATION" if basis_pct < -0.1 else "NEUTRAL"

    move_type = "ALIGNED"
    if abs(basis_pct) > 0.1:
        move_type = "FUTURES_LED" if abs(basis_pct) > 0.25 else "SPOT_LED"

    return {
        "binance_spot_price": round(spot_price, 2),
        "binance_futures_price": round(futures_price, 2),
        "basis_usd": round(basis_usd, 2),
        "basis_pct": round(basis_pct, 4),
        "basis_signal": basis_signal,
        "move_type": move_type,
    }


def fetch_coinbase_premium(binance_spot: float) -> dict:
    r = _fetch_json(COINBASE_URL)
    if not isinstance(r, dict):
        return {}
    try:
        cb_price = float(((r.get("data") or {}).get("amount")))
    except Exception:
        return {}
    if binance_spot <= 0:
        return {}
    premium_usd = cb_price - binance_spot
    premium_pct = (premium_usd / binance_spot) * 100
    premium_signal = "POSITIVE" if premium_pct > 0.05 else "NEGATIVE" if premium_pct < -0.05 else "NEUTRAL"
    return {
        "coinbase_price": round(cb_price, 2),
        "premium_usd": round(premium_usd, 2),
        "premium_pct": round(premium_pct, 4),
        "premium_signal": premium_signal,
    }


def fetch_multi_exchange_delta(binance_spot: float) -> dict:
    r = _fetch_json(KRAKEN_URL)
    if not isinstance(r, dict):
        return {}
    try:
        result = r.get("result", {})
        pair = next(iter(result.values()), {})
        kr_price = float((pair.get("c") or [0])[0])
    except Exception:
        return {}
    if binance_spot <= 0 or kr_price <= 0:
        return {}
    delta_pct = (kr_price - binance_spot) / binance_spot * 100
    return {
        "kraken_price": round(kr_price, 2),
        "kraken_delta_pct": round(delta_pct, 4),
        "kraken_signal": "WEST_PREMIUM" if delta_pct > 0.05 else "EAST_PREMIUM" if delta_pct < -0.05 else "NEUTRAL",
    }


def fetch_etf_data(btc_spot: float) -> dict:
    # Approximate BTC entitlement per ETF share (2026 Q2). These values drift
    # slowly with fees and share creation/redemption and must be reviewed.
    btc_per_share = {
        "IBIT": 0.000571,
        "FBTC": 0.000877,
        "GBTC": 0.000781,
    }
    results: dict[str, dict] = {}
    total_volume_usd = 0.0
    for ticker, per_share in btc_per_share.items():
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
        r = _fetch_json(url)
        if not isinstance(r, dict):
            continue
        try:
            meta = (r.get("chart") or {}).get("result")[0].get("meta", {})
            price = float(meta.get("regularMarketPrice", 0) or 0)
            prev = float(meta.get("chartPreviousClose", price) or price)
            volume = float(meta.get("regularMarketVolume", 0) or 0)
        except Exception:
            continue
        if price <= 0 or btc_spot <= 0:
            continue
        change_pct = (price - prev) / prev * 100 if prev > 0 else 0.0
        nav_proxy = btc_spot * per_share
        prem_disc_pct = (price - nav_proxy) / nav_proxy * 100 if nav_proxy > 0 else 0.0
        vol_usd = volume * price
        total_volume_usd += vol_usd
        results[ticker] = {
            "price": round(price, 4),
            "change_pct": round(change_pct, 3),
            "volume_shares": int(volume),
            "volume_usd": round(vol_usd, 0),
            "nav_proxy": round(nav_proxy, 4),
            "premium_discount_pct": round(prem_disc_pct, 4),
            "sentiment": "PREMIUM" if prem_disc_pct > 0.2 else "DISCOUNT" if prem_disc_pct < -0.2 else "NEUTRAL",
        }

    if not results:
        return {"etfs": {}, "total_etf_volume_usd": 0.0, "etf_sentiment": "NO_DATA", "etf_signal": "NEUTRAL"}

    premium_count = sum(1 for e in results.values() if e["sentiment"] == "PREMIUM")
    discount_count = sum(1 for e in results.values() if e["sentiment"] == "DISCOUNT")
    avg_change = sum(e["change_pct"] for e in results.values()) / len(results)
    if premium_count >= 2 and avg_change > 0:
        etf_sentiment, etf_signal = "INFLOW_LIKELY", "BULLISH"
    elif discount_count >= 2 or avg_change < -1:
        etf_sentiment, etf_signal = "OUTFLOW_LIKELY", "BEARISH"
    else:
        etf_sentiment, etf_signal = "NEUTRAL", "NEUTRAL"

    return {
        "etfs": results,
        "total_etf_volume_usd": round(total_volume_usd, 0),
        "etf_sentiment": etf_sentiment,
        "etf_signal": etf_signal,
    }


def classify_move(basis: dict, coinbase: dict, kraken: dict, etf: dict) -> dict:
    signals = {
        "basis_signal": basis.get("basis_signal", "NEUTRAL"),
        "coinbase_signal": coinbase.get("premium_signal", "NEUTRAL"),
        "kraken_signal": kraken.get("kraken_signal", "NEUTRAL"),
        "etf_signal": etf.get("etf_signal", "NEUTRAL"),
    }
    bull_score = 0
    bear_score = 0
    reasons: list[str] = []

    if signals["basis_signal"] == "BACKWARDATION":
        bull_score += 2
        reasons.append("Spot öncü (backwardation) -> gerçek alım")
    elif signals["basis_signal"] == "CONTANGO":
        bull_score += 1
        reasons.append("Futures öncü (contango) -> spekülatif alım")

    if signals["coinbase_signal"] == "POSITIVE":
        bull_score += 2
        reasons.append("Coinbase premium pozitif -> ABD kurumsal alım")
    elif signals["coinbase_signal"] == "NEGATIVE":
        bear_score += 2
        reasons.append("Coinbase premium negatif -> ABD kurumsal satış")

    if signals["etf_signal"] == "BULLISH":
        bull_score += 2
        reasons.append("ETF primli -> kurumsal giriş")
    elif signals["etf_signal"] == "BEARISH":
        bear_score += 2
        reasons.append("ETF iskontolu -> kurumsal çıkış")

    if signals["kraken_signal"] == "WEST_PREMIUM":
        bull_score += 1
        reasons.append("Batı borsaları premium -> Avrupa/ABD talebi")
    elif signals["kraken_signal"] == "EAST_PREMIUM":
        bear_score += 1
        reasons.append("Binance premium -> Asya/kripto-native baskısı")

    if bull_score >= 4 and bear_score <= 1:
        move_type, directional_bias, signal_reliability = "GENUINE_BULL", "bullish", "HIGH"
    elif bear_score >= 4 and bull_score <= 1:
        move_type, directional_bias, signal_reliability = "GENUINE_BEAR", "bearish", "HIGH"
    elif bull_score >= 3 and bear_score <= 1:
        move_type, directional_bias, signal_reliability = "SPECULATIVE_BULL", "bullish", "MEDIUM"
    elif bear_score >= 3 and bull_score <= 1:
        move_type, directional_bias, signal_reliability = "SPECULATIVE_BEAR", "bearish", "MEDIUM"
    else:
        move_type, directional_bias, signal_reliability = "MIXED", "neutral", "LOW"

    move_confidence = "HIGH" if abs(bull_score - bear_score) >= 4 else "MEDIUM" if abs(bull_score - bear_score) >= 2 else "LOW"
    return {
        "move_type": move_type,
        "move_confidence": move_confidence,
        "directional_bias": directional_bias,
        "signal_reliability": signal_reliability,
        "bull_score": bull_score,
        "bear_score": bear_score,
        "reason": reasons,
        "raw_signals": signals,
    }


def fetch_spot_netflow_proxy() -> dict:
    raw = subprocess.getoutput("tail -1 data/bias_context.jsonl 2>/dev/null")
    try:
        ctx = json.loads(raw)
    except Exception:
        ctx = {}
    funding = float(ctx.get("funding_rate", 0) or 0)
    oi_usd = float(ctx.get("open_interest_usd", 0) or 0)
    raw2 = subprocess.getoutput("tail -2 data/bias_context.jsonl 2>/dev/null | head -1")
    try:
        ctx2 = json.loads(raw2)
        prev_oi = float(ctx2.get("open_interest_usd", oi_usd) or oi_usd)
    except Exception:
        prev_oi = oi_usd
    oi_change_pct = ((oi_usd - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0.0
    if oi_change_pct > 0.5 and funding > 0.0001:
        return {"funding_rate": round(funding, 8), "oi_usd_m": round(oi_usd / 1e6, 1) if oi_usd > 0 else None, "oi_change_pct": round(oi_change_pct, 4), "netflow_proxy": "INFLOW", "netflow_signal": "SPECULATIVE"}
    if oi_change_pct < -0.5:
        return {"funding_rate": round(funding, 8), "oi_usd_m": round(oi_usd / 1e6, 1) if oi_usd > 0 else None, "oi_change_pct": round(oi_change_pct, 4), "netflow_proxy": "OUTFLOW", "netflow_signal": "DELEVERAGE"}
    return {"funding_rate": round(funding, 8), "oi_usd_m": round(oi_usd / 1e6, 1) if oi_usd > 0 else None, "oi_change_pct": round(oi_change_pct, 4), "netflow_proxy": "NEUTRAL", "netflow_signal": "SPOT_DRIVEN"}


def fetch_top_trader_divergence() -> dict:
    """Compare Binance top-trader positioning with the global account ratio."""
    base = "https://fapi.binance.com/futures/data"
    top_accounts_r = _fetch_json(
        f"{base}/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1"
    )
    top_positions_r = _fetch_json(
        f"{base}/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1"
    )
    global_r = _fetch_json(
        f"{base}/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1"
    )

    top_accounts = (
        top_accounts_r[0]
        if isinstance(top_accounts_r, list) and top_accounts_r
        else top_accounts_r if isinstance(top_accounts_r, dict) else {}
    )
    top_positions = (
        top_positions_r[0]
        if isinstance(top_positions_r, list) and top_positions_r
        else top_positions_r if isinstance(top_positions_r, dict) else {}
    )
    global_ratio_data = (
        global_r[0]
        if isinstance(global_r, list) and global_r
        else global_r if isinstance(global_r, dict) else {}
    )
    if not top_accounts and not global_ratio_data:
        return {}

    def _account_ratio(payload: dict) -> tuple[float, float, float]:
        try:
            long_pct = float(payload.get("longAccount", 0) or 0)
            short_pct = float(payload.get("shortAccount", 0) or 0)
            ratio = long_pct / short_pct if short_pct > 0 else 1.0
            return long_pct, short_pct, ratio
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0, 0.0, 1.0

    top_acc_l, top_acc_s, top_acc_ratio = _account_ratio(top_accounts)
    top_pos_l, _, top_pos_ratio = _account_ratio(top_positions)
    global_l, global_s, global_ratio = _account_ratio(global_ratio_data)
    divergence = round(top_acc_ratio - global_ratio, 4)

    if divergence > 0.4:
        divergence_signal = "TOP_TRADER_LONG_RETAIL_SHORT"
        smart_money_bias = "bullish"
    elif divergence < -0.4:
        divergence_signal = "TOP_TRADER_SHORT_RETAIL_LONG"
        smart_money_bias = "bearish"
    else:
        divergence_signal = "NEUTRAL"
        smart_money_bias = "neutral"

    acc_pos_gap = abs(top_acc_ratio - top_pos_ratio)
    return {
        "top_accounts_long_pct": round(top_acc_l, 4),
        "top_accounts_short_pct": round(top_acc_s, 4),
        "top_accounts_ratio": round(top_acc_ratio, 4),
        "top_positions_long_pct": round(top_pos_l, 4),
        "top_positions_ratio": round(top_pos_ratio, 4),
        "global_long_pct": round(global_l, 4),
        "global_short_pct": round(global_s, 4),
        "global_ratio": round(global_ratio, 4),
        "divergence": divergence,
        "divergence_signal": divergence_signal,
        "smart_money_bias": smart_money_bias,
        "conviction": "LOW" if acc_pos_gap > 0.5 else "HIGH",
        "acc_pos_gap": round(acc_pos_gap, 4),
    }


def fetch_oi_momentum(btc_spot: float) -> dict:
    """Combine the 15-minute OI change with recent local price direction."""
    base = "https://fapi.binance.com"
    oi_r = _fetch_json(f"{base}/fapi/v1/openInterest?symbol=BTCUSDT")
    hist_r = _fetch_json(
        f"{base}/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=3"
    )
    if not isinstance(oi_r, dict):
        return {}

    try:
        oi_btc = float(oi_r.get("openInterest", 0) or 0)
        oi_usd = oi_btc * btc_spot if btc_spot > 0 else 0.0
    except (TypeError, ValueError):
        oi_btc = 0.0
        oi_usd = 0.0

    oi_delta_pct = 0.0
    if isinstance(hist_r, list) and len(hist_r) >= 2:
        try:
            newest = float(hist_r[-1].get("sumOpenInterest", 0) or 0)
            oldest = float(hist_r[0].get("sumOpenInterest", 0) or 0)
            if oldest > 0:
                oi_delta_pct = (newest - oldest) / oldest * 100
        except (AttributeError, TypeError, ValueError, IndexError):
            pass

    price_dir = "neutral"
    try:
        raw = subprocess.getoutput("tail -2 data/bias_context.jsonl 2>/dev/null")
        lines = [line for line in raw.splitlines() if line.strip()]
        if len(lines) >= 2:
            newest_price = float(json.loads(lines[-1]).get("current_price", 0) or 0)
            oldest_price = float(json.loads(lines[-2]).get("current_price", 0) or 0)
            if oldest_price > 0:
                price_change = (newest_price - oldest_price) / oldest_price
                if price_change > 0.001:
                    price_dir = "up"
                elif price_change < -0.001:
                    price_dir = "down"
    except Exception:
        pass

    oi_dir = "up" if oi_delta_pct > 0.1 else "down" if oi_delta_pct < -0.1 else "flat"
    regimes = {
        ("up", "up"): "STRONG_LONG_MOMENTUM",
        ("up", "down"): "SHORT_COVERING",
        ("down", "up"): "STRONG_SHORT_MOMENTUM",
        ("down", "down"): "LONG_UNWINDING",
    }
    return {
        "oi_btc": round(oi_btc, 2),
        "oi_usd_m": round(oi_usd / 1e6, 1) if oi_usd > 0 else None,
        "oi_delta_pct": round(oi_delta_pct, 4),
        "oi_dir": oi_dir,
        "price_dir": price_dir,
        "price_oi_regime": regimes.get((price_dir, oi_dir), "NEUTRAL"),
    }


def fetch_ls_trend() -> dict:
    """Measure the 30-minute trend in Binance global long participation."""
    base = "https://fapi.binance.com/futures/data"
    hist = _fetch_json(
        f"{base}/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=6"
    )
    if not isinstance(hist, list) or len(hist) < 3:
        return {}

    try:
        ratios = [float(row.get("longAccount", 0) or 0) for row in hist]
        change = (sum(ratios[-2:]) / 2) - (sum(ratios[:2]) / 2)
        if change > 0.03:
            trend = "LONGS_INCREASING"
            momentum = "STRONG" if change > 0.07 else "WEAK"
        elif change < -0.03:
            trend = "SHORTS_INCREASING"
            momentum = "STRONG" if change < -0.07 else "WEAK"
        else:
            trend = "STABLE"
            momentum = "WEAK"
        return {
            "ls_trend": trend,
            "ls_momentum": momentum,
            "ls_change_30m": round(change, 4),
            "current_long_pct": round(ratios[-1], 4),
        }
    except (AttributeError, TypeError, ValueError, ZeroDivisionError):
        return {}


def run_once() -> None:
    ts = int(time.time() * 1000)
    cache = _read_cache()

    basis = fetch_spot_futures_basis() or {}
    if not basis:
        basis = cache.get("spot_futures") or {}
    else:
        cached_basis = cache.get("spot_futures") or {}
        if cached_basis and basis.get("basis_signal") == "NEUTRAL" and cached_basis.get("basis_signal") in {"CONTANGO", "BACKWARDATION"}:
            basis = cached_basis
    spot_price = float(basis.get("binance_spot_price", 0.0) or cache.get("current_price", 0.0) or 0.0)

    coinbase = fetch_coinbase_premium(spot_price) or {}
    if not coinbase:
        coinbase = cache.get("coinbase") or {}

    kraken = fetch_multi_exchange_delta(spot_price) or {}
    if not kraken:
        kraken = cache.get("multi_exchange") or {}

    etf = fetch_etf_data(spot_price) or {}
    if not etf.get("etfs"):
        etf = cache.get("etf") or etf

    netflow = fetch_spot_netflow_proxy() or {}
    if netflow.get("netflow_signal") is None:
        netflow = cache.get("netflow_proxy") or netflow

    top_trader = fetch_top_trader_divergence() or cache.get("top_trader") or {}
    oi_momentum = fetch_oi_momentum(spot_price) or cache.get("oi_momentum") or {}
    ls_trend = fetch_ls_trend() or cache.get("ls_trend") or {}

    classification = classify_move(basis, coinbase, kraken, etf)
    smart_bias = top_trader.get("smart_money_bias", "neutral")
    directional_bias = classification.get("directional_bias", "neutral")
    if smart_bias != "neutral" and smart_bias == directional_bias:
        classification["smart_money_confirmation"] = True
        classification["move_confidence"] = "HIGH"
    elif smart_bias != "neutral" and smart_bias != directional_bias:
        classification["smart_money_confirmation"] = False
        if classification.get("move_confidence") == "HIGH":
            classification["move_confidence"] = "MEDIUM"

    result = {
        "engine": "macro_context_engine",
        "ts": ts,
        "current_price": spot_price,
        "spot_futures": basis,
        "coinbase": coinbase,
        "multi_exchange": kraken,
        "etf": etf,
        "netflow_proxy": netflow,
        "top_trader": top_trader,
        "oi_momentum": oi_momentum,
        "ls_trend": ls_trend,
        "classification": classification,
        "move_type": classification.get("move_type"),
        "directional_bias": classification.get("directional_bias"),
        "signal_reliability": classification.get("signal_reliability"),
        "smart_money_bias": smart_bias,
        "smart_money_conviction": top_trader.get("conviction", "HIGH"),
        "divergence_signal": top_trader.get("divergence_signal", "NEUTRAL"),
        "price_oi_regime": oi_momentum.get("price_oi_regime", "NEUTRAL"),
        "ls_trend_signal": ls_trend.get("ls_trend", "STABLE"),
        "etf_signal": etf.get("etf_signal", "NEUTRAL"),
        "coinbase_signal": coinbase.get("premium_signal", "NEUTRAL"),
        "basis_signal": basis.get("basis_signal", "NEUTRAL"),
        "netflow_signal": netflow.get("netflow_signal", "SPOT_DRIVEN"),
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MACRO_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        f"[MACRO] {result['move_type']} | smart_money={smart_bias} "
        f"({top_trader.get('conviction', '?')}) | "
        f"divergence={top_trader.get('divergence_signal', '?')} | "
        f"OI_regime={oi_momentum.get('price_oi_regime', '?')} | "
        f"reliability={result['signal_reliability']}",
        flush=True,
    )


def run_live() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("live", "once"), default="live")
    args = parser.parse_args()

    if args.mode == "once":
        run_once()
        return

    print("[MACRO] Live mode - 5 dakikada bir güncelleme", flush=True)
    while True:
        if HALT_FILE.exists():
            print("[MACRO] SYSTEM_HALT - bekliyor", flush=True)
            time.sleep(60)
            continue
        try:
            run_once()
        except Exception as exc:
            print(f"[MACRO] Error: {exc}", flush=True)
        time.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    run_live()
