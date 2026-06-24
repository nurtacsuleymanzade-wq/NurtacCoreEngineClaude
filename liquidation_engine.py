#!/usr/bin/env python3
"""Binance futures liquidation, footprint, and order-book context engine."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

import websockets


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
HALT_FILE = DATA_DIR / "SYSTEM_HALT"

FOOTPRINT_FILE = DATA_DIR / "footprint_live.jsonl"
CLUSTER_FILE = DATA_DIR / "liquidation_clusters.jsonl"
REAL_LIQ_FILE = DATA_DIR / "real_liquidations.jsonl"
WALL_FILE = DATA_DIR / "orderbook_walls.jsonl"
WHALE_TRADE_FILE = DATA_DIR / "whale_trades.jsonl"
WHALE_SUMMARY_FILE = DATA_DIR / "whale_trade_summary.jsonl"
WHALE_ORDER_FILE = DATA_DIR / "whale_orders.jsonl"
OB_STATS_FILE = DATA_DIR / "orderbook_stats.jsonl"
CALIBRATION_FILE = DATA_DIR / "liquidation_calibration.json"
HEALTH_FILE = DATA_DIR / "liquidation_health.json"
MARKET_CONTEXT_FILE = DATA_DIR / "market_context.jsonl"

STREAMS = [
    "btcusdt@aggTrade",
    "btcusdt@trade",
    "btcusdt@depth20@100ms",
    "btcusdt@forceOrder",
]
WS_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(STREAMS)

BUCKET_SIZE = 25.0
FOOTPRINT_INTERVAL_S = 5.0
WALL_INTERVAL_S = 10.0
CLUSTER_INTERVAL_S = 30.0
WHALE_SUMMARY_INTERVAL_S = 30.0
OB_STATS_INTERVAL_S = 15.0
CALIBRATION_INTERVAL_S = 7 * 24 * 60 * 60

LEVERAGE_DIST = [
    (5, 0.15),
    (10, 0.30),
    (20, 0.25),
    (50, 0.20),
    (100, 0.10),
]

MIN_WHALE_TRADE_USD = 100_000
MIN_LARGE_TRADE_USD = 500_000
MIN_MEGA_TRADE_USD = 1_000_000


def _sf(value, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else default
    except (TypeError, ValueError):
        return default


def _append_jsonl(path: Path, record: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def _write_health(state: dict, status: str, error: str | None = None) -> None:
    payload = {
        "engine": "liquidation_engine",
        "status": status,
        "connected": bool(state.get("connected", False)),
        "messages_seen": int(state.get("messages_seen", 0)),
        "last_message_ts": state.get("last_message_ts"),
        "last_error": error,
        "updated_at": int(time.time() * 1000),
    }
    temporary = HEALTH_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(HEALTH_FILE)


def price_to_bucket(price: float) -> float:
    """Round a price to the nearest $25 band."""
    return round(price / BUCKET_SIZE) * BUCKET_SIZE


def process_agg_trade(msg: dict, footprint: dict[float, dict]) -> float:
    price = _sf(msg.get("p"), 0.0)
    qty = _sf(msg.get("q"), 0.0)
    if price <= 0 or qty <= 0:
        return 0.0
    bucket = price_to_bucket(price)
    entry = footprint.setdefault(
        bucket, {"buy_vol": 0.0, "sell_vol": 0.0, "trades": 0},
    )
    if bool(msg.get("m", False)):
        entry.update({"sell_vol": _sf(entry.get("sell_vol")) + qty})
    else:
        entry.update({"buy_vol": _sf(entry.get("buy_vol")) + qty})
    entry.update({"trades": int(entry.get("trades", 0)) + 1})
    return price


def process_whale_trade(msg: dict, whale_trade_window: list[dict]) -> dict | None:
    """Persist qualifying aggregate trades and retain the current summary window."""
    price = _sf(msg.get("p"), 0.0)
    qty = _sf(msg.get("q"), 0.0)
    usd_value = price * qty
    if price <= 0 or qty <= 0 or usd_value < MIN_WHALE_TRADE_USD:
        return None
    side = "sell" if bool(msg.get("m", False)) else "buy"
    record = {
        "engine": "liquidation_engine",
        "type": "whale_trade",
        "ts": int(msg.get("T", time.time() * 1000) or time.time() * 1000),
        "side": side,
        "qty_btc": round(qty, 4),
        "price": price,
        "usd_value": round(usd_value, 0),
        "tier": (
            "MEGA" if usd_value >= MIN_MEGA_TRADE_USD else
            "LARGE" if usd_value >= MIN_LARGE_TRADE_USD else "WHALE"
        ),
    }
    _append_jsonl(WHALE_TRADE_FILE, record)
    whale_trade_window.append(record)
    print(
        f"[WHALE TRADE] {side.upper()} ${usd_value / 1000:.0f}K "
        f"@ {price} [{record.get('tier')}]", flush=True,
    )
    return record


def flush_whale_trade_summary(
    whale_trade_window: list[dict], current_price: float,
) -> dict | None:
    if not whale_trade_window:
        return None
    buy_usd = sum(_sf(item.get("usd_value")) for item in whale_trade_window
                  if item.get("side") == "buy")
    sell_usd = sum(_sf(item.get("usd_value")) for item in whale_trade_window
                   if item.get("side") == "sell")
    net_flow = buy_usd - sell_usd
    record = {
        "engine": "liquidation_engine",
        "type": "whale_trade_summary",
        "ts": int(time.time() * 1000),
        "current_price": current_price,
        "window_seconds": int(WHALE_SUMMARY_INTERVAL_S),
        "total_trades": len(whale_trade_window),
        "buy_usd": round(buy_usd, 0),
        "sell_usd": round(sell_usd, 0),
        "net_flow_usd": round(net_flow, 0),
        "whale_pressure": "buy" if net_flow > 0 else "sell" if net_flow < 0 else "neutral",
        "pressure_strength": (
            "STRONG" if abs(net_flow) > 1_000_000 else
            "MEDIUM" if abs(net_flow) > 300_000 else "WEAK"
        ),
        "mega_trades": [item for item in whale_trade_window if item.get("tier") == "MEGA"],
    }
    _append_jsonl(WHALE_SUMMARY_FILE, record)
    whale_trade_window.clear()
    return record


def flush_footprint(footprint: dict[float, dict], current_price: float) -> dict:
    buckets: list[dict] = []
    total_buy = sum(_sf(value.get("buy_vol")) for value in footprint.values())
    total_sell = sum(_sf(value.get("sell_vol")) for value in footprint.values())
    for bucket_price, data in sorted(footprint.items()):
        buy = _sf(data.get("buy_vol"))
        sell = _sf(data.get("sell_vol"))
        if buy + sell <= 0:
            continue
        delta = buy - sell
        buckets.append({
            "price": bucket_price,
            "buy_vol": round(buy, 4),
            "sell_vol": round(sell, 4),
            "delta": round(delta, 4),
            "delta_pct": round(delta / (buy + sell) * 100, 2),
            "trades": int(data.get("trades", 0)),
        })

    visible = [
        bucket for bucket in buckets
        if current_price > 0 and abs(_sf(bucket.get("price")) - current_price) <= 750
    ][:60]
    record = {
        "engine": "liquidation_engine",
        "type": "footprint",
        "ts": int(time.time() * 1000),
        "current_price": current_price,
        "bucket_size": BUCKET_SIZE,
        "total_buy_vol": round(total_buy, 4),
        "total_sell_vol": round(total_sell, 4),
        "net_delta": round(total_buy - total_sell, 4),
        "dominant": "buy" if total_buy > total_sell else "sell",
        "buckets": visible,
        "top_buy_bucket": max(buckets, key=lambda item: _sf(item.get("buy_vol"))).get("price") if buckets else None,
        "top_sell_bucket": max(buckets, key=lambda item: _sf(item.get("sell_vol"))).get("price") if buckets else None,
        "max_delta_bucket": max(buckets, key=lambda item: abs(_sf(item.get("delta")))).get("price") if buckets else None,
    }
    _append_jsonl(FOOTPRINT_FILE, record)
    footprint.clear()
    return record


def calc_liquidation_clusters(
    current_price: float, open_interest_usd: float, funding_rate: float,
) -> dict:
    tier_names = {
        (5,): "LOW",
        (10, 20): "MEDIUM",
        (50, 100): "HIGH",
    }
    clusters_by_tier: dict[str, dict[str, dict[float, float]]] = {
        "LOW": {"long": {}, "short": {}},
        "MEDIUM": {"long": {}, "short": {}},
        "HIGH": {"long": {}, "short": {}},
    }
    if funding_rate > 0:
        long_oi, short_oi = open_interest_usd * 0.55, open_interest_usd * 0.45
    elif funding_rate < 0:
        long_oi, short_oi = open_interest_usd * 0.45, open_interest_usd * 0.55
    else:
        long_oi = short_oi = open_interest_usd * 0.50

    for leverage, pct in LEVERAGE_DIST:
        liquidation_margin = 1.0 / leverage
        maintenance_margin = 0.004
        long_bucket = price_to_bucket(
            current_price * (1 - liquidation_margin + maintenance_margin),
        )
        short_bucket = price_to_bucket(
            current_price * (1 + liquidation_margin - maintenance_margin),
        )
        tier = next(
            (name for leverages, name in tier_names.items() if leverage in leverages),
            "HIGH",
        )
        tier_long = clusters_by_tier.get(tier, {}).get("long", {})
        tier_short = clusters_by_tier.get(tier, {}).get("short", {})
        tier_long[long_bucket] = tier_long.get(long_bucket, 0.0) + long_oi * pct
        tier_short[short_bucket] = tier_short.get(short_bucket, 0.0) + short_oi * pct

    clusters_long: dict[float, float] = {}
    clusters_short: dict[float, float] = {}
    for tier_data in clusters_by_tier.values():
        for price, value in tier_data.get("long", {}).items():
            clusters_long[price] = clusters_long.get(price, 0.0) + value
        for price, value in tier_data.get("short", {}).items():
            clusters_short[price] = clusters_short.get(price, 0.0) + value

    def build_top(side: str, clusters: dict[float, float]) -> list[dict]:
        result: list[dict] = []
        for price, value in sorted(
            clusters.items(), key=lambda item: item[1], reverse=True,
        )[:10]:
            dominant_tier = max(
                ("LOW", "MEDIUM", "HIGH"),
                key=lambda tier: clusters_by_tier.get(tier, {}).get(
                    side, {},
                ).get(price, 0.0),
            )
            result.append({
                "price": price,
                "usd_at_risk": round(value / 1e6, 2),
                "side": side,
                "leverage_tier": dominant_tier,
                "cascade_capable": dominant_tier in ("MEDIUM", "HIGH"),
            })
        return result

    top_long = build_top("long", clusters_long)
    top_short = build_top("short", clusters_short)

    all_clusters = top_long + top_short
    max_usd = max(
        (_sf(item.get("usd_at_risk")) for item in all_clusters),
        default=0.0,
    )
    if max_usd > 0:
        for item in all_clusters:
            intensity = round(_sf(item.get("usd_at_risk")) / max_usd, 3)
            item["intensity"] = intensity
            item["intensity_label"] = (
                "HOT" if intensity >= 0.7 else
                "WARM" if intensity >= 0.4 else
                "COOL"
            )

    liquidity_threshold = 0.7
    hot_long = [
        item for item in top_long
        if _sf(item.get("intensity"), 1.0) >= liquidity_threshold
    ]
    hot_short = [
        item for item in top_short
        if _sf(item.get("intensity"), 1.0) >= liquidity_threshold
    ]
    nearby_long = [
        item for item in hot_long
        if current_price > 0 and abs(_sf(item.get("price")) - current_price) / current_price < 0.05
    ]
    nearby_short = [
        item for item in hot_short
        if current_price > 0 and abs(_sf(item.get("price")) - current_price) / current_price < 0.05
    ]
    nearby = nearby_long + nearby_short
    if any(_sf(item.get("usd_at_risk")) > 50 for item in nearby):
        cascade_risk = "HIGH"
    elif any(_sf(item.get("usd_at_risk")) > 20 for item in nearby):
        cascade_risk = "MEDIUM"
    else:
        cascade_risk = "LOW"
    return {
        "engine": "liquidation_engine",
        "type": "liquidation_clusters",
        "ts": int(time.time() * 1000),
        "current_price": current_price,
        "open_interest_usd_m": round(open_interest_usd / 1e6, 2),
        "funding_rate": funding_rate,
        "long_dominant_price": top_long[0].get("price") if top_long else None,
        "short_dominant_price": top_short[0].get("price") if top_short else None,
        "cascade_risk": cascade_risk,
        "nearby_long_clusters": nearby_long,
        "nearby_short_clusters": nearby_short,
        "top_long_clusters": top_long,
        "top_short_clusters": top_short,
        "hot_long_clusters": hot_long,
        "hot_short_clusters": hot_short,
        "liquidity_threshold": liquidity_threshold,
        "max_cluster_usd_m": round(max_usd, 2),
        "by_tier": {
            tier: {
                side_key: sorted(
                    [
                        {"price": price, "usd_m": round(value / 1e6, 2)}
                        for price, value in clusters_by_tier.get(tier, {}).get(
                            side, {},
                        ).items()
                    ],
                    key=lambda item: _sf(item.get("usd_m")),
                    reverse=True,
                )[:5]
                for side, side_key in (
                    ("long", "long_clusters"),
                    ("short", "short_clusters"),
                )
            }
            for tier in ("HIGH", "MEDIUM")
        },
    }


def process_force_order(msg: dict) -> dict | None:
    order = msg.get("o") or {}
    side = order.get("S")
    qty = _sf(order.get("q"), 0.0)
    price = _sf(order.get("p"), 0.0)
    avg_price = _sf(order.get("ap"), price)
    usd_value = qty * avg_price
    if side not in ("BUY", "SELL") or qty <= 0 or avg_price <= 0:
        return None
    liquidation_type = "long_liquidated" if side == "SELL" else "short_liquidated"
    record = {
        "engine": "liquidation_engine",
        "type": "real_liquidation",
        "ts": int(msg.get("E", time.time() * 1000) or time.time() * 1000),
        "liq_type": liquidation_type,
        "side": side,
        "qty_btc": round(qty, 4),
        "price": price,
        "avg_fill_price": avg_price,
        "usd_value": round(usd_value, 2),
        "size_category": (
            "WHALE" if usd_value > 1_000_000 else
            "LARGE" if usd_value > 100_000 else
            "MEDIUM" if usd_value > 10_000 else "SMALL"
        ),
    }
    _append_jsonl(REAL_LIQ_FILE, record)
    if usd_value > 100_000:
        print(
            f"[LIQ] {liquidation_type.upper()} ${usd_value / 1000:.0f}K "
            f"@ {price} | {record.get('size_category')}", flush=True,
        )
    return record


class OrderBookWallDetector:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.recent_sizes: deque[float] = deque(maxlen=200)
        self.wall_threshold = 10.0

    def update_threshold(self) -> None:
        if len(self.recent_sizes) > 20:
            average = sum(self.recent_sizes) / len(self.recent_sizes)
            self.wall_threshold = max(2.0, min(100.0, average * 8))

    def process_depth(self, msg: dict) -> None:
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        for raw in msg.get("b", []):
            if len(raw) < 2:
                continue
            price, qty = _sf(raw[0]), _sf(raw[1])
            if price > 0 and qty > 0:
                bids.update({price: qty})
                self.recent_sizes.append(qty)
        for raw in msg.get("a", []):
            if len(raw) < 2:
                continue
            price, qty = _sf(raw[0]), _sf(raw[1])
            if price > 0 and qty > 0:
                asks.update({price: qty})
                self.recent_sizes.append(qty)
        self.bids = bids
        self.asks = asks
        self.update_threshold()

    def flush_walls(self, current_price: float) -> dict:
        walls: list[dict] = []
        price_range = current_price * 0.03
        for side, levels in (("bid", self.bids), ("ask", self.asks)):
            for price, qty in levels.items():
                if abs(price - current_price) > price_range or qty < self.wall_threshold:
                    continue
                distance = (
                    (current_price - price) if side == "bid" else (price - current_price)
                ) / current_price * 100 if current_price > 0 else 0.0
                walls.append({
                    "price": price,
                    "side": side,
                    "qty_btc": round(qty, 4),
                    "usd_value": round(qty * price, 0),
                    "distance_pct": round(distance, 3),
                })
        walls.sort(key=lambda item: _sf(item.get("usd_value")), reverse=True)
        top_walls = walls[:20]
        bid_walls = [item for item in top_walls if item.get("side") == "bid"]
        ask_walls = [item for item in top_walls if item.get("side") == "ask"]
        nearest_bid = min(bid_walls, key=lambda item: abs(_sf(item.get("distance_pct"))), default=None)
        nearest_ask = min(ask_walls, key=lambda item: abs(_sf(item.get("distance_pct"))), default=None)
        record = {
            "engine": "liquidation_engine",
            "type": "orderbook_walls",
            "ts": int(time.time() * 1000),
            "current_price": current_price,
            "wall_threshold_btc": round(self.wall_threshold, 2),
            "walls": top_walls,
            "nearest_bid_wall": nearest_bid,
            "nearest_ask_wall": nearest_ask,
            "bid_wall_count": len(bid_walls),
            "ask_wall_count": len(ask_walls),
            "total_bid_wall_usd_m": round(sum(_sf(item.get("usd_value")) for item in bid_walls) / 1e6, 2),
            "total_ask_wall_usd_m": round(sum(_sf(item.get("usd_value")) for item in ask_walls) / 1e6, 2),
        }
        _append_jsonl(WALL_FILE, record)
        return record


class WhaleOrderTracker:
    """Track large top-of-book orders across depth snapshots."""

    WHALE_MULTIPLIER = 15

    def __init__(self) -> None:
        self.prev_bids: dict[float, float] = {}
        self.prev_asks: dict[float, float] = {}
        self.active_whale_orders: dict[str, dict] = {}
        self.whale_order_threshold = 15.0
        self.all_sizes: deque[float] = deque(maxlen=500)
        self.samples_seen = 0

    def update_threshold(self) -> None:
        if len(self.all_sizes) > 100:
            average = sum(self.all_sizes) / len(self.all_sizes)
            self.whale_order_threshold = max(
                5.0, min(200.0, average * self.WHALE_MULTIPLIER),
            )

    def _process_side(
        self, side: str, previous: dict[float, float], current: dict[float, float],
        last_trade_price: float, ts: int,
    ) -> None:
        for price in previous.keys() | current.keys():
            qty = current.get(price, 0.0)
            prev_qty = previous.get(price, 0.0)
            key = f"{side}_{price}"
            if qty >= self.whale_order_threshold and prev_qty < self.whale_order_threshold:
                order = {
                    "side": side,
                    "price": price,
                    "qty_btc": qty,
                    "usd_value": round(qty * price, 0),
                    "first_seen_ts": ts,
                    "status": "ACTIVE",
                }
                self.active_whale_orders[key] = order
                _append_jsonl(WHALE_ORDER_FILE, {
                    **order, "engine": "liquidation_engine", "type": "whale_order",
                    "ts": ts, "event": "APPEARED",
                })
                print(f"[WHALE ORDER] {side.upper()} {qty:.1f} BTC @ {price} APPEARED", flush=True)
            elif prev_qty >= self.whale_order_threshold and qty < self.whale_order_threshold:
                order = self.active_whale_orders.pop(key, None)
                if not order:
                    continue
                distance = (
                    abs(last_trade_price - price) / price * 100
                    if last_trade_price > 0 and price > 0 else 999.0
                )
                event = "FILLED" if distance < 0.05 else "CANCELLED"
                _append_jsonl(WHALE_ORDER_FILE, {
                    **order, "engine": "liquidation_engine", "type": "whale_order",
                    "ts": ts, "event": event,
                    "duration_seconds": round((ts - int(order.get("first_seen_ts", ts))) / 1000),
                    "price_distance_pct": round(distance, 4),
                    "spoofing_suspected": event == "CANCELLED",
                })
                if event == "CANCELLED":
                    print(
                        f"[SPOOF DETECTED] {side.upper()} {_sf(order.get('qty_btc')):.1f} "
                        f"BTC @ {price} CANCELLED (price was {last_trade_price})", flush=True,
                    )

    def process_depth_update(
        self, new_bids: dict[float, float], new_asks: dict[float, float],
        current_price: float, last_trade_price: float,
    ) -> None:
        del current_price  # reserved for future distance filtering
        ts = int(time.time() * 1000)
        sizes = list(new_bids.values()) + list(new_asks.values())
        self.all_sizes.extend(sizes)
        self.samples_seen += len(sizes)
        if self.samples_seen >= 100:
            self.update_threshold()
            self.samples_seen %= 100
        self._process_side("bid", self.prev_bids, new_bids, last_trade_price, ts)
        self._process_side("ask", self.prev_asks, new_asks, last_trade_price, ts)
        self.prev_bids = dict(new_bids)
        self.prev_asks = dict(new_asks)


def flush_orderbook_stats(
    bids: dict[float, float], asks: dict[float, float], current_price: float,
    whale_threshold: float,
) -> dict:
    price_range = current_price * 0.02
    near_bids = [(price, qty) for price, qty in bids.items()
                 if abs(price - current_price) <= price_range]
    near_asks = [(price, qty) for price, qty in asks.items()
                 if abs(price - current_price) <= price_range]
    near_bid_vol = sum(qty for _, qty in near_bids)
    near_ask_vol = sum(qty for _, qty in near_asks)
    whale_bids = {price: qty for price, qty in bids.items() if qty >= whale_threshold}
    whale_asks = {price: qty for price, qty in asks.items() if qty >= whale_threshold}
    wap_bid = (sum(price * qty for price, qty in near_bids) / near_bid_vol
               if near_bid_vol > 0 else current_price)
    wap_ask = (sum(price * qty for price, qty in near_asks) / near_ask_vol
               if near_ask_vol > 0 else current_price)
    total_volume = near_bid_vol + near_ask_vol
    bid_pct = near_bid_vol / total_volume * 100 if total_volume > 0 else 50.0
    top_bids = sorted(bids.items(), key=lambda item: item[1], reverse=True)[:5]
    top_asks = sorted(asks.items(), key=lambda item: item[1], reverse=True)[:5]
    record = {
        "engine": "liquidation_engine", "type": "orderbook_stats",
        "ts": int(time.time() * 1000), "current_price": current_price,
        "near_bid_vol_btc": round(near_bid_vol, 2),
        "near_ask_vol_btc": round(near_ask_vol, 2),
        "bid_pct": round(bid_pct, 1), "ask_pct": round(100 - bid_pct, 1),
        "ob_pressure": "bid" if bid_pct > 55 else "ask" if bid_pct < 45 else "neutral",
        "whale_bid_count": len(whale_bids), "whale_ask_count": len(whale_asks),
        "whale_bid_total_btc": round(sum(whale_bids.values()), 2),
        "whale_ask_total_btc": round(sum(whale_asks.values()), 2),
        "whale_threshold_btc": round(whale_threshold, 2),
        "wap_bid": round(wap_bid, 2), "wap_ask": round(wap_ask, 2),
        "bid_ask_spread": round(wap_ask - wap_bid, 2),
        "top_bid_levels": [{"price": p, "qty": round(q, 2)} for p, q in top_bids],
        "top_ask_levels": [{"price": p, "qty": round(q, 2)} for p, q in top_asks],
        "wall_threshold_btc": round(whale_threshold, 2),
    }
    _append_jsonl(OB_STATS_FILE, record)
    return record


def _latest_market_context() -> tuple[float, float, float]:
    if not MARKET_CONTEXT_FILE.exists():
        return 0.0, 0.0, 0.0
    raw = subprocess.getoutput(f"tail -1 {MARKET_CONTEXT_FILE}").strip()
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return 0.0, 0.0, 0.0
    price = _sf(record.get("current_price"), 0.0)
    oi_btc = _sf(record.get("oi_value"), 0.0)
    funding = _sf(record.get("funding_rate"), 0.0)
    return price, oi_btc * price, funding


def calibrate_leverage_model() -> dict | None:
    if not REAL_LIQ_FILE.exists():
        return None
    raw = subprocess.getoutput(f"tail -500 {REAL_LIQ_FILE}")
    records: list[dict] = []
    for line in raw.splitlines():
        try:
            record = json.loads(line)
            if isinstance(record, dict):
                records.append(record)
        except json.JSONDecodeError:
            pass
    if len(records) < 50:
        return None
    sizes = [_sf(record.get("usd_value")) for record in records]
    whale_pct = sum(1 for size in sizes if size > 500_000) / len(sizes)
    large_pct = sum(1 for size in sizes if 100_000 < size <= 500_000) / len(sizes)
    calibration = {
        "calibrated_at": int(time.time() * 1000),
        "sample_size": len(records),
        "avg_liq_usd": round(sum(sizes) / len(sizes), 0),
        "whale_pct": round(whale_pct * 100, 1),
        "large_pct": round(large_pct * 100, 1),
        "high_leverage_indicator": whale_pct > 0.05,
    }
    temporary = CALIBRATION_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(calibration, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(CALIBRATION_FILE)
    print(
        f"[LIQ] Calibration updated: {len(records)} samples, "
        f"avg=${calibration.get('avg_liq_usd', 0):,.0f}", flush=True,
    )
    return calibration


async def _periodic_outputs(
    footprint: dict[float, dict], walls: OrderBookWallDetector,
    whale_orders: WhaleOrderTracker, whale_trade_window: list[dict], state: dict,
) -> None:
    next_footprint = time.monotonic() + FOOTPRINT_INTERVAL_S
    next_walls = time.monotonic() + WALL_INTERVAL_S
    next_clusters = time.monotonic() + CLUSTER_INTERVAL_S
    next_whale_summary = time.monotonic() + WHALE_SUMMARY_INTERVAL_S
    next_ob_stats = time.monotonic() + OB_STATS_INTERVAL_S
    next_calibration = time.monotonic()
    while not HALT_FILE.exists():
        now = time.monotonic()
        current_price = _sf(state.get("current_price"), 0.0)
        if now >= next_footprint and current_price > 0:
            flush_footprint(footprint, current_price)
            next_footprint = now + FOOTPRINT_INTERVAL_S
        if now >= next_walls and current_price > 0:
            walls.flush_walls(current_price)
            next_walls = now + WALL_INTERVAL_S
        if now >= next_clusters:
            context_price, oi_usd, funding = _latest_market_context()
            cluster_price = current_price or context_price
            if cluster_price > 0 and oi_usd > 0:
                _append_jsonl(
                    CLUSTER_FILE,
                    calc_liquidation_clusters(cluster_price, oi_usd, funding),
                )
            next_clusters = now + CLUSTER_INTERVAL_S
        if now >= next_whale_summary:
            flush_whale_trade_summary(whale_trade_window, current_price)
            next_whale_summary = now + WHALE_SUMMARY_INTERVAL_S
        if now >= next_ob_stats and current_price > 0:
            flush_orderbook_stats(
                walls.bids, walls.asks, current_price,
                whale_orders.whale_order_threshold,
            )
            next_ob_stats = now + OB_STATS_INTERVAL_S
        if now >= next_calibration:
            calibrate_leverage_model()
            next_calibration = now + CALIBRATION_INTERVAL_S
        await asyncio.sleep(0.2)


async def _websocket_loop(
    footprint: dict[float, dict], walls: OrderBookWallDetector,
    whale_orders: WhaleOrderTracker, whale_trade_window: list[dict], state: dict,
) -> None:
    backoff = 1
    while not HALT_FILE.exists():
        try:
            async with websockets.connect(
                WS_URL, ping_interval=20, ping_timeout=10, max_queue=1024,
            ) as websocket:
                print("[LIQ] Binance combined WebSocket connected", flush=True)
                state.update({"connected": True})
                _write_health(state, "OK")
                backoff = 1
                async for raw in websocket:
                    if HALT_FILE.exists():
                        return
                    try:
                        envelope = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    stream = str(envelope.get("stream", ""))
                    stream_lower = stream.lower()
                    data = envelope.get("data") or {}
                    event_type = str(data.get("e", "")).lower()
                    state.update({
                        "messages_seen": int(state.get("messages_seen", 0)) + 1,
                        "last_message_ts": int(time.time() * 1000),
                    })
                    if int(state.get("messages_seen", 0)) == 1:
                        _write_health(state, "OK")
                    if event_type in ("aggtrade", "trade") or stream_lower.endswith(
                        ("@aggtrade", "@trade")
                    ):
                        price = process_agg_trade(data, footprint)
                        process_whale_trade(data, whale_trade_window)
                        if price > 0:
                            state.update({"current_price": price})
                    elif event_type == "depthupdate" or "@depth20" in stream_lower:
                        walls.process_depth(data)
                        if _sf(state.get("current_price")) <= 0 and walls.bids and walls.asks:
                            state.update({
                                "current_price": (max(walls.bids) + min(walls.asks)) / 2,
                            })
                        whale_orders.process_depth_update(
                            walls.bids, walls.asks,
                            _sf(state.get("current_price")),
                            _sf(state.get("current_price")),
                        )
                    elif event_type == "forceorder" or stream_lower.endswith("@forceorder"):
                        process_force_order(data)
        except Exception as error:
            state.update({"connected": False})
            _write_health(state, "ERROR", str(error))
            print(f"[LIQ] WebSocket error: {error}; retry in {backoff}s", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def run_live() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    footprint: dict[float, dict] = {}
    walls = OrderBookWallDetector()
    whale_orders = WhaleOrderTracker()
    whale_trade_window: list[dict] = []
    state: dict = {
        "current_price": 0.0,
        "connected": False,
        "messages_seen": 0,
        "last_message_ts": None,
    }
    await asyncio.gather(
        _websocket_loop(footprint, walls, whale_orders, whale_trade_window, state),
        _periodic_outputs(
            footprint, walls, whale_orders, whale_trade_window, state,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Liquidation context engine")
    parser.add_argument("--mode", choices=["live"], default="live")
    parser.parse_args()
    asyncio.run(run_live())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
