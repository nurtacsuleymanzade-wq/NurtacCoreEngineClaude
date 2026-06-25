#!/usr/bin/env python3
import os, json, time, urllib.request, urllib.parse, subprocess
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"

STATE = DATA / "curated_telegram_state.json"
LIFECYCLE = DATA / "setup_lifecycle.jsonl"
SUMMARY = DATA / "setup_intelligence_summary.json"
DNA = DATA / "setup_dna_latest.jsonl"
MAX_SETUP_AGE_SECONDS = 180  # Telegram freshness gate: only live setups

def now():
    return int(time.time())

def utc_from_ms(ts):
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts / 1000
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))
    except Exception:
        return "unknown"

def load_state():
    if STATE.exists():
        try:
            return json.loads(STATE.read_text())
        except Exception:
            pass
    return {"sent": {}, "last_summary_hour": None, "cutoff_ms": int(time.time()*1000), "summary_sent_once": False}

def ensure_state_defaults(st):
    st.setdefault("sent", {})
    st.setdefault("last_summary_hour", None)
    st.setdefault("cutoff_ms", int(time.time()*1000))
    st.setdefault("summary_sent_once", False)
    return st

def save_state(st):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, indent=2, ensure_ascii=False))
    tmp.replace(STATE)

def env():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                token = line.split("=", 1)[1].strip()
            if line.startswith("TELEGRAM_CHAT_ID="):
                chat = line.split("=", 1)[1].strip()
    return token, chat

def send(text):
    token, chat = env()
    if not token or not chat:
        print("[CURATED TG NO ENV]\n" + text)
        return False
    try:
        body = text[:3900]
        data = urllib.parse.urlencode({"chat_id": chat, "text": body}).encode()
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=10).read()
        return True
    except Exception as e:
        print("[CURATED TG ERROR]", repr(e))
        print(text)
        return False

def read_jsonl_tail(path, n=10000):
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

def latest_lifecycles():
    latest = {}
    for row in read_jsonl_tail(LIFECYCLE, 15000):
        sid = row.get("setup_id")
        if sid:
            latest[sid] = row
    return latest

def get_birth(lc):
    return lc.get("birth") or {}

def get_raw(lc):
    return get_birth(lc).get("raw_setup") or {}

def direction_score(lc):
    birth = get_birth(lc)
    scores = birth.get("scores") or {}
    direction = lc.get("direction")
    try:
        if direction == "long":
            return float(scores.get("long_score") or 0)
        if direction == "short":
            return float(scores.get("short_score") or 0)
    except Exception:
        return 0.0
    return 0.0


def setup_birth_ts_ms(lc):
    raw = get_raw(lc)
    ts = raw.get("window_start_ts") or raw.get("qualification_ts") or raw.get("ts") or 0
    try:
        ts = int(ts)
    except Exception:
        return 0
    if ts and ts < 10_000_000_000:
        ts *= 1000
    return ts

def setup_age_seconds(lc):
    ts = setup_birth_ts_ms(lc)
    if not ts:
        return None
    return int((time.time()*1000 - ts) / 1000)

def is_fresh_setup(lc):
    age = setup_age_seconds(lc)
    if age is None:
        return False
    return 0 <= age <= MAX_SETUP_AGE_SECONDS

def setup_quality_tier(lc):
    """
    Telegram reporting tier. Trade decision değildir.
    L1_LOW        = 4.0+
    L2_MEDIUM     = 6.0+
    L3_GOOD_A+    = 8.0+
    L4_PREMIUM    = 11.0+
    """
    score = direction_score(lc)
    if score >= 11.0:
        return "L4_PREMIUM"
    if score >= 8.0:
        return "L3_GOOD_A_PLUS"
    if score >= 6.0:
        return "L2_MEDIUM"
    if score >= 4.0:
        return "L1_LOW"
    return "L0_WEAK"

def is_a_plus(lc):
    """
    Telegram filtresi:
    A+ = direction score >= 8.0.
    Bu trade kararı/sinyal değildir; sadece yüksek kaliteli setup raporlama filtresidir.
    """
    return direction_score(lc) >= 8.0

def top_layers(lc):
    return (get_birth(lc).get("top_contributors") or [])[:6]

def layer_text(lc):
    items = top_layers(lc)
    if not items:
        return "Katman katkısı yok"
    return "\n".join([f"• {x.get('layer')}: +{x.get('contribution')}" for x in items])

def trigger_text(lc):
    triggers = get_birth(lc).get("true_triggers") or []
    if not triggers:
        return "Trigger yok"
    return "\n".join([f"✅ {x}" for x in triggers])

def prices(lc):
    raw = get_raw(lc)
    birth = get_birth(lc)
    entry = raw.get("entry") or birth.get("entry") or {}
    sl = raw.get("sl") or (birth.get("risk") or {}).get("sl") or {}
    tp1 = raw.get("tp1") or (birth.get("targets") or {}).get("tp1") or {}
    tp2 = raw.get("tp2") or (birth.get("targets") or {}).get("tp2") or {}
    tp3 = raw.get("tp3") or (birth.get("targets") or {}).get("tp3") or {}
    return entry.get("price"), sl.get("price"), tp1.get("price"), tp2.get("price"), tp3.get("price")

def format_a_plus_birth(lc):
    raw = get_raw(lc)
    birth = get_birth(lc)
    ctx = birth.get("context") or {}
    scores = birth.get("scores") or {}
    entry, sl, tp1, tp2, tp3 = prices(lc)
    tier = setup_quality_tier(lc)
    score = direction_score(lc)

    return f"""🟢 A+ SETUP AÇILDI

Setup: {lc.get("setup_id")}
Kalite: {tier}
Direction score: {score}
Setup age: {setup_age_seconds(lc)} sn
Symbol: {raw.get("symbol", "BTCUSDT")}
Saat: {utc_from_ms(raw.get("window_start_ts"))}
Timeframe: {raw.get("dominant_timeframe") or (raw.get("entry") or {}).get("timeframe_context") or "unknown"}
Yön: {(lc.get("direction") or "").upper()}
Fiyat/Entry: {entry}
SL: {sl}
TP1/TP2/TP3: {tp1} / {tp2} / {tp3}

Skor:
Long: {scores.get("long_score")}
Short: {scores.get("short_score")}
Gap: {scores.get("score_gap")}

Neden A+?
{trigger_text(lc)}

Nasıl beslendi?
{layer_text(lc)}

Context:
Gate: {ctx.get("gate_grade")}
1S: {ctx.get("trend_1s")}
1M: {ctx.get("trend_1m")}
5M: {ctx.get("trend_5m")}
BOS: {ctx.get("micro_bos") or ctx.get("macro_bos")}
OB/FVG: {ctx.get("active_ob_count")} / {ctx.get("active_fvg_count")}

Devam takibi:
• Observer sonucu bekleniyor
• TP/SL sonucu bekleniyor
• Aynı setup_id üzerinden takip edilecek
"""

def format_outcome(lc):
    sid = lc.get("setup_id")
    state = lc.get("current_state")
    paper = lc.get("paper") or {}
    closed = paper.get("closed") or lc.get("result") or {}
    obs = (lc.get("observer") or {}).get("last_event") or {}

    if state == "paper_closed" or closed:
        outcome = closed.get("outcome")
        hit = "✅ HEDEFE ULAŞTI" if str(outcome).upper() == "WIN" else "❌ HEDEFE ULAŞAMADI"
        return f"""📊 A+ SETUP SONUÇ

Setup: {sid}
Durum: {hit}
Outcome: {outcome}
PnL R: {closed.get("pnl_r")}
MFE: {closed.get("mfe")}
MAE: {closed.get("mae")}
Süre: {closed.get("duration_seconds")} sn
Kapanış nedeni: {closed.get("close_reason")}
"""

    if state in ("invalidated", "expired", "waiting_timeout"):
        return f"""⚠️ A+ SETUP AKIBET

Setup: {sid}
Durum: {state}
Son observer event: {obs.get("event_type")}
Fiyat: {obs.get("current_price")}
Detay: {obs.get("details")}
"""

    if state == "qualified":
        q = lc.get("qualification") or {}
        return f"""✅ A+ SETUP QUALIFIED

Setup: {sid}
Yön: {q.get("direction") or lc.get("direction")}
Qualification ts: {q.get("qualification_ts")}
Time to qualify: {q.get("time_to_qualify_seconds")} sn
"""

    return None


def utc4_from_ms(ts):
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts / 1000
        ts += 4 * 3600
        return time.strftime("%Y-%m-%d %H:%M:%S UTC+4", time.gmtime(ts))
    except Exception:
        return "unknown"

def latest_btc_price():
    candidates = [
        DATA / "combined_1s_dna_btcusdt.jsonl",
        DATA / "one_second_combined_dna.jsonl",
        DATA / "rolling_3s_dna.jsonl",
        DATA / "rolling_5s_dna.jsonl",
        DATA / "rolling_15s_dna.jsonl",
        DATA / "aligned_1m_candle_dna.jsonl",
    ]

    def pick_price(row):
        keys = [
            "close", "price", "last_price", "last_trade_price",
            "mark_price", "index_price", "best_bid", "best_ask"
        ]
        for k in keys:
            v = row.get(k)
            if v is not None:
                return v

        for nested in ("ohlc", "candle", "price_dna", "trade_dna", "summary"):
            obj = row.get(nested)
            if isinstance(obj, dict):
                for k in keys:
                    v = obj.get(k)
                    if v is not None:
                        return v
        return None

    for path in candidates:
        rows = read_jsonl_tail(path, 20)
        for row in reversed(rows):
            price = pick_price(row)
            if price is not None:
                return price
    return None


def row_setup_ts(row):
    sid = str(row.get("setup_id") or "")
    try:
        if sid and sid.split("_")[0].isdigit():
            return int(sid.split("_")[0])
    except Exception:
        pass
    return row.get("setup_ts") or row.get("opened_ts") or row.get("window_start_ts") or 0


def score_from_dna(row):
    try:
        if row.get("direction") == "long":
            return float(row.get("score_long") or 0)
        if row.get("direction") == "short":
            return float(row.get("score_short") or 0)
    except Exception:
        return 0.0
    return 0.0

def tier_from_score(score):
    if score >= 11.0:
        return "L4_PREMIUM"
    if score >= 8.0:
        return "L3_GOOD_A_PLUS"
    if score >= 6.0:
        return "L2_MEDIUM"
    if score >= 4.0:
        return "L1_LOW"
    return "L0_WEAK"

def summary_distribution_blocks(dna_rows):
    quality = {
        "L0_WEAK": 0,
        "L1_LOW": 0,
        "L2_MEDIUM": 0,
        "L3_GOOD_A_PLUS": 0,
        "L4_PREMIUM": 0,
    }
    direction = {"long": 0, "short": 0, "unknown": 0}
    lifecycle = {}
    layers = {}

    for row in dna_rows:
        score = score_from_dna(row)
        tier = tier_from_score(score)
        quality[tier] = quality.get(tier, 0) + 1

        d = row.get("direction") or "unknown"
        direction[d] = direction.get(d, 0) + 1

        state = row.get("state") or "unknown"
        lifecycle[state] = lifecycle.get(state, 0) + 1

        for item in row.get("top_contributors") or []:
            layer = item.get("layer")
            if layer:
                layers[layer] = layers.get(layer, 0) + 1

    quality_text = (
        "🏆 Kalite Dağılımı\n"
        f"L0_WEAK (<4.0): {quality.get('L0_WEAK', 0)}\n"
        f"L1_LOW (4.0+): {quality.get('L1_LOW', 0)}\n"
        f"L2_MEDIUM (6.0+): {quality.get('L2_MEDIUM', 0)}\n"
        f"L3_GOOD_A+ (8.0+): {quality.get('L3_GOOD_A_PLUS', 0)}\n"
        f"L4_PREMIUM (11.0+): {quality.get('L4_PREMIUM', 0)}"
    )

    direction_text = (
        "📈 Direction Dağılımı\n"
        f"LONG: {direction.get('long', 0)}\n"
        f"SHORT: {direction.get('short', 0)}\n"
        f"UNKNOWN: {direction.get('unknown', 0)}"
    )

    lifecycle_lines = [f"{k}: {v}" for k, v in sorted(lifecycle.items())]
    lifecycle_text = "📌 Lifecycle Dağılımı\n" + ("\n".join(lifecycle_lines) if lifecycle_lines else "Yok")

    layer_lines = [
        f"{k}: {v}" for k, v in sorted(layers.items(), key=lambda x: x[1], reverse=True)
    ]
    layer_text = "🧠 Katman Katkı Sayısı\n" + ("\n".join(layer_lines[:12]) if layer_lines else "Yok")

    return quality_text, direction_text, lifecycle_text, layer_text

def format_summary():
    if not SUMMARY.exists():
        return None
    try:
        s = json.loads(SUMMARY.read_text())
    except Exception:
        return None

    total = s.get("total_setups")
    closed = s.get("closed_count")
    win_rate = s.get("win_rate")
    states = s.get("result_counts") or {}

    dna_rows = read_jsonl_tail(DNA, 5000)
    quality_text, direction_text, lifecycle_text, layer_text = summary_distribution_blocks(dna_rows)
    recent = []
    for row in dna_rows[-5:]:
        score = row.get("score_long") if row.get("direction") == "long" else row.get("score_short")
        layers = row.get("top_contributors") or []
        layers_txt = ", ".join([f"{x.get('layer')}+{x.get('contribution')}" for x in layers[:4]])
        setup_time = utc4_from_ms(row_setup_ts(row))
        recent.append(f"- {setup_time} | BTC={latest_btc_price()} | {row.get('setup_id')} | {row.get('direction')} | score={score} | {row.get('state')} | {layers_txt}")

    return f"""📈 SETUP ÖZET RAPOR

Rapor saati: {time.strftime("%Y-%m-%d %H:%M:%S UTC+4", time.gmtime(time.time()+4*3600))}
Güncel BTC: {latest_btc_price()}

Toplam setup: {total}
Kapanan: {closed}
Win rate: {win_rate}
Durumlar: {json.dumps(states, ensure_ascii=False)}

{quality_text}

{direction_text}

{lifecycle_text}

{layer_text}

Son setup beslenmeleri:
{chr(10).join(recent) if recent else "Yok"}

Kalite filtresi:
L1_LOW=4.0+ | L2_MEDIUM=6.0+ | L3_GOOD_A+=8.0+ | L4_PREMIUM=11.0+
Telegram sadece direction score >= 8.0 setup gönderir.

Not: Bu rapor karar üretmez; sadece setup geçmişini açıklar.
"""

def main():
    st = ensure_state_defaults(load_state())
    lcs = latest_lifecycles()

    for sid, lc in lcs.items():
        if not is_a_plus(lc):
            continue

        # LIVE ONLY: geçmiş setup'ı canlı sinyal gibi Telegram'a gönderme.
        if not is_fresh_setup(lc):
            continue

        raw = get_raw(lc)
        birth_ts = raw.get("window_start_ts") or 0

        try:
            birth_ts = int(birth_ts)
        except Exception:
            birth_ts = 0

        # Reporter kurulduktan önce oluşmuş setup'ları Telegram'a gönderme.
        # cutoff_ms state içinde kalıcıdır; timer her çalıştığında değişmez.
        if birth_ts and birth_ts < int(st.get("cutoff_ms") or 0):
            continue

        birth_key = f"{sid}:aplus_birth_v2_score8"
        if not st["sent"].get(birth_key):
            if send(format_a_plus_birth(lc)):
                st["sent"][birth_key] = now()

        state = lc.get("current_state")
        if state in ("qualified", "invalidated", "expired", "waiting_timeout", "paper_closed"):
            out_key = f"{sid}:outcome_v2:{state}"
            if not st["sent"].get(out_key):
                text = format_outcome(lc)
                if text and send(text):
                    st["sent"][out_key] = now()

    summary_key = "summary_marker_score_tier"
    try:
        summary = json.loads(SUMMARY.read_text()) if SUMMARY.exists() else {}
    except Exception:
        summary = {}

    marker = f"{summary.get('total_setups')}:{summary.get('closed_count')}:{summary.get('win_rate')}"
    last_marker = st.get(summary_key)
    current_hour = int(time.strftime("%H", time.gmtime()))
    daily_window = current_hour in (0, 12)

    # Özet spam önleme:
    # - İlk kurulumdan sonra sadece 1 kez
    # - Sonra yalnızca closed_count / win_rate değişirse
    # - Ya da UTC 00/12 saatinde bir defa
    closed_marker = f"{summary.get('closed_count')}:{summary.get('win_rate')}"
    last_closed_marker = st.get("summary_closed_marker")
    hour_key = time.strftime("%Y-%m-%d-%H", time.gmtime())
    already_sent_this_hour = st.get("last_summary_hour") == hour_key

    should_send_summary = False
    if not st.get("summary_sent_once"):
        should_send_summary = True
    elif closed_marker != last_closed_marker:
        should_send_summary = True
    elif daily_window and not already_sent_this_hour:
        should_send_summary = True

    # Closed yoksa özet spam yapma; sadece ilk kurulumda bir kez izin ver.
    if should_send_summary:
        if int(summary.get("closed_count") or 0) == 0 and st.get("summary_sent_once"):
            should_send_summary = False

    if should_send_summary:
        text = format_summary()
        if text and send(text):
            st[summary_key] = marker
            st["summary_closed_marker"] = closed_marker
            st["summary_sent_once"] = True
            st["last_summary_hour"] = hour_key

    save_state(st)
    print(json.dumps({
        "status": "ok",
        "checked_setups": len(lcs),
        "a_plus_rule": "direction_score >= 8.0",
        "tiers": {
            "L1_LOW": "4.0+",
            "L2_MEDIUM": "6.0+",
            "L3_GOOD_A_PLUS": "8.0+",
            "L4_PREMIUM": "11.0+"
        },
        "sent_keys": len(st.get("sent", {}))
    }, indent=2))

if __name__ == "__main__":
    main()
