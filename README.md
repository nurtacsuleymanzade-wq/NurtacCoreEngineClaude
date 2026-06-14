# NurtacCoreEngineClaude

Algorithmic trading engine — core data layer for BTCUSDT on Binance USDⓈ-M Futures.

## Layers

| Layer | File | Description |
|---|---|---|
| Layer-0 | `main.py` | Live WebSocket → 1S DNA (Candle, Footprint, Depth, Combined) |
| Layer-1 | `rolling_window_engine.py` | Combined 1S → Rolling 3S / 5S / 15S DNA |
| Layer-2 | `aligned_candle_engine.py` | Combined 1S → Aligned 1M / 5M / 15M / 1H / 4H / 1D DNA |
| Layer-3 | `historical_baseline_engine.py` | All DNA files → ATR / VWAP / CVD / Percentile baseline |
| Layer-4 | `detector_engine.py` | 6 parallel detectors → Absorption / Sweep / Exhaustion / Iceberg / Trapped Trader / Initiative Flow labels |
| Layer-5 | `decision_gate.py` | Layer-4 labels → Setup grade (A/B/C) + confluence scoring |
| Layer-6 | `smart_money_engine.py` | 5 timeframes → Market structure (Swings / BOS / CHoCH / MSB / OB / FVG) |

## Layer-0 Output Files

| Output | File |
|---|---|
| Candle DNA | `data/candle_dna_btcusdt.jsonl` |
| Footprint DNA | `data/footprint_dna_btcusdt.jsonl` |
| Depth DNA | `data/depth_dna_btcusdt.jsonl` |
| Combined 1S DNA | `data/combined_1s_dna_btcusdt.jsonl` |

## Layer-2 Output Files

| Output | File |
|---|---|
| Aligned 1M DNA | `data/aligned_1m_candle_dna.jsonl` |
| Aligned 5M DNA | `data/aligned_5m_candle_dna.jsonl` |
| Aligned 15M DNA | `data/aligned_15m_candle_dna.jsonl` |
| Aligned 1H DNA | `data/aligned_1h_candle_dna.jsonl` |
| Aligned 4H DNA | `data/aligned_4h_candle_dna.jsonl` |
| Aligned 1D DNA | `data/aligned_1d_candle_dna.jsonl` |

## Layer-1 Output Files

| Output | File |
|---|---|
| Rolling 3S DNA | `data/rolling_3s_dna.jsonl` |
| Rolling 5S DNA | `data/rolling_5s_dna.jsonl` |
| Rolling 15S DNA | `data/rolling_15s_dna.jsonl` |

## Data Quality Log

All three engines write anomaly and status events to:

| File | Description |
|---|---|
| `data/data_quality_log.jsonl` | Stream disconnects, reconnects, late events, anomalies, gaps, validation failures |

## Layer-6 Smart Money Engine

Reads Layer-0/2/3 JSONL outputs (1S candles, aligned OHLC, baseline ATR) and
runs 5 independent timeframe processors (1S, 1M, 5M, 15M, 1H) in parallel.
Produces market structure analysis — no signals, no trade decisions.

| Output | File |
|---|---|
| 1S Structure | `data/structure_1s.jsonl` |
| 1M Structure | `data/structure_1m.jsonl` |
| 5M Structure | `data/structure_5m.jsonl` |
| 15M Structure | `data/structure_15m.jsonl` |
| 1H Structure | `data/structure_1h.jsonl` |

```bash
# Batch: process all existing data, write last record per timeframe
python3 smart_money_engine.py --mode batch

# Live: warm-up from existing data, then tail-follow all 5 input files
python3 smart_money_engine.py --mode live

# Full output (print every bar)
FULL_PRINT=true python3 smart_money_engine.py --mode live
```

**Structure labels per timeframe:** Swing High/Low (HH/HL/LH/LL/EQH/EQL) ·
Trend (uptrend/downtrend/ranging/unknown + strong/weak/unclear) ·
BOS (micro for 1S, macro for 1M+) · CHoCH Phase 1/2 · MSB ·
Order Blocks (max 3, status: active/mitigated/breaker) ·
Fair Value Gaps (max 5, status: active/mitigated/filled) ·
Equal High/Low levels

## Layer-5 Decision Gate

Reads all 6 Layer-4 detector label files, groups labels by `window_start_ts`,
and classifies each window into a setup grade (A/B/C/none) based on how many
detectors fire in the same direction. No signals or trade decisions — setup
classification only.

| Output | File |
|---|---|
| Gate Decisions | `data/decision_gate_output.jsonl` |

```bash
# Batch: process all existing label data
python3 decision_gate.py --mode batch

# Live: tail all 6 label files, settle 2s per window, then emit
python3 decision_gate.py --mode live

# Full JSON output (including grade=none windows)
FULL_PRINT=true python3 decision_gate.py --mode live
```

**Setup grades:** A (quality≥4.0, confluence≥3) · B (quality≥2.5, confluence≥2) · C (quality≥1.5, confluence≥1)

## Layer-4 Detector Engine

Reads Layer-0/1/2/3 JSONL outputs and runs 6 parallel detectors, each producing
labels only. No Binance API calls. No mock data. No signals or trade decisions.

| Detector | Output File |
|---|---|
| Absorption | `data/labels_absorption.jsonl` |
| Sweep | `data/labels_sweep.jsonl` |
| Exhaustion | `data/labels_exhaustion.jsonl` |
| Iceberg | `data/labels_iceberg.jsonl` |
| Trapped Trader | `data/labels_trapped_trader.jsonl` |
| Initiative Flow | `data/labels_initiative_flow.jsonl` |

```bash
# Batch: process all existing data, write last label per detector
python3 detector_engine.py --mode batch

# Live: batch warm-up then continuous tail-follow
python3 detector_engine.py --mode live

# Full JSON output (all labels including none)
FULL_PRINT=true python3 detector_engine.py --mode live
```

## Layer-3 Historical Baseline + Context Metrics Engine

Reads all Layer-0/1/2 JSONL DNA files (no Binance API calls) and produces
ATR, VWAP, CVD, and percentile/z-score baseline statistics for each timeframe.
Does **not** emit signals or trade decisions.

| Output | File |
|---|---|
| Baseline DNA | `data/historical_baseline_dna.jsonl` |

**Baseline windows:** short=20, medium=100, long=500 records per timeframe.

**Metrics per window:** range, total_volume, buy_volume, sell_volume, delta,
absolute_delta, trade_count, footprint_price_level_count, bid_update_count,
ask_update_count, depth_balance, depth_imbalance, close_price — each with
mean, median, std, p10/p25/p50/p75/p90, latest_percentile, z_score.

**ATR:** True Range 14-period simple average with percentile/z-score context and
status (`normal` / `high` / `extreme_high` / `low` / `extreme_low`).

**VWAP:** Cumulative session VWAP (resets daily at UTC 00:00). Reports
`price_vs_vwap` (`above`/`below`/`at`/`unknown`) and distance in absolute and %.

**CVD:** Cumulative Volume Delta (session-scoped), with `cvd_direction`
(`rising`/`falling`/`flat`).

## Validator Output

| File | Description |
|---|---|
| `data/validation_report.jsonl` | Per-candle comparison: our values vs Binance REST kline (close/high/low, diff %, status) |
| `data/SYSTEM_HALT` | Created by validator on critical divergence; presence halts all engines |

## SYSTEM HALT

When the validator detects a critical price divergence (`data/SYSTEM_HALT` is created):

- All engines (`main.py`, `rolling_window_engine.py`, `aligned_candle_engine.py`) detect the file at the start of their next loop iteration and exit with code 1.
- The halt file contains a JSON object with `ts`, `reason`, and the triggering validation report entry.

**HALT'tan çıkmak için `data/SYSTEM_HALT` dosyasını MANUEL silmen ve sorunu incelemen gerekir.** Dosya silinmeden hiçbir engine yeniden başlatıldığında çalışmaya devam etmez (systemd `Restart=always` olsa bile engine başlar ve hemen tekrar çıkar).

```bash
# HALT nedenini incele
cat data/SYSTEM_HALT

# Validation raporunu incele
tail -20 data/validation_report.jsonl

# Sorun incelendikten sonra HALT'ı kaldır
rm data/SYSTEM_HALT

# Engine'leri yeniden başlat
sudo systemctl restart nurtac-layer0 nurtac-layer1 nurtac-layer2 nurtac-watchdog nurtac-validator
```

## Setup & Run

**Linux / macOS**
```bash
cd NurtacCoreEngineClaude
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Terminal 1 — Layer-0 (live WebSocket streams)
python3 main.py

# Terminal 2 — Layer-1 (rolling windows, reads Layer-0 output)
python3 rolling_window_engine.py

# Optional: full JSON output instead of summary lines
FULL_PRINT=true python3 rolling_window_engine.py

# Terminal 3 — Layer-2 (UTC-aligned candles: 1M, 5M, 15M, 1H, 4H, 1D)
python3 aligned_candle_engine.py
FULL_PRINT=true python3 aligned_candle_engine.py

# Terminal 4 — Watchdog (data freshness + disk + stream health)
python3 watchdog.py

# Terminal 5 — Validator (Binance REST comparison + SYSTEM_HALT)
python3 validator.py

# Terminal 6 — Layer-3 Baseline (batch: one-shot; live: continuous)
python3 historical_baseline_engine.py --mode batch
python3 historical_baseline_engine.py --mode live
FULL_PRINT=true python3 historical_baseline_engine.py --mode batch

# Optional Telegram alerts:
# export TELEGRAM_BOT_TOKEN=<your bot token>
# export TELEGRAM_CHAT_ID=<your chat id>
# python3 watchdog.py   # or validator.py — both read the same env vars
```

**Windows**
```powershell
cd NurtacCoreEngineClaude
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Terminal 1
python main.py

# Terminal 2
python rolling_window_engine.py

# Terminal 3
python aligned_candle_engine.py

# Terminal 4
python watchdog.py

# Terminal 5
python validator.py

# Terminal 6
python historical_baseline_engine.py --mode batch
python historical_baseline_engine.py --mode live
```

## Log Rotation

Rotate all `data/*.jsonl` files to `data/archive/YYYY-MM-DD/` and compress
archives older than 30 days:

```bash
python3 rotate_logs.py
```

Crontab example (UTC midnight daily):
```
0 0 * * * cd /root/NurtacCoreEngineClaude && python3 rotate_logs.py
```

## Deployment

This repository is named **NurtacCoreEngineClaude** on GitHub.
Every update is pushed to the same repository and the same VPS.
The remote URL and push command are configured by the user — no
hardcoded remote is included in the codebase.

### systemd Services (VPS)

Service files are in `deploy/`. Copy them to `/etc/systemd/system/` and enable:

```bash
sudo cp deploy/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nurtac-layer0 nurtac-layer1 nurtac-layer2 nurtac-watchdog nurtac-validator
```

To configure Telegram alerts in the watchdog service, edit
`/etc/systemd/system/nurtac-watchdog.service` and uncomment the
`Environment=` lines with your bot token and chat ID, then:

```bash
sudo systemctl daemon-reload
sudo systemctl restart nurtac-watchdog
```

## Requirements

- Python 3.10+
- Live internet access to Binance futures WebSocket endpoints (Layer-0 only)
- No mock / dummy / hardcoded data is used anywhere

## Architecture notes

**Layer-0**
- Two independent WebSocket connections (trade stream + depth stream)
- Event-time bucketing: each event is bucketed by its own timestamp (T for trades, E for depth), not by wall-clock arrival time
- Grace period of 300 ms after window_end_ts before finalizing each bucket
- Late events (arriving after grace period) are logged and dropped; the already-written JSONL row is never modified
- One-time Binance schema sanity check on first trade and depth message; fatal exit if required fields are missing
- Exponential backoff reconnect: 1 s → 2 s → 4 s → 8 s → 16 s → 30 s (per stream)
- Warm-up: output suppressed until at least one trade AND one depth event received
- All shared state protected by `asyncio.Lock`
- Each JSONL write is followed by `flush()` + `fsync()`
- Disconnects, reconnects, late events, and anomalies written to `data/data_quality_log.jsonl`

**Layer-1**
- Reads `data/combined_1s_dna_btcusdt.jsonl` from the beginning, then follows live (tail -f)
- Single `deque(maxlen=15)` buffer; 3S / 5S / 15S windows are sliced from it without re-reading
- Gap detection: non-consecutive source timestamps logged to `data/data_quality_log.jsonl`
- 11-point validation before each write; failures logged to terminal and quality log
- No external dependencies (stdlib only: json, os, time, collections)

**Layer-2**
- Reads `data/combined_1s_dna_btcusdt.jsonl` from the beginning, then follows live (tail -f)
- Hierarchical state machine: 1S -> 1M -> 5M -> 15M -> 1H -> 4H -> 1D (each level feeds the next)
- UTC-aligned, non-overlapping candles; ALL periods are always emitted (no minimum count threshold)
- Each candle includes `expected_count`, `source_count`, `missing_units`, `data_completeness` fields
- Includes Volume Profile (POC, VAH/VAL 70%, HVN, LVN) for each candle
- 11-point validation before each write; failures logged to terminal and quality log
- No external dependencies (stdlib only: json, os, time)

**Watchdog**
- Polls every 30 seconds
- Checks data freshness (alerts if last record is >15 s old), disk usage (alerts if >85%), and stream health (alerts on unrecovered disconnects)
- Optional Telegram alerts via `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` env vars; 5-minute per-type cooldown to prevent spam
- `requests` library required only for Telegram; watchdog works without it (terminal-only alerts)
