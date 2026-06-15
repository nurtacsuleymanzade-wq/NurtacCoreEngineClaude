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
| Layer-7 | `evidence_engine.py` | Evidence scoring (candle/gate/structure/detector/baseline/market_context/scenario) → Evidence stream + Setup generator |
| Layer-9 | `scenario_engine.py` | 9 scenario detectors → Market behavior pattern recognition (Scenario stream + Memory) |
| Layer-10 | `observer_engine.py` | Observes L7 setups, runs state machines per setup, qualifies via 7 transition events → Observations + Qualified setups |
| Layer-11 | `historical_outcome_engine.py` | Normalizes events from all signal sources, opens forward-horizon observations, measures outcomes, writes calibration profiles — no scoring |
| Layer-12 | `paper_trade_engine.py` | Reads Layer-10 qualified setups, simulates paper trades against live 1S price feed, tracks TP1/TP2/TP3/SL milestones, writes per-trade records and cumulative stats |
| Layer-13 | `telegram_reporter.py` | Monitors all signal/trade/system events from lower layers, sends curated Telegram reports with rate limiting and graceful fallback to terminal |
| Market Context | `market_context_engine.py` | Binance Public API → OI / Funding / L/S / Taker / Liquidation heatmap → Bias context |

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

## Market Context Engine

Polls Binance Public REST APIs (no API key) and the liquidation WebSocket to
produce macro bias context. Updates every 30s (OI) and 5m (funding/L/S/taker).

| Output | File |
|---|---|
| Market metrics snapshot | `data/market_context.jsonl` |
| Liquidation events | `data/liquidation_events.jsonl` |
| Liquidation heatmap | `data/liquidation_heatmap.jsonl` |
| Bias context | `data/bias_context.jsonl` |

```bash
python3 market_context_engine.py --mode batch
python3 market_context_engine.py --mode live
FULL_PRINT=true python3 market_context_engine.py --mode live
```

**Sources:** OI change vs price direction · Funding rate (contrarian) ·
Global L/S ratio (contrarian) · Top Trader L/S ratio (follow) ·
Taker buy/sell (from klines) · Futures delta alignment ·
Liquidation stream cascade detection · Liquidation heatmap max pain

**Bias integration in Layer-7:** `evidence_engine.py` reads the latest
`bias_context.jsonl` record; `dominant_bias=="long"` adds +1.0/+2.0 to
`long_score`; `dominant_bias=="short"` adds to `short_score`.

## Layer-10 Observer + Setup Qualifier

Watches Layer-7 setups from `data/setups.jsonl`, runs a per-setup state machine,
and qualifies them when all transition and confirmation criteria are met.
Triggered by each new 1S bar. No signals, no orders.

| Output | File |
|---|---|
| Observation stream | `data/observations.jsonl` |
| Qualified setups | `data/qualified_setups.jsonl` |

```bash
python3 observer_engine.py --mode batch
python3 observer_engine.py --mode live
FULL_PRINT=true python3 observer_engine.py --mode live
```

**State machine per setup:** WAITING → DEVELOPING → QUALIFYING → QUALIFIED / INVALIDATED / EXPIRED

**Transition events detected:** RECLAIM_VALUE · RECLAIM_POC · PULLBACK_IN_PROGRESS ·
PULLBACK_COMPLETE · HOLD_CONFIRMED · BREAKOUT_CONFIRMED · BREAKOUT_WEAK ·
FOLLOW_THROUGH_STRONG · FOLLOW_THROUGH_WEAK · REJECTION_CONFIRMED · FAILED_BREAKOUT

**Qualification criteria (F1–F5):** delta aligned · structure aligned (trend/BOS) ·
location valid · scenario direction aligned · bias aligned

**Invalidation triggers:** close beyond SL · FAILED_BREAKOUT · confirmed opposing scenario

**Timeouts:** WAITING 60s · DEVELOPING 120s · global lifetime 300s

**Max concurrent setups:** 10 (oldest expires if exceeded)

## Layer-13 Telegram Reporter

Monitors all signal, trade, structural, and system events across lower layers.
Sends curated Telegram messages with rate limiting, retry logic, and cooldowns.
If TELEGRAM_BOT_TOKEN/CHAT_ID not configured, messages print to terminal instead.

| Output | File |
|---|---|
| Message log (append) | `data/telegram_log.jsonl` |
| Health snapshot | `data/telegram_health.json` |

```bash
python3 telegram_reporter.py --mode batch
python3 telegram_reporter.py --mode live
```

**Configuration:**
```bash
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id
```

Or create `.env` file (excluded from git):
```
TELEGRAM_BOT_TOKEN=xxx
TELEGRAM_CHAT_ID=yyy
```

**Message types:** Setup opened · Trade close (WIN/LOSS/BREAKEVEN) · Structural events (1M CHoCH/MSB/macro BOS) · Gate A setup · Scenario change · Hourly summary · Daily summary · System halt · Watchdog alarm · Validator alert

**Rate limiting:** 1 msg/sec, queue max 50, retry 3x (2s/4s/8s)

**Cooldowns:** Structure 5min · Gate A 30s · Scenario 3min · Watchdog 5min · Validator 5min

**Message queue:** Async, per-message type cooldown tracking, automatic oldest removal on overflow

**Fallback:** If Telegram not configured, all formatted messages print to terminal with `[TELEGRAM] NOT CONFIGURED` prefix — program continues normally

**Restart recovery:** Reads `telegram_log.jsonl` to rebuild processed event IDs on startup

## Layer-12 Paper Trade Lifecycle Engine

Reads qualified setups from Layer-10, simulates paper trades against the live 1S price feed,
tracks TP1/TP2/TP3 milestones and SL, applies breakeven stop mechanics, and computes PnL.
No real orders. No real money. No Binance API.

| Output | File |
|---|---|
| Closed trades (append) | `data/paper_trades.jsonl` |
| Open trades snapshot | `data/paper_trades_open.json` |
| Cumulative summary | `data/paper_trade_summary.json` |
| Health snapshot | `data/paper_trade_health.json` |

```bash
python3 paper_trade_engine.py --mode batch
python3 paper_trade_engine.py --mode live
FULL_PRINT=true python3 paper_trade_engine.py --mode live
```

**Trade lifecycle:** Open → TP1 hit (stop→breakeven) → TP2 hit → TP3 hit (WIN) or SL hit (LOSS) or breakeven stop (BREAKEVEN) or timeout

**Close reasons:** `sl_hit` · `tp3_hit` · `breakeven_stop` · `timeout`

**Outcomes:** `win` · `loss` · `breakeven` · `timeout_win` · `timeout_loss` · `timeout_flat`

**Timeouts:** flash/1S=300bars · 1M=1800bars · 5M=5400bars · default=900bars

**Max concurrent trades:** 3

**Summary breakdowns:** by_direction · by_setup_type · by_timeframe · by_scenario · by_gate_grade · by_close_reason

**PnL metrics:** pnl_r (R-multiples) · pnl_pct · max_favorable_r · max_adverse_r · profit_factor · consecutive win/loss streaks

**Restart recovery:** Reads closed trades from `paper_trades.jsonl` to rebuild processed setup IDs; never re-opens an already-processed setup.

## Layer-11 Historical Outcome Engine

Reads all signal sources (6 detectors, evidence, 3 structure files, scenarios, observer),
opens a forward-horizon observation per qualifying event, then measures what actually happened
at 30s / 60s / 180s / 300s / 900s / 3600s. Writes calibration profiles grouped by pattern.
No scoring, no confidence, no thresholds, no signals.

| Output | File |
|---|---|
| Completed observations | `data/historical_outcome_observations.jsonl` |
| Open positions (for restart) | `data/historical_outcome_open_positions.json` |
| Calibration profiles | `data/calibration_profiles.json` |
| Health snapshot | `data/historical_outcome_health.json` |
| Errors | `data/historical_outcome_errors.jsonl` |

```bash
python3 historical_outcome_engine.py --mode batch
python3 historical_outcome_engine.py --mode live
FULL_PRINT=true python3 historical_outcome_engine.py --mode live
```

**Event sources:** 6 detector label files · evidence stream · structure 1S/1M/5M · scenarios · observer qualified setups (optional)

**Horizons:** 30s · 60s · 180s · 300s · 900s · 3600s

**Per observation:** observation_id · event_id · pattern_signature · reference price (at or before event_ts) · outcomes per horizon (raw_return / side_adjusted_return / directional_result / max_favorable / max_adverse) · validation block

**Composite patterns:** events at the same window_start_ts (±2000ms) from different sources form a composite observation in addition to individual ones.

**Calibration profiles:** grouped by (symbol, timeframe, source, event_type, side, direction, pattern_signature). All `scores` fields are `null`. `calibration_status` is always `"observed_not_scored"`.

**Future leakage prevention:** `reference_price_ts <= event_window_start_ts` enforced; any violation logged and observation skipped.

**Restart recovery:** open observations persist to `historical_outcome_open_positions.json` every 30s and are restored on startup.

**Max open observations:** 500 (oldest force-closed if exceeded). Observation timeout: event_ts + 3600s.

## Layer-9 Scenario Engine

Reads all lower-layer JSONL outputs; triggered by each new 1S bar with `has_trade=true`.
Evaluates 9 market behavior scenarios simultaneously. No signals, no orders — scenario
detection and market question answers only.

| Output | File |
|---|---|
| Scenario stream | `data/scenarios.jsonl` |
| Scenario memory | `data/scenario_memory.jsonl` |

```bash
python3 scenario_engine.py --mode batch
python3 scenario_engine.py --mode live
FULL_PRINT=true python3 scenario_engine.py --mode live
```

**Scenarios:** S1 Long Trap · S2 Short Trap · S3 Institutional Accumulation ·
S4 Failed Auction · S5 Breakout Continuation · S6 Exhaustion Reversal ·
S7 Reclaim · S8 Liquidity Sweep · S9 Balance→Breakout Anticipation

**Per-scenario output:** score/max_score · status (confirmed/developing) ·
direction (bullish/bearish/neutral) · 5 condition flags · 9 market questions
(location/aggression/absorption/exhaustion/trap/acceptance/continuation/invalidation/target)

**Scenario memory:** Last 100 confirmed scenarios stored in `data/scenario_memory.jsonl`
for future Edge Matrix and Paper Trade Lifecycle analytics.

**Evidence integration (Section H):** `evidence_engine.py` reads `scenarios.jsonl`
and adds up to +3.0 (confirmed) or +1.5 (developing) to long/short score based on
dominant scenario direction, plus +1.0 multi-scenario alignment bonus.

## Layer-7 Evidence Accumulator + Setup Generator

Reads all lower-layer outputs; triggered by each new 1S bar with `has_trade=true`.
Computes long and short evidence scores from 5 source categories, then checks
setup conditions. No signals, no orders — structural setup records only.

| Output | File |
|---|---|
| Evidence stream | `data/evidence_stream.jsonl` |
| Setups | `data/setups.jsonl` |

```bash
python3 evidence_engine.py --mode batch
python3 evidence_engine.py --mode live
FULL_PRINT=true python3 evidence_engine.py --mode live
```

**Evidence sources:** Candle DNA (delta/volume/depth) · Gate grade ·
Smart Money (1S/1M/5M structure: BOS/CHoCH/MSB/trend) · 6 Detectors ·
Baseline (VWAP/CVD/ATR context)

**Setup conditions:** dominant_side + min score (8.0 normal / 12.0 flash) +
gate grade A/B + trend/BOS alignment + active OB/FVG + no counter-trend upper TF.
Entry/SL/TP computed from ATR with optional OB-based SL refinement.

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
