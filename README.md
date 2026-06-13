# NurtacCoreEngineClaude

Algorithmic trading engine — core data layer for BTCUSDT on Binance USDⓈ-M Futures.

## Layers

| Layer | File | Description |
|---|---|---|
| Layer-0 | `main.py` | Live WebSocket → 1S DNA (Candle, Footprint, Depth, Combined) |
| Layer-1 | `rolling_window_engine.py` | Combined 1S → Rolling 3S / 5S / 15S DNA |
| Layer-2 | `aligned_candle_engine.py` | Combined 1S → Aligned 1M / 5M / 15M / 1H / 4H / 1D DNA |

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

# Optional Telegram alerts:
# export TELEGRAM_BOT_TOKEN=<your bot token>
# export TELEGRAM_CHAT_ID=<your chat id>
# python3 watchdog.py
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
sudo systemctl enable --now nurtac-layer0 nurtac-layer1 nurtac-layer2 nurtac-watchdog
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
