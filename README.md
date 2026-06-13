# NurtacCoreEngineClaude

Algorithmic trading engine — core data layer for BTCUSDT on Binance USDⓈ-M Futures.

## Layers

| Layer | File | Description |
|---|---|---|
| Layer-0 | `main.py` | Live WebSocket → 1S DNA (Candle, Footprint, Depth, Combined) |
| Layer-1 | `rolling_window_engine.py` | Combined 1S → Rolling 3S / 5S / 15S DNA |

## Layer-0 Output Files

| Output | File |
|---|---|
| Candle DNA | `data/candle_dna_btcusdt.jsonl` |
| Footprint DNA | `data/footprint_dna_btcusdt.jsonl` |
| Depth DNA | `data/depth_dna_btcusdt.jsonl` |
| Combined 1S DNA | `data/combined_1s_dna_btcusdt.jsonl` |

## Layer-1 Output Files

| Output | File |
|---|---|
| Rolling 3S DNA | `data/rolling_3s_dna.jsonl` |
| Rolling 5S DNA | `data/rolling_5s_dna.jsonl` |
| Rolling 15S DNA | `data/rolling_15s_dna.jsonl` |

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
```

## Deployment

This repository is named **NurtacCoreEngineClaude** on GitHub.
Every update is pushed to the same repository and the same VPS.
The remote URL and push command are configured by the user — no
hardcoded remote is included in the codebase.

## Requirements

- Python 3.10+
- Live internet access to Binance futures WebSocket endpoints (Layer-0 only)
- No mock / dummy / hardcoded data is used anywhere

## Architecture notes

**Layer-0**
- Two independent WebSocket connections (trade stream + depth stream)
- 1-second windows aligned to wall-clock boundaries with drift compensation
- Exponential backoff reconnect: 1 s → 2 s → 4 s → 8 s → 16 s → 30 s (per stream)
- Warm-up: output suppressed until at least one trade AND one depth event received
- All shared state protected by `asyncio.Lock`
- Each JSONL write is followed by `flush()` + `fsync()`

**Layer-1**
- Reads `data/combined_1s_dna_btcusdt.jsonl` from the beginning, then follows live (tail -f)
- Single `deque(maxlen=15)` buffer; 3S / 5S / 15S windows are sliced from it without re-reading
- 11-point validation before each write; failures logged to terminal, engine continues
- No external dependencies (stdlib only: json, os, time, collections)
