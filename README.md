# NurtacCoreEngineClaude

Core engine layer of an algorithmic trading system.

Connects to Binance USDⓈ-M Futures live WebSocket streams and produces
four DNA objects per 1-second window for BTCUSDT:

| Output | File |
|---|---|
| Candle DNA | `data/candle_dna_btcusdt.jsonl` |
| Footprint DNA | `data/footprint_dna_btcusdt.jsonl` |
| Depth DNA | `data/depth_dna_btcusdt.jsonl` |
| Combined 1S DNA | `data/combined_1s_dna_btcusdt.jsonl` |

## Setup & Run

**Linux / macOS**
```bash
cd NurtacCoreEngineClaude
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

**Windows**
```powershell
cd NurtacCoreEngineClaude
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

## Deployment

This repository is named **NurtacCoreEngineClaude** on GitHub.
Every update is pushed to the same repository and the same VPS.
The remote URL and push command are configured by the user — no
hardcoded remote is included in the codebase.

## Requirements

- Python 3.10+
- Live internet access to Binance futures WebSocket endpoints
- No mock / dummy / hardcoded data is used anywhere

## Architecture notes

- Two independent WebSocket connections (trade stream + depth stream)
- 1-second windows aligned to wall-clock boundaries with drift compensation
- Exponential backoff reconnect: 1 s → 2 s → 4 s → 8 s → 16 s → 30 s (per stream)
- Warm-up: output suppressed until at least one trade AND one depth event received
- All shared state protected by `asyncio.Lock`
- Each JSONL write is followed by `flush()` + `fsync()`
