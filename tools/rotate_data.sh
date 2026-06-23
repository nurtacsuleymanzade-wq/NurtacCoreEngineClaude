#!/bin/bash
# Günlük veri rotasyonu - Python ile (disk dolsa bile çalışır)
cd /root/NurtacCoreEngineClaude
echo "=== ROTATE $(date) ===" 
echo "Disk before:" && df -h /root | tail -1

python3 << 'PYROTATE'
import subprocess
from pathlib import Path

DATA = Path("/root/NurtacCoreEngineClaude/data")

files = {
    "rolling_3s_dna.jsonl": 2000,
    "rolling_5s_dna.jsonl": 2000,
    "rolling_15s_dna.jsonl": 2000,
    "aligned_1m_candle_dna.jsonl": 500,
    "aligned_5m_candle_dna.jsonl": 500,
    "aligned_15m_candle_dna.jsonl": 300,
    "aligned_1h_candle_dna.jsonl": 200,
    "aligned_4h_candle_dna.jsonl": 100,
    "aligned_1d_candle_dna.jsonl": 50,
    "historical_baseline_dna.jsonl": 500,
    "labels_absorption.jsonl": 5000,
    "labels_sweep.jsonl": 5000,
    "labels_exhaustion.jsonl": 5000,
    "labels_iceberg.jsonl": 5000,
    "labels_trapped_trader.jsonl": 5000,
    "labels_initiative_flow.jsonl": 5000,
    "decision_gate_output.jsonl": 2000,
    "evidence_stream.jsonl": 2000,
    "scenarios.jsonl": 1000,
    "setups.jsonl": 1000,
    "structure_1s.jsonl": 2000,
    "structure_1m.jsonl": 1000,
    "structure_5m.jsonl": 500,
    "volume_profile_session.jsonl": 500,
    "volume_profile_1s.jsonl": 500,
    "footprint_dna_btcusdt.jsonl": 2000,
}

for fname, n in files.items():
    f = DATA / fname
    if not f.exists():
        continue
    data = subprocess.getoutput(f"tail -{n} {f}").encode()
    f.write_bytes(data)

print("Rotation complete")
PYROTATE

echo "Disk after:" && df -h /root | tail -1
