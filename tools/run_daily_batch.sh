#!/bin/bash
# Manuel tetikleyici — timer yok, OOM riski yok
cd /root/NurtacCoreEngineClaude
echo "=== BATCH START $(date) ===" && free -h | grep Mem

echo "--- paper_close ---"
.venv/bin/python3 tools/paper_close_engine.py
free -h | grep Mem

echo "--- historical_outcome ---"
.venv/bin/python3 tools/historical_outcome_feed.py 2>/dev/null || echo "SKIP: not ready"
free -h | grep Mem

echo "--- calibration ---"
.venv/bin/python3 tools/calibration_feed.py 2>/dev/null || echo "SKIP: not ready"
free -h | grep Mem

echo "--- trade_outcome_explanations ---"
.venv/bin/python3 tools/trade_outcome_explanation_engine.py 2>/dev/null || echo "SKIP: not ready"
free -h | grep Mem

echo "=== BATCH DONE ===" && free -h | grep Mem
