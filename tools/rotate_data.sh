#!/bin/bash
# Günlük veri rotasyonu — disk dolmasını önler
# Cron: 0 3 * * * bash /root/NurtacCoreEngineClaude/tools/rotate_data.sh
cd /root/NurtacCoreEngineClaude
echo "=== ROTATE $(date) ===" 
echo "Disk before:" && df -h /root | tail -1

# Rolling DNA — sadece son 5000 satır (engine zaten anlık okuyor)
for f in data/rolling_3s_dna.jsonl data/rolling_5s_dna.jsonl data/rolling_15s_dna.jsonl; do
    [ -f "$f" ] && tail -5000 "$f" > /tmp/rt && mv /tmp/rt "$f"
done

# Aligned candle — son 10000 satır
for f in data/aligned_1m_candle_dna.jsonl data/aligned_5m_candle_dna.jsonl \
         data/aligned_15m_candle_dna.jsonl data/aligned_1h_candle_dna.jsonl \
         data/aligned_4h_candle_dna.jsonl data/aligned_1d_candle_dna.jsonl; do
    [ -f "$f" ] && tail -10000 "$f" > /tmp/rt && mv /tmp/rt "$f"
done

# Baseline — son 5000 satır
[ -f data/historical_baseline_dna.jsonl ] && \
    tail -5000 data/historical_baseline_dna.jsonl > /tmp/rt && \
    mv /tmp/rt data/historical_baseline_dna.jsonl

# Labels — son 50000 satır
for f in data/labels_absorption.jsonl data/labels_sweep.jsonl \
         data/labels_exhaustion.jsonl data/labels_iceberg.jsonl \
         data/labels_trapped_trader.jsonl data/labels_initiative_flow.jsonl; do
    [ -f "$f" ] && tail -50000 "$f" > /tmp/rt && mv /tmp/rt "$f"
done

# Decision gate + volume profile — son 10000 satır
for f in data/decision_gate_output.jsonl data/volume_profile_session.jsonl \
         data/volume_profile_1s.jsonl data/evidence_stream.jsonl \
         data/scenarios.jsonl data/setups.jsonl; do
    [ -f "$f" ] && tail -10000 "$f" > /tmp/rt && mv /tmp/rt "$f"
done

# Structure files — son 5000 satır
for f in data/structure_1s.jsonl data/structure_1m.jsonl data/structure_5m.jsonl; do
    [ -f "$f" ] && tail -5000 "$f" > /tmp/rt && mv /tmp/rt "$f"
done

echo "Disk after:" && df -h /root | tail -1
