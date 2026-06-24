#!/bin/bash
set -euo pipefail

cd /root/NurtacCoreEngineClaude
exec 9>/tmp/nurtac-rotate.lock
if ! flock -n 9; then
    echo "Rotation already running; skipped"
    exit 0
fi

echo "=== ROTATE $(date -u) ==="
df -h /root | tail -1

SUPERVISOR_WAS_ACTIVE=false
if systemctl is-active --quiet nurtac-supervisor; then
    SUPERVISOR_WAS_ACTIVE=true
    systemctl stop nurtac-supervisor
    sleep 3
fi

restart_supervisor() {
    if [ "$SUPERVISOR_WAS_ACTIVE" = true ]; then
        systemctl start nurtac-supervisor
    fi
}
trap restart_supervisor EXIT

python3 <<'PYROTATE'
import os
from pathlib import Path

DATA = Path("/root/NurtacCoreEngineClaude/data")
MIB = 1024 * 1024

# line limit, hard byte cap. The byte cap is authoritative because one DNA
# record can contain a large footprint and line count alone is not RAM-safe.
FILES = {
    "rolling_15s_dna.jsonl": (1000, 96 * MIB),
    "rolling_5s_dna.jsonl": (1000, 96 * MIB),
    "rolling_3s_dna.jsonl": (1000, 96 * MIB),
    "combined_1s_dna_btcusdt.jsonl": (30000, 128 * MIB),
    "aligned_1m_candle_dna.jsonl": (5000, 96 * MIB),
    "aligned_5m_candle_dna.jsonl": (3000, 96 * MIB),
    "aligned_15m_candle_dna.jsonl": (2000, 96 * MIB),
    "aligned_1h_candle_dna.jsonl": (2000, 96 * MIB),
    "aligned_4h_candle_dna.jsonl": (1000, 96 * MIB),
    "aligned_1d_candle_dna.jsonl": (500, 64 * MIB),
    "historical_baseline_dna.jsonl": (5000, 64 * MIB),
    "volume_profile_session.jsonl": (2000, 64 * MIB),
    "volume_profile_1m.jsonl": (2000, 64 * MIB),
    "volume_profile_1s.jsonl": (2000, 64 * MIB),
    "structure_1s.jsonl": (5000, 32 * MIB),
    "structure_1m.jsonl": (2000, 32 * MIB),
    "structure_5m.jsonl": (1000, 32 * MIB),
    "evidence_stream.jsonl": (3000, 32 * MIB),
    "validation_report.jsonl": (2000, 16 * MIB),
    "candle_dna_btcusdt.jsonl": (5000, 32 * MIB),
    "depth_dna_btcusdt.jsonl": (5000, 32 * MIB),
    "footprint_dna_btcusdt.jsonl": (3000, 32 * MIB),
    "labels_absorption.jsonl": (5000, 16 * MIB),
    "labels_sweep.jsonl": (5000, 16 * MIB),
    "labels_exhaustion.jsonl": (5000, 16 * MIB),
    "labels_iceberg.jsonl": (5000, 16 * MIB),
    "labels_trapped_trader.jsonl": (5000, 16 * MIB),
    "labels_initiative_flow.jsonl": (5000, 16 * MIB),
    "decision_gate_output.jsonl": (3000, 16 * MIB),
    "scenarios.jsonl": (3000, 16 * MIB),
    "setups.jsonl": (3000, 16 * MIB),
    "regime_context.jsonl": (5000, 16 * MIB),
    "footprint_live.jsonl": (2000, 16 * MIB),
    "liquidation_clusters.jsonl": (2000, 16 * MIB),
    "liquidation_setups.jsonl": (2000, 16 * MIB),
    "real_liquidations.jsonl": (5000, 16 * MIB),
    "orderbook_walls.jsonl": (2000, 16 * MIB),
    "whale_trades.jsonl": (5000, 16 * MIB),
    "whale_trade_summary.jsonl": (1000, 16 * MIB),
    "whale_orders.jsonl": (3000, 16 * MIB),
    "orderbook_stats.jsonl": (2000, 16 * MIB),
}


def bounded_tail(path: Path, line_limit: int, byte_limit: int) -> bytes:
    size = path.stat().st_size
    if size == 0:
        return b""

    chunks = []
    newline_count = 0
    bytes_read = 0
    position = size
    with path.open("rb") as source:
        while position > 0 and newline_count <= line_limit and bytes_read < byte_limit:
            read_size = min(MIB, position, byte_limit - bytes_read)
            position -= read_size
            source.seek(position)
            chunk = source.read(read_size)
            chunks.append(chunk)
            bytes_read += len(chunk)
            newline_count += chunk.count(b"\n")

    data = b"".join(reversed(chunks))
    if position > 0:
        first_newline = data.find(b"\n")
        data = data[first_newline + 1:] if first_newline >= 0 else b""
    lines = data.splitlines(keepends=True)[-line_limit:]
    output = b"".join(lines)
    if output and not output.endswith(b"\n"):
        output += b"\n"
    return output


for name, (line_limit, byte_limit) in FILES.items():
    path = DATA / name
    if not path.exists():
        continue
    old_size = path.stat().st_size
    retained = bounded_tail(path, line_limit, byte_limit)
    tmp = path.with_suffix(path.suffix + ".rotate.tmp")
    with tmp.open("wb") as target:
        target.write(retained)
        target.flush()
        os.fsync(target.fileno())
    os.replace(tmp, path)
    print(
        f"{name}: {old_size / MIB:.1f}MB -> {len(retained) / MIB:.1f}MB "
        f"({retained.count(bytes([10]))} lines)"
    )

print("Rotation complete")
PYROTATE

sync
df -h /root | tail -1
