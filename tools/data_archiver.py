#!/usr/bin/env python3
import gzip, json, shutil, subprocess, time
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
ARCHIVE = ROOT / "archive"
REPORT = DATA / "data_archiver_report.json"

# Conservative limits. Purpose: prevent 68GB-style explosions without disturbing normal flow.
CONFIG = {
    "setups.jsonl": (64, 5000),
    "observations.jsonl": (256, 10000),
    "qualified_setups.jsonl": (64, 5000),
    "paper_trades.jsonl": (64, 5000),
    "evidence_stream.jsonl": (768, 15000),
    "smart_money_dna.jsonl": (768, 15000),
    "structure_1s.jsonl": (512, 15000),
    "rolling_3s_dna.jsonl": (768, 15000),
    "rolling_5s_dna.jsonl": (1024, 15000),
    "rolling_15s_dna.jsonl": (1536, 15000),
    "historical_baseline_dna.jsonl": (1024, 15000),
    "decision_gate_output.jsonl": (512, 10000),
}

def mb(n): return n / 1024 / 1024

def tail_to_file(src: Path, dst: Path, lines: int):
    out = subprocess.getoutput(f"tail -{int(lines)} {src}")
    dst.write_text(out + ("\n" if out else ""), encoding="utf-8")

def archive_file(path: Path, keep_lines: int):
    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    raw = ARCHIVE / f"{path.name}.{ts}.bak"
    gz = ARCHIVE / f"{path.name}.{ts}.bak.gz"
    keep = path.with_suffix(path.suffix + ".keep")

    shutil.copy2(path, raw)
    with raw.open("rb") as f_in, gzip.open(gz, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    raw.unlink(missing_ok=True)

    tail_to_file(path, keep, keep_lines)
    keep.replace(path)

    return str(gz)

def main():
    DATA.mkdir(exist_ok=True)
    ARCHIVE.mkdir(exist_ok=True)
    actions = []
    warnings = []

    for name, (limit_mb, keep_lines) in CONFIG.items():
        p = DATA / name
        if not p.exists():
            continue
        size = p.stat().st_size
        if mb(size) >= limit_mb:
            try:
                gz = archive_file(p, keep_lines)
                actions.append({
                    "file": str(p),
                    "old_size_mb": round(mb(size), 2),
                    "limit_mb": limit_mb,
                    "kept_lines": keep_lines,
                    "archive": gz,
                    "new_size_mb": round(mb(p.stat().st_size), 2),
                })
            except Exception as e:
                warnings.append({"file": str(p), "error": repr(e)})

    report = {
        "checked_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "actions": actions,
        "warnings": warnings,
    }
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
