#!/usr/bin/env python3
import importlib.util
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
mod_path = ROOT / "tools" / "curated_telegram_reporter.py"

spec = importlib.util.spec_from_file_location("curated", mod_path)
curated = importlib.util.module_from_spec(spec)
spec.loader.exec_module(curated)

text = curated.format_summary()
print("=== SUMMARY TEXT ===")
print(text)

if not text:
    print("summary_text=false")
else:
    ok = curated.send(text)
    print(f"telegram_sent={ok}")
