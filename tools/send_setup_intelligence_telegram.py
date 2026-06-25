#!/usr/bin/env python3
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
TEXT = ROOT / "data" / "setup_intelligence_report.txt"

def main():
    body = TEXT.read_text(encoding="utf-8") if TEXT.exists() else ""
    print("[INTELLIGENCE TG SUPPRESSED]")
    print(body[:1200])

if __name__ == "__main__":
    main()
