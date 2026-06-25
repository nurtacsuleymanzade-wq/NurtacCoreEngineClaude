#!/usr/bin/env python3
import json, re, os
from pathlib import Path

ROOT = Path("/root/NurtacCoreEngineClaude")
OUT = ROOT / "data" / "jsonl_memory_risk_report.json"

PATTERNS = {
    "read_all_lines": re.compile(r"read_all_lines"),
    "readlines": re.compile(r"\.readlines\s*\("),
    "file_read": re.compile(r"\.read\s*\("),
    "json_load": re.compile(r"json\.load\s*\("),
    "records_append_json": re.compile(r"(records|last_records|lines|ev_lines)\.append\s*\(\s*json\.loads"),
    "list_comprehension_json_loads": re.compile(r"\[\s*json\.loads\(.*for .* in open\("),
}

SAFE_SMALL_FILES_HINTS = [
    "SYSTEM_HALT",
    "health",
    "summary",
    "open_positions",
    "telegram_health",
]

def classify(path: Path, line: str, kind: str) -> str:
    l = line.lower()
    if any(h.lower() in l for h in SAFE_SMALL_FILES_HINTS):
        return "LOW"
    if kind in {"read_all_lines", "readlines", "records_append_json", "list_comprehension_json_loads"}:
        return "HIGH"
    if kind in {"json_load", "file_read"}:
        return "MEDIUM"
    return "MEDIUM"

findings = []

for py in sorted(ROOT.glob("*.py")):
    if py.name.startswith("verify_"):
        continue
    try:
        lines = py.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        continue

    for idx, line in enumerate(lines, 1):
        for kind, rx in PATTERNS.items():
            if rx.search(line):
                findings.append({
                    "file": py.name,
                    "line_no": idx,
                    "kind": kind,
                    "risk": classify(py, line, kind),
                    "line": line.strip()
                })

summary = {
    "HIGH": sum(1 for f in findings if f["risk"] == "HIGH"),
    "MEDIUM": sum(1 for f in findings if f["risk"] == "MEDIUM"),
    "LOW": sum(1 for f in findings if f["risk"] == "LOW"),
}

by_file = {}
for f in findings:
    by_file.setdefault(f["file"], {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "total": 0})
    by_file[f["file"]][f["risk"]] += 1
    by_file[f["file"]]["total"] += 1

report = {
    "goal": "2/5 RAM-safe JSONL Standard audit",
    "summary": summary,
    "by_file": by_file,
    "findings": findings,
    "decision": "Patch HIGH-risk production readers first. Do not patch validate/debug files unless they run in production."
}

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

print(json.dumps({
    "summary": summary,
    "top_files": sorted(by_file.items(), key=lambda x: (x[1]["HIGH"], x[1]["total"]), reverse=True)[:15],
    "report": str(OUT)
}, indent=2))
