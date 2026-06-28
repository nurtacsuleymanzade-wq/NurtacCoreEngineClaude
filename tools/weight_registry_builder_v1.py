#!/usr/bin/env python3
import argparse
import ast
import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path("/root/NurtacCoreEngineClaude")
DATA = ROOT / "data"
TRADE_BRAIN = ROOT / "trade_brain_engine.py"
STAGED_PROFILE = DATA / "staged_trade_brain_weights.json"
PROMOTION_REGISTRY = DATA / "learning_promotion_registry.json"
APPROVAL_CANDIDATES = DATA / "weight_approval_candidates.json"
LEARNING_CANDIDATES = DATA / "trade_brain_learning_candidates.json"

OUT_JSON = DATA / "trade_brain_weight_registry.json"
OUT_JSONL = DATA / "trade_brain_weight_registry.jsonl"
HEALTH = DATA / "trade_brain_weight_registry_health.json"
AUDIT = DATA / "trade_brain_weight_registry_audit.jsonl"
REPORT = DATA / "trade_brain_weight_registry_report.md"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _direction_from_key(key: str) -> str:
    k = key.lower()
    if k.endswith("_long") or "long" in k:
        return "long"
    if k.endswith("_short") or "short" in k:
        return "short"
    return "unknown"


def _category_from_key(key: str) -> str:
    k = key.lower()
    for prefix in ("candle_dna", "gate", "smart_money", "detector", "baseline", "market_context", "volume_profile", "scenario", "liq_magnet", "whale_pressure", "ob_imbalance", "macro_genuine", "etf", "coinbase_premium", "iceberg", "absorption", "sweep", "initiative_flow", "trapped_trader", "exhaustion"):
        if k.startswith(prefix):
            return prefix
    if "_" in k:
        return k.split("_", 1)[0]
    return "unknown"


def _exact_scores_from_trade_brain() -> tuple[dict[str, dict[str, Any]], list[str]]:
    if not TRADE_BRAIN.exists():
        return {}, ["trade_brain_missing"]
    text = TRADE_BRAIN.read_text()
    weights: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    for i, line in enumerate(text.splitlines(), start=1):
        m = re.search(r'scores\["([^"]+)"\]\s*=\s*\(([^,]+),\s*([^,]+),\s*(.+)\)', line)
        if not m:
            continue
        key = m.group(1)
        try:
            left = ast.literal_eval(m.group(2).strip())
            right = ast.literal_eval(m.group(3).strip())
        except Exception:
            left = right = None
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            val = max(float(left), float(right))
            exact = True
        else:
            val = None
            exact = False
        weights[key] = {
            "value": val,
            "source": "discovered" if exact and val is not None else "unknown",
            "confidence": "exact" if exact and val is not None else "unknown",
            "direction": _direction_from_key(key),
            "category": _category_from_key(key),
            "used_by": ["trade_brain_engine"],
            "line_refs": [i],
            "notes": [],
        }
    if not weights:
        warnings.append("no_weights_discovered")
    return weights, warnings


def _infer_weights_from_assignments() -> tuple[dict[str, dict[str, Any]], list[str]]:
    inferred = {}
    warnings = []
    if not TRADE_BRAIN.exists():
        return inferred, ["trade_brain_missing"]
    for i, line in enumerate(TRADE_BRAIN.read_text().splitlines(), start=1):
        m = re.search(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([0-9]+(?:\.[0-9]+)?)', line)
        if not m:
            continue
        key = m.group(1)
        if key in {"MIN_CONFIDENCE", "COOLDOWN_S", "POLL_SLEEP"}:
            continue
        val = float(m.group(2))
        if key not in inferred:
            inferred[key] = {
                "value": val,
                "source": "discovered",
                "confidence": "exact",
                "direction": _direction_from_key(key),
                "category": _category_from_key(key),
                "used_by": ["trade_brain_engine"],
                "line_refs": [i],
                "notes": [],
            }
    return inferred, warnings


def _import_optional_profiles() -> dict[str, Any]:
    staged = _read_json(STAGED_PROFILE)
    promo = _read_json(PROMOTION_REGISTRY)
    appr = _read_json(APPROVAL_CANDIDATES)
    learning = _read_json(LEARNING_CANDIDATES)
    return {
        "staged_profile": staged if staged else None,
        "promoted_lifecycle_snapshot": promo if promo else None,
        "approval_candidates": appr if appr else None,
        "learning_candidates": learning if learning else None,
    }


def build_batch() -> dict[str, Any]:
    ts = int(time.time() * 1000)
    exact, exact_warnings = _exact_scores_from_trade_brain()
    inferred, inferred_warnings = _infer_weights_from_assignments()

    weights = dict(exact)
    for key, meta in inferred.items():
        weights.setdefault(key, meta)

    exact_count = sum(1 for v in weights.values() if v.get("confidence") == "exact")
    inferred_count = sum(1 for v in weights.values() if v.get("confidence") == "inferred")
    unknown_count = sum(1 for v in weights.values() if v.get("confidence") == "unknown" or v.get("value") is None)

    categories = Counter(v.get("category", "unknown") for v in weights.values())
    directions = Counter(v.get("direction", "unknown") for v in weights.values())

    optional = _import_optional_profiles()
    staged_imported = bool(optional.get("staged_profile"))
    promotion_imported = bool(optional.get("promoted_lifecycle_snapshot"))

    profiles = {
        "current_discovered_weights": {
            "profile_id": "current_discovered_weights",
            "status": "active_snapshot_only",
            "active": False,
            "applied_to_live": False,
            "weights": weights,
        }
    }
    if staged_imported:
        profiles["staged_profile"] = {
            "profile_id": "staged_profile",
            "status": "staged_imported",
            "active": False,
            "applied_to_live": False,
            "weights": optional["staged_profile"].get("weights") if isinstance(optional["staged_profile"], dict) else {},
        }
    if promotion_imported:
        profiles["promoted_lifecycle_snapshot"] = {
            "profile_id": "promoted_lifecycle_snapshot",
            "status": "promotion_lineage_only",
            "active": False,
            "applied_to_live": False,
            "weights": {},
            "lineage": optional["promoted_lifecycle_snapshot"],
        }

    registry = {
        "registry_engine": "weight_registry_builder_v1",
        "version": "weight_registry_v1",
        "generated_ts": ts,
        "active_profile_id": "current_discovered_weights",
        "active_profile_source": "discovered_from_trade_brain_engine",
        "live_update_allowed": False,
        "trade_brain_changed": False,
        "weights": weights,
        "profiles": profiles,
        "lineage": {
            "learning_candidates": str(LEARNING_CANDIDATES),
            "simulation_results": str(DATA / "weight_simulation_results.json"),
            "controlled_staged_profile": str(STAGED_PROFILE),
            "promotion_registry": str(PROMOTION_REGISTRY),
        },
        "safety": {
            "manual_approval_required": True,
            "live_update_allowed": False,
            "registry_read_only_for_now": True,
            "rollback_available": True,
        },
        "warnings": exact_warnings + inferred_warnings + ([] if weights else ["no_weights_discovered"]),
    }

    _write_json(OUT_JSON, registry)
    _write_jsonl(OUT_JSONL, registry)

    audit = {
        "ts": ts,
        "engine": "weight_registry_builder_v1",
        "weights_discovered": len(weights),
        "exact_count": exact_count,
        "inferred_count": inferred_count,
        "unknown_count": unknown_count,
        "staged_profiles_seen": 1 if staged_imported else 0,
        "promoted_profiles_seen": 1 if promotion_imported else 0,
        "trade_brain_changed": False,
        "live_weight_changed": False,
        "warnings": registry["warnings"],
    }
    _write_jsonl(AUDIT, audit)

    health = {
        "status": "alive" if weights else "blocked",
        "last_run_ts": ts,
        "weights_discovered": len(weights),
        "exact_count": exact_count,
        "inferred_count": inferred_count,
        "unknown_count": unknown_count,
        "staged_profile_imported": staged_imported,
        "promotion_registry_imported": promotion_imported,
        "active_profile_id": "current_discovered_weights",
        "registry_read_only": True,
        "live_update_allowed": False,
        "trade_brain_changed": False,
        "last_blocker": None if weights else "no_weights_discovered",
        "warnings": registry["warnings"],
    }
    _write_json(HEALTH, health)

    report = [
        "# Trade Brain Weight Registry Report",
        "",
        "## Status",
        f"- status: {health['status']}",
        "- No live Trade Brain weights were changed.",
        "- Registry is read-only and not yet connected to Trade Brain.",
        "",
        "## Git / Version",
        f"- trade_brain_engine_present: {TRADE_BRAIN.exists()}",
        f"- active_profile_id: current_discovered_weights",
        "",
        "## Discovered Weights",
        f"- weights_discovered: {len(weights)}",
        "",
        "## Exact Weights",
        f"- exact_count: {exact_count}",
        "",
        "## Inferred Weights",
        f"- inferred_count: {inferred_count}",
        "",
        "## Unknown Weights",
        f"- unknown_count: {unknown_count}",
        "",
        "## Categories",
        "\n".join(f"- {k}: {v}" for k, v in categories.most_common()) or "- none",
        "",
        "## Directions",
        "\n".join(f"- {k}: {v}" for k, v in directions.most_common()) or "- none",
        "",
        "## Staged Profile Import",
        f"- imported: {staged_imported}",
        "",
        "## Promotion Registry Import",
        f"- imported: {promotion_imported}",
        "",
        "## Safety",
        "- manual_approval_required=true",
        "- live_update_allowed=false",
        "- registry_read_only_for_now=true",
        "",
        "## Loader Functions",
        "- load_weight_registry()",
        "- get_weight()",
        "- list_weights()",
        "- validate_registry()",
        "- registry_is_safe_for_live()",
        "",
        "## No Live Weight Change Guarantee",
        "No live Trade Brain weights were changed.",
        "",
        "## Next Recommended Step",
        "- Review exact vs inferred mapping, then decide whether a future canonical weight registry should be wired into Trade Brain in a separate phase.",
    ]
    REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
    return health


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="batch", choices=["batch"])
    args = parser.parse_args()
    if args.mode == "batch":
        build_batch()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
