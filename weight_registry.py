#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_REGISTRY_PATH = Path("/root/NurtacCoreEngineClaude/data/trade_brain_weight_registry.json")


def load_weight_registry(path: str | Path = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def validate_registry(registry: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not isinstance(registry, dict):
        return ["registry_not_dict"]
    if registry.get("version") != "weight_registry_v1":
        errors.append("version_missing_or_invalid")
    if "weights" not in registry or not isinstance(registry.get("weights"), dict):
        errors.append("weights_missing_or_invalid")
    safety = registry.get("safety") or {}
    if not isinstance(safety, dict):
        errors.append("safety_missing_or_invalid")
    if safety and safety.get("live_update_allowed") is not False:
        errors.append("live_update_allowed_not_false")
    return errors


def registry_is_safe_for_live(registry: dict[str, Any]) -> bool:
    if not isinstance(registry, dict):
        return False
    if registry.get("live_update_allowed") is not False:
        return False
    safety = registry.get("safety") or {}
    if not isinstance(safety, dict):
        return False
    if safety.get("live_update_allowed") is not False:
        return False
    if safety.get("registry_read_only_for_now") is not True:
        return False
    return False


def _select_profile(registry: dict[str, Any], profile_id: str | None = None) -> dict[str, Any]:
    profiles = registry.get("profiles") or {}
    if not isinstance(profiles, dict):
        return {}
    if profile_id and profile_id in profiles and isinstance(profiles[profile_id], dict):
        return profiles[profile_id]
    current = registry.get("active_profile_id")
    if current and current in profiles and isinstance(profiles[current], dict):
        return profiles[current]
    return profiles.get("current_discovered_weights") if isinstance(profiles.get("current_discovered_weights"), dict) else {}


def get_weight(key: str, default: Any = None, profile_id: str | None = None) -> float | None:
    registry = load_weight_registry()
    profile = _select_profile(registry, profile_id)
    weights = profile.get("weights") if isinstance(profile, dict) else registry.get("weights")
    if not isinstance(weights, dict):
        return default
    entry = weights.get(key)
    if isinstance(entry, dict):
        val = entry.get("value")
        try:
            return float(val) if val is not None else default
        except Exception:
            return default
    try:
        return float(entry) if entry is not None else default
    except Exception:
        return default


def list_weights(category: str | None = None, direction: str | None = None) -> dict[str, Any]:
    registry = load_weight_registry()
    weights = registry.get("weights") or {}
    if not isinstance(weights, dict):
        return {}
    out: dict[str, Any] = {}
    for key, meta in weights.items():
        if category and isinstance(meta, dict) and meta.get("category") != category:
            continue
        if direction and isinstance(meta, dict) and meta.get("direction") != direction:
            continue
        out[key] = meta
    return out

