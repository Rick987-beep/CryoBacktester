"""Policy preset bundles and threshold resolution."""

from __future__ import annotations

from typing import Any

POLICY_PRESETS: dict[str, dict[str, Any]] = {
    "conservative": {
        "aggressive_iv_rank_max": 0.33,
        "aggressive_mci_min": 0.67,
        "middle_iv_rank_max": 0.50,
        "middle_mci_min": 0.50,
        "tier1_entry_action": "skip",
        "non_quiet_action": "late_base",
    },
    "balanced": {
        "aggressive_iv_rank_max": 0.50,
        "aggressive_mci_min": 0.55,
        "middle_iv_rank_max": 0.67,
        "middle_mci_min": 0.50,
        "tier1_entry_action": "skip",
        "non_quiet_action": "late_base",
    },
    "loose": {
        "aggressive_iv_rank_max": 0.67,
        "aggressive_mci_min": 0.55,
        "middle_iv_rank_max": 0.67,
        "middle_mci_min": 0.50,
        "tier1_entry_action": "skip",
        "non_quiet_action": "late_base",
    },
}


def apply_policy_preset(params: dict[str, Any]) -> dict[str, Any]:
    """Merge preset bundle into params when policy_preset != custom."""
    preset = params.get("policy_preset", "balanced")
    if preset == "custom":
        return dict(params)
    bundle = POLICY_PRESETS.get(preset, POLICY_PRESETS["balanced"])
    merged = dict(params)
    for key, val in bundle.items():
        merged[key] = val
    return merged
