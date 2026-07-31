"""Catalog / STRATEGIES façade — stable IDs and families."""
from __future__ import annotations

from backtester.run import STRATEGIES
from workspace.catalog import SPECS, family_for, strategies_dict

# Frozen allowlist of pre-split registry keys — never rename.
_STABLE_IDS = frozenset({
    "short_str_turb_dyn",
    "tudysho",
    "tudysho_eisbach",
    "tudysho_starnberg",
    "stradysho",
    "tudysho_v1",
    "tudysho_v2",
    "cadysho",
    "blueprint_howto",
    "long_gamma_move",
    "pagoda",
    "covered_call_put",
    "cal_spread_atm",
    "theta_engine_v1",
    "theta_engine_v2",
    "theta_engine_v3",
    "theta_engine_v4",
    "theta_engine_v5",
    "theta_engine_v6",
})


def test_stable_ids_present():
    assert _STABLE_IDS <= set(STRATEGIES.keys())


def test_strategies_dict_matches_facade():
    assert set(strategies_dict().keys()) == set(STRATEGIES.keys())
    for sid, cls in STRATEGIES.items():
        assert SPECS[sid].cls is cls


def test_family_for_known_and_unknown():
    assert family_for("theta_engine_v6") == "theta_engine"
    assert family_for("tudysho_eisbach") == "tudysho"
    assert family_for("blueprint_howto") == "other"
    assert family_for("unknown_legacy_strategy") == "other"
