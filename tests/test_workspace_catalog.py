"""Catalog / STRATEGIES façade — stable IDs and families."""
from __future__ import annotations

import pytest

from backtester.catalog import SPECS, family_for, strategies_dict, using_private_workspace
from backtester.run import STRATEGIES


def test_blueprint_always_registered():
    assert "blueprint_howto" in STRATEGIES
    # STRATEGIES is a process-wide map; job tests may inject ephemeral keys (job_smoke).
    catalog_ids = set(strategies_dict().keys())
    assert catalog_ids <= set(STRATEGIES.keys())
    assert set(STRATEGIES.keys()) - catalog_ids <= {"job_smoke"}
    assert SPECS["blueprint_howto"].cls is STRATEGIES["blueprint_howto"]


def test_family_for_blueprint_and_unknown():
    assert family_for("blueprint_howto") == "other"
    assert family_for("unknown_legacy_strategy") == "other"


@pytest.mark.skipif(not using_private_workspace(), reason="private workspace submodule")
def test_private_workspace_has_full_registry():
    _STABLE_IDS = frozenset({
        "short_str_turb_dyn",
        "tudysho",
        "tudysho_eisbach",
        "tudysho_starnberg",
        "stradysho",
        "tudysho_v1",
        "tudysho_v2",
        "tudysho_v3",
        "tudysho_v4",
        "tudysho_monopteros",
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
    assert _STABLE_IDS <= set(STRATEGIES.keys())
    assert family_for("theta_engine_v6") == "theta_engine"
    assert family_for("tudysho_monopteros") == "tudysho"


@pytest.mark.skipif(not using_private_workspace(), reason="private workspace submodule")
def test_tudysho_archive_statuses_and_picker_hide():
    """Archived IDs stay registered but omit from New Run strategy_options."""
    from backtester.catalog import strategy_options

    assert SPECS["tudysho_v1"].status == "archived"
    assert SPECS["tudysho_v2"].status == "archived"
    assert SPECS["tudysho_v3"].status == "archived"
    assert SPECS["tudysho_eisbach"].status == "archived"
    assert SPECS["tudysho_v4"].status == "frozen"
    assert SPECS["tudysho"].status == "active"
    assert SPECS["stradysho"].status == "active"
    assert SPECS["tudysho_monopteros"].status == "active"

    for sid in ("tudysho_v1", "tudysho_v2", "tudysho_v3", "tudysho_eisbach"):
        assert sid in STRATEGIES

    opts = strategy_options("tudysho")
    assert "tudysho_eisbach" not in opts.values()
    assert "tudysho_v1" not in opts.values()
    assert "tudysho" in opts.values()
    assert "stradysho" in opts.values()
    assert "tudysho_v4" in opts.values()
    assert "tudysho_monopteros" in opts.values()
