"""Public catalog — blueprint only (demo package)."""
from __future__ import annotations

from backtester.catalog import SPECS, family_for, strategies_dict, using_private_workspace
from backtester.run import STRATEGIES


def test_blueprint_always_registered():
    assert "blueprint_howto" in STRATEGIES
    catalog_ids = set(strategies_dict().keys())
    assert catalog_ids <= set(STRATEGIES.keys())
    assert set(STRATEGIES.keys()) - catalog_ids <= {"job_smoke"}
    assert SPECS["blueprint_howto"].cls is STRATEGIES["blueprint_howto"]


def test_family_for_blueprint_and_unknown():
    assert family_for("blueprint_howto") == "other"
    assert family_for("unknown_legacy_strategy") == "other"


def test_demo_has_no_private_workspace():
    assert using_private_workspace() is False
    assert set(strategies_dict().keys()) == {"blueprint_howto"}
