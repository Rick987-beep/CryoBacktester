"""Import shims under backtester.strategies still resolve to workspace modules."""
from __future__ import annotations

import inspect
from pathlib import Path

from backtester.strategies.blueprint_howto import BlueprintHowto
from backtester.strategies.theta_engine_v6 import ThetaEngineV6
from backtester.strategies.tudysho_eisbach import TuDyShoEisbach


def test_shim_classes_live_under_workspace():
    for cls in (ThetaEngineV6, TuDyShoEisbach, BlueprintHowto):
        path = Path(inspect.getfile(cls))
        assert "workspace" in path.parts, path
