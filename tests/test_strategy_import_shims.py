"""Public blueprint strategy import path."""
from __future__ import annotations

import inspect
from pathlib import Path

from backtester.strategies.blueprint_howto import BlueprintHowto


def test_blueprint_lives_under_backtester_strategies():
    path = Path(inspect.getfile(BlueprintHowto))
    assert "backtester" in path.parts
    assert path.name == "blueprint_howto.py"
