"""Strategy catalog loader — private workspace submodule or public fallback."""
from __future__ import annotations

from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_WORKSPACE_PRIVATE = _PROJECT_ROOT / "workspace" / ".private"


def using_private_workspace() -> bool:
    """True when the private CryoBacktester-workspace submodule is checked out."""
    return _WORKSPACE_PRIVATE.is_file()


if using_private_workspace():
    from workspace.catalog import (
        FAMILIES,
        SPECS,
        family_for,
        family_label,
        specs_in_family,
        strategies_dict,
        strategy_options,
    )
else:
    from backtester.public_catalog import (
        FAMILIES,
        SPECS,
        family_for,
        family_label,
        specs_in_family,
        strategies_dict,
        strategy_options,
    )
