"""Public blueprint strategy import path + sync with workspace copy."""
from __future__ import annotations

import hashlib
import inspect
from pathlib import Path

from backtester.core.paths import repo_root
from backtester.strategies.blueprint_howto import BlueprintHowto


def test_blueprint_lives_under_backtester_strategies():
    path = Path(inspect.getfile(BlueprintHowto))
    assert "backtester" in path.parts
    assert path.name == "blueprint_howto.py"


def test_blueprint_matches_workspace_copy_when_present():
    """Public and workspace blueprint_howto must stay byte-identical (no shim)."""
    public = repo_root() / "backtester" / "strategies" / "blueprint_howto.py"
    private = repo_root() / "workspace" / "strategies" / "other" / "blueprint_howto.py"
    if not (repo_root() / "workspace" / ".private").is_file():
        return
    assert private.is_file(), "workspace blueprint missing"
    pub_hash = hashlib.sha256(public.read_bytes()).hexdigest()
    priv_hash = hashlib.sha256(private.read_bytes()).hexdigest()
    assert pub_hash == priv_hash, (
        "backtester/strategies/blueprint_howto.py drifted from "
        "workspace/strategies/other/blueprint_howto.py — sync them"
    )


def test_no_workspace_reexport_shims_in_backtester_strategies():
    strat_dir = repo_root() / "backtester" / "strategies"
    py_files = {p.name for p in strat_dir.glob("*.py")}
    assert py_files <= {"__init__.py", "blueprint_howto.py"}
