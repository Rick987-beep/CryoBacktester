"""Safety checks for the physical data-plane layout."""
from __future__ import annotations

from pathlib import Path

from backtester.core.paths import market_data_dir, repo_root, runs_dir


def test_legacy_paths_are_symlinks_or_absent():
    root = repo_root()
    for rel in (
        "backtester/data",
        "backtester/reports",
        "backtester/indicators/data",
    ):
        p = root / rel
        if p.exists():
            assert p.is_symlink(), f"{p} should be a transitional symlink"


def test_data_plane_roots_are_real_directories():
    market = market_data_dir()
    runs = runs_dir()
    assert market.is_dir() and not market.is_symlink()
    assert runs.is_dir() and not runs.is_symlink()
    # Spot-check that market history landed
    assert any(market.glob("options_*.parquet")) or any(market.glob("**/options_*.parquet"))
