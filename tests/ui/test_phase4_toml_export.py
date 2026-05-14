"""
tests/ui/test_phase4_toml_export.py — Tests for favourite_to_toml.

Tests:
  - test_roundtrip_via_tomllib  (TOML string is parseable)
  - test_single_value_param_grid
"""
import tomllib

import pytest

from backtester.ui.services.store_service import FavRow
from backtester.ui.services.toml_export import favourite_to_toml


def _make_fav(combo_key, strategy="test_strat", name="my combo", note="some note"):
    """Build a minimal FavRow for testing."""
    from backtester.ui.services.store_service import key_to_json, key_hash
    return FavRow(
        id=1,
        run_id=42,
        combo_hash=key_hash(combo_key),
        combo_key_json=key_to_json(combo_key),
        name=name,
        strategy=strategy,
        note=note,
        score=0.75,
        sharpe=1.5,
        total_pnl=300.0,
        params_str="delta=0.20  dte=1",
        added_at="2026-01-15T12:00:00Z",
    )


def test_roundtrip_via_tomllib():
    """favourite_to_toml output must be valid TOML parseable by tomllib."""
    key = (("delta", 0.20), ("dte", 1), ("stop_loss_pct", 3.5))
    fav = _make_fav(key)
    toml_str = favourite_to_toml(fav)
    # Must not raise
    parsed = tomllib.loads(toml_str)
    assert parsed["strategy"] == "test_strat"
    assert "param_grid" in parsed
    assert parsed["param_grid"]["dte"] == [1]
    assert abs(parsed["param_grid"]["delta"][0] - 0.20) < 1e-9
    assert abs(parsed["param_grid"]["stop_loss_pct"][0] - 3.5) < 1e-9


def test_single_value_param_grid():
    """Each param should appear as a single-element list."""
    key = (("width", 50), ("premium", 0.05))
    fav = _make_fav(key)
    toml_str = favourite_to_toml(fav)
    parsed = tomllib.loads(toml_str)
    assert parsed["param_grid"]["width"] == [50]
    assert abs(parsed["param_grid"]["premium"][0] - 0.05) < 1e-9


def test_bool_param():
    """Boolean parameters must round-trip correctly."""
    key = (("use_filter", True), ("delta", 0.25))
    fav = _make_fav(key)
    toml_str = favourite_to_toml(fav)
    parsed = tomllib.loads(toml_str)
    assert parsed["param_grid"]["use_filter"] == [True]


def test_string_param():
    """String parameters must round-trip correctly."""
    key = (("mode", "aggressive"), ("delta", 0.30))
    fav = _make_fav(key)
    toml_str = favourite_to_toml(fav)
    parsed = tomllib.loads(toml_str)
    assert parsed["param_grid"]["mode"] == ["aggressive"]
