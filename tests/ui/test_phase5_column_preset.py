"""
tests/ui/test_phase5_column_preset.py — Column preset persistence in StoreService.

Tests:
  - test_save_and_load_preset      — basic round-trip
  - test_load_missing_returns_none — no preset → None (fallback to defaults)
  - test_mismatched_schema_returns_none — different param_hash → None
  - test_update_existing_preset    — upsert overwrites correctly
  - test_empty_hidden_list         — empty list is valid (all cols visible)
"""
import pytest


def test_save_and_load_preset(sqlite_store):
    sqlite_store.save_column_preset("my_strat", "abc123", ["profit_factor", "omega"])
    result = sqlite_store.load_column_preset("my_strat", "abc123")
    assert result == ["profit_factor", "omega"]


def test_load_missing_returns_none(sqlite_store):
    result = sqlite_store.load_column_preset("unknown_strat", "000000")
    assert result is None


def test_mismatched_schema_returns_none(sqlite_store):
    """Different param_hash = different strategy schema = fall back to defaults."""
    sqlite_store.save_column_preset("strat_a", "hash1", ["col_x"])
    # same strategy, different hash (schema changed) → None
    result = sqlite_store.load_column_preset("strat_a", "hash2")
    assert result is None


def test_update_existing_preset(sqlite_store):
    sqlite_store.save_column_preset("strat_b", "hashX", ["col_a"])
    sqlite_store.save_column_preset("strat_b", "hashX", ["col_b", "col_c"])
    result = sqlite_store.load_column_preset("strat_b", "hashX")
    assert result == ["col_b", "col_c"]


def test_empty_hidden_list(sqlite_store):
    """Saving an empty list (no hidden cols) is valid and round-trips correctly."""
    sqlite_store.save_column_preset("strat_c", "hashY", [])
    result = sqlite_store.load_column_preset("strat_c", "hashY")
    assert result == []


def test_user_pref_set_get(sqlite_store):
    sqlite_store.set_pref("dark_mode", "1")
    assert sqlite_store.get_pref("dark_mode") == "1"


def test_user_pref_default(sqlite_store):
    assert sqlite_store.get_pref("nonexistent", "0") == "0"


def test_user_pref_upsert(sqlite_store):
    sqlite_store.set_pref("theme", "dark")
    sqlite_store.set_pref("theme", "light")
    assert sqlite_store.get_pref("theme") == "light"
