"""
tests/ui/test_phase5_range_shorthand_parser.py — Param-grid range shorthand.

Tests:
  - test_integer_range           — 10..50:5
  - test_float_range             — 0.1..0.5:0.1
  - test_default_step_int        — 3..7 (step=1)
  - test_default_step_float      — 0.0..0.2 (step=1.0 → [0.0, 1.0] would be odd, but valid)
  - test_single_value_range      — 5..5:1 → [5]
  - test_invalid_step_rejected   — step <= 0
  - test_start_gt_end_rejected   — start > end
  - test_csv_with_range          — parse_param_csv mix: "0.1..0.3:0.1, 0.5"
  - test_csv_pure_range          — parse_param_csv pure range
  - test_integer_range_in_int_param
"""
import pytest


def test_integer_range():
    from backtester.ui.views.sidebar import _expand_range_token
    result = _expand_range_token("10..50:5")
    assert result == [10, 15, 20, 25, 30, 35, 40, 45, 50]
    assert all(isinstance(v, int) for v in result)


def test_float_range():
    from backtester.ui.views.sidebar import _expand_range_token
    result = _expand_range_token("0.1..0.5:0.1")
    assert len(result) == 5
    assert abs(result[0] - 0.1) < 1e-9
    assert abs(result[-1] - 0.5) < 1e-9
    assert all(isinstance(v, float) for v in result)


def test_default_step_int():
    from backtester.ui.views.sidebar import _expand_range_token
    result = _expand_range_token("3..7")
    assert result == [3, 4, 5, 6, 7]


def test_single_value_range():
    from backtester.ui.views.sidebar import _expand_range_token
    result = _expand_range_token("5..5:1")
    assert result == [5]


def test_large_int_step():
    from backtester.ui.views.sidebar import _expand_range_token
    result = _expand_range_token("0..100:25")
    assert result == [0, 25, 50, 75, 100]


def test_invalid_step_rejected():
    from backtester.ui.views.sidebar import _expand_range_token
    with pytest.raises(ValueError):
        _expand_range_token("1..5:-1")


def test_zero_step_rejected():
    from backtester.ui.views.sidebar import _expand_range_token
    with pytest.raises(ValueError):
        _expand_range_token("1..5:0")


def test_start_gt_end_rejected():
    from backtester.ui.views.sidebar import _expand_range_token
    with pytest.raises(ValueError):
        _expand_range_token("10..5:1")


def test_csv_with_range_mixed(tiny_grid_result):
    """parse_param_csv: range shorthand mixed with plain CSV values."""
    from backtester.ui.views.sidebar import parse_param_csv
    vals, err = parse_param_csv("delta", "0.1..0.3:0.1, 0.5", 0.1)
    assert err is None
    assert len(vals) == 4
    assert abs(vals[0] - 0.1) < 1e-9
    assert abs(vals[-1] - 0.5) < 1e-9


def test_csv_pure_range_float():
    from backtester.ui.views.sidebar import parse_param_csv
    vals, err = parse_param_csv("tp_pct", "0.05..0.20:0.05", 0.05)
    assert err is None
    assert len(vals) == 4


def test_csv_pure_range_int():
    from backtester.ui.views.sidebar import parse_param_csv
    vals, err = parse_param_csv("dte", "1..5:1", 1)
    assert err is None
    assert vals == [1, 2, 3, 4, 5]
