"""
tests/ui/test_param_csv_parser.py — Unit tests for parse_param_csv.
"""
import pytest
from backtester.ui.views.sidebar import parse_param_csv


# ── int parsing ───────────────────────────────────────────────────────────────

def test_parse_int_csv_single():
    vals, err = parse_param_csv("dte", "7", 1)
    assert err is None
    assert vals == [7]


def test_parse_int_csv_multiple():
    vals, err = parse_param_csv("dte", "7, 14, 21", 1)
    assert err is None
    assert vals == [7, 14, 21]


def test_parse_int_csv_leading_trailing_spaces():
    vals, err = parse_param_csv("dte", "  7 , 14 ", 1)
    assert err is None
    assert vals == [7, 14]


def test_parse_int_csv_bad_value():
    vals, err = parse_param_csv("dte", "7, abc", 1)
    assert vals is None
    assert err is not None
    assert "dte" in err


# ── float parsing ─────────────────────────────────────────────────────────────

def test_parse_float_csv_single():
    vals, err = parse_param_csv("delta", "0.24", 0.1)
    assert err is None
    assert vals == pytest.approx([0.24])


def test_parse_float_csv_multiple():
    vals, err = parse_param_csv("delta", "0.20, 0.25, 0.30", 0.1)
    assert err is None
    assert vals == pytest.approx([0.20, 0.25, 0.30])


def test_parse_float_csv_integer_input_for_float_sample():
    # "3" is valid as float
    vals, err = parse_param_csv("take_profit", "3", 1.0)
    assert err is None
    assert vals == pytest.approx([3.0])


def test_parse_float_csv_bad_value():
    vals, err = parse_param_csv("delta", "0.24, !", 0.1)
    assert vals is None
    assert err is not None


# ── bool parsing ─────────────────────────────────────────────────────────────

def test_parse_bool_csv_true_variants():
    for v in ("true", "True", "1", "yes", "YES"):
        vals, err = parse_param_csv("hedge", v, True)
        assert err is None, f"expected success for {v!r}"
        assert vals == [True]


def test_parse_bool_csv_false_variants():
    for v in ("false", "False", "0", "no", "NO"):
        vals, err = parse_param_csv("hedge", v, True)
        assert err is None, f"expected success for {v!r}"
        assert vals == [False]


def test_parse_bool_csv_mixed():
    vals, err = parse_param_csv("hedge", "true, false", True)
    assert err is None
    assert vals == [True, False]


def test_parse_bool_csv_bad_value():
    vals, err = parse_param_csv("hedge", "maybe", True)
    assert vals is None
    assert err is not None
    assert "hedge" in err


# ── string parsing ────────────────────────────────────────────────────────────

def test_parse_str_csv():
    vals, err = parse_param_csv("mode", "aggressive, conservative", "default")
    assert err is None
    assert vals == ["aggressive", "conservative"]


# ── empty input ───────────────────────────────────────────────────────────────

def test_empty_csv_returns_error():
    vals, err = parse_param_csv("dte", "", 1)
    assert vals is None
    assert err is not None
    assert "dte" in err


def test_whitespace_only_csv_returns_error():
    vals, err = parse_param_csv("dte", "   ,  ", 1)
    assert vals is None
    assert err is not None
