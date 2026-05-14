"""Unit tests for the Results Grid filter expression parser and DataFrame filter."""
import pytest
import pandas as pd

from backtester.ui.views.grid_view import _parse_filter_expr, _filter_dataframe

COLS = ["rank", "score", "sharpe", "pnl", "max_dd_pct", "exit_reason", "strategy"]

_SAMPLE_DF = pd.DataFrame({
    "rank":        [1, 2, 3, 4, 5],
    "sharpe":      [2.1, 1.5, 0.8, 1.9, 0.3],
    "pnl":         [5000.0, 1200.0, -300.0, 3000.0, -100.0],
    "exit_reason": ["trigger", "expiry", "trigger", "time_exit", "expiry"],
    "_key_hash":   ["a", "b", "c", "d", "e"],
})


# ── Operator syntax ──────────────────────────────────────────────────────────

def test_gt_operator():
    flt, err = _parse_filter_expr("sharpe>1.5", COLS)
    assert err == ""
    assert flt == [{"field": "sharpe", "type": ">", "value": 1.5}]


def test_gte_operator():
    flt, err = _parse_filter_expr("sharpe>=2", COLS)
    assert err == ""
    assert flt == [{"field": "sharpe", "type": ">=", "value": 2.0}]


def test_lt_operator():
    flt, err = _parse_filter_expr("max_dd_pct<20", COLS)
    assert err == ""
    assert flt == [{"field": "max_dd_pct", "type": "<", "value": 20.0}]


def test_lte_operator():
    flt, err = _parse_filter_expr("max_dd_pct<=15", COLS)
    assert err == ""
    assert flt == [{"field": "max_dd_pct", "type": "<=", "value": 15.0}]


def test_eq_operator():
    flt, err = _parse_filter_expr("rank=1", COLS)
    assert err == ""
    assert flt == [{"field": "rank", "type": "=", "value": 1.0}]


def test_neq_operator():
    flt, err = _parse_filter_expr("rank!=1", COLS)
    assert err == ""
    assert flt == [{"field": "rank", "type": "!=", "value": 1.0}]


def test_multiple_operator_tokens():
    flt, err = _parse_filter_expr("sharpe>1 pnl>0", COLS)
    assert err == ""
    assert len(flt) == 2
    assert flt[0] == {"field": "sharpe", "type": ">", "value": 1.0}
    assert flt[1] == {"field": "pnl", "type": ">", "value": 0.0}


# ── Colon range syntax ───────────────────────────────────────────────────────

def test_range_syntax():
    flt, err = _parse_filter_expr("pnl:0..5000", COLS)
    assert err == ""
    assert len(flt) == 2
    assert {"field": "pnl", "type": ">=", "value": 0.0} in flt
    assert {"field": "pnl", "type": "<=", "value": 5000.0} in flt


def test_range_negative():
    flt, err = _parse_filter_expr("pnl:-500..500", COLS)
    assert err == ""
    assert any(f["type"] == ">=" and f["value"] == -500.0 for f in flt)
    assert any(f["type"] == "<=" and f["value"] == 500.0 for f in flt)


# ── Colon list syntax ────────────────────────────────────────────────────────

def test_list_syntax_strings():
    flt, err = _parse_filter_expr("exit_reason:trigger,expiry", COLS)
    assert err == ""
    assert len(flt) == 1
    assert flt[0]["field"] == "exit_reason"
    assert flt[0]["type"] == "regex"
    # Regex should match both values
    import re
    pattern = flt[0]["value"]
    assert re.match(pattern, "trigger")
    assert re.match(pattern, "expiry")
    assert not re.match(pattern, "other")


# ── Colon single value ───────────────────────────────────────────────────────

def test_single_numeric_value():
    flt, err = _parse_filter_expr("rank:1", COLS)
    assert err == ""
    assert flt == [{"field": "rank", "type": "=", "value": 1.0}]


def test_single_string_value():
    flt, err = _parse_filter_expr("strategy:short", COLS)
    assert err == ""
    assert flt == [{"field": "strategy", "type": "like", "value": "short"}]


# ── Case-insensitive column names ─────────────────────────────────────────────

def test_column_name_case_insensitive():
    flt, err = _parse_filter_expr("SHARPE>1.5", COLS)
    assert err == ""
    assert flt[0]["field"] == "sharpe"


# ── Error handling ───────────────────────────────────────────────────────────

def test_unknown_column_reports_error():
    flt, err = _parse_filter_expr("notacol>1", COLS)
    assert "unknown column" in err
    assert flt == []


def test_invalid_range_reports_error():
    flt, err = _parse_filter_expr("pnl:abc..def", COLS)
    assert "invalid range" in err


def test_unrecognized_token_reports_error():
    flt, err = _parse_filter_expr("justtext", COLS)
    assert "unrecognized token" in err


def test_unknown_column_partial_success():
    """Valid tokens still produce filters; only the bad token reports an error."""
    flt, err = _parse_filter_expr("sharpe>1 notacol>1", COLS)
    assert "unknown column" in err
    assert len(flt) == 1
    assert flt[0]["field"] == "sharpe"


# ── Empty / whitespace input ─────────────────────────────────────────────────

def test_empty_expr():
    flt, err = _parse_filter_expr("", COLS)
    assert flt == []
    assert err == ""


def test_whitespace_only_expr():
    flt, err = _parse_filter_expr("   ", COLS)
    assert flt == []
    assert err == ""


# ── _filter_dataframe ────────────────────────────────────────────────────────

def test_filter_df_gt():
    flt = [{"field": "sharpe", "type": ">", "value": 1.5}]
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert list(result["sharpe"]) == [2.1, 1.9]


def test_filter_df_range():
    flt = [{"field": "pnl", "type": ">=", "value": 0.0},
           {"field": "pnl", "type": "<=", "value": 3000.0}]
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert set(result["pnl"]) == {1200.0, 3000.0}


def test_filter_df_like():
    flt = [{"field": "exit_reason", "type": "like", "value": "trigger"}]
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert len(result) == 2
    assert all(result["exit_reason"] == "trigger")


def test_filter_df_regex_list():
    flt, _ = _parse_filter_expr("exit_reason:trigger,expiry", list(_SAMPLE_DF.columns))
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert set(result["exit_reason"]) == {"trigger", "expiry"}
    assert "time_exit" not in result["exit_reason"].values


def test_filter_df_empty_filters():
    result = _filter_dataframe(_SAMPLE_DF, [])
    assert len(result) == len(_SAMPLE_DF)


def test_filter_df_unknown_field_ignored():
    flt = [{"field": "nonexistent", "type": ">", "value": 0}]
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert len(result) == len(_SAMPLE_DF)


def test_filter_df_full_pipeline():
    """Parse an expression and apply it end-to-end."""
    flt, err = _parse_filter_expr("sharpe>1.5 pnl>0", list(_SAMPLE_DF.columns))
    assert err == ""
    result = _filter_dataframe(_SAMPLE_DF, flt)
    assert len(result) == 2
    assert all(result["sharpe"] > 1.5)
    assert all(result["pnl"] > 0)
