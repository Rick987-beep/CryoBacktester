"""tests/ui/test_grid_layout.py — Results Grid three-zone table layout helpers."""
import pytest


def test_split_grid_columns_partitions_zones():
    from backtester.ui.views.grid_view import _split_grid_columns

    cols = [
        "rank", "score",
        "delta", "dte",
        "n", "total_pnl", "sharpe",
        "_key_hash",
    ]
    rank, param, perf = _split_grid_columns(cols)
    assert rank == ["rank", "score"]
    assert param == ["delta", "dte"]
    assert perf == ["n", "total_pnl", "sharpe"]


def test_header_display_title_breaks_snake_case():
    from backtester.ui.views.grid_view import _header_display_title

    assert _header_display_title("rank") == "rank"
    assert _header_display_title("entry_time") == "entry\ntime"
    assert _header_display_title("proximity_buffer_usd") == "proximity\nbuffer\nusd"


def test_header_tooltips_maps_all_display_columns():
    from backtester.ui.views.grid_view import _header_tooltips

    tips = _header_tooltips(["rank", "delta", "_key_hash"])
    assert tips == {"rank": "rank", "delta": "delta"}
    assert "_key_hash" not in tips


def test_column_layout_config_grow_zone_budgets():
    from backtester.ui.views.grid_view import (
        _SCORE_COL_WIDTH,
        _RANK_COL_WIDTH,
        _ZONE_PARAM_PCT,
        _ZONE_PERF_PCT,
        _column_layout_config,
    )

    rank = ["rank", "score"]
    param = ["delta", "dte", "qty"]
    perf = ["total_pnl", "sharpe"]

    cfg = _column_layout_config(rank, param, perf)
    by_field = {c["field"]: c for c in cfg["columns"]}

    assert by_field["rank"]["width"] == _RANK_COL_WIDTH
    assert by_field["score"]["width"] == _SCORE_COL_WIDTH
    assert by_field["rank"]["widthGrow"] == 0
    assert by_field["score"]["widthShrink"] == 0

    assert sum(by_field[c]["widthGrow"] for c in param) == pytest.approx(_ZONE_PARAM_PCT)
    assert sum(by_field[c]["widthGrow"] for c in perf) == pytest.approx(_ZONE_PERF_PCT)

    assert by_field["rank"]["frozen"] is True
    assert by_field["total_pnl"]["frozen"] is True
    assert "frozen" not in by_field["delta"]


def test_column_layout_config_param_columns_shrink_more_than_perf():
    from backtester.ui.views.grid_view import _column_layout_config

    cfg = _column_layout_config(["rank"], ["delta"], ["sharpe"])
    by_field = {c["field"]: c for c in cfg["columns"]}
    assert by_field["delta"]["widthShrink"] > by_field["sharpe"]["widthShrink"]


def test_column_layout_config_perf_columns_have_max_width():
    from backtester.ui.views.grid_view import (
        _PERF_MAX_COL_WIDTH,
        _column_layout_config,
    )

    cfg = _column_layout_config(["rank"], [], ["total_pnl", "sharpe"])
    by_field = {c["field"]: c for c in cfg["columns"]}
    assert by_field["total_pnl"]["maxWidth"] == _PERF_MAX_COL_WIDTH
    assert by_field["sharpe"]["maxWidth"] == _PERF_MAX_COL_WIDTH


def test_column_layout_config_param_titles_are_multiline():
    from backtester.ui.views.grid_view import _column_layout_config

    cfg = _column_layout_config([], ["entry_time"], [])
    by_field = {c["field"]: c for c in cfg["columns"]}
    assert by_field["entry_time"]["title"] == "entry\ntime"


def test_frozen_columns_map_pins_left_and_right():
    from backtester.ui.views.grid_view import _frozen_columns_map

    frozen = _frozen_columns_map(["rank", "score"], ["total_pnl", "sharpe"])
    assert frozen == {
        "rank": "left",
        "score": "left",
        "total_pnl": "right",
        "sharpe": "right",
    }


def test_header_zone_css_includes_field_selectors():
    from backtester.ui.views.grid_view import _header_zone_css

    css = _header_zone_css(["delta"], ["sharpe"])
    assert 'tabulator-field="delta"' in css
    assert 'tabulator-field="sharpe"' in css
    assert 'tabulator-field="rank"' in css


def test_make_column_chooser_has_wrap_stylesheet():
    from backtester.ui.views.grid_view import (
        _COL_CHOOSER_WRAP_CSS,
        _make_column_chooser,
    )

    chooser = _make_column_chooser()
    assert chooser.stylesheets == [_COL_CHOOSER_WRAP_CSS]
    assert "auto-fill" in _COL_CHOOSER_WRAP_CSS
    assert "bk-input-group.bk-inline" in _COL_CHOOSER_WRAP_CSS


def test_build_grid_view_imports_and_returns_column(tiny_grid_result):
    """Smoke test: build_grid_view still constructs after layout refactor."""
    import panel as pn

    from backtester.ui.state import AppState
    from backtester.ui.views.grid_view import build_grid_view

    pn.extension("tabulator", sizing_mode="stretch_width")

    class _Cache:
        def get(self, run_id):
            return tiny_grid_result

    state = AppState()
    col = build_grid_view(state, _Cache())
    assert isinstance(col, pn.Column)
    assert len(col.objects) >= 3


def test_grid_formatters_param_delta_is_plain_number():
    import pandas as pd

    from backtester.ui.views.grid_view import _grid_formatters

    df = pd.DataFrame({"delta": [0.20, 0.25], "mode": ["a", "b"]})
    fmts = _grid_formatters(df, ["delta", "mode"])
    assert fmts["delta"] == {"type": "number", "precision": 2}
    assert "mode" not in fmts


def test_patch_layout_merge_by_field_keeps_formatters_aligned():
    """Layout merge by field must not shift formatters onto wrong columns."""
    import panel as pn

    from backtester.ui.views.grid_view import (
        _FIXED_DISPLAY_COLS,
        _column_layout_config,
        _frozen_columns_map,
        _grid_dataframe,
        _grid_formatters,
        _header_tooltips,
        _patch_layout_merge_by_field,
        _split_grid_columns,
    )
    from tests.ui.conftest import _make_tiny_grid_result

    pn.extension("tabulator", sizing_mode="stretch_width")

    result = _make_tiny_grid_result()
    df, _ = _grid_dataframe(result)
    param_cols = [
        c for c in df.columns
        if c not in _FIXED_DISPLAY_COLS and c != "_key_hash"
    ]
    ordered_cols = ["rank", "score"] + param_cols + [
        c for c in _FIXED_DISPLAY_COLS[2:] if c in df.columns
    ]
    rank_cols, param_cols_zoned, perf_cols = _split_grid_columns(ordered_cols)
    layout_cfg = _column_layout_config(rank_cols, param_cols_zoned, perf_cols)
    layout_columns = layout_cfg.pop("columns", [])
    df_display = df[ordered_cols + ["_key_hash"]]

    tab = pn.widgets.Tabulator(
        df_display,
        hidden_columns=["_key_hash"],
        configuration=layout_cfg,
        frozen_columns=_frozen_columns_map(rank_cols, perf_cols),
        formatters=_grid_formatters(df_display, param_cols_zoned),
        header_tooltips=_header_tooltips(ordered_cols),
        layout="fit_columns",
        pagination="remote",
        page_size=200,
        selectable="checkbox",
        show_index=False,
    )
    _patch_layout_merge_by_field(tab, layout_columns)

    cfg_cols = tab._get_configuration(tab._get_columns())["columns"]
    by_field = {c["field"]: c for c in cfg_cols if c.get("field")}

    assert by_field["delta"]["formatter"] == "number"
    assert by_field["delta"]["formatterParams"] == {"precision": 2}
    assert by_field["score"]["formatter"] == "progress"
    assert by_field["profit_factor"]["formatter"] == "number"
    assert by_field["profit_factor"]["formatterParams"] == {"precision": 2}
    assert by_field["total_pnl"]["formatter"] == "money"
    assert by_field["sharpe"]["formatter"] == "number"
