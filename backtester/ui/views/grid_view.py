"""
views/grid_view.py — Results Grid tab.

Displays a Tabulator of all combo stats for a loaded GridResult.
Multi-select rows → updates state.selected_combo_keys.

Table layout
------------
The grid uses Tabulator ``fit_columns`` so the table always fills the main
pane width instead of growing past the viewport (Panel's default
``fit_data_table`` sizes columns to data and expands the table horizontally).

Columns are split into three zones with approximate width budget 5:47.5:47.5:

  1. Rank & score — frozen left, fixed pixel widths (~5% total)
  2. Strategy parameters — scrollable middle (~47.5% ``widthGrow`` budget)
  3. Performance metrics — frozen right (~47.5%, capped with ``maxWidth``)

Within each grow zone, ``widthGrow`` is split evenly across columns so the
zone totals stay balanced even when parameter and performance column counts
differ. Parameter headers use multi-line ``snake_case`` breaks plus hover
tooltips for the full field name. A horizontal scrollbar on the table holder
is enabled as a fallback when min-widths cannot all be satisfied at once.

Column chooser
--------------
Three checkbox sections mirror the table zones.  Each ``CheckBoxGroup`` carries
a shadow-DOM stylesheet that lays out checkboxes in a responsive CSS grid
(``auto-fill`` / ``minmax(140px, 1fr)``) so long parameter lists wrap inside
the pane instead of overflowing horizontally.
"""
import hashlib
import re as _re

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.store_service import key_hash

log = get_ui_logger(__name__)

# Columns shown by default (param columns are added dynamically)
_FIXED_DISPLAY_COLS = [
    "rank", "score", "n", "total_pnl", "sharpe", "profit_factor",
    "max_dd_pct", "win_rate", "avg_pnl", "omega", "consistency",
]

_RANK_SCORE_COLS = ("rank", "score")

# Width budget for grow zones (rank/score use fixed pixels; must sum to 100).
_ZONE_RANK_PCT = 5          # documentation reference; rank/score are fixed-width
_ZONE_PARAM_PCT = 47.5
_ZONE_PERF_PCT = 47.5

# Fixed pixel widths for the rank/score strip (checkbox col is separate).
_RANK_COL_WIDTH = 40
_SCORE_COL_WIDTH = 56

# Performance column sizing — maxWidth stops money formatters from ballooning.
_PERF_MIN_COL_WIDTH = 56
_PERF_MAX_COL_WIDTH = 76
_PARAM_MIN_COL_WIDTH = 48

_COL_FORMATTERS = {
    "score":          {"type": "progress", "min": 0, "max": 1, "color": "#1a9641"},
    "total_pnl":      {"type": "money", "symbol": "$", "precision": 0},
    "sharpe":         {"type": "number", "precision": 2},
    "profit_factor":  {"type": "number", "precision": 2},
    "max_dd_pct":     {"type": "number", "precision": 1, "suffix": "%"},
    "win_rate":       {"type": "number", "precision": 2},
    "avg_pnl":        {"type": "money", "symbol": "$", "precision": 1},
    "omega":          {"type": "number", "precision": 2},
    "consistency":    {"type": "number", "precision": 2},
}

# Keep the table inside the viewport; show a scrollbar only when min-widths
# cannot all be satisfied at once.
_TABLE_LAYOUT_CSS = """
.tabulator {
    width: 100% !important;
    max-width: 100% !important;
}
.tabulator-tableholder {
    overflow-x: auto !important;
}
"""

# Multi-line column headers (Tabulator defaults to single-line + ellipsis).
# pre-line honours embedded newlines from _header_display_title; anywhere
# breaks long tokens that have no underscore when space is very tight.
_HEADER_WRAP_CSS = """
.tabulator .tabulator-header {
    min-height: 52px !important;
}
.tabulator .tabulator-header .tabulator-col .tabulator-col-content .tabulator-col-title {
    white-space: pre-line !important;
    word-break: break-word;
    overflow-wrap: anywhere;
    line-height: 1.15;
    font-size: 11px;
}
"""

# Column chooser: Bokeh CheckBoxGroup renders in a shadow root with
# flex-wrap:nowrap on .bk-input-group.bk-inline — parent Column CSS cannot
# override it.  Pass this stylesheet on each CheckBoxGroup widget instead.
_COL_CHOOSER_WRAP_CSS = """
:host {
    width: 100%;
    max-width: 100%;
    display: block;
}
.bk-input-group.bk-inline {
    display: grid !important;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 4px 10px;
    width: 100%;
    white-space: normal !important;
    align-items: start;
}
.bk-input-group.bk-inline > label {
    margin-left: 0 !important;
    display: inline-flex;
    align-items: flex-start;
    min-width: 0;
}
.bk-input-group.bk-inline > label > span {
    white-space: normal;
    word-break: break-word;
    overflow-wrap: anywhere;
    line-height: 1.2;
    font-size: 12px;
}
"""

# Labels for the three chooser sections (plain DOM, not shadow-root).
_COL_CHOOSER_SECTION_CSS = """
.cryo-col-chooser-section {
    margin-bottom: 6px;
    width: 100%;
    max-width: 100%;
    overflow: hidden;
}
.cryo-col-chooser-label {
    font-size: 12px;
    font-weight: 600;
    color: #4b5563;
    margin: 4px 4px 2px 4px;
}
"""


def _param_hash(param_names: list) -> str:
    """Return a stable 12-char hex hash of the sorted param names."""
    return hashlib.sha256("|".join(sorted(param_names)).encode()).hexdigest()[:12]


def _split_grid_columns(columns: list[str]) -> tuple[list[str], list[str], list[str]]:
    """Partition display column names into rank/score, params, and performance.

    Args:
        columns: Ordered list of DataFrame column names (may include ``_key_hash``).

    Returns:
        (rank_cols, param_cols, perf_cols) preserving relative order within each zone.
    """
    rank_cols: list[str] = []
    param_cols: list[str] = []
    perf_cols: list[str] = []
    perf_set = {c for c in _FIXED_DISPLAY_COLS if c not in _RANK_SCORE_COLS}

    for col in columns:
        if col == "_key_hash":
            continue
        if col in _RANK_SCORE_COLS:
            rank_cols.append(col)
        elif col in perf_set:
            perf_cols.append(col)
        else:
            param_cols.append(col)

    return rank_cols, param_cols, perf_cols


def _header_display_title(field: str) -> str:
    """Return a multi-line column title, breaking snake_case at underscores.

    ``entry_time`` becomes ``entry`` + newline + ``time`` (rendered via
    ``pre-line`` CSS).  Short names without underscores are unchanged.
    """
    if "_" not in field:
        return field
    return field.replace("_", "\n")


def _header_tooltips(columns: list[str]) -> dict[str, str]:
    """Full field names for Tabulator header hover tooltips."""
    return {col: col for col in columns if col != "_key_hash"}


def _column_layout_config(
    rank_cols: list[str],
    param_cols: list[str],
    perf_cols: list[str],
) -> dict:
    """Build Tabulator ``configuration`` overrides for the three-zone layout.

    Uses flat column definitions (no Tabulator column groups) so Panel can
    merge per-field ``width`` / ``widthGrow`` / ``frozen`` settings reliably.

    Rank/score use fixed pixel widths.  Parameter and performance zones each
    receive ``_ZONE_PARAM_PCT`` / ``_ZONE_PERF_PCT`` of the grow budget,
    divided evenly across columns in that zone.

    Returns:
        Dict suitable for ``pn.widgets.Tabulator(..., configuration=cfg)``.
    """
    n_param = max(len(param_cols), 1)
    n_perf = max(len(perf_cols), 1)

    col_defs: list[dict] = []

    for col in rank_cols:
        width = _RANK_COL_WIDTH if col == "rank" else _SCORE_COL_WIDTH
        col_defs.append({
            "field": col,
            "title": col,
            "frozen": True,
            "width": width,
            "minWidth": width,
            "widthGrow": 0,
            "widthShrink": 0,
        })

    for col in param_cols:
        col_defs.append({
            "field": col,
            "title": _header_display_title(col),
            "widthGrow": _ZONE_PARAM_PCT / n_param,
            "minWidth": _PARAM_MIN_COL_WIDTH,
            "widthShrink": 4,
        })

    for col in perf_cols:
        col_defs.append({
            "field": col,
            "title": _header_display_title(col),
            "frozen": True,
            "widthGrow": _ZONE_PERF_PCT / n_perf,
            "minWidth": _PERF_MIN_COL_WIDTH,
            "maxWidth": _PERF_MAX_COL_WIDTH,
            "widthShrink": 1,
        })

    return {"columns": col_defs}


def _frozen_columns_map(
    rank_cols: list[str],
    perf_cols: list[str],
) -> dict[str, str]:
    """Map column names to freeze side for Panel's ``frozen_columns`` param."""
    frozen: dict[str, str] = {col: "left" for col in rank_cols}
    frozen.update({col: "right" for col in perf_cols})
    return frozen


def _header_zone_css(param_cols: list[str], perf_cols: list[str]) -> str:
    """Generate per-field header background tints for the three zones."""
    rank_sel = ", ".join(
        f'.tabulator-col[tabulator-field="{c}"]' for c in _RANK_SCORE_COLS
    )
    param_rules = "\n".join(
        f'.tabulator-col[tabulator-field="{c}"] '
        f'{{ background-color: #eff6ff !important; '
        f'border-bottom: 2px solid #93c5fd !important; }}'
        for c in param_cols
    )
    perf_rules = "\n".join(
        f'.tabulator-col[tabulator-field="{c}"] '
        f'{{ background-color: #f0fdf4 !important; '
        f'border-bottom: 2px solid #86efac !important; }}'
        for c in perf_cols
    )
    rank_rules = ""
    if rank_sel:
        rank_rules = f"""
{rank_sel} {{
    background-color: #f8fafc !important;
    border-bottom: 2px solid #cbd5e1 !important;
}}
"""
    return rank_rules + param_rules + perf_rules


def _grid_dataframe(result) -> tuple[pd.DataFrame, dict[str, tuple]]:
    """Build a flat DataFrame from GridResult for display in Tabulator.

    Returns:
        df   — one row per combo, columns = rank + score + params + stats + _key_hash
        hash_to_key — {key_hash_str: param_tuple}
    """
    if not result or not result.ranked:
        return pd.DataFrame(), {}

    rows = []
    hash_to_key: dict[str, tuple] = {}

    for rank, (key, stats) in enumerate(result.ranked, 1):
        kh = key_hash(key)
        hash_to_key[kh] = key
        params = dict(key)
        row = {"rank": rank, "score": round(result.scores[key], 4)}
        # Param columns
        for p in result.param_names:
            row[p] = params.get(p)
        # Stats columns
        row["n"] = stats.get("n", 0)
        row["total_pnl"] = round(float(stats.get("total_pnl", 0)), 2)
        row["sharpe"] = round(float(stats.get("sharpe", 0)), 3)
        row["profit_factor"] = round(float(stats.get("profit_factor", 0)), 2)
        row["max_dd_pct"] = round(float(stats.get("max_dd_pct", 0)), 2)
        row["win_rate"] = round(float(stats.get("win_rate", 0)), 3)
        row["avg_pnl"] = round(float(stats.get("avg_pnl", 0)), 2)
        row["omega"] = round(float(stats.get("omega", 0)), 2)
        row["consistency"] = round(float(stats.get("consistency", 0)), 3)
        # Hidden identity column
        row["_key_hash"] = kh
        rows.append(row)

    df = pd.DataFrame(rows)
    return df, hash_to_key


# ---------------------------------------------------------------------------
# Smart filter expression parser
# ---------------------------------------------------------------------------

_FILTER_OP_RE = _re.compile(r'^([A-Za-z_]\w*)([><!]=?|=)(.+)$')


def _parse_filter_expr(expr: str, columns: list[str]) -> tuple[list[dict], str]:
    """Parse a filter expression into Panel Tabulator filter dicts.

    Supported token syntax (whitespace-separated):
      col>1.5          →  {type: ">",  value: 1.5}
      col>=1.5         →  {type: ">=", value: 1.5}
      col:lo..hi       →  two filters: >= lo and <= hi
      col:a,b,c        →  regex match ^(a|b|c)$ (best for strings)
      col:text         →  {type: "like", value: "text"} (substring)

    Column names are matched case-insensitively.
    Returns (filters, error_str). error_str is "" on full success.
    """
    col_lower = {c.lower(): c for c in columns}
    filters: list[dict] = []
    errors: list[str] = []

    for token in expr.split():
        token = token.strip()
        if not token:
            continue

        # --- operator syntax: col>=value ---
        m = _FILTER_OP_RE.match(token)
        if m:
            col_raw, op, val_str = m.group(1), m.group(2), m.group(3)
            col = col_lower.get(col_raw.lower())
            if col is None:
                errors.append(f"unknown column '{col_raw}'")
                continue
            op_map = {">":  ">", ">=": ">=", "<":  "<", "<=": "<=",
                      "=":  "=", "!=": "!=", "!": "!="}
            ftype = op_map.get(op)
            if ftype is None:
                errors.append(f"unknown operator '{op}'")
                continue
            try:
                val: float | str = float(val_str)
            except ValueError:
                val = val_str
            filters.append({"field": col, "type": ftype, "value": val})
            continue

        # --- colon syntax: col:expr ---
        if ":" in token:
            col_raw, rest = token.split(":", 1)
            col = col_lower.get(col_raw.lower())
            if col is None:
                errors.append(f"unknown column '{col_raw}'")
                continue

            # Range: lo..hi
            if ".." in rest:
                parts = rest.split("..", 1)
                try:
                    lo, hi = float(parts[0]), float(parts[1])
                    filters.append({"field": col, "type": ">=", "value": lo})
                    filters.append({"field": col, "type": "<=", "value": hi})
                except (ValueError, IndexError):
                    errors.append(f"invalid range '{rest}'")
                continue

            # List: a,b,c  → non-capturing regex ^(?:a|b|c)$
            if "," in rest:
                items = [_re.escape(x.strip()) for x in rest.split(",") if x.strip()]
                filters.append({"field": col, "type": "regex",
                                 "value": "^(?:" + "|".join(items) + ")$"})
                continue

            # Single value
            try:
                val = float(rest)
                filters.append({"field": col, "type": "=", "value": val})
            except ValueError:
                filters.append({"field": col, "type": "like", "value": rest})
            continue

        errors.append(f"unrecognized token '{token}'")

    return filters, "; ".join(errors)


def _filter_dataframe(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
    """Apply _parse_filter_expr filter dicts to a DataFrame in Python.

    Used instead of tab.filters which behaves unreliably with
    pagination='remote' (tabulator.js resets client-side state on re-renders).
    """
    if not filters or df.empty:
        return df
    mask = pd.Series(True, index=df.index)
    for f in filters:
        field = f.get("field")
        ftype = f.get("type")
        val   = f.get("value")
        if field not in df.columns:
            continue
        col = df[field]
        try:
            if ftype == ">":
                mask &= col > val
            elif ftype == ">=":
                mask &= col >= val
            elif ftype == "<":
                mask &= col < val
            elif ftype == "<=":
                mask &= col <= val
            elif ftype in ("=", "=="):
                mask &= col == val
            elif ftype in ("!=", "!"):
                mask &= col != val
            elif ftype == "like":
                mask &= col.astype(str).str.contains(
                    str(val), case=False, na=False, regex=False)
            elif ftype == "regex":
                mask &= col.astype(str).str.fullmatch(
                    str(val), case=False, na=False)
        except (TypeError, ValueError):
            pass
    return df[mask].copy()


def _make_column_chooser() -> pn.widgets.CheckBoxGroup:
    """Inline CheckBoxGroup with grid-wrapping labels (shadow-DOM stylesheet)."""
    return pn.widgets.CheckBoxGroup(
        name="",
        options=[],
        value=[],
        inline=True,
        sizing_mode="stretch_width",
        margin=(0, 4),
        stylesheets=[_COL_CHOOSER_WRAP_CSS],
    )


def _make_chooser_section(label: str, chooser: pn.widgets.CheckBoxGroup) -> pn.Column:
    """Wrap a CheckBoxGroup with a zone label inside the column chooser panel."""
    return pn.Column(
        pn.pane.HTML(
            f'<div class="cryo-col-chooser-label">{label}</div>',
            margin=(0, 0),
            sizing_mode="stretch_width",
        ),
        chooser,
        css_classes=["cryo-col-chooser-section"],
        sizing_mode="stretch_width",
    )


def build_grid_view(state, cache, store=None) -> pn.Column:
    """Build the Results Grid tab component.

    Returns a Panel Column that re-renders when state.active_run_id changes.
    """
    # Mutable container so the callback can close over it
    _ctx: dict = {"tabulator": None, "hash_to_key": {}, "df_full": None, "ordered_cols": []}

    # --- selection indicator (plain text label) ---
    _sel_label = pn.pane.HTML("", styles={"font-size": "13px", "color": "#6b7280"}, margin=(8, 8))

    # --- "View Detail" button (enabled when exactly 1 combo is selected) ---
    _view_detail_btn = pn.widgets.Button(
        name="View Detail", button_type="primary", disabled=True, width=110
    )

    # --- "Star" button (Phase 4) ---
    _star_btn = pn.widgets.Button(
        name="☆ Star", button_type="light", disabled=True, width=90,
    )
    _star_feedback = pn.pane.HTML("", styles={"font-size": "11px"}, width=150)

    # --- CSV download button (Phase 5) ---
    def _get_csv():
        import io
        tab = _ctx.get("tabulator")
        if tab is None or tab.value is None or tab.value.empty:
            return io.StringIO("No data\n")
        df = tab.value.drop(columns=["_key_hash"], errors="ignore")
        return io.StringIO(df.to_csv(index=False))

    _csv_download = pn.widgets.FileDownload(
        callback=_get_csv,
        filename="results.csv",
        label="⬇ CSV",
        button_type="light",
        width=80,
        margin=(0, 4),
    )

    # --- Column chooser: three zones matching the table layout ---
    # CheckBoxGroup keeps deselected columns visible (unlike MultiChoice).
    _col_chooser_rank = _make_column_chooser()
    _col_chooser_params = _make_column_chooser()
    _col_chooser_perf = _make_column_chooser()
    _col_choosers = (_col_chooser_rank, _col_chooser_params, _col_chooser_perf)

    _col_chooser_panel = pn.Column(
        _make_chooser_section("Rank & score", _col_chooser_rank),
        _make_chooser_section("Parameters", _col_chooser_params),
        _make_chooser_section("Performance", _col_chooser_perf),
        visible=False,
        sizing_mode="stretch_width",
        css_classes=["cryo-col-chooser"],
        stylesheets=[_COL_CHOOSER_SECTION_CSS],
    )
    _cols_toggle = pn.widgets.Toggle(
        name="⚙ Columns", value=False, button_type="light",
        width=105, margin=(0, 4),
    )
    _cols_toggle.param.watch(
        lambda e: setattr(_col_chooser_panel, "visible", e.new), "value"
    )
    # (chooser, watcher_id) pairs — cleared when the active run changes
    _col_watchers: list[tuple[pn.widgets.CheckBoxGroup, object]] = []

    def _merged_chooser_selection() -> list[str]:
        return (
            list(_col_chooser_rank.value)
            + list(_col_chooser_params.value)
            + list(_col_chooser_perf.value)
        )

    def _clear_col_choosers():
        for chooser in _col_choosers:
            chooser.options = []
            chooser.value = []

    def _set_col_choosers(rank_opts, param_opts, perf_opts, visible: list[str]):
        _col_chooser_rank.options = rank_opts
        _col_chooser_params.options = param_opts
        _col_chooser_perf.options = perf_opts
        _col_chooser_rank.value = [c for c in visible if c in rank_opts]
        _col_chooser_params.value = [c for c in visible if c in param_opts]
        _col_chooser_perf.value = [c for c in visible if c in perf_opts]

    # --- Smart filter expression input ---
    _filter_input = pn.widgets.TextInput(
        placeholder="e.g.  sharpe>1.5   pnl:0..5000   exit_reason:trigger,expiry",
        name="",
        sizing_mode="stretch_width",
        margin=(2, 4),
    )
    _filter_feedback = pn.pane.HTML("", styles={"font-size": "11px"}, width=320)
    _filter_clear = pn.widgets.Button(
        name="✕ Clear", width=70, button_type="light", margin=(2, 4),
    )
    _filter_row = pn.Row(
        pn.pane.Markdown("**Filter:**", margin=(8, 4)),
        _filter_input,
        _filter_clear,
        _filter_feedback,
        pn.Spacer(),
        _csv_download,
        _cols_toggle,
        sizing_mode="stretch_width",
    )

    def _apply_current_filter(tab):
        """Filter the tabulator's source DataFrame based on the filter expression.

        Sets tab.value directly (Python-side) rather than using tab.filters,
        which is unreliable with pagination='remote'.
        """
        df_full = _ctx.get("df_full")
        if df_full is None:
            return
        expr = _filter_input.value.strip()
        if not expr:
            tab.value = df_full
            _filter_feedback.object = ""
            return
        flt, err = _parse_filter_expr(expr, list(df_full.columns))
        filtered = _filter_dataframe(df_full, flt)
        tab.value = filtered
        n_shown, n_total = len(filtered), len(df_full)
        _filter_feedback.object = (
            f"<span style='color:#dc2626'>&#9888; {err}</span>" if err else
            f"<span style='color:#6b7280'>{n_shown} / {n_total} rows</span>"
        )

    def _on_filter_change(event):
        tab = _ctx.get("tabulator")
        if tab is not None:
            _apply_current_filter(tab)

    _filter_input.param.watch(_on_filter_change, "value")
    _filter_clear.on_click(lambda e: setattr(_filter_input, "value", ""))

    # --- placeholder while no run loaded ---
    _placeholder = pn.pane.Markdown(
        "_No run loaded — select a run from the sidebar._",
        sizing_mode="stretch_width",
    )

    _content = pn.Column(_placeholder, sizing_mode="stretch_width")

    def _build_tabulator(result):
        """(Re)build the Tabulator from a fresh GridResult."""
        df, hash_to_key = _grid_dataframe(result)
        _ctx["hash_to_key"] = hash_to_key

        if df.empty:
            _ctx["tabulator"] = None
            _ctx["ordered_cols"] = []
            _clear_col_choosers()
            return pn.pane.Markdown("_No combos to display._")

        # Param column names (not in fixed list, not hidden)
        param_cols = [c for c in df.columns
                      if c not in _FIXED_DISPLAY_COLS and c != "_key_hash"]
        ordered_cols = ["rank", "score"] + param_cols + [
            c for c in _FIXED_DISPLAY_COLS[2:] if c in df.columns
        ]
        ordered_cols = [c for c in ordered_cols if c in df.columns]
        _ctx["ordered_cols"] = ordered_cols

        rank_cols, param_cols_zoned, perf_cols = _split_grid_columns(ordered_cols)

        # Load column preset from store (Phase 5)
        hidden_user: list[str] = []
        strategy = ""
        ph = _param_hash(result.param_names)
        if store and state.active_run_id:
            rr = store.get_run(state.active_run_id)
            strategy = rr.strategy if rr else ""
            preset = store.load_column_preset(strategy, ph)
            if preset is not None:
                hidden_user = [c for c in preset if c in ordered_cols]

        visible_cols = [c for c in ordered_cols if c not in hidden_user]

        # Clear previous column chooser watchers
        for chooser, watcher in _col_watchers:
            try:
                chooser.param.unwatch(watcher)
            except Exception:
                pass
        _col_watchers.clear()

        _set_col_choosers(rank_cols, param_cols_zoned, perf_cols, visible_cols)

        # Panel Tabulator does not accept a `columns` arg — subset the
        # DataFrame directly to control which columns appear and in what order.
        df_display = df[ordered_cols + ["_key_hash"]]
        _ctx["df_full"] = df_display  # kept for Python-side filter operations

        tab_hidden = ["_key_hash"] + hidden_user
        layout_cfg = _column_layout_config(rank_cols, param_cols_zoned, perf_cols)
        zone_css = _header_zone_css(param_cols_zoned, perf_cols)

        tab = pn.widgets.Tabulator(
            df_display,
            hidden_columns=tab_hidden,
            configuration=layout_cfg,
            frozen_columns=_frozen_columns_map(rank_cols, perf_cols),
            formatters=_COL_FORMATTERS,
            header_tooltips=_header_tooltips(ordered_cols),
            layout="fit_columns",
            pagination="remote",
            page_size=200,
            selectable="checkbox",
            header_filters=True,
            sizing_mode="stretch_width",
            show_index=False,
            stylesheets=[_TABLE_LAYOUT_CSS, _HEADER_WRAP_CSS, zone_css],
        )
        tab.editable = False
        tab.editors = {col: None for col in df_display.columns}

        def _on_selection(event):
            indices = event.new
            keys = [
                _ctx["hash_to_key"][tab.value.iloc[i]["_key_hash"]]
                for i in indices
                if i < len(tab.value) and tab.value.iloc[i]["_key_hash"] in _ctx["hash_to_key"]
            ]
            state.selected_combo_keys = keys
            n = len(keys)
            _sel_label.object = (f"<span>{n} combo{'s' if n != 1 else ''} selected</span>" if n else "")
            # Enable "View Detail" only when exactly one combo is selected
            _view_detail_btn.disabled = len(keys) != 1
            if len(keys) == 1:
                state.active_combo_key = keys[0]
                _refresh_star_btn(keys[0])
            else:
                _star_btn.disabled = True
                _star_btn.name = "☆ Star"
                _star_feedback.object = ""
            log.debug("Grid selection: %d combos", len(keys))

        tab.param.watch(_on_selection, "selection")
        _ctx["tabulator"] = tab
        # Re-apply current filter expression to the freshly built tabulator
        _apply_current_filter(tab)

        # Wire column choosers to this tabulator (Phase 5)
        def _on_col_change(event):
            selected = _merged_chooser_selection()
            ordered = _ctx["ordered_cols"]
            new_hidden = ["_key_hash"] + [c for c in ordered if c not in selected]
            tab.hidden_columns = new_hidden
            if store and strategy:
                store.save_column_preset(
                    strategy, ph,
                    [c for c in ordered if c not in selected],
                )

        for chooser in _col_choosers:
            watcher = chooser.param.watch(_on_col_change, "value")
            _col_watchers.append((chooser, watcher))

        return tab

    def _on_run_change(event):
        run_id = event.new
        if run_id is None:
            _content[:] = [_placeholder]
            _sel_label.object = ""
            return
        try:
            result = cache.get(run_id)
            _content[:] = [_build_tabulator(result)]
        except Exception as exc:
            log.error("grid_view: failed to load run_id=%s — %s", run_id, exc)
            _content[:] = [pn.pane.Markdown(f"⚠ Error loading run: {exc}")]

    state.param.watch(_on_run_change, "active_run_id")

    # ── Star button helpers (Phase 4) ─────────────────────────────────────────
    def _refresh_star_btn(key):
        if store is None:
            _star_btn.disabled = True
            return
        run_id = state.active_run_id
        if run_id is None:
            _star_btn.disabled = True
            return
        fav = store.get_favourite_by_combo(run_id, key)
        _star_btn.disabled = False
        _star_btn.name = "★ Unstar" if fav else "☆ Star"

    def _on_star(event):
        if store is None:
            return
        key = state.active_combo_key
        run_id = state.active_run_id
        if key is None or run_id is None:
            return
        try:
            fav = store.get_favourite_by_combo(run_id, key)
            if fav:
                store.remove_favourite(fav.id)
                _star_btn.name = "☆ Star"
                _star_feedback.object = "<span style='color:#d97706'>Removed from favourites.</span>"
            else:
                result = cache.get(run_id)
                stats = result.all_stats.get(key, {}) if result else {}
                rr = store.get_run(run_id)
                strategy = rr.strategy if rr else ""
                params_str = "  ".join(f"{k}={v}" for k, v in key)
                store.add_favourite(
                    run_id=run_id,
                    combo_key=key,
                    name=params_str[:60],
                    score=result.scores.get(key) if result else None,
                    sharpe=float(stats.get("sharpe", 0)) if stats.get("sharpe") is not None else None,
                    total_pnl=float(stats.get("total_pnl", 0)) if stats.get("total_pnl") is not None else None,
                    params_str=params_str,
                    strategy=strategy,
                )
                _star_btn.name = "★ Unstar"
                _star_feedback.object = "<span style='color:#16a34a'>★ Starred!</span>"
        except Exception as exc:
            _star_feedback.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("grid_view: star toggle failed: %s", exc)

    _star_btn.on_click(_on_star)

    _action_row = pn.Row(
        pn.pane.Markdown("### Results Grid", margin=(5, 10)),
        pn.Spacer(),
        _view_detail_btn,
        _star_btn,
        _star_feedback,
        _sel_label,
        sizing_mode="stretch_width",
    )

    def _on_view_detail(event):
        """Switch to the Combo Detail tab for the selected combo."""
        if state.selected_combo_keys:
            state.active_combo_key = state.selected_combo_keys[0]
        state.active_tab = "Combo Detail"

    _view_detail_btn.on_click(_on_view_detail)

    return pn.Column(_action_row, _filter_row, _col_chooser_panel, _content, sizing_mode="stretch_width")
