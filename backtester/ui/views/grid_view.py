"""
views/grid_view.py — Results Grid tab.

Displays a Tabulator of all combo stats for a loaded GridResult.
Multi-select rows → updates state.selected_combo_keys.
"""
import hashlib
import json
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


def _param_hash(param_names: list) -> str:
    """Return a stable 12-char hex hash of the sorted param names."""
    return hashlib.sha256("|".join(sorted(param_names)).encode()).hexdigest()[:12]


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


def build_grid_view(state, cache, store=None) -> pn.Column:
    """Build the Results Grid tab component.

    Returns a Panel Column that re-renders when state.active_run_id changes.
    """
    # Mutable container so the callback can close over it
    _ctx: dict = {"tabulator": None, "hash_to_key": {}, "df_full": None}

    # --- selection counter indicator ---
    sel_count = pn.indicators.Number(
        name="Selected", value=0, format="{value}",
        default_color="gray", font_size="14pt",
    )

    # --- "View Detail" button (enabled when exactly 1 combo is selected) ---
    _view_detail_btn = pn.widgets.Button(
        name="View Detail", button_type="primary", disabled=True, width=110
    )

    # --- "Star" button (Phase 4) ---
    _star_btn = pn.widgets.Button(
        name="☆ Star", button_type="default", disabled=True, width=90,
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
        button_type="default",
        width=80,
        margin=(0, 4),
    )

    # --- Column chooser (Phase 5) ---
    # CheckBoxGroup: all columns always visible as checkboxes (checked = visible).
    # MultiChoice was replaced because deselected items disappeared from its dropdown.
    _col_chooser = pn.widgets.CheckBoxGroup(
        name="",
        options=[],
        value=[],
        inline=True,
        sizing_mode="stretch_width",
        margin=(2, 4),
    )
    _col_chooser_panel = pn.Column(
        _col_chooser,
        visible=False,
        sizing_mode="stretch_width",
    )
    _cols_toggle = pn.widgets.Toggle(
        name="⚙ Cols", value=False, button_type="default",
        width=70, margin=(0, 4),
    )
    _cols_toggle.param.watch(
        lambda e: setattr(_col_chooser_panel, "visible", e.new), "value"
    )
    # Track current watcher so it can be cleared on run change
    _col_watcher: dict = {"watch": None}

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
            _col_chooser.options = []
            _col_chooser.value = []
            return pn.pane.Markdown("_No combos to display._")

        # Param column names (not in fixed list, not hidden)
        param_cols = [c for c in df.columns
                      if c not in _FIXED_DISPLAY_COLS and c != "_key_hash"]
        ordered_cols = ["rank", "score"] + param_cols + [
            c for c in _FIXED_DISPLAY_COLS[2:] if c in df.columns
        ]
        # Ensure all ordered cols actually exist
        ordered_cols = [c for c in ordered_cols if c in df.columns]

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

        # Clear previous column chooser watcher
        if _col_watcher["watch"] is not None:
            try:
                _col_chooser.param.unwatch(_col_watcher["watch"])
            except Exception:
                pass
            _col_watcher["watch"] = None

        # Update chooser options + value (suppress watcher during update)
        _col_chooser.options = ordered_cols
        _col_chooser.value = visible_cols

        # Panel Tabulator does not accept a `columns` arg — subset the
        # DataFrame directly to control which columns appear and in what order.
        df_display = df[ordered_cols + ["_key_hash"]]
        _ctx["df_full"] = df_display  # kept for Python-side filter operations

        tab_hidden = ["_key_hash"] + hidden_user
        tab = pn.widgets.Tabulator(
            df_display,
            hidden_columns=tab_hidden,
            pagination="remote",
            page_size=200,
            selectable="checkbox",
            header_filters=True,
            sizing_mode="stretch_width",
            show_index=False,
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
            sel_count.value = len(keys)
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

        # Wire column chooser to this tabulator (Phase 5)
        def _on_col_change(event):
            selected = event.new
            new_hidden = ["_key_hash"] + [c for c in ordered_cols if c not in selected]
            tab.hidden_columns = new_hidden
            if store and strategy:
                store.save_column_preset(
                    strategy, ph,
                    [c for c in ordered_cols if c not in selected],
                )

        watcher = _col_chooser.param.watch(_on_col_change, "value")
        _col_watcher["watch"] = watcher

        return tab

    def _on_run_change(event):
        run_id = event.new
        if run_id is None:
            _content[:] = [_placeholder]
            sel_count.value = 0
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

    toolbar = pn.Row(
        pn.pane.Markdown("### Results Grid", margin=(5, 10)),
        pn.Spacer(),
        _view_detail_btn,
        _star_btn,
        _star_feedback,
        _csv_download,
        _cols_toggle,
        pn.Row(pn.pane.Markdown("Selected:", margin=(8, 4)), sel_count),
        sizing_mode="stretch_width",
    )

    def _on_view_detail(event):
        """Switch to the Combo Detail tab for the selected combo."""
        if state.selected_combo_keys:
            state.active_combo_key = state.selected_combo_keys[0]
        state.active_tab = "Combo Detail"

    _view_detail_btn.on_click(_on_view_detail)

    return pn.Column(toolbar, _filter_row, _col_chooser_panel, _content, sizing_mode="stretch_width")
