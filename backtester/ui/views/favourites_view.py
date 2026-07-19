"""
views/favourites_view.py — Favourites tab.

Shows a Tabulator of starred combos. Row actions (on selected row):
  Open    — load that run and focus the combo in Results Grid
  Re-run  — prefill New Run with {k: [v]} param_grid for the combo's strategy
  Unstar  — remove from favourites
  Edit Note — inline text editor for the note field
  Copy TOML — copy params as experiment-style TOML snippet

Also embeds the Compare section (former Compare tab) below the favourites table.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.store_service import key_from_json

log = get_ui_logger(__name__)

_DISPLAY_COLS = [
    "combo_id", "added_at", "score", "total_pnl", "ann_return", "sharpe",
    "strategy", "note",
]

_SORT_COL = "added_at_sort"

_COL_TITLES = {
    "combo_id":   "ID",
    "added_at":   "Added",
    "score":      "Score",
    "total_pnl":  "Total PnL",
    "ann_return": "Ann. Return",
    "sharpe":     "Sharpe",
    "strategy":   "Strategy",
    "note":       "Note",
}

_ID_COL_WIDTH = 106          # +10% vs original 96
_ADDED_COL_WIDTH = 138       # dd-mm-yyyy hh:mm
_SCORE_COL_WIDTH = 70        # +25% vs 56
_TOTAL_PNL_COL_WIDTH = 90    # +25% vs 72
_TOTAL_PNL_MAX_WIDTH = 110   # +25% vs 88
_ANN_RETURN_COL_WIDTH = 90
_ANN_RETURN_MAX_WIDTH = 105
_SHARPE_COL_WIDTH = 80       # +25% vs 64
_SHARPE_MAX_WIDTH = 95       # +25% vs 76
_PARAMS_TEXTAREA_ROWS = 25

_PARAMS_TEXTAREA_CSS = """
:host textarea.bk-input {
    overflow-y: auto !important;
    user-select: text !important;
    -webkit-user-select: text !important;
    cursor: text;
}
"""

_TABLE_LAYOUT_CSS = """
.tabulator {
    width: 100% !important;
    max-width: 100% !important;
}
.tabulator-tableholder {
    overflow-x: auto !important;
}
"""


def _format_added_at(iso: str) -> str:
    """Format stored UTC ISO timestamp as ``dd-mm-yyyy hh:mm``."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%d-%m-%Y %H:%M")
    except Exception:
        return iso[:16].replace("T", " ")


def _params_lines_from_fav(fav) -> str:
    """Format favourite params as one ``k=v`` line per parameter."""
    if fav is None:
        return ""
    try:
        key = key_from_json(fav.combo_key_json)
        return "\n".join(f"{k}={v}" for k, v in key)
    except Exception:
        raw = (fav.params_str or "").strip()
        if not raw:
            return ""
        return "\n".join(part.strip() for part in raw.split("  ") if part.strip())


def _favourites_column_config() -> dict:
    """Tabulator column sizing — metrics fixed, Strategy/Note grow to fill pane."""
    no_sort = {"headerSort": False}
    return {
        "initialSort": [{"column": _SORT_COL, "dir": "desc"}],
        "columns": [
            {"field": "ID", "width": _ID_COL_WIDTH, "widthGrow": 0, "widthShrink": 0, **no_sort},
            {"field": "Added", "width": _ADDED_COL_WIDTH, "widthGrow": 0, "widthShrink": 0, **no_sort},
            {"field": "Score", "width": _SCORE_COL_WIDTH, "widthGrow": 0, "maxWidth": _SCORE_COL_WIDTH, **no_sort},
            {"field": "Total PnL", "width": _TOTAL_PNL_COL_WIDTH, "widthGrow": 0,
             "maxWidth": _TOTAL_PNL_MAX_WIDTH, **no_sort},
            {"field": "Ann. Return", "width": _ANN_RETURN_COL_WIDTH, "widthGrow": 0,
             "maxWidth": _ANN_RETURN_MAX_WIDTH, **no_sort},
            {"field": "Sharpe", "width": _SHARPE_COL_WIDTH, "widthGrow": 0,
             "maxWidth": _SHARPE_MAX_WIDTH, **no_sort},
            {"field": "Strategy", "minWidth": 80, "widthGrow": 1, **no_sort},
            {"field": "Note", "minWidth": 80, "widthGrow": 2, **no_sort},
            {"field": _SORT_COL, "visible": False, "sorter": "string"},
        ],
    }


def _fav_by_combo_id(favs: list, combo_id: str):
    """Look up a favourite by combo_hash (stable across table re-sorts)."""
    for fav in favs:
        if fav.combo_hash == combo_id:
            return fav
    return None


def build_favourites_view(state, store, cache) -> pn.Column:
    """Return the Favourites tab component."""

    title = pn.pane.Markdown("## Favourites", margin=(8, 4, 4, 4))
    empty_msg = pn.pane.Markdown(
        "_No favourites yet. Star combos from the Results Grid or Combo Detail tab._",
        sizing_mode="stretch_width",
    )

    tab_holder = pn.Column(sizing_mode="stretch_width")
    selected_fav: dict = {"row": None, "fav": None}  # mutable ref

    params_input = pn.widgets.TextAreaInput(
        name="Parameters",
        value="",
        rows=_PARAMS_TEXTAREA_ROWS,
        sizing_mode="stretch_width",
        margin=(4, 4),
        stylesheets=[_PARAMS_TEXTAREA_CSS],
    )
    _params_snapshot: dict = {"text": ""}

    def _set_params_text(text: str) -> None:
        _params_snapshot["text"] = text
        params_input.value = text

    def _revert_params_edit(event) -> None:
        if event.new != _params_snapshot["text"]:
            params_input.value = _params_snapshot["text"]

    params_input.param.watch(_revert_params_edit, "value")

    # ── Action buttons ──────────────────────────────────────────────────────
    open_btn = pn.widgets.Button(
        name="Open", button_type="primary", disabled=True, width=90, margin=(4, 4),
    )
    rerun_btn = pn.widgets.Button(
        name="Re-run", button_type="default", disabled=True, width=90, margin=(4, 4),
    )
    unstar_btn = pn.widgets.Button(
        name="☆ Unstar", button_type="warning", disabled=True, width=90, margin=(4, 4),
    )
    copy_toml_btn = pn.widgets.Button(
        name="Copy TOML", button_type="default", disabled=True, width=100, margin=(4, 4),
    )
    note_input = pn.widgets.TextInput(
        name="", placeholder="Edit note…", visible=False,
        sizing_mode="stretch_width", margin=(4, 4),
    )
    save_note_btn = pn.widgets.Button(
        name="Save Note", button_type="default", disabled=True,
        visible=False, width=100, margin=(4, 4),
    )
    action_feedback = pn.pane.HTML("", sizing_mode="stretch_width",
                                   styles={"font-size": "12px"})

    def _set_action_buttons_enabled(enabled: bool):
        open_btn.disabled = not enabled
        rerun_btn.disabled = not enabled
        unstar_btn.disabled = not enabled
        copy_toml_btn.disabled = not enabled
        note_input.visible = enabled
        save_note_btn.visible = enabled

    _set_action_buttons_enabled(False)

    # ── Tabulator ───────────────────────────────────────────────────────────
    _fav_rows: dict = {"data": []}  # {id: FavRow}

    def _build_df() -> pd.DataFrame:
        favs = store.list_favourites()
        _fav_rows["data"] = favs
        if not favs:
            return pd.DataFrame(columns=_DISPLAY_COLS + [_SORT_COL, "_fav_id"])
        rows = []
        for fav in favs:
            raw_added = fav.added_at or ""
            rows.append({
                "combo_id":   fav.combo_hash or "",
                "added_at":   _format_added_at(raw_added),
                "score":      round(fav.score, 4) if fav.score is not None else None,
                "total_pnl":  round(fav.total_pnl, 2) if fav.total_pnl is not None else None,
                # Percent units for Tabulator "%" formatter (fraction stored in DB).
                "ann_return": (
                    round(fav.ann_return * 100, 1) if fav.ann_return is not None else None
                ),
                "sharpe":     round(fav.sharpe, 3) if fav.sharpe is not None else None,
                "strategy":   fav.strategy or "",
                "note":       fav.note or "",
                _SORT_COL:    raw_added,
                "_fav_id":    fav.id,
            })
        return pd.DataFrame(rows)

    def _refresh():
        df = _build_df()
        selected_fav["row"] = None
        selected_fav["fav"] = None
        _set_action_buttons_enabled(False)
        action_feedback.object = ""
        note_input.value = ""
        _set_params_text("")
        save_note_btn.disabled = True

        if df.empty or len(df) == 0:
            tab_holder[:] = [empty_msg]
            return

        tab_cols = _DISPLAY_COLS + [_SORT_COL]
        display_df = df[tab_cols].copy()
        display_df.columns = [_COL_TITLES.get(c, c) for c in tab_cols]

        tab = pn.widgets.Tabulator(
            display_df,
            selectable=1,
            show_index=False,
            sizing_mode="stretch_width",
            height=400,
            layout="fit_columns",
            hidden_columns=[_SORT_COL],
            configuration=_favourites_column_config(),
            stylesheets=[_TABLE_LAYOUT_CSS],
            formatters={
                "Score": {"type": "progress", "min": 0, "max": 1, "color": "#1a9641"},
                "Total PnL": {"type": "money", "symbol": "$", "precision": 0},
                "Ann. Return": {"type": "number", "precision": 1, "suffix": "%"},
                "Sharpe": {"type": "number", "precision": 3},
            },
        )
        tab.editable = False
        tab.editors = {col: None for col in display_df.columns}

        def _on_tab_selection(event):
            idxs = event.new
            if not idxs:
                selected_fav["row"] = None
                selected_fav["fav"] = None
                _set_action_buttons_enabled(False)
                note_input.value = ""
                _set_params_text("")
                save_note_btn.disabled = True
                return
            idx = idxs[0]
            combo_id = ""
            if idx < len(tab.value):
                combo_id = str(tab.value.iloc[idx].get("ID", ""))
            fav = _fav_by_combo_id(_fav_rows["data"], combo_id)
            selected_fav["row"] = idx
            selected_fav["fav"] = fav
            _set_action_buttons_enabled(True)
            note_input.value = fav.note if fav else ""
            _set_params_text(_params_lines_from_fav(fav))
            save_note_btn.disabled = False

        tab.param.watch(_on_tab_selection, "selection")
        tab_holder[:] = [tab]

    _refresh()

    # ── Refresh button ───────────────────────────────────────────────────────
    refresh_btn = pn.widgets.Button(
        name="↺ Refresh", button_type="light", width=90, margin=(4, 4),
    )
    refresh_btn.on_click(lambda e: _refresh())

    # Auto-refresh when this tab becomes active
    def _on_tab_change(event):
        if event.new == "Favourites":
            _refresh()

    state.param.watch(_on_tab_change, ["active_tab"])

    # ── Action handlers ─────────────────────────────────────────────────────
    def _on_open(event):
        fav = selected_fav.get("fav")
        if fav is None:
            return
        try:
            cache.get(fav.run_id)
            combo_key = key_from_json(fav.combo_key_json)
            state.active_run_id = fav.run_id
            state.active_combo_key = combo_key
            state.active_tab = "Combo Detail"
            action_feedback.object = "<span style='color:#16a34a'>Opened.</span>"
        except Exception as exc:
            action_feedback.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("favourites_view: open failed: %s", exc)

    open_btn.on_click(_on_open)

    def _on_rerun(event):
        fav = selected_fav.get("fav")
        if fav is None:
            return
        combo_key = key_from_json(fav.combo_key_json)
        param_grid = {k: [v] for k, v in combo_key}
        state.rerun_request = {"strategy": fav.strategy, "param_grid": param_grid}
        state.active_tab = "New Run"
        action_feedback.object = (
            "<span style='color:#2563eb'>New Run prefilled — review and Run.</span>"
        )

    rerun_btn.on_click(_on_rerun)

    def _on_unstar(event):
        fav = selected_fav.get("fav")
        if fav is None:
            return
        try:
            store.remove_favourite(fav.id)
            action_feedback.object = "<span style='color:#16a34a'>Removed.</span>"
            _refresh()
        except Exception as exc:
            action_feedback.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("favourites_view: unstar failed: %s", exc)

    unstar_btn.on_click(_on_unstar)

    def _on_copy_toml(event):
        fav = selected_fav.get("fav")
        if fav is None:
            return
        from backtester.ui.services.toml_export import copy_to_clipboard, favourite_to_toml
        toml_str = favourite_to_toml(fav)
        ok = copy_to_clipboard(toml_str)
        if ok:
            action_feedback.object = "<span style='color:#16a34a'>TOML copied to clipboard.</span>"
        else:
            # Show inline instead
            action_feedback.object = (
                f"<pre style='font-size:11px;background:#f3f4f6;padding:6px'>"
                f"{toml_str}</pre>"
            )

    copy_toml_btn.on_click(_on_copy_toml)

    def _on_save_note(event):
        fav = selected_fav.get("fav")
        if fav is None:
            return
        try:
            store.update_favourite(fav.id, note=note_input.value.strip())
            action_feedback.object = "<span style='color:#16a34a'>Note saved.</span>"
            _refresh()
        except Exception as exc:
            action_feedback.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("favourites_view: save note failed: %s", exc)

    save_note_btn.on_click(_on_save_note)
    note_input.param.watch(lambda e: None, "value")  # ensure reactive

    # ── Layout ───────────────────────────────────────────────────────────────
    action_row = pn.Row(
        open_btn, rerun_btn, unstar_btn, copy_toml_btn, refresh_btn,
        sizing_mode="stretch_width",
    )
    note_row = pn.Row(note_input, save_note_btn, sizing_mode="stretch_width")

    from backtester.ui.views.compare_view import build_compare_view
    compare_section = build_compare_view(state, store, cache)

    return pn.Column(
        title,
        action_row,
        note_row,
        action_feedback,
        tab_holder,
        params_input,
        pn.pane.HTML(
            "<hr style='margin:24px 0 8px 0;border:none;border-top:1px solid #e5e7eb'>"
        ),
        compare_section,
        sizing_mode="stretch_width",
    )
