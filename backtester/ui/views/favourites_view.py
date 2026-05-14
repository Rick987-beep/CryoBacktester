"""
views/favourites_view.py — Favourites tab.

Shows a Tabulator of starred combos. Row actions (on selected row):
  Open    — load that run and focus the combo in Results Grid
  Re-run  — prefill sidebar with {k: [v]} param_grid for the combo's strategy
  Unstar  — remove from favourites
  Edit Note — inline text editor for the note field
  Copy TOML — copy params as experiment-style TOML snippet
"""
from __future__ import annotations

import logging

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.store_service import key_from_json

log = get_ui_logger(__name__)

_DISPLAY_COLS = [
    "name", "strategy", "params_str", "score", "total_pnl", "sharpe",
    "note", "added_at",
]

_COL_TITLES = {
    "name":       "Name",
    "strategy":   "Strategy",
    "params_str": "Params",
    "score":      "Score",
    "total_pnl":  "Total PnL",
    "sharpe":     "Sharpe",
    "note":       "Note",
    "added_at":   "Added",
}


def build_favourites_view(state, store, cache) -> pn.Column:
    """Return the Favourites tab component."""

    title = pn.pane.Markdown("## Favourites", margin=(8, 4, 4, 4))
    empty_msg = pn.pane.Markdown(
        "_No favourites yet. Star combos from the Results Grid or Combo Detail tab._",
        sizing_mode="stretch_width",
    )

    tab_holder = pn.Column(sizing_mode="stretch_width")
    selected_fav: dict = {"row": None, "fav": None}  # mutable ref

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
            return pd.DataFrame(columns=_DISPLAY_COLS + ["_fav_id"])
        rows = []
        for fav in favs:
            rows.append({
                "name":       fav.name or "",
                "strategy":   fav.strategy or "",
                "params_str": fav.params_str or "",
                "score":      round(fav.score, 4) if fav.score is not None else None,
                "total_pnl":  round(fav.total_pnl, 2) if fav.total_pnl is not None else None,
                "sharpe":     round(fav.sharpe, 3) if fav.sharpe is not None else None,
                "note":       fav.note or "",
                "added_at":   (fav.added_at or "")[:16].replace("T", " "),
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
        save_note_btn.disabled = True

        if df.empty or len(df) == 0:
            tab_holder[:] = [empty_msg]
            return

        display_df = df[_DISPLAY_COLS].copy()
        display_df.columns = [_COL_TITLES.get(c, c) for c in _DISPLAY_COLS]

        tab = pn.widgets.Tabulator(
            display_df,
            selectable=1,
            show_index=False,
            sizing_mode="stretch_width",
            height=400,
            formatters={
                "Score": {"type": "progress", "min": 0, "max": 1, "color": "#1a9641"},
                "Total PnL": {"type": "money", "symbol": "$", "precision": 0},
                "Sharpe": {"type": "number", "precision": 3},
            },
        )
        tab.editable = False
        tab.editors = {col: None for col in display_df.columns}
        # Store mapping: display row index → FavRow
        _row_idx_map = {i: fav for i, fav in enumerate(_fav_rows["data"])}

        def _on_tab_selection(event):
            idxs = event.new
            if not idxs:
                selected_fav["row"] = None
                selected_fav["fav"] = None
                _set_action_buttons_enabled(False)
                note_input.value = ""
                save_note_btn.disabled = True
                return
            idx = idxs[0]
            fav = _row_idx_map.get(idx)
            selected_fav["row"] = idx
            selected_fav["fav"] = fav
            _set_action_buttons_enabled(True)
            note_input.value = fav.note if fav else ""
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
        state.active_tab = "Results Grid"
        action_feedback.object = "<span style='color:#2563eb'>Sidebar prefilled.</span>"

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

    return pn.Column(
        title,
        action_row,
        note_row,
        action_feedback,
        tab_holder,
        sizing_mode="stretch_width",
    )
