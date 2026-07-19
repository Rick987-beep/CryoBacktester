"""
views/runs_view.py — Full-page run management (list, pin, delete, prune).
"""
from __future__ import annotations

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_COLS = ["id", "created_at", "strategy", "label", "n_combos", "n_trades",
         "pinned", "git_dirty", "runtime_s"]


def build_runs_view(state, store, cache) -> pn.Column:
    """Build the Runs management page."""
    try:
        store.scan_bundles()
    except Exception as exc:
        log.warning("scan_bundles failed: %s", exc)

    status = pn.pane.HTML("", sizing_mode="stretch_width", margin=(4, 4))
    table_holder = pn.Column(sizing_mode="stretch_width")
    _tab_ref: dict = {"tab": None}

    refresh_btn = pn.widgets.Button(
        name="↺ Refresh", button_type="light", width=100, margin=(4, 4),
    )
    open_btn = pn.widgets.Button(
        name="Open → Grid", button_type="primary", width=120, margin=(4, 4),
        disabled=True,
    )
    pin_btn = pn.widgets.Button(
        name="📌 Pin", button_type="default", width=90, margin=(4, 4),
        disabled=True,
    )
    unpin_btn = pn.widgets.Button(
        name="Unpin", button_type="default", width=90, margin=(4, 4),
        disabled=True,
    )
    delete_btn = pn.widgets.Button(
        name="🗑 Delete selected", button_type="danger", width=140, margin=(4, 4),
        disabled=True,
    )

    # ── Prune panel ──────────────────────────────────────────────────────────
    prune_days = pn.widgets.IntInput(
        name="Delete unpinned runs older than (days)",
        value=30, start=1, width=280, margin=(4, 4),
    )
    prune_preview_btn = pn.widgets.Button(
        name="Preview prune", button_type="warning", width=120, margin=(4, 4),
    )
    prune_confirm_btn = pn.widgets.Button(
        name="✓ Confirm prune", button_type="danger", width=130, margin=(4, 4),
        disabled=True,
    )
    prune_output = pn.pane.HTML("", sizing_mode="stretch_width",
                                styles={"font-size": "12px"})

    def _runs_df() -> pd.DataFrame:
        rows = store.list_runs()
        if not rows:
            return pd.DataFrame(columns=_COLS)
        records = []
        for rr in rows:
            records.append({
                "id": rr.id,
                "created_at": (rr.created_at or "")[:19].replace("T", " "),
                "strategy": rr.strategy,
                "label": rr.label or "",
                "n_combos": rr.n_combos,
                "n_trades": rr.n_trades,
                "pinned": bool(rr.pinned),
                "git_dirty": bool(rr.git_dirty) if rr.git_dirty is not None else False,
                "runtime_s": round(rr.runtime_s, 1) if rr.runtime_s is not None else None,
            })
        return pd.DataFrame(records)

    def _selected_ids() -> list[int]:
        tab = _tab_ref.get("tab")
        if tab is None or not tab.selection:
            return []
        df = tab.value
        ids = []
        for idx in tab.selection:
            try:
                ids.append(int(df.iloc[idx]["id"]))
            except Exception:
                continue
        return ids

    def _update_action_buttons(*_):
        ids = _selected_ids()
        n = len(ids)
        open_btn.disabled = n != 1
        pin_btn.disabled = n == 0
        unpin_btn.disabled = n == 0
        delete_btn.disabled = n == 0

    def _refresh(*_):
        try:
            store.scan_bundles()
        except Exception as exc:
            log.warning("scan_bundles failed: %s", exc)
        df = _runs_df()
        if df.empty:
            table_holder[:] = [pn.pane.Markdown("_No runs found._")]
            _tab_ref["tab"] = None
            _update_action_buttons()
            return

        tab = pn.widgets.Tabulator(
            df,
            pagination="remote",
            page_size=50,
            selectable="checkbox",
            sortable=True,
            sizing_mode="stretch_width",
            height=520,
            show_index=False,
            configuration={"columnDefaults": {"headerSort": True}},
        )
        tab.editable = False
        tab.editors = {c: None for c in df.columns}
        tab.param.watch(lambda e: _update_action_buttons(), "selection")
        _tab_ref["tab"] = tab
        table_holder[:] = [tab]
        status.object = (
            f"<span style='color:#6b7280'>{len(df)} run(s) — "
            f"sort by clicking column headers; select rows for pin/delete.</span>"
        )
        _update_action_buttons()

    def _on_open(event):
        ids = _selected_ids()
        if len(ids) != 1:
            return
        rid = ids[0]
        try:
            cache.get(rid)
            state.active_run_id = rid
            state.active_tab = "Results Grid"
            status.object = f"<span style='color:#16a34a'>Opened run #{rid}.</span>"
        except Exception as exc:
            status.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("runs_view: open failed: %s", exc)

    def _on_pin(event):
        for rid in _selected_ids():
            try:
                cache.pin(rid)
            except Exception as exc:
                log.warning("runs_view: pin %s failed: %s", rid, exc)
        _refresh()
        status.object = "<span style='color:#16a34a'>Pinned.</span>"

    def _on_unpin(event):
        for rid in _selected_ids():
            try:
                cache.unpin(rid)
            except Exception as exc:
                log.warning("runs_view: unpin %s failed: %s", rid, exc)
        _refresh()
        status.object = "<span style='color:#16a34a'>Unpinned.</span>"

    def _on_delete(event):
        ids = _selected_ids()
        if not ids:
            return
        deleted = store.delete_runs(ids, allow_pinned=False)
        skipped = len(ids) - len(deleted)
        for rr in deleted:
            try:
                cache.evict(rr.id)
            except Exception:
                pass
        if state.active_run_id in {rr.id for rr in deleted}:
            state.active_run_id = None
            state.active_combo_key = None
            state.selected_combo_keys = []
        _refresh()
        msg = f"Deleted {len(deleted)} run(s)."
        if skipped:
            msg += f" Skipped {skipped} pinned — unpin first."
        status.object = f"<span style='color:#16a34a'>{msg}</span>"

    def _on_prune_preview(event):
        days = prune_days.value
        to_prune = store.prune_runs(days, dry_run=True)
        if not to_prune:
            prune_output.object = "<span style='color:#16a34a'>Nothing to prune.</span>"
            prune_confirm_btn.disabled = True
            return
        lines = [f"<b>Would delete {len(to_prune)} unpinned run(s):</b>"]
        for rr in to_prune[:12]:
            ts = (rr.created_at or "")[:16].replace("T", " ")
            lines.append(f"&nbsp;&nbsp;#{rr.id}&nbsp;{rr.strategy}&nbsp;{ts}")
        if len(to_prune) > 12:
            lines.append(f"&nbsp;&nbsp;…and {len(to_prune) - 12} more")
        prune_output.object = "<br>".join(lines)
        prune_confirm_btn.disabled = False

    def _on_prune_confirm(event):
        days = prune_days.value
        pruned = store.prune_runs(days, dry_run=False)
        for rr in pruned:
            try:
                cache.evict(rr.id)
            except Exception:
                pass
        prune_output.object = (
            f"<span style='color:#16a34a'>Deleted {len(pruned)} run(s).</span>"
        )
        prune_confirm_btn.disabled = True
        _refresh()

    refresh_btn.on_click(lambda e: _refresh())
    open_btn.on_click(_on_open)
    pin_btn.on_click(_on_pin)
    unpin_btn.on_click(_on_unpin)
    delete_btn.on_click(_on_delete)
    prune_preview_btn.on_click(_on_prune_preview)
    prune_confirm_btn.on_click(_on_prune_confirm)

    # Refresh when navigating to Runs
    def _on_tab(event):
        if event.new == "Runs":
            _refresh()

    state.param.watch(_on_tab, ["active_tab"])
    _refresh()

    actions = pn.Row(
        refresh_btn, open_btn, pin_btn, unpin_btn, delete_btn,
        sizing_mode="stretch_width",
    )
    prune_row = pn.Row(
        prune_days, prune_preview_btn, prune_confirm_btn,
        sizing_mode="stretch_width",
    )

    return pn.Column(
        pn.pane.Markdown("## Runs", margin=(8, 4, 4, 4)),
        actions,
        status,
        table_holder,
        pn.pane.Markdown("### Prune", margin=(16, 4, 4, 4)),
        prune_row,
        prune_output,
        sizing_mode="stretch_width",
    )
