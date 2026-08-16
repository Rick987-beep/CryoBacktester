"""
views/runs_view.py — Full-page run management (list, favourite, delete, prune).
"""
from __future__ import annotations

import json

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_COLS = ["id", "favourite", "created_at", "strategy", "family", "label", "n_combos",
         "n_trades", "runtime_s"]


def build_runs_view(state, store, cache) -> pn.Column:
    """Build the Runs management page."""
    from workspace.catalog import FAMILIES, family_for, family_label

    try:
        store.scan_bundles()
    except Exception as exc:
        log.warning("scan_bundles failed: %s", exc)

    status = pn.pane.HTML("", sizing_mode="stretch_width", margin=(4, 4))
    table_holder = pn.Column(sizing_mode="stretch_width")
    _tab_ref: dict = {"tab": None}

    _FAMILY_ALL = "all"
    _family_opts = {"All": _FAMILY_ALL}
    _family_opts.update({fam.label: fid for fid, fam in FAMILIES.items()})
    family_filter = pn.widgets.Select(
        name="Family",
        options=_family_opts,
        value=_FAMILY_ALL,
        width=160,
        margin=(4, 4),
    )

    refresh_btn = pn.widgets.Button(
        name="↺ Refresh", button_type="light", width=100, margin=(4, 4),
    )
    open_btn = pn.widgets.Button(
        name="Open → Grid", button_type="primary", width=120, margin=(4, 4),
        disabled=True,
    )
    rerun_btn = pn.widgets.Button(
        name="Re-run", button_type="default", width=90, margin=(4, 4),
        disabled=True,
    )
    delete_btn = pn.widgets.Button(
        name="Delete Selected", button_type="danger", width=140, margin=(4, 4),
        disabled=True,
    )

    # ── Prune panel ──────────────────────────────────────────────────────────
    prune_days = pn.widgets.IntInput(
        name="Delete non-favourited runs older than (days)",
        value=30, start=1, width=320, margin=(4, 4),
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
            fam_id = rr.family or family_for(rr.strategy or "")
            records.append({
                "id": rr.id,
                "favourite": "★" if rr.pinned else "☆",
                "created_at": (rr.created_at or "")[:19].replace("T", " "),
                "strategy": rr.strategy,
                "family": family_label(fam_id),
                "family_id": fam_id,
                "label": rr.label or "",
                "n_combos": rr.n_combos,
                "n_trades": rr.n_trades,
                "runtime_s": round(rr.runtime_s, 1) if rr.runtime_s is not None else None,
            })
        df = pd.DataFrame(records)
        selected = family_filter.value
        if selected and selected != _FAMILY_ALL:
            df = df[df["family_id"] == selected].reset_index(drop=True)
        return df.drop(columns=["family_id"], errors="ignore")

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
        rerun_btn.disabled = n != 1
        delete_btn.disabled = n == 0

    def _on_label_edit(event):
        if event.column != "label":
            return
        tab = _tab_ref.get("tab")
        if tab is None:
            return
        try:
            run_id = int(tab.value.iloc[event.row]["id"])
        except Exception:
            return
        label = (event.value or "").strip() or None
        try:
            store.set_label(run_id, label)
            status.object = (
                f"<span style='color:#16a34a'>Label saved for run #{run_id}.</span>"
            )
        except Exception as exc:
            status.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("runs_view: set_label failed: %s", exc)

    def _on_star_click(event):
        tab = _tab_ref.get("tab")
        if tab is None:
            return
        try:
            run_id = int(tab.value.iloc[event.row]["id"])
        except Exception:
            return
        rr = store.get_run(run_id)
        if rr is None:
            return
        try:
            if rr.pinned:
                cache.unpin(run_id)
                status.object = (
                    f"<span style='color:#16a34a'>Unfavourited run #{run_id}.</span>"
                )
            else:
                cache.pin(run_id)
                status.object = (
                    f"<span style='color:#16a34a'>Favourited run #{run_id}.</span>"
                )
        except Exception as exc:
            status.object = f"<span style='color:#dc2626'>⚠ {exc}</span>"
            log.error("runs_view: star toggle failed: %s", exc)
            return
        _refresh()

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
            titles={"favourite": "★"},
            widths={"favourite": 48},
            configuration={"columnDefaults": {"headerSort": True}},
        )
        tab.editable = True
        tab.editors = {c: ("input" if c == "label" else None) for c in df.columns}
        tab.on_edit(_on_label_edit)
        tab.on_click(_on_star_click, column="favourite")
        tab.param.watch(lambda e: _update_action_buttons(), "selection")
        _tab_ref["tab"] = tab
        table_holder[:] = [tab]
        status.object = (
            f"<span style='color:#6b7280'>{len(df)} run(s) — "
            f"click ★/☆ to favourite; click a label cell to edit.</span>"
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

    def _on_rerun(event):
        ids = _selected_ids()
        if len(ids) != 1:
            return
        rid = ids[0]
        rr = store.get_run(rid)
        if rr is None:
            status.object = f"<span style='color:#dc2626'>⚠ Run #{rid} not found.</span>"
            return
        try:
            param_grid = json.loads(rr.param_grid_json) if rr.param_grid_json else {}
        except json.JSONDecodeError as exc:
            status.object = f"<span style='color:#dc2626'>⚠ Invalid param grid: {exc}</span>"
            return
        state.rerun_request = {
            "strategy": rr.strategy,
            "param_grid": param_grid,
            "date_from": rr.date_from,
            "date_to": rr.date_to,
        }
        state.active_tab = "New Run"
        status.object = (
            f"<span style='color:#2563eb'>New Run prefilled from run #{rid} — review and Run.</span>"
        )

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
            msg += f" Skipped {skipped} favourited — click ★ to unfavourite first."
        status.object = f"<span style='color:#16a34a'>{msg}</span>"

    def _on_prune_preview(event):
        days = prune_days.value
        to_prune = store.prune_runs(days, dry_run=True)
        if not to_prune:
            prune_output.object = "<span style='color:#16a34a'>Nothing to prune.</span>"
            prune_confirm_btn.disabled = True
            return
        lines = [f"<b>Would delete {len(to_prune)} non-favourited run(s):</b>"]
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
    family_filter.param.watch(lambda e: _refresh(), "value")
    open_btn.on_click(_on_open)
    rerun_btn.on_click(_on_rerun)
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
        refresh_btn, family_filter, open_btn, rerun_btn, delete_btn,
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
