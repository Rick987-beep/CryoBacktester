"""
views/new_run_view.py — Full-page New Run form (strategy, params, progress).
"""
from __future__ import annotations

import importlib
import sys

import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.param_parse import csv_from_values, parse_param_csv

log = get_ui_logger(__name__)

_PARAM_TABLE_CSS = """
.param-help {
  font-size: 12px;
  color: #6b7280;
  line-height: 1.35;
  padding: 4px 8px 8px 4px;
}
.param-name {
  font-weight: 600;
  font-size: 13px;
  padding: 8px 4px 0 4px;
  color: #1a2332;
}
"""


def build_new_run_view(state, store, cache, run_service) -> pn.Column:
    """Build the New Run page."""
    from backtester.run import STRATEGIES

    strategy_select = pn.widgets.Select(
        name="Strategy",
        options=sorted(STRATEGIES.keys()),
        value=("short_generic" if "short_generic" in STRATEGIES
               else sorted(STRATEGIES.keys())[0]),
        width=320,
    )
    reload_btn = pn.widgets.Button(
        name="↻ Reload strategy",
        button_type="light",
        width=140,
        margin=(22, 0, 0, 8),
    )

    _param_inputs: dict = {}
    _param_errors: dict = {}
    param_editor_col = pn.Column(sizing_mode="stretch_width", stylesheets=[_PARAM_TABLE_CSS])

    _date_fmt = "%Y-%m-%d"
    date_from_input = pn.widgets.TextInput(
        name="Date from (YYYY-MM-DD)", value="", width=200, margin=(2, 8),
    )
    date_to_input = pn.widgets.TextInput(
        name="Date to (YYYY-MM-DD)", value="", width=200, margin=(2, 8),
    )
    date_error = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"color": "#dc2626", "font-size": "12px"},
    )

    run_btn = pn.widgets.Button(
        name="▶ Run", button_type="success", disabled=True, width=120, margin=(6, 4),
    )
    cancel_btn = pn.widgets.Button(
        name="■ Cancel", button_type="danger", disabled=True, width=120, margin=(6, 4),
    )

    progress_bar = pn.widgets.Progress(
        name="Progress", value=0, max=100,
        bar_color="primary", sizing_mode="stretch_width",
        height=28, visible=False, margin=(8, 4),
    )
    progress_label = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "13px", "color": "#6b7280", "min-height": "24px"},
    )
    status_label = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "14px", "min-height": "28px"},
    )

    def _validate_dates() -> bool:
        from datetime import datetime
        f_str = date_from_input.value.strip()
        t_str = date_to_input.value.strip()
        if not f_str and not t_str:
            date_error.object = ""
            return True
        try:
            if f_str:
                datetime.strptime(f_str, _date_fmt)
            if t_str:
                datetime.strptime(t_str, _date_fmt)
            date_error.object = ""
            return True
        except ValueError as exc:
            date_error.object = f"Date: {exc}"
            return False

    def _validate_all() -> bool:
        all_ok = True
        cls = STRATEGIES.get(strategy_select.value)
        grid = getattr(cls, "PARAM_GRID", {}) if cls else {}
        for pname, ti in _param_inputs.items():
            sample = grid.get(pname, [None])[0]
            _, err_msg = parse_param_csv(pname, ti.value, sample)
            if err_msg:
                _param_errors[pname].object = (
                    f"<span style='color:#dc2626;font-size:11px'>{err_msg}</span>"
                )
                all_ok = False
            else:
                _param_errors[pname].object = ""
        if not _validate_dates():
            all_ok = False
        run_btn.disabled = not all_ok or state.active_run_handle is not None
        return all_ok

    def _load_strategy_params(key: str):
        cls = STRATEGIES.get(key)
        if cls is None:
            return
        grid = getattr(cls, "PARAM_GRID", {})
        help_map = getattr(cls, "PARAM_HELP", {}) or {}
        _param_inputs.clear()
        _param_errors.clear()
        rows = [
            pn.pane.HTML(
                "<div style='display:grid;grid-template-columns:180px 1fr 1.2fr;"
                "gap:8px;font-size:12px;color:#6b7280;padding:4px 4px 8px 4px;"
                "border-bottom:1px solid #e5e7eb'>"
                "<div><b>Parameter</b></div>"
                "<div><b>Values</b> (CSV or <code>start..end:step</code>)</div>"
                "<div><b>Help</b></div></div>"
            )
        ]
        for pname in sorted(grid.keys()):
            vals = grid[pname]
            ti = pn.widgets.TextInput(
                name="",
                value=csv_from_values(vals),
                sizing_mode="stretch_width",
                margin=(2, 4),
            )
            err = pn.pane.HTML("", sizing_mode="stretch_width")
            help_txt = help_map.get(pname, "")
            help_pane = pn.pane.HTML(
                f"<div class='param-help'>{help_txt or '—'}</div>",
                sizing_mode="stretch_width",
            )
            name_pane = pn.pane.HTML(
                f"<div class='param-name'>{pname}</div>",
                width=180,
            )
            _param_inputs[pname] = ti
            _param_errors[pname] = err
            ti.param.watch(lambda e: _validate_all(), "value")
            rows.append(
                pn.Row(
                    name_pane,
                    pn.Column(ti, err, sizing_mode="stretch_width"),
                    help_pane,
                    sizing_mode="stretch_width",
                )
            )
        param_editor_col[:] = rows
        _validate_all()

    def _load_date_range(key: str):
        cls = STRATEGIES.get(key)
        if cls is None:
            return
        dr = getattr(cls, "DATE_RANGE", (None, None))
        date_from_input.value = dr[0] or ""
        date_to_input.value = dr[1] or ""

    def _on_strategy_change(event):
        _load_strategy_params(event.new)
        _load_date_range(event.new)

    strategy_select.param.watch(_on_strategy_change, "value")

    def _on_reload(event):
        key = strategy_select.value
        cls = STRATEGIES.get(key)
        if cls is None:
            return
        module_name = cls.__module__
        if module_name in sys.modules:
            module = importlib.reload(sys.modules[module_name])
            new_cls = getattr(module, cls.__name__, None)
            if new_cls is not None:
                STRATEGIES[key] = new_cls
        _load_strategy_params(key)
        _load_date_range(key)
        status_label.object = (
            "<span style='color:#16a34a'>Strategy module reloaded.</span>"
        )

    reload_btn.on_click(_on_reload)
    date_from_input.param.watch(lambda e: _validate_all(), "value")
    date_to_input.param.watch(lambda e: _validate_all(), "value")

    _load_strategy_params(strategy_select.value)
    _load_date_range(strategy_select.value)

    _cb_handle: dict = {"cb": None, "handle": None}

    def _stop_cb():
        cb = _cb_handle.get("cb")
        if cb:
            try:
                cb.stop()
            except Exception:
                pass
        _cb_handle["cb"] = None
        _cb_handle["handle"] = None

    def _on_run_done(line):
        _stop_cb()
        bundle_path = line.get("bundle_path")
        try:
            run_id = store.register_bundle(bundle_path)
            cache.get(run_id)
            state.active_run_id = run_id
            status_label.object = (
                f"<span style='color:#16a34a'>✓ Done — run #{run_id} loaded. "
                f"Switch to Results Grid to inspect.</span>"
            )
            progress_bar.value = 100
            state.active_tab = "Results Grid"
        except Exception as exc:
            log.error("new_run_view: failed to register completed run: %s", exc)
            status_label.object = (
                f"<span style='color:#dc2626'>⚠ Run done but load failed: {exc}</span>"
            )
        state.active_run_handle = None
        cancel_btn.disabled = True
        run_btn.disabled = False

    def _on_run_ended(line):
        _stop_cb()
        status_code = line.get("status", "error")
        msg = line.get("message", "")
        if status_code == "cancelled":
            status_label.object = "<span style='color:#d97706'>Cancelled.</span>"
        else:
            status_label.object = (
                f"<span style='color:#dc2626'>⚠ Error: {msg}</span>"
            )
        progress_bar.visible = False
        state.active_run_handle = None
        cancel_btn.disabled = True
        run_btn.disabled = False

    def _on_run(event):
        if not _validate_all():
            return
        cls = STRATEGIES.get(strategy_select.value)
        grid = getattr(cls, "PARAM_GRID", {}) if cls else {}

        param_grid = {}
        for pname, ti in _param_inputs.items():
            sample = grid.get(pname, [None])[0]
            vals, _ = parse_param_csv(pname, ti.value, sample)
            if vals is not None:
                param_grid[pname] = vals

        f_str = date_from_input.value.strip() or None
        t_str = date_to_input.value.strip() or None

        import backtester.core.config as _bcfg
        account_size = float(_bcfg.cfg.simulation.account_size_usd)

        try:
            handle = run_service.submit(
                strategy_key=strategy_select.value,
                param_grid=param_grid,
                date_range=(f_str, t_str),
                account_size=account_size,
            )
        except Exception as exc:
            status_label.object = (
                f"<span style='color:#dc2626'>⚠ Failed to start: {exc}</span>"
            )
            log.error("new_run_view: submit failed: %s", exc)
            return

        state.active_run_handle = handle
        run_btn.disabled = True
        cancel_btn.disabled = False
        progress_bar.value = 0
        progress_bar.visible = True
        progress_label.object = ""
        status_label.object = "<span style='color:#2563eb'>Running…</span>"
        _cb_handle["handle"] = handle

        def _poll():
            h = _cb_handle.get("handle")
            if h is None:
                return
            for line in run_service.tail_progress(h):
                if "phase" in line:
                    phase = line["phase"]
                    msg = line.get("msg", "")
                    progress_label.object = msg
                    if phase == "loading_data":
                        progress_bar.value = 3
                    elif phase == "building_indicators":
                        progress_bar.value = 8
                    elif phase == "backtesting":
                        progress_bar.value = 12
                elif "current" in line and "total" in line:
                    total = line["total"]
                    current = line["current"]
                    if total > 0:
                        progress_bar.value = 12 + int(88 * current / total)
                    if line.get("date"):
                        progress_label.object = f"Processing {line['date']}"
                elif line.get("status") == "done":
                    _on_run_done(line)
                    return
                elif line.get("status") in ("error", "cancelled"):
                    _on_run_ended(line)
                    return
            if not h.is_alive():
                remaining = list(run_service.tail_progress(h))
                final = next((l for l in reversed(remaining) if "status" in l), None)
                if final:
                    if final.get("status") == "done":
                        _on_run_done(final)
                    else:
                        _on_run_ended(final)
                else:
                    _on_run_ended({
                        "status": "error",
                        "message": "worker exited unexpectedly",
                    })

        cb = pn.state.add_periodic_callback(_poll, period=500)
        _cb_handle["cb"] = cb

    run_btn.on_click(_on_run)

    def _on_cancel(event):
        h = _cb_handle.get("handle") or state.active_run_handle
        if h:
            run_service.cancel(h)
        cancel_btn.disabled = True

    cancel_btn.on_click(_on_cancel)

    def _on_rerun_request(event):
        req = event.new
        if req is None:
            return
        strat = req.get("strategy")
        pg = req.get("param_grid", {})
        if strat and strat in strategy_select.options:
            strategy_select.value = strat
            for pname, ti in _param_inputs.items():
                if pname in pg:
                    ti.value = csv_from_values(pg[pname])
        state.rerun_request = None
        state.active_tab = "New Run"
        status_label.object = (
            "<span style='color:#2563eb'>Prefilled from favourite — review and Run.</span>"
        )

    state.param.watch(_on_rerun_request, ["rerun_request"])

    return pn.Column(
        pn.pane.Markdown("## New Run", margin=(8, 4, 4, 4)),
        pn.Row(strategy_select, reload_btn, sizing_mode="stretch_width"),
        pn.pane.Markdown("### Parameters", margin=(12, 4, 4, 4)),
        param_editor_col,
        pn.pane.Markdown("### Date range", margin=(12, 4, 4, 4)),
        pn.Row(date_from_input, date_to_input, sizing_mode="stretch_width"),
        date_error,
        pn.Row(run_btn, cancel_btn, sizing_mode="stretch_width"),
        pn.pane.Markdown("### Progress", margin=(16, 4, 4, 4)),
        progress_bar,
        progress_label,
        status_label,
        sizing_mode="stretch_width",
    )
