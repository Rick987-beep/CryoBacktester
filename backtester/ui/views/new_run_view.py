"""
views/new_run_view.py — Full-page New Run form (strategy, params, enqueue).
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
  color: #8a9ab0;
  line-height: 1.35;
  padding: 4px 8px 8px 4px;
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
}
.param-name {
  font-weight: 600;
  font-size: 12px;
  padding: 8px 4px 0 4px;
  color: #1a1a2e;
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
}
"""


def build_new_run_view(state, store, cache, run_service) -> pn.Column:
    """Build the New Run page (form only — progress lives on Backtester Run)."""
    from backtester.run import STRATEGIES
    from backtester.catalog import (
        FAMILIES,
        family_for,
        strategy_options,
    )

    _FAMILY_ALL = "all"
    _family_opts = {"All": _FAMILY_ALL}
    _family_opts.update({fam.label: fid for fid, fam in FAMILIES.items()})
    family_select = pn.widgets.Select(
        name="Family",
        options=_family_opts,
        value=_FAMILY_ALL,
        width=180,
    )

    def _strategy_select_options(family_id: str) -> dict:
        return strategy_options(None if family_id == _FAMILY_ALL else family_id)

    _init_opts = _strategy_select_options(_FAMILY_ALL)
    _default_id = (
        "blueprint_howto" if "blueprint_howto" in _init_opts.values()
        else next(iter(_init_opts.values()))
    )
    strategy_select = pn.widgets.Select(
        name="Strategy",
        options=_init_opts,
        value=_default_id,
        width=320,
    )
    reload_btn = pn.widgets.Button(
        name="↻ Reload strategy",
        button_type="light",
        width=140,
        margin=(4, 4),
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
        # jobd accepts a queue — Run stays enabled while another job is active
        run_btn.disabled = not all_ok
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

    def _on_family_change(event):
        opts = _strategy_select_options(event.new)
        strategy_select.options = opts
        if strategy_select.value not in opts.values():
            strategy_select.value = next(iter(opts.values()))

    family_select.param.watch(_on_family_change, "value")

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

        status_label.object = (
            f"<span style='color:#16a34a'>Enqueued "
            f"<code>{getattr(handle, 'job_id', '')}</code> — "
            f"see queue on Backtester Run.</span>"
        )
        # Keep watching the in-flight job; only adopt the new handle if idle
        current = state.active_run_handle
        current_alive = False
        if current is not None:
            try:
                current_alive = bool(current.is_alive())
            except Exception:
                current_alive = False
        if not current_alive:
            state.active_run_handle = handle
        state.active_tab = "Backtester Run"
        _validate_all()

    run_btn.on_click(_on_run)

    def _on_handle_change(event):
        _validate_all()

    state.param.watch(_on_handle_change, "active_run_handle")

    def _on_rerun_request(event):
        req = event.new
        if req is None:
            return
        strat = req.get("strategy")
        pg = req.get("param_grid", {})
        if strat:
            from backtester.catalog import SPECS

            fam = family_for(strat)
            family_select.value = fam if fam in family_select.options.values() else _FAMILY_ALL
            opts = _strategy_select_options(family_select.value)
            # Archived IDs are hidden from the normal picker but must still
            # resolve for Completed Runs → Rerun / historical favourites.
            if strat not in opts.values() and strat in STRATEGIES:
                spec = SPECS.get(strat)
                label = spec.label() if spec is not None else strat
                opts = {**opts, label: strat}
            strategy_select.options = opts
            if strat in strategy_select.options.values():
                strategy_select.value = strat
            for pname, ti in _param_inputs.items():
                if pname in pg:
                    ti.value = csv_from_values(pg[pname])
        date_from = req.get("date_from")
        date_to = req.get("date_to")
        if date_from is not None:
            date_from_input.value = date_from or ""
        if date_to is not None:
            date_to_input.value = date_to or ""
        state.rerun_request = None
        state.active_tab = "New Run"
        status_label.object = (
            "<span style='color:#2563eb'>Prefilled — review parameters and Run.</span>"
        )

    state.param.watch(_on_rerun_request, ["rerun_request"])

    return pn.Column(
        pn.pane.Markdown("## New Run", margin=(8, 4, 4, 4)),
        pn.pane.Markdown(
            "Runs enqueue on **jobd** (same as `python -m backtester.run --detach`). "
            "You can queue several jobs while one is running — watch them on "
            "**Backtester Run**. Closing the window does not stop jobs.",
            margin=(0, 4, 8, 4),
        ),
        pn.FlexBox(
            family_select, strategy_select, reload_btn,
            align_items="flex-end",
            gap="8px",
            flex_wrap="wrap",
            sizing_mode="stretch_width",
        ),
        pn.pane.Markdown("### Parameters", margin=(12, 4, 4, 4)),
        param_editor_col,
        pn.pane.Markdown("### Date range", margin=(12, 4, 4, 4)),
        pn.Row(date_from_input, date_to_input, sizing_mode="stretch_width"),
        date_error,
        pn.Row(run_btn, sizing_mode="stretch_width"),
        status_label,
        sizing_mode="stretch_width",
    )
