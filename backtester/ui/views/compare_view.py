"""
views/compare_view.py — Compare tab.

Pick Run A and Run B (from all registered runs), then pick a Combo from
each.  Renders:
  - Overlaid equity curves (reuses equity_overlay_figure)
  - Stats-delta table: metric / A / B / Δ / winner
"""
from __future__ import annotations

import logging

import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.equity_service import equity_for_key

log = get_ui_logger(__name__)

# Metrics shown in the delta table (lower_better = True means lower is better)
_COMPARE_METRICS: list[tuple[str, str, bool]] = [
    ("total_pnl",   "Total PnL ($)",     False),
    ("sharpe",      "Sharpe",            False),
    ("sortino",     "Sortino",           False),
    ("calmar",      "Calmar",            False),
    ("max_dd_pct",  "Max DD %",          True),
    ("omega",       "Omega",             False),
    ("n_trades",    "# Trades",          False),
    ("win_rate",    "Win Rate",          False),
    ("profit_factor","Profit Factor",    False),
]


def _run_label(rr) -> str:
    ts = (rr.created_at or "")[:16].replace("T", " ")
    return f"#{rr.id}  {rr.strategy}  {ts}"


def _combo_label(key) -> str:
    if key is None:
        return "—"
    return "  ".join(f"{k}={v}" for k, v in key)


def _best_key(result):
    """Return the top-ranked combo key for a GridResult."""
    if result is None:
        return None
    return result.best_key if hasattr(result, "best_key") else (result.ranked[0][0] if result.ranked else None)


def _get_stat(stats: dict, metric: str):
    """Pull a metric value from a stats dict."""
    v = stats.get(metric)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_compare_view(state, store, cache) -> pn.Column:
    """Return the Compare tab component."""

    title = pn.pane.Markdown("## Compare Runs", margin=(8, 4, 4, 4))

    # ── Run selectors ───────────────────────────────────────────────────────
    def _run_options():
        rows = store.list_runs()
        if not rows:
            return {}
        return {_run_label(r): r.id for r in rows}

    run_opts = _run_options()
    run_opt_keys = list(run_opts.keys())

    run_a_sel = pn.widgets.Select(
        name="Run A",
        options=run_opt_keys,
        value=run_opt_keys[0] if run_opt_keys else None,
        sizing_mode="stretch_width",
        margin=(4, 4),
    )
    run_b_sel = pn.widgets.Select(
        name="Run B",
        options=run_opt_keys,
        value=run_opt_keys[1] if len(run_opt_keys) > 1 else (run_opt_keys[0] if run_opt_keys else None),
        sizing_mode="stretch_width",
        margin=(4, 4),
    )

    combo_a_sel = pn.widgets.Select(
        name="Combo A", options=[], sizing_mode="stretch_width", margin=(4, 4),
    )
    combo_b_sel = pn.widgets.Select(
        name="Combo B", options=[], sizing_mode="stretch_width", margin=(4, 4),
    )

    refresh_btn = pn.widgets.Button(
        name="↺ Refresh runs", button_type="light", width=110, margin=(4, 4),
    )

    compare_btn = pn.widgets.Button(
        name="▶ Compare", button_type="primary", width=110, margin=(4, 4),
        disabled=not bool(run_opt_keys),
    )

    status_msg = pn.pane.HTML("", sizing_mode="stretch_width",
                              styles={"font-size": "12px"})

    chart_holder = pn.Column(sizing_mode="stretch_width")
    table_holder = pn.Column(sizing_mode="stretch_width")

    # ── State ────────────────────────────────────────────────────────────────
    _results: dict = {"a": None, "b": None, "key_a": None, "key_b": None}

    # ── Combo list population ────────────────────────────────────────────────
    def _populate_combos(result, selector, side: str):
        if result is None:
            selector.options = []
            return
        ranked_keys = [k for k, _ in result.ranked[:50]]  # top-50 options
        opts = {_combo_label(k): k for k in ranked_keys}
        selector.options = list(opts.keys())
        if opts:
            selector.value = list(opts.keys())[0]
        # Attach key lookup
        selector._key_map = opts

    def _load_result(run_id):
        if run_id is None:
            return None
        try:
            return cache.get(run_id)
        except Exception as exc:
            log.warning("compare_view: could not load run %s: %s", run_id, exc)
            return None

    def _on_run_a_change(event):
        rid = run_opts.get(event.new)
        result = _load_result(rid)
        _results["a"] = result
        _populate_combos(result, combo_a_sel, "A")

    def _on_run_b_change(event):
        rid = run_opts.get(event.new)
        result = _load_result(rid)
        _results["b"] = result
        _populate_combos(result, combo_b_sel, "B")

    run_a_sel.param.watch(_on_run_a_change, "value")
    run_b_sel.param.watch(_on_run_b_change, "value")

    # ── Refresh runs list ────────────────────────────────────────────────────
    def _refresh_runs(event=None):
        nonlocal run_opts
        run_opts = _run_options()
        opt_keys = list(run_opts.keys())
        run_a_sel.options = opt_keys
        run_b_sel.options = opt_keys
        compare_btn.disabled = not bool(opt_keys)
        if opt_keys:
            run_a_sel.value = opt_keys[0]
            run_b_sel.value = opt_keys[1] if len(opt_keys) > 1 else opt_keys[0]

    refresh_btn.on_click(_refresh_runs)

    # ── Initial combo population ─────────────────────────────────────────────
    if run_opt_keys:
        rid_a = run_opts.get(run_opt_keys[0])
        _results["a"] = _load_result(rid_a)
        _populate_combos(_results["a"], combo_a_sel, "A")
        rid_b = run_opts.get(run_opt_keys[1] if len(run_opt_keys) > 1 else run_opt_keys[0])
        _results["b"] = _load_result(rid_b)
        _populate_combos(_results["b"], combo_b_sel, "B")

    # ── Compare handler ──────────────────────────────────────────────────────
    def _on_compare(event):
        from backtester.ui.charts.equity import equity_overlay_figure

        result_a = _results.get("a")
        result_b = _results.get("b")
        if result_a is None or result_b is None:
            status_msg.object = "<span style='color:#dc2626'>Both runs must be loaded.</span>"
            return

        # Resolve selected keys
        key_a = getattr(combo_a_sel, "_key_map", {}).get(combo_a_sel.value)
        key_b = getattr(combo_b_sel, "_key_map", {}).get(combo_b_sel.value)

        if key_a is None:
            key_a = _best_key(result_a)
        if key_b is None:
            key_b = _best_key(result_b)

        if key_a is None or key_b is None:
            status_msg.object = "<span style='color:#dc2626'>No combo data available.</span>"
            return

        eq_a = equity_for_key(result_a, key_a)
        eq_b = equity_for_key(result_b, key_b)

        label_a = "A: " + _combo_label(key_a)[:40]
        label_b = "B: " + _combo_label(key_b)[:40]

        eqs = {}
        if eq_a:
            eqs[label_a] = eq_a
        if eq_b:
            eqs[label_b] = eq_b

        fig = equity_overlay_figure(eqs, y_mode="nav",
                                    capital=result_a.account_size)
        chart_holder[:] = [pn.pane.Plotly(fig, sizing_mode="stretch_width", height=320)]

        # Stats delta table
        stats_a = result_a.all_stats.get(key_a, {})
        stats_b = result_b.all_stats.get(key_b, {})

        delta_rows = []
        for metric_key, metric_label, lower_better in _COMPARE_METRICS:
            v_a = _get_stat(stats_a, metric_key)
            v_b = _get_stat(stats_b, metric_key)
            if v_a is None and v_b is None:
                continue
            delta = None
            winner = "—"
            if v_a is not None and v_b is not None:
                delta = v_b - v_a
                if lower_better:
                    winner = "A" if v_a < v_b else ("B" if v_b < v_a else "tie")
                else:
                    winner = "A" if v_a > v_b else ("B" if v_b > v_a else "tie")
            delta_rows.append({
                "Metric": metric_label,
                "A": _fmt(v_a),
                "B": _fmt(v_b),
                "Δ (B−A)": _fmt(delta),
                "Winner": winner,
            })

        if delta_rows:
            delta_df = pd.DataFrame(delta_rows)
            dtab = pn.widgets.Tabulator(
                delta_df,
                show_index=False,
                selectable=False,
                sizing_mode="stretch_width",
                height=min(40 + len(delta_rows) * 35, 420),
            )
            dtab.editable = False
            dtab.editors = {col: None for col in delta_df.columns}
            table_holder[:] = [dtab]
        else:
            table_holder[:] = [pn.pane.Markdown("_No stats available._")]

        status_msg.object = "<span style='color:#16a34a'>Done.</span>"

    compare_btn.on_click(_on_compare)

    # ── Layout ───────────────────────────────────────────────────────────────
    selectors_row = pn.GridSpec(sizing_mode="stretch_width", height=90)
    selectors_row[0, 0] = run_a_sel
    selectors_row[0, 1] = run_b_sel

    combo_row = pn.GridSpec(sizing_mode="stretch_width", height=90)
    combo_row[0, 0] = combo_a_sel
    combo_row[0, 1] = combo_b_sel

    controls_row = pn.Row(compare_btn, refresh_btn, status_msg, sizing_mode="stretch_width")

    return pn.Column(
        title,
        selectors_row,
        combo_row,
        controls_row,
        chart_holder,
        table_holder,
        sizing_mode="stretch_width",
    )


def _fmt(v) -> str:
    if v is None:
        return "—"
    try:
        fv = float(v)
        if abs(fv) >= 1000:
            return f"{fv:,.0f}"
        return f"{fv:.3f}"
    except (TypeError, ValueError):
        return str(v)
