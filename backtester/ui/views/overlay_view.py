"""
views/overlay_view.py — Equity Overlay tab.

Renders a multi-combo equity overlay chart.
Responds to state.selected_combo_keys and state.active_run_id.

Controls:
  - Y-mode toggle: NAV vs Cum PnL
  - Log-y toggle
  - Underwater subplot toggle (not yet implemented: wrapped for future)
"""
import panel as pn
import param

from backtester.ui.log import get_ui_logger
from backtester.ui.services.equity_service import equity_many
from backtester.ui.charts.equity import equity_overlay_figure

log = get_ui_logger(__name__)

_MAX_OVERLAY = 50  # soft limit for readable overlays


def _key_label(key, rank: int | None = None) -> str:
    """Short label for a combo key, suitable for Plotly legend."""
    parts = ", ".join(f"{k}={v}" for k, v in key)
    prefix = f"#{rank} " if rank is not None else ""
    return prefix + parts


def build_overlay_view(state, cache) -> pn.Column:
    """Build the Equity Overlay tab component."""

    # Controls
    y_mode = pn.widgets.RadioButtonGroup(
        options=["NAV", "Cum PnL"],
        value="NAV",
        button_type="default",
        sizing_mode="fixed",
        width=200,
    )
    log_y = pn.widgets.Toggle(
        name="Log Y",
        value=False,
        button_type="default",
        width=90,
    )

    warn_pane = pn.pane.HTML("", sizing_mode="stretch_width")
    chart_pane = pn.pane.Plotly(
        equity_overlay_figure({}),
        sizing_mode="stretch_width",
    )

    def _rebuild(*_):
        run_id = state.active_run_id
        keys   = state.selected_combo_keys

        if run_id is None or not keys:
            chart_pane.object = equity_overlay_figure({})
            warn_pane.object = ""
            return

        if len(keys) > _MAX_OVERLAY:
            warn_pane.object = (
                f"<div style='color:#d97706;font-size:12px'>"
                f"⚠ {len(keys)} combos selected — showing first {_MAX_OVERLAY}. "
                f"Legend may be crowded.</div>"
            )
            keys = keys[:_MAX_OVERLAY]
        else:
            warn_pane.object = ""

        try:
            result = cache.get(run_id)
        except Exception as exc:
            chart_pane.object = equity_overlay_figure({})
            warn_pane.object = f"<div style='color:#dc2626'>⚠ Error loading run: {exc}</div>"
            return

        eqs = equity_many(result, keys)

        # Build label map: key → label
        rank_map = {k: i + 1 for i, (k, _) in enumerate(result.ranked)}
        labeled_eqs = {
            _key_label(k, rank_map.get(k)): eq
            for k, eq in eqs.items()
        }

        mode = "nav" if y_mode.value == "NAV" else "cumpnl"
        fig = equity_overlay_figure(labeled_eqs, y_mode=mode,
                                    capital=result.account_size)

        if log_y.value:
            fig.update_yaxes(type="log")

        chart_pane.object = fig
        log.debug("overlay_view: rendered %d curves", len(labeled_eqs))

    # Wire up reactive dependencies
    state.param.watch(lambda e: _rebuild(), ["active_run_id", "selected_combo_keys"])
    y_mode.param.watch(lambda e: _rebuild(), "value")
    log_y.param.watch(lambda e: _rebuild(), "value")

    toolbar = pn.Row(
        pn.pane.Markdown("### Equity Overlay", margin=(5, 10)),
        pn.Spacer(),
        pn.Row(
            pn.pane.Markdown("Y-axis:", margin=(8, 4)),
            y_mode,
            log_y,
        ),
        sizing_mode="stretch_width",
    )

    return pn.Column(
        toolbar,
        warn_pane,
        chart_pane,
        sizing_mode="stretch_width",
    )
