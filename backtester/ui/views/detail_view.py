"""
views/detail_view.py — Combo Detail tab.

Three stacked sections for one focused combo:
  1. Stats card — key/value grid of all scalar metrics.
  2. Equity + drawdown — Plotly two-subplot chart.
  3. Trades table — Tabulator of all trades for this combo.
     Clicking a row opens the Trade Inspector panel (replaces a placeholder pane).

Layout switches whenever state.active_combo_key or state.active_run_id changes.
"""
import pandas as pd
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.equity_service import equity_for_key
from backtester.ui.charts.equity import equity_figure

log = get_ui_logger(__name__)

# Trades table columns to display (all others exist on df but aren't useful here)
_TRADE_COLS = ["entry_time", "exit_time", "days_held", "entry_spot", "pnl",
               "pnl_pct", "exit_reason"]

_STATS_LABELS = {
    "n":              "Trades",
    "total_pnl":      "Total PnL ($)",
    "sharpe":         "Sharpe",
    "sortino":        "Sortino",
    "calmar":         "Calmar",
    "profit_factor":  "Profit Factor",
    "max_dd_pct":     "Max DD %",
    "win_rate":       "Win Rate",
    "avg_pnl":        "Avg PnL ($)",
    "omega":          "Omega",
    "consistency":    "Consistency",
    "consec_wins":    "Max Consec Wins",
    "consec_losses":  "Max Consec Losses",
}


def _fmt(v) -> str:
    """Format a scalar stat value for display."""
    if v is None:
        return "—"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:.3f}"
    return str(v)


# Columns whose values are USD amounts → format as xx,xxx.xx
_DOLLAR_COLS = frozenset({
    "entry_spot", "exit_spot", "entry_price_usd", "exit_price_usd", "fees", "pnl",
})
# Columns that are percentages → format as xx.xx
_PCT_COLS = frozenset({"pnl_pct", "win_rate", "max_dd_pct"})


def _fmt_trade_val(col: str, val) -> str:
    """Format a single trade-row value for the inspector panel."""
    import math
    if val is None:
        return "—"
    if isinstance(val, float) and math.isnan(val):
        return "—"
    if col in _DOLLAR_COLS:
        try:
            return f"{float(val):,.2f}"
        except (TypeError, ValueError):
            return str(val)
    if col in _PCT_COLS:
        try:
            return f"{float(val):.2f}%"
        except (TypeError, ValueError):
            return str(val)
    if isinstance(val, float):
        if abs(val) >= 1_000:
            return f"{val:,.0f}"
        if abs(val) >= 10:
            return f"{val:.2f}"
        return f"{val:.4g}"
    return str(val)


def _stats_card_html(stats: dict, eq: dict | None, key: tuple, rank: int | None) -> str:
    """Build an HTML snippet for the stats card."""
    params = dict(key) if key else {}
    rank_str = f"Rank #{rank}" if rank is not None else ""

    # Params section
    param_rows = "".join(
        f"<tr><td><b>{k}</b></td><td>{v}</td></tr>"
        for k, v in params.items()
    )

    # Stats section — from all_stats
    merged = dict(stats) if stats else {}
    # Overlay equity-only metrics if available
    if eq:
        for k in ("sortino", "calmar", "consec_wins", "consec_losses"):
            if k in eq:
                merged[k] = eq[k]

    stat_rows = ""
    for field, label in _STATS_LABELS.items():
        val = merged.get(field)
        stat_rows += f"<tr><td style='color:#6b7280'>{label}</td><td><b>{_fmt(val)}</b></td></tr>"

    return f"""
<div style="display:flex;gap:24px;flex-wrap:wrap;font-size:13px;line-height:1.6">
  <div>
    <div style="font-weight:600;margin-bottom:4px;color:#1a1a2e">Params
      {f'<span style="color:#6b7280;font-weight:normal;margin-left:8px">{rank_str}</span>' if rank_str else ''}
    </div>
    <table style="border-collapse:collapse">
      {param_rows}
    </table>
  </div>
  <div>
    <div style="font-weight:600;margin-bottom:4px;color:#1a1a2e">Metrics</div>
    <table style="border-collapse:collapse">
      {stat_rows}
    </table>
  </div>
</div>"""


def _trades_df(result, combo_idx: int) -> pd.DataFrame:
    """Build display-ready trades DataFrame for one combo."""
    df = result.df
    if df is None or df.empty:
        return pd.DataFrame()

    df_c = df[df["combo_idx"] == combo_idx].copy()
    if df_c.empty:
        return df_c

    # Add derived columns
    if "entry_time" in df_c.columns and "exit_time" in df_c.columns:
        df_c["days_held"] = (
            (pd.to_datetime(df_c["exit_time"]) - pd.to_datetime(df_c["entry_time"]))
            .dt.total_seconds() / 86400
        ).round(2)

    if "pnl" in df_c.columns and "entry_price_usd" in df_c.columns:
        df_c["pnl_pct"] = (
            df_c["pnl"] / df_c["entry_price_usd"].replace(0, float("nan")) * 100
        ).round(2)

    # Format timestamps
    for col in ("entry_time", "exit_time"):
        if col in df_c.columns:
            df_c[col] = pd.to_datetime(df_c[col]).dt.strftime("%Y-%m-%d %H:%M")

    # Select and order display columns
    avail = [c for c in _TRADE_COLS if c in df_c.columns]
    # Append any extra columns from df not in the standard list (leg data, etc.)
    extras = [c for c in df_c.columns if c not in avail and c not in ("combo_idx",)]
    return df_c[avail + extras].reset_index(drop=True)


def _inspector_panel(trade_row: pd.Series, result) -> pn.Column:
    """Build the Trade Inspector content for a single trade row."""
    # Key-value pairs from the trade row
    rows_html = "".join(
        f"<tr><td style='color:#6b7280;padding:2px 12px 2px 0'>{col}</td>"
        f"<td><b>{_fmt_trade_val(col, val)}</b></td></tr>"
        for col, val in trade_row.items()
    )
    kv_pane = pn.pane.HTML(
        f"<table style='font-size:12px;line-height:1.7;border-collapse:collapse'>"
        f"{rows_html}</table>",
        sizing_mode="stretch_width",
    )

    # Mini spot chart — entry_spot / exit_spot are the only time-labelled spots
    # available without re-reading parquet files (per spec §4.3.1).
    try:
        import plotly.graph_objects as go
        entry_t = str(trade_row.get("entry_time", "entry"))
        exit_t  = str(trade_row.get("exit_time",  "exit"))
        e_spot  = float(trade_row.get("entry_spot", 0))
        x_spot  = float(trade_row.get("exit_spot",  0) if "exit_spot" in trade_row.index else 0)
        pnl_v   = float(trade_row.get("pnl", 0))
        color   = "#16a34a" if pnl_v >= 0 else "#dc2626"
        mini_fig = go.Figure(go.Scatter(
            x=[entry_t, exit_t],
            y=[e_spot, x_spot],
            mode="lines+markers",
            line=dict(color=color, width=2),
            marker=dict(size=8),
            hovertemplate="%{x}: $%{y:,.0f}<extra></extra>",
        ))
        mini_fig.update_layout(
            height=140, margin=dict(l=40, r=10, t=10, b=30),
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="#e5e7eb"),
            showlegend=False,
        )
        mini_chart = pn.pane.Plotly(mini_fig, sizing_mode="stretch_width")
    except Exception as exc:
        log.debug("detail_view: mini-chart unavailable — %s", exc)
        mini_chart = pn.pane.Markdown("_Spot chart unavailable_")

    exit_reason = trade_row.get("exit_reason", "")
    header = pn.pane.HTML(
        f"<h4 style='margin:0 0 8px 0;color:#1a1a2e'>Trade Inspector"
        f"<span style='font-size:12px;color:#6b7280;margin-left:8px'>"
        f"exit: {exit_reason}</span></h4>",
        sizing_mode="stretch_width",
    )
    return pn.Column(header, kv_pane, mini_chart, sizing_mode="stretch_width")


def build_detail_view(state, cache, store=None) -> pn.Column:
    """Build the Combo Detail tab component.

    Renders stats card + equity chart + trades table.
    Responds to state.active_combo_key and state.active_run_id.
    """
    _placeholder = pn.pane.Markdown(
        "_No combo selected — click a row in the Results Grid and choose 'View Detail'._",
        sizing_mode="stretch_width",
    )
    _content = pn.Column(_placeholder, sizing_mode="stretch_width")

    # Inspector pane (shown below trades when a trade row is clicked)
    _inspector = pn.Column(sizing_mode="stretch_width")

    # ── Star toggle button (Phase 4) ─────────────────────────────────────────
    _star_btn = pn.widgets.Button(
        name="☆ Star", button_type="default", width=90, disabled=True,
    )
    _star_feedback = pn.pane.HTML("", styles={"font-size": "11px"}, width=160)

    def _refresh_star_btn(run_id, key):
        if store is None or run_id is None or key is None:
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
                _star_feedback.object = "<span style='color:#d97706'>Removed.</span>"
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
            log.error("detail_view: star toggle failed: %s", exc)

    _star_btn.on_click(_on_star)

    def _render(run_id, key):
        """(Re)build the detail view for a given run + combo key."""
        _inspector[:] = []
        if run_id is None or key is None:
            _content[:] = [_placeholder]
            _star_btn.disabled = True
            return
        try:
            result = cache.get(run_id)
        except Exception as exc:
            _content[:] = [pn.pane.Markdown(f"⚠ Error loading run: {exc}")]
            return

        combo_idx = result.key_to_idx.get(key)
        if combo_idx is None:
            _content[:] = [pn.pane.Markdown("⚠ Combo key not found in this run.")]
            return

        stats = result.all_stats.get(key, {})
        eq = equity_for_key(result, key)

        # Rank lookup
        rank = next(
            (i + 1 for i, (k, _) in enumerate(result.ranked) if k == key),
            None,
        )

        _refresh_star_btn(run_id, key)

        # 1 — Stats card (with star button)
        card = pn.pane.HTML(
            _stats_card_html(stats, eq, key, rank),
            sizing_mode="stretch_width",
        )
        star_row = pn.Row(_star_btn, _star_feedback, sizing_mode="stretch_width")

        # 2 — Equity + drawdown chart
        if eq and eq.get("daily"):
            title = f"Rank #{rank} equity" if rank else "Equity"
            fig = equity_figure(eq, title=title, capital=result.account_size)
            equity_pane = pn.pane.Plotly(fig, sizing_mode="stretch_width")
        else:
            equity_pane = pn.pane.Markdown("_No equity data for this combo._")

        # 3 — WFO IS/OOS section (Phase 4) — shown only when meta has wfo_result
        wfo_pane = _wfo_section(run_id)

        # 4 — Trades table
        df_trades = _trades_df(result, combo_idx)

        if df_trades.empty:
            trades_pane: pn.viewable.Viewable = pn.pane.Markdown("_No trades._")
        else:
            # Add a hidden _row_idx column for inspector lookup
            df_trades["_row_idx"] = range(len(df_trades))
            # Keep a reference to the full original df_c for the inspector
            _df_full = result.df[result.df["combo_idx"] == combo_idx].reset_index(drop=True)

            display_cols = [c for c in df_trades.columns if c != "_row_idx"] + ["_row_idx"]

            # Build display-only formatters for numeric columns (doesn't alter data)
            from bokeh.models.widgets.tables import NumberFormatter
            _tab_fmts = {}
            for _c in display_cols:
                if _c in _DOLLAR_COLS:
                    _tab_fmts[_c] = NumberFormatter(format="0,0.00")
                elif _c in _PCT_COLS:
                    _tab_fmts[_c] = NumberFormatter(format="0.00")
                elif _c == "days_held":
                    _tab_fmts[_c] = NumberFormatter(format="0.00")

            tab = pn.widgets.Tabulator(
                df_trades[display_cols],
                hidden_columns=["_row_idx"],
                formatters=_tab_fmts,
                pagination="remote",
                page_size=100,
                selectable=1,
                sizing_mode="stretch_width",
                show_index=False,
            )
            tab.editable = False
            tab.editors = {col: None for col in df_trades[display_cols].columns}

            def _on_trade_select(event):
                idx_list = event.new
                if not idx_list:
                    _inspector[:] = []
                    return
                row_idx = tab.value.iloc[idx_list[0]]["_row_idx"]
                trade_row = _df_full.iloc[int(row_idx)]
                _inspector[:] = [_inspector_panel(trade_row, result)]

            tab.param.watch(_on_trade_select, "selection")
            trades_section = pn.Column(
                pn.pane.HTML("<h4 style='margin:8px 0 4px 0;color:#1a1a2e'>Trades</h4>"),
                tab,
                pn.pane.HTML(
                    "<div style='color:#6b7280;font-size:12px;margin-top:4px'>"
                    "Click a row to inspect the trade.</div>"
                ),
                _inspector,
                sizing_mode="stretch_width",
            )
            trades_pane = trades_section

        _content[:] = [star_row, card, equity_pane, wfo_pane, trades_pane]

    # ── WFO helper ─────────────────────────────────────────────────────────────
    def _wfo_section(run_id) -> pn.viewable.Viewable:
        """Build IS/OOS delta table if meta has wfo_result, else empty."""
        if store is None or run_id is None:
            return pn.pane.HTML("")
        try:
            meta = store.get_bundle_meta(run_id)
            wfo_data = meta.get("wfo_result")
            if not wfo_data:
                return pn.pane.HTML("")
            windows = wfo_data.get("windows", [])
            if not windows:
                return pn.pane.HTML("")
            rows = []
            for w in windows:
                is_s = w.get("is_sharpe")
                oos_s = w.get("oos_sharpe")
                delta_s = (oos_s - is_s) if (is_s is not None and oos_s is not None) else None
                rows.append({
                    "Window":       w.get("idx", ""),
                    "IS Period":    f"{(w.get('is_start') or '')[:10]} → {(w.get('is_end') or '')[:10]}",
                    "OOS Period":   f"{(w.get('oos_start') or '')[:10]} → {(w.get('oos_end') or '')[:10]}",
                    "IS Sharpe":    round(is_s, 3) if is_s is not None else None,
                    "OOS Sharpe":   round(oos_s, 3) if oos_s is not None else None,
                    "Δ Sharpe":     round(delta_s, 3) if delta_s is not None else None,
                    "IS PnL":       round(float(w.get("is_pnl", 0)), 0),
                    "OOS PnL":      round(float(w.get("oos_pnl", 0)), 0),
                    "OOS Win":      "✓" if w.get("oos_win") else "✗",
                })
            df_wfo = pd.DataFrame(rows)
            agg = wfo_data
            wr = agg.get("oos_win_rate")
            avg_s = agg.get("oos_avg_sharpe")
            tot_pnl = agg.get("oos_total_pnl")
            summary_html = (
                f"<div style='font-size:12px;color:#6b7280;margin:4px 0 8px 0'>"
                f"OOS win rate: <b>{wr:.0%}</b>"
                f" &nbsp;|&nbsp; Avg OOS Sharpe: <b>{avg_s:.3f}</b>"
                f" &nbsp;|&nbsp; Total OOS PnL: <b>${tot_pnl:,.0f}</b>"
                f"</div>"
                if all(v is not None for v in (wr, avg_s, tot_pnl)) else ""
            )
            tab = pn.widgets.Tabulator(
                df_wfo, show_index=False, selectable=False,
                sizing_mode="stretch_width",
                height=40 + len(rows) * 35,
            )
            tab.editable = False
            tab.editors = {col: None for col in df_wfo.columns}
            return pn.Column(
                pn.pane.HTML("<h4 style='margin:12px 0 4px 0;color:#1a1a2e'>Walk-Forward Validation</h4>"),
                pn.pane.HTML(summary_html) if summary_html else pn.pane.HTML(""),
                tab,
                sizing_mode="stretch_width",
            )
        except Exception as exc:
            log.warning("detail_view: wfo_section failed: %s", exc)
            return pn.pane.HTML("")

    def _on_change(event=None):
        _render(state.active_run_id, state.active_combo_key)

    state.param.watch(_on_change, ["active_run_id", "active_combo_key"])

    return pn.Column(_content, sizing_mode="stretch_width")