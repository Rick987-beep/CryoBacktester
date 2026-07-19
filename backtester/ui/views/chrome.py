"""
views/chrome.py — Top navigation + context detail bar for the Research UI.
"""
from __future__ import annotations

import panel as pn

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

# Canonical nav pages (order = left-to-right)
NAV_PAGES = [
    "New Run",
    "Runs",
    "Results Grid",
    "Combo Detail",
    "Favourites",
]

# Legacy URL ?tab= values → current page names
_LEGACY_TAB_MAP = {
    "Equity Overlay": "Combo Detail",
    "Compare": "Favourites",
}

# Nav sits on the blue template header — use amber/slate, not header blue
_NAV_CSS = """
:host .bk-btn-group {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}
:host .bk-btn-group .bk-btn {
  background: rgba(255, 255, 255, 0.12) !important;
  color: #f8fafc !important;
  border: 1px solid rgba(255, 255, 255, 0.28) !important;
  box-shadow: none !important;
  font-weight: 500;
  padding: 6px 12px;
}
:host .bk-btn-group .bk-btn:hover {
  background: rgba(255, 255, 255, 0.22) !important;
}
:host .bk-btn-group .bk-btn.bk-active,
:host .bk-btn-group .bk-btn[aria-pressed="true"] {
  background: #f59e0b !important;
  color: #1a1a2e !important;
  border-color: #d97706 !important;
  font-weight: 600;
}
"""


def normalize_tab_name(name: str | None) -> str:
    """Map legacy or unknown tab names onto a canonical NAV_PAGES entry."""
    if not name:
        return "Results Grid"
    if name in _LEGACY_TAB_MAP:
        return _LEGACY_TAB_MAP[name]
    if name in NAV_PAGES:
        return name
    return "Results Grid"


def build_nav(state) -> pn.widgets.RadioButtonGroup:
    """Header navigation buttons bound to state.active_tab (no brand text)."""
    nav = pn.widgets.RadioButtonGroup(
        name="",
        options=NAV_PAGES,
        value=normalize_tab_name(state.active_tab),
        button_type="default",
        sizing_mode="fixed",
        margin=(6, 8),
        stylesheets=[_NAV_CSS],
    )

    def _nav_to_state(event):
        state.active_tab = event.new

    def _state_to_nav(event):
        mapped = normalize_tab_name(event.new)
        if mapped != event.new:
            state.active_tab = mapped
            return
        if nav.value != mapped:
            nav.value = mapped

    nav.param.watch(_nav_to_state, "value")
    state.param.watch(_state_to_nav, "active_tab")

    state.active_tab = normalize_tab_name(state.active_tab)
    return nav


def _combo_params_str(key) -> str:
    try:
        return " · ".join(f"{k}={v}" for k, v in key)
    except Exception:
        return str(key)


def _fmt_metric(v) -> str:
    if v is None:
        return "—"
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(fv) >= 1000:
        return f"{fv:,.0f}"
    if abs(fv) >= 10:
        return f"{fv:.2f}"
    return f"{fv:.3f}"


def _metrics_str(result, key) -> str:
    """Compact one-line performance summary for the selection bar."""
    if result is None or key is None:
        return ""
    stats = result.all_stats.get(key, {}) if getattr(result, "all_stats", None) else {}
    score = None
    if getattr(result, "scores", None) is not None:
        score = result.scores.get(key)
    rank = next(
        (i + 1 for i, (k, _) in enumerate(getattr(result, "ranked", []) or []) if k == key),
        None,
    )
    parts = []
    if rank is not None:
        parts.append(f"#{rank}")
    if score is not None:
        parts.append(f"score={_fmt_metric(score)}")
    for field, label in (
        ("sharpe", "sharpe"),
        ("total_pnl", "pnl"),
        ("max_dd_pct", "dd%"),
        ("n", "trades"),
        ("win_rate", "wr"),
    ):
        if field in stats and stats[field] is not None:
            parts.append(f"{label}={_fmt_metric(stats[field])}")
    return " · ".join(parts)


def _esc(text: str) -> str:
    return (
        (text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _ellipsis_cell(label: str, text: str, flex: str = "1 1 0") -> str:
    """Single-line cell that truncates with …; full text in title tooltip."""
    safe = _esc(text)
    display = safe if safe else "—"
    return (
        f'<div style="flex:{flex};min-width:0;overflow:hidden;white-space:nowrap;'
        f'text-overflow:ellipsis;padding:0 10px;border-left:1px solid #e5e7eb" '
        f'title="{safe}">'
        f'<span style="color:#6b7280;font-weight:600;margin-right:6px">{label}</span>'
        f'{display}</div>'
    )


def build_detail_bar(state, store, run_service=None, cache=None) -> pn.Row:
    """Single-line selection bar: Run | Combo ID | Params | Metrics [| Cancel]."""
    bar_html = pn.pane.HTML(
        "",
        sizing_mode="stretch_width",
        margin=(0, 0),
        height=36,
    )
    cancel_btn = pn.widgets.Button(
        name="■ Cancel",
        button_type="danger",
        width=100,
        visible=False,
        margin=(2, 8, 2, 4),
        height=28,
    )

    def _refresh_labels(*_):
        # ── Run ──────────────────────────────────────────────────────────────
        rid = state.active_run_id
        if rid is None:
            run_txt = "No run selected"
        else:
            rr = store.get_run(rid) if store is not None else None
            if rr is None:
                run_txt = f"#{rid} (not in store)"
            else:
                ts = (rr.created_at or "")[:16].replace("T", " ")
                label = rr.label or rr.strategy
                run_txt = f"#{rr.id} · {label} · {ts} · {rr.n_combos or '—'} combos"

        # ── Combo ID + params + metrics ──────────────────────────────────────
        key = state.active_combo_key
        if key is None:
            cid_txt = "—"
            params_txt = ""
            metrics_txt = ""
        else:
            from backtester.ui.services.store_service import key_hash as _kh
            cid_txt = _kh(key)
            params_txt = _combo_params_str(key)
            metrics_txt = ""
            if cache is not None and rid is not None:
                try:
                    result = cache.get(rid)
                    metrics_txt = _metrics_str(result, key)
                except Exception as exc:
                    log.debug("selection_bar: metrics unavailable: %s", exc)

        # ── In-flight strip ──────────────────────────────────────────────────
        handle = state.active_run_handle
        flight = ""
        if handle is not None:
            cancel_btn.visible = True
            flight = (
                '<div style="flex:0 0 auto;padding:0 8px;color:#2563eb;'
                'font-weight:600;white-space:nowrap">Running…</div>'
            )
        else:
            cancel_btn.visible = False

        run_safe = _esc(run_txt)
        cid_safe = _esc(cid_txt)
        bar_html.object = (
            '<div style="display:flex;align-items:center;height:36px;font-size:13px;'
            'line-height:36px;background:#f8fafc;border-bottom:1px solid #e5e7eb;'
            'overflow:hidden;white-space:nowrap">'
            f'<div style="flex:0 1 auto;max-width:28%;min-width:140px;overflow:hidden;'
            f'text-overflow:ellipsis;padding:0 10px" title="{run_safe}">'
            f'<span style="color:#6b7280;font-weight:600;margin-right:6px">Run</span>'
            f'{run_safe}</div>'
            f'<div style="flex:0 0 auto;padding:0 10px;border-left:1px solid #e5e7eb;'
            f'font-family:ui-monospace,Menlo,monospace;font-size:12px" title="{cid_safe}">'
            f'<span style="color:#6b7280;font-weight:600;margin-right:6px;'
            f'font-family:inherit">ID</span>{cid_safe}</div>'
            + _ellipsis_cell("Params", params_txt, "1 1 0")
            + _ellipsis_cell("Metrics", metrics_txt, "1 1 0")
            + flight
            + "</div>"
        )

    def _on_cancel(event):
        handle = state.active_run_handle
        if handle is None or run_service is None:
            return
        try:
            run_service.cancel(handle)
            _refresh_labels()
        except Exception as exc:
            log.error("selection_bar: cancel failed: %s", exc)

    cancel_btn.on_click(_on_cancel)
    state.param.watch(
        _refresh_labels,
        ["active_run_id", "active_combo_key", "active_run_handle"],
    )
    _refresh_labels()

    return pn.Row(
        bar_html,
        cancel_btn,
        sizing_mode="stretch_width",
        height=36,
        margin=(0, 0, 4, 0),
        styles={"overflow": "hidden"},
    )
