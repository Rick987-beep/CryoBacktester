"""
views/sidebar.py — Sidebar with runs list + strategy picker + run control.

Phase 1: runs list (buttons, Refresh).
Phase 3: strategy dropdown, param CSV editor, date range, Run / Cancel,
         progress bar + periodic progress tailing.
"""
import panel as pn

from backtester.ui.log import get_ui_logger
from backtester.ui.services.store_service import RunRow

log = get_ui_logger(__name__)

# ── param CSV parser ───────────────────────────────────────────────────────────


def _expand_range_token(token: str) -> list:
    """Parse a range shorthand token ``start..end[:step]`` into a list of values.

    Uses integer arithmetic when all of *start*, *end*, and *step* are written
    without decimal points; float arithmetic otherwise.

    Examples::

        _expand_range_token("10..50:5")    → [10, 15, 20, 25, 30, 35, 40, 45, 50]
        _expand_range_token("0.1..0.5:0.1") → [0.1, 0.2, 0.3, 0.4, 0.5]
        _expand_range_token("3..7")         → [3, 4, 5, 6, 7]

    Raises:
        ValueError: if step is not > 0, or start > end, or no values produced.
    """
    token = token.strip()
    step_raw: str | None = None

    if ":" in token:
        range_part, step_raw = token.rsplit(":", 1)
        step_raw = step_raw.strip()
    else:
        range_part = token

    if ".." not in range_part:
        raise ValueError(f"not a range token: '{token}'")

    start_raw, end_raw = range_part.split("..", 1)
    start_raw = start_raw.strip()
    end_raw = end_raw.strip()

    # Decide int vs float based on presence of '.' in any component
    use_int = (
        "." not in start_raw
        and "." not in end_raw
        and (step_raw is None or "." not in step_raw)
    )

    if use_int:
        start = int(start_raw)
        end = int(end_raw)
        step = int(step_raw) if step_raw else 1
        if step <= 0:
            raise ValueError(f"step must be > 0, got {step}")
        if start > end:
            raise ValueError(f"start ({start}) > end ({end})")
        result = list(range(start, end + 1, step))
    else:
        start = float(start_raw)
        end = float(end_raw)
        step = float(step_raw) if step_raw else 1.0
        if step <= 0.0:
            raise ValueError(f"step must be > 0, got {step}")
        if start > end + 1e-9 * abs(step):
            raise ValueError(f"start ({start}) > end ({end})")
        # Determine decimal precision from step (or start if no step given)
        _src = step_raw if step_raw else start_raw
        if "." in _src:
            _decimals = len(_src.rstrip("0").split(".")[-1])
        else:
            _decimals = 6
        n = int(round((end - start) / step)) + 1
        result = []
        for i in range(n):
            v = round(start + i * step, _decimals + 1)
            if v > end + step * 1e-9:
                break
            result.append(round(v, _decimals))

    if not result:
        raise ValueError(f"range '{token}' produces no values")
    return result


def parse_param_csv(key: str, csv_str: str, sample) -> tuple:
    """Parse a CSV string into a typed list.

    Args:
        key:     Param name (used only in error messages).
        csv_str: Comma-separated value string e.g. "0, 3.0, 6.0".
        sample:  A representative value from the strategy's PARAM_GRID
                 (used to infer target type).

    Returns:
        (values, error_msg) — values is a list if successful, None on error.
        error_msg is None on success.
    """
    parts = [p.strip() for p in csv_str.split(",") if p.strip()]
    if not parts:
        return None, f"{key}: at least one value required"
    try:
        # First pass: expand range tokens so the rest of the logic is uniform
        expanded: list = []
        for p in parts:
            if ".." in p:
                expanded.extend(_expand_range_token(p))
            else:
                expanded.append(p)  # raw string; type-coerced below

        if isinstance(sample, bool):
            result = []
            for item in expanded:
                s = str(item).lower() if not isinstance(item, bool) else ("true" if item else "false")
                if s in ("1", "true", "yes"):
                    result.append(True)
                elif s in ("0", "false", "no"):
                    result.append(False)
                else:
                    raise ValueError(f"expected bool, got '{item}'")
            return result, None
        elif isinstance(sample, int):
            result = []
            for item in expanded:
                if isinstance(item, (int, float)) and not isinstance(item, bool):
                    result.append(item)  # keep native type from range expansion
                else:
                    try:
                        result.append(int(str(item)))
                    except ValueError:
                        result.append(float(str(item)))
            return result, None
        elif isinstance(sample, float):
            return [float(item) for item in expanded], None
        else:
            return [str(item) for item in expanded], None
    except (ValueError, TypeError) as exc:
        return None, f"{key}: {exc}"


# ── helpers ────────────────────────────────────────────────────────────────────


def _csv_from_values(values) -> str:
    """Convert a list of param values back to a CSV string."""
    return ", ".join(str(v) for v in values)


# ── build_sidebar ──────────────────────────────────────────────────────────────


def build_sidebar(state, store, cache, run_service=None) -> pn.Column:
    """Build the full sidebar component.

    Args:
        state:       AppState.
        store:       StoreService.
        cache:       ResultCache.
        run_service: RunService (Phase 3). If None, run controls are omitted.
    """
    # ── Runs list ─────────────────────────────────────────────────────────────
    try:
        store.scan_bundles()
    except Exception as exc:
        log.warning("scan_bundles failed: %s", exc)

    runs_container = pn.Column(sizing_mode="stretch_width")

    def _refresh_runs():
        rows = store.list_runs()
        if not rows:
            runs_container[:] = [pn.pane.Markdown("_No runs found._")]
            return
        items = []
        for rr in rows:
            label = rr.label or rr.strategy
            ts_short = rr.created_at[:16].replace("T", " ")
            n_str = f"{rr.n_combos} combos" if rr.n_combos else ""
            pin_icon = "📌 " if rr.pinned else ""
            dirty_icon = " ⚠" if rr.git_dirty else ""
            btn = pn.widgets.Button(
                name=f"{pin_icon}{label}{dirty_icon}\n{ts_short}  {n_str}",
                button_type="light",
                sizing_mode="stretch_width",
                margin=(2, 4),
            )
            run_id = rr.id

            def _on_click(event, rid=run_id):
                log.debug("sidebar: activating run_id=%d", rid)
                state.active_run_id = rid

            btn.on_click(_on_click)
            items.append(btn)
        runs_container[:] = items

    _refresh_runs()

    refresh_btn = pn.widgets.Button(
        name="↺ Refresh", button_type="light", width=90, margin=(4, 4),
    )
    refresh_btn.on_click(lambda e: _refresh_runs())

    runs_section = pn.Column(
        pn.pane.Markdown("## Runs", margin=(8, 4)),
        pn.Row(refresh_btn, sizing_mode="stretch_width"),
        pn.pane.HTML("<hr style='margin:4px 0;border-color:#ccc'>"),
        runs_container,
        sizing_mode="stretch_width",
    )

    # ── Run control (Phase 3) — only when run_service is provided ─────────────
    if run_service is None:
        return runs_section

    from backtester.run import STRATEGIES

    # ── Strategy dropdown ──────────────────────────────────────────────────────
    strategy_select = pn.widgets.Select(
        name="Strategy",
        options=sorted(STRATEGIES.keys()),
        value="short_generic" if "short_generic" in STRATEGIES else sorted(STRATEGIES.keys())[0],
        sizing_mode="stretch_width",
    )

    # ── Param editor ───────────────────────────────────────────────────────────
    _param_inputs: dict = {}
    _param_errors: dict = {}
    param_editor_col = pn.Column(sizing_mode="stretch_width")

    def _load_strategy_params(key: str):
        cls = STRATEGIES.get(key)
        if cls is None:
            return
        grid = getattr(cls, "PARAM_GRID", {})
        _param_inputs.clear()
        _param_errors.clear()
        rows = []
        for pname in sorted(grid.keys()):
            vals = grid[pname]
            ti = pn.widgets.TextInput(
                name=pname,
                value=_csv_from_values(vals),
                sizing_mode="stretch_width",
                margin=(2, 4),
            )
            err = pn.pane.HTML("", sizing_mode="stretch_width",
                               styles={"color": "#dc2626", "font-size": "11px"})
            _param_inputs[pname] = ti
            _param_errors[pname] = err
            ti.param.watch(lambda e: _validate_all(), "value")
            rows.append(pn.Column(ti, err, sizing_mode="stretch_width"))
        param_editor_col[:] = rows
        _validate_all()

    # ── Date range ─────────────────────────────────────────────────────────────
    _date_fmt = "%Y-%m-%d"

    date_from_input = pn.widgets.TextInput(
        name="Date from (YYYY-MM-DD)", value="",
        sizing_mode="stretch_width", margin=(2, 4),
    )
    date_to_input = pn.widgets.TextInput(
        name="Date to (YYYY-MM-DD)", value="",
        sizing_mode="stretch_width", margin=(2, 4),
    )
    date_error = pn.pane.HTML("", sizing_mode="stretch_width",
                              styles={"color": "#dc2626", "font-size": "11px"})

    def _load_date_range(key: str):
        cls = STRATEGIES.get(key)
        if cls is None:
            return
        dr = getattr(cls, "DATE_RANGE", (None, None))
        date_from_input.value = dr[0] or ""
        date_to_input.value   = dr[1] or ""

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

    date_from_input.param.watch(lambda e: _validate_all(), "value")
    date_to_input.param.watch(lambda e: _validate_all(), "value")

    # ── Run / Cancel buttons ───────────────────────────────────────────────────
    run_btn = pn.widgets.Button(
        name="▶ Run", button_type="success", disabled=True,
        sizing_mode="stretch_width", margin=(6, 4),
    )
    cancel_btn = pn.widgets.Button(
        name="■ Cancel", button_type="danger", disabled=True,
        sizing_mode="stretch_width", margin=(2, 4),
    )

    # ── Progress widgets ───────────────────────────────────────────────────────
    progress_bar = pn.widgets.Progress(
        name="Progress", value=0, max=100,
        bar_color="primary", sizing_mode="stretch_width",
        visible=False,
    )
    progress_label = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "11px", "color": "#6b7280"},
    )
    status_label = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "12px"},
    )

    # ── Validation ─────────────────────────────────────────────────────────────
    def _validate_all() -> bool:
        """Validate all param inputs + dates; enable/disable Run button."""
        all_ok = True
        cls = STRATEGIES.get(strategy_select.value)
        grid = getattr(cls, "PARAM_GRID", {}) if cls else {}
        for pname, ti in _param_inputs.items():
            sample = grid.get(pname, [None])[0]
            _, err_msg = parse_param_csv(pname, ti.value, sample)
            if err_msg:
                _param_errors[pname].object = err_msg
                all_ok = False
            else:
                _param_errors[pname].object = ""
        if not _validate_dates():
            all_ok = False
        run_btn.disabled = not all_ok or state.active_run_handle is not None
        return all_ok

    # ── Strategy change ────────────────────────────────────────────────────────
    def _on_strategy_change(event):
        _load_strategy_params(event.new)
        _load_date_range(event.new)

    strategy_select.param.watch(_on_strategy_change, "value")

    _load_strategy_params(strategy_select.value)
    _load_date_range(strategy_select.value)

    # ── Run handler ────────────────────────────────────────────────────────────
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
            _refresh_runs()
            status_label.object = "<span style='color:#16a34a'>✓ Done</span>"
            progress_bar.value = 100
        except Exception as exc:
            log.error("sidebar: failed to register completed run: %s", exc)
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

        import backtester.config as _bcfg
        account_size = float(_bcfg.cfg.simulation.account_size_usd)

        try:
            handle = run_service.submit(
                strategy_key=strategy_select.value,
                param_grid=param_grid,
                date_range=(f_str, t_str),
                account_size=account_size,
            )
        except Exception as exc:
            status_label.object = f"<span style='color:#dc2626'>⚠ Failed to start: {exc}</span>"
            log.error("sidebar: submit failed: %s", exc)
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
                if "current" in line and "total" in line:
                    total = line["total"]
                    current = line["current"]
                    if total > 0:
                        progress_bar.value = int(100 * current / total)
                    if line.get("date"):
                        progress_label.object = f"Processing {line['date']}"
                elif line.get("status") == "done":
                    _on_run_done(line)
                    return
                elif line.get("status") in ("error", "cancelled"):
                    _on_run_ended(line)
                    return
            # Process exited without a final status line
            if not h.is_alive():
                remaining = list(run_service.tail_progress(h))
                final = next((l for l in reversed(remaining) if "status" in l), None)
                if final:
                    if final.get("status") == "done":
                        _on_run_done(final)
                    else:
                        _on_run_ended(final)
                else:
                    _on_run_ended({"status": "error", "message": "worker exited unexpectedly"})

        cb = pn.state.add_periodic_callback(_poll, period=500)
        _cb_handle["cb"] = cb

    run_btn.on_click(_on_run)

    # ── Cancel handler ─────────────────────────────────────────────────────────
    def _on_cancel(event):
        h = _cb_handle.get("handle") or state.active_run_handle
        if h:
            run_service.cancel(h)
        cancel_btn.disabled = True

    cancel_btn.on_click(_on_cancel)

    # ── Rerun prefill (Phase 4) — Favourites "Re-run" button ──────────────────
    def _on_rerun_request(event):
        req = event.new
        if req is None:
            return
        strat = req.get("strategy")
        pg = req.get("param_grid", {})
        if strat and strat in strategy_select.options:
            strategy_select.value = strat
            # _on_strategy_change will fire, loading fresh param inputs.
            # Override each known param with the single value from pg.
            for pname, ti in _param_inputs.items():
                if pname in pg:
                    vals = pg[pname]
                    ti.value = _csv_from_values(vals)
        state.rerun_request = None

    state.param.watch(_on_rerun_request, ["rerun_request"])

    # ── Prune runs (Phase 5) ───────────────────────────────────────────────────
    prune_toggle = pn.widgets.Toggle(
        name="⚙ Prune runs", button_type="warning",
        sizing_mode="stretch_width", margin=(4, 4),
    )
    prune_days_input = pn.widgets.IntInput(
        name="Delete unpinned runs older than (days)",
        value=30, start=1,
        sizing_mode="stretch_width", margin=(2, 4),
    )
    prune_preview_btn = pn.widgets.Button(
        name="Preview", button_type="warning", width=90, margin=(4, 4),
    )
    prune_confirm_btn = pn.widgets.Button(
        name="✓ Delete", button_type="danger",
        disabled=True, width=90, margin=(4, 4),
    )
    prune_output = pn.pane.HTML(
        "", sizing_mode="stretch_width",
        styles={"font-size": "11px"},
    )
    prune_inner = pn.Column(
        prune_days_input,
        pn.Row(prune_preview_btn, prune_confirm_btn),
        prune_output,
        visible=False,
        sizing_mode="stretch_width",
    )
    prune_toggle.param.watch(
        lambda e: setattr(prune_inner, "visible", e.new), "value"
    )

    def _on_prune_preview(event):
        days = prune_days_input.value
        to_prune = store.prune_runs(days, dry_run=True)
        if not to_prune:
            prune_output.object = "<span style='color:#16a34a'>Nothing to prune.</span>"
            prune_confirm_btn.disabled = True
            return
        lines = [f"<b>Would delete {len(to_prune)} unpinned run(s):</b>"]
        for rr in to_prune[:8]:
            ts = (rr.created_at or "")[:16].replace("T", " ")
            lines.append(f"&nbsp;&nbsp;#{rr.id}&nbsp;{rr.strategy}&nbsp;{ts}")
        if len(to_prune) > 8:
            lines.append(f"&nbsp;&nbsp;…and {len(to_prune) - 8} more")
        prune_output.object = "<br>".join(lines)
        prune_confirm_btn.disabled = False

    def _on_prune_confirm(event):
        days = prune_days_input.value
        pruned = store.prune_runs(days, dry_run=False)
        prune_output.object = (
            f"<span style='color:#16a34a'>Deleted {len(pruned)} run(s).</span>"
        )
        prune_confirm_btn.disabled = True
        _refresh_runs()

    prune_preview_btn.on_click(_on_prune_preview)
    prune_confirm_btn.on_click(_on_prune_confirm)

    prune_section = pn.Column(
        pn.pane.HTML("<hr style='margin:8px 0;border-color:#ccc'>"),
        prune_toggle,
        prune_inner,
        sizing_mode="stretch_width",
    )

    # ── Assemble ───────────────────────────────────────────────────────────────
    run_control = pn.Column(
        pn.pane.HTML("<hr style='margin:8px 0;border-color:#ccc'>"),
        pn.pane.Markdown("## New Run", margin=(8, 4)),
        strategy_select,
        pn.pane.Markdown("**Parameters** (CSV or range `start..end:step` per row):", margin=(4, 4)),
        param_editor_col,
        date_from_input,
        date_to_input,
        date_error,
        run_btn,
        cancel_btn,
        progress_bar,
        progress_label,
        status_label,
        sizing_mode="stretch_width",
    )

    return pn.Column(runs_section, run_control, prune_section, sizing_mode="stretch_width")
