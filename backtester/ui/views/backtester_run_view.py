"""
views/backtester_run_view.py — Live job progress, queue, and job log.

Shown after **Run** on New Run (and on reconnect via ``adopt_in_flight``).
Cancel lives on the selection bar (chrome), not on this page. Multiple jobs
may be queued while one runs — this page lists them and tails ``job.log``.
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timezone

import panel as pn

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

_PHASE_LABELS = {
    "queued": "Queued",
    "starting": "Starting…",
    "loading_data": "Loading data",
    "building_indicators": "Building indicators",
    "backtesting": "Processing market dates",
    "scoring": "Scoring / write",
    "writing": "Writing results",
    "done": "Done",
}

_RE_INNER_WORKERS = re.compile(r"inner workers:\s*(\d+)", re.I)
_RE_MSG_WORKERS = re.compile(r"(\d+)\s+workers", re.I)
_RE_SPAWN_SHARDS = re.compile(r"spawn\s+(\d+)\s+shards", re.I)
_RE_COMBOS = re.compile(r"Running\s+(\d[\d,]*)\s+parameter combos", re.I)
_RE_SHARD_DONE = re.compile(r"\[grid-w(\d+)/(\d+)\]\s+done", re.I)
_RE_SHARD_DONE_ALT = re.compile(r"grid shard\s+(\d+)\s+done", re.I)

_PANEL_CARD_STYLE = (
    "padding:16px;border:1px solid rgba(38,51,71,0.22);border-radius:4px;"
    "background:#fff;min-width:0;box-sizing:border-box;"
    "height:36vh;min-height:260px;max-height:420px;"
    "display:flex;flex-direction:column"
)

_LOG_BOX_STYLE = (
    "margin:0;box-sizing:border-box;flex:1 1 auto;min-height:0;"
    "overflow-x:hidden;overflow-y:auto;padding:12px;"
    "background:#0d1b2a;border-radius:4px;"
    "font-family:SF Mono,Fira Code,monospace;font-size:12px;line-height:1.55;"
    "white-space:pre-wrap;word-break:break-word"
)


def _esc(text: str) -> str:
    return html.escape(str(text), quote=True)


def _parse_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    raw = ts.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_elapsed(started: datetime | None) -> str:
    if started is None:
        return "—"
    secs = max(0, int((datetime.now(timezone.utc) - started).total_seconds()))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _progress_pct(phase: str | None, current: int | None, total: int | None) -> int:
    if phase == "queued":
        return 1
    if phase in ("starting", "loading_data"):
        return 3
    if phase == "building_indicators":
        return 8
    if phase in ("scoring", "writing", "done"):
        return 100 if phase == "done" else 95
    if current is not None and total and total > 0:
        return 12 + int(88 * current / total)
    if phase == "backtesting":
        return 12
    return 0


def _phase_label(phase: str | None, *, queued: bool) -> str:
    if queued or phase == "queued":
        return "Queued"
    if phase and phase in _PHASE_LABELS:
        return _PHASE_LABELS[phase]
    return (phase or "Running").replace("_", " ").capitalize()


def _status_kind(state: str | None, *, queued: bool) -> str:
    if queued or state == "queued":
        return "queued"
    if state in ("error", "dead"):
        return "error"
    if state == "cancelled":
        return "cancelled"
    if state == "done":
        return "done"
    return "running"


def _colorize_log_line(line: str) -> str:
    safe = _esc(line)
    upper = line.upper()
    if "ERROR" in upper or "TRACEBACK" in upper:
        return f'<span style="color:#e74c3c">{safe}</span>'
    if "WARN" in upper:
        return f'<span style="color:#c8a84b">{safe}</span>'
    if "INFO" in upper:
        return f'<span style="color:#2ecc71">{safe}</span>'
    return safe


def _parse_parallelism(view, log_text: str) -> dict:
    """Workers / shards / combos from status fields, then job.log fallback."""
    workers = None
    n_shards = None
    n_combos = None
    shards_done = None

    if view is not None:
        if getattr(view, "inner_workers_effective", None) is not None:
            workers = int(view.inner_workers_effective)
        if getattr(view, "n_shards", None) is not None:
            n_shards = int(view.n_shards)
        msg = getattr(view, "message", None) or ""
        m = _RE_MSG_WORKERS.search(msg)
        if m and workers is None:
            workers = int(m.group(1))
        spec = getattr(view, "spec", None)
        req = getattr(spec, "requested_inner_workers", None) if spec else None
        if workers is None and req is not None:
            workers = int(req)

    text = log_text or ""
    if workers is None:
        m = _RE_INNER_WORKERS.search(text)
        if m:
            workers = int(m.group(1))
    if n_shards is None:
        m = _RE_SPAWN_SHARDS.search(text)
        if m:
            n_shards = int(m.group(1))
    if n_shards is None and workers is not None and workers > 1:
        n_shards = workers
    if n_combos is None:
        m = _RE_COMBOS.search(text)
        if m:
            n_combos = int(m.group(1).replace(",", ""))

    done_ids: set[int] = set()
    for m in _RE_SHARD_DONE.finditer(text):
        done_ids.add(int(m.group(1)))
        n_shards = n_shards or int(m.group(2))
    for m in _RE_SHARD_DONE_ALT.finditer(text):
        done_ids.add(int(m.group(1)))
    if done_ids:
        shards_done = len(done_ids)

    return {
        "workers": workers,
        "n_shards": n_shards,
        "n_combos": n_combos,
        "shards_done": shards_done,
    }


def build_backtester_run_view(state, store, cache, run_service) -> pn.Column:
    """Build the Backtester Run page (live progress + queue + log)."""

    body = pn.pane.HTML("", sizing_mode="stretch_width", margin=(0, 4))

    _watch: dict = {"cb": None, "handle": None, "pct": 0, "msg": "", "ended": None}

    def _stop_cb():
        cb = _watch.get("cb")
        if cb:
            try:
                cb.stop()
            except Exception:
                pass
        _watch["cb"] = None

    def _job_view(handle):
        if handle is None:
            return None
        getter = getattr(handle, "_job_view", None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                return None
        return None

    def _queue_rows_html() -> str:
        try:
            snap = run_service.queue_snapshot()
        except Exception as exc:
            log.debug("backtester_run: snapshot failed: %s", exc)
            return (
                "<p style='font-size:13px;color:#8a9ab0;margin:0'>"
                "Queue unavailable (jobd offline?).</p>"
            )
        rows = list(snap.running) + list(snap.queued)
        if not rows:
            return (
                "<p style='font-size:13px;color:#8a9ab0;margin:0'>"
                "Queue is empty.</p>"
            )
        parts = [
            '<table style="width:100%;border-collapse:collapse;font-size:13px;'
            'font-family:Segoe UI,system-ui,sans-serif">',
            "<thead><tr>"
            '<th style="text-align:left;font-size:12px;color:#8a9ab0;padding:0 8px 8px 0;'
            'border-bottom:1px solid rgba(38,51,71,0.18)">#</th>'
            '<th style="text-align:left;font-size:12px;color:#8a9ab0;padding:0 8px 8px 0;'
            'border-bottom:1px solid rgba(38,51,71,0.18)">Job</th>'
            '<th style="text-align:left;font-size:12px;color:#8a9ab0;padding:0 8px 8px 0;'
            'border-bottom:1px solid rgba(38,51,71,0.18)">Strategy</th>'
            '<th style="text-align:left;font-size:12px;color:#8a9ab0;padding:0 8px 8px 0;'
            'border-bottom:1px solid rgba(38,51,71,0.18)">Status</th>'
            "</tr></thead><tbody>",
        ]
        for i, view in enumerate(rows, start=1):
            st = view.state
            if st == "running":
                badge = (
                    '<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
                    "font-size:11px;font-weight:600;background:rgba(46,204,113,0.12);"
                    'color:#1e7a4a;border:1px solid rgba(46,204,113,0.45)">running</span>'
                )
            else:
                badge = (
                    '<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
                    "font-size:11px;font-weight:600;background:rgba(30,111,191,0.12);"
                    'color:#1e6fbf;border:1px solid rgba(30,111,191,0.35)">queued</span>'
                )
            strat = getattr(view.spec, "strategy", "—") if view.spec else "—"
            parts.append(
                "<tr>"
                f'<td style="padding:10px 8px 10px 0;border-bottom:1px solid '
                f'rgba(38,51,71,0.10)">{i}</td>'
                f'<td style="padding:10px 8px 10px 0;border-bottom:1px solid '
                f'rgba(38,51,71,0.10);font-family:SF Mono,Fira Code,monospace;'
                f'font-size:12px">{_esc(view.job_id)}</td>'
                f'<td style="padding:10px 8px 10px 0;border-bottom:1px solid '
                f'rgba(38,51,71,0.10);font-family:SF Mono,Fira Code,monospace;'
                f'font-size:12px">{_esc(strat)}</td>'
                f'<td style="padding:10px 8px 10px 0;border-bottom:1px solid '
                f'rgba(38,51,71,0.10)">{badge}</td>'
                "</tr>"
            )
        parts.append("</tbody></table>")
        parts.append(
            '<p style="font-size:13px;color:#8a9ab0;margin:12px 0 0">'
            "jobd processes one active job; queued items wait in order.</p>"
        )
        return "".join(parts)

    def _log_html(job_id: str | None, ended_msg: str | None, log_text: str = "") -> str:
        lines: list[str] = []
        if ended_msg:
            lines.append(ended_msg)
        if log_text.strip():
            lines.extend(log_text.strip().splitlines()[-40:])
        if job_id:
            err_view = None
            try:
                err_view = run_service._client.store.get(job_id)
            except Exception:
                err_view = None
            if err_view and err_view.error and err_view.error not in "\n".join(lines):
                lines.append(f"ERROR {err_view.error}")
        if not lines:
            return (
                f'<pre style="{_LOG_BOX_STYLE};color:#8a9ab0">No log output yet.</pre>'
            )
        colored = "<br>".join(_colorize_log_line(ln) for ln in lines)
        return f'<pre style="{_LOG_BOX_STYLE};color:#f5f7fa">{colored}</pre>'

    def _idle_html() -> str:
        return (
            '<div style="padding:8px 0">'
            '<h2 style="margin:0 0 12px;font-size:20px;font-weight:600;color:#0d1b2a;'
            'font-family:Segoe UI,system-ui,sans-serif">Backtester Run</h2>'
            '<div style="padding:16px;border:1px solid rgba(38,51,71,0.22);'
            'border-radius:4px;background:#fff">'
            '<p style="margin:0 0 8px;font-size:14px;font-weight:600;color:#1a1a2e;'
            'font-family:Segoe UI,system-ui,sans-serif">No active backtest</p>'
            '<p style="margin:0;font-size:13px;color:#8a9ab0;'
            'font-family:Segoe UI,system-ui,sans-serif">'
            "Queue is empty. Start a job from <b>New Run</b> "
            "(this page updates automatically).</p>"
            "</div>"
            '<div style="margin-top:16px;padding:16px;border:1px solid rgba(38,51,71,0.22);'
            'border-radius:4px;background:#fff">'
            '<h3 style="margin:0 0 10px;font-size:14px;font-weight:600;color:#1a1a2e;'
            'font-family:Segoe UI,system-ui,sans-serif">Queue</h3>'
            f"{_queue_rows_html()}"
            "</div>"
            "</div>"
        )

    def _active_html(handle, *, reconnect: bool = False) -> str:
        view = _job_view(handle)
        job_id = getattr(handle, "job_id", None) or (view.job_id if view else "—")
        queued = bool(getattr(handle, "is_queued", lambda: False)())
        state_name = view.state if view else ("queued" if queued else "running")
        phase = view.phase if view else ("queued" if queued else None)
        if queued:
            phase = "queued"
        kind = _status_kind(state_name, queued=queued)
        if _watch.get("ended"):
            kind = _watch["ended"].get("kind", kind)

        pill_map = {
            "running": ("RUNNING", "#1e7a4a", "rgba(46,204,113,0.12)", "rgba(46,204,113,0.45)"),
            "queued": ("QUEUED", "#1e6fbf", "rgba(30,111,191,0.12)", "rgba(30,111,191,0.35)"),
            "done": ("DONE", "#0d1b2a", "#f0fdf4", "rgba(61,122,106,0.45)"),
            "error": ("ERROR", "#e74c3c", "rgba(231,76,60,0.12)", "rgba(231,76,60,0.45)"),
            "cancelled": ("CANCELLED", "#c8a84b", "rgba(200,168,75,0.18)", "rgba(200,168,75,0.55)"),
        }
        pill_label, pill_fg, pill_bg, pill_bd = pill_map.get(
            kind, pill_map["running"]
        )

        spec = view.spec if view else None
        strategy = getattr(spec, "strategy", "—") if spec else "—"
        d0 = getattr(spec, "date_from", None) if spec else None
        d1 = getattr(spec, "date_to", None) if spec else None
        date_range = (
            f"{d0 or '…'} → {d1 or '…'}" if (d0 or d1) else "—"
        )
        workers_meta = (
            view.inner_workers_effective
            if view and view.inner_workers_effective is not None
            else None
        )
        started = _parse_utc(view.submitted_at if view else None)
        elapsed = _fmt_elapsed(started)
        started_txt = (
            started.strftime("%Y-%m-%d %H:%M:%S UTC") if started else "—"
        )

        current = view.current if view else None
        total = view.total if view else None
        day = view.date if view else None
        pct = _watch.get("pct") or _progress_pct(phase, current, total)
        if kind == "done":
            pct = 100

        job_id_str = job_id if isinstance(job_id, str) else None
        log_text = run_service.job_log_tail(job_id_str) if job_id_str else ""
        parallel = _parse_parallelism(view, log_text)
        workers = parallel["workers"] if parallel["workers"] is not None else workers_meta
        n_shards = parallel["n_shards"]
        n_combos = parallel["n_combos"]
        shards_done = parallel["shards_done"]

        status_msg = getattr(view, "message", None) if view else None
        phase_txt = (
            _watch.get("msg")
            or status_msg
            or _phase_label(phase, queued=queued)
        )
        if reconnect and kind == "running" and not phase_txt:
            phase_txt = "Reconnected — job still running…"

        sub_bits = []
        day_str = str(day) if day else ""
        # Avoid "Processing 2026-05-09 · date 2026-05-09"
        if day_str and day_str not in str(phase_txt):
            sub_bits.append(f"date {_esc(day_str)}")
        if current is not None and total:
            sub_bits.append(f"{current:,} / {total:,} days")
        if workers is not None:
            sub_bits.append(f"{int(workers)} workers")
        if n_shards is not None and int(n_shards) > 1:
            if shards_done is not None:
                sub_bits.append(
                    f"combo shard {_esc(shards_done)} / {_esc(n_shards)} done"
                )
            else:
                sub_bits.append(f"{int(n_shards)} combo shards")
        detail = " · ".join(sub_bits)

        status_line = _esc(phase_txt)
        if detail:
            status_line = f"{status_line} · {detail}"

        grid_parts = []
        if n_combos is not None:
            grid_parts.append(f"{n_combos:,} combos")
        elif total:
            grid_parts.append(f"{total:,} days")
        if workers is not None:
            grid_parts.append(f"{int(workers)} workers")
        grid = " · ".join(grid_parts) if grid_parts else "—"

        meta = [
            ("Job", job_id),
            ("Strategy", strategy),
            ("Date range", date_range),
            ("Grid", grid),
            ("Started", started_txt),
            ("Elapsed", elapsed),
        ]
        meta_html = []
        for k, v in meta:
            meta_html.append(
                f'<div><span style="display:block;font-size:12px;font-weight:600;'
                f'color:#8a9ab0;margin-bottom:2px;font-family:Segoe UI,system-ui,'
                f'sans-serif">{_esc(k)}</span>'
                f'<span style="font-size:14px;color:#1a1a2e;font-family:SF Mono,'
                f'Fira Code,monospace">{_esc(v)}</span></div>'
            )

        ended = _watch.get("ended")
        ended_line = None
        if ended:
            ended_line = f"{ended.get('level', 'INFO')} {ended.get('message', '')}"

        return (
            '<div style="padding:8px 0">'
            '<div style="display:flex;flex-wrap:wrap;align-items:flex-end;'
            'justify-content:space-between;gap:12px;margin-bottom:16px">'
            "<div>"
            '<h2 style="margin:0;font-size:20px;font-weight:600;color:#0d1b2a;'
            'font-family:Segoe UI,system-ui,sans-serif">Backtester Run</h2>'
            f'<div style="margin-top:8px"><span style="display:inline-flex;'
            f"align-items:center;gap:6px;padding:4px 10px;border-radius:4px;"
            f"font-size:12px;font-weight:600;letter-spacing:0.02em;"
            f'font-family:Segoe UI,system-ui,sans-serif;text-transform:uppercase;'
            f"background:{pill_bg};color:{pill_fg};border:1px solid {pill_bd}\">"
            f"{pill_label}</span></div>"
            "</div></div>"
            '<div style="padding:16px;border:1px solid rgba(38,51,71,0.22);'
            'border-radius:4px;background:#fff;margin-bottom:16px">'
            '<div style="display:grid;grid-template-columns:repeat(auto-fill,'
            f'minmax(160px,1fr));gap:12px 20px;margin-bottom:20px">'
            f"{''.join(meta_html)}</div>"
            '<div style="display:flex;justify-content:flex-end;'
            'align-items:baseline;margin-bottom:8px">'
            f'<span style="font-size:14px;font-weight:600;font-family:SF Mono,'
            f'Fira Code,monospace;color:#0d1b2a">{int(pct)}%</span></div>'
            '<div style="height:14px;border-radius:4px;background:rgba(38,51,71,0.12);'
            'overflow:hidden;border:1px solid rgba(38,51,71,0.18)">'
            f'<div style="height:100%;width:{int(pct)}%;background:linear-gradient('
            '90deg,#0d1b2a,#3d7a6a)"></div></div>'
            f'<p style="margin:12px 0 0;font-size:14px;color:#1a1a2e;'
            f'font-family:Segoe UI,system-ui,sans-serif">'
            f'<span style="font-weight:600;color:#8a9ab0">Status:</span> '
            f"{status_line}</p>"
            "</div>"
            '<div style="display:grid;grid-template-columns:1.2fr 1fr;gap:16px;'
            'align-items:stretch">'
            f'<div style="{_PANEL_CARD_STYLE}">'
            '<h3 style="margin:0 0 10px;flex:0 0 auto;font-size:14px;font-weight:600;'
            'color:#1a1a2e;font-family:Segoe UI,system-ui,sans-serif">Queue</h3>'
            '<div style="flex:1 1 auto;min-height:0;overflow:auto">'
            f"{_queue_rows_html()}"
            "</div></div>"
            f'<div style="{_PANEL_CARD_STYLE}">'
            '<h3 style="margin:0 0 10px;flex:0 0 auto;font-size:14px;font-weight:600;'
            'color:#1a1a2e;font-family:Segoe UI,system-ui,sans-serif">Log</h3>'
            f"{_log_html(job_id_str, ended_line, log_text)}"
            "</div></div></div>"
        )

    def _refresh_ui(*, reconnect: bool = False):
        handle = state.active_run_handle
        if handle is None:
            body.object = _idle_html()
            return
        body.object = _active_html(handle, reconnect=reconnect)

    def _on_run_done(line):
        _stop_cb()
        _watch["pct"] = 100
        _watch["ended"] = {
            "kind": "done",
            "level": "INFO",
            "message": "Job finished successfully",
        }
        bundle_path = line.get("bundle_path")
        try:
            run_id = store.register_bundle(bundle_path)
            cache.get(run_id)
            state.active_run_id = run_id
            state.active_run_handle = None
            _watch["handle"] = None
            _watch["ended"] = None
            _watch["pct"] = 0
            _watch["msg"] = ""
            state.active_tab = "Results Grid"
        except Exception as exc:
            log.error("backtester_run: failed to register completed run: %s", exc)
            _watch["ended"] = {
                "kind": "error",
                "level": "ERROR",
                "message": f"Run done but load failed: {exc}",
            }
            state.active_run_handle = None
            _watch["handle"] = None
            _refresh_ui()

    def _on_run_ended(line):
        _stop_cb()
        status_code = line.get("status", "error")
        msg = line.get("message", "")
        if status_code == "cancelled":
            _watch["ended"] = {
                "kind": "cancelled",
                "level": "WARN",
                "message": "Cancelled.",
            }
        else:
            _watch["ended"] = {
                "kind": "error",
                "level": "ERROR",
                "message": msg or "job exited unexpectedly",
            }
        state.active_run_handle = None
        _watch["handle"] = None
        _refresh_ui()

    def _begin_watch(handle, *, reconnect: bool = False):
        _stop_cb()
        _watch["handle"] = handle
        _watch["pct"] = 0
        _watch["msg"] = ""
        _watch["ended"] = None
        if state.active_run_handle is not handle:
            state.active_run_handle = handle
        _refresh_ui(reconnect=reconnect)

        def _poll():
            h = _watch.get("handle")
            if h is None:
                return
            for line in run_service.tail_progress(h):
                if "phase" in line:
                    phase = line["phase"]
                    msg = line.get("msg", "")
                    _watch["msg"] = msg or _phase_label(phase, queued=(phase == "queued"))
                    _watch["pct"] = max(
                        _watch["pct"],
                        _progress_pct(phase, None, None),
                    )
                elif "current" in line and "total" in line:
                    total = line["total"]
                    current = line["current"]
                    _watch["pct"] = _progress_pct("backtesting", current, total)
                    if line.get("date"):
                        _watch["msg"] = f"Processing {line['date']}"
                elif line.get("status") == "done":
                    _on_run_done(line)
                    return
                elif line.get("status") in ("error", "cancelled"):
                    _on_run_ended(line)
                    return
            _refresh_ui()
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
                        "message": "job exited unexpectedly",
                    })

        cb = pn.state.add_periodic_callback(_poll, period=500)
        _watch["cb"] = cb

    def _on_handle_change(event):
        handle = event.new
        if handle is None:
            _stop_cb()
            _watch["handle"] = None
            _refresh_ui()
            return
        if handle is _watch.get("handle"):
            return
        _begin_watch(handle)

    state.param.watch(_on_handle_change, "active_run_handle")

    def _adopt_later():
        try:
            adopted = run_service.adopt_in_flight()
        except Exception as exc:
            log.debug("backtester_run: adopt_in_flight failed: %s", exc)
            return
        if adopted is not None:
            if state.active_run_handle is None:
                state.active_tab = "Backtester Run"
            _begin_watch(adopted, reconnect=True)

    try:
        pn.state.onload(_adopt_later)
    except Exception as exc:
        log.debug("backtester_run: onload adopt not available: %s", exc)

    if state.active_run_handle is not None:
        _begin_watch(state.active_run_handle)
    else:
        _refresh_ui()

    return pn.Column(
        body,
        sizing_mode="stretch_width",
    )
