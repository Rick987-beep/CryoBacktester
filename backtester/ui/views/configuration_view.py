"""
views/configuration_view.py — Editable application config.toml page.

Layout matches ``brand/mockups/configuration.html`` (signed-off).
OK validates + writes config.toml and hot-reloads ``backtester.core.config.cfg``.
"""
from __future__ import annotations

import html

import panel as pn

from backtester.ui.brand import load_research_css
from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)

# Page chrome — mirrors brand/mockups/configuration.html (no invented lines).
# Editor height is viewport-relative; only the textarea host uses calc (not the page column).
_PAGE_CSS = """
.ca-config-page-head {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: space-between;
  gap: 12px;
  margin: 0 0 12px 0;
}
.ca-config-page-head h2 {
  margin: 0;
  font-size: var(--ca-fs-title, 20px);
  font-weight: 600;
  color: var(--ca-navy-900, #0d1b2a);
  font-family: var(--ca-font-ui, "Segoe UI", system-ui, -apple-system, sans-serif);
  line-height: 1.2;
}
.ca-config-path-row {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 8px 16px;
  margin: 0 0 16px 0;
}
.ca-config-footer {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-top: 16px;
}
.ca-config-footer .status {
  font-size: var(--ca-fs-label, 12px);
  font-family: var(--ca-font-ui, "Segoe UI", system-ui, -apple-system, sans-serif);
  color: var(--ca-muted, #8a9ab0);
}
.ca-config-footer .status.is-dirty {
  color: var(--ca-navy-900, #0d1b2a);
  font-weight: 600;
}
.ca-config-footer .status.is-ok {
  color: #1a7a45;
  font-weight: 600;
}
.ca-config-footer .status.is-err {
  color: var(--ca-negative, #e74c3c);
  font-weight: 600;
}
"""

_EDITOR_CSS = """
/* Viewport fill — applied only to the TextAreaInput host (not the page column). */
:host {
  display: block !important;
  width: 100% !important;
  /* header~56 + selection~40 + page pad~32 + title~36 + path~40 + footer~56 + gaps */
  height: calc(100vh - 260px) !important;
  min-height: 320px !important;
  max-height: none !important;
  box-sizing: border-box !important;
  border: 1px solid rgba(38, 51, 71, 0.18) !important;
  border-radius: 4px !important;
  background: #ffffff !important;
  overflow: hidden !important;
}
:host .bk-input-group {
  height: 100% !important;
  margin: 0 !important;
  display: flex !important;
  flex-direction: column !important;
}
:host textarea.bk-input,
:host textarea {
  flex: 1 1 auto !important;
  height: 100% !important;
  min-height: 0 !important;
  width: 100% !important;
  box-sizing: border-box !important;
  margin: 0 !important;
  padding: 14px 16px !important;
  border: none !important;
  border-radius: 0 !important;
  resize: none !important;
  outline: none !important;
  box-shadow: none !important;
  font-family: var(--ca-font-data, "SF Mono", "Fira Code", "Cascadia Code", ui-monospace, monospace) !important;
  font-size: var(--ca-fs-data, 13px) !important;
  line-height: 1.5 !important;
  white-space: pre !important;
  tab-size: 2;
  color: var(--ca-navy-900, #0d1b2a) !important;
  background: #ffffff !important;
}
:host textarea.bk-input:focus,
:host textarea:focus {
  box-shadow: inset 0 0 0 2px rgba(30, 111, 191, 0.25) !important;
  border: none !important;
}
"""

_REVERT_CSS = """
:host .bk-btn {
  background: transparent !important;
  color: var(--ca-blue, #1e6fbf) !important;
  border: none !important;
  box-shadow: none !important;
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif !important;
  font-size: 14px !important;
  font-weight: 600 !important;
  padding: 6px 10px !important;
}
:host .bk-btn:hover:not([disabled]) {
  color: var(--ca-blue-400, #2e8fd9) !important;
  text-decoration: underline !important;
}
:host .bk-btn[disabled] {
  opacity: 0.45 !important;
}
"""


def build_configuration_view() -> pn.Column:
    """Build the Configuration page — layout locked to brand/mockups/configuration.html."""
    from backtester.core.config import (
        apply_config_text,
        config_display_path,
        read_config_text,
    )

    brand_css = load_research_css()

    try:
        initial = read_config_text()
    except Exception as exc:
        log.error("configuration_view: failed to read config.toml: %s", exc)
        initial = f"# Failed to read config.toml: {exc}\n"

    baseline = {"text": initial}
    path_disp = html.escape(config_display_path())

    title_html = pn.pane.HTML(
        '<div class="ca-config-page-head"><h2>Configuration</h2></div>',
        margin=(0, 0),
        sizing_mode="stretch_width",
        stylesheets=[brand_css, _PAGE_CSS],
    )
    revert_btn = pn.widgets.Button(
        name="Revert",
        button_type="light",
        width=80,
        height=32,
        disabled=True,
        margin=(0, 0),
        stylesheets=[_REVERT_CSS],
    )
    head = pn.Row(
        title_html,
        pn.Spacer(),
        revert_btn,
        sizing_mode="stretch_width",
        margin=(0, 0, 0, 0),
        align="end",
    )

    def _path_html(badge_class: str, badge_text: str) -> str:
        return (
            '<div class="ca-config-path-row">'
            '<span class="ca-label">Path</span>'
            f'<span class="ca-data">{path_disp}</span>'
            f'<span class="ca-badge {badge_class}">{html.escape(badge_text)}</span>'
            "</div>"
        )

    path_row = pn.pane.HTML(
        _path_html("ca-badge-ok", "live"),
        margin=(0, 0),
        sizing_mode="stretch_width",
        stylesheets=[brand_css, _PAGE_CSS],
    )

    editor = pn.widgets.TextAreaInput(
        name="",
        value=initial,
        sizing_mode="stretch_width",
        # Bokeh still wants a height; CSS calc overrides at paint time.
        height=640,
        max_length=500_000,
        margin=(0, 0),
        css_classes=["ca-config-editor"],
        stylesheets=[brand_css, _EDITOR_CSS],
    )

    status_text = pn.pane.HTML(
        '<span class="status">Loaded · no unsaved changes</span>',
        margin=(0, 0),
        sizing_mode="stretch_width",
        stylesheets=[brand_css, _PAGE_CSS],
    )
    cancel_btn = pn.widgets.Button(
        name="Cancel",
        button_type="default",
        width=100,
        height=34,
        margin=(0, 4, 0, 0),
    )
    ok_btn = pn.widgets.Button(
        name="OK",
        button_type="primary",
        width=100,
        height=34,
        disabled=True,
        margin=(0, 0),
    )
    footer = pn.Row(
        status_text,
        pn.Spacer(),
        cancel_btn,
        ok_btn,
        sizing_mode="stretch_width",
        margin=(16, 0, 0, 0),
        css_classes=["ca-config-footer"],
        stylesheets=[brand_css, _PAGE_CSS],
    )

    def _set_dirty(dirty: bool) -> None:
        ok_btn.disabled = not dirty
        revert_btn.disabled = not dirty
        if dirty:
            path_row.object = _path_html("ca-badge-muted", "edited")
            status_text.object = (
                '<span class="status is-dirty">'
                "Unsaved edits · OK applies to disk and reloads live config"
                "</span>"
            )
        else:
            path_row.object = _path_html("ca-badge-ok", "live")
            status_text.object = (
                '<span class="status">Loaded · no unsaved changes</span>'
            )

    def _on_edit(event=None) -> None:
        _set_dirty(editor.value != baseline["text"])

    def _on_ok(event=None) -> None:
        text = editor.value if editor.value is not None else ""
        try:
            apply_config_text(text)
        except Exception as exc:
            log.warning("configuration_view: apply failed: %s", exc)
            msg = html.escape(str(exc))
            status_text.object = (
                f'<span class="status is-err">Invalid config · {msg}</span>'
            )
            return
        baseline["text"] = text
        _set_dirty(False)
        status_text.object = (
            '<span class="status is-ok">'
            "Saved · config reloaded · live for new runs</span>"
        )
        log.info("configuration_view: config.toml applied and reloaded")

    def _on_cancel(event=None) -> None:
        editor.value = baseline["text"]
        _set_dirty(False)

    def _on_revert(event=None) -> None:
        editor.value = baseline["text"]
        _set_dirty(False)

    editor.param.watch(_on_edit, "value")
    ok_btn.on_click(_on_ok)
    cancel_btn.on_click(_on_cancel)
    revert_btn.on_click(_on_revert)

    return pn.Column(
        head,
        path_row,
        editor,
        footer,
        sizing_mode="stretch_width",
        margin=(0, 0),
        stylesheets=[brand_css, _PAGE_CSS],
    )
