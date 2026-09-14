"""Configuration page + hot-reload of backtester.core.config."""
from __future__ import annotations

from pathlib import Path

import panel as pn
import pytest


def test_configuration_view_matches_mockup_chrome():
    from backtester.ui.views.configuration_view import build_configuration_view

    view = build_configuration_view()
    html_bits = " ".join(
        str(getattr(p, "object", "") or "") for p in view.select(pn.pane.HTML)
    )
    assert "Configuration" in html_bits
    assert 'class="ca-label">Path</span>' in html_bits
    assert "backtester/core/config.toml" in html_bits
    assert "ca-badge-ok" in html_bits
    assert "live" in html_bits
    # No invented toolbar / hint copy
    assert "Application config" not in html_bits
    assert "PARAM_GRID stays" not in html_bits

    areas = list(view.select(pn.widgets.TextAreaInput))
    assert areas
    assert "[simulation]" in (areas[0].value or "")
    assert "ca-config-editor" in (areas[0].css_classes or [])
    sheets = "\n".join(str(s) for s in (areas[0].stylesheets or []))
    assert "100vh" in sheets
    assert "SF Mono" in sheets or "ca-font-data" in sheets

    buttons = {b.name: b for b in view.select(pn.widgets.Button)}
    assert set(buttons) >= {"OK", "Cancel", "Revert"}
    assert buttons["OK"].disabled is True


def test_brand_css_config_editor_viewport_height():
    from backtester.ui.brand import load_research_css

    css = load_research_css()
    assert "ca-config-editor" in css
    assert "var(--ca-font-data)" in css
    assert "100vh" in css


def test_apply_config_text_hot_reloads(tmp_path):
    from backtester.core import config as cfgmod
    from backtester.core import pricing

    src = Path(cfgmod.config_path()).read_text(encoding="utf-8")
    dest = tmp_path / "config.toml"
    dest.write_text(src, encoding="utf-8")

    original = cfgmod.cfg.simulation.account_size_usd
    edited = src.replace(
        f"account_size_usd        = {int(original):_}",
        "account_size_usd        = 123_456",
        1,
    )
    if edited == src:
        edited = src.replace(
            f"account_size_usd        = {original}",
            "account_size_usd        = 123456.0",
            1,
        )
    assert edited != src

    cfg_id_before = id(cfgmod.cfg)
    live = cfgmod.apply_config_text(edited, path=str(dest))
    assert id(live) == cfg_id_before
    assert live.simulation.account_size_usd == 123456.0
    assert cfgmod.cfg.simulation.account_size_usd == 123456.0

    cfgmod.reload_config()
    assert cfgmod.cfg.simulation.account_size_usd == original
    pricing.refresh_config_constants()


def test_apply_config_text_rejects_invalid(tmp_path):
    from backtester.core import config as cfgmod

    dest = tmp_path / "config.toml"
    dest.write_text(Path(cfgmod.config_path()).read_text(encoding="utf-8"), encoding="utf-8")
    before = dest.read_text(encoding="utf-8")
    with pytest.raises(Exception):
        cfgmod.apply_config_text("not = valid = toml [[[", path=str(dest))
    assert dest.read_text(encoding="utf-8") == before


def test_build_app_includes_configuration_page(tmp_path):
    from backtester.ui.app import build_app

    app = build_app(
        state_dir=str(tmp_path / "state"),
        bundles_root=str(tmp_path / "bundles"),
    )
    assert "Configuration" in app._cryo_nav_pages
