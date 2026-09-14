"""Brand shell wiring — light cryo_aureas research UI."""
from __future__ import annotations

from pathlib import Path

import panel as pn


def test_brand_assets_exist():
    from backtester.ui import brand

    assert brand.icon_path().is_file()
    css = brand.load_research_css()
    assert "--ca-navy-900" in css
    assert "--ca-gold" in css
    assert ".bk-btn-primary" in css
    assert brand.NAVY_900 == "#0d1b2a"
    assert brand.GOLD == "#c8a84b"


def test_build_app_applies_brand(tmp_path):
    from backtester.ui.app import build_app
    from backtester.ui.brand import GOLD, NAVY_900, load_research_css

    app = build_app(
        state_dir=str(tmp_path / "state"),
        bundles_root=str(tmp_path / "bundles"),
    )
    assert isinstance(app, pn.template.VanillaTemplate)
    assert app.title == "CryoBacktester"
    assert app.header_background == NAVY_900
    assert app.header_color == "#ffffff"
    assert Path(app.logo).name == "backtester.png"
    assert Path(app.logo).is_file()
    css = load_research_css()
    assert css in app.config.raw_css
    # Shadow-DOM stamp: selection bar / page widgets carry brand CSS
    main = app.main[0]
    stamped = [
        obj for obj in main.select(lambda o: hasattr(o, "stylesheets"))
        if css in (list(getattr(obj, "stylesheets", None) or []))
    ]
    assert stamped, "expected brand CSS stamped onto main-area widgets"
    # Nav still amber-free (gold active, not Tailwind amber)
    from backtester.ui.views import chrome

    assert "#c8a84b" in chrome._NAV_CSS
    assert "#f59e0b" not in chrome._NAV_CSS
    assert GOLD == "#c8a84b"


def test_detail_bar_uses_brand_selection_markup(tmp_path):
    from backtester.ui.state import AppState
    from backtester.ui.services.store_service import StoreService
    from backtester.ui.views.chrome import build_detail_bar

    store = StoreService(tmp_path / "state", tmp_path / "bundles")
    state = AppState()
    bar = build_detail_bar(state, store)
    html_panes = list(bar.select(pn.pane.HTML))
    assert html_panes
    html = html_panes[0].object
    assert "ca-selection-bar" in html
    assert "Selected Run:" in html
    assert "#f8fafc" not in html
    assert "#6b7280" not in html
    assert bar.height == 40
    assert bar.margin == (0, 0) or bar.margin == 0
    assert "ca-selection-row" in (bar.css_classes or [])


def test_header_chrome_padding_matches_specimen():
    """Panel #header padding must match .ca-shell-header (8px 24px)."""
    from backtester.ui.brand import load_research_css

    css = load_research_css()
    assert "padding: var(--ca-space-sm) var(--ca-space-xl)" in css
    assert "#main" in css
    assert "padding: 0 !important" in css
    assert "min-height: 40px" in css  # selection bar
    # Nav cluster pushed right of title (specimen .ca-nav)
    assert "margin: 0 0 0 auto" in css
    # Old looser header pad must be gone
    assert "padding: 6px 16px" not in css


def test_nav_has_no_extra_vertical_margin():
    from backtester.ui.state import AppState
    from backtester.ui.views.chrome import build_nav

    nav = build_nav(AppState())
    assert nav.margin == (0, 0) or nav.margin == 0
    assert nav.height == 32


def test_page_holder_has_content_gutter(tmp_path):
    """Main content keeps specimen left/right pad; selection bar stays flush."""
    from backtester.ui.app import build_app

    app = build_app(
        state_dir=str(tmp_path / "state"),
        bundles_root=str(tmp_path / "bundles"),
    )
    main = app.main[0]
    assert len(main) >= 2
    page_holder = main[1]
    pad = (page_holder.styles or {}).get("padding", "")
    assert "24px" in pad
    # Selection bar (first child) must not inherit that gutter
    detail = main[0]
    assert getattr(detail, "height", None) == 40
    assert detail.margin == (0, 0) or detail.margin == 0
