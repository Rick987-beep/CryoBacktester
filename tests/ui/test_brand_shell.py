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
