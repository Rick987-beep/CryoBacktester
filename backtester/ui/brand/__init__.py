"""Cryo/Aureas light research-UI brand assets for the Panel shell.

Product copy of pack tokens / research_light.css / backtester icon.
Document ``raw_css`` alone does not pierce Bokeh 3 shadow roots — call
``stamp_brand_stylesheets`` on viewable roots after build.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

_BRAND_DIR = Path(__file__).resolve().parent
_CSS_PATH = _BRAND_DIR / "research_light.css"
_ICON_PATH = _BRAND_DIR / "icons" / "backtester.png"

# Pack primitives (cryo_aureas tokens.json) — do not invent hex.
NAVY_900 = "#0d1b2a"
NAVY_700 = "#1a2e44"
GOLD = "#c8a84b"
BLUE = "#1e6fbf"
PAGE = "#f5f7fa"
TEXT = "#1a1a2e"
MUTED = "#8a9ab0"
BORDER = "#263347"
POSITIVE = "#2ecc71"
NEGATIVE = "#e74c3c"
PRODUCT_BT = "#3d7a6a"


def icon_path() -> Path:
    return _ICON_PATH


@lru_cache(maxsize=1)
def load_research_css() -> str:
    return _CSS_PATH.read_text(encoding="utf-8")


def stamp_brand_stylesheets(root) -> int:
    """Inject brand CSS into Panel/Bokeh shadow roots (document raw_css is not enough)."""
    css = load_research_css()
    stamped = 0
    try:
        objs = root.select(lambda o: hasattr(o, "stylesheets"))
    except Exception:
        objs = [root] if hasattr(root, "stylesheets") else []
    for obj in objs:
        try:
            sheets = list(getattr(obj, "stylesheets", None) or [])
            if css in sheets:
                continue
            obj.stylesheets = sheets + [css]
            stamped += 1
        except Exception:
            continue
    return stamped


def apply_to_template(template) -> None:
    """Attach light research brand CSS + header chrome to a Panel template."""
    css = load_research_css()
    if css not in template.config.raw_css:
        template.config.raw_css.append(css)
    template.param.update(
        title="CryoBacktester",
        logo=str(icon_path()),
        header_background=NAVY_900,
        header_color="#ffffff",
    )
