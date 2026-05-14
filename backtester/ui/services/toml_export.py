"""
toml_export.py — Export a favourite combo as an experiment-style TOML snippet.

Usage::

    toml_str = favourite_to_toml(fav_row)
    # Optionally copy to clipboard (requires pyperclip):
    copy_to_clipboard(toml_str)

The TOML output is valid and round-trips through tomllib.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def favourite_to_toml(fav) -> str:
    """Return an experiment-style TOML string for *fav* (a FavRow).

    The snippet is compatible with the TOML experiment files in
    ``backtester/experiments/`` and can be parsed by ``tomllib``.
    """
    from backtester.ui.services.store_service import key_from_json

    combo_key = key_from_json(fav.combo_key_json)
    # Build single-value param_grid
    param_grid: dict = {k: [v] for k, v in combo_key}

    strat = fav.strategy or "unknown"
    lines: list[str] = [
        f'strategy = "{strat}"',
    ]
    if fav.name:
        lines.append(f'# Favourite: {fav.name}')
    if fav.note:
        lines.append(f'# Note: {fav.note}')
    if fav.added_at:
        lines.append(f'# Added: {fav.added_at}')
    lines.append("")
    lines.append("[param_grid]")

    for k, vals in param_grid.items():
        v = vals[0]
        toml_val = _toml_value(v)
        lines.append(f"{k} = [{toml_val}]")

    return "\n".join(lines) + "\n"


def copy_to_clipboard(text: str) -> bool:
    """Copy *text* to clipboard using pyperclip.

    Returns True on success, False if pyperclip is unavailable.
    """
    try:
        import pyperclip  # type: ignore
        pyperclip.copy(text)
        return True
    except ImportError:
        log.debug("pyperclip not available; clipboard copy skipped")
        return False
    except Exception as exc:
        log.warning("Clipboard copy failed: %s", exc)
        return False


# ── Internal helpers ──────────────────────────────────────────────────────────

def _toml_value(v) -> str:
    """Format a single value as a TOML inline value (no brackets)."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        # TOML requires a decimal point for floats
        formatted = repr(v)
        if "." not in formatted and "e" not in formatted:
            formatted += ".0"
        return formatted
    if isinstance(v, str):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return f'"{v}"'
