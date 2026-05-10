"""bt_option_selection.py — shared option-selection helpers for backtester strategies.

Named with the 'bt_' prefix to avoid confusion with the live-system
``option_selection.py`` in the repo root, which works on exchange API objects
and has an incompatible interface.
"""
from typing import Any, List, Optional


def select_by_delta(chain, target_delta):
    # type: (List[Any], float) -> Optional[Any]
    """Return the option in ``chain`` whose delta is closest to ``target_delta``.

    Prefers non-zero delta candidates; falls back to full chain if all are zero.
    Returns ``None`` if the chain is empty.
    """
    candidates = [q for q in chain if q.delta != 0.0]
    if not candidates:
        candidates = chain
    if not candidates:
        return None
    return min(candidates, key=lambda q: abs(q.delta - target_delta))


def apply_min_otm(chain, selected, spot, min_pct, is_call):
    # type: (List[Any], Any, float, float, bool) -> Optional[Any]
    """Push ``selected`` outward if it is within ``min_pct``% of spot.

    Call leg: requires strike >= spot * (1 + min_pct/100).
    Put  leg: requires strike <= spot * (1 - min_pct/100).

    Returns the nearest qualifying strike, or ``None`` if none exists.
    If ``selected`` already satisfies the constraint it is returned unchanged.
    """
    factor = min_pct / 100.0
    if is_call:
        floor = spot * (1.0 + factor)
        if selected.strike >= floor:
            return selected  # already far enough out
        # lowest call strike that meets the floor
        candidates = sorted(
            [q for q in chain if q.strike >= floor],
            key=lambda q: q.strike,
        )
    else:
        floor = spot * (1.0 - factor)
        if selected.strike <= floor:
            return selected  # already far enough out
        # highest put strike that meets the floor
        candidates = sorted(
            [q for q in chain if q.strike <= floor],
            key=lambda q: q.strike,
            reverse=True,
        )
    return candidates[0] if candidates else None
