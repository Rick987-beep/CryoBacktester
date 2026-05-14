"""
equity_service.py — On-demand equity curve access for any combo key.

Two public functions:

    equity_for_key(grid_result, key) -> dict | None
        Returns top_n_eq[key] if pre-computed; otherwise computes via
        results.equity_metrics() and memoises on grid_result._lazy_eq.

    equity_many(grid_result, keys) -> dict[key, eq]
        Batches equity_for_key() across a list of keys.
"""
from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)


def equity_for_key(grid_result, key) -> dict | None:
    """Return equity metrics for *key*, computing on demand if needed.

    The result is memoised on ``grid_result._lazy_eq`` so repeated access
    for the same key does not re-run equity_metrics().

    Args:
        grid_result: A ``GridResult`` instance.
        key:         A param-tuple key (from ``grid_result.keys``).

    Returns:
        The equity_metrics dict, or None if the key is not found or has no trades.
    """
    # Fast path: pre-computed in top_n_eq
    if key in grid_result.top_n_eq:
        return grid_result.top_n_eq[key]

    # Lazy cache on the result object
    if not hasattr(grid_result, "_lazy_eq"):
        grid_result._lazy_eq = {}

    if key in grid_result._lazy_eq:
        return grid_result._lazy_eq[key]

    # Compute on demand
    combo_idx = grid_result.key_to_idx.get(key)
    if combo_idx is None:
        log.warning("equity_service: key not found in grid_result.key_to_idx: %s", key)
        return None

    from backtester.results import equity_metrics

    df = grid_result.df
    nav_daily_df = grid_result.nav_daily_df
    date_from, date_to = grid_result.date_range

    df_combo = df[df["combo_idx"] == combo_idx] if df is not None and not df.empty else None
    nav_combo = (
        nav_daily_df[nav_daily_df["combo_idx"] == combo_idx]
        if nav_daily_df is not None and not nav_daily_df.empty
        else None
    )

    log.debug("equity_service: computing equity_metrics for key %s (combo_idx=%d)", key, combo_idx)
    eq = equity_metrics(
        df_combo,
        capital=grid_result.account_size,
        nav_daily_combo=nav_combo,
        date_from=date_from,
        date_to=date_to,
    )
    grid_result._lazy_eq[key] = eq
    return eq


def equity_many(grid_result, keys) -> dict:
    """Return {key: eq_dict} for a sequence of keys.

    Keys without equity data (no trades) are excluded from the returned dict.
    """
    result = {}
    for key in keys:
        eq = equity_for_key(grid_result, key)
        if eq is not None:
            result[key] = eq
    return result
