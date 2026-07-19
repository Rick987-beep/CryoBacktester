"""
robustness.py — Statistical helpers for grid robustness analysis.

Functions:
    deflated_sharpe_ratio   Bailey & López de Prado (2014) DSR
    _robustness_stats       Grid-wide PnL distribution and parameter sensitivity
"""
import math
import numpy as np
from scipy.stats import norm as _norm


# ── Deflated Sharpe Ratio ────────────────────────────────────────

def deflated_sharpe_ratio(trades_pnl, capital, n_trials):
    """Bailey & López de Prado (2014) Deflated Sharpe Ratio.

    Corrects the observed (best-combo) Sharpe for the number of strategy
    combinations tested and for non-normality of trade returns.

    Args:
        trades_pnl: array-like of per-trade PnL values for the best combo.
        capital:    account size used to convert PnL to returns.
        n_trials:   total number of parameter combinations evaluated.

    Returns:
        dsr (float in [0, 1]) — probability that the true Sharpe > 0 after
        correcting for multiple testing.  DSR >= 0.95 is strong; < 0.5 is
        likely noise.
        Returns None if there are fewer than 4 trades (not enough to estimate
        higher moments).
    """
    x = np.asarray(trades_pnl, dtype=float) / float(capital)
    n = len(x)
    if n < 4 or n_trials < 1:
        return None

    mu  = float(np.mean(x))
    sig = float(np.std(x, ddof=1))
    if sig == 0.0:
        return None

    # Per-trade (unannualised) sample Sharpe ratio
    sr_hat = mu / sig

    # Skewness (gamma_3) and regular kurtosis (gamma_4, NOT excess)
    z          = (x - mu) / sig
    skew       = float(np.mean(z ** 3))
    kurt       = float(np.mean(z ** 4))        # regular kurtosis (≈3 for normal)

    # Expected maximum SR from n_trials IID trials (Euler-Mascheroni formula).
    # Equation (4) from Bailey & López de Prado (2014).
    # Scaled by 1/sqrt(n) because our SR is per-observation, not annualised.
    gamma = 0.5772156649  # Euler-Mascheroni constant
    e_max = (
        (1.0 - gamma) * _norm.ppf(1.0 - 1.0 / n_trials)
        + gamma       * _norm.ppf(1.0 - 1.0 / (n_trials * math.e))
    ) / math.sqrt(n)

    # DSR numerator: observed SR adjusted for expected selection bias
    numerator = (sr_hat - e_max) * math.sqrt(n - 1)

    # DSR denominator: inflation due to non-normality
    denom_sq = 1.0 - skew * sr_hat + ((kurt - 1.0) / 4.0) * sr_hat ** 2
    if denom_sq <= 0.0:
        return None
    denom = math.sqrt(denom_sq)

    z_score = numerator / denom
    return float(_norm.cdf(z_score))


# ── Robustness Stats ─────────────────────────────────────────────

def _robustness_stats(all_stats, keys, param_grid):
    """Compute grid-wide robustness metrics from per-combo stats.

    Returns a dict with:
        pnl_all          — list of (key, total_pnl) sorted by combo index order
        pct_profitable   — fraction of combos with total_pnl > 0
        median_pnl       — median total PnL across all combos
        p10_pnl          — 10th-percentile PnL
        p90_pnl          — 90th-percentile PnL
        pnl_iqr          — interquartile range (P75 - P25)
        fragility_score  — (max - min) / abs(median); lower = more robust
        param_sensitivity — dict[param_name → list of (value, mean_pnl, p10, p90)]
                            marginal curve: for each unique param value, aggregate
                            PnL over all combos sharing that value
        monotonicity      — dict[param_name → Spearman ρ]
                            |ρ| near 1 = smooth hill; near 0 = no clear trend
        heatmap_pairs     — list of (pa, pb) sorted by PnL spread, most informative first
    """
    if not all_stats:
        return {
            "pnl_all": [],
            "pct_profitable": 0.0,
            "median_pnl": 0.0,
            "p10_pnl": 0.0,
            "p90_pnl": 0.0,
            "pnl_iqr": 0.0,
            "fragility_score": 0.0,
            "param_sensitivity": {},
            "monotonicity": {},
            "heatmap_pairs": [],
        }

    items = list(all_stats.items())
    pnl_all = [(k, s["total_pnl"]) for k, s in items]
    pnls = sorted(s["total_pnl"] for _, s in items)
    n = len(pnls)

    def _percentile(sorted_vals, p):
        if not sorted_vals:
            return 0.0
        idx = (len(sorted_vals) - 1) * p / 100.0
        lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)

    median_pnl = _percentile(pnls, 50)
    p10 = _percentile(pnls, 10)
    p25 = _percentile(pnls, 25)
    p75 = _percentile(pnls, 75)
    p90 = _percentile(pnls, 90)
    pnl_min, pnl_max = pnls[0], pnls[-1]

    pct_profitable = sum(1 for v in pnls if v > 0) / n
    iqr = p75 - p25
    fragility = (pnl_max - pnl_min) / abs(median_pnl) if abs(median_pnl) > 1e-9 else 0.0

    # ── Per-parameter marginal curves ────────────────────────────
    # Continuous params: more than 1 unique value
    continuous = {p: vals for p, vals in param_grid.items() if len(vals) > 1}

    param_sensitivity = {}
    monotonicity = {}

    for param, values in continuous.items():
        marginal = []
        for val in sorted(set(values)):
            subset_pnls = sorted(
                s["total_pnl"] for k, s in items
                if dict(k).get(param) == val
            )
            if not subset_pnls:
                continue
            mean_v = sum(subset_pnls) / len(subset_pnls)
            p10_v = _percentile(subset_pnls, 10)
            p90_v = _percentile(subset_pnls, 90)
            marginal.append((val, mean_v, p10_v, p90_v))
        param_sensitivity[param] = marginal

        # Spearman ρ between param value rank and mean_pnl rank
        if len(marginal) >= 3:
            x_vals = [pt[0] for pt in marginal]
            y_vals = [pt[1] for pt in marginal]
            n_m = len(x_vals)

            def _ranks(lst):
                order = sorted(range(n_m), key=lambda i: lst[i])
                r = [0.0] * n_m
                for pos, idx in enumerate(order):
                    r[idx] = pos + 1
                return r

            rx = _ranks(x_vals)
            ry = _ranks(y_vals)
            r_mean = (n_m + 1) / 2.0
            num = sum((rx[i] - r_mean) * (ry[i] - r_mean) for i in range(n_m))
            denom_x = sum((rx[i] - r_mean) ** 2 for i in range(n_m)) ** 0.5
            denom_y = sum((ry[i] - r_mean) ** 2 for i in range(n_m)) ** 0.5
            denom = denom_x * denom_y
            monotonicity[param] = num / denom if denom > 1e-12 else 0.0
        else:
            monotonicity[param] = 0.0

    # ── Heatmap pair ranking (moved from reporting_v2._select_pairs) ──
    # Pool PnL by (pa_val, pb_val) and rank pairs by spread.
    all_param_names = sorted(continuous.keys())
    heatmap_pairs = []
    if len(all_param_names) >= 2:
        from itertools import combinations
        pair_spreads = []
        for pa, pb in combinations(all_param_names, 2):
            cell_pnls = {}
            for k, s in items:
                kd = dict(k)
                cell_key = (kd.get(pa), kd.get(pb))
                cell_pnls.setdefault(cell_key, []).append(s["total_pnl"])
            pooled = {ck: sum(vs) for ck, vs in cell_pnls.items()}
            if pooled:
                spread = max(pooled.values()) - min(pooled.values())
                pair_spreads.append((spread, pa, pb))
        pair_spreads.sort(reverse=True)
        heatmap_pairs = [(pa, pb) for _, pa, pb in pair_spreads[:3]]

    return {
        "pnl_all": pnl_all,
        "pct_profitable": pct_profitable,
        "median_pnl": median_pnl,
        "p10_pnl": p10,
        "p90_pnl": p90,
        "pnl_iqr": iqr,
        "fragility_score": fragility,
        "param_sensitivity": param_sensitivity,
        "monotonicity": monotonicity,
        "heatmap_pairs": heatmap_pairs,
    }
