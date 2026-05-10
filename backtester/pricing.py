"""
Black-Scholes option pricing, strike grid, vol estimation, and fee model.

Reusable across any crypto option backtesting strategy.

Key design decisions:
    - r=0 in BS (negligible for sub-24h options)
    - norm_cdf via math.erf (no scipy dependency)
    - Strike grid defaults to Deribit's $500 steps
    - Vol clamped to prevent numerical blowups (bounds from config.toml)
    - Fees use Deribit's MIN(0.03% x index, 12.5% x leg_price) model

Used by: straddle_strangle.py, reporting.py
"""

import math
import statistics

from backtester.config import cfg as _cfg

# ── Constants (sourced from backtester/config.toml) ──────────────

HOURS_PER_YEAR    = _cfg.pricing.hours_per_year
STRIKE_STEP       = _cfg.pricing.strike_step_usd
VOL_LOOKBACK      = _cfg.pricing.vol_lookback_candles
MIN_VOL_CANDLES   = _cfg.pricing.min_vol_candles
DEFAULT_VOL       = _cfg.pricing.default_vol
EXPIRY_HOUR_UTC   = _cfg.pricing.expiry_hour_utc


# ── Strike Grid ───────────────────────────────────────────────────

def snap_strike(price, step=STRIKE_STEP):
    """Round to nearest strike increment (e.g. $500 on Deribit)."""
    return round(price / step) * step


def get_strikes(spot, offset, step=STRIKE_STEP):
    """Snapped call/put strikes for a structure.
    offset=0 → ATM straddle; offset>0 → strangle."""
    K_call = snap_strike(spot + offset, step)
    K_put = snap_strike(spot - offset, step)
    return K_call, K_put


# ── Black-Scholes ─────────────────────────────────────────────────

def norm_cdf(x):
    """Standard normal CDF via math.erf (no scipy needed)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_call(S, K, T_years, sigma):
    """BS call price with r=0."""
    if T_years <= 1e-12 or sigma <= 1e-12:
        return max(0.0, S - K)
    sqrt_T = math.sqrt(T_years)
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T_years) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return S * norm_cdf(d1) - K * norm_cdf(d2)


def bs_put(S, K, T_years, sigma):
    """BS put price with r=0."""
    if T_years <= 1e-12 or sigma <= 1e-12:
        return max(0.0, K - S)
    sqrt_T = math.sqrt(T_years)
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T_years) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return K * norm_cdf(-d2) - S * norm_cdf(-d1)


def price_structure(spot, offset, dte_hours, sigma, step=STRIKE_STEP):
    """Price a straddle/strangle at entry using BS with snapped strikes.
    Returns (total, call_price, put_price, K_call, K_put) in USD."""
    T = dte_hours / HOURS_PER_YEAR
    K_call, K_put = get_strikes(spot, offset, step)
    call_price = bs_call(spot, K_call, T, sigma)
    put_price = bs_put(spot, K_put, T, sigma)
    return call_price + put_price, call_price, put_price, K_call, K_put


def price_at_exit(exit_spot, K_call, K_put, remaining_hours, sigma):
    """Price at exit using BS with the SAME strikes from entry.
    Returns (total, call_price, put_price) in USD."""
    T = remaining_hours / HOURS_PER_YEAR
    call_price = bs_call(exit_spot, K_call, T, sigma)
    put_price = bs_put(exit_spot, K_put, T, sigma)
    return call_price + put_price, call_price, put_price


def hours_to_expiry(entry_hour, expiry_hour=EXPIRY_HOUR_UTC):
    """Hours until next-day expiry (default 08:00 UTC)."""
    return 24 + expiry_hour - entry_hour


# ── Realized Vol ──────────────────────────────────────────────────

def estimate_vol(sorted_candles, entry_index, lookback=VOL_LOOKBACK):
    """Annualized vol from trailing hourly log returns.
    Resets on gaps > 2h. Returns DEFAULT_VOL if insufficient data."""
    start = max(0, entry_index - lookback)
    window = sorted_candles[start:entry_index]

    if len(window) < 2:
        return DEFAULT_VOL

    log_rets = []
    for j in range(1, len(window)):
        dt_gap = (window[j]["dt"] - window[j - 1]["dt"]).total_seconds()
        if dt_gap > _cfg.pricing.gap_reset_seconds:
            log_rets.clear()
            continue
        prev_close = window[j - 1]["close"]
        curr_close = window[j]["close"]
        if prev_close > 0 and curr_close > 0:
            log_rets.append(math.log(curr_close / prev_close))

    if len(log_rets) < MIN_VOL_CANDLES:
        return DEFAULT_VOL

    hourly_std = statistics.stdev(log_rets)
    annualized = hourly_std * math.sqrt(HOURS_PER_YEAR)
    return max(_cfg.pricing.min_vol, min(annualized, _cfg.pricing.max_vol))


# ── Fee Model ─────────────────────────────────────────────────────

def deribit_fee_per_leg(btc_price, leg_price_usd):
    """Deribit: MIN(0.03% of index, 12.5% of option price) per leg per trade."""
    base = _cfg.fees.index_rate * btc_price
    cap = _cfg.fees.price_cap_frac * max(leg_price_usd, 0)
    return min(base, cap)


def deribit_perp_fee(notional_usd):
    """Deribit BTC-PERPETUAL taker fee: 0.05% of |notional| per trade."""
    return _cfg.fees.perp_taker_rate * abs(notional_usd)


# ── Greeks ────────────────────────────────────────────────────────

def bs_call_delta(S, K, T_years, sigma):
    """BS call delta (r=0). Range [0, 1]."""
    if T_years <= 1e-12 or sigma <= 1e-12:
        return 1.0 if S >= K else 0.0
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T_years) / (sigma * math.sqrt(T_years))
    return norm_cdf(d1)


def bs_put_delta(S, K, T_years, sigma):
    """BS put delta (r=0). Range [-1, 0]. Equal to bs_call_delta - 1."""
    return bs_call_delta(S, K, T_years, sigma) - 1.0
