"""
config.py — Backtester2 application configuration loader.

Reads backtester/config.toml and exposes typed dataclasses for each section.
Import the module-level ``cfg`` singleton — it is loaded once at import time.

Usage::

    from backtester.config import cfg

    print(cfg.pricing.expiry_hour_utc)  # 8
    print(cfg.data.options_parquet)     # absolute path to snapshot parquet
"""
import os
from dataclasses import dataclass

import tomllib

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.toml")


# ── Config dataclasses ────────────────────────────────────────────

@dataclass
class DataConfig:
    options_parquet: str
    spot_parquet: str
    tardis_data_dir: str
    snapshots_dir: str
    reports_dir: str
    snapshot_interval_min: int
    spot_interval_min: int
    parquet_compression: str


@dataclass
class SimulationConfig:
    account_size_usd: float
    progress_interval: int
    top_n_console: int
    top_n_report: int


@dataclass
class PricingConfig:
    hours_per_year: float
    risk_free_rate: float
    expiry_hour_utc: int
    strike_step_usd: int
    vol_lookback_candles: int
    min_vol_candles: int
    default_vol: float
    gap_reset_seconds: int
    min_vol: float
    max_vol: float


@dataclass
class RepricingConfig:
    min_mark_usd: float
    slip_pct_zero_price: float


@dataclass
class FeesConfig:
    model: str
    index_rate: float
    price_cap_frac: float
    perp_taker_rate: float


@dataclass
class ScoringConfig:
    min_trades:      int
    w_sharpe:        float
    w_pnl:           float
    w_max_dd:        float
    w_profit_factor: float
    w_r_squared:     float
    w_omega:         float
    w_ulcer:         float
    w_consistency:   float
    # Recency overlay
    recency_pct:          float
    recency_weight:       float
    recency_min_trades:   int
    recency_gate_enabled: bool
    recency_gate_sharpe:  float


@dataclass
class BacktesterConfig:
    data: DataConfig
    simulation: SimulationConfig
    pricing: PricingConfig
    repricing: RepricingConfig
    fees: FeesConfig
    scoring: ScoringConfig


# ── Loader ────────────────────────────────────────────────────────

def load_config(path=_CONFIG_PATH):
    # type: (str) -> BacktesterConfig
    """Load backtester/config.toml and return a BacktesterConfig instance.

    Path fields are resolved to absolute paths relative to the config file's
    directory so callers don't need to know the repo layout.
    """
    with open(path, "rb") as fh:
        raw = tomllib.load(fh)

    base = os.path.dirname(os.path.abspath(path))

    def _abs(p):
        # type: (str) -> str
        """Resolve a config-relative path to an absolute path."""
        return os.path.normpath(os.path.join(base, p))

    d = raw["data"]
    s = raw["simulation"]
    p = raw["pricing"]
    r = raw["repricing"]
    f = raw["fees"]
    sc = raw["scoring"]

    return BacktesterConfig(
        data=DataConfig(
            options_parquet=_abs(d["options_parquet"]),
            spot_parquet=_abs(d["spot_parquet"]),
            tardis_data_dir=_abs(d["tardis_data_dir"]),
            snapshots_dir=_abs(d["snapshots_dir"]),
            reports_dir=_abs(d["reports_dir"]),
            snapshot_interval_min=int(d["snapshot_interval_min"]),
            spot_interval_min=int(d["spot_interval_min"]),
            parquet_compression=str(d["parquet_compression"]),
        ),
        simulation=SimulationConfig(
            account_size_usd=float(s["account_size_usd"]),
            progress_interval=int(s["progress_interval"]),
            top_n_console=int(s["top_n_console"]),
            top_n_report=int(s["top_n_report"]),
        ),
        pricing=PricingConfig(
            hours_per_year=float(p["hours_per_year"]),
            risk_free_rate=float(p["risk_free_rate"]),
            expiry_hour_utc=int(p["expiry_hour_utc"]),
            strike_step_usd=int(p["strike_step_usd"]),
            vol_lookback_candles=int(p["vol_lookback_candles"]),
            min_vol_candles=int(p["min_vol_candles"]),
            default_vol=float(p["default_vol"]),
            gap_reset_seconds=int(p["gap_reset_seconds"]),
            min_vol=float(p["min_vol"]),
            max_vol=float(p["max_vol"]),
        ),
        repricing=RepricingConfig(
            min_mark_usd=float(r["min_mark_usd"]),
            slip_pct_zero_price=float(r["slip_pct_zero_price"]),
        ),
        fees=FeesConfig(
            model=str(f["model"]),
            index_rate=float(f["index_rate"]),
            price_cap_frac=float(f["price_cap_frac"]),
            perp_taker_rate=float(f["perp_taker_rate"]),
        ),
        scoring=ScoringConfig(
            min_trades=int(sc["min_trades"]),
            w_sharpe=float(sc["w_sharpe"]),
            w_pnl=float(sc["w_pnl"]),
            w_max_dd=float(sc["w_max_dd"]),
            w_profit_factor=float(sc["w_profit_factor"]),
            w_r_squared=float(sc["w_r_squared"]),
            w_omega=float(sc["w_omega"]),
            w_ulcer=float(sc["w_ulcer"]),
            w_consistency=float(sc["w_consistency"]),
            recency_pct=float(sc["recency_pct"]),
            recency_weight=float(sc["recency_weight"]),
            recency_min_trades=int(sc["recency_min_trades"]),
            recency_gate_enabled=bool(sc["recency_gate_enabled"]),
            recency_gate_sharpe=float(sc["recency_gate_sharpe"]),
        ),
    )


# ── Module-level singleton ────────────────────────────────────────

cfg = load_config()
