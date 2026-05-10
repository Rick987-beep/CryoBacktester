#!/usr/bin/env python3
"""
market_replay.py — Market state iterator for backtesting.

Loads pre-built snapshot parquets (from snapshot_builder.py) and provides
a time-stepped iterator that yields MarketState objects at each 5-min
interval. Strategies see a clean, read-only market view at each step.

Key design:
    - Simple iterator (not event bus). Strategies pull data, no callbacks.
    - Option data stored as contiguous NumPy arrays (columnar layout).
    - Per-tick access via timestamp index (ts_starts/ts_lens) — no Python
      tuple boxing, ~5× less RAM than the old dict-of-tuples approach.
    - Spot track as NumPy arrays for fast excursion range queries.
    - Pre-computed cumulative max/min for O(1) excursion lookups.
    - Strategy-scoped expiry filtering at load time — one snapshot serves all.
    - Supports single parquet file OR directory of per-day parquets.

Usage:
    replay = MarketReplay(
        "data/options_20260309_20260323.parquet",
        "data/spot_track_20260309_20260323.parquet",
    )
    for state in replay:
        # state.spot, state.get_option(...), state.spot_bars, etc.
        pass
"""
import glob
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd


# ------------------------------------------------------------------
# Data types
# ------------------------------------------------------------------

@dataclass
class OptionQuote:
    """Single option quote at a point in time."""
    strike: float
    is_call: bool
    expiry: str
    bid: float              # BTC-denominated
    ask: float
    mark: float
    mark_iv: float
    delta: float
    spot: float             # Underlying BTC price at this snapshot

    @property
    def bid_usd(self):
        # type: () -> float
        """Bid price in USD (bid_btc × spot)."""
        return self.bid * self.spot

    @property
    def ask_usd(self):
        # type: () -> float
        """Ask price in USD (ask_btc × spot)."""
        return self.ask * self.spot

    @property
    def mark_usd(self):
        # type: () -> float
        """Mark price in USD (mark_btc × spot)."""
        return self.mark * self.spot


@dataclass
class SpotBar:
    """1-minute OHLC bar for BTC spot price."""
    timestamp: int      # Microseconds since epoch
    open: float
    high: float
    low: float
    close: float


@dataclass
class MarketState:
    """Snapshot of the market at one 5-min interval.

    Provides option chain lookups and spot price data. Constructed by
    MarketReplay — strategies consume this, never build it.

    Option data is accessed lazily from the replay's NumPy arrays via
    vectorised lookups (~0.5 µs per get_option call on ~300 rows).
    No Python float allocation until strategy actually consumes the result.
    """
    timestamp: int              # Microseconds (5-min aligned)
    dt: datetime                # UTC datetime
    spot: float                 # BTC/USD (close of latest 1-min bar)

    # Internal: references to the replay's NumPy arrays for this tick's slice.
    # _start/_length define the row range in the global arrays.
    _expiry_table: List[str] = field(default_factory=list, repr=False)
    _expiry_idx: Optional[np.ndarray] = field(default=None, repr=False)
    _strike: Optional[np.ndarray] = field(default=None, repr=False)
    _is_call: Optional[np.ndarray] = field(default=None, repr=False)
    _bid: Optional[np.ndarray] = field(default=None, repr=False)
    _ask: Optional[np.ndarray] = field(default=None, repr=False)
    _mark: Optional[np.ndarray] = field(default=None, repr=False)
    _mark_iv_arr: Optional[np.ndarray] = field(default=None, repr=False)
    _delta_arr: Optional[np.ndarray] = field(default=None, repr=False)
    _length: int = field(default=0, repr=False)

    _quote_cache: Dict[Tuple[str, float, bool], "OptionQuote"] = field(
        default_factory=dict, repr=False
    )
    _expiries_cache: Optional[List[str]] = field(default=None, repr=False)

    # Internal: spot bar range (lazy — SpotBar objects built on first access)
    _spot_bar_start: int = field(default=0, repr=False)
    _spot_bar_end: int = field(default=0, repr=False)
    _spot_bars_cache: Optional[List[SpotBar]] = field(default=None, repr=False)

    # Internal: reference to replay's spot arrays for excursion lookups
    _spot_ts: Optional[np.ndarray] = field(default=None, repr=False)
    _spot_open: Optional[np.ndarray] = field(default=None, repr=False)
    _spot_high: Optional[np.ndarray] = field(default=None, repr=False)
    _spot_low: Optional[np.ndarray] = field(default=None, repr=False)
    _spot_close: Optional[np.ndarray] = field(default=None, repr=False)

    @property
    def spot_bars(self):
        # type: () -> List[SpotBar]
        """1-min bars since last MarketState (up to 5). Lazy — built on first access."""
        if self._spot_bars_cache is not None:
            return self._spot_bars_cache
        bars = []
        if self._spot_ts is not None:
            for i in range(self._spot_bar_start, self._spot_bar_end):
                bars.append(SpotBar(
                    timestamp=int(self._spot_ts[i]),
                    open=float(self._spot_open[i]),
                    high=float(self._spot_high[i]),
                    low=float(self._spot_low[i]),
                    close=float(self._spot_close[i]),
                ))
        self._spot_bars_cache = bars
        return bars

    def _lookup_row(self, expiry, strike, is_call):
        # type: (str, float, bool) -> int
        """Find row index within this tick's slice. Returns -1 if not found."""
        if self._length == 0:
            return -1
        # Map expiry string to uint8 index
        try:
            exp_idx = self._expiry_table.index(expiry)
        except ValueError:
            return -1
        # Vectorised match over ~300 rows — ~0.5 µs
        # Note: strike comparison promotes float32 array to float64 automatically.
        # This is correct since stored strikes are round numbers (e.g. 85000.0)
        # that roundtrip float32↔float64 losslessly.
        mask = (
            (self._expiry_idx == exp_idx) &
            (self._strike == strike) &
            (self._is_call == is_call)
        )
        indices = np.flatnonzero(mask)
        if len(indices) == 0:
            return -1
        return int(indices[0])

    def _quote_from_row(self, j, expiry, strike, is_call):
        # type: (int, str, float, bool) -> OptionQuote
        """Build OptionQuote from a row index, applying data-quality filters."""
        raw_bid = float(self._bid[j])
        raw_ask = float(self._ask[j])
        raw_mark = float(self._mark[j])

        # NaN → 0.0
        if raw_bid != raw_bid:
            raw_bid = 0.0
        if raw_ask != raw_ask:
            raw_ask = 0.0

        # Data quality: mark==0 → exchange has no pricing model for this tick
        if raw_mark == 0.0:
            raw_bid = 0.0
            raw_ask = 0.0
        # Clamp corrupted ask: if ask > 10× mark, treat as missing.
        elif raw_ask > raw_mark * 10:
            raw_ask = 0.0

        return OptionQuote(
            strike=strike,
            is_call=is_call,
            expiry=expiry,
            bid=raw_bid,
            ask=raw_ask,
            mark=raw_mark,
            mark_iv=float(self._mark_iv_arr[j]),
            delta=float(self._delta_arr[j]),
            spot=self.spot,
        )

    def get_option(self, expiry, strike, is_call):
        # type: (str, float, bool) -> Optional[OptionQuote]
        """Single option lookup. Vectorised search + lazy OptionQuote construction."""
        key = (expiry, float(strike), bool(is_call))
        q = self._quote_cache.get(key)
        if q is not None:
            return q
        j = self._lookup_row(key[0], key[1], key[2])
        if j < 0:
            return None
        q = self._quote_from_row(j, key[0], key[1], key[2])
        self._quote_cache[key] = q
        return q

    def get_chain(self, expiry):
        # type: (str) -> List[OptionQuote]
        """All options for one expiry, sorted by strike."""
        if self._length == 0:
            return []
        try:
            exp_idx = self._expiry_table.index(expiry)
        except ValueError:
            return []
        mask = self._expiry_idx == exp_idx
        indices = np.flatnonzero(mask)
        result = []
        for j in indices:
            j = int(j)
            strike = float(self._strike[j])
            is_call = bool(self._is_call[j])
            key = (expiry, strike, is_call)
            q = self._quote_cache.get(key)
            if q is None:
                q = self._quote_from_row(j, expiry, strike, is_call)
                self._quote_cache[key] = q
            result.append(q)
        result.sort(key=lambda q: (q.strike, q.is_call))
        return result

    def get_atm_strike(self, expiry):
        # type: (str) -> Optional[float]
        """ATM strike (nearest to spot) for an expiry."""
        if self._length == 0:
            return None
        try:
            exp_idx = self._expiry_table.index(expiry)
        except ValueError:
            return None
        mask = self._expiry_idx == exp_idx
        strikes = np.unique(self._strike[mask])
        if len(strikes) == 0:
            return None
        idx = np.argmin(np.abs(strikes - self.spot))
        return float(strikes[idx])

    def get_straddle(self, expiry, strike=None):
        # type: (str, Optional[float]) -> Tuple[Optional[OptionQuote], Optional[OptionQuote]]
        """ATM or specific-strike call+put pair."""
        if strike is None:
            strike = self.get_atm_strike(expiry)
        if strike is None:
            return None, None
        call = self.get_option(expiry, strike, True)
        put = self.get_option(expiry, strike, False)
        return call, put

    def get_strangle(self, expiry, offset):
        # type: (str, float) -> Tuple[Optional[OptionQuote], Optional[OptionQuote]]
        """OTM call+put at ±offset from ATM.

        offset=0 is equivalent to get_straddle(expiry).
        """
        atm = self.get_atm_strike(expiry)
        if atm is None:
            return None, None
        if self._length == 0:
            return None, None
        try:
            exp_idx = self._expiry_table.index(expiry)
        except ValueError:
            return None, None
        mask = self._expiry_idx == exp_idx
        strikes = np.unique(self._strike[mask])
        if len(strikes) == 0:
            return None, None
        call_target = atm + offset
        put_target = atm - offset
        # For ties (equidistant strikes), prefer the more OTM strike:
        # call → higher strike, put → lower strike. This is the standard
        # strangle convention and produces deterministic results.
        call_dists = np.abs(strikes.astype(np.float64) - call_target)
        call_min_d = np.min(call_dists)
        call_candidates = strikes[call_dists - call_min_d < 0.01]
        call_strike = float(np.max(call_candidates))  # highest = more OTM

        put_dists = np.abs(strikes.astype(np.float64) - put_target)
        put_min_d = np.min(put_dists)
        put_candidates = strikes[put_dists - put_min_d < 0.01]
        put_strike = float(np.min(put_candidates))  # lowest = more OTM
        return (
            self.get_option(expiry, call_strike, True),
            self.get_option(expiry, put_strike, False),
        )

    def expiries(self):
        # type: () -> List[str]
        """Available expiries at this time step."""
        if self._expiries_cache is not None:
            return list(self._expiries_cache)
        if self._length == 0:
            self._expiries_cache = []
            return []
        unique_idxs = np.unique(self._expiry_idx)
        result = sorted(self._expiry_table[int(i)] for i in unique_idxs)
        self._expiries_cache = result
        return list(result)

    def spot_high_since(self, entry_time_us):
        # type: (int) -> float
        """Highest spot high across all 1-min bars from entry_time_us to now."""
        if self._spot_ts is None:
            return self.spot
        i_start = int(np.searchsorted(self._spot_ts, entry_time_us, side="left"))
        i_end = int(np.searchsorted(self._spot_ts, self.timestamp, side="right"))
        if i_end > i_start:
            return float(np.max(self._spot_high[i_start:i_end]))
        return self.spot

    def spot_low_since(self, entry_time_us):
        # type: (int) -> float
        """Lowest spot low across all 1-min bars from entry_time_us to now."""
        if self._spot_ts is None:
            return self.spot
        i_start = int(np.searchsorted(self._spot_ts, entry_time_us, side="left"))
        i_end = int(np.searchsorted(self._spot_ts, self.timestamp, side="right"))
        if i_end > i_start:
            return float(np.min(self._spot_low[i_start:i_end]))
        return self.spot


# ------------------------------------------------------------------
# MarketReplay — the iterator
# ------------------------------------------------------------------

class MarketReplay:
    """Loads snapshot parquets and iterates as MarketState objects.

    Stores option data as contiguous NumPy arrays (columnar layout) with a
    timestamp index for O(1) slice access per tick. This uses ~5× less RAM
    than the previous dict-of-Python-tuples approach.

    Args:
        snapshot_path: Path to option snapshot parquet, or directory of
            per-day parquets (options_YYYY-MM-DD.parquet).
        spot_track_path: Path to spot track OHLC parquet, or directory of
            per-day parquets (spot_YYYY-MM-DD.parquet).
        expiry_filter: Optional list of expiry codes to keep (runtime filter).
        start: Optional start time (inclusive). Accepts str/int/datetime.
        end: Optional end time (inclusive).
        step_minutes: Iteration step (default 5, must be >= snapshot interval).
    """

    def __init__(
        self,
        snapshot_path,      # type: str
        spot_track_path,    # type: str
        expiry_filter=None, # type: Optional[List[str]]
        start=None,         # type: Optional[Any]
        end=None,           # type: Optional[Any]
        step_minutes=5,     # type: int
    ):
        # ----------------------------------------------------------
        # Load option snapshots (single file or directory of per-day files)
        # ----------------------------------------------------------
        opt_df = self._load_parquets(snapshot_path, "options_")
        if expiry_filter:
            opt_df = opt_df[opt_df["expiry"].isin(expiry_filter)].reset_index(drop=True)

        # Load spot track
        spot_df = self._load_parquets(spot_track_path, "spot_")

        # Time filtering
        if start is not None:
            start_us = self._to_us(start)
            opt_df = opt_df[opt_df["timestamp"] >= start_us].reset_index(drop=True)
            spot_df = spot_df[spot_df["timestamp"] >= start_us].reset_index(drop=True)
        if end is not None:
            end_us = self._to_us(end)
            opt_df = opt_df[opt_df["timestamp"] <= end_us].reset_index(drop=True)
            spot_df = spot_df[spot_df["timestamp"] <= end_us].reset_index(drop=True)

        # ----------------------------------------------------------
        # Columnar NumPy storage for option data
        # ----------------------------------------------------------
        # Sort by timestamp for contiguous slicing
        opt_df.sort_values("timestamp", inplace=True, kind="mergesort")
        opt_df.reset_index(drop=True, inplace=True)

        # Encode expiry strings as uint16 indices (up to 65535 unique expiries)
        expiry_cat = opt_df["expiry"].astype("category")
        self._expiry_table = list(expiry_cat.cat.categories)  # str lookup table
        self._opt_expiry_idx = expiry_cat.cat.codes.values.astype(np.uint16)

        # Extract columns as contiguous NumPy arrays
        self._opt_timestamps = opt_df["timestamp"].values.astype(np.int64)
        self._opt_strike = opt_df["strike"].values.astype(np.float32)
        self._opt_is_call = opt_df["is_call"].values.astype(np.bool_)
        self._opt_bid = opt_df["bid_price"].values.astype(np.float32)
        self._opt_ask = opt_df["ask_price"].values.astype(np.float32)
        self._opt_mark = opt_df["mark_price"].values.astype(np.float32)
        self._opt_mark_iv = opt_df["mark_iv"].values.astype(np.float32)
        self._opt_delta = opt_df["delta"].values.astype(np.float32)

        n_opt = len(self._opt_timestamps)

        # Build timestamp index: for each unique timestamp, store the start
        # row and row count so _build_state can slice directly into the arrays.
        self._ts_sorted, self._ts_starts, ts_counts = np.unique(
            self._opt_timestamps, return_index=True, return_counts=True
        )
        self._ts_lens = ts_counts.astype(np.int32)
        self._ts_starts = self._ts_starts.astype(np.int32)

        # Build a fast lookup: timestamp → index into ts_sorted
        self._ts_to_idx = {}  # type: Dict[int, int]
        for i, ts_val in enumerate(self._ts_sorted):
            self._ts_to_idx[int(ts_val)] = i

        # Free the DataFrame
        del opt_df

        # Filter timestamps by step
        all_ts = self._ts_sorted.copy()
        if step_minutes > 5:
            step_us = step_minutes * 60 * 1_000_000
            all_ts = all_ts[all_ts % step_us == 0]
        self._timestamps = all_ts

        # ----------------------------------------------------------
        # Spot track as NumPy arrays
        # ----------------------------------------------------------
        self._spot_ts = spot_df["timestamp"].values.astype(np.int64)
        self._spot_open = spot_df["open"].values.astype(np.float64)
        self._spot_high = spot_df["high"].values.astype(np.float64)
        self._spot_low = spot_df["low"].values.astype(np.float64)
        self._spot_close = spot_df["close"].values.astype(np.float64)

        del spot_df

        n_ts = len(self._timestamps)
        n_spot = len(self._spot_ts)
        # Estimate RAM usage for option arrays
        opt_ram_mb = (
            n_opt * (8 + 2 + 4 + 1 + 4 + 4 + 4 + 4 + 4)  # int64+uint16+float32×6+bool
        ) / (1024 * 1024)
        print(
            f"MarketReplay loaded: {n_opt:,} option rows ({opt_ram_mb:.0f} MB), "
            f"{n_ts} intervals, {n_spot} spot bars"
        )

    @staticmethod
    def _load_parquets(path, prefix):
        # type: (str, str) -> pd.DataFrame
        """Load a single parquet file or all matching parquets from a directory.

        Supports both the old single-file layout and the new per-day layout.
        """
        if os.path.isfile(path):
            return pd.read_parquet(path)

        if os.path.isdir(path):
            pattern = os.path.join(path, f"{prefix}*.parquet")
            files = sorted(glob.glob(pattern))
            if not files:
                # Fallback: try any .parquet file in the directory
                files = sorted(glob.glob(os.path.join(path, "*.parquet")))
            if not files:
                raise FileNotFoundError(
                    f"No parquet files found in {path} (prefix={prefix})"
                )
            dfs = [pd.read_parquet(f) for f in files]
            return pd.concat(dfs, ignore_index=True)

        raise FileNotFoundError(f"Path not found: {path}")

    @staticmethod
    def _to_us(t):
        """Convert time arg to microseconds."""
        if isinstance(t, (int, np.integer)):
            return int(t)
        if isinstance(t, str):
            t = pd.Timestamp(t, tz="UTC")
        if isinstance(t, datetime):
            t = pd.Timestamp(t)
        if isinstance(t, pd.Timestamp):
            if t.tz is None:
                t = t.tz_localize("UTC")
            return int(t.timestamp() * 1_000_000)
        raise TypeError(f"Cannot convert {type(t)} to timestamp")

    @property
    def timestamps(self):
        # type: () -> np.ndarray
        """All available 5-min timestamps (microseconds)."""
        return self._timestamps

    @property
    def time_range(self):
        # type: () -> Tuple[datetime, datetime]
        """Data coverage as (start, end) UTC datetimes."""
        return (
            datetime.fromtimestamp(
                self._timestamps[0] / 1_000_000, tz=timezone.utc
            ),
            datetime.fromtimestamp(
                self._timestamps[-1] / 1_000_000, tz=timezone.utc
            ),
        )

    def date_range(self):
        # type: () -> Tuple[datetime, datetime]
        """Data coverage as (start, end) UTC datetimes. Callable alias for time_range."""
        return self.time_range

    def __len__(self):
        # type: () -> int
        return len(self._timestamps)

    def __iter__(self):
        # type: () -> Iterator[MarketState]
        """Yield MarketState for each time step."""
        prev_ts = None
        for ts in self._timestamps:
            state = self._build_state(ts, prev_ts)
            yield state
            prev_ts = ts

    def _build_state(self, ts, prev_ts):
        # type: (int, Optional[int]) -> MarketState
        """Construct MarketState for one 5-min interval.

        Passes NumPy array slices to MarketState — no Python loop, no dict
        construction. All option lookups happen lazily via vectorised search.
        """
        dt = datetime.fromtimestamp(ts / 1_000_000, tz=timezone.utc)

        # Spot: close of the latest 1-min bar at or before this timestamp
        spot_idx = np.searchsorted(self._spot_ts, ts, side="right") - 1
        if spot_idx < 0:
            spot_idx = 0
        spot = float(self._spot_close[spot_idx])

        # Spot bar range (lazy — SpotBar objects built on first access)
        if prev_ts is not None:
            bar_start = int(np.searchsorted(self._spot_ts, prev_ts, side="right"))
        else:
            bar_start = max(0, spot_idx - 4)  # First state: grab up to 5 bars
        bar_end = min(spot_idx + 1, len(self._spot_ts))

        # Options: pass array slices — no Python loop here
        ts_idx = self._ts_to_idx.get(int(ts))
        if ts_idx is not None:
            start = int(self._ts_starts[ts_idx])
            length = int(self._ts_lens[ts_idx])
            end = start + length
            exp_slice = self._opt_expiry_idx[start:end]
            strike_slice = self._opt_strike[start:end]
            is_call_slice = self._opt_is_call[start:end]
            bid_slice = self._opt_bid[start:end]
            ask_slice = self._opt_ask[start:end]
            mark_slice = self._opt_mark[start:end]
            iv_slice = self._opt_mark_iv[start:end]
            delta_slice = self._opt_delta[start:end]
        else:
            length = 0
            exp_slice = None
            strike_slice = None
            is_call_slice = None
            bid_slice = None
            ask_slice = None
            mark_slice = None
            iv_slice = None
            delta_slice = None

        return MarketState(
            timestamp=ts,
            dt=dt,
            spot=spot,
            _expiry_table=self._expiry_table,
            _expiry_idx=exp_slice,
            _strike=strike_slice,
            _is_call=is_call_slice,
            _bid=bid_slice,
            _ask=ask_slice,
            _mark=mark_slice,
            _mark_iv_arr=iv_slice,
            _delta_arr=delta_slice,
            _length=length,
            _spot_bar_start=bar_start,
            _spot_bar_end=bar_end,
            _spot_ts=self._spot_ts,
            _spot_open=self._spot_open,
            _spot_high=self._spot_high,
            _spot_low=self._spot_low,
            _spot_close=self._spot_close,
        )
