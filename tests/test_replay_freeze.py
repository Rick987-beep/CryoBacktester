"""MarketReplay.freeze_readonly contract."""
from __future__ import annotations

import numpy as np
import pytest

from backtester.core.market_replay import MarketReplay


def test_freeze_readonly_method_on_loaded_like_object(monkeypatch):
    """Avoid parquet: attach arrays then call the real method."""
    replay = MarketReplay.__new__(MarketReplay)
    replay._opt_bid = np.arange(3, dtype=np.float32)
    replay._spot_close = np.array([1.0, 2.0])
    replay.freeze_readonly()
    assert replay._opt_bid.flags.writeable is False
    assert replay._spot_close.flags.writeable is False
    with pytest.raises(ValueError):
        replay._opt_bid[1] = 0.0
    assert replay._opt_bid[1] == 1.0
