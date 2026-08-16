"""Duck-typed investor_greeks sidecar collection from run_grid_full instances."""


class _WithSidecar:
    def investor_greeks_sidecar(self):
        return {"live_bars": 7, "recovered_pct": 12.5}


class _TrackOff:
    def investor_greeks_sidecar(self):
        return None


class _NoHook:
    pass


def test_collect_extra_parquets_merges_combo_keys():
    from backtester.core.engine import _collect_extra_parquets

    keys = [
        (("entry_policy", "fav_pnl_daily_1500"), ("wing_budget_usd", 100.0)),
        (("entry_policy", "fav_pnl_daily_1500"), ("wing_budget_usd", 0.0)),
        (("entry_policy", "fav_sharpe_rich4_f5_1600"), ("wing_budget_usd", 250.0)),
    ]
    extra = _collect_extra_parquets(
        [_WithSidecar(), _TrackOff(), _NoHook()],
        keys,
    )
    assert list(extra) == ["investor_greeks.parquet"]
    df = extra["investor_greeks.parquet"]
    assert len(df) == 1
    row = df.iloc[0]
    assert int(row["combo_idx"]) == 0
    assert int(row["live_bars"]) == 7
    assert row["entry_policy"] == "fav_pnl_daily_1500"
    assert float(row["wing_budget_usd"]) == 100.0
