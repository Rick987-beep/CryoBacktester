"""In-repo synthetic strategy for job-runner tests. Not a catalog product.

``job_smoke`` emits one end-of-replay trade per combo with PnL = ``x``.
Optional ``CRYOBT_JOB_SMOKE_HOLD`` (seconds) sleeps once on combo x=0 so an
E2E queue can observe running/queued overlap. Default hold is 0.
"""
from __future__ import annotations

import os
import time

from backtester.core.strategy_base import Trade


class JobSmokeStrategy:
    name = "job_smoke"

    def configure(self, params):
        self.x = int(params["x"])
        self._slept = False

    def on_market_state(self, state):
        if self.x == 0 and not self._slept:
            hold = float(os.environ.get("CRYOBT_JOB_SMOKE_HOLD", "0") or 0)
            if hold > 0:
                time.sleep(hold)
            self._slept = True
        return []

    def on_end(self, state):
        return [
            Trade(
                entry_time=state.dt,
                exit_time=state.dt,
                entry_spot=float(state.spot),
                exit_spot=float(state.spot),
                entry_price_usd=0.0,
                exit_price_usd=0.0,
                fees=0.0,
                pnl=float(self.x),
                triggered=False,
                exit_reason="end",
                exit_hour=0,
                entry_date=state.dt.strftime("%Y-%m-%d"),
                side="close",
            )
        ]

    def reset(self):
        self._slept = False

    def describe_params(self):
        return {"x": self.x}


def register_smoke():
    """Install into the in-process STRATEGIES map. CLI catalog stays clean."""
    from backtester.run import STRATEGIES

    STRATEGIES.setdefault("job_smoke", JobSmokeStrategy)
    return JobSmokeStrategy
