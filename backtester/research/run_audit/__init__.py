"""Grid-quality audit for an existing backtester run.

Reuse ``backtester.inspect`` to resolve the run, then compute parameter
influence, danger, curve-fit diagnostics, and diverse live candidates.
"""
from __future__ import annotations

from backtester.research.run_audit.compute import LivePickConfig, audit_run

__all__ = ["audit_run", "LivePickConfig"]
