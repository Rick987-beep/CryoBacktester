"""
Shared test bootstrap for backtester/strategies/tests/.

Backtester strategy tests live outside the main test suite (pyproject.toml
testpaths) deliberately: strategies are "user content" — they are added,
modified, and deleted frequently and must not break the core suite.

Run backtester strategy tests:
    python -m pytest backtester/strategies/tests/ -v
"""

import os
import sys

# ── Ensure the repo root is on sys.path ────────────────────────────────────
_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
