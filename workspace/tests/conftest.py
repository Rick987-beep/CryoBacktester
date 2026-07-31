"""
Shared test bootstrap for workspace/tests/.

Strategy tests live with workspace use artifacts (not the core product suite).

Run:
    python -m pytest workspace/tests/ -v
"""

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
