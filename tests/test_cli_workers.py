"""CLI --workers and --detach flags are documented in --help."""
from __future__ import annotations

import subprocess
import sys


def test_cli_help_includes_workers():
    out = subprocess.check_output(
        [sys.executable, "-m", "backtester.run", "--help"],
        text=True,
        cwd=".",
    )
    assert "--workers" in out
    assert "CRYOBT_GRID_WORKERS" in out
    assert "--detach" in out
