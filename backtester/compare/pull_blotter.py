"""Pull prod trade blotter from CryoTrader VPS."""
from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from backtester.compare.models import RunSpec
from backtester.compare.io_utils import log_stage


def pull_blotter(spec: RunSpec, host: str | None = None) -> Path:
    host = host or __import__("os").environ.get("CT_HOST", "root@46.225.137.92")
    out = spec.out_dir / "data" / "blotter.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    remote = f"{host}:{spec.blotter_remote}"
    log_stage("pull_blotter", remote=remote, out=str(out))
    subprocess.run(["scp", remote, str(out)], check=True)
    n_lines = sum(1 for _ in out.open())
    meta = {
        "pulled_at": datetime.now(timezone.utc).isoformat(),
        "host": host,
        "remote": spec.blotter_remote,
        "n_lines": n_lines,
    }
    from backtester.compare.io_utils import write_json
    write_json(spec.out_dir / "blotter_meta.json", meta)
    log_stage("pull_blotter_done", n_lines=n_lines)
    return out
