"""I/O helpers for livecompare runs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def log_stage(stage: str, **kwargs: Any) -> None:
    print(json.dumps({"stage": stage, **kwargs}, default=str))
