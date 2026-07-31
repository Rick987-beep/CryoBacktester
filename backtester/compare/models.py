"""Data models for live vs backtester comparison runs."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class WarningCode(str, Enum):
    SIZING_DIFF = "SIZING_DIFF"
    FILL_MODEL = "FILL_MODEL"
    PARTIAL_FILL = "PARTIAL_FILL"
    DATA_GAP = "DATA_GAP"
    NO_BT_TRADE = "NO_BT_TRADE"
    STRATEGY_MISMATCH = "STRATEGY_MISMATCH"
    CONFIG_DRIFT = "CONFIG_DRIFT"
    STRIKE_MISMATCH = "STRIKE_MISMATCH"
    EXIT_MISMATCH = "EXIT_MISMATCH"
    MON_EARLY_DISABLED = "MON_EARLY_DISABLED"
    DATA_INCOMPLETE = "DATA_INCOMPLETE"


class Comparability(str, Enum):
    OK = "OK"
    WARN = "WARN"
    EXCLUDE = "EXCLUDE"


@dataclass
class ParityWarning:
    code: WarningCode
    severity: str  # error | warn | info
    message: str
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["code"] = self.code.value
        return d


@dataclass
class RunSpec:
    slot: str
    cryotrader_root: Path
    repo_root: Path
    out_dir: Path
    last_n: Optional[int] = 7
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    skip_pull: bool = False
    skip_bt: bool = False
    account_size_usd: float = 100_000.0
    sizing_mode: str = "bt_default"

    @property
    def slot_padded(self) -> str:
        return self.slot.zfill(2) if self.slot.isdigit() else self.slot

    @property
    def blotter_remote(self) -> str:
        return f"/opt/ct/trade_history/slot-{self.slot_padded}.jsonl"

    @property
    def slot_toml(self) -> Path:
        return self.cryotrader_root / "slots" / f"slot-{self.slot_padded}.toml"
