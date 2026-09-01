"""Public strategy catalog — blueprint only (no private workspace submodule)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Type

from backtester.strategies.blueprint_howto import BlueprintHowto


@dataclass(frozen=True)
class Family:
    id: str
    label: str
    description: str = ""


@dataclass(frozen=True)
class StrategySpec:
    id: str
    family: str
    cls: Type
    status: str = "active"  # active | frozen | live_parity | archived
    display_name: str = ""

    def label(self) -> str:
        name = self.display_name or self.id
        if self.status and self.status != "active":
            return f"{name} ({self.status})"
        return name


FAMILIES: Dict[str, Family] = {
    "other": Family(
        id="other",
        label="Other",
        description="Standalone strategies",
    ),
}


def _build_specs() -> Dict[str, StrategySpec]:
    rows = [
        StrategySpec("blueprint_howto", "other", BlueprintHowto),
    ]
    return {s.id: s for s in rows}


SPECS: Dict[str, StrategySpec] = _build_specs()


def strategies_dict() -> Dict[str, Type]:
    return {sid: spec.cls for sid, spec in SPECS.items()}


def family_for(strategy_id: str) -> str:
    spec = SPECS.get(strategy_id)
    return spec.family if spec is not None else "other"


def specs_in_family(family_id: str) -> List[StrategySpec]:
    return [s for s in SPECS.values() if s.family == family_id]


def family_label(family_id: str) -> str:
    fam = FAMILIES.get(family_id)
    return fam.label if fam is not None else family_id


def strategy_options(family_id: str | None = None) -> Dict[str, str]:
    """Map display label → stable strategy id for Panel Select widgets."""
    specs = list(SPECS.values())
    if family_id and family_id != "all":
        specs = [s for s in specs if s.family == family_id]
    out: Dict[str, str] = {}
    for s in sorted(specs, key=lambda x: x.id):
        out[s.label()] = s.id
    return out
