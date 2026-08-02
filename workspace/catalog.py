"""Strategy family catalog — stable IDs, families, and class registry.

Strategy IDs must never be renamed (bundles, favourites, livecompare, experiments).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Type


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
    "tudysho": Family(
        id="tudysho",
        label="TuDySho",
        description="Turbulence-gated short premium family",
    ),
    "theta_engine": Family(
        id="theta_engine",
        label="Theta Engine",
        description="Daily short-vol theta income family",
    ),
    "other": Family(
        id="other",
        label="Other",
        description="Standalone strategies",
    ),
}


def _build_specs() -> Dict[str, StrategySpec]:
    from workspace.strategies.other.blueprint_howto import BlueprintHowto
    from workspace.strategies.other.cadysho import Cadysho
    from workspace.strategies.other.cal_spread_atm import CalSpreadAtm
    from workspace.strategies.other.covered_call_put import CoveredCallPut
    from workspace.strategies.other.long_gamma_move import LongGammaMove
    from workspace.strategies.other.pagoda import Pagoda
    from workspace.strategies.other.short_str_turb_dyn import ShortStrTurbDyn
    from workspace.strategies.theta_engine.v1 import ThetaEnginev1
    from workspace.strategies.theta_engine.v2 import ThetaEngineV2
    from workspace.strategies.theta_engine.v3 import ThetaEngineV3
    from workspace.strategies.theta_engine.v4 import ThetaEngineV4
    from workspace.strategies.theta_engine.v5 import ThetaEngineV5
    from workspace.strategies.theta_engine.v6 import ThetaEngineV6
    from workspace.strategies.theta_engine.v7 import ThetaEngineV7
    from workspace.strategies.theta_engine.v8 import ThetaEngineV8
    from workspace.strategies.tudysho.eisbach import TuDyShoEisbach
    from workspace.strategies.tudysho.starnberg import TuDyShoStarnberg
    from workspace.strategies.tudysho.stradysho import StraDySho
    from workspace.strategies.tudysho.tudysho import TuDySho
    from workspace.strategies.tudysho.v1 import TuDyShoV1
    from workspace.strategies.tudysho.v2 import TuDyShoV2

    rows = [
        StrategySpec("tudysho", "tudysho", TuDySho, status="active"),
        StrategySpec("tudysho_v1", "tudysho", TuDyShoV1, status="frozen"),
        StrategySpec("tudysho_v2", "tudysho", TuDyShoV2, status="frozen"),
        StrategySpec("tudysho_eisbach", "tudysho", TuDyShoEisbach, status="live_parity"),
        StrategySpec("tudysho_starnberg", "tudysho", TuDyShoStarnberg, status="active"),
        StrategySpec("stradysho", "tudysho", StraDySho, status="active"),
        StrategySpec("theta_engine_v1", "theta_engine", ThetaEnginev1, status="frozen"),
        StrategySpec("theta_engine_v2", "theta_engine", ThetaEngineV2, status="frozen"),
        StrategySpec("theta_engine_v3", "theta_engine", ThetaEngineV3, status="frozen"),
        StrategySpec("theta_engine_v4", "theta_engine", ThetaEngineV4, status="frozen"),
        StrategySpec("theta_engine_v5", "theta_engine", ThetaEngineV5, status="frozen"),
        StrategySpec("theta_engine_v6", "theta_engine", ThetaEngineV6, status="active"),
        StrategySpec("theta_engine_v7", "theta_engine", ThetaEngineV7, status="active"),
        StrategySpec("theta_engine_v8", "theta_engine", ThetaEngineV8, status="active"),
        StrategySpec("short_str_turb_dyn", "other", ShortStrTurbDyn),
        StrategySpec("cadysho", "other", Cadysho),
        StrategySpec("blueprint_howto", "other", BlueprintHowto),
        StrategySpec("long_gamma_move", "other", LongGammaMove),
        StrategySpec("pagoda", "other", Pagoda),
        StrategySpec("covered_call_put", "other", CoveredCallPut),
        StrategySpec("cal_spread_atm", "other", CalSpreadAtm),
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
