"""
experiment.py — Load and interpret backtester experiment TOML files.

An experiment file captures a specific research step (sensitivity analysis
or walk-forward validation) against a named strategy candidate.

File layout: backtester/experiments/<name>.toml

Usage:
    from backtester.experiment import load_experiment
    exp = load_experiment("delta_strangle_tp_v1")

    # Build the sensitivity grid
    grid = exp.build_sensitivity_grid()

    # Read WFO window params
    print(exp.wfo_is_days, exp.wfo_oos_days, exp.wfo_step_days)
"""
import math
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import tomllib

EXPERIMENTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "experiments")


# ── Data classes ─────────────────────────────────────────────────

@dataclass
class DeviationSpec:
    """How to perturb one parameter around its best-known value.

    type:
        "pct"   — ±amount % of the best value, evenly distributed over steps.
                  E.g. best=0.15, amount=10, steps=5 →
                  [0.135, 0.143, 0.15, 0.158, 0.165]
        "abs"   — ±amount in the parameter's natural unit, evenly distributed.
                  E.g. best=18, amount=2, steps=5 → [16, 17, 18, 19, 20]
        "fixed" — Do not perturb; always use [best_value].
    """
    type: str       # "pct" | "abs" | "fixed"
    amount: float = 0.0

    @classmethod
    def from_dict(cls, d):
        # type: (dict) -> "DeviationSpec"
        return cls(type=d["type"], amount=float(d.get("amount", 0.0)))


@dataclass
class Experiment:
    """Loaded experiment, ready to produce grids for sensitivity or WFO runs."""
    name: str
    strategy: str

    # Sensitivity
    sensitivity_best: Dict[str, Any]        # param → centre value
    sensitivity_steps: int                  # odd; centre counts as one step
    sensitivity_deviations: Dict[str, DeviationSpec]

    # Walk-forward
    wfo_is_days: int
    wfo_oos_days: int
    wfo_step_days: int

    def build_sensitivity_grid(self):
        # type: () -> Dict[str, List]
        """Generate the full sensitivity param grid from best + deviation specs.

        Each param with type="fixed" or with no spec gets [best_value].
        Each continuous param gets `sensitivity_steps` values centred on best.
        """
        grid = {}
        steps = self.sensitivity_steps
        for param, best_val in self.sensitivity_best.items():
            spec = self.sensitivity_deviations.get(param)
            if spec is None or spec.type == "fixed":
                grid[param] = [best_val]
            else:
                grid[param] = _build_range(best_val, spec.type, spec.amount, steps)
        return grid

    def describe(self):
        # type: () -> str
        """Human-readable summary of the experiment grid."""
        grid = self.build_sensitivity_grid()
        lines = [
            f"Experiment: {self.name}  (strategy: {self.strategy})",
            f"Sensitivity steps: {self.sensitivity_steps}",
        ]
        for param, vals in grid.items():
            centre = self.sensitivity_best.get(param)
            spec = self.sensitivity_deviations.get(param)
            dev_label = (
                f"fixed"
                if spec is None or spec.type == "fixed"
                else f"{spec.type} ±{spec.amount}"
            )
            lines.append(f"  {param:22s} = {vals}  [{dev_label}, centre={centre}]")
        n_combos = 1
        for v in grid.values():
            n_combos *= len(v)
        lines.append(f"Total combos: {n_combos:,}")
        lines.append(
            f"WFO: IS={self.wfo_is_days}d / OOS={self.wfo_oos_days}d / "
            f"step={self.wfo_step_days}d"
        )
        return "\n".join(lines)


# ── Grid generator ───────────────────────────────────────────────

def _build_range(best, dev_type, amount, steps):
    # type: (Any, str, float, int) -> List
    """Generate `steps` values centred on `best` with the given deviation rule.

    Returns a deduplicated list; if rounding causes fewer than `steps` unique
    values, the list will be shorter (avoids silent duplicates in the grid).
    """
    if steps <= 1:
        return [best]

    half = (steps - 1) / 2.0

    if dev_type == "pct":
        spacing = (amount / 100.0) * abs(float(best)) / half if half > 0 else 0.0
    elif dev_type == "abs":
        spacing = float(amount) / half if half > 0 else 0.0
    else:
        raise ValueError(f"Unknown deviation type: {dev_type!r}. Use 'pct', 'abs', or 'fixed'.")

    raw = [float(best) + (i - half) * spacing for i in range(steps)]

    # Determine rounding precision from best value and spacing
    n_dec = _infer_decimals(best, spacing)

    cleaned = []
    seen = set()
    for v in raw:
        if n_dec == 0:
            v_r = int(round(v))         # type: Any
        else:
            v_r = round(v, n_dec)
        if v_r not in seen:
            seen.add(v_r)
            cleaned.append(v_r)

    return cleaned


def _infer_decimals(best, spacing):
    # type: (Any, float) -> int
    """Decide how many decimal places to round generated values to.

    Rules:
    - If best is an integer AND spacing >= 1.0 → 0 (integer output)
    - Otherwise: max(decimal places of best, decimal places of spacing), capped at 4
    """
    best_is_int = isinstance(best, int) or (
        isinstance(best, float) and best == math.floor(best)
    )
    if best_is_int and spacing >= 1.0 - 1e-9:
        return 0

    def _count(v):
        # type: (float) -> int
        s = f"{v:.8f}".rstrip("0")
        return len(s.split(".")[1]) if "." in s else 0

    return min(max(_count(float(best)), _count(spacing)), 4)


# ── Loader ───────────────────────────────────────────────────────

def load_experiment(name):
    # type: (str) -> Experiment
    """Load an experiment by name (without the .toml extension).

    Searches in backtester/experiments/<name>.toml.
    """
    path = os.path.join(EXPERIMENTS_DIR, f"{name}.toml")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Experiment file not found: {path}\n"
            f"Expected location: backtester/experiments/{name}.toml"
        )
    with open(path, "rb") as f:
        data = tomllib.load(f)

    sens = data.get("sensitivity", {})
    wfo = data.get("wfo", {})

    deviations = {}
    for param, spec_dict in sens.get("deviation", {}).items():
        deviations[param] = DeviationSpec.from_dict(spec_dict)

    return Experiment(
        name=name,
        strategy=data["strategy"],
        sensitivity_best=sens.get("best", {}),
        sensitivity_steps=int(sens.get("steps", 5)),
        sensitivity_deviations=deviations,
        wfo_is_days=int(wfo.get("is_days", 45)),
        wfo_oos_days=int(wfo.get("oos_days", 15)),
        wfo_step_days=int(wfo.get("step_days", 15)),
    )
