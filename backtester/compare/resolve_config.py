"""Resolve CryoTrader slot TOML → backtester strategy + param grid."""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from backtester.compare.models import ParityWarning, RunSpec, WarningCode
from backtester.compare.io_utils import log_stage


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_strategy_map() -> dict:
    path = _repo_root() / "analysis/livecompare/config/strategy_map.yaml"
    return yaml.safe_load(path.read_text())


def _parse_toml_simple(path: Path) -> dict:
    """Minimal TOML parser for slot files (no external dep)."""
    try:
        import tomllib
        return tomllib.loads(path.read_text())
    except ImportError:
        pass
    # Fallback: regex sections for our slot format
    text = path.read_text()
    root: dict = {}
    current: dict = root
    stack: list[tuple[str, dict]] = []

    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        sec = re.match(r"^\[([^\]]+)\]$", line)
        if sec:
            parts = sec.group(1).split(".")
            current = root
            for p in parts:
                current = current.setdefault(p, {})
            continue
        m = re.match(r"^(\w+)\s*=\s*(.+)$", line)
        if m:
            key, raw = m.group(1), m.group(2).strip()
            if raw in ("true", "false"):
                current[key] = raw == "true"
            elif raw.startswith('"') and raw.endswith('"'):
                current[key] = raw[1:-1]
            else:
                try:
                    current[key] = float(raw) if "." in raw else int(raw)
                except ValueError:
                    current[key] = raw
    return root


def _schedule_keys(params: dict, prefix: str) -> List[str]:
    return sorted(
        k[len(prefix):]
        for k in params
        if k.startswith(prefix) and isinstance(params[k], dict)
    )


def resolve(spec: RunSpec) -> Tuple[dict, List[ParityWarning]]:
    smap = load_strategy_map()
    if not spec.slot_toml.exists():
        raise FileNotFoundError(f"Slot TOML not found: {spec.slot_toml}")

    slot = _parse_toml_simple(spec.slot_toml)
    live_strategy = slot.get("strategy", "")
    strategy_cfg = smap["strategies"].get(live_strategy)
    if not strategy_cfg:
        raise ValueError(f"No strategy_map entry for live strategy '{live_strategy}'")

    warnings: List[ParityWarning] = []
    params = slot.get("params", {})
    bt_strategy = strategy_cfg["bt_strategy"]
    bt_params: Dict[str, Any] = {}

    defaults = smap["defaults"]
    sizing_mode = spec.sizing_mode or defaults["sizing_mode"]
    if sizing_mode == "bt_default":
        bt_params.update(defaults["bt_sizing"])
        bt_params["equity_drawdown_stop_pct"] = 0
        bt_params["equity_sl_only_final_hours"] = 0
        bt_params["equity_sl_except_final_hours"] = 0
        warnings.append(ParityWarning(
            code=WarningCode.SIZING_DIFF,
            severity="warn",
            message="BT uses default sizing (0.8% NAV / 12 per BTC-equity); compare $/lot only",
            context={"live": {k: params.get(k) for k in defaults["live_sizing_keys"]}},
        ))
    else:
        for k in defaults["live_sizing_keys"]:
            if k in params and params[k] is not None:
                bt_params[k.replace("max_quantity", "max_qty_per_1btc_equity")] = params[k]

    warnings.append(ParityWarning(
        code=WarningCode.FILL_MODEL,
        severity="warn",
        message="BT requires bid>0; live min_qty_price_floor=0 allows mark fallback",
        context={"live_min_qty_price_floor": params.get("min_qty_price_floor")},
    ))

    if int(params.get("min_qty_price_floor", 0)) == 0:
        bt_params["leg_min_price"] = 0

    prefix = strategy_cfg.get("schedule_prefix", "schedule_")
    if strategy_cfg.get("multi_schedule"):
        for sched_id in _schedule_keys(params, prefix):
            sched = params[f"{prefix}{sched_id}"]
            override = dict(sched)
            # Normalize entry_time "1:00" → "01:00" style handled by strategy
            if sched.get("enabled") is False:
                disable = strategy_cfg.get("mon_early_disable", {})
                if sched_id == "mon_early" and disable.get("method") == "turbulence_threshold":
                    override["turbulence_threshold"] = disable["value"]
                    warnings.append(ParityWarning(
                        code=WarningCode.MON_EARLY_DISABLED,
                        severity="info",
                        message="mon_early disabled on live; BT uses turbulence_threshold=999",
                    ))
            bt_params[f"schedule_{sched_id}"] = override
    else:
        # Single-schedule tudysho-style: flatten first schedule or top-level
        for k, v in params.items():
            if not k.startswith(prefix) and k not in defaults["live_sizing_keys"]:
                if isinstance(v, (int, float, str, bool)):
                    bt_params[k] = v

    toml_hash = hashlib.sha256(spec.slot_toml.read_bytes()).hexdigest()[:12]
    resolved = {
        "slot": spec.slot_padded,
        "live_strategy": live_strategy,
        "bt_strategy": bt_strategy,
        "slot_toml": str(spec.slot_toml),
        "slot_toml_hash": toml_hash,
        "param_grid": {k: [v] for k, v in bt_params.items()},
        "bt_params_flat": bt_params,
        "sizing_mode": sizing_mode,
        "account_size_usd": spec.account_size_usd,
        "strategy_map_notes": strategy_cfg.get("notes", []),
    }
    log_stage("resolve_config", live_strategy=live_strategy, bt_strategy=bt_strategy)
    return resolved, warnings
