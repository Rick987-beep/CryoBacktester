"""Tests for backtester.research.run_audit."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backtester.research.run_audit.candidates import LivePickConfig, select_live_picks
from backtester.research.run_audit.compute import audit_run
from backtester.research.run_audit.curve_fit import economic_fingerprint, grid_summary
from backtester.research.run_audit.influence import eta_squared, param_influence
from backtester.research.run_audit.render import render_html


@pytest.fixture
def seeded_store(sqlite_store, tiny_grid_result):
    bundle = sqlite_store.write_bundle(
        tiny_grid_result, strategy="inspect_tiny", runtime_s=0.1, source="test"
    )
    run_id = sqlite_store.register_bundle(bundle)
    return sqlite_store, run_id, bundle, tiny_grid_result


def _synthetic_frame(n_a: int = 3, n_b: int = 4, seed: int = 0) -> pd.DataFrame:
    """Small factorial grid: param_a × param_b with known Sharpe structure."""
    rng = np.random.default_rng(seed)
    rows = []
    idx = 0
    for a in range(n_a):
        for b in range(n_b):
            # Sharpe driven mostly by param_a
            sharpe = 1.0 + 2.0 * a + 0.1 * b + rng.normal(0, 0.05)
            pnl = 1000 * sharpe + rng.normal(0, 50)
            n_loss = 0 if a == n_a - 1 and b == 0 else 2 + (b % 2)
            n = 50 + b
            wr = 1.0 if n_loss == 0 else (n - n_loss) / n
            rows.append(
                {
                    "combo_idx": idx,
                    "combo_hash": f"{idx:012x}",
                    "param_a": float(a),
                    "param_b": float(b),
                    "n": n,
                    "n_win": n - n_loss,
                    "n_loss": n_loss,
                    "total_pnl": pnl,
                    "win_rate": wr,
                    "profit_factor": 2.0 + 0.1 * a,
                    "sharpe": sharpe,
                    "max_dd_pct": 5.0 + b,
                    "ann_return": 0.1 + 0.05 * a,
                    "calmar": 2.0,
                    "avg_win": 100.0,
                    "avg_loss": -300.0 if n_loss else 0.0,
                    "loss_win_ratio": 3.0 if n_loss else np.nan,
                    "worst_trade": -400.0 if n_loss else 50.0,
                    "best_trade": 200.0,
                    "pnl_h1": pnl * 0.45,
                    "pnl_h2": pnl * 0.55,
                    "both_halves_profit": True,
                    "perfect_wr": n_loss == 0,
                    "exits": {"expiry": wr, "stop_loss": 1 - wr},
                }
            )
            idx += 1
    return pd.DataFrame(rows)


def test_eta_squared_prefers_driving_factor():
    df = _synthetic_frame()
    eta_a = eta_squared(df["sharpe"], df["param_a"])
    eta_b = eta_squared(df["sharpe"], df["param_b"])
    assert eta_a > eta_b
    assert eta_a > 0.8


def test_param_influence_orders_by_eta():
    df = _synthetic_frame()
    inf = param_influence(df, ["param_a", "param_b"])
    assert inf[0]["param"] == "param_a"
    assert inf[0]["eta_sharpe"] > inf[1]["eta_sharpe"]


def test_grid_summary_unique_outcomes():
    df = _synthetic_frame()
    # Duplicate last row economically
    twin = df.iloc[-1].copy()
    twin["combo_idx"] = 999
    twin["combo_hash"] = "deadbeef0001"
    twin["param_b"] = 99.0  # different param, same economics
    df2 = pd.concat([df, pd.DataFrame([twin])], ignore_index=True)
    fp = economic_fingerprint(df2)
    assert fp.nunique() < len(df2)
    summary = grid_summary(df2, "2025-06-01")
    assert summary["n_combos"] == len(df2)
    assert summary["n_unique_economic_outcomes"] == int(fp.nunique())


def test_live_picks_exclude_perfect_wr_and_diversify():
    df = _synthetic_frame()
    out = select_live_picks(
        df,
        ["param_a", "param_b"],
        LivePickConfig(
            min_n=40,
            min_n_loss=2,
            max_win_rate=0.97,
            max_dd_pct=20,
            min_sharpe=0.5,
            min_profit_factor=1.0,
            n_picks=3,
            min_param_distance=1,
        ),
    )
    assert out["pool_size"] >= 1
    for p in out["picks"]:
        assert p["n_loss"] >= 2
        assert p["win_rate"] <= 0.97
    # Diversity on params when multiple picks
    if len(out["picks"]) >= 2:
        a0 = out["picks"][0]["params"]
        a1 = out["picks"][1]["params"]
        assert a0 != a1


def test_render_html_contains_four_sections():
    df = _synthetic_frame()
    pack = {
        "schema_version": 1,
        "meta": {
            "run_id": 1,
            "bundle": "toy.bundle",
            "strategy": "toy",
            "date_from": "2025-01-01",
            "date_to": "2025-06-01",
            "n_combos": len(df),
            "account_size": 100000,
        },
        "grid_summary": grid_summary(df, "2025-03-15"),
        "influence_bar": [
            {"param": "param_a", "eta_sharpe": 0.9, "eta_pnl": 0.8, "eta_dd": 0.1}
        ],
        "influence": [
            {
                "param": "param_a",
                "eta_sharpe": 0.9,
                "eta_pnl": 0.8,
                "eta_dd": 0.1,
                "levels": [
                    {
                        "level": 0,
                        "med_sharpe": 1.0,
                        "med_pnl": 1000,
                        "med_dd": 5,
                        "p95_dd": 8,
                        "med_n": 50,
                    }
                ],
            }
        ],
        "danger_rank": [],
        "danger_verdict": {"headline": "test danger"},
        "curve_fit": {
            "verdict": {"level": "MODERATE", "score": 3, "evidence": ["e1"]}
        },
        "live_candidates": {
            "pool_size": 1,
            "note": "note",
            "picks": [
                {
                    "archetype": "A",
                    "combo_hash": "abc",
                    "params": {"param_a": 1.0},
                    "total_pnl": 1000,
                    "sharpe": 2.0,
                    "max_dd_pct": 5,
                    "win_rate": 0.9,
                    "n_loss": 2,
                    "n": 50,
                    "pnl_h1": 400,
                    "pnl_h2": 600,
                }
            ],
        },
    }
    html = render_html(pack)
    assert "Parameter influence" in html
    assert "Most dangerous" in html
    assert "Curve-fitting" in html
    assert "Live candidates" in html
    assert "abc" in html


def test_audit_run_on_seeded_bundle(seeded_store, tmp_path):
    store, run_id, bundle, result = seeded_store
    from backtester.inspect.resolve import resolve_run

    run = resolve_run(store, str(run_id), scan=False)
    pack = audit_run(run, live_cfg=LivePickConfig(min_n=1, min_n_loss=0, max_win_rate=1.0, min_sharpe=-5, min_profit_factor=0, require_both_halves=False, n_picks=2, min_param_distance=1))
    assert pack["meta"]["run_id"] == run_id
    assert pack["meta"]["n_combos"] == 3
    assert "delta" in pack["meta"]["varying_params"]
    assert pack["grid_summary"]["n_combos"] == 3
    assert len(pack["influence"]) >= 1
    assert "questions" in pack
    html = render_html(pack)
    out = tmp_path / "report.html"
    out.write_text(html)
    assert out.stat().st_size > 100


def test_cli_audit_writes_json(seeded_store, tmp_path):
    store, run_id, bundle, _ = seeded_store
    from backtester.research.run_audit.cli import run_audit_cli

    out_dir = tmp_path / "audit_out"
    rc = run_audit_cli(
        [
            str(run_id),
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "--out-dir",
            str(out_dir),
            "--html",
            "--min-n",
            "1",
            "--min-n-loss",
            "0",
            "--max-win-rate",
            "1.0",
            "--min-sharpe",
            "-5",
            "--min-profit-factor",
            "0",
            "--no-both-halves",
        ]
    )
    assert rc == 0
    data = json.loads((out_dir / "audit.json").read_text())
    assert data["schema_version"] == 1
    assert (out_dir / "report.html").is_file()


def test_inspect_audit_subcommand(seeded_store, tmp_path):
    store, run_id, _, _ = seeded_store
    from backtester.inspect.cli import main

    out_dir = tmp_path / "via_inspect"
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "audit",
            str(run_id),
            "--out-dir",
            str(out_dir),
            "--min-n",
            "1",
            "--min-n-loss",
            "0",
            "--allow-perfect-wr",
            "--min-sharpe",
            "-5",
            "--min-profit-factor",
            "0",
            "--no-both-halves",
        ]
    )
    assert rc == 0
    assert (out_dir / "audit.json").is_file()
