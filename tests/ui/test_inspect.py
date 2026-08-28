"""tests/test_inspect.py — backtester.inspect fast lookup CLI."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backtester.inspect.cli import main
from backtester.inspect.resolve import (
    AmbiguousMatch,
    NotFound,
    filter_combos,
    hash_index,
    resolve_combo,
    resolve_run,
)
from backtester.ui.services.store_service import key_hash


@pytest.fixture
def seeded_store(sqlite_store, tiny_grid_result):
    bundle = sqlite_store.write_bundle(
        tiny_grid_result, strategy="inspect_tiny", runtime_s=0.1, source="test"
    )
    run_id = sqlite_store.register_bundle(bundle)
    return sqlite_store, run_id, bundle, tiny_grid_result


def test_resolve_run_by_id(seeded_store):
    store, run_id, bundle, _ = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    assert run.run_id == run_id
    assert run.bundle_path == bundle
    assert run.strategy == "inspect_tiny"
    assert run.n_combos == 3


def test_resolve_run_by_bundle_name(seeded_store):
    store, run_id, bundle, _ = seeded_store
    run = resolve_run(store, bundle.name, scan=False)
    assert run.run_id == run_id


def test_resolve_combo_by_hash_and_idx(seeded_store):
    store, run_id, _, result = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    h0 = key_hash(result.keys[0])
    c = resolve_combo(run, h0)
    assert c.combo_idx == 0
    assert c.combo_hash == h0
    c2 = resolve_combo(run, "#1")
    assert c2.combo_idx == 1
    assert c2.params["delta"] == 0.25


def test_resolve_combo_by_params(seeded_store):
    store, run_id, _, _ = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    c = resolve_combo(run, params={"delta": 0.30, "dte": 1})
    assert c.combo_idx == 2


def test_ambiguous_params(seeded_store):
    store, run_id, _, _ = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    with pytest.raises(AmbiguousMatch) as ei:
        resolve_combo(run, params={"dte": 1})
    assert len(ei.value.candidates) == 3


def test_hash_index_stable(seeded_store):
    store, run_id, _, result = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    hmap = hash_index(run.meta)
    for i, key in enumerate(result.keys):
        assert hmap[key_hash(key)] == i


def test_cli_combo_metrics(seeded_store, capsys):
    store, run_id, _, result = seeded_store
    h = key_hash(result.keys[1])
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "combo",
            str(run_id),
            h,
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["combo_hash"] == h
    assert out["combo_idx"] == 1
    assert "sharpe" in out["metrics"]
    assert "total_pnl" in out["metrics"]
    assert out["params"]["delta"] == 0.25


def test_cli_trades_filter(seeded_store, capsys):
    store, run_id, _, result = seeded_store
    h = key_hash(result.keys[0])
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "trades",
            str(run_id),
            h,
            "--pnl-lt",
            "0",
            "--limit",
            "5",
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "trades"
    assert out["n"] >= 1
    assert all(row["pnl"] < 0 for row in out["rows"])
    assert "schema" in out


def test_cli_show_and_runs(seeded_store, capsys):
    store, run_id, bundle, _ = seeded_store
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "show",
            str(run_id),
        ]
    )
    assert rc == 0
    show = json.loads(capsys.readouterr().out)
    assert show["bundle"] == bundle.name
    assert "trade_log.parquet" in show["files"]

    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "runs",
            "--strategy",
            "inspect_tiny",
        ]
    )
    assert rc == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["n"] >= 1
    assert any(r["run_id"] == run_id for r in listed["rows"])


def test_cli_not_found(seeded_store, capsys):
    store, _, _, _ = seeded_store
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "show",
            "999999",
        ]
    )
    assert rc == 1
    err = json.loads(capsys.readouterr().err)
    assert err["error"] == "NotFound"


def test_filter_combos_q(seeded_store):
    store, run_id, _, _ = seeded_store
    run = resolve_run(store, str(run_id), scan=False)
    hits = filter_combos(run, q="0.25")
    assert len(hits) == 1
    assert hits[0].params["delta"] == 0.25


def test_no_load_run_used(seeded_store, monkeypatch):
    """Inspect path must not call StoreService.load_run."""
    store, run_id, _, result = seeded_store

    def boom(*a, **k):
        raise AssertionError("load_run must not be called")

    monkeypatch.setattr(store, "load_run", boom)
    run = resolve_run(store, str(run_id), scan=False)
    c = resolve_combo(run, key_hash(result.keys[0]))
    from backtester.inspect.load import metrics_for_combos, read_trades

    metrics_for_combos(run, [c])
    df = read_trades(run, [c.combo_idx])
    assert not df.empty


def test_cli_ambiguous_exit_2(seeded_store, capsys):
    store, run_id, _, _ = seeded_store
    # Register a second bundle so fragment "inspect_tiny" is unique… use param ambiguity
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "combo",
            str(run_id),
            "--param",
            "dte=1",
        ]
    )
    assert rc == 2
    err = json.loads(capsys.readouterr().err)
    assert err["error"] == "AmbiguousMatch"
    assert len(err["candidates"]) == 3


def test_favourite_on_combo(seeded_store, capsys):
    store, run_id, _, result = seeded_store
    key = result.keys[0]
    store.add_favourite(
        run_id, key, name="Fav20", note="note", strategy="inspect_tiny"
    )
    h = key_hash(key)
    rc = main(
        [
            "--state-dir",
            str(store._state_dir),
            "--bundles-root",
            str(store._bundles_root),
            "combo",
            str(run_id),
            h,
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["favourite"]["name"] == "Fav20"
