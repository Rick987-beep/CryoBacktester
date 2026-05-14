"""
tests/ui/test_phase5_csv_export.py — CSV export from the Results Grid.

Tests:
  - test_grid_dataframe_has_key_hash   — internal _key_hash column present
  - test_csv_drops_key_hash            — _key_hash excluded from export
  - test_csv_has_expected_columns      — fixed stat columns present
  - test_csv_has_all_combos            — row count matches result
  - test_csv_is_parseable              — output is valid CSV
  - test_prune_runs_dry_run            — prune dry-run returns list without deleting
"""
import io
import pytest


def test_grid_dataframe_has_key_hash(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    df, hash_to_key = _grid_dataframe(tiny_grid_result)
    assert "_key_hash" in df.columns
    assert len(hash_to_key) == 3


def test_csv_drops_key_hash(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    df, _ = _grid_dataframe(tiny_grid_result)
    csv_df = df.drop(columns=["_key_hash"], errors="ignore")
    assert "_key_hash" not in csv_df.columns


def test_csv_has_expected_columns(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    import pandas as pd
    df, _ = _grid_dataframe(tiny_grid_result)
    csv_df = df.drop(columns=["_key_hash"], errors="ignore")
    csv_str = csv_df.to_csv(index=False)
    parsed = pd.read_csv(io.StringIO(csv_str))
    for col in ("rank", "score", "total_pnl", "sharpe"):
        assert col in parsed.columns, f"Expected column '{col}' in CSV"


def test_csv_has_all_combos(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    import pandas as pd
    df, _ = _grid_dataframe(tiny_grid_result)
    csv_df = df.drop(columns=["_key_hash"], errors="ignore")
    parsed = pd.read_csv(io.StringIO(csv_df.to_csv(index=False)))
    assert len(parsed) == 3  # tiny_grid_result has 3 combos


def test_csv_is_parseable(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    import pandas as pd
    df, _ = _grid_dataframe(tiny_grid_result)
    csv_str = df.drop(columns=["_key_hash"], errors="ignore").to_csv(index=False)
    # Should not raise
    parsed = pd.read_csv(io.StringIO(csv_str))
    assert not parsed.empty


def test_prune_runs_dry_run_returns_empty_when_no_runs(sqlite_store):
    """dry_run=True with no matching runs returns an empty list."""
    result = sqlite_store.prune_runs(older_than_days=30, dry_run=True)
    assert result == []


def test_prune_runs_dry_run_does_not_delete(sqlite_store, tiny_grid_result):
    """dry_run=True returns candidates without actually deleting."""
    from datetime import datetime, timedelta, timezone
    import sqlite3

    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="prune_test", runtime_s=1.0
    )
    run_id = sqlite_store.register_bundle(bundle_path)

    # Backdate the run's created_at so it's old enough to prune
    old_ts = (datetime.now(timezone.utc) - timedelta(days=60)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    con = sqlite3.connect(str(sqlite_store._db_path))
    con.execute("UPDATE runs SET created_at = ? WHERE id = ?", (old_ts, run_id))
    con.commit()
    con.close()

    candidates = sqlite_store.prune_runs(older_than_days=30, dry_run=True)
    assert any(r.id == run_id for r in candidates)

    # Verify run still exists (dry_run did NOT delete)
    assert sqlite_store.get_run(run_id) is not None
    assert bundle_path.exists()


def test_prune_runs_skips_pinned(sqlite_store, tiny_grid_result):
    """Pinned runs are never pruned."""
    from datetime import datetime, timedelta, timezone
    import sqlite3

    bundle_path = sqlite_store.write_bundle(
        tiny_grid_result, strategy="pinned_test", runtime_s=1.0
    )
    run_id = sqlite_store.register_bundle(bundle_path)
    sqlite_store.set_pinned(run_id, True)

    old_ts = (datetime.now(timezone.utc) - timedelta(days=60)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    con = sqlite3.connect(str(sqlite_store._db_path))
    con.execute("UPDATE runs SET created_at = ? WHERE id = ?", (old_ts, run_id))
    con.commit()
    con.close()

    candidates = sqlite_store.prune_runs(older_than_days=30, dry_run=True)
    assert all(r.id != run_id for r in candidates)
