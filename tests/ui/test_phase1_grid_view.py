"""tests/ui/test_phase1_grid_view.py — grid_view helper tests."""
import pytest


def test_grid_dataframe_shape(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    df, hash_to_key = _grid_dataframe(tiny_grid_result)
    n_combos = len(tiny_grid_result.keys)
    assert len(df) == n_combos
    assert "rank" in df.columns
    assert "score" in df.columns
    # Every param name is a column
    for p in tiny_grid_result.param_names:
        assert p in df.columns
    assert "_key_hash" in df.columns


def test_grid_dataframe_sorted_by_score_desc(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    df, _ = _grid_dataframe(tiny_grid_result)
    scores = df["score"].tolist()
    assert scores == sorted(scores, reverse=True)


def test_grid_dataframe_rank_starts_at_1(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    df, _ = _grid_dataframe(tiny_grid_result)
    assert df["rank"].min() == 1
    assert list(df["rank"]) == list(range(1, len(df) + 1))


def test_hash_to_key_maps_correctly(tiny_grid_result):
    from backtester.ui.views.grid_view import _grid_dataframe
    from backtester.ui.services.store_service import key_hash
    df, hash_to_key = _grid_dataframe(tiny_grid_result)
    # Every row's _key_hash should resolve to a valid key
    for _, row in df.iterrows():
        kh = row["_key_hash"]
        assert kh in hash_to_key
        assert hash_to_key[kh] in tiny_grid_result.keys


def test_grid_dataframe_empty_result():
    from backtester.ui.views.grid_view import _grid_dataframe
    df, hash_to_key = _grid_dataframe(None)
    assert df.empty
    assert hash_to_key == {}
