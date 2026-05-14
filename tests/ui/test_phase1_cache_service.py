"""tests/ui/test_phase1_cache_service.py — ResultCache tests."""
import pytest

from backtester.ui.services.cache_service import ResultCache


class _FakeStore:
    """Minimal store stub for cache tests."""
    def __init__(self, results: dict):
        self._results = results
        self.load_calls: list[int] = []
        self._pinned: dict[int, bool] = {}

    def load_run(self, run_id: int):
        self.load_calls.append(run_id)
        if run_id not in self._results:
            raise KeyError(run_id)
        return self._results[run_id]

    def set_pinned(self, run_id: int, pinned: bool):
        self._pinned[run_id] = pinned


def _make_store(n: int) -> _FakeStore:
    """Create a FakeStore with n dummy results (objects with different identity)."""
    return _FakeStore({i: object() for i in range(n)})


def test_get_loads_on_miss():
    store = _make_store(3)
    cache = ResultCache(store, max_unpinned=5)
    r = cache.get(0)
    assert r is store._results[0]
    assert store.load_calls == [0]


def test_get_cached_on_second_call():
    store = _make_store(3)
    cache = ResultCache(store, max_unpinned=5)
    r1 = cache.get(1)
    r2 = cache.get(1)
    assert r1 is r2
    assert store.load_calls.count(1) == 1  # loaded only once


def test_lru_evicts_after_max():
    store = _make_store(10)
    cache = ResultCache(store, max_unpinned=5)
    for i in range(7):
        cache.get(i)
    # Only the 5 most recently accessed (2-6) should remain in memory
    assert 0 not in cache.cached_ids()
    assert 1 not in cache.cached_ids()
    for i in range(2, 7):
        assert i in cache.cached_ids()


def test_pinned_not_evicted():
    store = _make_store(10)
    cache = ResultCache(store, max_unpinned=5)
    cache.pin(0)  # pin run 0
    for i in range(1, 8):
        cache.get(i)  # fill + overflow unpinned
    assert 0 in cache.cached_ids()          # still here
    assert 0 in cache.pinned_ids()


def test_unpin_moves_to_lru():
    store = _make_store(5)
    cache = ResultCache(store, max_unpinned=5)
    cache.pin(0)
    cache.unpin(0)
    assert 0 not in cache.pinned_ids()
    assert 0 in cache.cached_ids()


def test_evict_removes_from_memory():
    store = _make_store(3)
    cache = ResultCache(store, max_unpinned=5)
    cache.get(0)
    cache.evict(0)
    assert 0 not in cache.cached_ids()
    # A subsequent get re-loads from store
    cache.get(0)
    assert store.load_calls.count(0) == 2
