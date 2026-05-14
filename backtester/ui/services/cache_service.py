"""
services/cache_service.py — In-memory LRU cache of GridResult objects.

Pinned runs never evict.  Unpinned runs evict LRU-style when the cache
exceeds max_unpinned.

Usage:
    cache = ResultCache(store, max_unpinned=5)
    result = cache.get(run_id)   # loads from store on miss
    cache.pin(run_id)
    cache.unpin(run_id)
"""
from collections import OrderedDict

from backtester.ui.log import get_ui_logger

log = get_ui_logger(__name__)


class ResultCache:
    """LRU cache of run_id → GridResult.

    Pinned entries are kept in a separate dict and never evicted.
    Unpinned entries are held in an OrderedDict (insertion=access order).
    """

    def __init__(self, store, max_unpinned: int = 5):
        self._store = store
        self._max_unpinned = max_unpinned
        self._pinned: dict = {}        # run_id → GridResult
        self._unpinned: OrderedDict = OrderedDict()   # run_id → GridResult (LRU)

    # ── Public API ───────────────────────────────────────────────────────────

    def get(self, run_id: int):
        """Return the GridResult for *run_id*, loading from store on miss."""
        if run_id in self._pinned:
            return self._pinned[run_id]
        if run_id in self._unpinned:
            # Move to end (most recently used)
            self._unpinned.move_to_end(run_id)
            return self._unpinned[run_id]
        # Cache miss — load from store
        log.debug("cache miss run_id=%d — loading from store", run_id)
        result = self._store.load_run(run_id)
        self._insert_unpinned(run_id, result)
        return result

    def pin(self, run_id: int):
        """Pin *run_id* so it is never evicted."""
        if run_id in self._pinned:
            return
        result = self._unpinned.pop(run_id, None)
        if result is None:
            result = self._store.load_run(run_id)
        self._pinned[run_id] = result
        self._store.set_pinned(run_id, True)
        log.debug("pinned run_id=%d", run_id)

    def unpin(self, run_id: int):
        """Move *run_id* back to the unpinned LRU pool."""
        result = self._pinned.pop(run_id, None)
        if result is not None:
            self._insert_unpinned(run_id, result)
        self._store.set_pinned(run_id, False)
        log.debug("unpinned run_id=%d", run_id)

    def pinned_ids(self) -> list[int]:
        return list(self._pinned.keys())

    def evict(self, run_id: int):
        """Remove *run_id* from the cache (does not delete from store)."""
        self._pinned.pop(run_id, None)
        self._unpinned.pop(run_id, None)

    def cached_ids(self) -> set[int]:
        return set(self._pinned) | set(self._unpinned)

    # ── Internal ─────────────────────────────────────────────────────────────

    def _insert_unpinned(self, run_id: int, result):
        self._unpinned[run_id] = result
        self._unpinned.move_to_end(run_id)
        # Evict oldest unpinned if over capacity
        while len(self._unpinned) > self._max_unpinned:
            evicted_id, _ = self._unpinned.popitem(last=False)
            log.debug("LRU evicted run_id=%d from memory", evicted_id)
