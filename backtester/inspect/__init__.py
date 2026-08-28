"""Fast run / combo retrieval for agents and humans.

Use ``python -m backtester.inspect`` — do not glob ``data/runs/`` or call
``StoreService.load_run()`` for lookup.
"""

from backtester.inspect.resolve import (
    AmbiguousMatch,
    NotFound,
    ResolveError,
    default_store,
)

__all__ = [
    "AmbiguousMatch",
    "NotFound",
    "ResolveError",
    "default_store",
]
