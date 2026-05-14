"""tests/ui/test_phase1_repro.py — repro.py tests."""
from backtester.ui.services import repro


def test_git_sha_returns_hex_or_none():
    sha = repro.git_sha()
    assert sha is None or (isinstance(sha, str) and len(sha) > 0)
    if sha is not None:
        # Should be hex characters only
        int(sha, 16)


def test_config_hash_stable():
    h1 = repro.config_hash()
    h2 = repro.config_hash()
    assert h1 == h2
    if h1 is not None:
        assert h1.startswith("sha256:")


def test_git_dirty_bool_or_none():
    dirty = repro.git_dirty()
    assert dirty is None or isinstance(dirty, bool)
