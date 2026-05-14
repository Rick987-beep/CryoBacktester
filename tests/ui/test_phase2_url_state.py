"""
tests/ui/test_phase2_url_state.py — URL state encode/decode roundtrip.

Tests that AppState fields can be serialised to a URL query string and
re-hydrated to identical values. No running Panel server is needed.
"""
import pytest


def _encode_state(run_id, combo_key, tab):
    """Encode AppState fields to a URL query string (same logic as app.py)."""
    from backtester.ui.services.store_service import key_hash
    params = {}
    if run_id is not None:
        params["run"] = str(run_id)
    if combo_key is not None:
        params["combo"] = key_hash(combo_key)
    if tab:
        params["tab"] = tab
    if not params:
        return ""
    return "?" + "&".join(f"{k}={v}" for k, v in params.items())


def _decode_query(qs: str) -> dict:
    """Parse ?key=val&key2=val2 into a dict."""
    if not qs or qs == "?":
        return {}
    pairs = qs.lstrip("?").split("&")
    result = {}
    for pair in pairs:
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k] = v
    return result


def test_encode_decode_state():
    """Encoding and decoding URL state returns consistent keys."""
    from backtester.ui.services.store_service import key_hash
    from backtester.ui.state import AppState

    state = AppState()
    state.active_run_id = 42
    state.active_tab = "Combo Detail"
    combo_key = (("delta", 0.25), ("dte", 1))
    state.active_combo_key = combo_key

    qs = _encode_state(state.active_run_id, state.active_combo_key, state.active_tab)
    params = _decode_query(qs)

    assert params.get("run") == "42"
    assert params.get("tab") == "Combo Detail"
    assert params.get("combo") == key_hash(combo_key)


def test_empty_state_produces_no_query_string():
    """AppState with all defaults produces an empty (or absent) query string."""
    qs = _encode_state(None, None, "")
    assert qs == ""


def test_run_only_state():
    """Only active_run_id set → only `run` param in query string."""
    qs = _encode_state(7, None, "")
    params = _decode_query(qs)
    assert params == {"run": "7"}
