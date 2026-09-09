"""probe_machine with mocked sysctl — no real Mac model asserts."""
from __future__ import annotations

from backtester.core import grid_workers as gw


def test_probe_cached(monkeypatch):
    gw.reset_machine_profile_cache()
    calls = {"n": 0}

    def fake_cpu():
        calls["n"] += 1
        return 8

    monkeypatch.setattr(gw.os, "cpu_count", fake_cpu)
    monkeypatch.setattr(gw, "_sysctl_int", lambda name: {"hw.physicalcpu": 8, "hw.perflevel0.physicalcpu": 4, "hw.memsize": 16 * 1024**3}.get(name))
    monkeypatch.setattr(gw, "_ram_gb", lambda: (16.0, 8.0))
    monkeypatch.setattr(gw, "_on_battery", lambda: False)
    monkeypatch.setattr(gw.platform, "system", lambda: "Darwin")

    a = gw.probe_machine()
    b = gw.probe_machine()
    assert a is b
    assert calls["n"] == 1
    assert a.performance_cpus == 4
    assert a.total_ram_gb == 16.0
    gw.reset_machine_profile_cache()
