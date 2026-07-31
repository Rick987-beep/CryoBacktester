"""Compatibility shim — canonical module: workspace.strategies.other.blueprint_howto"""
import workspace.strategies.other.blueprint_howto as _impl

globals().update(
    {name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")}
)
