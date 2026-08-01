"""Compatibility shim — canonical module: workspace.strategies.theta_engine.v7"""
import workspace.strategies.theta_engine.v7 as _impl

globals().update(
    {name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")}
)
