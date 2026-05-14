"""
services/repro.py — Reproducibility metadata helpers.

All functions return None / sensible defaults if the information is
unavailable (not a git checkout, config missing, etc.).
"""
import hashlib
import os
import subprocess

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.toml"
)


def git_sha() -> str | None:
    """Return HEAD commit SHA (short 12-char), or None if unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except Exception:
        pass
    return None


def git_dirty() -> bool | None:
    """Return True if the working tree has uncommitted changes, None if unavailable."""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            return bool(result.stdout.strip())
    except Exception:
        pass
    return None


def config_hash() -> str | None:
    """Return sha256 of backtester/config.toml, or None if unavailable."""
    try:
        with open(_CONFIG_PATH, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        return f"sha256:{digest}"
    except Exception:
        pass
    return None
