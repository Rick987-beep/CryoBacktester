"""
services/repro.py — Reproducibility metadata helpers.

All functions return None / sensible defaults if the information is
unavailable (not a git checkout, config missing, etc.).
"""
import hashlib
import inspect
import os
import shutil
import subprocess
from pathlib import Path

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


def snapshot_strategy_source(strategy_cls, bundle_dir: str | Path) -> dict | None:
    """Copy the strategy module .py file into *bundle_dir*/strategy/.

    Writes a byte-for-byte copy (not a symlink or repo path reference).
    Returns a manifest dict for meta.json, or None on failure.
    """
    try:
        src_path = Path(inspect.getfile(strategy_cls)).resolve()
        if not src_path.is_file():
            return None

        bundle_dir = Path(bundle_dir)
        dest_dir = bundle_dir / "strategy"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / src_path.name
        shutil.copy2(src_path, dest_path)

        data = dest_path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()

        try:
            repo_path = str(src_path.relative_to(Path(_REPO_ROOT).resolve()))
        except ValueError:
            repo_path = str(src_path)

        return {
            "module": strategy_cls.__module__,
            "repo_path": repo_path.replace("\\", "/"),
            "bundle_path": f"strategy/{src_path.name}",
            "sha256": f"sha256:{digest}",
            "size_bytes": len(data),
        }
    except Exception:
        return None
