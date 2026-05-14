"""
services/store_service.py — Run bundle persistence (SQLite + Parquet).

A "run bundle" is a directory under bundles_root containing:
  trade_log.parquet   — engine's df (trade log)
  nav_daily.parquet   — engine's nav_daily_df
  final_nav.parquet   — engine's final_nav_df
  fills.parquet       — engine's df_fills (optional)
  meta.json           — strategy, param_grid, keys, date_range, repro fields

SQLite (state_dir/ui_state.db) stores a lightweight index of runs for fast
listing.  The DB is rebuilt automatically from bundle dirs on first scan.

Thread safety: a module-level Lock guards all SQLite writes.
"""
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from backtester.ui.log import get_ui_logger
from backtester.ui.services import repro as _repro

log = get_ui_logger(__name__)

_WRITE_LOCK = threading.Lock()


# ── Public data types ─────────────────────────────────────────────────────────

class RunRow(NamedTuple):
    id: int
    created_at: str
    strategy: str
    param_grid_json: str
    date_from: str | None
    date_to: str | None
    n_combos: int | None
    n_trades: int | None
    runtime_s: float | None
    bundle_path: str
    pinned: bool
    label: str | None
    git_sha: str | None
    git_dirty: bool | None
    config_hash: str | None


class FavRow(NamedTuple):
    id: int
    run_id: int
    combo_hash: str
    combo_key_json: str
    name: str
    strategy: str
    note: str
    score: float | None
    sharpe: float | None
    total_pnl: float | None
    params_str: str
    added_at: str


# ── Schema ────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,
    strategy         TEXT NOT NULL,
    param_grid_json  TEXT NOT NULL,
    date_from        TEXT,
    date_to          TEXT,
    n_combos         INTEGER,
    n_trades         INTEGER,
    runtime_s        REAL,
    bundle_path      TEXT NOT NULL UNIQUE,
    pinned           INTEGER NOT NULL DEFAULT 0,
    label            TEXT,
    git_sha          TEXT,
    git_dirty        INTEGER,
    config_hash      TEXT
);
"""

_DDL_FAVOURITES = """
CREATE TABLE IF NOT EXISTS favourites (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL,
    combo_hash      TEXT NOT NULL,
    combo_key_json  TEXT NOT NULL,
    name            TEXT NOT NULL DEFAULT '',
    strategy        TEXT NOT NULL DEFAULT '',
    note            TEXT NOT NULL DEFAULT '',
    score           REAL,
    sharpe          REAL,
    total_pnl       REAL,
    params_str      TEXT NOT NULL DEFAULT '',
    added_at        TEXT NOT NULL,
    UNIQUE(run_id, combo_hash)
);
"""

_DDL_COLUMN_PRESETS = """
CREATE TABLE IF NOT EXISTS column_presets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy    TEXT NOT NULL,
    param_hash  TEXT NOT NULL,
    hidden_json TEXT NOT NULL DEFAULT '[]',
    updated_at  TEXT NOT NULL,
    UNIQUE(strategy, param_hash)
);
"""

_DDL_USER_PREFS = """
CREATE TABLE IF NOT EXISTS user_prefs (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# ── Key serialisation helpers ─────────────────────────────────────────────────

def key_to_json(key: tuple) -> str:
    """Serialise a param-tuple to a JSON string.  Stable across calls."""
    return json.dumps([[k, v] for k, v in key], separators=(",", ":"))


def key_from_json(s: str) -> tuple:
    """Deserialise a JSON string back to a param-tuple."""
    return tuple((k, v) for k, v in json.loads(s))


# ── Key hashing (for UI row identity) ────────────────────────────────────────

import hashlib as _hashlib


def key_hash(key: tuple) -> str:
    """Return a stable 12-char hex hash of *key* for use as a row identifier."""
    return _hashlib.sha256(key_to_json(key).encode()).hexdigest()[:12]


# ── StoreService ──────────────────────────────────────────────────────────────

class StoreService:
    """Persistent store for run bundles and their SQLite metadata."""

    def __init__(self, state_dir: str | Path, bundles_root: str | Path):
        self._state_dir = Path(state_dir)
        self._bundles_root = Path(bundles_root)
        self._db_path = self._state_dir / "ui_state.db"
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._bundles_root.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── DB bootstrap ────────────────────────────────────────────────────────

    def _init_db(self):
        con = self._connect()
        con.execute(_DDL)
        con.execute(_DDL_FAVOURITES)
        con.execute(_DDL_COLUMN_PRESETS)
        con.execute(_DDL_USER_PREFS)
        con.commit()
        con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self._db_path), check_same_thread=False)
        con.row_factory = sqlite3.Row
        return con

    # ── Bundle writing ───────────────────────────────────────────────────────

    def write_bundle(self, grid_result, strategy: str, runtime_s: float,
                     source: str = "cli", wfo_result=None) -> Path:
        """Persist *grid_result* as a run bundle directory.

        Returns the path to the newly created bundle dir.
        """
        from backtester.engine import _grid_combos  # noqa: avoid circular at module level

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bundle_dir = self._bundles_root / f"{strategy}_{ts}.bundle"
        bundle_dir.mkdir(parents=True, exist_ok=True)

        # --- Parquets ---
        grid_result.df.to_parquet(bundle_dir / "trade_log.parquet", index=False)
        grid_result.nav_daily_df.to_parquet(bundle_dir / "nav_daily.parquet", index=False)
        grid_result.final_nav_df.to_parquet(bundle_dir / "final_nav.parquet", index=False)
        if grid_result.df_fills is not None and not grid_result.df_fills.empty:
            grid_result.df_fills.to_parquet(bundle_dir / "fills.parquet", index=False)

        # --- meta.json ---
        # Keys: list of [[param, value], ...] lists (JSON-serialisable)
        keys_serial = [[[k, v] for k, v in key] for key in grid_result.keys]
        meta = {
            "strategy": strategy,
            "param_grid": grid_result.param_grid,
            "keys": keys_serial,
            "date_range": list(grid_result.date_range),
            "account_size": float(grid_result.account_size),
            "runtime_s": float(runtime_s),
            "source": source,
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "n_combos": len(grid_result.keys),
            "n_trades": int(len(grid_result.df)),
            "git_sha": _repro.git_sha(),
            "git_dirty": _repro.git_dirty(),
            "config_hash": _repro.config_hash(),
        }
        if wfo_result is not None:
            try:
                meta["wfo_result"] = _serialize_wfo_result(wfo_result)
            except Exception as exc:
                log.warning("Could not serialize wfo_result: %s", exc)
        (bundle_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        log.info("Bundle written: %s", bundle_dir)
        return bundle_dir

    # ── Bundle registration ──────────────────────────────────────────────────

    def register_bundle(self, bundle_path: str | Path) -> int:
        """Insert a runs row for *bundle_path*.  Idempotent.

        Returns the run id (existing or newly inserted).
        """
        bundle_path = Path(bundle_path)
        meta_file = bundle_path / "meta.json"
        if not meta_file.exists():
            raise FileNotFoundError(f"meta.json not found in {bundle_path}")
        meta = json.loads(meta_file.read_text())

        with _WRITE_LOCK:
            con = self._connect()
            try:
                # Idempotency check
                row = con.execute(
                    "SELECT id FROM runs WHERE bundle_path = ?",
                    (str(bundle_path),),
                ).fetchone()
                if row:
                    return int(row["id"])

                dr = meta.get("date_range") or [None, None]
                con.execute(
                    """INSERT INTO runs
                       (created_at, strategy, param_grid_json, date_from, date_to,
                        n_combos, n_trades, runtime_s, bundle_path,
                        git_sha, git_dirty, config_hash)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        meta.get("created_at",
                                 datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
                        meta.get("strategy", "unknown"),
                        json.dumps(meta.get("param_grid", {})),
                        dr[0] if dr else None,
                        dr[1] if len(dr) > 1 else None,
                        meta.get("n_combos"),
                        meta.get("n_trades"),
                        meta.get("runtime_s"),
                        str(bundle_path),
                        meta.get("git_sha"),
                        1 if meta.get("git_dirty") else 0,
                        meta.get("config_hash"),
                    ),
                )
                con.commit()
                run_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
                log.debug("Registered bundle id=%d %s", run_id, bundle_path)
                return int(run_id)
            finally:
                con.close()

    # ── Bundle scanning ──────────────────────────────────────────────────────

    def scan_bundles(self) -> list[int]:
        """Scan bundles_root for *.bundle dirs; register any new ones.

        Returns list of all run ids (including pre-existing).
        """
        ids = []
        for entry in sorted(self._bundles_root.iterdir()):
            if entry.is_dir() and entry.name.endswith(".bundle"):
                try:
                    rid = self.register_bundle(entry)
                    ids.append(rid)
                except Exception as exc:
                    log.warning("scan_bundles: skipping %s — %s", entry, exc)
        return ids

    # ── Listing ──────────────────────────────────────────────────────────────

    def list_runs(self) -> list[RunRow]:
        """Return all runs ordered by created_at DESC."""
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT * FROM runs ORDER BY created_at DESC"
            ).fetchall()
            return [_row_to_run_row(r) for r in rows]
        finally:
            con.close()

    def get_run(self, run_id: int) -> RunRow | None:
        """Return a single RunRow by id, or None if not found."""
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            return _row_to_run_row(row) if row else None
        finally:
            con.close()

    def set_pinned(self, run_id: int, pinned: bool):
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute(
                    "UPDATE runs SET pinned = ? WHERE id = ?",
                    (1 if pinned else 0, run_id)
                )
                con.commit()
            finally:
                con.close()

    def set_label(self, run_id: int, label: str | None):
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute("UPDATE runs SET label = ? WHERE id = ?", (label, run_id))
                con.commit()
            finally:
                con.close()

    # ── GridResult loading ───────────────────────────────────────────────────

    def load_run(self, run_id: int):
        """Load a GridResult from its bundle parquets.

        Returns a fresh GridResult.  Recomputes all stats (~1 s on 10k combos).
        """
        from backtester.results import GridResult

        row = self.get_run(run_id)
        if row is None:
            raise KeyError(f"run_id {run_id} not found")

        bundle_path = Path(row.bundle_path)
        meta = json.loads((bundle_path / "meta.json").read_text())

        df = pd.read_parquet(bundle_path / "trade_log.parquet")
        nav_daily_df = pd.read_parquet(bundle_path / "nav_daily.parquet")
        final_nav_df = pd.read_parquet(bundle_path / "final_nav.parquet")
        fills_path = bundle_path / "fills.parquet"
        df_fills = pd.read_parquet(fills_path) if fills_path.exists() else None

        # Reconstruct keys from stored key lists (preserves original types)
        keys = [tuple((k, v) for k, v in kv_list)
                for kv_list in meta["keys"]]

        param_grid = meta["param_grid"]
        account_size = meta.get("account_size", 10000.0)
        date_range = tuple(meta.get("date_range", [None, None]))

        log.debug("Loading run id=%d from %s", run_id, bundle_path)
        result = GridResult(
            df, keys, nav_daily_df, final_nav_df,
            param_grid=param_grid,
            account_size=account_size,
            date_range=date_range,
            df_fills=df_fills,
        )
        return result

    # ── Favourites ───────────────────────────────────────────────────────────

    def add_favourite(
        self,
        run_id: int,
        combo_key,
        name: str = "",
        note: str = "",
        score: float | None = None,
        sharpe: float | None = None,
        total_pnl: float | None = None,
        params_str: str = "",
        strategy: str = "",
    ) -> int:
        """Star a combo.  Returns the new favourite id.

        Raises sqlite3.IntegrityError if already favourited (run_id, combo_hash pair).
        """
        combo_hash = key_hash(combo_key)
        combo_key_json = key_to_json(combo_key)
        added_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute(
                    """INSERT INTO favourites
                       (run_id, combo_hash, combo_key_json, name, strategy, note,
                        score, sharpe, total_pnl, params_str, added_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (run_id, combo_hash, combo_key_json, name, strategy, note,
                     score, sharpe, total_pnl, params_str, added_at),
                )
                con.commit()
                fav_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
                return int(fav_id)
            finally:
                con.close()

    def list_favourites(self) -> list["FavRow"]:
        """Return all favourites ordered by added_at DESC."""
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT * FROM favourites ORDER BY added_at DESC"
            ).fetchall()
            return [_row_to_fav_row(r) for r in rows]
        finally:
            con.close()

    def remove_favourite(self, fav_id: int) -> None:
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute("DELETE FROM favourites WHERE id = ?", (fav_id,))
                con.commit()
            finally:
                con.close()

    def update_favourite(self, fav_id: int, **fields) -> None:
        """Update mutable fields (name, note, etc.) by keyword argument."""
        allowed = {"name", "note", "score", "sharpe", "total_pnl", "params_str", "strategy"}
        sets = {k: v for k, v in fields.items() if k in allowed}
        if not sets:
            return
        sql = "UPDATE favourites SET " + ", ".join(f"{k} = ?" for k in sets) + " WHERE id = ?"
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute(sql, list(sets.values()) + [fav_id])
                con.commit()
            finally:
                con.close()

    def get_favourite_by_combo(self, run_id: int, combo_key) -> "FavRow | None":
        """Return an existing FavRow for (run_id, combo_hash), or None."""
        combo_hash = key_hash(combo_key)
        con = self._connect()
        try:
            row = con.execute(
                "SELECT * FROM favourites WHERE run_id = ? AND combo_hash = ?",
                (run_id, combo_hash),
            ).fetchone()
            return _row_to_fav_row(row) if row else None
        finally:
            con.close()

    def get_bundle_meta(self, run_id: int) -> dict:
        """Return the parsed meta.json dict for a run, or empty dict on error."""
        row = self.get_run(run_id)
        if row is None:
            return {}
        try:
            return json.loads((Path(row.bundle_path) / "meta.json").read_text())
        except Exception:
            return {}

    # ── Column presets (Phase 5) ─────────────────────────────────────────────

    def save_column_preset(self, strategy: str, param_hash: str,
                           hidden_cols: list[str]) -> None:
        """Upsert the hidden-column list for (strategy, param_hash)."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute(
                    """INSERT INTO column_presets (strategy, param_hash, hidden_json, updated_at)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(strategy, param_hash)
                       DO UPDATE SET hidden_json = excluded.hidden_json,
                                     updated_at  = excluded.updated_at""",
                    (strategy, param_hash, json.dumps(hidden_cols), now),
                )
                con.commit()
            finally:
                con.close()

    def load_column_preset(self, strategy: str, param_hash: str) -> list[str] | None:
        """Return the saved hidden-column list, or None if no preset exists."""
        con = self._connect()
        try:
            row = con.execute(
                "SELECT hidden_json FROM column_presets WHERE strategy = ? AND param_hash = ?",
                (strategy, param_hash),
            ).fetchone()
            if row is None:
                return None
            return json.loads(row["hidden_json"])
        finally:
            con.close()

    # ── User preferences (Phase 5) ───────────────────────────────────────────

    def set_pref(self, key: str, value: str) -> None:
        """Upsert a user preference string."""
        with _WRITE_LOCK:
            con = self._connect()
            try:
                con.execute(
                    """INSERT INTO user_prefs (key, value) VALUES (?, ?)
                       ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                    (key, value),
                )
                con.commit()
            finally:
                con.close()

    def get_pref(self, key: str, default: str | None = None) -> str | None:
        """Return a user preference string, or *default* if not set."""
        con = self._connect()
        try:
            row = con.execute(
                "SELECT value FROM user_prefs WHERE key = ?", (key,)
            ).fetchone()
            return row["value"] if row else default
        finally:
            con.close()

    # ── Prune runs (Phase 5) ─────────────────────────────────────────────────

    def prune_runs(self, older_than_days: int, dry_run: bool = True) -> list[RunRow]:
        """Return (and optionally delete) unpinned runs older than *older_than_days*.

        If dry_run=True (default), returns the list without deleting anything.
        If dry_run=False, deletes favourites, the SQLite rows, and the bundle
        directories for every matching run.  Pinned runs are always skipped.
        """
        import shutil
        from datetime import timedelta

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=older_than_days)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        con = self._connect()
        try:
            rows = con.execute(
                "SELECT * FROM runs WHERE pinned = 0 AND created_at < ?",
                (cutoff,),
            ).fetchall()
            to_prune = [_row_to_run_row(r) for r in rows]
        finally:
            con.close()

        if dry_run or not to_prune:
            return to_prune

        with _WRITE_LOCK:
            con = self._connect()
            try:
                for rr in to_prune:
                    con.execute("DELETE FROM favourites WHERE run_id = ?", (rr.id,))
                    con.execute("DELETE FROM runs WHERE id = ?", (rr.id,))
                con.commit()
            finally:
                con.close()

        # Delete bundle dirs after DB commit so a DB failure leaves dirs intact
        for rr in to_prune:
            bundle_path = Path(rr.bundle_path)
            if bundle_path.exists() and bundle_path.is_dir():
                shutil.rmtree(bundle_path, ignore_errors=True)
                log.info("Pruned bundle: %s", bundle_path)

        return to_prune

def _row_to_run_row(row: sqlite3.Row) -> RunRow:
    return RunRow(
        id=int(row["id"]),
        created_at=row["created_at"],
        strategy=row["strategy"],
        param_grid_json=row["param_grid_json"],
        date_from=row["date_from"],
        date_to=row["date_to"],
        n_combos=row["n_combos"],
        n_trades=row["n_trades"],
        runtime_s=row["runtime_s"],
        bundle_path=row["bundle_path"],
        pinned=bool(row["pinned"]),
        label=row["label"],
        git_sha=row["git_sha"],
        git_dirty=bool(row["git_dirty"]) if row["git_dirty"] is not None else None,
        config_hash=row["config_hash"],
    )

def _row_to_fav_row(row: sqlite3.Row) -> "FavRow":
    return FavRow(
        id=int(row["id"]),
        run_id=int(row["run_id"]),
        combo_hash=row["combo_hash"],
        combo_key_json=row["combo_key_json"],
        name=row["name"] or "",
        strategy=row["strategy"] or "",
        note=row["note"] or "",
        score=row["score"],
        sharpe=row["sharpe"],
        total_pnl=row["total_pnl"],
        params_str=row["params_str"] or "",
        added_at=row["added_at"],
    )


def _serialize_wfo_result(wfo_result) -> dict:
    """Convert a WFOResult dataclass to a JSON-serialisable dict."""
    import dataclasses

    if dataclasses.is_dataclass(wfo_result):
        return dataclasses.asdict(wfo_result)
    if isinstance(wfo_result, dict):
        return dict(wfo_result)
    # Fallback: convert to string representation
    return {"raw": str(wfo_result)}