"""Resolve run and combo identities without loading full GridResults."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backtester.core.paths import repo_root, runs_dir
from backtester.ui.services.store_service import (
    FavRow,
    RunRow,
    StoreService,
    key_from_json,
    key_hash,
    key_to_json,
)

_DEFAULT_STATE_DIR = repo_root() / "backtester" / "ui" / "state"


class ResolveError(Exception):
    """Base for lookup failures."""


class NotFound(ResolveError):
    """Zero matches."""


class AmbiguousMatch(ResolveError):
    """More than one match; ``candidates`` lists them."""

    def __init__(self, message: str, candidates: list[Any]):
        super().__init__(message)
        self.candidates = candidates


@dataclass(frozen=True)
class ResolvedRun:
    run_id: int | None
    bundle_path: Path
    bundle_name: str
    strategy: str
    family: str | None
    date_from: str | None
    date_to: str | None
    n_combos: int | None
    n_trades: int | None
    created_at: str | None
    label: str | None
    pinned: bool
    git_sha: str | None
    meta: dict[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "bundle": self.bundle_name,
            "bundle_path": str(self.bundle_path),
            "strategy": self.strategy,
            "family": self.family,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "n_combos": self.n_combos,
            "n_trades": self.n_trades,
            "created_at": self.created_at,
            "label": self.label,
            "pinned": self.pinned,
            "git_sha": self.git_sha,
            "sidecars": list(self.meta.get("sidecars") or []),
            "account_size": self.meta.get("account_size"),
            "source": self.meta.get("source"),
        }


@dataclass(frozen=True)
class ResolvedCombo:
    combo_idx: int
    combo_hash: str
    params: dict[str, Any]
    key: tuple

    def summary(self) -> dict[str, Any]:
        return {
            "combo_idx": self.combo_idx,
            "combo_hash": self.combo_hash,
            "params": self.params,
        }


def default_store(
    state_dir: Path | str | None = None,
    bundles_root: Path | str | None = None,
) -> StoreService:
    return StoreService(
        Path(state_dir) if state_dir else _DEFAULT_STATE_DIR,
        Path(bundles_root) if bundles_root else runs_dir(),
    )


def load_meta(bundle: Path) -> dict[str, Any]:
    meta_path = bundle / "meta.json"
    if not meta_path.is_file():
        raise NotFound(f"meta.json not found in {bundle}")
    return json.loads(meta_path.read_text())


def keys_from_meta(meta: dict[str, Any]) -> list[tuple]:
    return [tuple((k, v) for k, v in kv_list) for kv_list in meta.get("keys") or []]


def params_from_key(key: tuple) -> dict[str, Any]:
    return {k: v for k, v in key}


def hash_index(meta: dict[str, Any]) -> dict[str, int]:
    """combo_hash → combo_idx for one run."""
    out: dict[str, int] = {}
    for i, key in enumerate(keys_from_meta(meta)):
        out[key_hash(key)] = i
    return out


def _run_from_row(row: RunRow, meta: dict[str, Any] | None = None) -> ResolvedRun:
    bundle = Path(row.bundle_path)
    if meta is None:
        meta = load_meta(bundle) if (bundle / "meta.json").is_file() else {}
    return ResolvedRun(
        run_id=row.id,
        bundle_path=bundle,
        bundle_name=bundle.name,
        strategy=row.strategy or meta.get("strategy") or "unknown",
        family=row.family or meta.get("family"),
        date_from=row.date_from,
        date_to=row.date_to,
        n_combos=row.n_combos if row.n_combos is not None else meta.get("n_combos"),
        n_trades=row.n_trades if row.n_trades is not None else meta.get("n_trades"),
        created_at=row.created_at,
        label=row.label,
        pinned=bool(row.pinned),
        git_sha=row.git_sha,
        meta=meta,
    )


def _run_from_bundle(bundle: Path, run_id: int | None = None) -> ResolvedRun:
    meta = load_meta(bundle)
    dr = meta.get("date_range") or [None, None]
    return ResolvedRun(
        run_id=run_id,
        bundle_path=bundle.resolve(),
        bundle_name=bundle.name,
        strategy=meta.get("strategy") or "unknown",
        family=meta.get("family"),
        date_from=dr[0] if dr else None,
        date_to=dr[1] if len(dr) > 1 else None,
        n_combos=meta.get("n_combos"),
        n_trades=meta.get("n_trades"),
        created_at=meta.get("created_at"),
        label=None,
        pinned=False,
        git_sha=meta.get("git_sha"),
        meta=meta,
    )


def ensure_scanned(store: StoreService) -> None:
    """Register any on-disk bundles missing from the SQLite index."""
    store.scan_bundles()


def resolve_run(store: StoreService, token: str, *, scan: bool = True) -> ResolvedRun:
    """Resolve run_id, bundle path, dirname, or unique glob fragment."""
    token = token.strip()
    if not token:
        raise NotFound("empty run token")

    # Integer run_id
    if re.fullmatch(r"\d+", token):
        row = store.get_run(int(token))
        if row is None and scan:
            ensure_scanned(store)
            row = store.get_run(int(token))
        if row is None:
            raise NotFound(f"run_id {token} not found in UI store")
        bundle = Path(row.bundle_path)
        if not bundle.is_dir():
            raise NotFound(f"run_id {token} points to missing bundle: {bundle}")
        return _run_from_row(row)

    path = Path(token).expanduser()
    # Absolute / relative path to .bundle
    if path.is_dir() and path.name.endswith(".bundle"):
        run_id = _run_id_for_bundle(store, path)
        return _run_from_bundle(path, run_id)

    # Bundle dirname under bundles_root
    root = Path(store._bundles_root)
    candidate = root / token
    if candidate.is_dir() and candidate.name.endswith(".bundle"):
        run_id = _run_id_for_bundle(store, candidate)
        return _run_from_bundle(candidate, run_id)

    # Unique substring / glob against registered runs + on-disk bundles
    if scan:
        ensure_scanned(store)

    needle = token.replace(".bundle", "")
    matches: list[ResolvedRun] = []
    seen: set[str] = set()

    for row in store.list_runs():
        name = Path(row.bundle_path).name
        if needle in name or needle in (row.strategy or "") or (
            row.label and needle in row.label
        ):
            key = str(Path(row.bundle_path).resolve())
            if key not in seen and Path(row.bundle_path).is_dir():
                seen.add(key)
                matches.append(_run_from_row(row))

    if root.is_dir():
        for entry in root.iterdir():
            if not (entry.is_dir() and entry.name.endswith(".bundle")):
                continue
            if needle not in entry.name:
                continue
            key = str(entry.resolve())
            if key in seen:
                continue
            seen.add(key)
            matches.append(_run_from_bundle(entry, _run_id_for_bundle(store, entry)))

    if not matches:
        raise NotFound(f"no run matching {token!r}")
    if len(matches) > 1:
        raise AmbiguousMatch(
            f"{len(matches)} runs match {token!r}; pick one",
            [m.summary() for m in matches[:25]],
        )
    return matches[0]


def _run_id_for_bundle(store: StoreService, bundle: Path) -> int | None:
    target = str(bundle.resolve())
    for row in store.list_runs():
        try:
            if str(Path(row.bundle_path).resolve()) == target:
                return row.id
        except OSError:
            continue
    # Try register if present on disk
    try:
        if (bundle / "meta.json").is_file():
            return store.register_bundle(bundle)
    except Exception:
        pass
    return None


def resolve_combo(
    run: ResolvedRun,
    token: str | None = None,
    *,
    combo_hash: str | None = None,
    combo_idx: int | None = None,
    params: dict[str, Any] | None = None,
) -> ResolvedCombo:
    """Resolve a combo inside *run*. Exactly one match required."""
    keys = keys_from_meta(run.meta)
    if not keys:
        raise NotFound(f"run {run.bundle_name} has no keys in meta.json")

    hmap = hash_index(run.meta)
    candidates: list[ResolvedCombo] = []

    def _add(idx: int) -> None:
        if idx < 0 or idx >= len(keys):
            raise NotFound(f"combo_idx {idx} out of range (0..{len(keys) - 1})")
        key = keys[idx]
        candidates.append(
            ResolvedCombo(
                combo_idx=idx,
                combo_hash=key_hash(key),
                params=params_from_key(key),
                key=key,
            )
        )

    if combo_idx is not None:
        _add(int(combo_idx))
    elif combo_hash is not None:
        h = combo_hash.strip().lower()
        if h not in hmap:
            raise NotFound(f"combo_hash {h!r} not in {run.bundle_name}")
        _add(hmap[h])
    elif token:
        t = token.strip()
        if t.startswith("#") and re.fullmatch(r"#\d+", t):
            _add(int(t[1:]))
        elif re.fullmatch(r"\d+", t) and len(t) < 12:
            # bare small int → idx; 12-char hex hashes are never all-digit
            _add(int(t))
        elif re.fullmatch(r"[0-9a-fA-F]{8,16}", t):
            h = t.lower()
            # prefix match if unique
            hits = [hh for hh in hmap if hh.startswith(h) or h.startswith(hh)]
            if not hits and h in hmap:
                hits = [h]
            # exact first
            if h in hmap:
                _add(hmap[h])
            elif len(hits) == 1:
                _add(hmap[hits[0]])
            elif len(hits) > 1:
                raise AmbiguousMatch(
                    f"combo hash prefix {t!r} is ambiguous in {run.bundle_name}",
                    [{"combo_hash": hh, "combo_idx": hmap[hh]} for hh in hits],
                )
            else:
                raise NotFound(f"combo_hash {t!r} not in {run.bundle_name}")
        else:
            raise NotFound(
                f"cannot parse combo token {t!r} "
                "(use 12-char hash, #idx, or --param k=v)"
            )
    elif params:
        for i, key in enumerate(keys):
            p = params_from_key(key)
            if all(p.get(k) == v for k, v in params.items()):
                _add(i)
        if not candidates:
            raise NotFound(f"no combo matching params {params} in {run.bundle_name}")
        if len(candidates) > 1:
            raise AmbiguousMatch(
                f"{len(candidates)} combos match params {params}",
                [c.summary() for c in candidates[:40]],
            )
    else:
        raise NotFound("no combo selector provided")

    # Dedup if multiple paths added same idx
    by_idx = {c.combo_idx: c for c in candidates}
    uniq = list(by_idx.values())
    if params and uniq:
        uniq = [
            c
            for c in uniq
            if all(c.params.get(k) == v for k, v in params.items())
        ]
        if not uniq:
            raise NotFound(f"combo does not match params {params}")
    if len(uniq) != 1:
        raise AmbiguousMatch(
            f"{len(uniq)} combos matched",
            [c.summary() for c in uniq[:40]],
        )
    return uniq[0]


def filter_combos(
    run: ResolvedRun,
    *,
    combo_hash: str | None = None,
    combo_idx: int | None = None,
    params: dict[str, Any] | None = None,
    q: str | None = None,
) -> list[ResolvedCombo]:
    """Return matching combos (may be empty or many)."""
    keys = keys_from_meta(run.meta)
    out: list[ResolvedCombo] = []
    for i, key in enumerate(keys):
        h = key_hash(key)
        p = params_from_key(key)
        if combo_idx is not None and i != combo_idx:
            continue
        if combo_hash and not (
            h == combo_hash.lower() or h.startswith(combo_hash.lower())
        ):
            continue
        if params and not all(p.get(k) == v for k, v in params.items()):
            continue
        if q:
            blob = json.dumps(p, sort_keys=True).lower()
            if q.lower() not in blob and q.lower() not in h:
                continue
        out.append(ResolvedCombo(combo_idx=i, combo_hash=h, params=p, key=key))
    return out


def find_hash_across_runs(
    store: StoreService,
    combo_hash: str,
    *,
    limit: int = 40,
) -> list[dict[str, Any]]:
    """List runs containing *combo_hash* (favourites first, then meta scan)."""
    h = combo_hash.strip().lower()
    found: list[dict[str, Any]] = []
    seen: set[tuple[int | None, str]] = set()

    for fav in store.list_favourites():
        if fav.combo_hash == h or fav.combo_hash.startswith(h):
            row = store.get_run(fav.run_id)
            if row is None:
                continue
            key = (fav.run_id, fav.combo_hash)
            if key in seen:
                continue
            seen.add(key)
            found.append(
                {
                    "run_id": fav.run_id,
                    "bundle": Path(row.bundle_path).name,
                    "strategy": fav.strategy or row.strategy,
                    "combo_hash": fav.combo_hash,
                    "favourite_name": fav.name or None,
                    "favourite_note": fav.note or None,
                    "source": "favourite",
                }
            )

    ensure_scanned(store)
    for row in store.list_runs():
        if len(found) >= limit:
            break
        bundle = Path(row.bundle_path)
        if not (bundle / "meta.json").is_file():
            continue
        try:
            meta = load_meta(bundle)
            hmap = hash_index(meta)
        except Exception:
            continue
        hits = [hh for hh in hmap if hh == h or hh.startswith(h)]
        for hh in hits:
            key = (row.id, hh)
            if key in seen:
                continue
            seen.add(key)
            found.append(
                {
                    "run_id": row.id,
                    "bundle": bundle.name,
                    "strategy": row.strategy,
                    "combo_hash": hh,
                    "combo_idx": hmap[hh],
                    "source": "bundle",
                }
            )
            if len(found) >= limit:
                break
    return found


def favourite_for(
    store: StoreService, run: ResolvedRun, combo: ResolvedCombo
) -> FavRow | None:
    if run.run_id is None:
        return None
    for fav in store.list_favourites():
        if fav.run_id == run.run_id and fav.combo_hash == combo.combo_hash:
            return fav
    return None


def parse_param_filters(items: list[str] | None) -> dict[str, Any]:
    """Parse repeated ``k=v`` strings into a typed param dict."""
    if not items:
        return {}
    out: dict[str, Any] = {}
    for raw in items:
        if "=" not in raw:
            raise ValueError(f"param filter must be k=v, got {raw!r}")
        k, v = raw.split("=", 1)
        out[k.strip()] = _coerce(v.strip())
    return out


def _coerce(v: str) -> Any:
    low = v.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    if low in ("none", "null"):
        return None
    try:
        if re.fullmatch(r"-?\d+", v):
            return int(v)
        if re.fullmatch(r"-?\d+\.\d+", v):
            return float(v)
    except ValueError:
        pass
    # strip optional quotes
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1]
    return v


def key_json(key: tuple) -> str:
    return key_to_json(key)


def key_from_stored(s: str) -> tuple:
    return key_from_json(s)
