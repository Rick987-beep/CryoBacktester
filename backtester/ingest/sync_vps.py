#!/usr/bin/env python3
"""
sync_vps.py — Download recorder parquets from CryoTrader VPS to backtester/data/

Wraps rsync over SSH to pull completed daily parquets produced by the
CryoTrader tick recorder (ct-recorder service) to the local backtester.
With --delete-after, removes transferred files from the server after
verifying checksums — but never touches the current or previous day.

Files land in backtester/data/ — the directory MarketReplay reads by default.

Usage:
    # Dry run — show what would be downloaded (safe default)
    python -m backtester.ingest.sync_vps --days 14

    # Actually download last 14 days (runs QA on the date window after transfer)
    python -m backtester.ingest.sync_vps --days 14 --confirm

    # Download all available data on server
    python -m backtester.ingest.sync_vps --all --confirm

    # Download last 30 days, then remove from server (keeps last 2 days)
    python -m backtester.ingest.sync_vps --days 30 --delete-after --confirm

    # Skip post-sync completeness / size QA
    python -m backtester.ingest.sync_vps --all --confirm --no-qa

After a real sync (``--confirm``), each date in scope is checked for missing
files, options snapshot gaps, thin instrument lists, spot bar gaps / head
truncation, and oddly small or large parquet sizes vs local baselines.
Findings are logged as warnings; sync still exits 0 unless transfer failed.

Config (via .env or environment variables — all optional, hardcoded defaults work):
    RECORDER_VPS_HOST       e.g. root@46.225.137.92      (default: production server)
    RECORDER_VPS_DATA_DIR   e.g. /opt/ct/recorder/data   (default: production path)
    RECORDER_SSH_KEY        path to SSH private key       (falls back to SSH_KEY in .env)

See .env.example for a template.

Naming note:
    The recorder writes:
        options_YYYY-MM-DD.parquet
        spot_track_YYYY-MM-DD.parquet
    Tardis bulk-download writes:
        options_YYYY-MM-DD.parquet
        spot_YYYY-MM-DD.parquet
    Both coexist in backtester/data/ — MarketReplay globs 'spot_*.parquet'
    and handles both prefixes transparently.
"""
import argparse
import hashlib
import logging
import os
import statistics
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Local target directory: backtester/data/ (two levels up from this file at
# backtester/ingest/sync_vps.py).
_LOCAL_DATA_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)

# Production server defaults — mirrors CryoTrader's .env.recorder / servers.toml.
# Override via environment variables if the server changes.
_DEFAULT_VPS_HOST     = "root@46.225.137.92"
_DEFAULT_VPS_DATA_DIR = "/opt/ct/recorder/data"

# Minimum age (days) before a file can be deleted from the server.
# Protects the current and previous day.
_KEEP_DAYS_ON_SERVER = 2

# Post-sync QA thresholds (5-min options grid; 1-min spot_track).
_OPTS_SNAPS_FULL = 288
_OPTS_SNAPS_WARN = 270
_OPTS_GAP_WARN_MIN = 15
_OPTS_INST_THIN = 600
_OPTS_SIZE_FLOOR = 500_000          # absolute tiny floor (~0.5 MB)
_SPOT_BARS_WARN = 1000
_SPOT_SIZE_FLOOR = 8_000            # absolute tiny floor
_SIZE_TINY_FRAC = 0.40              # vs median of sibling local files
_SIZE_HUGE_FRAC = 2.50


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_env(name, required=True, default=None, fallback=None):
    # type: (str, bool, Optional[str], Optional[str]) -> Optional[str]
    """Read an env var, with optional fallback to a second var and a hardcoded default."""
    val = os.getenv(name, "").strip()
    if not val and fallback:
        val = os.getenv(fallback, "").strip()
    if not val and default:
        val = default
    if not val and required:
        logger.error("Missing required env var: %s", name)
        sys.exit(1)
    return val or None


def _date_range(days):
    # type: (int) -> List[str]
    """Return YYYY-MM-DD strings for the last N days, oldest first."""
    today = datetime.now(timezone.utc).date()
    return [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days - 1, -1, -1)
    ]


def _safe_to_delete(date_str):
    # type: (str) -> bool
    """True only if the date is old enough to safely remove from server."""
    today = datetime.now(timezone.utc).date()
    try:
        file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    return (today - file_date).days >= _KEEP_DAYS_ON_SERVER


def _ssh_base_cmd(vps_host, ssh_key):
    # type: (str, Optional[str]) -> List[str]
    cmd = ["ssh"]
    if ssh_key:
        cmd += ["-i", ssh_key]
    cmd += ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]
    return cmd


def _rsync(vps_host, ssh_key, remote_dir, local_dir, filenames):
    # type: (str, Optional[str], str, str, List[str]) -> bool
    """rsync the named files from remote to local. Returns True on success."""
    ssh_str = "ssh -o BatchMode=yes -o StrictHostKeyChecking=no"
    if ssh_key:
        ssh_str += f" -i {ssh_key}"

    filter_args = []
    for f in filenames:
        filter_args += ["--include", f]
    filter_args += ["--exclude", "*"]

    cmd = (
        ["rsync", "-avz", "--progress", "-e", ssh_str]
        + filter_args
        + [f"{vps_host}:{remote_dir}/", f"{local_dir}/"]
    )

    logger.info("rsync: transferring %d file(s)", len(filenames))
    result = subprocess.run(cmd)
    return result.returncode == 0


def _local_sha256(path):
    # type: (str) -> str
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _remote_sha256(vps_host, ssh_key, remote_path):
    # type: (str, Optional[str], str) -> Optional[str]
    cmd = _ssh_base_cmd(vps_host, ssh_key) + [vps_host, f"sha256sum {remote_path}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    parts = result.stdout.strip().split()
    return parts[0] if parts else None


def _remote_delete(vps_host, ssh_key, remote_paths):
    # type: (str, Optional[str], List[str]) -> bool
    """Delete a list of files on the remote server."""
    files_str = " ".join(f'"{p}"' for p in remote_paths)
    cmd = _ssh_base_cmd(vps_host, ssh_key) + [vps_host, f"rm -f {files_str}"]
    result = subprocess.run(cmd)
    return result.returncode == 0


def _remote_list(vps_host, ssh_key, remote_dir):
    # type: (str, Optional[str], str) -> List[str]
    """List parquet filenames in the remote data directory."""
    cmd = _ssh_base_cmd(vps_host, ssh_key) + [
        vps_host, f"ls {remote_dir}/*.parquet 2>/dev/null || true"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return [
        os.path.basename(p.strip())
        for p in result.stdout.strip().splitlines()
        if p.strip().endswith(".parquet")
    ]


# ---------------------------------------------------------------------------
# Post-sync QA
# ---------------------------------------------------------------------------

def _ts_to_utc(ts):
    # type: (float) -> datetime
    """Convert recorder/Tardis timestamps (ns/us/ms/s) to aware UTC datetime."""
    v = float(ts)
    if v > 1e17:
        return datetime.fromtimestamp(v / 1e9, tz=timezone.utc)
    if v > 1e14:
        return datetime.fromtimestamp(v / 1e6, tz=timezone.utc)
    if v > 1e11:
        return datetime.fromtimestamp(v / 1e3, tz=timezone.utc)
    return datetime.fromtimestamp(v, tz=timezone.utc)


def _fmt_hhmm(dt):
    # type: (datetime) -> str
    return dt.strftime("%H:%M")


def _median_sibling_size(data_dir, prefix, exclude_name=None):
    # type: (str, str, Optional[str]) -> Optional[float]
    """Median byte size of ``{prefix}_YYYY-MM-DD.parquet`` siblings in data_dir."""
    sizes = []
    try:
        names = os.listdir(data_dir)
    except OSError:
        return None
    for name in names:
        if not name.startswith(f"{prefix}_") or not name.endswith(".parquet"):
            continue
        if exclude_name and name == exclude_name:
            continue
        path = os.path.join(data_dir, name)
        try:
            sizes.append(os.path.getsize(path))
        except OSError:
            continue
    if not sizes:
        return None
    return float(statistics.median(sizes))


def _size_notes(path, median, floor, label):
    # type: (str, Optional[float], int, str) -> List[str]
    """Return size-related issue strings (tiny / huge) for one parquet."""
    notes = []  # type: List[str]
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        return [f"{label} unreadable size ({exc})"]
    if size < floor:
        notes.append(f"{label} tiny ({size} B < floor {floor})")
    elif median is not None and size < median * _SIZE_TINY_FRAC:
        notes.append(
            f"{label} tiny vs median ({size} B < {_SIZE_TINY_FRAC:.0%}×{int(median)})"
        )
    if median is not None and size > median * _SIZE_HUGE_FRAC:
        notes.append(
            f"{label} huge vs median ({size} B > {_SIZE_HUGE_FRAC:.0%}×{int(median)})"
        )
    return notes


def qa_options_file(path, date_str, size_median=None):
    # type: (str, str, Optional[float]) -> Tuple[List[str], Dict[str, object]]
    """
    Check one options daily parquet.

    Returns (issue strings, summary stats dict).
    """
    issues = []  # type: List[str]
    stats = {
        "snaps": 0,
        "inst_median": 0,
        "gaps_ge_15m": 0,
    }  # type: Dict[str, object]

    issues.extend(_size_notes(path, size_median, _OPTS_SIZE_FLOOR, "options"))

    try:
        import pyarrow.parquet as pq
    except ImportError:
        issues.append("options QA skipped (pyarrow not installed)")
        return issues, stats

    try:
        schema_names = set(pq.read_schema(path).names)
    except Exception as exc:
        issues.append(f"options unreadable ({exc})")
        return issues, stats

    for col in ("timestamp", "expiry"):
        if col not in schema_names:
            issues.append(f"options missing column '{col}'")
    if "timestamp" not in schema_names:
        return issues, stats

    try:
        table = pq.read_table(path, columns=["timestamp"])
    except Exception as exc:
        issues.append(f"options read failed ({exc})")
        return issues, stats

    ts_raw = table.column("timestamp").to_pylist()
    if not ts_raw:
        issues.append("options empty")
        return issues, stats

    from collections import Counter

    counts = Counter(ts_raw)
    snaps_sorted = sorted(counts.keys())
    n_snaps = len(snaps_sorted)
    inst_vals = list(counts.values())
    inst_median = int(statistics.median(inst_vals)) if inst_vals else 0
    stats["snaps"] = n_snaps
    stats["inst_median"] = inst_median

    if n_snaps < _OPTS_SNAPS_WARN:
        issues.append(f"options snaps {n_snaps}/{_OPTS_SNAPS_FULL} (warn <{_OPTS_SNAPS_WARN})")
    elif n_snaps < _OPTS_SNAPS_FULL:
        issues.append(f"options snaps {n_snaps}/{_OPTS_SNAPS_FULL} (incomplete day)")

    if inst_median < _OPTS_INST_THIN:
        issues.append(f"options thin instruments/snap (median {inst_median} < {_OPTS_INST_THIN})")

    gap_count = 0
    for a, b in zip(snaps_sorted, snaps_sorted[1:]):
        gap_min = (_ts_to_utc(b) - _ts_to_utc(a)).total_seconds() / 60.0
        if gap_min >= _OPTS_GAP_WARN_MIN:
            gap_count += 1
            if gap_count <= 3:
                issues.append(
                    f"options gap {_fmt_hhmm(_ts_to_utc(a))}→{_fmt_hhmm(_ts_to_utc(b))} "
                    f"({gap_min:.0f}m)"
                )
    stats["gaps_ge_15m"] = gap_count
    if gap_count > 3:
        issues.append(f"options … +{gap_count - 3} more gap(s) ≥{_OPTS_GAP_WARN_MIN}m")

    return issues, stats


def qa_spot_file(path, date_str, size_median=None):
    # type: (str, str, Optional[float]) -> Tuple[List[str], Dict[str, object]]
    """
    Check one spot_track daily parquet.

    Returns (issue strings, summary stats dict).
    """
    issues = []  # type: List[str]
    stats = {
        "bars": 0,
        "first_on_day": None,
        "gaps_gt_90s": 0,
    }  # type: Dict[str, object]

    issues.extend(_size_notes(path, size_median, _SPOT_SIZE_FLOOR, "spot"))

    try:
        import pyarrow.parquet as pq
    except ImportError:
        issues.append("spot QA skipped (pyarrow not installed)")
        return issues, stats

    try:
        schema_names = set(pq.read_schema(path).names)
    except Exception as exc:
        issues.append(f"spot unreadable ({exc})")
        return issues, stats

    if "timestamp" not in schema_names:
        issues.append("spot missing column 'timestamp'")
        return issues, stats

    try:
        table = pq.read_table(path, columns=["timestamp"])
    except Exception as exc:
        issues.append(f"spot read failed ({exc})")
        return issues, stats

    ts_raw = sorted(table.column("timestamp").to_pylist())
    if not ts_raw:
        issues.append("spot empty")
        return issues, stats

    dts = [_ts_to_utc(t) for t in ts_raw]
    day = date.fromisoformat(date_str)
    on_day = [dt for dt in dts if dt.date() == day]
    stats["bars"] = len(ts_raw)

    if len(ts_raw) < _SPOT_BARS_WARN:
        issues.append(f"spot bars {len(ts_raw)} (warn <{_SPOT_BARS_WARN})")

    if not on_day:
        issues.append(f"spot has no bars on {date_str}")
    else:
        first_on = on_day[0]
        stats["first_on_day"] = first_on.isoformat()
        midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        head_min = (first_on - midnight).total_seconds() / 60.0
        # Healthy files often start at 00:00; late start = truncated morning.
        if head_min >= 30.0:
            issues.append(
                f"spot head truncation (first on-day bar {_fmt_hhmm(first_on)} UTC, "
                f"~{head_min:.0f}m after midnight)"
            )

    gap_count = 0
    for a, b in zip(dts, dts[1:]):
        gap_s = (b - a).total_seconds()
        if gap_s > 90.0:
            gap_count += 1
            if gap_count <= 3:
                if gap_s >= 120.0:
                    gap_txt = f"{gap_s / 60.0:.0f}m"
                else:
                    gap_txt = f"{gap_s:.0f}s"
                issues.append(
                    f"spot gap {_fmt_hhmm(a)}→{_fmt_hhmm(b)} ({gap_txt})"
                )
    stats["gaps_gt_90s"] = gap_count
    if gap_count > 3:
        issues.append(f"spot … +{gap_count - 3} more gap(s) >90s")

    return issues, stats


def qa_day(date_str, data_dir):
    # type: (str, str) -> Tuple[str, List[str]]
    """
    QA one calendar day under data_dir.

    Returns (one-line summary, list of issue strings). Prefer ``spot_track_``
    over Tardis ``spot_`` for the spot check.
    """
    issues = []  # type: List[str]
    opts_path = os.path.join(data_dir, f"options_{date_str}.parquet")
    spot_path = os.path.join(data_dir, f"spot_track_{date_str}.parquet")
    if not os.path.exists(spot_path):
        alt = os.path.join(data_dir, f"spot_{date_str}.parquet")
        if os.path.exists(alt):
            spot_path = alt

    opts_med = _median_sibling_size(
        data_dir, "options", exclude_name=os.path.basename(opts_path)
    )
    spot_prefix = "spot_track" if os.path.basename(spot_path).startswith("spot_track_") else "spot"
    spot_med = _median_sibling_size(
        data_dir, spot_prefix, exclude_name=os.path.basename(spot_path)
    )

    opts_snaps = "-"
    inst_med = "-"
    spot_bars = "-"

    if not os.path.exists(opts_path):
        issues.append("MISSING options parquet")
    else:
        o_issues, o_stats = qa_options_file(opts_path, date_str, size_median=opts_med)
        issues.extend(o_issues)
        opts_snaps = str(o_stats.get("snaps", "-"))
        inst_med = str(o_stats.get("inst_median", "-"))

    if not os.path.exists(spot_path):
        issues.append("MISSING spot parquet")
    else:
        s_issues, s_stats = qa_spot_file(spot_path, date_str, size_median=spot_med)
        issues.extend(s_issues)
        spot_bars = str(s_stats.get("bars", "-"))

    status = "ok" if not issues else "ISSUES"
    summary = (
        f"{date_str}: options {opts_snaps}/{_OPTS_SNAPS_FULL} "
        f"inst/snap~{inst_med} spot {spot_bars} — {status}"
    )
    return summary, issues


def run_post_sync_qa(dates, data_dir):
    # type: (Sequence[str], str) -> List[str]
    """
    Log per-day QA lines and an issues summary for dates in scope.

    Returns a flat list of ``\"YYYY-MM-DD: note\"`` strings (empty if clean).
    Warnings only — does not change exit code.
    """
    if not dates:
        return []

    logger.info("QA: checking %d day(s) under %s", len(dates), data_dir)
    all_notes = []  # type: List[str]
    for date_str in dates:
        summary, issues = qa_day(date_str, data_dir)
        logger.info("  %s", summary)
        for note in issues:
            all_notes.append(f"{date_str}: {note}")

    if all_notes:
        logger.warning("=== QA issues ===")
        for note in all_notes:
            logger.warning("  %s", note)
    else:
        logger.info("=== QA issues === none")
    return all_notes


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def run(days=None, all_files=False, delete_after=False, confirm=False, dry_run=True,
        no_qa=False, data_dir=None):
    # type: (Optional[int], bool, bool, bool, bool, bool, Optional[str]) -> int
    """Main sync logic. Returns exit code (0 = success)."""
    vps_host   = _get_env("RECORDER_VPS_HOST",     required=False, default=_DEFAULT_VPS_HOST)
    remote_dir = _get_env("RECORDER_VPS_DATA_DIR",  required=False, default=_DEFAULT_VPS_DATA_DIR)
    ssh_key    = _get_env("RECORDER_SSH_KEY",       required=False, fallback="SSH_KEY")
    local_dir  = data_dir or _LOCAL_DATA_DIR

    os.makedirs(local_dir, exist_ok=True)

    # Determine which dates to sync
    if all_files:
        remote_files   = _remote_list(vps_host, ssh_key, remote_dir)
        dates_to_sync  = sorted(set(
            f.replace("options_", "").replace(".parquet", "")
            for f in remote_files
            if f.startswith("options_") and not f.startswith(".partial")
        ))
    else:
        days = days or 7
        dates_to_sync = _date_range(days)

    if not dates_to_sync:
        logger.info("No dates to sync.")
        return 0

    logger.info(
        "Dates to sync: %s → %s (%d day(s))",
        dates_to_sync[0], dates_to_sync[-1], len(dates_to_sync),
    )

    filenames = []
    for date_str in dates_to_sync:
        filenames.append(f"options_{date_str}.parquet")
        filenames.append(f"spot_track_{date_str}.parquet")

    # Filter out files already present locally
    already_local = [f for f in filenames if os.path.exists(os.path.join(local_dir, f))]
    filenames = [f for f in filenames if f not in already_local]

    if already_local:
        logger.info("Already local (%d file(s)) — skipping:", len(already_local))
        for f in already_local:
            logger.info("  %s", f)

    def _maybe_qa():
        if dry_run or no_qa:
            return
        run_post_sync_qa(dates_to_sync, local_dir)

    if not filenames:
        logger.info("Nothing to download — all files already present locally.")
        _maybe_qa()
        return 0

    if dry_run:
        logger.info("[DRY RUN] Would download %d file(s):", len(filenames))
        for f in filenames:
            logger.info("  %s", f)
        logger.info("Pass --confirm to actually transfer.")
        return 0

    # Transfer
    ok = _rsync(vps_host, ssh_key, remote_dir, local_dir, filenames)
    if not ok:
        logger.error("rsync failed. Aborting.")
        return 1

    # Optional verified deletion from server
    if delete_after and confirm:
        to_delete    = []
        failed_verify = []

        for date_str in dates_to_sync:
            if not _safe_to_delete(date_str):
                logger.info(
                    "Skipping delete for %s (too recent — keeping on server)", date_str
                )
                continue

            for prefix in ("options", "spot_track"):
                fname       = f"{prefix}_{date_str}.parquet"
                local_path  = os.path.join(local_dir, fname)
                remote_path = f"{remote_dir}/{fname}"

                if not os.path.exists(local_path):
                    logger.warning("Local file missing after transfer: %s", fname)
                    failed_verify.append(fname)
                    continue

                local_hash  = _local_sha256(local_path)
                remote_hash = _remote_sha256(vps_host, ssh_key, remote_path)

                if remote_hash is None:
                    logger.warning("Could not get remote checksum for %s", fname)
                    failed_verify.append(fname)
                    continue

                if local_hash == remote_hash:
                    to_delete.append(remote_path)
                    logger.debug("Verified %s — queued for deletion", fname)
                else:
                    logger.warning(
                        "Checksum mismatch for %s (local=%s remote=%s) — keeping on server",
                        fname, local_hash[:12], remote_hash[:12],
                    )
                    failed_verify.append(fname)

        if failed_verify:
            logger.warning(
                "%d file(s) failed verification — not deleting those from server",
                len(failed_verify),
            )

        if to_delete:
            logger.info("Deleting %d verified file(s) from server...", len(to_delete))
            if _remote_delete(vps_host, ssh_key, to_delete):
                logger.info("Server cleanup done. %d file(s) removed.", len(to_delete))
            else:
                logger.warning("Remote delete returned non-zero — check server manually.")
                return 1

    elif delete_after and not confirm:
        logger.info("--delete-after set but --confirm missing. No files deleted from server.")

    logger.info("Sync complete.")
    _maybe_qa()
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    # type: () -> None
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Download CryoTrader recorder parquets from VPS to backtester/data/. "
            "Dry-run by default — pass --confirm to actually transfer. "
            "After a real sync, runs a local completeness / size QA pass "
            "(disable with --no-qa)."
        )
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--days", type=int, metavar="N",
        help="Sync last N days (default: 7)",
    )
    group.add_argument(
        "--all", action="store_true",
        help="Sync all available parquets on server",
    )
    parser.add_argument(
        "--delete-after", action="store_true",
        help=(
            "Delete synced files from server after checksum verification. "
            "Always keeps the last 2 days on server. Requires --confirm."
        ),
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="Actually transfer (and delete if --delete-after). Without this flag: dry run.",
    )
    parser.add_argument(
        "--no-qa", action="store_true",
        help="Skip post-sync local QA of dates in scope (default: QA after --confirm).",
    )

    args = parser.parse_args()
    dry_run = not args.confirm

    if dry_run:
        logger.info("DRY RUN — pass --confirm to execute")

    sys.exit(run(
        days=args.days,
        all_files=args.all,
        delete_after=args.delete_after,
        confirm=args.confirm,
        dry_run=dry_run,
        no_qa=args.no_qa,
    ))


if __name__ == "__main__":
    main()
