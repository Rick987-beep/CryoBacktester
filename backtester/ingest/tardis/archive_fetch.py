#!/usr/bin/env python3
"""
archive_fetch.py — Download-only Tardis options_chain archive to remote storage.

Per day: probe → download → gzip -t → rsync → manifest → delete local staging file.

Designed for multi-day unattended runs: day-level retries, rsync retries,
manifest resume, single-instance lock, continue-on-error with failures log.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

try:
    from .tardis_common import (
        DEFAULT_EXCHANGE,
        EXCHANGE_PRIORITY,
        archive_filename,
        date_range_reverse,
        download_options_chain,
        probe_options_chain,
        remote_size,
    )
except ImportError:
    from tardis_common import (
        DEFAULT_EXCHANGE,
        EXCHANGE_PRIORITY,
        archive_filename,
        date_range_reverse,
        download_options_chain,
        probe_options_chain,
        remote_size,
    )

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_STAGING_DIR = os.path.join(_SCRIPT_DIR, "staging")
DEFAULT_LOG_DIR = os.path.join(_SCRIPT_DIR, "logs")
DEFAULT_MANIFEST = os.path.join(DEFAULT_LOG_DIR, "manifest.jsonl")
DEFAULT_FAILURES = os.path.join(DEFAULT_LOG_DIR, "failures.jsonl")
DEFAULT_LOCK = os.path.join(DEFAULT_LOG_DIR, "archive.lock")

# Minimum free bytes on staging filesystem before accepting a new download.
MIN_STAGING_FREE_BYTES = 12 * 1024**3  # ~12 GB (largest deribit days ~10 GB)

DAY_RETRY_DELAYS = [30, 120, 300]
RSYNC_RETRY_DELAYS = [10, 30, 60]
PROBE_RETRY_DELAYS = [5, 15, 30]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def manifest_key(exchange: str, date_str: str) -> Tuple[str, str]:
    return exchange, date_str


def load_manifest(path: str) -> Set[Tuple[str, str]]:
    done: Set[Tuple[str, str]] = set()
    if not os.path.exists(path):
        return done
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                done.add(manifest_key(rec["exchange"], rec["date"]))
            except (json.JSONDecodeError, KeyError):
                continue
    return done


def _append_jsonl(path: str, rec: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())


def append_manifest(
    path: str,
    exchange: str,
    date_str: str,
    bytes_downloaded: int,
    remote_path: str,
    status: str = "ok",
) -> None:
    rec = {
        "ts": _utc_now_iso(),
        "exchange": exchange,
        "date": date_str,
        "status": status,
        "bytes_downloaded": bytes_downloaded,
        "remote_path": remote_path,
        "filename": archive_filename(date_str),
    }
    _append_jsonl(path, rec)


def append_failure(
    path: str,
    exchange: str,
    date_str: str,
    error: str,
    attempt: int,
) -> None:
    rec = {
        "ts": _utc_now_iso(),
        "exchange": exchange,
        "date": date_str,
        "attempt": attempt,
        "error": error,
    }
    _append_jsonl(path, rec)


def exchange_upload_base(upload_base: str, exchange: str) -> str:
    base = upload_base.rstrip("/")
    if base.endswith(f"/{exchange}") or base.endswith(f":{exchange}"):
        return base
    return f"{base}/{exchange}"


def check_staging_disk(staging_dir: str, min_free: int = MIN_STAGING_FREE_BYTES) -> None:
    parent = staging_dir
    while not os.path.isdir(parent):
        parent = os.path.dirname(parent)
        if parent == staging_dir or not parent:
            parent = staging_dir
            break
    st = os.statvfs(parent)
    free = st.f_bavail * st.f_frsize
    if free < min_free:
        raise RuntimeError(
            f"Insufficient staging disk on {parent}: "
            f"{free / 1e9:.2f} GB free, need {min_free / 1e9:.2f} GB"
        )


@contextmanager
def archive_lock(lock_path: str) -> Iterator[None]:
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    lock_file = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError(
            f"Another archive_fetch is already running (lock: {lock_path})"
        ) from exc
    try:
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()} started={_utc_now_iso()}\n")
        lock_file.flush()
        yield
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def _ssh_cmd(ssh_port: Optional[int], ssh_key: Optional[str]) -> List[str]:
    parts = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
    if ssh_port:
        parts.extend(["-p", str(ssh_port)])
    if ssh_key:
        parts.extend(["-i", ssh_key])
    return parts


def remote_file_size(
    upload_base: str,
    filename: str,
    ssh_port: Optional[int],
    ssh_key: Optional[str],
) -> Optional[int]:
    """Return remote file size via ssh stat, or None if missing/unreachable."""
    if "@" not in upload_base:
        path = os.path.join(upload_base.rstrip("/"), filename)
        return os.path.getsize(path) if os.path.isfile(path) else None

    dest_dir = upload_base.rstrip("/") + "/"
    if ":" in dest_dir:
        remote_spec, remote_path = dest_dir.split(":", 1)
    else:
        return None
    remote_path = remote_path.rstrip("/") + "/" + filename
    cmd = _ssh_cmd(ssh_port, ssh_key) + [remote_spec, f"stat -c %s {remote_path}"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return None
        return int(result.stdout.strip())
    except (ValueError, subprocess.TimeoutExpired):
        return None


def rsync_upload(
    local_path: str,
    upload_base: str,
    ssh_port: Optional[int] = None,
    ssh_key: Optional[str] = None,
    max_retries: int = 3,
) -> str:
    """rsync local file to upload_base. Returns destination path string."""
    dest_dir = upload_base.rstrip("/") + "/"
    remote_file = dest_dir + os.path.basename(local_path)
    local_size = os.path.getsize(local_path)
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = RSYNC_RETRY_DELAYS[min(attempt - 1, len(RSYNC_RETRY_DELAYS) - 1)]
            print(
                f"[rsync] retry {attempt}/{max_retries} in {delay}s"
                f"  (last error: {last_exc})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)

        try:
            if "@" not in upload_base:
                os.makedirs(dest_dir, exist_ok=True)
                cmd = ["rsync", "-av", local_path, dest_dir]
            else:
                ssh_cmd = " ".join(_ssh_cmd(ssh_port, ssh_key))
                cmd = ["rsync", "-av", "--partial", "-e", ssh_cmd, local_path, dest_dir]

            print(f"[rsync] {' '.join(cmd)}", flush=True)
            result = subprocess.run(cmd, timeout=7200)
            if result.returncode != 0:
                raise RuntimeError(f"rsync exit code {result.returncode}")

            remote_size_bytes = remote_file_size(
                dest_dir, os.path.basename(local_path), ssh_port, ssh_key
            )
            if remote_size_bytes is None:
                raise RuntimeError("remote file missing after rsync")
            if remote_size_bytes != local_size:
                raise RuntimeError(
                    f"remote size mismatch: local={local_size:,}"
                    f" remote={remote_size_bytes:,}"
                )
            return remote_file

        except Exception as exc:
            last_exc = exc
            if attempt == max_retries:
                raise RuntimeError(
                    f"rsync failed after {max_retries + 1} attempts: {exc}"
                ) from exc

    raise RuntimeError("rsync exhausted retries")


def verify_gzip(path: str) -> None:
    print(f"[gzip -t] {path}", flush=True)
    with gzip.open(path, "rb") as f:
        while f.read(1024 * 1024):
            pass


def probe_with_retries(
    exchange: str,
    date_str: str,
    api_key: Optional[str],
    max_retries: int = 3,
) -> Tuple[str, int]:
    last_status = "error"
    last_nbytes = 0
    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = PROBE_RETRY_DELAYS[min(attempt - 1, len(PROBE_RETRY_DELAYS) - 1)]
            print(
                f"[probe] retry {attempt}/{max_retries} in {delay}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
        status, nbytes = probe_options_chain(exchange, date_str, api_key=api_key)
        last_status, last_nbytes = status, nbytes
        if status in ("available", "empty"):
            return status, nbytes
        if status == "forbidden":
            return status, nbytes
    return last_status, last_nbytes


def _cleanup_local(local_path: str) -> None:
    if os.path.exists(local_path):
        try:
            os.unlink(local_path)
        except OSError:
            pass


def _download_or_reuse(
    exchange: str,
    date_str: str,
    local_path: str,
    api_key: Optional[str],
) -> int:
    """Download file, reusing staging copy if size matches Tardis HEAD."""
    expected = remote_size(exchange, date_str, api_key=api_key)
    if os.path.exists(local_path):
        local_sz = os.path.getsize(local_path)
        if expected is not None and local_sz == expected:
            print(
                f"[download] {exchange} {date_str}  reusing staging gz"
                f"  ({local_sz / 1e9:.3f} GB — verified via HEAD)",
                flush=True,
            )
            return local_sz
        if expected is not None and local_sz != expected:
            print(
                f"[download] {exchange} {date_str}  partial staging gz"
                f"  ({local_sz / 1e9:.3f} of {expected / 1e9:.3f} GB) — re-downloading",
                flush=True,
            )
        _cleanup_local(local_path)

    return download_options_chain(
        exchange,
        date_str,
        local_path,
        api_key=api_key,
        log_prefix=f"[download] {exchange} {date_str}",
    )


def process_day_once(
    exchange: str,
    date_str: str,
    api_key: Optional[str],
    staging_dir: str,
    upload_base: str,
    manifest_path: str,
    ssh_port: Optional[int],
    ssh_key: Optional[str],
    skip_probe: bool = False,
) -> str:
    """Single attempt. Returns: ok | empty | failed"""
    local_name = archive_filename(date_str)
    local_path = os.path.join(staging_dir, local_name)
    dest_base = exchange_upload_base(upload_base, exchange)

    if not skip_probe:
        status, nbytes = probe_with_retries(exchange, date_str, api_key=api_key)
        if status == "empty":
            print(f"[empty] {exchange} {date_str} — Tardis placeholder, skipping", flush=True)
            append_manifest(
                manifest_path, exchange, date_str, 0, "", status="empty"
            )
            return "empty"
        if status == "forbidden":
            raise RuntimeError(f"HTTP 403 forbidden for {exchange} {date_str}")
        if status == "error":
            raise RuntimeError(f"probe failed for {exchange} {date_str}")
        if nbytes:
            print(f"[probe] {exchange} {date_str} — {nbytes / 1e9:.3f} GB available", flush=True)

    check_staging_disk(staging_dir)
    nbytes = _download_or_reuse(exchange, date_str, local_path, api_key=api_key)
    verify_gzip(local_path)
    remote_path = rsync_upload(
        local_path,
        dest_base,
        ssh_port=ssh_port,
        ssh_key=ssh_key,
    )
    append_manifest(manifest_path, exchange, date_str, nbytes, remote_path, status="ok")
    _cleanup_local(local_path)
    print(f"[done] {exchange} {date_str}  {nbytes / 1e9:.3f} GB → {remote_path}", flush=True)
    return "ok"


def process_day(
    exchange: str,
    date_str: str,
    api_key: Optional[str],
    staging_dir: str,
    upload_base: str,
    manifest_path: str,
    failures_path: str,
    ssh_port: Optional[int],
    ssh_key: Optional[str],
    day_retries: int = 3,
    skip_probe: bool = False,
) -> str:
    """Day-level retries (bulk_fetch pattern). Returns: ok | empty | failed"""
    local_name = archive_filename(date_str)
    local_path = os.path.join(staging_dir, local_name)
    last_exc: Optional[Exception] = None

    for attempt in range(day_retries + 1):
        if attempt > 0:
            delay = DAY_RETRY_DELAYS[min(attempt - 1, len(DAY_RETRY_DELAYS) - 1)]
            print(
                f"[retry] {exchange} {date_str}  day attempt {attempt}/{day_retries}"
                f" in {delay}s  (last error: {last_exc})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            _cleanup_local(local_path)

        try:
            return process_day_once(
                exchange=exchange,
                date_str=date_str,
                api_key=api_key,
                staging_dir=staging_dir,
                upload_base=upload_base,
                manifest_path=manifest_path,
                ssh_port=ssh_port,
                ssh_key=ssh_key,
                skip_probe=skip_probe or attempt > 0,
            )
        except RuntimeError as exc:
            last_exc = exc
            msg = str(exc)
            # Auth / entitlement errors won't heal on retry — fail fast.
            if "401" in msg or "403 forbidden" in msg.lower():
                print(f"[failed] {exchange} {date_str}: {exc}", file=sys.stderr, flush=True)
                append_failure(failures_path, exchange, date_str, msg, attempt)
                _cleanup_local(local_path)
                return "failed"
            if attempt == day_retries:
                print(f"[failed] {exchange} {date_str}: {exc}", file=sys.stderr, flush=True)
                append_failure(failures_path, exchange, date_str, msg, attempt)
                _cleanup_local(local_path)
                return "failed"

    return "failed"


def iter_exchange_jobs(
    exchanges: Iterable[str],
    from_date: str,
    to_date: str,
    done: Set[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    jobs: List[Tuple[str, str]] = []
    for exchange in exchanges:
        for d in date_range_reverse(from_date, to_date):
            date_str = d.isoformat()
            if manifest_key(exchange, date_str) in done:
                continue
            jobs.append((exchange, date_str))
    return jobs


def resolve_upload_base(cli_upload_base: Optional[str]) -> str:
    if cli_upload_base:
        return cli_upload_base
    user = os.environ.get("STORAGE_BOX_USER", "").strip()
    host = os.environ.get("STORAGE_BOX_HOST", "").strip()
    base = os.environ.get("STORAGE_BOX_BASE", "tardis_raw").strip()
    if user and host:
        return f"{user}@{host}:{base}"
    raise SystemExit(
        "Missing --upload-base or STORAGE_BOX_USER + STORAGE_BOX_HOST in environment"
    )


def archive_fetch(
    exchanges: List[str],
    from_date: str,
    to_date: str,
    upload_base: str,
    api_key: Optional[str] = None,
    staging_dir: str = DEFAULT_STAGING_DIR,
    manifest_path: str = DEFAULT_MANIFEST,
    failures_path: str = DEFAULT_FAILURES,
    lock_path: str = DEFAULT_LOCK,
    ssh_port: Optional[int] = None,
    ssh_key: Optional[str] = None,
    day_retries: int = 3,
    dry_run: bool = False,
    use_lock: bool = True,
) -> Dict[str, int]:
    if api_key is None:
        api_key = os.environ.get("TARDIS_API_KEY")

    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(os.path.dirname(manifest_path) or ".", exist_ok=True)

    done = load_manifest(manifest_path)
    jobs = iter_exchange_jobs(exchanges, from_date, to_date, done)

    print(
        f"\n{'='*60}\n"
        f"archive_fetch  |  {len(jobs)} jobs  |  {from_date} → {to_date}\n"
        f"exchanges={exchanges}\n"
        f"upload_base={upload_base}\n"
        f"manifest={manifest_path}  (already done: {len(done)})\n"
        f"day_retries={day_retries}\n"
        f"{'='*60}\n",
        flush=True,
    )

    counts: Dict[str, int] = {"ok": 0, "skipped": 0, "empty": 0, "failed": 0}
    if dry_run:
        for exchange, date_str in jobs[:20]:
            print(f"  TODO  {exchange}  {date_str}")
        if len(jobs) > 20:
            print(f"  ... and {len(jobs) - 20} more")
        counts["skipped"] = len(jobs)
        return counts

    def _run() -> Dict[str, int]:
        t0 = time.time()
        for i, (exchange, date_str) in enumerate(jobs, start=1):
            print(f"\n─── [{i}/{len(jobs)}]  {exchange}  {date_str} ───", flush=True)
            status = process_day(
                exchange=exchange,
                date_str=date_str,
                api_key=api_key,
                staging_dir=staging_dir,
                upload_base=upload_base,
                manifest_path=manifest_path,
                failures_path=failures_path,
                ssh_port=ssh_port,
                ssh_key=ssh_key,
                day_retries=day_retries,
            )
            counts[status] = counts.get(status, 0) + 1

        elapsed = time.time() - t0
        print(
            f"\n{'='*60}\n"
            f"archive_fetch done in {elapsed/3600:.2f}h\n"
            f"  ok={counts['ok']}  empty={counts['empty']}"
            f"  failed={counts['failed']}\n"
            f"{'='*60}\n",
            flush=True,
        )
        if counts["failed"]:
            print(f"Failures logged to {failures_path}", flush=True)
            sys.exit(1)
        return counts

    if use_lock:
        with archive_lock(lock_path):
            return _run()
    return _run()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        description="Download-only Tardis options_chain archive (newest-first)"
    )
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--to", dest="to_date", required=True)
    parser.add_argument("--exchange", default=None)
    parser.add_argument(
        "--priority-mode",
        choices=["deribit-first", "single"],
        default="deribit-first",
    )
    parser.add_argument("--upload-base", default=None)
    parser.add_argument("--staging-dir", default=DEFAULT_STAGING_DIR)
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    parser.add_argument("--failures", default=DEFAULT_FAILURES)
    parser.add_argument("--lock-file", default=DEFAULT_LOCK)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--ssh-port", type=int, default=None)
    parser.add_argument("--ssh-key", default=None)
    parser.add_argument("--day-retries", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-lock", action="store_true",
                        help="Disable single-instance lock (not recommended)")
    args = parser.parse_args()

    ssh_port = args.ssh_port
    if ssh_port is None:
        env_port = os.environ.get("STORAGE_BOX_PORT", "").strip()
        if env_port:
            ssh_port = int(env_port)

    ssh_key = args.ssh_key or os.environ.get("STORAGE_BOX_SSH_KEY") or None
    upload_base = resolve_upload_base(args.upload_base)

    if args.exchange:
        exchanges = [args.exchange]
    else:
        exchanges = list(EXCHANGE_PRIORITY)

    archive_fetch(
        exchanges=exchanges,
        from_date=args.from_date,
        to_date=args.to_date,
        upload_base=upload_base,
        api_key=args.api_key,
        staging_dir=args.staging_dir,
        manifest_path=args.manifest,
        failures_path=args.failures,
        lock_path=args.lock_file,
        ssh_port=ssh_port,
        ssh_key=ssh_key,
        day_retries=args.day_retries,
        dry_run=args.dry_run,
        use_lock=not args.no_lock,
    )


if __name__ == "__main__":
    main()
