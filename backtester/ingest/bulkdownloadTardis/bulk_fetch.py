#!/usr/bin/env python3
"""
bulk_fetch.py — Reverse-order bulk downloader and extractor for Tardis options data.

For each day in the given date range (processed newest-first):
    1. Skip if both output parquets already exist.
    2. Download OPTIONS.csv.gz from tardis.dev (~9.5 GB, no server-side resume).
    3. stream_extract: stream .csv.gz → options_YYYY-MM-DD.parquet + spot_YYYY-MM-DD.parquet
    4. clean: apply 6 cleaning steps, rewrite parquets in-place.
    5. Delete raw .csv.gz (UNLESS the day was flagged suspect — raw kept for inspection).

Processing is newest-first so recent data is available first for backtesting.
Each worker handles its own date range independently — no cross-worker coordination.

Designed to run unattended inside tmux on a Hetzner server.

Usage:
    # Worker A: most recent 100-day block
    python bulk_fetch.py --from 2025-12-29 --to 2026-03-08 --worker A

    # Custom data dir
    python bulk_fetch.py --from 2025-06-10 --to 2025-09-18 --worker C --data-dir /bulk/data

    # Keep raw .csv.gz files (useful for debugging)
    python bulk_fetch.py --from 2026-03-01 --to 2026-03-08 --worker A --keep-raw

    # Dry-run: show what would be processed without downloading
    python bulk_fetch.py --from 2026-03-01 --to 2026-03-08 --worker A --dry-run

API key:
    Set TARDIS_API_KEY environment variable, or pass --api-key.
    Without a key only the 1st of each month is available (free tier).
    Free days are useful for smoke tests without spending API quota.

tmux (recommended for long runs):
    tmux new -s bulk
    TARDIS_API_KEY=... python bulk_fetch.py --from ... --to ... --worker A
    # Ctrl-B D to detach
    # tmux attach -t bulk to reattach
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta
from typing import List, Optional

# ── Inline download logic (no dependency on existing backtester pipeline) ─────
# Reproduced here so the bulk/ folder is self-contained on the Hetzner server.

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)

try:
    import pyarrow.parquet as pq
except ImportError:
    print("pip install pyarrow", file=sys.stderr)
    sys.exit(1)

from stream_extract import stream_extract
from clean import clean_parquets

DATASETS_BASE = "https://datasets.tardis.dev/v1"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

_RETRY_DELAYS = [10, 30, 60, 120, 300]


# ── Download ──────────────────────────────────────────────────────────────────

def _download(date_str, api_key=None, data_dir=DATA_DIR, max_retries=20):
    # type: (str, Optional[str], str, int) -> str
    """Download OPTIONS.csv.gz for one date. Returns path to gz file.

    Retries with exponential backoff on any connection error. tardis.dev does
    NOT support HTTP Range — every retry starts from byte 0. Run in tmux to
    survive terminal disconnects.
    """
    os.makedirs(data_dir, exist_ok=True)

    year, month, day = date_str.split("-")
    url = f"{DATASETS_BASE}/deribit/options_chain/{year}/{month}/{day}/OPTIONS.csv.gz"
    gz_path = os.path.join(data_dir, f"options_chain_{date_str}.csv.gz")

    print(f"[download] {date_str}", flush=True)

    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    last_exc = None  # type: Optional[Exception]

    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = _RETRY_DELAYS[min(attempt - 1, len(_RETRY_DELAYS) - 1)]
            print(
                f"  Retry {attempt}/{max_retries} in {delay}s"
                f"  (last error: {last_exc})",
                file=sys.stderr, flush=True,
            )
            time.sleep(delay)

        resp = None
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=(30, 120))
            # Abort immediately on 4xx — retrying won't help (bad key, wrong date, etc.)
            if resp.status_code == 401:
                raise RuntimeError(
                    f"[download] {date_str}: HTTP 401 Unauthorized — check TARDIS_API_KEY"
                )
            if resp.status_code == 403:
                raise RuntimeError(
                    f"[download] {date_str}: HTTP 403 Forbidden — date not available on this plan"
                )
            if 400 <= resp.status_code < 500:
                raise RuntimeError(
                    f"[download] {date_str}: HTTP {resp.status_code} — not retrying"
                )
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0))
            print(
                f"  Size: {total / 1024**3:.2f} GB" if total else "  Size: unknown",
                flush=True,
            )

            written = 0
            t0 = time.time()
            t_last_log = t0
            with open(gz_path, "wb") as fout:
                for chunk in resp.iter_content(chunk_size=512 * 1024):
                    if not chunk:
                        continue
                    fout.write(chunk)
                    written += len(chunk)
                    now = time.time()
                    if now - t_last_log >= 10:
                        elapsed = now - t0
                        speed = written / elapsed / 1024**2 if elapsed > 0 else 0
                        if total:
                            eta = (total - written) / (written / elapsed) if written > 0 else 0
                            print(
                                f"  {written/1024**3:.2f}/{total/1024**3:.2f} GB"
                                f"  ({written/total*100:.0f}%)"
                                f"  {speed:.1f} MB/s"
                                f"  ETA {eta/60:.0f}m",
                                flush=True,
                            )
                        else:
                            print(
                                f"  {written/1024**3:.2f} GB  {speed:.1f} MB/s",
                                flush=True,
                            )
                        t_last_log = now

            final_size = os.path.getsize(gz_path)
            if total > 0 and final_size < total:
                raise IOError(
                    f"Truncated: got {final_size:,} of {total:,} bytes"
                )

            elapsed = time.time() - t0
            speed = written / elapsed / 1024**2 if elapsed > 0 else 0
            print(
                f"\n  Downloaded: {final_size / 1024**3:.2f} GB"
                f"  in {elapsed:.0f}s  ({speed:.1f} MB/s avg)",
                flush=True,
            )
            return gz_path

        except RuntimeError:
            # Hard failures (4xx) — propagate immediately without retry
            raise
        except Exception as exc:
            last_exc = exc
            # Wipe partial file so next attempt writes a clean file.
            # tardis.dev has no server-side resume — a partial gz is unusable.
            if gz_path and os.path.exists(gz_path):
                os.unlink(gz_path)
            print(f"\n  Attempt {attempt + 1} failed: {exc}", file=sys.stderr, flush=True)
            if attempt == max_retries:
                raise RuntimeError(
                    f"[download] {date_str}: failed after {max_retries + 1} attempts"
                ) from exc
        finally:
            if resp is not None:
                resp.close()

    raise RuntimeError(f"[download] {date_str}: exhausted retries")


# ── Remote size check ────────────────────────────────────────────────────────

def _validate_gz_size(date_str, api_key=None):
    # type: (str, Optional[str]) -> Optional[int]
    """Return expected byte size from Tardis HEAD request, or None if unavailable."""
    year, month, day = date_str.split("-")
    url = f"{DATASETS_BASE}/deribit/options_chain/{year}/{month}/{day}/OPTIONS.csv.gz"
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        resp = requests.head(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            length = int(resp.headers.get("content-length", 0))
            return length if length > 0 else None
    except Exception:
        pass
    return None


# ── Date range ────────────────────────────────────────────────────────────────

def _date_range_reverse(from_date, to_date):
    # type: (str, str) -> List[date]
    """Return dates from to_date down to from_date inclusive (newest-first)."""
    start = date.fromisoformat(from_date)
    end   = date.fromisoformat(to_date)
    if start > end:
        raise ValueError(f"--from {from_date} is after --to {to_date}")
    days = []
    current = end
    while current >= start:
        days.append(current)
        current -= timedelta(days=1)
    return days


# ── Per-day pipeline ──────────────────────────────────────────────────────────

def _process_day(
    date_str,           # type: str
    api_key,            # type: Optional[str]
    data_dir,           # type: str
    max_dte,            # type: int
    keep_raw,           # type: bool
    day_retries,        # type: int
    force=False,        # type: bool
):
    # type: (...) -> str
    """Download + extract + clean one day. Returns 'ok', 'skipped', or 'failed'.

    If cleaning flags the day as suspect (< 10k rows), the raw .csv.gz is kept
    and a .warn file is written next to the parquet.
    """
    opts_path = os.path.join(data_dir, f"options_{date_str}.parquet")
    spot_path = os.path.join(data_dir, f"spot_{date_str}.parquet")
    gz_path   = os.path.join(data_dir, f"options_chain_{date_str}.csv.gz")

    # Skip if both outputs already exist (unless --force)
    if not force and os.path.exists(opts_path) and os.path.exists(spot_path):
        print(f"[skip] {date_str}  (parquets already exist)", flush=True)
        return "skipped"

    t_day = time.time()

    for attempt in range(day_retries + 1):
        try:
            if attempt > 0:
                print(
                    f"[retry] {date_str}  day-level attempt {attempt}/{day_retries}",
                    flush=True,
                )

            # 1. Download — skip if gz already on disk from a previous crashed run.
            # Validate size via HEAD request; re-download if the file is partial.
            if os.path.exists(gz_path):
                gz_size = os.path.getsize(gz_path)
                expected = _validate_gz_size(date_str, api_key=api_key)
                if expected is None or gz_size == expected:
                    print(
                        f"[download] {date_str}  reusing gz"
                        f"  ({gz_size/1024**3:.2f} GB"
                        + (" — verified" if expected else " — unverified")
                        + ")",
                        flush=True,
                    )
                else:
                    print(
                        f"[download] {date_str}  partial gz detected"
                        f"  ({gz_size/1024**3:.2f} of {expected/1024**3:.2f} GB)"
                        f" — re-downloading",
                        flush=True,
                    )
                    os.unlink(gz_path)
                    _download(date_str, api_key=api_key, data_dir=data_dir)
            else:
                _download(date_str, api_key=api_key, data_dir=data_dir)

            # 2. stream_extract: .csv.gz → two parquets
            stream_extract(date_str, gz_path=gz_path, max_dte=max_dte, data_dir=data_dir)

            # 3. Clean: validate + fix in-place, get report
            report = clean_parquets(opts_path, spot_path, date_str)

            # 4. Handle suspect days
            if report.suspect:
                warn_path = opts_path.replace(".parquet", ".warn")
                with open(warn_path, "w") as wf:
                    wf.write(report.summary() + "\n")
                print(
                    f"[WARN] {date_str}  suspect day ({report.final_rows:,} rows)"
                    f"  — raw file kept, .warn written",
                    flush=True,
                )
                # Keep raw file for inspection — do not delete
                elapsed = time.time() - t_day
                print(f"[done] {date_str}  {elapsed:.0f}s  (suspect)", flush=True)
                return "suspect"

            # 5. Delete raw .csv.gz
            if not keep_raw and os.path.exists(gz_path):
                os.unlink(gz_path)
                print(f"  Deleted raw: {gz_path}", flush=True)

            elapsed = time.time() - t_day
            print(f"[done] {date_str}  {elapsed:.0f}s", flush=True)
            return "ok"

        except RuntimeError as exc:
            # Hard failures (e.g. HTTP 4xx) — don't retry, fail the whole worker
            print(f"[HARD FAIL] {date_str}: {exc}", file=sys.stderr, flush=True)
            return "failed"
        except Exception as exc:
            print(f"[error] {date_str}  attempt {attempt + 1}: {exc}", file=sys.stderr, flush=True)
            # Clean up partial parquets and gz before retry.
            # _download() already wipes its own partial writes on network errors,
            # so any gz still present here is either a reused file that turned out
            # truncated/corrupt, or a freshly completed download that stream_extract
            # failed on.  Either way it must be deleted so the next attempt
            # re-downloads a fresh copy.
            for p in (opts_path, spot_path, gz_path):
                if os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
            if attempt == day_retries:
                print(f"[FAILED] {date_str}  giving up after {day_retries + 1} attempts", flush=True)
                return "failed"

    return "failed"  # unreachable


# ── Main orchestrator ─────────────────────────────────────────────────────────

def bulk_fetch(
    from_date,          # type: str
    to_date,            # type: str
    worker="?",         # type: str
    api_key=None,       # type: Optional[str]
    max_dte=700,        # type: int
    keep_raw=False,     # type: bool
    data_dir=DATA_DIR,  # type: str
    day_retries=3,      # type: int
    dry_run=False,      # type: bool
    force=False,        # type: bool
):
    # type: (...) -> None
    """Process a date range newest-first, skipping days already done.

    Args:
        from_date:   Start of range YYYY-MM-DD (inclusive, oldest).
        to_date:     End of range YYYY-MM-DD (inclusive, newest, processed first).
        worker:      Label for this worker (A/B/C/D) — used only for log headings.
        api_key:     Tardis.dev API key. Falls back to TARDIS_API_KEY env var.
        max_dte:     Max DTE to include in snapshots (default 700 = no practical cap).
        keep_raw:    Keep .csv.gz after extraction (default False).
        data_dir:    Directory for all files.
        day_retries: Day-level retry count before marking a day failed.
        dry_run:     Print what would run without downloading anything.
        force:       Re-process days that already have parquets (e.g. after a bug fix).
    """
    if api_key is None:
        api_key = os.environ.get("TARDIS_API_KEY")

    os.makedirs(data_dir, exist_ok=True)
    dates = _date_range_reverse(from_date, to_date)

    print(
        f"\n{'='*60}\n"
        f"Worker {worker}  |  {len(dates)} days  |  {to_date} → {from_date}\n"
        f"data_dir={data_dir}  max_dte={max_dte}  keep_raw={keep_raw}\n"
        f"{'='*60}\n",
        flush=True,
    )

    if dry_run:
        print("DRY RUN — no downloads will happen\n")
        opts_existing = sum(
            1 for d in dates
            if os.path.exists(os.path.join(data_dir, f"options_{d}.parquet"))
            and os.path.exists(os.path.join(data_dir, f"spot_{d}.parquet"))
        )
        needed = len(dates) - opts_existing
        print(f"  Already done: {opts_existing}")
        print(f"  Would download: {needed}")
        for d in dates[:10]:
            date_str = d.isoformat()
            opts_path = os.path.join(data_dir, f"options_{date_str}.parquet")
            spot_path = os.path.join(data_dir, f"spot_{date_str}.parquet")
            status = "SKIP" if (os.path.exists(opts_path) and os.path.exists(spot_path)) else "TODO"
            print(f"  {date_str}  [{status}]")
        if len(dates) > 10:
            print(f"  ... ({len(dates) - 10} more days)")
        return

    counts = {"ok": 0, "skipped": 0, "suspect": 0, "failed": 0}
    t_start = time.time()

    for i, d in enumerate(dates):
        date_str = d.isoformat()
        print(f"\n─── Worker {worker}  [{i+1}/{len(dates)}]  {date_str} ───", flush=True)
        status = _process_day(
            date_str=date_str,
            api_key=api_key,
            data_dir=data_dir,
            max_dte=max_dte,
            keep_raw=keep_raw,
            day_retries=day_retries,
            force=force,
        )
        counts[status] = counts.get(status, 0) + 1

    elapsed = time.time() - t_start
    print(
        f"\n{'='*60}\n"
        f"Worker {worker} done in {elapsed/3600:.1f}h\n"
        f"  ok={counts['ok']}  skipped={counts['skipped']}"
        f"  suspect={counts['suspect']}  failed={counts['failed']}\n"
        f"{'='*60}\n",
        flush=True,
    )

    if counts["failed"]:
        sys.exit(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    # type: () -> None
    parser = argparse.ArgumentParser(
        description="Bulk download + extract + clean Tardis options data (newest-first)"
    )
    parser.add_argument("--from", dest="from_date", required=True,
                        help="Start date YYYY-MM-DD (oldest, inclusive)")
    parser.add_argument("--to",   dest="to_date",   required=True,
                        help="End date YYYY-MM-DD (newest, inclusive, processed first)")
    parser.add_argument("--worker", default="?",
                        help="Worker label A/B/C/D (for log output)")
    parser.add_argument("--api-key", default=None,
                        help="Tardis.dev API key (or set TARDIS_API_KEY env var)")
    parser.add_argument("--max-dte", type=int, default=700,
                        help="Max calendar DTE to include (default 28)")
    parser.add_argument("--keep-raw", action="store_true",
                        help="Keep OPTIONS.csv.gz after extraction")
    parser.add_argument("--data-dir", default=DATA_DIR,
                        help="Data directory (default: data/ next to this script)")
    parser.add_argument("--day-retries", type=int, default=3,
                        help="Day-level retry attempts before marking a day failed (default 3)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would run without downloading")
    parser.add_argument("--force", action="store_true",
                        help="Re-process days that already have parquets")
    args = parser.parse_args()

    bulk_fetch(
        from_date=args.from_date,
        to_date=args.to_date,
        worker=args.worker,
        api_key=args.api_key,
        max_dte=args.max_dte,
        keep_raw=args.keep_raw,
        data_dir=args.data_dir,
        day_retries=args.day_retries,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
