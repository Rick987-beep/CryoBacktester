#!/usr/bin/env python3
"""Shared Tardis options_chain download helpers for bulk_fetch and archive_fetch."""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, timedelta
from typing import List, Optional, Tuple

try:
    import requests
except ImportError as exc:  # pragma: no cover
    raise SystemExit("pip install requests") from exc

logger = logging.getLogger(__name__)

DATASETS_BASE = "https://datasets.tardis.dev/v1"
RETRY_DELAYS = [10, 30, 60, 120, 300]

DEFAULT_EXCHANGE = "deribit"

EXCHANGE_PRIORITY = [
    "deribit",
    "binance-european-options",
    "okex-options",
    "bybit-options",
]

# Empty gzip placeholder from Tardis when no data exists for a day.
EMPTY_GZIP_MAX_BYTES = 20


def options_chain_url(exchange: str, date_str: str) -> str:
    year, month, day = date_str.split("-")
    return (
        f"{DATASETS_BASE}/{exchange}/options_chain/"
        f"{year}/{month}/{day}/OPTIONS.csv.gz"
    )


def archive_filename(date_str: str) -> str:
    return f"options_chain_{date_str}.csv.gz"


def date_range_reverse(from_date: str, to_date: str) -> List[date]:
    """Return dates from to_date down to from_date inclusive (newest-first)."""
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    if start > end:
        raise ValueError(f"--from {from_date} is after --to {to_date}")
    days: List[date] = []
    current = end
    while current >= start:
        days.append(current)
        current -= timedelta(days=1)
    return days


def auth_headers(api_key: Optional[str]) -> dict:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


def remote_size(
    exchange: str,
    date_str: str,
    api_key: Optional[str] = None,
) -> Optional[int]:
    """Return expected byte size from Tardis HEAD request, or None if unavailable."""
    url = options_chain_url(exchange, date_str)
    try:
        resp = requests.head(
            url,
            headers=auth_headers(api_key),
            timeout=15,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            length = int(resp.headers.get("content-length", 0))
            return length if length > 0 else None
    except Exception:
        pass
    return None


def probe_options_chain(
    exchange: str,
    date_str: str,
    api_key: Optional[str] = None,
) -> Tuple[str, int]:
    """Cheap availability check without downloading the full file.

    Returns (status, bytes_received) where status is one of:
      available  — real data (Content-Length > EMPTY_GZIP_MAX_BYTES or body > threshold)
      empty      — Tardis placeholder gzip (~20 bytes)
      forbidden  — HTTP 403
      error      — other failure
    """
    url = options_chain_url(exchange, date_str)
    try:
        resp = requests.get(
            url,
            headers=auth_headers(api_key),
            stream=True,
            timeout=(15, 30),
        )
        if resp.status_code == 403:
            return "forbidden", 0
        if resp.status_code == 401:
            return "error", 0
        if resp.status_code >= 400:
            return "error", 0

        cl = int(resp.headers.get("content-length", 0))
        if cl > EMPTY_GZIP_MAX_BYTES:
            resp.close()
            return "available", cl

        received = 0
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                received += len(chunk)
            if received > EMPTY_GZIP_MAX_BYTES:
                break
        resp.close()
        if received > EMPTY_GZIP_MAX_BYTES:
            return "available", received
        return "empty", received
    except Exception as exc:
        logger.debug("probe failed %s %s: %s", exchange, date_str, exc)
        return "error", 0


def download_options_chain(
    exchange: str,
    date_str: str,
    dest_path: str,
    api_key: Optional[str] = None,
    max_retries: int = 20,
    log_prefix: str = "",
) -> int:
    """Download OPTIONS.csv.gz for one exchange/date. Returns bytes written.

    Retries with exponential backoff on connection errors. tardis.dev does NOT
    support HTTP Range — every retry starts from byte 0.
    """
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    url = options_chain_url(exchange, date_str)
    prefix = log_prefix or f"[download] {exchange} {date_str}"
    print(f"{prefix}", flush=True)

    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            print(
                f"  Retry {attempt}/{max_retries} in {delay}s"
                f"  (last error: {last_exc})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)

        resp = None
        try:
            resp = requests.get(
                url,
                headers=auth_headers(api_key),
                stream=True,
                timeout=(30, 1800),
            )
            if resp.status_code == 401:
                raise RuntimeError(
                    f"{prefix}: HTTP 401 Unauthorized — check TARDIS_API_KEY"
                )
            if resp.status_code == 403:
                body = (resp.text or "")[:200]
                raise RuntimeError(
                    f"{prefix}: HTTP 403 Forbidden"
                    f" (transfer limit or date unavailable) body={body!r}"
                )
            if 400 <= resp.status_code < 500:
                raise RuntimeError(
                    f"{prefix}: HTTP {resp.status_code} — not retrying"
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
            with open(dest_path, "wb") as fout:
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
                            eta = (
                                (total - written) / (written / elapsed)
                                if written > 0
                                else 0
                            )
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

            final_size = os.path.getsize(dest_path)
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
            return final_size

        except RuntimeError:
            raise
        except Exception as exc:
            last_exc = exc
            if os.path.exists(dest_path):
                os.unlink(dest_path)
            print(
                f"\n  Attempt {attempt + 1} failed: {exc}",
                file=sys.stderr,
                flush=True,
            )
            if attempt == max_retries:
                raise RuntimeError(
                    f"{prefix}: failed after {max_retries + 1} attempts"
                ) from exc
        finally:
            if resp is not None:
                resp.close()

    raise RuntimeError(f"{prefix}: exhausted retries")
