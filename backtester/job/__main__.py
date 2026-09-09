"""CLI: python -m backtester.job <enqueue|snapshot|status|cancel|…>

Control commands land in I2/I3 once jobd exists. Snapshot already works
from files.
"""
from __future__ import annotations

import argparse
import json
import sys

from backtester.job.api import QueueClient


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m backtester.job")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot", help="Print queue snapshot from files (no jobd required)")
    sub.add_parser("ping", help="Is jobd listening on queue.sock?")
    args = parser.parse_args(argv)

    client = QueueClient()
    if args.cmd == "ping":
        print("ok" if client.ping() else "down")
        return 0 if client.ping() else 1
    if args.cmd == "snapshot":
        snap = client.snapshot()
        print(
            json.dumps(
                {
                    "concurrency": snap.concurrency,
                    "supervisor_pid": snap.supervisor_pid,
                    "running": [v.job_id for v in snap.running],
                    "queued": [v.job_id for v in snap.queued],
                    "recent": [v.job_id for v in snap.recent],
                },
                indent=2,
            )
        )
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
