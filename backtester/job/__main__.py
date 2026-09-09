"""CLI: python -m backtester.job snapshot|ping|status|cancel|set-concurrency"""
from __future__ import annotations

import argparse
import json
import sys

from backtester.job.api import QueueClient


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m backtester.job")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("snapshot", help="Queue snapshot from files")
    sub.add_parser("ping", help="Is jobd listening?")
    p_status = sub.add_parser("status")
    p_status.add_argument("job_id")
    p_cancel = sub.add_parser("cancel")
    p_cancel.add_argument("job_id")
    p_conc = sub.add_parser("set-concurrency")
    p_conc.add_argument("n", type=int)
    args = parser.parse_args(argv)

    client = QueueClient()
    if args.cmd == "ping":
        ok = client.ping()
        print("ok" if ok else "down")
        return 0 if ok else 1
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
    if args.cmd == "status":
        view = client.store.get(args.job_id)
        if view is None:
            print("missing", file=sys.stderr)
            return 1
        print(json.dumps({"job_id": view.job_id, "state": view.state, "pid": view.pid}))
        return 0
    if args.cmd == "cancel":
        view = client.cancel(args.job_id)
        print(view.state)
        return 0
    if args.cmd == "set-concurrency":
        print(client.set_concurrency(args.n))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
