"""jobd: FIFO queue, concurrency cap, Unix-domain JSON commands.

Unit tests drive ``Supervisor`` in-process. The socket server is only for
QueueClient / CLI. Stub jobs must finish in well under a second.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import select
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections import deque
from pathlib import Path

from backtester.core.paths import jobs_dir
from backtester.job.api import JobSpec, JobStore, atomic_write_json


class Supervisor:
    def __init__(self, root: Path, *, stub: bool = False):
        self.store = JobStore(root)
        self.root = Path(root)
        self.stub = stub
        self.concurrency = 1
        self.queued: deque[str] = deque()
        self.running: dict[str, subprocess.Popen] = {}
        self._sock: socket.socket | None = None
        self._stop = False

    def persist_queue(self) -> None:
        self.store.write_queue(
            concurrency=self.concurrency,
            queued=list(self.queued),
            running=list(self.running),
            supervisor_pid=os.getpid(),
        )

    def kill_remaining(self) -> None:
        for proc in list(self.running.values()):
            _kill_proc(proc)
        self.running.clear()

    def _spawn(self, job_id: str) -> subprocess.Popen:
        log_path = self.store.job_dir(job_id) / "job.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["CRYOBT_JOBS"] = str(self.root)
        env["CRYOBT_JOB_PEERS"] = str(max(1, self.concurrency))
        env["PYTHONUNBUFFERED"] = "1"
        from backtester.core.paths import repo_root

        env["PYTHONPATH"] = str(repo_root()) + os.pathsep + env.get("PYTHONPATH", "")
        if self.stub:
            env["CRYOBT_JOB_STUB"] = "1"
            env.setdefault("CRYOBT_JOB_STUB_SECS", "0.5")
        logf = open(log_path, "ab")
        proc = subprocess.Popen(
            [sys.executable, "-m", "backtester.job.runner", job_id],
            stdout=logf,
            stderr=logf,
            env=env,
            start_new_session=True,
            close_fds=True,
        )
        self.store.write_status(
            job_id,
            {
                "state": "running",
                "pid": proc.pid,
                "phase": "starting",
                "heartbeat_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )
        return proc

    def fill_slots(self) -> None:
        while len(self.running) < max(1, self.concurrency) and self.queued:
            job_id = self.queued.popleft()
            view = self.store.get(job_id)
            if view is None or view.state in ("cancelled", "done", "error", "dead"):
                continue
            self.running[job_id] = self._spawn(job_id)
        self.persist_queue()

    def reap(self) -> None:
        finished = []
        for job_id, proc in list(self.running.items()):
            code = proc.poll()
            if code is None:
                continue
            finished.append(job_id)
            view = self.store.get(job_id)
            if view is None or view.state in ("done", "cancelled", "error"):
                continue
            dead = code < 0 or code == 9
            state = "dead" if dead else "error"
            self.store.write_status(
                job_id,
                {
                    "state": state,
                    "pid": proc.pid,
                    "error": f"exit {code}",
                    "heartbeat_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
            )
            self.store.write_result(job_id, {"state": state, "exit": code})
        for job_id in finished:
            self.running.pop(job_id, None)
        if finished:
            self.fill_slots()

    def enqueue(self, spec: JobSpec) -> dict:
        job_id = uuid.uuid4().hex[:12]
        self.store.write_spec(job_id, spec)
        self.store.write_status(job_id, {"state": "queued"})
        self.queued.append(job_id)
        self.fill_slots()
        view = self.store.get(job_id)
        return {"ok": True, "job_id": job_id, "state": view.state if view else "queued"}

    def cancel(self, job_id: str) -> dict:
        if job_id in self.queued:
            self.queued = deque(j for j in self.queued if j != job_id)
            self.store.write_status(job_id, {"state": "cancelled"})
            self.store.write_result(job_id, {"state": "cancelled", "bundle_path": None})
            self.persist_queue()
            return {"ok": True, "state": "cancelled"}
        proc = self.running.get(job_id)
        if proc is not None and proc.poll() is None:
            _kill_proc(proc, sig=signal.SIGTERM)
            self.store.write_status(job_id, {"state": "cancelled", "pid": proc.pid})
            self.store.write_result(job_id, {"state": "cancelled", "bundle_path": None})
            return {"ok": True, "state": "cancel_requested"}
        view = self.store.get(job_id)
        if view is None:
            return {"ok": False, "error": "unknown job"}
        return {"ok": True, "state": view.state}

    def set_concurrency(self, n: int) -> dict:
        self.concurrency = max(1, int(n))
        self.fill_slots()
        return {"ok": True, "concurrency": self.concurrency}

    def handle(self, msg: dict) -> dict:
        op = msg.get("op")
        if op == "ping":
            return {"ok": True, "pid": os.getpid()}
        if op == "enqueue":
            return self.enqueue(JobSpec.from_dict(msg.get("spec") or {}))
        if op == "snapshot":
            snap = self.store.read_snapshot()
            return {
                "ok": True,
                "concurrency": snap.concurrency,
                "running": [v.job_id for v in snap.running],
                "queued": [v.job_id for v in snap.queued],
                "recent": [v.job_id for v in snap.recent],
                "supervisor_pid": os.getpid(),
            }
        if op == "status":
            view = self.store.get(str(msg.get("job_id") or ""))
            if view is None:
                return {"ok": False, "error": "unknown job"}
            return {"ok": True, "job": {"job_id": view.job_id, "state": view.state, "pid": view.pid}}
        if op == "cancel":
            return self.cancel(str(msg.get("job_id") or ""))
        if op == "set_concurrency":
            return self.set_concurrency(int(msg.get("n") or 1))
        if op == "shutdown":
            self._stop = True
            return {"ok": True}
        return {"ok": False, "error": f"unknown op {op}"}

    def serve_forever(self, *, idle_sec: float | None = None) -> None:
        """Listen on queue.sock. idle_sec=0 means never auto-exit (CLI). Tests pass 2."""
        if idle_sec is None:
            idle_sec = float(os.environ.get("CRYOBT_JOBD_IDLE_SEC", "8"))
        self.root.mkdir(parents=True, exist_ok=True)
        path = sock_path(self.root)
        if path.exists():
            path.unlink()
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(path))
        srv.listen(8)
        srv.setblocking(False)
        self._sock = srv
        atomic_write_json(self.root / "supervisor.pid", {"pid": os.getpid()})
        self.persist_queue()
        idle_since = None
        try:
            while not self._stop:
                self.reap()
                if idle_sec > 0 and not self.running and not self.queued:
                    if idle_since is None:
                        idle_since = time.time()
                    elif time.time() - idle_since >= idle_sec:
                        break
                else:
                    idle_since = None
                ready, _, _ = select.select([self._sock], [], [], 0.1)
                if not ready:
                    continue
                try:
                    conn, _ = self._sock.accept()
                except BlockingIOError:
                    continue
                with conn:
                    conn.settimeout(2)
                    buf = b""
                    try:
                        while b"\n" not in buf:
                            chunk = conn.recv(65536)
                            if not chunk:
                                break
                            buf += chunk
                    except TimeoutError:
                        continue
                    if not buf.strip():
                        continue
                    try:
                        msg = json.loads(buf.decode())
                    except json.JSONDecodeError:
                        conn.sendall(b'{"ok":false,"error":"bad json"}\n')
                        continue
                    reply = self.handle(msg)
                    conn.sendall((json.dumps(reply) + "\n").encode())
        finally:
            self.kill_remaining()
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
            if path.exists():
                path.unlink()


def _kill_proc(proc: subprocess.Popen, sig: int = signal.SIGKILL) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, sig)
    except OSError:
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=1)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except OSError:
            pass


def sock_path(root: Path) -> Path:
    """Unix socket path. macOS sockaddr_un is ~104 bytes, so long pytest
    tmp dirs spill into /tmp/cryobt-job-<hash>.sock."""
    candidate = Path(root) / "queue.sock"
    if len(str(candidate.resolve())) < 90:
        return candidate
    digest = hashlib.sha1(str(Path(root).resolve()).encode()).hexdigest()[:12]
    return Path("/tmp") / f"cryobt-job-{digest}.sock"


def rpc(root: Path, payload: dict, timeout: float = 2.0) -> dict:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(sock_path(root)))
        sock.sendall((json.dumps(payload) + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
        if not buf:
            raise ConnectionError("empty jobd response")
        return json.loads(buf.decode())
    finally:
        sock.close()


def ensure_jobd(root: Path, timeout: float = 5.0) -> None:
    """Start a jobd subprocess if the socket is down. Idle-exits (default 8s)."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    try:
        if rpc(root, {"op": "ping"}, timeout=0.3).get("ok"):
            return
    except OSError:
        pass
    sp = sock_path(root)
    if sp.exists():
        try:
            sp.unlink()
        except OSError:
            pass
    env = os.environ.copy()
    env["CRYOBT_JOBS"] = str(root)
    env.setdefault("CRYOBT_JOBD_IDLE_SEC", "8")
    env["PYTHONUNBUFFERED"] = "1"
    from backtester.core.paths import repo_root

    env["PYTHONPATH"] = str(repo_root()) + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable, "-m", "backtester.job.supervisor", "--root", str(root)]
    if env.get("CRYOBT_JOB_STUB") == "1":
        cmd.append("--stub")
    subprocess.Popen(
        cmd,
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if rpc(root, {"op": "ping"}, timeout=0.3).get("ok"):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"jobd did not start at {sp}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m backtester.job.supervisor")
    parser.add_argument("--root", default=None)
    parser.add_argument("--stub", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.root) if args.root else Path(os.environ.get("CRYOBT_JOBS") or jobs_dir())
    stub = bool(args.stub) or os.environ.get("CRYOBT_JOB_STUB") == "1"
    Supervisor(root, stub=stub).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
