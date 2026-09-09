"""Detached-job file protocol and GUI-facing types.

Observation is files-only (``JobStore``). Commands go through ``QueueClient``
(Unix socket to jobd) in a later step — this module already defines the
types that freeze.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from backtester.core.paths import jobs_dir as default_jobs_dir

JobState = Literal["queued", "running", "done", "error", "cancelled", "dead"]

HEARTBEAT_STALE_SEC = 90
RECENT_TERMINAL = 20
TERMINAL_STATES = frozenset({"done", "error", "cancelled", "dead"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def atomic_write_json(path: Path, obj: Any) -> None:
    """Write JSON via tmp+rename so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, indent=2, default=str)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(payload)
    os.replace(tmp, path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@dataclass
class JobSpec:
    strategy: str
    param_grid: dict[str, list[Any]]
    date_from: str | None = None
    date_to: str | None = None
    account_size: float = 100_000.0
    requested_inner_workers: int | None = None
    options_path: str | None = None
    spot_path: str | None = None
    source: str = "cli"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobSpec":
        return cls(
            strategy=str(data["strategy"]),
            param_grid=dict(data.get("param_grid") or {}),
            date_from=data.get("date_from"),
            date_to=data.get("date_to"),
            account_size=float(data.get("account_size", 100_000.0)),
            requested_inner_workers=data.get("requested_inner_workers"),
            options_path=data.get("options_path"),
            spot_path=data.get("spot_path"),
            source=str(data.get("source") or "cli"),
        )


@dataclass
class JobView:
    job_id: str
    state: JobState
    spec: JobSpec
    phase: str | None = None
    current: int | None = None
    total: int | None = None
    date: str | None = None
    inner_workers_effective: int | None = None
    pid: int | None = None
    submitted_at: str = ""
    heartbeat_ts: str | None = None
    bundle_path: str | None = None
    error: str | None = None
    queue_position: int | None = None


@dataclass
class QueueSnapshot:
    concurrency: int
    supervisor_pid: int | None
    running: list[JobView] = field(default_factory=list)
    queued: list[JobView] = field(default_factory=list)
    recent: list[JobView] = field(default_factory=list)


class JobStore:
    """File-only job observation. No socket. Safe if jobd is down."""

    def __init__(self, root: str | Path | None = None):
        self.root = Path(root) if root is not None else default_jobs_dir()

    def job_dir(self, job_id: str) -> Path:
        return self.root / job_id

    def queue_path(self) -> Path:
        return self.root / "queue.json"

    def write_spec(self, job_id: str, spec: JobSpec, *, submitted_at: str | None = None) -> Path:
        """Persist spec.json. Refuses overwrite — PARAM_GRID is immutable after start."""
        d = self.job_dir(job_id)
        spec_path = d / "spec.json"
        if spec_path.exists():
            raise FileExistsError(f"job spec is immutable: {spec_path}")
        submitted = submitted_at or _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")
        atomic_write_json(
            spec_path,
            {"job_id": job_id, "submitted_at": submitted, **spec.to_dict()},
        )
        return spec_path

    def write_status(self, job_id: str, payload: dict[str, Any]) -> Path:
        path = self.job_dir(job_id) / "status.json"
        atomic_write_json(path, payload)
        return path

    def write_result(self, job_id: str, payload: dict[str, Any]) -> Path:
        path = self.job_dir(job_id) / "result.json"
        atomic_write_json(path, payload)
        return path

    def write_queue(
        self,
        *,
        concurrency: int = 1,
        queued: list[str] | None = None,
        running: list[str] | None = None,
        supervisor_pid: int | None = None,
    ) -> Path:
        atomic_write_json(
            self.queue_path(),
            {
                "concurrency": int(concurrency),
                "queued": list(queued or []),
                "running": list(running or []),
                "supervisor_pid": supervisor_pid,
            },
        )
        return self.queue_path()

    def get(self, job_id: str) -> JobView | None:
        d = self.job_dir(job_id)
        spec_path = d / "spec.json"
        if not spec_path.is_file():
            return None
        return self._view_from_dir(d)

    def list_jobs(self) -> list[JobView]:
        if not self.root.is_dir():
            return []
        out: list[JobView] = []
        for child in sorted(self.root.iterdir()):
            if child.is_dir() and (child / "spec.json").is_file():
                view = self._view_from_dir(child)
                if view is not None:
                    out.append(view)
        return out

    def read_snapshot(self) -> QueueSnapshot:
        views = {v.job_id: v for v in self.list_jobs()}
        concurrency = 1
        supervisor_pid = None
        queued_ids: list[str] = []
        running_ids: list[str] = []
        qpath = self.queue_path()
        if qpath.is_file():
            raw = read_json(qpath)
            concurrency = int(raw.get("concurrency") or 1)
            supervisor_pid = raw.get("supervisor_pid")
            queued_ids = [str(x) for x in (raw.get("queued") or [])]
            running_ids = [str(x) for x in (raw.get("running") or [])]
        else:
            running_ids = [v.job_id for v in views.values() if v.state == "running"]
            queued = [v for v in views.values() if v.state == "queued"]
            queued.sort(key=lambda v: v.submitted_at)
            queued_ids = [v.job_id for v in queued]

        def _take(job_id: str, idx: int, fallback_state: JobState) -> JobView | None:
            view = views.get(job_id)
            if view is None or view.state in TERMINAL_STATES:
                return None
            view.queue_position = idx
            view.state = fallback_state
            return view

        running = []
        for jid in running_ids:
            v = _take(jid, 0, "running")
            if v is not None:
                running.append(v)
        queued = []
        for i, jid in enumerate(queued_ids):
            v = _take(jid, i + (1 if running else 0), "queued")
            if v is not None:
                queued.append(v)

        seen = {v.job_id for v in running + queued}
        recent = [v for v in views.values() if v.job_id not in seen and v.state in TERMINAL_STATES]
        recent.sort(key=lambda v: v.submitted_at, reverse=True)
        recent = recent[:RECENT_TERMINAL]
        for v in recent:
            v.queue_position = None
        return QueueSnapshot(
            concurrency=concurrency,
            supervisor_pid=supervisor_pid,
            running=running,
            queued=queued,
            recent=recent,
        )

    def _view_from_dir(self, d: Path) -> JobView | None:
        spec_path = d / "spec.json"
        if not spec_path.is_file():
            return None
        blob = read_json(spec_path)
        job_id = str(blob.get("job_id") or d.name)
        submitted_at = str(blob.get("submitted_at") or "")
        spec = JobSpec.from_dict(blob)
        status: dict[str, Any] = {}
        status_path = d / "status.json"
        if status_path.is_file():
            try:
                status = read_json(status_path)
            except json.JSONDecodeError:
                status = {}
        result: dict[str, Any] = {}
        result_path = d / "result.json"
        if result_path.is_file():
            try:
                result = read_json(result_path)
            except json.JSONDecodeError:
                result = {}

        state = str(status.get("state") or result.get("state") or "queued")
        heartbeat_ts = status.get("heartbeat_ts")
        if state == "running" and _heartbeat_stale(heartbeat_ts):
            state = "dead"
        if state not in {"queued", "running", "done", "error", "cancelled", "dead"}:
            state = "error"

        return JobView(
            job_id=job_id,
            state=state,  # type: ignore[arg-type]
            spec=spec,
            phase=status.get("phase"),
            current=status.get("current"),
            total=status.get("total"),
            date=status.get("date"),
            inner_workers_effective=status.get("inner_workers_effective"),
            pid=status.get("pid"),
            submitted_at=submitted_at,
            heartbeat_ts=heartbeat_ts,
            bundle_path=result.get("bundle_path") or status.get("bundle_path"),
            error=result.get("error") or status.get("error"),
        )


def _heartbeat_stale(heartbeat_ts: str | None, *, now: datetime | None = None) -> bool:
    dt = parse_ts(heartbeat_ts)
    if dt is None:
        return True
    age = ((now or _utc_now()) - dt).total_seconds()
    return age > HEARTBEAT_STALE_SEC


class QueueClient:
    """Socket client to jobd. I1: files-only snapshot; enqueue arrives in I3."""

    def __init__(self, root: str | Path | None = None):
        self.store = JobStore(root)

    def ping(self) -> bool:
        sock = self.store.root / "queue.sock"
        return sock.exists()

    def snapshot(self) -> QueueSnapshot:
        return self.store.read_snapshot()

    def enqueue(self, spec: JobSpec) -> JobView:
        raise NotImplementedError("QueueClient.enqueue needs jobd (I2/I3)")

    def cancel(self, job_id: str) -> JobView:
        raise NotImplementedError("QueueClient.cancel needs jobd (I2/I3)")

    def set_concurrency(self, n: int) -> int:
        raise NotImplementedError("QueueClient.set_concurrency needs jobd (I2/I3)")
