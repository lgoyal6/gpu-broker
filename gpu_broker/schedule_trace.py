"""Privacy-bounded scheduler traces and deterministic history replay.

The broker already stores jobs, transitions, and ledger entries durably. This
module exports only fields needed for offline evaluation. Commands, names,
backend handles, logs, and wall-clock timestamps never leave the database.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .states import JobState
from .store import Store

SCHEMA = "gpu-broker.schedule-trace/v1"
MIN_OPERATIONAL_JOBS = 20
MIN_OPERATIONAL_USERS = 5


class TraceError(ValueError):
    pass


@dataclass(frozen=True)
class ReplaySummary:
    jobs: int
    users: int
    completed: int
    max_active: int
    p95_wait_seconds: float | None
    useful_gpu_hours: float
    jain_fairness: float | None
    spend: dict[str, str]
    digest: str


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(bundle: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in bundle.items() if key != "content_sha256"}
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _seconds(moment, epoch) -> float | None:
    return None if moment is None else round((moment - epoch).total_seconds(), 6)


def export_trace(store: Store, output: Path | str) -> dict[str, Any]:
    jobs = store.list_jobs(limit=None)
    if not jobs:
        raise TraceError("no jobs are available to export")
    operational = [job for job in jobs if job.origin != "seeded"]
    operational_users = {job.user_id for job in operational}
    if operational and (
        len(operational) < MIN_OPERATIONAL_JOBS
        or len(operational_users) < MIN_OPERATIONAL_USERS
    ):
        raise TraceError(
            "operational history is too small to anonymize safely: "
            f"{len(operational)} job(s), {len(operational_users)} user(s); need "
            f"at least {MIN_OPERATIONAL_JOBS} jobs and {MIN_OPERATIONAL_USERS} users"
        )

    ordered = sorted(jobs, key=lambda job: (job.submitted_at, job.job_id))
    epoch = ordered[0].submitted_at
    user_keys = {
        user_id: f"user-{index:03d}"
        for index, user_id in enumerate(sorted({job.user_id for job in ordered}), 1)
    }
    records = []
    for index, job in enumerate(ordered, 1):
        records.append({
            "job_key": f"job-{index:05d}",
            "user_key": user_keys[job.user_id],
            "gpu_type": job.gpu_type,
            "currency": str(job.currency),
            "reserved": str(job.reserved),
            "spent": str(store.job_spend(job.job_id)),
            "requested_hours": job.requested_hours,
            "state": str(job.state),
            "origin": job.origin,
            "submitted_offset_seconds": _seconds(job.submitted_at, epoch),
            "started_offset_seconds": _seconds(job.started_at, epoch),
            "finished_offset_seconds": _seconds(job.finished_at, epoch),
            "attempts": job.attempts,
            "preemptions": job.preemptions,
        })

    bundle: dict[str, Any] = {
        "schema": SCHEMA,
        "scheduler": {
            "name": "gpu-broker fair-share plus age",
            "implementation": "gpu_broker.fairshare.order_queue",
            "pool_policy": "strict head under pool cap, capacity misses may skip",
        },
        "privacy": {
            "identifiers": "deterministic aliases scoped to this export",
            "excluded": [
                "commands", "display names", "backend handles", "logs",
                "wall-clock timestamps",
            ],
            "minimum_operational_jobs": MIN_OPERATIONAL_JOBS,
            "minimum_operational_users": MIN_OPERATIONAL_USERS,
        },
        "evaluation_limits": {
            "deadlines": "unavailable",
            "capacity_snapshots": "unavailable",
            "optimizer_comparison": (
                "blocked until both fields and enough real history exist"
            ),
        },
        "jobs": records,
    }
    bundle["content_sha256"] = _digest(bundle)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n")
    return bundle


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def _jain(values: list[float]) -> float | None:
    if not values or sum(values) == 0:
        return None
    return (sum(values) ** 2) / (len(values) * sum(value * value for value in values))


def replay_trace(path: Path | str) -> ReplaySummary:
    bundle = json.loads(Path(path).read_text())
    if bundle.get("schema") != SCHEMA:
        raise TraceError(f"unsupported trace schema: {bundle.get('schema')!r}")
    expected, actual = bundle.get("content_sha256"), _digest(bundle)
    if expected != actual:
        raise TraceError("trace digest does not match its contents")
    jobs = bundle.get("jobs")
    if not isinstance(jobs, list):
        raise TraceError("trace jobs must be a list")

    events: list[tuple[float, int, str, str]] = []
    waits: list[float] = []
    useful_by_user: dict[str, float] = defaultdict(float)
    spend: dict[str, Decimal] = defaultdict(Decimal)
    completed = 0
    for job in jobs:
        key = str(job["job_key"])
        submitted = float(job["submitted_offset_seconds"])
        start = job.get("started_offset_seconds")
        finish = job.get("finished_offset_seconds")
        events.append((submitted, 0, "submit", key))
        if start is not None:
            start = float(start)
            if start < submitted:
                raise TraceError(f"{key} starts before it was submitted")
            events.append((start, 1, "start", key))
            waits.append(start - submitted)
        if finish is not None:
            finish = float(finish)
            if finish < (start if start is not None else submitted):
                raise TraceError(f"{key} has an invalid finish time")
            action = "finish" if start is not None else "finish_unstarted"
            events.append((finish, 2, action, key))
            if job.get("state") == str(JobState.COMPLETED):
                useful_by_user[str(job["user_key"])] += (finish - start) / 3600.0
                completed += 1
        spend[str(job["currency"])] += Decimal(str(job["spent"]))

    active: set[str] = set()
    submitted_jobs: set[str] = set()
    max_active = 0
    for _, _, action, key in sorted(events):
        if action == "submit":
            if key in submitted_jobs:
                raise TraceError(f"duplicate submit for {key}")
            submitted_jobs.add(key)
        elif action == "start":
            if key not in submitted_jobs or key in active:
                raise TraceError(f"invalid start for {key}")
            active.add(key)
            max_active = max(max_active, len(active))
        elif action == "finish":
            if key not in active:
                raise TraceError(f"finish without active job for {key}")
            active.remove(key)
        elif key not in submitted_jobs:
            raise TraceError(f"terminal state before submit for {key}")

    return ReplaySummary(
        jobs=len(jobs), users=len({str(job["user_key"]) for job in jobs}),
        completed=completed, max_active=max_active, p95_wait_seconds=_p95(waits),
        useful_gpu_hours=sum(useful_by_user.values()),
        jain_fairness=_jain(list(useful_by_user.values())),
        spend={key: str(value) for key, value in sorted(spend.items())}, digest=actual,
    )
