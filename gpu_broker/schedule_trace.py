"""Privacy-bounded scheduler traces and deterministic history replay.

The broker already stores jobs, transitions, and ledger entries durably. This
module exports only fields needed for offline evaluation. Commands, names,
backend handles, logs, and wall-clock timestamps never leave the database.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import BrokerConfig
from .money import money
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


def anonymous_user_id(user_id: str, salt: str) -> str:
    """Stable within an operator's secret salt, unlinkable across salts."""
    if len(salt.encode()) < 16:
        raise TraceError("GPU_BROKER_TRACE_SALT must contain at least 16 bytes")
    return hmac.new(salt.encode(), user_id.encode(), hashlib.sha256).hexdigest()[:24]


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def build_cost_report(
    store: Store, config: BrokerConfig, *, salt: str
) -> dict[str, Any]:
    """Build an aggregate-only cost report and FIFO replay from one trace.

    The report deliberately contains no job rows, timestamps, commands, handles,
    or pseudonymous IDs. The IDs exist only long enough to count distinct users.
    """
    jobs = [job for job in store.list_jobs(limit=None) if job.origin != "seeded"]
    user_keys = {anonymous_user_id(job.user_id, salt) for job in jobs}
    if len(jobs) < MIN_OPERATIONAL_JOBS or len(user_keys) < MIN_OPERATIONAL_USERS:
        raise TraceError(
            f"insufficient data: the operational trace holds {len(jobs)} job(s) "
            f"from {len(user_keys)} user(s); a cost comparison needs at least "
            f"{MIN_OPERATIONAL_JOBS} jobs from {MIN_OPERATIONAL_USERS} users, "
            "so no cost-comparison verdict is emitted"
        )

    completed = [job for job in jobs if job.state is JobState.COMPLETED]
    started = [job for job in jobs if job.started_at is not None]
    waits = [(job.started_at - job.submitted_at).total_seconds() for job in started]
    settled: dict[str, Decimal] = defaultdict(Decimal)
    completed_cost: dict[str, Decimal] = defaultdict(Decimal)
    retry_cost: dict[str, Decimal] = defaultdict(Decimal)
    estimated_reserved: dict[str, Decimal] = defaultdict(Decimal)
    for job in jobs:
        cost = store.job_spend(job.job_id)
        currency = str(job.currency)
        settled[currency] += cost
        estimated_reserved[currency] += job.reserved
        if job in completed:
            completed_cost[currency] += cost
        if job.attempts > 1:
            retry_cost[currency] += cost * money(job.attempts - 1) / money(job.attempts)

    abandoned: dict[str, Decimal] = defaultdict(Decimal)
    for row in store.conn.execute(
        "SELECT currency, amount FROM ledger WHERE kind = 'ABANDONED'"
    ):
        abandoned[row["currency"]] += money(row["amount"])

    # Replay the exact observed jobs through one FIFO slot per resource class.
    # Durations and completion outcomes are held fixed. This isolates ordering;
    # it is not a counterfactual cloud-capacity or price model. Only jobs that
    # really started are replayed: a refused or cancelled-while-queued job never
    # held capacity, and giving it a zero-wait sample under FIFO would compare
    # the two policies over different job sets.
    available: dict[str, float] = defaultdict(float)
    fifo_waits: list[float] = []
    if started:
        epoch = min(job.submitted_at for job in started)
        for job in sorted(started, key=lambda item: (item.submitted_at, item.job_id)):
            submitted = (job.submitted_at - epoch).total_seconds()
            start = max(submitted, available[job.gpu_type])
            fifo_waits.append(start - submitted)
            duration = (
                max(0.0, (job.finished_at - job.started_at).total_seconds())
                if job.finished_at is not None
                else 0.0
            )
            available[job.gpu_type] = start + duration

    observations = store.conn.execute(
        "SELECT COUNT(*) AS n FROM schedule_observations"
    ).fetchone()["n"]
    interruptions = sum(job.preemptions for job in jobs)
    resumed = sum(
        1 for job in jobs if job.attempts > 1 and job.checkpoint_step is not None
    )
    refused = [job for job in jobs if job.state is JobState.REFUSED]
    budget_refusals = sum(
        1 for job in refused if "budget" in (job.refusal_reason or "").lower()
    )
    by_currency = {}
    for currency in sorted(set(settled) | set(estimated_reserved) | set(abandoned)):
        count = sum(1 for job in completed if str(job.currency) == currency)
        by_currency[currency] = {
            "settled_cost": str(settled[currency]),
            "estimated_reserved_cost": str(estimated_reserved[currency]),
            "cost_per_completed_job": (
                str(completed_cost[currency] / count) if count else None
            ),
            "estimated_retry_cost": str(retry_cost[currency]),
            "abandoned_capacity_cost": str(abandoned[currency]),
        }

    report: dict[str, Any] = {
        "schema": "gpu-broker.cost-study/v1",
        "evidence_boundary": {
            "source": "organic operational rows only; seeded rows excluded",
            "jobs": len(jobs),
            "anonymous_users": len(user_keys),
            "minimum_jobs": MIN_OPERATIONAL_JOBS,
            "minimum_users": MIN_OPERATIONAL_USERS,
        },
        "instrumentation": {
            "scheduler_observations": observations,
            "interruptions": interruptions,
            "checkpoint_resumes": resumed,
            "completed": len(completed),
            "refused": len(refused),
            "budget_refusals": budget_refusals,
        },
        "current_policy": {
            "name": "fair-share plus age, observed",
            "completion_rate": len(completed) / len(jobs),
            "queue_p50_seconds": _percentile(waits, 0.50),
            "queue_p95_seconds": _percentile(waits, 0.95),
            "cost": by_currency,
        },
        "fifo_replay": {
            "name": "FIFO, one observed-duration slot per resource class",
            "same_trace_jobs": len(started),
            "completion_rate_held_constant": len(completed) / len(jobs),
            "queue_p50_seconds": _percentile(fifo_waits, 0.50),
            "queue_p95_seconds": _percentile(fifo_waits, 0.95),
            "cost_held_constant": True,
        },
        "limits": [
            "FIFO changes ordering only; capacity, duration, outcome, and price are held constant",
            "retry cost is allocated in proportion to extra attempts, not provider invoices",
        ],
    }
    report["content_sha256"] = _digest(report)
    return report


def export_cost_report(
    store: Store, config: BrokerConfig, output: Path | str, *, salt: str
) -> dict[str, Any]:
    report = build_cost_report(store, config, salt=salt)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


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
        records.append(
            {
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
            }
        )

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
                "commands",
                "display names",
                "backend handles",
                "logs",
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
        jobs=len(jobs),
        users=len({str(job["user_key"]) for job in jobs}),
        completed=completed,
        max_active=max_active,
        p95_wait_seconds=_p95(waits),
        useful_gpu_hours=sum(useful_by_user.values()),
        jain_fairness=_jain(list(useful_by_user.values())),
        spend={key: str(value) for key, value in sorted(spend.items())},
        digest=actual,
    )
