"""The one page with no sign-in.

The club app sits behind GitHub OAuth, which is right for a thing that spends
money and wrong for showing somebody the pool is real. This module is the
compromise: a narrow set of aggregates anybody can read.

Everything here is built by *subtraction from nothing* rather than by hiding
fields on a richer object. The dashboard's `Job` carries a command line, a
hostname, a backend handle and a username; if this page rendered those objects
and relied on the template not to print them, it would leak the first time
somebody added a column to the table. So a `PublicJob` is constructed field by
field, and the fields that would identify a person or their work are never
copied into one.

Never crosses this boundary: usernames, job commands, job ids, hostnames,
instance ids, log lines, notification text.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from decimal import Decimal

from ..money import ZERO, Currency
from ..states import ACTIVE, JobState

WEEK = dt.timedelta(days=7)
MONTH = dt.timedelta(days=30)


@dataclasses.dataclass(frozen=True)
class PublicJob:
    """One row of recent activity, with the identifying half left out."""

    member: str
    gpu_type: str
    state: str
    where: str
    hours: float
    utilization: float | None


@dataclasses.dataclass(frozen=True)
class Outcomes:
    """Job counts over one window, split the way a reader cares about."""

    completed: int = 0
    preempted: int = 0
    """Still waiting to resume right now."""
    interrupted: int = 0
    """Lost their machine at least once and carried on anyway. This is the
    number worth reading: `preempted` is only ever a snapshot of the jobs that
    happen to be mid-restart when the page is rendered."""
    reclaimed: int = 0
    failed: int = 0

    @property
    def total(self) -> int:
        return self.completed + self.preempted + self.reclaimed + self.failed


@dataclasses.dataclass(frozen=True)
class BackendUse:
    """How much of the work went to one kind of capacity."""

    name: str
    kind: str
    """'cloud' or 'local'. What the reader is actually asking is 'did this cost
    money', so the split is by that and not by hostname."""
    jobs: int
    hours: float
    share: float


@dataclasses.dataclass(frozen=True)
class PublicStatus:
    generated_at: dt.datetime
    demo: bool
    """Every job in this database is seeded. The page says so, loudly and
    without a way to turn it off.

    A status page exists to be shown to people who were not there. Seeded
    numbers presented as a real club would be a claim about twenty people who
    do not exist, so this is not a display preference.
    """
    pilot: bool
    """True while the pool has effectively one user. Said out loud on the page,
    because a queue of one is not evidence of a queue."""

    members_all_time: int
    members_this_week: int
    repeat_members: int

    week: Outcomes
    month: Outcomes
    jobs_all_time: int

    queue_depth: int
    running_now: int
    running: tuple[PublicJob, ...]

    dollars_spent: Decimal
    dollars_reclaimed: Decimal
    gpu_hours_used: Decimal
    runway_days: float | None
    burn_per_day: Decimal
    pool_remaining: Decimal
    runway_confident: bool

    utilization_7d: list[float | None]
    utilization_30d: list[float | None]
    queue_7d: list[float | None]
    utilization_mean_7d: float | None
    utilization_peak_7d: float | None
    utilization_mean_30d: float | None

    backends: tuple[BackendUse, ...]
    recent: tuple[PublicJob, ...]

    @property
    def quiet(self) -> bool:
        return self.jobs_all_time == 0


def _labels(store) -> dict[str, str]:
    """A stable letter per member, by when they first used the broker.

    An ordinal rather than a hash of the username: a hash is reversible by
    anybody holding the club roster, because the input space is twenty names.
    """
    users = sorted(store.list_users(), key=lambda u: (u.created_at, u.user_id))
    out = {}
    for index, user in enumerate(users):
        suffix = ""
        n = index
        while True:
            suffix = chr(ord("a") + n % 26) + suffix
            n = n // 26 - 1
            if n < 0:
                break
        out[user.user_id] = f"user-{suffix}"
    return out


def _kind(backend: str | None, currency: Currency) -> str:
    """Cloud capacity costs money; local and free-tier capacity does not."""
    if currency == Currency.GPU_HOUR:
        return "local"
    if backend and backend.startswith("local"):
        return "local"
    return "cloud"


def _ran_for(job, now: dt.datetime) -> float:
    if not job.started_at:
        return 0.0
    end = job.finished_at or now
    return max(0.0, (end - job.started_at).total_seconds() / 3600.0)


def _outcomes(jobs, since: dt.datetime) -> Outcomes:
    counts = {"completed": 0, "preempted": 0, "interrupted": 0, "reclaimed": 0, "failed": 0}
    for job in jobs:
        if job.submitted_at < since:
            continue
        if job.state == JobState.COMPLETED:
            counts["completed"] += 1
        elif job.state == JobState.RECLAIMED:
            counts["reclaimed"] += 1
        elif job.state == JobState.FAILED:
            counts["failed"] += 1
        # A job preempted and then finished is a completed job that was
        # interrupted, not a lost one. Counting it in both columns would make
        # the outcome columns sum to more than the jobs run, so preemption is
        # counted only where it is still the job's current state.
        if job.state == JobState.PREEMPTED:
            counts["preempted"] += 1
        if job.preemptions > 0:
            counts["interrupted"] += 1
    return Outcomes(**counts)


def build(broker, recent: int = 6, buckets: int = 56) -> PublicStatus:
    """Assemble the public view. Reads only; nothing here writes."""
    store = broker.store
    now = broker.clock.now()
    labels = _labels(store)
    jobs = store.list_jobs(limit=None)
    real = [job for job in jobs if job.state != JobState.REFUSED]

    per_member: dict[str, int] = {}
    for job in real:
        per_member[job.user_id] = per_member.get(job.user_id, 0) + 1

    week_ago, month_ago = now - WEEK, now - MONTH
    active_this_week = {job.user_id for job in real if job.submitted_at >= week_ago}

    # --- money ---
    spent = ZERO
    gpu_hours = ZERO
    reclaimed = ZERO
    for job in real:
        actual = store.job_spend(job.job_id)
        if job.currency == Currency.USD:
            spent += actual
        else:
            gpu_hours += actual
        if job.state == JobState.RECLAIMED and job.currency == Currency.USD:
            # What idle detection handed back: the part of the hold the job did
            # not get to spend. It assumes the job would otherwise have sat on
            # its full reservation, which is what it was doing when it was
            # caught, but it is an assumption and the page says so.
            reclaimed += max(ZERO, job.reserved - actual)

    forecast = broker.forecast(Currency.USD)

    # --- capacity split ---
    by_backend: dict[tuple[str, str], list[float]] = {}
    for job in real:
        if not job.backend:
            continue
        key = (job.backend, _kind(job.backend, job.currency))
        by_backend.setdefault(key, []).append(_ran_for(job, now))
    total_hours = sum(sum(v) for v in by_backend.values()) or 1.0
    backends = tuple(
        sorted(
            (
                BackendUse(
                    name=name,
                    kind=kind,
                    jobs=len(hours),
                    hours=round(sum(hours), 2),
                    share=round(100.0 * sum(hours) / total_hours, 1),
                )
                for (name, kind), hours in by_backend.items()
            ),
            key=lambda b: -b.hours,
        )
    )

    # --- series ---
    from .. import metrics as m

    series = m.Metrics(store)

    def util(days: int) -> list[float | None]:
        return series.bucketed(
            m.UTILIZATION, now - dt.timedelta(days=days), now, buckets
        )

    util_7 = util(7)
    util_30 = util(30)
    seen = [v for v in util_7 if v is not None]
    seen_30 = [v for v in util_30 if v is not None]

    def public_job(job) -> PublicJob:
        samples = store.samples_for(job.job_id, limit=200)
        peak = max((s.gpu_percent for s in samples), default=None)
        return PublicJob(
            member=labels.get(job.user_id, "user-?"),
            gpu_type=job.gpu_type,
            state=str(job.state),
            where=_kind(job.backend, job.currency),
            hours=round(_ran_for(job, now), 2),
            utilization=peak,
        )

    running = [job for job in real if job.state in ACTIVE]
    finished = [job for job in real if job.finished_at]
    finished.sort(key=lambda j: j.finished_at, reverse=True)

    origins = {job.origin for job in real}
    demo = bool(real) and origins <= {"seeded"}

    return PublicStatus(
        generated_at=now,
        demo=demo,
        pilot=not demo and (origins <= {"pilot"} or len(per_member) <= 1) and bool(real),
        members_all_time=len(per_member),
        members_this_week=len(active_this_week),
        repeat_members=sum(1 for count in per_member.values() if count > 1),
        week=_outcomes(real, week_ago),
        month=_outcomes(real, month_ago),
        jobs_all_time=len(real),
        queue_depth=sum(1 for job in real if job.state == JobState.QUEUED),
        running_now=len(running),
        running=tuple(public_job(job) for job in running[:recent]),
        dollars_spent=spent,
        dollars_reclaimed=reclaimed,
        gpu_hours_used=gpu_hours,
        runway_days=forecast.days_left,
        burn_per_day=forecast.burn_per_day,
        pool_remaining=forecast.available,
        runway_confident=forecast.confident,
        utilization_7d=util_7,
        utilization_30d=util_30,
        queue_7d=series.bucketed(m.QUEUE_DEPTH, now - WEEK, now, buckets),
        utilization_mean_7d=(sum(seen) / len(seen)) if seen else None,
        utilization_peak_7d=max(seen) if seen else None,
        utilization_mean_30d=(sum(seen_30) / len(seen_30)) if seen_30 else None,
        backends=backends,
        recent=tuple(public_job(job) for job in finished[:recent]),
    )
