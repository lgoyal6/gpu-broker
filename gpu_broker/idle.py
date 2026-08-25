"""Finding GPUs that are held but not used, and saying so before killing anything.

This is the failure the club actually has. Somebody launches an instance, the
training script dies at 2am, and the machine bills until Thursday. Nobody is
being careless; nobody is looking.

Three rules shape everything here, and two of them are restrictions:

*Nothing is reclaimed without a recorded notification.* The check is a lookup in
the notifications table, not a promise in a comment. If the row is not there, the
reclaim does not happen.

*The samples that justified a reclaim are kept, including after the job is gone.*
Somebody whose run was killed gets to see the evidence. Deleting the samples with
the job would make the decision unauditable exactly when it matters.

*Reclaim reports before it acts.* Off by default. `gpu reclaim` lists what it
would have killed and why, so the club can watch it be right for a few weeks
before it is allowed to be wrong.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal

from .clock import Clock
from .config import BrokerConfig
from .models import Job, Sample
from .money import ZERO, Currency, fmt, money, quantize

IDLE_NOTIFICATION = "idle-warning"


@dataclass(frozen=True)
class IdleVerdict:
    """What the samples say about one running job."""

    job: Job
    idle: bool
    reason: str
    samples: tuple[Sample, ...]
    idle_since: dt.datetime | None = None
    peak_percent: float = 0.0
    notified_at: dt.datetime | None = None
    grace_ends_at: dt.datetime | None = None
    wasted: Decimal = ZERO
    currency: Currency = Currency.USD

    @property
    def notified(self) -> bool:
        return self.notified_at is not None

    def idle_hours(self, now: dt.datetime) -> float:
        if self.idle_since is None:
            return 0.0
        return max(0.0, (now - self.idle_since).total_seconds() / 3600.0)

    def evidence(self, limit: int = 8) -> str:
        """The samples, for the person whose job this is."""
        shown = self.samples[-limit:]
        return "\n".join(
            f"  {sample.at:%Y-%m-%d %H:%M:%S}  gpu {sample.gpu_percent:5.1f}%  "
            f"{sample.memory_mb}MB"
            for sample in shown
        )

    def describe(self, now: dt.datetime) -> str:
        return (
            f"{self.job.short_id} ({self.job.user_id}, {self.job.gpu_type}) "
            f"idle {self.idle_hours(now):.1f}h, peak {self.peak_percent:.1f}%, "
            f"{fmt(self.wasted, self.currency)} spent doing nothing"
        )


def _last_notified(store, job: Job) -> dt.datetime | None:
    """When we last warned this job's owner that it looked idle.

    Read from the notifications table on every evaluation rather than cached.
    The rule downstream is that nothing is reclaimed without a recorded
    notification, and a cached answer would make that rule true in memory and
    unverifiable on disk.
    """
    warnings = store.notifications_for_job(job.job_id, kind=IDLE_NOTIFICATION)
    return warnings[-1].at if warnings else None


def evaluate(store, job: Job, config: BrokerConfig, clock: Clock) -> IdleVerdict:
    """Decide whether one running job is idle, from its samples alone."""
    now = clock.now()
    window = dt.timedelta(minutes=config.idle_window_minutes)
    samples = tuple(store.samples_for(job.job_id, since=now - window))

    notified = _last_notified(store, job)
    grace_ends = (
        notified + dt.timedelta(minutes=config.idle_grace_minutes) if notified else None
    )

    if len(samples) < config.idle_min_samples:
        return IdleVerdict(
            job=job,
            idle=False,
            reason=(
                f"only {len(samples)} sample(s) in the last "
                f"{config.idle_window_minutes:g} minutes; needs "
                f"{config.idle_min_samples} before anything is decided"
            ),
            samples=samples,
            notified_at=notified,
            grace_ends_at=grace_ends,
        )

    peak = max(sample.gpu_percent for sample in samples)
    if peak > config.idle_threshold_percent:
        return IdleVerdict(
            job=job,
            idle=False,
            reason=(
                f"peaked at {peak:.1f}% in the last "
                f"{config.idle_window_minutes:g} minutes"
            ),
            samples=samples,
            peak_percent=peak,
            notified_at=notified,
            grace_ends_at=grace_ends,
        )

    idle_since = _idle_since(store, job, config, samples)
    idle_hours = max(0.0, (now - idle_since).total_seconds() / 3600.0)
    price = config.gpu(job.gpu_type)
    wasted = quantize(money(price.hourly_price) * money(idle_hours), price.currency)

    return IdleVerdict(
        job=job,
        idle=True,
        reason=(
            f"never went above {config.idle_threshold_percent:g}% across "
            f"{len(samples)} samples over {config.idle_window_minutes:g} minutes"
        ),
        samples=samples,
        idle_since=idle_since,
        peak_percent=peak,
        notified_at=notified,
        grace_ends_at=grace_ends,
        wasted=wasted,
        currency=price.currency,
    )


def _idle_since(store, job: Job, config: BrokerConfig, window_samples) -> dt.datetime:
    """When this job actually stopped working, not when we started looking.

    The detection window is ten minutes; the idleness can be six hours old. Using
    the window's first sample would report "idle 0.2h, $0.15 wasted" for a job
    that has burned an afternoon, which is exactly the number somebody needs to
    see to care.

    Walks back through the job's whole sample history to the start of the current
    unbroken run below the threshold.
    """
    history = store.samples_for(job.job_id, limit=20_000)
    if not history:
        return window_samples[0].at

    since = history[-1].at
    for sample in reversed(history):
        if sample.gpu_percent > config.idle_threshold_percent:
            break
        since = sample.at
    return since


def notify(store, verdict: IdleVerdict, config: BrokerConfig) -> IdleVerdict:
    """Tell the owner, once, and start the grace period.

    Returns an updated verdict so the caller does not have to re-read. Nothing
    downstream may act on an idle job whose verdict has no `notified_at`.
    """
    if verdict.notified or not verdict.idle:
        return verdict

    job = verdict.job
    message = (
        f"Job {job.short_id} ({job.command[:60]}) has used no GPU for "
        f"{config.idle_window_minutes:g} minutes -- it never went above "
        f"{verdict.peak_percent:.1f}%. It has spent "
        f"{fmt(verdict.wasted, verdict.currency)} doing nothing. "
        f"If it is still working, ignore this. Otherwise it will be listed for "
        f"reclaim in {config.idle_grace_minutes:g} minutes. "
        f"Run `gpu status {job.short_id}` to see the samples."
    )
    record = store.notify(
        user_id=job.user_id, kind=IDLE_NOTIFICATION, message=message, job_id=job.job_id
    )
    store.append_log(job.job_id, "broker", f"idle warning sent to {job.user_id}")

    from dataclasses import replace

    return replace(
        verdict,
        notified_at=record.at,
        grace_ends_at=record.at + dt.timedelta(minutes=config.idle_grace_minutes),
    )


def reclaimable(verdict: IdleVerdict, clock: Clock) -> tuple[bool, str]:
    """May this job be reclaimed right now, and if not, why not.

    Every condition is checked against stored state rather than inferred, so
    "we told them" cannot be true in memory and false on disk.
    """
    if not verdict.idle:
        return False, "not idle"
    if not verdict.notified:
        return False, "nobody has been told yet"
    if verdict.grace_ends_at is None:
        return False, "no grace period recorded"
    now = clock.now()
    if now < verdict.grace_ends_at:
        remaining = (verdict.grace_ends_at - now).total_seconds() / 60.0
        return False, f"{remaining:.0f} minutes of grace left"
    return True, "idle, notified, and out of grace"


def candidates(store, config: BrokerConfig, clock: Clock) -> list[IdleVerdict]:
    """Every running job that is idle, in the order worth acting on."""
    from .states import ACTIVE

    verdicts = [
        evaluate(store, job, config, clock)
        for job in store.list_jobs(states=ACTIVE)
    ]
    idle = [verdict for verdict in verdicts if verdict.idle]
    idle.sort(key=lambda verdict: -verdict.wasted)
    return idle
