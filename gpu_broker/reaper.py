"""What is running that nobody is accounting for.

`reconcile` compares records against reality in both directions and is what runs
at startup. `reap` asks the narrower, more expensive question: which machines are
alive right now with no live job behind them, who launched them, and how much have
they burned since.

It reports. It does not terminate anything, and there is no flag that makes it.
An automatic cleanup that is wrong once deletes somebody's training run, and the
only way to know it is right is to watch it be right for a few weeks first.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal

from .backends.base import Backend
from .clock import Clock
from .config import BrokerConfig
from .errors import BrokerError, UnknownJob
from .money import ZERO, Currency, fmt, money, quantize
from .states import TERMINAL
from .store import Store


@dataclass(frozen=True)
class Orphan:
    """One machine that is alive with nothing legitimate behind it."""

    backend: str
    handle: str
    gpu_type: str
    user_id: str | None
    job_id: str | None
    launched_at: dt.datetime | None
    age_hours: float
    burned: Decimal
    currency: Currency
    reason: str

    @property
    def attributable(self) -> bool:
        return bool(self.user_id)

    @property
    def cost_known(self) -> bool:
        """Whether `burned` is a measurement or a placeholder.

        Without a launch timestamp there is no way to say how long this has been
        up, so `burned` falls out as $0.00. That number must never reach the
        ledger: a row saying a running machine cost nothing is a stronger claim
        than saying nothing at all, and it is false. The machine is reported,
        loudly, with its cost named as unknown.
        """
        return self.launched_at is not None

    def describe(self) -> str:
        who = self.user_id or "nobody (untagged)"
        age = f"{self.age_hours:.1f}h" if self.launched_at else "unknown age"
        burned = (
            f"{fmt(self.burned, self.currency)} burned"
            if self.cost_known
            else "cost unknown (no launch time to measure from)"
        )
        return (
            f"{self.backend}/{self.handle} ({self.gpu_type}) launched by {who}, "
            f"{age} ago, {burned}: {self.reason}"
        )


@dataclass(frozen=True)
class ReapReport:
    at: dt.datetime
    orphans: tuple[Orphan, ...]
    scanned: int

    @property
    def total_burned(self) -> dict[Currency, Decimal]:
        totals = {currency: ZERO for currency in Currency}
        for orphan in self.orphans:
            totals[orphan.currency] += orphan.burned
        return totals

    @property
    def by_user(self) -> dict[str, Decimal]:
        """Who to go and ask. Sorted by cost, because that is the order the
        conversations should happen in."""
        totals: dict[str, Decimal] = {}
        for orphan in self.orphans:
            key = orphan.user_id or "<untagged>"
            totals[key] = totals.get(key, ZERO) + orphan.burned
        return dict(sorted(totals.items(), key=lambda pair: -pair[1]))


def reap(
    store: Store,
    backends: list[Backend],
    config: BrokerConfig,
    clock: Clock,
) -> ReapReport:
    now = clock.now()
    orphans: list[Orphan] = []
    scanned = 0

    for backend in backends:
        for resource in backend.list_resources():
            scanned += 1
            reason = _why_orphaned(store, backend, resource)
            if reason is None:
                continue

            launched_at = _launched_at(resource)
            age_hours = (
                max(0.0, (now - launched_at).total_seconds() / 3600.0)
                if launched_at
                else 0.0
            )
            currency, burned = _burn(config, resource.gpu_type, age_hours)
            orphans.append(
                Orphan(
                    backend=backend.name,
                    handle=resource.handle,
                    gpu_type=resource.gpu_type,
                    user_id=resource.user_id,
                    job_id=resource.job_id,
                    launched_at=launched_at,
                    age_hours=age_hours,
                    burned=burned,
                    currency=currency,
                    reason=reason,
                )
            )

    # Most expensive first: that is the one worth acting on today.
    orphans.sort(key=lambda orphan: (-orphan.burned, orphan.handle))
    return ReapReport(at=now, orphans=tuple(orphans), scanned=scanned)


def _why_orphaned(store: Store, backend: Backend, resource) -> str | None:
    """None means this machine is legitimately busy."""
    if resource.job_id is None:
        return (
            "no broker-job-id tag, so the broker cannot say whose this is. "
            "Either it was not launched by the broker, or the broker died "
            "between launching and tagging"
        )

    try:
        job = store.get_job(resource.job_id)
    except UnknownJob:
        return "the broker has no record of this job at all"

    if job.state in TERMINAL:
        return f"the job is {job.state}, but the machine is still up and billing"

    if job.backend_handle != resource.handle:
        # The broker relaunched somewhere else and lost this one.
        held = job.backend_handle or "nothing"
        return (
            f"the job is {job.state} but the broker records it on {held}, "
            f"not this machine. Two machines are running for one job"
        )

    return None


def _launched_at(resource) -> dt.datetime | None:
    from .aws import launched_at_of

    return launched_at_of(resource.tags)


def _burn(config: BrokerConfig, gpu_type: str, age_hours: float) -> tuple[Currency, Decimal]:
    """What this has cost since it was launched.

    Falls back to dollars at zero if the type is unknown, rather than raising:
    an instance type nobody configured is exactly the kind of thing reap should
    still be able to tell you about.
    """
    try:
        gpu = config.gpu(gpu_type)
    except BrokerError:
        return Currency.USD, ZERO
    return gpu.currency, quantize(gpu.hourly_price * money(age_hours), gpu.currency)
