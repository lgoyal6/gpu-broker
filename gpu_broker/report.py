"""The measurement report, generated rather than hand-written.

A report somebody types up is a report that says what they remember. This one is
computed from the ledger, the job history, and the utilization samples, which
means it can be wrong but it cannot be flattering.

It has a section for where the broker loses. That is not modesty: a club member
deciding whether to use this deserves to know that fair share can make them wait
longer than launching an instance themselves would have, and the only way that
number ever gets looked at is if it is printed next to the good ones.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from decimal import Decimal

from .config import BrokerConfig
from .money import ZERO, Currency, fmt, money, quantize
from .states import JobState

SELF_LAUNCH_MINUTES = 3.0
"""How long it takes somebody to launch their own instance from the console.
The bar the queue has to beat, and the number the wait-time section is measured
against. A guess, and labelled as one wherever it is used."""


@dataclass(frozen=True)
class Adoption:
    """The numbers that cannot be backfilled, which is why they were recorded
    from the first deploy rather than reconstructed later."""

    distinct_users: int = 0
    repeat_users: int = 0
    jobs_submitted: int = 0
    jobs_completed: int = 0
    jobs_refused: int = 0
    dollars_spent: Decimal = ZERO
    gpu_hours_spent: Decimal = ZERO
    dollars_reclaimed: Decimal = ZERO

    @property
    def repeat_rate(self) -> float | None:
        """The real signal. Anyone can be talked into trying something once."""
        if not self.distinct_users:
            return None
        return self.repeat_users / self.distinct_users

    @property
    def completion_rate(self) -> float | None:
        if not self.jobs_submitted:
            return None
        return self.jobs_completed / self.jobs_submitted


@dataclass(frozen=True)
class Efficiency:
    gpu_hours_paid: float = 0.0
    """Hours the club was billed for, including boot and idle."""
    gpu_hours_useful: float = 0.0
    """Hours a GPU was above the idle threshold. The denominator that matters."""
    dollars: Decimal = ZERO

    @property
    def dollars_per_useful_hour(self) -> Decimal | None:
        if self.gpu_hours_useful <= 0:
            return None
        return quantize(self.dollars / money(self.gpu_hours_useful), Currency.USD)

    @property
    def dollars_per_paid_hour(self) -> Decimal | None:
        if self.gpu_hours_paid <= 0:
            return None
        return quantize(self.dollars / money(self.gpu_hours_paid), Currency.USD)

    @property
    def useful_fraction(self) -> float | None:
        if self.gpu_hours_paid <= 0:
            return None
        return self.gpu_hours_useful / self.gpu_hours_paid


@dataclass(frozen=True)
class WorkLost:
    """The number the build prompt says must be zero.

    Split three ways on purpose. Work *lost* is a job that was interrupted and
    never finished -- somebody's afternoon, gone. Work *redone* is time paid for
    twice because a job had no checkpoint; wasteful, but the result survived.
    Reclaimed hours were, by construction, hours in which the GPU was doing
    nothing, and the samples that prove it are still on disk.
    """

    jobs_lost: tuple[str, ...] = ()
    hours_redone: float = 0.0
    jobs_redone: int = 0
    reclaimed_hours: float = 0.0
    reclaimed_useful_hours: float = 0.0
    jobs_reclaimed: int = 0

    @property
    def zero(self) -> bool:
        return not self.jobs_lost and self.reclaimed_useful_hours == 0.0


@dataclass(frozen=True)
class WhereItLoses:
    startup_hours_mean: float = 0.0
    startup_dollars: Decimal = ZERO
    waits_longer_than_self_launch: int = 0
    jobs_measured: int = 0
    worst_wait_hours: float = 0.0
    worst_wait_user: str = ""
    heavy_user_penalty_hours: float = 0.0
    heavy_user: str = ""
    unmeasured: tuple[str, ...] = ()

    @property
    def wait_penalty_rate(self) -> float | None:
        if not self.jobs_measured:
            return None
        return self.waits_longer_than_self_launch / self.jobs_measured


@dataclass(frozen=True)
class Baseline:
    period: str
    instance_hours: float
    dollars: Decimal
    source: str


@dataclass(frozen=True)
class Report:
    generated_at: dt.datetime
    since: dt.datetime
    adoption: Adoption
    efficiency: Efficiency
    lost: WorkLost
    loses: WhereItLoses
    baseline: tuple[Baseline, ...] = ()
    notes: tuple[str, ...] = field(default_factory=tuple)


# --- the computation --------------------------------------------------------


def build(store, config: BrokerConfig, clock, since: dt.datetime | None = None) -> Report:
    now = clock.now()
    since = since or (now - dt.timedelta(days=90))
    jobs = [job for job in store.list_jobs() if job.submitted_at >= since]

    return Report(
        generated_at=now,
        since=since,
        adoption=_adoption(store, jobs),
        efficiency=_efficiency(store, config, jobs, now),
        lost=_lost(store, config, jobs, now),
        loses=_loses(store, config, jobs, now),
        baseline=tuple(_baselines(store)),
        notes=(
            "Utilization is sampled, not continuous, so useful hours are the "
            f"sampled fraction above {config.idle_threshold_percent:g}% multiplied "
            "by billed time.",
            f"Self-launch is assumed to take {SELF_LAUNCH_MINUTES:g} minutes. That is "
            "a guess, not a measurement.",
        ),
    )


def _adoption(store, jobs) -> Adoption:
    per_user: dict[str, int] = {}
    for job in jobs:
        if job.state is not JobState.REFUSED:
            per_user[job.user_id] = per_user.get(job.user_id, 0) + 1

    reclaimed = ZERO
    for job in jobs:
        if job.state is JobState.RECLAIMED and job.currency is Currency.USD:
            # What the club got back: the part of the reservation the job never
            # got to spend.
            reclaimed += max(ZERO, job.reserved - store.job_spend(job.job_id))

    return Adoption(
        distinct_users=len(per_user),
        repeat_users=sum(1 for count in per_user.values() if count > 1),
        jobs_submitted=sum(1 for job in jobs if job.state is not JobState.REFUSED),
        jobs_completed=sum(1 for job in jobs if job.state is JobState.COMPLETED),
        jobs_refused=sum(1 for job in jobs if job.state is JobState.REFUSED),
        dollars_spent=sum(
            (store.job_spend(job.job_id) for job in jobs if job.currency is Currency.USD), ZERO
        ),
        gpu_hours_spent=sum(
            (store.job_spend(job.job_id) for job in jobs if job.currency is Currency.GPU_HOUR), ZERO
        ),
        dollars_reclaimed=quantize(reclaimed, Currency.USD),
    )


def _useful_fraction(store, config: BrokerConfig, job_id: str) -> float | None:
    samples = store.samples_for(job_id, limit=10_000)
    if not samples:
        return None
    busy = sum(1 for s in samples if s.gpu_percent > config.idle_threshold_percent)
    return busy / len(samples)


def _efficiency(store, config: BrokerConfig, jobs, now) -> Efficiency:
    paid = 0.0
    useful = 0.0
    dollars = ZERO
    for job in jobs:
        if job.currency is not Currency.USD or job.started_at is None:
            continue
        hours = job.elapsed_hours(now)
        paid += hours
        dollars += store.job_spend(job.job_id)
        fraction = _useful_fraction(store, config, job.job_id)
        if fraction is not None:
            useful += hours * fraction
    return Efficiency(gpu_hours_paid=paid, gpu_hours_useful=useful, dollars=dollars)


def _lost(store, config: BrokerConfig, jobs, now) -> WorkLost:
    lost: list[str] = []
    redone = 0.0
    redone_jobs = 0
    reclaimed_hours = 0.0
    reclaimed_useful = 0.0
    reclaimed_count = 0

    for job in jobs:
        if job.state is JobState.RECLAIMED:
            reclaimed_count += 1
            hours = job.elapsed_hours(now)
            reclaimed_hours += hours
            fraction = _useful_fraction(store, config, job.job_id) or 0.0
            reclaimed_useful += hours * fraction
            continue

        if job.preemptions and job.state is not JobState.COMPLETED and job.is_terminal:
            # Interrupted and never finished. Somebody's afternoon.
            lost.append(job.job_id)

        if job.preemptions and job.checkpoint_step is None and job.state is JobState.COMPLETED:
            # It finished, but every preemption threw away everything it had.
            redone_jobs += 1
            redone += job.elapsed_hours(now)

    return WorkLost(
        jobs_lost=tuple(lost),
        hours_redone=redone,
        jobs_redone=redone_jobs,
        reclaimed_hours=reclaimed_hours,
        reclaimed_useful_hours=reclaimed_useful,
        jobs_reclaimed=reclaimed_count,
    )


def _loses(store, config: BrokerConfig, jobs, now) -> WhereItLoses:
    startups: list[float] = []
    startup_cost = ZERO
    waited_longer = 0
    measured = 0
    worst = 0.0
    worst_user = ""
    per_user_wait: dict[str, float] = {}

    threshold = SELF_LAUNCH_MINUTES / 60.0

    for job in jobs:
        if job.state is JobState.REFUSED:
            continue

        wait = job.wait_hours(now)
        measured += 1
        if wait > threshold:
            waited_longer += 1
        if wait > worst:
            worst, worst_user = wait, job.user_id
        per_user_wait[job.user_id] = per_user_wait.get(job.user_id, 0.0) + wait

        if job.started_at is None:
            continue
        # Boot time: from taking capacity to the first line of the user's own
        # output. Billed, and doing nothing for them.
        first_run = next(
            (at for _, to_state, at, _ in store.history(job.job_id)
             if to_state == JobState.RUNNING),
            None,
        )
        if first_run is not None:
            overhead = max(0.0, (first_run - job.started_at).total_seconds() / 3600.0)
            startups.append(overhead)
            if job.currency is Currency.USD:
                startup_cost += money(config.gpu(job.gpu_type).hourly_price) * money(overhead)

    heavy_user, heavy_penalty = "", 0.0
    if per_user_wait:
        heavy_user = max(per_user_wait, key=lambda user: per_user_wait[user])
        heavy_penalty = per_user_wait[heavy_user]

    return WhereItLoses(
        startup_hours_mean=sum(startups) / len(startups) if startups else 0.0,
        startup_dollars=quantize(startup_cost, Currency.USD),
        waits_longer_than_self_launch=waited_longer,
        jobs_measured=measured,
        worst_wait_hours=worst,
        worst_wait_user=worst_user,
        heavy_user_penalty_hours=heavy_penalty,
        heavy_user=heavy_user,
        unmeasured=(
            "MPS overhead against exclusive access on the A6000. Two jobs sharing "
            "one card is slower than one job having it, and by how much is not in "
            "any record here -- it needs the same job run both ways on the real "
            "hardware and timed.",
            "Whether a member would actually have got an instance. Comparing a "
            "queue wait against a self-launch assumes capacity and quota were "
            "there, which on a fresh account they are not.",
        ),
    )


def _baselines(store):
    rows = store.conn.execute(
        "SELECT period, instance_hours, dollars, source FROM baseline ORDER BY period"
    ).fetchall()
    return [
        Baseline(row["period"], row["instance_hours"], money(row["dollars"]), row["source"])
        for row in rows
    ]


# --- rendering --------------------------------------------------------------


def markdown(report: Report) -> str:
    """The report as a document somebody can paste into a club channel."""
    a, e, lost, loses = report.adoption, report.efficiency, report.lost, report.loses
    out: list[str] = [
        "# GPU broker: what actually happened",
        "",
        f"Generated {report.generated_at:%Y-%m-%d %H:%M UTC}, "
        f"covering {report.since:%Y-%m-%d} onwards.",
        "",
        "## Headline",
        "",
    ]

    if e.dollars_per_useful_hour is not None:
        out += [
            f"**{fmt(e.dollars_per_useful_hour, Currency.USD)} per useful GPU-hour.**",
            "",
            f"{e.gpu_hours_useful:.1f} of {e.gpu_hours_paid:.1f} billed hours had a GPU "
            f"doing something ({(e.useful_fraction or 0) * 100:.0f}%), for "
            f"{fmt(e.dollars, Currency.USD)}.",
        ]
    else:
        out.append("Not enough billed GPU time yet to divide by.")
    out.append("")

    out += ["## Work lost", ""]
    if lost.zero:
        out.append(
            f"**Zero.** No job was interrupted and left unfinished. "
            f"{lost.jobs_reclaimed} job(s) were reclaimed as idle, accounting for "
            f"{lost.reclaimed_hours:.1f} hours in which a GPU did "
            f"{lost.reclaimed_useful_hours:.2f} hours of work."
        )
    else:
        out.append(
            f"**{len(lost.jobs_lost)} job(s) lost their work.** "
            + ", ".join(job_id[:8] for job_id in lost.jobs_lost[:10])
        )
    if lost.jobs_redone:
        out += [
            "",
            f"{lost.jobs_redone} job(s) finished but were preempted without a "
            f"checkpoint, so {lost.hours_redone:.1f} hours were paid for twice. "
            "Nothing was lost; money was.",
        ]
    out.append("")

    out += [
        "## Adoption",
        "",
        f"- {a.distinct_users} distinct user(s)",
        f"- {a.repeat_users} came back "
        + (f"({(a.repeat_rate or 0) * 100:.0f}%)" if a.repeat_rate is not None else ""),
        f"- {a.jobs_submitted} job(s) submitted, {a.jobs_completed} completed"
        + (f" ({(a.completion_rate or 0) * 100:.0f}%)" if a.completion_rate is not None else ""),
        f"- {a.jobs_refused} refused for budget",
        f"- {fmt(a.dollars_spent, Currency.USD)} spent, "
        f"{fmt(a.gpu_hours_spent, Currency.GPU_HOUR)} of free capacity used",
        f"- {fmt(a.dollars_reclaimed, Currency.USD)} returned by reclaiming idle jobs",
        "",
        "Repeat use is the real signal. Anyone can be talked into trying something once.",
        "",
    ]

    if report.baseline:
        out += ["## Before the broker", "", "| month | instance-hours | spend | source |", "|---|---|---|---|"]
        for entry in report.baseline:
            out.append(
                f"| {entry.period} | {entry.instance_hours:.0f} | "
                f"{fmt(entry.dollars, Currency.USD)} | {entry.source} |"
            )
        out.append("")
    else:
        out += [
            "## Before the broker",
            "",
            "No baseline recorded. `gpu report --baseline` pulls it from Cost Explorer, "
            "or put the numbers in by hand if the IAM user cannot read it.",
            "",
        ]

    out += ["## Where it loses", ""]
    if loses.startup_hours_mean:
        out.append(
            f"- **Startup overhead.** {loses.startup_hours_mean * 60:.1f} minutes per job "
            f"between taking a machine and the job's first output, "
            f"{fmt(loses.startup_dollars, Currency.USD)} in total. Launching it yourself "
            "has the same boot, but the broker adds its poll interval on top."
        )
    if loses.wait_penalty_rate is not None:
        out.append(
            f"- **Queue waits.** {loses.waits_longer_than_self_launch} of "
            f"{loses.jobs_measured} jobs waited longer than the "
            f"{SELF_LAUNCH_MINUTES:g} minutes it takes to launch an instance yourself "
            f"({(loses.wait_penalty_rate or 0) * 100:.0f}%). Worst was "
            f"{loses.worst_wait_hours:.1f}h, for {loses.worst_wait_user or 'nobody'}."
        )
    if loses.heavy_user:
        out.append(
            f"- **The heaviest user pays for fair share.** {loses.heavy_user} has waited "
            f"{loses.heavy_user_penalty_hours:.1f} hours in total. If that is you and "
            "you are the only person using the pool, this broker is costing you time."
        )
    out.append("")
    out += ["### Not measured here", ""]
    out += [f"- {note}" for note in loses.unmeasured]
    out += ["", "### Caveats", ""]
    out += [f"- {note}" for note in report.notes]
    out.append("")
    return "\n".join(out)
