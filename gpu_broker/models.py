"""Plain records. No behaviour that touches the database lives here."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from .money import Currency, fmt
from .states import TERMINAL, JobState


class LedgerKind(StrEnum):
    """What a ledger row means. The ledger is append-only: a mistake is
    corrected by writing a compensating row, never by editing history."""

    RESERVE = "RESERVE"
    """Budget held at admission. The job may cost up to this much."""
    RELEASE = "RELEASE"
    """Part of a hold given back, because the job cost less than it reserved."""
    SETTLE = "SETTLE"
    """Capacity actually consumed. This is the number fair-share reads."""


@dataclass(frozen=True)
class User:
    user_id: str
    display_name: str
    budget_usd: Decimal
    budget_gpu_hours: Decimal
    is_admin: bool
    created_at: dt.datetime

    def budget(self, currency: Currency) -> Decimal:
        return (
            self.budget_usd if currency is Currency.USD else self.budget_gpu_hours
        )


@dataclass(frozen=True)
class Job:
    job_id: str
    user_id: str
    command: str
    gpu_type: str
    requested_hours: float
    currency: Currency
    reserved: Decimal
    """The job's hard ceiling. What `--budget` was set to."""
    state: JobState
    submitted_at: dt.datetime
    updated_at: dt.datetime
    backend: str | None = None
    backend_handle: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    exit_code: int | None = None
    refusal_reason: str | None = None
    logs_fetched: int = 0
    attempts: int = 0
    """How many times this job has been started. More than one means it was
    preempted and came back."""
    preemptions: int = 0
    pinned_tier: str | None = None
    """Set once a job has been preempted repeatedly without saving anything.
    After that it stops being offered spot: paying twice to redo the same hour
    costs more than on-demand would have."""
    checkpoint_step: int | None = None
    checkpoint_key: str | None = None
    checkpoint_at: dt.datetime | None = None
    environment: str | None = None
    """Named environment this job runs in. None means the machine as it comes."""
    cancel_requested: str | None = None

    origin: str = "real"
    """'real', 'pilot', or 'seeded'. See migration 009. This is a label, never a
    filter the accounting relies on: pilot jobs spend real money and are counted
    like any other."""
    """Who asked for this job to stop. Set by the web app, acted on by the
    scheduler daemon, because only the daemon can reach the machines."""
    """How many lines of this job's output the broker has already stored. The
    cursor into the backend's stream, kept apart from the broker's own log
    messages so the two sequences cannot drift."""

    @property
    def resumable(self) -> bool:
        return self.checkpoint_key is not None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL

    @property
    def short_id(self) -> str:
        return self.job_id[:8]

    def wait_hours(self, now: dt.datetime) -> float:
        """How long this job has been waiting for capacity.

        Measured from submission to whichever came first: dispatch, or now.
        """
        end = self.started_at or now
        return max(0.0, (end - self.submitted_at).total_seconds() / 3600.0)

    def elapsed_hours(self, now: dt.datetime) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at or now
        return max(0.0, (end - self.started_at).total_seconds() / 3600.0)


@dataclass(frozen=True)
class LedgerEntry:
    entry_id: int
    job_id: str | None
    user_id: str
    currency: Currency
    kind: LedgerKind
    amount: Decimal
    period: str
    at: dt.datetime
    note: str | None = None


@dataclass(frozen=True)
class Balance:
    """Where one account stands, in one currency, in one month."""

    user_id: str
    currency: Currency
    budget: Decimal
    held: Decimal
    """Reserved by jobs that have not finished."""
    spent: Decimal
    """Actually consumed."""

    @property
    def committed(self) -> Decimal:
        return self.held + self.spent

    @property
    def available(self) -> Decimal:
        return self.budget - self.committed

    def describe(self) -> str:
        return (
            f"{fmt(self.spent, self.currency)} spent, "
            f"{fmt(self.held, self.currency)} held, "
            f"{fmt(self.available, self.currency)} of "
            f"{fmt(self.budget, self.currency)} left"
        )


@dataclass(frozen=True)
class Sample:
    """One look at what a running job is doing to its GPU."""

    at: dt.datetime
    gpu_percent: float
    memory_mb: int = 0
    source: str = ""

    @property
    def busy(self) -> bool:
        return self.gpu_percent > 0


@dataclass(frozen=True)
class Notification:
    notification_id: int
    job_id: str | None
    user_id: str
    kind: str
    message: str
    at: dt.datetime
    seen_at: dt.datetime | None = None


@dataclass(frozen=True)
class Price:
    gpu_type: str
    hourly: Decimal
    currency: "Currency"
    source: str
    """`aws` if it came from the pricing API, `builtin` if it is the fallback
    table. Printed next to the number, because a price nobody refreshed is a
    different kind of number from one AWS gave us this morning."""
    priced_at: dt.datetime
    instance_type: str = ""
    region: str = ""

    def age_days(self, now: dt.datetime) -> float:
        return max(0.0, (now - self.priced_at).total_seconds() / 86400.0)


@dataclass(frozen=True)
class Priority:
    """Why a job is where it is in the queue.

    Every field here is printed by `gpu queue --why`. If somebody asks why they
    are behind, the answer is a row of numbers, not a shrug.
    """

    job_id: str
    user_id: str
    score: float
    fair_term: float
    age_term: float
    usage_share: float
    wait_hours: float

    def explain(self) -> str:
        return (
            f"score {self.score:.3f} = fair {self.fair_term:.3f} "
            f"(share {self.usage_share:.1%}) + age {self.age_term:.3f} "
            f"(waited {self.wait_hours:.1f}h)"
        )


@dataclass(frozen=True)
class Refusal:
    """A submission the broker would not accept, and exactly why."""

    reason: str
    code: str
    shortfall: Decimal | None = None
    currency: Currency | None = None

    def __str__(self) -> str:
        return self.reason


@dataclass(frozen=True)
class SubmitResult:
    job: Job
    refusal: Refusal | None = None
    warning: str | None = None

    @property
    def accepted(self) -> bool:
        return self.refusal is None


@dataclass(frozen=True)
class Drift:
    """One disagreement between what the broker recorded and what a backend
    actually has."""

    kind: str
    """ORPHAN (backend has it, we do not) or LOST (we have it, backend does not)
    or STATE_MISMATCH."""
    backend: str
    handle: str
    job_id: str | None
    detail: str

    def __str__(self) -> str:
        target = f"job {self.job_id[:8]}" if self.job_id else "no job record"
        return f"[{self.kind}] {self.backend}/{self.handle} ({target}): {self.detail}"


@dataclass(frozen=True)
class Decision:
    """What the scheduler concluded about one queued job on one pass.

    Produced by a single code path that both `tick` and `plan` consume, so a
    dry run cannot report something different from what a real tick would do.
    Two implementations of the same rules would drift, and the drift would only
    show up on the deploy the dry run was supposed to protect.
    """

    job: "Job"
    action: str
    """DISPATCH, BLOCKED_POOL, BLOCKED_CAPACITY, or BLOCKED_TENANT."""
    backend: str | None
    detail: str


@dataclass(frozen=True)
class DryRunPlan:
    """A dry run: what a tick would do, with nothing done."""

    at: dt.datetime
    decisions: tuple[Decision, ...] = ()
    checks: tuple[tuple[str, str], ...] = ()
    """(job_id, what the backend said a real launch would do). Populated by
    asking the provider, so a missing IAM permission shows up here rather than
    on the first real launch."""
    problems: tuple[tuple[str, str], ...] = ()

    @property
    def would_dispatch(self) -> tuple[Decision, ...]:
        return tuple(d for d in self.decisions if d.action == "DISPATCH")


@dataclass(frozen=True)
class TickReport:
    """What one pass of the scheduler did. Printed by `gpu tick`, returned to
    tests so they can assert on it instead of scraping logs."""

    at: dt.datetime
    dispatched: tuple[str, ...] = ()
    completed: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    settled: tuple[str, ...] = ()
    blocked_on_pool_cap: tuple[str, ...] = ()
    blocked_on_capacity: tuple[str, ...] = ()
    blocked_on_tenant: tuple[str, ...] = ()
    """Waiting on their own owner rather than on the pool: that member is
    already holding `max_running_jobs_per_user` machines."""
    stopped_at_ceiling: tuple[str, ...] = ()
