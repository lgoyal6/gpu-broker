"""Whether a submission is allowed in at all.

The standing rule is that a budget is never silently exceeded. That splits into
two behaviours that look similar and are not:

  refused   The user's own monthly budget cannot cover this job. Nothing about
            waiting fixes it. Say the number and the shortfall, record the
            refusal, and stop.

  waits     The pool-wide cap is currently committed. That frees up as jobs
            settle, so the job sits in the queue rather than being thrown away.
            Checked at dispatch, not here.

Typos are not refusals. An unknown GPU type or a negative hour count raises,
because there is nothing to audit later and nothing the club needs a record of.
"""

from __future__ import annotations

from decimal import Decimal

from .config import BrokerConfig
from .errors import BrokerError
from .models import Balance, Refusal, User
from .money import ZERO, Currency, fmt, money, quantize


def validate_request(
    config: BrokerConfig, gpu_type: str, hours: float, command: str = ""
) -> None:
    """Reject nonsense before anything is written. Raises, does not refuse."""
    config.gpu(gpu_type)  # raises UnknownGpuType with the list of known types
    if hours <= 0:
        raise BrokerError(f"--hours must be positive, got {hours}")
    if hours > config.max_job_hours:
        raise BrokerError(
            f"--hours {hours:g} exceeds the per-job limit of "
            f"{config.max_job_hours:g}h. Split the run, or checkpoint and resubmit."
        )
    if len(command) > config.max_command_bytes:
        # Raises rather than refusing on purpose. A recorded refusal keeps the
        # command, so recording a ten-megabyte one is the damage the limit
        # exists to prevent.
        raise BrokerError(
            f"the command is {len(command):,} characters, over the limit of "
            f"{config.max_command_bytes:,}. Put it in a script under /data and "
            f"submit the script."
        )


def billable_hours(config: BrokerConfig, hours: float) -> float:
    """Requested run time plus the startup the broker will be billed for.

    Every cost estimate in the system goes through this, so the number quoted at
    submit time and the number charged at tick time cannot drift apart.
    """
    return hours + config.startup_allowance_hours


def default_reservation(config: BrokerConfig, gpu_type: str, hours: float) -> Decimal:
    """What the job costs if it runs for exactly as long as it asked.

    This is the default `--budget`. It includes the startup allowance: a
    reservation that covers only run time would be short by however long the
    machine took to boot, and the job would be stopped at its ceiling just before
    finishing. That failure is invisible in a unit test with an instant fake and
    obvious the first time a real g5 takes ninety seconds to come up.
    """
    gpu = config.gpu(gpu_type)
    return quantize(gpu.hourly_price * money(billable_hours(config, hours)), gpu.currency)


def check_job_cap(
    config: BrokerConfig, currency: Currency, reserved: Decimal
) -> Refusal | None:
    if reserved <= ZERO:
        return Refusal(
            code="INVALID_BUDGET",
            reason=f"--budget must be positive, got {fmt(reserved, currency)}",
        )
    cap = config.max_job(currency)
    if reserved > cap:
        return Refusal(
            code="JOB_CAP_EXCEEDED",
            reason=(
                f"refused: this job reserves {fmt(reserved, currency)}, over the "
                f"per-job cap of {fmt(cap, currency)} "
                f"(short {fmt(quantize(reserved - cap, currency), currency)}). "
                f"Lower --hours, or set --budget under the cap to stop it early."
            ),
            shortfall=quantize(reserved - cap, currency),
            currency=currency,
        )
    return None


def check_queue_depth(config: BrokerConfig, queued: int) -> Refusal | None:
    """How many jobs one member may have waiting at once.

    A second mechanism doing the budget's job, which NOTES.md says to be
    suspicious of, and it is here because the budget's version of this can be
    dodged: a reservation is whatever `--budget` says, so `--budget 0.01` turns
    the 33 queued jobs a $25 budget allows into 2500. The limit is set just
    under what the default budget already permits, so it bites the dodge and
    nothing else.

    Refused, not raised: this one is worth a record. A member who hits it
    repeatedly is the thing an officer wants to be able to look up.
    """
    if queued < config.max_queued_jobs_per_user:
        return None
    return Refusal(
        code="QUEUE_DEPTH_EXCEEDED",
        reason=(
            f"refused: you already have {queued} jobs waiting, which is the limit "
            f"of {config.max_queued_jobs_per_user}. They go as capacity frees up; "
            f"cancel one with `gpu cancel` if you would rather submit this instead."
        ),
    )


def check_membership(user: User) -> Refusal | None:
    """Is this person still allowed to spend the club's money.

    Refused rather than raised, so an officer can look up what a suspended
    member tried to do and when. That is the whole reason a refusal is a row.
    """
    if not user.is_suspended:
        return None
    reason = user.suspended_reason or "no reason recorded"
    return Refusal(
        code="USER_SUSPENDED",
        reason=(
            f"refused: {user.user_id} is suspended from the pool ({reason}). "
            f"Ask an officer to restore you with `gpu admin restore {user.user_id}`."
        ),
    )


def check_user_budget(balance: Balance, reserved: Decimal) -> Refusal | None:
    """The refusal the gate cares about: named number, named shortfall."""
    if reserved <= balance.available:
        return None

    shortfall = quantize(reserved - balance.available, balance.currency)
    currency = balance.currency
    return Refusal(
        code="USER_BUDGET_EXCEEDED",
        reason=(
            f"refused: this job needs {fmt(reserved, currency)} but you have "
            f"{fmt(balance.available, currency)} left this month "
            f"(short {fmt(shortfall, currency)}). "
            f"Your {fmt(balance.budget, currency)} budget is "
            f"{fmt(balance.spent, currency)} spent and "
            f"{fmt(balance.held, currency)} held by running jobs."
        ),
        shortfall=shortfall,
        currency=currency,
    )


def ceiling_warning(
    config: BrokerConfig, gpu_type: str, hours: float, reserved: Decimal
) -> str | None:
    """Warn when `--budget` cannot pay for `--hours`.

    Not a refusal. The job is legal and will run; it will just be stopped when
    it hits its ceiling. Saying so at submit time is the difference between an
    expected outcome and a confusing one.
    """
    gpu = config.gpu(gpu_type)
    full_cost = quantize(gpu.hourly_price * money(billable_hours(config, hours)), gpu.currency)
    if full_cost <= reserved:
        return None
    usable = max(
        0.0, float(reserved / gpu.hourly_price) - config.startup_allowance_hours
    )
    return (
        f"heads up: {fmt(reserved, gpu.currency)} buys about {usable:.1f}h of compute "
        f"on {gpu_type} (after startup), but you asked for {hours:g}h. The job will be "
        f"stopped at its ceiling. Raise --budget to {fmt(full_cost, gpu.currency)} to "
        f"run the full {hours:g}h."
    )

