"""Refusals: the number named, the shortfall named, and nothing written that
should not have been."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gpu_broker.errors import BrokerError, UnknownGpuType, UnknownUser
from gpu_broker.money import Currency
from gpu_broker.states import JobState


def test_a_normal_submission_is_accepted(broker, users):
    result = broker.submit(user_id="ana", command="python train.py", gpu_type="a10g", hours=4)
    assert result.accepted
    assert result.job.state is JobState.QUEUED
    assert result.refusal is None


def test_over_budget_is_refused_with_the_shortfall_named(broker, users):
    broker.add_user("cy", budget_usd=Decimal("3.00"))
    result = broker.submit(user_id="cy", command="x", gpu_type="a10g", hours=4)

    assert not result.accepted
    refusal = result.refusal
    assert refusal.code == "USER_BUDGET_EXCEEDED"
    assert refusal.shortfall is not None and refusal.shortfall > 0
    # The three numbers a person needs to act on.
    assert "$3.00" in refusal.reason, "the budget itself is not named"
    assert f"${refusal.shortfall}" in refusal.reason, "the shortfall is not named"
    assert str(result.job.reserved) in refusal.reason, "the job's cost is not named"


def test_a_refusal_accounts_for_budget_already_held(broker, users):
    """Two jobs that each fit but together do not. The second is refused, and
    the message says the first one is holding the difference."""
    broker.add_user("cy", budget_usd=Decimal("6.00"))
    first = broker.submit(user_id="cy", command="a", gpu_type="a10g", hours=4, budget="5")
    assert first.accepted

    second = broker.submit(user_id="cy", command="b", gpu_type="a10g", hours=4, budget="5")
    assert not second.accepted
    assert "held by running jobs" in second.refusal.reason
    assert "$5.00 held" in second.refusal.reason


def test_over_the_per_job_cap_is_refused(broker, users):
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="9999")
    assert result.refusal.code == "JOB_CAP_EXCEEDED"
    assert str(broker.config.max_job_usd) in result.refusal.reason


def test_a_refusal_is_recorded_but_holds_nothing(broker, users):
    """'Why was I refused on Tuesday' has to be answerable on Friday."""
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="9999")
    stored = broker.status(result.job.job_id)
    assert stored.state is JobState.REFUSED
    assert stored.refusal_reason == result.refusal.reason
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


def test_a_refused_job_is_not_in_the_queue(broker, users):
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="9999")
    assert result.job.job_id not in [job.job_id for job, _ in broker.queue()]


def test_a_typo_raises_instead_of_being_recorded_as_a_refusal(broker, users):
    """An unknown GPU type is a mistake, not a policy decision. Recording it as
    a refusal would fill the audit log with noise."""
    with pytest.raises(UnknownGpuType, match="a10g"):
        broker.submit(user_id="ana", command="x", gpu_type="h100", hours=1)
    assert broker.history(user_id="ana") == []


def test_an_unknown_user_raises_before_anything_is_written(broker, users):
    with pytest.raises(UnknownUser, match="admin add-user"):
        broker.submit(user_id="nobody", command="x", gpu_type="a10g", hours=1)


@pytest.mark.parametrize("hours", [0, -1, 25])
def test_impossible_hours_are_rejected(broker, users, hours):
    with pytest.raises(BrokerError):
        broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=hours)


def test_the_default_budget_covers_startup_as_well_as_run_time(broker, users, clock):
    """Billing starts at allocation, so a reservation covering only run time is
    short by however long the machine took to boot, and the job is stopped just
    before it finishes."""
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2)
    price = broker.config.gpu("a10g").hourly_price
    assert result.job.reserved > price * 2
    assert result.warning is None

    broker.run_until_idle(step_seconds=120)
    assert broker.status(result.job.job_id).state is JobState.COMPLETED


def test_an_underfunded_budget_warns_instead_of_refusing(broker, users):
    """It is a legal job. It will just stop early, and saying so up front is the
    difference between an expected outcome and a confusing one."""
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="1.50")
    assert result.accepted
    assert "stopped at its ceiling" in result.warning
    assert "1.2h" in result.warning


def test_free_capacity_is_budgeted_in_gpu_hours_not_dollars(broker, users):
    result = broker.submit(user_id="ana", command="x", gpu_type="a6000", hours=3)
    assert result.job.currency is Currency.GPU_HOUR
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")
    assert broker.budgets("ana")[Currency.GPU_HOUR].held == Decimal("3.25")


def test_running_out_of_free_hours_is_refused_in_hours(broker, users):
    broker.add_user("cy", budget_gpu_hours=Decimal("2.00"))
    result = broker.submit(user_id="cy", command="x", gpu_type="a6000", hours=8)
    assert not result.accepted
    assert "gpu-hr" in result.refusal.reason
    assert "$" not in result.refusal.reason, "free hours were priced in dollars"
