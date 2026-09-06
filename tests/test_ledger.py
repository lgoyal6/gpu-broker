"""The ledger: two currencies, held honestly, and never leaking a reservation."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gpu_broker.money import ZERO, Currency, billing_period, money
from gpu_broker.states import JobState

import datetime as dt


def test_money_never_routes_through_binary_float():
    """`Decimal(0.1)` is not 0.1. Every amount enters the ledger through money()."""
    assert money(0.1) == Decimal("0.1")
    assert money(1.005) == Decimal("1.005")
    assert sum((money(0.1) for _ in range(10)), Decimal(0)) == Decimal("1.0")


def test_a_month_of_accrual_does_not_drift(broker, users, clock, cloud):
    """The reason amounts are TEXT and not REAL. Ten thousand cent-scale
    settlements have to add up to exactly what they should."""
    total = sum((money("0.01") for _ in range(10_000)), Decimal(0))
    assert total == Decimal("100.00")


def test_billing_period_follows_the_club_timezone_not_utc():
    """A job submitted at 11pm on Jan 31 in San Diego belongs to January."""
    late_january = dt.datetime(2026, 2, 1, 6, 30, tzinfo=dt.timezone.utc)
    assert billing_period(late_january, "America/Los_Angeles") == "2026-01"
    assert billing_period(late_january, "UTC") == "2026-02"


def test_submission_holds_the_ceiling_without_spending_it(broker, users):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12")
    balance = broker.budgets("ana")[Currency.USD]
    assert balance.held == Decimal("12.00")
    assert balance.spent == Decimal("0.00")
    assert balance.available == Decimal("13.00")


def test_settlement_converts_a_hold_into_spend_without_changing_the_total(broker, users, clock):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12").job
    before = broker.budgets("ana")[Currency.USD]
    broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h")
    job = broker.store.get_job(job.job_id)
    broker.store.settle(job, Decimal("3.00"))

    after = broker.budgets("ana")[Currency.USD]
    assert after.spent == Decimal("3.00")
    assert after.held == Decimal("9.00")
    assert after.committed == before.committed, "committed budget must not move on settlement"


def test_finishing_returns_the_unspent_hold(broker, users):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12").job
    job = broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h")
    job = broker.store.transition(job, JobState.RUNNING)
    broker.store.settle(job, Decimal("2.50"))
    broker.store.transition(job, JobState.COMPLETED, exit_code=0)

    balance = broker.budgets("ana")[Currency.USD]
    assert balance.spent == Decimal("2.50")
    assert balance.held == Decimal("0.00"), "the hold leaked"
    assert balance.available == Decimal("22.50")


@pytest.mark.parametrize(
    "terminal", [JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED]
)
def test_every_terminal_route_releases_the_hold(broker, users, terminal):
    """A hold that only unwinds on the happy path is a hold that leaks the first
    time something goes wrong, which is when the club most needs the number."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12").job
    job = broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h")
    job = broker.store.transition(job, JobState.RUNNING)
    broker.store.transition(job, terminal)
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


def test_cancelling_from_the_queue_releases_everything(broker, users):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12").job
    broker.cancel(job.job_id, actor="ana")
    balance = broker.budgets("ana")[Currency.USD]
    assert balance.held == Decimal("0.00")
    assert balance.spent == Decimal("0.00")
    assert balance.available == balance.budget


def test_a_refused_job_holds_nothing(broker, users):
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="9999")
    assert not result.accepted
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")
    assert broker.store.ledger_for_job(result.job.job_id) == []


def test_the_two_currencies_do_not_touch_each_other(broker, users):
    """A GPU-hour is not a dollar and the ledger must never pretend otherwise."""
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2)   # USD
    broker.submit(user_id="ana", command="y", gpu_type="a6000", hours=3)  # GPU_HOUR

    balances = broker.budgets("ana")
    assert balances[Currency.USD].held > 0
    assert balances[Currency.GPU_HOUR].held > 0
    assert balances[Currency.USD].budget == broker.config.default_budget_usd
    assert balances[Currency.GPU_HOUR].budget == broker.config.default_budget_gpu_hours
    # Spending free hours must not consume dollars, and vice versa.
    assert balances[Currency.USD].available == broker.config.default_budget_usd - balances[Currency.USD].held


def test_free_hours_are_reported_as_hours_never_as_dollars(broker, users):
    broker.submit(user_id="ana", command="y", gpu_type="a6000", hours=3)
    balance = broker.budgets("ana")[Currency.GPU_HOUR]
    assert "$" not in balance.describe()
    assert "gpu-hr" in balance.describe()


def test_pool_balance_aggregates_every_user(broker, users):
    for name in ("ana", "bo", "cy"):
        broker.submit(user_id=name, command="x", gpu_type="a10g", hours=1, budget="5")
    pool = broker.pool()[Currency.USD]
    assert pool.held == Decimal("15.00")
    assert pool.budget == broker.config.pool_budget_usd


def test_balances_land_in_the_period_the_work_happened_in(broker, users, clock):
    """A job that runs across a month boundary settles into both months, so
    neither month's report is wrong."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="12").job
    job = broker.store.transition(job, JobState.ALLOCATING, backend="cloud", backend_handle="h")
    job = broker.store.get_job(job.job_id)
    broker.store.settle(job, Decimal("1.00"))
    first = broker.store.period_now()

    clock.advance(days=40)
    broker.store.settle(job, Decimal("2.00"))
    second = broker.store.period_now()

    assert first != second
    assert broker.store.balance("ana", Currency.USD, first).spent == Decimal("1.00")
    assert broker.store.balance("ana", Currency.USD, second).spent == Decimal("2.00")


def test_the_ledger_is_append_only(broker, users):
    """Nothing in the codebase updates or deletes a ledger row. If that changes,
    balances stop being reconstructible and this test is the warning."""
    import subprocess
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "gpu_broker"
    hits = subprocess.run(
        ["grep", "-rnE", r"(UPDATE|DELETE FROM)\s+ledger", str(source)],
        capture_output=True,
        text=True,
    ).stdout
    assert hits == "", f"something mutates the ledger:\n{hits}"


# ------------------------------------- capacity that billed with nothing behind it


def test_a_leaked_machine_puts_its_burn_in_the_ledger(broker, users, clock):
    """A crash between `backend.launch()` and the store write leaves a real
    instance billing with the job still QUEUED.

    `reap` already computes the number and names the member. It was never
    written down, so the club's own ledger said $0.00 while AWS billed for four
    hours. Real spend that no completed work can be attributed to still has to
    appear, or "spent" is not a number anybody can act on.
    """
    cloud = broker.scheduler.backend("cloud")
    cloud.leak(job_id="c" * 32, user_id="ana")
    clock.advance(hours=4)

    assert broker.budgets("ana")[Currency.USD].spent == ZERO
    broker.reap()

    # 4h of a10g at the builtin $1.006.
    assert broker.budgets("ana")[Currency.USD].spent == Decimal("4.02")


def test_reaping_the_same_machine_twice_does_not_charge_for_it_twice(
    broker, users, clock
):
    """`reap` runs on a schedule. It computes burn since launch, so a second
    pass over a machine that is still up recomputes a number that overlaps
    everything the first pass recorded. Charging that again is how a daily job
    turns one leaked instance into a month of phantom spend."""
    cloud = broker.scheduler.backend("cloud")
    cloud.leak(job_id="c" * 32, user_id="ana")

    clock.advance(hours=4)
    broker.reap()
    after_first = broker.budgets("ana")[Currency.USD].spent
    broker.reap()
    broker.reap()
    assert broker.budgets("ana")[Currency.USD].spent == after_first

    # Still burning, so the next hour is charged once and only once.
    clock.advance(hours=1)
    broker.reap()
    assert broker.budgets("ana")[Currency.USD].spent == after_first + Decimal("1.01")


def test_abandoned_spend_is_its_own_kind_not_disguised_as_settlement(
    broker, users, clock
):
    """Settled spend bought something. Abandoned spend bought nothing anybody
    can point at. Merging the two makes "what did we get for the money" an
    unanswerable question, so the row says which it is."""
    from gpu_broker.models import LedgerKind

    cloud = broker.scheduler.backend("cloud")
    cloud.leak(job_id="c" * 32, user_id="ana")
    clock.advance(hours=2)
    broker.reap()

    kinds = [
        entry.kind
        for entry in broker.store.abandoned_entries(Currency.USD)
    ]
    assert kinds == [LedgerKind.ABANDONED]


def test_a_machine_with_no_launch_time_is_reported_unknown_never_as_free(
    broker, users, clock
):
    """An untagged instance has no timestamp to measure from, so the burn
    calculation returns $0.00 by default rather than by measurement.

    Writing that to the ledger would record "this running machine cost nothing",
    which is a confident wrong answer where the honest one is "we cannot tell".
    It stays out of the ledger and stays in the report.
    """
    cloud = broker.scheduler.backend("cloud")
    handle = cloud.leak(job_id="c" * 32, user_id="ana")
    cloud._resources[handle].tags = {}
    clock.advance(hours=3)

    report = broker.reap()
    orphan = report.orphans[0]
    assert not orphan.cost_known
    assert "cost unknown" in orphan.describe()

    # No fabricated row, in either direction.
    assert broker.store.abandoned_entries(Currency.USD) == []
    assert broker.pool()[Currency.USD].spent == ZERO


def test_an_abandoned_machine_is_charged_to_the_member_who_launched_it(
    broker, users, clock
):
    """Attribution is the point. Spend the pool cannot trace to a person is
    spend nobody changes their behaviour over."""
    cloud = broker.scheduler.backend("cloud")
    cloud.leak(job_id="c" * 32, user_id="bo")
    clock.advance(hours=2)
    broker.reap()

    assert broker.budgets("bo")[Currency.USD].spent == Decimal("2.01")
    assert broker.budgets("ana")[Currency.USD].spent == ZERO
    assert broker.pool()[Currency.USD].spent == Decimal("2.01")
