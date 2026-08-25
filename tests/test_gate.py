"""Phase 0 gate.

Stated in the build prompt as:

    20 jobs, 5 users, unequal prior usage, assert fair-share ordering.
    Over-budget refused with the shortfall named. Over-cap waits.
    Kill and restart mid-queue, assert queue, ledger, and running-job
    records survive.

One test per clause, named after the clause, so a failure says which part of
the gate broke rather than which helper did.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from conftest import settle_history
from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.money import ZERO, Currency
from gpu_broker.states import ACTIVE, JobState

REPO = Path(__file__).resolve().parents[1]

# Ascending prior consumption. `ana` has used nothing, `eo` has used the most.
# Backdated, so it is last month's usage: it shapes this month's queue position
# without eating this month's budget.
PRIOR_USAGE = [("ana", "0.00"), ("bo", "6.00"), ("cy", "14.00"), ("di", "22.00"), ("eo", "31.00")]


def seed_prior_usage(broker: Broker) -> None:
    for name, amount in PRIOR_USAGE:
        if amount != "0.00":
            settle_history(broker, name, amount, days_ago=20)


# ---------------------------------------------------------------- clause one


def test_20_jobs_5_users_unequal_prior_usage_are_ordered_by_fair_share(broker, users, clock):
    seed_prior_usage(broker)

    # Submitted heaviest-user-first, so submit order is the exact opposite of
    # the order fair share should produce. If ordering were even partly FIFO,
    # this fails.
    for round_number in range(4):
        for name, _ in reversed(PRIOR_USAGE):
            broker.submit(
                user_id=name,
                command=f"python train.py --round {round_number}",
                gpu_type="a10g",
                hours=1,
                budget="2.00",
            )
            clock.advance(minutes=1)

    entries = broker.queue()
    assert len(entries) == 20, "not every job made it into the queue"

    order_of_first_appearance: list[str] = []
    for job, _ in entries:
        if job.user_id not in order_of_first_appearance:
            order_of_first_appearance.append(job.user_id)

    assert order_of_first_appearance == [name for name, _ in PRIOR_USAGE], (
        "queue is not ordered by prior usage"
    )

    # And the priorities are monotone in usage share, not merely in the right order.
    by_user = {}
    for job, priority in entries:
        by_user.setdefault(job.user_id, priority)
    shares = [by_user[name].usage_share for name, _ in PRIOR_USAGE]
    assert shares == sorted(shares), f"usage shares are not monotone: {shares}"


def test_fair_share_actually_redistributes_capacity_not_just_position(broker, users, clock, cloud):
    """Ordering that never changes who gets served is not fair share.

    Run all twenty jobs to completion on one slot and check that the user who
    arrived heaviest ends up having consumed the least this month. That is the
    outcome the club cares about; the sort key is only the mechanism.
    """
    cloud.capacity = {"a10g": 1}
    seed_prior_usage(broker)

    for round_number in range(4):
        for name, _ in reversed(PRIOR_USAGE):
            broker.submit(
                user_id=name, command=f"r{round_number}", gpu_type="a10g",
                hours=0.5, budget="2.00",
            )
            clock.advance(minutes=1)

    broker.run_until_idle(step_seconds=300)

    finished = [job for job in broker.store.list_jobs() if job.state is JobState.COMPLETED]
    assert len(finished) == 20, f"only {len(finished)} of 20 jobs completed"

    # Everyone got served, and nobody was locked out.
    served = {job.user_id for job in finished}
    assert served == {name for name, _ in PRIOR_USAGE}

    # This month's spend is near-equal, because the jobs were identical and every
    # user got the same number of them.
    spend = [broker.budgets(name)[Currency.USD].spent for name, _ in PRIOR_USAGE]
    assert max(spend) - min(spend) < Decimal("0.20"), f"lopsided spend: {spend}"


# ---------------------------------------------------------------- clause two


def test_over_budget_is_refused_with_the_shortfall_named(broker, users):
    broker.add_user("cy", budget_usd=Decimal("5.00"))
    broker.submit(user_id="cy", command="first", gpu_type="a10g", hours=2, budget="3.00")

    result = broker.submit(user_id="cy", command="second", gpu_type="a10g", hours=4, budget="4.00")

    assert not result.accepted
    assert result.job.state is JobState.REFUSED
    refusal = result.refusal

    # The shortfall is a real number, and it is right: $4.00 wanted against
    # $2.00 available after the $3.00 already held.
    assert refusal.shortfall == Decimal("2.00")
    assert refusal.currency is Currency.USD

    # And it is named in the text a person reads, along with what they have left.
    assert "short $2.00" in refusal.reason
    assert "$2.00 left this month" in refusal.reason
    assert "$3.00 held" in refusal.reason

    # Nothing was consumed by the refusal itself.
    assert broker.budgets("cy")[Currency.USD].held == Decimal("3.00")


# -------------------------------------------------------------- clause three


def test_over_the_pool_cap_waits(broker, users, state_dir, clock, cloud, lab):
    """The pool cap is a hard stop, and a job it stops is queued, not refused."""
    cap = Decimal("8.00")
    config = replace(load_config(state_dir), pool_budget_usd=cap)
    tight = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)
    for name in users:
        tight.add_user(name)

    jobs = []
    for name in users:
        jobs.append(tight.submit(user_id=name, command="x", gpu_type="a10g", hours=1, budget="3.00").job)
        clock.advance(minutes=1)

    # All five were admitted. None was refused for the pool's sake.
    assert all(job.state is JobState.QUEUED for job in jobs)

    report = tight.tick()
    assert len(report.dispatched) == 2, "the pool cap did not stop dispatch at 2 x $3.00"
    assert report.blocked_on_pool_cap, "the blocked jobs were not reported as such"

    # The ones that did not fit are still queued, and still theirs.
    waiting = [tight.status(job.job_id) for job in jobs if job.job_id not in report.dispatched]
    assert all(job.state is JobState.QUEUED for job in waiting)

    # The cap is never crossed, no matter how long this runs.
    for _ in range(60):
        tight.tick()
        assert tight.store.pool_committed(Currency.USD) <= cap
        assert tight.pool()[Currency.USD].spent <= cap
        clock.advance(minutes=10)

    # And the waiting jobs did eventually get their turn as room appeared.
    assert any(
        tight.status(job.job_id).state is not JobState.QUEUED for job in waiting
    ), "jobs blocked by the pool cap never ran"
    tight.close()


# --------------------------------------------------------------- clause four


@pytest.mark.timeout(120)
def test_kill_and_restart_mid_queue_preserves_queue_ledger_and_running_records(tmp_path):
    """SIGKILL a real process mid-queue and check the three things by name.

    `crash_helper.py` submits 20 jobs across 5 users, dispatches some, and then
    keeps writing until it is killed, so the kill lands inside the write path.
    """
    state_dir = tmp_path / "broker"
    state_dir.mkdir(parents=True)

    process = subprocess.Popen(
        [sys.executable, str(REPO / "tests" / "crash_helper.py"), str(state_dir)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=str(REPO),
    )
    try:
        deadline = time.time() + 45
        while time.time() < deadline:
            if process.stdout.readline().strip() == "READY":
                break
            if process.poll() is not None:
                pytest.fail(f"helper died early:\n{process.stderr.read()}")
        else:
            pytest.fail("helper never became ready")
        time.sleep(0.4)
        os.kill(process.pid, signal.SIGKILL)
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()

    clock = ManualClock()
    config = load_config(state_dir)
    cloud = FakeBackend(
        "cloud", clock=clock, currency=Currency.USD,
        capacity={"a10g": 3, "t4": 2}, state_path=state_dir / "cloud.json",
    )
    broker = Broker.open(state_dir, clock=clock, backends=[cloud], config=config)

    jobs = broker.store.list_jobs()

    # --- the queue survived ---
    assert len(jobs) >= 20, f"lost jobs: only {len(jobs)} survived"
    assert len(broker.users()) == 5
    queued = broker.store.queued_jobs()
    assert queued, "the whole queue was lost"
    ordering = [job.job_id for job, _ in broker.queue()]
    assert len(ordering) == len(queued) and len(set(ordering)) == len(ordering)

    # --- the ledger survived, and still balances ---
    for user in broker.users():
        for currency in Currency:
            balance = broker.store.balance(user.user_id, currency)
            assert balance.held >= ZERO, f"{user.user_id} holds {balance.held}"
            assert balance.spent >= ZERO
            assert balance.committed <= balance.budget, (
                f"{user.user_id} is committed past their budget"
            )
    # Every queued job's reservation is still on the books.
    for job in queued:
        assert broker.store.job_holding(job.job_id, job.currency) > ZERO, (
            f"queued job {job.short_id} lost its reservation"
        )

    # --- the running-job records survived ---
    active = broker.store.list_jobs(states=ACTIVE)
    assert active, "no running jobs survived the crash"
    for job in active:
        assert job.backend and job.backend_handle, "a running job lost its machine"
        assert job.started_at is not None
    # And they still point at machines the backend agrees exist.
    live = {(r.handle) for r in cloud.list_resources()}
    for job in active:
        assert job.backend_handle in live, (
            f"{job.short_id} points at {job.backend_handle}, which the backend does not have"
        )

    # --- and the broker is usable again ---
    assert broker.reconcile() == [], "a clean restart reported drift"
    broker.run_until_idle(step_seconds=600)
    assert not broker.store.queued_jobs()
    assert not broker.store.active_jobs()
    assert cloud.list_resources() == [], "machines were left running after the queue drained"
    broker.close()
