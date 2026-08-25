"""Dispatch, accrual, ceilings, and the pool cap."""

from __future__ import annotations

from decimal import Decimal

import pytest

from gpu_broker.backends import BackendStatus
from gpu_broker.money import Currency
from gpu_broker.states import ACTIVE, JobState


def test_a_job_runs_and_completes(broker, users, clock):
    job = broker.submit(user_id="ana", command="python train.py", gpu_type="a10g", hours=1).job
    broker.run_until_idle(step_seconds=120)

    final = broker.status(job.job_id)
    assert final.state is JobState.COMPLETED
    assert final.exit_code == 0
    assert final.started_at is not None and final.finished_at is not None


def test_capacity_is_released_when_a_job_completes(broker, users, cloud):
    """The failure this whole project exists to prevent: a finished job whose
    instance is still up and still billing."""
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.run_until_idle(step_seconds=120)

    assert cloud.list_resources() == [], "a finished job left its instance running"
    assert cloud.free_slots("a10g") == 2


def test_cost_accrues_from_allocation_not_from_first_output(broker, users, clock, cloud):
    """An EC2 instance charges for its boot time. So does this."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.tick()  # dispatch
    assert broker.status(job.job_id).state is JobState.ALLOCATING

    clock.advance(seconds=30)  # still booting; cloud fixture boots in 60s
    broker.tick()
    assert broker.job_spend(job.job_id) > 0, "boot time was billed to nobody"


def test_a_job_is_stopped_at_its_ceiling(broker, users, clock, cloud):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="1.50").job
    reports = broker.run_until_idle(step_seconds=120)

    final = broker.status(job.job_id)
    assert final.state is JobState.FAILED
    assert broker.job_spend(job.job_id) == Decimal("1.50")
    assert any(job.job_id in report.stopped_at_ceiling for report in reports)
    assert cloud.list_resources() == [], "the instance kept running past the ceiling"


def test_a_ceiling_kill_says_so_in_the_history(broker, users):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4, budget="1.50").job
    broker.run_until_idle(step_seconds=120)
    reasons = [reason for _, _, _, reason in broker.store.history(job.job_id) if reason]
    assert any("ceiling" in reason for reason in reasons)


def test_a_job_that_finishes_exactly_at_its_ceiling_counts_as_completed(broker, users, clock, cloud):
    """A successful run must not land in somebody's failure column because the
    ceiling check happened to be evaluated first."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    cloud.plan(job.job_id, run_hours=1.0)
    broker.run_until_idle(step_seconds=60)
    assert broker.status(job.job_id).state is JobState.COMPLETED


def test_a_failing_job_is_recorded_as_failed_and_still_billed(broker, users, cloud):
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    cloud.plan(result.job.job_id, outcome=BackendStatus.FAILED, exit_code=1)
    broker.run_until_idle(step_seconds=120)

    final = broker.status(result.job.job_id)
    assert final.state is JobState.FAILED
    assert final.exit_code == 1
    assert broker.job_spend(final.job_id) > 0, "a failed run still consumed real capacity"
    assert broker.budgets("ana")[Currency.USD].held == Decimal("0.00")


def test_a_launch_failure_leaves_the_job_queued_for_a_retry(broker, users, cloud):
    result = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    cloud.plan(result.job.job_id, launch_error="InsufficientInstanceCapacity")

    report = broker.tick()
    assert result.job.job_id in report.blocked_on_capacity
    assert broker.status(result.job.job_id).state is JobState.QUEUED
    lines = [line for _, _, _, line in broker.logs(result.job.job_id)]
    assert any("InsufficientInstanceCapacity" in line for line in lines)


def test_jobs_wait_when_there_are_no_free_slots(broker, users, cloud, clock):
    cloud.capacity = {"a10g": 1}
    first = broker.submit(user_id="ana", command="a", gpu_type="a10g", hours=1).job
    clock.advance(minutes=1)  # equal priority otherwise; submit time is the tiebreak
    second = broker.submit(user_id="bo", command="b", gpu_type="a10g", hours=1).job

    report = broker.tick()
    assert first.job_id in report.dispatched
    assert second.job_id in report.blocked_on_capacity
    assert broker.status(second.job_id).state is JobState.QUEUED


def test_scarcity_of_one_gpu_type_does_not_block_another(broker, users, cloud):
    """A job waiting on an A100 must not hold up a job that wants a T4."""
    cloud.capacity = {"a100": 0, "t4": 2}
    blocked = broker.submit(user_id="ana", command="big", gpu_type="a100", hours=1, budget="20").job
    small = broker.submit(user_id="bo", command="small", gpu_type="t4", hours=1).job

    report = broker.tick()
    assert small.job_id in report.dispatched
    assert blocked.job_id in report.blocked_on_capacity


def test_the_pool_cap_makes_jobs_wait_rather_than_refusing_them(broker, users, state_dir, clock, cloud, lab):
    """The pool cap is a hard stop, but it frees up as jobs settle, so a job it
    blocks belongs in the queue rather than in the bin."""
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config
    from dataclasses import replace

    config = replace(load_config(state_dir), pool_budget_usd=Decimal("6.00"))
    tight = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)
    for name in users:
        tight.add_user(name)

    first = tight.submit(user_id="ana", command="a", gpu_type="a10g", hours=1, budget="5").job
    clock.advance(minutes=1)
    second = tight.submit(user_id="bo", command="b", gpu_type="a10g", hours=1, budget="5").job
    assert second.state is JobState.QUEUED, "the pool cap refused instead of queueing"

    report = tight.tick()
    assert first.job_id in report.dispatched
    assert second.job_id in report.blocked_on_pool_cap
    assert tight.status(second.job_id).state is JobState.QUEUED
    tight.close()


def test_the_pool_cap_is_never_exceeded(broker, users, state_dir, clock, cloud, lab):
    from dataclasses import replace

    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    cap = Decimal("6.00")
    config = replace(load_config(state_dir), pool_budget_usd=cap)
    tight = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)
    for name in users:
        tight.add_user(name)
    for name in users:
        tight.submit(user_id=name, command="x", gpu_type="a10g", hours=1, budget="5")

    for _ in range(50):
        tight.tick()
        committed = tight.store.pool_committed(Currency.USD)
        assert committed <= cap, f"pool committed {committed}, cap is {cap}"
        assert tight.pool()[Currency.USD].spent <= cap, "the pool cap was actually overspent"
        clock.advance(minutes=10)
    tight.close()


def test_a_full_dollar_pool_does_not_idle_the_free_lab_machine(broker, users, state_dir, clock, cloud, lab):
    """Two currencies, two caps. Running out of one must not stop the other."""
    from dataclasses import replace

    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    config = replace(load_config(state_dir), pool_budget_usd=Decimal("1.00"))
    tight = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)
    for name in users:
        tight.add_user(name)

    paid = tight.submit(user_id="ana", command="paid", gpu_type="a10g", hours=1).job
    free = tight.submit(user_id="bo", command="free", gpu_type="a6000", hours=1).job

    report = tight.tick()
    assert paid.job_id in report.blocked_on_pool_cap
    assert free.job_id in report.dispatched
    tight.close()


def test_cancelling_a_running_job_stops_the_machine(broker, users, cloud, clock):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()
    assert broker.status(job.job_id).state in ACTIVE

    broker.cancel(job.job_id, actor="ana")
    assert broker.status(job.job_id).state is JobState.CANCELLED
    assert cloud.list_resources() == [], "cancel left the instance running"


def test_cancelling_a_finished_job_is_an_error_not_a_silent_no_op(broker, users):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.run_until_idle(step_seconds=120)
    from gpu_broker.errors import BrokerError

    with pytest.raises(BrokerError, match="already"):
        broker.cancel(job.job_id, actor="ana")


def test_free_and_paid_capacity_are_scheduled_from_one_queue(broker, users, clock):
    """The whole point of the project: one queue in front of heterogeneous capacity."""
    paid = broker.submit(user_id="ana", command="paid", gpu_type="a10g", hours=1).job
    free = broker.submit(user_id="bo", command="free", gpu_type="a6000", hours=1).job

    broker.run_until_idle(step_seconds=120)
    assert broker.status(paid.job_id).state is JobState.COMPLETED
    assert broker.status(free.job_id).state is JobState.COMPLETED
    assert broker.budgets("ana")[Currency.USD].spent > 0
    assert broker.budgets("bo")[Currency.GPU_HOUR].spent > 0
    assert broker.budgets("bo")[Currency.USD].spent == Decimal("0.00")


def test_a_job_never_spends_more_than_it_reserved(broker, users, clock, cloud):
    """The core promise, checked on every tick of a long overrunning job."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2, budget="2.00").job
    cloud.plan(job.job_id, run_hours=10.0)  # tries to run far past its ceiling

    for _ in range(60):
        broker.tick()
        assert broker.job_spend(job.job_id) <= Decimal("2.00")
        if broker.status(job.job_id).is_terminal:
            break
        clock.advance(minutes=10)

    assert broker.status(job.job_id).is_terminal
    assert broker.job_spend(job.job_id) == Decimal("2.00")


def test_logs_are_captured_and_not_duplicated(broker, users, clock):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.run_until_idle(step_seconds=120)
    lines = [line for _, _, _, line in broker.logs(job.job_id)]

    assert lines, "no output was captured at all"
    assert len(lines) == len(set(lines)), f"duplicated log lines: {lines}"
    assert any("dispatched" in line for line in lines)


def test_a_stale_backend_report_does_not_walk_a_job_backwards(broker, users, clock, cloud):
    """`describe-instances` is eventually consistent and will answer from before
    a change. Believing it would move a RUNNING job back to ALLOCATING."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job
    broker.tick()
    clock.advance(minutes=5)
    broker.tick()
    assert broker.status(job.job_id).state is JobState.RUNNING

    handle = broker.status(job.job_id).backend_handle
    cloud._resources[handle].ready_at = clock.now().timestamp() + 3600  # "still booting"

    broker.tick()  # must not raise, and must not regress the job
    assert broker.status(job.job_id).state is JobState.RUNNING
    lines = [line for _, _, _, line in broker.logs(job.job_id)]
    assert any("stale" in line for line in lines), "the stale report was not recorded"


def test_the_brokers_own_messages_do_not_swallow_the_jobs_output(broker, users, clock, cloud):
    """Regression: the log cursor used to be the number of rows on the job.

    The broker writes its own lines into the same table ("dispatched to
    fake/..."), so every one of those pushed the cursor forward and skipped a
    real line of the user's output. The cursor is now the job's own
    `logs_fetched`, which only the backend advances.
    """
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.run_until_idle(step_seconds=120)

    stored = [(stream, line) for _, _, stream, line in broker.logs(job.job_id)]
    from_backend = [line for stream, line in stored if stream != "broker"]

    assert from_backend, "none of the job's own output was stored"
    assert "training started" in " ".join(from_backend)
    assert any("completed" in line for line in from_backend)


def test_the_log_cursor_survives_a_restart(broker, users, state_dir, clock, cloud, lab):
    """On the way back up the broker resumes reading where it stopped. Not from
    the beginning, which would duplicate, and not from a guess."""
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()
    before = [line for _, _, _, line in broker.logs(job.job_id)]
    cursor = broker.status(job.job_id).logs_fetched
    assert cursor > 0
    broker.close()

    restarted = Broker.open(
        state_dir, clock=clock, backends=[cloud, lab], config=load_config(state_dir)
    )
    assert restarted.status(job.job_id).logs_fetched == cursor
    restarted.run_until_idle(step_seconds=300)

    after = [line for _, _, _, line in restarted.logs(job.job_id)]
    assert after[: len(before)] == before, "the restart rewrote earlier output"
    assert len(after) == len(set(after)), f"lines duplicated across the restart: {after}"
    restarted.close()
