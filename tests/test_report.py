"""The measurement report.

The build prompt asks for the test that proves work lost is zero, not the
assertion that it is. So the important tests here run jobs through real
preemptions and reclaims and then check what the report computed -- and, just as
importantly, check that the report is still *able* to say bad news. A generated
report that can only flatter is a hand-written one with extra steps.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.checkpoint import LocalCheckpointStore
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.report import SELF_LAUNCH_MINUTES, markdown
from gpu_broker.states import ACTIVE, JobState


@pytest.fixture
def rig(tmp_path: Path, clock):
    """Spot with an on-demand fallback, and a checkpoint store."""
    state_dir = tmp_path / "broker"
    checkpoints = LocalCheckpointStore(tmp_path / "ckpt")
    made = {
        name: FakeBackend(
            name, clock=clock, checkpoints=checkpoints, currency=Currency.USD,
            capacity={"a10g": 4}, startup_seconds=30.0, tier=tier,
            state_path=state_dir / f"{name}.json",
        )
        for name, tier in (("spot", "spot"), ("ondemand", "ondemand"))
    }
    config = replace(
        load_config(state_dir),
        pool_budget_usd=Decimal("10000"), default_budget_usd=Decimal("10000"),
    )
    broker = Broker.open(
        state_dir, clock=clock, backends=list(made.values()), config=config,
        checkpoints=checkpoints,
    )
    yield {"broker": broker, "backends": made, "clock": clock}
    broker.store.conn.close()


def submit(rig, user_id, hours=2.0, steps=600, every=60):
    broker = rig["broker"]
    broker.add_user(user_id)
    job = broker.submit(
        user_id=user_id, command=f"python train.py --{user_id}",
        gpu_type="a10g", hours=hours, budget="40",
    ).job
    for backend in rig["backends"].values():
        backend.plan(job.job_id, run_hours=hours, total_steps=steps, checkpoint_every=every)
    rig["clock"].advance(seconds=30)
    return job.job_id


def drain(rig, interrupt_every=0, max_ticks=600):
    broker, clock = rig["broker"], rig["clock"]
    ticks = 0
    for index in range(max_ticks):
        broker.tick()
        if not broker.store.queued_jobs() and not broker.store.active_jobs():
            return ticks
        if interrupt_every and index % interrupt_every == interrupt_every - 1:
            for job in broker.store.list_jobs(states=ACTIVE):
                if job.backend == "spot" and job.backend_handle:
                    rig["backends"]["spot"].interrupt(job.backend_handle)
        clock.advance(minutes=5)
        ticks += 1
    pytest.fail("the queue never drained")


# --------------------------------------------- the number that must be zero


def test_work_lost_is_zero_even_with_preemptions(rig):
    """The proof, not the claim. Jobs are really interrupted, really requeued,
    and really finish; the report then computes what was lost from the record."""
    for name in ("ana", "bo", "cy"):
        submit(rig, name)
    drain(rig, interrupt_every=3)

    report = rig["broker"].report()
    assert report.lost.jobs_lost == ()
    assert report.lost.zero
    assert report.adoption.jobs_completed == 3

    body = markdown(report)
    assert "**Zero.**" in body


def test_the_jobs_really_were_preempted(rig):
    """Otherwise the test above proves only that nothing happened."""
    ids = [submit(rig, name) for name in ("ana", "bo", "cy")]
    drain(rig, interrupt_every=3)
    assert sum(rig["broker"].status(job_id).preemptions for job_id in ids) > 0


def test_the_report_can_say_work_was_lost(rig):
    """A report that can only flatter is a hand-written one with extra steps."""
    job_id = submit(rig, "ana")
    rig["broker"].tick()
    rig["clock"].advance(minutes=30)
    rig["broker"].tick()
    rig["backends"]["spot"].interrupt(rig["broker"].status(job_id).backend_handle)
    rig["broker"].tick()
    # The member gives up rather than letting it resume.
    rig["broker"].cancel(job_id, actor="ana")

    report = rig["broker"].report()
    assert job_id in report.lost.jobs_lost
    assert not report.lost.zero
    assert "lost their work" in markdown(report)


def test_time_paid_for_twice_is_reported_separately_from_work_lost(rig):
    """A job with no checkpoint that still finishes lost nothing. It cost money
    twice, which is a different complaint and deserves a different line."""
    job_id = submit(rig, "ana", steps=0, every=0)  # never checkpoints
    rig["broker"].tick()
    rig["clock"].advance(minutes=30)
    rig["broker"].tick()
    rig["backends"]["spot"].interrupt(rig["broker"].status(job_id).backend_handle)
    drain(rig)

    report = rig["broker"].report()
    assert rig["broker"].status(job_id).state is JobState.COMPLETED
    assert report.lost.zero, "a job that finished was counted as lost"
    assert report.lost.jobs_redone == 1
    assert "paid for twice" in markdown(report)


def test_reclaimed_hours_are_counted_as_doing_nothing(broker, users, clock, cloud):
    """A reclaimed job did lose its work. The samples say the work was zero, and
    they are still on disk to say so."""
    job = broker.submit(user_id="bo", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    cloud.set_utilization(job.job_id, 0.0)
    for _ in range(40):
        clock.advance(minutes=1)
        broker.tick()
    broker.reclaim(force=True)

    report = broker.report()
    assert report.lost.jobs_reclaimed == 1
    assert report.lost.reclaimed_hours > 0
    assert report.lost.reclaimed_useful_hours == 0.0
    assert report.lost.zero


# ------------------------------------------------------------------ headline


def test_dollars_per_useful_hour_uses_useful_hours(broker, users, clock, cloud):
    """Not billed hours. A pool where half the time nothing is running costs
    twice as much per hour of work as the invoice suggests."""
    broker.submit(user_id="ana", command="busy", gpu_type="a10g", hours=2)
    clock.advance(minutes=1)
    idle = broker.submit(user_id="bo", command="idle", gpu_type="a10g", hours=2).job
    broker.tick()
    cloud.set_utilization(idle.job_id, 0.0)
    broker.run_until_idle(step_seconds=300)

    report = broker.report()
    assert report.efficiency.gpu_hours_useful < report.efficiency.gpu_hours_paid
    assert report.efficiency.dollars_per_useful_hour > report.efficiency.dollars_per_paid_hour


def test_a_pool_with_no_billed_time_says_so_rather_than_dividing_by_zero(broker, users):
    report = broker.report()
    assert report.efficiency.dollars_per_useful_hour is None
    assert "Not enough billed GPU time" in markdown(report)


# ------------------------------------------------------------------ adoption


def test_repeat_use_is_counted_separately(broker, users, clock):
    """Anyone can be talked into trying something once."""
    for _ in range(3):
        broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
        clock.advance(minutes=1)
    broker.submit(user_id="bo", command="x", gpu_type="a10g", hours=1)

    adoption = broker.report().adoption
    assert adoption.distinct_users == 2
    assert adoption.repeat_users == 1
    assert adoption.repeat_rate == 0.5


def test_refusals_are_counted_but_are_not_jobs(broker, users):
    broker.add_user("cy", budget_usd=Decimal("1.00"))
    broker.submit(user_id="cy", command="x", gpu_type="a10g", hours=4)
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)

    adoption = broker.report().adoption
    assert adoption.jobs_refused == 1
    assert adoption.jobs_submitted == 1
    assert adoption.distinct_users == 1, "a refused user was counted as a user"


def test_dollars_reclaimed_is_what_came_back(broker, users, clock, cloud):
    job = broker.submit(user_id="bo", command="x", gpu_type="a10g", hours=4, budget="20").job
    broker.tick()
    cloud.set_utilization(job.job_id, 0.0)
    for _ in range(40):
        clock.advance(minutes=1)
        broker.tick()
    broker.reclaim(force=True)

    assert broker.report().adoption.dollars_reclaimed > Decimal("0")


# -------------------------------------------------------- where it loses


def test_the_report_always_has_a_where_it_loses_section(broker, users):
    """The only way that number ever gets looked at is if it is printed next to
    the good ones."""
    assert "## Where it loses" in markdown(broker.report())


def test_startup_overhead_is_measured_and_priced(broker, users, clock, cloud):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.run_until_idle(step_seconds=60)

    loses = broker.report().loses
    assert loses.startup_hours_mean > 0
    assert "Startup overhead" in markdown(broker.report())


def test_queue_waits_are_compared_against_launching_it_yourself(broker, users, clock, cloud):
    cloud.capacity = {"a10g": 1}
    broker.submit(user_id="ana", command="a", gpu_type="a10g", hours=4)
    clock.advance(minutes=1)
    broker.submit(user_id="bo", command="b", gpu_type="a10g", hours=4)
    broker.tick()
    clock.advance(hours=2)
    broker.tick()

    loses = broker.report().loses
    assert loses.waits_longer_than_self_launch >= 1
    assert loses.worst_wait_hours > SELF_LAUNCH_MINUTES / 60.0
    assert "launch an instance yourself" in markdown(broker.report())


def test_the_heaviest_users_wait_is_named(broker, users, clock, cloud):
    cloud.capacity = {"a10g": 0}
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    broker.tick()
    clock.advance(hours=6)
    broker.tick()

    body = markdown(broker.report())
    assert "The heaviest user pays for fair share" in body
    assert "ana" in body


def test_what_cannot_be_measured_here_is_named(broker, users):
    """MPS overhead needs the same job run both ways on the real card. Saying so
    is better than leaving a gap somebody assumes was measured."""
    body = markdown(broker.report())
    assert "MPS overhead" in body
    assert "Not measured here" in body


def test_the_self_launch_assumption_is_labelled_as_a_guess(broker, users):
    assert "That is a guess, not a measurement" in markdown(broker.report())


def test_the_sampling_caveat_is_stated(broker, users):
    assert "sampled, not continuous" in markdown(broker.report())
