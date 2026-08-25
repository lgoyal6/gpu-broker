"""`gpu reap`: what is alive with no live job behind it, and what it has cost.

The rule that matters most here is negative. Reap reports. It never terminates,
and there is no flag that makes it. An automatic cleanup that is wrong once
deletes somebody's training run.
"""

from __future__ import annotations

from decimal import Decimal

from gpu_broker.money import Currency
from gpu_broker.states import JobState


def test_a_healthy_system_has_nothing_to_reap(broker, users):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2)
    broker.tick()

    report = broker.reap()
    assert report.orphans == ()
    assert report.scanned == 1


def test_a_finished_job_whose_machine_is_still_up(broker, users, clock, cloud):
    """The failure the club actually has: the job ended, the instance did not."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.tick()
    handle = broker.status(job.job_id).backend_handle

    # The job finishes, but the terminate never lands: a crash, a throttle, a
    # network blip between recording the state and releasing the machine.
    broker.store.transition(broker.status(job.job_id), JobState.RUNNING)
    broker.store.transition(broker.status(job.job_id), JobState.COMPLETED, exit_code=0)

    report = broker.reap()
    assert len(report.orphans) == 1
    orphan = report.orphans[0]
    assert orphan.handle == handle
    assert orphan.user_id == "ana"
    assert "still up and billing" in orphan.reason


def test_a_machine_with_no_job_record_at_all(broker, users, cloud):
    """The broker died between run_instances returning and the handle being
    committed. Nothing will ever clean this up on its own."""
    cloud.leak(job_id="f" * 32, user_id="bo")

    report = broker.reap()
    assert len(report.orphans) == 1
    assert report.orphans[0].user_id == "bo"
    assert "no record of this job" in report.orphans[0].reason


def test_two_machines_running_for_one_job(broker, users, cloud):
    """The broker relaunched somewhere else and lost track of the first machine.
    Both are billing; only one is doing anything."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    stray = cloud.leak(job_id=job.job_id, user_id="ana")

    report = broker.reap()
    assert [orphan.handle for orphan in report.orphans] == [stray]
    assert "Two machines are running for one job" in report.orphans[0].reason


def test_an_untagged_machine_is_reported_as_unattributable(broker, users, cloud):
    handle = cloud.leak(job_id="g" * 32, user_id="cy")
    cloud._resources[handle].tags = {}

    report = broker.reap()
    assert len(report.orphans) == 1
    assert not report.orphans[0].attributable
    assert "cannot say whose this is" in report.orphans[0].reason


def test_an_orphan_reports_what_it_has_burned(broker, users, clock, cloud):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1).job
    broker.tick()
    broker.store.transition(broker.status(job.job_id), JobState.RUNNING)
    broker.store.transition(broker.status(job.job_id), JobState.COMPLETED)

    clock.advance(hours=10)
    orphan = broker.reap().orphans[0]

    assert orphan.age_hours == 10.0
    # Ten hours of a10g at the configured rate.
    assert orphan.burned == Decimal("10.06")
    assert orphan.currency is Currency.USD
    assert "$10.06" in orphan.describe()


def test_free_capacity_is_reported_in_gpu_hours(broker, users, clock, lab):
    """A leaked lab job costs hours nobody else could use, not dollars."""
    lab.leak(job_id="h" * 32, user_id="di", gpu_type="a6000")
    clock.advance(hours=5)

    orphan = broker.reap().orphans[0]
    assert orphan.currency is Currency.GPU_HOUR
    assert "$" not in orphan.describe()
    assert "gpu-hr" in orphan.describe()


def test_orphans_are_listed_most_expensive_first(broker, users, clock, cloud):
    """That is the order the conversations should happen in."""
    cloud.leak(job_id="i" * 32, user_id="cheap", gpu_type="t4")
    cloud.leak(job_id="j" * 32, user_id="expensive", gpu_type="a100")
    clock.advance(hours=4)

    assert [orphan.user_id for orphan in broker.reap().orphans] == ["expensive", "cheap"]


def test_the_report_says_who_to_go_and_ask(broker, users, clock, cloud):
    cloud.leak(job_id="k" * 32, user_id="ana", gpu_type="t4")
    cloud.leak(job_id="l" * 32, user_id="bo", gpu_type="a100")
    clock.advance(hours=2)

    by_user = broker.reap().by_user
    assert list(by_user) == ["bo", "ana"], "not sorted by cost"
    assert by_user["bo"] > by_user["ana"]


def test_reap_never_terminates_anything(broker, users, cloud):
    """Report, do not terminate, until somebody has watched it work."""
    handle = cloud.leak(job_id="m" * 32, user_id="ana")

    for _ in range(3):
        assert broker.reap().orphans, "the orphan disappeared between reports"
    assert any(resource.handle == handle for resource in cloud.list_resources())


def test_a_running_job_on_the_machine_it_should_be_on_is_not_an_orphan(broker, users, clock):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()

    assert broker.status(job.job_id).state is JobState.RUNNING
    assert broker.reap().orphans == ()


def test_a_queued_job_holds_no_machine_and_reaps_nothing(broker, users):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=1)
    report = broker.reap()
    assert report.scanned == 0
    assert report.orphans == ()


def test_an_unknown_gpu_type_is_still_reported(broker, users, cloud, clock):
    """An instance type nobody configured is exactly the kind of thing reap
    should still be able to tell you about, even if it cannot price it."""
    cloud.capacity["h100"] = 1
    cloud.leak(job_id="n" * 32, user_id="eo", gpu_type="h100")
    clock.advance(hours=3)

    orphan = broker.reap().orphans[0]
    assert orphan.gpu_type == "h100"
    assert orphan.burned == Decimal("0")
