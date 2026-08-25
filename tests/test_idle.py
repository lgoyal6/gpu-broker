"""Idle detection and reclaim.

The failure this exists for: somebody launches an instance, the training script
dies at 2am, and the machine bills until Thursday. Nobody is being careless;
nobody is looking.

Three rules, and two of them are restrictions:

  Nothing is reclaimed without a recorded notification.
  The samples that justified a reclaim survive the job.
  Reclaim reports before it acts, and is off until somebody has watched it.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from gpu_broker.idle import IDLE_NOTIFICATION, evaluate, reclaimable
from gpu_broker.states import JobState


def run_for(broker, minutes: int, clock, every: int = 1) -> None:
    """Tick minute by minute, the way a scheduler loop would."""
    for _ in range(minutes // every):
        clock.advance(minutes=every)
        broker.tick()


@pytest.fixture
def running(broker, users, clock, cloud):
    """One busy job and one whose script died silently."""
    busy = broker.submit(user_id="ana", command="python real.py", gpu_type="a10g", hours=4).job
    dead = broker.submit(user_id="bo", command="python dead.py", gpu_type="a10g", hours=4).job
    broker.tick()
    cloud.set_utilization(dead.job_id, 0.0)
    return {"busy": busy, "dead": dead}


# ---------------------------------------------------------------- detecting


def test_a_working_job_is_never_idle(broker, running, clock):
    run_for(broker, 30, clock)
    assert [v.job.user_id for v in broker.idle_jobs()] == ["bo"]


def test_a_dead_script_is_found(broker, running, clock):
    run_for(broker, 15, clock)
    verdict = evaluate(broker.store, broker.status(running["dead"].job_id), broker.config, broker.clock)
    assert verdict.idle
    assert verdict.peak_percent == 0.0
    assert "never went above 5%" in verdict.reason


def test_nothing_is_decided_before_there_are_enough_samples(broker, running, clock):
    """One reading through a checkpoint save is not an idle job."""
    run_for(broker, 2, clock)
    verdict = evaluate(broker.store, broker.status(running["dead"].job_id), broker.config, broker.clock)
    assert not verdict.idle
    assert "before anything is decided" in verdict.reason


def test_a_brief_dip_does_not_count(broker, users, clock, cloud):
    """Utilization drops to zero every time a job writes a checkpoint."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    run_for(broker, 6, clock)
    cloud.set_utilization(job.job_id, 0.0)
    run_for(broker, 3, clock)  # a short dip inside the 10-minute window

    verdict = evaluate(broker.store, broker.status(job.job_id), broker.config, broker.clock)
    assert not verdict.idle
    assert "peaked at" in verdict.reason


def test_the_verdict_says_what_it_has_cost(broker, running, clock):
    run_for(broker, 20, clock)
    verdict = next(v for v in broker.idle_jobs() if v.job.user_id == "bo")
    assert verdict.wasted > Decimal("0")
    assert "spent doing nothing" in verdict.describe(clock.now())


def test_free_capacity_wastes_gpu_hours_not_dollars(broker, users, clock, lab):
    job = broker.submit(user_id="ana", command="x", gpu_type="a6000", hours=4).job
    broker.tick()
    lab.set_utilization(job.job_id, 0.0)
    run_for(broker, 20, clock)

    verdict = next(v for v in broker.idle_jobs() if v.job.job_id == job.job_id)
    assert "gpu-hr" in verdict.describe(clock.now())
    assert "$" not in verdict.describe(clock.now())


def test_idle_jobs_are_listed_most_expensive_first(broker, users, clock, cloud):
    cheap = broker.submit(user_id="ana", command="x", gpu_type="t4", hours=4).job
    clock.advance(minutes=1)
    dear = broker.submit(user_id="bo", command="x", gpu_type="a100", hours=4).job
    broker.tick()
    cloud.set_utilization(cheap.job_id, 0.0)
    cloud.set_utilization(dear.job_id, 0.0)
    run_for(broker, 20, clock)

    assert [v.job.user_id for v in broker.idle_jobs()] == ["bo", "ana"]


# ------------------------------------------------------------- notification


def test_the_owner_is_told_and_it_is_recorded(broker, running, clock):
    run_for(broker, 15, clock)
    notices = broker.store.notifications_for_job(running["dead"].job_id, IDLE_NOTIFICATION)

    assert len(notices) == 1
    assert notices[0].user_id == "bo"
    assert "used no GPU" in notices[0].message


def test_the_notice_says_what_to_do_about_it(broker, running, clock):
    run_for(broker, 15, clock)
    message = broker.store.notifications_for_job(running["dead"].job_id)[0].message
    assert "If it is still working, ignore this" in message
    assert "gpu status" in message


def test_the_owner_is_told_once_not_every_tick(broker, running, clock):
    run_for(broker, 60, clock)
    notices = broker.store.notifications_for_job(running["dead"].job_id, IDLE_NOTIFICATION)
    assert len(notices) == 1, f"sent {len(notices)} identical warnings"


def test_a_busy_job_is_never_warned(broker, running, clock):
    run_for(broker, 60, clock)
    assert broker.store.notifications_for_job(running["busy"].job_id) == []


# ---------------------------------------------------------------- reclaim


def test_nothing_is_reclaimable_before_the_owner_is_told(broker, running, clock):
    run_for(broker, 15, clock)
    verdict = next(v for v in broker.idle_jobs() if v.job.user_id == "bo")
    stripped = replace(verdict, notified_at=None, grace_ends_at=None)
    allowed, why = reclaimable(stripped, broker.clock)
    assert not allowed
    assert "nobody has been told" in why


def test_the_grace_period_is_honoured(broker, running, clock):
    run_for(broker, 15, clock)
    report = broker.reclaim()
    assert report.ready == ()
    assert any("grace left" in why for _, why in report.held)


def test_after_the_grace_period_it_is_ready(broker, running, clock):
    run_for(broker, 40, clock)
    report = broker.reclaim()
    assert len(report.ready) == 1
    assert report.ready[0].job.user_id == "bo"


def test_report_only_kills_nothing(broker, running, clock, cloud):
    """Off until somebody has watched it be right."""
    run_for(broker, 40, clock)
    report = broker.reclaim()

    assert not report.enabled
    assert report.reclaimed == ()
    assert len(report.would_reclaim) == 1
    assert broker.status(running["dead"].job_id).state is JobState.RUNNING
    assert len(cloud.list_resources()) == 2


def test_forcing_it_reclaims_only_the_idle_job(broker, running, clock, cloud):
    run_for(broker, 40, clock)
    report = broker.reclaim(force=True)

    assert len(report.reclaimed) == 1
    assert broker.status(running["dead"].job_id).state is JobState.RECLAIMED
    assert broker.status(running["busy"].job_id).state is JobState.RUNNING
    assert [r.tags["broker-user"] for r in cloud.list_resources()] == ["ana"]


def test_enabling_it_in_config_reclaims(broker, users, state_dir, clock, cloud, lab):
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    keen = Broker.open(
        state_dir, clock=clock, backends=[cloud, lab],
        config=replace(load_config(state_dir), reclaim_enabled=True),
    )
    for name in users:
        keen.add_user(name)
    job = keen.submit(user_id="bo", command="python dead.py", gpu_type="a10g", hours=4).job
    keen.tick()
    cloud.set_utilization(job.job_id, 0.0)
    run_for(keen, 40, clock)

    assert keen.reclaim().reclaimed == (job.job_id,)
    assert keen.status(job.job_id).state is JobState.RECLAIMED
    keen.close()


def test_a_reclaim_records_the_evidence_where_the_owner_can_see_it(broker, running, clock):
    run_for(broker, 40, clock)
    broker.reclaim(force=True)

    notices = broker.store.notifications_for_job(running["dead"].job_id, "reclaimed")
    assert len(notices) == 1
    assert "gpu   0.0%" in notices[0].message
    assert "samples that justified it" in notices[0].message


def test_the_samples_survive_the_job(broker, running, clock):
    """Deleting them with the job would make the decision unauditable exactly
    when somebody wants to audit it."""
    run_for(broker, 40, clock)
    broker.reclaim(force=True)

    samples = broker.store.samples_for(running["dead"].job_id)
    assert len(samples) > 10
    assert all(sample.gpu_percent == 0.0 for sample in samples)


def test_a_reclaimed_job_releases_its_budget(broker, running, clock):
    from gpu_broker.money import Currency

    run_for(broker, 40, clock)
    broker.reclaim(force=True)
    assert broker.budgets("bo")[Currency.USD].held == Decimal("0.00")


def test_reclaim_is_a_terminal_state_with_a_reason(broker, running, clock):
    run_for(broker, 40, clock)
    broker.reclaim(force=True)

    reasons = [r for _, _, _, r in broker.store.history(running["dead"].job_id) if r]
    assert any("idle:" in reason for reason in reasons)
    assert broker.status(running["dead"].job_id).is_terminal


# --------------------------------------------------------------- sampling


def test_samples_are_taken_on_an_interval_not_every_tick(broker, users, clock, cloud):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    for _ in range(10):
        clock.advance(seconds=5)
        broker.tick()
    assert len(broker.store.samples_for(job.job_id)) <= 2


def test_samples_are_not_duplicated_across_a_restart(broker, users, state_dir, clock, cloud, lab):
    """A backend that re-reads its source from the top after a restart would
    write every sample twice, and a duplicated run of zeroes is exactly what
    triggers a reclaim."""
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    run_for(broker, 10, clock)
    before = broker.store.samples_for(job.job_id)
    broker.close()

    restarted = Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=load_config(state_dir))
    restarted.tick()
    after = restarted.store.samples_for(job.job_id)

    stamps = [sample.at for sample in after]
    assert len(stamps) == len(set(stamps)), "samples were written twice"
    assert len(after) >= len(before)
    restarted.close()


def test_a_backend_that_cannot_attribute_usage_records_nothing(broker, users, clock, cloud, monkeypatch):
    """Recording 0% for a failed lookup is indistinguishable from an idle job
    and would get somebody reclaimed for it."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    monkeypatch.setattr(cloud, "sample_utilization", lambda handle: [])
    run_for(broker, 20, clock)

    assert broker.store.samples_for(job.job_id) == []
    assert broker.idle_jobs() == []


def test_idle_time_is_measured_from_when_it_stopped_working(broker, running, clock):
    """Regression: this used to be measured from the start of the ten-minute
    detection window, so a job that had burned an afternoon reported "idle 0.2h,
    $0.15 wasted" -- which is exactly the number nobody acts on."""
    run_for(broker, 90, clock)
    verdict = next(v for v in broker.idle_jobs() if v.job.user_id == "bo")

    assert verdict.idle_hours(clock.now()) > 1.0, "idle time collapsed to the window"
    assert verdict.wasted > Decimal("1.00")


def test_idle_time_starts_after_the_last_time_it_did_work(broker, users, clock, cloud):
    """A job that worked, then stopped, is idle from when it stopped -- not from
    when it started."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=6).job
    broker.tick()
    run_for(broker, 30, clock)          # half an hour of real work
    cloud.set_utilization(job.job_id, 0.0)
    run_for(broker, 30, clock)          # then half an hour of nothing

    verdict = next(v for v in broker.idle_jobs() if v.job.job_id == job.job_id)
    hours = verdict.idle_hours(clock.now())
    assert 0.4 < hours < 0.6, f"idle for {hours:.2f}h; should be about half an hour"
