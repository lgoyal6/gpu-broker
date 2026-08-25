"""Phase 4 gate.

Stated in the build prompt as:

    With `FakeBackend`, inject interruption at a random point in 100 simulated
    jobs. Every job eventually completes. No job completes twice. Resumed jobs
    produce the same final state as uninterrupted ones.

The third clause is the one that needs a real invariant behind it, because
"the job finished" says nothing about whether it finished *correctly*. So the
simulated job does `total_steps` units of work and accumulates
`sum(range(step))` as it goes. That number comes out right only if every step
ran exactly once across every attempt: a resume that starts too early
double-counts, one that starts too late skips, and both produce a job that
completes with the wrong answer.
"""

from __future__ import annotations

import random
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.checkpoint import LocalCheckpointStore
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.states import ACTIVE, JobState

TOTAL_STEPS = 1_000
CHECKPOINT_EVERY = 100
EXPECTED = sum(range(TOTAL_STEPS))


@pytest.fixture
def rig(tmp_path: Path, clock):
    """A broker with a checkpoint store and plenty of capacity."""
    state_dir = tmp_path / "broker"
    checkpoints = LocalCheckpointStore(tmp_path / "checkpoints")
    spot = FakeBackend(
        "spot",
        clock=clock,
        checkpoints=checkpoints,
        currency=Currency.USD,
        capacity={"a10g": 10},
        startup_seconds=30.0,
        tier="spot",
        state_path=state_dir / "spot.json",
    )
    # Spot only makes sense with something behind it. A job repeatedly preempted
    # without saving anything gets moved here, which is the whole policy.
    ondemand = FakeBackend(
        "ondemand",
        clock=clock,
        checkpoints=checkpoints,
        currency=Currency.USD,
        capacity={"a10g": 10},
        startup_seconds=60.0,
        tier="ondemand",
        state_path=state_dir / "ondemand.json",
    )
    config = replace(
        load_config(state_dir),
        pool_budget_usd=Decimal("100000"),
        default_budget_usd=Decimal("100000"),
        max_job_usd=Decimal("1000"),
    )
    broker = Broker.open(
        state_dir, clock=clock, backends=[spot, ondemand], config=config,
        checkpoints=checkpoints,
    )
    yield {
        "broker": broker, "backend": spot, "ondemand": ondemand,
        "checkpoints": checkpoints, "clock": clock,
    }
    broker.store.conn.close()


def submit_all(rig, count: int, hours: float = 2.0) -> list[str]:
    broker, backend = rig["broker"], rig["backend"]
    ids: list[str] = []
    for index in range(count):
        user = f"member{index % 5}"
        broker.add_user(user)
        job = broker.submit(
            user_id=user, command=f"python train.py --seed {index}",
            gpu_type="a10g", hours=hours, budget="50",
        ).job
        for target in (backend, rig["ondemand"]):
            target.plan(job.job_id, run_hours=hours, total_steps=TOTAL_STEPS,
                        checkpoint_every=CHECKPOINT_EVERY)
        ids.append(job.job_id)
        rig["clock"].advance(seconds=1)
    return ids


def drive(rig, job_ids: list[str], interrupt_chance: float, seed: int, max_ticks: int = 4000):
    """Run the scheduler, pulling capacity out from under jobs at random."""
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    dice = random.Random(seed)
    interruptions = 0

    for _ in range(max_ticks):
        broker.tick()
        if not broker.store.queued_jobs() and not broker.store.active_jobs():
            return interruptions
        if interrupt_chance:
            for job in broker.store.list_jobs(states=ACTIVE):
                # Only spot gets pulled. On-demand capacity is not taken back,
                # which is exactly why a job that cannot checkpoint ends up there.
                if job.backend == "spot" and job.backend_handle and dice.random() < interrupt_chance:
                    backend.interrupt(job.backend_handle)
                    interruptions += 1
        clock.advance(minutes=5)

    pytest.fail(
        f"the queue never drained: "
        f"{len(broker.store.queued_jobs())} queued, "
        f"{len(broker.store.active_jobs())} active"
    )


def completions(broker, job_id: str) -> int:
    return sum(
        1 for _, to_state, _, _ in broker.store.history(job_id)
        if to_state == JobState.COMPLETED
    )


# ------------------------------------------------------------------ the gate


@pytest.mark.timeout(180)
def test_100_jobs_with_random_interruptions(rig):
    broker, backend = rig["broker"], rig["backend"]
    job_ids = submit_all(rig, 100)

    interruptions = drive(rig, job_ids, interrupt_chance=0.15, seed=20260115)
    assert interruptions > 50, f"only {interruptions} interruptions; not much of a test"

    # --- every job eventually completes ---
    states = {job_id: broker.status(job_id).state for job_id in job_ids}
    unfinished = {j: s for j, s in states.items() if s is not JobState.COMPLETED}
    assert not unfinished, f"{len(unfinished)} jobs did not complete: {list(unfinished.values())[:5]}"

    # --- no job completes twice ---
    for job_id in job_ids:
        assert completions(broker, job_id) == 1, (
            f"{job_id[:8]} entered COMPLETED {completions(broker, job_id)} times"
        )

    # --- resumed jobs produce the same final state as uninterrupted ones ---
    for job_id in job_ids:
        assert backend.final_value(job_id) == EXPECTED, (
            f"{job_id[:8]} finished with the wrong answer: "
            f"{backend.final_value(job_id)} instead of {EXPECTED}. "
            f"It was preempted {broker.status(job_id).preemptions} times"
        )

    # And the run was genuinely disturbed, not quietly interruption-free.
    preempted = [j for j in job_ids if broker.status(j).preemptions > 0]
    assert len(preempted) > 30, f"only {len(preempted)} of 100 jobs were ever preempted"


@pytest.mark.timeout(120)
def test_an_uninterrupted_run_produces_the_same_answer(rig):
    """The control. Without this the gate above only proves the jobs agree with
    each other, not that they are right."""
    job_ids = submit_all(rig, 10)
    drive(rig, job_ids, interrupt_chance=0.0, seed=1)

    for job_id in job_ids:
        assert rig["broker"].status(job_id).preemptions == 0
        assert rig["backend"].final_value(job_id) == EXPECTED


@pytest.mark.timeout(120)
def test_a_job_interrupted_over_and_over_still_finishes_correctly(rig):
    """The pathological case: interrupted more often than it can checkpoint."""
    job_ids = submit_all(rig, 5, hours=4.0)
    drive(rig, job_ids, interrupt_chance=0.6, seed=99)

    broker, backend = rig["broker"], rig["backend"]
    for job_id in job_ids:
        assert broker.status(job_id).state is JobState.COMPLETED
        assert backend.final_value(job_id) == EXPECTED
    assert max(broker.status(j).preemptions for j in job_ids) >= 3


# ------------------------------------------------- what the gate implies


def test_a_preempted_job_is_not_a_failed_one(rig):
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    job_id = submit_all(rig, 1)[0]
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()

    backend.interrupt(broker.status(job_id).backend_handle)
    broker.tick()

    job = broker.status(job_id)
    assert not job.is_terminal, "a preemption ended the job"
    assert job.preemptions == 1
    # The same tick that requeued it may also have redispatched it, which is the
    # point: a preemption should not cost a scheduling round.
    seen = [to_state for _, to_state, _, _ in broker.store.history(job_id)]
    assert JobState.PREEMPTED in seen and JobState.QUEUED in seen[1:]


def test_a_preempted_job_keeps_its_budget_reservation(rig):
    """It is not finished, it is between attempts. Releasing the hold would let
    somebody else commit money this job still needs."""
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    job_id = submit_all(rig, 1)[0]
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()
    held_before = broker.budgets(broker.status(job_id).user_id)[Currency.USD].held

    backend.interrupt(broker.status(job_id).backend_handle)
    broker.tick()

    assert broker.budgets(broker.status(job_id).user_id)[Currency.USD].held == held_before


def test_time_already_burned_stays_billed(rig):
    """The instance really did run for half an hour. Refunding that would make
    spot look free and the dollars-per-useful-hour number a lie."""
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    job_id = submit_all(rig, 1)[0]
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()
    spent = broker.job_spend(job_id)
    assert spent > Decimal("0")

    backend.interrupt(broker.status(job_id).backend_handle)
    broker.tick()
    assert broker.job_spend(job_id) >= spent


def test_a_resumed_job_says_where_it_is_resuming_from(rig):
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    job_id = submit_all(rig, 1)[0]
    broker.tick()
    clock.advance(minutes=40)
    broker.tick()
    backend.interrupt(broker.status(job_id).backend_handle)
    broker.tick()

    job = broker.status(job_id)
    assert job.resumable, "nothing was saved before the interruption"
    lines = [line for _, _, _, line in broker.logs(job_id)]
    assert any("resuming from step" in line for line in lines), lines


def test_a_job_that_never_checkpoints_is_moved_off_spot(rig):
    """The policy: everything tries spot, and a job repeatedly preempted with
    nothing saved stops paying to redo the same hour."""
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    broker.add_user("cy")
    job = broker.submit(
        user_id="cy", command="python unsaveable.py", gpu_type="a10g", hours=2, budget="50"
    ).job
    # No checkpointing at all.
    for target in (backend, rig["ondemand"]):
        target.plan(job.job_id, run_hours=2.0, total_steps=0, checkpoint_every=0)

    for _ in range(broker.config.spot_max_preemptions_without_progress):
        broker.tick()
        clock.advance(minutes=5)
        broker.tick()
        backend.interrupt(broker.status(job.job_id).backend_handle)
        broker.tick()

    pinned = broker.status(job.job_id)
    assert pinned.pinned_tier == "ondemand"
    assert pinned.preemptions >= broker.config.spot_max_preemptions_without_progress

    notices = [n.kind for n in broker.store.notifications_for_job(job.job_id)]
    assert "pinned-ondemand" in notices


def test_a_job_that_does_checkpoint_stays_on_spot(rig):
    """Progress is the whole justification for the cheap tier. Losing an hour
    you already saved is not the same as losing an hour."""
    broker, backend, clock = rig["broker"], rig["backend"], rig["clock"]
    job_id = submit_all(rig, 1, hours=4.0)[0]

    for _ in range(4):
        broker.tick()
        clock.advance(minutes=40)
        broker.tick()
        handle = broker.status(job_id).backend_handle
        if handle:
            backend.interrupt(handle)
        broker.tick()

    job = broker.status(job_id)
    assert job.preemptions >= 3
    assert job.pinned_tier is None, "a job that saves its work was moved off spot"
