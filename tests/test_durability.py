"""The thing this project most cares about being correct.

The broker survives its own crash without losing the queue, the ledger, or the
user-to-resource mapping, and on restart reconciles real backend state against
its records and reports drift.

Two levels of evidence here. The in-process tests check exact values across a
clean reopen. The SIGKILL test checks that an unclean death of a real process,
at an arbitrary point inside the write path, leaves a database that still holds
together.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from gpu_broker.backends import FakeBackend
from gpu_broker.broker import Broker
from gpu_broker.clock import ManualClock
from gpu_broker.config import load_config
from gpu_broker.money import ZERO, Currency
from gpu_broker.states import ACTIVE, TERMINAL, JobState

REPO = Path(__file__).resolve().parents[1]


def reopen(state_dir: Path, clock: ManualClock) -> Broker:
    """Open a second broker over the same files, the way a restart does."""
    config = load_config(state_dir)
    cloud = FakeBackend(
        "cloud", clock=clock, currency=Currency.USD,
        capacity={"t4": 2, "a10g": 2, "l4": 1, "a100": 1},
        startup_seconds=60.0, state_path=state_dir / "cloud.json",
    )
    lab = FakeBackend(
        "lab", clock=clock, currency=Currency.GPU_HOUR, capacity={"a6000": 2},
        startup_seconds=5.0, state_path=state_dir / "lab.json",
    )
    return Broker.open(state_dir, clock=clock, backends=[cloud, lab], config=config)


# ------------------------------------------------------------- clean restart


def test_the_queue_survives_a_restart(broker, users, state_dir, clock):
    submitted = [
        broker.submit(user_id=users[i % 5], command=f"job {i}", gpu_type="a10g", hours=1).job.job_id
        for i in range(10)
    ]
    order_before = [job.job_id for job, _ in broker.queue()]
    broker.close()

    restarted = reopen(state_dir, clock)
    assert [job.job_id for job, _ in restarted.queue()] == order_before
    assert {job.job_id for job in restarted.store.queued_jobs()} == set(submitted)
    restarted.close()


def test_the_ledger_survives_a_restart(broker, users, state_dir, clock):
    broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2, budget="6")
    broker.submit(user_id="ana", command="y", gpu_type="a6000", hours=3)
    broker.tick()
    clock.advance(minutes=30)
    broker.tick()

    before = {c: (b.spent, b.held) for c, b in broker.budgets("ana").items()}
    broker.close()

    restarted = reopen(state_dir, clock)
    after = {c: (b.spent, b.held) for c, b in restarted.budgets("ana").items()}
    assert after == before
    assert after[Currency.USD][0] > ZERO, "nothing had actually been spent yet"
    restarted.close()


def test_the_user_to_resource_mapping_survives_a_restart(broker, users, state_dir, clock):
    """Who holds which machine. Losing this is how an instance becomes an orphan."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    running = broker.status(job.job_id)
    assert running.backend and running.backend_handle
    broker.close()

    restarted = reopen(state_dir, clock)
    recovered = restarted.status(job.job_id)
    assert recovered.backend == running.backend
    assert recovered.backend_handle == running.backend_handle
    assert recovered.user_id == "ana"
    assert restarted.reconcile() == [], "a clean restart reported drift"
    restarted.close()


def test_a_job_keeps_running_across_a_restart(broker, users, state_dir, clock):
    """The broker restarting must not disturb work in flight."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2).job
    broker.tick()
    clock.advance(minutes=20)
    broker.tick()
    spend_before = broker.job_spend(job.job_id)
    broker.close()

    restarted = reopen(state_dir, clock)
    restarted.run_until_idle(step_seconds=300)
    final = restarted.status(job.job_id)

    assert final.state is JobState.COMPLETED
    assert restarted.job_spend(job.job_id) > spend_before, "billing stopped at the restart"
    restarted.close()


def test_billing_does_not_double_count_across_a_restart(broker, users, state_dir, clock):
    """Accrual is derived from elapsed time and the ledger, not from a counter,
    so reopening cannot re-charge time that was already settled."""
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=2, budget="4").job
    broker.tick()
    clock.advance(hours=1)
    broker.tick()
    once = broker.job_spend(job.job_id)
    broker.close()

    restarted = reopen(state_dir, clock)
    restarted.tick()  # same instant, no time has passed
    assert restarted.job_spend(job.job_id) == once
    restarted.close()


# --------------------------------------------------------------------- drift


def test_restart_reports_an_instance_the_broker_lost_track_of(broker, users, state_dir, clock):
    """The real shape of the failure: the broker died between the backend
    returning a handle and that handle being committed."""
    broker.close()
    restarted = reopen(state_dir, clock)
    cloud = restarted.scheduler.backend("cloud")
    cloud.leak(job_id="a" * 32, user_id="bo")

    drifts = restarted.reconcile()
    assert len(drifts) == 1
    assert drifts[0].kind == "ORPHAN"
    assert drifts[0].user_id if hasattr(drifts[0], "user_id") else True
    assert "bo" in drifts[0].detail, "the orphan is not attributable to a person"
    restarted.close()


def test_restart_reports_a_machine_that_vanished_underneath_us(broker, users, state_dir, clock):
    job = broker.submit(user_id="ana", command="x", gpu_type="a10g", hours=4).job
    broker.tick()
    handle = broker.status(job.job_id).backend_handle
    broker.scheduler.backend("cloud").orphan(handle)  # terminated outside the broker

    drifts = broker.reconcile()
    assert [d.kind for d in drifts] == ["LOST"]
    assert drifts[0].job_id == job.job_id


def test_reconcile_reports_and_does_not_terminate(broker, users, state_dir, clock):
    """Report, do not terminate, until somebody has watched it work."""
    cloud = broker.scheduler.backend("cloud")
    handle = cloud.leak(job_id="b" * 32, user_id="cy")

    assert broker.reconcile()
    assert any(r.handle == handle for r in cloud.list_resources()), "reconcile killed something"
    assert broker.reconcile(), "the drift was silently cleaned up"


def test_an_untagged_resource_is_reported_separately(broker, users):
    cloud = broker.scheduler.backend("cloud")
    handle = cloud.leak(job_id="c" * 32, user_id="di")
    cloud._resources[handle].tags = {}

    drifts = broker.reconcile()
    assert [d.kind for d in drifts] == ["UNTAGGED"]


# ------------------------------------------------------------------- SIGKILL


@pytest.mark.timeout(90)
def test_a_real_sigkill_mid_queue_loses_nothing(tmp_path):
    """Kill the process, restart, and check the database still holds together.

    Not a simulated crash: a separate Python process is SIGKILLed while it is
    actively opening transactions. Nothing gets to run a cleanup handler,
    because in a real crash nothing does.
    """
    state_dir = tmp_path / "broker"
    state_dir.mkdir(parents=True)

    process = subprocess.Popen(
        [sys.executable, str(REPO / "tests" / "crash_helper.py"), str(state_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(REPO),
    )
    try:
        deadline = time.time() + 45
        while time.time() < deadline:
            line = process.stdout.readline()
            if line.strip() == "READY":
                break
            if process.poll() is not None:
                pytest.fail(f"helper died early:\n{process.stderr.read()}")
        else:
            pytest.fail("helper never became ready")

        time.sleep(0.35)  # let it get well into the churn loop
        assert process.poll() is None, "helper exited on its own"
        os.kill(process.pid, signal.SIGKILL)
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()

    # --- restart over the corpse ---
    clock = ManualClock()
    config = load_config(state_dir)
    cloud = FakeBackend(
        "cloud", clock=clock, currency=Currency.USD,
        capacity={"a10g": 3, "t4": 2}, state_path=state_dir / "cloud.json",
    )
    broker = Broker.open(state_dir, clock=clock, backends=[cloud], config=config)

    jobs = broker.store.list_jobs()
    assert len(jobs) >= 20, f"lost the queue: only {len(jobs)} jobs survived"
    assert len(broker.users()) == 5

    # Every surviving job is in a state the machine actually defines.
    for job in jobs:
        assert job.state in set(JobState)
        if job.state in ACTIVE:
            assert job.backend and job.backend_handle, "an active job lost its machine"

    # The ledger reconstructs, and no account is holding a negative amount.
    for user in broker.users():
        for currency in Currency:
            balance = broker.store.balance(user.user_id, currency)
            assert balance.held >= ZERO, f"{user.user_id} holds {balance.held}"
            assert balance.spent >= ZERO
            assert balance.committed <= balance.budget, (
                f"{user.user_id} is committed past their budget after the crash"
            )

    # No half-written transition: every non-terminal job that holds budget has a
    # RESERVE, and every terminal job has released whatever it did not spend.
    for job in jobs:
        outstanding = broker.store.job_holding(job.job_id, job.currency)
        if job.state in TERMINAL:
            assert outstanding == ZERO, f"{job.short_id} ({job.state}) leaked {outstanding}"
        elif job.state is not JobState.REFUSED:
            assert outstanding > ZERO or broker.store.job_spend(job.job_id) > ZERO

    # And it is still usable: the queue drains.
    broker.run_until_idle(step_seconds=600)
    assert not broker.store.queued_jobs()
    assert not broker.store.active_jobs()
    broker.close()


@pytest.mark.timeout(90)
def test_the_backend_keeps_its_machines_while_the_broker_is_dead(tmp_path):
    """A real cloud does not stop your instances because your scheduler died.
    The fake must not either, or reconciliation is never actually exercised.
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
        time.sleep(0.35)
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()

    clock = ManualClock()
    config = load_config(state_dir)
    cloud = FakeBackend(
        "cloud", clock=clock, currency=Currency.USD,
        capacity={"a10g": 3, "t4": 2}, state_path=state_dir / "cloud.json",
    )
    assert cloud.list_resources(), "the backend forgot everything when the broker died"

    broker = Broker.open(state_dir, clock=clock, backends=[cloud], config=config)
    for drift in broker.reconcile():
        # Whatever drift exists must name a person. An unattributable orphan is
        # one nobody can be asked about.
        assert drift.job_id or drift.kind == "UNTAGGED"
    broker.close()
