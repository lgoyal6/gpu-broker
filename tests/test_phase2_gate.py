"""Phase 2 gate.

Stated in the build prompt as:

    Two jobs from different users run concurrently on one GPU under MPS without
    either exceeding its memory limit, and killing one does not disturb the
    other.

**What these tests prove, and what they cannot.** They prove the broker sets a
distinct per-client MPS memory limit for each job, that the simulated device
honours each client's own limit independently, and that removing one client
leaves the other untouched. They run against a simulated GPU host, so they say
nothing about how a real A6000 behaves with two concurrent training jobs -- not
the memory enforcement, not the throughput cost of MPS against exclusive access.
Those are measurements, they have to be taken on the actual card, and Phase 7 is
where they get published whether or not they flatter the design.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from conftest import local_job
from gpu_broker.backends.base import BackendStatus
from gpu_broker.broker import Broker
from gpu_broker.config import load_config
from gpu_broker.money import Currency
from gpu_broker.states import JobState


@pytest.fixture
def two_jobs(lab_backend, gpu_host):
    """One GPU, two users, both running."""
    ana = local_job("ana1", "ana")
    bo = local_job("bo1", "bo")
    handles = {
        "ana": lab_backend.launch(ana).handle,
        "bo": lab_backend.launch(bo).handle,
    }
    return {"ana": ana, "bo": bo, "handles": handles}


# ------------------------------------------------------- concurrent, one GPU


def test_two_users_run_concurrently_on_one_gpu(lab_backend, gpu_host, two_jobs):
    units = gpu_host.active_units()
    assert len(units) == 2
    assert {unit.user for unit in units} == {"ana", "bo"}
    assert {unit.gpu for unit in units} == {0}, "they did not land on the same card"

    for handle in two_jobs["handles"].values():
        assert lab_backend.poll(handle).status is BackendStatus.RUNNING


def test_they_are_MPS_clients_of_the_same_daemon(gpu_host, two_jobs):
    """Concurrent kernels from multiple processes is the whole reason MPS is
    here. The A6000 has no MIG, so the alternative is time-slicing."""
    pipes = {unit.env["CUDA_MPS_PIPE_DIRECTORY"] for unit in gpu_host.active_units()}
    assert len(pipes) == 1
    assert gpu_host.mps_running
    assert gpu_host.mps_starts == 1, "each job started its own daemon"


# ------------------------------------------------ neither exceeds its limit


def test_each_job_gets_its_own_memory_limit(gpu_host, two_jobs):
    limits = {unit.user: unit.gpu_memory_limit_mb for unit in gpu_host.active_units()}
    assert limits == {"ana": 16_384, "bo": 16_384}


def test_a_job_can_use_everything_up_to_its_limit(gpu_host, two_jobs):
    assert gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, 16_384)


def test_a_job_cannot_exceed_its_limit(gpu_host, two_jobs):
    assert gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, 16_000)
    assert not gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, 1_000), (
        "the client got past its own memory limit"
    )


def test_neither_job_can_take_the_whole_card(gpu_host, two_jobs):
    """A6000 has 48GB. Without per-client limits one job takes it all and the
    other fails to start a context."""
    total = gpu_host.gpus[0][2]
    assert not gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, total)
    assert not gpu_host.allocate_gpu_memory(two_jobs["bo"].job_id, total)


def test_one_job_filling_its_limit_leaves_the_other_untouched(gpu_host, two_jobs):
    """The point of a per-client limit rather than a daemon-wide default: what
    ana does with her allocation is irrelevant to bo."""
    assert gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, 16_384)
    assert not gpu_host.allocate_gpu_memory(two_jobs["ana"].job_id, 1)

    assert gpu_host.allocate_gpu_memory(two_jobs["bo"].job_id, 16_384), (
        "bo was refused memory because ana had used hers"
    )


def test_host_ram_is_capped_separately_from_gpu_memory(gpu_host, two_jobs):
    """MPS caps GPU memory and nothing else. A dataloader can still OOM the
    machine and take the other job with it, which is what cgroup v2 is for."""
    for unit in gpu_host.active_units():
        assert unit.memory_max_mb == 32_768
        assert unit.cpu_quota_percent == 400


# --------------------------------------------- killing one leaves the other


def test_killing_one_job_does_not_disturb_the_other(lab_backend, gpu_host, two_jobs):
    lab_backend.terminate(two_jobs["handles"]["ana"], "cancelled by ana")

    assert lab_backend.poll(two_jobs["handles"]["ana"]).status is BackendStatus.FAILED
    assert lab_backend.poll(two_jobs["handles"]["bo"]).status is BackendStatus.RUNNING
    assert gpu_host.unit_for_job(two_jobs["bo"].job_id).active


def test_the_survivor_keeps_its_memory_and_its_limit(lab_backend, gpu_host, two_jobs):
    assert gpu_host.allocate_gpu_memory(two_jobs["bo"].job_id, 8_000)
    lab_backend.terminate(two_jobs["handles"]["ana"], "cancelled")

    survivor = gpu_host.unit_for_job(two_jobs["bo"].job_id)
    assert survivor.gpu_memory_used_mb == 8_000
    assert gpu_host.allocate_gpu_memory(two_jobs["bo"].job_id, 8_384)


def test_the_survivor_still_finishes_normally(lab_backend, gpu_host, two_jobs):
    lab_backend.terminate(two_jobs["handles"]["ana"], "cancelled")
    gpu_host.finish(two_jobs["bo"].job_id, 0)

    observation = lab_backend.poll(two_jobs["handles"]["bo"])
    assert observation.status is BackendStatus.COMPLETED
    assert observation.exit_code == 0


def test_one_job_being_oom_killed_does_not_take_the_other_with_it(
    lab_backend, gpu_host, two_jobs
):
    gpu_host.oom(two_jobs["ana"].job_id)

    assert "host memory limit" in lab_backend.poll(two_jobs["handles"]["ana"]).detail
    assert lab_backend.poll(two_jobs["handles"]["bo"]).status is BackendStatus.RUNNING


def test_killing_one_frees_exactly_one_slot(lab_backend, gpu_host, two_jobs):
    assert lab_backend.free_slots("a6000") == 0
    lab_backend.terminate(two_jobs["handles"]["ana"], "cancelled")
    assert lab_backend.free_slots("a6000") == 1


# ------------------------------------------------- through the whole broker


@pytest.fixture
def lab_broker(tmp_path: Path, clock, local_config, transport):
    """A broker whose only capacity is the lab machine."""
    from gpu_broker.backends.local import LocalBackend

    state_dir = tmp_path / "broker"
    config = load_config(state_dir)
    backend = LocalBackend(clock=clock, config=local_config, transport=transport)
    broker = Broker.open(state_dir, clock=clock, backends=[backend], config=config)
    for name in ("ana", "bo"):
        broker.add_user(name)
    yield broker
    broker.store.conn.close()


def test_two_users_get_the_lab_gpu_at_once_through_the_queue(lab_broker, gpu_host, clock):
    first = lab_broker.submit(user_id="ana", command="python a.py", gpu_type="a6000", hours=1).job
    clock.advance(minutes=1)
    second = lab_broker.submit(user_id="bo", command="python b.py", gpu_type="a6000", hours=1).job

    report = lab_broker.tick()
    assert set(report.dispatched) == {first.job_id, second.job_id}
    assert {unit.user for unit in gpu_host.active_units()} == {"ana", "bo"}


def test_the_lab_bills_gpu_hours_and_never_dollars(lab_broker, gpu_host, clock):
    job = lab_broker.submit(user_id="ana", command="python a.py", gpu_type="a6000", hours=2).job
    lab_broker.tick()
    clock.advance(hours=1)
    lab_broker.tick()

    balances = lab_broker.budgets("ana")
    assert balances[Currency.GPU_HOUR].spent > Decimal("0")
    assert balances[Currency.USD].spent == Decimal("0.00")
    assert balances[Currency.USD].held == Decimal("0.00")
    assert lab_broker.pool()[Currency.USD].spent == Decimal("0.00")
    assert job.currency is Currency.GPU_HOUR


def test_a_third_job_waits_rather_than_oversubscribing_the_card(lab_broker, gpu_host, clock):
    for name in ("ana", "bo"):
        lab_broker.submit(user_id=name, command="x", gpu_type="a6000", hours=1)
        clock.advance(minutes=1)
    lab_broker.tick()

    lab_broker.add_user("cy")
    third = lab_broker.submit(user_id="cy", command="x", gpu_type="a6000", hours=1).job
    report = lab_broker.tick()

    assert third.job_id in report.blocked_on_capacity
    assert lab_broker.status(third.job_id).state is JobState.QUEUED
    assert len(gpu_host.active_units()) == 2


def test_a_third_job_runs_as_soon_as_a_slot_frees(lab_broker, gpu_host, clock):
    jobs = []
    for name in ("ana", "bo"):
        jobs.append(lab_broker.submit(user_id=name, command="x", gpu_type="a6000", hours=1).job)
        clock.advance(minutes=1)
    lab_broker.tick()
    lab_broker.add_user("cy")
    third = lab_broker.submit(user_id="cy", command="x", gpu_type="a6000", hours=1).job

    gpu_host.finish(jobs[0].job_id, 0)
    clock.advance(minutes=5)
    lab_broker.tick()

    assert lab_broker.status(third.job_id).state is not JobState.QUEUED


def test_a_drained_host_makes_jobs_wait_it_does_not_fail_them(lab_broker, gpu_host, clock):
    """The gate for health checks: a host that fails to report is drained, not
    silently retried, and the queue survives it."""
    gpu_host.nvidia_smi_works = False
    job = lab_broker.submit(user_id="ana", command="x", gpu_type="a6000", hours=1).job
    clock.advance(seconds=200)

    report = lab_broker.tick()
    assert job.job_id in report.blocked_on_capacity
    assert lab_broker.status(job.job_id).state is JobState.QUEUED

    gpu_host.nvidia_smi_works = True
    clock.advance(seconds=200)
    lab_broker.tick()
    assert lab_broker.status(job.job_id).state is not JobState.QUEUED


def test_reconcile_is_clean_while_two_jobs_share_the_card(lab_broker, gpu_host, clock):
    for name in ("ana", "bo"):
        lab_broker.submit(user_id=name, command="x", gpu_type="a6000", hours=1)
        clock.advance(minutes=1)
    lab_broker.tick()

    assert lab_broker.reconcile() == []
    assert lab_broker.reap().orphans == ()
