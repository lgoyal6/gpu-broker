"""Pilot mode: real work on the lab GPU, through the broker, before the club is on it.

These run the real `LocalBackend` against an in-process SSH server, so the
command strings, the systemd units and the nvidia-smi parsing are the ones that
would reach a real machine. What is simulated is the machine, not the broker.
"""

from __future__ import annotations

from typer.testing import CliRunner

from gpu_broker.cli import app
from gpu_broker.money import Currency
from gpu_broker.states import ACTIVE, JobState

runner = CliRunner()


def submit_pilot(broker, user_id="laksh", hours=2.0, command="python train.py"):
    from gpu_broker.money import money

    broker.add_user(user_id, budget_gpu_hours=money("100"))
    result = broker.submit(
        user_id=user_id, command=command, gpu_type="a6000", hours=hours, origin="pilot"
    )
    assert result.accepted, getattr(result.refusal, "reason", None)
    return broker.store.pin_tier(result.job, "local", "pilot: local capacity only")


# ------------------------------------------------------------ what it is not


def test_a_pilot_job_costs_gpu_hours_not_dollars(pilot_broker):
    job = submit_pilot(pilot_broker)
    assert job.currency is Currency.GPU_HOUR
    assert pilot_broker.store.balance("laksh", Currency.USD).spent == 0


def test_a_pilot_job_is_pinned_to_local_so_it_cannot_fall_back_to_the_cloud(pilot_broker):
    job = submit_pilot(pilot_broker)
    assert job.pinned_tier == "local"


def test_the_cli_refuses_a_cloud_gpu_under_pilot(tmp_path):
    result = runner.invoke(
        app,
        ["submit", "--pilot", "--gpu", "a10g", "--state-dir", str(tmp_path), "--", "python", "x.py"],
        env={"GPU_BROKER_USER": "laksh"},
    )
    assert result.exit_code != 0
    assert "cloud capacity" in result.output


def test_the_cli_tags_the_job_and_keeps_it_local(tmp_path):
    result = runner.invoke(
        app,
        ["submit", "--pilot", "--hours", "1", "--state-dir", str(tmp_path), "--", "python", "x.py"],
        env={"GPU_BROKER_USER": "laksh"},
    )
    assert result.exit_code == 0, result.output

    from gpu_broker.broker import Broker
    from gpu_broker.clock import SystemClock

    with Broker.open(tmp_path, clock=SystemClock(), backends=[]) as broker:
        job = broker.store.list_jobs(limit=1)[0]
        assert job.origin == "pilot"
        assert job.pinned_tier == "local"
        assert job.gpu_type == "a6000"


# --------------------------------------------------------- end to end on the box


def test_it_queues_launches_and_runs_on_the_real_local_backend(pilot_broker, gpu_host, clock):
    job = submit_pilot(pilot_broker)

    pilot_broker.tick()
    running = pilot_broker.store.get_job(job.job_id)
    assert running.state in ACTIVE
    assert running.backend == "local"

    unit = gpu_host.unit_for_job(job.job_id)
    assert "python train.py" in unit.command
    assert unit.env["CUDA_MPS_PINNED_DEVICE_MEM_LIMIT"]

    gpu_host.emit(job.job_id, "epoch 1 loss 2.3")
    pilot_broker.tick()
    assert any("epoch 1" in line for _, _, _, line in pilot_broker.logs(job.job_id))

    gpu_host.finish(job.job_id, exit_code=0)
    clock.sleep(60)
    pilot_broker.tick()
    assert pilot_broker.store.get_job(job.job_id).state is JobState.COMPLETED


def test_utilization_is_sampled_off_the_real_nvidia_smi_output(pilot_broker, gpu_host, clock):
    job = submit_pilot(pilot_broker)
    pilot_broker.tick()

    for _ in range(6):
        clock.sleep(60)
        pilot_broker.tick()

    samples = pilot_broker.store.samples_for(job.job_id, limit=100)
    assert samples, "a running pilot job recorded no utilization"
    assert all(0 <= sample.gpu_percent <= 100 for sample in samples)


def test_an_idle_pilot_job_is_noticed_notified_and_only_then_reclaimed(
    pilot_broker, gpu_host, clock
):
    """The whole point of the pilot: watch reclaim be right before it is loose
    on other people's jobs."""
    job = submit_pilot(pilot_broker, hours=8.0)
    pilot_broker.tick()
    # Per-process, not per-card: under MPS the card's own number cannot say
    # whose job is idle, so the probe attributes through the cgroup.
    gpu_host.set_utilization(job.job_id, 1.0)

    for _ in range(15):
        clock.sleep(60)
        pilot_broker.tick()

    verdicts = [v for v in pilot_broker.idle_jobs() if v.job.job_id == job.job_id]
    assert verdicts and verdicts[0].idle, "an idle GPU was not noticed"

    first = pilot_broker.reclaim()
    assert job.job_id not in first.reclaimed, "reclaimed before the grace period"
    assert pilot_broker.store.notifications_for_job(job.job_id), "killed without telling anyone"

    clock.sleep(16 * 60)
    for _ in range(6):
        clock.sleep(60)
        pilot_broker.tick()

    second = pilot_broker.reclaim()
    assert job.job_id in second.reclaimed
    assert pilot_broker.store.get_job(job.job_id).state is JobState.RECLAIMED
    assert pilot_broker.store.samples_for(job.job_id, limit=500), "evidence was thrown away"


def test_the_broker_can_crash_mid_job_and_pick_the_job_back_up(
    state_dir, clock, lab_backend, gpu_host
):
    """A pilot that cannot survive a restart is not a pilot, it is a demo."""
    from gpu_broker.broker import Broker
    from gpu_broker.config import load_config

    config = load_config(state_dir)

    # Deliberately not a `with`. A SIGKILL does not run cleanup, and closing
    # this broker politely would also close the SSH transport the fixture
    # shares with the second one -- which would make the machine look
    # unreachable rather than make the broker look crashed.
    doomed = Broker.open(state_dir, clock=clock, backends=[lab_backend], config=config)
    doomed.add_user("laksh")
    result = doomed.submit(
        user_id="laksh",
        command="python train.py",
        gpu_type="a6000",
        hours=2.0,
        origin="pilot",
    )
    job_id = result.job.job_id
    doomed.tick()  # launches
    clock.sleep(60)
    doomed.tick()  # observes it running
    assert doomed.store.get_job(job_id).state is JobState.RUNNING
    handle = doomed.store.get_job(job_id).backend_handle
    assert handle
    doomed.store.conn.close()  # the process is gone; the job is not

    # The machine is still running the job.
    gpu_host.emit(job_id, "epoch 2 loss 1.1")

    with Broker.open(state_dir, clock=clock, backends=[lab_backend], config=config) as broker:
        recovered = broker.store.get_job(job_id)
        assert recovered.state in ACTIVE
        assert recovered.backend_handle == handle
        assert recovered.origin == "pilot"

        assert broker.reconcile() == [], "the broker lost track of a job it left running"

        gpu_host.finish(job_id, exit_code=0)
        clock.sleep(60)
        broker.tick()
        assert broker.store.get_job(job_id).state is JobState.COMPLETED


def test_a_pilot_pool_says_it_is_single_user_on_the_status_page(pilot_broker, gpu_host, clock):
    from gpu_broker.web.public import build

    job = submit_pilot(pilot_broker)
    pilot_broker.tick()
    gpu_host.finish(job.job_id, exit_code=0)
    clock.sleep(60)
    pilot_broker.tick()

    status = build(pilot_broker)
    assert status.pilot, "a one-person pool must not be presented as a queue"
    assert not status.demo, "pilot data is real work and must not be labelled demo"
