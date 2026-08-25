"""The lab backend: MPS, cgroups, capacity, and draining."""

from __future__ import annotations

import pytest

from conftest import local_job
from gpu_broker.backends.base import BackendStatus
from gpu_broker.errors import BackendError
from gpu_broker.local import commands
from gpu_broker.local.hosts import HostState
from gpu_broker.money import Currency


def test_the_lab_pool_is_denominated_in_gpu_hours(lab_backend):
    """The lab machine costs the club nothing. Pricing it in dollars would be a
    lie, and the ledger has carried a currency since Phase 0 so that this is a
    declaration rather than a special case."""
    assert lab_backend.currency is Currency.GPU_HOUR


def test_it_only_offers_the_gpu_type_it_has(lab_backend):
    assert lab_backend.supports("a6000")
    assert not lab_backend.supports("a10g")
    assert lab_backend.free_slots("a10g") == 0


# ------------------------------------------------------------------- launch


def test_a_job_runs_under_mps_with_its_own_memory_limit(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    lab_backend.launch(job)

    unit = gpu_host.unit_for_job(job.job_id)
    assert unit.env["CUDA_MPS_PIPE_DIRECTORY"] == "/tmp/gpu-broker-mps/pipe"
    assert unit.env["CUDA_MPS_PINNED_DEVICE_MEM_LIMIT"] == "0=16384M"
    assert unit.env["CUDA_VISIBLE_DEVICES"] == "0"


def test_thread_percentage_is_split_between_the_clients_a_gpu_allows(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    lab_backend.launch(job)
    assert gpu_host.unit_for_job(job.job_id).env["CUDA_MPS_ACTIVE_THREAD_PERCENTAGE"] == "50"


def test_host_ram_and_cpu_are_capped_by_cgroup(lab_backend, gpu_host):
    """MPS caps GPU memory and nothing else. A dataloader can still OOM the box
    and take every other job with it."""
    job = local_job("j1", "ana")
    lab_backend.launch(job)

    unit = gpu_host.unit_for_job(job.job_id)
    assert unit.memory_max_mb == 32_768
    assert unit.cpu_quota_percent == 400
    assert "--property=MemorySwapMax=0" in gpu_host.commands[-1]


def test_the_job_does_not_run_as_root(lab_backend, gpu_host):
    """The broker sudoes to create the unit, not to run somebody's training
    script with the whole machine's privileges."""
    job = local_job("j1", "ana")
    lab_backend.launch(job)
    assert gpu_host.unit_for_job(job.job_id).run_as == "broker"


def test_a_command_with_quotes_in_it_survives(lab_backend, gpu_host):
    """Somebody will submit this. Getting the quoting wrong means executing a
    fragment of their command as a separate shell word."""
    tricky = """python train.py --note "it's fine" --tag 'a b'"""
    job = local_job("j1", "ana", command=tricky)
    lab_backend.launch(job)
    assert gpu_host.unit_for_job(job.job_id).command == tricky


def test_the_job_is_tagged_so_an_orphan_is_attributable(lab_backend, gpu_host):
    job = local_job("j1", "bo")
    allocation = lab_backend.launch(job)
    assert allocation.tags["broker-job-id"] == job.job_id
    assert allocation.tags["broker-user"] == "bo"
    assert allocation.tags["broker-launched-at"]


def test_mps_is_started_once_and_only_when_needed(lab_backend, gpu_host):
    gpu_host.mps_running = False
    lab_backend.launch(local_job("j1", "ana"))
    lab_backend.launch(local_job("j2", "bo"))
    assert gpu_host.mps_starts == 1


def test_mps_is_not_restarted_if_it_is_already_up(lab_backend, gpu_host):
    gpu_host.mps_running = True
    lab_backend.launch(local_job("j1", "ana"))
    assert gpu_host.mps_starts == 0


# ----------------------------------------------------------------- capacity


def test_capacity_is_gpus_times_jobs_per_gpu(lab_backend):
    assert lab_backend.free_slots("a6000") == 2


def test_slots_fill_as_jobs_land(lab_backend):
    lab_backend.launch(local_job("j1", "ana"))
    assert lab_backend.free_slots("a6000") == 1
    lab_backend.launch(local_job("j2", "bo"))
    assert lab_backend.free_slots("a6000") == 0


def test_a_full_gpu_refuses_rather_than_oversubscribing(lab_backend):
    lab_backend.launch(local_job("j1", "ana"))
    lab_backend.launch(local_job("j2", "bo"))
    with pytest.raises(BackendError, match="limit of 2 concurrent"):
        lab_backend.launch(local_job("j3", "cy"))


def test_a_finished_job_frees_its_slot(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    lab_backend.launch(job)
    assert lab_backend.free_slots("a6000") == 1
    gpu_host.finish(job.job_id, 0)
    assert lab_backend.free_slots("a6000") == 2


def test_jobs_spread_across_cards_before_sharing_one(clock, transport, gpu_host):
    """Two concurrent jobs should land on different GPUs if there are two."""
    from gpu_broker.backends.local import LocalBackend
    from gpu_broker.local.hosts import LocalConfig

    gpu_host.gpus = [
        (0, "NVIDIA RTX A6000", 49140, 0, 0, "Default"),
        (1, "NVIDIA RTX A6000", 49140, 0, 0, "Default"),
    ]
    backend = LocalBackend(
        clock=clock,
        config=LocalConfig(hosts=(gpu_host.server.spec(),), max_jobs_per_gpu=2),
        transport=transport,
    )
    backend.launch(local_job("j1", "ana"))
    backend.launch(local_job("j2", "bo"))

    devices = {unit.gpu for unit in gpu_host.active_units()}
    assert devices == {0, 1}, f"both jobs landed on {devices}"


# ------------------------------------------------------------------ draining


def test_a_drained_host_takes_no_new_jobs(lab_backend, gpu_host):
    lab_backend.drain(gpu_host.server.spec().hostname, "swapping a fan")
    assert lab_backend.free_slots("a6000") == 0
    with pytest.raises(BackendError, match="every local host is unavailable"):
        lab_backend.launch(local_job("j1", "ana"))


def test_draining_leaves_running_jobs_alone(lab_backend, gpu_host):
    """Pulling the rug out from under somebody's training run is worse than
    whatever caused the drain."""
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    lab_backend.drain(gpu_host.server.spec().hostname, "maintenance")

    assert lab_backend.poll(handle).status is BackendStatus.RUNNING
    assert gpu_host.unit_for_job(job.job_id).active


def test_a_drain_says_why(lab_backend, gpu_host):
    lab_backend.drain(gpu_host.server.spec().hostname, "swapping a fan")
    health = lab_backend.health()[0]
    assert health.state is HostState.DRAINING
    assert "swapping a fan" in health.reason


def test_undraining_re_checks_the_host(lab_backend, gpu_host):
    hostname = gpu_host.server.spec().hostname
    lab_backend.drain(hostname, "maintenance")
    lab_backend.undrain(hostname)
    assert lab_backend.health()[0].state is HostState.HEALTHY


def test_draining_an_unknown_host_is_an_error(lab_backend):
    with pytest.raises(BackendError, match="no host"):
        lab_backend.drain("not-a-host", "typo")


def test_a_broken_host_drains_itself_and_stops_taking_work(lab_backend, gpu_host, clock):
    gpu_host.nvidia_smi_works = False
    clock.advance(seconds=200)  # past the health interval
    assert lab_backend.free_slots("a6000") == 0


def test_health_is_cached_between_checks(lab_backend, gpu_host, clock):
    """Several SSH round trips per host, and the scheduler asks once per queued
    job per tick."""
    lab_backend.health()
    before = len(gpu_host.commands)
    for _ in range(5):
        lab_backend.health()
    assert len(gpu_host.commands) == before


def test_health_is_re_checked_after_the_interval(lab_backend, gpu_host, clock):
    lab_backend.health()
    before = len(gpu_host.commands)
    clock.advance(seconds=200)
    lab_backend.health()
    assert len(gpu_host.commands) > before


# --------------------------------------------------------------------- poll


def test_a_running_job_polls_as_running(lab_backend):
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    assert lab_backend.poll(handle).status is BackendStatus.RUNNING


def test_a_clean_exit_is_completed(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.finish(job.job_id, 0)

    observation = lab_backend.poll(handle)
    assert observation.status is BackendStatus.COMPLETED
    assert observation.exit_code == 0


def test_a_nonzero_exit_is_failed_and_keeps_the_code(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.finish(job.job_id, 137)

    observation = lab_backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert observation.exit_code == 137


def test_an_oom_kill_says_it_was_the_memory_limit(lab_backend, gpu_host):
    """Otherwise it looks like the job crashed for no reason."""
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.oom(job.job_id)

    observation = lab_backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert "host memory limit" in observation.detail
    assert "32768MB" in observation.detail


def test_a_job_killed_without_an_exit_status_is_failed_not_completed(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.kill(job.job_id)

    observation = lab_backend.poll(handle)
    assert observation.status is BackendStatus.FAILED
    assert "killed rather than finishing" in observation.detail


def test_an_unreachable_host_does_not_kill_the_job(lab_backend, gpu_host):
    """A network hiccup is not a finished job. Reporting FAILED here would end
    somebody's four-hour run because a switch blinked."""
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.server.close()

    observation = lab_backend.poll(handle)
    assert observation.status is BackendStatus.PENDING
    assert "not answering" in observation.detail


def test_this_backend_never_reports_ready(lab_backend):
    """`systemd-run` starts the command as it creates the unit, so there is no
    gap between the machine being usable and the job running."""
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    assert lab_backend.poll(handle).status is not BackendStatus.READY


# --------------------------------------------------------------------- logs


def test_output_comes_back(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.emit(job.job_id, "epoch 1 loss 2.31", "epoch 2 loss 1.88")

    assert lab_backend.fetch_logs(handle) == [
        ("stdout", "epoch 1 loss 2.31"),
        ("stdout", "epoch 2 loss 1.88"),
    ]


def test_the_cursor_only_returns_new_lines(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    gpu_host.emit(job.job_id, "first")
    seen = len(lab_backend.fetch_logs(handle))
    gpu_host.emit(job.job_id, "second")

    assert lab_backend.fetch_logs(handle, after=seen) == [("stdout", "second")]


def test_no_output_yet_is_not_an_error(lab_backend):
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    assert lab_backend.fetch_logs(handle) == []


# ---------------------------------------------------------------- terminate


def test_terminate_stops_the_unit(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    lab_backend.terminate(handle, "cancelled by ana")
    assert not gpu_host.unit_for_job(job.job_id).active


def test_terminate_clears_the_failed_state_so_the_name_is_reusable(lab_backend, gpu_host):
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    lab_backend.terminate(handle, "cancelled")
    assert any("reset-failed" in command for command in gpu_host.commands)


def test_terminating_something_already_gone_is_not_an_error(lab_backend):
    lab_backend.terminate("127.0.0.1/deadbeef", "already dead")


def test_terminating_on_an_unreachable_host_does_not_raise(lab_backend, gpu_host):
    """The health check will drain it and reconcile will report what is left."""
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    gpu_host.server.close()
    lab_backend.terminate(handle, "cancelled")


# ------------------------------------------------------- reconciliation view


def test_live_jobs_are_listed_with_who_owns_them(lab_backend, gpu_host):
    lab_backend.launch(local_job("j1", "ana"))
    lab_backend.launch(local_job("j2", "bo"))

    owners = {resource.user_id for resource in lab_backend.list_resources()}
    assert owners == {"ana", "bo"}


def test_finished_jobs_are_not_resources(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    lab_backend.launch(job)
    gpu_host.finish(job.job_id, 0)
    assert lab_backend.list_resources() == []


def test_an_unreachable_host_reports_nothing_rather_than_claiming_it_is_empty(
    lab_backend, gpu_host
):
    """'Nothing is running here' and 'we cannot see this host' are different
    claims, and only one of them is comforting."""
    lab_backend.launch(local_job("j1", "ana"))
    gpu_host.server.close()
    lab_backend.health(refresh=True)
    assert lab_backend.list_resources() == []


# ------------------------------------------------------------------ dry run


def test_validate_launch_describes_the_limits_and_starts_nothing(lab_backend, gpu_host):
    description = lab_backend.validate_launch(local_job("j1", "ana"))
    assert "16384MB of GPU memory" in description
    assert "32768MB of host RAM" in description
    assert gpu_host.active_units() == []


def test_validate_launch_fails_when_every_host_is_drained(lab_backend, gpu_host):
    lab_backend.drain(gpu_host.server.spec().hostname, "maintenance")
    with pytest.raises(BackendError, match="unavailable"):
        lab_backend.validate_launch(local_job("j1", "ana"))


def test_validate_terminate_stops_nothing(lab_backend, gpu_host):
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    assert "would stop" in lab_backend.validate_terminate(handle)
    assert gpu_host.unit_for_job(job.job_id).active


# --------------------------------------------------------- the command strings


def test_the_unit_and_directory_both_use_the_whole_job_id(lab_backend, gpu_host):
    """Truncating would mean a resource found on the host could only be matched
    back to a job by prefix search. An orphan has to be attributable exactly."""
    job = local_job("j1", "ana")
    lab_backend.launch(job)
    assert commands.unit_for(job.job_id).endswith(job.job_id)
    assert job.job_id in gpu_host.commands[-1]


def test_sudo_is_non_interactive(lab_backend, gpu_host):
    """`sudo -n`, so a host that would prompt fails immediately rather than
    hanging a tick until the SSH timeout."""
    lab_backend.launch(local_job("j1", "ana"))
    sudo_commands = [c for c in gpu_host.commands if c.startswith("sudo")]
    assert sudo_commands
    assert all(c.startswith("sudo -n ") for c in sudo_commands)


def test_no_path_reaches_the_host_with_an_unexpanded_tilde(lab_backend, gpu_host):
    """A tilde inside shell quotes is never expanded; the machine would end up
    with a directory literally named `~`."""
    lab_backend.launch(local_job("j1", "ana"))
    assert not any("~" in command for command in gpu_host.commands)


# ------------------------------------------------------------- gpu sampling


def test_utilization_is_attributed_per_process_not_per_card(lab_backend, gpu_host):
    """Under MPS two users share one A6000. The card's own utilization says
    nothing about whose job is working, and reclaiming on that number would kill
    an idle job's busy neighbour."""
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    lab_backend.sample_utilization(handle)

    sent = [c for c in gpu_host.commands if "pmon" in c]
    assert sent, "never asked for per-process utilization"
    assert "cgroup.procs" in sent[-1], "did not scope the reading to this job's processes"


def test_a_reading_that_cannot_be_attributed_records_nothing(lab_backend, gpu_host, monkeypatch):
    """Returning 0% for a failed lookup is indistinguishable from an idle job."""
    job = local_job("j1", "ana")
    handle = lab_backend.launch(job).handle
    monkeypatch.setattr(gpu_host, "handle", lambda command: (0, "###PIDS\n###PMON\n###MEM\n", ""))
    assert lab_backend.sample_utilization(handle) == []


def test_an_unreachable_host_yields_no_samples(lab_backend, gpu_host):
    handle = lab_backend.launch(local_job("j1", "ana")).handle
    gpu_host.server.close()
    lab_backend.health(refresh=True)
    assert lab_backend.sample_utilization(handle) == []


# --------------------------------------------------- environments and volumes


@pytest.fixture
def env_lab(clock, local_config, transport):
    from gpu_broker.backends.local import LocalBackend
    from gpu_broker.environments import Environment
    from gpu_broker.volumes import VolumeConfig

    vision = Environment("vision", requirements=("torch==2.3.0",))
    backend = LocalBackend(
        clock=clock, config=local_config, transport=transport,
        environments=lambda name: vision if name == "vision" else None,
        volumes=VolumeConfig(enabled=True),
        environment_root="~/.gpu-broker/envs",
    )
    return backend, vision


def env_job(job_id="j1", user_id="ana", environment="vision"):
    from dataclasses import replace

    return replace(local_job(job_id, user_id), environment=environment)


def test_the_lab_uses_a_cached_virtualenv_not_a_container(env_lab, gpu_host):
    """Docker there would move the cgroup limits into Docker's flags and need
    the MPS pipe bind-mounted -- a rework of isolation that already works."""
    backend, vision = env_lab
    backend.launch(env_job())
    script = gpu_host.unit_for_job("j1".ljust(32, "0")).command

    assert "docker" not in script
    assert "python3 -m venv" in script
    assert vision.digest in script


def test_the_venv_is_built_inside_the_job_not_before_dispatch(env_lab, gpu_host):
    """A cache miss takes minutes. Doing it in `launch` would hold the whole
    tick open while one job installs torch."""
    backend, _ = env_lab
    backend.launch(env_job())
    assert not any("venv" in command for command in gpu_host.commands[:-1])
    assert "venv" in gpu_host.unit_for_job("j1".ljust(32, "0")).command


def test_the_venv_is_activated_before_the_users_command(env_lab, gpu_host):
    backend, vision = env_lab
    backend.launch(env_job())
    script = gpu_host.unit_for_job("j1".ljust(32, "0")).command

    assert script.index("bin/activate") < script.index("python train.py")


def test_a_second_job_with_the_same_environment_finds_it_built(env_lab, gpu_host):
    """The script short-circuits, so the second job pays nothing."""
    backend, vision = env_lab
    backend.launch(env_job("j1", "ana"))
    backend.launch(env_job("j2", "bo"))

    for job_id in ("j1", "j2"):
        script = gpu_host.unit_for_job(job_id.ljust(32, "0")).command
        assert script.count("if [ -x") == 1
        assert vision.digest in script


def test_the_data_directory_is_made_and_exported(env_lab, gpu_host):
    backend, _ = env_lab
    backend.launch(env_job(user_id="bo"))
    script = gpu_host.unit_for_job("j1".ljust(32, "0")).command

    assert "mkdir -p /home/broker/.gpu-broker/data/bo" in script
    assert "GPU_BROKER_DATA=/home/broker/.gpu-broker/data/bo" in script


def test_a_job_with_no_environment_just_runs(env_lab, gpu_host):
    backend, _ = env_lab
    backend.launch(env_job(environment=None))
    script = gpu_host.unit_for_job("j1".ljust(32, "0")).command

    assert "venv" not in script
    assert script.strip().endswith("python train.py")
