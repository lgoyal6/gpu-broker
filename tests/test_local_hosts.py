"""Host health, and the rule that a host which fails to report is drained.

Silently retrying a broken machine is how a wedged driver eats the queue one job
at a time while everyone assumes the broker is slow.
"""

from __future__ import annotations

import pytest

from gpu_broker.errors import BackendError, ConfigError
from gpu_broker.local.commands import PROBE_MEMORY_BYTES
from gpu_broker.local.hosts import (
    HostState,
    LocalConfig,
    check_host,
    load_local_config,
    start_mps,
)
from gpu_broker.local.transport import HostSpec


def health_of(transport, gpu_host, config, clock):
    return check_host(transport, gpu_host.server.spec(), config, clock.now())


# ------------------------------------------------------------------- healthy


def test_a_working_host_is_healthy(transport, gpu_host, local_config, clock):
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.HEALTHY
    assert health.gpu_count == 1
    assert health.gpus[0].name == "NVIDIA RTX A6000"
    assert health.limits_enforced


def test_the_home_directory_comes_from_the_host(transport, gpu_host, local_config, clock):
    """Config may say `~/.gpu-broker/jobs`. Only the machine knows what that is,
    and a tilde inside shell quotes is never expanded."""
    gpu_host.home = "/data/broker"
    assert health_of(transport, gpu_host, local_config, clock).home == "/data/broker"


# --------------------------------------------------------------- the drains


def test_an_unreachable_host_is_unreachable_not_merely_drained(local_config, transport, clock):
    """Different states because they mean different things: a drained host is
    one we can see and have chosen not to use."""
    dead = HostSpec(hostname="127.0.0.1", username="broker", port=1)
    health = check_host(transport, dead, local_config, clock.now())
    assert health.state is HostState.UNREACHABLE


def test_a_broken_driver_drains_the_host(transport, gpu_host, local_config, clock):
    gpu_host.nvidia_smi_works = False
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.DRAINING
    assert "nvidia-smi failed" in health.reason
    assert "reboot" in health.reason


def test_a_host_with_no_gpus_is_drained(transport, gpu_host, local_config, clock):
    gpu_host.gpus = []
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.DRAINING
    assert "no GPUs" in health.reason


def test_a_prohibited_gpu_is_drained_with_the_command_to_fix_it(
    transport, gpu_host, local_config, clock
):
    """MPS cannot open a Prohibited device, so the card is useless to us. Saying
    which `nvidia-smi` line fixes it is the difference between a five-second fix
    and a message to me."""
    gpu_host.gpus = [(0, "NVIDIA RTX A6000", 49140, 0, 0, "Prohibited")]
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.DRAINING
    assert "nvidia-smi -i 0 -c DEFAULT" in health.reason


@pytest.mark.parametrize("mode", ["Default", "Exclusive_Process"])
def test_both_usable_compute_modes_are_accepted(transport, gpu_host, local_config, clock, mode):
    gpu_host.gpus = [(0, "NVIDIA RTX A6000", 49140, 0, 0, mode)]
    assert health_of(transport, gpu_host, local_config, clock).state is HostState.HEALTHY


# ------------------------------------------------ the check that matters most


def test_limits_that_do_not_actually_apply_drain_the_host(
    transport, gpu_host, local_config, clock
):
    """`systemd-run` exits zero on a host where the memory controller is not
    delegated, and applies nothing. A check that only looked at the exit status
    would pass, and the first anyone would hear of it is a job taking the whole
    machine down.
    """
    gpu_host.limits_apply = False
    health = health_of(transport, gpu_host, local_config, clock)

    assert health.state is HostState.DRAINING
    assert not health.limits_enforced
    assert "do not take effect" in health.reason
    assert str(PROBE_MEMORY_BYTES) in health.reason
    assert "passwordless sudo" in health.reason


def test_no_sudo_drains_the_host(transport, gpu_host, local_config, clock):
    gpu_host.sudo_works = False
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.DRAINING
    assert not health.limits_enforced


def test_the_probe_verifies_by_reading_the_cgroup_back(transport, gpu_host, local_config, clock):
    """Not by trusting the exit code."""
    health_of(transport, gpu_host, local_config, clock)
    assert any("cat /sys/fs/cgroup/memory.max" in command for command in gpu_host.commands)


# --------------------------------------------------------------------- MPS


def test_health_reports_mps_but_does_not_start_it(transport, gpu_host, local_config, clock):
    """A health check that quietly repairs things cannot be trusted to say what
    is wrong."""
    gpu_host.mps_running = False
    health = health_of(transport, gpu_host, local_config, clock)
    assert health.state is HostState.HEALTHY
    assert not health.mps_ready
    assert gpu_host.mps_starts == 0


def test_start_mps_brings_the_daemon_up(transport, gpu_host, local_config):
    start_mps(transport, gpu_host.server.spec(), local_config)
    assert gpu_host.mps_running
    assert gpu_host.mps_starts == 1


def test_a_daemon_that_will_not_start_is_an_error(transport, gpu_host, local_config):
    gpu_host.mps_can_start = False
    with pytest.raises(BackendError, match="MPS daemon"):
        start_mps(transport, gpu_host.server.spec(), local_config)


# ------------------------------------------------------------------- config


def test_hosts_can_be_written_as_bare_names():
    config = load_local_config({"hosts": ["lab1.ucsd.edu"]})
    assert config.hosts[0].hostname == "lab1.ucsd.edu"
    assert config.hosts[0].username == "broker"


def test_a_typo_in_a_host_entry_lists_the_real_keys():
    with pytest.raises(ConfigError, match="hostname"):
        load_local_config({"hosts": [{"hostnme": "lab1"}]})


def test_duplicate_hosts_are_refused():
    with pytest.raises(ConfigError, match="duplicate"):
        load_local_config({"hosts": ["lab1", "lab1"]})


def test_a_pool_that_could_never_run_anything_is_refused():
    with pytest.raises(ConfigError, match="max_jobs_per_gpu"):
        LocalConfig(max_jobs_per_gpu=0).validate()
