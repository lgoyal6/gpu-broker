"""Whether a lab host is fit to take work, and what to do when it is not.

The rule from the build prompt: a host that fails to report is drained, not
silently retried. Silently retrying is how a machine with a wedged driver eats
the queue one job at a time while everyone assumes the broker is slow.

The health check verifies rather than assumes. In particular it proves that
cgroup limits actually take effect, by running a throwaway scope with a known
memory cap and having it read its own cgroup back. `systemd-run` exits zero on a
host where the memory controller is not delegated and applies nothing at all; a
check that only looked at the exit status would pass, and the first anyone would
hear of it is a job taking the whole machine down.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from enum import StrEnum

from ..errors import BackendError, ConfigError
from .commands import (
    GPU_QUERY,
    HELLO,
    PROBE_MEMORY_BYTES,
    GpuInfo,
    limit_probe,
    mps_running,
    mps_start,
    parse_gpus,
    parse_hello,
    parse_limit_probe,
)
from .transport import HostSpec, Transport


class HostState(StrEnum):
    HEALTHY = "HEALTHY"
    """Accepting new jobs."""

    DRAINING = "DRAINING"
    """No new jobs. Whatever is already running is left alone, because pulling
    the rug out from under somebody's training run is worse than the problem
    that caused the drain."""

    UNREACHABLE = "UNREACHABLE"
    """SSH itself failed. We cannot see what is running there, so `reconcile`
    reports the jobs we believe are on it as drift rather than guessing."""

    @property
    def accepts_jobs(self) -> bool:
        return self is HostState.HEALTHY


@dataclass(frozen=True)
class HostHealth:
    hostname: str
    state: HostState
    reason: str
    checked_at: dt.datetime
    gpus: tuple[GpuInfo, ...] = ()
    mps_ready: bool = False
    limits_enforced: bool = False
    limits_tested: bool = False
    """Whether the cgroup probe actually ran. A host whose driver is broken
    never gets that far, and reporting "limits are not enforced" there would
    point at the wrong problem entirely."""
    home: str = ""
    """The SSH user's home directory, read from the host. Config may say
    `~/.gpu-broker/jobs`; only the host knows what that means."""

    @property
    def gpu_count(self) -> int:
        return len(self.gpus)

    def describe(self) -> str:
        if self.state is HostState.HEALTHY:
            names = ", ".join(f"{gpu.name} ({gpu.memory_total_mb}MB)" for gpu in self.gpus)
            return f"{self.hostname}: healthy, {names or 'no gpus'}"
        return f"{self.hostname}: {self.state.lower()}, {self.reason}"


@dataclass(frozen=True)
class LocalConfig:
    """The lab pool's settings."""

    hosts: tuple[HostSpec, ...] = ()

    gpu_type: str = "a6000"
    """What the broker calls this hardware. Must be a configured gpu type, and
    it should be one denominated in GPU_HOUR: the lab machine costs nobody any
    dollars, and pricing it in dollars would be a lie."""

    max_jobs_per_gpu: int = 2
    """How many MPS clients share one card. Two is a starting point, not a
    finding. Measure it before raising it -- the whole point of Phase 7 is that
    the MPS overhead against exclusive access gets published either way."""

    job_root: str = "~/.gpu-broker/jobs"
    working_dir: str = "~"

    mps_pipe_dir: str = "/tmp/gpu-broker-mps/pipe"
    mps_log_dir: str = "/tmp/gpu-broker-mps/log"

    default_gpu_memory_mb: int = 16_384
    default_memory_max_mb: int = 32_768
    default_cpu_quota_percent: int = 400

    use_sudo: bool = True
    """Passwordless sudo for `systemd-run`. Without it, cgroup limits either do
    not apply or apply only partially, and this host will be drained."""

    run_as: str = ""
    """Which user a job's process runs as. Empty means the SSH user. Never root:
    the broker sudoes to *create* the unit, not to run somebody's training
    script with the whole machine's privileges."""

    health_interval_seconds: float = 120.0
    command_timeout_seconds: float = 60.0
    poll_timeout_seconds: float = 15.0
    """Shorter than `command_timeout_seconds`, because polls happen on every
    tick for every running job. A host that has stopped answering must not be
    able to hold a tick open for a minute per job it is hosting."""

    def resolved_run_as(self, host: HostSpec) -> str:
        return self.run_as or host.username

    def validate(self) -> None:
        if self.max_jobs_per_gpu < 1:
            raise ConfigError("local.max_jobs_per_gpu must be at least 1")
        if self.default_gpu_memory_mb < 256:
            raise ConfigError("local.default_gpu_memory_mb is too small to run anything")
        names = [host.hostname for host in self.hosts]
        if len(names) != len(set(names)):
            raise ConfigError(f"duplicate hosts in local.hosts: {names}")


def load_local_config(raw: dict | None) -> LocalConfig:
    if not raw:
        return LocalConfig()
    known = set(LocalConfig.__dataclass_fields__)
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"unknown keys under 'local' in config.json: {', '.join(sorted(unknown))}. "
            f"Known: {', '.join(sorted(known))}"
        )
    settings = dict(raw)
    if "hosts" in settings:
        settings["hosts"] = tuple(_host_from(entry) for entry in settings["hosts"])
    config = replace(LocalConfig(), **settings)
    config.validate()
    return config


def _host_from(entry: dict | str) -> HostSpec:
    if isinstance(entry, str):
        return HostSpec(hostname=entry)
    known = set(HostSpec.__dataclass_fields__)
    unknown = set(entry) - known
    if unknown:
        raise ConfigError(
            f"unknown keys in a local.hosts entry: {', '.join(sorted(unknown))}. "
            f"Known: {', '.join(sorted(known))}"
        )
    return HostSpec(**entry)


def check_host(
    transport: Transport, host: HostSpec, config: LocalConfig, now: dt.datetime
) -> HostHealth:
    """Look at a host and decide whether it may take work. Changes nothing."""
    timeout = config.command_timeout_seconds

    home = ""

    def unhealthy(state: HostState, reason: str, **extra) -> HostHealth:
        return HostHealth(
            hostname=host.hostname, state=state, reason=reason, checked_at=now,
            home=home, **extra
        )

    try:
        alive = transport.run(host, HELLO, timeout=timeout)
    except BackendError as exc:
        return unhealthy(HostState.UNREACHABLE, str(exc))
    home = parse_hello(alive.stdout) if alive.ok else None
    if not alive.ok or not home:
        return unhealthy(HostState.UNREACHABLE, "ssh connected but the shell failed")

    try:
        smi = transport.run(host, GPU_QUERY, timeout=timeout)
    except BackendError as exc:
        return unhealthy(HostState.DRAINING, f"nvidia-smi did not answer: {exc}")
    if not smi.ok:
        first = (smi.stderr or smi.stdout).strip().splitlines()
        return unhealthy(
            HostState.DRAINING,
            f"nvidia-smi failed: {first[0] if first else 'no output'}. "
            "Usually a driver that needs a reboot",
        )

    gpus = tuple(parse_gpus(smi.stdout))
    if not gpus:
        return unhealthy(HostState.DRAINING, "nvidia-smi reported no GPUs")

    unusable = [gpu for gpu in gpus if not gpu.supports_mps]
    if unusable:
        modes = ", ".join(f"gpu {gpu.index} is {gpu.compute_mode}" for gpu in unusable)
        return unhealthy(
            HostState.DRAINING,
            f"{modes}. MPS cannot open a Prohibited device; "
            f"set it with: sudo nvidia-smi -i {unusable[0].index} -c DEFAULT",
            gpus=gpus,
        )

    # The check that matters: do limits actually bite on this host?
    try:
        probe = transport.run(host, limit_probe(config.use_sudo), timeout=timeout)
    except BackendError as exc:
        return unhealthy(HostState.DRAINING, f"could not test cgroup limits: {exc}", gpus=gpus)

    applied = parse_limit_probe(probe.stdout) if probe.ok else None
    if applied != PROBE_MEMORY_BYTES:
        detail = (probe.stderr or probe.stdout).strip().splitlines()
        why = detail[-1] if detail else f"exit {probe.exit_status}"
        return unhealthy(
            HostState.DRAINING,
            (
                f"cgroup limits do not take effect here (asked for "
                f"{PROBE_MEMORY_BYTES} bytes, the scope reported {applied}): {why}. "
                "Without them one job can take the whole machine down, so no new "
                "jobs will be placed here. Give the SSH user passwordless sudo "
                "for systemd-run"
            ),
            gpus=gpus,
            limits_tested=True,
        )

    mps = transport.run(host, mps_running(config.mps_pipe_dir), timeout=timeout)
    return HostHealth(
        hostname=host.hostname,
        state=HostState.HEALTHY,
        reason="",
        checked_at=now,
        gpus=gpus,
        mps_ready=mps.ok,
        limits_enforced=True,
        limits_tested=True,
        home=home,
    )


def start_mps(transport: Transport, host: HostSpec, config: LocalConfig) -> None:
    """Bring the MPS control daemon up. Idempotent.

    Separate from `check_host` on purpose: a health check that quietly repairs
    things cannot be trusted to tell you what is wrong. This is called when the
    backend is about to need MPS, and it is allowed to change the machine.
    """
    result = transport.run(
        host,
        mps_start(config.mps_pipe_dir, config.mps_log_dir),
        timeout=config.command_timeout_seconds,
    )
    if not result.ok:
        detail = (result.stderr or result.stdout).strip().splitlines()
        raise BackendError(
            f"{host.hostname}: could not start the MPS daemon: "
            f"{detail[0] if detail else 'no output'}"
        )
