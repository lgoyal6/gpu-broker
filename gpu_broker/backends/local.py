"""The lab pool: real GPUs on machines we have SSH access to.

This is where multi-tenancy stops being a scheduling abstraction. On EC2 two
jobs get two machines. Here two jobs get one A6000, and something has to stop
either of them taking the whole card.

That something is **CUDA MPS**. The A6000 does not support MIG, so the choices
are MPS or time-slicing, and MPS is the one that lets kernels from different
processes run concurrently instead of taking turns. Each job runs as an MPS
client with its own `CUDA_MPS_PINNED_DEVICE_MEM_LIMIT`, which is set per client
rather than as a daemon-wide default precisely so two jobs can have different
limits.

Host RAM and CPU are handled separately, by cgroup v2 through a transient
systemd service. MPS caps GPU memory and nothing else; a dataloader can still
OOM the machine and take every other job with it.

Capacity here is denominated in GPU-hours, not dollars, because the lab machine
costs the club nothing. That is not a special case in this file -- the ledger,
the budgets, and fair share have carried a currency since Phase 0, and this
backend simply declares which one it deals in.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace


from ..clock import Clock, to_iso
from ..errors import BackendError
import shlex

from ..environments import Environment
from ..local import commands
from ..local.hosts import HostHealth, HostState, LocalConfig, check_host, start_mps
from ..local.transport import HostSpec, Transport
from ..models import Job
from ..money import Currency
from ..volumes import VolumeConfig, local_data_dir, local_mount_script
from .base import Allocation, BackendStatus, Observation, Resource, UtilizationSample


class LocalBackend:
    name: str
    currency = Currency.GPU_HOUR

    def __init__(
        self,
        name: str = "local",
        *,
        clock: Clock,
        config: LocalConfig,
        transport: Transport,
        tier: str = "local",
        environments: "Callable[[str], Environment | None] | None" = None,
        volumes: VolumeConfig | None = None,
        environment_root: str = "~/.gpu-broker/envs",
    ) -> None:
        self.name = name
        self.clock = clock
        self.tier = tier
        self.config = config
        self.transport = transport

        self._health: dict[str, HostHealth] = {}
        self._manual_drain: dict[str, str] = {}
        self._mps_started: set[str] = set()
        self.environments = environments
        self.volumes = volumes or VolumeConfig()
        self.environment_root = environment_root

    # ------------------------------------------------------------- health

    def hosts(self) -> tuple[HostSpec, ...]:
        return self.config.hosts

    def host(self, hostname: str) -> HostSpec | None:
        for host in self.config.hosts:
            if host.hostname == hostname:
                return host
        return None

    def health(self, refresh: bool = False) -> list[HostHealth]:
        """Current health of every host, re-checked on an interval.

        Cached because this is several SSH round trips per host and the
        scheduler asks about capacity once per queued job per tick.
        """
        now = self.clock.now()
        out: list[HostHealth] = []
        for host in self.config.hosts:
            known = self._health.get(host.hostname)
            stale = (
                known is None
                or refresh
                or (now - known.checked_at).total_seconds()
                >= self.config.health_interval_seconds
            )
            if stale:
                known = check_host(self.transport, host, self.config, now)
                self._health[host.hostname] = known
            out.append(self._with_manual_drain(known))
        return out

    def _with_manual_drain(self, health: HostHealth) -> HostHealth:
        reason = self._manual_drain.get(health.hostname)
        if reason is None:
            return health
        return replace(health, state=HostState.DRAINING, reason=f"drained by hand: {reason}")

    def drain(self, hostname: str, reason: str) -> None:
        """Stop placing new jobs here. Running jobs are left alone."""
        if self.host(hostname) is None:
            raise BackendError(f"no host {hostname!r} in local.hosts")
        self._manual_drain[hostname] = reason

    def undrain(self, hostname: str) -> None:
        self._manual_drain.pop(hostname, None)
        self._health.pop(hostname, None)  # force a fresh check

    def healthy_hosts(self) -> list[HostHealth]:
        return [health for health in self.health() if health.state.accepts_jobs]

    # ----------------------------------------------------------- capacity

    def supports(self, gpu_type: str) -> bool:
        return gpu_type == self.config.gpu_type

    def free_slots(self, gpu_type: str) -> int:
        if not self.supports(gpu_type):
            return 0
        return sum(self._free_on(health) for health in self.healthy_hosts())

    def _free_on(self, health: HostHealth) -> int:
        capacity = health.gpu_count * self.config.max_jobs_per_gpu
        return max(0, capacity - len(self._live_on(health.hostname)))

    def _live_on(self, hostname: str) -> list[dict[str, str]]:
        host = self.host(hostname)
        if host is None:
            return []
        try:
            result = self.transport.run(
                host,
                commands.list_live_jobs(self._root_for(hostname), self.config.use_sudo),
                timeout=self.config.command_timeout_seconds,
            )
        except BackendError:
            return []
        return commands.parse_live_jobs(result.stdout)

    def _mark_unreachable(self, hostname: str, reason: str) -> None:
        """Record what a failed command just told us, so the next poll is cheap.

        The health check would find this out on its own schedule; recording it
        here means the rest of this tick does not pay for the discovery again.
        """
        known = self._health.get(hostname)
        self._health[hostname] = HostHealth(
            hostname=hostname,
            state=HostState.UNREACHABLE,
            reason=reason,
            checked_at=self.clock.now(),
            gpus=known.gpus if known else (),
            home=known.home if known else "",
        )

    def _root_for(self, hostname: str) -> str:
        known = self._health.get(hostname)
        return commands.expand(self.config.job_root, known.home if known else "")

    # ------------------------------------------------------------- launch

    def launch(self, job: Job) -> Allocation:
        health = self._pick_host(job)
        host = self.host(health.hostname)
        assert host is not None
        device = self._pick_device(health)

        self._ensure_mps(host, health)

        now = self.clock.now()
        root = commands.expand(self.config.job_root, health.home)
        directory = commands.job_dir(root, job.job_id)
        env = commands.mps_client_env(
            pipe_dir=self.config.mps_pipe_dir,
            device=device,
            memory_mb=self.config.default_gpu_memory_mb,
            thread_percent=max(1, 100 // self.config.max_jobs_per_gpu),
        )
        env["GPU_BROKER_JOB_ID"] = job.job_id
        env["GPU_BROKER_USER"] = job.user_id

        command = commands.launch(
            unit=commands.unit_for(job.job_id),
            directory=directory,
            working_dir=commands.expand(self.config.working_dir, health.home),
            command=self._command_for(job, health.home),
            env=env,
            memory_max_mb=self.config.default_memory_max_mb,
            cpu_quota_percent=self.config.default_cpu_quota_percent,
            run_as=self.config.resolved_run_as(host),
            meta=commands.meta_lines(job.job_id, job.user_id, to_iso(now), device),
            use_sudo=self.config.use_sudo,
        )
        result = self.transport.run(host, command, timeout=self.config.command_timeout_seconds)
        if not result.ok:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise BackendError(
                f"{host.hostname}: could not start the job: "
                f"{detail[-1] if detail else f'exit {result.exit_status}'}"
            )

        return Allocation(
            handle=_handle(host.hostname, job.job_id),
            gpu_type=self.config.gpu_type,
            tags={
                "broker-job-id": job.job_id,
                "broker-user": job.user_id,
                "broker-launched-at": to_iso(now),
            },
        )

    def _command_for(self, job: Job, home: str) -> str:
        """The user's command, with their environment in front of it.

        The virtualenv is built here rather than before dispatch on purpose. A
        cache miss takes minutes, and doing that inside `launch` would hold the
        whole tick open while one job installs torch. Inside the unit it is the
        job's own time, it is billed to the job, and every later job with the
        same digest finds it already there.

        Not a container. The A6000 shares one card between users via MPS with
        per-client memory limits, and host RAM and CPU are capped by systemd
        cgroups; putting jobs in Docker there would move both into Docker's
        flags and bind-mount the MPS pipe through. A cached virtualenv keyed on
        the same digest gets the thing that was actually wanted, which is that
        everybody's packages match.
        """
        parts: list[str] = []
        if self.volumes.enabled:
            parts.append(local_mount_script(self.volumes, job.user_id, home))
            data = local_data_dir(self.volumes, job.user_id, home)
            parts.append(f"export GPU_BROKER_DATA={shlex.quote(data)}")

        environment = (
            self.environments(job.environment)
            if job.environment and self.environments
            else None
        )
        if environment is not None:
            root = commands.expand(self.environment_root, home)
            parts.append(environment.venv_script(root))
            parts.append(f'. {shlex.quote(environment.venv_path(root))}/bin/activate')

        parts.append(job.command)
        return "\n".join(parts)

    def start(self, handle: str, job: Job) -> None:
        """Nothing to do. `systemd-run` starts the command as it creates the
        unit, so this backend never reports READY."""

    def validate_launch(self, job: Job) -> str:
        health = self._pick_host(job)
        return (
            f"would run on {health.hostname} under MPS with "
            f"{self.config.default_gpu_memory_mb}MB of GPU memory, "
            f"{self.config.default_memory_max_mb}MB of host RAM and "
            f"{self.config.default_cpu_quota_percent}% CPU"
        )

    def _pick_host(self, job: Job) -> HostHealth:
        if not self.supports(job.gpu_type):
            raise BackendError(f"{self.name} has no {job.gpu_type} capacity")
        all_hosts = self.health()
        if not all_hosts:
            raise BackendError("no hosts are configured under local.hosts")

        usable = [health for health in all_hosts if health.state.accepts_jobs]
        if not usable:
            reasons = "; ".join(f"{h.hostname} is {h.state.lower()} ({h.reason})" for h in all_hosts)
            raise BackendError(f"every local host is unavailable: {reasons}")

        # Emptiest first, so two concurrent jobs land on different cards before
        # they start sharing one.
        with_room = [(self._free_on(health), health) for health in usable]
        with_room = [(free, health) for free, health in with_room if free > 0]
        if not with_room:
            raise BackendError(
                f"every local GPU is at its limit of "
                f"{self.config.max_jobs_per_gpu} concurrent jobs"
            )
        with_room.sort(key=lambda pair: (-pair[0], pair[1].hostname))
        return with_room[0][1]

    def _pick_device(self, health: HostHealth) -> int:
        """Least-loaded card on the chosen host."""
        counts = {gpu.index: 0 for gpu in health.gpus}
        for live in self._live_on(health.hostname):
            try:
                counts[int(live.get("gpu", 0))] = counts.get(int(live.get("gpu", 0)), 0) + 1
            except ValueError:
                continue
        return min(counts, key=lambda index: (counts[index], index)) if counts else 0

    def _ensure_mps(self, host: HostSpec, health: HostHealth) -> None:
        if health.mps_ready or host.hostname in self._mps_started:
            return
        start_mps(self.transport, host, self.config)
        self._mps_started.add(host.hostname)

    # --------------------------------------------------------------- poll

    def poll(self, handle: str) -> Observation:
        hostname, job_id = _split(handle)
        host = self.host(hostname)
        if host is None:
            return Observation(BackendStatus.GONE, detail=f"host {hostname} is not configured")

        known = self._health.get(hostname)
        if known is not None and known.state is HostState.UNREACHABLE:
            # Do not spend a timeout per job discovering the same thing. The
            # health check owns deciding when this host is back; with twenty
            # jobs on a machine that has gone away, retrying here would hold a
            # tick open for twenty timeouts in a row.
            return Observation(
                BackendStatus.PENDING,
                detail=f"host not answering: {known.reason}",
            )

        try:
            result = self.transport.run(
                host,
                commands.unit_status(
                    commands.unit_for(job_id),
                    commands.job_dir(self._root_for(hostname), job_id),
                    self.config.use_sudo,
                ),
                timeout=self.config.poll_timeout_seconds,
            )
        except BackendError as exc:
            self._mark_unreachable(hostname, str(exc))
            # An unreachable host is not a finished job. Report PENDING and let
            # the health check decide the host's fate; killing somebody's run
            # because the network hiccuped would be worse than waiting.
            return Observation(BackendStatus.PENDING, detail=f"host not answering: {exc}")

        status = commands.parse_unit_status(result.stdout)

        if status.exit_code is not None:
            if status.exit_code == 0:
                return Observation(BackendStatus.COMPLETED, exit_code=0, detail="exited cleanly")
            return Observation(
                BackendStatus.FAILED,
                exit_code=status.exit_code,
                detail=f"exited with status {status.exit_code}",
            )

        if status.active_state in ("activating", "active", "reloading"):
            return Observation(BackendStatus.RUNNING, detail=status.sub_state or "running")

        if status.active_state in ("", "inactive", "failed", "deactivating"):
            if status.was_oom_killed:
                return Observation(
                    BackendStatus.FAILED,
                    detail=(
                        f"killed for exceeding its "
                        f"{self.config.default_memory_max_mb}MB host memory limit"
                    ),
                )
            return Observation(
                BackendStatus.FAILED,
                detail=(
                    f"the unit is {status.active_state or 'gone'} "
                    f"({status.result or 'no result'}) and wrote no exit status, "
                    "so it was killed rather than finishing"
                ),
            )

        return Observation(BackendStatus.RUNNING, detail=status.active_state)

    # --------------------------------------------------------------- logs

    def fetch_logs(self, handle: str, after: int = 0) -> list[tuple[str, str]]:
        hostname, job_id = _split(handle)
        host = self.host(hostname)
        if host is None:
            return []
        try:
            result = self.transport.run(
                host,
                commands.tail_output(
                    commands.job_dir(self._root_for(hostname), job_id), after
                ),
                timeout=self.config.poll_timeout_seconds,
            )
        except BackendError:
            return []
        if not result.stdout:
            return []
        # stderr is merged into stdout by the launch wrapper, which is what you
        # see running the same command in a terminal. Keeping them apart would
        # need two cursors that stay consistent across a broker restart, and the
        # ordering between them would still be a guess.
        return [("stdout", line) for line in result.stdout.splitlines()]

    def sample_utilization(self, handle: str) -> list[UtilizationSample]:
        hostname, job_id = _split(handle)
        host = self.host(hostname)
        if host is None:
            return []
        known = self._health.get(hostname)
        if known is not None and known.state is HostState.UNREACHABLE:
            return []
        try:
            result = self.transport.run(
                host,
                commands.utilization(commands.unit_for(job_id), self.config.use_sudo),
                timeout=self.config.poll_timeout_seconds,
            )
        except BackendError:
            return []
        if not result.ok:
            return []
        parsed = commands.parse_utilization(result.stdout)
        if parsed is None:
            # We could not attribute anything to this job. Recording 0% would be
            # indistinguishable from an idle job and could get somebody
            # reclaimed for a failed lookup.
            return []
        return [
            UtilizationSample(
                at=self.clock.now().timestamp(),
                gpu_percent=parsed.gpu_percent,
                memory_mb=parsed.memory_mb,
            )
        ]

    # ---------------------------------------------------------- terminate

    def terminate(self, handle: str, reason: str) -> None:
        hostname, job_id = _split(handle)
        host = self.host(hostname)
        if host is None:
            return
        try:
            self.transport.run(
                host,
                commands.stop_unit(commands.unit_for(job_id), self.config.use_sudo),
                timeout=self.config.poll_timeout_seconds,
            )
        except BackendError:
            # Best effort. A host we cannot reach will be drained by the health
            # check, and reconcile will report whatever is still running on it.
            return

    def validate_terminate(self, handle: str) -> str:
        hostname, job_id = _split(handle)
        if self.host(hostname) is None:
            return f"{hostname} is not configured; nothing would be stopped"
        return f"would stop {commands.unit_for(job_id)} on {hostname}"

    # ------------------------------------------------------ reconciliation

    def list_resources(self) -> list[Resource]:
        found: list[Resource] = []
        for health in self.health():
            if health.state is HostState.UNREACHABLE:
                # We genuinely cannot see this host. Reporting nothing would
                # look like "nothing is running here", which is a different and
                # much more comforting claim than "we do not know".
                continue
            for live in self._live_on(health.hostname):
                job_id = live.get("job-id", "")
                found.append(
                    Resource(
                        handle=_handle(health.hostname, job_id),
                        gpu_type=self.config.gpu_type,
                        status=BackendStatus.RUNNING,
                        tags={
                            "broker-job-id": job_id,
                            "broker-user": live.get("user", ""),
                            "broker-launched-at": live.get("launched-at", ""),
                        }
                        if job_id
                        else {},
                    )
                )
        return found


def _handle(hostname: str, job_id: str) -> str:
    return f"{hostname}/{job_id}"


def _split(handle: str) -> tuple[str, str]:
    hostname, _, job_id = handle.partition("/")
    return hostname, job_id
