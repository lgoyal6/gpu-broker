"""A simulated GPU lab host, reachable over real SSH.

`asyncssh` can run an SSH server in-process, so these tests exercise the actual
client path -- connection pooling, exit statuses, stdout and stderr -- rather
than a hand-written stand-in for it. The server costs about 10ms to start, which
is why the local tests stay fast where the AWS ones do not.

`FakeGpuHost` is the shell on the other end. It parses the real command strings
`gpu_broker.local.commands` produces, so a test failing here means the broker
would send something a real machine would reject.

**What this does not prove.** It simulates the MPS *contract*: each client gets
the memory limit the broker asked for, and one client exceeding its own limit
does not disturb another. It does not run NVIDIA's MPS daemon, and no test can
tell you how a real A6000 behaves under two concurrent training jobs. That
number has to be measured on the actual card, and Phase 7 is where it gets
published.
"""

from __future__ import annotations

import asyncio
import base64
import re
import shlex
import threading
from dataclasses import dataclass, field

import asyncssh

from gpu_broker.local.commands import UNIT_PREFIX
from gpu_broker.local.transport import HostSpec

DEFAULT_GPUS = [
    # index, name, total MB, used MB, util %, compute mode
    (0, "NVIDIA RTX A6000", 49140, 512, 3, "Default"),
]


@dataclass
class Unit:
    name: str
    job_id: str
    user: str
    gpu: int
    env: dict[str, str]
    memory_max_mb: int
    cpu_quota_percent: int
    run_as: str
    command: str
    active: bool = True
    exit_code: int | None = None
    result: str = ""
    output: list[str] = field(default_factory=list)
    gpu_memory_used_mb: int = 0

    @property
    def gpu_memory_limit_mb(self) -> int:
        raw = self.env.get("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT", "")
        match = re.match(r"\d+=(\d+)M", raw)
        return int(match.group(1)) if match else 0


class FakeGpuHost:
    """The machine on the other end of the SSH connection."""

    def __init__(
        self,
        hostname: str = "lab1",
        gpus: list[tuple] | None = None,
        *,
        sudo_works: bool = True,
        limits_apply: bool = True,
        nvidia_smi_works: bool = True,
        mps_running: bool = False,
        mps_can_start: bool = True,
    ) -> None:
        self.hostname = hostname
        self.gpus = list(gpus if gpus is not None else DEFAULT_GPUS)
        self.sudo_works = sudo_works
        self.limits_apply = limits_apply
        self.nvidia_smi_works = nvidia_smi_works
        self.mps_running = mps_running
        self.mps_can_start = mps_can_start

        self.home = "/home/broker"
        self.units: dict[str, Unit] = {}
        self.commands: list[str] = []
        self.mps_starts = 0

    # ------------------------------------------------------ test controls

    def unit_for_job(self, job_id: str) -> Unit:
        unit = self.units.get(UNIT_PREFIX + job_id)
        assert unit is not None, f"no unit for {job_id}. Have: {list(self.units)}"
        return unit

    def emit(self, job_id: str, *lines: str) -> None:
        self.unit_for_job(job_id).output.extend(lines)

    def finish(self, job_id: str, exit_code: int = 0) -> None:
        unit = self.unit_for_job(job_id)
        unit.active = False
        unit.exit_code = exit_code
        unit.result = "success" if exit_code == 0 else "exit-code"

    def kill(self, job_id: str, result: str = "signal") -> None:
        """Stopped without writing an exit status. What a `kill -9` looks like."""
        unit = self.unit_for_job(job_id)
        unit.active = False
        unit.exit_code = None
        unit.result = result

    def oom(self, job_id: str) -> None:
        self.kill(job_id, result="oom-kill")

    def allocate_gpu_memory(self, job_id: str, megabytes: int) -> bool:
        """A job asks MPS for device memory.

        Refused if it would take the client past the limit the broker set for
        it. Deliberately per client: another job's usage is irrelevant, which is
        the entire point of a per-client limit.
        """
        unit = self.unit_for_job(job_id)
        if unit.gpu_memory_used_mb + megabytes > unit.gpu_memory_limit_mb:
            return False
        unit.gpu_memory_used_mb += megabytes
        return True

    def gpu_memory_in_use(self, gpu: int = 0) -> int:
        return sum(u.gpu_memory_used_mb for u in self.units.values() if u.gpu == gpu and u.active)

    def active_units(self) -> list[Unit]:
        return [unit for unit in self.units.values() if unit.active]

    # ---------------------------------------------------------- the shell

    def handle(self, command: str) -> tuple[int, str, str]:
        self.commands.append(command)
        command = command.replace("systemctl --user ", "systemctl ").replace(
            "systemd-run --user ", "systemd-run "
        )

        if command.startswith("sudo -n ") and not self.sudo_works:
            return 1, "", "sudo: a password is required\n"
        bare = command.removeprefix("sudo -n ")
        # The user manager is the same manager as far as this simulator is
        # concerned; what matters is that the broker addressed one of them.
        bare = bare.replace("systemctl --user ", "systemctl ").replace(
            "systemd-run --user ", "systemd-run "
        )

        if command.startswith("printf '%s\\n' ok "):
            return 0, f"ok\n{self.home}\n", ""

        if command.startswith("nvidia-smi --query-gpu"):
            if not self.nvidia_smi_works:
                return 9, "", "NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver\n"
            rows = "\n".join(
                f"{i}, {name}, {total}, {used + self.gpu_memory_in_use(i)}, {util}, {mode}"
                for (i, name, total, used, util, mode) in self.gpus
            )
            return 0, rows + "\n", ""

        if "/proc/self/cgroup" in bare and "memory.max" in bare:
            # The limit probe. `systemd-run` exits zero either way; what
            # differs is whether the scope actually got the cap.
            return 0, ("67108864\n" if self.limits_apply else "max\n"), ""

        if "nvidia-cuda-mps-control -d" in command:
            if not self.mps_can_start:
                return 1, "", "An instance of this daemon is already running\n"
            self.mps_running = True
            self.mps_starts += 1
            return 0, "", ""

        if "get_server_list" in command:
            if not self.mps_running:
                return 1, "", "cannot open pipe\n"
            return 0, "\n", ""

        if bare.startswith("systemd-run --unit="):
            return self._launch(bare)

        if command.startswith("printf 'exitfile="):
            return self._status(command)

        if command.startswith("tail -n +"):
            return self._tail(command)

        if "systemctl stop " in command:
            return self._stop(command)

        if command.startswith("for d in "):
            return self._list_live(command)

        if "--property=MemoryMax --property=CPUQuotaPerSecUSec" in bare:
            return self._show_limits(bare)

        # A real shell has `echo`. Kept last so the piped MPS commands above,
        # which also start with `echo`, are matched first.
        if command.startswith("echo ") and "|" not in command:
            return 0, command[len("echo ") :].strip().strip("'\"") + "\n", ""

        return 127, "", f"bash: {command.split()[0]}: command not found\n"

    # ------------------------------------------------------------ handlers

    def _launch(self, command: str) -> tuple[int, str, str]:
        """Parse the launch the way a shell would, not with regexes.

        The inner script is `shlex.quote`d into `/bin/bash -lc '...'`, so its own
        single quotes come through as `'"'"'`. Tokenising the whole line and
        pulling out the `-lc` argument unwraps that correctly; pattern-matching
        the raw string does not, and a double that mis-parses is worse than no
        double at all.
        """
        tokens = shlex.split(command)
        unit_name = _from_tokens(tokens, "--unit=") or ""
        memory = int((_from_tokens(tokens, "--property=MemoryMax=") or "0M").rstrip("M"))
        cpu = int((_from_tokens(tokens, "--property=CPUQuota=") or "0%").rstrip("%"))
        run_as = _from_tokens(tokens, "--uid=") or ""
        env = {}
        for token in tokens:
            if token.startswith("--setenv="):
                key, _, value = token.removeprefix("--setenv=").partition("=")
                env[key] = value

        inner = tokens[tokens.index("-lc") + 1] if "-lc" in tokens else ""
        inner_tokens = shlex.split(inner)

        fields: dict[str, str] = {}
        if "printf" in inner_tokens:
            after_format = inner_tokens[inner_tokens.index("printf") + 2 :]
            for token in after_format:
                if token == ">":
                    break
                if "=" in token:
                    key, _, value = token.partition("=")
                    fields[key] = value

        payload = _group(inner, r"echo ([A-Za-z0-9+/=]+) \| base64 -d")
        script = base64.b64decode(payload).decode() if payload else ""

        if unit_name in self.units and self.units[unit_name].active:
            return 1, "", f"Unit {unit_name}.service already exists.\n"

        self.units[unit_name] = Unit(
            name=unit_name,
            job_id=fields.get("job-id", unit_name.removeprefix(UNIT_PREFIX)),
            user=fields.get("user", ""),
            gpu=int(fields.get("gpu", 0) or 0),
            env=env,
            memory_max_mb=memory,
            cpu_quota_percent=cpu,
            run_as=run_as,
            command=script,
        )
        return 0, "", f"Running as unit: {unit_name}.service\n"

    def _status(self, command: str) -> tuple[int, str, str]:
        unit_name = _group(command, r"systemctl show (\S+) --property=ActiveState")
        unit = self.units.get(unit_name or "")
        if unit is None:
            return 0, "exitfile=\nActiveState=inactive\nSubState=dead\nResult=\nExecMainStatus=\n", ""
        exit_text = "" if unit.exit_code is None else str(unit.exit_code)
        state = "active" if unit.active else ("failed" if unit.result not in ("success", "") else "inactive")
        return 0, (
            f"exitfile={exit_text}\n"
            f"ActiveState={state}\n"
            f"SubState={'running' if unit.active else 'dead'}\n"
            f"Result={unit.result}\n"
            f"ExecMainStatus={exit_text}\n"
        ), ""

    def _tail(self, command: str) -> tuple[int, str, str]:
        start = int(_group(command, r"tail -n \+(\d+)") or 1)
        directory = _group(command, r"tail -n \+\d+ '?([^' ]+)'?/output\.log") or ""
        job_id = directory.rstrip("/").rsplit("/", 1)[-1]
        unit = self.units.get(UNIT_PREFIX + job_id)
        if unit is None:
            return 0, "", ""
        lines = unit.output[start - 1 :]
        return 0, ("\n".join(lines) + "\n" if lines else ""), ""

    def _stop(self, command: str) -> tuple[int, str, str]:
        unit_name = _group(command, r"systemctl stop (\S+)")
        unit = self.units.get(unit_name or "")
        if unit is not None and unit.active:
            unit.active = False
            unit.result = "signal"
        return 0, "", ""

    def _list_live(self, command: str) -> tuple[int, str, str]:
        out = []
        for unit in self.units.values():
            if not unit.active:
                continue
            out.append(f"### {unit.job_id}")
            out.append(f"job-id={unit.job_id}")
            out.append(f"user={unit.user}")
            out.append(f"gpu={unit.gpu}")
        return 0, ("\n".join(out) + "\n" if out else ""), ""

    def _show_limits(self, command: str) -> tuple[int, str, str]:
        unit_name = _group(command, r"systemctl show (\S+) ")
        unit = self.units.get(unit_name or "")
        if unit is None:
            return 0, "MemoryMax=infinity\nCPUQuotaPerSecUSec=infinity\n", ""
        return 0, (
            f"MemoryMax={unit.memory_max_mb * 1024 * 1024}\n"
            f"CPUQuotaPerSecUSec={unit.cpu_quota_percent * 10_000}us\n"
        ), ""


def _from_tokens(tokens: list[str], prefix: str) -> str | None:
    for token in tokens:
        if token.startswith(prefix):
            return token.removeprefix(prefix)
    return None


def _group(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text)
    return match.group(1) if match else None


# --------------------------------------------------------------- the server


class _Server(asyncssh.SSHServer):
    def begin_auth(self, username: str) -> bool:
        return False  # the test server takes anyone


class SshHostServer:
    """Runs `FakeGpuHost` behind a real SSH listener on localhost."""

    def __init__(self, host: FakeGpuHost) -> None:
        self.host = host
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self.port = asyncio.run_coroutine_threadsafe(self._start(), self._loop).result(10)

    async def _start(self) -> int:
        key = asyncssh.generate_private_key("ssh-ed25519")

        async def process(proc):
            code, out, err = self.host.handle(proc.command or "")
            if out:
                proc.stdout.write(out)
            if err:
                proc.stderr.write(err)
            proc.exit(code)

        self._server = await asyncssh.listen(
            "127.0.0.1", 0, server_factory=_Server,
            server_host_keys=[key], process_factory=process,
        )
        return self._server.sockets[0].getsockname()[1]

    def spec(self) -> HostSpec:
        return HostSpec(hostname="127.0.0.1", username="broker", port=self.port)

    def close(self) -> None:
        """Idempotent: tests close the server to simulate a host vanishing, and
        the fixture closes it again on the way out."""
        if self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._server.close)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()
