"""Every shell command the broker sends to a lab host, in one file.

Kept together for two reasons. Tests can assert on the exact string that will
reach a real machine, rather than on a paraphrase. And when something behaves
oddly on the real host, the thing to read is one page long.

Nothing here runs anything. These are strings and parsers.
"""

from __future__ import annotations

import base64
import shlex
from dataclasses import dataclass

UNIT_PREFIX = "gpu-broker-"
"""Every transient unit the broker creates starts with this, so `list_units`
can find ours and only ours."""


def unit_for(job_id: str) -> str:
    """The unit and the directory both use the *full* job id.

    Truncating would make unit names prettier and would mean a resource found on
    the host could only be matched back to a job by prefix search. An orphan has
    to be attributable without ambiguity, so the whole id goes in both places.
    """
    return f"{UNIT_PREFIX}{job_id}"


def job_dir(root: str, job_id: str) -> str:
    return f"{root}/{job_id}"


def job_id_of(unit: str) -> str:
    return unit.removeprefix(UNIT_PREFIX).removesuffix(".service")


# --- inspecting the machine -------------------------------------------------

HELLO = 'printf \'%s\\n\' ok "$HOME"'
"""Liveness and the SSH user's home directory, in one round trip.

The home directory has to come from the host, not from config. Paths are passed
through `shlex.quote`, which wraps anything containing a tilde in single quotes,
and a quoted tilde is not expanded by any shell -- the machine would end up with
a directory literally named `~`. Resolving it once here means every path the
broker sends afterwards is absolute.
"""


def parse_hello(stdout: str) -> str | None:
    lines = [line.strip() for line in stdout.strip().splitlines()]
    if len(lines) < 2 or lines[0] != "ok":
        return None
    return lines[1] or None


def expand(path: str, home: str) -> str:
    if path == "~":
        return home
    if path.startswith("~/"):
        return f"{home}/{path[2:]}"
    return path


GPU_QUERY = (
    "nvidia-smi --query-gpu=index,name,memory.total,memory.used,"
    "utilization.gpu,compute_mode --format=csv,noheader,nounits"
)


@dataclass(frozen=True)
class GpuInfo:
    index: int
    name: str
    memory_total_mb: int
    memory_used_mb: int
    utilization_percent: int
    compute_mode: str

    @property
    def supports_mps(self) -> bool:
        """MPS needs to be able to open the device.

        `Prohibited` blocks every context, MPS included. `Exclusive_Process` is
        the mode NVIDIA recommends for multi-user MPS, since it stops anything
        from bypassing the daemon, but `Default` works too and is what an
        unconfigured card ships as.
        """
        return self.compute_mode in ("Default", "Exclusive_Process", "Exclusive_Thread")


def parse_gpus(stdout: str) -> list[GpuInfo]:
    gpus: list[GpuInfo] = []
    for line in stdout.strip().splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 6:
            continue
        try:
            gpus.append(
                GpuInfo(
                    index=int(parts[0]),
                    name=parts[1],
                    memory_total_mb=int(float(parts[2])),
                    memory_used_mb=int(float(parts[3])),
                    utilization_percent=int(float(parts[4])),
                    compute_mode=parts[5],
                )
            )
        except ValueError:
            continue
    return gpus


# --- CUDA MPS ---------------------------------------------------------------
#
# The target card is an RTX A6000, which does not support MIG. So the isolation
# mechanisms available are MPS and time-slicing, and MPS is the one that lets
# kernels from different processes run concurrently rather than taking turns.


def mps_env(pipe_dir: str, log_dir: str) -> str:
    return f"CUDA_MPS_PIPE_DIRECTORY={shlex.quote(pipe_dir)} CUDA_MPS_LOG_DIRECTORY={shlex.quote(log_dir)}"


def mps_running(pipe_dir: str) -> str:
    """Is the control daemon up and answering?

    `pgrep` would say whether a process exists; asking the daemon for its server
    list says whether it is actually usable, which is the thing that matters.
    """
    return f"echo get_server_list | {mps_env(pipe_dir, pipe_dir)} nvidia-cuda-mps-control 2>&1"


def mps_start(pipe_dir: str, log_dir: str) -> str:
    return (
        f"mkdir -p {shlex.quote(pipe_dir)} {shlex.quote(log_dir)} && "
        f"{mps_env(pipe_dir, log_dir)} nvidia-cuda-mps-control -d"
    )


def mps_stop(pipe_dir: str) -> str:
    return f"echo quit | {mps_env(pipe_dir, pipe_dir)} nvidia-cuda-mps-control"


def mps_client_env(
    pipe_dir: str, device: int, memory_mb: int, thread_percent: int
) -> dict[str, str]:
    """What a job's process needs in its environment to be an MPS client.

    `CUDA_MPS_PINNED_DEVICE_MEM_LIMIT` is the per-client cap, and it is set on
    the client rather than on the daemon precisely so two jobs on one GPU can
    have different limits. The daemon-wide default would give them the same one.
    """
    return {
        "CUDA_MPS_PIPE_DIRECTORY": pipe_dir,
        "CUDA_MPS_PINNED_DEVICE_MEM_LIMIT": f"{device}={memory_mb}M",
        "CUDA_MPS_ACTIVE_THREAD_PERCENTAGE": str(thread_percent),
        "CUDA_VISIBLE_DEVICES": str(device),
    }


# --- cgroup v2, through systemd ---------------------------------------------


def sudo(command: str, use_sudo: bool = True) -> str:
    """`-n` so a host that would prompt for a password fails immediately rather
    than hanging a tick until the SSH timeout."""
    return f"sudo -n {command}" if use_sudo else command


PROBE_MEMORY_BYTES = 67_108_864  # 64 MiB


def limit_probe(use_sudo: bool = True) -> str:
    """Prove that cgroup limits actually take effect on this host.

    Runs a throwaway scope with a known memory cap and has it read its own
    cgroup back. Checking that `systemd-run` merely exits zero is not enough:
    on a host without the memory controller delegated it exits zero and applies
    nothing, and the first thing anyone would learn about it is a job taking the
    whole machine down.
    """
    return sudo(
        "systemd-run --scope --quiet -p MemoryMax=64M -- "
        "cat /sys/fs/cgroup/memory.max",
        use_sudo,
    )


def parse_limit_probe(stdout: str) -> int | None:
    text = stdout.strip().splitlines()
    if not text:
        return None
    try:
        return int(text[-1].strip())
    except ValueError:
        return None


def launch(
    *,
    unit: str,
    directory: str,
    working_dir: str,
    command: str,
    env: dict[str, str],
    memory_max_mb: int,
    cpu_quota_percent: int,
    run_as: str,
    meta: tuple[str, ...],
    use_sudo: bool = True,
) -> str:
    """Start a job as a transient systemd service under cgroup v2 limits.

    A *service*, not a scope, because a scope runs in the foreground and this
    has to outlive the SSH call.

    The user's command is base64-encoded rather than quoted into the string.
    Somebody will eventually submit something containing a single quote, and the
    failure mode of getting that wrong is executing a fragment of their command
    as a separate shell word.

    Output and exit status go to files rather than to the journal. The journal
    would work, but transient units are garbage-collected once they succeed, and
    with them the record of what the job printed and what it returned.
    """
    encoded = base64.b64encode(command.encode()).decode()
    inner = (
        f"mkdir -p {shlex.quote(directory)} && "
        # Written from inside the unit, so it exists for as long as the unit
        # does. This is the local equivalent of tagging an EC2 instance at
        # creation: reconciliation reads it, and without it a running job is
        # something nobody can be asked about.
        f"printf '%s\n' {' '.join(shlex.quote(line) for line in meta)} "
        f"> {shlex.quote(directory)}/meta && "
        f"cd {shlex.quote(working_dir)} && "
        f"echo {encoded} | base64 -d > {shlex.quote(directory)}/run.sh && "
        f"bash {shlex.quote(directory)}/run.sh > {shlex.quote(directory)}/output.log 2>&1; "
        f"echo $? > {shlex.quote(directory)}/exit"
    )
    settings = " ".join(
        f"--setenv={key}={shlex.quote(value)}" for key, value in sorted(env.items())
    )
    return sudo(
        f"systemd-run --unit={unit} --service-type=exec "
        f"--property=MemoryMax={memory_max_mb}M "
        f"--property=MemorySwapMax=0 "
        f"--property=CPUQuota={cpu_quota_percent}% "
        f"--uid={shlex.quote(run_as)} "
        f"{settings} "
        f"/bin/bash -lc {shlex.quote(inner)}",
        use_sudo,
    )


def unit_status(unit: str, directory: str) -> str:
    """One round trip for both halves of 'is it done, and how did it go'.

    The exit file is the authority. The unit's own state is a fallback for the
    case where the job was killed and never got to write one.
    """
    return (
        f"printf 'exitfile=%s\\n' \"$(cat {shlex.quote(directory)}/exit 2>/dev/null)\"; "
        f"systemctl show {unit} --property=ActiveState --property=SubState "
        f"--property=Result --property=ExecMainStatus 2>/dev/null || true"
    )


@dataclass(frozen=True)
class UnitStatus:
    exit_code: int | None
    active_state: str
    sub_state: str
    result: str

    @property
    def finished(self) -> bool:
        return self.exit_code is not None or self.active_state in ("inactive", "failed")

    @property
    def was_oom_killed(self) -> bool:
        return self.result == "oom-kill"


def parse_unit_status(stdout: str) -> UnitStatus:
    fields: dict[str, str] = {}
    for line in stdout.strip().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            fields[key.strip()] = value.strip()

    raw_exit = fields.get("exitfile", "")
    exit_code: int | None = None
    if raw_exit:
        try:
            exit_code = int(raw_exit)
        except ValueError:
            exit_code = None

    return UnitStatus(
        exit_code=exit_code,
        active_state=fields.get("ActiveState", ""),
        sub_state=fields.get("SubState", ""),
        result=fields.get("Result", ""),
    )


def verify_limits(unit: str) -> str:
    return f"systemctl show {unit} --property=MemoryMax --property=CPUQuotaPerSecUSec"


def tail_output(directory: str, after: int) -> str:
    """Lines from `after` onward. `tail -n +N` is 1-indexed, hence the +1."""
    return f"tail -n +{after + 1} {shlex.quote(directory)}/output.log 2>/dev/null || true"


def utilization(unit: str) -> str:
    """What this job's processes are doing to the GPU, in one round trip.

    Per-process, not per-card. Under MPS two users share one A6000, so the
    card's own utilization says nothing about whose job is working -- reclaiming
    on that number would kill an idle job's busy neighbour.

    `pmon` gives SM utilization per process; `query-compute-apps` gives memory
    per process. Both are matched against the PIDs in the job's cgroup, which is
    what ties a process back to a job.
    """
    return (
        "printf '###PIDS\n'; "
        f"cg=$(systemctl show {unit} --property=ControlGroup --value 2>/dev/null); "
        '[ -n "$cg" ] && cat "/sys/fs/cgroup$cg/cgroup.procs" 2>/dev/null; '
        "printf '###PMON\n'; "
        "nvidia-smi pmon -c 1 -s u 2>/dev/null || true; "
        "printf '###MEM\n'; "
        "nvidia-smi --query-compute-apps=pid,used_gpu_memory "
        "--format=csv,noheader,nounits 2>/dev/null || true"
    )


@dataclass(frozen=True)
class Utilization:
    gpu_percent: float
    memory_mb: int
    processes: int


def parse_utilization(stdout: str) -> Utilization | None:
    """Sum this job's processes. None means nvidia-smi told us nothing usable."""
    section = ""
    pids: set[str] = set()
    sm_by_pid: dict[str, float] = {}
    mem_by_pid: dict[str, int] = {}

    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("###"):
            section = stripped
            continue
        if not stripped or stripped.startswith("#"):
            continue

        if section == "###PIDS":
            if stripped.isdigit():
                pids.add(stripped)
        elif section == "###PMON":
            parts = stripped.split()
            # gpu, pid, type, sm, ...  with '-' for idle columns
            if len(parts) >= 4 and parts[1].isdigit():
                sm_by_pid[parts[1]] = _percent(parts[3])
        elif section == "###MEM":
            parts = [part.strip() for part in stripped.split(",")]
            if len(parts) >= 2 and parts[0].isdigit():
                try:
                    mem_by_pid[parts[0]] = int(float(parts[1]))
                except ValueError:
                    continue

    if not pids:
        # No cgroup, so nothing can be attributed. Saying "0%" here would look
        # exactly like an idle job and get somebody reclaimed for it.
        return None

    ours = pids & (set(sm_by_pid) | set(mem_by_pid))
    return Utilization(
        gpu_percent=sum(sm_by_pid.get(pid, 0.0) for pid in ours),
        memory_mb=sum(mem_by_pid.get(pid, 0) for pid in ours),
        processes=len(ours),
    )


def _percent(raw: str) -> float:
    try:
        return float(raw)
    except ValueError:
        return 0.0  # nvidia-smi writes '-' when a column does not apply


def stop_unit(unit: str, use_sudo: bool = True) -> str:
    """Stop, then clear. Without `reset-failed`, a unit that exited non-zero
    stays in the failed state and the name cannot be reused."""
    return (
        sudo(f"systemctl stop {unit}", use_sudo)
        + " >/dev/null 2>&1; "
        + sudo(f"systemctl reset-failed {unit}", use_sudo)
        + " >/dev/null 2>&1; true"
    )


def list_units(use_sudo: bool = False) -> str:
    return sudo(
        f"systemctl list-units '{UNIT_PREFIX}*' --all --no-legend --plain --no-pager",
        use_sudo,
    )


def parse_units(stdout: str) -> list[str]:
    units: list[str] = []
    for line in stdout.strip().splitlines():
        parts = line.split()
        if parts and parts[0].startswith(UNIT_PREFIX):
            units.append(parts[0].removesuffix(".service"))
    return units


def list_live_jobs(root: str) -> str:
    """Every job directory whose unit is still active, with its metadata.

    One round trip. A directory whose unit has gone is finished work, not a
    resource, so it is skipped here -- `reap` is what notices a directory that
    outlived its unit.
    """
    return (
        f"for d in {shlex.quote(root)}/*/; do "
        f'[ -d "$d" ] || continue; '
        f'id=$(basename "$d"); '
        f'if systemctl is-active --quiet {UNIT_PREFIX}$id 2>/dev/null; then '
        f'printf "### %s\n" "$id"; cat "$d/meta" 2>/dev/null; '
        f"fi; done"
    )


def parse_live_jobs(stdout: str) -> list[dict[str, str]]:
    jobs: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in stdout.splitlines():
        if line.startswith("### "):
            current = {"job-id": line[4:].strip()}
            jobs.append(current)
        elif current is not None and "=" in line:
            key, _, value = line.partition("=")
            current[key.strip()] = value.strip()
    return jobs


def meta_lines(job_id: str, user_id: str, launched_at: str, gpu: int) -> tuple[str, ...]:
    """The local equivalent of an EC2 tag set.

    Returned as separate lines, and written with one `printf` argument each,
    because `printf '%s\\n' 'a\\nb'` does not do what it looks like: the format
    string's escapes are interpreted, the argument's are not, so a single packed
    string lands in the file as a literal backslash-n and the metadata parses as
    one unreadable line.
    """
    return (
        f"job-id={job_id}",
        f"user={user_id}",
        f"launched-at={launched_at}",
        f"gpu={gpu}",
    )


def remove_job_dir(root: str, job_id: str) -> str:
    return f"rm -rf {shlex.quote(job_dir(root, job_id))}"
