"""The interface every kind of capacity sits behind.

Cloud, lab machine, and free-tier all satisfy this. So does the fake, which is
what makes "every phase must run with no AWS account" true rather than aspirational.

Backends do not know about budgets, fair-share, or the queue. They launch
things, report what they see, and stop things. All policy lives above them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from ..models import Job
from ..money import Currency


class BackendStatus(StrEnum):
    """What a backend says about one allocation. Deliberately smaller than
    `JobState`: the backend reports facts, the broker decides what they mean."""

    PENDING = "PENDING"
    """Machine is coming up. Not usable yet."""
    READY = "READY"
    """The machine is up and can accept work, but the command has not been
    started. EC2 genuinely has this phase: the instance is running and billing
    for several minutes before the SSM agent registers and anything can be sent
    to it. Naming it keeps the mutation that starts a job in `start()`, where it
    can be dry-run, instead of hidden inside `poll()`."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    """Capacity was taken away by the provider. Phase 4 acts on this; Phase 0
    treats it as a failure."""
    GONE = "GONE"
    """The backend has no record of this handle. Either it was cleaned up, or
    the broker is holding a handle that never existed."""


TERMINAL_STATUSES = frozenset(
    {
        BackendStatus.COMPLETED,
        BackendStatus.FAILED,
        BackendStatus.INTERRUPTED,
        BackendStatus.GONE,
    }
)


@dataclass(frozen=True)
class Allocation:
    """What a backend hands back when it accepts a job."""

    handle: str
    """Opaque to the broker. An instance id, a host:pid, a container id."""
    gpu_type: str
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Observation:
    """One poll of one allocation."""

    status: BackendStatus
    exit_code: int | None = None
    detail: str | None = None


@dataclass(frozen=True)
class UtilizationSample:
    """One reading of a GPU's business, timestamped by the machine that took it."""

    at: float
    """Unix seconds, from the host. Not the broker's clock: the sample happened
    on the machine, possibly minutes before anything read it."""
    gpu_percent: float
    memory_mb: int = 0


@dataclass(frozen=True)
class Resource:
    """Something a backend is holding right now.

    Reconciliation compares this list against the broker's records. Every
    resource carries the tags that say which job and user it belongs to, so an
    orphan is always attributable to a person rather than a mystery.
    """

    handle: str
    gpu_type: str
    status: BackendStatus
    tags: dict[str, str]

    @property
    def job_id(self) -> str | None:
        return self.tags.get("broker-job-id")

    @property
    def user_id(self) -> str | None:
        return self.tags.get("broker-user")


@runtime_checkable
class Backend(Protocol):
    """Implement this and the scheduler can use your capacity."""

    name: str
    currency: Currency
    """What running here costs. Cloud backends bill dollars; the lab machine
    bills GPU-hours."""

    tier: str
    """Which rung of the placement policy this is: `local`, `spot`, or
    `ondemand`. The policy is an ordered list of these, so adding a backend
    means declaring where it sits rather than editing the scheduler."""

    def supports(self, gpu_type: str) -> bool: ...

    def free_slots(self, gpu_type: str) -> int:
        """How many more jobs of this type could start right now."""
        ...

    def launch(self, job: Job) -> Allocation:
        """Acquire capacity and start the job's command. May raise BackendError."""
        ...

    def start(self, handle: str, job: Job) -> None:
        """Begin executing the job's command on an allocation that is READY.

        Called exactly once per allocation, by the scheduler, when `poll`
        reports READY. Backends that have nothing to do here (the command was
        part of the launch) never report READY and may leave this unimplemented.
        """
        ...

    def poll(self, handle: str) -> Observation:
        """Read-only. Must not start, stop, or change anything."""
        ...

    def validate_launch(self, job: Job) -> str:
        """Check that `launch` would succeed, without launching.

        Returns a description of what would happen. Backends that can ask the
        provider -- EC2 has `DryRun` -- should, because the failure worth
        catching before the first real deploy is a missing IAM permission, and
        only the provider knows about those.
        """
        ...

    def validate_terminate(self, handle: str) -> str:
        """Check that `terminate` would succeed, without terminating."""
        ...

    def fetch_logs(self, handle: str, after: int = 0) -> list[tuple[str, str]]:
        """New (stream, line) pairs since offset `after`."""
        ...

    def terminate(self, handle: str, reason: str) -> None:
        """Stop and release. Must be safe to call on an already-dead handle."""
        ...

    def list_resources(self) -> list[Resource]:
        """Everything this backend is holding, for reconciliation."""
        ...

    def sample_utilization(self, handle: str) -> list["UtilizationSample"]:
        """What this allocation's GPU has actually been doing.

        Returns whatever samples are newly available, oldest first, possibly
        none. Each backend gets these the way that suits its hardware: over SSH
        for a machine we can reach, out of a log stream for one we cannot.

        Read-only, like `poll`. Called on every running job.
        """
        ...


def tags_for(job: Job, launched_at_iso: str) -> dict[str, str]:
    """The tags every backend must stamp on every resource it creates.

    Reconciliation works off these three keys and nothing else, so they are
    defined once, here, rather than per backend.
    """
    return {
        "broker-job-id": job.job_id,
        "broker-user": job.user_id,
        "broker-launched-at": launched_at_iso,
    }
