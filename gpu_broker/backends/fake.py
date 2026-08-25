"""Capacity that does not exist, behaving exactly like capacity that does.

Our G and P vCPU quota may be zero, so every phase has to be runnable and
testable with no AWS account. This backend is how. It is driven by an injected
clock, so a four-hour job finishes in a microsecond, and a hundred simulated
spot interruptions run in under a second.

Two things here are not conveniences and should not be simplified away:

*Its state lives in a file, not in the object.* A real cloud keeps running your
instances while your broker is dead. If the fake forgot everything on restart,
reconciliation would always find a clean slate and the drift-detection code
would never actually be exercised.

*Outcomes are scripted, not random.* A test that says "this job fails at 2.5
hours" is a test you can read. `random` in a backend is a flaky suite.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from ..clock import Clock, to_iso
from ..errors import BackendError
from ..models import Job
from ..money import Currency
from .base import (
    Allocation,
    BackendStatus,
    Observation,
    Resource,
    UtilizationSample,
    tags_for,
)


@dataclass
class Plan:
    """What a scripted job is going to do. Set by tests before dispatch."""

    run_hours: float | None = None
    """None means 'take exactly as long as it asked for'."""
    outcome: BackendStatus = BackendStatus.COMPLETED
    exit_code: int = 0
    launch_error: str | None = None
    detail: str | None = None

    total_steps: int = 0
    """Simulated work. Non-zero turns on the checkpoint model below: the job
    advances one step at a time, saves periodically, and on resume carries on
    from where it stopped."""
    checkpoint_every: int = 0


@dataclass
class _Resource:
    handle: str
    job_id: str
    user_id: str
    gpu_type: str
    launched_at: float
    ready_at: float
    finish_at: float
    outcome: str
    exit_code: int
    tags: dict[str, str]
    logs: list[list[str]] = field(default_factory=list)
    terminated: bool = False
    terminate_reason: str | None = None
    logged_start: bool = False
    total_steps: int = 0
    checkpoint_every: int = 0
    start_step: int = 0
    """Where this attempt began. Non-zero means it resumed from a checkpoint."""
    start_value: int = 0
    last_saved_step: int = -1

    def to_json(self) -> dict:
        data = self.__dict__.copy()
        return data

    @classmethod
    def from_json(cls, data: dict) -> "_Resource":
        return cls(**data)


class FakeBackend:
    """A `Backend` that simulates machines against an injected clock."""

    def __init__(
        self,
        name: str = "fake",
        *,
        clock: Clock,
        checkpoints: object | None = None,
        currency: Currency = Currency.USD,
        capacity: dict[str, int] | None = None,
        startup_seconds: float = 60.0,
        state_path: Path | str | None = None,
        tier: str = "ondemand",
    ) -> None:
        self.name = name
        self.clock = clock
        self.tier = tier
        self.checkpoints = checkpoints
        self.currency = currency
        self.capacity = dict(capacity or {"a10g": 2, "t4": 2, "l4": 1, "a100": 1})
        self.startup_seconds = startup_seconds
        self.state_path = Path(state_path) if state_path else None

        self.plans: dict[str, Plan] = {}
        self.utilization: dict[str, float] = {}
        self._resources: dict[str, _Resource] = {}
        self._counter = 0
        self._load()

    # ------------------------------------------------------------- scripting

    def plan(self, job_id: str, **kwargs) -> Plan:
        """Decide in advance what a job will do. Tests call this; nothing else does."""
        plan = Plan(**kwargs)
        self.plans[job_id] = plan
        return plan

    # --------------------------------------------------------- Backend protocol

    def supports(self, gpu_type: str) -> bool:
        return gpu_type in self.capacity

    def free_slots(self, gpu_type: str) -> int:
        if not self.supports(gpu_type):
            return 0
        in_use = sum(
            1
            for resource in self._resources.values()
            if resource.gpu_type == gpu_type and not self._is_finished(resource)
        )
        return max(0, self.capacity[gpu_type] - in_use)

    def launch(self, job: Job) -> Allocation:
        if not self.supports(job.gpu_type):
            raise BackendError(f"{self.name} has no {job.gpu_type} capacity configured")
        if self.free_slots(job.gpu_type) <= 0:
            raise BackendError(f"{self.name} has no free {job.gpu_type} slots")

        plan = self.plans.get(job.job_id, Plan())
        if plan.launch_error:
            raise BackendError(plan.launch_error)

        # Pick up where a previous attempt left off. This is the whole point of
        # the model: if resumption is wrong, the accumulator below ends up
        # somewhere other than sum(range(total_steps)).
        start_step, start_value = self._restore(job)

        now = self.clock.now()
        self._counter += 1
        handle = f"{self.name}-{self._counter:04d}"
        run_hours = plan.run_hours if plan.run_hours is not None else job.requested_hours

        epoch = now.timestamp()
        resource = _Resource(
            handle=handle,
            job_id=job.job_id,
            user_id=job.user_id,
            gpu_type=job.gpu_type,
            launched_at=epoch,
            ready_at=epoch + self.startup_seconds,
            finish_at=epoch + self.startup_seconds + run_hours * 3600.0,
            outcome=str(plan.outcome),
            exit_code=plan.exit_code,
            tags=tags_for(job, to_iso(now)),
            total_steps=plan.total_steps,
            checkpoint_every=plan.checkpoint_every,
            start_step=start_step,
            start_value=start_value,
        )
        resource.logs.append(["broker", f"launched {handle} ({job.gpu_type})"])
        self._resources[handle] = resource
        self._save_state()
        return Allocation(handle=handle, gpu_type=job.gpu_type, tags=dict(resource.tags))

    def start(self, handle: str, job: Job) -> None:
        """Nothing to do. The fake's command begins the moment it is ready, so
        it never reports READY and this is never called."""

    def validate_launch(self, job: Job) -> str:
        if not self.supports(job.gpu_type):
            raise BackendError(f"{self.name} has no {job.gpu_type} capacity configured")
        if self.free_slots(job.gpu_type) <= 0:
            raise BackendError(f"{self.name} has no free {job.gpu_type} slots")
        plan = self.plans.get(job.job_id, Plan())
        if plan.launch_error:
            raise BackendError(plan.launch_error)
        hours = plan.run_hours if plan.run_hours is not None else job.requested_hours
        return f"would simulate {job.gpu_type} on {self.name} for {hours:g}h"

    def validate_terminate(self, handle: str) -> str:
        if handle not in self._resources:
            return f"{handle} is already gone"
        return f"would terminate {handle}"

    def poll(self, handle: str) -> Observation:
        resource = self._resources.get(handle)
        if resource is None:
            return Observation(BackendStatus.GONE, detail="no such handle")

        now = self.clock.now().timestamp()
        if resource.terminated:
            return Observation(
                BackendStatus.GONE, detail=resource.terminate_reason or "terminated"
            )
        if now < resource.ready_at:
            return Observation(BackendStatus.PENDING, detail="booting")
        if now < resource.finish_at:
            self._maybe_checkpoint(resource)
            if not resource.logged_start:
                resource.logs.append(["stdout", f"{resource.job_id[:8]}: training started"])
                resource.logged_start = True
                self._save_state()
            return Observation(BackendStatus.RUNNING)

        if resource.total_steps > 0 and resource.outcome == str(BackendStatus.COMPLETED):
            # The last step, so a completed job's accumulator is the full sum.
            self._save(resource, resource.total_steps)

        status = BackendStatus(resource.outcome)
        return Observation(
            status,
            exit_code=resource.exit_code if status is BackendStatus.COMPLETED else resource.exit_code,
            detail=f"finished as {status}",
        )

    def fetch_logs(self, handle: str, after: int = 0) -> list[tuple[str, str]]:
        resource = self._resources.get(handle)
        if resource is None:
            return []
        # Append the closing line exactly once, when the job is actually over.
        if (
            self.clock.now().timestamp() >= resource.finish_at
            and not resource.terminated
            and (not resource.logs or resource.logs[-1][1] != _closing_line(resource))
        ):
            resource.logs.append(["stdout", _closing_line(resource)])
            self._save_state()
        return [(stream, line) for stream, line in resource.logs[after:]]

    def terminate(self, handle: str, reason: str) -> None:
        resource = self._resources.get(handle)
        if resource is None:
            return  # already gone; terminating twice is not an error
        resource.terminated = True
        resource.terminate_reason = reason
        resource.logs.append(["broker", f"terminated: {reason}"])
        self._save_state()

    def sample_utilization(self, handle: str) -> list[UtilizationSample]:
        """Whatever a test said this job is doing.

        Defaults to busy. An idle job has to be asked for explicitly, so a test
        that forgets is a test about a working job rather than a surprise
        reclaim.
        """
        resource = self._resources.get(handle)
        if resource is None or self.clock.now().timestamp() < resource.ready_at:
            return []
        percent = self.utilization.get(resource.job_id, 85.0)
        return [
            UtilizationSample(
                at=self.clock.now().timestamp(), gpu_percent=percent, memory_mb=8_192
            )
        ]

    def set_utilization(self, job_id: str, percent: float) -> None:
        self.utilization[job_id] = percent

    def list_resources(self) -> list[Resource]:
        """What this backend is holding right now.

        Terminated resources are gone, the way a terminated EC2 instance stops
        showing up in `describe-instances` with a running state.
        """
        out: list[Resource] = []
        for resource in self._resources.values():
            if resource.terminated:
                continue
            observation = self.poll(resource.handle)
            out.append(
                Resource(
                    handle=resource.handle,
                    gpu_type=resource.gpu_type,
                    status=observation.status,
                    tags=dict(resource.tags),
                )
            )
        return out

    # --------------------------------------------------------- simulated work
    #
    # A job with `total_steps` does one step per equal slice of its runtime and
    # accumulates `sum(range(step))` as it goes. That number is the point: it is
    # only correct if every step ran exactly once across every attempt. A resume
    # that starts too early double-counts; one that starts too late skips. Both
    # come out wrong, and neither would be visible from "the job finished".

    def _restore(self, job: Job) -> tuple[int, int]:
        if self.checkpoints is None:
            return 0, 0
        ref = self.checkpoints.latest(job.job_id)
        if ref is None:
            return 0, 0
        directory = Path(tempfile.mkdtemp())
        self.checkpoints.fetch(ref, directory)
        try:
            state = json.loads((directory / "state.json").read_text())
            # `step` here is a *count* of completed steps, so resuming starts at
            # exactly that number. Reading it as a last-completed-step index and
            # adding one silently skips a step per resume, which is invisible in
            # "the job finished" and shows up only in the accumulator.
            return int(state["step"]), int(state["value"])
        except (OSError, ValueError, KeyError):
            return 0, 0

    def steps_done(self, resource: _Resource) -> int:
        """How far this attempt has got, from the clock alone."""
        if resource.total_steps <= 0:
            return 0
        span = max(resource.finish_at - resource.ready_at, 1e-9)
        elapsed = self.clock.now().timestamp() - resource.ready_at
        fraction = min(max(elapsed / span, 0.0), 1.0)
        remaining = resource.total_steps - resource.start_step
        return resource.start_step + int(fraction * remaining)

    def _value_at(self, resource: _Resource, step: int) -> int:
        """The accumulator after `step` steps, counting only this attempt's work
        on top of whatever the checkpoint carried."""
        return resource.start_value + sum(range(resource.start_step, step))

    def _save(self, resource: _Resource, step: int) -> None:
        if self.checkpoints is None or step <= resource.last_saved_step:
            return
        directory = Path(tempfile.mkdtemp())
        (directory / "state.json").write_text(
            json.dumps({"step": step, "value": self._value_at(resource, step)})
        )
        self.checkpoints.put(resource.job_id, step, directory, self.clock.now())
        resource.last_saved_step = step
        self._save_state()

    def _maybe_checkpoint(self, resource: _Resource) -> None:
        if resource.checkpoint_every <= 0 or resource.total_steps <= 0:
            return
        step = self.steps_done(resource)
        boundary = (step // resource.checkpoint_every) * resource.checkpoint_every
        if boundary > resource.last_saved_step and boundary > resource.start_step:
            self._save(resource, boundary)

    def final_value(self, job_id: str) -> int | None:
        """The accumulator a completed job ended on. The gate compares this
        against an uninterrupted control run."""
        if self.checkpoints is None:
            return None
        ref = self.checkpoints.latest(job_id)
        if ref is None:
            return None
        directory = Path(tempfile.mkdtemp())
        self.checkpoints.fetch(ref, directory)
        try:
            return int(json.loads((directory / "state.json").read_text())["value"])
        except (OSError, ValueError, KeyError):
            return None

    # ------------------------------------------------------------- test hooks

    def interrupt(self, handle: str) -> None:
        """Take the capacity away, now.

        Models a spot interruption end to end: the two-minute notice reaches the
        machine, the job is told to checkpoint, it writes whatever it has, and
        then the instance goes. The checkpoint written here is the one a resumed
        attempt will find.
        """
        resource = self._resources.get(handle)
        if resource is None or resource.terminated:
            return
        step = self.steps_done(resource)
        if resource.total_steps > 0 and step > resource.start_step:
            # What the watcher would have uploaded during the notice window.
            self._save(resource, step)
        resource.finish_at = self.clock.now().timestamp()
        resource.outcome = str(BackendStatus.INTERRUPTED)
        resource.logs.append(["broker", f"capacity interrupted at step {step}"])
        self._save_state()

    def orphan(self, handle: str) -> None:
        """Make the backend forget a handle without terminating it.

        Simulates the broker's record and reality diverging in the direction
        that matters least. The opposite direction -- backend holds something we
        have no record of -- is what `leak()` produces.
        """
        self._resources.pop(handle, None)
        self._save_state()

    def leak(self, job_id: str, user_id: str, gpu_type: str = "a10g") -> str:
        """Create a tagged resource the broker has no record of.

        This is the shape of the real failure: the broker crashed between
        `launch()` returning and the handle being committed, so an instance is
        running that nothing will ever clean up.
        """
        now = self.clock.now()
        epoch = now.timestamp()
        self._counter += 1
        handle = f"{self.name}-{self._counter:04d}"
        self._resources[handle] = _Resource(
            handle=handle,
            job_id=job_id,
            user_id=user_id,
            gpu_type=gpu_type,
            launched_at=epoch,
            ready_at=epoch,
            finish_at=epoch + 86400.0,
            outcome=str(BackendStatus.COMPLETED),
            exit_code=0,
            tags={
                "broker-job-id": job_id,
                "broker-user": user_id,
                "broker-launched-at": to_iso(now),
            },
        )
        self._save_state()
        return handle

    # ------------------------------------------------------------ persistence

    def _is_finished(self, resource: _Resource) -> bool:
        if resource.terminated:
            return True
        return self.clock.now().timestamp() >= resource.finish_at

    def _load(self) -> None:
        if not self.state_path or not self.state_path.exists():
            return
        data = json.loads(self.state_path.read_text())
        self._counter = data.get("counter", 0)
        self._resources = {
            handle: _Resource.from_json(raw)
            for handle, raw in data.get("resources", {}).items()
        }

    def _save_state(self) -> None:
        """Atomic write. A fake that corrupts its own state file on a crash
        would fail the durability tests for the wrong reason."""
        if not self.state_path:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "counter": self._counter,
            "resources": {
                handle: resource.to_json() for handle, resource in self._resources.items()
            },
        }
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        os.replace(tmp, self.state_path)


def _closing_line(resource: _Resource) -> str:
    return f"{resource.job_id[:8]}: {resource.outcome.lower()} (exit {resource.exit_code})"
