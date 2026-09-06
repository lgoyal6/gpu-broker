"""One pass of the broker: settle, observe, order, dispatch.

`tick()` is idempotent in the sense that matters -- it derives everything it
needs from committed state, so calling it after a crash picks up exactly where
the last committed transaction left off. There is no in-memory bookkeeping to
lose.

Ordering rules, both deliberate:

  Pool cap blocks the head of the queue. If the top job cannot fit under the
  pool-wide cap, scanning stops. Letting cheaper jobs backfill past it would
  keep more capacity busy and would also let a large job sit at the top of the
  queue forever. A hard stop keeps the promise that being first in line means
  something.

  Missing capacity does not. A job waiting on an A100 should not hold up a job
  that wants a T4. Per-type scarcity skips; pool-wide scarcity stops.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from dataclasses import replace
from decimal import Decimal

from .backends.base import Backend, BackendStatus
from .checkpoint import CheckpointStore
from .config import BrokerConfig
from .errors import BackendError
from .fairshare import order_queue
from .idle import evaluate, notify
from .metrics import Metrics
from .metrics import collect as collect_metrics
from .models import Decision, Drift, DryRunPlan, Job, Priority, Sample, TickReport
from .placement import Placement, choose
from .money import ZERO, Currency, fmt, money, quantize
from .states import TERMINAL, JobState, can_transition
from .store import Store


class Scheduler:
    def __init__(
        self,
        store: Store,
        backends: list[Backend],
        config: BrokerConfig,
        checkpoints: CheckpointStore | None = None,
    ) -> None:
        self.store = store
        self.backends = list(backends)
        self.config = config
        self.clock = store.clock
        self.checkpoints = checkpoints

    def backend(self, name: str) -> Backend | None:
        for backend in self.backends:
            if backend.name == name:
                return backend
        return None

    # ------------------------------------------------------------------ tick

    def tick(self) -> TickReport:
        now = self.clock.now()
        completed: list[str] = []
        failed: list[str] = []
        settled: list[str] = []
        ceilinged: list[str] = []

        for job in self.store.active_jobs():
            outcome = self._advance(job, now)
            if outcome.settled:
                settled.append(job.job_id)
            if outcome.hit_ceiling:
                ceilinged.append(job.job_id)
            if outcome.final_state is JobState.COMPLETED:
                completed.append(job.job_id)
            elif outcome.final_state in (JobState.FAILED, JobState.CANCELLED):
                failed.append(job.job_id)

        (
            dispatched,
            blocked_pool,
            blocked_capacity,
            blocked_tenant,
        ) = self._dispatch(now)

        # After dispatch, so the gauges describe the state this tick left behind
        # rather than the one it found. Failures here must not take down a tick:
        # losing a data point is a gap in a chart, losing a tick is a job that
        # did not start.
        try:
            Metrics(self.store).record(
                collect_metrics(self.store, self.config, self.clock, self.backends)
            )
        except Exception:  # noqa: BLE001 - deliberately total
            # Losing a data point is a gap in a chart. Losing a tick is a job
            # that did not start. Never let the second happen for the first.
            pass

        return TickReport(
            at=now,
            dispatched=tuple(dispatched),
            completed=tuple(completed),
            failed=tuple(failed),
            settled=tuple(settled),
            blocked_on_pool_cap=tuple(blocked_pool),
            blocked_on_capacity=tuple(blocked_capacity),
            blocked_on_tenant=tuple(blocked_tenant),
            stopped_at_ceiling=tuple(ceilinged),
        )

    # --------------------------------------------------------- running jobs

    class _Outcome:
        __slots__ = ("settled", "hit_ceiling", "preempted", "final_state")

        def __init__(self) -> None:
            self.settled = False
            self.hit_ceiling = False
            self.preempted = False
            self.final_state: JobState | None = None

    def _advance(self, job: Job, now: dt.datetime) -> "Scheduler._Outcome":
        outcome = self._Outcome()
        backend = self.backend(job.backend or "")
        if backend is None or job.backend_handle is None:
            # We think this job is running but have no way to look at it. That is
            # drift, and `reconcile()` reports it; do not guess here.
            return outcome

        accrued = self._accrue(job, now)
        if accrued > ZERO:
            self.store.settle(job, accrued, note=f"{job.elapsed_hours(now):.4f}h elapsed")
            outcome.settled = True

        if job.cancel_requested:
            # Somebody asked for this from a page that cannot reach machines.
            # This tick can.
            backend.terminate(job.backend_handle or "", f"cancelled by {job.cancel_requested}")
            job = self._drain_logs(job, backend)
            outcome.final_state = self.store.transition(
                job,
                JobState.CANCELLED,
                reason=f"cancelled by {job.cancel_requested}",
                clear_handle=True,
            ).state
            return outcome

        observation = backend.poll(job.backend_handle)
        job = self._drain_logs(job, backend)
        self._sample(job, backend)

        if observation.status is BackendStatus.READY:
            # The machine is up and billing, but nothing is running on it yet.
            # This is the one mutation the scheduler makes on a running job, and
            # it lives here rather than inside `poll` so that it is explicit,
            # happens once, and can be seen in a dry run.
            try:
                backend.start(job.backend_handle, job)
            except BackendError as exc:
                self.store.append_log(job.job_id, "broker", f"could not start: {exc}")
                backend.terminate(job.backend_handle, "failed to start")
                outcome.final_state = self.store.transition(
                    job, JobState.FAILED, reason=f"could not start: {exc}", clear_handle=True
                ).state
                return outcome
            self.store.append_log(job.job_id, "broker", "command sent; job is running")
            return outcome

        target = _state_for(observation.status)

        if target is JobState.PREEMPTED:
            return self._preempt(job, backend, observation, outcome)

        # The backend's word comes first. A job that finished on the same tick it
        # reached its budget ceiling completed; it was not killed. Reporting that
        # backwards would put a successful run in somebody's failure column.
        if target in TERMINAL:
            # Release the capacity before recording the state. A finished job
            # whose instance is still up is precisely the failure this broker
            # exists to prevent: nobody is watching it, and it bills until
            # somebody notices. `terminate` is required to be safe on an
            # already-dead handle, so doing it on every terminal path costs
            # nothing and closes the case where the backend finished on its own.
            backend.terminate(job.backend_handle, f"job {target.lower()}")
            outcome.final_state = self.store.transition(
                job,
                target,
                reason=observation.detail or f"backend reported {observation.status}",
                exit_code=observation.exit_code,
                clear_handle=True,
            ).state
            return outcome

        if self._at_ceiling(job, now):
            backend.terminate(job.backend_handle, "budget ceiling reached")
            job = self._drain_logs(job, backend)
            self.store.transition(
                job,
                JobState.FAILED,
                reason=(
                    f"stopped at its {fmt(job.reserved, job.currency)} ceiling after "
                    f"{job.elapsed_hours(now):.2f}h"
                ),
                clear_handle=True,
            )
            outcome.hit_ceiling = True
            outcome.final_state = JobState.FAILED
            return outcome

        if target is JobState.ALLOCATING and job.resumable and job.state is JobState.ALLOCATING:
            # The machine is coming up and this job has a checkpoint to restore.
            # That is a different thing from a fresh allocation and the queue
            # should say so.
            target = JobState.RESUMING

        if target is None or target == job.state:
            return outcome

        if not can_transition(job.state, target):
            # The backend reported something less advanced than what we already
            # recorded -- a stale poll, or an eventually-consistent API answering
            # from before the change. `describe-instances` does this. Our record
            # is the more advanced of the two, so keep it. A job must never walk
            # backwards through its own lifecycle because of a slow read.
            self.store.append_log(
                job.job_id,
                "broker",
                f"ignored stale {observation.status} from {backend.name}; "
                f"job is already {job.state}",
            )
            return outcome

        outcome.final_state = self.store.transition(
            job,
            target,
            reason=observation.detail or f"backend reported {observation.status}",
            exit_code=observation.exit_code,
        ).state
        return outcome

    def _preempt(
        self,
        job: Job,
        backend: Backend,
        observation,
        outcome: "Scheduler._Outcome",
    ) -> "Scheduler._Outcome":
        """The provider took the machine. Save what survived and get back in line.

        Not a terminal state, so the budget reservation stays outstanding: this
        job is not finished, it is between attempts. What it already burned stays
        settled, because that time was really spent.
        """
        backend.terminate(job.backend_handle or "", "preempted")

        progressed = self._collect_checkpoint(job)
        if progressed is not None:
            job = progressed

        job = self.store.record_preemption(job)
        job = self.store.transition(
            job,
            JobState.PREEMPTED,
            reason=observation.detail or "capacity was taken away",
            clear_handle=True,
        )

        made_progress = progressed is not None
        if not made_progress and job.preemptions >= self.config.spot_max_preemptions_without_progress:
            # Repeatedly losing the same hour costs more than on-demand would
            # have. Stop offering this job the cheap tier -- but only if there is
            # somewhere else for it to go. Pinning a job to a tier with no
            # backend behind it strands it in the queue forever, and the message
            # it leaves ("no backend is in any configured tier") points at the
            # config rather than at what actually happened.
            fallback = self._fallback_tier(job)
            if fallback is None:
                self.store.append_log(
                    job.job_id,
                    "broker",
                    f"preempted {job.preemptions} times with nothing saved, but no "
                    f"cheaper-than-spot alternative is configured, so it stays here",
                )
                return self._requeue(job, outcome)
            job = self.store.pin_tier(
                job,
                fallback,
                f"preempted {job.preemptions} times with nothing saved",
            )
            self.store.notify(
                user_id=job.user_id,
                job_id=job.job_id,
                kind="pinned-ondemand",
                message=(
                    f"Job {job.short_id} has been preempted {job.preemptions} times "
                    "without saving a checkpoint, so it will run on on-demand "
                    "capacity from now on. To use the cheaper spot pool, have it "
                    "write checkpoints -- see the Checkpointer in the README."
                ),
            )

        return self._requeue(job, outcome)

    def _requeue(self, job: Job, outcome: "Scheduler._Outcome") -> "Scheduler._Outcome":
        where = (
            f"resuming from step {job.checkpoint_step}"
            if job.resumable
            else "starting again from the beginning"
        )
        self.store.append_log(job.job_id, "broker", f"preempted; requeued, {where}")
        outcome.final_state = self.store.transition(
            job, JobState.QUEUED, reason=f"requeued after preemption ({where})"
        ).state
        outcome.preempted = True
        return outcome

    def _fallback_tier(self, job: Job) -> str | None:
        """A tier below this job's current one that can actually run it.

        Returns None when there is nothing to fall back to, which is the case
        that has to be handled rather than assumed away: a club running spot
        only, or a gpu type no on-demand backend offers.
        """
        gpu = self.config.gpu(job.gpu_type)
        order = self.config.placement_order or ()
        current = job.pinned_tier or "spot"
        after = order[order.index(current) + 1 :] if current in order else order
        for tier in after:
            for backend in self.backends:
                if (
                    getattr(backend, "tier", "ondemand") == tier
                    and backend.currency is gpu.currency
                    and backend.supports(job.gpu_type)
                ):
                    return tier
        return None

    def _collect_checkpoint(self, job: Job) -> Job | None:
        """Pick up anything the job managed to save. None means it saved nothing new."""
        if self.checkpoints is None:
            return None
        try:
            ref = self.checkpoints.latest(job.job_id)
        except Exception as exc:  # noqa: BLE001 - S3, disk, anything
            self.store.append_log(
                job.job_id, "broker", f"could not look for a checkpoint: {exc}"
            )
            return None
        if ref is None:
            return None
        if job.checkpoint_step is not None and ref.step <= job.checkpoint_step:
            return None
        return self.store.record_checkpoint(job, ref.step, ref.key)

    def _accrue(self, job: Job, now: dt.datetime) -> Decimal:
        """Cost incurred since the last settlement, never more than the ceiling.

        Derived from elapsed time and the ledger, not from a counter, so a crash
        between two ticks loses nothing.
        """
        gpu = self.config.gpu(job.gpu_type)
        total = gpu.hourly_price * money(job.elapsed_hours(now))
        total = min(quantize(total, job.currency), job.reserved)
        already = self.store.job_spend(job.job_id)
        return max(ZERO, total - already)

    def _at_ceiling(self, job: Job, now: dt.datetime) -> bool:
        gpu = self.config.gpu(job.gpu_type)
        would_be = gpu.hourly_price * money(job.elapsed_hours(now))
        return quantize(would_be, job.currency) >= job.reserved

    def _sample(self, job: Job, backend: Backend) -> None:
        """Record what this job's GPU has been doing, on an interval.

        On an interval rather than every tick: a tick can be seconds apart, and
        a sample costs a round trip to the machine. The interval is also what
        `idle_min_samples` is counted in, so shortening it makes the idle
        decision faster and noisier in equal measure.
        """
        now = self.clock.now()
        last = self.store.last_sample_at(job.job_id)
        if (
            last is not None
            and (now - last).total_seconds() < self.config.idle_sample_interval_seconds
        ):
            return

        try:
            readings = backend.sample_utilization(job.backend_handle or "")
        except BackendError:
            return
        if not readings:
            return

        # A backend that re-reads its source from the beginning after a restart
        # would otherwise write every sample twice, and a duplicated run of
        # zeroes is exactly what triggers a reclaim.
        cutoff = last.timestamp() if last else 0.0
        fresh = [reading for reading in readings if reading.at > cutoff]
        if not fresh:
            return

        self.store.record_samples(
            job.job_id,
            [
                Sample(
                    at=dt.datetime.fromtimestamp(reading.at, dt.timezone.utc),
                    gpu_percent=reading.gpu_percent,
                    memory_mb=reading.memory_mb,
                    source=backend.name,
                )
                for reading in fresh
            ],
        )

        verdict = evaluate(self.store, job, self.config, self.clock)
        if verdict.idle and not verdict.notified:
            notify(self.store, verdict, self.config)

    def _drain_logs(self, job: Job, backend: Backend) -> Job:
        """Pull whatever the backend has produced since we last looked.

        The cursor is the job's own `logs_fetched`, not the number of rows in the
        log table. The broker writes its own lines there too ("dispatched to
        ec2/i-..."), and counting those would advance the cursor past real output
        and drop it on the floor.
        """
        try:
            new_lines = backend.fetch_logs(job.backend_handle or "", after=job.logs_fetched)
        except BackendError as exc:
            self.store.append_log(job.job_id, "broker", f"could not read logs: {exc}")
            return job
        if not new_lines:
            return job
        cursor = self.store.append_backend_logs(job, new_lines)
        return replace(job, logs_fetched=cursor)

    # ---------------------------------------------------------------- queue

    def ordered_queue(self, now: dt.datetime | None = None) -> list[tuple[Job, Priority]]:
        now = now or self.clock.now()
        return order_queue(self.store.queued_jobs(), self.store, now, self.config)

    def decisions(self, now: dt.datetime) -> Iterator[tuple[Decision, Backend | None]]:
        """Walk the queue and decide each job's fate, changing nothing.

        The single source of truth for scheduling policy. `_dispatch` consumes
        this and acts; `plan` consumes it and reports. Writing the rules twice
        would let a dry run and a real tick disagree, which defeats the purpose
        of having a dry run.
        """
        # Headroom, not the display balance: a queued job's reservation must not
        # count against the cap that decides whether it may be dispatched.
        headroom = {
            currency: self.store.pool_headroom(currency) for currency in Currency
        }
        committed: dict[Currency, Decimal] = {currency: ZERO for currency in Currency}

        queue = self.ordered_queue(now)
        blocked_currencies: set[Currency] = set()
        # How many machines each member is already holding. Counted once, then
        # kept up to date as this pass commits dispatches, so a member cannot
        # take the whole pool inside a single tick -- which is what happened
        # when it was measured: 63 of 64 free slots to one person.
        held: dict[str, int] = {}

        for index, (job, _priority) in enumerate(queue):
            if job.currency in blocked_currencies:
                yield Decision(job, "BLOCKED_POOL", None, "pool cap already reached"), None
                continue

            if job.user_id not in held:
                held[job.user_id] = self.store.running_count(job.user_id)
            if held[job.user_id] >= self.config.max_running_jobs_per_user:
                # Blocked, not refused: the job keeps its place in the queue and
                # goes as soon as one of this member's jobs finishes. Skipping
                # to the next job is right here in a way it is not for the pool
                # cap -- this job is not waiting on capacity the pool lacks, it
                # is waiting on its own owner.
                yield Decision(
                    job,
                    "BLOCKED_TENANT",
                    None,
                    f"{job.user_id} is holding {held[job.user_id]} jobs, the limit "
                    f"is {self.config.max_running_jobs_per_user}",
                ), None
                continue

            remaining = headroom[job.currency] - committed[job.currency]
            if job.reserved > remaining:
                # Hard stop for this currency. See the module docstring: the head
                # of the queue keeps its place instead of being backfilled past
                # forever. Other currencies are unaffected -- a full dollar pool
                # should not idle the free lab machine.
                blocked_currencies.add(job.currency)
                yield Decision(
                    job,
                    "BLOCKED_POOL",
                    None,
                    f"needs {fmt(job.reserved, job.currency)}, pool has "
                    f"{fmt(remaining, job.currency)} free",
                ), None
                if len(blocked_currencies) == len(Currency):
                    for other, _ in queue[index + 1 :]:
                        yield Decision(other, "BLOCKED_POOL", None, "pool cap reached"), None
                    return
                continue

            placement = self._place(job)
            if not placement.placed:
                yield Decision(
                    job, "BLOCKED_CAPACITY", None, placement.reason
                ), None
                continue

            committed[job.currency] += job.reserved
            held[job.user_id] += 1
            yield Decision(
                job, "DISPATCH", placement.backend.name, placement.one_line()
            ), placement.backend

    def plan(self, now: dt.datetime | None = None) -> DryRunPlan:
        """A dry run. Decides everything a tick would, launches nothing.

        Where a backend can ask its provider whether the call would work -- EC2
        has DryRun, which checks IAM and every parameter against the real account
        -- it does. A missing permission found here costs nothing; found on the
        first real launch it costs a half-built instance and a confused member.
        """
        now = now or self.clock.now()
        decisions: list[Decision] = []
        checks: list[tuple[str, str]] = []
        problems: list[tuple[str, str]] = []

        for decision, backend in self.decisions(now):
            decisions.append(decision)
            if backend is None:
                continue
            try:
                checks.append((decision.job.job_id, backend.validate_launch(decision.job)))
            except BackendError as exc:
                problems.append((decision.job.job_id, str(exc)))

        return DryRunPlan(
            at=now,
            decisions=tuple(decisions),
            checks=tuple(checks),
            problems=tuple(problems),
        )

    def _dispatch(
        self, now: dt.datetime
    ) -> tuple[list[str], list[str], list[str], list[str]]:
        dispatched: list[str] = []
        blocked_pool: list[str] = []
        blocked_capacity: list[str] = []
        blocked_tenant: list[str] = []

        for decision, backend in self.decisions(now):
            job = decision.job
            if decision.action == "BLOCKED_POOL":
                blocked_pool.append(job.job_id)
                continue
            if decision.action == "BLOCKED_TENANT":
                blocked_tenant.append(job.job_id)
                continue
            if decision.action == "BLOCKED_CAPACITY" or backend is None:
                blocked_capacity.append(job.job_id)
                continue

            try:
                allocation = backend.launch(job)
            except BackendError as exc:
                self.store.append_log(job.job_id, "broker", f"launch failed: {exc}")
                blocked_capacity.append(job.job_id)
                continue

            job = self.store.record_attempt(job)
            self.store.transition(
                job,
                JobState.ALLOCATING,
                reason=(
                    f"placed on {backend.name} ({allocation.handle})"
                    + (f", attempt {job.attempts}" if job.attempts > 1 else "")
                ),
                backend=backend.name,
                backend_handle=allocation.handle,
            )
            self.store.append_log(
                job.job_id,
                "broker",
                f"dispatched to {backend.name}/{allocation.handle} on {job.gpu_type}",
            )
            # The reasoning, not just the outcome. When somebody asks why their
            # job cost money, the answer is on their own job's log.
            self.store.append_log(job.job_id, "broker", f"placement: {decision.detail}")
            dispatched.append(job.job_id)

        return (
            dispatched,
            _dedupe(blocked_pool),
            blocked_capacity,
            blocked_tenant,
        )

    def _place(self, job: Job) -> Placement:
        return choose(job, self.backends, self.config)

    # ---------------------------------------------------------- reconciliation

    def reconcile(self) -> list[Drift]:
        """Compare every backend's reality against our records.

        Reports. Never terminates. Until somebody has watched this be right for
        a few weeks, an automatic cleanup is a way to delete a working job.
        """
        drifts: list[Drift] = []
        our_active = {
            (job.backend, job.backend_handle): job
            for job in self.store.active_jobs()
            if job.backend_handle
        }
        seen: set[tuple[str, str]] = set()

        for backend in self.backends:
            for resource in backend.list_resources():
                key = (backend.name, resource.handle)
                seen.add(key)

                if resource.job_id is None:
                    drifts.append(
                        Drift(
                            kind="UNTAGGED",
                            backend=backend.name,
                            handle=resource.handle,
                            job_id=None,
                            detail=(
                                "holding capacity with no broker-job-id tag. "
                                "The broker did not create this, or created it and "
                                "died before tagging."
                            ),
                        )
                    )
                    continue

                job = our_active.get(key)
                if job is None:
                    known = self._known_state(resource.job_id)
                    drifts.append(
                        Drift(
                            kind="ORPHAN",
                            backend=backend.name,
                            handle=resource.handle,
                            job_id=resource.job_id,
                            detail=(
                                f"backend is holding this for {resource.user_id}, but "
                                f"our record says {known}. Nothing will clean it up."
                            ),
                        )
                    )

        for (backend_name, handle), job in our_active.items():
            if (backend_name, handle) in seen:
                continue
            drifts.append(
                Drift(
                    kind="LOST",
                    backend=backend_name or "?",
                    handle=handle or "?",
                    job_id=job.job_id,
                    detail=(
                        f"we record this job as {job.state} on {backend_name}, but the "
                        "backend has no such resource. It may have been terminated "
                        "outside the broker."
                    ),
                )
            )

        return drifts

    def _known_state(self, job_id: str) -> str:
        try:
            return str(self.store.get_job(job_id).state)
        except Exception:
            return "no job record at all"


def _state_for(status: BackendStatus) -> JobState | None:
    """Translate what a backend saw into what the broker calls it."""
    return {
        BackendStatus.PENDING: JobState.ALLOCATING,
        # READY means the machine is up but idle. Still allocating, from the
        # job's point of view: nothing of the user's is running yet.
        BackendStatus.READY: JobState.ALLOCATING,
        BackendStatus.RUNNING: JobState.RUNNING,
        BackendStatus.COMPLETED: JobState.COMPLETED,
        BackendStatus.FAILED: JobState.FAILED,
        # Capacity taken away by the provider. Not a failure: the job goes back
        # in the queue and resumes from its last checkpoint.
        BackendStatus.INTERRUPTED: JobState.PREEMPTED,
        BackendStatus.GONE: JobState.FAILED,
    }.get(status)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out
