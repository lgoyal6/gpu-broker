"""The job lifecycle, as an explicit graph.

Every later phase adds states -- preempted, checkpointing, resuming, reclaimed --
so the transition table is declared up front and in one place. Phase 0 only
*drives* the subset it needs, but the edges the later phases will use are already
named here so that adding them is a change to the code that uses them, not a
rediscovery of what the legal shape was.

The rule that matters: a job's state only ever changes through
`Store.transition()`, which checks this table and commits one transaction.
"""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    # --- Phase 0 ---
    QUEUED = "QUEUED"
    """Admitted, budget reserved, waiting for capacity."""

    ALLOCATING = "ALLOCATING"
    """A backend is bringing up a machine. Costs may already be accruing."""

    RUNNING = "RUNNING"
    """The user's command is executing."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    REFUSED = "REFUSED"
    """Never entered the queue. Recorded anyway, so 'why was I refused' is
    answerable a week later."""

    # --- Phase 4 (spot / checkpoint / resume) ---
    CHECKPOINTING = "CHECKPOINTING"
    """Interruption notice received; the job is writing its checkpoint."""

    PREEMPTED = "PREEMPTED"
    """Capacity was taken away. The job goes back in the queue."""

    RESUMING = "RESUMING"
    """A machine is up and the job is restoring from its last checkpoint."""

    # --- Phase 3 (idle reclaim) ---
    RECLAIMED = "RECLAIMED"
    """Held a GPU without using it, was notified, did not respond, was stopped."""


TERMINAL: frozenset[JobState] = frozenset(
    {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.CANCELLED,
        JobState.REFUSED,
        JobState.RECLAIMED,
    }
)

ACTIVE: frozenset[JobState] = frozenset(
    {
        JobState.ALLOCATING,
        JobState.RUNNING,
        JobState.CHECKPOINTING,
        JobState.RESUMING,
    }
)
"""States in which a backend is holding real capacity on this job's behalf.
Reconciliation and `gpu who` both key off this set."""


HOLDS_BUDGET: frozenset[JobState] = frozenset({JobState.QUEUED}) | ACTIVE | frozenset(
    {JobState.PREEMPTED}
)
"""States in which a reservation is still outstanding. Leaving any of these for a
terminal state must release whatever is left of the hold."""


LEGAL_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.QUEUED: frozenset(
        {
            JobState.ALLOCATING,
            JobState.CANCELLED,
            JobState.FAILED,
        }
    ),
    JobState.ALLOCATING: frozenset(
        {
            JobState.RUNNING,
            JobState.RESUMING,  # phase 4: this job has a checkpoint to restore
            JobState.COMPLETED,
            # ^ The broker polls on an interval. A short job can boot, run, and
            # exit between two polls, so RUNNING is never observed. Without this
            # edge such a job wedges in ALLOCATING forever, and the shorter the
            # job the more likely it is -- exactly the jobs a new member runs
            # first while deciding whether the broker works.
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.PREEMPTED,  # phase 4: spot pulled before we ever ran
        }
    ),
    JobState.RESUMING: frozenset(
        {
            JobState.RUNNING,
            JobState.COMPLETED,  # same coarse-poll case as above
            JobState.FAILED,
            JobState.CANCELLED,
        }
    ),
    JobState.RUNNING: frozenset(
        {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.CANCELLED,
            JobState.CHECKPOINTING,  # phase 4
            JobState.PREEMPTED,      # phase 4: no time to checkpoint
            JobState.RECLAIMED,      # phase 3
        }
    ),
    JobState.CHECKPOINTING: frozenset(
        {
            JobState.PREEMPTED,
            JobState.FAILED,
            JobState.COMPLETED,  # finished while writing its checkpoint
        }
    ),
    JobState.PREEMPTED: frozenset(
        {
            JobState.QUEUED,     # back in line, resumes from checkpoint
            JobState.CANCELLED,
            JobState.FAILED,
        }
    ),
    # Terminal states have no outgoing edges. Listed so the table is total and
    # `LEGAL_TRANSITIONS[state]` never raises KeyError.
    JobState.COMPLETED: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.CANCELLED: frozenset(),
    JobState.REFUSED: frozenset(),
    JobState.RECLAIMED: frozenset(),
}


def can_transition(from_state: JobState, to_state: JobState) -> bool:
    return to_state in LEGAL_TRANSITIONS[from_state]



def _assert_table_is_total() -> None:
    """Every state is a key, and every target is a real state.

    Runs at import. A typo in the table above is a startup failure, not a
    mystery three phases later.
    """
    missing = set(JobState) - set(LEGAL_TRANSITIONS)
    if missing:
        raise AssertionError(f"LEGAL_TRANSITIONS is missing states: {sorted(missing)}")
    for source, targets in LEGAL_TRANSITIONS.items():
        for target in targets:
            if target not in JobState:
                raise AssertionError(f"{source} -> {target!r} is not a JobState")
        if source in TERMINAL and targets:
            raise AssertionError(f"{source} is terminal but has outgoing edges {targets}")


_assert_table_is_total()
